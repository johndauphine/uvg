mod cli;
mod codegen;
mod db;
mod ddl_typemap;
mod dialect;
mod error;
mod introspect;
mod naming;
mod output;
mod schema;
#[cfg(test)]
mod testutil;
mod tui;
mod typemap;

use std::fs;

use anyhow::Result;
use clap::Parser;
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

use crate::cli::{Cli, ConnectionConfig};
use crate::codegen::declarative::DeclarativeGenerator;
use crate::codegen::ddl_diff::compute_changes;
use crate::codegen::tables::TablesGenerator;
use crate::codegen::Generator;
use crate::output::{apply_order, write_split_changes, Manifest, OutputContext};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    if cli.interactive {
        return tui::run(cli).await;
    }

    let config = cli.parse_connection()?;
    let dialect = config.dialect();
    // MySQL default schema = database name from URL; others use static defaults.
    let schemas = if let Some(db) = config.database_name() {
        cli.schema_list_or(&db)
    } else {
        cli.schema_list_or(dialect.default_schema())
    };
    let table_filter = cli.table_list();
    let options = cli.generator_options();

    tracing::debug!("Connecting to database...");

    let schema = match config {
        ConnectionConfig::Postgres(url) => {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            tracing::debug!("Introspecting schema...");
            let s = introspect::pg::introspect(
                &pool,
                &schemas,
                &table_filter,
                cli.noviews,
                &options,
            )
            .await;
            pool.close().await;
            s?
        }
        ConnectionConfig::Mssql {
            host,
            port,
            database,
            user,
            password,
            trust_cert,
        } => {
            let mut client =
                introspect::mssql::connect(&host, port, &database, &user, &password, trust_cert)
                    .await?;
            tracing::debug!("Introspecting schema...");
            introspect::mssql::introspect(
                &mut client,
                &schemas,
                &table_filter,
                cli.noviews,
                &options,
            )
            .await?
        }
        ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            tracing::debug!("Introspecting schema...");
            let s = introspect::mysql::introspect(
                &pool,
                &schemas,
                &table_filter,
                cli.noviews,
                &options,
            )
            .await;
            pool.close().await;
            s?
        }
        ConnectionConfig::Sqlite(url) => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            tracing::debug!("Introspecting schema...");
            let s = introspect::sqlite::introspect(
                &pool,
                &table_filter,
                cli.noviews,
                &options,
            )
            .await;
            pool.close().await;
            s?
        }
    };

    tracing::debug!("Found {} tables/views", schema.tables.len());

    match cli.generator.as_str() {
        "tables" => {
            if cli.split_tables {
                let files = TablesGenerator.generate_split(&schema, &options);
                write_split_output(&files, &cli.outfile)?;
            } else {
                write_output(&TablesGenerator.generate(&schema, &options), &cli.outfile)?;
            }
        }
        "declarative" => {
            if cli.split_tables {
                let files = DeclarativeGenerator.generate_split(&schema, &options);
                write_split_output(&files, &cli.outfile)?;
            } else {
                write_output(&DeclarativeGenerator.generate(&schema, &options), &cli.outfile)?;
            }
        }
        "ddl" => {
            use crate::codegen::ddl::{DdlGenerator, DdlOutput};

            let ddl_opts = cli.ddl_options(dialect)?;

            // --apply requires a target URL — fail fast before we do any
            // work introspecting source/target schemas.
            if cli.apply && cli.target_url.is_none() {
                return Err(anyhow::anyhow!(
                    "--apply requires a target database URL to execute against"
                ));
            }

            // If a target URL is provided, introspect it for diff
            let target_schema = if let Some(ref target_url) = cli.target_url {
                let target_config = cli.parse_target_connection(target_url)?;
                let target_dialect = target_config.dialect();
                let target_schemas = if let Some(db) = target_config.database_name() {
                    cli.schema_list_or(&db)
                } else {
                    cli.schema_list_or(target_dialect.default_schema())
                };
                Some(
                    db::introspect_with_config(
                        target_config,
                        &target_schemas,
                        &table_filter,
                        cli.noviews,
                        &options,
                    )
                    .await?,
                )
            } else {
                None
            };

            // --out-dir: per-table diff layout. Only kicks in when there's
            // a target to diff against and --outfile is not set (--outfile
            // wins per docs/migration-output-layout.md).
            if cli.outfile.is_none() {
                if let Some(ref out_dir) = cli.out_dir {
                    let Some(target) = target_schema.as_ref() else {
                        return Err(anyhow::anyhow!(
                            "--out-dir requires a target database URL to diff against"
                        ));
                    };
                    let changes = compute_changes(&schema, target, &ddl_opts);
                    let ctx = OutputContext::now(
                        out_dir.clone(),
                        cli.name.clone(),
                        dialect,
                        ddl_opts.target_dialect,
                    );
                    let run_id = ctx.run_id.clone();
                    match write_split_changes(&changes, &ctx)? {
                        None => {
                            eprintln!("uvg: no schema changes");
                        }
                        Some(manifest) => {
                            eprintln!(
                                "uvg: wrote {} file(s) under {} (manifest: _runs/{}.json)",
                                manifest.files.len(),
                                out_dir.display(),
                                run_id,
                            );
                            if cli.apply {
                                let target_url = cli.target_url.as_ref().unwrap();
                                let target_config = cli.parse_target_connection(target_url)?;
                                let applied = apply_manifest(&target_config, out_dir, &manifest).await?;
                                eprintln!(
                                    "uvg: applied {applied} statement(s) across {} table(s) to {target_url}",
                                    manifest.files.len(),
                                );
                            }
                        }
                    }
                    return Ok(());
                }
            }

            let gen = DdlGenerator;
            let ddl_output = gen.generate(&schema, target_schema.as_ref(), &ddl_opts);

            match ddl_output {
                DdlOutput::Single(content) => {
                    // --apply: execute against the target and suppress stdout
                    // (the user got what they asked for via the eprintln summary;
                    // dumping the DDL to stdout in addition is noise). If they
                    // also want a file artifact they can pass --outfile.
                    if cli.apply {
                        let target_url = cli.target_url.as_ref().unwrap();
                        let target_config = cli.parse_target_connection(target_url)?;
                        if cli.outfile.is_some() {
                            write_output(&content, &cli.outfile)?;
                        }
                        let applied = apply_blob(&target_config, &content).await?;
                        eprintln!(
                            "uvg: applied {applied} statement(s) to {target_url}"
                        );
                    } else {
                        write_output(&content, &cli.outfile)?;
                    }
                }
                DdlOutput::Split(files) => {
                    if cli.apply {
                        // The Split output path is used by --split-tables for
                        // Python codegen; mixing it with --apply (an executable
                        // DDL operation) crosses purposes. Per-table apply is
                        // available via --out-dir.
                        return Err(anyhow::anyhow!(
                            "--apply with --split-tables is not supported (use --out-dir for per-table apply)"
                        ));
                    }
                    match cli.outfile {
                        Some(ref dir) => {
                            let dir_path = std::path::PathBuf::from(dir);
                            fs::create_dir_all(&dir_path)?;
                            for (filename, content) in &files {
                                let path = dir_path.join(filename);
                                fs::write(&path, content)?;
                                tracing::info!("Written {}", path.display());
                            }
                        }
                        None => {
                            for (filename, content) in &files {
                                println!("-- File: {filename}");
                                print!("{content}\n");
                            }
                        }
                    }
                }
            }
        }
        other => {
            return Err(error::UvgError::UnknownGenerator(other.to_string()).into());
        }
    };

    Ok(())
}

fn write_split_output(files: &[(String, String)], outfile: &Option<String>) -> anyhow::Result<()> {
    match outfile {
        Some(ref dir) => {
            let dir_path = std::path::PathBuf::from(dir);
            fs::create_dir_all(&dir_path)?;
            for (filename, content) in files {
                let path = dir_path.join(filename);
                fs::write(&path, content)?;
                tracing::info!("Written {}", path.display());
            }
        }
        None => {
            for (filename, content) in files {
                println!("# --- {filename} ---");
                print!("{content}");
            }
        }
    }
    Ok(())
}

fn write_output(output: &str, outfile: &Option<String>) -> anyhow::Result<()> {
    match outfile {
        Some(ref path) => {
            fs::write(path, output)?;
            tracing::info!("Output written to {path}");
        }
        None => {
            print!("{output}");
        }
    }
    Ok(())
}

/// Apply a single DDL blob to the target. Returns the count of
/// successful statements. On any failure, returns a contextual error
/// quoting the offending statement and the database's error message —
/// the binary then exits non-zero, which is load-bearing for CI/scripted
/// callers per issue #57's "side benefits" section.
async fn apply_blob(target_config: &ConnectionConfig, sql: &str) -> anyhow::Result<usize> {
    let results = db::execute_ddl(target_config, sql).await?;
    let applied = results.iter().filter(|r| r.error.is_none()).count();
    if let Some(failed) = results.iter().find(|r| r.error.is_some()) {
        let first_line = failed.sql.lines().next().unwrap_or("").trim();
        return Err(anyhow::anyhow!(
            "DDL apply failed after {applied} statement(s); first failure:\n  {first_line}\n  Error: {}",
            failed.error.as_ref().unwrap()
        ));
    }
    Ok(applied)
}

/// Apply every `.sql` file referenced by a manifest, in manifest order
/// (which is `_schema/` first, then table files in topological FK order
/// — see [`output::apply_order`] and `test_manifest_preserves_topological_order`).
/// Returns the total count of statements applied across all files.
async fn apply_manifest(
    target_config: &ConnectionConfig,
    out_dir: &std::path::Path,
    manifest: &Manifest,
) -> anyhow::Result<usize> {
    let mut total = 0;
    for path in apply_order(manifest, out_dir) {
        let sql = fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
        total += apply_blob(target_config, &sql).await?;
    }
    Ok(total)
}
