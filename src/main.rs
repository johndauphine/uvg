use std::fs;
use std::path::Path;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

use uvg::apply::{apply_inline, apply_manifest, ApplyOptions};
use uvg::cli::{Cli, Command, ConnectionConfig, GeneratorOptions, SnapshotCommand};
use uvg::codegen::ddl_diff::{compute_changes, render_changes};
use uvg::codegen::{declarative, tables};
use uvg::output::{write_split_changes, OutputContext};
use uvg::schema::{IntrospectedSchema, TableType};
use uvg::table_filter::TableFilter;
use uvg::{db, error, migrations, risk_classify, snapshot, tui};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse_with_profile()?;

    if let Some(command) = cli.command.as_ref() {
        return match command {
            Command::Snapshot(args) => run_snapshot(&cli, args).await,
            _ => migrations::run(&cli, command).await,
        };
    }

    if cli.interactive {
        return tui::run(cli).await;
    }

    validate_apply_cli(&cli)?;

    let table_filter = cli.table_filter()?;
    let options = cli.generator_options();
    let source_input = cli
        .url
        .as_deref()
        .ok_or_else(|| error::UvgError::Connection("database URL is required".to_string()))?;

    tracing::debug!("Connecting to database...");

    let schema =
        load_schema_input(&cli, source_input, &table_filter, cli.noviews, &options).await?;
    let dialect = schema.dialect;

    tracing::debug!("Found {} tables/views", schema.tables.len());

    match cli.generator.as_str() {
        "tables" => {
            if cli.split_tables {
                let files = tables::generate_split(&schema, &options);
                write_split_output(&files, &cli.outfile)?;
            } else {
                write_output(&tables::generate(&schema, &options), &cli.outfile)?;
            }
        }
        "declarative" => {
            if cli.split_tables {
                let files = declarative::generate_split(&schema, &options);
                write_split_output(&files, &cli.outfile)?;
            } else {
                write_output(&declarative::generate(&schema, &options), &cli.outfile)?;
            }
        }
        "ddl" => {
            use uvg::codegen::ddl::{DdlGenerator, DdlOutput};

            // --apply needs a target to execute against. Fail fast before we
            // do any work the user would have to throw away.
            if cli.apply && cli.target_url.is_none() {
                return Err(anyhow::anyhow!("--apply requires a target database URL"));
            }
            if cli.apply && cli.target_url.as_deref().is_some_and(is_snapshot_input) {
                return Err(anyhow::anyhow!(
                    "--apply requires a live target database URL, not a snapshot"
                ));
            }

            // If a target URL or snapshot is provided, load it for diff.
            let target_schema = if let Some(ref target_url) = cli.target_url {
                Some(
                    load_schema_input(&cli, target_url, &table_filter, cli.noviews, &options)
                        .await?,
                )
            } else {
                None
            };
            let ddl_opts = if let Some(target) = target_schema.as_ref() {
                cli.ddl_options_with_target_dialect(dialect, Some(target.dialect))?
            } else {
                cli.ddl_options(dialect)?
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
                    let changes =
                        classify_or_warn(&cli, compute_changes(&schema, target, &ddl_opts)).await?;
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
                            if ddl_opts.apply {
                                // target_url is guaranteed Some here: --out-dir
                                // already errored above without one, and the
                                // early --apply check enforces it too.
                                let target_url = cli.target_url.as_deref().unwrap();
                                let target_config = cli.parse_target_connection(target_url)?;
                                apply_manifest(
                                    &target_config,
                                    &manifest,
                                    out_dir,
                                    target_url,
                                    ApplyOptions::new(
                                        !cli.no_parse_check,
                                        cli.apply_retries,
                                        cli.progress.resolved(),
                                    ),
                                )
                                .await?;
                            }
                        }
                    }
                    return Ok(());
                }
            }

            if cli.risk_classify {
                let Some(target) = target_schema.as_ref() else {
                    return Err(anyhow::anyhow!(
                        "--risk-classify requires a target database URL or @snapshot to diff against"
                    ));
                };
                let changes =
                    classify_or_warn(&cli, compute_changes(&schema, target, &ddl_opts)).await?;
                let content = render_changes(&changes, dialect, ddl_opts.target_dialect);
                write_output(&content, &cli.outfile)?;
                if ddl_opts.apply {
                    let target_url = cli.target_url.as_deref().unwrap();
                    let target_config = cli.parse_target_connection(target_url)?;
                    apply_inline(
                        &target_config,
                        &content,
                        target_url,
                        ApplyOptions::new(
                            !cli.no_parse_check,
                            cli.apply_retries,
                            cli.progress.resolved(),
                        ),
                    )
                    .await?;
                }
                return Ok(());
            }

            let gen = DdlGenerator;
            let ddl_output = gen.generate(&schema, target_schema.as_ref(), &ddl_opts);

            match ddl_output {
                DdlOutput::Single(content) => {
                    write_output(&content, &cli.outfile)?;
                    if ddl_opts.apply {
                        // target_url is Some: enforced by the early --apply
                        // guard at the top of this arm.
                        let target_url = cli.target_url.as_deref().unwrap();
                        let target_config = cli.parse_target_connection(target_url)?;
                        apply_inline(
                            &target_config,
                            &content,
                            target_url,
                            ApplyOptions::new(
                                !cli.no_parse_check,
                                cli.apply_retries,
                                cli.progress.resolved(),
                            ),
                        )
                        .await?;
                    }
                }
                DdlOutput::Split(files) => match cli.outfile {
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
                            println!("{content}");
                        }
                    }
                },
            }
        }
        other => {
            return Err(error::UvgError::UnknownGenerator(other.to_string()).into());
        }
    };

    Ok(())
}

async fn run_snapshot(cli: &Cli, args: &SnapshotCommand) -> Result<()> {
    let table_filter = cli.table_filter()?;
    let options = cli.generator_options();
    let schema = load_schema_input(cli, &args.url, &table_filter, cli.noviews, &options).await?;
    snapshot::write(&args.output, &schema)?;
    eprintln!("uvg: wrote snapshot {}", args.output.display());
    Ok(())
}

async fn load_schema_input(
    cli: &Cli,
    raw: &str,
    table_filter: &TableFilter,
    noviews: bool,
    options: &GeneratorOptions,
) -> Result<IntrospectedSchema> {
    if let Some(path) = raw.strip_prefix('@') {
        if path.is_empty() {
            return Err(anyhow::anyhow!("snapshot input must be @<path>"));
        }
        let mut schema = snapshot::load(Path::new(path))?;
        schema.tables.retain(|table| {
            (!noviews || table.table_type != TableType::View) && table_filter.matches(&table.name)
        });
        return Ok(schema);
    }

    let config = cli.parse_connection_url(raw)?;
    let schemas = schemas_for_config(cli, &config);
    tracing::debug!("Introspecting schema...");
    db::introspect_with_config(
        config,
        &schemas,
        table_filter,
        noviews,
        options,
        cli.introspect_concurrency,
    )
    .await
}

fn is_snapshot_input(raw: &str) -> bool {
    raw.starts_with('@')
}

fn schemas_for_config(cli: &Cli, config: &ConnectionConfig) -> Vec<String> {
    if let Some(db) = config.database_name() {
        cli.schema_list_or(&db)
    } else {
        cli.schema_list_or(config.dialect().default_schema())
    }
}

fn validate_apply_cli(cli: &Cli) -> Result<()> {
    if !cli.apply {
        return Ok(());
    }
    if cli.generator != "ddl" {
        return Err(anyhow::anyhow!(
            "--apply only works with --generator ddl (current: {})",
            cli.generator,
        ));
    }
    let Some(target_url) = cli.target_url.as_deref() else {
        return Err(anyhow::anyhow!("--apply requires a target database URL"));
    };
    if is_snapshot_input(target_url) {
        return Err(anyhow::anyhow!(
            "--apply requires a live target database URL, not a snapshot"
        ));
    }
    if cli.split_tables {
        return Err(anyhow::anyhow!(
            "--apply with --split-tables is not supported (use --out-dir for per-table apply)"
        ));
    }
    if let Some(target_dialect) = cli.target_dialect.as_deref() {
        let explicit = target_dialect
            .parse::<uvg::dialect::Dialect>()
            .map_err(error::UvgError::InvalidDialect)?;
        let url_dialect = cli.parse_target_connection(target_url)?.dialect();
        if explicit != url_dialect {
            return Err(anyhow::anyhow!(
                "--apply: --target-dialect ({}) does not match the dialect inferred from the target URL ({}). \
                 Drop --target-dialect, or change the URL scheme to match.",
                explicit,
                url_dialect,
            ));
        }
    }
    Ok(())
}

async fn classify_or_warn(
    cli: &Cli,
    changes: Vec<uvg::output::Change>,
) -> Result<Vec<uvg::output::Change>> {
    if !cli.risk_classify {
        return Ok(changes);
    }
    let config = risk_classify::AnthropicConfig::from_env()?;
    if changes.is_empty() {
        return Ok(changes);
    }
    match risk_classify::classify_changes(&config, &changes).await {
        Ok(risks) => risk_classify::annotate_changes(&changes, &risks),
        Err(err) => {
            eprintln!("uvg: risk classification failed: {err}; continuing without annotations");
            Ok(changes)
        }
    }
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

#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;
