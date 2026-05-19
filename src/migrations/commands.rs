use std::collections::HashSet;

use anyhow::{anyhow, Context, Result};

use crate::cli::{
    Cli, ConnectionConfig, DdlOptions, DowngradeCommand, HistoryCommand, MergeCommand,
    RevisionCommand, StampCommand, UpgradeCommand,
};
use crate::codegen::ddl_diff::compute_changes;
use crate::db;

use super::apply::{
    apply_down_migration, apply_migration, migration_parse_check_enabled, parse_check_migration,
};
use super::files::{
    next_revision_id, write_merge_revision_file, write_meta_file, write_revision_file,
};
use super::graph::MigrationGraph;
use super::model::MigrationDirection;
use super::version_table::{
    clear_revision, confirm_stamp, current_revision, ensure_version_table, record_revision,
    redact_url, stamp_revision,
};

pub(super) async fn run_revision(cli: &Cli, args: &RevisionCommand) -> Result<()> {
    let source_config = cli.parse_connection_url(&args.source_url)?;
    let target_config = cli.parse_connection_url(&args.target_url)?;
    let source_dialect = source_config.dialect();
    let target_dialect = target_config.dialect();
    let source_schemas = schemas_for(cli, &source_config);
    let target_schemas = schemas_for(cli, &target_config);
    let table_filter = cli.table_filter()?;
    let gen_opts = cli.generator_options();

    let source = db::introspect_with_config(
        source_config,
        &source_schemas,
        &table_filter,
        cli.noviews,
        &gen_opts,
        cli.introspect_concurrency,
    )
    .await?;
    let target = db::introspect_with_config(
        target_config,
        &target_schemas,
        &table_filter,
        cli.noviews,
        &gen_opts,
        cli.introspect_concurrency,
    )
    .await?;

    let options = DdlOptions {
        target_dialect,
        split_tables: false,
        apply: false,
        noindexes: gen_opts.noindexes,
        noconstraints: gen_opts.noconstraints,
        nocomments: gen_opts.nocomments,
    };
    let changes = compute_changes(&source, &target, &options);
    if changes.is_empty() {
        eprintln!("uvg: no schema changes");
        return Ok(());
    }

    let graph = MigrationGraph::load(&args.migrations_dir)?;
    let parent = graph.single_head()?;
    let revision = next_revision_id(&graph);
    let path = write_revision_file(
        &args.migrations_dir,
        &revision,
        parent.as_deref(),
        &args.message,
        source_dialect,
        target_dialect,
        &changes,
    )?;

    let refreshed = MigrationGraph::load(&args.migrations_dir)?;
    write_meta_file(&args.migrations_dir, &refreshed)?;

    println!("Wrote: {}", path.display());
    Ok(())
}

pub(super) async fn run_upgrade(cli: &Cli, args: &UpgradeCommand) -> Result<()> {
    let graph = MigrationGraph::load(&args.migrations_dir)?;
    if graph.is_empty() {
        eprintln!(
            "uvg: no migrations found in {}",
            args.migrations_dir.display()
        );
        return Ok(());
    }
    let target = graph.resolve_target(args.revision.as_deref())?;
    let config = cli.parse_connection_url(&args.target_url)?;

    ensure_version_table(&config).await?;
    let current = current_revision(&config).await?;
    let plan = graph.plan_upgrade(current.as_deref(), target.as_deref())?;
    if plan.is_empty() {
        let label = current.as_deref().unwrap_or("base");
        eprintln!("uvg: already at {label}");
        return Ok(());
    }

    let parse_check = migration_parse_check_enabled(cli, &config);
    for migration in plan {
        if parse_check {
            parse_check_migration(&config, migration, MigrationDirection::Up).await?;
        }
        apply_migration(&config, migration).await.with_context(|| {
            format!(
                "uvg: migration {} UP failed before uvg_version was changed",
                migration.revision
            )
        })?;
        record_revision(&config, &migration.revision, &migration.description)
            .await
            .with_context(|| {
                format!(
                    "uvg: migration {} SQL was applied, but failed to record uvg_version at {}; verify the target, then run `uvg stamp <target-url> {} --yes` if it already matches",
                    migration.revision, migration.revision, migration.revision
                )
            })?;
        eprintln!("uvg: applied {}", migration.revision);
    }

    Ok(())
}

pub(super) async fn run_downgrade(cli: &Cli, args: &DowngradeCommand) -> Result<()> {
    let graph = MigrationGraph::load(&args.migrations_dir)?;
    if graph.is_empty() {
        eprintln!(
            "uvg: no migrations found in {}",
            args.migrations_dir.display()
        );
        return Ok(());
    }
    let config = cli.parse_connection_url(&args.target_url)?;

    ensure_version_table(&config).await?;
    let current = current_revision(&config).await?;
    let plan = graph.plan_downgrade(current.as_deref(), args.revision.as_deref())?;
    if plan.is_empty() {
        let label = current.as_deref().unwrap_or("base");
        eprintln!("uvg: already at {label}");
        return Ok(());
    }

    let parse_check = migration_parse_check_enabled(cli, &config);
    for migration in plan {
        if parse_check {
            parse_check_migration(&config, migration, MigrationDirection::Down).await?;
        }
        apply_down_migration(&config, migration)
            .await
            .with_context(|| {
                format!(
                    "uvg: migration {} DOWN failed before uvg_version was changed",
                    migration.revision
                )
            })?;
        if let Some(parent) = migration.parents.first() {
            record_revision(&config, parent, parent_description(&graph, parent))
                .await
                .with_context(|| {
                    format!(
                        "uvg: migration {} DOWN SQL was applied, but failed to record uvg_version at parent {}; verify the target, then run `uvg stamp <target-url> {} --yes` if it already matches",
                        migration.revision, parent, parent
                    )
                })?;
            eprintln!("uvg: downgraded {} -> {}", migration.revision, parent);
        } else {
            clear_revision(&config).await.with_context(|| {
                format!(
                    "uvg: migration {} DOWN SQL was applied, but failed to clear uvg_version; verify the target is at base, then clear uvg_version manually",
                    migration.revision
                )
            })?;
            eprintln!("uvg: downgraded {} -> base", migration.revision);
        }
    }

    Ok(())
}

pub(super) fn run_merge(args: &MergeCommand) -> Result<()> {
    let graph = MigrationGraph::load(&args.migrations_dir)?;
    let heads = graph.heads();
    if heads.len() < 2 {
        return Err(anyhow!(
            "uvg merge requires at least two heads; current heads: {}",
            if heads.is_empty() {
                "(none)".to_string()
            } else {
                heads.join(", ")
            }
        ));
    }

    let revision = next_revision_id(&graph);
    let path = write_merge_revision_file(&args.migrations_dir, &revision, &heads, &args.message)?;
    let refreshed = MigrationGraph::load(&args.migrations_dir)?;
    write_meta_file(&args.migrations_dir, &refreshed)?;

    println!("Wrote: {}", path.display());
    eprintln!(
        "uvg: merge revision {} joins heads {}",
        revision,
        heads.join(", ")
    );
    Ok(())
}

pub(super) async fn run_stamp(cli: &Cli, args: &StampCommand) -> Result<()> {
    let graph = MigrationGraph::load(&args.migrations_dir)?;
    let migration = graph.require_revision(&args.revision)?;
    let config = cli.parse_connection_url(&args.target_url)?;

    if !args.yes && !confirm_stamp(&args.target_url, &args.revision)? {
        eprintln!("uvg: stamp cancelled");
        return Ok(());
    }

    stamp_revision(&config, migration).await?;
    eprintln!(
        "uvg: stamped {} at revision {}",
        redact_url(&args.target_url),
        migration.revision
    );
    Ok(())
}

pub(super) async fn run_history(cli: &Cli, args: &HistoryCommand) -> Result<()> {
    let graph = MigrationGraph::load(&args.migrations_dir)?;
    if graph.is_empty() {
        eprintln!(
            "uvg: no migrations found in {}",
            args.migrations_dir.display()
        );
        return Ok(());
    }

    let current = if let Some(url) = args.target_url.as_deref() {
        let config = cli.parse_connection_url(url)?;
        current_revision(&config).await?
    } else {
        None
    };
    let applied = current
        .as_deref()
        .map(|revision| graph.ancestor_set(revision))
        .transpose()?
        .unwrap_or_default();
    let heads: HashSet<String> = graph.heads().into_iter().collect();

    for migration in graph.ordered() {
        let (parent_label, parent_value) = if migration.parents.is_empty() {
            ("parent", "base".to_string())
        } else if migration.parents.len() == 1 {
            ("parent", migration.parents[0].clone())
        } else {
            ("parents", migration.parents.join(","))
        };
        let mut markers = Vec::new();
        if applied.contains(&migration.revision) {
            markers.push("applied");
        }
        if current.as_deref() == Some(migration.revision.as_str()) {
            markers.push("current");
        }
        if heads.contains(&migration.revision) {
            markers.push("head");
        }
        let suffix = if markers.is_empty() {
            String::new()
        } else {
            format!(" [{}]", markers.join(", "))
        };
        println!(
            "{}  {}  ({}: {}){}",
            migration.revision, migration.description, parent_label, parent_value, suffix
        );
    }

    Ok(())
}

fn schemas_for(cli: &Cli, config: &ConnectionConfig) -> Vec<String> {
    if let Some(db) = config.database_name() {
        cli.schema_list_or(&db)
    } else {
        cli.schema_list_or(config.dialect().default_schema())
    }
}

fn parent_description<'a>(graph: &'a MigrationGraph, parent: &str) -> &'a str {
    graph
        .migrations
        .get(parent)
        .map(|migration| migration.description.as_str())
        .unwrap_or("")
}
