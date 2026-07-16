use anyhow::Result;

use super::app::{App, TreeNode};
use crate::apply::{apply_sql, ApplyOptions, ApplyReport};
use crate::cli::DdlOptions;
use crate::codegen::ddl_diff::compute_changes;
use crate::connection::parse_connection_url;
use crate::db;
use crate::output::Change;

pub(super) async fn generate_ddl(app: &mut App) -> Result<Vec<Change>> {
    let source_url = app.source_url.trim().to_string();
    let target_url = app.target_url.trim().to_string();

    let source_config = parse_connection_url(&source_url, app.trust_cert)?;
    let source_dialect = source_config.dialect();

    let target_config = parse_connection_url(&target_url, app.trust_cert)?;
    let target_dialect = target_config.dialect();

    // Introspect source
    let source_schemas = if let Some(db) = source_config.database_name() {
        vec![db]
    } else {
        vec![source_dialect.default_schema().to_string()]
    };
    let options = crate::cli::GeneratorOptions::default();
    let source_schema = db::introspect_with_config(
        source_config,
        &source_schemas,
        &crate::table_filter::TableFilter::allow_all(),
        false,
        &options,
        crate::cli::DEFAULT_INTROSPECT_CONCURRENCY,
    )
    .await?;

    // Introspect target
    let target_schemas = if let Some(db) = target_config.database_name() {
        vec![db]
    } else {
        vec![target_dialect.default_schema().to_string()]
    };

    let target_schema_data = db::introspect_with_config(
        target_config,
        &target_schemas,
        &crate::table_filter::TableFilter::allow_all(),
        false,
        &options,
        crate::cli::DEFAULT_INTROSPECT_CONCURRENCY,
    )
    .await?;

    let ddl_opts = DdlOptions {
        target_dialect,
        split_tables: false,
        apply: false,
        noindexes: false,
        noconstraints: false,
        nocomments: false,
    };

    Ok(compute_changes(
        &source_schema,
        &target_schema_data,
        &ddl_opts,
    ))
}

pub(super) async fn apply_ddl(app: &mut App) -> Result<ApplyReport> {
    let target_url = app.target_url.trim().to_string();
    let config = parse_connection_url(&target_url, app.trust_cert)?;
    let sql = collect_apply_sql(&app.nodes);
    // The TUI renders its own per-statement status from the returned results,
    // but validation, parse-check, and retry behavior are exactly the same as
    // the headless path. CLI flags provided with --interactive are retained.
    apply_sql(
        &config,
        &sql,
        "interactive ddl",
        ApplyOptions::new(app.parse_check, app.apply_retries, false),
    )
    .await
}

/// Concatenate the SQL of every checked node into a single blob suitable
/// for `db::execute_ddl()`. `_schema` always sorts first so enums and
/// schemas exist before tables that reference them; the remaining
/// per-table nodes keep their original (topo-sorted) order.
pub(super) fn collect_apply_sql(nodes: &[TreeNode]) -> String {
    let mut ordered: Vec<&TreeNode> = nodes.iter().filter(|n| n.checked).collect();
    ordered.sort_by_key(|n| if n.name == "_schema" { 0 } else { 1 });
    let mut parts: Vec<String> = Vec::new();
    for node in ordered {
        for change in &node.changes {
            parts.push(change.sql.clone());
        }
    }
    parts.join("\n\n")
}

#[cfg(test)]
pub(super) fn count_statements(ddl: &str, dialect: crate::dialect::Dialect) -> usize {
    db::split_statements(ddl, dialect).len()
}
