//! Schema diff engine for the DDL generator.
//!
//! Compares source and target `IntrospectedSchema` and emits ALTER statements
//! for new/dropped/modified tables and columns. Inspired by Alembic's autogenerate.

use std::collections::{HashMap, HashSet};

use crate::cli::DdlOptions;
use crate::codegen::{is_auto_increment_column, is_unique_constraint_index, topo_sort_tables};
use crate::ddl_typemap;
use crate::dialect::Dialect;
use crate::output::{Change, ChangeKind};
use crate::schema::{
    ColumnInfo, ConstraintInfo, ConstraintType, IndexInfo, IntrospectedSchema, TableInfo, TableType,
};

use super::ddl::{
    check_predicate_is_portable, format_ddl_default_typed, generate_column_def,
    generate_create_table, generate_indexes, qualified_table_name, quote_identifier,
    translate_check_predicate,
};

/// Compute the schema diff as a stream of tagged `Change` records.
///
/// Pure data — no formatting concerns. Use `render_changes()` to serialize
/// for stdout or `--outfile`, or pass directly into the per-table splitter.
pub fn compute_changes(
    source: &IntrospectedSchema,
    target: &IntrospectedSchema,
    options: &DdlOptions,
) -> Vec<Change> {
    let source_dialect = source.dialect;
    let target_dialect = options.target_dialect;

    // For MySQL, the schema is the database name. When each side has exactly
    // one schema (the common case), treat those as defaults so sourcedb.users
    // matches targetdb.users. Non-default schemas are preserved for multi-schema diffs.
    let mysql_defaults = build_mysql_defaults(source, target, source_dialect, target_dialect);

    let source_map: HashMap<(&str, &str), &TableInfo> = source
        .tables
        .iter()
        .map(|t| {
            (
                (
                    normalize_schema(&t.schema, &mysql_defaults),
                    t.name.as_str(),
                ),
                t,
            )
        })
        .collect();
    let target_map: HashMap<(&str, &str), &TableInfo> = target
        .tables
        .iter()
        .map(|t| {
            (
                (
                    normalize_schema(&t.schema, &mysql_defaults),
                    t.name.as_str(),
                ),
                t,
            )
        })
        .collect();

    let mut changes: Vec<Change> = Vec::new();

    // New tables (in source, not in target)
    let sorted_source = topo_sort_tables(&source.tables);
    for table in &sorted_source {
        if table.table_type != TableType::Table {
            continue;
        }
        let key = (
            normalize_schema(&table.schema, &mysql_defaults),
            table.name.as_str(),
        );
        if !target_map.contains_key(&key) {
            let schema = normalize_schema(&table.schema, &mysql_defaults).to_string();
            let name = table.name.clone();
            changes.push(Change {
                table_schema: schema.clone(),
                table_name: Some(name.clone()),
                sql: generate_create_table(table, source_dialect, target_dialect, options),
                kind: ChangeKind::CreateTable,
            });
            if !options.noindexes {
                for sql in generate_indexes(table, source_dialect, target_dialect) {
                    changes.push(Change {
                        table_schema: schema.clone(),
                        table_name: Some(name.clone()),
                        sql,
                        kind: ChangeKind::CreateIndex,
                    });
                }
            }
        }
    }

    // Modified tables (in both): compare columns
    for table in &sorted_source {
        if table.table_type != TableType::Table {
            continue;
        }
        let key = (
            normalize_schema(&table.schema, &mysql_defaults),
            table.name.as_str(),
        );
        if let Some(target_table) = target_map.get(&key) {
            let schema = normalize_schema(&table.schema, &mysql_defaults).to_string();
            let name = table.name.clone();
            // Target-side constraint/index drops must precede column changes:
            // MSSQL rejects DROP COLUMN while a dependent index or constraint
            // exists, and MySQL can auto-drop an index with its column and
            // then fail on the later explicit DROP INDEX. Adds stay after
            // column changes so they can reference newly added columns.
            let (constraint_drops, constraint_adds) = if options.noconstraints {
                (Vec::new(), Vec::new())
            } else {
                diff_table_constraints(table, target_table, source_dialect, target_dialect)
            };
            let (index_drops, index_adds) = if options.noindexes {
                (Vec::new(), Vec::new())
            } else {
                diff_table_indexes(table, target_table, source_dialect, target_dialect)
            };
            // Tag each group with its structural kind before flattening, so
            // the down-migration generator can reverse by operation rather
            // than re-parsing rendered SQL. Order is unchanged (see the
            // ordering note above); only the kind tag is added.
            let mut table_sql: Vec<(ChangeKind, String)> = Vec::new();
            table_sql.extend(
                constraint_drops
                    .into_iter()
                    .map(|sql| (ChangeKind::DropConstraint, sql)),
            );
            table_sql.extend(
                index_drops
                    .into_iter()
                    .map(|sql| (ChangeKind::DropIndex, sql)),
            );
            table_sql.extend(diff_table_columns(
                table,
                target_table,
                source_dialect,
                target_dialect,
            ));
            table_sql.extend(
                constraint_adds
                    .into_iter()
                    .map(|sql| (ChangeKind::AddConstraint, sql)),
            );
            table_sql.extend(
                index_adds
                    .into_iter()
                    .map(|sql| (ChangeKind::CreateIndex, sql)),
            );
            for (kind, sql) in table_sql {
                changes.push(Change {
                    table_schema: schema.clone(),
                    table_name: Some(name.clone()),
                    sql,
                    kind,
                });
            }
        }
    }

    // Dropped tables (in target, not in source)
    let mut dropped: Vec<(&str, &str)> = target_map
        .keys()
        .filter(|key| !source_map.contains_key(*key))
        .copied()
        .collect();
    dropped.sort();
    for (schema, name) in dropped {
        // Dropped tables come from the target's introspection — the schema
        // here is already in the target's namespace, so source_dialect is
        // immaterial for the qualification rule. Pass target_dialect for
        // both sides to mean "no source-specific suppression."
        let qname = qualified_table_name(schema, name, target_dialect, target_dialect);
        changes.push(Change {
            table_schema: schema.to_string(),
            table_name: Some(name.to_string()),
            sql: format!("-- WARNING: destructive operation\nDROP TABLE IF EXISTS {qname};"),
            kind: ChangeKind::DropTable,
        });
    }

    changes
}

/// Serialize a sequence of `Change` records into the legacy single-blob
/// format that `diff_schemas()` returns. Empty input yields the
/// "no schema changes detected" sentinel so existing string-grep callers
/// (e.g. the TUI's empty-check at `src/tui/mod.rs:307`) keep working.
pub fn render_changes(
    changes: &[Change],
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> String {
    if changes.is_empty() {
        return "-- No schema changes detected.\n".to_string();
    }
    let header = format!(
        "-- Generated by uvg (diff)\n-- Source: {source_dialect}, Target: {target_dialect}\n\n"
    );
    let stmts: Vec<&str> = changes.iter().map(|c| c.sql.as_str()).collect();
    format!("{header}{}\n", stmts.join("\n\n"))
}

/// Diff two schemas and emit ALTER statements.
/// Detects new/dropped tables and new/dropped/modified columns.
pub fn diff_schemas(
    source: &IntrospectedSchema,
    target: &IntrospectedSchema,
    options: &DdlOptions,
) -> String {
    let source_dialect = source.dialect;
    let target_dialect = options.target_dialect;
    let changes = compute_changes(source, target, options);
    render_changes(&changes, source_dialect, target_dialect)
}

/// Build the set of MySQL database names to treat as defaults for diff normalization.
fn build_mysql_defaults(
    source: &IntrospectedSchema,
    target: &IntrospectedSchema,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> HashSet<String> {
    let mut defaults = HashSet::new();
    if source_dialect == Dialect::Mysql {
        let schemas: HashSet<&str> = source.tables.iter().map(|t| t.schema.as_str()).collect();
        if schemas.len() == 1 {
            defaults.insert(schemas.into_iter().next().unwrap().to_string());
        }
    }
    if target_dialect == Dialect::Mysql {
        let schemas: HashSet<&str> = target.tables.iter().map(|t| t.schema.as_str()).collect();
        if schemas.len() == 1 {
            defaults.insert(schemas.into_iter().next().unwrap().to_string());
        }
    }
    defaults
}

/// Normalize default schemas to empty string for cross-dialect comparison.
/// PG "public", MSSQL "dbo", SQLite "main" are well-known defaults.
/// MySQL database names in `mysql_defaults` are also treated as defaults.
fn normalize_schema<'a>(schema: &'a str, mysql_defaults: &HashSet<String>) -> &'a str {
    if matches!(schema, "public" | "dbo" | "main" | "") {
        return "";
    }
    if mysql_defaults.contains(schema) {
        return "";
    }
    schema
}

/// Compare columns between source and target tables, emit ALTER statements.
fn diff_table_columns(
    source: &TableInfo,
    target: &TableInfo,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> Vec<(ChangeKind, String)> {
    let mut stmts: Vec<(ChangeKind, String)> = Vec::new();
    let tname = qualified_table_name(&source.schema, &source.name, source_dialect, target_dialect);

    let source_cols: HashMap<&str, &ColumnInfo> = source
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();
    let target_cols: HashMap<&str, &ColumnInfo> = target
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    // New columns
    for col in &source.columns {
        if !target_cols.contains_key(col.name.as_str()) {
            let col_def =
                generate_column_def(col, &source.constraints, source_dialect, target_dialect);
            let col_def = col_def.trim();
            let add_clause = match target_dialect {
                Dialect::Mssql => "ADD",
                _ => "ADD COLUMN",
            };
            stmts.push((
                ChangeKind::AddColumn,
                format!("ALTER TABLE {tname} {add_clause} {col_def};"),
            ));
        }
    }

    // Modified columns
    for col in &source.columns {
        if let Some(target_col) = target_cols.get(col.name.as_str()) {
            let alters = diff_column(
                col,
                target_col,
                &source.schema,
                &source.name,
                source_dialect,
                target_dialect,
            );
            stmts.extend(alters.into_iter().map(|sql| (ChangeKind::AlterColumn, sql)));
        }
    }

    // Dropped columns
    let mut dropped: Vec<&str> = target_cols
        .keys()
        .filter(|name| !source_cols.contains_key(*name))
        .copied()
        .collect();
    dropped.sort();
    for name in dropped {
        let qcol = quote_identifier(name, target_dialect);
        stmts.push((
            ChangeKind::DropColumn,
            format!("-- WARNING: destructive operation\nALTER TABLE {tname} DROP COLUMN {qcol};"),
        ));
    }

    stmts
}

/// Returns (drops, adds) separately so the caller can order target-side
/// drops before column changes — see the ordering note in compute_changes.
fn diff_table_constraints(
    source: &TableInfo,
    target: &TableInfo,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> (Vec<String>, Vec<String>) {
    if target_dialect == Dialect::Sqlite {
        return (Vec::new(), Vec::new());
    }

    let target_names: HashSet<&str> = target.constraints.iter().map(|c| c.name.as_str()).collect();
    let mut drops = Vec::new();
    let mut adds = Vec::new();

    for constraint in &target.constraints {
        if source
            .constraints
            .iter()
            .any(|source_constraint| source_constraint.name == constraint.name)
        {
            continue;
        }
        if matches!(constraint.constraint_type, ConstraintType::PrimaryKey)
            && source.constraints.iter().any(|source_constraint| {
                matches!(
                    source_constraint.constraint_type,
                    ConstraintType::PrimaryKey
                ) && source_constraint.columns == constraint.columns
            })
        {
            continue;
        }
        drops.push(render_dropped_constraint(
            source,
            constraint,
            source_dialect,
            target_dialect,
        ));
    }

    for constraint in &source.constraints {
        if target_names.contains(constraint.name.as_str()) {
            continue;
        }
        if matches!(constraint.constraint_type, ConstraintType::PrimaryKey)
            && target.constraints.iter().any(|target_constraint| {
                matches!(
                    target_constraint.constraint_type,
                    ConstraintType::PrimaryKey
                ) && target_constraint.columns == constraint.columns
            })
        {
            continue;
        }
        if let Some(sql) =
            render_added_constraint(source, constraint, source_dialect, target_dialect)
        {
            adds.push(sql);
        }
    }

    (drops, adds)
}

fn render_dropped_constraint(
    table: &TableInfo,
    constraint: &ConstraintInfo,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> String {
    let tname = qualified_table_name(&table.schema, &table.name, source_dialect, target_dialect);
    let cname = quote_identifier(&constraint.name, target_dialect);

    let sql = match target_dialect {
        Dialect::Postgres => format!("ALTER TABLE {tname} DROP CONSTRAINT IF EXISTS {cname};"),
        Dialect::Mssql => format!("ALTER TABLE {tname} DROP CONSTRAINT {cname};"),
        Dialect::Mysql => match constraint.constraint_type {
            ConstraintType::ForeignKey => format!("ALTER TABLE {tname} DROP FOREIGN KEY {cname};"),
            ConstraintType::PrimaryKey => format!("ALTER TABLE {tname} DROP PRIMARY KEY;"),
            ConstraintType::Unique => format!("ALTER TABLE {tname} DROP INDEX {cname};"),
            ConstraintType::Check => format!("ALTER TABLE {tname} DROP CHECK {cname};"),
        },
        Dialect::Sqlite => format!(
            "-- WARNING: SQLite cannot drop constraint {} without rebuilding table {}",
            constraint.name, table.name
        ),
    };
    if matches!(target_dialect, Dialect::Sqlite) {
        sql
    } else {
        format!("-- WARNING: destructive operation\n{sql}")
    }
}

fn render_added_constraint(
    table: &TableInfo,
    constraint: &ConstraintInfo,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> Option<String> {
    let tname = qualified_table_name(&table.schema, &table.name, source_dialect, target_dialect);
    let cname = quote_identifier(&constraint.name, target_dialect);
    let cols: Vec<String> = constraint
        .columns
        .iter()
        .map(|col| quote_identifier(col, target_dialect))
        .collect();

    match constraint.constraint_type {
        ConstraintType::PrimaryKey => Some(format!(
            "ALTER TABLE {tname} ADD CONSTRAINT {cname} PRIMARY KEY ({});",
            cols.join(", ")
        )),
        ConstraintType::Unique => Some(format!(
            "ALTER TABLE {tname} ADD CONSTRAINT {cname} UNIQUE ({});",
            cols.join(", ")
        )),
        ConstraintType::ForeignKey => {
            let fk = constraint.foreign_key.as_ref()?;
            let ref_table = qualified_table_name(
                &fk.ref_schema,
                &fk.ref_table,
                source_dialect,
                target_dialect,
            );
            let ref_cols: Vec<String> = fk
                .ref_columns
                .iter()
                .map(|col| quote_identifier(col, target_dialect))
                .collect();
            let mut sql = format!(
                "ALTER TABLE {tname} ADD CONSTRAINT {cname} FOREIGN KEY ({}) REFERENCES {ref_table} ({});",
                cols.join(", "),
                ref_cols.join(", ")
            );
            if fk.delete_rule != "NO ACTION" {
                sql.insert_str(sql.len() - 1, &format!(" ON DELETE {}", fk.delete_rule));
            }
            if fk.update_rule != "NO ACTION" {
                sql.insert_str(sql.len() - 1, &format!(" ON UPDATE {}", fk.update_rule));
            }
            Some(sql)
        }
        ConstraintType::Check => {
            let expr = constraint.check_expression.as_ref()?;
            if source_dialect != target_dialect
                && !check_predicate_is_portable(expr, source_dialect, target_dialect)
            {
                return Some(format!(
                    "-- DROPPED CHECK {}: predicate uses non-portable syntax\n--   source: {}",
                    constraint.name,
                    expr.replace('\n', " ")
                ));
            }
            let translated = translate_check_predicate(expr, source_dialect, target_dialect);
            Some(format!(
                "ALTER TABLE {tname} ADD CONSTRAINT {cname} CHECK ({translated});"
            ))
        }
    }
}

/// Returns (drops, adds) separately so the caller can order target-side
/// drops before column changes — see the ordering note in compute_changes.
fn diff_table_indexes(
    source: &TableInfo,
    target: &TableInfo,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> (Vec<String>, Vec<String>) {
    let target_names: HashSet<&str> = target.indexes.iter().map(|idx| idx.name.as_str()).collect();
    let source_names: HashSet<&str> = source.indexes.iter().map(|idx| idx.name.as_str()).collect();
    let drops: Vec<String> = target
        .indexes
        .iter()
        .filter(|idx| !source_names.contains(idx.name.as_str()))
        .filter(|idx| !is_constraint_backing_index(idx, &target.constraints, target_dialect))
        .map(|idx| render_dropped_index(source, idx, source_dialect, target_dialect))
        .collect();

    let adds: Vec<String> = source
        .indexes
        .iter()
        .filter(|idx| !target_names.contains(idx.name.as_str()))
        .filter(|idx| !is_unique_constraint_index(idx, &source.constraints))
        .map(|idx| render_added_index(source, idx, source_dialect, target_dialect))
        .collect();
    (drops, adds)
}

fn is_constraint_backing_index(
    index: &IndexInfo,
    constraints: &[ConstraintInfo],
    target_dialect: Dialect,
) -> bool {
    if is_unique_constraint_index(index, constraints) {
        return true;
    }
    if index.is_unique
        && constraints.iter().any(|constraint| {
            matches!(constraint.constraint_type, ConstraintType::PrimaryKey)
                && constraint.columns == index.columns
        })
    {
        return true;
    }
    // Only MySQL/InnoDB auto-creates FK backing indexes (and refuses to drop
    // them while the FK exists). On PG/MSSQL an index on FK columns is always
    // user-created and must participate in drift, or a target-only index
    // would falsely converge.
    target_dialect == Dialect::Mysql
        && constraints.iter().any(|constraint| {
            matches!(constraint.constraint_type, ConstraintType::ForeignKey)
                && constraint.columns == index.columns
        })
}

fn render_dropped_index(
    table: &TableInfo,
    index: &IndexInfo,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> String {
    let tname = qualified_table_name(&table.schema, &table.name, source_dialect, target_dialect);
    let iname = quote_identifier(&index.name, target_dialect);
    match target_dialect {
        Dialect::Postgres | Dialect::Sqlite => {
            let qname =
                qualified_table_name(&table.schema, &index.name, source_dialect, target_dialect);
            format!("DROP INDEX IF EXISTS {qname};")
        }
        Dialect::Mssql | Dialect::Mysql => format!("DROP INDEX {iname} ON {tname};"),
    }
}

fn render_added_index(
    table: &TableInfo,
    index: &IndexInfo,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> String {
    let tname = qualified_table_name(&table.schema, &table.name, source_dialect, target_dialect);
    let unique = if index.is_unique { "UNIQUE " } else { "" };
    let cols: Vec<String> = index
        .columns
        .iter()
        .map(|col| quote_identifier(col, target_dialect))
        .collect();
    format!(
        "CREATE {unique}INDEX {} ON {tname} ({});",
        quote_identifier(&index.name, target_dialect),
        cols.join(", ")
    )
}

/// Compare a single column and emit ALTER statements if different.
/// Compares type, nullability, and default values.
fn diff_column(
    source: &ColumnInfo,
    target: &ColumnInfo,
    table_schema: &str,
    table_name: &str,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> Vec<String> {
    let mut stmts = Vec::new();
    let tname = qualified_table_name(table_schema, table_name, source_dialect, target_dialect);
    let cname = quote_identifier(&source.name, target_dialect);

    let source_type = ddl_typemap::map_ddl_type(source, source_dialect, target_dialect);
    let target_type = ddl_typemap::map_ddl_type(target, target_dialect, target_dialect);

    let type_changed = source_type.sql_type != target_type.sql_type;
    let source_auto = is_auto_increment_column(source, source_dialect);
    let target_auto = is_auto_increment_column(target, target_dialect);
    let nullable_changed = if source_dialect != target_dialect && source_auto && target_auto {
        false
    } else {
        source.is_nullable != target.is_nullable
    };

    // Compare defaults with boolean-aware normalization
    let canonical = ddl_typemap::to_canonical(source, source_dialect);
    let is_boolean = matches!(canonical, ddl_typemap::CanonicalType::Boolean);
    let source_default = source
        .column_default
        .as_deref()
        .map(|d| format_ddl_default_typed(d, source_dialect, target_dialect, is_boolean));
    let target_default = target
        .column_default
        .as_deref()
        .map(|d| format_ddl_default_typed(d, target_dialect, target_dialect, is_boolean));
    // Auto-increment columns express their default through dialect-specific
    // mechanisms (MSSQL IDENTITY → no default; PG SERIAL → nextval(...)). For
    // cross-dialect diffs, ignore the resulting default-string mismatch when
    // both sides are auto-increment. Same-dialect diffs keep the literal
    // comparison so divergent sequences (e.g. nextval('a') vs nextval('b'))
    // still surface as real drift.
    let default_changed = if source_auto && target_auto && source_dialect != target_dialect {
        false
    } else {
        source_default != target_default
    };

    if !type_changed && !nullable_changed && !default_changed {
        return stmts;
    }

    match target_dialect {
        Dialect::Postgres => {
            if type_changed {
                stmts.push(format!(
                    "ALTER TABLE {tname} ALTER COLUMN {cname} TYPE {};",
                    source_type.sql_type
                ));
            }
            if nullable_changed {
                if source.is_nullable {
                    stmts.push(format!(
                        "ALTER TABLE {tname} ALTER COLUMN {cname} DROP NOT NULL;"
                    ));
                } else {
                    stmts.push(format!(
                        "ALTER TABLE {tname} ALTER COLUMN {cname} SET NOT NULL;"
                    ));
                }
            }
            if default_changed {
                match &source_default {
                    Some(d) => stmts.push(format!(
                        "ALTER TABLE {tname} ALTER COLUMN {cname} SET DEFAULT {d};"
                    )),
                    None => stmts.push(format!(
                        "ALTER TABLE {tname} ALTER COLUMN {cname} DROP DEFAULT;"
                    )),
                }
            }
        }
        Dialect::Mysql => {
            let not_null = if !source.is_nullable { " NOT NULL" } else { "" };
            let default_clause = match &source_default {
                Some(d) => format!(" DEFAULT {d}"),
                None => String::new(),
            };
            stmts.push(format!(
                "ALTER TABLE {tname} MODIFY COLUMN {cname} {}{not_null}{default_clause};",
                source_type.sql_type
            ));
        }
        Dialect::Mssql => {
            if type_changed || nullable_changed {
                let not_null = if !source.is_nullable {
                    " NOT NULL"
                } else {
                    " NULL"
                };
                stmts.push(format!(
                    "ALTER TABLE {tname} ALTER COLUMN {cname} {}{not_null};",
                    source_type.sql_type
                ));
            }
            if default_changed {
                stmts.push(format!(
                    "-- NOTE: MSSQL requires dropping the named default constraint first.\n-- Run: SELECT name FROM sys.default_constraints WHERE parent_object_id = OBJECT_ID('{tname_raw}') AND col_name(parent_object_id, parent_column_id) = '{col_name}'\n-- Then: ALTER TABLE {tname} DROP CONSTRAINT <name>;",
                    tname_raw = table_name,
                    col_name = source.name
                ));
                if let Some(ref d) = source_default {
                    stmts.push(format!("ALTER TABLE {tname} ADD DEFAULT {d} FOR {cname};"));
                }
            }
        }
        Dialect::Sqlite => {
            stmts.push(format!(
                "-- WARNING: SQLite does not support ALTER COLUMN. Table recreation required.\n-- ALTER TABLE {tname} ALTER COLUMN {cname} TYPE {};",
                source_type.sql_type
            ));
        }
    }

    stmts
}

#[cfg(test)]
#[path = "ddl_diff_tests.rs"]
mod tests;
