use std::collections::BTreeSet;

use crate::codegen::parse_sequence_name;
use crate::codegen::{
    is_auto_increment_column, is_enum_array_column, is_primary_key_column, is_serial_default,
};
use crate::ddl_typemap;
use crate::dialect::Dialect;
use crate::schema::{ColumnInfo, ConstraintInfo, EnumInfo};

use super::defaults::{
    format_ddl_default_typed, reattach_now_family_precision, temporal_precision,
};
use super::ident::{qualified_object_name, quote_identifier};

/// Generate a column definition line.
pub(in crate::codegen) fn generate_column_def(
    col: &ColumnInfo,
    constraints: &[ConstraintInfo],
    source_dialect: Dialect,
    target_dialect: Dialect,
    shared_sequences: &BTreeSet<String>,
    enum_info: Option<&EnumInfo>,
) -> String {
    let qname = quote_identifier(&col.name, target_dialect);

    // Detect auto-increment
    let is_auto = is_auto_increment_column(col, source_dialect);
    // Re-emitting every PostgreSQL nextval() as SERIAL invents a fresh
    // table-local sequence. Real schemas (including partitioned Pagila
    // tables) can intentionally share one sequence, so same-dialect output
    // preserves the explicit default and creates referenced sequences once at
    // schema scope.
    let preserve_pg_sequence = source_dialect == Dialect::Postgres
        && target_dialect == Dialect::Postgres
        && col
            .column_default
            .as_deref()
            .filter(|default| is_serial_default(default, source_dialect))
            .and_then(parse_sequence_name)
            .is_some_and(|sequence| shared_sequences.contains(&sequence));
    let render_as_auto = is_auto && !preserve_pg_sequence;

    let is_pk = is_primary_key_column(&col.name, constraints);

    // Compute canonical type once (used for type mapping and boolean default detection)
    let canonical = crate::ddl_typemap::to_canonical(col, source_dialect);
    let is_boolean = matches!(canonical, crate::ddl_typemap::CanonicalType::Boolean);

    // Type
    let type_str = if render_as_auto {
        format_autoincrement_type(col, source_dialect, target_dialect, is_pk)
    } else if source_dialect.supports_native_enums() && target_dialect.supports_native_enums() {
        match enum_info {
            Some(enum_info) => {
                let mut enum_type = qualified_object_name(
                    enum_info.schema.as_deref(),
                    &enum_info.name,
                    target_dialect,
                );
                if is_enum_array_column(col) {
                    enum_type.push_str("[]");
                }
                enum_type
            }
            None => ddl_typemap::from_canonical(&canonical, target_dialect).sql_type,
        }
    } else {
        ddl_typemap::from_canonical(&canonical, target_dialect).sql_type
    };

    let mut parts = vec![format!("    {qname} {type_str}")];

    // NOT NULL (skip for auto-increment PKs where NOT NULL is implied)
    if !(col.is_nullable || render_as_auto && is_pk) {
        parts.push("NOT NULL".to_string());
    }

    // DEFAULT (skip for auto-increment columns).
    // MySQL <8.0.13 rejects DEFAULT on BLOB/TEXT/GEOMETRY/JSON columns with
    // ERROR 1101; >=8.0.13 only accepts expression defaults wrapped in
    // parens, which would still drop a literal default like '{}'. Conservative
    // behavior here: drop the default when targeting MySQL on a column whose
    // canonical type is in that no-default class. The column still gets
    // created, just without the default. See #34.
    let mysql_target = target_dialect == Dialect::Mysql;
    // CanonicalType::Array also lands here — pg `text[]` serializes to MySQL
    // `JSON` (see mysql::from_canonical), so it inherits the same "no
    // DEFAULT on JSON" rule. Without this, pg→mysql arrays with `'{}'`
    // defaults trip ERROR 1101 the same way native JSON columns do.
    let no_default_on_mysql = mysql_target
        && matches!(
            canonical,
            crate::ddl_typemap::CanonicalType::Json
                | crate::ddl_typemap::CanonicalType::Jsonb
                | crate::ddl_typemap::CanonicalType::Text
                | crate::ddl_typemap::CanonicalType::Bytes { length: None }
                | crate::ddl_typemap::CanonicalType::Array { .. }
        );
    if !render_as_auto && !no_default_on_mysql {
        if let Some(ref default) = col.column_default {
            let mut ddl_default =
                format_ddl_default_typed(default, source_dialect, target_dialect, is_boolean);
            // Sub-second precision symmetry on MySQL: when the column is
            // `DATETIME(N)` / `TIMESTAMP(N)`, MySQL requires the matching
            // function-default to also carry `(N)` — `DATETIME(6) DEFAULT
            // CURRENT_TIMESTAMP` (no precision) is rejected with
            // ERROR 1067 "Invalid default value". translate_default_function
            // strips precision from the now-family for cross-dialect work
            // (#32); we re-attach it here when targeting mysql so the
            // column type and default precision match. See #36.
            if mysql_target {
                if let Some(p) = temporal_precision(&canonical) {
                    ddl_default = reattach_now_family_precision(&ddl_default, p);
                }
            }
            parts.push(format!("DEFAULT {ddl_default}"));
        }
    }

    // Auto-increment suffix (MySQL, MSSQL, SQLite)
    if render_as_auto {
        let suffix = format_autoincrement_suffix(col, target_dialect, is_pk);
        if !suffix.is_empty() {
            parts.push(suffix);
        }
    }

    // MySQL inline column comment
    if target_dialect == Dialect::Mysql {
        if let Some(ref comment) = col.comment {
            parts.push(format!("COMMENT '{}'", comment.replace('\'', "''")));
        }
    }

    parts.join(" ")
}

/// Get the type string for an auto-increment column, potentially overriding the base type.
fn format_autoincrement_type(
    col: &ColumnInfo,
    source_dialect: Dialect,
    target_dialect: Dialect,
    is_pk: bool,
) -> String {
    let base_type = ddl_typemap::map_ddl_type(col, source_dialect, target_dialect).sql_type;

    match target_dialect {
        Dialect::Postgres => {
            // Use SERIAL/BIGSERIAL which implies the type
            if base_type.contains("BIG") {
                "BIGSERIAL".to_string()
            } else {
                "SERIAL".to_string()
            }
        }
        Dialect::Sqlite if is_pk => {
            // SQLite AUTOINCREMENT only works with INTEGER PRIMARY KEY
            "INTEGER".to_string()
        }
        _ => base_type,
    }
}

/// Get the auto-increment suffix to append after the type.
fn format_autoincrement_suffix(col: &ColumnInfo, target_dialect: Dialect, is_pk: bool) -> String {
    match target_dialect {
        Dialect::Postgres => String::new(), // SERIAL/BIGSERIAL handles it
        Dialect::Mysql => "AUTO_INCREMENT".to_string(),
        Dialect::Sqlite if is_pk => "PRIMARY KEY AUTOINCREMENT".to_string(),
        Dialect::Sqlite => String::new(),
        Dialect::Mssql => {
            let (start, inc) = col
                .identity
                .as_ref()
                .map(|id| (id.start, id.increment))
                .unwrap_or((1, 1));
            format!("IDENTITY({start}, {inc})")
        }
    }
}
