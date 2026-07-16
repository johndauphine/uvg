use std::collections::HashSet;

use crate::codegen::find_enum_for_ddl_column;
use crate::dialect::Dialect;
use crate::schema::{EnumInfo, IntrospectedSchema, TableType};

use crate::codegen::render::ident::qualified_object_name;

/// Generate CREATE TYPE statements for enums (PG target only).
pub(super) fn generate_enum_types(
    schema: &IntrospectedSchema,
    target_dialect: Dialect,
) -> Vec<String> {
    if !target_dialect.supports_native_enums() || schema.enums.is_empty() {
        return vec![];
    }

    referenced_enums(schema)
        .into_iter()
        .map(|e| generate_enum_type(e, target_dialect))
        .collect()
}

/// Return only enum definitions referenced by the already-filtered source
/// tables. Introspection deliberately retains the complete enum registry so
/// diffs can detect an existing type identity, but DDL must not create types
/// belonging only to tables excluded by `--tables`.
pub(crate) fn referenced_enums(schema: &IntrospectedSchema) -> Vec<&EnumInfo> {
    let mut identities: HashSet<(Option<&str>, &str)> = HashSet::new();
    for table in schema
        .tables
        .iter()
        .filter(|table| table.table_type == TableType::Table)
    {
        for column in &table.columns {
            if let Some(enum_info) = find_enum_for_ddl_column(column, &table.schema, &schema.enums)
            {
                identities.insert((enum_info.schema.as_deref(), enum_info.name.as_str()));
            }
        }
    }

    schema
        .enums
        .iter()
        .filter(|enum_info| {
            identities.contains(&(enum_info.schema.as_deref(), enum_info.name.as_str()))
        })
        .collect()
}

/// Generate one PostgreSQL enum declaration. Schema diffs reuse this so enum
/// types are created before any newly added table references them.
pub(crate) fn generate_enum_type(enum_info: &EnumInfo, target_dialect: Dialect) -> String {
    let qname = qualified_object_name(enum_info.schema.as_deref(), &enum_info.name, target_dialect);
    let values: Vec<String> = enum_info
        .values
        .iter()
        .map(|v| format!("'{}'", v.replace('\'', "''")))
        .collect();
    format!("CREATE TYPE {qname} AS ENUM ({});", values.join(", "))
}
