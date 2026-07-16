//! Small pure queries over introspected schema structures.

/// Check if a table has any primary key constraint.
pub fn has_primary_key(constraints: &[crate::schema::ConstraintInfo]) -> bool {
    constraints
        .iter()
        .any(|c| c.constraint_type == crate::schema::ConstraintType::PrimaryKey)
}

/// Check if a column is part of the primary key.
pub fn is_primary_key_column(
    col_name: &str,
    constraints: &[crate::schema::ConstraintInfo],
) -> bool {
    constraints.iter().any(|c| {
        c.constraint_type == crate::schema::ConstraintType::PrimaryKey
            && c.columns.contains(&col_name.to_string())
    })
}

/// Check if an index is just backing a unique constraint (same columns).
pub fn is_unique_constraint_index(
    index: &crate::schema::IndexInfo,
    constraints: &[crate::schema::ConstraintInfo],
) -> bool {
    if !index.is_unique {
        return false;
    }
    constraints.iter().any(|c| {
        c.constraint_type == crate::schema::ConstraintType::Unique && c.columns == index.columns
    })
}

/// Find the enum info for a column's udt_name in the schema.
pub fn find_enum_for_column<'a>(
    udt_name: &str,
    enums: &'a [crate::schema::EnumInfo],
) -> Option<&'a crate::schema::EnumInfo> {
    enums.iter().find(|e| e.name == udt_name)
}

/// Resolve a PostgreSQL enum column by its full type identity when that
/// identity is available. Older snapshots do not carry `udt_schema`, so they
/// fall back to the table schema and finally to an unambiguous name match.
pub(crate) fn find_enum_for_ddl_column<'a>(
    column: &crate::schema::ColumnInfo,
    table_schema: &str,
    enums: &'a [crate::schema::EnumInfo],
) -> Option<&'a crate::schema::EnumInfo> {
    let udt_name = enum_udt_name(column);
    let candidates: Vec<&crate::schema::EnumInfo> =
        enums.iter().filter(|e| e.name == udt_name).collect();

    if let Some(udt_schema) = column.udt_schema.as_deref() {
        return candidates
            .iter()
            .copied()
            .find(|e| e.schema.as_deref() == Some(udt_schema));
    } else if let Some(local) = candidates
        .iter()
        .copied()
        .find(|e| e.schema.as_deref() == Some(table_schema))
    {
        return Some(local);
    }

    match candidates.as_slice() {
        [only] => Some(*only),
        _ => None,
    }
}

pub(crate) fn is_enum_array_column(column: &crate::schema::ColumnInfo) -> bool {
    column.data_type.eq_ignore_ascii_case("array")
}

fn enum_udt_name(column: &crate::schema::ColumnInfo) -> &str {
    if is_enum_array_column(column) {
        column
            .udt_name
            .strip_prefix('_')
            .unwrap_or(&column.udt_name)
    } else {
        &column.udt_name
    }
}
