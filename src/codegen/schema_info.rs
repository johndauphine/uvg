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
