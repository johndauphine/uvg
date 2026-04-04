//! Relationship inference from foreign key constraints.
//!
//! Analyzes FK constraints across tables to determine:
//! - Which columns should use inline `ForeignKey()` vs `ForeignKeyConstraint` in `__table_args__`
//! - What `relationship()` calls to generate on each class

use crate::naming::table_to_class_name;
use crate::schema::{ConstraintInfo, ConstraintType, IntrospectedSchema, TableInfo};

/// A single-column FK that should be rendered inline on mapped_column().
#[derive(Debug, Clone)]
pub struct InlineFK {
    /// The FK target: "table.column"
    pub target: String,
}

/// A relationship() call to generate on a class.
#[derive(Debug, Clone)]
pub struct RelationshipInfo {
    /// The Python attribute name for this relationship
    pub attr_name: String,
    /// The target class name
    pub target_class: String,
    /// Whether this is a collection (list) or scalar (Optional)
    pub is_collection: bool,
    /// Whether the FK column is nullable (affects Mapped type)
    pub is_nullable: bool,
    /// back_populates value
    pub back_populates: String,
    /// Optional remote_side (for self-referential)
    pub remote_side: Option<String>,
    /// Optional foreign_keys (for multi-reference disambiguation)
    pub foreign_keys: Option<String>,
}

/// Find the single-column FK constraint for a given column, if any.
pub fn find_inline_fk<'a>(
    col_name: &str,
    constraints: &'a [ConstraintInfo],
) -> Option<&'a ConstraintInfo> {
    constraints.iter().find(|c| {
        c.constraint_type == ConstraintType::ForeignKey
            && c.columns.len() == 1
            && c.columns[0] == col_name
    })
}

/// Check if a FK constraint is single-column (should be rendered inline).
pub fn is_single_column_fk(constraint: &ConstraintInfo) -> bool {
    constraint.constraint_type == ConstraintType::ForeignKey && constraint.columns.len() == 1
}

/// Derive the relationship attribute name on the "many" side (child table).
/// Strips `_id` suffix from the FK column name.
/// e.g., "container_id" → "container", "parent_item_id" → "parent_item"
fn fk_col_to_relationship_name(col_name: &str) -> String {
    col_name
        .strip_suffix("_id")
        .unwrap_or(col_name)
        .to_string()
}

/// Count how many FK constraints from `table` reference `target_table`.
fn count_fks_to_table(table: &TableInfo, target_table: &str) -> usize {
    table
        .constraints
        .iter()
        .filter(|c| {
            c.constraint_type == ConstraintType::ForeignKey
                && c.foreign_key
                    .as_ref()
                    .map_or(false, |fk| fk.ref_table == target_table)
        })
        .count()
}

/// Check if a FK is self-referential (references the same table).
fn is_self_referential(constraint: &ConstraintInfo, table_name: &str) -> bool {
    constraint
        .foreign_key
        .as_ref()
        .map_or(false, |fk| fk.ref_table == table_name)
}

/// Generate relationships for a table based on its FK constraints.
/// Returns relationships for this table (the "many"/child side).
pub fn generate_child_relationships(
    table: &TableInfo,
    schema: &IntrospectedSchema,
) -> Vec<RelationshipInfo> {
    let mut rels = Vec::new();

    let fk_constraints: Vec<&ConstraintInfo> = table
        .constraints
        .iter()
        .filter(|c| c.constraint_type == ConstraintType::ForeignKey)
        .collect();

    for constraint in &fk_constraints {
        let fk = match &constraint.foreign_key {
            Some(fk) => fk,
            None => continue,
        };

        let target_class = table_to_class_name(&fk.ref_table);
        let is_selfref = fk.ref_table == table.name;
        let multi_ref = count_fks_to_table(table, &fk.ref_table) > 1;

        if is_single_column_fk(constraint) {
            let col_name = &constraint.columns[0];
            let rel_name = fk_col_to_relationship_name(col_name);

            // Determine if the FK column is nullable
            let is_nullable = table
                .columns
                .iter()
                .find(|c| c.name == *col_name)
                .map_or(true, |c| c.is_nullable);

            if is_selfref {
                // Self-referential: generate forward + reverse pair
                let reverse_name = format!("{rel_name}_reverse");

                rels.push(RelationshipInfo {
                    attr_name: rel_name.clone(),
                    target_class: target_class.clone(),
                    is_collection: false,
                    is_nullable,
                    back_populates: reverse_name.clone(),
                    remote_side: Some("id".to_string()),
                    foreign_keys: if multi_ref {
                        Some(format!("[{col_name}]"))
                    } else {
                        None
                    },
                });
                rels.push(RelationshipInfo {
                    attr_name: reverse_name,
                    target_class,
                    is_collection: true,
                    is_nullable: false,
                    back_populates: rel_name,
                    remote_side: Some(col_name.to_string()),
                    foreign_keys: if multi_ref {
                        Some(format!("[{col_name}]"))
                    } else {
                        None
                    },
                });
            } else {
                // Normal FK: child-side relationship
                let back_pop = if multi_ref {
                    // Disambiguated: parent uses "{child_table}_{rel_name}"
                    format!(
                        "{}_{}",
                        table_to_variable_name_bare(&table.name),
                        rel_name
                    )
                } else {
                    table_to_variable_name_bare(&table.name)
                };

                rels.push(RelationshipInfo {
                    attr_name: rel_name,
                    target_class,
                    is_collection: false,
                    is_nullable,
                    back_populates: back_pop,
                    remote_side: None,
                    foreign_keys: if multi_ref {
                        Some(format!("[{col_name}]"))
                    } else {
                        None
                    },
                });
            }
        } else {
            // Composite FK: relationship without inline FK
            let is_nullable = constraint.columns.iter().any(|col_name| {
                table
                    .columns
                    .iter()
                    .find(|c| c.name == *col_name)
                    .map_or(true, |c| c.is_nullable)
            });

            let rel_name = table_to_variable_name_bare(&fk.ref_table);
            let back_pop = table_to_variable_name_bare(&table.name);

            rels.push(RelationshipInfo {
                attr_name: rel_name,
                target_class,
                is_collection: false,
                is_nullable,
                back_populates: back_pop,
                remote_side: None,
                foreign_keys: None,
            });
        }
    }

    rels
}

/// Generate reverse relationships for a parent table based on child FKs pointing to it.
pub fn generate_parent_relationships(
    parent_table: &TableInfo,
    schema: &IntrospectedSchema,
) -> Vec<RelationshipInfo> {
    let mut rels = Vec::new();

    for child_table in &schema.tables {
        // Skip self — self-referential rels are handled on the child side
        if child_table.name == parent_table.name {
            continue;
        }

        let fk_constraints: Vec<&ConstraintInfo> = child_table
            .constraints
            .iter()
            .filter(|c| {
                c.constraint_type == ConstraintType::ForeignKey
                    && c.foreign_key
                        .as_ref()
                        .map_or(false, |fk| fk.ref_table == parent_table.name)
            })
            .collect();

        let multi_ref = fk_constraints.len() > 1;
        let child_class = table_to_class_name(&child_table.name);

        for constraint in &fk_constraints {
            if is_single_column_fk(constraint) {
                let col_name = &constraint.columns[0];
                let child_rel_name = fk_col_to_relationship_name(col_name);

                let attr_name = if multi_ref {
                    format!(
                        "{}_{}",
                        table_to_variable_name_bare(&child_table.name),
                        child_rel_name
                    )
                } else {
                    table_to_variable_name_bare(&child_table.name)
                };

                rels.push(RelationshipInfo {
                    attr_name,
                    target_class: child_class.clone(),
                    is_collection: true,
                    is_nullable: false,
                    back_populates: child_rel_name,
                    remote_side: None,
                    foreign_keys: if multi_ref {
                        Some(format!(
                            "'[{}.{}]'",
                            child_class, col_name
                        ))
                    } else {
                        None
                    },
                });
            } else {
                // Composite FK reverse
                let attr_name = table_to_variable_name_bare(&child_table.name);
                let back_pop = table_to_variable_name_bare(&parent_table.name);

                rels.push(RelationshipInfo {
                    attr_name,
                    target_class: child_class.clone(),
                    is_collection: true,
                    is_nullable: false,
                    back_populates: back_pop,
                    remote_side: None,
                    foreign_keys: None,
                });
            }
        }
    }

    rels
}

/// Table name without the "t_" prefix (for relationship attribute names).
fn table_to_variable_name_bare(table_name: &str) -> String {
    table_name.to_string()
}

/// Render a relationship line.
pub fn render_relationship(rel: &RelationshipInfo, class_name: &str) -> String {
    let type_annotation = if rel.is_collection {
        format!("list['{}']", rel.target_class)
    } else if rel.is_nullable {
        format!("Optional['{}']", rel.target_class)
    } else {
        format!("'{}'", rel.target_class)
    };

    let mut args = Vec::new();
    args.push(format!("'{}'", rel.target_class));

    if let Some(ref rs) = rel.remote_side {
        args.push(format!("remote_side=[{}]", rs));
    }

    if let Some(ref fk) = rel.foreign_keys {
        args.push(format!("foreign_keys={fk}"));
    }

    args.push(format!("back_populates='{}'", rel.back_populates));

    let args_str = args.join(", ");
    format!(
        "    {}: Mapped[{type_annotation}] = relationship({args_str})",
        rel.attr_name
    )
}
