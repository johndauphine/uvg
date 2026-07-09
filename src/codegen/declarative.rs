mod association;
mod attrs;
mod class;
mod fallback;
mod table_args;

use self::association::generate_association_table;
use self::class::generate_class;
use self::fallback::generate_table_fallback;
use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::python::PythonOutput;
use crate::codegen::relationships::is_association_table;
use crate::codegen::{
    enum_class_name, find_enum_for_column, generate_enum_class, has_primary_key, parse_check_enum,
    topo_sort_tables,
};
use crate::naming::{table_to_class_name, table_to_variable_name};
use crate::schema::EnumInfo;
use crate::schema::{ConstraintType, IntrospectedSchema};
use std::collections::{HashMap, HashSet};

/// Generate declarative ORM output as a single file.
pub fn generate(schema: &IntrospectedSchema, options: &GeneratorOptions) -> String {
    parts(schema, options).render()
}

/// Generate declarative ORM output split one file per model.
pub fn generate_split(
    schema: &IntrospectedSchema,
    options: &GeneratorOptions,
) -> Vec<(String, String)> {
    parts(schema, options).split()
}

/// Build the structured output: prelude (imports, enum classes, Base or
/// metadata) plus one named block per model class / fallback table.
fn parts(schema: &IntrospectedSchema, options: &GeneratorOptions) -> PythonOutput {
    let mut imports = ImportCollector::new();
    let mut blocks: Vec<(String, String)> = Vec::new();
    let mut needs_optional = false;
    let mut needs_datetime = false;
    let mut needs_decimal = false;
    let mut needs_uuid = false;

    let has_any_pk = schema
        .tables
        .iter()
        .any(|t| has_primary_key(&t.constraints));
    let has_any_no_pk = schema
        .tables
        .iter()
        .any(|t| !has_primary_key(&t.constraints));

    if has_any_pk {
        imports.add("sqlalchemy.orm", "DeclarativeBase");
        imports.add("sqlalchemy.orm", "Mapped");
        imports.add("sqlalchemy.orm", "mapped_column");
    } else {
        imports.add("sqlalchemy", "MetaData");
    }

    if has_any_no_pk {
        imports.add("sqlalchemy", "Table");
        imports.add("sqlalchemy", "Column");
    }

    let metadata_ref = if has_any_pk {
        "Base.metadata"
    } else {
        "metadata"
    };

    // Collect named enums and synthetic enums from check constraints.
    let mut all_enums: Vec<EnumInfo> = schema.enums.clone();
    let mut synthetic_enum_cols: HashMap<(String, String), String> = HashMap::new();

    let sorted_tables = topo_sort_tables(&schema.tables);

    if !options.nosyntheticenums {
        for table_ref in &sorted_tables {
            for constraint in &table_ref.constraints {
                if constraint.constraint_type == ConstraintType::Check {
                    if let Some(ref expr) = constraint.check_expression {
                        if let Some((col_name, values)) = parse_check_enum(expr) {
                            let key = (table_ref.name.clone(), col_name.clone());
                            if let std::collections::hash_map::Entry::Vacant(entry) =
                                synthetic_enum_cols.entry(key)
                            {
                                use heck::ToUpperCamelCase;
                                let enum_name = format!("{}_{}", table_ref.name, col_name)
                                    .to_upper_camel_case();
                                let ei = EnumInfo {
                                    name: enum_name.clone(),
                                    schema: None,
                                    values,
                                };
                                all_enums.push(ei);
                                entry.insert(enum_name);
                            }
                        }
                    }
                }
            }
        }
    }

    let mut used_enum_names: HashSet<String> = HashSet::new();

    for table in &sorted_tables {
        // Only track enum usage for tables that will render Enum() types
        // (classes with PK, not fallback Table() or association tables).
        let renders_enums = has_primary_key(&table.constraints) && !is_association_table(table);
        if renders_enums {
            for col_info in &table.columns {
                if find_enum_for_column(&col_info.udt_name, &all_enums).is_some() {
                    used_enum_names.insert(col_info.udt_name.clone());
                }
                let key = (table.name.clone(), col_info.name.clone());
                if let Some(class_name) = synthetic_enum_cols.get(&key) {
                    used_enum_names.insert(class_name.clone());
                }
            }
        }

        if is_association_table(table) {
            let block = generate_association_table(
                table,
                &mut imports,
                options,
                schema.dialect,
                metadata_ref,
            );
            blocks.push((table_to_variable_name(&table.name), block));
        } else if has_primary_key(&table.constraints) {
            let (block, meta) = generate_class(
                table,
                &mut imports,
                options,
                schema.dialect,
                schema,
                &all_enums,
                &synthetic_enum_cols,
            );
            if meta.needs_optional {
                needs_optional = true;
            }
            if meta.needs_datetime {
                needs_datetime = true;
            }
            if meta.needs_decimal {
                needs_decimal = true;
            }
            if meta.needs_uuid {
                needs_uuid = true;
            }
            // Module name matches the historical text-splitter output:
            // snake_case of the generated class name.
            use heck::ToSnakeCase;
            blocks.push((table_to_class_name(&table.name).to_snake_case(), block));
        } else {
            let block =
                generate_table_fallback(table, &mut imports, options, schema.dialect, metadata_ref);
            blocks.push((table_to_variable_name(&table.name), block));
        }
    }

    let used_enums: Vec<&EnumInfo> = all_enums
        .iter()
        .filter(|ei| {
            used_enum_names.contains(&ei.name)
                || used_enum_names.contains(&enum_class_name(&ei.name))
        })
        .collect();

    if !used_enums.is_empty() {
        imports.add_bare("enum");
        imports.add("sqlalchemy", "Enum");
    }

    if needs_optional {
        imports.add("typing", "Optional");
    }
    if needs_datetime {
        imports.add_bare("datetime");
    }
    if needs_decimal {
        imports.add_bare("decimal");
    }
    if needs_uuid {
        imports.add_bare("uuid");
    }

    let mut prelude = imports.render();

    for ei in &used_enums {
        prelude.push_str("\n\n");
        prelude.push_str(&generate_enum_class(ei));
    }

    if has_any_pk {
        prelude.push_str("\n\nclass Base(DeclarativeBase):\n    pass");
    } else {
        prelude.push_str("\n\nmetadata = MetaData()");
    }

    PythonOutput {
        prelude,
        models: blocks,
        separator: "\n\n\n",
    }
}

#[cfg(test)]
#[path = "declarative_tests/mod.rs"]
mod tests;
