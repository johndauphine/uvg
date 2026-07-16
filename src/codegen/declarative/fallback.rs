use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::{
    enum_class_name, escape_python_string, find_enum_for_column, format_fk_options,
    format_index_kwargs, format_python_string_literal, format_server_default, is_serial_default,
    is_unique_constraint_index, quote_constraint_columns,
};
use crate::dialect::Dialect;
use crate::naming::table_to_variable_name;
use crate::schema::{ConstraintType, EnumInfo, TableInfo};
use crate::typemap::{map_column_type, map_column_type_dialect};
use std::collections::HashMap;

/// Generate a Table() assignment for a table without a primary key.
/// Uses the provided `metadata_ref` (e.g. `Base.metadata` or standalone `metadata`).
pub(super) fn generate_table_fallback(
    table: &TableInfo,
    imports: &mut ImportCollector,
    options: &GeneratorOptions,
    dialect: Dialect,
    metadata_ref: &str,
    enums: &[EnumInfo],
    synthetic_enum_cols: &HashMap<(String, String), String>,
) -> String {
    let var_name = table_to_variable_name(&table.name);
    let mut lines: Vec<String> = Vec::new();

    lines.push(format!("{var_name} = Table("));
    lines.push(format!("    '{}', {metadata_ref},", table.name));

    let mut body_items: Vec<String> = Vec::new();

    for col in &table.columns {
        let enum_key = (table.name.clone(), col.name.clone());
        let sa_type = if let Some(class_name) = synthetic_enum_cols.get(&enum_key) {
            format!(
                "Enum({class_name}, values_callable=lambda cls: [member.value for member in cls])"
            )
        } else if let Some(enum_info) = find_enum_for_column(&col.udt_name, enums) {
            let mut enum_parts = vec![
                enum_class_name(&enum_info.name),
                "values_callable=lambda cls: [member.value for member in cls]".to_string(),
                format!("name={}", format_python_string_literal(&enum_info.name)),
            ];
            if let Some(ref schema) = enum_info.schema {
                if !schema.is_empty() {
                    enum_parts.push(format!("schema={}", format_python_string_literal(schema)));
                }
            }
            format!("Enum({})", enum_parts.join(", "))
        } else {
            let mapped = if options.keep_dialect_types {
                map_column_type_dialect(col, dialect)
            } else {
                map_column_type(col, dialect)
            };
            imports.add(&mapped.import_module, &mapped.import_name);
            if let Some((ref elem_mod, ref elem_name)) = mapped.element_import {
                imports.add(elem_mod, elem_name);
            }
            mapped.sa_type
        };

        let mut col_args: Vec<String> = Vec::new();
        col_args.push(format!("'{}'", col.name));
        col_args.push(sa_type);

        if let Some(ref identity) = col.identity {
            imports.add("sqlalchemy", "Identity");
            match dialect {
                Dialect::Postgres => {
                    col_args.push(format!(
                        "Identity(start={}, increment={}, minvalue={}, maxvalue={}, cycle=False, cache={})",
                        identity.start, identity.increment, identity.min_value, identity.max_value, identity.cache
                    ));
                }
                Dialect::Mssql | Dialect::Mysql | Dialect::Sqlite => {
                    col_args.push(format!(
                        "Identity(start={}, increment={})",
                        identity.start, identity.increment
                    ));
                }
            }
        }

        if !col.is_nullable {
            col_args.push("nullable=False".to_string());
        }

        if let Some(ref default) = col.column_default {
            if !is_serial_default(default, dialect) {
                imports.add("sqlalchemy", "text");
                let formatted = format_server_default(default, dialect);
                col_args.push(format!("server_default={formatted}"));
            }
        }

        if !options.nocomments {
            if let Some(ref comment) = col.comment {
                col_args.push(format!("comment='{}'", escape_python_string(comment)));
            }
        }

        body_items.push(format!("Column({})", col_args.join(", ")));
    }

    if !options.noconstraints {
        for constraint in &table.constraints {
            if constraint.constraint_type == ConstraintType::ForeignKey {
                if let Some(ref fk) = constraint.foreign_key {
                    imports.add("sqlalchemy", "ForeignKeyConstraint");
                    let local_cols: Vec<String> = constraint
                        .columns
                        .iter()
                        .map(|c| format!("'{c}'"))
                        .collect();
                    let ref_cols: Vec<String> = fk
                        .ref_columns
                        .iter()
                        .map(|c| format!("'{}.{c}'", fk.ref_table))
                        .collect();
                    let fk_opts = format_fk_options(fk);
                    let name_part = if !options.nofknames {
                        format!(", name='{}'", constraint.name)
                    } else {
                        String::new()
                    };
                    body_items.push(format!(
                        "ForeignKeyConstraint([{}], [{}]{}{})",
                        local_cols.join(", "),
                        ref_cols.join(", "),
                        name_part,
                        fk_opts
                    ));
                }
            }
        }
    }

    if !options.noconstraints {
        for constraint in &table.constraints {
            if constraint.constraint_type == ConstraintType::Unique {
                imports.add("sqlalchemy", "UniqueConstraint");
                let cols = quote_constraint_columns(&constraint.columns);
                body_items.push(format!(
                    "UniqueConstraint({}, name='{}')",
                    cols.join(", "),
                    constraint.name
                ));
            }
        }
    }

    if !options.noindexes {
        for index in &table.indexes {
            if is_unique_constraint_index(index, &table.constraints) {
                continue;
            }
            imports.add("sqlalchemy", "Index");
            let cols = quote_constraint_columns(&index.columns);
            let unique_str = if index.is_unique { ", unique=True" } else { "" };
            let kwargs_str = format_index_kwargs(&index.kwargs);
            body_items.push(format!(
                "Index('{}', {}{}{})",
                index.name,
                cols.join(", "),
                unique_str,
                kwargs_str
            ));
        }
    }

    if table.schema != dialect.default_schema() {
        body_items.push(format!("schema='{}'", table.schema));
    }

    let last = body_items.len().saturating_sub(1);
    for (i, item) in body_items.iter().enumerate() {
        if i < last {
            lines.push(format!("    {item},"));
        } else {
            lines.push(format!("    {item}"));
        }
    }

    lines.push(")".to_string());

    lines.join("\n")
}
