mod columns;
mod constraints;
mod indexes;
mod tables;

use sqlx::SqlitePool;

use crate::cli::GeneratorOptions;
use crate::dialect::Dialect;
use crate::error::UvgError;
use crate::schema::IntrospectedSchema;

/// Introspect a SQLite database and return the full schema metadata.
/// SQLite has no schema concept, so schemas parameter is not needed.
pub async fn introspect(
    pool: &SqlitePool,
    table_filter: &[String],
    noviews: bool,
    _options: &GeneratorOptions,
) -> Result<IntrospectedSchema, UvgError> {
    let mut all_tables = tables::query_tables(pool, noviews).await?;

    if !table_filter.is_empty() {
        all_tables.retain(|t| table_filter.contains(&t.name));
    }

    for table in &mut all_tables {
        let create_sql = tables::query_create_sql(pool, &table.name).await?;
        table.columns = columns::query_columns(pool, &table.name, &create_sql).await?;
        table.constraints =
            constraints::query_constraints(pool, &table.name, &create_sql).await?;
        table.indexes = indexes::query_indexes(pool, &table.name).await?;
    }

    // Sort alphabetically to match sqlacodegen output
    all_tables.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(IntrospectedSchema {
        dialect: Dialect::Sqlite,
        tables: all_tables,
        enums: vec![],
        domains: vec![],
    })
}
