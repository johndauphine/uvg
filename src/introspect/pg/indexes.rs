use sqlx::PgPool;

use crate::error::UvgError;
use crate::schema::IndexInfo;

pub async fn query_indexes(
    pool: &PgPool,
    schema: &str,
    table_name: &str,
) -> Result<Vec<IndexInfo>, UvgError> {
    let rows = sqlx::query_as::<_, IndexRow>(
        r#"
        SELECT i.relname AS index_name, ix.indisunique AS is_unique,
               am.amname AS access_method,
               array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) AS columns
        FROM pg_index ix
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON am.oid = i.relam
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE n.nspname = $1 AND t.relname = $2 AND NOT ix.indisprimary
        GROUP BY i.relname, ix.indisunique, am.amname
        ORDER BY i.relname
        "#,
    )
    .bind(schema)
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    let indexes = rows.into_iter().map(index_from_row).collect();

    Ok(indexes)
}

#[derive(sqlx::FromRow)]
struct IndexRow {
    index_name: String,
    is_unique: bool,
    access_method: String,
    columns: Vec<String>,
}

fn index_from_row(row: IndexRow) -> IndexInfo {
    let mut index = IndexInfo::new(row.index_name, row.is_unique, row.columns);
    if row.access_method != "btree" {
        index
            .kwargs
            .insert("postgresql_using".to_string(), row.access_method);
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_non_btree_access_method() {
        let index = index_from_row(IndexRow {
            index_name: "film_fulltext_idx".to_string(),
            is_unique: false,
            access_method: "gist".to_string(),
            columns: vec!["fulltext".to_string()],
        });

        assert_eq!(
            index.kwargs.get("postgresql_using").map(String::as_str),
            Some("gist")
        );
    }

    #[test]
    fn omits_default_btree_access_method() {
        let index = index_from_row(IndexRow {
            index_name: "ix_title".to_string(),
            is_unique: false,
            access_method: "btree".to_string(),
            columns: vec!["title".to_string()],
        });

        assert!(!index.kwargs.contains_key("postgresql_using"));
    }
}
