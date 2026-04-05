use std::collections::BTreeMap;

use sqlx::SqlitePool;

use crate::error::UvgError;
use crate::schema::IndexInfo;

pub async fn query_indexes(
    pool: &SqlitePool,
    table_name: &str,
) -> Result<Vec<IndexInfo>, UvgError> {
    // Get user-created indexes (origin = 'c') — excludes constraint-backing indexes
    let index_rows = sqlx::query_as::<_, IndexListRow>(
        r#"SELECT name, "unique", origin FROM pragma_index_list(?) WHERE origin = 'c'"#,
    )
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    let mut indexes = Vec::new();
    for idx in index_rows {
        let col_rows = sqlx::query_as::<_, IndexInfoRow>(
            "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
        )
        .bind(&idx.name)
        .fetch_all(pool)
        .await?;

        let columns: Vec<String> = col_rows.into_iter().map(|r| r.name).collect();
        indexes.push(IndexInfo {
            name: idx.name,
            is_unique: idx.unique,
            columns,
            kwargs: BTreeMap::new(),
        });
    }

    Ok(indexes)
}

#[derive(sqlx::FromRow)]
struct IndexListRow {
    name: String,
    unique: bool,
    #[allow(dead_code)]
    origin: String,
}

#[derive(sqlx::FromRow)]
struct IndexInfoRow {
    name: String,
}
