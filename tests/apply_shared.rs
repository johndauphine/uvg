mod common;

use common::{exec_sql, tmpdir};
use uvg::apply::{apply_manifest, apply_sql, ApplyOptions, ParseCheckStatus};
use uvg::connection::parse_connection_url;
use uvg::output::{Manifest, Stats};

async fn table_exists(db_path: &std::path::Path, table: &str) -> bool {
    let url = format!("sqlite://{}?mode=ro", db_path.display());
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("sqlite connect");
    let found: Option<(i64,)> =
        sqlx::query_as("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1")
            .bind(table)
            .fetch_optional(&pool)
            .await
            .expect("table lookup");
    pool.close().await;
    found.is_some()
}

#[tokio::test]
async fn shared_apply_executes_valid_sql_and_blocks_marker_blobs_atomically() {
    let dir = tmpdir("shared-apply-safety");
    let target = dir.join("target.db");
    exec_sql(
        &target,
        "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
    )
    .await;

    let target_url = format!(
        "sqlite:////{}",
        target.display().to_string().trim_start_matches('/')
    );
    let config = parse_connection_url(&target_url, false).unwrap();
    let options = ApplyOptions::new(true, 7, false);

    let report = apply_sql(
        &config,
        "CREATE TABLE accepted(id INTEGER PRIMARY KEY);",
        "integration test",
        options,
    )
    .await
    .unwrap();
    assert_eq!(report.parse_check, ParseCheckStatus::SkippedUnsupported);
    assert_eq!(report.statements.len(), 1);
    assert!(report.statements[0].error.is_none());
    assert!(table_exists(&target, "accepted").await);

    let error = apply_sql(
        &config,
        "CREATE TABLE must_not_land(id INTEGER);\n\
         -- WARNING: SQLite does not support ALTER COLUMN. Table recreation required.",
        "interactive ddl",
        options,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("refusing to apply"), "{error}");
    assert!(
        !table_exists(&target, "must_not_land").await,
        "marker validation must reject the whole blob before execution"
    );

    let error = apply_sql(
        &config,
        "CREATE TABLE constraint_drop_must_not_land(id INTEGER);\n\
         -- WARNING: SQLite cannot drop constraint fk_child_parent without rebuilding table child",
        "interactive ddl",
        options,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(error.contains("refusing to apply"), "{error}");
    assert!(error.contains("SQLite cannot drop constraint"), "{error}");
    assert!(
        !table_exists(&target, "constraint_drop_must_not_land").await,
        "constraint-drop validation must reject the whole blob before execution"
    );

    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn manifest_failure_warns_that_nontransactional_work_may_have_persisted() {
    let dir = tmpdir("manifest-partial-apply");
    let target = dir.join("target.db");
    exec_sql(
        &target,
        "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
    )
    .await;

    let migration_dir = dir.join("users");
    std::fs::create_dir_all(&migration_dir).unwrap();
    let relative_file = "users/change.sql";
    std::fs::write(
        dir.join(relative_file),
        "CREATE TABLE persisted(id INTEGER PRIMARY KEY);\n\
         CREATE TABLE persisted(id INTEGER PRIMARY KEY);",
    )
    .unwrap();
    let manifest = Manifest {
        run_id: "test-run".to_string(),
        generated_at: "2026-01-01T00:00:00Z".to_string(),
        uvg_version: "test".to_string(),
        source_dialect: "sqlite".to_string(),
        target_dialect: "sqlite".to_string(),
        files: vec![relative_file.to_string()],
        stats: Stats { changes: 2 },
    };

    let target_url = format!(
        "sqlite:////{}",
        target.display().to_string().trim_start_matches('/')
    );
    let config = parse_connection_url(&target_url, false).unwrap();
    let error = apply_manifest(
        &config,
        &manifest,
        &dir,
        &target_url,
        ApplyOptions::new(false, 0, false),
    )
    .await
    .unwrap_err()
    .to_string();

    assert!(error.contains("nontransactional"), "{error}");
    assert!(error.contains("may have persisted"), "{error}");
    assert!(
        error.contains("target may be partially migrated"),
        "{error}"
    );
    assert!(table_exists(&target, "persisted").await);

    std::fs::remove_dir_all(dir).ok();
}
