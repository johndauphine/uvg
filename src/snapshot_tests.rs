use super::*;
use crate::testutil::{col, schema_pg, schema_sqlite, table};

#[test]
fn snapshot_round_trips_schema() {
    let schema = schema_sqlite(vec![table("users")
        .column(col("id").udt("INTEGER").build())
        .pk("users_pkey", &["id"])
        .build()]);
    let raw = serde_yaml::to_string(&SnapshotFile::from_schema(&schema)).unwrap();

    let loaded = load_str(&raw).unwrap();

    assert_eq!(loaded.dialect, Dialect::Sqlite);
    assert_eq!(loaded.tables.len(), 1);
    assert_eq!(loaded.tables[0].name, "users");
    assert_eq!(loaded.tables[0].constraints.len(), 1);
}

#[test]
fn snapshot_round_trips_postgres_udt_schema() {
    let schema = schema_pg(vec![table("events")
        .column(
            col("status")
                .udt("status")
                .udt_schema("shared types")
                .data_type("USER-DEFINED")
                .build(),
        )
        .build()]);
    let raw = serde_yaml::to_string(&SnapshotFile::from_schema(&schema)).unwrap();

    let loaded = load_str(&raw).unwrap();

    assert_eq!(
        loaded.tables[0].columns[0].udt_schema.as_deref(),
        Some("shared types")
    );
}

#[test]
fn legacy_snapshot_without_udt_schema_still_loads() {
    let schema = schema_pg(vec![table("events")
        .column(
            col("status")
                .udt("status")
                .data_type("USER-DEFINED")
                .build(),
        )
        .build()]);
    let raw = serde_yaml::to_string(&SnapshotFile::from_schema(&schema)).unwrap();
    assert!(!raw.contains("udt_schema:"));

    let loaded = load_str(&raw).unwrap();

    assert_eq!(loaded.tables[0].columns[0].udt_schema, None);
}

#[test]
fn missing_format_version_is_clear() {
    let err = load_str("uvg_version: 1.5.0\ndialect: sqlite\ntables: []\nenums: []\ndomains: []\n")
        .unwrap_err()
        .to_string();

    assert!(err.contains("missing format_version"), "got: {err}");
}

#[test]
fn unsupported_format_version_is_clear() {
    let err =
        load_str("format_version: 999\ndialect: sqlite\ntables: []\nenums: []\ndomains: []\n")
            .unwrap_err()
            .to_string();

    assert!(
        err.contains("unsupported snapshot format_version 999"),
        "got: {err}"
    );
}

#[test]
fn malformed_yaml_is_clear() {
    let err = load_str("format_version: [").unwrap_err().to_string();

    assert!(err.contains("snapshot is not valid YAML"), "got: {err}");
}
