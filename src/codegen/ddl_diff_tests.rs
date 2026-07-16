use super::*;
use crate::cli::DdlOptions;
use crate::schema::EnumInfo;
use crate::testutil::{
    col, schema_mssql, schema_mysql, schema_pg, schema_pg_with_enums, schema_sqlite, table,
};

fn default_options(target: Dialect) -> DdlOptions {
    DdlOptions {
        target_dialect: target,
        split_tables: false,
        apply: false,
        noindexes: false,
        noconstraints: false,
        nocomments: false,
    }
}

#[test]
fn test_diff_new_table() {
    let source = schema_pg(vec![table("users")
        .column(col("id").build())
        .pk("pk", &["id"])
        .build()]);
    let target = schema_pg(vec![]);
    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(ddl.contains("CREATE TABLE \"users\""));
}

#[test]
fn test_diff_creates_postgres_enum_before_table_that_uses_it() {
    let enum_info = EnumInfo {
        name: "mpaa_rating".to_string(),
        schema: Some("public".to_string()),
        values: vec!["G".to_string(), "PG-13".to_string()],
    };
    let source = schema_pg_with_enums(
        vec![table("film")
            .column(col("id").build())
            .column(
                col("rating")
                    .udt("mpaa_rating")
                    .udt_schema("public")
                    .data_type("USER-DEFINED")
                    .nullable()
                    .build(),
            )
            .pk("film_pkey", &["id"])
            .build()],
        vec![enum_info.clone()],
    );
    let target = schema_pg(vec![]);

    let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

    assert_eq!(changes[0].kind, crate::output::ChangeKind::CreateType);
    assert_eq!(
        changes[0].sql,
        "CREATE TYPE \"public\".\"mpaa_rating\" AS ENUM ('G', 'PG-13');"
    );
    assert_eq!(changes[1].kind, crate::output::ChangeKind::CreateTable);
    assert!(changes[1]
        .sql
        .contains("\"rating\" \"public\".\"mpaa_rating\""));

    let target_with_enum = schema_pg_with_enums(vec![], vec![enum_info]);
    let ddl = diff_schemas(
        &source,
        &target_with_enum,
        &default_options(Dialect::Postgres),
    );
    assert_eq!(ddl.matches("CREATE TYPE").count(), 0, "{ddl}");
}

#[test]
fn test_diff_enum_identity_is_schema_scoped_and_unused_enum_is_filtered() {
    let source = schema_pg_with_enums(
        vec![table("events")
            .schema("a")
            .column(col("id").build())
            .column(
                col("status")
                    .udt("status")
                    .udt_schema("a")
                    .data_type("USER-DEFINED")
                    .build(),
            )
            .pk("events_pkey", &["id"])
            .build()],
        vec![
            EnumInfo {
                name: "status".to_string(),
                schema: Some("a".to_string()),
                values: vec!["open".to_string()],
            },
            EnumInfo {
                name: "status".to_string(),
                schema: Some("b".to_string()),
                values: vec!["unrelated".to_string()],
            },
        ],
    );
    let target = schema_pg_with_enums(
        vec![],
        vec![EnumInfo {
            name: "status".to_string(),
            schema: Some("b".to_string()),
            values: vec!["unrelated".to_string()],
        }],
    );

    let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));
    let type_changes: Vec<&crate::output::Change> = changes
        .iter()
        .filter(|change| change.kind == crate::output::ChangeKind::CreateType)
        .collect();

    assert_eq!(type_changes.len(), 1, "changes: {changes:#?}");
    assert!(type_changes[0]
        .sql
        .starts_with("CREATE TYPE \"a\".\"status\" AS ENUM"));
    assert!(!changes
        .iter()
        .any(|change| change.sql.contains("\"b\".\"status\"")));
}

#[test]
fn test_diff_does_not_create_unreferenced_enum() {
    let users = table("users")
        .column(col("id").build())
        .pk("users_pkey", &["id"])
        .build();
    let source = schema_pg_with_enums(
        vec![users.clone()],
        vec![EnumInfo {
            name: "orphan_state".to_string(),
            schema: Some("public".to_string()),
            values: vec!["unused".to_string()],
        }],
    );
    let target = schema_pg(vec![users]);

    let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

    assert!(changes.is_empty(), "changes: {changes:#?}");
}

#[test]
fn test_diff_treats_missing_enum_schema_as_postgres_default() {
    let table = table("events")
        .column(col("id").build())
        .column(
            col("status")
                .udt("status")
                .data_type("USER-DEFINED")
                .build(),
        )
        .pk("events_pkey", &["id"])
        .build();
    let source = schema_pg_with_enums(
        vec![table.clone()],
        vec![EnumInfo {
            name: "status".to_string(),
            schema: None,
            values: vec!["open".to_string(), "closed".to_string()],
        }],
    );
    let target = schema_pg_with_enums(
        vec![table],
        vec![EnumInfo {
            name: "status".to_string(),
            schema: Some("public".to_string()),
            values: vec!["open".to_string(), "closed".to_string()],
        }],
    );

    let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

    assert!(changes.is_empty(), "changes: {changes:#?}");
}

#[test]
fn test_diff_blocks_existing_postgres_enum_column_identity_drift() {
    let desired_enum = EnumInfo {
        name: "order status".to_string(),
        schema: Some("tenant \"types".to_string()),
        values: vec!["open".to_string(), "closed".to_string()],
    };
    let current_enum = EnumInfo {
        name: "order status".to_string(),
        schema: Some("legacy types".to_string()),
        values: desired_enum.values.clone(),
    };
    let source = schema_pg_with_enums(
        vec![
            table("events")
                .column(col("id").build())
                .column(
                    col("status")
                        .udt("order status")
                        .udt_schema("tenant \"types")
                        .data_type("USER-DEFINED")
                        .build(),
                )
                .pk("events_pkey", &["id"])
                .build(),
            table("audit_log")
                .column(col("id").build())
                .pk("audit_log_pkey", &["id"])
                .build(),
        ],
        vec![desired_enum.clone()],
    );
    // Both types already exist on the target, but the column is bound to the
    // same-named enum in the wrong schema. A generic cast is not safe for all
    // data/default/shape combinations, so the entire plan must be withheld.
    let target = schema_pg_with_enums(
        vec![table("events")
            .column(col("id").build())
            .column(
                col("status")
                    .udt("order status")
                    .udt_schema("legacy types")
                    .data_type("USER-DEFINED")
                    .build(),
            )
            .pk("events_pkey", &["id"])
            .build()],
        vec![desired_enum, current_enum],
    );

    let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

    assert_eq!(changes.len(), 1, "changes: {changes:#?}");
    assert_eq!(changes[0].kind, crate::output::ChangeKind::Other);
    assert_eq!(
        changes[0].sql,
        "-- UVG-BLOCKED: PostgreSQL enum column type drift for \"public\".\"events\".\"status\"\n\
         -- Desired source type: \"tenant \"\"types\".\"order status\"\n\
         -- Current target type: \"legacy types\".\"order status\"\n\
         -- uvg will not alter enum column identity automatically: distinct enums require explicit data casts, defaults are converted separately, and array or scalar-shape changes may need custom transforms.\n\
         -- Migrate this column manually to the exact desired enum type, then rerun uvg. Other schema changes are withheld."
    );
    assert!(changes[0].sql.lines().all(|line| line.starts_with("--")));
    assert!(!changes[0].sql.contains("CREATE TABLE \"audit_log\""));
}

#[test]
fn test_diff_blocks_existing_postgres_enum_array_identity_drift() {
    let desired_enum = EnumInfo {
        name: "order status".to_string(),
        schema: Some("tenant types".to_string()),
        values: vec!["open".to_string(), "closed".to_string()],
    };
    let current_enum = EnumInfo {
        name: "order status".to_string(),
        schema: Some("legacy types".to_string()),
        values: desired_enum.values.clone(),
    };
    let source = schema_pg_with_enums(
        vec![table("events")
            .column(col("id").build())
            .column(
                col("statuses")
                    .udt("_order status")
                    .udt_schema("tenant types")
                    .data_type("ARRAY")
                    .build(),
            )
            .pk("events_pkey", &["id"])
            .build()],
        vec![desired_enum.clone()],
    );
    let target = schema_pg_with_enums(
        vec![table("events")
            .column(col("id").build())
            .column(
                col("statuses")
                    .udt("_order status")
                    .udt_schema("legacy types")
                    .data_type("ARRAY")
                    .build(),
            )
            .pk("events_pkey", &["id"])
            .build()],
        vec![desired_enum, current_enum],
    );

    let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

    assert_eq!(changes.len(), 1, "changes: {changes:#?}");
    assert_eq!(changes[0].kind, crate::output::ChangeKind::Other);
    assert_eq!(
        changes[0].sql,
        "-- UVG-BLOCKED: PostgreSQL enum column type drift for \"public\".\"events\".\"statuses\"\n\
         -- Desired source type: \"tenant types\".\"order status\"[]\n\
         -- Current target type: \"legacy types\".\"order status\"[]\n\
         -- uvg will not alter enum column identity automatically: distinct enums require explicit data casts, defaults are converted separately, and array or scalar-shape changes may need custom transforms.\n\
         -- Migrate this column manually to the exact desired enum type, then rerun uvg. Other schema changes are withheld."
    );
}

#[test]
fn test_diff_postgres_builtin_udt_schema_is_not_enum_identity() {
    let source = schema_pg(vec![table("events")
        .column(col("id").udt("int4").udt_schema("pg_catalog").build())
        .pk("events_pkey", &["id"])
        .build()]);
    let target = source.clone();

    let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

    assert!(
        changes.is_empty(),
        "built-in type unexpectedly drifted: {changes:#?}"
    );
}

#[test]
fn test_diff_blocks_all_sql_when_postgres_enum_labels_drift() {
    let enum_table = table("events")
        .column(col("id").build())
        .column(
            col("status")
                .udt("status")
                .udt_schema("public")
                .data_type("USER-DEFINED")
                .build(),
        )
        .pk("events_pkey", &["id"])
        .build();
    let unrelated_new_table = table("audit_log")
        .column(col("id").build())
        .pk("audit_log_pkey", &["id"])
        .build();
    let desired_values = vec!["draft".to_string(), "live\nDROP TABLE users;".to_string()];
    let drift_cases = [
        vec!["draft".to_string()],
        vec!["draft".to_string(), "obsolete".to_string()],
        vec!["live\nDROP TABLE users;".to_string(), "draft".to_string()],
    ];

    for target_values in drift_cases {
        let source = schema_pg_with_enums(
            vec![enum_table.clone(), unrelated_new_table.clone()],
            vec![EnumInfo {
                name: "status".to_string(),
                schema: Some("public".to_string()),
                values: desired_values.clone(),
            }],
        );
        let target = schema_pg_with_enums(
            vec![enum_table.clone()],
            vec![EnumInfo {
                name: "status".to_string(),
                schema: Some("public".to_string()),
                values: target_values,
            }],
        );

        let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

        assert_eq!(changes.len(), 1, "changes: {changes:#?}");
        assert_eq!(changes[0].kind, crate::output::ChangeKind::Other);
        assert!(changes[0].sql.starts_with("-- UVG-BLOCKED:"));
        assert!(changes[0].sql.contains("\\nDROP TABLE users;"));
        assert!(
            changes[0].sql.lines().all(|line| line.starts_with("--")),
            "blocker leaked executable SQL: {}",
            changes[0].sql
        );
        assert!(!changes[0].sql.contains("CREATE TABLE"));

        let rendered = render_changes(&changes, Dialect::Postgres, Dialect::Postgres);
        assert!(rendered.contains("UVG-BLOCKED"));
        assert!(!rendered.contains("No schema changes detected"));
        assert!(!rendered.contains("CREATE TABLE \"audit_log\""));
    }
}

#[test]
fn test_diff_dropped_table() {
    let source = schema_pg(vec![]);
    let target = schema_pg(vec![table("old")
        .column(col("id").build())
        .pk("pk", &["id"])
        .build()]);
    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(ddl.contains("DROP TABLE IF EXISTS"));
    assert!(ddl.contains("WARNING: destructive"));
}

#[test]
fn test_diff_new_column() {
    let source = schema_pg(vec![table("users")
        .column(col("id").build())
        .column(col("email").udt("varchar").max_length(255).build())
        .pk("pk", &["id"])
        .build()]);
    let target = schema_pg(vec![table("users")
        .column(col("id").build())
        .pk("pk", &["id"])
        .build()]);
    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(ddl.contains("ADD COLUMN \"email\" VARCHAR(255) NOT NULL"));
}

#[test]
fn test_diff_no_changes() {
    let schema = schema_pg(vec![table("users")
        .column(col("id").build())
        .pk("pk", &["id"])
        .build()]);
    let ddl = diff_schemas(&schema, &schema, &default_options(Dialect::Postgres));
    assert!(ddl.contains("No schema changes detected"));
}

#[test]
fn test_diff_cross_dialect_default_schemas_match() {
    let source = schema_pg(vec![table("users")
        .column(col("id").build())
        .pk("pk", &["id"])
        .build()]);
    let target = schema_pg(vec![table("users")
        .schema("dbo")
        .column(col("id").build())
        .pk("pk", &["id"])
        .build()]);
    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        ddl.contains("No schema changes detected"),
        "public should match dbo: {ddl}"
    );
}

#[test]
fn test_diff_mssql_identity_to_pg_serial_converges() {
    // MSSQL source: IDENTITY column with no SQL default.
    let source = schema_mssql(vec![table("Badges")
        .schema("dbo")
        .column(col("Id").udt("int").identity().build())
        .pk("PK_Badges", &["Id"])
        .build()]);
    // PG target: same logical column expressed as SERIAL (nextval(...) default).
    let target = schema_pg(vec![table("Badges")
        .column(
            col("Id")
                .udt("int4")
                .default_val("nextval('\"Badges_Id_seq\"'::regclass)")
                .build(),
        )
        .pk("Badges_pkey", &["Id"])
        .build()]);
    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        ddl.contains("No schema changes detected"),
        "MSSQL IDENTITY ↔ PG SERIAL should round-trip with zero diff, got: {ddl}"
    );
}

#[test]
fn test_diff_pg_serial_with_divergent_sequences_still_drifts() {
    // Same-dialect (PG→PG): two SERIAL-shaped columns pointing at different
    // sequences should NOT be silently treated as equivalent — that would
    // hide real drift from custom or renamed sequences.
    let source = schema_pg(vec![table("users")
        .column(
            col("id")
                .udt("int4")
                .default_val("nextval('seq_a'::regclass)")
                .build(),
        )
        .pk("pk", &["id"])
        .build()]);
    let target = schema_pg(vec![table("users")
        .column(
            col("id")
                .udt("int4")
                .default_val("nextval('seq_b'::regclass)")
                .build(),
        )
        .pk("pk", &["id"])
        .build()]);
    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        ddl.contains("SET DEFAULT") || ddl.contains("DROP DEFAULT"),
        "Same-dialect divergent sequences should drift, got: {ddl}"
    );
}

#[test]
fn test_diff_empty_postgres_target_creates_and_preserves_shared_sequence() {
    let shared_default = "nextval('payment_payment_id_seq'::regclass)";
    let source = schema_pg(vec![
        table("payment")
            .column(col("payment_id").default_val(shared_default).build())
            .pk("payment_pkey", &["payment_id"])
            .build(),
        table("payment_p2022_01")
            .column(col("payment_id").default_val(shared_default).build())
            .pk("payment_p2022_01_pkey", &["payment_id"])
            .build(),
    ]);
    let target = schema_pg(Vec::new());

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));

    let sequence_pos = ddl
        .find("CREATE SEQUENCE payment_payment_id_seq;")
        .expect("missing sequence declaration");
    let table_pos = ddl
        .find("CREATE TABLE \"payment_p2022_01\"")
        .expect("missing table declaration");
    assert!(sequence_pos < table_pos);
    assert!(ddl.contains(
        "\"payment_id\" INTEGER NOT NULL DEFAULT nextval('payment_payment_id_seq'::regclass)"
    ));

    let converged = diff_schemas(&source, &source, &default_options(Dialect::Postgres));
    assert!(converged.contains("No schema changes detected"));
}

#[test]
fn test_diff_replaces_same_named_postgres_index_when_method_changes() {
    let source = schema_pg(vec![table("film")
        .column(col("film_id").build())
        .column(col("fulltext").udt("tsvector").build())
        .pk("film_pkey", &["film_id"])
        .index_with_kwargs(
            "film_fulltext_idx",
            &["fulltext"],
            false,
            &[("postgresql_using", "gist")],
        )
        .build()]);
    let target = schema_pg(vec![table("film")
        .column(col("film_id").build())
        .column(col("fulltext").udt("tsvector").build())
        .pk("film_pkey", &["film_id"])
        .index("film_fulltext_idx", &["fulltext"], false)
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));

    assert!(ddl.contains("DROP INDEX IF EXISTS \"film_fulltext_idx\";"));
    assert!(
        ddl.contains("CREATE INDEX \"film_fulltext_idx\" ON \"film\" USING gist (\"fulltext\");")
    );
    assert!(
        ddl.find("DROP INDEX").unwrap() < ddl.find("CREATE INDEX").unwrap(),
        "method replacement must drop before reusing the name: {ddl}"
    );

    let converged = diff_schemas(&source, &source, &default_options(Dialect::Postgres));
    assert!(converged.contains("No schema changes detected"));
}

#[test]
fn test_diff_existing_table_constraints_indexes_and_mssql_literals() {
    let source = schema_mssql(vec![table("Users")
        .schema("dbo")
        .column(col("Id").udt("int").identity().build())
        .column(col("status").udt("nvarchar").max_length(40).build())
        .column(
            col("UpdatedAt")
                .udt("datetime2")
                .default_val("SYSUTCDATETIME()")
                .build(),
        )
        .pk("PK_Users", &["Id"])
        .unique("UQ_Users_status", &["status"])
        .check(
            "CK_Users_status",
            "([status]=N'open' OR [status]=N'closed')",
        )
        .index("IX_Users_status", &["status"], false)
        .build()]);
    let target = schema_pg(vec![table("Users")
        .column(
            col("Id")
                .udt("int4")
                .default_val("nextval('\"Users_Id_seq\"'::regclass)")
                .build(),
        )
        .column(col("status").udt("varchar").max_length(20).build())
        .pk("PK_Users", &["Id"])
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));

    assert!(
        ddl.contains("ALTER TABLE \"Users\" ALTER COLUMN \"status\" TYPE VARCHAR(40);"),
        "changed existing column length should be emitted: {ddl}"
    );
    assert!(
        ddl.contains(
            "ALTER TABLE \"Users\" ADD COLUMN \"UpdatedAt\" TIMESTAMP NOT NULL DEFAULT now();"
        ),
        "MSSQL SYSUTCDATETIME default should translate for new columns: {ddl}"
    );
    assert!(
        ddl.contains(
            "ALTER TABLE \"Users\" ADD CONSTRAINT \"UQ_Users_status\" UNIQUE (\"status\");"
        ),
        "new unique constraint on existing table should be emitted: {ddl}"
    );
    assert!(
        ddl.contains(
            "ALTER TABLE \"Users\" ADD CONSTRAINT \"CK_Users_status\" CHECK ((\"status\"='open' OR \"status\"='closed'));"
        ),
        "MSSQL brackets and N-prefixed string literals should translate in CHECKs: {ddl}"
    );
    assert!(
        ddl.contains("CREATE INDEX \"IX_Users_status\" ON \"Users\" (\"status\");"),
        "new index on existing table should be emitted: {ddl}"
    );
}

#[test]
fn test_diff_dropped_existing_table_constraints_indexes() {
    let source = schema_mssql(vec![table("Users")
        .schema("dbo")
        .column(col("Id").udt("int").identity().build())
        .column(col("status").udt("nvarchar").max_length(40).build())
        .pk("PK_Users", &["Id"])
        .build()]);
    let target = schema_pg(vec![table("Users")
        .column(
            col("Id")
                .udt("int4")
                .default_val("nextval('\"Users_Id_seq\"'::regclass)")
                .build(),
        )
        .column(col("status").udt("varchar").max_length(40).build())
        .pk("PK_Users", &["Id"])
        .unique("UQ_Users_status", &["status"])
        .check("CK_Users_status", "(\"status\" = 'open'::text)")
        .index("IX_Users_status", &["status"], false)
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));

    assert!(
        ddl.contains("ALTER TABLE \"Users\" DROP CONSTRAINT IF EXISTS \"UQ_Users_status\";"),
        "dropped unique constraint on existing table should be emitted: {ddl}"
    );
    assert!(
        ddl.contains("ALTER TABLE \"Users\" DROP CONSTRAINT IF EXISTS \"CK_Users_status\";"),
        "dropped check constraint on existing table should be emitted: {ddl}"
    );
    assert!(
        ddl.contains("DROP INDEX IF EXISTS \"IX_Users_status\";"),
        "dropped index on existing table should be emitted: {ddl}"
    );
}

#[test]
fn test_diff_target_pk_index_and_name_difference_do_not_drift() {
    let source = schema_mssql(vec![table("Badges")
        .schema("dbo")
        .column(col("Id").udt("int").identity().build())
        .pk("PK_Badges", &["Id"])
        .build()]);
    let target = schema_pg(vec![table("Badges")
        .column(
            col("Id")
                .udt("int4")
                .default_val("nextval('\"Badges_Id_seq\"'::regclass)")
                .build(),
        )
        .pk("Badges_pkey", &["Id"])
        .index("Badges_pkey", &["Id"], true)
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        ddl.contains("No schema changes detected"),
        "target-side PK names and backing indexes should not drift when columns match: {ddl}"
    );
}

#[test]
fn test_diff_mssql_identity_to_sqlite_autoincrement_converges() {
    let source = schema_mssql(vec![table("Badges")
        .schema("dbo")
        .column(col("Id").udt("int").identity().build())
        .pk("PK_Badges", &["Id"])
        .build()]);
    let target = schema_sqlite(vec![table("Badges")
        .schema("main")
        .column(col("Id").udt("integer").nullable().autoincrement().build())
        .pk("PK_Badges", &["Id"])
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Sqlite));
    assert!(
        ddl.contains("No schema changes detected"),
        "MSSQL IDENTITY -> SQLite INTEGER PRIMARY KEY AUTOINCREMENT should round-trip without nullable drift, got: {ddl}"
    );
}

#[test]
fn test_diff_target_fk_backing_index_does_not_drift() {
    let source = schema_mssql(vec![
        table("Users")
            .schema("dbo")
            .column(col("Id").udt("int").identity().build())
            .pk("PK_Users", &["Id"])
            .build(),
        table("Events")
            .schema("dbo")
            .column(col("Id").udt("int").identity().build())
            .column(col("UserId").udt("int").build())
            .pk("PK_Events", &["Id"])
            .fk_full(
                "FK_Events_Users",
                &["UserId"],
                "dbo",
                "Users",
                &["Id"],
                "NO ACTION",
                "NO ACTION",
            )
            .build(),
    ]);
    let target = schema_mysql(vec![
        table("Users")
            .schema("uvg")
            .column(col("Id").udt("int").autoincrement().build())
            .pk("PK_Users", &["Id"])
            .build(),
        table("Events")
            .schema("uvg")
            .column(col("Id").udt("int").autoincrement().build())
            .column(col("UserId").udt("int").build())
            .pk("PK_Events", &["Id"])
            .fk_full(
                "FK_Events_Users",
                &["UserId"],
                "uvg",
                "Users",
                &["Id"],
                "NO ACTION",
                "NO ACTION",
            )
            .index("FK_Events_Users", &["UserId"], false)
            .build(),
    ]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Mysql));
    assert!(
        ddl.contains("No schema changes detected"),
        "target-side FK backing indexes should not drift: {ddl}"
    );
}

#[test]
fn test_diff_target_user_fk_index_drops_on_non_mysql_target() {
    // The FK backing-index exemption is MySQL-only: PG never auto-creates
    // an index for a FK, so a target-only index on FK columns is
    // user-created and must be dropped or the diff falsely converges.
    let source = schema_mssql(vec![
        table("Users")
            .schema("dbo")
            .column(col("Id").udt("int").identity().build())
            .pk("PK_Users", &["Id"])
            .build(),
        table("Events")
            .schema("dbo")
            .column(col("Id").udt("int").identity().build())
            .column(col("UserId").udt("int").build())
            .pk("PK_Events", &["Id"])
            .fk_full(
                "FK_Events_Users",
                &["UserId"],
                "dbo",
                "Users",
                &["Id"],
                "NO ACTION",
                "NO ACTION",
            )
            .build(),
    ]);
    let target = schema_pg(vec![
        table("Users")
            .column(
                col("Id")
                    .udt("int4")
                    .default_val("nextval('\"Users_Id_seq\"'::regclass)")
                    .build(),
            )
            .pk("PK_Users", &["Id"])
            .build(),
        table("Events")
            .column(
                col("Id")
                    .udt("int4")
                    .default_val("nextval('\"Events_Id_seq\"'::regclass)")
                    .build(),
            )
            .column(col("UserId").udt("int4").build())
            .pk("PK_Events", &["Id"])
            .fk_full(
                "FK_Events_Users",
                &["UserId"],
                "",
                "Users",
                &["Id"],
                "NO ACTION",
                "NO ACTION",
            )
            .index("IX_Events_UserId", &["UserId"], false)
            .build(),
    ]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        ddl.contains("DROP INDEX IF EXISTS \"IX_Events_UserId\";"),
        "user-created index on FK columns must drop on PG targets: {ddl}"
    );
}

#[test]
fn test_diff_added_check_mysql_target_uses_backticks() {
    // MSSQL→MySQL: bracket identifiers in an added CHECK must become
    // backticks. MySQL's default sql_mode reads "..." as a string
    // literal, so double-quoted identifiers would make the predicate a
    // constant expression — the constraint applies but validates nothing.
    let source = schema_mssql(vec![table("Users")
        .schema("dbo")
        .column(col("Id").udt("int").identity().build())
        .column(col("ProfileScore").udt("int").build())
        .pk("PK_Users", &["Id"])
        .check("CK_Users_ProfileScore", "([ProfileScore]>=(0))")
        .build()]);
    let target = schema_mysql(vec![table("Users")
        .schema("uvg")
        .column(col("Id").udt("int").autoincrement().build())
        .column(col("ProfileScore").udt("int").build())
        .pk("PK_Users", &["Id"])
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Mysql));
    assert!(
        ddl.contains("ADD CONSTRAINT `CK_Users_ProfileScore` CHECK ((`ProfileScore`>=(0)));"),
        "MSSQL bracket identifiers must translate to backticks for MySQL CHECKs: {ddl}"
    );
}

#[test]
fn test_diff_drops_dependent_objects_before_column_drop() {
    // A dropped column's target-side index and check constraint must be
    // dropped first: MSSQL rejects DROP COLUMN while a dependent index or
    // constraint exists, and MySQL can auto-drop the index with the
    // column and then fail on the later explicit DROP INDEX.
    let source = schema_mssql(vec![table("Users")
        .schema("dbo")
        .column(col("Id").udt("int").identity().build())
        .pk("PK_Users", &["Id"])
        .build()]);
    let target = schema_mssql(vec![table("Users")
        .schema("dbo")
        .column(col("Id").udt("int").identity().build())
        .column(col("LastSeenAt").udt("datetime2").nullable().build())
        .pk("PK_Users", &["Id"])
        .check("CK_Users_LastSeenAt", "([LastSeenAt] IS NOT NULL)")
        .index("IX_Users_LastSeenAt", &["LastSeenAt"], false)
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Mssql));

    let col_drop = ddl
        .find("DROP COLUMN [LastSeenAt];")
        .expect("column drop should be emitted");
    let constraint_drop = ddl
        .find("DROP CONSTRAINT [CK_Users_LastSeenAt];")
        .expect("dependent check drop should be emitted");
    let index_drop = ddl
        .find("DROP INDEX [IX_Users_LastSeenAt] ON")
        .expect("dependent index drop should be emitted");
    assert!(
        constraint_drop < col_drop,
        "dependent constraint must drop before the column: {ddl}"
    );
    assert!(
        index_drop < col_drop,
        "dependent index must drop before the column: {ddl}"
    );
}

#[test]
fn test_diff_multi_schema_preserved() {
    let source = schema_pg(vec![table("users")
        .schema("schema_a")
        .column(col("id").build())
        .pk("pk", &["id"])
        .build()]);
    let target = schema_pg(vec![table("users")
        .schema("schema_b")
        .column(col("id").build())
        .pk("pk", &["id"])
        .build()]);
    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        ddl.contains("CREATE TABLE") && ddl.contains("DROP TABLE"),
        "Non-default schemas should not match: {ddl}"
    );
}

// -------- compute_changes / render_changes tagging tests --------

#[test]
fn test_compute_changes_new_table_tagged() {
    let source = schema_pg(vec![table("users")
        .column(col("id").build())
        .pk("pk", &["id"])
        .build()]);
    let target = schema_pg(vec![]);
    let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

    assert_eq!(changes.len(), 1, "expected one CREATE TABLE change");
    let c = &changes[0];
    assert_eq!(c.table_schema, "", "PG public should normalize to empty");
    assert_eq!(c.table_name.as_deref(), Some("users"));
    assert!(c.sql.contains("CREATE TABLE \"users\""));
}

#[test]
fn test_compute_changes_alter_column_tagged() {
    let source = schema_pg(vec![table("users")
        .column(col("id").build())
        .column(col("email").udt("varchar").max_length(255).build())
        .pk("pk", &["id"])
        .build()]);
    let target = schema_pg(vec![table("users")
        .column(col("id").build())
        .pk("pk", &["id"])
        .build()]);
    let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

    assert_eq!(changes.len(), 1, "expected one ALTER COLUMN change");
    let c = &changes[0];
    assert_eq!(c.table_schema, "");
    assert_eq!(c.table_name.as_deref(), Some("users"));
    assert!(c.sql.contains("ADD COLUMN \"email\""));
}

#[test]
fn test_compute_changes_dropped_table_tagged() {
    let source = schema_pg(vec![]);
    let target = schema_pg(vec![table("old_events")
        .column(col("id").build())
        .pk("pk", &["id"])
        .build()]);
    let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

    assert_eq!(changes.len(), 1, "expected one DROP TABLE change");
    let c = &changes[0];
    assert_eq!(c.table_schema, "", "default PG schema normalizes to empty");
    assert_eq!(c.table_name.as_deref(), Some("old_events"));
    assert!(c.sql.contains("DROP TABLE IF EXISTS"));
}

#[test]
fn test_compute_changes_pg_type_null_default_split() {
    // PG ALTER for type + nullability + default emits three separate
    // statements. Each must be its own Change so the per-table splitter
    // can place them in the same file without re-parsing.
    let source = schema_pg(vec![table("users")
        .column(
            col("name")
                .udt("text")
                .nullable()
                .default_val("'anon'::text")
                .build(),
        )
        .pk("pk", &["name"])
        .build()]);
    let target = schema_pg(vec![table("users")
        .column(col("name").udt("varchar").max_length(50).build()) // not-null, no default
        .pk("pk", &["name"])
        .build()]);
    let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

    assert_eq!(
        changes.len(),
        3,
        "type/null/default should split into 3 changes, got: {changes:#?}"
    );
    for c in &changes {
        assert_eq!(c.table_name.as_deref(), Some("users"));
        assert_eq!(c.table_schema, "");
        assert!(
            c.sql.starts_with("ALTER TABLE"),
            "each change is a standalone ALTER: {}",
            c.sql
        );
    }
}

#[test]
fn test_compute_changes_non_default_schema_preserved() {
    let source = schema_pg(vec![table("orders")
        .schema("billing")
        .column(col("id").build())
        .pk("pk", &["id"])
        .build()]);
    let target = schema_pg(vec![]);
    let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

    assert_eq!(changes.len(), 1);
    assert_eq!(
        changes[0].table_schema, "billing",
        "non-default schema should be preserved verbatim in the tag"
    );
    assert_eq!(changes[0].table_name.as_deref(), Some("orders"));
}

#[test]
fn test_compute_changes_new_table_with_index_tagged_together() {
    // A new table plus its indexes all tag to the same table — the splitter
    // writes them into one file, not separate ones.
    let source = schema_pg(vec![table("users")
        .column(col("id").build())
        .column(col("email").udt("varchar").max_length(255).build())
        .pk("pk", &["id"])
        .index("ix_users_email", &["email"], false)
        .build()]);
    let target = schema_pg(vec![]);
    let changes = compute_changes(&source, &target, &default_options(Dialect::Postgres));

    assert!(
        changes.len() >= 2,
        "expected CREATE TABLE + CREATE INDEX, got {}",
        changes.len()
    );
    for c in &changes {
        assert_eq!(
            c.table_name.as_deref(),
            Some("users"),
            "every change for a new table tags to that table: {}",
            c.sql
        );
    }
    assert!(changes.iter().any(|c| c.sql.contains("CREATE TABLE")));
    assert!(changes.iter().any(|c| c.sql.contains("CREATE INDEX")));
}

#[test]
fn test_render_changes_empty_returns_sentinel() {
    let out = render_changes(&[], Dialect::Postgres, Dialect::Postgres);
    assert_eq!(out, "-- No schema changes detected.\n");
}

#[test]
fn test_render_changes_round_trip_with_diff_schemas() {
    // diff_schemas() must produce byte-identical output to
    // render_changes(&compute_changes(...), ...). This protects the
    // CRM matrix and all existing string-grep tests.
    let source = schema_pg(vec![
        table("users")
            .column(col("id").build())
            .column(col("email").udt("varchar").max_length(255).build())
            .pk("pk_users", &["id"])
            .build(),
        table("posts")
            .column(col("id").build())
            .pk("pk_posts", &["id"])
            .build(),
    ]);
    let target = schema_pg(vec![table("posts")
        .column(col("id").build())
        .pk("pk_posts", &["id"])
        .build()]);
    let options = default_options(Dialect::Postgres);

    let direct = diff_schemas(&source, &target, &options);
    let via_changes = render_changes(
        &compute_changes(&source, &target, &options),
        Dialect::Postgres,
        Dialect::Postgres,
    );
    assert_eq!(direct, via_changes);
}

// ---- same-name constraint content comparison (#113) ----

#[test]
fn test_same_name_check_predicate_change_is_drift_same_dialect() {
    // Editing a CHECK predicate in place (same constraint name) was
    // invisible to the name-only diff. Same-dialect, it must now emit a
    // drop of the target's version and a re-add of the source's.
    let source = schema_pg(vec![table("orders")
        .column(col("price").udt("int4").build())
        .check("ck_price", "((price > 10))")
        .build()]);
    let target = schema_pg(vec![table("orders")
        .column(col("price").udt("int4").build())
        .check("ck_price", "((price > 0))")
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        ddl.contains("DROP CONSTRAINT IF EXISTS \"ck_price\""),
        "changed predicate under the same name must drop the old version: {ddl}"
    );
    assert!(
        ddl.contains("ADD CONSTRAINT \"ck_price\" CHECK"),
        "changed predicate must re-add the source version: {ddl}"
    );
}

#[test]
fn test_same_name_check_equivalent_modulo_formatting_is_not_drift() {
    // PG stores extra wrapping parens; whitespace and identifier quoting
    // vary. None of that is drift.
    let source = schema_pg(vec![table("orders")
        .column(col("price").udt("int4").build())
        .check("ck_price", "((\"price\" > 0))")
        .build()]);
    let target = schema_pg(vec![table("orders")
        .column(col("price").udt("int4").build())
        .check("ck_price", "( price  >  0 )")
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        !ddl.contains("ck_price"),
        "formatting-only differences must not churn the constraint: {ddl}"
    );
}

#[test]
fn test_same_name_check_cross_dialect_text_is_not_compared() {
    // Cross-dialect stored predicates never converge textually (each server
    // canonicalizes its own form), so comparing them would drop+add on
    // every run. They are deliberately skipped.
    let source = schema_mysql(vec![table("orders")
        .column(col("price").udt("int").build())
        .check("ck_price", "(`price` > 0)")
        .build()]);
    let target = schema_pg(vec![table("orders")
        .column(col("price").udt("int4").build())
        .check("ck_price", "((price >= 1))")
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        !ddl.contains("ck_price"),
        "cross-dialect predicate text must not be treated as drift: {ddl}"
    );
}

#[test]
fn test_same_name_unique_different_columns_is_drift() {
    let source = schema_pg(vec![table("users")
        .column(col("a").udt("int4").build())
        .column(col("b").udt("int4").build())
        .unique("uq_users", &["a"])
        .build()]);
    let target = schema_pg(vec![table("users")
        .column(col("a").udt("int4").build())
        .column(col("b").udt("int4").build())
        .unique("uq_users", &["b"])
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        ddl.contains("DROP CONSTRAINT IF EXISTS \"uq_users\""),
        "re-pointed UNIQUE must drop the target version: {ddl}"
    );
    assert!(
        ddl.contains("ADD CONSTRAINT \"uq_users\" UNIQUE (\"a\")"),
        "re-pointed UNIQUE must add the source version: {ddl}"
    );
}

#[test]
fn test_same_name_fk_retargeted_is_drift() {
    let source = schema_pg(vec![table("orders")
        .column(col("user_id").udt("int4").build())
        .fk("fk_orders_user", &["user_id"], "accounts", &["id"])
        .build()]);
    let target = schema_pg(vec![table("orders")
        .column(col("user_id").udt("int4").build())
        .fk("fk_orders_user", &["user_id"], "users", &["id"])
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        ddl.contains("DROP CONSTRAINT IF EXISTS \"fk_orders_user\""),
        "retargeted FK must drop the target version: {ddl}"
    );
    assert!(
        ddl.contains("REFERENCES \"accounts\""),
        "retargeted FK must re-add pointing at the source's table: {ddl}"
    );
}

#[test]
fn test_same_name_fk_delete_rule_change_same_dialect_is_drift() {
    let source = schema_pg(vec![table("orders")
        .column(col("user_id").udt("int4").build())
        .fk_full(
            "fk_orders_user",
            &["user_id"],
            "public",
            "users",
            &["id"],
            "NO ACTION",
            "CASCADE",
        )
        .build()]);
    let target = schema_pg(vec![table("orders")
        .column(col("user_id").udt("int4").build())
        .fk_full(
            "fk_orders_user",
            &["user_id"],
            "public",
            "users",
            &["id"],
            "NO ACTION",
            "NO ACTION",
        )
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        ddl.contains("ON DELETE CASCADE"),
        "delete-rule change must re-add with the source rule: {ddl}"
    );
}

#[test]
fn test_same_name_fk_default_rule_spellings_cross_dialect_not_drift() {
    // MySQL reports the default rule as RESTRICT where PG says NO ACTION;
    // cross-dialect rule comparison is skipped so this is not churn.
    let source = schema_mysql(vec![table("orders")
        .column(col("user_id").udt("int").build())
        .fk_full(
            "fk_orders_user",
            &["user_id"],
            "appdb",
            "users",
            &["id"],
            "RESTRICT",
            "RESTRICT",
        )
        .build()]);
    let target = schema_pg(vec![table("orders")
        .column(col("user_id").udt("int4").build())
        .fk_full(
            "fk_orders_user",
            &["user_id"],
            "public",
            "users",
            &["id"],
            "NO ACTION",
            "NO ACTION",
        )
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        !ddl.contains("fk_orders_user"),
        "default rule spellings across dialects must not be drift: {ddl}"
    );
}

#[test]
fn test_normalize_check_predicate_peels_only_wrapping_parens() {
    assert_eq!(normalize_check_predicate("((x > 0))"), "x>0");
    assert_eq!(normalize_check_predicate("(`x` >  0)"), "x>0");
    // (a) AND (b): the first paren closes mid-expression — not wrapping.
    assert_eq!(
        normalize_check_predicate("((a > 0) AND (b > 0))"),
        "(a>0)and(b>0)"
    );
    // MSSQL preserves authored spacing and case outside literals; neither
    // is drift.
    assert_eq!(
        normalize_check_predicate("([x]>=(0))"),
        normalize_check_predicate("([x] >= (0))")
    );
    assert_eq!(
        normalize_check_predicate("(x > 0 AND y > 0)"),
        normalize_check_predicate("(x > 0 and y > 0)")
    );
    // Literal content is verbatim: case and internal spacing are real drift.
    assert_ne!(
        normalize_check_predicate("(status = 'Active')"),
        normalize_check_predicate("(status = 'active')")
    );
    assert_ne!(
        normalize_check_predicate("(status = 'a b')"),
        normalize_check_predicate("(status = 'ab')")
    );
    // Parens inside literals don't confuse the wrapping-paren peel, and
    // escaped quotes stay inside the literal.
    assert_eq!(normalize_check_predicate("(s = ')')"), "s=')'");
    assert_eq!(normalize_check_predicate("(s = 'it''s')"), "s='it''s'");
}

#[test]
fn test_same_name_mysql_fk_column_change_drops_stale_backing_index() {
    // InnoDB auto-creates an FK backing index named after the constraint
    // and leaves it behind on DROP FOREIGN KEY. Replacing an FK with
    // different local columns must also drop that stale index (the index
    // diff suppresses FK-backing indexes, so nothing else would).
    let source = schema_mysql(vec![table("orders")
        .column(col("user_id").udt("int").build())
        .column(col("acct_id").udt("int").build())
        .fk("fk_orders_owner", &["acct_id"], "accounts", &["id"])
        .build()]);
    let target = schema_mysql(vec![table("orders")
        .column(col("user_id").udt("int").build())
        .column(col("acct_id").udt("int").build())
        .fk("fk_orders_owner", &["user_id"], "accounts", &["id"])
        .index("fk_orders_owner", &["user_id"], false)
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Mysql));
    let drop_fk = ddl
        .find("DROP FOREIGN KEY `fk_orders_owner`")
        .expect("FK must be dropped");
    let drop_idx = ddl
        .find("DROP INDEX `fk_orders_owner`")
        .expect("stale backing index must be dropped");
    assert!(
        drop_fk < drop_idx,
        "index drop must follow the FK drop (InnoDB refuses it earlier): {ddl}"
    );
    assert!(
        ddl.contains("ADD CONSTRAINT `fk_orders_owner` FOREIGN KEY (`acct_id`)"),
        "source FK must be re-added: {ddl}"
    );
}

#[test]
fn test_lossy_type_fallback_still_converges_to_no_drift() {
    // MySQL SET has no PG equivalent; it renders as a sized VARCHAR
    // fallback. Once the target carries that VARCHAR, the canonical types
    // still differ (Set vs Varchar) but the rendered target types match —
    // emitting an ALTER here would re-emit forever without converging.
    let mut flags = col("flags").udt("set").build();
    flags.data_type = "set('a','b','c')".to_string();
    let source = schema_mysql(vec![table("prefs").column(flags).build()]);
    let target = schema_pg(vec![table("prefs")
        .column(col("flags").udt("varchar").max_length(255).build())
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        !ddl.contains("ALTER COLUMN \"flags\" TYPE"),
        "lossy fallback that matches the target must not emit type drift: {ddl}"
    );
}

#[test]
fn test_same_name_mysql_fk_across_database_names_is_not_drift() {
    // MySQL's "schema" is the database name, so two databases under diff
    // always disagree on ref_schema; the emitted FK references the table
    // unqualified, so that difference could never converge. Identical FKs
    // apart from the database name must not churn.
    let source = schema_mysql(vec![table("orders")
        .schema("sourcedb")
        .column(col("user_id").udt("int").build())
        .fk_full(
            "fk_orders_user",
            &["user_id"],
            "sourcedb",
            "users",
            &["id"],
            "NO ACTION",
            "NO ACTION",
        )
        .build()]);
    let target = schema_mysql(vec![table("orders")
        .schema("targetdb")
        .column(col("user_id").udt("int").build())
        .fk_full(
            "fk_orders_user",
            &["user_id"],
            "targetdb",
            "users",
            &["id"],
            "NO ACTION",
            "NO ACTION",
        )
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Mysql));
    assert!(
        !ddl.contains("fk_orders_user"),
        "database-name-only FK difference must not be drift: {ddl}"
    );
}

#[test]
fn test_mysql_fk_restrict_no_action_spellings_not_drift() {
    // InnoDB treats RESTRICT and NO ACTION as the same behavior and reports
    // either spelling; our emitted ADD omits the clause for NO ACTION, so
    // spelling drift could never converge.
    let source = schema_mysql(vec![table("orders")
        .schema("appdb")
        .column(col("user_id").udt("int").build())
        .fk_full(
            "fk_orders_user",
            &["user_id"],
            "appdb",
            "users",
            &["id"],
            "NO ACTION",
            "NO ACTION",
        )
        .build()]);
    let target = schema_mysql(vec![table("orders")
        .schema("appdb")
        .column(col("user_id").udt("int").build())
        .fk_full(
            "fk_orders_user",
            &["user_id"],
            "appdb",
            "users",
            &["id"],
            "RESTRICT",
            "RESTRICT",
        )
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Mysql));
    assert!(
        !ddl.contains("fk_orders_user"),
        "RESTRICT vs NO ACTION on MySQL must not be drift: {ddl}"
    );
}

#[test]
fn test_mysql_stale_backing_index_kept_when_shared_by_another_fk() {
    // A second FK on the same local columns shares the backing index;
    // dropping it while that FK survives would make the apply fail.
    let source = schema_mysql(vec![table("orders")
        .column(col("user_id").udt("int").build())
        .column(col("acct_id").udt("int").build())
        .fk("fk_orders_owner", &["acct_id"], "accounts", &["id"])
        .fk("fk_orders_audit", &["user_id"], "audit_users", &["id"])
        .build()]);
    let target = schema_mysql(vec![table("orders")
        .column(col("user_id").udt("int").build())
        .column(col("acct_id").udt("int").build())
        .fk("fk_orders_owner", &["user_id"], "accounts", &["id"])
        .fk("fk_orders_audit", &["user_id"], "audit_users", &["id"])
        .index("fk_orders_owner", &["user_id"], false)
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Mysql));
    assert!(
        ddl.contains("DROP FOREIGN KEY `fk_orders_owner`"),
        "changed FK must still be replaced: {ddl}"
    );
    assert!(
        !ddl.contains("DROP INDEX `fk_orders_owner`"),
        "shared backing index must not be dropped while another FK uses it: {ddl}"
    );
}

#[test]
fn test_renamed_pk_with_name_reused_by_other_constraint_readds_pk() {
    // Target PK `old_pk(id)` collides by name with a source UNIQUE
    // `old_pk(email)` (content mismatch → old_pk is dropped), while the
    // source's PK lives under a new name `pk(id)`. The renamed-PK shortcut
    // must not treat the *dropped* old_pk as still covering the table — the
    // source PK must be re-added or the table ends up with no primary key.
    let source = schema_pg(vec![table("users")
        .column(col("id").udt("int4").build())
        .column(col("email").udt("varchar").max_length(100).build())
        .pk("pk", &["id"])
        .unique("old_pk", &["email"])
        .build()]);
    let target = schema_pg(vec![table("users")
        .column(col("id").udt("int4").build())
        .column(col("email").udt("varchar").max_length(100).build())
        .pk("old_pk", &["id"])
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        ddl.contains("DROP CONSTRAINT IF EXISTS \"old_pk\""),
        "name-collided target PK must be dropped: {ddl}"
    );
    assert!(
        ddl.contains("ADD CONSTRAINT \"pk\" PRIMARY KEY (\"id\")"),
        "the renamed source PK must be re-added since old_pk was dropped: {ddl}"
    );
}

#[test]
fn test_renamed_identical_pk_still_skipped() {
    // Plain rename with identical columns and no name collision: neither
    // side emits anything (the pre-#113 behavior is preserved).
    let source = schema_pg(vec![table("users")
        .column(col("id").udt("int4").build())
        .pk("users_pk_new", &["id"])
        .build()]);
    let target = schema_pg(vec![table("users")
        .column(col("id").udt("int4").build())
        .pk("users_pk_old", &["id"])
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        !ddl.contains("users_pk"),
        "renamed-but-identical PK must not churn: {ddl}"
    );
}

#[test]
fn test_noindexes_suppresses_fk_backing_index_drop() {
    let source = schema_mysql(vec![table("orders")
        .column(col("user_id").udt("int").build())
        .column(col("acct_id").udt("int").build())
        .fk("fk_orders_owner", &["acct_id"], "accounts", &["id"])
        .build()]);
    let target = schema_mysql(vec![table("orders")
        .column(col("user_id").udt("int").build())
        .column(col("acct_id").udt("int").build())
        .fk("fk_orders_owner", &["user_id"], "accounts", &["id"])
        .index("fk_orders_owner", &["user_id"], false)
        .build()]);

    let mut options = default_options(Dialect::Mysql);
    options.noindexes = true;
    let ddl = diff_schemas(&source, &target, &options);
    assert!(
        ddl.contains("DROP FOREIGN KEY `fk_orders_owner`"),
        "FK replacement itself is constraint work and stays: {ddl}"
    );
    assert!(
        !ddl.contains("DROP INDEX"),
        "noindexes must suppress the backing-index drop: {ddl}"
    );
}

#[test]
fn test_pk_add_suppressed_when_same_columns_pk_survives_name_collision() {
    // Mirror of the dropped-PK case: target has old_pk PRIMARY KEY(id) and
    // a UNIQUE named pk; source has just pk PRIMARY KEY(id). The UNIQUE pk
    // is dropped (type mismatch), but old_pk survives and covers (id) — so
    // adding the source's pk would fail with two primary keys.
    let source = schema_pg(vec![table("users")
        .column(col("id").udt("int4").build())
        .column(col("email").udt("varchar").max_length(100).build())
        .pk("pk", &["id"])
        .build()]);
    let target = schema_pg(vec![table("users")
        .column(col("id").udt("int4").build())
        .column(col("email").udt("varchar").max_length(100).build())
        .pk("old_pk", &["id"])
        .unique("pk", &["email"])
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Postgres));
    assert!(
        ddl.contains("DROP CONSTRAINT IF EXISTS \"pk\""),
        "the name-collided UNIQUE must be dropped: {ddl}"
    );
    assert!(
        !ddl.contains("ADD CONSTRAINT \"pk\" PRIMARY KEY"),
        "no PK may be added while the surviving old_pk covers (id): {ddl}"
    );
}

#[test]
fn test_check_normalizer_preserves_quoted_identifier_case() {
    // PG only quotes identifiers that need quoting, so "UserID" and userid
    // are different columns — stripping case here would hide real drift.
    assert_ne!(
        normalize_check_predicate("(\"UserID\" > 0)"),
        normalize_check_predicate("(userid > 0)")
    );
    // The same quoted identifier on both sides still matches.
    assert_eq!(
        normalize_check_predicate("((\"UserID\" > 0))"),
        normalize_check_predicate("( \"UserID\" >  0 )")
    );
}

#[test]
fn test_mysql_stale_backing_index_drop_tagged_as_drop_index() {
    // The injected DROP INDEX must carry ChangeKind::DropIndex, not ride
    // under DropConstraint, so manifests and down-migration classification
    // see it for what it is.
    let source = schema_mysql(vec![table("orders")
        .column(col("user_id").udt("int").build())
        .column(col("acct_id").udt("int").build())
        .fk("fk_orders_owner", &["acct_id"], "accounts", &["id"])
        .build()]);
    let target = schema_mysql(vec![table("orders")
        .column(col("user_id").udt("int").build())
        .column(col("acct_id").udt("int").build())
        .fk("fk_orders_owner", &["user_id"], "accounts", &["id"])
        .index("fk_orders_owner", &["user_id"], false)
        .build()]);

    let changes = compute_changes(&source, &target, &default_options(Dialect::Mysql));
    let index_drop = changes
        .iter()
        .find(|change| change.sql.contains("DROP INDEX `fk_orders_owner`"))
        .expect("stale backing index drop present");
    assert_eq!(index_drop.kind, crate::output::ChangeKind::DropIndex);
}

#[test]
fn test_mysql_shared_name_unique_and_fk_prefers_same_type_match() {
    // MySQL allows a UNIQUE key and an FK symbol to share a name. Pairing
    // the FK against the UNIQUE for content comparison would report drift
    // forever even though an identical FK exists under that name.
    let make = |db: &str| {
        table("orders")
            .schema(db)
            .column(col("user_id").udt("int").build())
            .unique("shared_name", &["user_id"])
            .fk_full(
                "shared_name",
                &["user_id"],
                db,
                "users",
                &["id"],
                "NO ACTION",
                "NO ACTION",
            )
            .build()
    };
    let source = schema_mysql(vec![make("appdb")]);
    let target = schema_mysql(vec![make("appdb")]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Mysql));
    assert!(
        !ddl.contains("shared_name"),
        "identical schemas must not churn on shared constraint names: {ddl}"
    );
}

#[test]
fn test_source_declared_index_with_fk_name_is_not_dropped() {
    // The source intentionally declares an index matching the old FK's
    // name/columns; the index diff won't re-add it (the target already has
    // the name), so the stale-index heuristic must leave it alone.
    let source = schema_mysql(vec![table("orders")
        .column(col("user_id").udt("int").build())
        .column(col("acct_id").udt("int").build())
        .fk("fk_orders_owner", &["acct_id"], "accounts", &["id"])
        .index("fk_orders_owner", &["user_id"], false)
        .build()]);
    let target = schema_mysql(vec![table("orders")
        .column(col("user_id").udt("int").build())
        .column(col("acct_id").udt("int").build())
        .fk("fk_orders_owner", &["user_id"], "accounts", &["id"])
        .index("fk_orders_owner", &["user_id"], false)
        .build()]);

    let ddl = diff_schemas(&source, &target, &default_options(Dialect::Mysql));
    assert!(
        ddl.contains("DROP FOREIGN KEY `fk_orders_owner`"),
        "the FK change itself is still drift: {ddl}"
    );
    assert!(
        !ddl.contains("DROP INDEX `fk_orders_owner`"),
        "an index the source still declares must not be dropped: {ddl}"
    );
}
