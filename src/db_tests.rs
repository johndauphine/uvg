use super::*;
use crate::dialect::Dialect;

#[test]
fn test_basic_split() {
    let ddl = "CREATE TABLE a (id INT); CREATE TABLE b (id INT);";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 2);
    assert_eq!(stmts[0], "CREATE TABLE a (id INT)");
    assert_eq!(stmts[1], "CREATE TABLE b (id INT)");
}

#[test]
fn test_semicolon_in_single_quotes() {
    let ddl = "COMMENT ON TABLE foo IS 'has; semicolons; inside';\nCREATE TABLE bar (id INT);";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 2);
    assert_eq!(
        stmts[0],
        "COMMENT ON TABLE foo IS 'has; semicolons; inside'"
    );
    assert_eq!(stmts[1], "CREATE TABLE bar (id INT)");
}

#[test]
fn test_escaped_single_quotes() {
    let ddl = "COMMENT ON TABLE foo IS 'it''s a test; with quotes';\nSELECT 1;";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 2);
    assert_eq!(
        stmts[0],
        "COMMENT ON TABLE foo IS 'it''s a test; with quotes'"
    );
    assert_eq!(stmts[1], "SELECT 1");
}

#[test]
fn test_dollar_quoting() {
    let ddl = "CREATE FUNCTION f() RETURNS void AS $$ BEGIN; END; $$ LANGUAGE plpgsql;\nSELECT 1;";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 2);
    assert!(stmts[0].contains("BEGIN; END;"));
    assert_eq!(stmts[1], "SELECT 1");
}

#[test]
fn test_named_dollar_quoting() {
    let ddl = "CREATE FUNCTION f() AS $body$ x; y; $body$ LANGUAGE sql;\nSELECT 2;";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 2);
    assert!(stmts[0].contains("x; y;"));
}

#[test]
fn test_line_comments_skipped() {
    let ddl = "-- header comment\n-- another\nCREATE TABLE a (id INT);";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0], "CREATE TABLE a (id INT)");
}

#[test]
fn test_risk_comments_skipped() {
    let ddl = "-- RISK: blocking\nALTER TABLE users ADD COLUMN email TEXT;";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0], "ALTER TABLE users ADD COLUMN email TEXT");
}

#[test]
fn test_semicolon_in_line_comment() {
    let ddl = "-- this; has; semicolons\nCREATE TABLE a (id INT);";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0], "CREATE TABLE a (id INT)");
}

#[test]
fn test_comment_only_blocks_stripped() {
    let ddl = "-- just a comment;\n-- nothing here;\n";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 0);
}

#[test]
fn test_trailing_content_without_semicolon() {
    let ddl = "CREATE TABLE a (id INT)";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0], "CREATE TABLE a (id INT)");
}

#[test]
fn test_empty_input() {
    assert_eq!(split_statements("", Dialect::Postgres).len(), 0);
    assert_eq!(split_statements("  \n  ", Dialect::Postgres).len(), 0);
    assert_eq!(split_statements(";;;", Dialect::Postgres).len(), 0);
}

// ---- quoted-identifier / block-comment hardening (#110) ----
//
// The generator quotes arbitrary introspected identifiers, so a name legally
// containing `;`, `'`, or `--` must not fracture the surrounding statement.
// One case per quote style, plus its dialect-specific escape sequence.

#[test]
fn test_semicolon_in_double_quoted_identifier() {
    // PG/SQLite: `"..."`, `;` inside must not terminate the statement.
    let ddl = "CREATE TABLE \"we;ird\" (id INT);\nSELECT 1;";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 2);
    assert_eq!(stmts[0], "CREATE TABLE \"we;ird\" (id INT)");
    assert_eq!(stmts[1], "SELECT 1");
}

#[test]
fn test_single_quote_and_dashes_in_double_quoted_identifier() {
    // A `'` inside `"..."` must not open a string, and `--` must not open a
    // line comment -- both would swallow the real terminator.
    let ddl = "CREATE TABLE \"a'b--c;d\" (x INT);\nSELECT 1;";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 2);
    assert_eq!(stmts[0], "CREATE TABLE \"a'b--c;d\" (x INT)");
    assert_eq!(stmts[1], "SELECT 1");
}

#[test]
fn test_escaped_double_quote_in_identifier() {
    // `""` is an escaped quote, so the identifier keeps scanning past it and
    // the `;c` stays inside the quotes.
    let ddl = "CREATE TABLE \"a\"\"b;c\" (x INT);\nSELECT 1;";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 2);
    assert_eq!(stmts[0], "CREATE TABLE \"a\"\"b;c\" (x INT)");
    assert_eq!(stmts[1], "SELECT 1");
}

#[test]
fn test_semicolon_in_backtick_identifier() {
    // MySQL: `` `...` `` with ``` `` ``` escape.
    let ddl = "CREATE TABLE `a``b;c` (id INT);\nSELECT 1;";
    let stmts = split_statements(ddl, Dialect::Mysql);
    assert_eq!(stmts.len(), 2);
    assert_eq!(stmts[0], "CREATE TABLE `a``b;c` (id INT)");
    assert_eq!(stmts[1], "SELECT 1");
}

#[test]
fn test_semicolon_in_bracket_identifier() {
    // MSSQL: `[...]` where only `]` is special and `]]` escapes it.
    let ddl = "CREATE TABLE [a]]b;c] (id INT);\nSELECT 1;";
    let stmts = split_statements(ddl, Dialect::Mssql);
    assert_eq!(stmts.len(), 2);
    assert_eq!(stmts[0], "CREATE TABLE [a]]b;c] (id INT)");
    assert_eq!(stmts[1], "SELECT 1");
}

#[test]
fn test_pg_array_type_brackets_are_not_identifiers() {
    // Under Postgres `[` is array syntax, not a bracket identifier, so
    // `integer[]` is ordinary text and the terminator still lands.
    let ddl = "CREATE TABLE \"t\" (\"a\" integer[]);\nSELECT 1;";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 2);
    assert_eq!(stmts[0], "CREATE TABLE \"t\" (\"a\" integer[])");
    assert_eq!(stmts[1], "SELECT 1");
}

#[test]
fn test_pg_array_constructor_adjacent_brackets_do_not_merge_statements() {
    // Regression for the bug an earlier dialect-blind version introduced:
    // a PG array constructor ending in `]]` (e.g. `ARRAY[[1,2],[3,4]]`) was
    // misread as an escaped MSSQL bracket, keeping the scanner "inside" a
    // bracket identifier and swallowing the following statement terminator.
    // With dialect-aware scanning, `[`/`]` are plain text under Postgres.
    let ddl = "CREATE TABLE t (a integer[] DEFAULT ARRAY[[1,2],[3,4]]);\nCREATE INDEX i ON t (a);";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(
        stmts.len(),
        2,
        "array constructor must not merge statements"
    );
    assert_eq!(
        stmts[0],
        "CREATE TABLE t (a integer[] DEFAULT ARRAY[[1,2],[3,4]])"
    );
    assert_eq!(stmts[1], "CREATE INDEX i ON t (a)");
}

#[test]
fn test_semicolon_in_block_comment() {
    // `/* ... */` block comments may contain `;` without terminating.
    let ddl = "CREATE TABLE a (id INT) /* note; with; semis */;\nSELECT 1;";
    let stmts = split_statements(ddl, Dialect::Postgres);
    assert_eq!(stmts.len(), 2);
    assert!(stmts[0].contains("CREATE TABLE a (id INT)"));
    assert_eq!(stmts[1], "SELECT 1");
}

/// Round-trip contract: statements joined the way the apply path joins them
/// (`collect_apply_sql` / `render_up_sql` use `"\n\n"`) must split back into
/// exactly the original statements. Each dialect is exercised with its own
/// identifier quote style embedding `;`, `'`, `--`, and the style's escape
/// sequence -- the payloads that would fracture a naive splitter. A single
/// DDL blob is always single-dialect in practice, so each case uses one.
#[test]
fn test_split_render_round_trip_per_dialect() {
    let cases: Vec<(Dialect, Vec<String>)> = vec![
        (
            Dialect::Postgres,
            vec![
                "CREATE TABLE \"pg;'--\"\"weird\" (\"c\" integer[] DEFAULT ARRAY[[1,2],[3,4]])"
                    .to_string(),
                "COMMENT ON TABLE plain IS 'a; b; c'".to_string(),
                "CREATE INDEX ix ON plain (id)".to_string(),
            ],
        ),
        (
            Dialect::Mysql,
            vec![
                "CREATE TABLE `my;'--``weird` (id INT)".to_string(),
                "INSERT INTO t VALUES ('a; b')".to_string(),
            ],
        ),
        (
            Dialect::Mssql,
            vec![
                "CREATE TABLE [ms;'--]]weird] (id INT)".to_string(),
                "INSERT INTO t VALUES ('a; b')".to_string(),
            ],
        ),
    ];

    for (dialect, statements) in cases {
        // Mirror the apply path: each statement ends with `;`, joined by "\n\n".
        let blob = statements
            .iter()
            .map(|s| format!("{s};"))
            .collect::<Vec<_>>()
            .join("\n\n");
        let split = split_statements(&blob, dialect);
        assert_eq!(
            split, statements,
            "split(render(statements)) must recover the originals for {dialect:?}"
        );
    }
}

// ---- retry primitive (#43) ----

#[test]
fn pg_sqlstate_retryable_matches_class_40_and_08() {
    // Class 40 — transaction rollback (serialization failure, deadlock).
    assert!(classify_pg_sqlstate_retryable("40001"));
    assert!(classify_pg_sqlstate_retryable("40P01"));
    // Class 08 — connection exception.
    assert!(classify_pg_sqlstate_retryable("08000"));
    assert!(classify_pg_sqlstate_retryable("08006"));
    // Non-retryable: logical errors (syntax, constraint, etc.).
    assert!(!classify_pg_sqlstate_retryable("23505")); // unique violation
    assert!(!classify_pg_sqlstate_retryable("42P01")); // undefined table
    assert!(!classify_pg_sqlstate_retryable("42601")); // syntax error
    assert!(!classify_pg_sqlstate_retryable(""));
    assert!(!classify_pg_sqlstate_retryable("00000"));
}

#[test]
fn mysql_code_retryable_matches_documented_codes_only() {
    // Documented retryable codes.
    assert!(classify_mysql_code_retryable("1213")); // deadlock
    assert!(classify_mysql_code_retryable("1205")); // lock wait timeout
    assert!(classify_mysql_code_retryable("2006")); // server gone
    assert!(classify_mysql_code_retryable("2013")); // connection lost
                                                    // Logical errors: not retryable.
    assert!(!classify_mysql_code_retryable("1062")); // dup entry
    assert!(!classify_mysql_code_retryable("1146")); // table doesn't exist
    assert!(!classify_mysql_code_retryable("1064")); // syntax error
                                                     // Don't substring-match — 12130 must NOT be confused with 1213.
    assert!(!classify_mysql_code_retryable("12130"));
    assert!(!classify_mysql_code_retryable(""));
}

#[test]
fn mssql_code_retryable_matches_documented_codes_only() {
    assert!(classify_mssql_code_retryable(1205)); // deadlock victim
    assert!(classify_mssql_code_retryable(4060)); // cannot open db (transient)
    assert!(classify_mssql_code_retryable(11001)); // host unreachable
                                                   // Logical errors.
    assert!(!classify_mssql_code_retryable(2627)); // PK violation
    assert!(!classify_mssql_code_retryable(208)); // invalid object name
    assert!(!classify_mssql_code_retryable(0));
}

#[test]
fn retry_delay_follows_schedule_with_jitter_in_bounds() {
    // Backoff base: 100ms / 500ms / 2000ms per the issue spec.
    // Each is allowed ±10% jitter; assert membership in [base*0.9, base*1.1]
    // with a 1ms floor for the smallest tier so 90ms passes.
    for _ in 0..50 {
        let d1 = retry_delay_ms(1);
        let d2 = retry_delay_ms(2);
        let d3 = retry_delay_ms(3);
        let d_overflow = retry_delay_ms(99);
        assert!((90..=110).contains(&d1), "attempt 1 out of band: {d1}");
        assert!((450..=550).contains(&d2), "attempt 2 out of band: {d2}");
        assert!((1800..=2200).contains(&d3), "attempt 3 out of band: {d3}");
        assert!(
            (1800..=2200).contains(&d_overflow),
            "attempts > 3 should saturate at the 2s tier: {d_overflow}"
        );
    }
}

#[test]
fn retry_delay_attempt_zero_is_floor_safe() {
    // Defensive: passing 0 must not panic or wrap. Treat as
    // first attempt (100ms tier).
    let d = retry_delay_ms(0);
    assert!(
        (90..=110).contains(&d),
        "attempt 0 should hit the 100ms tier, got {d}"
    );
}

// ---- run_with_retry behavior (#43) ----
// Uses `start_paused = true` so the backoff `tokio::time::sleep`
// calls auto-advance without real-time wall-clock waits — tests
// stay sub-millisecond even when exhausting the retry budget.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn retry_helper_retries_until_success() {
    // Fail twice with retryable=true, succeed on the third call.
    // max_retries=3 leaves enough budget; expect 3 invocations
    // and a None error.
    let calls = Arc::new(AtomicU8::new(0));
    let calls_c = calls.clone();
    let outcome = run_with_retry(3, move |_attempt| {
        let calls = calls_c.clone();
        async move {
            let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
            if n < 3 {
                Err(("transient".to_string(), true))
            } else {
                Ok(())
            }
        }
    })
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(outcome.error.is_none(), "expected success after retries");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn retry_helper_exhausts_budget_then_surfaces_error() {
    // Always retryable. max_retries=2 → 1 initial + 2 retries = 3
    // total invocations, terminal error after.
    let calls = Arc::new(AtomicU8::new(0));
    let calls_c = calls.clone();
    let outcome = run_with_retry(2, move |_attempt| {
        let calls = calls_c.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(("still transient".to_string(), true))
        }
    })
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 3, "1 initial + 2 retries");
    let err = outcome.error.expect("should surface terminal error");
    assert!(err.contains("still transient"), "got: {err}");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn retry_helper_does_not_retry_non_retryable() {
    // retryable=false on the first failure: no retry budget
    // consumed, exactly one invocation, error surfaces immediately.
    let calls = Arc::new(AtomicU8::new(0));
    let calls_c = calls.clone();
    let outcome = run_with_retry(5, move |_attempt| {
        let calls = calls_c.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(("logical bug".to_string(), false))
        }
    })
    .await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "no retries on non-retryable"
    );
    let err = outcome.error.expect("should surface immediately");
    assert!(err.contains("logical bug"), "got: {err}");
}

#[test]
fn supports_parse_check_only_pg() {
    // PG (BEGIN/ROLLBACK) has a server-side parse-check mode uvg can
    // safely use. MSSQL PARSEONLY is syntax-only and is not safe in
    // the Tiberius apply path, so it joins MySQL/SQLite in the
    // skipped dialect set.
    assert!(supports_parse_check(&ConnectionConfig::Postgres(
        "postgres://x".to_string()
    )));
    assert!(!supports_parse_check(&ConnectionConfig::Mssql {
        host: "x".to_string(),
        port: 1433,
        database: "x".to_string(),
        user: "x".to_string(),
        password: "x".to_string(),
        trust_cert: false,
    }));
    assert!(!supports_parse_check(&ConnectionConfig::Mysql(
        "mysql://x".to_string()
    )));
    assert!(!supports_parse_check(&ConnectionConfig::Sqlite(
        "sqlite::memory:".to_string()
    )));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn retry_helper_with_zero_retries_runs_once() {
    // --apply-retries 0 should disable retry entirely: a single
    // attempt, no second chance even for retryable errors.
    let calls = Arc::new(AtomicU8::new(0));
    let calls_c = calls.clone();
    let outcome = run_with_retry(0, move |_attempt| {
        let calls = calls_c.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(("transient".to_string(), true))
        }
    })
    .await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "max_retries=0 means single attempt"
    );
    assert!(outcome.error.is_some());
}

#[tokio::test]
async fn sqlx_ddl_helper_invokes_callbacks_and_stops_after_failure() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let statements = vec![
        "CREATE TABLE a (id INT)".to_string(),
        "CREATE TABLE a (id INT)".to_string(),
        "CREATE TABLE b (id INT)".to_string(),
    ];
    let mut seen = Vec::new();

    let results = execute_sqlx_ddl_statements(
        &pool,
        &statements,
        0,
        &mut |result, index, total| {
            seen.push((index, total, result.sql.clone(), result.error.is_some()));
        },
        |_| false,
    )
    .await;
    pool.close().await;

    assert_eq!(results.len(), 2);
    assert!(results[0].error.is_none());
    assert!(results[1].error.is_some());
    assert_eq!(
        seen,
        vec![
            (1, 3, "CREATE TABLE a (id INT)".to_string(), false),
            (2, 3, "CREATE TABLE a (id INT)".to_string(), true),
        ]
    );
}

// ---- transaction-control guard for PG transactional apply (#109) ----

#[test]
fn transaction_control_keyword_flags_tx_statements() {
    for (sql, expected) in [
        ("BEGIN", Some("BEGIN")),
        ("begin transaction", Some("BEGIN")),
        ("COMMIT", Some("COMMIT")),
        ("commit;", Some("COMMIT")),
        ("ROLLBACK", Some("ROLLBACK")),
        ("END", Some("END")),
        ("ABORT", Some("ABORT")),
        ("START TRANSACTION", Some("START")),
        ("SAVEPOINT sp1", Some("SAVEPOINT")),
        ("RELEASE SAVEPOINT sp1", Some("RELEASE")),
        ("PREPARE TRANSACTION 'gid'", Some("PREPARE TRANSACTION")),
    ] {
        assert_eq!(transaction_control_keyword(sql), expected, "sql: {sql}");
    }
}

#[test]
fn transaction_control_keyword_sees_through_leading_comments() {
    // PostgreSQL ignores leading comments, so the guard must too, or a
    // `/* x */ COMMIT` slips through and ends the wrapper transaction.
    assert_eq!(
        transaction_control_keyword("/* end */ COMMIT"),
        Some("COMMIT")
    );
    assert_eq!(
        transaction_control_keyword("-- note\nROLLBACK"),
        Some("ROLLBACK")
    );
    assert_eq!(
        transaction_control_keyword("/* a /* nested */ b */ BEGIN"),
        Some("BEGIN")
    );
    assert_eq!(
        transaction_control_keyword("  /* c1 */  /* c2 */  COMMIT ;"),
        Some("COMMIT")
    );
}

#[test]
fn transaction_control_keyword_ignores_ordinary_ddl() {
    for sql in [
        "CREATE TABLE t (id INT)",
        "ALTER TABLE t ADD COLUMN c INT",
        "DROP TABLE t",
        // A bare PREPARE (prepared statement) is not transaction control.
        "PREPARE plan AS SELECT 1",
        // Identifiers that merely start with a control word are fine.
        "CREATE TABLE beginnings (id INT)",
    ] {
        assert_eq!(transaction_control_keyword(sql), None, "sql: {sql}");
    }
}
