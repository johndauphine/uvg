use clap::Parser;
use crossterm::event::KeyCode;
use std::time::Duration;

use super::actions::{apply_ddl, collect_apply_sql, count_statements};
use super::app::{
    group_changes, node_detail_line_count, node_detail_text, App, AppState, TreeNode,
};
use super::events::{handle_view_keys, record_apply_report};
use crate::apply::{ApplyReport, ParseCheckStatus};
use crate::db::StmtResult;
use crate::output::Change;

fn ch(schema: &str, table: Option<&str>, sql: &str) -> Change {
    Change {
        table_schema: schema.to_string(),
        table_name: table.map(|s| s.to_string()),
        sql: sql.to_string(),
        kind: crate::output::ChangeKind::Other,
    }
}

fn node(name: &str) -> TreeNode {
    TreeNode {
        name: name.to_string(),
        changes: vec![ch("", Some(name), "CREATE TABLE t(id int);")],
        checked: true,
    }
}

fn view_app(nodes: Vec<TreeNode>) -> App {
    let mut app = App {
        state: AppState::ViewDdl,
        source_url: String::new(),
        target_url: String::new(),
        focused_field: 0,
        cursor_pos: [0, 0],
        nodes: Vec::new(),
        selected_idx: 0,
        scroll_offset: 0,
        empty_diff: false,
        executable_statement_count: 0,
        status_msg: String::new(),
        error_msg: None,
        success_msg: None,
        apply_results: Vec::new(),
        trust_cert: false,
        apply_retries: 3,
        parse_check: true,
    };
    app.set_nodes(nodes);
    app
}

fn apply_stmt(error: Option<&str>, rolled_back: bool) -> StmtResult {
    StmtResult {
        sql: "CREATE TABLE users(id INTEGER);".to_string(),
        error: error.map(str::to_string),
        duration: Duration::from_millis(1),
        rolled_back,
    }
}

#[test]
fn tui_discloses_partial_state_and_parse_check_skip_without_stderr() {
    let mut app = view_app(Vec::new());
    record_apply_report(
        &mut app,
        ApplyReport {
            statements: vec![apply_stmt(None, false), apply_stmt(Some("boom"), false)],
            parse_check: ParseCheckStatus::SkippedUnsupported,
        },
    );

    let message = app.error_msg.as_deref().unwrap();
    assert!(message.contains("nontransactional"), "{message}");
    assert!(message.contains("may have persisted"), "{message}");
    assert!(message.contains("partially migrated"), "{message}");
    assert!(message.contains("parse-check skipped"), "{message}");
    assert_eq!(app.apply_results.len(), 2);
}

#[test]
fn tui_retains_confirmed_rollback_wording() {
    let mut app = view_app(Vec::new());
    record_apply_report(
        &mut app,
        ApplyReport {
            statements: vec![apply_stmt(None, true), apply_stmt(Some("boom"), true)],
            parse_check: ParseCheckStatus::Passed,
        },
    );

    let message = app.error_msg.as_deref().unwrap();
    assert!(message.contains("rolled back"), "{message}");
    assert!(message.contains("target is unchanged"), "{message}");
    assert!(!message.contains("partially migrated"), "{message}");
    assert!(!message.contains("parse-check skipped"), "{message}");
}

#[test]
fn interactive_app_retains_apply_safety_options_from_cli() {
    let cli = crate::cli::Cli::try_parse_from([
        "uvg",
        "--interactive",
        "--apply-retries",
        "9",
        "--no-parse-check",
        "sqlite:///source.db",
        "sqlite:///target.db",
    ])
    .unwrap();

    let app = App::new(&cli);
    assert_eq!(app.apply_retries, 9);
    assert!(!app.parse_check);
}

#[tokio::test]
async fn interactive_apply_uses_shared_marker_guard_before_connecting() {
    let mut app = view_app(vec![TreeNode {
        name: "users".into(),
        changes: vec![ch(
            "",
            Some("users"),
            "CREATE TABLE must_not_land(id int);\n\
             -- WARNING: SQLite does not support ALTER COLUMN. Table recreation required.",
        )],
        checked: true,
    }]);
    app.target_url = "sqlite:////definitely/missing/target.db".into();

    let error = apply_ddl(&mut app).await.unwrap_err().to_string();
    assert!(error.contains("refusing to apply"), "{error}");
    assert!(
        error.contains("SQLite does not support ALTER COLUMN"),
        "{error}"
    );
}

#[test]
fn test_multi_node_up_down_boundaries_do_not_scroll_detail() {
    let mut app = view_app(vec![node("users"), node("posts")]);
    app.scroll_offset = 3;

    handle_view_keys(&mut app, KeyCode::Up);
    assert_eq!(app.selected_idx, 0);
    assert_eq!(
        app.scroll_offset, 3,
        "Up at the first node should not fall through to detail scrolling",
    );

    app.selected_idx = 1;
    app.scroll_offset = 4;
    handle_view_keys(&mut app, KeyCode::Down);
    assert_eq!(app.selected_idx, 1);
    assert_eq!(
        app.scroll_offset, 4,
        "Down at the last node should not fall through to detail scrolling",
    );
}

#[test]
fn test_group_changes_one_node_per_table_preserves_order() {
    let changes = vec![
        ch("", Some("users"), "CREATE TABLE users();"),
        ch("", Some("posts"), "CREATE TABLE posts();"),
        ch("", Some("users"), "CREATE INDEX ix ON users(email);"),
    ];
    let nodes = group_changes(changes);
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].name, "users");
    assert_eq!(nodes[1].name, "posts");
    // Two changes attributed to users/, in insertion order.
    assert_eq!(nodes[0].changes.len(), 2);
    assert!(nodes[0].changes[0].sql.contains("CREATE TABLE"));
    assert!(nodes[0].changes[1].sql.contains("CREATE INDEX"));
    // All nodes default to checked.
    assert!(nodes.iter().all(|n| n.checked));
}

#[test]
fn test_group_changes_schema_scoped_lands_in_underscore_schema() {
    let changes = vec![
        ch("", None, "CREATE TYPE status AS ENUM ('a');"),
        ch("", Some("users"), "CREATE TABLE users();"),
    ];
    let nodes = group_changes(changes);
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].name, "_schema");
    assert_eq!(nodes[1].name, "users");
}

#[test]
fn test_group_changes_non_default_schema_uses_double_underscore() {
    let changes = vec![ch(
        "billing",
        Some("orders"),
        "CREATE TABLE \"billing\".\"orders\"();",
    )];
    let nodes = group_changes(changes);
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].name, "billing__orders");
}

#[test]
fn test_collect_apply_sql_schema_first_then_tables() {
    // Insertion order: users, _schema, posts. _schema must still
    // come out first so enums/types exist before referencing tables.
    let nodes = vec![
        TreeNode {
            name: "users".into(),
            changes: vec![ch("", Some("users"), "ALTER TABLE users ADD COLUMN x int;")],
            checked: true,
        },
        TreeNode {
            name: "_schema".into(),
            changes: vec![ch("", None, "CREATE TYPE color AS ENUM ('r','g','b');")],
            checked: true,
        },
        TreeNode {
            name: "posts".into(),
            changes: vec![ch("", Some("posts"), "ALTER TABLE posts ADD COLUMN y int;")],
            checked: true,
        },
    ];
    let sql = collect_apply_sql(&nodes);
    let schema_pos = sql.find("CREATE TYPE").unwrap();
    let users_pos = sql.find("ALTER TABLE users").unwrap();
    let posts_pos = sql.find("ALTER TABLE posts").unwrap();
    assert!(schema_pos < users_pos, "_schema must precede users: {sql}");
    assert!(
        users_pos < posts_pos,
        "table order from compute_changes must be preserved among non-schema nodes: {sql}"
    );
}

#[test]
fn test_collect_apply_sql_unchecked_nodes_excluded() {
    let nodes = vec![
        TreeNode {
            name: "users".into(),
            changes: vec![ch("", Some("users"), "ALTER TABLE users ...;")],
            checked: true,
        },
        TreeNode {
            name: "posts".into(),
            changes: vec![ch("", Some("posts"), "ALTER TABLE posts ...;")],
            checked: false,
        },
    ];
    let sql = collect_apply_sql(&nodes);
    assert!(sql.contains("ALTER TABLE users"));
    assert!(
        !sql.contains("ALTER TABLE posts"),
        "unchecked node must not contribute SQL"
    );
}

#[test]
fn test_collect_apply_sql_no_checked_returns_empty() {
    let nodes = vec![TreeNode {
        name: "users".into(),
        changes: vec![ch("", Some("users"), "ALTER TABLE users ...;")],
        checked: false,
    }];
    assert_eq!(collect_apply_sql(&nodes), "");
}

#[test]
fn test_node_detail_line_count_matches_render_text() {
    let node = TreeNode {
        name: "users".into(),
        changes: vec![
            ch("", Some("users"), "CREATE TABLE users (\n  id int\n);"),
            ch("", Some("users"), "CREATE INDEX idx_users_id ON users(id);"),
        ],
        checked: true,
    };

    assert_eq!(
        node_detail_line_count(&node) as usize,
        node_detail_text(&node).lines().count()
    );
}

#[test]
fn test_cached_statement_count_updates_when_nodes_toggle() {
    let mut app = view_app(vec![
        TreeNode {
            name: "users".into(),
            changes: vec![ch(
                "",
                Some("users"),
                "ALTER TABLE users ADD COLUMN age int;",
            )],
            checked: true,
        },
        TreeNode {
            name: "notes".into(),
            changes: vec![ch("", Some("notes"), "-- comment only")],
            checked: true,
        },
    ]);

    assert_eq!(app.executable_statement_count(), 1);

    handle_view_keys(&mut app, KeyCode::Char(' '));
    assert_eq!(app.executable_statement_count(), 0);

    handle_view_keys(&mut app, KeyCode::Char(' '));
    assert_eq!(app.executable_statement_count(), 1);

    handle_view_keys(&mut app, KeyCode::Char('A'));
    assert_eq!(app.executable_statement_count(), 0);
}

#[test]
fn test_comment_only_changes_have_zero_executable_statements() {
    // Regression: codex round 5 caught that checked_count() > 0 is
    // not a sufficient apply gate — SQLite ALTER warnings and
    // MSSQL default-drop notes are comment-only, so they
    // contribute non-empty SQL to collect_apply_sql() but split to
    // zero executable statements. The apply gate now uses
    // count_statements(collect_apply_sql(...)) instead. This test
    // pins the property the gate depends on: a node holding only
    // comment SQL must split to 0 statements.
    let nodes = vec![TreeNode {
        name: "users".into(),
        changes: vec![ch(
            "",
            Some("users"),
            "-- WARNING: SQLite does not support ALTER COLUMN. Table recreation required.\n\
             -- ALTER TABLE \"users\" ALTER COLUMN \"email\" TYPE VARCHAR(255);",
        )],
        checked: true,
    }];
    let sql = collect_apply_sql(&nodes);
    assert!(!sql.is_empty(), "comment SQL is non-empty as text");
    assert_eq!(
        count_statements(&sql, crate::dialect::Dialect::Sqlite),
        0,
        "but it must split into zero executable statements; sql was: {sql}"
    );
}
