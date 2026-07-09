use super::sql_text::{strip_mssql_parens, strip_pg_typecast};
use super::*;
use crate::cli::GeneratorOptions;
use crate::dialect::Dialect;
use crate::testutil::{col, schema_pg, table};

#[test]
fn test_format_server_default_pg() {
    assert_eq!(
        format_server_default("now()", Dialect::Postgres),
        "text('now()')"
    );
    assert_eq!(format_server_default("0", Dialect::Postgres), "text('0')");
}

#[test]
fn test_strip_pg_typecast() {
    assert_eq!(strip_pg_typecast("0::integer"), "0");
    assert_eq!(strip_pg_typecast("'hello'::character varying"), "'hello'");
    assert_eq!(strip_pg_typecast("now()"), "now()");
    assert_eq!(
        strip_pg_typecast("nextval('seq'::regclass)"),
        "nextval('seq'::regclass)"
    );
}

#[test]
fn test_format_server_default_mssql() {
    assert_eq!(format_server_default("((0))", Dialect::Mssql), "text('0')");
    assert_eq!(
        format_server_default("(N'hello')", Dialect::Mssql),
        "text(\"'hello'\")"
    );
    assert_eq!(
        format_server_default("(getdate())", Dialect::Mssql),
        "text('getdate()')"
    );
}

#[test]
fn test_strip_mssql_parens() {
    assert_eq!(strip_mssql_parens("((0))"), "0");
    assert_eq!(strip_mssql_parens("(N'hello')"), "'hello'");
    assert_eq!(strip_mssql_parens("(getdate())"), "getdate()");
    assert_eq!(strip_mssql_parens("((1))"), "1");
}

#[test]
fn test_is_serial_default() {
    assert!(is_serial_default(
        "nextval('seq'::regclass)",
        Dialect::Postgres
    ));
    assert!(!is_serial_default("nextval('seq')", Dialect::Mssql));
    assert!(!is_serial_default("((1))", Dialect::Mssql));
}

#[test]
fn test_split_python_declarative() {
    let schema = schema_pg(vec![
        table("users")
            .column(col("id").build())
            .pk("users_pk", &["id"])
            .build(),
        table("posts")
            .column(col("id").build())
            .pk("posts_pk", &["id"])
            .build(),
    ]);
    let files = declarative::generate_split(&schema, &GeneratorOptions::default());
    let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();

    assert!(names.contains(&"base.py"), "missing base.py: {names:?}");
    assert!(names.contains(&"users.py"), "missing users.py: {names:?}");
    assert!(names.contains(&"posts.py"), "missing posts.py: {names:?}");
    assert!(
        names.contains(&"__init__.py"),
        "missing __init__.py: {names:?}"
    );

    // base.py should have imports and Base class
    let base = &files.iter().find(|(n, _)| n == "base.py").unwrap().1;
    assert!(base.contains("from sqlalchemy"), "base.py missing imports");
    assert!(base.contains("class Base"), "base.py missing Base class");

    // model files should have from .base import
    let users = &files.iter().find(|(n, _)| n == "users.py").unwrap().1;
    assert!(
        users.contains("from .base import"),
        "users.py missing base import"
    );
    assert!(
        users.contains("__tablename__"),
        "users.py missing tablename"
    );
}

#[test]
fn test_split_python_enum_stays_in_base() {
    // A synthetic enum from a CHECK constraint renders as an enum class in
    // the prelude; it must land in base.py, never in its own file.
    let schema = schema_pg(vec![table("users")
        .column(col("id").build())
        .column(col("status").udt("varchar").max_length(20).build())
        .pk("users_pk", &["id"])
        .check("users_status_check", "status IN ('active', 'inactive')")
        .build()]);
    let files = declarative::generate_split(&schema, &GeneratorOptions::default());
    let base = &files.iter().find(|(n, _)| n == "base.py").unwrap().1;
    assert!(base.contains("UsersStatus"), "enum should be in base.py");

    let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        !names.contains(&"users_status.py"),
        "enum should not be a separate file: {names:?}"
    );
}

#[test]
fn test_split_python_tables_generator() {
    let schema = schema_pg(vec![
        table("users").column(col("id").build()).build(),
        table("posts").column(col("id").build()).build(),
    ]);
    let files = tables::generate_split(&schema, &GeneratorOptions::default());
    let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names.contains(&"t_users.py"),
        "missing t_users.py: {names:?}"
    );
    assert!(
        names.contains(&"t_posts.py"),
        "missing t_posts.py: {names:?}"
    );
    // __init__ re-exports base + both modules.
    let init = &files.iter().find(|(n, _)| n == "__init__.py").unwrap().1;
    assert!(init.contains("from .base import *"));
    assert!(init.contains("from .t_users import *"));
}
