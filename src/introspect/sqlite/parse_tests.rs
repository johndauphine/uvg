use super::parse::{create_table_body, first_token, identifier_matches, split_respecting_parens};

#[test]
fn create_table_body_returns_outer_body() {
    let sql = "CREATE TABLE users (id INTEGER, name TEXT)";

    assert_eq!(create_table_body(sql), Some("id INTEGER, name TEXT"));
}

#[test]
fn split_respecting_parens_ignores_nested_and_quoted_commas() {
    let parts = split_respecting_parens(
        "label TEXT DEFAULT 'a,b', amount DECIMAL(10,2), CHECK(amount IN (1, 2))",
    );

    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0].trim(), "label TEXT DEFAULT 'a,b'");
    assert_eq!(parts[1].trim(), "amount DECIMAL(10,2)");
    assert_eq!(parts[2].trim(), "CHECK(amount IN (1, 2))");
}

#[test]
fn first_token_and_identifier_matches_handle_quoted_identifiers() {
    let token = first_token(r#""User ID" INTEGER PRIMARY KEY AUTOINCREMENT"#);
    assert_eq!(token, r#""User ID""#);
    assert!(identifier_matches(token, "user id"));

    let token = first_token("[order-id] INTEGER");
    assert_eq!(token, "[order-id]");
    assert!(identifier_matches(token, "ORDER-ID"));
}
