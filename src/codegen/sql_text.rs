//! SQL-text analysis shared across generators and the diff engine:
//! default-expression cleaning per dialect, CHECK-predicate parsing, and
//! serial/auto-increment detection.

use crate::dialect::Dialect;

/// Strip PostgreSQL type casts from a default expression.
/// e.g. "'hello'::character varying" -> "'hello'"
/// e.g. "0::integer" -> "0"
pub(crate) fn strip_pg_typecast(expr: &str) -> &str {
    // Find the last :: that's not inside quotes
    if let Some(pos) = find_typecast_pos(expr) {
        expr[..pos].trim()
    } else {
        expr.trim()
    }
}

fn find_typecast_pos(expr: &str) -> Option<usize> {
    let bytes = expr.as_bytes();
    let mut in_quotes = false;
    let mut in_parens = 0u32;
    let mut i = 0;
    let mut last_cast_pos = None;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => in_quotes = !in_quotes,
            b'(' if !in_quotes => in_parens += 1,
            b')' if !in_quotes => in_parens = in_parens.saturating_sub(1),
            b':' if !in_quotes && in_parens == 0 && i + 1 < bytes.len() && bytes[i + 1] == b':' => {
                last_cast_pos = Some(i);
                i += 1; // skip second ':'
            }
            _ => {}
        }
        i += 1;
    }

    last_cast_pos
}

/// Strip MSSQL wrapping parentheses and leading N from string literals.
/// e.g. "((0))" -> "0"
/// e.g. "(N'hello')" -> "'hello'"
pub(crate) fn strip_mssql_parens(expr: &str) -> &str {
    let mut s = expr.trim();
    // Strip outer parens: MSSQL defaults are often wrapped like ((value))
    while s.starts_with('(') && s.ends_with(')') {
        s = &s[1..s.len() - 1];
    }
    // Strip leading N from N'string' literals
    if s.starts_with("N'") || s.starts_with("N\"") {
        s = &s[1..];
    }
    s.trim()
}

/// Try to parse an IN-list from a check constraint expression.
/// Returns (column_name, values) if the expression matches `[table.]column IN ('a', 'b', 'c')`.
pub fn parse_check_enum(expression: &str) -> Option<(String, Vec<String>)> {
    // Match pattern: [optional_table.]column IN ('val1', 'val2', ...)
    let expr = expression.trim();

    // Find " IN (" (case-insensitive) using byte-level search to avoid
    // index mismatch from to_uppercase() on non-ASCII input.
    let needle = b" IN (";
    let in_pos = expr.as_bytes().windows(needle.len()).position(|window| {
        window
            .iter()
            .zip(needle.iter())
            .all(|(b, n)| b.to_ascii_uppercase() == *n)
    })?;
    let col_part = expr[..in_pos].trim();

    // Extract column name (strip optional table prefix)
    let col_name = if let Some(dot_pos) = col_part.rfind('.') {
        col_part[dot_pos + 1..].trim()
    } else {
        col_part
    };

    // Extract the IN list (needle " IN (" is 5 bytes)
    let list_start = in_pos + 4; // skip " IN " (the '(' is checked below)
    let list_str = expr[list_start..].trim();
    if !list_str.starts_with('(') || !list_str.ends_with(')') {
        return None;
    }

    let inner = &list_str[1..list_str.len() - 1];

    // Parse quoted string values
    let mut values = Vec::new();
    for item in inner.split(',') {
        let trimmed = item.trim();
        if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
            // Unescape SQL doubled quotes: '' → '
            let raw = &trimmed[1..trimmed.len() - 1];
            values.push(raw.replace("''", "'"));
        } else {
            // Not a string enum (could be numeric IN list)
            return None;
        }
    }

    if values.is_empty() {
        return None;
    }

    Some((col_name.to_string(), values))
}

/// Check if a check constraint expression represents a boolean column.
/// Returns the column name if the expression matches `[schema.][table.]column IN (0, 1)`.
pub fn parse_check_boolean(expression: &str) -> Option<String> {
    let expr = expression.trim();

    let needle = b" IN (";
    let in_pos = expr.as_bytes().windows(needle.len()).position(|window| {
        window
            .iter()
            .zip(needle.iter())
            .all(|(b, n)| b.to_ascii_uppercase() == *n)
    })?;
    let col_part = expr[..in_pos].trim();

    // Extract column name (strip optional schema.table prefix)
    let col_name = if let Some(dot_pos) = col_part.rfind('.') {
        col_part[dot_pos + 1..].trim()
    } else {
        col_part
    };

    let list_start = in_pos + 4;
    let list_str = expr[list_start..].trim();
    if !list_str.starts_with('(') || !list_str.ends_with(')') {
        return None;
    }

    let inner = &list_str[1..list_str.len() - 1];
    let items: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();

    // Must be exactly (0, 1) in any order
    if items.len() == 2
        && ((items[0] == "0" && items[1] == "1") || (items[0] == "1" && items[1] == "0"))
    {
        Some(col_name.to_string())
    } else {
        None
    }
}

/// Check if a column default is a serial/sequence default.
/// PG: starts with `nextval(`; MSSQL: always false (identity columns have NULL defaults).
pub fn is_serial_default(default: &str, dialect: Dialect) -> bool {
    match dialect {
        Dialect::Postgres => default.starts_with("nextval("),
        Dialect::Mssql | Dialect::Mysql | Dialect::Sqlite => false,
    }
}

/// Check if a column is auto-increment in its source dialect.
/// Unifies MSSQL `IDENTITY`, PG `GENERATED ... AS IDENTITY`, PG `SERIAL` (via
/// `nextval(...)` default), MySQL `AUTO_INCREMENT`, and SQLite `AUTOINCREMENT`.
pub fn is_auto_increment_column(col: &crate::schema::ColumnInfo, dialect: Dialect) -> bool {
    col.is_identity
        || col.autoincrement == Some(true)
        || col
            .column_default
            .as_deref()
            .map(|d| is_serial_default(d, dialect))
            .unwrap_or(false)
}

/// Extract the sequence name from a nextval default expression.
/// e.g. "nextval('my_seq'::regclass)" → Some("my_seq")
pub fn parse_sequence_name(default: &str) -> Option<String> {
    let s = default.strip_prefix("nextval('")?;
    let end = s.find('\'')?;
    Some(s[..end].to_string())
}

/// Check if a sequence name is "standard" (auto-generated by PG serial).
/// Standard pattern: {table}_{column}_seq
pub fn is_standard_sequence_name(seq_name: &str, table_name: &str, col_name: &str) -> bool {
    seq_name == format!("{table_name}_{col_name}_seq")
}
