use crate::dialect::Dialect;

use super::files::flatten_for_comment;

pub(super) fn reverse_change_sql(sql: &str, target_dialect: Dialect) -> String {
    let Some(statement) = executable_statement(sql) else {
        return "-- IRREVERSIBLE: no executable SQL found to reverse.".to_string();
    };
    let upper = statement.to_ascii_uppercase();

    if upper.starts_with("CREATE TABLE ") {
        let rest = statement["CREATE TABLE".len()..].trim();
        let table = rest
            .split_once('(')
            .map(|(name, _)| name)
            .unwrap_or(rest)
            .trim();
        return format!("DROP TABLE IF EXISTS {table};");
    }
    if upper.starts_with("DROP TABLE ") {
        return irreversible_down(
            "this migration drops a table; original schema/data is lost",
            sql,
        );
    }
    if upper.starts_with("ALTER TABLE ") {
        if let Some(reverse) = reverse_alter_table_add_column(&statement, target_dialect) {
            return reverse;
        }
        if upper.contains(" DROP COLUMN ") {
            return irreversible_down("this migration drops a column; column data is lost", sql);
        }
    }
    if upper.starts_with("CREATE INDEX ") || upper.starts_with("CREATE UNIQUE INDEX ") {
        if let Some(reverse) = reverse_create_index(&statement, target_dialect) {
            return reverse;
        }
    }
    if upper.starts_with("DROP INDEX ") {
        return irreversible_down(
            "this migration drops an index; original definition is not available here",
            sql,
        );
    }

    irreversible_down("uvg cannot automatically reverse this statement", sql)
}

fn executable_statement(sql: &str) -> Option<String> {
    let statement = sql
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("--"))
        .collect::<Vec<_>>()
        .join(" ");
    if statement.is_empty() {
        None
    } else {
        Some(statement)
    }
}

fn reverse_alter_table_add_column(statement: &str, target_dialect: Dialect) -> Option<String> {
    let upper = statement.to_ascii_uppercase();
    let add_idx = upper.find(" ADD COLUMN ").or_else(|| {
        if target_dialect == Dialect::Mssql {
            upper.find(" ADD ")
        } else {
            None
        }
    })?;
    let table = statement["ALTER TABLE".len()..add_idx].trim();
    let after_add = if upper[add_idx..].starts_with(" ADD COLUMN ") {
        &statement[add_idx + " ADD COLUMN ".len()..]
    } else {
        &statement[add_idx + " ADD ".len()..]
    };
    let column = first_sql_token(after_add)?;
    let column_upper = column
        .trim_matches(|c| matches!(c, '"' | '`' | '[' | ']'))
        .to_ascii_uppercase();
    if matches!(column_upper.as_str(), "CONSTRAINT" | "DEFAULT" | "CHECK") {
        return None;
    }
    Some(format!("ALTER TABLE {table} DROP COLUMN {column};"))
}

fn reverse_create_index(statement: &str, target_dialect: Dialect) -> Option<String> {
    let upper = statement.to_ascii_uppercase();
    let prefix_len = if upper.starts_with("CREATE UNIQUE INDEX ") {
        "CREATE UNIQUE INDEX ".len()
    } else {
        "CREATE INDEX ".len()
    };
    let rest = &statement[prefix_len..];
    let on_idx = rest.to_ascii_uppercase().find(" ON ")?;
    let index = rest[..on_idx].trim();
    if matches!(target_dialect, Dialect::Mysql | Dialect::Mssql) && index.contains('.') {
        return None;
    }
    let after_on = rest[on_idx + " ON ".len()..].trim();
    let table = after_on
        .split_once('(')
        .map(|(table, _)| table)
        .unwrap_or(after_on)
        .trim();
    match target_dialect {
        Dialect::Mysql | Dialect::Mssql => Some(format!("DROP INDEX {index} ON {table};")),
        Dialect::Postgres | Dialect::Sqlite => Some(format!("DROP INDEX {index};")),
    }
}

pub(super) fn first_sql_token(input: &str) -> Option<&str> {
    let input = input.trim_start();
    let mut chars = input.char_indices();
    match chars.next()? {
        (_, '"') => quoted_sql_token(input, '"'),
        (_, '`') => quoted_sql_token(input, '`'),
        (_, '[') => bracketed_sql_token(input),
        _ => input
            .find(|c: char| c.is_whitespace() || c == ';' || c == ',')
            .map(|idx| &input[..idx])
            .or(Some(input)),
    }
}

fn quoted_sql_token(input: &str, quote: char) -> Option<&str> {
    let mut chars = input.char_indices().peekable();
    chars.next()?;
    while let Some((idx, ch)) = chars.next() {
        if ch == quote {
            if chars.peek().is_some_and(|(_, next)| *next == quote) {
                chars.next();
                continue;
            }
            return Some(&input[..idx + ch.len_utf8()]);
        }
    }
    None
}

fn bracketed_sql_token(input: &str) -> Option<&str> {
    let mut chars = input.char_indices().peekable();
    chars.next()?;
    while let Some((idx, ch)) = chars.next() {
        if ch == ']' {
            if chars.peek().is_some_and(|(_, next)| *next == ']') {
                chars.next();
                continue;
            }
            return Some(&input[..idx + ch.len_utf8()]);
        }
    }
    None
}

fn irreversible_down(reason: &str, original_sql: &str) -> String {
    format!(
        "-- IRREVERSIBLE: {reason}.\n-- Original SQL:\n{}",
        original_sql
            .lines()
            .map(|line| format!("--   {}", flatten_for_comment(line)))
            .collect::<Vec<_>>()
            .join("\n")
    )
}
