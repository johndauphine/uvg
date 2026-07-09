use crate::dialect::Dialect;
use crate::output::{Change, ChangeKind};

use super::files::flatten_for_comment;

/// Derive the DOWN SQL that reverses a single forward `Change`.
///
/// Dispatch is driven by `change.kind` through an exhaustive `match`, so a
/// new `ChangeKind` variant fails to compile until its reversal is handled --
/// the coverage cannot silently drift from the generator's vocabulary the way
/// the previous prefix-matching implementation could. The rendered SQL is
/// still parsed to recover table/column/index names, but only inside the
/// branch already known to be the right operation.
pub(super) fn reverse_change(change: &Change, target_dialect: Dialect) -> String {
    let Some(statement) = executable_statement(&change.sql) else {
        return "-- IRREVERSIBLE: no executable SQL found to reverse.".to_string();
    };

    match change.kind {
        ChangeKind::CreateTable => reverse_create_table(&statement),
        ChangeKind::CreateIndex => {
            reverse_create_index(&statement, target_dialect).unwrap_or_else(|| {
                irreversible_down(
                    "uvg cannot automatically reverse this index creation",
                    &change.sql,
                )
            })
        }
        // Reversing an added column drops it. That destroys any data written
        // to the column since the upgrade, so it is flagged destructive (like
        // a forward DROP COLUMN) -- but it is a valid, applicable reversal, so
        // it is NOT marked IRREVERSIBLE (which would refuse to run).
        ChangeKind::AddColumn => {
            reverse_add_column(&statement, target_dialect).unwrap_or_else(|| {
                irreversible_down(
                    "uvg cannot automatically reverse this column addition",
                    &change.sql,
                )
            })
        }
        ChangeKind::DropTable => irreversible_down(
            "this migration drops a table; original schema and data are lost",
            &change.sql,
        ),
        ChangeKind::DropColumn => irreversible_down(
            "this migration drops a column; column data is lost",
            &change.sql,
        ),
        ChangeKind::DropIndex => irreversible_down(
            "this migration drops an index; the original definition is not available here",
            &change.sql,
        ),
        ChangeKind::AlterColumn => irreversible_down(
            "this migration alters a column; the prior definition is not captured here",
            &change.sql,
        ),
        ChangeKind::AddConstraint | ChangeKind::DropConstraint => irreversible_down(
            "uvg cannot automatically reverse this constraint change",
            &change.sql,
        ),
        ChangeKind::Other => irreversible_down(
            "uvg cannot automatically reverse this statement",
            &change.sql,
        ),
    }
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

fn reverse_create_table(statement: &str) -> String {
    let rest = statement["CREATE TABLE".len()..].trim();
    let table = rest
        .split_once('(')
        .map(|(name, _)| name)
        .unwrap_or(rest)
        .trim();
    format!("DROP TABLE IF EXISTS {table};")
}

fn reverse_add_column(statement: &str, target_dialect: Dialect) -> Option<String> {
    let upper = statement.to_ascii_uppercase();
    // MSSQL spells column additions `ADD`; the others use `ADD COLUMN`.
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
    Some(format!(
        "-- WARNING: destructive operation\nALTER TABLE {table} DROP COLUMN {column};"
    ))
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
    if target_dialect.drop_index_requires_table() && index.contains('.') {
        // The DROP INDEX ... ON form cannot take a schema-qualified index.
        return None;
    }
    let after_on = rest[on_idx + " ON ".len()..].trim();
    let table = after_on
        .split_once('(')
        .map(|(table, _)| table)
        .unwrap_or(after_on)
        .trim();
    if target_dialect.drop_index_requires_table() {
        Some(format!("DROP INDEX {index} ON {table};"))
    } else {
        Some(format!("DROP INDEX {index};"))
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
