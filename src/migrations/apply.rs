use anyhow::{anyhow, Context, Result};

use crate::cli::{Cli, ConnectionConfig};
use crate::db;

use super::model::{MigrationDirection, MigrationFile, MigrationSection};

pub(super) fn migration_parse_check_enabled(cli: &Cli, config: &ConnectionConfig) -> bool {
    if cli.no_parse_check {
        return false;
    }
    if !db::supports_parse_check(config) {
        eprintln!(
            "uvg: migration parse-check skipped (no parse-only mode for this dialect; pass --no-parse-check before the subcommand to silence)"
        );
        return false;
    }
    true
}

pub(super) async fn parse_check_migration(
    config: &ConnectionConfig,
    migration: &MigrationFile,
    direction: MigrationDirection,
) -> Result<()> {
    let sql = migration_plan_sql(migration, direction)?;
    if sql.trim().is_empty() {
        return Ok(());
    }

    let errors = db::parse_check_ddl(config, &sql).await.with_context(|| {
        format!(
            "uvg: migration {} {} parse-check could not run for {}; no migration SQL was applied and uvg_version was not changed",
            migration.revision,
            direction.label(),
            migration.path.display()
        )
    })?;
    if errors.is_empty() {
        return Ok(());
    }

    Err(anyhow!(
        "uvg: migration {} {} parse-check found {} error(s) in {} before applying; uvg_version was not changed. Fix the migration SQL and retry, or pass --no-parse-check before the subcommand to skip:\n{}",
        migration.revision,
        direction.label(),
        errors.len(),
        migration.path.display(),
        format_parse_error_lines(&errors)
    ))
}

pub(super) fn migration_plan_sql(
    migration: &MigrationFile,
    direction: MigrationDirection,
) -> Result<String> {
    let mut sql = String::new();
    match direction {
        MigrationDirection::Up => {
            append_migration_section(&mut sql, MigrationSection::Pre, &migration.pre_sql);
            append_migration_section(&mut sql, MigrationSection::Up, &migration.up_sql);
            append_migration_section(&mut sql, MigrationSection::Post, &migration.post_sql);
        }
        MigrationDirection::Down => {
            let down_sql = checked_down_sql(migration)?;
            append_migration_section(
                &mut sql,
                MigrationSection::PostDown,
                &migration.post_down_sql,
            );
            append_migration_section(&mut sql, MigrationSection::Down, down_sql);
            append_migration_section(&mut sql, MigrationSection::PreDown, &migration.pre_down_sql);
        }
    }
    Ok(sql)
}

fn append_migration_section(sql: &mut String, section: MigrationSection, section_sql: &str) {
    let trimmed = section_sql.trim();
    if trimmed.is_empty() {
        return;
    }
    if !sql.is_empty() && !sql.ends_with('\n') {
        sql.push('\n');
    }
    if !sql.is_empty() {
        sql.push('\n');
    }
    sql.push_str("-- ");
    sql.push_str(section.label());
    sql.push('\n');
    sql.push_str(trimmed);
    sql.push('\n');
}

fn checked_down_sql(migration: &MigrationFile) -> Result<&str> {
    let down_sql = migration.down_sql.as_deref().ok_or_else(|| {
        anyhow!(
            "uvg: migration {} in {} is missing required DOWN section",
            migration.revision,
            migration.path.display()
        )
    })?;
    if down_sql
        .lines()
        .any(|line| line.trim_start().starts_with("-- IRREVERSIBLE:"))
    {
        return Err(anyhow!(
            "uvg: migration {} in {} has an irreversible DOWN section; refusing to change uvg_version",
            migration.revision,
            migration.path.display()
        ));
    }
    Ok(down_sql)
}

pub(super) fn format_parse_error_lines(errors: &[db::ParseError]) -> String {
    let mut msg = String::new();
    for (i, e) in errors.iter().enumerate() {
        let collapsed: String = e.sql.split_whitespace().collect::<Vec<_>>().join(" ");
        let preview = if collapsed.chars().count() > 120 {
            let cut: String = collapsed.chars().take(117).collect();
            format!("{cut}...")
        } else {
            collapsed
        };
        msg.push_str(&format!(
            "  [{}/{}] {}\n      {}\n",
            i + 1,
            errors.len(),
            preview,
            e.error
        ));
    }
    msg
}

pub(super) async fn apply_migration(
    config: &ConnectionConfig,
    migration: &MigrationFile,
) -> Result<()> {
    execute_migration_section(config, migration, MigrationSection::Pre, &migration.pre_sql).await?;
    execute_migration_section(config, migration, MigrationSection::Up, &migration.up_sql).await?;
    execute_migration_section(
        config,
        migration,
        MigrationSection::Post,
        &migration.post_sql,
    )
    .await
}

pub(super) async fn apply_down_migration(
    config: &ConnectionConfig,
    migration: &MigrationFile,
) -> Result<()> {
    let down_sql = checked_down_sql(migration)?;
    execute_migration_section(
        config,
        migration,
        MigrationSection::PostDown,
        &migration.post_down_sql,
    )
    .await?;
    execute_migration_section(config, migration, MigrationSection::Down, down_sql).await?;
    execute_migration_section(
        config,
        migration,
        MigrationSection::PreDown,
        &migration.pre_down_sql,
    )
    .await
}

async fn execute_migration_section(
    config: &ConnectionConfig,
    migration: &MigrationFile,
    section: MigrationSection,
    sql: &str,
) -> Result<()> {
    if sql.trim().is_empty() {
        return Ok(());
    }
    let results = db::execute_ddl(config, sql, 3, |_, _, _| {}).await?;
    let applied = results.iter().take_while(|r| r.error.is_none()).count();
    if let Some(failed) = results.iter().find(|r| r.error.is_some()) {
        // On a transactional (PostgreSQL) backend the whole section rolled
        // back, so nothing landed; otherwise earlier statements may have.
        let aftermath = if results.iter().any(|r| r.rolled_back) {
            "This section ran in a transaction and was rolled back; no statements were applied and uvg_version was not changed. Fix the cause and retry."
        } else {
            "Earlier statements in this migration may have been applied; uvg_version was not changed. Fix the target manually if needed, then retry or use `uvg stamp` after verification."
        };
        return Err(anyhow!(
            "uvg: migration {} failed in {} section of {} at statement {}/{}: {}\n{}\n--- SQL ---\n{}",
            migration.revision,
            section.label(),
            migration.path.display(),
            applied + 1,
            results.len(),
            failed.error.as_deref().unwrap_or(""),
            aftermath,
            failed.sql
        ));
    }
    Ok(())
}
