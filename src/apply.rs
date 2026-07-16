//! Guarded DDL application shared by the CLI and interactive TUI.
//!
//! Every apply path validates generated advisory markers before opening a
//! target connection, optionally runs the dialect's non-committing parse
//! probe, and passes the caller-selected retry budget to the executor.

use std::fs;
use std::path::Path;

use anyhow::Result;

use crate::apply_progress::{self, ApplyStats};
use crate::connection::ConnectionConfig;
use crate::db::{self, StmtResult};
use crate::dialect::Dialect;
use crate::output::{apply_order, Manifest};

/// Runtime behavior for a guarded DDL apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyOptions {
    /// Run the dialect's non-committing parse/catalog probe first.
    pub parse_check: bool,
    /// Retry budget for transient failures on each statement.
    pub max_retries: u8,
    /// Emit headless per-statement progress lines to stderr.
    pub progress_enabled: bool,
}

impl Default for ApplyOptions {
    fn default() -> Self {
        Self {
            parse_check: true,
            max_retries: 3,
            progress_enabled: false,
        }
    }
}

impl ApplyOptions {
    pub const fn new(parse_check: bool, max_retries: u8, progress_enabled: bool) -> Self {
        Self {
            parse_check,
            max_retries,
            progress_enabled,
        }
    }

    const fn without_parse_check(self) -> Self {
        Self {
            parse_check: false,
            ..self
        }
    }
}

/// Outcome of the optional pre-apply parse/catalog probe.
///
/// This is returned to the caller instead of being printed by shared code so
/// terminal UIs can render the notice without corrupting their alternate
/// screen. Headless callers remain responsible for writing the notice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseCheckStatus {
    NotRequested,
    Passed,
    SkippedUnsupported,
}

impl ParseCheckStatus {
    /// Informational text callers should surface when the probe was skipped.
    pub const fn notice(self) -> Option<&'static str> {
        match self {
            Self::SkippedUnsupported => Some(
                "parse-check skipped (no parse-only mode for this dialect; pass --no-parse-check to silence)",
            ),
            Self::NotRequested | Self::Passed => None,
        }
    }
}

/// Statement outcomes plus caller-renderable safety information.
#[derive(Debug)]
pub struct ApplyReport {
    pub statements: Vec<StmtResult>,
    pub parse_check: ParseCheckStatus,
}

/// Advisory comments emitted when a schema change cannot be represented as
/// executable SQL. Sending a blob containing one of these markers would risk
/// silently applying only the executable subset and leaving a partial schema.
const UNAPPLIABLE_MARKERS: &[&str] = &[
    "-- UVG-BLOCKED:",
    "-- WARNING: SQLite does not support ALTER COLUMN",
    "-- WARNING: SQLite cannot drop constraint",
    "-- NOTE: MSSQL requires dropping the named default constraint",
    "-- DROPPED CHECK ",
];

/// Validate that generated output is safe to hand to the statement executor.
///
/// This is deliberately performed before parse-check or connection setup so a
/// known partial-migration blob cannot touch the target at all.
pub fn validate_apply_blob(sql: &str, source_label: &str, dialect: Dialect) -> Result<()> {
    if let Some(marker) = UNAPPLIABLE_MARKERS
        .iter()
        .find(|marker| sql.contains(*marker))
    {
        return Err(anyhow::anyhow!(
            "refusing to apply ({source_label}): contains an instruction uvg cannot execute on its own:\n  {marker}\n\
             Inspect the full diff with `--outfile` or `--out-dir` and apply the actionable parts \
             manually so the target doesn't end up partially migrated."
        ));
    }

    let statements = db::split_statements(sql, dialect);
    if statements.is_empty() {
        let trimmed = sql.trim();
        let is_noop_sentinel =
            trimmed.is_empty() || trimmed.starts_with("-- No schema changes detected");
        if !is_noop_sentinel {
            return Err(anyhow::anyhow!(
                "refusing to apply ({source_label}): produced changes but they're all non-executable text. \
                 Inspect with `--outfile` or `--out-dir` and apply the actionable parts by hand."
            ));
        }
    }
    Ok(())
}

/// Run the per-dialect parse-check probe.
///
/// All statement errors are reported together. Dialects without a safe
/// parse-only mode return a caller-renderable status and continue to real
/// execution.
pub async fn run_parse_check(config: &ConnectionConfig, content: &str) -> Result<ParseCheckStatus> {
    if !db::supports_parse_check(config) {
        return Ok(ParseCheckStatus::SkippedUnsupported);
    }
    let errors = db::parse_check_ddl(config, content).await?;
    if errors.is_empty() {
        return Ok(ParseCheckStatus::Passed);
    }

    let mut msg = format!(
        "uvg: parse-check found {} error(s) before applying — fix and retry, or pass --no-parse-check to skip:\n",
        errors.len()
    );
    for (i, error) in errors.iter().enumerate() {
        let collapsed: String = error.sql.split_whitespace().collect::<Vec<_>>().join(" ");
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
            error.error,
        ));
    }
    Err(anyhow::anyhow!(msg))
}

/// Apply one SQL blob through the shared safety pipeline.
///
/// Statement-level database failures are returned in the report so
/// callers can render them in their own UI. Validation, parse-check, and
/// connection-level failures are returned as `Err` before execution begins.
pub async fn apply_sql(
    config: &ConnectionConfig,
    content: &str,
    source_label: &str,
    options: ApplyOptions,
) -> Result<ApplyReport> {
    validate_apply_blob(content, source_label, config.dialect())?;
    let parse_check = if options.parse_check {
        run_parse_check(config, content).await?
    } else {
        ParseCheckStatus::NotRequested
    };
    let statements = execute_validated(config, content, options).await?;
    Ok(ApplyReport {
        statements,
        parse_check,
    })
}

async fn execute_validated(
    config: &ConnectionConfig,
    content: &str,
    options: ApplyOptions,
) -> Result<Vec<StmtResult>> {
    db::execute_ddl(
        config,
        content,
        options.max_retries,
        |result, index, total| {
            if options.progress_enabled {
                apply_progress::print_progress(result, index, total);
            }
        },
    )
    .await
}

/// Apply a freshly rendered single SQL blob and emit CLI-oriented summaries.
pub async fn apply_inline(
    config: &ConnectionConfig,
    content: &str,
    target_url: &str,
    options: ApplyOptions,
) -> Result<()> {
    let report = apply_sql(config, content, "inline ddl", options).await?;
    emit_parse_check_notice(report.parse_check);
    let results = report.statements;
    if results.is_empty() {
        eprintln!("uvg: no schema changes");
        return Ok(());
    }

    let label = redact_target_url(target_url);
    let applied = results
        .iter()
        .take_while(|result| result.error.is_none())
        .count();
    if let Some(failed) = results.iter().find(|result| result.error.is_some()) {
        return Err(anyhow::anyhow!(
            "uvg: apply failed on statement {}/{} against {}: {}{}\n--- SQL ---\n{}",
            applied + 1,
            results.len(),
            label,
            failed.error.as_deref().unwrap_or(""),
            apply_failure_note(&results, 0),
            failed.sql,
        ));
    }

    eprintln!("uvg: applied {} statement(s) to {}", applied, label);
    if options.progress_enabled {
        eprintln!("{}", render_stats(&results));
    }
    Ok(())
}

/// Apply a manifest's files in dependency-safe order and emit CLI summaries.
pub async fn apply_manifest(
    config: &ConnectionConfig,
    manifest: &Manifest,
    out_dir: &Path,
    target_url: &str,
    options: ApplyOptions,
) -> Result<()> {
    let paths = apply_order(manifest, out_dir);
    let contents = paths
        .iter()
        .map(|path| fs::read_to_string(path).map(|content| (path.clone(), content)))
        .collect::<std::io::Result<Vec<_>>>()?;
    for (path, content) in &contents {
        validate_apply_blob(content, &path.display().to_string(), config.dialect())?;
    }

    if options.parse_check {
        // Parse-check one combined batch so later files can reference objects
        // created by earlier manifest entries inside the probe transaction.
        let combined = contents
            .iter()
            .map(|(_, content)| format!("{content}\n"))
            .collect::<String>();
        let status = run_parse_check(config, &combined).await?;
        emit_parse_check_notice(status);
    }

    let mut total_applied = 0usize;
    let mut all_results = Vec::new();
    for (path, content) in &contents {
        // Validation is intentionally repeated at the shared entry point. It
        // is cheap and keeps every execution call independently guarded.
        let report = apply_sql(
            config,
            content,
            &path.display().to_string(),
            options.without_parse_check(),
        )
        .await?;
        let results = report.statements;
        let applied_here = results
            .iter()
            .take_while(|result| result.error.is_none())
            .count();
        if let Some(failed) = results.iter().find(|result| result.error.is_some()) {
            return Err(anyhow::anyhow!(
                "uvg: apply failed in {} (statement {}/{}): {}{}\n--- SQL ---\n{}",
                path.display(),
                applied_here + 1,
                results.len(),
                failed.error.as_deref().unwrap_or(""),
                apply_failure_note(&results, total_applied),
                failed.sql,
            ));
        }
        total_applied += applied_here;
        all_results.extend(results);
    }

    eprintln!(
        "uvg: applied {} statement(s) across {} file(s) to {}",
        total_applied,
        paths.len(),
        redact_target_url(target_url),
    );
    if options.progress_enabled {
        eprintln!("{}", render_stats(&all_results));
    }
    Ok(())
}

fn render_stats(results: &[StmtResult]) -> String {
    let mut stats = ApplyStats::new();
    for result in results {
        stats.record(result);
    }
    stats.render_summary()
}

fn emit_parse_check_notice(status: ParseCheckStatus) {
    if let Some(notice) = status.notice() {
        eprintln!("uvg: {notice}");
    }
}

/// Trailing note describing what may have persisted after an apply failure.
///
/// `previously_applied` is the number of successful statements from earlier
/// manifest files. A `rolled_back` result confirms only the current database
/// transaction was undone; earlier files have already committed.
pub fn apply_failure_note(results: &[StmtResult], previously_applied: usize) -> &'static str {
    if results.iter().any(|result| result.rolled_back) {
        if previously_applied > 0 {
            "\n(this file's transaction was rolled back, but earlier files in this run were already applied — the target is partially migrated; verify before retrying)"
        } else {
            "\n(the apply ran in a transaction and was rolled back; the target is unchanged)"
        }
    } else {
        "\n(the apply was nontransactional or its rollback could not be confirmed; earlier successful statements or files, if any, may have persisted, so the target may be partially migrated; verify before retrying)"
    }
}

/// Backward-compatible name retained for callers that only apply one SQL blob.
pub fn apply_rollback_note(results: &[StmtResult]) -> &'static str {
    apply_failure_note(results, 0)
}

/// Strip credentials from a connection URL before printing it.
pub fn redact_target_url(raw: &str) -> String {
    crate::redaction::redact_connection_url(raw)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn stmt(error: Option<&str>, rolled_back: bool) -> StmtResult {
        StmtResult {
            sql: "SELECT 1".to_string(),
            error: error.map(str::to_string),
            duration: Duration::from_millis(1),
            rolled_back,
        }
    }

    #[test]
    fn default_options_enable_safety_and_retries() {
        let options = ApplyOptions::default();
        assert!(options.parse_check);
        assert_eq!(options.max_retries, 3);
        assert!(!options.progress_enabled);
    }

    #[test]
    fn failure_note_distinguishes_confirmed_rollback_from_partial_apply_risk() {
        let rolled_back = apply_failure_note(&[stmt(None, true), stmt(Some("boom"), true)], 0);
        assert!(rolled_back.contains("rolled back"));
        assert!(rolled_back.contains("target is unchanged"));

        let unconfirmed = apply_failure_note(&[stmt(None, false), stmt(Some("boom"), false)], 0);
        assert!(unconfirmed.contains("nontransactional"));
        assert!(unconfirmed.contains("may have persisted"));
        assert!(unconfirmed.contains("partially migrated"));
    }

    #[test]
    fn failure_note_discloses_prior_manifest_files_after_current_rollback() {
        let note = apply_failure_note(&[stmt(Some("boom"), true)], 2);
        assert!(note.contains("this file's transaction was rolled back"));
        assert!(note.contains("earlier files"));
        assert!(note.contains("partially migrated"));
    }

    #[tokio::test]
    async fn unsupported_parse_check_is_reported_without_printing() {
        let config = ConnectionConfig::Sqlite("sqlite::memory:".to_string());
        let status = run_parse_check(&config, "CREATE TABLE t(id INTEGER);")
            .await
            .unwrap();
        assert_eq!(status, ParseCheckStatus::SkippedUnsupported);
        assert!(status.notice().unwrap().contains("parse-check skipped"));
    }

    #[test]
    fn validation_allows_executable_sql_and_noop_sentinel() {
        validate_apply_blob(
            "CREATE TABLE users(id INTEGER PRIMARY KEY);",
            "test",
            Dialect::Postgres,
        )
        .unwrap();
        validate_apply_blob("-- No schema changes detected.", "test", Dialect::Postgres).unwrap();
    }

    #[test]
    fn validation_rejects_each_unappliable_marker() {
        for marker in UNAPPLIABLE_MARKERS {
            let sql = format!("ALTER TABLE users ADD COLUMN x int;\n{marker} details");
            let error = validate_apply_blob(&sql, "test", Dialect::Postgres)
                .unwrap_err()
                .to_string();
            assert!(error.contains("refusing to apply"), "{error}");
            assert!(error.contains(*marker), "{error}");
        }
    }

    #[test]
    fn validation_rejects_comment_only_diff() {
        let error = validate_apply_blob("-- manual follow-up required", "test", Dialect::Postgres)
            .unwrap_err()
            .to_string();
        assert!(error.contains("non-executable text"));
    }

    #[tokio::test]
    async fn marker_validation_happens_before_target_connection() {
        let config = ConnectionConfig::Sqlite("sqlite:/definitely/missing/target.db".to_string());
        let error = apply_sql(
            &config,
            "-- WARNING: SQLite does not support ALTER COLUMN. Table recreation required.",
            "interactive ddl",
            ApplyOptions::default(),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("refusing to apply"), "{error}");
        assert!(
            !error.contains("database"),
            "validation should fail first: {error}"
        );
    }
}
