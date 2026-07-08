# Changelog

All notable user-facing changes to UVg are documented here.

This project follows [Semantic Versioning](docs/release.md#versioning-policy).
When cutting a release, move the relevant entries from `Unreleased` into a
dated `vX.Y.Z - YYYY-MM-DD` section and keep any empty headings out of the
released section.

Suggested headings:

- `Breaking changes`
- `Added`
- `Changed`
- `Fixed`
- `Generated output`
- `Migration output`
- `Release`

## Unreleased

### Added

- Documented the maintainer release process, Semantic Versioning expectations,
  artifact checksum policy, crates.io publishing process, and rollback plan.
- Added a beta/RC real-schema validation runbook and helper script for
  collecting repeatable validation bundles before a stable release.
- Added a StackOverflow2010 schema-drift fixture, local harness, and an
  on-demand (manual `workflow_dispatch`) GitHub Actions workflow for SQL Server
  source drift into SQL Server, PostgreSQL, MySQL, and SQLite targets.

### Fixed

- Skip SQL Server parse checks in UVG's apply path; only PostgreSQL now uses
  the pre-apply parse-check phase.
- Translate SQL Server `SYSUTCDATETIME()` defaults and `N'...'` CHECK literals
  when generating cross-dialect PostgreSQL DDL.
- Include newly added constraints and indexes on existing tables in DDL diffs,
  emit dropped constraints and indexes, and avoid duplicate primary-key diffs
  when only the target constraint name differs.
- Ignore target-owned foreign-key backing indexes (MySQL targets only, where
  InnoDB auto-creates them) and SQLite's constraint-name churn in drift diffs
  to avoid false positive schema drift; user-created indexes on foreign-key
  columns still participate in drift on PostgreSQL and SQL Server targets.
- Quote translated SQL Server CHECK identifiers with backticks on MySQL
  targets. MySQL's default SQL mode reads double-quoted identifiers as string
  literals, which made translated constraints validate nothing.
- Preserve SQL Server-native now-family defaults (`SYSUTCDATETIME()`,
  `GETUTCDATE()`, `SYSDATETIME()`, `SYSDATETIMEOFFSET()`) on SQL Server
  targets instead of collapsing them to `GETDATE()`, which silently changed
  UTC defaults to server-local time in same-dialect migrations.
- Emit target-side constraint and index drops before column drops in DDL
  diffs. SQL Server rejects dropping a column while a dependent index or
  constraint exists, so destructive diffs could fail at apply time.
