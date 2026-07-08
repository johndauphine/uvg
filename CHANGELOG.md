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
- Added a StackOverflow2010 schema-drift fixture, local harness, and nightly
  GitHub Actions workflow for SQL Server source drift into SQL Server,
  PostgreSQL, MySQL, and SQLite targets.

### Fixed

- Skip SQL Server parse checks in UVG's apply path; only PostgreSQL now uses
  the pre-apply parse-check phase.
- Translate SQL Server `SYSUTCDATETIME()` defaults and `N'...'` CHECK literals
  when generating cross-dialect PostgreSQL DDL.
- Include newly added constraints and indexes on existing tables in DDL diffs,
  emit dropped constraints and indexes, and avoid duplicate primary-key diffs
  when only the target constraint name differs.
- Ignore target-owned foreign-key backing indexes and SQLite's constraint-name
  churn in drift diffs to avoid false positive schema drift.
