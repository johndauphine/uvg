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

## v1.6.0 - 2026-07-08

### Added

- `uvg init` now scaffolds a ready-to-edit `profiles.yaml` at the user
  profiles path that `--profile` actually reads (`$XDG_CONFIG_HOME/uvg/`,
  `~/.config/uvg/`, or `%APPDATA%\uvg\`), replacing the previous inert
  `uvg.toml`; `--config` overrides the scaffold path, and existing files are
  left untouched. (#112)
- Documented the maintainer release process, Semantic Versioning expectations,
  artifact checksum policy, crates.io publishing process, and rollback plan.
- Added a beta/RC real-schema validation runbook and helper script for
  collecting repeatable validation bundles before a stable release.
- Added a StackOverflow2010 schema-drift fixture, local harness, and an
  on-demand (manual `workflow_dispatch`) GitHub Actions workflow for SQL Server
  source drift into SQL Server, PostgreSQL, MySQL, and SQLite targets.

### Changed

- PostgreSQL applies (`--apply`, per `--out-dir` file, and each migration
  section) now run inside a single transaction: any failure rolls the batch
  back and leaves the target unchanged, and transient serialization/deadlock
  aborts retry the whole transaction. Statements PostgreSQL cannot run inside
  a transaction block (`CREATE INDEX CONCURRENTLY`, `VACUUM`, `CLUSTER`,
  `REINDEX DATABASE`, enum add-then-use, ...) are detected at runtime and the
  batch automatically falls back to the previous statement-by-statement
  behavior. Migrations containing their own transaction-control statements
  (`COMMIT`, `BEGIN`, `SAVEPOINT`, ...) are refused up front on PostgreSQL
  rather than silently subverting the transaction. Apply failure messages now
  state whether the target was rolled back, partially migrated, or in an
  unknown state after a commit-time connection failure. (#109)
- The `uvg_version` bump is now a transactional DELETE+INSERT on every
  backend, so a crash mid-bump can no longer leave the version table empty
  (previously read back as "base"). (#109)

### Fixed

- The DDL statement splitter used by apply and parse-check now tracks the
  target dialect's quoted identifiers (`"..."` with `""`, `` `...` `` with
  ``` `` ```, `[...]` with `]]`) and `/* ... */` block comments, so
  identifiers legally containing `;`, `'`, or `--` can no longer fracture a
  statement mid-way and execute garbage. PostgreSQL array syntax
  (`integer[]`, `ARRAY[[1,2],[3,4]]`) is unaffected by bracket handling. (#110)
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

### Migration output

- Down-migration generation is driven by the diff engine's structural change
  kinds instead of prefix-matching rendered SQL. Reversing an `ADD COLUMN`
  now carries a `-- WARNING: destructive operation` marker — the generated
  DOWN drops data written to the column since the upgrade — while remaining
  applicable rather than being refused. (#111)
- DDL diffs now compare same-named constraints by content instead of assuming
  a name match means no drift: a CHECK predicate edited in place (same
  dialect), a UNIQUE re-pointed at different columns, a foreign key
  retargeted at another table, or changed FK update/delete rules now emit a
  drop and re-add. MySQL foreign-key replacements also drop the stale InnoDB
  backing index when it is safe to do so, and honor `noindexes`. Cross-dialect
  CHECK predicate text and MySQL's `RESTRICT`/`NO ACTION` spellings are
  deliberately not treated as drift because they can never converge. **This
  can add statements to diffs for existing inputs** where constraint drift
  was previously invisible. (#113)

### Release

- Internal type-system unification: the SQLAlchemy typemap now consumes the
  canonical DDL type layer, so each dialect's raw type parsing is written
  once (#114). Generated Python and DDL output verified byte-identical.
- Codegen restructured — generator trait removed, Python file-splitting now
  derives from generator structure instead of re-parsing rendered text, and
  rendering primitives moved to a layer shared with the diff engine (#116).
  Output verified byte-identical.
- Per-dialect capabilities (boolean literals, DROP INDEX form, native enums,
  `COMMENT ON`, parse-check, constraint alteration, InnoDB FK semantics,
  MySQL database-as-schema) centralized as exhaustive-match methods on
  `Dialect`, locked by a capability-table test (#115).
