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

## v1.7.0-rc.1 - 2026-07-16

### Added

- Added a reusable library surface for connection parsing and guarded DDL
  application. The CLI and interactive TUI now share marker validation,
  parse-check behavior, retry configuration, progress accounting, URL
  redaction, and rollback/partial-migration reporting. (#108)
- Added PostgreSQL sequence and index-method fidelity needed by the Pagila
  real-schema gate: shared `nextval()` sequences retain one identity across
  tables, and non-btree indexes retain their access method.
- Added repeatable generated-Python syntax/import checks and database/version
  metadata to the beta/RC validation helper without persisting connection
  URLs.

### Fixed

- Fixed PostgreSQL native enum creation in empty-target diffs and generated
  DOWN migrations for the new schema-scoped operation.
- Fixed generated SQLAlchemy output for PostgreSQL `TSVECTOR`, native enums in
  no-primary-key `Table()` fallbacks, positional `Sequence()` arguments, and
  singleton declarative `__table_args__` tuples. These defects were found by
  importing models generated from Pagila under SQLAlchemy 2.0.
- Preserve PostgreSQL GiST index declarations and detect same-named index drift
  in uniqueness, columns, or access method instead of comparing names alone.
- Windows release checksum files now use UTF-8 without a BOM and LF line
  endings, so the documented Unix `shasum -a 256 -c *.sha256` command can
  verify all platforms together. (#126)
- Credential-bearing connection and Anthropic configuration values no longer
  expose secrets through `Debug` formatting; the raw DDL executor is no longer
  part of the public library API.

### Generated output

- PostgreSQL-to-PostgreSQL DDL now emits one schema-scoped sequence plus exact
  `nextval()` defaults when multiple columns share a sequence. Ordinary
  single-owner auto-increment columns continue to use `SERIAL`/`BIGSERIAL`.
- PostgreSQL index DDL now includes `USING <method>` for non-btree access
  methods, and generated declarative singleton table arguments carry the
  required trailing comma.

### Migration output

- Empty-target PostgreSQL revisions create shared sequences and native enum
  types before dependent tables. Their generated DOWN sections reverse those
  schema-scoped creations after dependent tables are removed.

### Release

- Prerelease versions and promotion are documented, and hyphenated SemVer tags
  such as `v1.7.0-rc.1` are automatically marked as GitHub prereleases.
- Release-candidate validation now uses the independently maintained Pagila
  schema in addition to the StackOverflow2010 drift matrix. See
  `docs/beta-validation.md` for the recorded gate result.

### Known limitations

- PostgreSQL DDL generation does not yet preserve domain definitions,
  partition attachments/bounds, view definitions, routines, triggers, or
  standalone sequence ownership/options. Pagila validation covers the
  supported table, column, constraint, index, enum, generated-model, apply,
  and migration surfaces; the unsupported metadata remains explicitly
  deferred and must not be described as round-trip fidelity.

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
