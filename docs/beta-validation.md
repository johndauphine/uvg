# Beta/RC Real-Schema Validation

This is the production-readiness gate for validating UVG against authorized
real schemas before a stable release. The CRM matrix is valuable synthetic
coverage, but it does not close this gate by itself.

## Current Status

As of 2026-05-20, this gate has one completed real-schema baseline pass and a
repeatable schema-drift matrix: the public StackOverflow2010 SQL Server schema
migrated to SQL Server, PostgreSQL, MySQL, and SQLite targets, then evolved
through additive, column-change, constraint/index, and destructive drift packs
until each applicable target converged after each pack. Stable release readiness
still needs at least one additional independent schema source, or an explicit
release-note caveat explaining the narrower validation scope. Do not mark the
gate complete until the result table below contains the required runs and any
discovered blockers have an issue, fix, or explicit release-note warning.

## Schema Source Requirements

Each source should include:

- Engine and version, such as PostgreSQL 16, MySQL 8.4, MariaDB 10.11, SQL
  Server 2022, or SQLite 3.x.
- Source form: sanitized dump, disposable URL, local SQLite file, or generated
  snapshot from a private environment.
- Allowed artifact handling: whether generated Python, SQL, logs, and snapshot
  YAML can be committed, attached privately, summarized only, or deleted after
  review.
- Workflows to exercise: SQLAlchemy generation, DDL generation, diff/apply,
  versioned migrations, or a narrower subset.
- Known edge cases to watch, such as non-default schemas, views, generated
  columns, domains/enums, odd defaults, composite keys, partitioning, or legacy
  identifier names.

Treat connection URLs and validation bundles as sensitive. UVG does not read
table data, but generated artifacts can include schema comments, identifiers,
defaults, constraints, indexes, and type names.

## Running A Validation Pass

Build the release binary first so validation exercises the same path users will
run:

```bash
cargo build --release
```

For source-only validation:

```bash
scripts/beta_validate_schema.sh \
  --label app-postgres \
  --source "$SOURCE_URL" \
  --schemas public \
  --target-dialect postgres \
  --snapshot
```

For diff/apply validation, use a disposable target database. The target may be
empty or a clone that should converge to the source. Never point `--apply` at a
database that is not safe to mutate:

```bash
scripts/beta_validate_schema.sh \
  --label app-postgres-apply \
  --source "$SOURCE_URL" \
  --target "$DISPOSABLE_TARGET_URL" \
  --apply
```

For the versioned migration workflow, provide a separate disposable target so
the direct apply test and migration test do not step on each other:

```bash
scripts/beta_validate_schema.sh \
  --label app-postgres-full \
  --source "$SOURCE_URL" \
  --target "$DISPOSABLE_TARGET_URL" \
  --apply \
  --migration-target "$SECOND_DISPOSABLE_TARGET_URL"
```

The script writes a timestamped bundle under `/tmp/uvg-beta-validation/` unless
`--out` or `UVG_VALIDATION_OUT` is set. Each bundle includes:

- `summary.md`: human-readable pass/fail summary.
- `manifest.tsv`: step status, duration, artifact path, and log path.
- `declarative.py` and `tables.py`: generated SQLAlchemy output.
- `source.sql`: generated DDL for the requested or inferred target dialect.
- `diff.sql`, `direct_apply.sql`, and `post_apply_diff.sql` when a target is
  provided.
- `post-apply-convergence` and `post-migration-convergence` manifest checks
  that fail unless the relevant post-run diff is empty.
- `migrations/`, `migration_current.txt`, and `post_migration_diff.sql` when
  `--migration-target` is provided. The post-migration convergence check
  excludes UVG's own `uvg_version` bookkeeping table.
- `source.snapshot.yaml` only when `--snapshot` is requested.

`--schemas` is a single UVG option and applies to both source and target
introspection. For default-schema cross-dialect runs, such as SQL Server `dbo`
to PostgreSQL `public`, omit `--schemas` and let each dialect use its default.

## StackOverflow2010 Drift Harness

The repeatable StackOverflow2010 drift fixture lives under
`testdata/stackoverflow-drift/`. It resets a disposable SQL Server source clone
and target databases for SQL Server, PostgreSQL, MySQL, and SQLite, bootstraps
the baseline schema, applies staged SQL Server drift packs, runs `uvg --apply`,
requires an empty post-apply diff, and performs direct target catalog checks for
the expected objects. SQLite skips drift packs that require table rebuilds for
`ALTER COLUMN` or standalone constraint changes.

```bash
cargo build --release
testdata/stackoverflow-drift/run_drift.sh
```

The nightly GitHub Actions workflow `.github/workflows/stackoverflow-drift.yml`
restores the source database from the repository secret
`STACKOVERFLOW2010_BAK_URL`. That secret should point to a downloadable SQL
Server `.bak` file authorized for CI use.

## Go/No-Go Criteria

A stable release can proceed only when:

- At least two independent production-derived or externally maintained schemas
  have completed validation, and every dialect materially changed by the release
  is represented by either a real-schema run or an explicit release-note caveat.
- SQLAlchemy declarative generation, SQLAlchemy `Table()` generation, and
  source DDL generation complete without panic or invalid output for every
  selected schema.
- For every disposable target included in the pass, diff generation succeeds;
  if apply is in scope, apply succeeds and the post-apply diff is empty or only
  contains explicitly accepted non-convergent statements.
- If versioned migrations are in scope, `revision`, `upgrade`, `current`, and
  post-upgrade diff complete successfully on a disposable target, ignoring only
  UVG's own `uvg_version` bookkeeping table.
- Every production-blocking issue found during validation is fixed before the
  stable release, or is explicitly deferred with a release-note warning and a
  tracking issue.
- Every fixed production issue has focused regression coverage, preferably
  reduced to `src/testutil.rs` builders or a small integration fixture rather
  than a private schema dump.
- Validation results are summarized in this document or the release-prep pull
  request, with private artifact locations noted when artifacts cannot be
  shared publicly.

## Result Log

| Date | Label | Engine/version | Source type | Workflows | Result | Follow-up |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-05-20 | stackoverflow2010-mssql-to-postgres-default-schemas | SQL Server 2022 source to PostgreSQL 16.13 target | Public StackOverflow2010 database restored locally | Declarative, `Table()`, source DDL, snapshot, diff/apply, post-apply convergence, versioned migration, post-migration convergence | Pass | Bundle: `/tmp/uvg-beta-validation/20260520T095432Z_stackoverflow2010-mssql-to-postgres-default-schemas`; targets: `uvg_so2010_target`, `uvg_so2010_migration`. Need at least one additional independent schema before closing #86. |
| 2026-05-20 | stackoverflow2010-mssql-to-postgres-delta | SQL Server 2022 source clone to PostgreSQL 16.13 target | Public StackOverflow2010 schema cloned to disposable SQL Server and mutated with additive table, column, constraint, and index changes | Diff/apply from changed SQL Server source into existing PostgreSQL target, direct catalog verification, post-apply convergence | Pass | Delta artifacts: `/tmp/uvg-so2010-delta/delta_fixed.sql`, `/tmp/uvg-so2010-delta/delta_apply_fixed.sql`, `/tmp/uvg-so2010-delta/post_delta_diff_final.sql`; target `uvg_so2010_delta_target` converged with `-- No schema changes detected.` Fixed blockers found during this run: unsafe MSSQL parse-check handling, MSSQL `SYSUTCDATETIME()` default translation, MSSQL Unicode CHECK literal translation, and added constraints/indexes on existing-table diffs. This strengthens the StackOverflow2010 result but does not count as an independent second schema. |
| 2026-05-20 | stackoverflow2010-drift-matrix | SQL Server 2022 source clone to SQL Server 2022, PostgreSQL 16.13, MySQL 8.0, and SQLite targets | Public StackOverflow2010 schema cloned to disposable SQL Server and evolved through committed drift packs | Baseline convergence, additive drift, column evolution, added constraints/indexes, destructive table/column/index drift, dropped constraints where supported, direct catalog checks after every applicable pack | Pass | Bundle: `/tmp/uvg-stackoverflow-drift-full/20260520T104328Z`; workflow: `.github/workflows/stackoverflow-drift.yml`. SQLite skips table-rebuild-required packs. This is now the repeatable nightly drift gate once `STACKOVERFLOW2010_BAK_URL` is configured. |
| 2026-05-20 | pending-additional-schema | TBD | Awaiting another authorized schema source | TBD | Blocked | Required to satisfy the stable-release gate's independent-schema criterion. |

## Issue Triage

When a validation run fails:

1. Keep the raw bundle private unless the source owner approved sharing it.
2. Reduce the failure to the smallest schema fragment that reproduces it.
3. Add or update a focused regression test before fixing the bug.
4. Link the fix, regression test, and any remaining release-note warning from
   the result table above.
