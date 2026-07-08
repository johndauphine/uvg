# StackOverflow2010 drift fixture

This fixture validates real-schema evolution for the SQL Server
StackOverflow2010 database migrating into every supported target dialect:
SQL Server, PostgreSQL, MySQL, and SQLite. It is intentionally separate from
the CRM matrix: CRM is compact cross-dialect breadth, while this fixture is a
production-sized SQL Server schema with staged drift packs.

## What It Covers

For each target, the runner creates a disposable SQL Server source clone and a
disposable target:

- `UVG_SO2010_Drift_Source`: SQL Server schema clone generated from the
  pristine `StackOverflow2010` database.
- `UVG_SO2010_Drift_Target`: SQL Server target.
- `uvg_so2010_drift_pg`: PostgreSQL target.
- `uvg_so2010_drift_mysql`: MySQL target.
- bundle-local SQLite file target.

It then applies each SQL Server pack in order, runs `uvg --generator ddl
--target-dialect <target> --apply`, requires a clean post-apply diff, and
checks target catalogs directly.

| Pack | Coverage |
| --- | --- |
| `mssql/01_additive.sql` | Added table, identity PK, FKs, CHECK with `N'...'` literals, `SYSUTCDATETIME()` default, indexes, and added columns. |
| `mssql/02_column_evolution.sql` | Existing-column type/length changes, nullable to not-null, default added to an existing column, and altered column length on an added table. |
| `mssql/03_constraints_indexes.sql` | Added CHECKs, UNIQUEs, and composite index on existing tables. |
| `mssql/04_destructive.sql` | Dropped index, dropped column, and dropped table on disposable targets. |
| `mssql/05_drop_constraint.sql` | Dropped CHECK constraint on engines that support standalone constraint drops. |

SQLite participates in the matrix but skips packs that require table rebuilds
(`02_column_evolution.sql`, `03_constraints_indexes.sql`, and
`05_drop_constraint.sql`). It still runs baseline, additive, and destructive
table/column/index drift with strict convergence.

## Running Locally

Prerequisites:

- A SQL Server container with a restored `StackOverflow2010` database.
- A PostgreSQL container.
- A MySQL container.
- `sqlite3` on `PATH`.
- `cargo build --release`.
- `sqlcmd` on `PATH`, or the repo shim installed:

```bash
sudo install -m 0755 testdata/crm/sqlcmd-shim.sh /usr/local/bin/sqlcmd
```

Default container names match CI (`mssql-test`, `pg-test`, `mysql-test`).
Override them for a local bench environment:

```bash
MSSQL_CONTAINER=mssql-bench \
PG_CONTAINER=pg-bench \
MYSQL_CONTAINER=mysql-bench \
testdata/stackoverflow-drift/run_drift.sh \
  --source-db StackOverflow2010 \
  --work-source-db UVG_SO2010_Drift_Source
```

To run a subset while iterating:

```bash
testdata/stackoverflow-drift/run_drift.sh --targets postgres,sqlite
```

The runner writes bundles to `/tmp/uvg-stackoverflow-drift/<timestamp>/`,
including generated SQL, post-diff files, per-step logs, `results.tsv`, and
`summary.md`.

## Nightly CI

The nightly workflow restores `StackOverflow2010` from the repository secret
`STACKOVERFLOW2010_BAK_URL`, which must point to a SQL Server `.bak` file that
the CI runner may download. The restore step discovers logical data/log file
names with `RESTORE FILELISTONLY`, so common StackOverflow2010 backups do not
need repo-specific logical-name assumptions.

The workflow does not run on pull requests because the backup is large and
should remain controlled. Use `workflow_dispatch` for manual runs after changing
the drift harness or fixtures.
