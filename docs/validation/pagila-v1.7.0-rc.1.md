# Pagila Validation: v1.7.0-rc.1

## Decision

**GO for the v1.7.0 release-candidate line.** The independently maintained
[Pagila](https://github.com/devrimgunduz/pagila) PostgreSQL schema completed all
18 selected validation steps with zero failures and zero skips. Direct apply
and versioned migration each converged to `-- No schema changes detected.` on
separate empty targets.

This result validates UVg's supported table-oriented PostgreSQL surface. It is
not a claim that UVg reproduces every PostgreSQL object class; the observed
omissions are listed below and tracked in
[#128](https://github.com/johndauphine/uvg/issues/128).

## Candidate and Environment

| Item | Value |
| --- | --- |
| UVg version | `1.7.0-rc.1` |
| UVg candidate commit | `837c7a305ebdd8693864a0e4ed36482f4a89c44d` |
| UVg worktree | Clean |
| Pagila commit | `5ba5a57aeb159f75f02aca2432d3c262186d13d3` |
| Source | PostgreSQL 16.14, Pagila `public` schema |
| Targets | Two separate empty PostgreSQL 16.14 databases |
| Generated-code runtime | Python with SQLAlchemy 2.0.51 |
| Evidence bundle | `/tmp/uvg-beta-validation/20260716T220557Z_pagila-postgres-v1.7.0-rc.1-final` |

The validation helper did not persist connection URLs. The local evidence
bundle contains public schema metadata but remains outside the repository.

## Workflow Result

| Workflow | Result |
| --- | --- |
| Declarative generation, syntax check, import, and mapper configuration | Pass |
| SQLAlchemy `Table()` generation, syntax check, and import | Pass |
| PostgreSQL source DDL generation | Pass |
| Source snapshot | Pass |
| Empty-target diff and guarded direct apply | Pass |
| Post-apply convergence | Pass |
| Migration project initialization and revision generation | Pass |
| Migration upgrade and current-revision reporting | Pass |
| Post-migration convergence | Pass |
| Manifest artifact-existence check | Pass |

Both convergence artifacts contain only:

```sql
-- No schema changes detected.
```

## Catalog Assertions

The supported catalog assertions matched on the source, direct-apply target,
and migration target. The migration target's UVg bookkeeping table was
excluded from schema counts.

| Assertion | Source | Direct apply | Migration |
| --- | ---: | ---: | ---: |
| Tables | 22 | 22 | 22 |
| Constraints | 58 | 58 | 58 |
| `mpaa_rating` labels | `G, PG, PG-13, R, NC-17` | Same | Same |
| `film.fulltext` type | `tsvector` | `tsvector` | `tsvector` |
| `film_fulltext_idx` method | GiST | GiST | GiST |
| Columns using `payment_payment_id_seq` | 8 | 8 | 8 |
| Shared `payment_payment_id_seq` objects | 1 | 1 | 1 |

## Defects Found and Resolved

The Pagila pass produced focused regressions and fixes for:

- PostgreSQL `TSVECTOR` imports and native enum rendering in generated Python.
- Positional SQLAlchemy `Sequence()` arguments and singleton
  `__table_args__` tuple syntax.
- PostgreSQL enum creation, schema identity, arrays, safe label-drift blocking,
  and reversible qualified names.
- Shared PostgreSQL sequence identity across partition-related payment tables.
- PostgreSQL non-btree index introspection, rendering, and diff comparison.
- Generated baseline and merge no-op migration handling under apply preflight.
- Apply preflight for explicit blockers, including mixed hook/SQL plans.
- Validation-helper metadata, generated-code import checks, convergence checks,
  and current-revision artifact capture.

The complete Rust suite, formatting, Clippy, dependency policy, release build,
package verification, and crates.io publish dry run also passed. Cargo reported
the allowed transitive `spin 0.9.8` yank and duplicate-version warnings;
`cargo deny` completed with advisories, bans, licenses, and sources all OK.

## Explicitly Deferred PostgreSQL Metadata

Pagila contains metadata outside UVg's current table-oriented DDL scope:

| Object class | Source | Reproduced |
| --- | ---: | ---: |
| Domains | 2 | 0 |
| Partitioned table declarations | 1 | 0 |
| Partition attachments | 14 | 0 |
| Views | 8 | 0 |
| Routines | 10 | 0 |
| User triggers | 15 | 0 |

Standalone sequence ownership/options and cross-selected-schema enum discovery
also remain deferred. These limitations are release-note caveats, not silent
claims of full PostgreSQL round-trip fidelity, and are tracked in #128.
