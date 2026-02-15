# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

UVg is a Rust reimplementation of Python's [sqlacodegen](https://github.com/agronholm/sqlacodegen). It connects to PostgreSQL or Microsoft SQL Server databases, introspects their schema, and generates SQLAlchemy Python model code. The goal is drop-in CLI compatibility with sqlacodegen — same flags, same output format.

## Build & Test Commands

```bash
cargo build                    # Debug build
cargo build --release          # Release build
cargo install --path .         # Install binary locally
cargo test                     # Run all unit tests
cargo test test_name           # Run a single test by name/pattern
cargo test --test integration -- --ignored  # Integration tests (needs DATABASE_URL)
cargo test -- --nocapture      # Show println output during tests
```

Integration tests require a live database via `DATABASE_URL`:
```bash
DATABASE_URL=postgresql://user:pass@localhost/testdb cargo test --test integration -- --ignored
DATABASE_URL=mssql://user:pass@localhost/testdb cargo test --test integration -- --ignored
```

Enable debug logging with `RUST_LOG=debug`.

## Architecture

The application follows a pipeline: **CLI parsing → Connection → Introspection → Code Generation → Output**.

### Key modules (`src/`)

- **`cli.rs`** — Clap-based CLI parsing. Produces `ConnectionConfig` (Postgres URL string or MSSQL connection fields) and `GeneratorOptions`.
- **`dialect.rs`** — `Dialect` enum (Postgres, Mssql) for runtime dispatch based on URL scheme.
- **`schema.rs`** — Data structures representing introspected schema: `IntrospectedSchema`, `TableInfo`, `ColumnInfo`, `ConstraintInfo`, `IndexInfo`.
- **`introspect/pg/`** — PostgreSQL introspection via sqlx. Queries `information_schema` and `pg_catalog` across submodules: `tables.rs`, `columns.rs`, `constraints.rs`, `indexes.rs`.
- **`introspect/mssql/`** — MSSQL introspection via Tiberius. Same submodule structure as PG.
- **`typemap/pg.rs`**, **`typemap/mssql.rs`** — Map database column types to SQLAlchemy type expressions, Python type annotations, and import requirements. Returns `MappedType`.
- **`codegen/`** — `Generator` trait with two implementations:
  - `declarative.rs` — Generates modern SQLAlchemy ORM classes with `Mapped[]` type annotations.
  - `tables.rs` — Generates `Table()` metadata objects.
  - `imports.rs` — `ImportCollector` that accumulates, deduplicates, and renders import statements in a specific group order (typing → stdlib → sqlalchemy core → dialects → orm).
- **`naming.rs`** — Table/column name transformations (e.g., snake_case to UpperCamelCase via `heck`).
- **`error.rs`** — `UvgError` enum using `thiserror`.

### Testing patterns

- Unit tests are inline (`#[cfg(test)] mod tests`) within source files.
- Snapshot tests use the `insta` crate with YAML format (`assert_snapshot!`).
- `src/testutil.rs` provides builder helpers for constructing test schema data.
- Integration tests in `tests/integration.rs` are marked `#[ignore]` and require a live database.

### Adding a new database dialect

1. Add an introspection module under `src/introspect/<dialect>/` with `tables.rs`, `columns.rs`, `constraints.rs`, `indexes.rs`.
2. Add a type mapper in `src/typemap/<dialect>.rs`.
3. Extend `Dialect` enum in `dialect.rs` and `ConnectionConfig` in `cli.rs`.
4. Add the connection/introspection branch in `main.rs`.
