# Philosophy

## Why rewrite sqlacodegen in Rust?

sqlacodegen is a Python tool that introspects databases and generates SQLAlchemy model code. It works well but carries the overhead of the Python runtime, pip dependency resolution, and virtual environments. UVg replaces it with a single compiled binary that produces identical output 10-40x faster.

The choice of Rust is pragmatic, not ideological. The problem -- read metadata from a database, transform it, emit text -- is I/O-bound and string-heavy. Rust's advantages here are startup time, distribution simplicity (one binary, no runtime), and the confidence that comes from the type system when wiring together multiple output-formatting rules that must interlock precisely.

## Output fidelity over elegance

The primary design constraint is **byte-for-byte compatibility** with sqlacodegen's output. This means:

- Import ordering, grouping, and blank-line placement must match exactly.
- Constraint ordering within `Table()` and `__table_args__` must match exactly.
- Quoting style (single vs double quotes) must match exactly.
- Whitespace, indentation, trailing newlines must match exactly.

This constraint is non-negotiable. When faced with a choice between "cleaner" code generation and matching sqlacodegen, always match sqlacodegen. The test suite enforces this through exact string comparison against sqlacodegen's expected output.

## Drop-in CLI compatibility

UVg accepts the same flags and URL formats as sqlacodegen. A user should be able to replace `sqlacodegen` with `uvg` in their scripts without changing anything else. This includes supporting SQLAlchemy-style database URLs with their various driver suffixes (`postgresql+psycopg2://`, `mssql+pytds://`, etc.), even though UVg uses different database drivers internally.

## Dialect abstraction without over-abstraction

The codebase supports PostgreSQL and MSSQL. Rather than building an elaborate dialect plugin system, UVg uses a simple `Dialect` enum that flows through the pipeline. Each point of dialect-specific behavior (type mapping, default formatting, identity columns, schema naming) dispatches on this enum directly.

This is intentional. Two dialects don't justify a trait-based plugin architecture. If a third dialect is added, the enum approach still works. If five are added, it may be time to reconsider -- but not before.

## Deterministic output

Given the same database state, UVg must produce identical output every time. This requires care in several places:

- **Table ordering**: Topological sort by FK dependencies with alphabetical tiebreak ensures tables always appear in the same order regardless of the order the database returns them.
- **Import ordering**: `BTreeMap` and `BTreeSet` in the import collector guarantee sorted, deterministic imports.
- **Constraint ordering**: Fixed emission order (FK, PK, Unique, Index) within each table.

## Test-driven fidelity

The test strategy is adapted directly from sqlacodegen's own test suite. Tests construct in-memory schemas using builder helpers, feed them to a generator, and assert exact string equality against the expected Python output. This pattern makes it easy to port sqlacodegen tests one-for-one and immediately see where UVg diverges.

Snapshot tests (`insta`) are used for regression protection on complex multi-table outputs. Exact-match tests (`assert_eq!` with `indoc!`) are preferred for new tests adapted from sqlacodegen because they make the expected output visible and comparable.
