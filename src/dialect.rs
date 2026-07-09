use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Supported database backends.
///
/// # Adding a dialect
///
/// Extend this enum and let the compiler walk you through the rest: every
/// capability method below and every `match` over `Dialect` (type parsing in
/// `ddl_typemap`, SQLAlchemy leaves in `typemap`, rendering idioms in
/// `codegen/render`, backend dispatch on `ConnectionConfig` in `db.rs`) is an
/// exhaustive match and will fail to compile until the new dialect is
/// handled. The capability methods here answer the cross-cutting questions
/// once, so most scattered call sites need no per-dialect edits at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Dialect {
    Postgres,
    Mssql,
    Mysql,
    Sqlite,
}

impl Dialect {
    /// Return the default schema name for this dialect.
    /// For MySQL the real default is the database name (dynamic); callers
    /// should use `ConnectionConfig::database_name()` instead.
    pub fn default_schema(&self) -> &'static str {
        match self {
            Dialect::Postgres => "public",
            Dialect::Mssql => "dbo",
            Dialect::Mysql => "",
            Dialect::Sqlite => "main",
        }
    }

    /// Whether a batch of DDL statements can be wrapped in a single
    /// all-or-nothing transaction on this backend. Only PostgreSQL supports
    /// transactional DDL; MySQL and MSSQL implicitly commit on most DDL
    /// (`CREATE`/`ALTER`/`DROP`), so an outer transaction can't roll them
    /// back, and the SQLite apply path runs statement-by-statement against a
    /// file we typically own exclusively.
    pub fn supports_transactional_ddl(&self) -> bool {
        match self {
            Dialect::Postgres => true,
            Dialect::Mssql | Dialect::Mysql | Dialect::Sqlite => false,
        }
    }

    // ---- per-dialect capabilities (#115) -------------------------------
    //
    // Each capability is an exhaustive `match`, never a `==`/`matches!`
    // shortcut: adding a fifth dialect must fail to compile until every
    // capability is decided, instead of silently inheriting whichever side
    // of an `==` check it doesn't equal. Scattered ad-hoc dialect checks in
    // the codegen/diff/migration layers route through these.

    /// Whether the engine has a real boolean type with `true`/`false`
    /// literals. MySQL and MSSQL model booleans as tiny integers and expect
    /// `1`/`0` in DDL defaults.
    pub fn uses_boolean_literals(&self) -> bool {
        match self {
            Dialect::Postgres | Dialect::Sqlite => true,
            Dialect::Mssql | Dialect::Mysql => false,
        }
    }

    /// Whether `DROP INDEX` must name the owning table (`DROP INDEX i ON t`).
    /// MySQL and MSSQL scope indexes to their table; PostgreSQL and SQLite
    /// drop them as standalone schema objects (and, correspondingly, accept
    /// a schema-qualified index name where MySQL/MSSQL cannot).
    pub fn drop_index_requires_table(&self) -> bool {
        match self {
            Dialect::Mssql | Dialect::Mysql => true,
            Dialect::Postgres | Dialect::Sqlite => false,
        }
    }

    /// Whether the dialect has first-class enum types (`CREATE TYPE ... AS
    /// ENUM`). Non-PG targets emit CHECK constraints or fall back to text.
    pub fn supports_native_enums(&self) -> bool {
        match self {
            Dialect::Postgres => true,
            Dialect::Mssql | Dialect::Mysql | Dialect::Sqlite => false,
        }
    }

    /// Whether comments are attached with standalone `COMMENT ON` statements.
    /// MySQL inlines comments in the CREATE TABLE; MSSQL uses extended
    /// properties (unsupported); SQLite has no comment storage.
    pub fn supports_comment_on(&self) -> bool {
        match self {
            Dialect::Postgres => true,
            Dialect::Mssql | Dialect::Mysql | Dialect::Sqlite => false,
        }
    }

    /// Whether the backend offers a safe parse-only probe for DDL
    /// (statements executed in a transaction that is always rolled back).
    /// See `db::parse_check_ddl` for why MSSQL's `SET PARSEONLY` and
    /// MySQL/SQLite are excluded.
    pub fn supports_parse_check(&self) -> bool {
        match self {
            Dialect::Postgres => true,
            Dialect::Mssql | Dialect::Mysql | Dialect::Sqlite => false,
        }
    }

    /// Whether constraints can be added/dropped on an existing table with
    /// `ALTER TABLE`. SQLite requires a table rebuild instead, so the diff
    /// engine skips constraint alteration for it entirely.
    pub fn supports_constraint_alteration(&self) -> bool {
        match self {
            Dialect::Postgres | Dialect::Mssql | Dialect::Mysql => true,
            Dialect::Sqlite => false,
        }
    }

    /// Whether the engine auto-creates (and refuses to drop, while the FK
    /// exists) a backing index for each foreign key — InnoDB behavior. On
    /// other engines an index on FK columns is always user-created and must
    /// participate in index drift.
    pub fn auto_creates_fk_backing_indexes(&self) -> bool {
        match self {
            Dialect::Mysql => true,
            Dialect::Postgres | Dialect::Mssql | Dialect::Sqlite => false,
        }
    }

    /// Whether `RESTRICT` and `NO ACTION` referential actions are the same
    /// behavior and interchangeable in the catalog (InnoDB reports either
    /// spelling depending on how the FK was authored).
    pub fn treats_restrict_as_no_action(&self) -> bool {
        match self {
            Dialect::Mysql => true,
            Dialect::Postgres | Dialect::Mssql | Dialect::Sqlite => false,
        }
    }

    /// Whether the "schema" of introspected objects is really the database
    /// name (MySQL conflates the two). Such schemas never match across two
    /// databases under diff and cannot be re-qualified by emitted DDL, so
    /// diff normalization and FK ref_schema comparison treat them specially.
    pub fn schema_is_database(&self) -> bool {
        match self {
            Dialect::Mysql => true,
            Dialect::Postgres | Dialect::Mssql | Dialect::Sqlite => false,
        }
    }
}

impl FromStr for Dialect {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "postgres" | "postgresql" | "pg" => Ok(Dialect::Postgres),
            "mysql" | "mariadb" => Ok(Dialect::Mysql),
            "sqlite" => Ok(Dialect::Sqlite),
            "mssql" | "sqlserver" => Ok(Dialect::Mssql),
            _ => Err(format!(
                "Unknown dialect '{s}'. Expected: postgres, mysql, sqlite, mssql"
            )),
        }
    }
}

impl fmt::Display for Dialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Dialect::Postgres => write!(f, "postgres"),
            Dialect::Mssql => write!(f, "mssql"),
            Dialect::Mysql => write!(f, "mysql"),
            Dialect::Sqlite => write!(f, "sqlite"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_table_is_locked() {
        use Dialect::*;
        // One row per capability; a change here is a deliberate behavior
        // change, not a refactor.
        for d in [Postgres, Mssql, Mysql, Sqlite] {
            assert_eq!(
                d.uses_boolean_literals(),
                matches!(d, Postgres | Sqlite),
                "{d}"
            );
            assert_eq!(
                d.drop_index_requires_table(),
                matches!(d, Mssql | Mysql),
                "{d}"
            );
            assert_eq!(d.supports_native_enums(), matches!(d, Postgres), "{d}");
            assert_eq!(d.supports_comment_on(), matches!(d, Postgres), "{d}");
            assert_eq!(d.supports_parse_check(), matches!(d, Postgres), "{d}");
            assert_eq!(
                d.supports_constraint_alteration(),
                !matches!(d, Sqlite),
                "{d}"
            );
            assert_eq!(
                d.auto_creates_fk_backing_indexes(),
                matches!(d, Mysql),
                "{d}"
            );
            assert_eq!(d.treats_restrict_as_no_action(), matches!(d, Mysql), "{d}");
            assert_eq!(d.schema_is_database(), matches!(d, Mysql), "{d}");
        }
    }

    #[test]
    fn only_postgres_supports_transactional_ddl() {
        assert!(Dialect::Postgres.supports_transactional_ddl());
        for d in [Dialect::Mysql, Dialect::Mssql, Dialect::Sqlite] {
            assert!(
                !d.supports_transactional_ddl(),
                "{d} implicitly commits DDL and must not claim transactional apply"
            );
        }
    }
}
