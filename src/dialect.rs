use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Supported database backends.
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
        matches!(self, Dialect::Postgres)
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
