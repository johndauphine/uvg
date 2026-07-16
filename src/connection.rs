//! Connection URL parsing and normalization shared by every frontend.

use crate::dialect::Dialect;
use crate::error::UvgError;

/// Parsed connection configuration.
pub enum ConnectionConfig {
    Postgres(String),
    Mssql {
        host: String,
        port: u16,
        database: String,
        user: String,
        password: String,
        trust_cert: bool,
    },
    Mysql(String),
    Sqlite(String),
}

impl std::fmt::Debug for ConnectionConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postgres(url) => formatter
                .debug_tuple("Postgres")
                .field(&crate::redaction::redact_connection_url(url))
                .finish(),
            Self::Mysql(url) => formatter
                .debug_tuple("Mysql")
                .field(&crate::redaction::redact_connection_url(url))
                .finish(),
            Self::Sqlite(path) => formatter.debug_tuple("Sqlite").field(path).finish(),
            Self::Mssql {
                host,
                port,
                database,
                trust_cert,
                ..
            } => formatter
                .debug_struct("Mssql")
                .field("host", host)
                .field("port", port)
                .field("database", database)
                .field("user", &"***")
                .field("password", &"***")
                .field("trust_cert", trust_cert)
                .finish(),
        }
    }
}

impl ConnectionConfig {
    pub fn dialect(&self) -> Dialect {
        match self {
            ConnectionConfig::Postgres(_) => Dialect::Postgres,
            ConnectionConfig::Mssql { .. } => Dialect::Mssql,
            ConnectionConfig::Mysql(_) => Dialect::Mysql,
            ConnectionConfig::Sqlite(_) => Dialect::Sqlite,
        }
    }

    /// Extract the database name from a MySQL connection URL.
    /// Returns `None` if the URL has no database path or it is empty.
    pub fn database_name(&self) -> Option<String> {
        match self {
            ConnectionConfig::Mysql(url) => url::Url::parse(url).ok().and_then(|u| {
                let database = u.path().trim_start_matches('/').to_string();
                if database.is_empty() {
                    None
                } else {
                    Some(database)
                }
            }),
            _ => None,
        }
    }
}

/// Best-effort dialect inference from a connection URL's scheme, for
/// display-only paths that hold a raw URL rather than a parsed config.
/// Correctness-critical paths should use [`ConnectionConfig::dialect`].
pub fn dialect_from_url(url: &str) -> Dialect {
    let url = url.trim();
    if url.starts_with("mysql") || url.starts_with("mariadb") {
        Dialect::Mysql
    } else if url.starts_with("mssql") || url.starts_with("sqlserver") {
        Dialect::Mssql
    } else if url.starts_with("sqlite") {
        Dialect::Sqlite
    } else {
        Dialect::Postgres
    }
}

/// Parse a SQLAlchemy-style database URL into the native connection config.
///
/// `trust_cert` is only used for MSSQL. Keeping it as an explicit argument
/// lets non-CLI callers use exactly the same scheme rewriting, credential
/// decoding, and MySQL charset normalization as the command-line frontend.
pub fn parse_connection_url(url: &str, trust_cert: bool) -> Result<ConnectionConfig, UvgError> {
    // PostgreSQL schemes
    if let Some(rest) = url
        .strip_prefix("postgresql+psycopg2://")
        .or_else(|| url.strip_prefix("postgresql+asyncpg://"))
        .or_else(|| url.strip_prefix("postgresql+psycopg://"))
    {
        return Ok(ConnectionConfig::Postgres(format!("postgres://{rest}")));
    }
    if url.starts_with("postgresql://") || url.starts_with("postgres://") {
        return Ok(ConnectionConfig::Postgres(url.to_string()));
    }

    // MSSQL schemes
    if url.starts_with("mssql://")
        || url.starts_with("mssql+pytds://")
        || url.starts_with("mssql+pyodbc://")
        || url.starts_with("mssql+pymssql://")
    {
        return parse_mssql_url(url, trust_cert);
    }

    // MySQL schemes
    if let Some(rest) = url
        .strip_prefix("mysql+pymysql://")
        .or_else(|| url.strip_prefix("mysql+mysqldb://"))
        .or_else(|| url.strip_prefix("mysql+aiomysql://"))
        .or_else(|| url.strip_prefix("mysql+asyncmy://"))
    {
        return Ok(ConnectionConfig::Mysql(ensure_mysql_charset(&format!(
            "mysql://{rest}"
        ))));
    }
    if let Some(rest) = url
        .strip_prefix("mariadb+pymysql://")
        .or_else(|| url.strip_prefix("mariadb+mysqldb://"))
    {
        return Ok(ConnectionConfig::Mysql(ensure_mysql_charset(&format!(
            "mysql://{rest}"
        ))));
    }
    if let Some(rest) = url.strip_prefix("mariadb://") {
        return Ok(ConnectionConfig::Mysql(ensure_mysql_charset(&format!(
            "mysql://{rest}"
        ))));
    }
    if url.starts_with("mysql://") {
        return Ok(ConnectionConfig::Mysql(ensure_mysql_charset(url)));
    }

    // SQLite schemes
    if let Some(rest) = url.strip_prefix("sqlite:///") {
        // sqlacodegen format: sqlite:///relative or sqlite:////absolute
        // sqlx format: sqlite:relative or sqlite:///absolute
        if rest.starts_with('/') {
            return Ok(ConnectionConfig::Sqlite(format!("sqlite://{rest}")));
        }
        if rest == ":memory:" {
            return Ok(ConnectionConfig::Sqlite("sqlite::memory:".to_string()));
        }
        return Ok(ConnectionConfig::Sqlite(format!("sqlite:{rest}")));
    }

    Err(UvgError::UnsupportedScheme(
        url.split("://").next().unwrap_or("unknown").to_string(),
    ))
}

/// Ensure a MySQL URL includes `charset=utf8mb4` so that
/// `information_schema` returns proper VARCHAR columns instead of VARBINARY.
fn ensure_mysql_charset(url: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(url) else {
        return url.to_string();
    };

    let has_charset = parsed.query_pairs().any(|(key, _)| key == "charset");
    if !has_charset {
        parsed.query_pairs_mut().append_pair("charset", "utf8mb4");
    }

    parsed.into()
}

fn parse_mssql_url(raw: &str, trust_cert: bool) -> Result<ConnectionConfig, UvgError> {
    let normalized = if let Some(rest) = raw.strip_prefix("mssql+pytds://") {
        format!("mssql://{rest}")
    } else if let Some(rest) = raw.strip_prefix("mssql+pyodbc://") {
        format!("mssql://{rest}")
    } else if let Some(rest) = raw.strip_prefix("mssql+pymssql://") {
        format!("mssql://{rest}")
    } else {
        raw.to_string()
    };

    let parsed = url::Url::parse(&normalized)
        .map_err(|e| UvgError::Connection(format!("Invalid MSSQL URL: {e}")))?;

    let host = parsed.host_str().unwrap_or("localhost").to_string();
    let port = parsed.port().unwrap_or(1433);
    let database = parsed.path().trim_start_matches('/').to_string();
    if database.is_empty() {
        return Err(UvgError::Connection(
            "MSSQL URL must include a database name".to_string(),
        ));
    }
    let user = percent_encoding::percent_decode_str(parsed.username())
        .decode_utf8_lossy()
        .into_owned();
    let password = parsed
        .password()
        .map(|password| {
            percent_encoding::percent_decode_str(password)
                .decode_utf8_lossy()
                .into_owned()
        })
        .unwrap_or_default();

    Ok(ConnectionConfig::Mssql {
        host,
        port,
        database,
        user,
        password,
        trust_cert,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_sqlalchemy_mysql_url_and_adds_charset() {
        let config = parse_connection_url("mysql+pymysql://u:p@host/app", false).unwrap();
        assert!(matches!(
            config,
            ConnectionConfig::Mysql(ref url)
                if url == "mysql://u:p@host/app?charset=utf8mb4"
        ));
    }

    #[test]
    fn preserves_explicit_mysql_charset() {
        let config = parse_connection_url("mysql://u:p@host/app?charset=latin1", false).unwrap();
        assert!(matches!(
            config,
            ConnectionConfig::Mysql(ref url) if url.ends_with("charset=latin1")
        ));
    }

    #[test]
    fn carries_trust_cert_into_mssql_config() {
        let config = parse_connection_url("mssql://u:p@host/app", true).unwrap();
        assert!(matches!(
            config,
            ConnectionConfig::Mssql {
                trust_cert: true,
                ..
            }
        ));
    }

    #[test]
    fn debug_output_redacts_credentials() {
        let postgres =
            parse_connection_url("postgresql://alice:hunter2@db.example.com/orders", false)
                .unwrap();
        let postgres_debug = format!("{postgres:?}");
        assert!(!postgres_debug.contains("alice"));
        assert!(!postgres_debug.contains("hunter2"));

        let mssql = parse_connection_url("mssql://sa:SuperSecret@db/orders", false).unwrap();
        let mssql_debug = format!("{mssql:?}");
        assert!(!mssql_debug.contains("sa"));
        assert!(!mssql_debug.contains("SuperSecret"));
    }
}
