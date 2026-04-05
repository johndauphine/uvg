/// Supported database backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}
