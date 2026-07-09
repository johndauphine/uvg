use crate::ddl_typemap;
use crate::dialect::Dialect;
use crate::schema::ColumnInfo;

use super::{canonical_sa, simple, MappedType};

/// Map a SQLite column to generic SQLAlchemy types.
///
/// SQLite's free-form declared types and affinity rules are parsed once in
/// `ddl_typemap::to_canonical`; the shared canonical→SA core does the rest.
/// The only leaf: a column with no declared type reflects as NullType
/// (canonical calls it Text for DDL purposes).
pub fn map_column_type(col: &ColumnInfo) -> MappedType {
    if col.udt_name.is_empty() {
        return simple("NullType", "str", "sqlalchemy.sql.sqltypes");
    }
    let ct = ddl_typemap::to_canonical(col, Dialect::Sqlite);
    canonical_sa::generic(&ct, Dialect::Sqlite)
}

/// Map a SQLite column keeping dialect-specific types.
/// SQLite has very few dialect-specific types, so this is the same as the
/// generic mapping — SA handles SQLite reflection using generic types.
pub fn map_column_type_dialect(col: &ColumnInfo) -> MappedType {
    map_column_type(col)
}

#[cfg(test)]
#[path = "sqlite_tests.rs"]
mod tests;
