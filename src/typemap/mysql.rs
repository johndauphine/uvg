use crate::ddl_typemap::{self, mysql::is_tinyint_bool, mysql::is_unsigned, CanonicalType};
use crate::dialect::Dialect;
use crate::schema::ColumnInfo;

use super::{canonical_sa, simple, MappedType};

const MY: &str = "sqlalchemy.dialects.mysql";

/// Map a MySQL column to generic SQLAlchemy types.
///
/// Raw COLUMN_TYPE parsing (tinyint(1)-as-boolean, enum/set value lists,
/// unsigned detection, lengths/precision) lives in `ddl_typemap`; this
/// module keeps only the leaf table for MySQL-native SQLAlchemy types that
/// the canonical form deliberately collapses (TINYINT, MEDIUMINT, the
/// TEXT/BLOB size classes, TIMESTAMP-vs-DATETIME, YEAR, BIT, SET).
pub fn map_column_type(col: &ColumnInfo) -> MappedType {
    let ct = ddl_typemap::to_canonical(col, Dialect::Mysql);

    match col.udt_name.as_str() {
        // canonical folds non-bool tinyint into SmallInt; SA keeps TINYINT.
        "tinyint" if !is_tinyint_bool(col) => unsigned_aware("TINYINT", col),
        // canonical folds mediumint into Integer; SA keeps MEDIUMINT.
        "mediumint" => unsigned_aware("MEDIUMINT", col),
        // canonical folds the size classes into Text/Bytes.
        "tinytext" => simple("TINYTEXT", "str", MY),
        "mediumtext" => simple("MEDIUMTEXT", "str", MY),
        "longtext" => simple("LONGTEXT", "str", MY),
        "tinyblob" => simple("TINYBLOB", "bytes", MY),
        "mediumblob" => simple("MEDIUMBLOB", "bytes", MY),
        "longblob" => simple("LONGBLOB", "bytes", MY),
        // canonical folds timestamp into Timestamp alongside datetime.
        "timestamp" => simple("TIMESTAMP", "datetime.datetime", MY),
        // canonical maps year to SmallInt.
        "year" => simple("YEAR", "int", MY),
        // canonical maps bit(1) to Boolean and bit(n) to Raw.
        "bit" => simple("BIT", "int", MY),
        // SET keeps its dialect type; the parsed value list rides on the
        // canonical form.
        "set" => set_from_canonical(&ct),
        _ => canonical_sa::generic(&ct, Dialect::Mysql),
    }
}

/// Map a MySQL column keeping dialect-specific types from
/// `sqlalchemy.dialects.mysql` (`keep_dialect_types` option).
pub fn map_column_type_dialect(col: &ColumnInfo) -> MappedType {
    let ct = ddl_typemap::to_canonical(col, Dialect::Mysql);

    match col.udt_name.as_str() {
        "tinyint" if is_tinyint_bool(col) => MappedType {
            sa_type: "TINYINT(display_width=1)".to_string(),
            python_type: "int".to_string(),
            import_module: MY.to_string(),
            import_name: "TINYINT".to_string(),
            element_import: None,
        },
        "tinyint" => unsigned_aware("TINYINT", col),
        "smallint" => unsigned_aware("SMALLINT", col),
        "mediumint" => unsigned_aware("MEDIUMINT", col),
        "int" => unsigned_aware("INTEGER", col),
        "bigint" => unsigned_aware("BIGINT", col),
        "float" => simple("FLOAT", "float", MY),
        "double" => simple("DOUBLE", "float", MY),
        "decimal" | "numeric" => {
            let sa_type = match (col.numeric_precision, col.numeric_scale) {
                (Some(p), Some(s)) => format!("DECIMAL({p}, {s})"),
                (Some(p), None) => format!("DECIMAL({p})"),
                _ => "DECIMAL".to_string(),
            };
            named(sa_type, "decimal.Decimal", "DECIMAL")
        }
        "varchar" => sized("VARCHAR", col.character_maximum_length, "str"),
        "char" => sized("CHAR", col.character_maximum_length, "str"),
        "text" => simple("TEXT", "str", MY),
        "tinytext" => simple("TINYTEXT", "str", MY),
        "mediumtext" => simple("MEDIUMTEXT", "str", MY),
        "longtext" => simple("LONGTEXT", "str", MY),
        "binary" => sized("BINARY", col.character_maximum_length, "bytes"),
        "varbinary" => sized("VARBINARY", col.character_maximum_length, "bytes"),
        "blob" => simple("BLOB", "bytes", MY),
        "tinyblob" => simple("TINYBLOB", "bytes", MY),
        "mediumblob" => simple("MEDIUMBLOB", "bytes", MY),
        "longblob" => simple("LONGBLOB", "bytes", MY),
        "date" => simple("DATE", "datetime.date", MY),
        "time" => simple("TIME", "datetime.time", MY),
        "datetime" => simple("DATETIME", "datetime.datetime", MY),
        "timestamp" => simple("TIMESTAMP", "datetime.datetime", MY),
        "year" => simple("YEAR", "int", MY),
        "json" => simple("JSON", "dict", MY),
        "enum" => match &ct {
            CanonicalType::Enum { values } => {
                named(format!("ENUM({})", quote_values(values)), "str", "ENUM")
            }
            _ => named("ENUM()".to_string(), "str", "ENUM"),
        },
        "set" => set_from_canonical(&ct),
        "bit" => simple("BIT", "int", MY),
        "boolean" | "bool" => simple("BOOLEAN", "bool", MY),
        _ => {
            let upper = col.udt_name.to_uppercase();
            simple(&upper, "str", MY)
        }
    }
}

/// `SET('a', 'b')` from the canonical value list (parsed once in
/// ddl_typemap). Falls back to an empty SET if the canonical form is not a
/// Set — unreachable for a `set` udt.
fn set_from_canonical(ct: &CanonicalType) -> MappedType {
    let body = match ct {
        CanonicalType::Set { values } => quote_values(values),
        _ => String::new(),
    };
    named(format!("SET({body})"), "str", "SET")
}

fn quote_values(values: &[String]) -> String {
    let quoted: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
    quoted.join(", ")
}

/// A mysql-dialect integer that renders `NAME(unsigned=True)` when the
/// COLUMN_TYPE carries the unsigned attribute.
fn unsigned_aware(base: &str, col: &ColumnInfo) -> MappedType {
    if is_unsigned(col) {
        named(format!("{base}(unsigned=True)"), "int", base)
    } else {
        simple(base, "int", MY)
    }
}

fn sized(base: &str, length: Option<i32>, python_type: &str) -> MappedType {
    let sa_type = match length {
        Some(n) => format!("{base}({n})"),
        None => base.to_string(),
    };
    named(sa_type, python_type, base)
}

fn named(sa_type: String, python_type: &str, import_name: &str) -> MappedType {
    MappedType {
        sa_type,
        python_type: python_type.to_string(),
        import_module: MY.to_string(),
        import_name: import_name.to_string(),
        element_import: None,
    }
}

#[cfg(test)]
#[path = "mysql_tests.rs"]
mod tests;
