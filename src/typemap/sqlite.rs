use crate::schema::ColumnInfo;

use super::{simple, MappedType};

const SA: &str = "sqlalchemy";

/// Map a SQLite column to generic SQLAlchemy types.
/// SQLite uses type affinity — declared types are free-form strings.
pub fn map_column_type(col: &ColumnInfo) -> MappedType {
    let dt = col.udt_name.as_str();

    // First try exact matches on common declared types
    match dt {
        "integer" | "int" => simple("Integer", "int", SA),
        "smallint" => simple("SmallInteger", "int", SA),
        "bigint" => simple("BigInteger", "int", SA),
        "real" | "float" | "double" => simple("Float", "float", SA),
        "numeric" | "decimal" => {
            let sa_type = match (col.numeric_precision, col.numeric_scale) {
                (Some(p), Some(s)) => format!("Numeric({p}, {s})"),
                (Some(p), None) => format!("Numeric({p})"),
                _ => "Numeric".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "decimal.Decimal".to_string(),
                import_module: SA.to_string(),
                import_name: "Numeric".to_string(),
                element_import: None,
            }
        }
        "text" | "clob" => simple("Text", "str", SA),
        "varchar" | "character varying" => {
            let sa_type = match col.character_maximum_length {
                Some(n) => format!("String({n})"),
                None => "String".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: SA.to_string(),
                import_name: "String".to_string(),
                element_import: None,
            }
        }
        "char" | "character" | "nchar" | "nvarchar" => {
            let sa_type = match col.character_maximum_length {
                Some(n) => format!("String({n})"),
                None => "String".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: SA.to_string(),
                import_name: "String".to_string(),
                element_import: None,
            }
        }
        "blob" => simple("LargeBinary", "bytes", SA),
        "date" => simple("Date", "datetime.date", SA),
        "datetime" | "timestamp" => simple("DateTime", "datetime.datetime", SA),
        "time" => simple("Time", "datetime.time", SA),
        "boolean" | "bool" => simple("Boolean", "bool", SA),
        "json" => simple("JSON", "dict", SA),
        "" => {
            // No declared type — use NullType
            simple("NullType", "str", "sqlalchemy.sql.sqltypes")
        }
        _ => {
            // Apply SQLite type affinity rules
            map_by_affinity(dt)
        }
    }
}

/// Map a SQLite column keeping dialect-specific types.
/// SQLite has very few dialect-specific types, so this is mostly the same
/// as the generic mapping.
pub fn map_column_type_dialect(col: &ColumnInfo) -> MappedType {
    // SQLite's dialect types in SA are minimal (DATETIME, DATE, TIME, JSON).
    // For simplicity, use the generic mapping — SA handles SQLite reflection
    // using generic types.
    map_column_type(col)
}

/// Apply SQLite type affinity rules for unknown declared types.
/// See: https://www.sqlite.org/datatype3.html
fn map_by_affinity(dt: &str) -> MappedType {
    let upper = dt.to_uppercase();

    if upper.contains("INT") {
        simple("Integer", "int", SA)
    } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        simple("Text", "str", SA)
    } else if upper.contains("BLOB") {
        simple("LargeBinary", "bytes", SA)
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        simple("Float", "float", SA)
    } else {
        // NUMERIC affinity — fallback
        simple("Numeric", "decimal.Decimal", SA)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::col;

    fn sqlite_col(udt: &str) -> ColumnInfo {
        col("test").udt(udt).build()
    }

    #[test]
    fn test_integer() {
        let m = map_column_type(&sqlite_col("integer"));
        assert_eq!(m.sa_type, "Integer");
        assert_eq!(m.python_type, "int");
    }

    #[test]
    fn test_text() {
        let m = map_column_type(&sqlite_col("text"));
        assert_eq!(m.sa_type, "Text");
        assert_eq!(m.python_type, "str");
    }

    #[test]
    fn test_real() {
        let m = map_column_type(&sqlite_col("real"));
        assert_eq!(m.sa_type, "Float");
        assert_eq!(m.python_type, "float");
    }

    #[test]
    fn test_blob() {
        let m = map_column_type(&sqlite_col("blob"));
        assert_eq!(m.sa_type, "LargeBinary");
        assert_eq!(m.python_type, "bytes");
    }

    #[test]
    fn test_varchar_with_length() {
        let mut c = sqlite_col("varchar");
        c.character_maximum_length = Some(100);
        let m = map_column_type(&c);
        assert_eq!(m.sa_type, "String(100)");
    }

    #[test]
    fn test_boolean() {
        let m = map_column_type(&sqlite_col("boolean"));
        assert_eq!(m.sa_type, "Boolean");
        assert_eq!(m.python_type, "bool");
    }

    #[test]
    fn test_datetime() {
        let m = map_column_type(&sqlite_col("datetime"));
        assert_eq!(m.sa_type, "DateTime");
        assert_eq!(m.python_type, "datetime.datetime");
    }

    #[test]
    fn test_json() {
        let m = map_column_type(&sqlite_col("json"));
        assert_eq!(m.sa_type, "JSON");
        assert_eq!(m.python_type, "dict");
    }

    #[test]
    fn test_empty_type() {
        let m = map_column_type(&sqlite_col(""));
        assert_eq!(m.sa_type, "NullType");
    }

    #[test]
    fn test_affinity_int() {
        // "MEDIUMINT" contains "INT" -> Integer affinity
        let m = map_column_type(&sqlite_col("mediumint"));
        assert_eq!(m.sa_type, "Integer");
    }

    #[test]
    fn test_affinity_text() {
        // "LONGTEXT" contains "TEXT" -> Text affinity
        let m = map_column_type(&sqlite_col("longtext"));
        assert_eq!(m.sa_type, "Text");
    }

    #[test]
    fn test_affinity_real() {
        // "DOUBLE PRECISION" contains "DOUB" -> Float affinity
        let m = map_column_type(&sqlite_col("double precision"));
        assert_eq!(m.sa_type, "Float");
    }

    #[test]
    fn test_decimal() {
        let mut c = sqlite_col("decimal");
        c.numeric_precision = Some(10);
        c.numeric_scale = Some(2);
        let m = map_column_type(&c);
        assert_eq!(m.sa_type, "Numeric(10, 2)");
    }
}
