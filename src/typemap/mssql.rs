use crate::ddl_typemap;
use crate::dialect::Dialect;
use crate::schema::ColumnInfo;

use super::{canonical_sa, simple, MappedType};

const MS: &str = "sqlalchemy.dialects.mssql";

/// Map a MSSQL column to its SQLAlchemy type representation.
///
/// Numeric/temporal parsing rides on `ddl_typemap::to_canonical`
/// (money → Decimal(19,4), datetimeoffset → tz-aware Timestamp, ...); the
/// leaf table below keeps only what canonical collapses: the
/// String/Unicode split with collation, NTEXT, bare LargeBinary for the
/// binary family, TINYINT, and UNIQUEIDENTIFIER.
pub fn map_column_type(col: &ColumnInfo) -> MappedType {
    match col.udt_name.as_str() {
        // canonical folds tinyint into SmallInt; SA keeps the dialect type.
        "tinyint" => simple("TINYINT", "int", MS),
        // canonical folds nvarchar/nchar into Varchar/Char, losing the
        // unicode-ness and the collation that sqlacodegen renders.
        "varchar" | "char" => string_type("String", col),
        "nvarchar" | "nchar" => string_type("Unicode", col),
        // canonical folds ntext into Text.
        "ntext" => simple("UnicodeText", "str", "sqlalchemy"),
        // canonical carries the length for binary types; sqlacodegen renders
        // the MSSQL binary family as bare LargeBinary.
        "binary" | "varbinary" | "image" => simple("LargeBinary", "bytes", "sqlalchemy"),
        // canonical maps uniqueidentifier to Uuid; MSSQL reflects it as the
        // dialect's UNIQUEIDENTIFIER with a str annotation.
        "uniqueidentifier" => simple("UNIQUEIDENTIFIER", "str", MS),
        _ => {
            let ct = ddl_typemap::to_canonical(col, Dialect::Mssql);
            canonical_sa::generic(&ct, Dialect::Mssql)
        }
    }
}

/// Map a MSSQL column keeping dialect-specific types
/// (`keep_dialect_types` option).
pub fn map_column_type_dialect(col: &ColumnInfo) -> MappedType {
    match col.udt_name.as_str() {
        "bit" => simple("BIT", "bool", MS),
        "tinyint" => simple("TINYINT", "int", MS),
        "smallint" => simple("SMALLINT", "int", MS),
        "int" => simple("INTEGER", "int", MS),
        "bigint" => simple("BIGINT", "int", MS),
        "real" => simple("REAL", "float", MS),
        "float" => simple("FLOAT", "float", MS),
        "decimal" | "numeric" => {
            let sa_type = match (col.numeric_precision, col.numeric_scale) {
                (Some(p), Some(s)) => format!("NUMERIC({p}, {s})"),
                (Some(p), None) => format!("NUMERIC({p})"),
                _ => "NUMERIC".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "decimal.Decimal".to_string(),
                import_module: MS.to_string(),
                import_name: "NUMERIC".to_string(),
                element_import: None,
            }
        }
        "money" => simple("MONEY", "decimal.Decimal", MS),
        "smallmoney" => simple("SMALLMONEY", "decimal.Decimal", MS),
        "varchar" => sized("VARCHAR", col.character_maximum_length),
        "char" => sized("CHAR", col.character_maximum_length),
        "nvarchar" => sized("NVARCHAR", col.character_maximum_length),
        "nchar" => sized("NCHAR", col.character_maximum_length),
        "text" => simple("TEXT", "str", MS),
        "ntext" => simple("NTEXT", "str", MS),
        "binary" => simple("BINARY", "bytes", MS),
        "varbinary" => simple("VARBINARY", "bytes", MS),
        "image" => simple("IMAGE", "bytes", MS),
        "datetime" => simple("DATETIME", "datetime.datetime", MS),
        "datetime2" => simple("DATETIME2", "datetime.datetime", MS),
        "smalldatetime" => simple("SMALLDATETIME", "datetime.datetime", MS),
        "datetimeoffset" => simple("DATETIMEOFFSET", "datetime.datetime", MS),
        "date" => simple("DATE", "datetime.date", MS),
        "time" => simple("TIME", "datetime.time", MS),
        "uniqueidentifier" => simple("UNIQUEIDENTIFIER", "str", MS),
        other => {
            let upper = other.to_uppercase();
            simple(&upper, "str", MS)
        }
    }
}

/// Format a String/Unicode type expression with optional length and
/// collation, matching sqlacodegen output: `String(50, 'collation')` or
/// `Unicode(collation='collation')`.
fn string_type(base: &str, col: &ColumnInfo) -> MappedType {
    let sa_type = match (col.character_maximum_length, col.collation.as_deref()) {
        (Some(n), Some(c)) => format!("{base}({n}, '{c}')"),
        (Some(n), None) => format!("{base}({n})"),
        (None, Some(c)) => format!("{base}(collation='{c}')"),
        (None, None) => base.to_string(),
    };
    MappedType {
        sa_type,
        python_type: "str".to_string(),
        import_module: "sqlalchemy".to_string(),
        import_name: base.to_string(),
        element_import: None,
    }
}

fn sized(base: &str, length: Option<i32>) -> MappedType {
    let sa_type = match length {
        Some(n) => format!("{base}({n})"),
        None => base.to_string(),
    };
    MappedType {
        sa_type,
        python_type: "str".to_string(),
        import_module: MS.to_string(),
        import_name: base.to_string(),
        element_import: None,
    }
}

#[cfg(test)]
#[path = "mssql_tests.rs"]
mod tests;
