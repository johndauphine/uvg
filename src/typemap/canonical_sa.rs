//! Shared canonical → SQLAlchemy mapping (#114).
//!
//! The portable core of every dialect's generic type mapping lives here,
//! keyed off [`CanonicalType`] — the same parsed representation the DDL
//! translator uses. Per-dialect typemap modules parse a column **once**
//! via `ddl_typemap::to_canonical` and delegate to [`generic`], keeping only
//! thin leaf tables for dialect-native SQLAlchemy types that the canonical
//! form deliberately collapses (e.g. MySQL `MEDIUMINT`, MSSQL `Unicode`).
//!
//! `source` parameterizes the few spots where the same canonical type maps
//! to different SQLAlchemy imports per source dialect (JSON, UUID, `Raw`
//! resolution, SQLite's Double→Float affinity).

use crate::ddl_typemap::CanonicalType;
use crate::dialect::Dialect;

use super::{simple, MappedType};

const SA: &str = "sqlalchemy";
const PG: &str = "sqlalchemy.dialects.postgresql";

/// Map a canonical type to the generic SQLAlchemy representation, exactly
/// as the per-dialect tables did before unification. Dialect-native leaves
/// (types canonical collapses) are intercepted by the per-dialect adapters
/// *before* this is called and never reach here.
pub(super) fn generic(ct: &CanonicalType, source: Dialect) -> MappedType {
    match ct {
        CanonicalType::Boolean => simple("Boolean", "bool", SA),
        CanonicalType::SmallInt => simple("SmallInteger", "int", SA),
        CanonicalType::Integer => simple("Integer", "int", SA),
        CanonicalType::BigInt => simple("BigInteger", "int", SA),
        CanonicalType::Float => simple("Float", "float", SA),
        // SQLite's affinity mapping folds doubles into Float (its historical
        // behavior); every other dialect distinguishes Double.
        CanonicalType::Double => match source {
            Dialect::Sqlite => simple("Float", "float", SA),
            _ => simple("Double", "float", SA),
        },
        CanonicalType::Decimal { precision, scale } => {
            let sa_type = match (precision, scale) {
                (Some(p), Some(s)) => format!("Numeric({p}, {s})"),
                (Some(p), None) => format!("Numeric({p})"),
                _ => "Numeric".to_string(),
            };
            parameterized(sa_type, "decimal.Decimal", SA, "Numeric")
        }
        // Generic mode renders CHAR the same as VARCHAR: String(n).
        CanonicalType::Varchar { length } | CanonicalType::Char { length } => {
            let sa_type = match length {
                Some(n) => format!("String({n})"),
                None => "String".to_string(),
            };
            parameterized(sa_type, "str", SA, "String")
        }
        CanonicalType::Text => simple("Text", "str", SA),
        CanonicalType::Bytes { length: Some(n) } => {
            parameterized(format!("LargeBinary({n})"), "bytes", SA, "LargeBinary")
        }
        CanonicalType::Bytes { length: None } => simple("LargeBinary", "bytes", SA),
        CanonicalType::Date => simple("Date", "datetime.date", SA),
        // Sub-second precision is a DDL concern only; SQLAlchemy's generic
        // Time/DateTime take no precision argument.
        CanonicalType::Time { with_tz: true, .. } => {
            parameterized("Time(True)".to_string(), "datetime.time", SA, "Time")
        }
        CanonicalType::Time { with_tz: false, .. } => simple("Time", "datetime.time", SA),
        CanonicalType::Timestamp { with_tz: true, .. } => parameterized(
            "DateTime(True)".to_string(),
            "datetime.datetime",
            SA,
            "DateTime",
        ),
        CanonicalType::Timestamp { with_tz: false, .. } => {
            simple("DateTime", "datetime.datetime", SA)
        }
        CanonicalType::Interval => simple("Interval", "datetime.timedelta", SA),
        CanonicalType::Uuid => simple("UUID", "uuid.UUID", PG),
        CanonicalType::Json => match source {
            // PG's JSON reflects as the dialect type; MySQL/SQLite use the
            // generic sqlalchemy.JSON.
            Dialect::Postgres => simple("JSON", "dict", PG),
            _ => simple("JSON", "dict", SA),
        },
        CanonicalType::Jsonb => simple("JSONB", "dict", PG),
        CanonicalType::Enum { values } => {
            let quoted: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
            parameterized(format!("Enum({})", quoted.join(", ")), "str", SA, "Enum")
        }
        // MySQL's adapter intercepts SET before canonical dispatch; this arm
        // is defensive and mirrors that leaf.
        CanonicalType::Set { values } => {
            let quoted: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
            parameterized(
                format!("SET({})", quoted.join(", ")),
                "str",
                "sqlalchemy.dialects.mysql",
                "SET",
            )
        }
        CanonicalType::Array { element } => {
            let inner = generic(element, source);
            MappedType {
                sa_type: format!("ARRAY({})", inner.sa_type),
                python_type: "list".to_string(),
                import_module: SA.to_string(),
                import_name: "ARRAY".to_string(),
                element_import: Some((inner.import_module, inner.import_name)),
            }
        }
        CanonicalType::Raw { type_name } => raw(type_name, source),
    }
}

/// Resolve a `Raw` (non-portable) type to its SQLAlchemy form. PG has a few
/// dialect types (INET/CIDR) and reports untyped columns as NullType; every
/// dialect otherwise falls back to the uppercased name from `sqlalchemy`.
fn raw(type_name: &str, source: Dialect) -> MappedType {
    if source == Dialect::Postgres {
        match type_name {
            "INET" => return simple("INET", "str", PG),
            "CIDR" => return simple("CIDR", "str", PG),
            "TSVECTOR" => return simple("TSVECTOR", "str", PG),
            "" => return simple("NullType", "str", "sqlalchemy.sql.sqltypes"),
            _ => {}
        }
    }
    simple(type_name, "str", SA)
}

/// A MappedType whose rendered expression carries parameters while the
/// import is the bare base name.
fn parameterized(
    sa_type: String,
    python_type: &str,
    import_module: &str,
    import_name: &str,
) -> MappedType {
    MappedType {
        sa_type,
        python_type: python_type.to_string(),
        import_module: import_module.to_string(),
        import_name: import_name.to_string(),
        element_import: None,
    }
}
