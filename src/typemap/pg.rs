use crate::ddl_typemap::{self, CanonicalType};
use crate::dialect::Dialect;
use crate::schema::ColumnInfo;

use super::{canonical_sa, simple, MappedType};

const PG: &str = "sqlalchemy.dialects.postgresql";

/// Map a PostgreSQL column to its SQLAlchemy type representation.
///
/// Parsing (array `_` prefix, lengths, precision/scale, udt normalization)
/// happens once in `ddl_typemap::to_canonical`; the shared canonical→SA core
/// covers PG entirely — PG's dialect types (UUID/JSON/JSONB/INET/CIDR) are
/// resolved there from the canonical form.
pub fn map_column_type(col: &ColumnInfo) -> MappedType {
    let ct = ddl_typemap::to_canonical(col, Dialect::Postgres);
    canonical_sa::generic(&ct, Dialect::Postgres)
}

/// Map a PostgreSQL column keeping dialect-specific types
/// (`keep_dialect_types` option): everything imports from
/// `sqlalchemy.dialects.postgresql` under its native uppercase name.
pub fn map_column_type_dialect(col: &ColumnInfo) -> MappedType {
    let ct = ddl_typemap::to_canonical(col, Dialect::Postgres);
    dialect_from_canonical(&ct)
}

fn dialect_from_canonical(ct: &CanonicalType) -> MappedType {
    match ct {
        CanonicalType::Boolean => simple("BOOLEAN", "bool", PG),
        CanonicalType::SmallInt => simple("SMALLINT", "int", PG),
        CanonicalType::Integer => simple("INTEGER", "int", PG),
        CanonicalType::BigInt => simple("BIGINT", "int", PG),
        CanonicalType::Float => simple("REAL", "float", PG),
        CanonicalType::Double => simple("DOUBLE_PRECISION", "float", PG),
        CanonicalType::Decimal { precision, scale } => {
            let sa_type = match (precision, scale) {
                (Some(p), Some(s)) => format!("NUMERIC({p}, {s})"),
                (Some(p), None) => format!("NUMERIC({p})"),
                _ => "NUMERIC".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "decimal.Decimal".to_string(),
                import_module: PG.to_string(),
                import_name: "NUMERIC".to_string(),
                element_import: None,
            }
        }
        CanonicalType::Varchar { length } => sized("VARCHAR", *length, "str"),
        CanonicalType::Char { length } => sized("CHAR", *length, "str"),
        CanonicalType::Text => simple("TEXT", "str", PG),
        CanonicalType::Bytes { .. } => simple("BYTEA", "bytes", PG),
        CanonicalType::Date => simple("DATE", "datetime.date", PG),
        CanonicalType::Time { with_tz: false, .. } => simple("TIME", "datetime.time", PG),
        CanonicalType::Time { with_tz: true, .. } => MappedType {
            sa_type: "TIME(timezone=True)".to_string(),
            python_type: "datetime.time".to_string(),
            import_module: PG.to_string(),
            import_name: "TIME".to_string(),
            element_import: None,
        },
        CanonicalType::Timestamp { with_tz: false, .. } => {
            simple("TIMESTAMP", "datetime.datetime", PG)
        }
        CanonicalType::Timestamp { with_tz: true, .. } => MappedType {
            sa_type: "TIMESTAMP(timezone=True)".to_string(),
            python_type: "datetime.datetime".to_string(),
            import_module: PG.to_string(),
            import_name: "TIMESTAMP".to_string(),
            element_import: None,
        },
        CanonicalType::Interval => simple("INTERVAL", "datetime.timedelta", PG),
        CanonicalType::Uuid => simple("UUID", "uuid.UUID", PG),
        CanonicalType::Json => simple("JSON", "dict", PG),
        CanonicalType::Jsonb => simple("JSONB", "dict", PG),
        // PG's to_canonical never yields Enum/Set (native enums resolve
        // through the schema's enum registry, not the typemap); treat them
        // like the generic core would, defensively.
        CanonicalType::Enum { .. } | CanonicalType::Set { .. } => {
            canonical_sa::generic(ct, Dialect::Postgres)
        }
        CanonicalType::Array { element } => {
            let inner = dialect_from_canonical(element);
            MappedType {
                sa_type: format!("ARRAY({})", inner.sa_type),
                python_type: "list".to_string(),
                import_module: "sqlalchemy".to_string(),
                import_name: "ARRAY".to_string(),
                element_import: Some((inner.import_module, inner.import_name)),
            }
        }
        CanonicalType::Raw { type_name } => match type_name.as_str() {
            "INET" => simple("INET", "str", PG),
            "CIDR" => simple("CIDR", "str", PG),
            "TSVECTOR" => simple("TSVECTOR", "str", PG),
            "" => simple("NullType", "str", "sqlalchemy.sql.sqltypes"),
            // Fallback imports from sqlalchemy (not the dialect module) to
            // avoid generating invalid dialect imports.
            other => simple(other, "str", "sqlalchemy"),
        },
    }
}

fn sized(base: &str, length: Option<i32>, python_type: &str) -> MappedType {
    let sa_type = match length {
        Some(n) => format!("{base}({n})"),
        None => base.to_string(),
    };
    MappedType {
        sa_type,
        python_type: python_type.to_string(),
        import_module: PG.to_string(),
        import_name: base.to_string(),
        element_import: None,
    }
}

#[cfg(test)]
#[path = "pg_tests.rs"]
mod tests;
