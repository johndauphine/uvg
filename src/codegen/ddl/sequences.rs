use std::collections::{BTreeMap, BTreeSet};

use crate::codegen::parse_sequence_name;
use crate::dialect::Dialect;
use crate::schema::IntrospectedSchema;

/// PostgreSQL sequences referenced by `nextval(...)` column defaults.
///
/// The schema model does not yet carry standalone sequence metadata, but a
/// referenced sequence name is sufficient to reproduce SERIAL-style defaults
/// without inventing a different sequence for every table that shares one.
pub(crate) fn referenced_sequences(schema: &IntrospectedSchema) -> Vec<String> {
    if schema.dialect != Dialect::Postgres {
        return Vec::new();
    }

    schema
        .tables
        .iter()
        .flat_map(|table| &table.columns)
        .filter_map(|column| column.column_default.as_deref())
        .filter_map(parse_sequence_name)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Sequence names referenced by more than one column. These cannot be
/// represented by rendering each column as SERIAL, because SERIAL creates a
/// distinct sequence per table/column.
pub(crate) fn shared_sequences(schema: &IntrospectedSchema) -> BTreeSet<String> {
    if schema.dialect != Dialect::Postgres {
        return BTreeSet::new();
    }

    let mut counts = BTreeMap::<String, usize>::new();
    for sequence in schema
        .tables
        .iter()
        .flat_map(|table| &table.columns)
        .filter_map(|column| column.column_default.as_deref())
        .filter_map(parse_sequence_name)
    {
        *counts.entry(sequence).or_default() += 1;
    }
    counts
        .into_iter()
        .filter_map(|(sequence, count)| (count > 1).then_some(sequence))
        .collect()
}

pub(crate) fn generate_sequence(name: &str) -> String {
    // `name` comes from PostgreSQL's rendered regclass expression and already
    // carries any quoting/schema qualification required by the server.
    format!("CREATE SEQUENCE {name};")
}

pub(super) fn generate_sequences(
    schema: &IntrospectedSchema,
    target_dialect: Dialect,
) -> Vec<String> {
    if target_dialect != Dialect::Postgres {
        return Vec::new();
    }

    shared_sequences(schema)
        .iter()
        .map(|name| generate_sequence(name))
        .collect()
}
