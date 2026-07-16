//! SQL rendering primitives shared by the DDL generator (`ddl`) and the
//! schema diff engine (`ddl_diff`) — identifier quoting, column/table/index
//! rendering, default translation, and CHECK-predicate handling.
//!
//! Extracted (#116) so the diff engine consumes these as a first-class layer
//! instead of reaching into the generator's internals through a curated
//! re-export shim in `ddl.rs`.

pub(in crate::codegen) mod checks;
pub(in crate::codegen) mod column;
pub(in crate::codegen) mod create_table;
pub(in crate::codegen) mod defaults;
pub(in crate::codegen) mod ident;
pub(in crate::codegen) mod indexes;

pub(in crate::codegen) use checks::{check_predicate_is_portable, translate_check_predicate};
pub(in crate::codegen) use column::generate_column_def;
pub(in crate::codegen) use create_table::generate_create_table;
pub(in crate::codegen) use defaults::format_ddl_default_typed;
pub(in crate::codegen) use ident::{qualified_object_name, qualified_table_name, quote_identifier};
pub(in crate::codegen) use indexes::{generate_indexes, postgres_index_method};
