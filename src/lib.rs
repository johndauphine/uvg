//! Reusable library surface for UVg.
//!
//! The binary is intentionally a thin orchestration layer. Database
//! connection parsing and guarded DDL application live here so every caller
//! (including the interactive TUI) uses the same production-safety checks.

pub mod apply;
pub mod apply_progress;
pub mod cli;
pub mod codegen;
pub mod connection;
pub mod db;
pub mod ddl_typemap;
pub mod dialect;
pub mod error;
pub mod init;
pub mod introspect;
pub mod migrations;
pub mod naming;
pub mod output;
pub mod profile;
pub mod redaction;
pub mod risk_classify;
pub mod schema;
pub mod snapshot;
pub mod table_filter;
#[cfg(test)]
mod testutil;
pub mod tui;
pub mod typemap;
