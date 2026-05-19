//! Versioned migration workflow (`uvg revision`, `upgrade`, `current`, `history`).
//!
//! This is intentionally separate from the existing `--out-dir` layout. `--out-dir`
//! organizes one live diff into reviewable files; this module manages a revision
//! chain and records the target database's current revision in `uvg_version`.

mod apply;
mod commands;
mod files;
mod graph;
mod model;
mod render;
mod reverse;
mod version_table;

use crate::cli::{Cli, Command};
use anyhow::Result;

use commands::{run_downgrade, run_history, run_merge, run_revision, run_stamp, run_upgrade};
use version_table::current_revision;

#[cfg(test)]
use self::{
    apply::{apply_down_migration, apply_migration, format_parse_error_lines, migration_plan_sql},
    files::{
        parse_migration_file, revision_id_from_epoch, slugify, write_merge_revision_file,
        write_meta_file, write_revision_file,
    },
    graph::MigrationGraph,
    model::{MigrationDirection, MigrationFile},
    render::render_down_sql,
    reverse::{first_sql_token, reverse_change_sql},
    version_table::{clear_revision, ensure_version_table, record_revision, stamp_revision},
};
#[cfg(test)]
use crate::db;

pub(crate) async fn run(cli: &Cli, command: &Command) -> Result<()> {
    match command {
        Command::Init(args) => crate::init::run(args),
        Command::Revision(args) => run_revision(cli, args).await,
        Command::Upgrade(args) => run_upgrade(cli, args).await,
        Command::Downgrade(args) => run_downgrade(cli, args).await,
        Command::Merge(args) => run_merge(args),
        Command::Stamp(args) => run_stamp(cli, args).await,
        Command::Current(args) => {
            let config = cli.parse_connection_url(&args.target_url)?;
            match current_revision(&config).await? {
                Some(revision) => println!("{revision}"),
                None => println!(),
            }
            Ok(())
        }
        Command::History(args) => run_history(cli, args).await,
        Command::Snapshot(_) => unreachable!("snapshot is handled before migration dispatch"),
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
