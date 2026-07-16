use std::path::PathBuf;

use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};

pub use crate::connection::ConnectionConfig;
use crate::dialect::Dialect;

pub const DEFAULT_INTROSPECT_CONCURRENCY: usize = 8;

/// Generate SQLAlchemy model code from an existing database.
///
/// Drop-in compatible reimplementation of sqlacodegen in Rust.
#[derive(Parser, Debug)]
#[command(name = "uvg", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Named profile from ~/.config/uvg/profiles.yaml
    #[arg(long, env = "UVG_PROFILE")]
    pub profile: Option<String>,

    /// Source database URL (e.g. postgresql://, mysql://, sqlite:///path, mssql://)
    pub url: Option<String>,

    /// Target database URL for DDL generation/migration (optional)
    pub target_url: Option<String>,

    /// Code generator to use (declarative, tables, ddl)
    #[arg(long, default_value = "declarative")]
    pub generator: String,

    /// Target SQL dialect for DDL output (postgres, mysql, sqlite, mssql)
    #[arg(long)]
    pub target_dialect: Option<String>,

    /// Output one file per table into the outfile directory
    #[arg(long)]
    pub split_tables: bool,

    /// Execute generated DDL against the target database after rendering it.
    /// Requires a target URL. Combines naturally with `--out-dir`: the
    /// per-table files are written first, then applied in manifest order.
    /// Exits non-zero on the first failed statement.
    #[arg(long)]
    pub apply: bool,

    /// Per-statement progress reporting on `--apply`. Default `auto`
    /// emits when stderr is a terminal and stays silent when redirected.
    #[arg(long, value_enum, default_value_t = crate::apply_progress::ProgressMode::Auto)]
    pub progress: crate::apply_progress::ProgressMode,

    /// Maximum retry attempts per statement on CLI or interactive apply for transient
    /// errors (deadlock, lock-wait timeout, brief connection drops).
    /// Logical errors (constraint, syntax, missing column) fail
    /// immediately regardless. `0` disables retry; default `3`.
    /// Backoff is 100ms / 500ms / 2s with jitter.
    #[arg(long, default_value_t = 3)]
    pub apply_retries: u8,

    /// Skip the parse-check step that runs before a CLI or interactive apply
    /// would touch the target. By default uvg pre-validates every DDL
    /// statement via the dialect's parse-only mode:
    ///   - PG: savepoint-per-statement inside one outer transaction,
    ///     ROLLBACK at the end. Catches syntax errors AND catalog
    ///     errors (missing references, wrong column types, etc.).
    ///   - MSSQL / MySQL / SQLite: skipped (no safe parse-only mode).
    ///
    /// Bad DDL surfaces before any real change is made.
    #[arg(long)]
    pub no_parse_check: bool,

    /// Annotate DDL diff statements with AI-generated risk classes
    #[arg(long)]
    pub risk_classify: bool,

    /// Concurrent table metadata queries for PostgreSQL/MySQL introspection
    #[arg(long, env = "UVG_INTROSPECT_CONCURRENCY", default_value_t = DEFAULT_INTROSPECT_CONCURRENCY, value_parser = parse_positive_usize)]
    pub introspect_concurrency: usize,

    /// Tables to process (comma-delimited). Each item is a glob pattern
    /// (`*`, `?`, `[abc]`); bare names with no metacharacters match
    /// exactly. Default: all tables.
    #[arg(long)]
    pub tables: Option<String>,

    /// Tables to exclude (comma-delimited), evaluated after `--tables`.
    /// Same glob syntax as `--tables`.
    #[arg(long)]
    pub exclude_tables: Option<String>,

    /// Schemas to load (comma-delimited)
    #[arg(long)]
    pub schemas: Option<String>,

    /// Ignore views
    #[arg(long)]
    pub noviews: bool,

    /// Generator options (comma-delimited): noindexes, noconstraints, nocomments, nobidi, nofknames, noidsuffix, nosyntheticenums, nonativeenums, keep_dialect_types
    #[arg(long)]
    pub options: Option<String>,

    /// Output file or directory (default: stdout)
    #[arg(long)]
    pub outfile: Option<String>,

    /// Write per-table DDL diff into this directory. One subdir per
    /// modified table plus `_schema/` for non-table-scoped DDL and
    /// `_runs/` for the manifest. Empty diffs write nothing.
    /// Only meaningful for the `ddl` generator with a target URL.
    /// `--outfile` takes precedence if both are set.
    #[arg(long)]
    pub out_dir: Option<PathBuf>,

    /// Slug used in `--out-dir` filenames. Defaults to
    /// `<source>_to_<target>` (e.g. `postgres_to_mysql`).
    #[arg(long)]
    pub name: Option<String>,

    /// Trust the server certificate (MSSQL only)
    #[arg(long)]
    pub trust_cert: bool,

    /// Launch interactive TUI for DDL diff and apply
    #[arg(long, short = 'i')]
    pub interactive: bool,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Scaffold a migrations directory and project config
    Init(InitCommand),

    /// Generate a versioned migration file from a source/target diff
    Revision(RevisionCommand),

    /// Apply pending versioned migrations to a target database
    Upgrade(UpgradeCommand),

    /// Roll back versioned migrations on a target database
    Downgrade(DowngradeCommand),

    /// Create a merge revision from multiple migration heads
    Merge(MergeCommand),

    /// Mark a target database at a revision without running migrations
    Stamp(StampCommand),

    /// Print the target database's current uvg revision
    Current(CurrentCommand),

    /// Show the local migration graph
    History(HistoryCommand),

    /// Capture an introspected schema snapshot as YAML
    Snapshot(SnapshotCommand),
}

#[derive(Args, Debug, Clone)]
pub struct InitCommand {
    /// Directory for versioned migration files
    #[arg(long, default_value = "./migrations")]
    pub migrations_dir: PathBuf,

    /// Path to the profiles file to scaffold. Defaults to the user profiles
    /// path that `--profile` reads (`$XDG_CONFIG_HOME/uvg/profiles.yaml`, or
    /// `~/.config/uvg/profiles.yaml`). An existing file is left untouched.
    #[arg(long)]
    pub config: Option<PathBuf>,
}

#[derive(Args, Debug, Clone)]
pub struct RevisionCommand {
    /// Source database URL to diff from
    pub source_url: String,

    /// Target database URL to converge
    pub target_url: String,

    /// Human-readable migration description
    #[arg(long, short = 'm')]
    pub message: String,

    /// Directory containing versioned migration files
    #[arg(long, default_value = "./migrations")]
    pub migrations_dir: PathBuf,
}

#[derive(Args, Debug, Clone)]
pub struct UpgradeCommand {
    /// Target database URL to apply migrations to
    pub target_url: String,

    /// Revision to upgrade to; defaults to head
    pub revision: Option<String>,

    /// Directory containing versioned migration files
    #[arg(long, default_value = "./migrations")]
    pub migrations_dir: PathBuf,
}

#[derive(Args, Debug, Clone)]
pub struct DowngradeCommand {
    /// Target database URL to roll back
    pub target_url: String,

    /// Revision to downgrade to; defaults to one revision back
    pub revision: Option<String>,

    /// Directory containing versioned migration files
    #[arg(long, default_value = "./migrations")]
    pub migrations_dir: PathBuf,
}

#[derive(Args, Debug, Clone)]
pub struct MergeCommand {
    /// Human-readable merge revision description
    #[arg(long, short = 'm')]
    pub message: String,

    /// Directory containing versioned migration files
    #[arg(long, default_value = "./migrations")]
    pub migrations_dir: PathBuf,
}

#[derive(Args, Debug, Clone)]
pub struct StampCommand {
    /// Target database URL to stamp
    pub target_url: String,

    /// Existing migration revision to record
    pub revision: String,

    /// Directory containing versioned migration files
    #[arg(long, default_value = "./migrations")]
    pub migrations_dir: PathBuf,

    /// Skip confirmation prompt
    #[arg(long)]
    pub yes: bool,
}

#[derive(Args, Debug, Clone)]
pub struct CurrentCommand {
    /// Target database URL to inspect
    pub target_url: String,
}

#[derive(Args, Debug, Clone)]
pub struct HistoryCommand {
    /// Optional target database URL; marks applied/current revisions when provided
    pub target_url: Option<String>,

    /// Directory containing versioned migration files
    #[arg(long, default_value = "./migrations")]
    pub migrations_dir: PathBuf,
}

#[derive(Args, Debug, Clone)]
pub struct SnapshotCommand {
    /// Database URL to snapshot
    pub url: String,

    /// Output snapshot YAML file
    #[arg(long, short = 'o')]
    pub output: PathBuf,
}

#[derive(Debug, Default)]
pub struct GeneratorOptions {
    pub noindexes: bool,
    pub noconstraints: bool,
    pub nocomments: bool,
    pub nobidi: bool,
    pub nofknames: bool,
    pub noidsuffix: bool,
    pub nosyntheticenums: bool,
    pub nonativeenums: bool,
    pub keep_dialect_types: bool,
}

/// Options specific to the DDL generator.
#[derive(Debug)]
pub struct DdlOptions {
    pub target_dialect: Dialect,
    pub split_tables: bool,
    pub apply: bool,
    pub noindexes: bool,
    pub noconstraints: bool,
    pub nocomments: bool,
}

/// Split a comma-delimited CLI value, trimming whitespace and dropping
/// empty entries. `None` / empty string produce an empty vec.
fn split_csv(raw: Option<&str>) -> Vec<String> {
    let Some(s) = raw else { return Vec::new() };
    s.split(',')
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect()
}

fn parse_positive_usize(raw: &str) -> Result<usize, String> {
    let value = raw
        .parse::<usize>()
        .map_err(|e| format!("expected positive integer: {e}"))?;
    if value == 0 {
        return Err("must be at least 1".to_string());
    }
    Ok(value)
}

impl Cli {
    /// Parse CLI args and then apply any requested named profile.
    ///
    /// clap's derive parser gives us final values, but profile merging needs
    /// to know which values came from the command line so explicit flags can
    /// win over profile defaults.
    pub fn parse_with_profile() -> anyhow::Result<Self> {
        let matches = Self::command().get_matches();
        let mut cli =
            Self::from_arg_matches(&matches).map_err(|err| anyhow::anyhow!(err.to_string()))?;
        crate::profile::apply_requested_profile(&mut cli, &matches)?;
        Ok(cli)
    }

    /// Parse the comma-delimited --tables flag into a Vec of glob patterns.
    /// Bare names with no metacharacters degenerate to exact-match (back-compat
    /// with the original exact-name behavior). Empty / missing flag → empty vec.
    pub fn table_list(&self) -> Vec<String> {
        split_csv(self.tables.as_deref())
    }

    /// Parse the comma-delimited --exclude-tables flag into a Vec of glob
    /// patterns. Same syntax and degeneration rule as `table_list`.
    pub fn exclude_table_list(&self) -> Vec<String> {
        split_csv(self.exclude_tables.as_deref())
    }

    /// Build a `TableFilter` from `--tables` and `--exclude-tables`.
    /// Validates every glob pattern up front so bad input surfaces
    /// before any DB connection is opened.
    pub fn table_filter(&self) -> Result<crate::table_filter::TableFilter, crate::error::UvgError> {
        crate::table_filter::TableFilter::new(&self.table_list(), &self.exclude_table_list())
    }

    /// Parse the comma-delimited --schemas flag, falling back to the given default.
    pub fn schema_list_or(&self, default: &str) -> Vec<String> {
        let raw = self.schemas.as_deref().unwrap_or(default);
        raw.split(',').map(|s| s.trim().to_string()).collect()
    }

    /// Parse the comma-delimited --options flag into structured options.
    pub fn generator_options(&self) -> GeneratorOptions {
        let mut opts = GeneratorOptions::default();
        if let Some(ref options_str) = self.options {
            for opt in options_str.split(',').map(|s| s.trim()) {
                match opt {
                    "noindexes" => opts.noindexes = true,
                    "noconstraints" => opts.noconstraints = true,
                    "nocomments" => opts.nocomments = true,
                    "nobidi" => opts.nobidi = true,
                    "nofknames" => opts.nofknames = true,
                    "noidsuffix" => opts.noidsuffix = true,
                    "nosyntheticenums" => opts.nosyntheticenums = true,
                    "nonativeenums" => opts.nonativeenums = true,
                    "keep_dialect_types" => opts.keep_dialect_types = true,
                    _ => tracing::warn!("Unknown generator option: {}", opt),
                }
            }
        }
        opts
    }

    /// Build DDL-specific options. `source_dialect` is used as the default target
    /// when neither `--target-dialect` nor a target URL is provided.
    pub fn ddl_options(
        &self,
        source_dialect: Dialect,
    ) -> Result<DdlOptions, crate::error::UvgError> {
        self.ddl_options_with_target_dialect(source_dialect, None)
    }

    /// Build DDL-specific options with an optional already-loaded target
    /// dialect. This matters for `@snapshot.yaml` targets, where there is no
    /// URL scheme to infer from.
    pub fn ddl_options_with_target_dialect(
        &self,
        source_dialect: Dialect,
        target_dialect_hint: Option<Dialect>,
    ) -> Result<DdlOptions, crate::error::UvgError> {
        let target_dialect = if let Some(ref td) = self.target_dialect {
            td.parse::<Dialect>()
                .map_err(crate::error::UvgError::InvalidDialect)?
        } else if let Some(dialect) = target_dialect_hint {
            dialect
        } else if let Some(ref target_url) = self.target_url {
            // Infer dialect from target URL scheme
            self.parse_connection_url(target_url)?.dialect()
        } else {
            source_dialect
        };

        let gen_opts = self.generator_options();
        Ok(DdlOptions {
            target_dialect,
            split_tables: self.split_tables,
            apply: self.apply,
            noindexes: gen_opts.noindexes,
            noconstraints: gen_opts.noconstraints,
            nocomments: gen_opts.nocomments,
        })
    }

    /// Parse a target URL into a `ConnectionConfig`.
    pub fn parse_target_connection(
        &self,
        target_url: &str,
    ) -> Result<ConnectionConfig, crate::error::UvgError> {
        self.parse_connection_url(target_url)
    }

    /// Parse the URL into a `ConnectionConfig`.
    pub fn parse_connection(&self) -> Result<ConnectionConfig, crate::error::UvgError> {
        let Some(url) = self.url.as_deref() else {
            return Err(crate::error::UvgError::Connection(
                "database URL is required".to_string(),
            ));
        };
        self.parse_connection_url(url)
    }

    /// Parse a URL string into a `ConnectionConfig`.
    pub fn parse_connection_url(
        &self,
        url: &str,
    ) -> Result<ConnectionConfig, crate::error::UvgError> {
        crate::connection::parse_connection_url(url, self.trust_cert)
    }
}

#[cfg(test)]
#[path = "cli_tests.rs"]
mod tests;
