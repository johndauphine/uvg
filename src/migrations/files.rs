use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};

use crate::dialect::Dialect;
use crate::output::{format_utc_compact, Change};

use super::graph::MigrationGraph;
use super::model::{MigrationFile, MigrationSection};
use super::render::{render_down_sql, render_up_sql};

pub(super) const GENERATED_MERGE_UP_SQL: &str =
    "-- Empty merge revision. Branch migrations already carry the SQL.";
pub(super) const GENERATED_MERGE_DOWN_SQL: &str =
    "-- Merge downgrade is not automatic because uvg_version tracks one current revision.";

pub(super) fn parse_migration_file(body: &str, path: PathBuf) -> Result<MigrationFile> {
    let mut revision = None;
    let mut parents: Option<Vec<String>> = None;
    let mut description = String::new();
    let mut current_section = None;
    let mut section_lines: HashMap<MigrationSection, Vec<&str>> = HashMap::new();

    for line in body.lines() {
        let trimmed = line.trim();
        if let Some(section) = section_marker(trimmed) {
            current_section = Some(section);
            section_lines.entry(section).or_default();
            continue;
        }
        if let Some(section) = current_section {
            section_lines.entry(section).or_default().push(line);
            continue;
        }

        if let Some(value) = trimmed.strip_prefix("-- uvg revision:") {
            revision = Some(value.trim().to_string());
        } else if let Some(value) = trimmed.strip_prefix("-- parent:") {
            let value = value.trim();
            parents = Some(if value.is_empty() {
                Vec::new()
            } else {
                vec![value.to_string()]
            });
        } else if let Some(value) = trimmed.strip_prefix("-- parents:") {
            parents = Some(
                value
                    .split(',')
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty())
                    .collect(),
            );
        } else if let Some(value) = trimmed.strip_prefix("-- description:") {
            description = value.trim().to_string();
        }
    }

    let section_sql = |section| {
        section_lines
            .get(&section)
            .map(|lines| lines.join("\n").trim().to_string())
            .unwrap_or_default()
    };
    let revision = revision.ok_or_else(|| {
        anyhow!(
            "migration {} is missing `-- uvg revision:` header",
            path.display()
        )
    })?;
    if !section_lines.contains_key(&MigrationSection::Up) {
        return Err(anyhow!(
            "migration {} is missing required `-- UP` section",
            path.display()
        ));
    }

    Ok(MigrationFile {
        revision,
        parents: parents.unwrap_or_default(),
        description,
        path,
        pre_sql: section_sql(MigrationSection::Pre),
        up_sql: section_sql(MigrationSection::Up),
        post_sql: section_sql(MigrationSection::Post),
        pre_down_sql: section_sql(MigrationSection::PreDown),
        down_sql: section_lines
            .contains_key(&MigrationSection::Down)
            .then(|| section_sql(MigrationSection::Down)),
        post_down_sql: section_sql(MigrationSection::PostDown),
    })
}

fn section_marker(trimmed: &str) -> Option<MigrationSection> {
    match trimmed.to_ascii_uppercase().as_str() {
        "-- PRE" => Some(MigrationSection::Pre),
        "-- UP" => Some(MigrationSection::Up),
        "-- POST" => Some(MigrationSection::Post),
        "-- POST DOWN" => Some(MigrationSection::PostDown),
        "-- DOWN" => Some(MigrationSection::Down),
        "-- PRE DOWN" => Some(MigrationSection::PreDown),
        _ => None,
    }
}

pub(super) fn write_revision_file(
    migrations_dir: &Path,
    revision: &str,
    parent: Option<&str>,
    description: &str,
    source_dialect: Dialect,
    target_dialect: Dialect,
    changes: &[Change],
) -> Result<PathBuf> {
    fs::create_dir_all(migrations_dir)
        .with_context(|| format!("failed to create {}", migrations_dir.display()))?;
    let filename = format!("{}_{}.sql", revision, slugify(description));
    let path = migrations_dir.join(filename);
    if path.exists() {
        return Err(anyhow!(
            "refusing to overwrite existing migration {}",
            path.display()
        ));
    }

    let mut body = String::new();
    body.push_str(&format!(
        "-- uvg revision: {}\n",
        flatten_for_comment(revision)
    ));
    body.push_str(&format!(
        "-- parent: {}\n",
        flatten_for_comment(parent.unwrap_or(""))
    ));
    body.push_str(&format!(
        "-- description: {}\n",
        flatten_for_comment(description)
    ));
    body.push_str(&format!("-- source: {source_dialect}\n"));
    body.push_str(&format!("-- target: {target_dialect}\n\n"));
    body.push_str("-- UP\n");
    body.push_str(&render_up_sql(changes));
    if !body.ends_with('\n') {
        body.push('\n');
    }
    body.push('\n');
    body.push_str("-- DOWN\n");
    body.push_str(&render_down_sql(changes, target_dialect));
    if !body.ends_with('\n') {
        body.push('\n');
    }

    fs::write(&path, body).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

pub(super) fn write_merge_revision_file(
    migrations_dir: &Path,
    revision: &str,
    parents: &[String],
    description: &str,
) -> Result<PathBuf> {
    fs::create_dir_all(migrations_dir)
        .with_context(|| format!("failed to create {}", migrations_dir.display()))?;
    let filename = format!("{}_{}.sql", revision, slugify(description));
    let path = migrations_dir.join(filename);
    if path.exists() {
        return Err(anyhow!(
            "refusing to overwrite existing migration {}",
            path.display()
        ));
    }

    let mut body = String::new();
    body.push_str(&format!(
        "-- uvg revision: {}\n",
        flatten_for_comment(revision)
    ));
    body.push_str(&format!(
        "-- parents: {}\n",
        parents
            .iter()
            .map(|parent| flatten_for_comment(parent))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    body.push_str(&format!(
        "-- description: {}\n\n",
        flatten_for_comment(description)
    ));
    body.push_str("-- UP\n");
    body.push_str(GENERATED_MERGE_UP_SQL);
    body.push_str("\n\n");
    body.push_str("-- DOWN\n");
    body.push_str(GENERATED_MERGE_DOWN_SQL);
    body.push('\n');

    fs::write(&path, body).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

pub(super) fn write_meta_file(migrations_dir: &Path, graph: &MigrationGraph) -> Result<()> {
    let heads = graph.heads();
    let head = if heads.len() == 1 {
        heads[0].clone()
    } else {
        String::new()
    };
    let mut body = String::new();
    body.push_str("# Generated by uvg. Revision files are the source of truth.\n");
    body.push_str(&format!("head: {}\n", yaml_quote(&head)));
    body.push_str("revisions:\n");
    for migration in graph.ordered() {
        body.push_str(&format!(
            "  - revision: {}\n",
            yaml_quote(&migration.revision)
        ));
        if migration.parents.len() > 1 {
            body.push_str(&format!(
                "    parents: {}\n",
                yaml_inline_list(&migration.parents)
            ));
        } else {
            body.push_str(&format!(
                "    parent: {}\n",
                yaml_quote(migration.parents.first().map(String::as_str).unwrap_or(""))
            ));
        }
        body.push_str(&format!(
            "    description: {}\n",
            yaml_quote(&migration.description)
        ));
    }

    fs::write(migrations_dir.join("meta.yaml"), body).with_context(|| {
        format!(
            "failed to write {}",
            migrations_dir.join("meta.yaml").display()
        )
    })
}

pub(super) fn next_revision_id(graph: &MigrationGraph) -> String {
    let base = revision_id_from_epoch(now_epoch_secs());
    if !graph.migrations.contains_key(&base) {
        return base;
    }
    for i in 1..100 {
        let candidate = format!("{base}_{i:02}");
        if !graph.migrations.contains_key(&candidate) {
            return candidate;
        }
    }
    base
}

pub(super) fn revision_id_from_epoch(epoch_secs: u64) -> String {
    let compact = format_utc_compact(epoch_secs);
    format!("{}_{}", &compact[0..8], &compact[9..15])
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub(super) fn slugify(input: &str) -> String {
    let mut out = String::new();
    let mut last_dash = false;
    for c in input.chars().flat_map(|c| c.to_lowercase()) {
        if c.is_ascii_alphanumeric() {
            out.push(c);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
        if out.len() >= 64 {
            break;
        }
    }
    let trimmed = out.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "migration".to_string()
    } else {
        trimmed
    }
}

pub(super) fn flatten_for_comment(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '\n' => "\\n".to_string(),
            '\r' => "\\r".to_string(),
            '\t' => "\\t".to_string(),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => format!("\\x{:02x}", c as u32),
            c => c.to_string(),
        })
        .collect()
}

fn yaml_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn yaml_inline_list(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| yaml_quote(value))
            .collect::<Vec<_>>()
            .join(", ")
    )
}
