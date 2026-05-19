use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::Path;

use anyhow::{anyhow, Context, Result};

use super::files::parse_migration_file;
use super::model::MigrationFile;

pub(super) struct MigrationGraph {
    pub(super) migrations: BTreeMap<String, MigrationFile>,
}

impl MigrationGraph {
    pub(super) fn load(dir: &Path) -> Result<Self> {
        if !dir.exists() {
            return Ok(Self {
                migrations: BTreeMap::new(),
            });
        }
        let mut paths = Vec::new();
        for entry in fs::read_dir(dir)
            .with_context(|| format!("failed to read migrations directory {}", dir.display()))?
        {
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                paths.push(path);
            }
        }
        paths.sort();

        let mut migrations = BTreeMap::new();
        for path in paths {
            let body = fs::read_to_string(&path)
                .with_context(|| format!("failed to read migration {}", path.display()))?;
            let migration = parse_migration_file(&body, path.clone())?;
            if migrations
                .insert(migration.revision.clone(), migration.clone())
                .is_some()
            {
                return Err(anyhow!(
                    "duplicate migration revision `{}`",
                    migration.revision
                ));
            }
        }

        Ok(Self { migrations })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.migrations.is_empty()
    }

    pub(super) fn single_head(&self) -> Result<Option<String>> {
        let heads = self.heads();
        match heads.len() {
            0 => Ok(None),
            1 => Ok(heads.into_iter().next()),
            _ => Err(anyhow!(
                "multiple migration heads found: {}. Run `uvg merge --message <name>` or pass an explicit revision",
                heads.join(", ")
            )),
        }
    }

    pub(super) fn heads(&self) -> Vec<String> {
        let mut referenced = BTreeSet::new();
        for migration in self.migrations.values() {
            for parent in &migration.parents {
                referenced.insert(parent.clone());
            }
        }
        self.migrations
            .keys()
            .filter(|revision| !referenced.contains(*revision))
            .cloned()
            .collect()
    }

    pub(super) fn resolve_target(&self, requested: Option<&str>) -> Result<Option<String>> {
        match requested {
            Some("base") => Ok(None),
            Some(revision) => {
                if !self.migrations.contains_key(revision) {
                    return Err(anyhow!(
                        "unknown migration revision `{}`. Valid revisions: {}",
                        revision,
                        self.valid_revisions()
                    ));
                }
                Ok(Some(revision.to_string()))
            }
            None => self.single_head(),
        }
    }

    pub(super) fn require_revision(&self, revision: &str) -> Result<&MigrationFile> {
        self.migrations.get(revision).ok_or_else(|| {
            anyhow!(
                "unknown migration revision `{}`. Valid revisions: {}",
                revision,
                self.valid_revisions()
            )
        })
    }

    pub(super) fn plan_upgrade<'a>(
        &'a self,
        current: Option<&str>,
        target: Option<&str>,
    ) -> Result<Vec<&'a MigrationFile>> {
        if current == target {
            return Ok(Vec::new());
        }
        if let Some(current_revision) = current {
            if !self.migrations.contains_key(current_revision) {
                return Err(anyhow!(
                    "target database is stamped at unknown revision `{}`. Valid revisions: {}",
                    current_revision,
                    self.valid_revisions()
                ));
            }
        }

        let Some(target_revision) = target else {
            return Ok(Vec::new());
        };
        let target_ancestors = self.ancestor_set(target_revision)?;
        let current_ancestors = if let Some(current_revision) = current {
            if !target_ancestors.contains(current_revision) {
                return Err(anyhow!(
                    "revision `{}` is not an ancestor of `{}`; branched upgrade paths are not supported yet",
                    current_revision,
                    target_revision
                ));
            }
            self.ancestor_set(current_revision)?
        } else {
            HashSet::new()
        };
        let pending: HashSet<&str> = target_ancestors
            .iter()
            .map(String::as_str)
            .filter(|revision| !current_ancestors.contains(*revision))
            .collect();

        Ok(self
            .ordered()
            .into_iter()
            .filter(|migration| pending.contains(migration.revision.as_str()))
            .collect())
    }

    pub(super) fn plan_downgrade<'a>(
        &'a self,
        current: Option<&str>,
        requested: Option<&str>,
    ) -> Result<Vec<&'a MigrationFile>> {
        let Some(current_revision) = current else {
            if matches!(requested, None | Some("base")) {
                return Ok(Vec::new());
            }
            return Err(anyhow!(
                "target database has no current revision; cannot downgrade to `{}`",
                requested.unwrap_or("base")
            ));
        };
        let current_migration = self.require_revision(current_revision)?;

        if requested.is_none() {
            if current_migration.parents.len() > 1 {
                return Err(anyhow!(
                    "cannot downgrade through merge revision `{}` because uvg_version tracks a single current revision; resolve manually and use `uvg stamp`",
                    current_migration.revision
                ));
            }
            return Ok(vec![current_migration]);
        }

        let target = match requested {
            Some("base") => None,
            Some(revision) => {
                self.require_revision(revision)?;
                Some(revision)
            }
            None => unreachable!(),
        };
        if target == Some(current_revision) {
            return Ok(Vec::new());
        }

        let current_ancestors = self.ancestor_set(current_revision)?;
        let target_ancestors = if let Some(target_revision) = target {
            if !current_ancestors.contains(target_revision) {
                return Err(anyhow!(
                    "revision `{}` is not an ancestor of `{}`; cannot downgrade across unrelated branches",
                    target_revision,
                    current_revision
                ));
            }
            self.ancestor_set(target_revision)?
        } else {
            HashSet::new()
        };
        let pending: HashSet<&str> = current_ancestors
            .iter()
            .map(String::as_str)
            .filter(|revision| !target_ancestors.contains(*revision))
            .collect();

        let plan = self
            .ordered()
            .into_iter()
            .rev()
            .filter(|migration| pending.contains(migration.revision.as_str()))
            .collect::<Vec<_>>();
        if let Some(merge) = plan.iter().find(|migration| migration.parents.len() > 1) {
            return Err(anyhow!(
                "cannot downgrade through merge revision `{}` because uvg_version tracks a single current revision; resolve manually and use `uvg stamp`",
                merge.revision
            ));
        }

        Ok(plan)
    }

    pub(super) fn ordered(&self) -> Vec<&MigrationFile> {
        let mut indegree: HashMap<String, usize> = self
            .migrations
            .keys()
            .map(|revision| (revision.clone(), 0))
            .collect();
        let mut children: HashMap<String, Vec<String>> = HashMap::new();
        for migration in self.migrations.values() {
            for parent in &migration.parents {
                if self.migrations.contains_key(parent) {
                    *indegree.entry(migration.revision.clone()).or_default() += 1;
                    children
                        .entry(parent.clone())
                        .or_default()
                        .push(migration.revision.clone());
                }
            }
        }
        for revisions in children.values_mut() {
            revisions.sort();
        }

        let mut ordered = Vec::new();
        let mut ready: BTreeSet<String> = indegree
            .iter()
            .filter_map(|(revision, count)| {
                if *count == 0 {
                    Some(revision.clone())
                } else {
                    None
                }
            })
            .collect();
        while let Some(revision) = ready.iter().next().cloned() {
            ready.remove(&revision);
            if let Some(migration) = self.migrations.get(&revision) {
                ordered.push(migration);
                if let Some(kids) = children.get(&revision) {
                    for child in kids {
                        if let Some(count) = indegree.get_mut(child) {
                            *count -= 1;
                            if *count == 0 {
                                ready.insert(child.clone());
                            }
                        }
                    }
                }
            }
        }

        if ordered.len() < self.migrations.len() {
            let seen: HashSet<&str> = ordered.iter().map(|m| m.revision.as_str()).collect();
            for migration in self.migrations.values() {
                if !seen.contains(migration.revision.as_str()) {
                    ordered.push(migration);
                }
            }
        }
        ordered
    }

    pub(super) fn ancestor_set(&self, revision: &str) -> Result<HashSet<String>> {
        if !self.migrations.contains_key(revision) {
            return Err(anyhow!(
                "target database is stamped at unknown revision `{}`. Valid revisions: {}",
                revision,
                self.valid_revisions()
            ));
        }
        let mut seen = HashSet::new();
        let mut stack = vec![revision.to_string()];
        while let Some(rev) = stack.pop() {
            if !seen.insert(rev.clone()) {
                continue;
            }
            let migration = self
                .migrations
                .get(&rev)
                .ok_or_else(|| anyhow!("migration `{rev}` is missing from the graph"))?;
            for parent in &migration.parents {
                stack.push(parent.clone());
            }
        }
        Ok(seen)
    }

    pub(super) fn valid_revisions(&self) -> String {
        if self.migrations.is_empty() {
            "(none)".to_string()
        } else {
            self.migrations
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        }
    }
}
