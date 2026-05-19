use std::path::PathBuf;

#[derive(Debug, Clone)]
pub(super) struct MigrationFile {
    pub(super) revision: String,
    pub(super) parents: Vec<String>,
    pub(super) description: String,
    pub(super) path: PathBuf,
    pub(super) pre_sql: String,
    pub(super) up_sql: String,
    pub(super) post_sql: String,
    pub(super) pre_down_sql: String,
    pub(super) down_sql: Option<String>,
    pub(super) post_down_sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum MigrationSection {
    Pre,
    Up,
    Post,
    PostDown,
    Down,
    PreDown,
}

impl MigrationSection {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Pre => "PRE",
            Self::Up => "UP",
            Self::Post => "POST",
            Self::PostDown => "POST DOWN",
            Self::Down => "DOWN",
            Self::PreDown => "PRE DOWN",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum MigrationDirection {
    Up,
    Down,
}

impl MigrationDirection {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Up => "UP",
            Self::Down => "DOWN",
        }
    }
}
