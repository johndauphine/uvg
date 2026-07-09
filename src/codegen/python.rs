//! Python-side rendering helpers shared by the tables and declarative
//! generators: string-literal formatting, kwargs rendering, and enum class
//! generation.

use crate::dialect::Dialect;

use super::sql_text::{strip_mssql_parens, strip_pg_typecast};

/// Format a server_default expression. Wraps raw SQL in text('...').
/// Delegates escaping to format_python_string_literal for proper handling of
/// backslashes, newlines, and quote characters.
pub fn format_server_default(default: &str, dialect: Dialect) -> String {
    let cleaned = match dialect {
        Dialect::Postgres => strip_pg_typecast(default),
        Dialect::Mssql => strip_mssql_parens(default),
        Dialect::Mysql | Dialect::Sqlite => default.trim(),
    };

    format!("text({})", format_python_string_literal(cleaned))
}

/// Quote a list of column names for use in constraint arguments.
pub fn quote_constraint_columns(cols: &[String]) -> Vec<String> {
    cols.iter().map(|c| format!("'{c}'")).collect()
}

/// Escape single quotes in a string for Python string literals.
pub fn escape_python_string(s: &str) -> String {
    s.replace('\'', "\\'")
}

/// Format FK option kwargs (ondelete, onupdate) for ForeignKeyConstraint.
/// Returns empty string if both rules are NO ACTION (the default).
pub fn format_fk_options(fk: &crate::schema::ForeignKeyInfo) -> String {
    let mut opts = Vec::new();
    if fk.delete_rule != "NO ACTION" {
        opts.push(format!("ondelete='{}'", fk.delete_rule));
    }
    if fk.update_rule != "NO ACTION" {
        opts.push(format!("onupdate='{}'", fk.update_rule));
    }
    if opts.is_empty() {
        String::new()
    } else {
        format!(", {}", opts.join(", "))
    }
}

/// Format a string as a Python string literal, choosing quote style and escaping properly.
/// Uses double quotes if the string contains single quotes (and no double quotes),
/// otherwise uses single quotes with escaping. Newlines are always escaped.
pub fn format_python_string_literal(s: &str) -> String {
    let escaped = s.replace('\\', "\\\\").replace('\n', "\\n");
    if escaped.contains('\'') && !escaped.contains('"') {
        format!("\"{}\"", escaped)
    } else {
        format!("'{}'", escaped.replace('\'', "\\'"))
    }
}

/// Format index kwargs as a string of ", key='value'" pairs.
/// Empty values are skipped.
pub fn format_index_kwargs(kwargs: &std::collections::BTreeMap<String, String>) -> String {
    kwargs
        .iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(k, v)| format!(", {k}={}", format_python_string_literal(v)))
        .collect()
}

/// Generate a Python enum class from an EnumInfo.
/// Returns the class definition string (e.g. "class StatusEnum(str, enum.Enum):\n    ...").
pub fn generate_enum_class(enum_info: &crate::schema::EnumInfo) -> String {
    use heck::ToUpperCamelCase;

    let class_name = enum_info.name.to_upper_camel_case();
    let mut lines = Vec::new();
    lines.push(format!("class {class_name}(str, enum.Enum):"));
    for value in &enum_info.values {
        // Sanitize member name: uppercase, replace non-identifier chars, prefix if starts with digit
        let mut member_name: String = value
            .to_uppercase()
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        if member_name.starts_with(|c: char| c.is_ascii_digit()) {
            member_name = format!("_{member_name}");
        }
        if member_name.is_empty() {
            member_name = "_".to_string();
        }
        lines.push(format!(
            "    {member_name} = {}",
            format_python_string_literal(value)
        ));
    }
    lines.join("\n")
}

/// Get the Python class name for an enum.
pub fn enum_class_name(enum_name: &str) -> String {
    use heck::ToUpperCamelCase;
    enum_name.to_upper_camel_case()
}

/// Structured output of a Python code generator (#116): a shared prelude
/// plus one named block per model. Both output modes derive from this —
/// single-file rendering concatenates, `--split-tables` writes one file per
/// model — so the file layout comes from generator structure rather than
/// re-parsing rendered text with separator heuristics.
pub struct PythonOutput {
    /// Imports, enum classes, and Base/metadata — everything that renders
    /// ahead of the first model in single-file mode and lands in `base.py`
    /// when splitting. Internal blocks are separated by blank lines.
    pub prelude: String,
    /// `(module_name, code)` per model class / `Table()` assignment,
    /// in generator order.
    pub models: Vec<(String, String)>,
    /// Separator between model blocks in single-file mode: the declarative
    /// generator uses two blank lines (PEP 8 top-level), tables uses one.
    pub separator: &'static str,
}

impl PythonOutput {
    /// Render the single-file output.
    pub fn render(&self) -> String {
        let mut out = self.prelude.clone();
        for (_, code) in &self.models {
            out.push_str(self.separator);
            out.push_str(code);
        }
        out.push('\n');
        out
    }

    /// Render the split layout: `base.py` (prelude), one file per model
    /// (prefixed with `from .base import *` so each is independently
    /// importable), and an `__init__.py` re-exporting everything.
    pub fn split(&self) -> Vec<(String, String)> {
        let mut files: Vec<(String, String)> = Vec::new();

        let base_blocks: Vec<&str> = self
            .prelude
            .split("\n\n")
            .map(str::trim)
            .filter(|block| !block.is_empty())
            .collect();
        files.push(("base.py".to_string(), base_blocks.join("\n\n") + "\n"));

        for (module, code) in &self.models {
            files.push((
                format!("{module}.py"),
                format!("from .base import *  # noqa\n\n{}\n", code.trim()),
            ));
        }

        let mut init_lines = vec!["from .base import *  # noqa".to_string()];
        for (module, _) in &self.models {
            init_lines.push(format!("from .{module} import *  # noqa"));
        }
        init_lines.push(String::new());
        files.push(("__init__.py".to_string(), init_lines.join("\n")));

        files
    }
}
