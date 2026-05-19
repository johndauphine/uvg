use crate::dialect::Dialect;
use crate::output::Change;

use super::reverse::reverse_change_sql;

pub(super) fn render_up_sql(changes: &[Change]) -> String {
    changes
        .iter()
        .map(|change| change.sql.as_str())
        .collect::<Vec<_>>()
        .join("\n\n")
}

pub(super) fn render_down_sql(changes: &[Change], target_dialect: Dialect) -> String {
    let mut reversed = changes
        .iter()
        .rev()
        .map(|change| reverse_change_sql(&change.sql, target_dialect))
        .collect::<Vec<_>>()
        .join("\n\n");
    if reversed.is_empty() {
        reversed.push_str("-- No reverse SQL generated.");
    }
    reversed
}
