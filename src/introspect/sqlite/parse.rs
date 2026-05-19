pub(super) fn create_table_body(create_sql: &str) -> Option<&str> {
    match (create_sql.find('('), create_sql.rfind(')')) {
        (Some(start), Some(end)) if start < end => Some(&create_sql[start + 1..end]),
        _ => None,
    }
}

pub(super) fn split_respecting_parens(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth: i32 = 0;
    let mut in_quote = false;
    let mut start = 0;

    for (i, ch) in s.char_indices() {
        if in_quote {
            if ch == '\'' {
                in_quote = false;
            }
            continue;
        }
        match ch {
            '\'' => in_quote = true,
            '(' => depth += 1,
            ')' => depth = (depth - 1).max(0),
            ',' if depth == 0 => {
                result.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    result.push(&s[start..]);
    result
}

pub(super) fn find_keyword_ascii(haystack: &str, needle: &str) -> Option<usize> {
    let needle_bytes = needle.as_bytes();
    haystack
        .as_bytes()
        .windows(needle_bytes.len())
        .position(|window| {
            window
                .iter()
                .zip(needle_bytes.iter())
                .all(|(h, n)| h.eq_ignore_ascii_case(n))
        })
}

pub(super) fn extract_parenthesized_expression(s: &str) -> Option<String> {
    if !s.starts_with('(') {
        return None;
    }
    let mut depth = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(s[1..i].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

pub(super) fn first_token(s: &str) -> &str {
    let s = s.trim();
    for (open, close) in [('"', '"'), ('`', '`'), ('[', ']')] {
        if let Some(stripped) = s.strip_prefix(open) {
            if let Some(end) = stripped.find(close) {
                return &s[..end + 2];
            }
        }
    }
    s.split_whitespace().next().unwrap_or(s)
}

pub(super) fn identifier_matches(token: &str, identifier: &str) -> bool {
    unquote_identifier(token)
        .unwrap_or(token)
        .eq_ignore_ascii_case(identifier)
}

fn unquote_identifier(token: &str) -> Option<&str> {
    let token = token.trim();
    let pairs = [('"', '"'), ('`', '`'), ('[', ']')];
    for (open, close) in pairs {
        if let Some(stripped) = token.strip_prefix(open) {
            if let Some(stripped) = stripped.strip_suffix(close) {
                return Some(stripped);
            }
        }
    }
    None
}
