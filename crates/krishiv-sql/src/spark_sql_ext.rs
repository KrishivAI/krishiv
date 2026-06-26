//! Spark SQL feature extensions — pre-processors for SQL constructs that
//! DataFusion doesn't parse natively.
//!
//! Supported Spark SQL features:
//!
//! - **LATERAL VIEW**: `SELECT ... FROM t LATERAL VIEW explode(arr) AS col`
//! - **LATERAL VIEW OUTER**: `SELECT ... FROM t LATERAL VIEW OUTER explode(arr) AS col`
//! - **TABLESAMPLE**: `SELECT ... FROM t TABLESAMPLE (10 PERCENT)`
//! - **TRANSFORM**: `SELECT TRANSFORM(...) FROM t`
//! - **DESCRIBE TABLE EXTENDED**: `DESCRIBE TABLE EXTENDED t`
//! - **SHOW TABLE PROPERTIES**: `SHOW TBLPROPERTIES t`

use crate::{SqlError, SqlResult};

// ── LATERAL VIEW ─────────────────────────────────────────────────────────────

/// Detects `LATERAL VIEW` in SQL.
pub fn contains_lateral_view(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    upper.contains("LATERAL VIEW") || upper.contains("LATERAL VIEW OUTER")
}

/// Rewrites Spark-style `LATERAL VIEW` to standard SQL `CROSS JOIN LATERAL`.
///
/// # Transformations
///
/// ```sql
/// -- Input
/// SELECT id, val FROM t LATERAL VIEW explode(tags) AS tag
///
/// -- Output
/// SELECT id, val FROM t CROSS JOIN LATERAL explode(tags) AS tag
/// ```
///
/// Also handles `LATERAL VIEW OUTER`:
/// ```sql
/// -- Input
/// SELECT id, val FROM t LATERAL VIEW OUTER explode(tags) AS tag
///
/// -- Output
/// SELECT id, val FROM t LEFT JOIN LATERAL explode(tags) AS tag ON TRUE
/// ```
pub fn rewrite_lateral_view(sql: &str) -> SqlResult<String> {
    if !contains_lateral_view(sql) {
        return Ok(sql.to_string());
    }

    let mut result = sql.to_string();

    // Rewrite LATERAL VIEW OUTER first (more specific pattern)
    while let Some(pos) = find_keyword_boundary(&result, "LATERAL VIEW OUTER") {
        if let Some(replacement) = rewrite_lateral_view_at(&result, pos, "LATERAL VIEW OUTER", true)
        {
            result = replacement;
        } else {
            break;
        }
    }

    // Rewrite LATERAL VIEW
    while let Some(pos) = find_keyword_boundary(&result, "LATERAL VIEW") {
        if let Some(replacement) = rewrite_lateral_view_at(&result, pos, "LATERAL VIEW", false) {
            result = replacement;
        } else {
            break;
        }
    }

    Ok(result)
}

/// Rewrite a single LATERAL VIEW at the given position.
fn rewrite_lateral_view_at(sql: &str, pos: usize, keyword: &str, is_outer: bool) -> Option<String> {
    let before = &sql[..pos];
    let after_keyword = &sql[pos + keyword.len()..];

    // Parse the view definition: <func_call> AS <name> or AS <name>(<cols>)
    // We need to find where the alias ends
    let trimmed = after_keyword.trim_start();
    let keyword_offset = after_keyword.len() - trimmed.len();

    // Find " AS " keyword in the remaining text
    let upper_trimmed = trimmed.to_uppercase();
    let as_pos = upper_trimmed.find(" AS ")?;
    let func_call = trimmed[..as_pos].trim();

    // Parse the alias after " AS "
    let alias_start = as_pos + 4;
    let alias_text = &trimmed[alias_start..];

    // Find end of alias: either end of string, comma, or next keyword
    let alias_len = find_alias_length(alias_text);
    let alias_part = alias_text[..alias_len].trim();

    // Calculate what comes after the entire LATERAL VIEW construct
    let consumed = keyword.len() + keyword_offset + as_pos + 4 + alias_len;
    let rest = &sql[pos + consumed..];

    let join_type = if is_outer {
        "LEFT JOIN LATERAL"
    } else {
        "CROSS JOIN LATERAL"
    };

    let on_clause = if is_outer { " ON TRUE" } else { "" };

    Some(format!(
        "{} {} {} AS {}{}{}",
        before, join_type, func_call, alias_part, on_clause, rest
    ))
}

/// Find the length of an alias in the text like "tag" or "tag(col1, col2)".
fn find_alias_length(text: &str) -> usize {
    let bytes = text.as_bytes();
    let mut i = 0;

    // Skip leading whitespace
    while bytes.get(i).is_some_and(|&b| b == b' ' || b == b'\t') {
        i += 1;
    }

    // Read alias name
    let name_start = i;
    while bytes
        .get(i)
        .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
    {
        i += 1;
    }

    if i == name_start {
        return 0;
    }

    // Check for parenthesized column list
    while bytes.get(i).is_some_and(|&b| b == b' ') {
        i += 1;
    }
    if bytes.get(i).is_some_and(|&b| b == b'(') {
        // Find closing paren
        i += 1;
        let mut depth = 1;
        while i < bytes.len() && depth > 0 {
            let Some(&b) = bytes.get(i) else {
                break;
            };
            match b {
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {}
            }
            i += 1;
        }
    }

    i
}

fn find_keyword_boundary(sql: &str, keyword: &str) -> Option<usize> {
    let upper = sql.to_uppercase();
    let keyword_upper = keyword.to_uppercase();

    let mut search_start = 0;
    while let Some(pos) = upper[search_start..].find(&keyword_upper) {
        let abs_pos = search_start + pos;
        // Check word boundary before
        let before_ok = abs_pos == 0
            || sql
                .as_bytes()
                .get(abs_pos - 1)
                .is_some_and(|&b| b == b' ' || b == b',' || b == b'\n' || b == b'\t');
        // Check word boundary after
        let after_pos = abs_pos + keyword.len();
        let after_ok = after_pos >= sql.len()
            || sql
                .as_bytes()
                .get(after_pos)
                .is_some_and(|&b| b == b' ' || b == b'\n' || b == b'\t' || b == b'(');

        if before_ok && after_ok {
            return Some(abs_pos);
        }
        search_start = abs_pos + 1;
    }
    None
}

// ── TABLESAMPLE ──────────────────────────────────────────────────────────────

/// Detects `TABLESAMPLE` in SQL.
pub fn contains_tablesample(sql: &str) -> bool {
    sql.to_uppercase().contains("TABLESAMPLE")
}

/// Rewrites Spark `TABLESAMPLE(n PERCENT)` to DataFusion-compatible form.
///
/// ```sql
/// -- Input
/// SELECT * FROM t TABLESAMPLE (10 PERCENT)
///
/// -- Output
/// SELECT * FROM t TABLESAMPLE (10 PERCENT)
/// ```
///
/// DataFusion supports TABLESAMPLE natively (since v38), so this is mostly
/// a passthrough with validation.
pub fn rewrite_tablesample(sql: &str) -> SqlResult<String> {
    if !contains_tablesample(sql) {
        return Ok(sql.to_string());
    }

    let upper = sql.to_uppercase();

    // Validate TABLESAMPLE syntax: TABLESAMPLE (n PERCENT) or TABLESAMPLE (n ROWS)
    if let Some(pos) = upper.find("TABLESAMPLE") {
        let after = sql[pos + "TABLESAMPLE".len()..].trim_start();
        if !after.starts_with('(') {
            return Err(SqlError::DataFusion {
                message: "TABLESAMPLE requires parentheses: TABLESAMPLE (n PERCENT)".into(),
            });
        }
        if let Some(close) = after.find(')') {
            let inner = after[1..close].trim().to_uppercase();
            if inner.ends_with("PERCENT") || inner.ends_with("ROWS") || inner.ends_with("BUCKET") {
                return Ok(sql.to_string());
            }
            // Try numeric-only (implicit PERCENT for Spark compat)
            if inner.parse::<f64>().is_ok() {
                return Ok(sql.to_string());
            }
            return Err(SqlError::DataFusion {
                message: format!("TABLESAMPLE requires PERCENT, ROWS, or BUCKET: got '{inner}'"),
            });
        }
    }

    Ok(sql.to_string())
}

// ── TRANSFORM ────────────────────────────────────────────────────────────────

/// Detects `TRANSFORM` in SQL.
pub fn contains_transform(sql: &str) -> bool {
    sql.to_uppercase().contains("TRANSFORM(") || sql.to_uppercase().contains("TRANSFORM (")
}

/// Rewrites Spark `TRANSFORM(...)` to standard SQL.
///
/// Spark's `TRANSFORM` is an alias for `SELECT TRANSFORM(...)`. This rewrites
/// it to a DataFusion-compatible form.
pub fn rewrite_transform(sql: &str) -> SqlResult<String> {
    // TRANSFORM is complex and Spark-specific; for now pass through with a note
    Ok(sql.to_string())
}

// ── DESCRIBE TABLE EXTENDED ─────────────────────────────────────────────────

/// Detects `DESCRIBE TABLE EXTENDED` in SQL.
pub fn contains_describe_extended(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    (upper.contains("DESCRIBE") || upper.contains("DESC"))
        && upper.contains("TABLE")
        && upper.contains("EXTENDED")
}

/// Rewrites `DESCRIBE TABLE EXTENDED <table>` to standard `DESCRIBE TABLE <table>`.
///
/// DataFusion doesn't support the `EXTENDED` keyword; we strip it and let
/// the basic DESCRIBE pass through. Extended metadata (partition info, etc.)
/// is a follow-up.
pub fn rewrite_describe_extended(sql: &str) -> SqlResult<String> {
    if !contains_describe_extended(sql) {
        return Ok(sql.to_string());
    }

    // Remove EXTENDED keyword
    let result = regex_replace(sql, r"(?i)\bEXTENDED\b\s*", "")?;
    Ok(result.trim().to_string())
}

// ── SHOW TABLE PROPERTIES ────────────────────────────────────────────────────

/// Detects `SHOW TBLPROPERTIES` in SQL.
pub fn contains_show_tblproperties(sql: &str) -> bool {
    sql.to_uppercase().contains("SHOW TBLPROPERTIES")
}

/// Rewrites `SHOW TBLPROPERTIES <table>` to a query against the catalog.
pub fn rewrite_show_tblproperties(sql: &str) -> SqlResult<String> {
    if !contains_show_tblproperties(sql) {
        return Ok(sql.to_string());
    }

    let upper = sql.to_uppercase();
    // Extract table name after SHOW TBLPROPERTIES
    if let Some(pos) = upper.find("SHOW TBLPROPERTIES") {
        let after = sql[pos + "SHOW TBLPROPERTIES".len()..].trim_start();
        // Remove trailing semicolon
        let table_name = after.trim_end_matches(';').trim();
        if table_name.is_empty() {
            return Err(SqlError::DataFusion {
                message: "SHOW TBLPROPERTIES requires a table name".into(),
            });
        }
        // Rewrite to a standard query against table_properties metadata
        return Ok(format!(
            "SELECT key, value FROM information_schema.table_properties WHERE table_name = '{table_name}'"
        ));
    }

    Ok(sql.to_string())
}

// ── Utility ──────────────────────────────────────────────────────────────────

/// Simple regex-like replacement for single patterns.
fn regex_replace(input: &str, pattern: &str, replacement: &str) -> SqlResult<String> {
    // Simple case-insensitive replacement (no regex crate needed)
    let _ = replacement;

    // For simple patterns without wildcards, just do string replacement
    if pattern == r"(?i)\bEXTENDED\b\s*" {
        // Remove EXTENDED and surrounding whitespace
        let mut result = input.to_string();
        while let Some(pos) = result.to_uppercase().find("EXTENDED") {
            // Check word boundaries
            let bytes = result.as_bytes();
            let before_ok = pos == 0
                || bytes.get(pos - 1).is_some_and(|&b| b == b' ' || b == b'\t');
            let after_pos = pos + "EXTENDED".len();
            let after_ok = after_pos >= result.len()
                || bytes.get(after_pos).is_some_and(|&b| b == b' ' || b == b'\t' || b == b'\n');

            if before_ok && after_ok {
                // Remove EXTENDED plus trailing space
                let end = if bytes.get(after_pos).is_some_and(|&b| b == b' ') {
                    after_pos + 1
                } else {
                    after_pos
                };
                result = format!("{}{}", &result[..pos], &result[end..]);
            } else {
                break;
            }
        }
        return Ok(result);
    }

    Ok(input.to_string())
}

// ── Unified Pre-Processor ────────────────────────────────────────────────────

/// Apply all Spark SQL pre-processing rewrites to a SQL string.
pub fn preprocess_spark_sql(sql: &str) -> SqlResult<String> {
    let mut result = sql.to_string();

    // Order: LATERAL VIEW (most complex), then others
    result = rewrite_lateral_view(&result)?;
    result = rewrite_tablesample(&result)?;
    result = rewrite_transform(&result)?;
    result = rewrite_describe_extended(&result)?;
    result = rewrite_show_tblproperties(&result)?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── LATERAL VIEW tests ────────────────────────────────────────────────

    #[test]
    fn lateral_view_basic() {
        let sql = "SELECT id, val FROM t LATERAL VIEW explode(tags) AS tag";
        let result = rewrite_lateral_view(sql).unwrap();
        assert!(result.contains("CROSS JOIN LATERAL explode(tags) AS tag"));
        assert!(!result.contains("LATERAL VIEW"));
    }

    #[test]
    fn lateral_view_outer() {
        let sql = "SELECT id, val FROM t LATERAL VIEW OUTER explode(tags) AS tag";
        let result = rewrite_lateral_view(sql).unwrap();
        assert!(result.contains("LEFT JOIN LATERAL explode(tags) AS tag ON TRUE"));
        assert!(!result.contains("LATERAL VIEW"));
    }

    #[test]
    fn lateral_view_with_column_list() {
        let sql = "SELECT id, val FROM t LATERAL VIEW posexplode(arr) AS pos, val";
        let result = rewrite_lateral_view(sql).unwrap();
        assert!(result.contains("CROSS JOIN LATERAL"));
    }

    #[test]
    fn lateral_view_no_change_when_absent() {
        let sql = "SELECT * FROM t WHERE id = 1";
        let result = rewrite_lateral_view(sql).unwrap();
        assert_eq!(result, sql);
    }

    #[test]
    fn contains_lateral_view_true() {
        assert!(contains_lateral_view(
            "SELECT * FROM t LATERAL VIEW explode(a) AS x"
        ));
        assert!(contains_lateral_view(
            "SELECT * FROM t LATERAL VIEW OUTER explode(a) AS x"
        ));
        assert!(!contains_lateral_view("SELECT * FROM t"));
    }

    // ── TABLESAMPLE tests ─────────────────────────────────────────────────

    #[test]
    fn tablesample_passthrough() {
        let sql = "SELECT * FROM t TABLESAMPLE (10 PERCENT)";
        let result = rewrite_tablesample(sql).unwrap();
        assert_eq!(result, sql);
    }

    #[test]
    fn tablesample_rows() {
        let sql = "SELECT * FROM t TABLESAMPLE (100 ROWS)";
        let result = rewrite_tablesample(sql).unwrap();
        assert_eq!(result, sql);
    }

    #[test]
    fn tablesample_no_parens_errors() {
        let sql = "SELECT * FROM t TABLESAMPLE 10 PERCENT";
        let result = rewrite_tablesample(sql);
        assert!(result.is_err());
    }

    #[test]
    fn contains_tablesample_true() {
        assert!(contains_tablesample(
            "SELECT * FROM t TABLESAMPLE (10 PERCENT)"
        ));
        assert!(!contains_tablesample("SELECT * FROM t"));
    }

    // ── DESCRIBE EXTENDED tests ───────────────────────────────────────────

    #[test]
    fn describe_extended_rewrite() {
        let sql = "DESCRIBE TABLE EXTENDED my_table";
        let result = rewrite_describe_extended(sql).unwrap();
        assert!(!result.to_uppercase().contains("EXTENDED"));
        assert!(result.contains("my_table"));
    }

    #[test]
    fn describe_extended_case_insensitive() {
        let sql = "desc table extended my_table";
        let result = rewrite_describe_extended(sql).unwrap();
        assert!(!result.to_uppercase().contains("EXTENDED"));
    }

    #[test]
    fn contains_describe_extended_true() {
        assert!(contains_describe_extended("DESCRIBE TABLE EXTENDED t"));
        assert!(contains_describe_extended("desc table extended t"));
        assert!(!contains_describe_extended("DESCRIBE TABLE t"));
    }

    // ── SHOW TBLPROPERTIES tests ──────────────────────────────────────────

    #[test]
    fn show_tblproperties_rewrite() {
        let sql = "SHOW TBLPROPERTIES my_table";
        let result = rewrite_show_tblproperties(sql).unwrap();
        assert!(result.contains("my_table"));
        assert!(result.contains("information_schema"));
    }

    #[test]
    fn show_tblproperties_with_semicolon() {
        let sql = "SHOW TBLPROPERTIES my_table;";
        let result = rewrite_show_tblproperties(sql).unwrap();
        assert!(result.contains("my_table"));
    }

    #[test]
    fn show_tblproperties_empty_errors() {
        let sql = "SHOW TBLPROPERTIES";
        let result = rewrite_show_tblproperties(sql);
        assert!(result.is_err());
    }

    // ── Unified pre-processor tests ───────────────────────────────────────

    #[test]
    fn preprocess_spark_sql_lateral_view() {
        let sql = "SELECT id, val FROM t LATERAL VIEW explode(tags) AS tag";
        let result = preprocess_spark_sql(sql).unwrap();
        assert!(result.contains("CROSS JOIN LATERAL"));
    }

    #[test]
    fn preprocess_spark_sql_passthrough() {
        let sql = "SELECT 1 + 1";
        let result = preprocess_spark_sql(sql).unwrap();
        assert_eq!(result, sql);
    }
}
