//! Spark SQL feature extensions — pre-processors for SQL constructs that
//! DataFusion doesn't parse natively.
//!
//! Supported Spark SQL features:
//!
//! - **LATERAL VIEW**: `SELECT ... FROM t LATERAL VIEW explode(arr) AS col`
//! - **LATERAL VIEW OUTER**: `SELECT ... FROM t LATERAL VIEW OUTER explode(arr) AS col`
//! - **TABLESAMPLE**: `SELECT ... FROM t TABLESAMPLE (10 PERCENT)`
//! - **DESCRIBE TABLE EXTENDED**: `DESCRIBE TABLE EXTENDED t`
//!
//! # Status: not wired into the query path
//!
//! Nothing calls this module. Every one of its public functions, including the
//! aggregate [`preprocess_spark_sql`], has zero callers in the workspace, so no
//! query reaches any of these rewrites. The module is compiled and its tests
//! run, which is why that was not obvious.
//!
//! Whether to wire it or delete it is a product decision about how much Spark
//! surface the SQL front door should carry, so it is recorded in
//! `docs/engineering-log/crate-audit-register.md` rather than decided here. What
//! *is* fixed here is everything that would have been wrong the moment it was
//! wired — the rewrites used to emit SQL naming functions and relations that do
//! not exist:
//!
//! - `LATERAL VIEW explode(x)` produced `explode(x)`, which is neither a
//!   DataFusion function nor registered by this engine. It now emits `UNNEST`,
//!   matching what [`crate::unnest_sql`] produces for the same shape.
//! - `SHOW TBLPROPERTIES` produced a query against
//!   `information_schema.table_properties`, which DataFusion does not define,
//!   and interpolated the table name unescaped. It now reports the feature as
//!   unsupported.
//! - `TRANSFORM` was a documented "rewrite" that returned its input untouched,
//!   so a `TRANSFORM` query would have been passed to DataFusion verbatim. It
//!   now reports the feature as unsupported.

use crate::{SqlError, SqlResult};

// ── LATERAL VIEW ─────────────────────────────────────────────────────────────

/// Detects `LATERAL VIEW` in SQL.
pub fn contains_lateral_view(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
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
/// SELECT id, val FROM t CROSS JOIN LATERAL UNNEST(tags) AS tag
/// ```
///
/// Also handles `LATERAL VIEW OUTER`:
/// ```sql
/// -- Input
/// SELECT id, val FROM t LATERAL VIEW OUTER explode(tags) AS tag
///
/// -- Output
/// SELECT id, val FROM t LEFT JOIN LATERAL UNNEST(tags) AS tag ON TRUE
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
    // ASCII folding: `as_pos` below indexes `trimmed`, not this copy, so the
    // two must have identical byte lengths. Unicode folding does not preserve
    // length (U+FB01 folds to "FI", 3 bytes to 2), which shifts the split and
    // can slice a multi-byte character in half.
    let upper_trimmed = trimmed.to_ascii_uppercase();
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

    // Spark's generator functions are spelled `explode`/`posexplode`; neither
    // exists in DataFusion. `UNNEST` is the equivalent and is what
    // `unnest_sql::rewrite_lateral_unnest` emits for the same shape, so the two
    // Spark-compat paths agree on one target.
    let func_call = &spark_generator_to_unnest(func_call);

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

/// Rewrite a Spark generator call to the equivalent `UNNEST`.
///
/// `explode(arr)` and `posexplode(arr)` both become `UNNEST(arr)`. Anything else
/// is left alone — a user-defined generator may well exist.
fn spark_generator_to_unnest(func_call: &str) -> String {
    let trimmed = func_call.trim();
    for generator in ["explode_outer", "posexplode_outer", "explode", "posexplode"] {
        let prefix_len = generator.len();
        if trimmed.len() > prefix_len
            && trimmed
                .get(..prefix_len)
                .is_some_and(|head| head.eq_ignore_ascii_case(generator))
            && trimmed
                .get(prefix_len..)
                .is_some_and(|r| r.starts_with('('))
        {
            let args = trimmed.get(prefix_len..).unwrap_or("");
            return format!("UNNEST{args}");
        }
    }
    trimmed.to_string()
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
    // ASCII folding: `abs_pos` is applied to `sql`.
    let upper = sql.to_ascii_uppercase();
    let keyword_upper = keyword.to_ascii_uppercase();

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
    sql_words(sql).iter().any(|w| w.upper == "TABLESAMPLE")
}

/// A bare word of SQL text — outside string literals, quoted identifiers and
/// comments — with its byte range in the original text.
pub(crate) struct SqlWord {
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) upper: String,
}

/// Split `sql` into bare words, skipping '…', "…", `…`, -- and /* */.
///
/// The text rewriters here used to search `sql.to_uppercase()` and slice the
/// original with the offsets it gave: that matched keywords inside string
/// literals (rewriting data), and panicked once a character changed byte
/// length when upper-cased. Offsets from this scan index `sql` itself.
pub(crate) fn sql_words(sql: &str) -> Vec<SqlWord> {
    let bytes = sql.as_bytes();
    let at = |i: usize| bytes.get(i).copied();
    let mut words = Vec::new();
    let mut i = 0;
    while let Some(b) = at(i) {
        match b {
            quote @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while let Some(c) = at(i) {
                    if c == quote {
                        // A doubled quote is an escaped quote inside the literal.
                        if at(i + 1) == Some(quote) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            b'-' if at(i + 1) == Some(b'-') => {
                while at(i).is_some_and(|c| c != b'\n') {
                    i += 1;
                }
            }
            b'/' if at(i + 1) == Some(b'*') => {
                i += 2;
                while at(i).is_some() && !(at(i) == Some(b'*') && at(i + 1) == Some(b'/')) {
                    i += 1;
                }
                i += 2;
            }
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                while at(i).is_some_and(|c| c.is_ascii_alphanumeric() || c == b'_') {
                    i += 1;
                }
                // Word bytes are ASCII, so these are char boundaries.
                if let Some(word) = sql.get(start..i) {
                    words.push(SqlWord {
                        start,
                        end: i,
                        upper: word.to_ascii_uppercase(),
                    });
                }
            }
            _ => i += 1,
        }
    }
    words
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

    // Validate TABLESAMPLE syntax: TABLESAMPLE (n PERCENT) or TABLESAMPLE (n ROWS)
    if let Some(word) = sql_words(sql)
        .into_iter()
        .find(|w| w.upper == "TABLESAMPLE")
    {
        let after = sql[word.end..].trim_start();
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
    // Spark's TRANSFORM *clause* pipes rows through an external process and is
    // always `SELECT TRANSFORM(cols) USING '<script>'`. Matching on `TRANSFORM(`
    // alone also matched `transform(array, x -> x * 2)` — the higher-order
    // function, which this crate genuinely supports. Wiring this module with
    // the loose guard turned every `transform()` call into
    // "TRANSFORM has no SQL equivalent"; the checklist and HOF tests caught it.
    //
    // Requiring `USING` after the call distinguishes the clause from the
    // function.
    let upper = sql.to_ascii_uppercase();
    let Some(at) = upper
        .find("TRANSFORM(")
        .or_else(|| upper.find("TRANSFORM ("))
    else {
        return false;
    };
    upper[at..].contains(" USING ")
}

/// Rewrites Spark `TRANSFORM(...)` to standard SQL.
///
/// Spark's `TRANSFORM` is an alias for `SELECT TRANSFORM(...)`. This rewrites
/// it to a DataFusion-compatible form.
pub fn rewrite_transform(sql: &str) -> SqlResult<String> {
    if !contains_transform(sql) {
        return Ok(sql.to_string());
    }
    // This used to return `sql` untouched while documenting itself as a
    // rewrite, so a TRANSFORM query would have reached DataFusion verbatim and
    // failed there with a parse error naming nothing useful. Spark's TRANSFORM
    // pipes rows through an external process; there is no SQL-level equivalent
    // to rewrite it into.
    Err(SqlError::Unsupported {
        feature: "Spark TRANSFORM (rows piped through an external script) has no SQL equivalent"
            .into(),
    })
}

// ── DESCRIBE TABLE EXTENDED ─────────────────────────────────────────────────

/// Detects `DESCRIBE TABLE EXTENDED` in SQL.
pub fn contains_describe_extended(sql: &str) -> bool {
    describe_extended_word(sql).is_some()
}

/// The `EXTENDED` of a statement that *is* `DESC[RIBE] [TABLE] EXTENDED …`.
fn describe_extended_word(sql: &str) -> Option<SqlWord> {
    let mut words = sql_words(sql).into_iter();
    let first = words.next()?;
    if first.upper != "DESC" && first.upper != "DESCRIBE" {
        return None;
    }
    let second = words.next()?;
    let candidate = if second.upper == "TABLE" {
        words.next()?
    } else {
        second
    };
    (candidate.upper == "EXTENDED").then_some(candidate)
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

    let Some(word) = describe_extended_word(sql) else {
        return Ok(sql.to_string());
    };
    let rest = sql[word.end..].trim_start();
    Ok(format!("{}{rest}", &sql[..word.start]).trim().to_string())
}

// ── SHOW TABLE PROPERTIES ────────────────────────────────────────────────────

/// Detects `SHOW TBLPROPERTIES` in SQL.
pub fn contains_show_tblproperties(sql: &str) -> bool {
    show_tblproperties_end(sql).is_some()
}

/// Byte offset just past `SHOW TBLPROPERTIES` as bare words.
fn show_tblproperties_end(sql: &str) -> Option<usize> {
    let words = sql_words(sql);
    words.windows(2).find_map(|pair| match pair {
        [show, tbl] if show.upper == "SHOW" && tbl.upper == "TBLPROPERTIES" => Some(tbl.end),
        _ => None,
    })
}

/// Rewrites `SHOW TBLPROPERTIES <table>` to a query against the catalog.
pub fn rewrite_show_tblproperties(sql: &str) -> SqlResult<String> {
    if !contains_show_tblproperties(sql) {
        return Ok(sql.to_string());
    }

    // Extract table name after SHOW TBLPROPERTIES
    if let Some(end) = show_tblproperties_end(sql) {
        let after = sql[end..].trim_start();
        // Remove trailing semicolon
        let table_name = after.trim_end_matches(';').trim();
        if table_name.is_empty() {
            return Err(SqlError::DataFusion {
                message: "SHOW TBLPROPERTIES requires a table name".into(),
            });
        }
        // `information_schema.table_properties` is not a relation DataFusion
        // defines, so the generated query could only ever fail with "table not
        // found" — and the name was interpolated unescaped on the way there.
        return Err(SqlError::Unsupported {
            feature: format!(
                "SHOW TBLPROPERTIES {table_name}: no table-properties relation is exposed by the \
                 catalog yet"
            ),
        });
    }

    Ok(sql.to_string())
}

// ── Utility ──────────────────────────────────────────────────────────────────

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

    /// M14: "extended" in a string literal is data, not a DESCRIBE keyword.
    #[test]
    fn describe_extended_rewrite_ignores_literals() {
        let sql = "SELECT id FROM timetable WHERE note = 'hours extended today' ORDER BY id DESC";
        assert!(!contains_describe_extended(sql));
        assert_eq!(rewrite_describe_extended(sql).unwrap(), sql);
        assert_eq!(
            rewrite_describe_extended("DESCRIBE TABLE EXTENDED orders").unwrap(),
            "DESCRIBE TABLE orders"
        );
        assert_eq!(
            rewrite_describe_extended("desc extended orders").unwrap(),
            "desc orders"
        );
    }

    /// M15: keyword offsets from `to_uppercase()` do not index the original
    /// text once a character changes length when upper-cased ('ŉ' → "ʼN"),
    /// and keywords inside literals are data.
    #[test]
    fn tablesample_and_tblproperties_are_unicode_and_literal_safe() {
        let sql = "SELECT 'ŉŉŉŉŉŉŉŉŉŉŉŉ' AS s FROM t TABLESAMPLE (1 ROWS)";
        assert_eq!(rewrite_tablesample(sql).unwrap(), sql);
        let sql = "SELECT msg FROM t WHERE msg LIKE '%tablesample%'";
        assert!(!contains_tablesample(sql));
        assert_eq!(rewrite_tablesample(sql).unwrap(), sql);
        let sql = "SELECT 'show tblproperties x' AS s";
        assert!(!contains_show_tblproperties(sql));
        assert!(rewrite_show_tblproperties("SELECT 'ŉŉŉŉ' AS s; SHOW TBLPROPERTIES t").is_err());
    }

    // ── LATERAL VIEW tests ────────────────────────────────────────────────

    #[test]
    fn lateral_view_basic() {
        let sql = "SELECT id, val FROM t LATERAL VIEW explode(tags) AS tag";
        let result = rewrite_lateral_view(sql).unwrap();
        assert!(
            result.contains("CROSS JOIN LATERAL UNNEST(tags) AS tag"),
            "explode must become UNNEST, which DataFusion actually has: {result}"
        );
        assert!(!result.contains("LATERAL VIEW"));
    }

    #[test]
    fn lateral_view_outer() {
        let sql = "SELECT id, val FROM t LATERAL VIEW OUTER explode(tags) AS tag";
        let result = rewrite_lateral_view(sql).unwrap();
        assert!(
            result.contains("LEFT JOIN LATERAL UNNEST(tags) AS tag ON TRUE"),
            "{result}"
        );
        assert!(!result.contains("LATERAL VIEW"));
    }

    /// `posexplode` maps too, and a non-Spark generator is left alone.
    #[test]
    fn only_spark_generators_are_mapped_to_unnest() {
        let mapped =
            rewrite_lateral_view("SELECT a FROM t LATERAL VIEW posexplode(arr) AS p").unwrap();
        assert!(mapped.contains("UNNEST(arr)"), "{mapped}");
        let untouched =
            rewrite_lateral_view("SELECT a FROM t LATERAL VIEW my_gen(arr) AS p").unwrap();
        assert!(
            untouched.contains("my_gen(arr)"),
            "a user-defined generator must survive: {untouched}"
        );
    }

    /// TRANSFORM used to return its input unchanged while documenting itself as
    /// a rewrite, so the query reached DataFusion verbatim.
    #[test]
    fn transform_reports_unsupported_instead_of_passing_through() {
        let sql = "SELECT TRANSFORM(a, b) USING 'script' AS (x, y) FROM t";
        let err = rewrite_transform(sql).expect_err("TRANSFORM has no SQL equivalent");
        assert!(matches!(err, SqlError::Unsupported { .. }), "{err}");
        // A query without TRANSFORM is still untouched.
        assert_eq!(rewrite_transform("SELECT 1").unwrap(), "SELECT 1");
    }

    /// SHOW TBLPROPERTIES targeted `information_schema.table_properties`, which
    /// DataFusion does not define, and interpolated the name unescaped.
    #[test]
    fn show_tblproperties_reports_unsupported() {
        let err = rewrite_show_tblproperties("SHOW TBLPROPERTIES my_table")
            .expect_err("no table-properties relation exists");
        assert!(matches!(err, SqlError::Unsupported { .. }), "{err}");
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
        // This asserted that the output referenced `information_schema` — i.e.
        // it pinned a rewrite to `information_schema.table_properties`, a
        // relation DataFusion does not define. The generated query could only
        // ever fail with "table not found", so the test was pinning the bug.
        let err = rewrite_show_tblproperties("SHOW TBLPROPERTIES my_table")
            .expect_err("no table-properties relation is exposed");
        assert!(err.to_string().contains("my_table"), "{err}");
    }

    #[test]
    fn show_tblproperties_with_semicolon() {
        // The trailing semicolon must still be stripped from the reported name.
        let err = rewrite_show_tblproperties("SHOW TBLPROPERTIES my_table;")
            .expect_err("no table-properties relation is exposed");
        let message = err.to_string();
        assert!(message.contains("my_table"), "{message}");
        assert!(
            !message.contains("my_table;"),
            "semicolon not stripped: {message}"
        );
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
