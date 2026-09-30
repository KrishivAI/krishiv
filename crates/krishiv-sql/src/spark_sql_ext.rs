//! Spark SQL feature extensions — pre-processors for SQL constructs that
//! DataFusion doesn't parse natively.
//!
//! - **TABLESAMPLE**: `SELECT ... FROM t TABLESAMPLE (10 PERCENT)`
//! - **DESCRIBE TABLE EXTENDED**: `DESCRIBE TABLE EXTENDED t`
//! - `TRANSFORM` and `SHOW TBLPROPERTIES` are recognised and reported as
//!   unsupported, rather than passed to DataFusion to fail obscurely.
//!
//! [`preprocess_spark_sql`] runs on every statement the SQL front door plans.
//!
//! `LATERAL VIEW` is not here: it is rewritten on the parsed statement by
//! [`crate::spark_generators`], together with `explode` and the other
//! generators. The text rewrite that used to live in this module emitted a
//! lateral `UNNEST`, which DataFusion 54 plans but cannot execute.

use crate::{SqlError, SqlResult};

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
    fn preprocess_spark_sql_passthrough() {
        let sql = "SELECT 1 + 1";
        let result = preprocess_spark_sql(sql).unwrap();
        assert_eq!(result, sql);
    }
}
