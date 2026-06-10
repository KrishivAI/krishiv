//! E5.4 — PIVOT / UNPIVOT SQL macro rewrite layer.
//!
//! DataFusion does not parse `PIVOT` or `UNPIVOT` natively. This module rewrites
//! those constructs into equivalent standard SQL before passing the query to DataFusion.
//!
//! # PIVOT rewrite
//!
//! ```sql
//! SELECT * FROM sales
//! PIVOT (SUM(amount) FOR category IN ('food', 'tech', 'clothing'))
//! ```
//! becomes:
//! ```sql
//! SELECT
//!   SUM(CASE WHEN category = 'food' THEN amount END) AS "food",
//!   SUM(CASE WHEN category = 'tech' THEN amount END) AS "tech",
//!   SUM(CASE WHEN category = 'clothing' THEN amount END) AS "clothing"
//! FROM sales
//! ```
//!
//! # UNPIVOT rewrite
//!
//! ```sql
//! SELECT * FROM monthly
//! UNPIVOT (value FOR month IN (jan, feb, mar))
//! ```
//! becomes a UNION ALL of individual SELECT statements:
//! ```sql
//! SELECT 'jan' AS month, jan AS value FROM monthly
//! UNION ALL
//! SELECT 'feb' AS month, feb AS value FROM monthly
//! UNION ALL
//! SELECT 'mar' AS month, mar AS value FROM monthly
//! ```

use crate::{SqlError, SqlResult};

// ── Detection ─────────────────────────────────────────────────────────────────

/// Returns `true` if `sql` contains a `PIVOT` clause (case-insensitive).
pub fn contains_pivot(sql: &str) -> bool {
    sql.to_ascii_uppercase().contains(" PIVOT (") || sql.to_ascii_uppercase().contains(" PIVOT(")
}

/// Returns `true` if `sql` contains an `UNPIVOT` clause (case-insensitive).
pub fn contains_unpivot(sql: &str) -> bool {
    sql.to_ascii_uppercase().contains(" UNPIVOT (")
        || sql.to_ascii_uppercase().contains(" UNPIVOT(")
}

// ── PIVOT rewrite ─────────────────────────────────────────────────────────────

/// Parsed representation of a PIVOT clause.
#[derive(Debug, Clone)]
pub struct PivotClause {
    /// Aggregate function name (e.g. "SUM", "COUNT", "MAX").
    pub agg_fn: String,
    /// Column to aggregate (e.g. "amount").
    pub agg_column: String,
    /// Pivot dimension column (e.g. "category").
    pub for_column: String,
    /// Values to pivot into columns.
    pub in_values: Vec<String>,
    /// Source table or subquery (the part before PIVOT).
    pub source: String,
}

/// Parse a simple `SELECT * FROM <source> PIVOT (<agg>(<col>) FOR <dim> IN (<vals>))` statement.
///
/// Returns `Ok(None)` when the SQL does not contain a PIVOT clause.
pub fn parse_pivot(sql: &str) -> SqlResult<Option<PivotClause>> {
    let upper = sql.to_ascii_uppercase();
    let pivot_kw = " PIVOT (";
    let pivot_pos = match upper.find(pivot_kw) {
        Some(p) => p,
        None => {
            // Try without space before paren.
            match upper.find(" PIVOT(") {
                Some(p) => p,
                None => return Ok(None),
            }
        }
    };

    let source = sql[..pivot_pos].trim().to_owned();

    // Find the matching closing paren.
    let body_start = pivot_pos + pivot_kw.len();
    let body_end = find_closing_paren(&sql[body_start..]).ok_or_else(|| SqlError::Unsupported {
        feature: "PIVOT: unmatched parenthesis".into(),
    })?
        + body_start;

    let body = sql[body_start..body_end].trim();
    let body_upper = body.to_ascii_uppercase();

    // Parse: AGG(col) FOR dim IN (v1, v2, ...)
    let for_pos = body_upper.find(" FOR ").ok_or_else(|| SqlError::Unsupported {
        feature: "PIVOT: missing FOR keyword".into(),
    })?;
    let in_pos = body_upper.find(" IN (").ok_or_else(|| SqlError::Unsupported {
        feature: "PIVOT: missing IN keyword".into(),
    })?;

    let agg_expr = body[..for_pos].trim();
    let for_column = body[for_pos + 5..in_pos].trim().to_owned();

    // Parse AGG(col)
    let lp = agg_expr.find('(').ok_or_else(|| SqlError::Unsupported {
        feature: "PIVOT: aggregation must be in the form AGG(column)".into(),
    })?;
    let rp = agg_expr.rfind(')').ok_or_else(|| SqlError::Unsupported {
        feature: "PIVOT: aggregation must end with ')'".into(),
    })?;
    let agg_fn = agg_expr[..lp].trim().to_owned();
    let agg_column = agg_expr[lp + 1..rp].trim().to_owned();

    // Parse IN (v1, v2, ...)
    let in_list_start = in_pos + 5;
    let in_list_end = body[in_list_start..]
        .find(')')
        .ok_or_else(|| SqlError::Unsupported {
            feature: "PIVOT: IN list is not closed".into(),
        })?
        + in_list_start;
    let in_list = &body[in_list_start..in_list_end];

    let in_values: Vec<String> =
        in_list.split(',').map(|v| v.trim().to_owned()).filter(|v| !v.is_empty()).collect();

    if in_values.is_empty() {
        return Err(SqlError::Unsupported {
            feature: "PIVOT: IN list must contain at least one value".into(),
        });
    }

    Ok(Some(PivotClause { agg_fn, agg_column, for_column, in_values, source }))
}

/// Rewrite a PIVOT statement to equivalent `CASE WHEN` SQL.
///
/// Returns the original `sql` unchanged when no PIVOT clause is found.
pub fn rewrite_pivot(sql: &str) -> SqlResult<String> {
    let Some(pivot) = parse_pivot(sql)? else {
        return Ok(sql.to_owned());
    };

    let mut cols = Vec::with_capacity(pivot.in_values.len());
    for val in &pivot.in_values {
        // Strip surrounding quotes from the alias name for readability.
        let alias = val.trim_matches('\'').trim_matches('"');
        cols.push(format!(
            "{}(CASE WHEN {} = {} THEN {} END) AS \"{}\"",
            pivot.agg_fn, pivot.for_column, val, pivot.agg_column, alias,
        ));
    }

    // Strip the leading SELECT ... FROM from source to get just the FROM clause.
    let from_clause = strip_select_star_prefix(&pivot.source);

    Ok(format!("SELECT {} FROM {}", cols.join(", "), from_clause))
}

// ── UNPIVOT rewrite ───────────────────────────────────────────────────────────

/// Parsed representation of an UNPIVOT clause.
#[derive(Debug, Clone)]
pub struct UnpivotClause {
    /// Output column that receives the values.
    pub value_column: String,
    /// Output column that receives the pivot dimension name.
    pub name_column: String,
    /// Source columns to unpivot.
    pub in_columns: Vec<String>,
    /// Source table or subquery.
    pub source: String,
}

/// Parse a simple `SELECT * FROM <source> UNPIVOT (<val_col> FOR <name_col> IN (<cols>))`.
///
/// Returns `Ok(None)` when the SQL does not contain an UNPIVOT clause.
pub fn parse_unpivot(sql: &str) -> SqlResult<Option<UnpivotClause>> {
    let upper = sql.to_ascii_uppercase();
    let kw = " UNPIVOT (";
    let kw_short = " UNPIVOT(";
    let unpivot_pos = match upper.find(kw) {
        Some(p) => p,
        None => match upper.find(kw_short) {
            Some(p) => p,
            None => return Ok(None),
        },
    };

    let source = sql[..unpivot_pos].trim().to_owned();
    let body_start = unpivot_pos
        + sql[unpivot_pos..].find('(').ok_or_else(|| SqlError::Unsupported {
            feature: "UNPIVOT: missing opening parenthesis".into(),
        })?
        + 1;
    let body_end = find_closing_paren(&sql[body_start..]).ok_or_else(|| {
        SqlError::Unsupported { feature: "UNPIVOT: unmatched parenthesis".into() }
    })?
        + body_start;
    let body = sql[body_start..body_end].trim();
    let body_upper = body.to_ascii_uppercase();

    let for_pos = body_upper.find(" FOR ").ok_or_else(|| SqlError::Unsupported {
        feature: "UNPIVOT: missing FOR keyword".into(),
    })?;
    let in_pos = body_upper.find(" IN (").ok_or_else(|| SqlError::Unsupported {
        feature: "UNPIVOT: missing IN keyword".into(),
    })?;

    let value_column = body[..for_pos].trim().to_owned();
    let name_column = body[for_pos + 5..in_pos].trim().to_owned();

    let in_list_start = in_pos + 5;
    let in_list_end = body[in_list_start..].find(')').ok_or_else(|| SqlError::Unsupported {
        feature: "UNPIVOT: IN list is not closed".into(),
    })?
        + in_list_start;
    let in_list = &body[in_list_start..in_list_end];

    let in_columns: Vec<String> =
        in_list.split(',').map(|v| v.trim().to_owned()).filter(|v| !v.is_empty()).collect();

    if in_columns.is_empty() {
        return Err(SqlError::Unsupported {
            feature: "UNPIVOT: IN list must contain at least one column".into(),
        });
    }

    Ok(Some(UnpivotClause { value_column, name_column, in_columns, source }))
}

/// Rewrite an UNPIVOT statement to a `UNION ALL` of SELECT statements.
///
/// Returns the original `sql` unchanged when no UNPIVOT clause is found.
pub fn rewrite_unpivot(sql: &str) -> SqlResult<String> {
    let Some(unpivot) = parse_unpivot(sql)? else {
        return Ok(sql.to_owned());
    };

    let from_clause = strip_select_star_prefix(&unpivot.source);

    let mut branches = Vec::with_capacity(unpivot.in_columns.len());
    for col in &unpivot.in_columns {
        branches.push(format!(
            "SELECT '{}' AS {}, {} AS {} FROM {}",
            col, unpivot.name_column, col, unpivot.value_column, from_clause,
        ));
    }

    Ok(branches.join(" UNION ALL "))
}

/// Entry point: rewrite PIVOT or UNPIVOT if present, otherwise return unchanged.
pub fn rewrite_pivot_unpivot(sql: &str) -> SqlResult<String> {
    if contains_pivot(sql) {
        rewrite_pivot(sql)
    } else if contains_unpivot(sql) {
        rewrite_unpivot(sql)
    } else {
        Ok(sql.to_owned())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Find the index of the closing `)` matching the first `(` already consumed.
///
/// `s` starts *after* the opening `(`. Returns the byte index of `)` relative
/// to `s`.
fn find_closing_paren(s: &str) -> Option<usize> {
    let mut depth = 1usize;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Strip a leading `SELECT * FROM ` or `SELECT … FROM ` prefix from the source
/// fragment so the caller can use it directly as a FROM clause.
fn strip_select_star_prefix(s: &str) -> &str {
    let upper = s.to_ascii_uppercase();
    if let Some(from_pos) = upper.rfind(" FROM ") {
        s[from_pos + 6..].trim()
    } else {
        s.trim()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── PIVOT ──────────────────────────────────────────────────────────────────

    #[test]
    fn detects_pivot() {
        assert!(contains_pivot("SELECT * FROM t PIVOT (SUM(x) FOR y IN ('a'))"));
        assert!(!contains_pivot("SELECT * FROM t WHERE x = 1"));
    }

    #[test]
    fn parses_pivot() {
        let sql = "SELECT * FROM sales PIVOT (SUM(amount) FOR category IN ('food', 'tech'))";
        let pivot = parse_pivot(sql).unwrap().unwrap();
        assert_eq!(pivot.agg_fn, "SUM");
        assert_eq!(pivot.agg_column, "amount");
        assert_eq!(pivot.for_column, "category");
        assert_eq!(pivot.in_values, vec!["'food'", "'tech'"]);
    }

    #[test]
    fn rewrites_pivot_to_case_when() {
        let sql = "SELECT * FROM sales PIVOT (SUM(amount) FOR category IN ('food', 'tech'))";
        let rewritten = rewrite_pivot(sql).unwrap();
        assert!(rewritten.to_ascii_uppercase().contains("CASE WHEN"));
        assert!(rewritten.to_ascii_uppercase().contains("SUM("));
        assert!(rewritten.contains("'food'"));
        assert!(rewritten.contains("'tech'"));
        assert!(!rewritten.to_ascii_uppercase().contains("PIVOT"));
    }

    #[test]
    fn pivot_rewrite_generates_correct_aliases() {
        let sql = "SELECT * FROM t PIVOT (MAX(val) FOR dim IN ('x', 'y'))";
        let rewritten = rewrite_pivot(sql).unwrap();
        assert!(rewritten.contains("\"x\""));
        assert!(rewritten.contains("\"y\""));
    }

    #[test]
    fn returns_unchanged_when_no_pivot() {
        let sql = "SELECT * FROM t WHERE x = 1";
        let result = rewrite_pivot(sql).unwrap();
        assert_eq!(result, sql);
    }

    #[test]
    fn rejects_pivot_without_for() {
        let sql = "SELECT * FROM t PIVOT (SUM(x) IN ('a'))";
        let err = parse_pivot(sql).unwrap_err();
        assert!(matches!(err, SqlError::Unsupported { .. }));
    }

    // ── UNPIVOT ────────────────────────────────────────────────────────────────

    #[test]
    fn detects_unpivot() {
        assert!(contains_unpivot("SELECT * FROM t UNPIVOT (val FOR month IN (jan, feb))"));
        assert!(!contains_unpivot("SELECT * FROM t WHERE x = 1"));
    }

    #[test]
    fn parses_unpivot() {
        let sql = "SELECT * FROM monthly UNPIVOT (value FOR month IN (jan, feb, mar))";
        let unpivot = parse_unpivot(sql).unwrap().unwrap();
        assert_eq!(unpivot.value_column, "value");
        assert_eq!(unpivot.name_column, "month");
        assert_eq!(unpivot.in_columns, vec!["jan", "feb", "mar"]);
    }

    #[test]
    fn rewrites_unpivot_to_union_all() {
        let sql = "SELECT * FROM monthly UNPIVOT (value FOR month IN (jan, feb, mar))";
        let rewritten = rewrite_unpivot(sql).unwrap();
        assert!(rewritten.to_ascii_uppercase().contains("UNION ALL"));
        assert!(rewritten.contains("'jan'"));
        assert!(rewritten.contains("'feb'"));
        assert!(rewritten.contains("'mar'"));
        assert!(!rewritten.to_ascii_uppercase().contains("UNPIVOT"));
    }

    #[test]
    fn returns_unchanged_when_no_unpivot() {
        let sql = "SELECT * FROM t";
        let result = rewrite_unpivot(sql).unwrap();
        assert_eq!(result, sql);
    }

    #[test]
    fn rewrite_pivot_unpivot_dispatches_correctly() {
        let pivot_sql = "SELECT * FROM t PIVOT (SUM(v) FOR k IN ('a', 'b'))";
        let result = rewrite_pivot_unpivot(pivot_sql).unwrap();
        assert!(result.to_ascii_uppercase().contains("CASE WHEN"));

        let unpivot_sql = "SELECT * FROM t UNPIVOT (val FOR month IN (jan, feb))";
        let result2 = rewrite_pivot_unpivot(unpivot_sql).unwrap();
        assert!(result2.to_ascii_uppercase().contains("UNION ALL"));

        let plain = "SELECT * FROM t";
        let result3 = rewrite_pivot_unpivot(plain).unwrap();
        assert_eq!(result3, plain);
    }
}
