//! E5.2 — LATERAL / UNNEST SQL pre-processing.
//!
//! DataFusion 53 supports `UNNEST` inside `SELECT` and `FROM` clauses for
//! fixed-size arrays. This module provides:
//!
//! 1. **Detection**: identify UNNEST calls in SQL text before passing to DataFusion.
//! 2. **Rewriter**: normalise the common `LATERAL UNNEST` idiom to a canonical
//!    form that DataFusion can plan (`CROSS JOIN UNNEST`).
//! 3. **NodeOp builder**: return a `NodeOp::Unnest` descriptor so the Krishiv
//!    plan layer can record the unnest operator.

use krishiv_plan::NodeOp;

// ── Detection ─────────────────────────────────────────────────────────────────

/// Returns `true` if `sql` contains an `UNNEST` call (case-insensitive).
pub fn contains_unnest(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    upper.contains("UNNEST(") || upper.contains("UNNEST (")
}

/// Returns `true` if `sql` contains a `LATERAL` keyword (case-insensitive).
pub fn contains_lateral(sql: &str) -> bool {
    sql.to_ascii_uppercase().contains(" LATERAL ")
}

// ── Rewriter ──────────────────────────────────────────────────────────────────

/// Rewrite `LATERAL UNNEST(...)` idioms to a form DataFusion understands.
///
/// Normalises:
/// ```sql
/// SELECT * FROM t, LATERAL UNNEST(t.tags) AS tag(value)
/// ```
/// to:
/// ```sql
/// SELECT * FROM t CROSS JOIN UNNEST(t.tags) AS tag(value)
/// ```
///
/// Queries that do not contain `LATERAL UNNEST` are returned unchanged.
///
/// # Limitations
/// Only handles the common single-table `LATERAL UNNEST` idiom. Complex uses
/// (multiple LATERAL joins, LATERAL subqueries) are passed through to DataFusion
/// which will either handle them or return a clear error.
pub fn rewrite_lateral_unnest(sql: &str) -> String {
    if !contains_lateral(sql) || !contains_unnest(sql) {
        return sql.to_owned();
    }

    let patterns: &[(&str, &str)] = &[
        (", LATERAL UNNEST(", " CROSS JOIN UNNEST("),
        (",LATERAL UNNEST(", " CROSS JOIN UNNEST("),
        (" LATERAL UNNEST(", " CROSS JOIN UNNEST("),
    ];

    let mut result = sql.to_owned();
    for (from, to) in patterns {
        let upper_from = from.to_ascii_uppercase();
        // Compute the uppercase view once per pattern pass; track the search
        // position to avoid re-scanning the prefix on each replacement.
        let mut search_start = 0;
        loop {
            let upper_result = result[search_start..].to_ascii_uppercase();
            match upper_result.find(&upper_from) {
                Some(rel_pos) => {
                    let pos = search_start + rel_pos;
                    result.replace_range(pos..pos + from.len(), to);
                    search_start = pos + to.len();
                }
                None => break,
            }
        }
    }
    result
}

// ── NodeOp builder ────────────────────────────────────────────────────────────

/// Build a `NodeOp::Unnest` descriptor.
///
/// * `array_column` — the source column that contains the array.
/// * `output_column` — the name of the column produced for each element.
/// * `with_ordinality` — when `true` an extra `ordinality` column (`u64`) is
///   appended with the 1-based position of each element.
pub fn build_unnest_op(
    array_column: impl Into<String>,
    output_column: impl Into<String>,
    with_ordinality: bool,
) -> NodeOp {
    NodeOp::Unnest {
        array_column: array_column.into(),
        output_column: output_column.into(),
        with_ordinality,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_unnest_call() {
        assert!(contains_unnest("SELECT UNNEST(tags) FROM t"));
        assert!(contains_unnest("SELECT * FROM t, LATERAL UNNEST(t.ids) AS id(v)"));
        assert!(!contains_unnest("SELECT * FROM t WHERE x = 1"));
    }

    #[test]
    fn detects_lateral() {
        assert!(contains_lateral("SELECT * FROM t, LATERAL UNNEST(t.ids) AS id(v)"));
        assert!(!contains_lateral("SELECT * FROM t CROSS JOIN UNNEST(t.ids)"));
    }

    #[test]
    fn rewrites_lateral_unnest_with_comma() {
        let sql = "SELECT * FROM t, LATERAL UNNEST(t.tags) AS tag(value)";
        let rewritten = rewrite_lateral_unnest(sql);
        assert!(!rewritten.to_ascii_uppercase().contains(" LATERAL "));
        assert!(rewritten.to_ascii_uppercase().contains("CROSS JOIN UNNEST"));
    }

    #[test]
    fn rewrites_lateral_unnest_preserves_alias() {
        let sql = "SELECT t.id, tag.value FROM t, LATERAL UNNEST(t.tags) AS tag(value)";
        let rewritten = rewrite_lateral_unnest(sql);
        assert!(rewritten.contains("tag(value)"), "alias preserved: {rewritten}");
    }

    #[test]
    fn passthrough_plain_unnest() {
        let sql = "SELECT id, UNNEST(tags) AS tag FROM t";
        let rewritten = rewrite_lateral_unnest(sql);
        assert_eq!(rewritten, sql, "unchanged: {rewritten}");
    }

    #[test]
    fn passthrough_non_lateral_unnest_unchanged() {
        let sql = "SELECT * FROM t CROSS JOIN UNNEST(t.ids) AS elem(v)";
        let rewritten = rewrite_lateral_unnest(sql);
        assert_eq!(rewritten, sql);
    }

    #[test]
    fn build_unnest_op_returns_correct_variant() {
        let op = build_unnest_op("tags", "tag", false);
        match op {
            NodeOp::Unnest { array_column, output_column, with_ordinality } => {
                assert_eq!(array_column, "tags");
                assert_eq!(output_column, "tag");
                assert!(!with_ordinality);
            }
            _ => panic!("expected Unnest variant"),
        }
    }

    #[test]
    fn build_unnest_op_with_ordinality() {
        let op = build_unnest_op("items", "item", true);
        match op {
            NodeOp::Unnest { with_ordinality, .. } => assert!(with_ordinality),
            _ => panic!("expected Unnest"),
        }
    }
}
