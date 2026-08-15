//! E5.1 — Correlated subquery decorrelation: EXISTS/IN/scalar subquery analysis.
//!
//! DataFusion 53 already handles subquery decorrelation for batch queries via
//! the `DecorrelatePredicateSubquery` optimizer rule. This module adds:
//!
//! 1. **AST-level detection** of EXISTS/IN/NOT IN/scalar subquery patterns.
//! 2. **Streaming guard**: rejects correlated subqueries that reference a
//!    registered streaming table — DataFusion does not handle these.
//! 3. **Kind classification** so callers can adapt error messages and explain output.

use std::collections::HashSet;

use datafusion::sql::sqlparser::ast::{Expr, Query, Statement, visit_expressions, visit_relations};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::{SqlError, SqlResult};

// ── Subquery kind ─────────────────────────────────────────────────────────────

/// Classification of a subquery occurrence detected in a SQL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubqueryKind {
    /// `expr IN (SELECT ...)` — rewritten by DataFusion to a left-semi join.
    InSubquery,
    /// `expr NOT IN (SELECT ...)` — rewritten to a left-anti join.
    NotInSubquery,
    /// `EXISTS (SELECT ...)` — rewritten to a left-semi join.
    Exists,
    /// `NOT EXISTS (SELECT ...)` — rewritten to a left-anti join.
    NotExists,
    /// `(SELECT single_value)` used as a scalar expression — rewritten to an
    /// apply/cross-join with a LIMIT 1 inner query.
    Scalar,
}

/// A subquery occurrence found in a SQL statement.
#[derive(Debug, Clone)]
pub struct DetectedSubquery {
    pub kind: SubqueryKind,
    /// The inner query text (as rendered by the AST `Display` impl).
    pub inner_query: String,
}

// ── Detection ─────────────────────────────────────────────────────────────────

/// Analyse `sql` and return every subquery occurrence.
///
/// Returns an empty vec if the SQL contains no subqueries.
/// Returns a parse error only when the SQL is syntactically invalid.
pub fn detect_subqueries(sql: &str) -> SqlResult<Vec<DetectedSubquery>> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::Unsupported {
        feature: format!("subquery detection: parse error: {e}"),
    })?;

    let mut found = Vec::new();

    for stmt in &stmts {
        collect_subqueries(stmt, &mut found);
    }

    Ok(found)
}

/// Push every subquery occurrence anywhere inside `node`.
///
/// This walks the AST with sqlparser's own expression visitor rather than a
/// hand-written traversal. The hand-written one descended only
/// `SetExpr::Select` and a fixed list of `Expr` variants, so it silently saw
/// nothing in a `UNION` branch, a CTE, a derived table, a `JOIN ... ON`
/// condition, or even a parenthesised predicate (`Expr::Nested`) — and this
/// module's whole job is to *reject* streaming subqueries, so each gap was a
/// way past the guard rather than a missing nicety. Delegating to the visitor
/// makes the coverage a property of the parser instead of of this list.
///
/// The visitor already recurses through subquery bodies, so nested occurrences
/// are reported without descending explicitly.
fn collect_subqueries<V>(node: &V, out: &mut Vec<DetectedSubquery>)
where
    V: datafusion::sql::sqlparser::ast::Visit,
{
    let _ = visit_expressions(node, |expr| {
        match expr {
            Expr::InSubquery {
                subquery, negated, ..
            } => out.push(DetectedSubquery {
                kind: if *negated {
                    SubqueryKind::NotInSubquery
                } else {
                    SubqueryKind::InSubquery
                },
                inner_query: subquery.to_string(),
            }),
            Expr::Exists { subquery, negated } => out.push(DetectedSubquery {
                kind: if *negated {
                    SubqueryKind::NotExists
                } else {
                    SubqueryKind::Exists
                },
                inner_query: subquery.to_string(),
            }),
            Expr::Subquery(q) => out.push(DetectedSubquery {
                kind: SubqueryKind::Scalar,
                inner_query: q.to_string(),
            }),
            _ => {}
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
}

// ── Streaming guard ───────────────────────────────────────────────────────────

/// Validate that `sql` contains no subqueries that reference a streaming table.
///
/// Returns `Ok(())` when either:
/// - No subqueries are present, or
/// - No subquery body references a name in `streaming_tables`.
///
/// Returns `Err` when a subquery body contains a streaming table name (case-
/// insensitive), because DataFusion's decorrelation rules do not handle unbounded
/// inputs.
pub fn validate_no_streaming_subqueries(
    sql: &str,
    streaming_tables: &HashSet<String>,
) -> SqlResult<()> {
    if streaming_tables.is_empty() {
        return Ok(());
    }

    // Normalize to lowercase for case-insensitive matching against the SQL
    // identifier names produced by extract_table_names_from_query.
    let lower_tables: HashSet<String> = streaming_tables.iter().map(|s| s.to_lowercase()).collect();

    let dialect = GenericDialect {};
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Ok(()), // parse errors are surfaced later by DataFusion
    };

    for stmt in &stmts {
        {
            let mut subqueries = Vec::new();
            collect_subqueries(stmt, &mut subqueries);
            for sq in &subqueries {
                let inner_stmts =
                    Parser::parse_sql(&GenericDialect {}, &sq.inner_query).unwrap_or_default();
                for s in &inner_stmts {
                    if let Statement::Query(iq) = s {
                        let names = extract_table_names_from_query(iq);
                        if names.iter().any(|t| lower_tables.contains(t)) {
                            return Err(SqlError::Unsupported {
                                feature: "correlated subquery over a streaming (unbounded) table \
                                          is not supported; use a streaming join or MATCH_RECOGNIZE \
                                          for event-pattern matching"
                                    .into(),
                            });
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn extract_table_names_from_query(query: &Query) -> HashSet<String> {
    let mut names = HashSet::new();
    let _ = visit_relations(query, |relation| {
        names.insert(relation.to_string().to_lowercase());
        std::ops::ControlFlow::<()>::Continue(())
    });
    names
}

// ── Explain helpers ───────────────────────────────────────────────────────────

/// Return a human-readable summary of subquery kinds found in `sql`.
///
/// Returns `None` when `sql` has no subqueries.
pub fn explain_subqueries(sql: &str) -> Option<String> {
    let found = detect_subqueries(sql).unwrap_or_default();
    if found.is_empty() {
        return None;
    }
    let summary = found
        .iter()
        .map(|sq| match sq.kind {
            SubqueryKind::InSubquery => "IN-subquery → semi-join",
            SubqueryKind::NotInSubquery => "NOT IN-subquery → anti-join",
            SubqueryKind::Exists => "EXISTS → semi-join",
            SubqueryKind::NotExists => "NOT EXISTS → anti-join",
            SubqueryKind::Scalar => "scalar subquery → cross-apply",
        })
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!("subqueries: [{summary}]"))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod reachability_tests {
    /// The guard has to be reached from `SqlEngine::sql`, not merely correct.
    ///
    /// `validate_no_streaming_subqueries` had zero callers anywhere in the
    /// workspace, so a subquery over a streaming source went straight to
    /// DataFusion — whose decorrelation assumes a bounded input. The unit tests
    /// below all called the function directly and passed throughout.
    #[tokio::test]
    async fn a_subquery_over_a_streaming_source_is_refused_by_the_engine() {
        let engine = crate::SqlEngine::new();
        engine
            .register_streaming_source_name("live_events")
            .expect("register streaming source");

        let error = engine
            .sql("SELECT * FROM t WHERE id IN (SELECT id FROM live_events)")
            .await
            .expect_err("a subquery over a streaming source must be refused");
        let message = error.to_string();
        assert!(
            message.to_lowercase().contains("streaming"),
            "the refusal must name the reason, got: {message}"
        );
    }

    /// ...and an ordinary subquery over batch tables is untouched.
    #[tokio::test]
    async fn a_batch_subquery_is_not_affected_by_the_guard() {
        let engine = crate::SqlEngine::new();
        engine
            .register_streaming_source_name("live_events")
            .expect("register streaming source");

        // References no streaming source, so the guard must not fire. Planning
        // fails on the missing table, not on the guard.
        let error = engine
            .sql("SELECT * FROM absent_a WHERE id IN (SELECT id FROM absent_b)")
            .await
            .expect_err("tables do not exist");
        assert!(
            !error.to_string().to_lowercase().contains("streaming"),
            "the guard must not fire for batch-only subqueries: {error}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_in_subquery() {
        let sql = "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM vip_customers)";
        let found = detect_subqueries(sql).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, SubqueryKind::InSubquery);
    }

    #[test]
    fn detects_not_in_subquery() {
        let sql = "SELECT * FROM orders WHERE customer_id NOT IN (SELECT id FROM banned)";
        let found = detect_subqueries(sql).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, SubqueryKind::NotInSubquery);
    }

    #[test]
    fn detects_exists_subquery() {
        let sql = "SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id)";
        let found = detect_subqueries(sql).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, SubqueryKind::Exists);
    }

    #[test]
    fn detects_not_exists_subquery() {
        let sql = "SELECT * FROM orders o WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id)";
        let found = detect_subqueries(sql).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, SubqueryKind::NotExists);
    }

    #[test]
    fn detects_scalar_subquery() {
        let sql = "SELECT id, (SELECT MAX(amount) FROM payments WHERE order_id = o.id) as max_payment FROM orders o";
        let found = detect_subqueries(sql).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, SubqueryKind::Scalar);
    }

    #[test]
    fn detects_nested_subqueries() {
        let sql = "SELECT * FROM a WHERE x IN (SELECT y FROM b WHERE y NOT IN (SELECT z FROM c))";
        let found = detect_subqueries(sql).unwrap();
        assert!(found.len() >= 2);
        assert!(found.iter().any(|s| s.kind == SubqueryKind::InSubquery));
        assert!(found.iter().any(|s| s.kind == SubqueryKind::NotInSubquery));
    }

    #[test]
    fn no_subqueries_returns_empty() {
        let sql = "SELECT id, amount FROM orders WHERE status = 'completed'";
        let found = detect_subqueries(sql).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn streaming_guard_passes_when_no_streaming_tables() {
        let sql = "SELECT * FROM t WHERE id IN (SELECT id FROM s)";
        let streaming: HashSet<String> = HashSet::new();
        assert!(validate_no_streaming_subqueries(sql, &streaming).is_ok());
    }

    #[test]
    fn streaming_guard_rejects_subquery_over_streaming_table() {
        let sql = "SELECT * FROM events WHERE id IN (SELECT id FROM live_stream)";
        let mut streaming = HashSet::new();
        streaming.insert("live_stream".into());
        let err = validate_no_streaming_subqueries(sql, &streaming).unwrap_err();
        assert!(matches!(err, SqlError::Unsupported { .. }));
    }

    #[test]
    fn streaming_guard_passes_for_batch_tables() {
        let sql = "SELECT * FROM events WHERE id IN (SELECT id FROM reference_table)";
        let mut streaming = HashSet::new();
        streaming.insert("live_stream".into());
        assert!(validate_no_streaming_subqueries(sql, &streaming).is_ok());
    }

    #[test]
    fn explain_subqueries_returns_none_for_plain_sql() {
        assert!(explain_subqueries("SELECT 1").is_none());
    }

    #[test]
    fn explain_subqueries_describes_kinds() {
        let sql = "SELECT * FROM t WHERE x IN (SELECT y FROM s)";
        let desc = explain_subqueries(sql).unwrap();
        assert!(desc.contains("semi-join"));
    }

    #[test]
    fn case_expression_does_not_panic() {
        let sql = "SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END FROM t";
        let found = detect_subqueries(sql).unwrap();
        assert!(found.is_empty());
    }

    // ── Places the hand-written traversal never looked ────────────────────
    //
    // Each of these parses to a node the old walk did not descend, so it
    // reported zero subqueries and `validate_no_streaming_subqueries` waved
    // the statement through. They are listed one per construct because the
    // failure mode is silent: a guard that returns `Ok(())` looks exactly
    // like a guard that ran.

    /// `query.body` is a `SetOperation`, not a `Select`.
    #[test]
    fn finds_a_subquery_in_a_union_branch() {
        let sql = "SELECT a FROM t WHERE a IN (SELECT id FROM s) UNION ALL SELECT b FROM u";
        let found = detect_subqueries(sql).unwrap();
        assert_eq!(found.len(), 1, "union branch not scanned: {found:?}");
        assert_eq!(found[0].kind, SubqueryKind::InSubquery);
    }

    /// `query.with` was never visited at all.
    #[test]
    fn finds_a_subquery_inside_a_cte() {
        let sql = "WITH c AS (SELECT id FROM t WHERE id IN (SELECT id FROM s)) SELECT * FROM c";
        let found = detect_subqueries(sql).unwrap();
        assert_eq!(found.len(), 1, "CTE body not scanned: {found:?}");
    }

    /// Join constraints live in `sel.from`, which was not walked.
    #[test]
    fn finds_a_subquery_in_a_join_condition() {
        let sql = "SELECT * FROM a JOIN b ON b.id IN (SELECT id FROM s)";
        let found = detect_subqueries(sql).unwrap();
        assert_eq!(found.len(), 1, "join condition not scanned: {found:?}");
    }

    /// A derived table is a whole query hiding in `sel.from`.
    #[test]
    fn finds_a_subquery_inside_a_derived_table() {
        let sql = "SELECT * FROM (SELECT id FROM t WHERE id IN (SELECT id FROM s)) d";
        let found = detect_subqueries(sql).unwrap();
        assert_eq!(found.len(), 1, "derived table not scanned: {found:?}");
    }

    /// Parentheses alone were enough to hide a subquery: they wrap the
    /// predicate in `Expr::Nested`, which the old `collect_from_expr` did not
    /// match, so it stopped there.
    #[test]
    fn finds_a_subquery_behind_parentheses() {
        let sql = "SELECT * FROM t WHERE (id IN (SELECT id FROM s))";
        let found = detect_subqueries(sql).unwrap();
        assert_eq!(
            found.len(),
            1,
            "parenthesised predicate not scanned: {found:?}"
        );
    }

    /// The point of the module: the guard must not be bypassable by writing
    /// the same query with a UNION.
    #[test]
    fn the_streaming_guard_is_not_bypassed_by_a_union() {
        let sql = "SELECT a FROM events WHERE a IN (SELECT id FROM live_stream) \
                   UNION ALL SELECT b FROM other";
        let mut streaming = HashSet::new();
        streaming.insert("live_stream".into());
        assert!(
            validate_no_streaming_subqueries(sql, &streaming).is_err(),
            "a streaming subquery in a UNION branch slipped past the guard"
        );
    }

    /// Same, hidden in a CTE.
    #[test]
    fn the_streaming_guard_is_not_bypassed_by_a_cte() {
        let sql = "WITH c AS (SELECT id FROM events WHERE id IN (SELECT id FROM live_stream)) \
                   SELECT * FROM c";
        let mut streaming = HashSet::new();
        streaming.insert("live_stream".into());
        assert!(
            validate_no_streaming_subqueries(sql, &streaming).is_err(),
            "a streaming subquery in a CTE slipped past the guard"
        );
    }

    /// A statement that is not a bare `Query` — the old loop matched only
    /// `Statement::Query`, so `INSERT ... SELECT` was never examined.
    #[test]
    fn the_streaming_guard_examines_insert_select() {
        let sql = "INSERT INTO sink SELECT id FROM events WHERE id IN (SELECT id FROM live_stream)";
        let mut streaming = HashSet::new();
        streaming.insert("live_stream".into());
        assert!(
            validate_no_streaming_subqueries(sql, &streaming).is_err(),
            "INSERT ... SELECT was not examined"
        );
    }
}
