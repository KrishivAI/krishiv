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

use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, Select, SelectItem, SetExpr,
    Statement,
};
use datafusion::sql::sqlparser::ast::visit_relations;
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
        if let Statement::Query(q) = stmt {
            collect_subqueries_from_query(q, &mut found);
        }
    }

    Ok(found)
}

fn collect_subqueries_from_query(query: &Query, out: &mut Vec<DetectedSubquery>) {
    if let SetExpr::Select(sel) = query.body.as_ref() {
        collect_from_select(sel, out);
    }
}

fn collect_from_select(sel: &Select, out: &mut Vec<DetectedSubquery>) {
    for item in &sel.projection {
        match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                collect_from_expr(e, out);
            }
            _ => {}
        }
    }
    if let Some(e) = &sel.selection {
        collect_from_expr(e, out);
    }
    if let Some(e) = &sel.having {
        collect_from_expr(e, out);
    }
}

fn collect_from_expr(expr: &Expr, out: &mut Vec<DetectedSubquery>) {
    match expr {
        Expr::InSubquery { subquery, negated, .. } => {
            let kind =
                if *negated { SubqueryKind::NotInSubquery } else { SubqueryKind::InSubquery };
            out.push(DetectedSubquery { kind, inner_query: subquery.to_string() });
            collect_subqueries_from_query(subquery, out);
        }
        Expr::Exists { subquery, negated } => {
            let kind = if *negated { SubqueryKind::NotExists } else { SubqueryKind::Exists };
            out.push(DetectedSubquery { kind, inner_query: subquery.to_string() });
            collect_subqueries_from_query(subquery, out);
        }
        Expr::Subquery(q) => {
            out.push(DetectedSubquery {
                kind: SubqueryKind::Scalar,
                inner_query: q.to_string(),
            });
            collect_subqueries_from_query(q, out);
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_from_expr(left, out);
            collect_from_expr(right, out);
        }
        Expr::UnaryOp { expr, .. } => collect_from_expr(expr, out),
        Expr::IsNull(e) | Expr::IsNotNull(e) => collect_from_expr(e, out),
        Expr::Between { expr, low, high, .. } => {
            collect_from_expr(expr, out);
            collect_from_expr(low, out);
            collect_from_expr(high, out);
        }
        Expr::Case { operand, conditions, else_result, .. } => {
            if let Some(e) = operand {
                collect_from_expr(e, out);
            }
            for cw in conditions {
                collect_from_expr(&cw.condition, out);
                collect_from_expr(&cw.result, out);
            }
            if let Some(e) = else_result {
                collect_from_expr(e, out);
            }
        }
        Expr::Function(f) => {
            if let FunctionArguments::List(list) = &f.args {
                for fa in &list.args {
                    let inner = match fa {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                        FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. } => Some(e),
                        _ => None,
                    };
                    if let Some(e) = inner {
                        collect_from_expr(e, out);
                    }
                }
            }
        }
        _ => {}
    }
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

    let dialect = GenericDialect {};
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Ok(()), // parse errors are surfaced later by DataFusion
    };

    for stmt in &stmts {
        if let Statement::Query(q) = stmt {
            let mut subqueries = Vec::new();
            collect_subqueries_from_query(q, &mut subqueries);
            for sq in &subqueries {
                let inner_stmts =
                    Parser::parse_sql(&GenericDialect {}, &sq.inner_query).unwrap_or_default();
                for s in &inner_stmts {
                    if let Statement::Query(iq) = s {
                        let names = extract_table_names_from_query(iq);
                        if names.iter().any(|t| streaming_tables.contains(t)) {
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
    visit_relations(query, |relation| {
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
mod tests {
    use super::*;

    #[test]
    fn detects_in_subquery() {
        let sql =
            "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM vip_customers)";
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
        let sql =
            "SELECT * FROM a WHERE x IN (SELECT y FROM b WHERE y NOT IN (SELECT z FROM c))";
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
}
