//! Compile a windowed streaming SQL query into a [`WindowExecutionSpec`].
//!
//! Supports the canonical keyed windowed-aggregation shape:
//!
//! ```sql
//! SELECT key, AGG(col) AS out [, ...]
//! FROM TUMBLE(TABLE src, DESCRIPTOR(ts), <size>)   -- or HOP / SESSION
//! GROUP BY key, window_start, window_end
//! ```
//!
//! # One SQL front door (Phase 60)
//!
//! Streaming and batch share **one** front door for the window TVF and **one**
//! parse of the query:
//!
//! 1. The window TVF is rewritten to a subquery exactly once by
//!    [`rewrite_window_tvfs`] (`streaming_tvf.rs`) — the *same* rewrite the batch
//!    planner consumes, so `TUMBLE`/`HOP`/`SESSION` has a single canonical
//!    lowering, not one for batch and another hand-rolled for streaming.
//! 2. The rewritten SQL is parsed **once** with the front-door dialect
//!    ([`DuckDbDialect`], matching `SqlEngine`'s `sql_parser.dialect`). Parsing
//!    streaming SQL with a *different* dialect than batch was the divergence
//!    class behind the `SUM(CASE WHEN …)` 409 in prod; a single dialect closes
//!    it structurally.
//! 3. Everything the operator needs — the window kind/size/slide/gap, the event
//!    time column, the grouping key, and the aggregate list — is derived from
//!    that one parsed plan. Window recognition is now *structural*: "does the
//!    parsed plan carry a `window_start` boundary projection over a recognised
//!    window function?", so [`SqlError::Unsupported`] means "the planner cannot
//!    lower this shape to a continuous plan", not "a text matcher failed to
//!    recognise the SQL".
//!
//! The dataflow `ContinuousWindowExecutor` still computes the aggregation from
//! the resulting [`WindowExecutionSpec`]; consuming DataFusion's own
//! `LogicalPlan` (rather than the shared sqlparser AST) is the deeper
//! unification that grows with Phase 55's operator coverage.

use std::collections::HashMap;

use datafusion::sql::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, Value,
};
use datafusion::sql::sqlparser::dialect::DuckDbDialect;
use datafusion::sql::sqlparser::parser::Parser;
use krishiv_plan::window::{
    AggFilterCompareOp, AggFilterValue, FloatLiteral, WindowAgg, WindowAggFilter, WindowAggKind,
    WindowExecutionSpec, WindowKind,
};

use crate::streaming_tvf::{find_window_tvf, rewrite_window_tvfs};
use crate::{SqlError, SqlResult};

/// A compiled windowed streaming plan: the operator spec plus the name of the
/// source table the window reads from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingWindowPlan {
    /// The keyed-window operator specification.
    pub spec: WindowExecutionSpec,
    /// The source table the window TVF reads from.
    pub source: String,
}

fn unsupported(msg: impl Into<String>) -> SqlError {
    SqlError::Unsupported {
        feature: msg.into(),
    }
}

fn parse_ms(raw: &str) -> SqlResult<u64> {
    raw.trim().parse::<u64>().map_err(|_| {
        unsupported(format!(
            "window interval '{raw}' is not a millisecond count"
        ))
    })
}

/// Returns `true` when `sql` contains a TUMBLE/HOP/SESSION window TVF.
pub fn is_windowed_streaming_sql(sql: &str) -> bool {
    find_window_tvf(sql).is_some()
}

/// Compile a windowed streaming SQL query into a [`StreamingWindowPlan`].
///
/// Returns [`SqlError::Unsupported`] when the query is not a recognised keyed
/// windowed aggregation.
///
/// The compile is one shared TVF rewrite + one parse: [`rewrite_window_tvfs`]
/// (the same rewrite batch uses) turns the window TVF into a `_tvf_window`
/// subquery, then the rewritten SQL is parsed once with the front-door dialect
/// and both the window parameters and the key/aggregates are derived from that
/// single parsed plan.
pub fn compile_streaming_window_sql(sql: &str) -> SqlResult<StreamingWindowPlan> {
    let rewritten = rewrite_window_tvfs(sql);
    let select = parse_single_select(&rewritten)?;

    let (window, source) = extract_window(&select)?;
    let (key_column, agg_exprs) = extract_key_and_aggs(&select)?;

    let spec = WindowExecutionSpec {
        key_column,
        key_column_type: String::from("utf8"),
        event_time_column: window.event_time_column,
        watermark_lag_ms: 0,
        window_kind: window.kind,
        window_size_ms: window.window_size_ms,
        slide_ms: window.slide_ms,
        session_gap_ms: window.session_gap_ms,
        agg_exprs,
        state_ttl_ms: None,
        allowed_lateness_ms: None,
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
        window_timezone: None,
    };
    Ok(StreamingWindowPlan { spec, source })
}

/// Parse the (already TVF-rewritten) SQL once with the front-door dialect and
/// return its single top-level `SELECT`. This is the *only* parse in the
/// streaming compile path, and it uses the same dialect as the batch front door.
fn parse_single_select(sql: &str) -> SqlResult<Select> {
    let dialect = DuckDbDialect {};
    let stmts = Parser::parse_sql(&dialect, sql)
        .map_err(|e| unsupported(format!("streaming window query parse error: {e}")))?;
    let query = stmts
        .into_iter()
        .find_map(|s| match s {
            Statement::Query(q) => Some(q),
            _ => None,
        })
        .ok_or_else(|| unsupported("streaming window query must be a SELECT"))?;
    match *query.body {
        SetExpr::Select(select) => Ok(*select),
        _ => Err(unsupported("streaming window query must be a plain SELECT")),
    }
}

/// The window parameters recovered structurally from the parsed plan.
struct WindowParams {
    kind: WindowKind,
    event_time_column: String,
    window_size_ms: u64,
    slide_ms: Option<u64>,
    session_gap_ms: Option<u64>,
}

/// Recover the window parameters and source table from the parsed plan. The TVF
/// rewrite wraps the source in a derived table aliased `_tvf_window` whose
/// projection carries `window_start = <window_fn>(ts, …)`, so recognising the
/// window is a structural check over the parsed plan rather than a text scan.
fn extract_window(select: &Select) -> SqlResult<(WindowParams, String)> {
    let subquery = find_tvf_window_subquery(select)
        .ok_or_else(|| unsupported("query has no TUMBLE/HOP/SESSION window"))?;
    let SetExpr::Select(inner) = subquery.body.as_ref() else {
        return Err(unsupported("windowed source must be a plain SELECT"));
    };
    let boundary = projection_alias_expr(inner, "window_start")
        .ok_or_else(|| unsupported("windowed source is missing its window_start boundary"))?;
    let Expr::Function(func) = boundary else {
        return Err(unsupported(
            "window_start must be produced by a window boundary function",
        ));
    };
    let params = window_params_from_udf(func)?;
    let source = single_source_name(inner)?;
    Ok((params, source))
}

/// Find the `_tvf_window` derived table emitted by the TVF rewrite.
fn find_tvf_window_subquery(select: &Select) -> Option<&Query> {
    fn from_relation(relation: &TableFactor) -> Option<&Query> {
        match relation {
            TableFactor::Derived { subquery, alias, .. }
                if alias.as_ref().map(|a| a.name.value.as_str()) == Some("_tvf_window") =>
            {
                Some(subquery.as_ref())
            }
            _ => None,
        }
    }
    for twj in &select.from {
        if let Some(q) = from_relation(&twj.relation) {
            return Some(q);
        }
        for join in &twj.joins {
            if let Some(q) = from_relation(&join.relation) {
                return Some(q);
            }
        }
    }
    None
}

/// The expression of the projection item with the given output alias, if any.
fn projection_alias_expr<'a>(select: &'a Select, alias: &str) -> Option<&'a Expr> {
    select.projection.iter().find_map(|item| match item {
        SelectItem::ExprWithAlias { expr, alias: a } if a.value == alias => Some(expr),
        _ => None,
    })
}

/// Translate a recognised window boundary function call
/// (`tumble_start`/`hop_start`/`session_start`) into [`WindowParams`].
fn window_params_from_udf(func: &Function) -> SqlResult<WindowParams> {
    let name = func.name.to_string().to_ascii_lowercase();
    let args = function_arg_exprs(func);
    let event_time_column = args
        .first()
        .and_then(|e| ident_name(e))
        .ok_or_else(|| unsupported("window function needs an event-time column argument"))?;
    let ms = |slot: Option<&&Expr>, what: &str| -> SqlResult<u64> {
        let expr = slot.ok_or_else(|| unsupported(format!("window function needs a {what}")))?;
        let literal = number_literal(expr).ok_or_else(|| {
            unsupported(format!("window {what} must be an integer millisecond literal"))
        })?;
        parse_ms(&literal)
    };
    match name.as_str() {
        "tumble_start" => Ok(WindowParams {
            kind: WindowKind::Tumbling,
            event_time_column,
            window_size_ms: ms(args.get(1), "size")?,
            slide_ms: None,
            session_gap_ms: None,
        }),
        "hop_start" => Ok(WindowParams {
            kind: WindowKind::Sliding,
            event_time_column,
            slide_ms: Some(ms(args.get(1), "slide")?),
            window_size_ms: ms(args.get(2), "size")?,
            session_gap_ms: None,
        }),
        "session_start" => {
            let gap = ms(args.get(1), "gap")?;
            Ok(WindowParams {
                kind: WindowKind::Session,
                event_time_column,
                window_size_ms: gap,
                slide_ms: None,
                session_gap_ms: Some(gap),
            })
        }
        other => Err(unsupported(format!(
            "unrecognised window boundary function '{other}'"
        ))),
    }
}

/// The single base-table name the windowed source reads from.
fn single_source_name(inner: &Select) -> SqlResult<String> {
    let [only] = inner.from.as_slice() else {
        return Err(unsupported(
            "windowed source must read from exactly one table",
        ));
    };
    if !only.joins.is_empty() {
        return Err(unsupported(
            "streaming windows do not support joins in the windowed source yet",
        ));
    }
    match &only.relation {
        TableFactor::Table { name, .. } => Ok(name.to_string()),
        _ => Err(unsupported("windowed source must be a base table")),
    }
}

/// Collect the positional expression arguments of a function call.
fn function_arg_exprs(func: &Function) -> Vec<&Expr> {
    let FunctionArguments::List(list) = &func.args else {
        return Vec::new();
    };
    list.args
        .iter()
        .filter_map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
            FunctionArg::Named {
                arg: FunctionArgExpr::Expr(e),
                ..
            } => Some(e),
            _ => None,
        })
        .collect()
}

/// The (last) identifier name of an expression, if it is a plain/compound column.
fn ident_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
        _ => None,
    }
}

/// The numeric literal text of an expression, if it is a number value.
fn number_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => Some(n.clone()),
            _ => None,
        },
        _ => None,
    }
}

const WINDOW_BOUNDARY_COLS: [&str; 2] = ["window_start", "window_end"];

fn extract_key_and_aggs(select: &Select) -> SqlResult<(String, Vec<WindowAgg>)> {
    let mut key_column: Option<String> = None;
    let mut aggs: Vec<WindowAgg> = Vec::new();

    for item in &select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => continue,
        };
        match expr {
            Expr::Function(f) => aggs.push(function_to_agg(f, alias)?),
            Expr::Identifier(id) => maybe_set_key(&mut key_column, &id.value),
            Expr::CompoundIdentifier(parts) => {
                if let Some(last) = parts.last() {
                    maybe_set_key(&mut key_column, &last.value);
                }
            }
            _ => continue,
        }
    }

    let key_column = key_column.ok_or_else(|| {
        unsupported("streaming window query needs a grouping key column in the SELECT list")
    })?;
    if aggs.is_empty() {
        aggs.push(WindowAgg::count("count"));
    }
    Ok((key_column, aggs))
}

fn maybe_set_key(key: &mut Option<String>, name: &str) {
    if key.is_none() && !WINDOW_BOUNDARY_COLS.contains(&name) {
        *key = Some(name.to_string());
    }
}

fn function_to_agg(f: &Function, alias: Option<String>) -> SqlResult<WindowAgg> {
    let fname = f.name.to_string().to_ascii_lowercase();
    let mut kind = match fname.as_str() {
        "count" => WindowAggKind::Count,
        "sum" => WindowAggKind::Sum,
        "min" => WindowAggKind::Min,
        "max" => WindowAggKind::Max,
        "avg" => WindowAggKind::Avg,
        "stddev" | "stddev_samp" => WindowAggKind::Stddev,
        other => {
            return Err(unsupported(format!(
                "aggregate '{other}' is not supported in streaming windows; \
                 use count/sum/min/max/avg/stddev"
            )));
        }
    };

    // `AGG(x) FILTER (WHERE …)`.
    let mut filter = match &f.filter {
        Some(predicate) => Some(lower_filter_expr(predicate)?),
        None => None,
    };

    // The aggregate argument: a bare column, or the `CASE WHEN cond THEN x
    // [ELSE 0|NULL] END` conditional idiom, which lowers to a row filter.
    let mut input_column = None;
    if let Some(arg) = first_arg_expr(f) {
        match arg {
            Expr::Identifier(id) => input_column = Some(id.value.clone()),
            Expr::CompoundIdentifier(parts) => {
                input_column = parts.last().map(|p| p.value.clone());
            }
            Expr::Case { .. } => {
                let lowered = lower_case_arg(arg, kind, &fname)?;
                kind = lowered.kind;
                input_column = lowered.input_column;
                filter = Some(match filter {
                    Some(existing) => {
                        WindowAggFilter::And(Box::new(existing), Box::new(lowered.filter))
                    }
                    None => lowered.filter,
                });
            }
            // COUNT(*) and other wildcard forms fall through with no column.
            _ => {}
        }
    }

    let output_column = alias.unwrap_or_else(|| match &input_column {
        Some(col) => format!("{fname}_{col}"),
        None => fname.clone(),
    });
    Ok(WindowAgg {
        kind,
        input_column: input_column.unwrap_or_default(),
        output_column,
        filter,
    })
}

/// The lowering of a `CASE WHEN cond THEN value [ELSE …] END` aggregate
/// argument: the effective aggregate kind (SUM-of-1 collapses to COUNT), the
/// value column when the branch yields one, and the row filter.
struct LoweredCaseArg {
    kind: WindowAggKind,
    input_column: Option<String>,
    filter: WindowAggFilter,
}

fn lower_case_arg(case: &Expr, kind: WindowAggKind, fname: &str) -> SqlResult<LoweredCaseArg> {
    let Expr::Case {
        operand,
        conditions,
        else_result,
        ..
    } = case
    else {
        return Err(unsupported("expected a CASE aggregate argument"));
    };
    if operand.is_some() {
        return Err(unsupported(
            "CASE <operand> WHEN … aggregate arguments are not supported in streaming \
             windows; use a searched CASE WHEN <predicate> THEN …",
        ));
    }
    let [when] = conditions.as_slice() else {
        return Err(unsupported(
            "streaming windows support exactly one WHEN branch in a CASE aggregate argument",
        ));
    };
    let filter = lower_filter_expr(&when.condition)?;

    // ELSE must be the aggregate's identity (absent, NULL, or 0 for SUM/COUNT).
    match else_result.as_deref() {
        None => {}
        Some(Expr::Value(v)) if matches!(&v.value, Value::Null) => {}
        Some(Expr::Value(v))
            if matches!(kind, WindowAggKind::Sum | WindowAggKind::Count)
                && matches!(&v.value, Value::Number(n, _) if n == "0") => {}
        Some(other) => {
            return Err(unsupported(format!(
                "CASE aggregate argument ELSE branch '{other}' is not the {fname} identity; \
                 use ELSE NULL (or ELSE 0 for SUM/COUNT)"
            )));
        }
    }

    match &when.result {
        // SUM(CASE WHEN c THEN 1 …) / COUNT(CASE WHEN c THEN <literal> …):
        // a conditional row count.
        Expr::Value(v) => match (&v.value, kind) {
            (Value::Number(n, _), WindowAggKind::Sum) if n == "1" => Ok(LoweredCaseArg {
                kind: WindowAggKind::Count,
                input_column: None,
                filter,
            }),
            (Value::Number(_, _), WindowAggKind::Count) => Ok(LoweredCaseArg {
                kind: WindowAggKind::Count,
                input_column: None,
                filter,
            }),
            _ => Err(unsupported(format!(
                "CASE aggregate argument THEN branch must be a column (or the literal 1 \
                 for a conditional count); got a literal under {fname}"
            ))),
        },
        // AGG(CASE WHEN c THEN col …): filter + plain column aggregate. For
        // COUNT, SQL counts non-null results, so add the column null-check.
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let column = match &when.result {
                Expr::Identifier(id) => id.value.clone(),
                Expr::CompoundIdentifier(parts) => parts
                    .last()
                    .map(|p| p.value.clone())
                    .ok_or_else(|| unsupported("empty compound identifier in CASE THEN"))?,
                _ => unreachable!("outer match restricts to identifiers"),
            };
            let filter = if kind == WindowAggKind::Count {
                WindowAggFilter::And(
                    Box::new(filter),
                    Box::new(WindowAggFilter::IsNotNull {
                        column: column.clone(),
                    }),
                )
            } else {
                filter
            };
            let input_column = (kind != WindowAggKind::Count).then_some(column);
            Ok(LoweredCaseArg {
                kind,
                input_column,
                filter,
            })
        }
        other => Err(unsupported(format!(
            "CASE aggregate argument THEN branch '{other}' is not supported in streaming \
             windows; use a column or literal 1"
        ))),
    }
}

/// Lower a SQL predicate to the typed [`WindowAggFilter`] AST the dataflow
/// operators evaluate. Supports column-vs-literal comparisons, AND/OR/NOT,
/// and IS [NOT] NULL — the shapes `FILTER (WHERE …)` clauses use in practice.
fn lower_filter_expr(expr: &Expr) -> SqlResult<WindowAggFilter> {
    use datafusion::sql::sqlparser::ast::BinaryOperator;
    match expr {
        Expr::Nested(inner) => lower_filter_expr(inner),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(WindowAggFilter::And(
                Box::new(lower_filter_expr(left)?),
                Box::new(lower_filter_expr(right)?),
            )),
            BinaryOperator::Or => Ok(WindowAggFilter::Or(
                Box::new(lower_filter_expr(left)?),
                Box::new(lower_filter_expr(right)?),
            )),
            _ => lower_comparison(left, op, right),
        },
        Expr::IsNull(inner) => Ok(WindowAggFilter::IsNull {
            column: expr_column(inner)?,
        }),
        Expr::IsNotNull(inner) => Ok(WindowAggFilter::IsNotNull {
            column: expr_column(inner)?,
        }),
        Expr::UnaryOp {
            op: datafusion::sql::sqlparser::ast::UnaryOperator::Not,
            expr,
        } => Ok(WindowAggFilter::Not(Box::new(lower_filter_expr(expr)?))),
        // A bare boolean column used as the predicate (`WHERE is_bot`).
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Ok(WindowAggFilter::Compare {
            column: expr_column(expr)?,
            op: AggFilterCompareOp::Eq,
            value: AggFilterValue::Bool(true),
        }),
        other => Err(unsupported(format!(
            "aggregate filter predicate '{other}' is not supported in streaming windows; \
             use column-vs-literal comparisons combined with AND/OR/NOT and IS [NOT] NULL"
        ))),
    }
}

fn lower_comparison(
    left: &Expr,
    op: &datafusion::sql::sqlparser::ast::BinaryOperator,
    right: &Expr,
) -> SqlResult<WindowAggFilter> {
    use datafusion::sql::sqlparser::ast::BinaryOperator;
    let mapped = match op {
        BinaryOperator::Eq => AggFilterCompareOp::Eq,
        BinaryOperator::NotEq => AggFilterCompareOp::NotEq,
        BinaryOperator::Lt => AggFilterCompareOp::Lt,
        BinaryOperator::LtEq => AggFilterCompareOp::LtEq,
        BinaryOperator::Gt => AggFilterCompareOp::Gt,
        BinaryOperator::GtEq => AggFilterCompareOp::GtEq,
        other => {
            return Err(unsupported(format!(
                "aggregate filter operator '{other}' is not supported in streaming windows"
            )));
        }
    };
    // `column <op> literal` or the mirrored `literal <op> column`.
    if let (Ok(column), Some(value)) = (expr_column(left), expr_literal(right)) {
        Ok(WindowAggFilter::Compare {
            column,
            op: mapped,
            value,
        })
    } else if let (Some(value), Ok(column)) = (expr_literal(left), expr_column(right)) {
        let mirrored = match mapped {
            AggFilterCompareOp::Lt => AggFilterCompareOp::Gt,
            AggFilterCompareOp::LtEq => AggFilterCompareOp::GtEq,
            AggFilterCompareOp::Gt => AggFilterCompareOp::Lt,
            AggFilterCompareOp::GtEq => AggFilterCompareOp::LtEq,
            symmetric => symmetric,
        };
        Ok(WindowAggFilter::Compare {
            column,
            op: mirrored,
            value,
        })
    } else {
        Err(unsupported(
            "aggregate filter comparisons must be column-vs-literal in streaming windows",
        ))
    }
}

fn expr_column(expr: &Expr) -> SqlResult<String> {
    match expr {
        Expr::Identifier(id) => Ok(id.value.clone()),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .ok_or_else(|| unsupported("empty compound identifier in aggregate filter")),
        Expr::Nested(inner) => expr_column(inner),
        other => Err(unsupported(format!(
            "aggregate filter expected a column, got '{other}'"
        ))),
    }
}

fn expr_literal(expr: &Expr) -> Option<AggFilterValue> {
    let Expr::Value(v) = expr else {
        return None;
    };
    match &v.value {
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Some(AggFilterValue::Int(i))
            } else {
                n.parse::<f64>()
                    .ok()
                    .map(|f| AggFilterValue::Float(FloatLiteral(f)))
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Some(AggFilterValue::Utf8(s.clone()))
        }
        Value::Boolean(b) => Some(AggFilterValue::Bool(*b)),
        _ => None,
    }
}

fn first_arg_expr(f: &Function) -> Option<&Expr> {
    let FunctionArguments::List(list) = &f.args else {
        return None;
    };
    for fa in &list.args {
        match fa {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => return Some(e),
            FunctionArg::Named {
                arg: FunctionArgExpr::Expr(e),
                ..
            } => return Some(e),
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn compiles_tumbling_window() {
        let sql = "SELECT user_id, SUM(amount) AS total \
                   FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 60000) \
                   GROUP BY user_id, window_start, window_end";
        let plan = compile_streaming_window_sql(sql).unwrap();
        assert_eq!(plan.source, "events");
        assert_eq!(plan.spec.window_kind, WindowKind::Tumbling);
        assert_eq!(plan.spec.window_size_ms, 60000);
        assert_eq!(plan.spec.event_time_column, "ts");
        assert_eq!(plan.spec.key_column, "user_id");
        assert_eq!(plan.spec.agg_exprs.len(), 1);
        assert_eq!(plan.spec.agg_exprs[0].kind, WindowAggKind::Sum);
        assert_eq!(plan.spec.agg_exprs[0].input_column, "amount");
        assert_eq!(plan.spec.agg_exprs[0].output_column, "total");
    }

    #[test]
    fn compiles_tumbling_window_from_interval_string() {
        // The shared TVF rewrite normalises `'1 minute'` → 60000 ms, and the
        // unified parse recovers it structurally from the rewritten plan.
        let sql = "SELECT user_id, COUNT(*) AS c \
                   FROM TUMBLE(TABLE events, DESCRIPTOR(ts), '1 minute') \
                   GROUP BY user_id, window_start, window_end";
        let plan = compile_streaming_window_sql(sql).unwrap();
        assert_eq!(plan.spec.window_kind, WindowKind::Tumbling);
        assert_eq!(plan.spec.window_size_ms, 60_000);
        assert_eq!(plan.spec.event_time_column, "ts");
        assert_eq!(plan.source, "events");
    }

    #[test]
    fn compiles_with_schema_qualified_source() {
        // Source names flow through the single parse; a schema-qualified table
        // is recovered from the parsed plan, not a text slice.
        let sql = "SELECT user_id, SUM(amount) AS total \
                   FROM TUMBLE(TABLE analytics.events, DESCRIPTOR(ts), 60000) \
                   GROUP BY user_id, window_start, window_end";
        let plan = compile_streaming_window_sql(sql).unwrap();
        assert_eq!(plan.source, "analytics.events");
        assert_eq!(plan.spec.key_column, "user_id");
    }

    #[test]
    fn compiles_tumbling_window_with_stddev() {
        let sql = "SELECT k, STDDEV(v) AS spread \
                   FROM TUMBLE(TABLE m, DESCRIPTOR(ts), 60000) \
                   GROUP BY k, window_start, window_end";
        let plan = compile_streaming_window_sql(sql).unwrap();
        assert_eq!(plan.spec.agg_exprs[0].kind, WindowAggKind::Stddev);
        assert_eq!(plan.spec.agg_exprs[0].input_column, "v");
        assert_eq!(plan.spec.agg_exprs[0].output_column, "spread");
    }

    #[test]
    fn compiles_hop_window_with_slide() {
        let sql = "SELECT k, COUNT(*) AS c \
                   FROM HOP(TABLE clicks, DESCRIPTOR(ts), 30000, 60000) \
                   GROUP BY k, window_start, window_end";
        let plan = compile_streaming_window_sql(sql).unwrap();
        assert_eq!(plan.spec.window_kind, WindowKind::Sliding);
        assert_eq!(plan.spec.window_size_ms, 60000);
        assert_eq!(plan.spec.slide_ms, Some(30000));
        assert_eq!(plan.spec.agg_exprs[0].kind, WindowAggKind::Count);
    }

    #[test]
    fn compiles_session_window_with_gap() {
        let sql = "SELECT k, MAX(v) AS hi \
                   FROM SESSION(TABLE events, DESCRIPTOR(ts), 15000) \
                   GROUP BY k, window_start, window_end";
        let plan = compile_streaming_window_sql(sql).unwrap();
        assert_eq!(plan.spec.window_kind, WindowKind::Session);
        assert_eq!(plan.spec.session_gap_ms, Some(15000));
        assert_eq!(plan.spec.agg_exprs[0].kind, WindowAggKind::Max);
    }

    #[test]
    fn non_windowed_query_is_unsupported() {
        let err = compile_streaming_window_sql("SELECT a FROM t").unwrap_err();
        assert!(matches!(err, SqlError::Unsupported { .. }));
    }

    #[test]
    fn unsupported_aggregate_is_rejected() {
        let sql = "SELECT k, MEDIAN(v) AS s \
                   FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 60000) \
                   GROUP BY k, window_start, window_end";
        let err = compile_streaming_window_sql(sql).unwrap_err();
        assert!(matches!(err, SqlError::Unsupported { .. }));
    }

    #[test]
    fn compiles_count_filter_where() {
        let sql = "SELECT domain, COUNT(*) FILTER (WHERE kind = 'edit') AS edits \
                   FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 60000) \
                   GROUP BY domain, window_start, window_end";
        let plan = compile_streaming_window_sql(sql).unwrap();
        let agg = &plan.spec.agg_exprs[0];
        assert_eq!(agg.kind, WindowAggKind::Count);
        assert_eq!(agg.output_column, "edits");
        assert_eq!(
            agg.filter,
            Some(WindowAggFilter::Compare {
                column: "kind".into(),
                op: AggFilterCompareOp::Eq,
                value: AggFilterValue::Utf8("edit".into()),
            })
        );
    }

    #[test]
    fn compiles_sum_case_when_column() {
        let sql = "SELECT domain, SUM(CASE WHEN kind = 'edit' THEN size END) AS edit_bytes \
                   FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 60000) \
                   GROUP BY domain, window_start, window_end";
        let plan = compile_streaming_window_sql(sql).unwrap();
        let agg = &plan.spec.agg_exprs[0];
        assert_eq!(agg.kind, WindowAggKind::Sum);
        assert_eq!(agg.input_column, "size");
        assert_eq!(agg.output_column, "edit_bytes");
        assert!(agg.filter.is_some());
    }

    #[test]
    fn sum_case_when_one_lowers_to_conditional_count() {
        let sql = "SELECT domain, SUM(CASE WHEN is_bot = true THEN 1 ELSE 0 END) AS bots \
                   FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 60000) \
                   GROUP BY domain, window_start, window_end";
        let plan = compile_streaming_window_sql(sql).unwrap();
        let agg = &plan.spec.agg_exprs[0];
        assert_eq!(agg.kind, WindowAggKind::Count, "SUM of 1s is a count");
        assert_eq!(
            agg.filter,
            Some(WindowAggFilter::Compare {
                column: "is_bot".into(),
                op: AggFilterCompareOp::Eq,
                value: AggFilterValue::Bool(true),
            })
        );
    }

    #[test]
    fn bare_boolean_filter_predicate_compiles() {
        let sql = "SELECT domain, COUNT(*) FILTER (WHERE is_bot) AS bots \
                   FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 60000) \
                   GROUP BY domain, window_start, window_end";
        let plan = compile_streaming_window_sql(sql).unwrap();
        assert_eq!(
            plan.spec.agg_exprs[0].filter,
            Some(WindowAggFilter::Compare {
                column: "is_bot".into(),
                op: AggFilterCompareOp::Eq,
                value: AggFilterValue::Bool(true),
            })
        );
    }

    #[test]
    fn filter_and_case_combine_with_and() {
        let sql = "SELECT domain, \
                   SUM(CASE WHEN kind = 'edit' THEN size END) FILTER (WHERE size > 100) AS big \
                   FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 60000) \
                   GROUP BY domain, window_start, window_end";
        let plan = compile_streaming_window_sql(sql).unwrap();
        let agg = &plan.spec.agg_exprs[0];
        assert_eq!(agg.kind, WindowAggKind::Sum);
        assert_eq!(agg.input_column, "size");
        assert!(
            matches!(agg.filter, Some(WindowAggFilter::And(_, _))),
            "FILTER clause and CASE condition must both apply: {:?}",
            agg.filter
        );
    }

    #[test]
    fn rejects_case_with_multiple_when_branches() {
        let sql = "SELECT domain, \
                   SUM(CASE WHEN a = 1 THEN x WHEN b = 2 THEN y END) AS s \
                   FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 60000) \
                   GROUP BY domain, window_start, window_end";
        let err = compile_streaming_window_sql(sql).unwrap_err();
        assert!(matches!(err, SqlError::Unsupported { .. }));
    }

    #[test]
    fn rejects_non_identity_else_branch() {
        let sql = "SELECT domain, MAX(CASE WHEN a = 1 THEN x ELSE 0 END) AS m \
                   FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 60000) \
                   GROUP BY domain, window_start, window_end";
        let err = compile_streaming_window_sql(sql).unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "ELSE 0 under MAX changes semantics and must be rejected"
        );
    }

    #[test]
    fn window_boundary_columns_are_not_treated_as_key() {
        let sql = "SELECT window_start, user_id, COUNT(*) AS c \
                   FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 60000) \
                   GROUP BY user_id, window_start, window_end";
        let plan = compile_streaming_window_sql(sql).unwrap();
        assert_eq!(plan.spec.key_column, "user_id");
    }

    #[test]
    fn detects_windowed_sql() {
        assert!(is_windowed_streaming_sql(
            "SELECT k FROM TUMBLE(TABLE t, DESCRIPTOR(ts), 1000) GROUP BY k"
        ));
        assert!(!is_windowed_streaming_sql("SELECT k FROM t"));
    }
}
