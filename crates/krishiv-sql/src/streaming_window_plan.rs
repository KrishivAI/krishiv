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
//! The streaming engine uses the resulting [`WindowExecutionSpec`] to drive the
//! dataflow `ContinuousWindowExecutor`. The window operator computes the
//! aggregation itself, so the SELECT/GROUP BY is only mined for the grouping
//! key column and the aggregate list — the rest of the query shape is the
//! window TVF, which [`find_window_tvf`] already parses.

use std::collections::HashMap;

use datafusion::sql::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr, Statement,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use krishiv_plan::window::{WindowAgg, WindowAggKind, WindowExecutionSpec, WindowKind};

use crate::streaming_tvf::{WindowTvf, find_window_tvf, rewrite_window_tvfs};
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
pub fn compile_streaming_window_sql(sql: &str) -> SqlResult<StreamingWindowPlan> {
    let (_, tvf, _) = find_window_tvf(sql)
        .ok_or_else(|| unsupported("query has no TUMBLE/HOP/SESSION window"))?;

    let (source, event_time_column, window_kind, window_size_ms, slide_ms, session_gap_ms) =
        match &tvf {
            WindowTvf::Tumble {
                source,
                ts_col,
                size_ms,
            } => (
                (*source).to_string(),
                (*ts_col).to_string(),
                WindowKind::Tumbling,
                parse_ms(size_ms)?,
                None,
                None,
            ),
            WindowTvf::Hop {
                source,
                ts_col,
                slide_ms,
                size_ms,
            } => (
                (*source).to_string(),
                (*ts_col).to_string(),
                WindowKind::Sliding,
                parse_ms(size_ms)?,
                Some(parse_ms(slide_ms)?),
                None,
            ),
            WindowTvf::Session {
                source,
                ts_col,
                gap_ms,
            } => {
                let gap = parse_ms(gap_ms)?;
                (
                    (*source).to_string(),
                    (*ts_col).to_string(),
                    WindowKind::Session,
                    gap,
                    None,
                    Some(gap),
                )
            }
        };

    // Mine the SELECT projection for the key column and aggregates. Rewrite the
    // TVF to a plain subquery first so the parser accepts the SQL.
    let rewritten = rewrite_window_tvfs(sql);
    let (key_column, agg_exprs) = extract_key_and_aggs(&rewritten)?;

    let spec = WindowExecutionSpec {
        key_column,
        key_column_type: String::from("utf8"),
        event_time_column,
        watermark_lag_ms: 0,
        window_kind,
        window_size_ms,
        slide_ms,
        session_gap_ms,
        agg_exprs,
        state_ttl_ms: None,
        allowed_lateness_ms: None,
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
        window_timezone: None,
    };
    Ok(StreamingWindowPlan { spec, source })
}

const WINDOW_BOUNDARY_COLS: [&str; 2] = ["window_start", "window_end"];

fn extract_key_and_aggs(sql: &str) -> SqlResult<(String, Vec<WindowAgg>)> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql)
        .map_err(|e| unsupported(format!("streaming window query parse error: {e}")))?;
    let query = stmts
        .into_iter()
        .find_map(|s| match s {
            Statement::Query(q) => Some(q),
            _ => None,
        })
        .ok_or_else(|| unsupported("streaming window query must be a SELECT"))?;

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(unsupported("streaming window query must be a plain SELECT"));
    };

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
    let kind = match fname.as_str() {
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
    let input_column = first_arg_column(f);
    let output_column = alias.unwrap_or_else(|| match &input_column {
        Some(col) => format!("{fname}_{col}"),
        None => fname.clone(),
    });
    Ok(WindowAgg {
        kind,
        input_column: input_column.unwrap_or_default(),
        output_column,
    })
}

fn first_arg_column(f: &Function) -> Option<String> {
    let FunctionArguments::List(list) = &f.args else {
        return None;
    };
    for fa in &list.args {
        let expr = match fa {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
            FunctionArg::Named {
                arg: FunctionArgExpr::Expr(e),
                ..
            } => Some(e),
            _ => None,
        };
        match expr {
            Some(Expr::Identifier(id)) => return Some(id.value.clone()),
            Some(Expr::CompoundIdentifier(parts)) => return parts.last().map(|p| p.value.clone()),
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
