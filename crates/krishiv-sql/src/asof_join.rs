#![forbid(unsafe_code)]
//! `ASOF JOIN`: for each left row, the single closest right row in time.
//!
//! The accepted form is the one the parser knows, Snowflake's:
//!
//! ```sql
//! SELECT … FROM trades t
//! ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON t.sym = q.sym
//! ```
//!
//! and the semantics are Snowflake's too: `MATCH_CONDITION` is one `>=`, `>`,
//! `<=` or `<` comparison between a left and a right expression; each left row
//! gets the right row that satisfies it and is nearest (the latest `q.ts` for
//! `>=`, the earliest for `<=`); a left row with no such right row is kept,
//! with NULLs for the right columns. Which row wins a tie is unspecified.
//!
//! DataFusion 54 has no ASOF operator and rejects the syntax. It is planned
//! here as what it means: the left rows are numbered, joined to every right
//! row that satisfies the condition, and only the nearest match per left row
//! is kept. That reads every qualifying right row, so it costs what a range
//! join costs, not what a merge would.
//!
//! The two halves meet through [`ASOF_MARKER`]. The statement rewrite in
//! [`crate::spark_generators`] turns the join into a LEFT JOIN whose `ON`
//! carries the match condition inside a call to that function; [`AsofJoinRule`]
//! finds the call and builds the plan. The function itself only ever raises an
//! error, so a join the rule somehow missed fails loudly instead of running as
//! an ordinary join.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result, plan_err};
use datafusion::error::DataFusionError;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Expr, ExprFunctionExt, JoinType, LogicalPlan, LogicalPlanBuilder,
    Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, col, lit,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;

/// The function the statement rewrite wraps an ASOF match condition in.
pub(crate) const ASOF_MARKER: &str = "krishiv_asof_match";

const ROW_ID: &str = "__krishiv_asof_row";
const RANK: &str = "__krishiv_asof_rank";

/// Register the marker function and the rule that plans the join.
pub fn register_asof_join(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(AsofMarker {
        signature: Signature::exact(vec![DataType::Boolean], Volatility::Immutable),
    }));
    ctx.add_analyzer_rule(Arc::new(AsofJoinRule));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct AsofMarker {
    signature: Signature,
}

impl ScalarUDFImpl for AsofMarker {
    fn name(&self) -> &str {
        ASOF_MARKER
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(DataFusionError::Execution(String::from(
            "ASOF JOIN was not planned: the match condition reached execution unrewritten",
        )))
    }
}

/// Plans a join carrying [`ASOF_MARKER`] as "nearest match per left row".
#[derive(Debug)]
pub struct AsofJoinRule;

impl AnalyzerRule for AsofJoinRule {
    fn name(&self) -> &str {
        "asof_join"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|node| match plan_asof(&node)? {
            Some(rewritten) => Ok(Transformed::yes(rewritten)),
            None => Ok(Transformed::no(node)),
        })
        .map(|transformed| transformed.data)
    }
}

fn marker_argument(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::ScalarFunction(call) if call.func.name() == ASOF_MARKER => call.args.first(),
        _ => None,
    }
}

/// Whether every column `expr` reads is one of `schema`'s.
fn reads_only(expr: &Expr, schema: &DFSchema) -> bool {
    let columns = expr.column_refs();
    !columns.is_empty() && columns.iter().all(|column| schema.has_column(column))
}

fn plan_asof(node: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Join(join) = node else {
        return Ok(None);
    };
    let Some(filter) = &join.filter else {
        return Ok(None);
    };
    let (markers, rest): (Vec<Expr>, Vec<Expr>) = split_conjunction_owned(filter.clone())
        .into_iter()
        .partition(|conjunct| marker_argument(conjunct).is_some());
    let condition = match markers.as_slice() {
        [] => return Ok(None),
        [marker] => marker_argument(marker).cloned(),
        _ => return plan_err!("ASOF JOIN takes one MATCH_CONDITION"),
    };
    let Some(Expr::BinaryExpr(BinaryExpr { left, op, right })) = condition else {
        return plan_err!("ASOF JOIN: MATCH_CONDITION must be a single >=, >, <= or < comparison");
    };

    // `nearest_is_largest`: the right-hand value wanted is the greatest one
    // that still satisfies the comparison, as written left-vs-right.
    let nearest_is_largest = match op {
        Operator::GtEq | Operator::Gt => true,
        Operator::LtEq | Operator::Lt => false,
        other => {
            return plan_err!(
                "ASOF JOIN: MATCH_CONDITION must compare with >=, >, <= or <, not `{other}`"
            );
        }
    };
    let (left_schema, right_schema) = (join.left.schema(), join.right.schema());
    let (right_value, nearest_is_largest) =
        if reads_only(&left, left_schema) && reads_only(&right, right_schema) {
            (*right.clone(), nearest_is_largest)
        } else if reads_only(&left, right_schema) && reads_only(&right, left_schema) {
            // Written right-vs-left: the same comparison, read the other way.
            (*left.clone(), !nearest_is_largest)
        } else {
            return plan_err!(
                "ASOF JOIN: MATCH_CONDITION must compare an expression over the left table \
                 with one over the right table"
            );
        };

    let (left_keys, right_keys): (Vec<Expr>, Vec<Expr>) = join.on.iter().cloned().unzip();
    let mut filters = rest;
    filters.push(Expr::BinaryExpr(BinaryExpr { left, op, right }));

    let output: Vec<Expr> = node
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    let numbered = LogicalPlanBuilder::from(join.left.as_ref().clone())
        .window(vec![row_number().alias(ROW_ID)])?
        .build()?;
    let rank = row_number()
        .partition_by(vec![col(ROW_ID)])
        // Ascending when the smallest qualifying value is nearest. NULLs (a
        // left row with no match) sort last either way; there is only one.
        .order_by(vec![right_value.sort(!nearest_is_largest, false)])
        .build()?
        .alias(RANK);
    let plan = LogicalPlanBuilder::from(numbered)
        .join_with_expr_keys(
            join.right.as_ref().clone(),
            JoinType::Left,
            (left_keys, right_keys),
            conjunction(filters),
        )?
        .window(vec![rank])?
        .filter(col(RANK).eq(lit(1_u64)))?
        .project(output)?
        .build()?;
    Ok(Some(plan))
}

#[cfg(test)]
mod tests {
    use arrow::util::pretty::pretty_format_batches;

    async fn engine() -> crate::SqlEngine {
        let engine = crate::SqlEngine::new();
        for ddl in [
            "CREATE TABLE trades AS SELECT * FROM (VALUES \
               ('A', TIMESTAMP '2024-01-01 10:00:05', 100), \
               ('A', TIMESTAMP '2024-01-01 10:00:20', 101), \
               ('B', TIMESTAMP '2024-01-01 10:00:07', 50), \
               ('C', TIMESTAMP '2024-01-01 10:00:00', 7)) AS t(sym, ts, qty)",
            "CREATE TABLE quotes AS SELECT * FROM (VALUES \
               ('A', TIMESTAMP '2024-01-01 10:00:00', 1.0), \
               ('A', TIMESTAMP '2024-01-01 10:00:10', 1.5), \
               ('A', TIMESTAMP '2024-01-01 10:00:30', 2.0), \
               ('B', TIMESTAMP '2024-01-01 10:00:09', 9.0)) AS t(sym, ts, px)",
        ] {
            engine
                .sql(ddl)
                .await
                .expect("plan ddl")
                .collect()
                .await
                .expect("run ddl");
        }
        engine
    }

    async fn rows(sql: &str) -> Vec<String> {
        let batches = engine()
            .await
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("plan `{sql}`: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("run `{sql}`: {e}"));
        pretty_format_batches(&batches)
            .expect("format")
            .to_string()
            .lines()
            .filter(|line| line.starts_with('|'))
            .skip(1)
            .map(|line| {
                line.trim_matches('|')
                    .split('|')
                    .map(str::trim)
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect()
    }

    async fn error(sql: &str) -> String {
        let engine = engine().await;
        match engine.sql(sql).await {
            Err(e) => e.to_string(),
            Ok(df) => df.collect().await.expect_err("must fail").to_string(),
        }
    }

    /// Each trade gets the latest quote at or before it; a trade with no
    /// earlier quote (B at :07, C) is kept with NULLs.
    #[tokio::test]
    async fn takes_the_latest_earlier_row() {
        assert_eq!(
            rows(
                "SELECT t.sym, t.qty, q.px FROM trades t \
                 ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON t.sym = q.sym \
                 ORDER BY t.sym, t.qty"
            )
            .await,
            ["A|100|1.0", "A|101|1.5", "B|50|", "C|7|"]
        );
    }

    #[tokio::test]
    async fn takes_the_earliest_later_row() {
        assert_eq!(
            rows(
                "SELECT t.sym, t.qty, q.px FROM trades t \
                 ASOF JOIN quotes q MATCH_CONDITION (t.ts <= q.ts) ON t.sym = q.sym \
                 ORDER BY t.sym, t.qty"
            )
            .await,
            ["A|100|1.5", "A|101|2.0", "B|50|9.0", "C|7|"]
        );
    }

    /// The comparison may be written with the right side first, and strictness
    /// is honoured: a quote at the very same instant does not match `>`.
    #[tokio::test]
    async fn the_comparison_reads_either_way_round_and_can_be_strict() {
        assert_eq!(
            rows(
                "SELECT t.sym, t.qty, q.px FROM trades t \
                 ASOF JOIN quotes q MATCH_CONDITION (q.ts <= t.ts) ON t.sym = q.sym \
                 ORDER BY t.sym, t.qty"
            )
            .await,
            ["A|100|1.0", "A|101|1.5", "B|50|", "C|7|"]
        );
        // A quote exists at exactly 10:00:00 for a trade made then.
        assert_eq!(
            rows(
                "SELECT x.px FROM (SELECT 'A' AS sym, TIMESTAMP '2024-01-01 10:00:00' AS ts) t \
                 ASOF JOIN quotes x MATCH_CONDITION (t.ts >= x.ts) ON t.sym = x.sym"
            )
            .await,
            ["1.0"]
        );
        assert_eq!(
            rows(
                "SELECT x.px FROM (SELECT 'A' AS sym, TIMESTAMP '2024-01-01 10:00:00' AS ts) t \
                 ASOF JOIN quotes x MATCH_CONDITION (t.ts > x.ts) ON t.sym = x.sym"
            )
            .await,
            [""]
        );
    }

    /// One output row per left row, however many right rows qualify, and the
    /// join composes with what follows it.
    #[tokio::test]
    async fn yields_one_row_per_left_row_and_composes() {
        assert_eq!(
            rows(
                "SELECT count(*) AS n FROM trades t \
                 ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON t.sym = q.sym"
            )
            .await,
            ["4"]
        );
        assert_eq!(
            rows(
                "SELECT t.sym, sum(t.qty * q.px) AS notional FROM trades t \
                 ASOF JOIN quotes q MATCH_CONDITION (t.ts >= q.ts) ON t.sym = q.sym \
                 WHERE q.px IS NOT NULL GROUP BY t.sym"
            )
            .await,
            ["A|251.5"]
        );
    }

    #[tokio::test]
    async fn a_match_condition_that_is_not_one_comparison_is_refused() {
        assert!(
            error(
                "SELECT 1 FROM trades t ASOF JOIN quotes q \
                 MATCH_CONDITION (t.ts = q.ts) ON t.sym = q.sym"
            )
            .await
            .contains(">=, >, <= or <")
        );
        assert!(
            error(
                "SELECT 1 FROM trades t ASOF JOIN quotes q \
                 MATCH_CONDITION (t.ts >= t.ts) ON t.sym = q.sym"
            )
            .await
            .contains("left table")
        );
    }
}
