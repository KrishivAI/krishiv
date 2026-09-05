//! `ROLLUP` / `CUBE` / `GROUPING SETS` as one aggregate plus re-aggregation.
//!
//! # The gap this closes
//!
//! DataFusion evaluates grouping sets by expanding **every input row once per
//! set** into one hash aggregate: a `ROLLUP` of four columns is five sets, so
//! 2.46 M inventory rows become 12.3 M grouped rows. `EXPLAIN ANALYZE` on
//! TPC-DS q22 at SF1 puts 875 ms of CPU in that `Partial` aggregate for 108 K
//! output groups; q67's `ROLLUP` of eight is nine sets and 848 ms.
//!
//! DuckDB aggregates the finest set once and rolls the results up. Rewriting
//! q22 and q67 that way by hand — the finest `GROUP BY` in a CTE, one
//! re-aggregation per set, `UNION ALL` — with results identical to two decimals:
//!
//! ```text
//!   q22  459 ms -> 160 ms   2.87x   (DuckDB 191 ms — now faster)
//!   q67  622 ms -> 480 ms   1.30x
//! ```
//!
//! # Measured across the suite
//!
//! All 99 TPC-DS queries at SF1, warm, best of three, paired and interleaved
//! on a quiet machine, with the CTE cache and this rewrite against neither:
//! 16940 -> 14125 ms, 99/99 rows identical. This rewrite's own share: q22
//! 501 -> 160 ms (3.14x), q67 664 -> 515 ms (1.29x); q18, q70 and q86 are
//! neutral — q18's `avg` is over a decimal and declines, and the other two have
//! little in the aggregate to begin with. Against DuckDB the suite went 14891
//! -> 14393 ms and q22 became faster than it.
//!
//! # The rewrite
//!
//! ```text
//!   Aggregate: groupBy=[[ROLLUP (a, b)]], aggr=[[avg(q), count(*), grouping(a)]]
//!
//!   ==>
//!
//!   Projection: <original names and qualifiers>
//!     Union
//!       Projection: a, b,       __grouping_id=0, grouping(a)=0, sum(s)/sum(c), sum(n)
//!         Aggregate: groupBy=[[a, b]], aggr=[[sum(s), sum(c), sum(n)]]
//!           SubqueryAlias: __krishiv_rollup_0            <- identical in every branch
//!             Aggregate: groupBy=[[a, b]], aggr=[[sum(q) AS s, count(q) AS c, count(*) AS n]]
//!       Projection: a, NULL,    __grouping_id=1, grouping(a)=0, …
//!         Aggregate: groupBy=[[a]], …  over the same alias
//!       Projection: NULL, NULL, __grouping_id=3, grouping(a)=1, …
//!         Aggregate: groupBy=[[]], …  over the same alias
//! ```
//!
//! The finest aggregate appears once per set, as the same `SubqueryAlias`, and
//! [`crate::cte_materialize`] — which runs right after this, on the same
//! unoptimized plan — collects it once and points every branch at the cache.
//! That coupling is deliberate: without the cache the rewrite would compute
//! the finest aggregate N+1 times and be a loss, so it runs only where the
//! cache does.
//!
//! # What is preserved, and how
//!
//! - **The schema, exactly.** Group columns, `__grouping_id`, and every
//!   aggregate output keep their name and qualifier via qualified aliases, and
//!   the rewrite is abandoned if the rebuilt schema's names, qualifiers or
//!   types differ from the original's.
//! - **`__grouping_id`.** Bit `n-1-i` is set when group expression `i` is
//!   absent from the set, matching `Aggregate`'s own encoding, so the
//!   analyzer's later rewrite of `grouping(x)` into bit tests reads the right
//!   answer; the literal takes the original column's integer type.
//! - **`grouping(x)`** is emitted directly as `0`/`1` per set, so it is right
//!   whether or not anything later reads `__grouping_id`.
//! - **A real `NULL` group value** stays distinguishable from a rolled-up one,
//!   because the two differ in `__grouping_id` exactly as before.
//!
//! # What it declines
//!
//! Any aggregate that is not `sum`, `count`, `min`, `max`, `avg` or
//! `grouping`; `DISTINCT`, a `FILTER`, an `ORDER BY` or null treatment on any
//! of them; an `avg` whose output is not `Float64` (a decimal average has a
//! scale rule of its own); a duplicated group expression (DataFusion then adds
//! ordinal bits to `__grouping_id`); more than 64 sets; anything not exactly
//! one grouping-set expression.

use crate::SqlResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, ScalarValue, TableReference};
use datafusion::logical_expr::ExprSchemable;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{
    Aggregate, Expr, GroupingSet, LogicalPlan, LogicalPlanBuilder, SubqueryAlias, col, lit,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Prefix for the alias the finest aggregate is shared through.
const ALIAS_PREFIX: &str = "__krishiv_rollup_";
const MAX_SETS: usize = 64;
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// Rewrite every grouping-set aggregate in `plan`; `None` if nothing changed.
pub fn rewrite_grouping_sets(plan: &LogicalPlan) -> SqlResult<Option<LogicalPlan>> {
    let rewritten = plan
        .clone()
        .transform_down_with_subqueries(|node| {
            Ok(match &node {
                LogicalPlan::Aggregate(aggregate) => match rewrite_one(aggregate) {
                    Some(new) => Transformed::yes(new),
                    None => Transformed::no(node),
                },
                _ => Transformed::no(node),
            })
        })
        .map_err(|error| crate::SqlError::DataFusion {
            message: format!("grouping-set rewrite: {error}"),
        })?;
    Ok(rewritten.transformed.then_some(rewritten.data))
}

/// One aggregate's worth of the rewrite; `None` leaves it alone.
fn rewrite_one(aggregate: &Aggregate) -> Option<LogicalPlan> {
    let [Expr::GroupingSet(grouping_set)] = aggregate.group_expr.as_slice() else {
        return None;
    };
    let (finest, sets) = enumerate(grouping_set)?;
    let n = finest.len();

    // The original output layout: group columns, `__grouping_id`, aggregates.
    let outputs: Vec<(Option<TableReference>, String, DataType)> = aggregate
        .schema
        .iter()
        .map(|(qualifier, field)| {
            (
                qualifier.cloned(),
                field.name().clone(),
                field.data_type().clone(),
            )
        })
        .collect();
    if outputs.len() != n + 1 + aggregate.aggr_expr.len() {
        return None;
    }
    let (_, grouping_id_name, grouping_id_type) = outputs.get(n)?.clone();
    if grouping_id_name != Aggregate::INTERNAL_GROUPING_ID {
        return None;
    }

    // Decompose each aggregate into finest-level partials and a re-aggregation.
    let mut partials: Vec<Expr> = Vec::new();
    let mut plans: Vec<Reagg> = Vec::with_capacity(aggregate.aggr_expr.len());
    for (index, expr) in aggregate.aggr_expr.iter().enumerate() {
        let (_, _, out_type) = outputs.get(n + 1 + index)?;
        plans.push(decompose(expr, index, out_type, &finest, &mut partials)?);
    }

    let alias = format!("{ALIAS_PREFIX}{}", NEXT_ID.fetch_add(1, Ordering::Relaxed));
    let finest_plan = LogicalPlanBuilder::from(aggregate.input.as_ref().clone())
        .aggregate(finest.clone(), partials)
        .ok()?
        .build()
        .ok()?;
    let finest_names: Vec<String> = finest_plan
        .schema()
        .fields()
        .iter()
        .take(n)
        .map(|field| field.name().clone())
        .collect();
    let shared = LogicalPlan::SubqueryAlias(
        SubqueryAlias::try_new(Arc::new(finest_plan), alias.clone()).ok()?,
    );
    let shared_col = |name: &str| Expr::Column(Column::new(Some(alias.clone()), name));

    let mut branches = Vec::with_capacity(sets.len());
    for set in &sets {
        let group: Vec<Expr> = set
            .iter()
            .map(|index| finest_names.get(*index).map(|name| shared_col(name)))
            .collect::<Option<_>>()?;
        let level_aggr: Vec<Expr> = plans
            .iter()
            .flat_map(|plan| plan.level_aggregates(&shared_col))
            .collect();
        let level = LogicalPlanBuilder::from(shared.clone())
            .aggregate(group, level_aggr)
            .ok()?
            .build()
            .ok()?;

        let mut projection: Vec<Expr> = Vec::with_capacity(outputs.len());
        let mut grouping_id: u64 = 0;
        for (index, (qualifier, name, data_type)) in outputs.iter().take(n).enumerate() {
            let present = set.contains(&index);
            if !present {
                grouping_id |= 1 << (n - 1 - index);
            }
            let expr = if present {
                shared_col(finest_names.get(index)?)
            } else {
                lit(ScalarValue::Null)
                    .cast_to(data_type, level.schema())
                    .ok()?
            };
            projection.push(qualified_alias(expr, qualifier, name));
        }
        let (gq, gname, _) = outputs.get(n)?;
        projection.push(qualified_alias(
            grouping_literal(grouping_id, &grouping_id_type)?,
            gq,
            gname,
        ));
        for (index, plan) in plans.iter().enumerate() {
            let (qualifier, name, data_type) = outputs.get(n + 1 + index)?;
            let expr = plan.output(set, &finest, level.schema(), data_type)?;
            projection.push(qualified_alias(expr, qualifier, name));
        }
        branches.push(
            LogicalPlanBuilder::from(level)
                .project(projection)
                .ok()?
                .build()
                .ok()?,
        );
    }

    let mut union = LogicalPlanBuilder::from(branches.first()?.clone());
    for branch in branches.iter().skip(1) {
        union = union.union(branch.clone()).ok()?;
    }
    let restore: Vec<Expr> = outputs
        .iter()
        .map(|(qualifier, name, _)| qualified_alias(col(name.as_str()), qualifier, name))
        .collect();
    let rebuilt = union.project(restore).ok()?.build().ok()?;

    // Names, qualifiers and types must round-trip exactly; nullability may
    // widen through the union and is not compared.
    let same = rebuilt
        .schema()
        .iter()
        .zip(aggregate.schema.iter())
        .all(|((q1, f1), (q2, f2))| {
            q1 == q2 && f1.name() == f2.name() && f1.data_type() == f2.data_type()
        })
        && rebuilt.schema().fields().len() == aggregate.schema.fields().len();
    same.then_some(rebuilt)
}

/// The distinct group expressions in order, and each set as indices into them.
fn enumerate(grouping_set: &GroupingSet) -> Option<(Vec<Expr>, Vec<Vec<usize>>)> {
    let distinct = |exprs: &[Expr]| -> Option<Vec<Expr>> {
        let mut out: Vec<Expr> = Vec::new();
        for expr in exprs {
            if out.contains(expr) {
                return None;
            }
            out.push(expr.clone());
        }
        Some(out)
    };
    let (finest, sets): (Vec<Expr>, Vec<Vec<usize>>) = match grouping_set {
        GroupingSet::Rollup(exprs) => {
            let finest = distinct(exprs)?;
            let sets = (0..=finest.len()).rev().map(|k| (0..k).collect()).collect();
            (finest, sets)
        }
        GroupingSet::Cube(exprs) => {
            let finest = distinct(exprs)?;
            let n = finest.len();
            if n > 6 {
                return None;
            }
            let sets = (0..(1usize << n))
                .rev()
                .map(|mask| (0..n).filter(|i| mask & (1 << i) != 0).collect())
                .collect();
            (finest, sets)
        }
        GroupingSet::GroupingSets(all) => {
            let mut finest: Vec<Expr> = Vec::new();
            for set in all {
                for expr in set {
                    if !finest.contains(expr) {
                        finest.push(expr.clone());
                    }
                }
            }
            let sets = all
                .iter()
                .map(|set| {
                    let mut indices: Vec<usize> = set
                        .iter()
                        .filter_map(|expr| finest.iter().position(|f| f == expr))
                        .collect();
                    indices.sort_unstable();
                    indices.dedup();
                    indices
                })
                .collect::<Vec<_>>();
            if sets
                .iter()
                .any(|s| s.is_empty() && !all.iter().any(|a| a.is_empty()))
            {
                return None;
            }
            (finest, sets)
        }
    };
    (!finest.is_empty() && !sets.is_empty() && sets.len() <= MAX_SETS).then_some((finest, sets))
}

/// How one original aggregate is computed at the finest level and above it.
enum Reagg {
    /// `sum`/`min`/`max`/`count`: one partial, one re-aggregation of it.
    Simple {
        partial: String,
        reagg: fn(Expr) -> Expr,
    },
    /// `avg`: a sum and a count at the finest level, divided above it.
    Avg { sum: String, count: String },
    /// `grouping(expr)`: a literal per set.
    Grouping { position: usize },
}

impl Reagg {
    fn level_aggregates(&self, shared_col: &dyn Fn(&str) -> Expr) -> Vec<Expr> {
        match self {
            Reagg::Simple { partial, reagg } => vec![reagg(shared_col(partial)).alias(partial)],
            Reagg::Avg { sum, count } => vec![
                sum_of(shared_col(sum)).alias(sum),
                sum_of(shared_col(count)).alias(count),
            ],
            Reagg::Grouping { .. } => vec![],
        }
    }

    fn output(
        &self,
        set: &[usize],
        _finest: &[Expr],
        schema: &datafusion::common::DFSchema,
        out_type: &DataType,
    ) -> Option<Expr> {
        match self {
            Reagg::Simple { partial, .. } => Some(col(partial.as_str())),
            Reagg::Avg { sum, count } => {
                let sum = col(sum.as_str()).cast_to(&DataType::Float64, schema).ok()?;
                let count = col(count.as_str())
                    .cast_to(&DataType::Float64, schema)
                    .ok()?;
                Some(sum / count)
            }
            Reagg::Grouping { position } => {
                let absent = !set.contains(position);
                Some(
                    lit(ScalarValue::Int32(Some(i32::from(absent))))
                        .cast_to(out_type, schema)
                        .ok()?,
                )
            }
        }
    }
}

fn sum_of(expr: Expr) -> Expr {
    datafusion::functions_aggregate::expr_fn::sum(expr)
}
fn min_of(expr: Expr) -> Expr {
    datafusion::functions_aggregate::expr_fn::min(expr)
}
fn max_of(expr: Expr) -> Expr {
    datafusion::functions_aggregate::expr_fn::max(expr)
}

/// Split one aggregate into its finest-level partials, appending them.
fn decompose(
    expr: &Expr,
    index: usize,
    out_type: &DataType,
    finest: &[Expr],
    partials: &mut Vec<Expr>,
) -> Option<Reagg> {
    let Expr::AggregateFunction(AggregateFunction { func, params }) = expr else {
        return None;
    };
    if params.distinct
        || params.filter.is_some()
        || !params.order_by.is_empty()
        || params.null_treatment.is_some()
    {
        return None;
    }
    let name = func.name();
    let args = &params.args;
    match (name, args.as_slice()) {
        ("grouping", [arg]) => {
            let position = finest.iter().position(|f| f == arg)?;
            Some(Reagg::Grouping { position })
        }
        ("sum", [_]) | ("min", [_]) | ("max", [_]) | ("count", [_]) => {
            let partial = format!("__p{index}");
            partials.push(expr.clone().alias(&partial));
            let reagg: fn(Expr) -> Expr = match name {
                "min" => min_of,
                "max" => max_of,
                _ => sum_of,
            };
            Some(Reagg::Simple { partial, reagg })
        }
        ("avg", [arg]) => {
            if *out_type != DataType::Float64 {
                return None;
            }
            let sum = format!("__p{index}_s");
            let count = format!("__p{index}_c");
            let as_float = Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(arg.clone()),
                DataType::Float64,
            ));
            partials.push(sum_of(as_float).alias(&sum));
            partials
                .push(datafusion::functions_aggregate::expr_fn::count(arg.clone()).alias(&count));
            Some(Reagg::Avg { sum, count })
        }
        _ => None,
    }
}

fn qualified_alias(expr: Expr, qualifier: &Option<TableReference>, name: &str) -> Expr {
    match qualifier {
        Some(relation) => expr.alias_qualified(Some(relation.clone()), name),
        None => expr.alias(name),
    }
}

fn grouping_literal(value: u64, data_type: &DataType) -> Option<Expr> {
    Some(lit(match data_type {
        DataType::UInt8 => ScalarValue::UInt8(Some(u8::try_from(value).ok()?)),
        DataType::UInt16 => ScalarValue::UInt16(Some(u16::try_from(value).ok()?)),
        DataType::UInt32 => ScalarValue::UInt32(Some(u32::try_from(value).ok()?)),
        DataType::UInt64 => ScalarValue::UInt64(Some(value)),
        _ => return None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cte_materialize::materialize_repeated_ctes_forced;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    /// A real `NULL` in `cat` — the case a rollup must keep distinct from a
    /// rolled-up `cat`, which only `__grouping_id` tells apart.
    fn context() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("cat", DataType::Utf8, true),
            Field::new("cls", DataType::Utf8, true),
            Field::new("q", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                // (a, x) twice, so a finest group has two rows and a count
                // re-aggregated as `count` (2 groups) differs from one
                // re-aggregated as `sum` (3 rows).
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    None,
                    Some("b"),
                    Some("a"),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("x"),
                    Some("y"),
                    Some("x"),
                    Some("y"),
                    Some("x"),
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 10, 5])),
            ],
        )
        .expect("batch");
        let ctx = SessionContext::new();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table")),
        )
        .expect("register");
        ctx
    }

    async fn rows(ctx: &SessionContext, sql: &str, rewrite: bool) -> String {
        let frame = ctx.sql(sql).await.expect("plan");
        let frame = if rewrite {
            materialize_repeated_ctes_forced(ctx, frame)
                .await
                .expect("rewrite")
        } else {
            frame
        };
        let batches = frame.collect().await.expect("collect");
        let text = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .expect("format")
            .to_string();
        let mut lines: Vec<&str> = text.lines().collect();
        lines.sort_unstable();
        lines.join("\n")
    }

    async fn plan(ctx: &SessionContext, sql: &str) -> String {
        let frame = ctx.sql(sql).await.expect("plan");
        let frame = materialize_repeated_ctes_forced(ctx, frame)
            .await
            .expect("rewrite");
        format!(
            "{}",
            frame
                .into_optimized_plan()
                .expect("optimize")
                .display_indent()
        )
    }

    const ROLLUP: &str = "SELECT cat, cls, grouping(cat) g1, grouping(cat) + grouping(cls) g2, \
                          avg(q) a, count(*) c, sum(q) s, min(q) mn, max(q) mx \
                          FROM t GROUP BY ROLLUP(cat, cls)";
    const CUBE: &str = "SELECT cat, cls, grouping(cls) g, avg(q) a, count(q) c \
                        FROM t GROUP BY CUBE(cat, cls)";
    const SETS: &str = "SELECT cat, cls, sum(q) s, count(*) c \
                        FROM t GROUP BY GROUPING SETS ((cat, cls), (cls), ())";

    /// The rewrite must fire and must not change one row — including the row
    /// for the real `NULL` category, its `grouping()` values, and the average.
    #[tokio::test]
    async fn a_rollup_rewritten_returns_the_same_rows() {
        let ctx = context();
        let plan = plan(&ctx, ROLLUP).await;
        assert!(!plan.contains("ROLLUP"), "the ROLLUP must be gone:\n{plan}");
        assert_eq!(
            rows(&ctx, ROLLUP, true).await,
            rows(&context(), ROLLUP, false).await
        );
    }

    #[tokio::test]
    async fn a_cube_rewritten_returns_the_same_rows() {
        let ctx = context();
        let plan = plan(&ctx, CUBE).await;
        assert!(!plan.contains("CUBE"), "the CUBE must be gone:\n{plan}");
        assert_eq!(
            rows(&ctx, CUBE, true).await,
            rows(&context(), CUBE, false).await
        );
    }

    #[tokio::test]
    async fn grouping_sets_rewritten_return_the_same_rows() {
        let ctx = context();
        let plan = plan(&ctx, SETS).await;
        assert!(
            !plan.contains("GROUPING SETS"),
            "the sets must be gone:\n{plan}"
        );
        assert_eq!(
            rows(&ctx, SETS, true).await,
            rows(&context(), SETS, false).await
        );
    }

    /// The finest aggregate is computed once: every branch reads the cache.
    ///
    /// Without this the rewrite is a loss — N+1 copies of the finest aggregate
    /// instead of one grouping-set pass — which is why it is coupled to the
    /// materialiser rather than registered as an optimizer rule.
    #[tokio::test]
    async fn the_finest_aggregate_is_shared_through_the_cache() {
        let ctx = context();
        let plan = plan(&ctx, ROLLUP).await;
        assert_eq!(
            plan.matches("TableScan: __krishiv_cte_").count(),
            3,
            "three rollup levels must read one cached finest aggregate:\n{plan}"
        );
        assert_eq!(
            plan.matches("TableScan: t").count(),
            0,
            "the base table must be scanned only inside the cached body:\n{plan}"
        );
    }

    /// An aggregate the rewrite cannot decompose leaves the plan alone.
    #[tokio::test]
    async fn a_distinct_count_declines_the_rewrite() {
        let ctx = context();
        let plan = plan(
            &ctx,
            "SELECT cat, count(DISTINCT cls) FROM t GROUP BY ROLLUP(cat)",
        )
        .await;
        assert!(
            plan.contains("ROLLUP"),
            "count(DISTINCT) cannot be re-aggregated:\n{plan}"
        );
    }
}
