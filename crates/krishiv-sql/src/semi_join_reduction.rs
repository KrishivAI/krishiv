//! Semi-join reduction through an aggregate.
//!
//! When a grouped aggregate is inner-joined on one of its own grouping keys,
//! only the groups whose key survives the join can appear in the result. Every
//! other group is computed and then discarded. Filtering the aggregate's
//! *input* down to the surviving keys first produces exactly the same groups,
//! because an aggregate value depends only on the rows sharing its key.
//!
//! # The query that motivated this
//!
//! TPC-H q17 decorrelates to this shape:
//!
//! ```text
//! Inner Join: part.p_partkey = __scalar_sq_1.l_partkey
//!   ├── Inner Join: lineitem.l_partkey = part.p_partkey
//!   │     └── Filter: p_brand = 'Brand#23' AND p_container = 'MED BOX'
//!   └── __scalar_sq_1:
//!         Aggregate: groupBy=[l_partkey], aggr=[avg(l_quantity)]
//!           TableScan: lineitem
//! ```
//!
//! At SF100 that aggregate groups all 600M lineitem rows into ~20M groups, and
//! the join then keeps the ~2000 partkeys matching the brand and container —
//! four orders of magnitude of thrown-away work. Measured with
//! `explain --analyze`, it was 221.03 s of a 252 s query, 88% of all compute,
//! with `spill_count=0`: not a memory problem, just work that need not happen.
//!
//! DataFusion's dynamic filter does not help here, and it is worth recording
//! why, because the plan *looks* like it should:
//!
//! ```text
//! DynamicFilter [ ... l_partkey >= 7682 AND l_partkey <= 19999654 AND hash_lookup ... ]
//! ```
//!
//! The min/max bounds span essentially the whole key domain, since the 2000
//! surviving partkeys are scattered uniformly across it, so row-group pruning
//! removes nothing. And the filter belongs to the *join*, which sits downstream
//! of the aggregate — no amount of selectivity there can reduce what the
//! aggregate already had to read.
//!
//! # What this rule does
//!
//! It rewrites the aggregate's input to a `LeftSemi` join against the smallest
//! subtree of the other side that still produces the join key *and* contains a
//! filter:
//!
//! ```text
//! Aggregate: groupBy=[l_partkey], aggr=[avg(l_quantity)]
//!   LeftSemi Join: lineitem.l_partkey = part.p_partkey
//!     TableScan: lineitem
//!     Projection: part.p_partkey
//!       Filter: p_brand = 'Brand#23' AND p_container = 'MED BOX'
//! ```
//!
//! # Why it is safe
//!
//! - **Inner joins only.** Under a left/right/full join the unmatched rows are
//!   preserved, so dropping groups would change the result. Anti/semi joins are
//!   also excluded — they have their own null semantics.
//! - **The key must be a grouping column**, matched by *schema position* rather
//!   than by name, so requalification through `SubqueryAlias` and projections
//!   cannot silently pair the wrong columns.
//! - **Aggregate values are unchanged.** Removing rows whose key is not in the
//!   probe side removes whole groups; it never removes part of a surviving
//!   group, so no aggregate is computed over a different row set.
//! - **Nulls agree.** A null key never satisfies an equi-join, so a null group
//!   would be dropped by the original join anyway; `LeftSemi` drops it too.
//! - **No duplication.** `LeftSemi` emits each left row at most once regardless
//!   of how many probe rows match, so counts and sums cannot inflate.
//!
//! # Why it is guarded
//!
//! The probe subtree is evaluated a second time, so the rule only fires when
//! that subtree contains a `Filter` — evidence there is real selectivity to
//! exploit. Against an unfiltered scan the semi-join would remove nothing and
//! we would have paid for the extra pass. Descent also stops *at* the filter
//! rather than continuing to the scan beneath it, which is what keeps the probe
//! small (~2000 rows in q17 rather than the whole `part` table).
//!
//! Set `KRISHIV_SEMI_JOIN_REDUCTION=off` to disable.

use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DFSchema, Result};
use datafusion::logical_expr::{
    Aggregate, Expr, Join, JoinType, LogicalPlan, LogicalPlanBuilder, Projection, SubqueryAlias,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use std::sync::Arc;

/// Environment switch for the rule.
pub const SEMI_JOIN_REDUCTION_ENV: &str = "KRISHIV_SEMI_JOIN_REDUCTION";

/// Whether semi-join reduction through aggregates is enabled (default: yes).
pub fn semi_join_reduction_enabled() -> bool {
    enabled_from(&std::env::var(SEMI_JOIN_REDUCTION_ENV).unwrap_or_default())
}

/// The switch's parsing, separated from reading the environment.
///
/// Kept pure so it can be tested directly: mutating process environment from a
/// test is unsound under a multi-threaded test runner, and the workspace denies
/// the `unsafe` that edition 2024 now requires for `set_var`.
fn enabled_from(value: &str) -> bool {
    !matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "0" | "off" | "false" | "no"
    )
}

/// Push a semi-join built from an inner join's other side into the input of a
/// grouped aggregate, when the join key is one of the grouping columns.
#[derive(Debug, Default)]
pub struct SemiJoinReductionThroughAggregate;

impl OptimizerRule for SemiJoinReductionThroughAggregate {
    fn name(&self) -> &str {
        "semi_join_reduction_through_aggregate"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // Bottom-up so inner joins are already in their final shape when we
        // look at them, and so DataFusion drives the recursion.
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !semi_join_reduction_enabled() {
            return Ok(Transformed::no(plan));
        }
        let LogicalPlan::Join(join) = &plan else {
            return Ok(Transformed::no(plan));
        };
        if join.join_type != JoinType::Inner || join.on.is_empty() {
            return Ok(Transformed::no(plan));
        }

        for (left_key, right_key) in &join.on {
            let (Expr::Column(left_col), Expr::Column(right_col)) = (left_key, right_key) else {
                continue;
            };
            // Either side may hold the aggregate; try both orientations.
            for (agg_side, agg_key, probe_side, probe_key) in [
                (&join.right, right_col, &join.left, left_col),
                (&join.left, left_col, &join.right, right_col),
            ] {
                let Some((probe, probe_col)) = selective_key_source(probe_side, probe_key)? else {
                    continue;
                };
                let Some(new_side) = push_through(agg_side, agg_key, &probe, &probe_col)? else {
                    continue;
                };
                let rebuilt = if Arc::ptr_eq(agg_side, &join.right) {
                    Join {
                        right: Arc::new(new_side),
                        ..join.clone()
                    }
                } else {
                    Join {
                        left: Arc::new(new_side),
                        ..join.clone()
                    }
                };
                return Ok(Transformed::yes(LogicalPlan::Join(rebuilt)));
            }
        }
        Ok(Transformed::no(plan))
    }
}

/// Position of `col` in `schema`, or `None` if it is not there.
///
/// Everything below matches columns by this index rather than by name.
/// `SubqueryAlias` requalifies every column and projections rename them, so
/// name matching across those boundaries is exactly where a rule like this
/// pairs the wrong two columns and silently returns wrong answers.
fn index_of(schema: &DFSchema, col: &Column) -> Option<usize> {
    schema.index_of_column(col).ok()
}

/// Rewrite `plan` so the aggregate beneath it filters its input by `probe`.
///
/// Returns `None` when the shape does not qualify, in which case the caller
/// leaves the plan alone. Descends only through nodes that pass rows through
/// one-for-one and preserve the key's position.
fn push_through(
    plan: &LogicalPlan,
    key: &Column,
    probe: &LogicalPlan,
    probe_key: &Column,
) -> Result<Option<LogicalPlan>> {
    let Some(idx) = index_of(plan.schema(), key) else {
        return Ok(None);
    };
    match plan {
        LogicalPlan::SubqueryAlias(alias) => {
            let (qualifier, field) = alias.input.schema().qualified_field(idx);
            let inner = Column::new(qualifier.cloned(), field.name());
            let Some(new_input) = push_through(&alias.input, &inner, probe, probe_key)? else {
                return Ok(None);
            };
            Ok(Some(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                Arc::new(new_input),
                alias.alias.clone(),
            )?)))
        }
        LogicalPlan::Projection(proj) => {
            // Only a straight column pass-through is safe to descend: an
            // expression could change the key's value, so the semi-join would
            // be filtering on something other than what the join compares.
            let Expr::Column(inner) = &proj.expr[idx] else {
                return Ok(None);
            };
            let inner = inner.clone();
            let Some(new_input) = push_through(&proj.input, &inner, probe, probe_key)? else {
                return Ok(None);
            };
            Ok(Some(LogicalPlan::Projection(Projection::try_new(
                proj.expr.clone(),
                Arc::new(new_input),
            )?)))
        }
        LogicalPlan::Aggregate(agg) => {
            // Grouping columns occupy the leading schema positions; anything
            // past them is an aggregate output, which is not a grouping key.
            if idx >= agg.group_expr.len() {
                return Ok(None);
            }
            let Expr::Column(group_col) = &agg.group_expr[idx] else {
                return Ok(None);
            };
            if already_reduced(&agg.input) {
                return Ok(None);
            }
            let reduced = LogicalPlanBuilder::from(agg.input.as_ref().clone())
                .join_on(
                    probe.clone(),
                    JoinType::LeftSemi,
                    [Expr::Column(group_col.clone()).eq(Expr::Column(probe_key.clone()))],
                )?
                .build()?;
            // LeftSemi preserves the left schema exactly, so the grouping and
            // aggregate expressions still resolve unchanged.
            Ok(Some(LogicalPlan::Aggregate(Aggregate::try_new(
                Arc::new(reduced),
                agg.group_expr.clone(),
                agg.aggr_expr.clone(),
            )?)))
        }
        _ => Ok(None),
    }
}

/// Has this aggregate input already been reduced by a previous pass?
///
/// The optimizer runs rules to a fixed point, so without this the rule would
/// stack a fresh semi-join on every iteration and never converge.
fn already_reduced(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Join(j) if j.join_type == JoinType::LeftSemi)
}

/// Smallest subtree of `plan` that still produces `key` and carries a filter.
///
/// Returns the subtree projected down to the key alone, plus the key's name
/// inside it. `None` means there is no filter on this side — the semi-join
/// would then remove nothing while costing an extra pass, so the rule declines.
fn selective_key_source(
    plan: &LogicalPlan,
    key: &Column,
) -> Result<Option<(LogicalPlan, Column)>> {
    let Some(source) = descend_to_filter(plan, key) else {
        return Ok(None);
    };
    let (subtree, col) = source;
    let projected = LogicalPlanBuilder::from(subtree)
        .project([Expr::Column(col.clone())])?
        .build()?;
    Ok(Some((projected, col)))
}

/// Walk down to the nearest `Filter` that still produces `key`.
///
/// Stopping *at* the filter rather than continuing to the scan below it is what
/// keeps the probe small: in q17 that is the ~2000 filtered parts instead of
/// the whole 20M-row `part` table.
fn descend_to_filter(plan: &LogicalPlan, key: &Column) -> Option<(LogicalPlan, Column)> {
    let idx = index_of(plan.schema(), key)?;
    match plan {
        LogicalPlan::Filter(_) => Some((plan.clone(), key.clone())),
        LogicalPlan::SubqueryAlias(alias) => {
            let (qualifier, field) = alias.input.schema().qualified_field(idx);
            descend_to_filter(&alias.input, &Column::new(qualifier.cloned(), field.name()))
        }
        LogicalPlan::Projection(proj) => match &proj.expr[idx] {
            Expr::Column(inner) => descend_to_filter(&proj.input, &inner.clone()),
            _ => None,
        },
        LogicalPlan::Join(join) => {
            // Follow whichever side actually carries the key. An outer join's
            // null-padded side cannot be used as a probe: it may manufacture
            // key values that the aggregate side should not be filtered by.
            if !matches!(join.join_type, JoinType::Inner) {
                return None;
            }
            for side in [&join.left, &join.right] {
                if let Some(found) =
                    index_of(side.schema(), key).and_then(|_| descend_to_filter(side, key))
                {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    /// `lineitem`-shaped: many rows per key.
    fn line_table() -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_quantity", DataType::Int64, false),
        ]));
        // keys 1..=5, four rows each with distinct quantities
        let mut keys = Vec::new();
        let mut qty = Vec::new();
        for k in 1..=5i64 {
            for q in 1..=4i64 {
                keys.push(k);
                qty.push(k * 10 + q);
            }
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(qty)),
            ],
        )
        .unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    /// `part`-shaped: one row per key, with a filterable attribute.
    fn part_table() -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_brand", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "keep", "skip", "keep", "skip", "skip",
                ])),
            ],
        )
        .unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    fn context(with_rule: bool) -> SessionContext {
        let mut builder = SessionStateBuilder::new().with_default_features();
        if with_rule {
            builder =
                builder.with_optimizer_rule(Arc::new(SemiJoinReductionThroughAggregate));
        }
        let ctx = SessionContext::new_with_state(builder.build());
        ctx.register_table("lineitem", line_table()).unwrap();
        ctx.register_table("part", part_table()).unwrap();
        ctx
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for b in &batches {
            for r in 0..b.num_rows() {
                let mut cells = Vec::new();
                for c in 0..b.num_columns() {
                    cells.push(
                        datafusion::common::cast::as_string_array(
                            &datafusion::arrow::compute::cast(b.column(c), &DataType::Utf8)
                                .unwrap(),
                        )
                        .unwrap()
                        .value(r)
                        .to_string(),
                    );
                }
                out.push(cells.join("|"));
            }
        }
        out.sort();
        out
    }

    async fn plan_of(ctx: &SessionContext, sql: &str) -> String {
        format!(
            "{}",
            ctx.sql(sql)
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap()
                .display_indent()
        )
    }

    /// The q17 shape: a grouped aggregate inner-joined on its grouping key,
    /// with a filtered relation on the other side.
    const Q17_SHAPE: &str = "SELECT p.p_partkey, s.avg_q FROM part p JOIN \
        (SELECT l_partkey, avg(l_quantity) AS avg_q FROM lineitem GROUP BY l_partkey) s \
        ON p.p_partkey = s.l_partkey WHERE p.p_brand = 'keep'";

    #[tokio::test]
    async fn the_rule_pushes_a_semi_join_into_the_aggregate_input() {
        let plan = plan_of(&context(true), Q17_SHAPE).await;
        assert!(
            plan.contains("LeftSemi"),
            "expected a LeftSemi reduction in:\n{plan}"
        );
        let baseline = plan_of(&context(false), Q17_SHAPE).await;
        assert!(
            !baseline.contains("LeftSemi"),
            "baseline should not already contain one:\n{baseline}"
        );
    }

    /// The property that actually matters. A faster wrong answer is worse than
    /// a slow right one, so the rule is only worth having if this holds.
    #[tokio::test]
    async fn results_are_identical_with_and_without_the_rule() {
        for sql in [
            Q17_SHAPE,
            // aggregate on the left of the join instead of the right
            "SELECT s.l_partkey, s.total FROM \
             (SELECT l_partkey, sum(l_quantity) AS total FROM lineitem GROUP BY l_partkey) s \
             JOIN part p ON s.l_partkey = p.p_partkey WHERE p.p_brand = 'keep'",
            // multiple aggregates, and a count that would inflate if the
            // semi-join ever duplicated a left row
            "SELECT p.p_partkey, s.n, s.total FROM part p JOIN \
             (SELECT l_partkey, count(*) AS n, sum(l_quantity) AS total \
              FROM lineitem GROUP BY l_partkey) s \
             ON p.p_partkey = s.l_partkey WHERE p.p_brand = 'keep'",
        ] {
            let with = rows(&context(true), sql).await;
            let without = rows(&context(false), sql).await;
            assert_eq!(with, without, "results diverged for:\n{sql}");
            assert!(!with.is_empty(), "test query returned nothing: {sql}");
        }
    }

    /// Under a LEFT join the unmatched rows are preserved, so dropping groups
    /// would change the answer. The rule must decline.
    #[tokio::test]
    async fn outer_joins_are_left_alone() {
        let sql = "SELECT p.p_partkey, s.avg_q FROM part p LEFT JOIN \
            (SELECT l_partkey, avg(l_quantity) AS avg_q FROM lineitem GROUP BY l_partkey) s \
            ON p.p_partkey = s.l_partkey WHERE p.p_brand = 'keep'";
        let plan = plan_of(&context(true), sql).await;
        assert!(
            !plan.contains("LeftSemi"),
            "must not reduce under an outer join:\n{plan}"
        );
        assert_eq!(
            rows(&context(true), sql).await,
            rows(&context(false), sql).await
        );
    }

    /// Joining on an *aggregate output* rather than a grouping key is not a
    /// key filter — restricting the input would change the aggregate values.
    #[tokio::test]
    async fn joining_on_an_aggregate_output_is_not_reduced() {
        let sql = "SELECT p.p_partkey FROM part p JOIN \
            (SELECT l_partkey, sum(l_quantity) AS total FROM lineitem GROUP BY l_partkey) s \
            ON p.p_partkey = s.total WHERE p.p_brand = 'keep'";
        let plan = plan_of(&context(true), sql).await;
        assert!(
            !plan.contains("LeftSemi"),
            "grouping keys only; an aggregate output is not one:\n{plan}"
        );
    }

    /// With no filter on the probe side the semi-join removes nothing and
    /// costs an extra pass, so the guard should decline.
    #[tokio::test]
    async fn an_unfiltered_probe_side_is_not_worth_reducing() {
        let sql = "SELECT p.p_partkey, s.avg_q FROM part p JOIN \
            (SELECT l_partkey, avg(l_quantity) AS avg_q FROM lineitem GROUP BY l_partkey) s \
            ON p.p_partkey = s.l_partkey";
        let plan = plan_of(&context(true), sql).await;
        assert!(
            !plan.contains("LeftSemi"),
            "no filter means no selectivity to exploit:\n{plan}"
        );
    }

    /// The optimizer runs rules to a fixed point. Without the `already_reduced`
    /// guard this stacks a new semi-join every iteration and never converges.
    #[tokio::test]
    async fn reduction_is_applied_at_most_once() {
        let plan = plan_of(&context(true), Q17_SHAPE).await;
        assert_eq!(
            plan.matches("LeftSemi").count(),
            1,
            "expected exactly one reduction:\n{plan}"
        );
    }

    /// The switch has to actually switch it off — a flag that is declared but
    /// never read is worse than no flag, because the registry gate makes it
    /// look supported.
    #[test]
    fn the_env_switch_is_honoured() {
        for off in ["off", "OFF", "0", "false", "no", " off "] {
            assert!(!enabled_from(off), "{off:?} should disable the rule");
        }
        for on in ["", "on", "1", "true", "anything-else"] {
            assert!(enabled_from(on), "{on:?} should leave the rule enabled");
        }
    }

    /// The reduction must keep exactly the groups the join would have kept —
    /// 'keep' selects partkeys 1 and 3 of 5.
    #[tokio::test]
    async fn the_reduction_keeps_exactly_the_surviving_groups() {
        let out = rows(&context(true), Q17_SHAPE).await;
        assert_eq!(out.len(), 2, "expected two surviving groups, got {out:?}");
    }
}
