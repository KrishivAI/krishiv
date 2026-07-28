//! Per-join selection of a spillable algorithm under a memory cap.
//!
//! # The failure this fixes
//!
//! TPC-H q18 on a 4500 MiB executor:
//!
//! ```text
//! Resources exhausted: Failed to allocate additional 1015.5 KB for
//! HashJoinInput[0] with 732.4 MB already allocated for this reservation -
//! 205.0 KB remain available
//! ```
//!
//! Not a leak, not a mis-sized budget: the pool refused correctly. DataFusion's
//! hash join holds its entire build side in memory with no spill path, so when
//! the pool is exhausted the operator has nowhere to put the overflow and the
//! query fails. Sort-merge join spills.
//!
//! # Why per-join, and why this exists as a rule instead of a config bit
//!
//! The first attempt (8f72a340, reverted) set
//! `datafusion.optimizer.prefer_hash_join = false` for the whole session
//! whenever a cgroup limit existed. Measured on the cluster, q2 — ten stages of
//! joins whose build sides all fit comfortably — went from 189 s to past a
//! 2400 s timeout. Sorting both sides of every join to rescue the one join
//! that overflows is a catastrophic trade.
//!
//! So the decision is made where the information is: at each hash join, from
//! that join's *estimated build size* against the *per-task share* of the
//! query pool. Three deliberately conservative gates, each a direct lesson
//! from the q2 regression:
//!
//! 1. **No cap, no change.** An embedded engine on 23 GB keeps hash joins.
//! 2. **Unknown statistics keep hash join.** A missing estimate is not
//!    evidence of a big build side, and guessing "big" re-creates the blanket
//!    regression. The cost of guessing "small" wrongly is the status quo —
//!    q18 fails as it does today — while the cost of guessing "big" wrongly
//!    is a q2-shaped timeout on healthy queries.
//! 3. **The join mode must be convertible.** `Partitioned` inputs are already
//!    hashed on the join keys — the distribution sort-merge needs — so the
//!    conversion adds per-partition sorts, not exchanges. `CollectLeft`
//!    converts too *when the plan has a single partition*, where sorting alone
//!    satisfies sort-merge. Anything else keeps hash join.
//!
//!    This bullet used to read "CollectLeft build sides are small by
//!    construction". They are not: `CollectLeft` is picked from an estimate
//!    and buffers the whole build side. Worse, a task engine plans with
//!    `target_partitions = cores / slots`, which is **1** on a 3-core, 3-slot
//!    executor — so *every* join was CollectLeft and the rule converted
//!    nothing at all while five SF100 queries died on it.
//!
//! The sorts are inserted explicitly (with partitioning preserved) rather than
//! left to `EnforceSorting`, because appended optimizer rules run *after* the
//! enforcement passes — a requirement declared here would never be satisfied.

use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

/// Environment override for the build-size threshold, in bytes.
pub const SPILL_JOIN_BUILD_BYTES_ENV: &str = "KRISHIV_SPILL_JOIN_BUILD_BYTES";

/// Share of the per-task memory allowance above which an estimated build side
/// is treated as "will not fit as a hash table".
///
/// A hash table costs more than the raw bytes it holds (buckets, hashes,
/// padding), and the build side is not the task's only consumer, so the
/// threshold sits well below 1.0. Below it, hash join stays — it is the right
/// algorithm when it fits.
const BUILD_FRACTION_OF_TASK_SHARE: f64 = 0.5;

/// Bytes assumed for a column whose type carries no fixed width.
///
/// Varlen columns (`Utf8`, `Binary`, and their `View`/`Large` forms) have no
/// width until the data arrives. 32 bytes is deliberately modest: this
/// estimate only ever *adds* conversions, so overestimating would convert
/// joins that would have fitted, and sort-merge is the slower plan when hash
/// join fits. Under-guessing costs nothing that is not already the status quo.
const ASSUMED_VARLEN_COLUMN_BYTES: usize = 32;

/// Build-side bytes derived from a row count when `total_byte_size` is absent.
///
/// Returns `None` when the row count is absent too — the one case where the
/// planner genuinely knows nothing and guessing would be a coin flip.
fn estimated_build_bytes_from_rows(
    stats: &datafusion::common::Statistics,
    build_schema: &arrow::datatypes::Schema,
) -> Option<u64> {
    let rows = match stats.num_rows {
        Precision::Exact(rows) | Precision::Inexact(rows) => rows,
        Precision::Absent => return None,
    };
    let row_width: usize = build_schema
        .fields()
        .iter()
        .map(|f| {
            f.data_type()
                .primitive_width()
                .unwrap_or(ASSUMED_VARLEN_COLUMN_BYTES)
        })
        .sum();
    u64::try_from(rows.saturating_mul(row_width.max(1))).ok()
}

/// Convert hash joins whose estimated build side cannot fit the per-task
/// memory share into sort-merge joins, which can spill.
#[derive(Debug)]
pub struct SpillableJoinSelection {
    /// Build-size threshold in bytes; `None` disables the rule entirely
    /// (no memory cap → nothing to protect against).
    threshold_bytes: Option<u64>,
}

impl SpillableJoinSelection {
    /// Derive the threshold from the process's capacity decision, honouring
    /// [`SPILL_JOIN_BUILD_BYTES_ENV`].
    pub fn from_capacity() -> Self {
        let threshold_bytes = std::env::var(SPILL_JOIN_BUILD_BYTES_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|n| *n > 0)
            .or_else(|| {
                let share = krishiv_common::executor_capacity::ExecutorCapacity::detect_cached()
                    .min_task_memory_share_bytes()?;
                #[expect(
                    clippy::cast_precision_loss,
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "byte counts are far below f64's exact-integer range"
                )]
                Some((share as f64 * BUILD_FRACTION_OF_TASK_SHARE) as u64)
            });
        Self { threshold_bytes }
    }

    /// Explicit threshold, for tests.
    #[must_use]
    pub fn with_threshold(threshold_bytes: Option<u64>) -> Self {
        Self { threshold_bytes }
    }


    /// Restore `hash_join`'s built-in projection on top of `converted`.
    ///
    /// A no-op when the join carried none, which is the common case.
    fn reapply_projection(
        converted: Arc<dyn ExecutionPlan>,
        hash_join: &HashJoinExec,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::projection::ProjectionExec;

        let Some(projection) = hash_join.projection.as_ref() else {
            return Ok(converted);
        };
        let schema = converted.schema();
        let mut exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
            Vec::with_capacity(projection.len());
        for &index in projection.iter() {
            let Some(field) = schema.fields().get(index) else {
                // The projection does not address this plan's schema after all.
                // Refusing here is safe: the caller keeps the hash join.
                return datafusion::error::Result::Err(
                    datafusion::error::DataFusionError::Plan(format!(
                        "spillable-join: projection index {index} is outside the \
                         converted join's {} columns",
                        schema.fields().len()
                    )),
                );
            };
            exprs.push((
                Arc::new(Column::new(field.name(), index)),
                field.name().clone(),
            ));
        }
        Ok(Arc::new(ProjectionExec::try_new(exprs, converted)?))
    }

    /// Whether this hash join should become a sort-merge join, and if so, the
    /// converted node.
    fn convert(
        &self,
        hash_join: &HashJoinExec,
        threshold: u64,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Gate 3: the join mode must be convertible to sort-merge.
        // `CollectLeft` is convertible exactly when the plan has one partition
        // — which, on this engine, is the *common* case rather than an edge
        // case. A task engine is built with
        // `target_partitions = cores / slots`, and a 3-core executor running 3
        // slots gets **1**. DataFusion never emits `Partitioned` at one
        // partition, so every join in every fragment was `CollectLeft`, and
        // this gate turned all of them away: the rule converted zero joins in
        // three hours across three executors while five SF100 queries died on
        // build sides it was written to rescue.
        //
        // The original premise — "CollectLeft build sides are small by
        // construction" — is false. `CollectLeft` is chosen from an *estimate*
        // and buffers the whole build side, so a wrong estimate makes it the
        // worst mode to be in, not the safest. q9 and q10 each took the entire
        // 797 MB pool this way.
        //
        // Sort-merge needs its inputs sorted on the join keys and co-located.
        // With one partition, "sorted" is the whole requirement — there is no
        // distribution to preserve — so the conversion is *simpler* here than
        // in the partitioned case, not riskier.
        let single_partition = hash_join.left().output_partitioning().partition_count() == 1
            && hash_join.right().output_partitioning().partition_count() == 1;
        let preserve_partitioning = match hash_join.partition_mode() {
            PartitionMode::Partitioned => true,
            PartitionMode::CollectLeft if single_partition => false,
            mode => {
                tracing::debug!(
                    ?mode,
                    single_partition,
                    threshold,
                    "spillable-join: join mode is not convertible"
                );
                return Ok(None);
            }
        };
        // Gate 2: the build side must be *known* to be large. Absent statistics
        // keep hash join — guessing "big" is how the reverted session-wide
        // switch timed out q2.
        //
        // Logged, because a rule that silently declines is indistinguishable
        // from a rule that was never installed. Three SF100 queries died on
        // un-spillable hash joins while this rule sat registered and converted
        // nothing, and the logs could not say which gate turned each one away.
        let stats = match hash_join.left().partition_statistics(None) {
            Ok(stats) => stats,
            // An error computing statistics is not evidence of a large build
            // side, and this rule is an optimisation: declining is always a
            // valid answer, failing the query never is.
            Err(error) => {
                tracing::debug!(%error, "spillable-join: statistics unavailable, keeping hash join");
                return Ok(None);
            }
        };
        let build_bytes = match stats.total_byte_size {
            Precision::Exact(bytes) | Precision::Inexact(bytes) => bytes as u64,
            // `total_byte_size` absent does not mean "size unknown" — DataFusion
            // often has a row count when it has no byte size (a shuffle read, a
            // filter over a scan with row stats). Deriving bytes from rows uses
            // information the planner already holds instead of surrendering at
            // the first absent field, which is how q9/SF100 kept a hash join
            // whose build side then took 797.5 MB of a 797.6 MB pool.
            //
            // Still conservative: if the row count is *also* absent we keep the
            // hash join rather than guess, because guessing "big" for every
            // join is the session-wide switch that timed q2 out.
            Precision::Absent => match estimated_build_bytes_from_rows(
                &stats,
                &hash_join.left().schema(),
            ) {
                Some(bytes) => bytes,
                None => {
                    tracing::debug!(
                        threshold,
                        "spillable-join: build-side size and row count both unknown, \
                         keeping hash join"
                    );
                    return Ok(None);
                }
            },
        };
        if build_bytes <= threshold {
            tracing::debug!(
                build_bytes,
                threshold,
                "spillable-join: build side fits, keeping hash join"
            );
            return Ok(None);
        }

        // Sort both sides on the join keys. Partition preservation follows the
        // mode decided above: keep it for `Partitioned` (so no exchange is
        // re-planned), drop it for single-partition `CollectLeft` (where there
        // is nothing to preserve).
        let on = hash_join.on();
        let left_keys: Vec<PhysicalSortExpr> = on
            .iter()
            .map(|(l, _)| PhysicalSortExpr::new_default(Arc::clone(l)))
            .collect();
        let right_keys: Vec<PhysicalSortExpr> = on
            .iter()
            .map(|(_, r)| PhysicalSortExpr::new_default(Arc::clone(r)))
            .collect();
        let (Some(left_ordering), Some(right_ordering)) = (
            LexOrdering::new(left_keys),
            LexOrdering::new(right_keys),
        ) else {
            return Ok(None);
        };
        let sort_options = left_ordering
            .iter()
            .map(|sort_expr| sort_expr.options)
            .collect();

        let sorted_left = Arc::new(
            SortExec::new(left_ordering, Arc::clone(hash_join.left()))
                .with_preserve_partitioning(preserve_partitioning),
        );
        let sorted_right = Arc::new(
            SortExec::new(right_ordering, Arc::clone(hash_join.right()))
                .with_preserve_partitioning(preserve_partitioning),
        );

        // Let SortMergeJoinExec's own validation decide whether this join
        // shape (type, filter) is supported; on refusal, keep the hash join
        // rather than fail the query.
        match SortMergeJoinExec::try_new(
            sorted_left,
            sorted_right,
            on.to_vec(),
            hash_join.filter().cloned(),
            *hash_join.join_type(),
            sort_options,
            hash_join.null_equality(),
        ) {
            Ok(smj) => {
                // `HashJoinExec` has a built-in projection; `SortMergeJoinExec`
                // does not (there is a TODO to that effect in DataFusion's
                // source). Converting a projected join therefore silently
                // widens the output back to the full left++right schema, and
                // the *parent* join's positional `on` columns then point at the
                // wrong fields — live q7/q8/q9 failed with
                // `Missing on the right: Column { name: "o_custkey", index: 3 }`.
                //
                // Reproduce the projection explicitly. The join's projection
                // indices address the same full join schema `SortMergeJoinExec`
                // produces (DataFusion validates them against it with
                // `can_project(&join_schema, ..)`), so selecting those indices
                // off the converted join yields the identical output columns,
                // order and names.
                let converted = Self::reapply_projection(Arc::new(smj), hash_join)?;
                tracing::info!(
                    build_bytes,
                    threshold,
                    mode = ?hash_join.partition_mode(),
                    join_type = ?hash_join.join_type(),
                    projected = hash_join.contains_projection(),
                    "hash join build side exceeds per-task memory share; using sort-merge join"
                );
                Ok(Some(converted))
            }
            Err(error) => {
                tracing::debug!(%error, "sort-merge conversion declined; keeping hash join");
                Ok(None)
            }
        }
    }
}

impl PhysicalOptimizerRule for SpillableJoinSelection {
    fn name(&self) -> &str {
        "spillable_join_selection"
    }

    fn schema_check(&self) -> bool {
        // The conversion preserves the join's output schema exactly; sorts add
        // no columns.
        true
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Gate 1: no cap, no change.
        let Some(threshold) = self.threshold_bytes else {
            tracing::debug!("spillable-join: no memory cap configured, rule inactive");
            return Ok(plan);
        };
        let mut seen = 0usize;
        let mut converted = 0usize;
        let mut declined_on_error = 0usize;
        let out = plan
            .transform_up(|node| {
                // `ExecutionPlan: Any` — upcast to downcast (DF 54 has no `as_any`).
                let any = node.as_ref() as &dyn std::any::Any;
                let Some(hash_join) = any.downcast_ref::<HashJoinExec>() else {
                    return Ok(Transformed::no(node));
                };
                seen += 1;
                // A rule that rewrites plans for *memory* reasons must never be
                // the reason a query fails. Live q7/q8/q9 turned an internal
                // refusal ("the left or right side of the join does not have
                // all columns on `on`") into a failed fragment, trading an
                // out-of-memory error for a planning error — strictly worse,
                // because the un-converted plan at least had a chance of
                // fitting. Declining is always available; erroring is not.
                match self.convert(hash_join, threshold) {
                    Ok(Some(plan)) => {
                        converted += 1;
                        Ok(Transformed::yes(plan))
                    }
                    Ok(None) => Ok(Transformed::no(node)),
                    Err(error) => {
                        declined_on_error += 1;
                        tracing::warn!(
                            %error,
                            mode = ?hash_join.partition_mode(),
                            join_type = ?hash_join.join_type(),
                            "spillable-join: conversion errored; keeping hash join"
                        );
                        Ok(Transformed::no(node))
                    }
                }
            })
            .map(|t| t.data)?;
        // One line that distinguishes "no hash joins in this plan", "joins seen
        // and left alone", and "rule not installed" — three states that were
        // previously identical from outside, which is what made three SF100
        // failures take a live investigation to attribute.
        if seen > 0 {
            // At info, not debug: executors run RUST_LOG=info, and the
            // debug-level version of this line was invisible in the only
            // environment that had the bug it was added to diagnose.
            tracing::info!(
                hash_joins = seen,
                converted,
                declined_on_error,
                threshold,
                "spillable-join: pass complete"
            );
        }
        Ok(out)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use datafusion::prelude::{SessionConfig, SessionContext};

    /// A session whose joins plan as `PartitionMode::Partitioned` even at test
    /// sizes — the mode the rule targets. Without forcing the thresholds down,
    /// DataFusion plans tiny joins as CollectLeft and the rule (correctly)
    /// declines, which makes the conversion test pass vacuously.
    fn partitioned_join_ctx() -> SessionContext {
        let mut config = SessionConfig::new().with_target_partitions(4);
        config.options_mut().optimizer.hash_join_single_partition_threshold = 0;
        config.options_mut().optimizer.hash_join_single_partition_threshold_rows = 0;
        SessionContext::new_with_config(config)
    }

    async fn joined_plan(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
        ctx.sql("CREATE TABLE big AS SELECT v % 1000 AS k, v AS payload FROM (VALUES (1)) t(x), UNNEST(range(0, 20000)) AS u(v)")
            .await.unwrap().collect().await.unwrap();
        ctx.sql("CREATE TABLE small AS SELECT v AS k FROM (VALUES (1)) t(x), UNNEST(range(0, 100)) AS u(v)")
            .await.unwrap().collect().await.unwrap();
        ctx.sql("SELECT b.k, count(*) FROM big b JOIN small s ON b.k = s.k GROUP BY b.k")
            .await.unwrap().create_physical_plan().await.unwrap()
    }

    fn contains(plan: &Arc<dyn ExecutionPlan>, name: &str) -> bool {
        datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string()
            .contains(name)
    }

    /// With a threshold below every build side, partitioned hash joins become
    /// sort-merge joins — the conversion mechanics work end to end, and the
    /// converted plan still executes to the same answer.
    #[tokio::test]
    async fn an_oversized_build_side_converts_and_still_answers_correctly() {
        let ctx = partitioned_join_ctx();
        let plan = joined_plan(&ctx).await;
        assert!(contains(&plan, "HashJoinExec"), "precondition: hash join planned");
        assert!(
            contains(&plan, "mode=Partitioned"),
            "precondition: the join must be Partitioned or the rule correctly declines:\n{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );

        let rule = SpillableJoinSelection::with_threshold(Some(1));
        let optimized = rule.optimize(Arc::clone(&plan), &ConfigOptions::default()).unwrap();
        assert!(
            contains(&optimized, "SortMergeJoin"),
            "an over-threshold build side must convert:\n{}",
            datafusion::physical_plan::displayable(optimized.as_ref()).indent(true)
        );

        // A converted plan that returns different rows would be worse than the
        // failure it prevents. Baseline is planned afresh: the optimized tree
        // shares untransformed Arc subtrees with `plan`, and RepartitionExec
        // panics ("partition not used yet") if one instance is executed twice.
        let baseline_plan = ctx
            .sql("SELECT b.k, count(*) FROM big b JOIN small s ON b.k = s.k GROUP BY b.k")
            .await.unwrap().create_physical_plan().await.unwrap();
        let baseline =
            datafusion::physical_plan::collect(baseline_plan, ctx.task_ctx()).await.unwrap();
        let converted =
            datafusion::physical_plan::collect(optimized, ctx.task_ctx()).await.unwrap();
        let count = |bs: &[arrow::record_batch::RecordBatch]| -> usize {
            bs.iter().map(|b| b.num_rows()).sum()
        };
        assert_eq!(count(&baseline), count(&converted));
    }

    /// A build side comfortably under the threshold keeps its hash join. This
    /// is the q2 protection — the reverted session-wide switch failed exactly
    /// this property.
    #[tokio::test]
    async fn a_small_build_side_keeps_its_hash_join() {
        let ctx = partitioned_join_ctx();
        let plan = joined_plan(&ctx).await;
        let rule = SpillableJoinSelection::with_threshold(Some(u64::MAX));
        let optimized = rule.optimize(Arc::clone(&plan), &ConfigOptions::default()).unwrap();
        assert!(contains(&optimized, "HashJoinExec"), "under-threshold joins stay hash");
        assert!(!contains(&optimized, "SortMergeJoin"));
    }

    /// No memory cap means no threshold means no change — the embedded engine
    /// on a big machine must keep the fast path untouched.
    #[tokio::test]
    async fn no_cap_leaves_the_plan_alone() {
        let ctx = partitioned_join_ctx();
        let plan = joined_plan(&ctx).await;
        let rule = SpillableJoinSelection::with_threshold(None);
        let optimized = rule.optimize(Arc::clone(&plan), &ConfigOptions::default()).unwrap();
        assert!(contains(&optimized, "HashJoinExec"));
        assert!(!contains(&optimized, "SortMergeJoin"));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod row_count_fallback_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{ColumnStatistics, Statistics};

    fn schema(fields: Vec<Field>) -> Schema {
        Schema::new(fields)
    }

    fn stats_with(num_rows: Precision<usize>, columns: usize) -> Statistics {
        Statistics {
            num_rows,
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics::new_unknown(); columns],
        }
    }

    #[test]
    fn absent_rows_and_bytes_yields_no_estimate() {
        // The one case where the planner truly knows nothing: keep hash join
        // rather than guess. This is the guard against re-creating the
        // session-wide switch that timed q2 out.
        let s = schema(vec![Field::new("k", DataType::Int64, false)]);
        assert_eq!(
            estimated_build_bytes_from_rows(&stats_with(Precision::Absent, 1), &s),
            None
        );
    }

    #[test]
    fn a_row_count_gives_an_estimate_when_byte_size_is_absent() {
        // The q9 case: rows known, bytes not. 1M rows x one 8-byte column.
        let s = schema(vec![Field::new("k", DataType::Int64, false)]);
        assert_eq!(
            estimated_build_bytes_from_rows(&stats_with(Precision::Exact(1_000_000), 1), &s),
            Some(8_000_000)
        );
    }

    #[test]
    fn inexact_row_counts_count_too() {
        // Post-filter estimates are Inexact; refusing them would leave the
        // fallback inert on exactly the plans that need it.
        let s = schema(vec![Field::new("k", DataType::Int64, false)]);
        assert_eq!(
            estimated_build_bytes_from_rows(&stats_with(Precision::Inexact(1_000), 1), &s),
            Some(8_000)
        );
    }

    #[test]
    fn varlen_columns_get_a_modest_assumed_width() {
        // Utf8 has no fixed width. The estimate must still produce something,
        // and must not be wild: one Int64 + one Utf8 = 8 + 32 per row.
        let s = schema(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let want = 100 * (8 + ASSUMED_VARLEN_COLUMN_BYTES as u64);
        assert_eq!(
            estimated_build_bytes_from_rows(&stats_with(Precision::Exact(100), 2), &s),
            Some(want)
        );
    }

    #[test]
    fn a_zero_row_build_side_estimates_zero_not_unknown() {
        // Zero rows must not be conflated with "unknown": an empty build side
        // is the strongest possible reason to keep the hash join, and a `None`
        // here would read as "no information" instead.
        let s = schema(vec![Field::new("k", DataType::Int64, false)]);
        assert_eq!(
            estimated_build_bytes_from_rows(&stats_with(Precision::Exact(0), 1), &s),
            Some(0)
        );
    }

    #[test]
    fn a_huge_row_count_does_not_overflow_into_a_small_estimate() {
        // Saturating arithmetic: an absurd row count must stay absurd rather
        // than wrap around to something that looks like it fits.
        let s = schema(vec![Field::new("k", DataType::Int64, false)]);
        let est = estimated_build_bytes_from_rows(&stats_with(Precision::Exact(usize::MAX), 1), &s)
            .expect("a known row count always yields an estimate");
        assert!(est > u64::from(u32::MAX), "estimate collapsed to {est}");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod collect_left_tests {
    use super::*;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    /// A session that plans exactly the way a task engine does on a saturated
    /// executor: one target partition, because `cores / slots` is 1. This is
    /// the configuration in which every join is `CollectLeft` — the shape the
    /// rule used to skip entirely.
    fn single_partition_ctx() -> SessionContext {
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1))
    }

    fn shows(plan: &Arc<dyn ExecutionPlan>, name: &str) -> bool {
        displayable(plan.as_ref()).indent(true).to_string().contains(name)
    }

    async fn one_partition_join_plan(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
        ctx.sql("CREATE TABLE l(k INT, v INT) AS VALUES (1, 10), (2, 20), (3, 30)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("CREATE TABLE r(k INT, w INT) AS VALUES (1, 100), (2, 200)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("SELECT l.v, r.w FROM l JOIN r ON l.k = r.k")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_single_partition_plan_really_does_produce_collect_left() {
        // Pins the premise the fix rests on. If DataFusion ever stops choosing
        // CollectLeft at one partition, the tests below stop testing anything
        // and this one says so first.
        let ctx = single_partition_ctx();
        let plan = one_partition_join_plan(&ctx).await;
        assert!(
            shows(&plan, "CollectLeft"),
            "expected CollectLeft at target_partitions=1, got:\n{}",
            displayable(plan.as_ref()).indent(true)
        );
    }

    #[tokio::test]
    async fn a_large_collect_left_join_becomes_sort_merge() {
        // The regression that mattered: with a threshold below the build side,
        // the rule must now convert. Before this fix it returned the plan
        // untouched no matter how large the build side was.
        let ctx = single_partition_ctx();
        let plan = one_partition_join_plan(&ctx).await;
        let rule = SpillableJoinSelection::with_threshold(Some(1));
        let out = rule.optimize(plan, ctx.copied_config().options()).unwrap();
        assert!(
            shows(&out, "SortMergeJoin"),
            "CollectLeft join was not converted:\n{}",
            displayable(out.as_ref()).indent(true)
        );
    }

    #[tokio::test]
    async fn a_small_collect_left_join_is_left_alone() {
        // Hash join is the right algorithm when it fits; the fix must not
        // convert everything just because it now *can*.
        let ctx = single_partition_ctx();
        let plan = one_partition_join_plan(&ctx).await;
        let rule = SpillableJoinSelection::with_threshold(Some(64 * 1024 * 1024));
        let out = rule.optimize(plan, ctx.copied_config().options()).unwrap();
        assert!(shows(&out, "HashJoin"), "small join should stay a hash join");
        assert!(!shows(&out, "SortMergeJoin"));
    }

    #[tokio::test]
    async fn the_converted_plan_returns_the_same_rows() {
        // A spillable plan that answers differently is not a fix. Compare the
        // converted plan's output against the hash-join plan's.
        use datafusion::physical_plan::collect;
        let ctx = single_partition_ctx();
        let plan = one_partition_join_plan(&ctx).await;
        let task_ctx = ctx.task_ctx();

        let hash_rows = collect(Arc::clone(&plan), Arc::clone(&task_ctx)).await.unwrap();
        let converted = SpillableJoinSelection::with_threshold(Some(1))
            .optimize(plan, ctx.copied_config().options())
            .unwrap();
        assert!(shows(&converted, "SortMergeJoin"));
        let smj_rows = collect(converted, task_ctx).await.unwrap();

        let total = |b: &[arrow::array::RecordBatch]| -> usize {
            b.iter().map(arrow::array::RecordBatch::num_rows).sum()
        };
        assert_eq!(total(&hash_rows), total(&smj_rows), "row count changed");
        assert_eq!(total(&smj_rows), 2, "expected the two matching keys");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod never_fails_the_query_tests {
    use super::*;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    /// A plan whose joins the rule will want to convert.
    async fn joined_plan(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
        ctx.sql("CREATE TABLE a(k INT, v INT) AS VALUES (1, 1), (2, 2)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("CREATE TABLE b(k INT, w INT) AS VALUES (1, 9)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("CREATE TABLE c(k INT, z INT) AS VALUES (1, 5)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        // Two stacked joins: `transform_up` converts the inner one first, so
        // the outer one is asked about a child the rule already rewrote — the
        // shape that produced the live failure.
        ctx.sql("SELECT a.v, b.w, c.z FROM a JOIN b ON a.k = b.k JOIN c ON a.k = c.k")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn stacked_joins_never_make_the_rule_return_an_error() {
        // The live regression: q7/q8/q9 stopped failing with "Resources
        // exhausted" and started failing with
        // `spillable_join_selection / Error during planning: The left or right
        // side of the join does not have all columns on "on"`. Trading an
        // out-of-memory error for a planning error is strictly worse — the
        // un-converted plan at least had a chance of fitting. This rule is an
        // optimisation and must always be able to decline.
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let plan = joined_plan(&ctx).await;
        let out = SpillableJoinSelection::with_threshold(Some(1))
            .optimize(plan, ctx.copied_config().options());
        assert!(
            out.is_ok(),
            "the rule must never fail a plan; got {:?}",
            out.err()
        );
    }

    #[tokio::test]
    async fn a_plan_the_rule_declines_is_returned_unchanged_and_still_runs() {
        use datafusion::physical_plan::collect;
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let plan = joined_plan(&ctx).await;
        let before = displayable(plan.as_ref()).indent(true).to_string();
        let task_ctx = ctx.task_ctx();

        // Threshold far above anything here: every join declines.
        let out = SpillableJoinSelection::with_threshold(Some(1 << 40))
            .optimize(Arc::clone(&plan), ctx.copied_config().options())
            .unwrap();
        assert_eq!(
            before,
            displayable(out.as_ref()).indent(true).to_string(),
            "declining must leave the plan untouched"
        );
        let rows = collect(out, task_ctx).await.unwrap();
        let total: usize = rows.iter().map(arrow::array::RecordBatch::num_rows).sum();
        assert_eq!(total, 1, "the declined plan must still produce the join result");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod projection_tests {
    use super::*;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::{SessionConfig, SessionContext};

    /// Two stacked joins where the inner one projects a subset of its columns.
    /// This is q10's shape: `customer JOIN orders JOIN lineitem`, where the
    /// middle join carries a projection and the outer join's `on` addresses
    /// its output positionally.
    async fn stacked_projected_plan(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
        for ddl in [
            "CREATE TABLE c(c_custkey INT, c_name VARCHAR) AS VALUES (1, 'a'), (2, 'b')",
            "CREATE TABLE o(o_orderkey INT, o_custkey INT, o_total INT) AS VALUES (10, 1, 5)",
            "CREATE TABLE l(l_orderkey INT, l_qty INT) AS VALUES (10, 3)",
        ] {
            ctx.sql(ddl).await.unwrap().collect().await.unwrap();
        }
        ctx.sql(
            "SELECT c.c_name, l.l_qty \
             FROM c JOIN o ON c.c_custkey = o.o_custkey \
                    JOIN l ON o.o_orderkey = l.l_orderkey",
        )
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
    }

    /// Number of `SortMergeJoinExec` nodes anywhere in `plan`.
    ///
    /// The precondition every test below depends on. `convert` has six ways to
    /// decline (mode, absent statistics, build side under threshold, no sort
    /// keys, `SortMergeJoinExec::try_new` refusing, projection out of range),
    /// and every one of them returns the plan **unchanged** — which passes an
    /// assertion that the output still matches the input. Without this count, a
    /// rule that silently stopped converting would leave the whole module
    /// green.
    fn sort_merge_join_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
        // `ExecutionPlan: Any` — upcast to downcast (DF 54 has no `as_any`).
        let any = plan.as_ref() as &dyn std::any::Any;
        let here = usize::from(any.downcast_ref::<SortMergeJoinExec>().is_some());
        here + plan
            .children()
            .iter()
            .map(|c| sort_merge_join_count(c))
            .sum::<usize>()
    }

    /// Whether any `HashJoinExec` in `plan` carries a built-in projection —
    /// the condition `reapply_projection` exists for.
    fn has_projected_hash_join(plan: &Arc<dyn ExecutionPlan>) -> bool {
        let any = plan.as_ref() as &dyn std::any::Any;
        any.downcast_ref::<HashJoinExec>()
            .is_some_and(HashJoinExec::contains_projection)
            || plan.children().iter().any(|c| has_projected_hash_join(c))
    }

    /// Every cell, row-sorted — not a row count.
    fn cells(batches: &[arrow::array::RecordBatch]) -> Vec<String> {
        let mut rows: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(move |r| {
                    (0..b.num_columns())
                        .map(|c| {
                            arrow::util::display::array_value_to_string(b.column(c), r)
                                .expect("cell")
                        })
                        .collect::<Vec<_>>()
                        .join("|")
                })
            })
            .collect();
        rows.sort();
        rows
    }

    #[tokio::test]
    async fn converting_a_projected_join_keeps_the_output_columns() {
        // The live failure: converting a join that carries a projection widened
        // its output back to the full left++right schema, so the parent join's
        // positional `on` broke with
        // `Missing on the right: Column { name: "o_custkey", index: 3 }`.
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let plan = stacked_projected_plan(&ctx).await;
        let before_schema = plan.schema();
        assert!(
            has_projected_hash_join(&plan),
            "fixture must build a hash join carrying a projection, or this tests nothing:\n{}",
            displayable(plan.as_ref()).indent(true)
        );

        let out = SpillableJoinSelection::with_threshold(Some(1))
            .optimize(Arc::clone(&plan), ctx.copied_config().options())
            .expect("the rule must not fail the plan");

        assert!(
            sort_merge_join_count(&out) > 0,
            "the rule declined, so the conversion under test never ran:\n{}",
            displayable(out.as_ref()).indent(true)
        );
        assert_eq!(
            out.schema(),
            before_schema,
            "conversion changed the plan's output schema:\n{}",
            displayable(out.as_ref()).indent(true)
        );
    }

    /// The values, not the shape.
    ///
    /// `reapply_projection` re-indexes the join's projection against the
    /// *converted* join's schema and takes each output column's name from that
    /// schema too. If those indices ever addressed different columns, the names
    /// and types would still line up — they are read from the same place the
    /// data is — so a schema comparison, a row count and a column count would
    /// all agree while every value was wrong. Only comparing cells catches it.
    #[tokio::test]
    async fn the_converted_projected_plan_returns_the_same_values() {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let plan = stacked_projected_plan(&ctx).await;
        let task_ctx = ctx.task_ctx();
        assert!(has_projected_hash_join(&plan), "fixture must project");

        let before = collect(Arc::clone(&plan), Arc::clone(&task_ctx)).await.unwrap();
        let out = SpillableJoinSelection::with_threshold(Some(1))
            .optimize(plan, ctx.copied_config().options())
            .unwrap();
        assert!(
            sort_merge_join_count(&out) > 0,
            "the rule declined, so the conversion under test never ran"
        );
        let after = collect(out, task_ctx).await.unwrap();

        assert_eq!(cells(&before), cells(&after), "converted plan changed the data");
        assert_eq!(cells(&after), vec![String::from("a|3")], "expected the single matching row");
    }
}
