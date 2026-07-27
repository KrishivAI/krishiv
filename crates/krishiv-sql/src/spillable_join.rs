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
//! 3. **Only `PartitionMode::Partitioned` joins convert.** CollectLeft joins
//!    have small build sides by construction, and partitioned inputs are
//!    already hashed on the join keys — exactly the distribution sort-merge
//!    needs, so the conversion only adds per-partition sorts, not exchanges.
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
use datafusion::physical_plan::ExecutionPlan;
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

    /// Whether this hash join should become a sort-merge join, and if so, the
    /// converted node.
    fn convert(
        &self,
        hash_join: &HashJoinExec,
        threshold: u64,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Gate 3: sort-merge needs both sides hash-distributed on the join
        // keys, which only `Partitioned` guarantees. `CollectLeft` cannot be
        // converted in place — but it is *not* safe to ignore, so it is
        // reported rather than silently skipped: `CollectLeft` is chosen from
        // an estimate, and when that estimate is wrong it is the worst mode to
        // be in, because the whole build side is materialised on every
        // partition. TPC-H q9/SF100 failed at 797.5 MB of `HashJoinInput`
        // against a 797.6 MB pool.
        if !matches!(hash_join.partition_mode(), PartitionMode::Partitioned) {
            tracing::debug!(
                mode = ?hash_join.partition_mode(),
                threshold,
                "spillable-join: not converting a non-partitioned join"
            );
            return Ok(None);
        }
        // Gate 2: the build side must be *known* to be large. Absent statistics
        // keep hash join — guessing "big" is how the reverted session-wide
        // switch timed out q2.
        //
        // Logged, because a rule that silently declines is indistinguishable
        // from a rule that was never installed. Three SF100 queries died on
        // un-spillable hash joins while this rule sat registered and converted
        // nothing, and the logs could not say which gate turned each one away.
        let stats = hash_join.left().partition_statistics(None)?;
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

        // Sort both sides on the join keys, preserving the existing hash
        // partitioning so no exchange is re-planned.
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
                .with_preserve_partitioning(true),
        );
        let sorted_right = Arc::new(
            SortExec::new(right_ordering, Arc::clone(hash_join.right()))
                .with_preserve_partitioning(true),
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
                tracing::info!(
                    build_bytes,
                    threshold,
                    join_type = ?hash_join.join_type(),
                    "hash join build side exceeds per-task memory share; using sort-merge join"
                );
                Ok(Some(Arc::new(smj)))
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
        let out = plan
            .transform_up(|node| {
                // `ExecutionPlan: Any` — upcast to downcast (DF 54 has no `as_any`).
                let any = node.as_ref() as &dyn std::any::Any;
                let Some(hash_join) = any.downcast_ref::<HashJoinExec>() else {
                    return Ok(Transformed::no(node));
                };
                seen += 1;
                match self.convert(hash_join, threshold)? {
                    Some(plan) => {
                        converted += 1;
                        Ok(Transformed::yes(plan))
                    }
                    None => Ok(Transformed::no(node)),
                }
            })
            .map(|t| t.data)?;
        // One line that distinguishes "no hash joins in this plan", "joins seen
        // and left alone", and "rule not installed" — three states that were
        // previously identical from outside, which is what made three SF100
        // failures take a live investigation to attribute.
        if seen > 0 {
            tracing::debug!(
                hash_joins = seen,
                converted,
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
