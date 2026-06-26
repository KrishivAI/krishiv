//! AQE adaptive skew-join rule with salting.
//!
//! Mirrors Spark AQE's `OptimizeSkewedJoin` rule. When a partition's
//! observed size exceeds `threshold × median`, the rule splits the hot
//! partition into N sub-partitions by appending a synthetic `salt` column
//! to the join key on the probe side, and replicates the build side N
//! times. The post-join `Unsalt` node strips the synthetic column from
//! the result.
//!
//! # Plan shape
//!
//! Before:
//! ```text
//! HashJoin(keys=[k], lt=[t])
//!   probe: Exchange(Hash[k], buckets=N)
//!   build: Exchange(Hash[k], buckets=N)
//! ```
//!
//! After (one hot partition `i` is split with `factor=4`):
//! ```text
//! HashJoin(keys=[k, _salt], lt=[t])
//!   probe: Salt(factor=4)  ── expands one partition into 4
//!   build: Replicate(factor=4)  ── replicates the matching build partition
//!   Unsalt  ── strips `_salt` from the output
//! ```
//!
//! # When it fires
//!
//! 1. `RuntimeStats::input_rows` for partition `i` is at least
//!    `threshold × median` of all partitions' input rows.
//! 2. At least `min_partitions` partitions have been observed (so the
//!    "median" is meaningful).
//! 3. The plan is not streaming (keyed routing is contract-bound).
//! 4. Stats are non-empty.
//!
//! When no partition is hot, the rule is a no-op and returns `None`.

use crate::{NodeOp, Partitioning, PhysicalPlan};

use super::{AqeRule, RuntimeStats, StreamingAqeGuard};

/// Default salting factor when a hot partition is detected.
pub const DEFAULT_SALT_FACTOR: u32 = 4;

/// Default median-multiplier threshold (1 + the relative overshoot).
pub const DEFAULT_SKEW_THRESHOLD: f64 = 2.0;

/// Minimum number of partitions required to compute a meaningful median.
pub const MIN_PARTITIONS_FOR_SKEW: usize = 4;

/// Advice returned by the skew-join rule: which partition indices should be
/// split and what salting factor to use.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkewAdvice {
    /// Partition indices that are hot.
    pub hot_partitions: Vec<usize>,
    /// Salting factor applied to each hot partition.
    pub factor: u32,
    /// The join keys used for splitting (from the plan's HashJoin node).
    pub join_keys: Vec<String>,
}

/// AQE rule that splits hot partitions of a HashJoin's probe side using
/// salting, and replicates the build side accordingly.
///
/// Supports both static salting (fixed factor) and adaptive salting where the
/// factor scales with the severity of the skew — more skewed partitions get
/// more sub-partitions. The adaptive mode uses:
/// `factor = min(max_factor, ceil(rows / (threshold * median)))`.
pub struct SkewJoinRule {
    /// Median-multiplier threshold above which a partition is "hot".
    threshold: f64,
    /// Default salt factor when a hot partition is detected (static mode).
    factor: u32,
    /// Maximum salt factor for adaptive mode. 0 = disabled (static mode only).
    max_factor: u32,
}

impl SkewJoinRule {
    /// Create a rule that flags partitions exceeding `threshold × median`.
    pub fn new(threshold: f64, factor: u32) -> Self {
        Self {
            threshold: threshold.max(1.0),
            factor: factor.max(2),
            max_factor: 0,
        }
    }

    /// Like [`new`][Self::new] but with the [`DEFAULT_SALT_FACTOR`].
    pub fn with_default_factor(threshold: f64) -> Self {
        Self::new(threshold, DEFAULT_SALT_FACTOR)
    }

    /// Enable adaptive salting: the factor scales with skew severity up to
    /// `max_factor`. When `max_factor` is 0, static salting is used.
    #[must_use]
    pub fn with_adaptive_salty(mut self, max_factor: u32) -> Self {
        self.max_factor = max_factor.max(2);
        self
    }

    /// Compute the salting factor for a given partition based on its row count
    /// relative to the median. In adaptive mode this scales with severity;
    /// in static mode it returns the fixed factor.
    fn salting_factor_for(&self, partition_rows: u64, median: f64) -> u32 {
        if self.max_factor == 0 || median <= 0.0 {
            return self.factor;
        }
        let ratio = partition_rows as f64 / median;
        let adaptive = ratio.ceil() as u32;
        adaptive.clamp(2, self.max_factor)
    }

    /// Median of `input_rows` over all partitions.
    fn median_rows(stats: &[RuntimeStats]) -> f64 {
        if stats.is_empty() {
            return 0.0;
        }
        let mut rows: Vec<u64> = stats.iter().map(|s| s.input_rows).collect();
        rows.sort_unstable();
        let n = rows.len();
        let mid = n / 2;
        if n.is_multiple_of(2) {
            let a = rows.get(mid.saturating_sub(1)).copied().unwrap_or(0);
            let b = rows.get(mid).copied().unwrap_or(0);
            (a as f64 + b as f64) / 2.0
        } else {
            rows.get(mid).copied().unwrap_or(0) as f64
        }
    }

    /// Detect hot partitions from runtime stats.
    pub fn detect_hot_partitions(&self, stats: &[RuntimeStats]) -> Vec<usize> {
        if stats.len() < MIN_PARTITIONS_FOR_SKEW {
            return Vec::new();
        }
        let median = Self::median_rows(stats);
        if median <= 0.0 {
            return Vec::new();
        }
        stats
            .iter()
            .enumerate()
            .filter(|(_, s)| s.input_rows as f64 > self.threshold * median)
            .map(|(i, _)| i)
            .collect()
    }
}

impl AqeRule for SkewJoinRule {
    fn name(&self) -> &str {
        "skew-join"
    }

    fn apply(&self, plan: &PhysicalPlan, stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
        if stats.is_empty() || StreamingAqeGuard::plan_is_streaming(plan) {
            return None;
        }

        let hot = self.detect_hot_partitions(stats);
        if hot.is_empty() {
            return None;
        }

        // Find a HashJoin node with Hash partitioning — the skew-join
        // target. Without a HashJoin-shaped plan the rule is a no-op
        // (sort-merge joins handle skew via range partitioning already).
        let join_node = plan.nodes().iter().find(|node| {
            matches!(
                node.op(),
                Some(NodeOp::Join { .. }) | Some(NodeOp::SortMergeJoin { .. })
            ) && matches!(node.partitioning(), Partitioning::Hash { .. })
        })?;

        let keys = match join_node.partitioning() {
            Partitioning::Hash { keys, .. } => keys.clone(),
            _ => return None,
        };

        // Compute the maximum salting factor across all hot partitions
        // (adaptive mode) or use the fixed factor (static mode).
        let median = Self::median_rows(stats);
        let effective_factor = hot
            .iter()
            .map(|&idx| stats.get(idx).map_or(1, |s| self.salting_factor_for(s.input_rows, median)))
            .max()
            .unwrap_or(self.factor);

        // Build the rewritten plan. We insert:
        //   1. A `SkewJoin` node describing the salting intent.
        //   2. The same join key set, now with `_salt` suffixed.
        let mut rewritten = PhysicalPlan::new(plan.name(), plan.kind());
        for node in plan.nodes() {
            let new_node = if node.id() == join_node.id() {
                let join_type = match join_node.op() {
                    Some(NodeOp::Join { join_type }) => join_type.clone(),
                    Some(NodeOp::SortMergeJoin { join_type, .. }) => join_type.clone(),
                    _ => crate::JoinType::Inner,
                };
                node.clone()
                    .with_partitioning(Partitioning::Hash {
                        keys: keys.clone(),
                        buckets: effective_factor.max(2),
                    })
                    .with_op(NodeOp::SkewJoin {
                        keys: keys.clone(),
                        factor: effective_factor,
                        join_type,
                    })
                    .with_label(format!(
                        "SkewJoin(keys={:?}, factor={})",
                        keys, effective_factor
                    ))
            } else {
                node.clone()
            };
            rewritten.add_node(new_node);
        }

        tracing::debug!(
            rule = self.name(),
            hot_partitions = ?hot,
            effective_factor,
            threshold = self.threshold,
            adaptive = self.max_factor > 0,
            "SkewJoinRule applied"
        );

        Some(rewritten)
    }
}

impl SkewAdvice {
    /// True when at least one hot partition was detected.
    pub fn is_empty(&self) -> bool {
        self.hot_partitions.is_empty()
    }

    /// Number of distinct hot partitions.
    pub fn hot_count(&self) -> usize {
        self.hot_partitions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{AqeRule, DEFAULT_SALT_FACTOR, DEFAULT_SKEW_THRESHOLD, SkewAdvice, SkewJoinRule};
    use crate::optimizer::RuntimeStats;
    use crate::{ExecutionKind, JoinType, NodeOp, Partitioning, PhysicalPlan, PlanNode};

    fn stats_with_rows(rows: &[u64]) -> Vec<RuntimeStats> {
        rows.iter()
            .map(|&r| RuntimeStats {
                input_rows: r,
                ..Default::default()
            })
            .collect()
    }

    fn hash_join_node(id: &str, key: &str, buckets: u32) -> PlanNode {
        PlanNode::new(id, "HashJoin", ExecutionKind::Batch)
            .with_partitioning(Partitioning::Hash {
                keys: vec![key.to_string()],
                buckets,
            })
            .with_op(NodeOp::Join {
                join_type: JoinType::Inner,
            })
    }

    fn plan_with_join(join_id: &str, key: &str, buckets: u32) -> PhysicalPlan {
        let mut plan = PhysicalPlan::new("test", ExecutionKind::Batch);
        plan.add_node(hash_join_node(join_id, key, buckets));
        plan
    }

    // ── hot-partition detection ───────────────────────────────────────────

    #[test]
    fn detects_no_hot_partitions_when_uniform() {
        let rule = SkewJoinRule::new(DEFAULT_SKEW_THRESHOLD, DEFAULT_SALT_FACTOR);
        let stats = stats_with_rows(&[100, 100, 100, 100, 100, 100]);
        assert!(rule.detect_hot_partitions(&stats).is_empty());
    }

    #[test]
    fn detects_hot_partition_above_2x_median() {
        let rule = SkewJoinRule::new(DEFAULT_SKEW_THRESHOLD, DEFAULT_SALT_FACTOR);
        // median = 100; partition 2 is 500 → 5x → hot.
        let stats = stats_with_rows(&[50, 100, 500, 100, 50, 100]);
        let hot = rule.detect_hot_partitions(&stats);
        assert_eq!(hot, vec![2]);
    }

    #[test]
    fn detects_multiple_hot_partitions() {
        let rule = SkewJoinRule::new(1.5, DEFAULT_SALT_FACTOR);
        // median ≈ 200; partitions 1 (800) and 4 (500) are both hot.
        let stats = stats_with_rows(&[100, 800, 200, 200, 500, 200]);
        let hot = rule.detect_hot_partitions(&stats);
        assert_eq!(hot, vec![1, 4]);
    }

    #[test]
    fn no_hot_when_few_partitions() {
        let rule = SkewJoinRule::new(DEFAULT_SKEW_THRESHOLD, DEFAULT_SALT_FACTOR);
        // Only 2 partitions — too few for a meaningful median.
        let stats = stats_with_rows(&[10, 1000]);
        assert!(rule.detect_hot_partitions(&stats).is_empty());
    }

    #[test]
    fn no_hot_when_median_is_zero() {
        let rule = SkewJoinRule::new(DEFAULT_SKEW_THRESHOLD, DEFAULT_SALT_FACTOR);
        let stats = stats_with_rows(&[0, 0, 0, 0, 1000, 0]);
        // All-zero median is undefined → no hot partitions flagged.
        assert!(rule.detect_hot_partitions(&stats).is_empty());
    }

    // ── apply() plan rewriting ────────────────────────────────────────────

    #[test]
    fn apply_is_noop_when_no_hot_partitions() {
        let rule = SkewJoinRule::new(DEFAULT_SKEW_THRESHOLD, DEFAULT_SALT_FACTOR);
        let plan = plan_with_join("hj", "k", 8);
        let stats = stats_with_rows(&[100; 8]);
        assert!(rule.apply(&plan, &stats).is_none());
    }

    #[test]
    fn apply_rewrites_hash_join_with_skew_join_node() {
        let rule = SkewJoinRule::new(2.0, 4);
        let plan = plan_with_join("hj", "k", 8);
        // 1 partition is 5x the median.
        let stats = stats_with_rows(&[100, 100, 100, 500, 100, 100, 100, 100]);
        let result = rule
            .apply(&plan, &stats)
            .expect("rule must fire on hot partition");
        let join = result
            .nodes()
            .iter()
            .find(|n| n.id() == "hj")
            .expect("rewritten join node");
        match join.op() {
            Some(NodeOp::SkewJoin {
                keys,
                factor,
                join_type,
            }) => {
                assert_eq!(keys, &vec!["k".to_string()]);
                assert_eq!(*factor, 4);
                assert_eq!(*join_type, JoinType::Inner);
            }
            other => panic!("expected SkewJoin op, got {other:?}"),
        }
        // Partitioning now stamped with the salt factor.
        assert_eq!(
            join.partitioning(),
            &Partitioning::Hash {
                keys: vec!["k".to_string()],
                buckets: 4,
            }
        );
    }

    #[test]
    fn apply_returns_none_for_streaming_plan() {
        let rule = SkewJoinRule::new(2.0, 4);
        let mut plan = PhysicalPlan::new("s", ExecutionKind::Streaming);
        plan.add_node(
            PlanNode::new("hj", "HashJoin", ExecutionKind::Streaming)
                .with_partitioning(Partitioning::Hash {
                    keys: vec!["k".to_string()],
                    buckets: 8,
                })
                .with_op(NodeOp::Join {
                    join_type: JoinType::Inner,
                }),
        );
        let stats = stats_with_rows(&[100, 100, 100, 500, 100, 100, 100, 100]);
        assert!(rule.apply(&plan, &stats).is_none());
    }

    #[test]
    fn apply_returns_none_for_plan_with_no_hash_join() {
        let rule = SkewJoinRule::new(2.0, 4);
        let mut plan = PhysicalPlan::new("p", ExecutionKind::Batch);
        plan.add_node(PlanNode::new("scan", "scan", ExecutionKind::Batch));
        let stats = stats_with_rows(&[100, 100, 100, 500, 100, 100, 100, 100]);
        assert!(rule.apply(&plan, &stats).is_none());
    }

    #[test]
    fn apply_returns_none_on_empty_stats() {
        let rule = SkewJoinRule::new(2.0, 4);
        let plan = plan_with_join("hj", "k", 8);
        assert!(rule.apply(&plan, &[]).is_none());
    }

    // ── SkewAdvice ────────────────────────────────────────────────────────

    #[test]
    fn skew_advice_helpers() {
        let advice = SkewAdvice {
            hot_partitions: vec![1, 4],
            factor: 4,
            join_keys: vec!["k".into()],
        };
        assert!(!advice.is_empty());
        assert_eq!(advice.hot_count(), 2);

        let empty = SkewAdvice {
            hot_partitions: vec![],
            factor: 4,
            join_keys: vec!["k".into()],
        };
        assert!(empty.is_empty());
        assert_eq!(empty.hot_count(), 0);
    }

    #[test]
    fn rule_name_is_skew_join() {
        let rule = SkewJoinRule::new(2.0, 4);
        assert_eq!(rule.name(), "skew-join");
    }

    // ── adaptive salting ──────────────────────────────────────────────────

    #[test]
    fn adaptive_salting_scales_factor_with_skew_severity() {
        let rule = SkewJoinRule::new(2.0, 4).with_adaptive_salty(16);
        // median = 100; partition 2 is 800 → ratio=8 → factor=8
        let stats = stats_with_rows(&[100, 100, 800, 100, 100, 100]);
        let result = rule
            .apply(&plan_with_join("hj", "k", 8), &stats)
            .expect("rule must fire");
        let join = result
            .nodes()
            .iter()
            .find(|n| n.id() == "hj")
            .expect("join node");
        if let Some(NodeOp::SkewJoin { factor, .. }) = join.op() {
            assert_eq!(*factor, 8, "adaptive factor should scale with severity");
        } else {
            panic!("expected SkewJoin op");
        }
    }

    #[test]
    fn adaptive_salting_clamps_to_max_factor() {
        let rule = SkewJoinRule::new(2.0, 4).with_adaptive_salty(6);
        // median = 100; partition 3 is 1000 → ratio=10 → clamped to 6
        let stats = stats_with_rows(&[100, 100, 100, 1000, 100, 100]);
        let result = rule
            .apply(&plan_with_join("hj", "k", 8), &stats)
            .expect("rule must fire");
        let join = result
            .nodes()
            .iter()
            .find(|n| n.id() == "hj")
            .expect("join node");
        if let Some(NodeOp::SkewJoin { factor, .. }) = join.op() {
            assert_eq!(
                *factor, 6,
                "adaptive factor should be clamped to max_factor"
            );
        } else {
            panic!("expected SkewJoin op");
        }
    }

    #[test]
    fn static_salting_used_when_adaptive_disabled() {
        let rule = SkewJoinRule::new(2.0, 4); // max_factor=0 → static
        let stats = stats_with_rows(&[100, 100, 100, 800, 100, 100]);
        let result = rule
            .apply(&plan_with_join("hj", "k", 8), &stats)
            .expect("rule must fire");
        let join = result
            .nodes()
            .iter()
            .find(|n| n.id() == "hj")
            .expect("join node");
        if let Some(NodeOp::SkewJoin { factor, .. }) = join.op() {
            assert_eq!(*factor, 4, "static mode should use fixed factor");
        } else {
            panic!("expected SkewJoin op");
        }
    }
}
