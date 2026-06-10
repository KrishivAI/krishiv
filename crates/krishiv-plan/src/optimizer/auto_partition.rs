//! AQE auto-partition rule.

use crate::{Partitioning, PhysicalPlan};

use super::{AqeRule, RuntimeStats, StreamingAqeGuard};

const DEFAULT_TARGET_PARTITION_BYTES: u64 = krishiv_common::partition::TARGET_BYTES_PER_PARTITION;

/// AQE rule that adjusts the bucket count of `Hash` and `RoundRobin` exchange
/// nodes based on the observed data volume from the previous execution.
///
/// The rule reads `RuntimeStats` (one per DataFusion partition), sums
/// `memory_bytes` to obtain the total stage output size, and computes a target
/// partition count:
///
/// `target = clamp(1, max_buckets, ceil(total_bytes / target_partition_bytes))`
///
/// The target is applied unconditionally: the rule can both increase and
/// decrease bucket counts. This matches Spark AQE's behavior — if early
/// execution stages produced far less data than expected, the rule shrinks
/// the downstream partition count to avoid over-parallelism (task scheduling
/// overhead dominating actual work). The minimum floor is always 1.
///
/// When stats are empty (first execution) or contain no measurable memory, the
/// rule is a no-op and returns `None`.
pub struct AutoPartitionRule {
    /// Desired bytes per partition.  Default: 128 MiB.
    target_partition_bytes: u64,
    /// Upper bound on the number of partitions.  Derived from
    /// `target_partitions` in the session config so we never ask for more
    /// parallelism than the runtime can supply.
    max_buckets: u32,
}

impl AutoPartitionRule {
    /// Create a new rule with the given max bucket count.
    ///
    /// Uses the default `target_partition_bytes` of 128 MiB.
    pub fn new(max_buckets: u32) -> Self {
        Self {
            target_partition_bytes: DEFAULT_TARGET_PARTITION_BYTES,
            max_buckets,
        }
    }

    /// Set a custom `target_partition_bytes`.
    #[must_use]
    pub fn with_target_partition_bytes(mut self, bytes: u64) -> Self {
        self.target_partition_bytes = bytes;
        self
    }
}

impl AqeRule for AutoPartitionRule {
    fn name(&self) -> &str {
        "auto-partition"
    }

    fn apply(&self, plan: &PhysicalPlan, stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
        // When an explicit shuffle_partitions override is set on the plan
        // (via SET shuffle.partitions = N or SessionBuilder), use it as the
        // target bucket count regardless of stats.  Stats may be empty on the
        // first execution and that's fine — the override is a user intent.
        if let Some(override_buckets) = plan.shuffle_partitions() {
            return self.apply_override(plan, override_buckets);
        }

        if stats.is_empty() || StreamingAqeGuard::plan_is_streaming(plan) {
            return None;
        }

        // Sum the best available size metric across all partitions.
        // Prefer serialized_bytes (shuffle wire size) over memory_bytes (peak
        // in-memory) because shuffle output is compressed/serialized and thus
        // a more accurate proxy for partition cost. Fall back to memory_bytes
        // when serialized_bytes is zero (non-shuffle tasks or older executors).
        let total_bytes: u64 = stats
            .iter()
            .map(|s| {
                if s.serialized_bytes > 0 {
                    s.serialized_bytes
                } else {
                    s.memory_bytes
                }
            })
            .sum();
        if total_bytes == 0 {
            return None;
        }

        // Compute target partition count.
        let target = u64::from(self.max_buckets)
            .min(total_bytes.div_ceil(self.target_partition_bytes));
        let target = target.max(1) as u32;

        self.stamp_target(plan, target)
    }
}

impl AutoPartitionRule {
    /// Apply the rule with an explicit override bucket count.
    /// Skips streaming plans, but does not require runtime stats.
    fn apply_override(&self, plan: &PhysicalPlan, target: u32) -> Option<PhysicalPlan> {
        if StreamingAqeGuard::plan_is_streaming(plan) {
            return None;
        }
        let target = target.max(1);
        self.stamp_target(plan, target)
    }

    /// Stamp `target` bucket count onto all Hash/RoundRobin exchange nodes
    /// whose current count differs from `target`. Returns `None` if no node
    /// needed adjustment.
    ///
    /// Both increases and decreases are applied — if the observed data volume
    /// implies fewer partitions than currently planned, the bucket count is
    /// lowered to avoid over-parallelism (task scheduling overhead > useful
    /// work). The caller guarantees `target >= 1`.
    fn stamp_target(&self, plan: &PhysicalPlan, target: u32) -> Option<PhysicalPlan> {
        let mut changed = false;
        for node in plan.nodes() {
            match node.partitioning() {
                Partitioning::Hash { buckets, .. } | Partitioning::RoundRobin { buckets, .. }
                    if *buckets != target =>
                {
                    changed = true;
                }
                _ => {}
            }
        }

        if !changed {
            return None;
        }

        // Only clone when we know a rewrite is needed.
        let mut plan = plan.clone();
        for node in plan.nodes_mut() {
            let old = node.partitioning().clone();
            match old {
                Partitioning::Hash { ref keys, buckets } if buckets != target => {
                    node.set_partitioning(Partitioning::Hash {
                        keys: keys.clone(),
                        buckets: target,
                    });
                }
                Partitioning::RoundRobin { buckets } if buckets != target => {
                    node.set_partitioning(Partitioning::RoundRobin { buckets: target });
                }
                _ => {}
            }
        }

        tracing::debug!(rule = "auto-partition", target, "AutoPartitionRule applied");

        Some(plan)
    }
}
