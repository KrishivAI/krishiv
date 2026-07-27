//! AQE coalesce-small-partitions rule.

use std::collections::HashSet;

use crate::{NodeOp, PhysicalPlan, PlanNode};

use super::{AqeRule, RuntimeStats, StreamingAqeGuard};

const DEFAULT_TARGET_PARTITION_BYTES: u64 = krishiv_common::partition::TARGET_BYTES_PER_PARTITION;

/// Advice returned by the coalesce rule: which partition indices should be merged.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoalesceAdvice {
    /// Groups of partition indices to merge. Each inner `Vec` is one merged partition.
    pub groups: Vec<Vec<usize>>,
}

/// Merges partitions whose `memory_bytes` falls below `min_partition_bytes`.
///
/// When coalescing is beneficial (i.e. the advised group count is smaller than
/// the current partition count), `apply` rewrites the physical plan by appending
/// a [`NodeOp::CoalescePartitions`] node that signals downstream operators to
/// merge the output into `target_partitions` partitions.
pub struct CoalesceRule {
    /// Partitions smaller than this threshold (bytes) are candidates for merging.
    min_partition_bytes: u64,
    /// Target size for each merged partition (bytes).
    ///
    /// Used to determine `target_partitions = ceil(total_bytes / target_partition_bytes)`
    /// when inserting a `CoalescePartitions` node.  Default: 128 MiB.
    target_partition_bytes: u64,
    /// Floor on the coalesced partition count — see [`Self::with_min_partitions`].
    min_partitions: usize,
}

impl CoalesceRule {
    /// Create a new `CoalesceRule` with the given minimum partition byte threshold.
    ///
    /// Uses the default `target_partition_bytes` of 128 MiB and no parallelism
    /// floor; see [`Self::with_min_partitions`].
    pub fn new(min_partition_bytes: u64) -> Self {
        Self {
            min_partition_bytes,
            target_partition_bytes: DEFAULT_TARGET_PARTITION_BYTES,
            min_partitions: 1,
        }
    }

    /// Set a custom `target_partition_bytes` (bytes per merged output partition).
    #[must_use]
    pub fn with_target_partition_bytes(mut self, target_partition_bytes: u64) -> Self {
        self.target_partition_bytes = target_partition_bytes;
        self
    }

    /// Return the configured `target_partition_bytes`.
    pub fn target_partition_bytes(&self) -> u64 {
        self.target_partition_bytes
    }

    /// Never coalesce below `min_partitions` partitions.
    ///
    /// Sizing partitions purely by bytes answers "how big should a partition
    /// be" and never asks "how many workers are there". A stage whose whole
    /// output is under `target_partition_bytes` collapses to a single group,
    /// so it runs as one task on one core — measured live on TPC-H q2 at
    /// SF100, where four stages coalesced to 1 partition and the cluster sat
    /// at one busy core per executor with eight of nine slots idle. Bytes were
    /// small; the *work* over them was not, and coalescing cannot see that.
    ///
    /// Callers pass the live slot count so the floor tracks the actual
    /// cluster. This mirrors Spark's `coalescePartitions.parallelismFirst`,
    /// which shrinks the advisory partition size for the same reason.
    ///
    /// The floor is advisory in one direction only: it never *raises* the
    /// partition count above what the stage already has, because coalescing
    /// may only merge.
    #[must_use]
    pub fn with_min_partitions(mut self, min_partitions: usize) -> Self {
        self.min_partitions = min_partitions.max(1);
        self
    }

    /// Return the configured parallelism floor.
    pub fn min_partitions(&self) -> usize {
        self.min_partitions
    }

    /// Bytes per merged group, shrunk so grouping cannot fall below the floor.
    ///
    /// `total_bytes / min_partitions` is the largest group size that still
    /// leaves `min_partitions` groups. Taking the min with the configured
    /// target means the floor only ever makes partitions *smaller* — it can
    /// never inflate them past the size the operator asked for.
    fn effective_target_bytes(&self, total_bytes: u128) -> u128 {
        let configured = u128::from(self.target_partition_bytes.max(1));
        if self.min_partitions <= 1 || total_bytes == 0 {
            return configured;
        }
        let by_parallelism = total_bytes.div_ceil(self.min_partitions as u128).max(1);
        configured.min(by_parallelism)
    }

    /// Compute coalesce advice from per-partition stats, without modifying the plan.
    ///
    /// Partitions are sorted by `memory_bytes` (ascending) before grouping so
    /// that all small partitions cluster together regardless of their original
    /// execution order. Without sorting, a large partition sitting between two
    /// small ones would prevent them from coalescing (Spark's AQE sorts before
    /// coalescing for the same reason). Each group of small partitions is
    /// capped at `target_partition_bytes`. Large partitions are always singleton
    /// groups.
    ///
    /// Each group contains the original partition indices (not sorted indices),
    /// so callers can map groups back to the original execution order.
    ///
    /// Example: `[small(0), big(1), small(2)]` → `[[0,2], [1]]` (2 groups)
    /// vs. the old consecutive-only approach: `[[0], [1], [2]]` (3 groups, no gain)
    pub fn advise(&self, stats: &[RuntimeStats]) -> CoalesceAdvice {
        if stats.is_empty() {
            return CoalesceAdvice { groups: Vec::new() };
        }

        // Sort by effective_bytes ascending so small partitions cluster together.
        // Prefer serialized_bytes over memory_bytes (same logic as in the loop
        // below). Stable sort preserves original order among equal-size partitions.
        let mut order: Vec<usize> = (0..stats.len()).collect();
        order.sort_by_key(|&i| {
            stats.get(i).map_or(0u128, |s| {
                u128::from(if s.serialized_bytes > 0 {
                    s.serialized_bytes
                } else {
                    s.memory_bytes
                })
            })
        });

        let mut groups: Vec<Vec<usize>> = Vec::new();
        let mut current_small: Vec<usize> = Vec::new();
        let mut current_small_bytes = 0u128;
        let total_bytes: u128 = stats
            .iter()
            .map(|s| {
                u128::from(if s.serialized_bytes > 0 {
                    s.serialized_bytes
                } else {
                    s.memory_bytes
                })
            })
            .sum();
        let target_bytes = self.effective_target_bytes(total_bytes);

        for i in order {
            let Some(s) = stats.get(i) else {
                continue;
            };
            // Prefer serialized_bytes over memory_bytes for the same reason as
            // AutoPartitionRule: shuffle output is compressed and a better
            // proxy for actual partition cost than peak in-memory footprint.
            let effective_bytes = if s.serialized_bytes > 0 {
                s.serialized_bytes
            } else {
                s.memory_bytes
            };
            if effective_bytes < self.min_partition_bytes {
                let partition_bytes = u128::from(effective_bytes);
                if !current_small.is_empty() && current_small_bytes + partition_bytes > target_bytes
                {
                    groups.push(std::mem::take(&mut current_small));
                    current_small_bytes = 0;
                }
                current_small.push(i);
                current_small_bytes += partition_bytes;
            } else {
                if !current_small.is_empty() {
                    groups.push(std::mem::take(&mut current_small));
                    current_small_bytes = 0;
                }
                groups.push(vec![i]);
            }
        }
        if !current_small.is_empty() {
            groups.push(current_small);
        }

        CoalesceAdvice { groups }
    }
}

impl AqeRule for CoalesceRule {
    fn name(&self) -> &str {
        "coalesce-small-partitions"
    }

    /// Compute coalesce advice and, when beneficial, rewrite the plan.
    ///
    /// When `advise()` produces fewer groups than the current partition count,
    /// stamps `coalesced_partition_count` on the plan and appends a
    /// [`NodeOp::CoalescePartitions`] node carrying the computed target count.
    fn apply(&self, plan: &PhysicalPlan, stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
        if stats.is_empty() || StreamingAqeGuard::plan_is_streaming(plan) {
            return None;
        }
        let advice = self.advise(stats);
        let original_count = stats.len();

        if advice.groups.len() >= original_count || original_count == 0 {
            return None;
        }

        let target_partitions = advice.groups.len().max(1);
        if target_partitions >= original_count {
            return None;
        }

        tracing::debug!(
            rule = self.name(),
            original_partitions = original_count,
            coalesced_partitions = advice.groups.len(),
            coalesce_groups = ?advice.groups,
            target_partitions,
            "CoalesceRule: {} partition(s) → {} group(s)",
            original_count,
            advice.groups.len(),
        );

        let referenced_ids = plan
            .nodes()
            .iter()
            .flat_map(|node| node.inputs().iter().map(String::as_str))
            .collect::<HashSet<_>>();
        let terminal_indexes = plan
            .nodes()
            .iter()
            .enumerate()
            .filter_map(|(index, node)| (!referenced_ids.contains(node.id())).then_some(index))
            .collect::<Vec<_>>();
        if terminal_indexes.len() > 1 {
            return None;
        }

        let label = format!("CoalescePartitions({original_count} → {target_partitions})");
        let existing_coalesce_index = terminal_indexes.first().and_then(|&terminal_index| {
            let terminal = plan.nodes().get(terminal_index)?;
            if matches!(terminal.op(), Some(NodeOp::CoalescePartitions { .. })) {
                return Some(terminal_index);
            }
            if matches!(terminal.op(), Some(NodeOp::Sink { .. })) && terminal.inputs().len() == 1 {
                let input_id = terminal.inputs().first()?;
                return plan.nodes().iter().position(|node| {
                    node.id() == input_id
                        && matches!(node.op(), Some(NodeOp::CoalescePartitions { .. }))
                });
            }
            None
        });
        if let Some(existing_coalesce_index) = existing_coalesce_index {
            let mut updated = PhysicalPlan::new(plan.name(), plan.kind());
            for (index, node) in plan.nodes().iter().enumerate() {
                let node = if index == existing_coalesce_index {
                    node.clone()
                        .with_label(label.clone())
                        .with_op(NodeOp::CoalescePartitions { target_partitions })
                } else {
                    node.clone()
                };
                updated.add_node(node);
            }
            return Some(updated.with_coalesced_partition_count(target_partitions));
        }

        let existing_ids = plan
            .nodes()
            .iter()
            .map(PlanNode::id)
            .collect::<HashSet<_>>();
        let mut suffix = 1usize;
        let coalesce_id = loop {
            let candidate = if suffix == 1 {
                "aqe:coalesce".to_string()
            } else {
                format!("aqe:coalesce:{suffix}")
            };
            if !existing_ids.contains(candidate.as_str()) {
                break candidate;
            }
            suffix = suffix.saturating_add(1);
        };

        let mut rewritten = PhysicalPlan::new(plan.name(), plan.kind());
        let mut coalesce_inputs = Vec::new();
        for (index, node) in plan.nodes().iter().enumerate() {
            if terminal_indexes.first() == Some(&index)
                && matches!(node.op(), Some(NodeOp::Sink { .. }))
                && node.inputs().len() == 1
            {
                coalesce_inputs.extend(node.inputs().iter().cloned());
                rewritten.add_node(node.clone().with_inputs([coalesce_id.clone()]));
            } else {
                rewritten.add_node(node.clone());
            }
        }
        if coalesce_inputs.is_empty()
            && let Some(&terminal_index) = terminal_indexes.first()
            && let Some(node) = plan.nodes().get(terminal_index)
        {
            coalesce_inputs.push(node.id().to_string());
        }
        rewritten.add_node(
            PlanNode::new(coalesce_id, label, plan.kind())
                .with_inputs(coalesce_inputs)
                .with_op(NodeOp::CoalescePartitions { target_partitions }),
        );
        Some(rewritten.with_coalesced_partition_count(target_partitions))
    }
}

#[cfg(test)]
mod parallelism_floor_tests {
    use super::CoalesceRule;
    use crate::optimizer::RuntimeStats;

    /// `n` partitions of `bytes` each — the shape a shuffle stage reports.
    fn stats(n: usize, bytes: u64) -> Vec<RuntimeStats> {
        (0..n)
            .map(|_| RuntimeStats {
                serialized_bytes: bytes,
                ..RuntimeStats::default()
            })
            .collect()
    }

    #[test]
    fn without_a_floor_a_small_stage_collapses_to_one_partition() {
        // The behaviour being fixed, pinned so the fix is visibly a change:
        // 18 partitions of 1 MiB is 18 MiB total, under the 128 MiB target,
        // so byte-only sizing merges the whole stage into a single task.
        let rule = CoalesceRule::new(64 * 1024 * 1024);
        let advice = rule.advise(&stats(18, 1024 * 1024));
        assert_eq!(advice.groups.len(), 1);
    }

    #[test]
    fn a_floor_keeps_a_small_stage_spread_across_the_cluster() {
        let rule = CoalesceRule::new(64 * 1024 * 1024).with_min_partitions(9);
        let advice = rule.advise(&stats(18, 1024 * 1024));
        assert_eq!(
            advice.groups.len(),
            9,
            "coalescing must not drop below the cluster's schedulable width",
        );
        // Still a real reduction — 18 partitions became 9, not 18.
        assert!(advice.groups.len() < 18);
    }

    #[test]
    fn the_floor_never_invents_partitions_the_stage_does_not_have() {
        // Four partitions on a nine-slot cluster stay four: coalescing merges,
        // it cannot split. Asking for nine groups from four inputs would be a
        // different rule (skew splitting), and silently producing empty groups
        // would hand the scheduler tasks with no work.
        let rule = CoalesceRule::new(64 * 1024 * 1024).with_min_partitions(9);
        let advice = rule.advise(&stats(4, 1024 * 1024));
        assert!(advice.groups.len() <= 4);
        assert!(advice.groups.iter().all(|g| !g.is_empty()));
    }

    #[test]
    fn the_floor_only_shrinks_partitions_never_grows_them() {
        // A stage already larger than the target must not have its partitions
        // inflated past `target_partition_bytes` just because the floor is
        // low: total/min_partitions could otherwise exceed the target.
        let rule = CoalesceRule::new(64 * 1024 * 1024)
            .with_target_partition_bytes(8 * 1024 * 1024)
            .with_min_partitions(2);
        // 16 x 4 MiB = 64 MiB total; total/2 = 32 MiB > the 8 MiB target.
        let advice = rule.advise(&stats(16, 4 * 1024 * 1024));
        // Groups are capped by the 8 MiB target (2 partitions each), not by
        // the 32 MiB the floor alone would allow.
        assert_eq!(advice.groups.len(), 8);
    }

    #[test]
    fn a_floor_of_one_is_exactly_the_old_behaviour() {
        let stats = stats(18, 1024 * 1024);
        let plain = CoalesceRule::new(64 * 1024 * 1024);
        let floored = CoalesceRule::new(64 * 1024 * 1024).with_min_partitions(1);
        assert_eq!(plain.advise(&stats), floored.advise(&stats));
    }

    #[test]
    fn every_input_partition_survives_grouping() {
        // Whatever the floor, coalescing is a partition of the index set:
        // losing an index loses that partition's rows.
        for floor in [1usize, 3, 9, 64] {
            let rule = CoalesceRule::new(64 * 1024 * 1024).with_min_partitions(floor);
            let advice = rule.advise(&stats(18, 1024 * 1024));
            let mut seen: Vec<usize> = advice.groups.iter().flatten().copied().collect();
            seen.sort_unstable();
            assert_eq!(seen, (0..18).collect::<Vec<_>>(), "floor={floor}");
        }
    }
}
