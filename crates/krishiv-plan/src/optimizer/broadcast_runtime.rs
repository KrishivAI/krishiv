//! AQE runtime broadcast-join promotion/demotion rule.

use crate::{Partitioning, PhysicalPlan};

use super::{AqeRule, RuntimeStats, StreamingAqeGuard};

/// Default maximum observed output size for runtime broadcast promotion:
/// 64 MiB.
///
/// Matches the spirit of Spark's `spark.sql.autoBroadcastJoinThreshold` family
/// of defaults (10 MiB static, larger when runtime sizes are known): once a
/// stage has actually executed we trust the observed size, so the threshold
/// can be more generous than the logical-time row estimate used by
/// `BroadcastAutoRule`.
pub const DEFAULT_MAX_BROADCAST_BYTES: u64 = 64 * 1024 * 1024;

/// Target bytes per partition used when sizing the demotion fallback.
/// Shared with `AutoPartitionRule` (128 MiB).
const DEMOTION_TARGET_PARTITION_BYTES: u64 = krishiv_common::partition::TARGET_BYTES_PER_PARTITION;

/// Bucket-count clamp for demoted nodes: at least 2 (a demoted broadcast is by
/// definition too large for one replica, so it must actually be split) and at
/// most 64 (matching the default `AutoPartitionRule` parallelism cap).
const DEMOTION_MIN_BUCKETS: u64 = 2;
const DEMOTION_MAX_BUCKETS: u64 = 64;

/// AQE rule that promotes or demotes broadcast joins based on the observed
/// output size from the previous execution.
///
/// `BroadcastAutoRule` makes a logical-time guess from `estimated_rows`; this
/// rule corrects that guess at runtime:
///
/// - **Promotion**: when the observed stage output is at or below
///   `max_broadcast_bytes` and a node is `broadcast_eligible()` with `Hash` or
///   `RoundRobin` partitioning, the node's partitioning is rewritten to
///   [`Partitioning::Broadcast`], replacing the shuffle with a replicate.
/// - **Demotion**: when a node is already [`Partitioning::Broadcast`] but the
///   observed output exceeds the threshold, the broadcast is undone.
///   `Partitioning::Broadcast` does not record the original hash keys, so they
///   cannot be recovered; the node is demoted to
///   `Partitioning::RoundRobin { buckets }` with
///   `buckets = clamp(ceil(observed / 128 MiB), 2, 64)`.  Round-robin is the
///   semantically safe choice — it makes no key-colocation promise, whereas
///   guessing hash keys could silently mis-distribute keyed data.
///
/// Like the other AQE sizing rules, the observed size is the sum over the
/// per-stage [`RuntimeStats`] slice, preferring `serialized_bytes` (shuffle
/// wire size) and falling back to `memory_bytes` when it is zero — the same
/// convention as `AutoPartitionRule`.
///
/// The rule is intrinsically disabled for streaming plans (changing
/// partitioning mid-job would orphan keyed state) and returns `None` when
/// stats are empty or nothing changes.
pub struct BroadcastRuntimeRule {
    /// Max observed output bytes for a node to be (or stay) broadcast.
    max_broadcast_bytes: u64,
}

impl BroadcastRuntimeRule {
    /// Create a new rule with the given broadcast size threshold in bytes.
    ///
    /// Use [`DEFAULT_MAX_BROADCAST_BYTES`] (64 MiB) for the standard default.
    pub fn new(max_broadcast_bytes: u64) -> Self {
        Self {
            max_broadcast_bytes,
        }
    }

    /// Compute the round-robin bucket count for a demoted broadcast node:
    /// `clamp(ceil(observed_bytes / 128 MiB), 2, 64)`.
    fn demotion_buckets(observed_bytes: u64) -> u32 {
        observed_bytes
            .div_ceil(DEMOTION_TARGET_PARTITION_BYTES)
            .clamp(DEMOTION_MIN_BUCKETS, DEMOTION_MAX_BUCKETS) as u32
    }
}

impl AqeRule for BroadcastRuntimeRule {
    fn name(&self) -> &str {
        "broadcast-runtime"
    }

    fn apply(&self, plan: &PhysicalPlan, stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
        if stats.is_empty() || StreamingAqeGuard::plan_is_streaming(plan) {
            return None;
        }

        // Sum the best available size metric across all partitions, preferring
        // serialized_bytes over memory_bytes (same convention as
        // AutoPartitionRule — see RuntimeStats::serialized_bytes docs).
        let observed_bytes: u64 = stats
            .iter()
            .map(|s| {
                if s.serialized_bytes > 0 {
                    s.serialized_bytes
                } else {
                    s.memory_bytes
                }
            })
            .sum();
        if observed_bytes == 0 {
            return None;
        }

        let fits_broadcast = observed_bytes <= self.max_broadcast_bytes;

        // First pass: detect whether any node needs rewriting so non-firing
        // applications pay no clone cost.
        let mut changed = false;
        for node in plan.nodes() {
            match node.partitioning() {
                Partitioning::Hash { .. } | Partitioning::RoundRobin { .. }
                    if fits_broadcast && node.broadcast_eligible() =>
                {
                    changed = true;
                }
                Partitioning::Broadcast if !fits_broadcast => {
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
            let eligible = node.broadcast_eligible();
            let old = node.partitioning().clone();
            match old {
                Partitioning::Hash { .. } | Partitioning::RoundRobin { .. }
                    if fits_broadcast && eligible =>
                {
                    node.set_partitioning(Partitioning::Broadcast);
                }
                Partitioning::Broadcast if !fits_broadcast => {
                    node.set_partitioning(Partitioning::RoundRobin {
                        buckets: Self::demotion_buckets(observed_bytes),
                    });
                }
                _ => {}
            }
        }

        tracing::debug!(
            rule = "broadcast-runtime",
            observed_bytes,
            promoted = fits_broadcast,
            "BroadcastRuntimeRule applied"
        );

        Some(plan)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::optimizer::AqeOptimizer;
    use crate::{ExecutionKind, Partitioning, PhysicalPlan, PlanNode};

    use super::{AqeRule, BroadcastRuntimeRule, DEFAULT_MAX_BROADCAST_BYTES, RuntimeStats};

    const ONE_MIB: u64 = 1024 * 1024;

    fn hash_node(id: &str, eligible: bool) -> PlanNode {
        PlanNode::new(id, "exchange", ExecutionKind::Batch)
            .with_partitioning(Partitioning::Hash {
                keys: vec!["k".into()],
                buckets: 8,
            })
            .with_broadcast_eligible(eligible)
    }

    fn broadcast_node(id: &str) -> PlanNode {
        PlanNode::new(id, "broadcast exchange", ExecutionKind::Batch)
            .with_partitioning(Partitioning::Broadcast)
            .with_broadcast_eligible(true)
    }

    fn plan_with(nodes: Vec<PlanNode>) -> PhysicalPlan {
        let mut plan = PhysicalPlan::new("test", ExecutionKind::Batch);
        for node in nodes {
            plan = plan.with_node(node);
        }
        plan
    }

    fn stats_with_serialized(bytes: &[u64]) -> Vec<RuntimeStats> {
        bytes
            .iter()
            .map(|&b| RuntimeStats {
                serialized_bytes: b,
                ..Default::default()
            })
            .collect()
    }

    // ── promotion ─────────────────────────────────────────────────────────

    #[test]
    fn promotion_fires_for_small_eligible_hash_node() {
        let plan = plan_with(vec![hash_node("xchg", true)]);
        let stats = stats_with_serialized(&[10 * ONE_MIB]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        let result = rule.apply(&plan, &stats).expect("promotion must fire");
        let node = result.nodes().iter().find(|n| n.id() == "xchg").unwrap();
        assert_eq!(node.partitioning(), &Partitioning::Broadcast);
    }

    #[test]
    fn promotion_fires_for_small_eligible_round_robin_node() {
        let plan = plan_with(vec![
            PlanNode::new("rr", "exchange", ExecutionKind::Batch)
                .with_partitioning(Partitioning::RoundRobin { buckets: 4 })
                .with_broadcast_eligible(true),
        ]);
        let stats = stats_with_serialized(&[ONE_MIB]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        let result = rule.apply(&plan, &stats).expect("promotion must fire");
        assert_eq!(result.nodes()[0].partitioning(), &Partitioning::Broadcast);
    }

    #[test]
    fn promotion_fires_at_exact_threshold() {
        // "at or below" — observed == threshold must still promote.
        let plan = plan_with(vec![hash_node("xchg", true)]);
        let stats = stats_with_serialized(&[DEFAULT_MAX_BROADCAST_BYTES]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        let result = rule.apply(&plan, &stats).expect("boundary must promote");
        assert_eq!(result.nodes()[0].partitioning(), &Partitioning::Broadcast);
    }

    #[test]
    fn promotion_aggregates_stats_across_partitions() {
        // Two partitions of 40 MiB each → 80 MiB total, above the 64 MiB
        // threshold even though each individual partition is below it.
        let plan = plan_with(vec![hash_node("xchg", true)]);
        let stats = stats_with_serialized(&[40 * ONE_MIB, 40 * ONE_MIB]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        assert!(
            rule.apply(&plan, &stats).is_none(),
            "summed size exceeds threshold → no promotion"
        );
    }

    #[test]
    fn promotion_prefers_serialized_bytes_over_memory_bytes() {
        // 200 MiB in memory but only 10 MiB serialized: the rule must use
        // serialized_bytes and promote.
        let plan = plan_with(vec![hash_node("xchg", true)]);
        let stats = vec![RuntimeStats {
            memory_bytes: 200 * ONE_MIB,
            serialized_bytes: 10 * ONE_MIB,
            ..Default::default()
        }];
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        let result = rule.apply(&plan, &stats).expect("promotion must fire");
        assert_eq!(result.nodes()[0].partitioning(), &Partitioning::Broadcast);
    }

    #[test]
    fn promotion_falls_back_to_memory_bytes_when_serialized_is_zero() {
        let plan = plan_with(vec![hash_node("xchg", true)]);
        let stats = vec![RuntimeStats {
            memory_bytes: ONE_MIB,
            serialized_bytes: 0,
            ..Default::default()
        }];
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        let result = rule.apply(&plan, &stats).expect("promotion must fire");
        assert_eq!(result.nodes()[0].partitioning(), &Partitioning::Broadcast);
    }

    #[test]
    fn no_promotion_when_not_broadcast_eligible() {
        let plan = plan_with(vec![hash_node("xchg", false)]);
        let stats = stats_with_serialized(&[ONE_MIB]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        assert!(
            rule.apply(&plan, &stats).is_none(),
            "ineligible node must not be promoted"
        );
    }

    #[test]
    fn no_promotion_above_threshold() {
        let plan = plan_with(vec![hash_node("xchg", true)]);
        let stats = stats_with_serialized(&[DEFAULT_MAX_BROADCAST_BYTES + 1]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        assert!(
            rule.apply(&plan, &stats).is_none(),
            "observed size above threshold must not promote"
        );
    }

    #[test]
    fn no_promotion_for_unpartitioned_node() {
        let plan = plan_with(vec![
            PlanNode::new("scan", "scan", ExecutionKind::Batch).with_broadcast_eligible(true),
        ]);
        let stats = stats_with_serialized(&[ONE_MIB]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        assert!(
            rule.apply(&plan, &stats).is_none(),
            "only Hash/RoundRobin nodes are promotion candidates"
        );
    }

    // ── demotion ──────────────────────────────────────────────────────────

    #[test]
    fn demotion_fires_when_broadcast_node_observed_too_large() {
        let plan = plan_with(vec![broadcast_node("bcast")]);
        // 300 MiB observed → demote; ceil(300 / 128) = 3 buckets.
        let stats = stats_with_serialized(&[300 * ONE_MIB]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        let result = rule.apply(&plan, &stats).expect("demotion must fire");
        assert_eq!(
            result.nodes()[0].partitioning(),
            &Partitioning::RoundRobin { buckets: 3 }
        );
    }

    #[test]
    fn demotion_bucket_count_clamped_to_minimum_two() {
        // Just above the broadcast threshold: ceil(65 MiB / 128 MiB) = 1, but a
        // demoted node must be split into at least 2 buckets.
        let plan = plan_with(vec![broadcast_node("bcast")]);
        let stats = stats_with_serialized(&[65 * ONE_MIB]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        let result = rule.apply(&plan, &stats).expect("demotion must fire");
        assert_eq!(
            result.nodes()[0].partitioning(),
            &Partitioning::RoundRobin { buckets: 2 }
        );
    }

    #[test]
    fn demotion_bucket_count_clamped_to_maximum_sixty_four() {
        // 64 GiB observed → ceil(64 GiB / 128 MiB) = 512, clamped to 64.
        let plan = plan_with(vec![broadcast_node("bcast")]);
        let stats = stats_with_serialized(&[64 * 1024 * ONE_MIB]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        let result = rule.apply(&plan, &stats).expect("demotion must fire");
        assert_eq!(
            result.nodes()[0].partitioning(),
            &Partitioning::RoundRobin { buckets: 64 }
        );
    }

    #[test]
    fn no_demotion_when_broadcast_node_within_threshold() {
        let plan = plan_with(vec![broadcast_node("bcast")]);
        let stats = stats_with_serialized(&[ONE_MIB]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        assert!(
            rule.apply(&plan, &stats).is_none(),
            "small broadcast node stays broadcast → no change → None"
        );
    }

    #[test]
    fn promotion_and_demotion_apply_together() {
        // One small-side eligible hash node and one oversized broadcast node
        // in the same plan: with the observed size above the threshold, the
        // hash node stays put and the broadcast node is demoted.
        let plan = plan_with(vec![hash_node("xchg", true), broadcast_node("bcast")]);
        let stats = stats_with_serialized(&[200 * ONE_MIB]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        let result = rule.apply(&plan, &stats).expect("demotion must fire");
        let xchg = result.nodes().iter().find(|n| n.id() == "xchg").unwrap();
        let bcast = result.nodes().iter().find(|n| n.id() == "bcast").unwrap();
        assert!(
            matches!(xchg.partitioning(), Partitioning::Hash { .. }),
            "hash node above threshold must not be promoted"
        );
        assert_eq!(
            bcast.partitioning(),
            &Partitioning::RoundRobin { buckets: 2 },
            "broadcast node above threshold must be demoted"
        );
    }

    // ── no-change / guard / empty-stats contracts ─────────────────────────

    #[test]
    fn returns_none_when_no_change() {
        // Unpartitioned, ineligible node: neither promotion nor demotion applies.
        let plan = plan_with(vec![PlanNode::new("scan", "scan", ExecutionKind::Batch)]);
        let stats = stats_with_serialized(&[ONE_MIB]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        assert!(rule.apply(&plan, &stats).is_none());
    }

    #[test]
    fn empty_stats_returns_none() {
        let plan = plan_with(vec![hash_node("xchg", true)]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        assert!(rule.apply(&plan, &[]).is_none());
    }

    #[test]
    fn zero_observed_bytes_returns_none() {
        let plan = plan_with(vec![hash_node("xchg", true)]);
        let stats = vec![RuntimeStats::default()];
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        assert!(rule.apply(&plan, &stats).is_none());
    }

    #[test]
    fn rule_is_intrinsically_disabled_for_streaming() {
        let mut plan = PhysicalPlan::new("stream", ExecutionKind::Streaming);
        plan = plan.with_node(
            PlanNode::new("xchg", "exchange", ExecutionKind::Streaming)
                .with_partitioning(Partitioning::Hash {
                    keys: vec!["k".into()],
                    buckets: 8,
                })
                .with_broadcast_eligible(true),
        );
        let stats = stats_with_serialized(&[ONE_MIB]);
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);

        assert!(rule.apply(&plan, &stats).is_none());
    }

    #[test]
    fn streaming_guard_respected_via_aqe_optimizer() {
        let mut aqe = AqeOptimizer::new();
        aqe.add_guarded_rule(Box::new(BroadcastRuntimeRule::new(
            DEFAULT_MAX_BROADCAST_BYTES,
        )));

        let plan = PhysicalPlan::new("stream", ExecutionKind::Streaming).with_node(
            PlanNode::new("xchg", "exchange", ExecutionKind::Streaming)
                .with_partitioning(Partitioning::Hash {
                    keys: vec!["k".into()],
                    buckets: 8,
                })
                .with_broadcast_eligible(true),
        );
        let stats = stats_with_serialized(&[ONE_MIB]);

        let (result, applied) = aqe.apply(plan.clone(), &stats).expect("aqe");
        assert_eq!(result, plan, "streaming plan must be untouched");
        assert!(applied.is_empty(), "guarded rule must not fire");
    }

    #[test]
    fn batch_plan_promoted_via_aqe_optimizer() {
        let mut aqe = AqeOptimizer::new();
        aqe.add_guarded_rule(Box::new(BroadcastRuntimeRule::new(
            DEFAULT_MAX_BROADCAST_BYTES,
        )));

        let plan = plan_with(vec![hash_node("xchg", true)]);
        let stats = stats_with_serialized(&[ONE_MIB]);

        let (result, applied) = aqe.apply(plan, &stats).expect("aqe");
        assert_eq!(applied, vec!["broadcast-runtime"]);
        assert_eq!(result.nodes()[0].partitioning(), &Partitioning::Broadcast);
    }

    #[test]
    fn rule_name_is_broadcast_runtime() {
        let rule = BroadcastRuntimeRule::new(DEFAULT_MAX_BROADCAST_BYTES);
        assert_eq!(rule.name(), "broadcast-runtime");
    }
}
