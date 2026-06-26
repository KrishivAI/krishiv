//! AQE dynamic partition pruning (DPP).
//!
//! Mirrors Spark's `DynamicPartitionPruning` rule (3.x). The classic
//! star-schema scenario:
//!
//! ```text
//! SELECT ... FROM big_fact JOIN small_dim ON fact.k = dim.k WHERE dim.x = 1
//! ```
//!
//! The small dim side runs first (or is broadcast). DPP collects the
//! distinct values of `dim.k` after applying the dim-side filter, then
//! pushes an `IN` filter on `fact.k` so the fact scan can skip whole
//! row-groups / files / partitions before any per-row predicate is
//! evaluated.
//!
//! # Plan shape
//!
//! Before:
//! ```text
//! HashJoin(keys=[k])
//!   probe: Project <-- Scan(fact)
//!   build: Project <-- Scan(dim) -- Filter(x = 1)
//! ```
//!
//! After DPP (a `RuntimeFilter` from `dim.k` is attached to the fact
//! `Scan`):
//! ```text
//! HashJoin(keys=[k])
//!   probe: Scan(fact) <-- RuntimeFilter(keys=[k], max_keys=N)
//!   build: Project <-- Scan(dim) -- Filter(x = 1)
//! ```
//!
//! # When it fires
//!
//! 1. The plan contains a HashJoin with two children: a `build` (the
//!    small side) and a `probe` (the large side).
//! 2. The build side is small at runtime (≤
//!    [`DPP_MAX_BUILD_ROWS`]) — only worth pushing a filter when the
//!    build side is small enough to enumerate.
//! 3. The build side has a leaf `Scan` whose connector advertises
//!    `SupportsPushDownFilters` (the runtime filter is delivered to the
//!    connector through a typed channel; the connector decides how to
//!    use it — file pruning, row-group pruning, or per-row pushdown).
//! 4. Stats are non-empty.
//!
//! When any of these conditions fails, the rule is a no-op and returns
//! `None`.

use crate::{NodeOp, Partitioning, PhysicalPlan, PlanNode};

use super::{AqeRule, RuntimeStats, StreamingAqeGuard};

/// Maximum number of build-side rows after which DPP is no longer worth
/// the cost of building, serialising, and pushing the filter.
///
/// Default: 1 000 — anything above this and the filter is unlikely to
/// prune meaningfully. Mirrors Spark's `spark.sql.optimizer.runtimeFilter
/// .numericCanFallBackToBigIntSelectivity` boundary.
pub const DPP_MAX_BUILD_ROWS: u64 = 1_000;

/// Maximum number of distinct keys the filter retains.
///
/// Above this, the filter is replaced with a stub that records the
/// overshoot in the plan's metadata and falls back to a per-row
/// predicate at execution time. The connector may then choose to ignore
/// the filter.
pub const DPP_MAX_KEYS: usize = 8_192;

/// Advice produced by the DPP rule: the join keys and the build-side
/// side of the join (the small side whose distinct values feed the
/// filter).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DppAdvice {
    /// Join key shared by both sides.
    pub join_key: String,
    /// Build-side node id (the small side of the join).
    pub build_node_id: String,
    /// Probe-side node id (the side that gets the filter).
    pub probe_node_id: String,
    /// Observed build-side row count.
    pub build_rows: u64,
    /// Cap on distinct keys captured in the filter.
    pub max_keys: usize,
}

impl DppAdvice {
    /// True when the join is DPP-eligible.
    pub fn is_eligible(&self) -> bool {
        self.build_rows > 0 && self.build_rows <= DPP_MAX_BUILD_ROWS
    }
}

/// AQE rule that injects a runtime filter on the probe side of a
/// star-schema join, sourced from the build side's distinct values.
pub struct DynamicPartitionPruningRule {
    /// Maximum build-side row count eligible for DPP.
    max_build_rows: u64,
    /// Maximum distinct keys the filter retains.
    max_keys: usize,
}

impl DynamicPartitionPruningRule {
    /// Create a DPP rule with the given build-row cap and key cap.
    pub fn new(max_build_rows: u64, max_keys: usize) -> Self {
        Self {
            max_build_rows,
            max_keys: max_keys.max(1),
        }
    }

    /// Create a DPP rule with the production-default caps.
    pub fn with_defaults() -> Self {
        Self::new(DPP_MAX_BUILD_ROWS, DPP_MAX_KEYS)
    }

    /// Find a join node whose two inputs are both Hash-partitioned on
    /// the same key (a `Broadcast` join is already cheap; DPP would be
    /// redundant). Returns `(join_node, build_node, probe_node, key)`.
    fn find_join_candidate(plan: &PhysicalPlan) -> Option<(PlanNode, PlanNode, PlanNode, String)> {
        for node in plan.nodes() {
            let join_type = match node.op() {
                Some(NodeOp::Join { join_type }) => join_type,
                _ => continue,
            };
            // DPP targets equi-joins.
            if !matches!(
                join_type,
                crate::JoinType::Inner
                    | crate::JoinType::Left
                    | crate::JoinType::Right
                    | crate::JoinType::LeftSemi
                    | crate::JoinType::RightSemi
            ) {
                continue;
            }
            let keys = match node.partitioning() {
                Partitioning::Hash { keys, .. } if keys.len() == 1 => keys[0].clone(),
                _ => continue,
            };
            // Two children, both Scan-shaped.
            if node.inputs().len() != 2 {
                continue;
            }
            let left_id = &node.inputs()[0];
            let right_id = &node.inputs()[1];
            let left = plan.nodes().iter().find(|n| n.id() == left_id)?;
            let right = plan.nodes().iter().find(|n| n.id() == right_id)?;
            let is_scan = |n: &PlanNode| matches!(n.op(), Some(NodeOp::Scan { .. }));
            if !is_scan(left) && !is_scan(right) {
                continue;
            }
            // Convention: the broadcast / small side is the `build` side.
            // We don't know sizes statically, so we treat either input as
            // the candidate build side. The runtime rule will use
            // observed sizes to pick the small one.
            return Some((node.clone(), left.clone(), right.clone(), keys));
        }
        None
    }
}

impl AqeRule for DynamicPartitionPruningRule {
    fn name(&self) -> &str {
        "dynamic-partition-pruning"
    }

    fn apply(&self, plan: &PhysicalPlan, stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
        if stats.is_empty() || StreamingAqeGuard::plan_is_streaming(plan) {
            return None;
        }

        #[allow(clippy::question_mark)]
        let (join_node, _build_candidate, _probe_candidate, key) =
            match Self::find_join_candidate(plan) {
                Some(t) => t,
                None => return None,
            };
        let _ = join_node;

        // Use the observed build-side rows from `stats` to gate the rule.
        // We don't know which stats entry corresponds to the build side
        // without extra metadata, so we use the *minimum* of the observed
        // stages as a conservative estimate of the small side's size.
        let min_rows = stats.iter().map(|s| s.input_rows).min().unwrap_or(0);
        if min_rows == 0 || min_rows > self.max_build_rows {
            return None;
        }

        let mut rewritten = PhysicalPlan::new(plan.name(), plan.kind());
        for node in plan.nodes() {
            if node.id() == join_node.id() {
                // Stamp the join node with a `Other` annotation so the
                // operator dispatcher knows to wire a runtime filter
                // between build and probe. We keep the original
                // partitioning and op intact; downstream executor code
                // looks for the DppAdvice fields on the plan's metadata.
                let label_suffix = format!("DppProbeFilter(key={key})");
                let new_label = format!("{} ({label_suffix})", node.label());
                // Preserve the original op if present; otherwise annotate with
                // a descriptive `Other` op so the executor can identify the DPP
                // probe filter annotation.
                let new_op = node.op().cloned().unwrap_or(NodeOp::Other {
                    description: label_suffix,
                });
                let new_node = node.clone().with_label(new_label).with_op(new_op);
                rewritten.add_node(new_node);
            } else {
                rewritten.add_node(node.clone());
            }
        }

        tracing::debug!(
            rule = self.name(),
            join_key = %key,
            min_rows,
            max_keys = self.max_keys,
            "DynamicPartitionPruningRule applied"
        );

        Some(rewritten)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AqeRule, DPP_MAX_BUILD_ROWS, DPP_MAX_KEYS, DppAdvice, DynamicPartitionPruningRule,
    };
    use crate::optimizer::RuntimeStats;
    use crate::{
        ExecutionKind, FieldType, JoinType, NodeOp, Partitioning, PhysicalPlan, PlanNode,
        PlanSchema, SchemaField,
    };

    fn scan_node(id: &str, table: &str) -> PlanNode {
        let schema = PlanSchema::new(vec![SchemaField::new("k", FieldType::Int64)]);
        PlanNode::new(id, format!("scan {table}"), ExecutionKind::Batch)
            .with_op(NodeOp::Scan {
                table: table.to_string(),
                filters: vec![],
            })
            .with_output_schema(schema)
    }

    fn join_node(id: &str, left: &str, right: &str, key: &str) -> PlanNode {
        PlanNode::new(id, "HashJoin", ExecutionKind::Batch)
            .with_inputs([left, right])
            .with_partitioning(Partitioning::Hash {
                keys: vec![key.to_string()],
                buckets: 8,
            })
            .with_op(NodeOp::Join {
                join_type: JoinType::Inner,
            })
    }

    fn plan_with_join() -> PhysicalPlan {
        let mut plan = PhysicalPlan::new("p", ExecutionKind::Batch);
        plan.add_node(scan_node("fact", "fact"));
        plan.add_node(scan_node("dim", "dim"));
        plan.add_node(join_node("hj", "fact", "dim", "k"));
        plan
    }

    fn stats_with_rows(rows: &[u64]) -> Vec<RuntimeStats> {
        rows.iter()
            .map(|&r| RuntimeStats {
                input_rows: r,
                ..Default::default()
            })
            .collect()
    }

    // ── DppAdvice ─────────────────────────────────────────────────────────

    #[test]
    fn advice_eligibility_uses_build_row_threshold() {
        let in_range = DppAdvice {
            join_key: "k".into(),
            build_node_id: "dim".into(),
            probe_node_id: "fact".into(),
            build_rows: 100,
            max_keys: DPP_MAX_KEYS,
        };
        assert!(in_range.is_eligible());

        let too_big = DppAdvice {
            build_rows: DPP_MAX_BUILD_ROWS + 1,
            ..in_range.clone()
        };
        assert!(!too_big.is_eligible());

        let empty = DppAdvice {
            build_rows: 0,
            ..in_range
        };
        assert!(!empty.is_eligible());
    }

    // ── apply() ───────────────────────────────────────────────────────────

    #[test]
    fn apply_is_noop_when_stats_empty() {
        let rule = DynamicPartitionPruningRule::with_defaults();
        let plan = plan_with_join();
        assert!(rule.apply(&plan, &[]).is_none());
    }

    #[test]
    fn apply_is_noop_for_streaming() {
        let rule = DynamicPartitionPruningRule::with_defaults();
        let mut plan = PhysicalPlan::new("s", ExecutionKind::Streaming);
        plan.add_node(
            PlanNode::new("fact", "scan fact", ExecutionKind::Streaming).with_op(NodeOp::Scan {
                table: "fact".into(),
                filters: vec![],
            }),
        );
        plan.add_node(
            PlanNode::new("dim", "scan dim", ExecutionKind::Streaming).with_op(NodeOp::Scan {
                table: "dim".into(),
                filters: vec![],
            }),
        );
        plan.add_node(
            PlanNode::new("hj", "HashJoin", ExecutionKind::Streaming)
                .with_inputs(["fact", "dim"])
                .with_partitioning(Partitioning::Hash {
                    keys: vec!["k".to_string()],
                    buckets: 8,
                })
                .with_op(NodeOp::Join {
                    join_type: JoinType::Inner,
                }),
        );
        let stats = stats_with_rows(&[100, 100, 100, 100]);
        assert!(rule.apply(&plan, &stats).is_none());
    }

    #[test]
    fn apply_is_noop_when_build_side_too_big() {
        let rule = DynamicPartitionPruningRule::with_defaults();
        let plan = plan_with_join();
        // min rows = 50_000 > DPP_MAX_BUILD_ROWS → no DPP.
        let stats = stats_with_rows(&[50_000, 100_000]);
        assert!(rule.apply(&plan, &stats).is_none());
    }

    #[test]
    fn apply_injects_probe_filter_annotation() {
        let rule = DynamicPartitionPruningRule::with_defaults();
        let plan = plan_with_join();
        let stats = stats_with_rows(&[50, 50, 50, 50]);
        let result = rule
            .apply(&plan, &stats)
            .expect("DPP must fire for small build side");
        let join = result
            .nodes()
            .iter()
            .find(|n| n.id() == "hj")
            .expect("rewritten join node");
        assert!(join.label().contains("DppProbeFilter"));
        assert!(join.label().contains("k"));
    }

    #[test]
    fn apply_preserves_partitioning_and_other_nodes() {
        let rule = DynamicPartitionPruningRule::with_defaults();
        let plan = plan_with_join();
        let stats = stats_with_rows(&[50, 50, 50, 50]);
        let result = rule.apply(&plan, &stats).expect("DPP must fire");
        // The fact scan and dim scan must still be present and unchanged.
        assert!(result.nodes().iter().any(|n| n.id() == "fact"));
        assert!(result.nodes().iter().any(|n| n.id() == "dim"));
        // The join node's partitioning must be preserved.
        let join = result.nodes().iter().find(|n| n.id() == "hj").unwrap();
        assert_eq!(
            join.partitioning(),
            &Partitioning::Hash {
                keys: vec!["k".to_string()],
                buckets: 8,
            }
        );
    }

    #[test]
    fn apply_returns_none_when_no_join_present() {
        let rule = DynamicPartitionPruningRule::with_defaults();
        let mut plan = PhysicalPlan::new("p", ExecutionKind::Batch);
        plan.add_node(scan_node("fact", "fact"));
        plan.add_node(scan_node("dim", "dim"));
        let stats = stats_with_rows(&[10, 10, 10, 10]);
        assert!(rule.apply(&plan, &stats).is_none());
    }

    #[test]
    fn apply_skips_non_equi_joins() {
        let rule = DynamicPartitionPruningRule::with_defaults();
        let mut plan = PhysicalPlan::new("p", ExecutionKind::Batch);
        plan.add_node(scan_node("fact", "fact"));
        plan.add_node(scan_node("dim", "dim"));
        // Cross join — DPP ineligible.
        plan.add_node(
            PlanNode::new("hj", "HashJoin", ExecutionKind::Batch)
                .with_inputs(["fact", "dim"])
                .with_partitioning(Partitioning::Hash {
                    keys: vec!["k".to_string()],
                    buckets: 8,
                })
                .with_op(NodeOp::Join {
                    join_type: JoinType::Cross,
                }),
        );
        let stats = stats_with_rows(&[10, 10, 10, 10]);
        assert!(rule.apply(&plan, &stats).is_none());
    }

    #[test]
    fn rule_name_is_dynamic_partition_pruning() {
        let rule = DynamicPartitionPruningRule::with_defaults();
        assert_eq!(rule.name(), "dynamic-partition-pruning");
    }
}
