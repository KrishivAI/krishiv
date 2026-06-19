//! Broadcast-auto logical optimizer rule.

use crate::{LogicalPlan, NodeOp, PlanNode};

use super::OptimizerRule;

/// Default threshold for auto-broadcast: tables with estimated rows below
/// this value are candidates for broadcast join.  ~1M rows ≈ 100 MiB at 100
/// bytes/row.
pub const DEFAULT_BROADCAST_THRESHOLD_ROWS: u64 = 1_000_000;

/// Logical optimizer rule that marks small scan nodes as broadcast-eligible.
///
/// Scans the logical plan for `NodeOp::Scan` nodes whose `estimated_rows` is
/// set and below the threshold.  Such nodes are annotated with
/// `broadcast_eligible = true` so the lowering pass promotes their exchange
/// to `Broadcast` partitioning.
///
/// The threshold is deliberately conservative (1M rows).  Without `estimated_rows`
/// populated from source metadata (parquet footer, Kafka stats, etc.) the rule
/// is a no-op.
pub struct BroadcastAutoRule {
    /// Max rows a table can have to be considered broadcast-eligible.
    max_rows: u64,
}

impl BroadcastAutoRule {
    /// Create a new rule with the given max row threshold.
    pub fn new(max_rows: u64) -> Self {
        Self { max_rows }
    }
}

impl OptimizerRule for BroadcastAutoRule {
    fn name(&self) -> &str {
        "broadcast-auto"
    }

    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let nodes = plan.nodes();
        let mut changed = false;
        let mut new_nodes: Vec<PlanNode> = Vec::with_capacity(nodes.len());

        for node in nodes {
            let is_small_scan = matches!(node.op(), Some(NodeOp::Scan { .. }))
                && node.estimated_rows().is_some_and(|r| r <= self.max_rows);

            if is_small_scan && !node.broadcast_eligible() {
                changed = true;
                new_nodes.push(node.clone().with_broadcast_eligible(true));
            } else {
                new_nodes.push(node.clone());
            }
        }

        if !changed {
            return None;
        }

        let mut new_plan = LogicalPlan::new(plan.name(), plan.kind());
        for n in new_nodes {
            new_plan = new_plan.with_node(n);
        }
        Some(new_plan)
    }
}
