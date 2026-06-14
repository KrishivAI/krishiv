//! Join-reordering logical optimizer rule.
//!
//! Reorders binary join inputs so that the smaller table (by `estimated_rows`)
//! appears on the LEFT.  For left-deep join trees this minimises the size of
//! intermediate results at each join step, reducing memory pressure and
//! improving cache locality.
//!
//! The rule is a no-op for join types where input order affects semantics
//! (`Left`, `Right`, `Full`, `Semi`, `Anti`).  Only `Inner` and `Cross` joins
//! are commutative and can be safely reordered.
//!
//! **Important**: the rule operates on `estimated_rows` annotations that must
//! already be present on the plan nodes.

use std::collections::HashMap;

use crate::{JoinType, LogicalPlan, NodeOp, PlanNode};

use super::OptimizerRule;

/// Logical optimizer rule that puts the smaller table on the left of commutative joins.
///
/// For each `Join { Inner }` or `Join { Cross }` node whose two inputs both
/// carry `estimated_rows`, the rule checks whether swapping the inputs would
/// place a smaller table on the left.  When it would, the inputs are swapped.
///
/// All other join types are left unchanged because input order is semantically
/// meaningful for outer, semi, and anti joins.
pub struct JoinReorderRule;

impl OptimizerRule for JoinReorderRule {
    fn name(&self) -> &str {
        "join-reorder"
    }

    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let nodes = plan.nodes();

        // Build a map from node ID → estimated_rows for O(1) lookup.
        let row_estimates: HashMap<&str, u64> = nodes
            .iter()
            .filter_map(|n| n.estimated_rows().map(|r| (n.id(), r)))
            .collect();

        let mut changed = false;
        let mut new_nodes: Vec<PlanNode> = Vec::with_capacity(nodes.len());

        for node in nodes {
            let Some(NodeOp::Join { join_type }) = node.op() else {
                new_nodes.push(node.clone());
                continue;
            };

            // Only commutative joins can be reordered without changing semantics.
            if !matches!(join_type, JoinType::Inner | JoinType::Cross) {
                new_nodes.push(node.clone());
                continue;
            }

            let inputs = node.inputs();
            if inputs.len() != 2 {
                new_nodes.push(node.clone());
                continue;
            }

            let left_rows = row_estimates
                .get(inputs[0].as_str())
                .copied()
                .unwrap_or(u64::MAX);
            let right_rows = row_estimates
                .get(inputs[1].as_str())
                .copied()
                .unwrap_or(u64::MAX);

            // Swap when the right input is strictly smaller than the left.
            // After the swap the smaller table is on the left, which is the
            // outer/driving side for nested-loop and sort-merge joins, and
            // keeps left-deep join trees in ascending size order so that each
            // successive join operates on the smallest available intermediate.
            if right_rows < left_rows {
                let swapped = vec![inputs[1].clone(), inputs[0].clone()];
                new_nodes.push(node.clone().with_inputs(swapped));
                changed = true;
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExecutionKind, JoinType, LogicalPlan, NodeOp, PlanNode};

    fn scan(id: &str, rows: u64) -> PlanNode {
        PlanNode::new(id, format!("scan {id}"), ExecutionKind::Batch)
            .with_op(NodeOp::Scan {
                table: id.to_string(),
                filters: vec![],
            })
            .with_estimated_rows(Some(rows))
    }

    fn scan_no_estimate(id: &str) -> PlanNode {
        PlanNode::new(id, format!("scan {id}"), ExecutionKind::Batch).with_op(NodeOp::Scan {
            table: id.to_string(),
            filters: vec![],
        })
    }

    fn inner_join(id: &str, left: &str, right: &str) -> PlanNode {
        PlanNode::new(id, "join", ExecutionKind::Batch)
            .with_inputs([left, right])
            .with_op(NodeOp::Join {
                join_type: JoinType::Inner,
            })
    }

    fn join_with_type(id: &str, left: &str, right: &str, jt: JoinType) -> PlanNode {
        PlanNode::new(id, "join", ExecutionKind::Batch)
            .with_inputs([left, right])
            .with_op(NodeOp::Join { join_type: jt })
    }

    // ── Rule name ─────────────────────────────────────────────────────────────

    #[test]
    fn join_reorder_rule_name() {
        assert_eq!(JoinReorderRule.name(), "join-reorder");
    }

    // ── No-op cases ───────────────────────────────────────────────────────────

    #[test]
    fn join_reorder_noop_when_already_ordered_correctly() {
        // small(10) on left, large(1000) on right → already correct; no swap
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(scan("small", 10))
            .with_node(scan("large", 1000))
            .with_node(inner_join("j", "small", "large"));

        let result = JoinReorderRule.apply(&plan);
        assert!(result.is_none(), "already ordered correctly → no change");
    }

    #[test]
    fn join_reorder_noop_when_no_estimates() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(scan_no_estimate("a"))
            .with_node(scan_no_estimate("b"))
            .with_node(inner_join("j", "a", "b"));

        let result = JoinReorderRule.apply(&plan);
        assert!(result.is_none(), "no estimates → no change");
    }

    #[test]
    fn join_reorder_noop_equal_estimates() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(scan("a", 500))
            .with_node(scan("b", 500))
            .with_node(inner_join("j", "a", "b"));

        let result = JoinReorderRule.apply(&plan);
        assert!(result.is_none(), "equal estimates → no change");
    }

    // ── Swap cases ────────────────────────────────────────────────────────────

    #[test]
    fn join_reorder_swaps_when_right_is_smaller() {
        // large(1000) on left, small(10) on right → swap so small is on left
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(scan("large", 1000))
            .with_node(scan("small", 10))
            .with_node(inner_join("j", "large", "small"));

        let result = JoinReorderRule.apply(&plan).expect("should swap");
        let join_node = result.nodes().iter().find(|n| n.id() == "j").unwrap();
        assert_eq!(
            join_node.inputs()[0],
            "small",
            "small table must be on left"
        );
        assert_eq!(
            join_node.inputs()[1],
            "large",
            "large table must be on right"
        );
    }

    #[test]
    fn join_reorder_cross_join_also_swapped() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(scan("big", 5000))
            .with_node(scan("tiny", 5))
            .with_node(join_with_type("j", "big", "tiny", JoinType::Cross));

        let result = JoinReorderRule
            .apply(&plan)
            .expect("cross join should swap");
        let join_node = result.nodes().iter().find(|n| n.id() == "j").unwrap();
        assert_eq!(join_node.inputs()[0], "tiny");
        assert_eq!(join_node.inputs()[1], "big");
    }

    // ── Non-commutative joins must not be reordered ───────────────────────────

    #[test]
    fn join_reorder_left_join_not_swapped() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(scan("big", 5000))
            .with_node(scan("small", 5))
            .with_node(join_with_type("j", "big", "small", JoinType::Left));

        let result = JoinReorderRule.apply(&plan);
        assert!(result.is_none(), "left join must not be reordered");
    }

    #[test]
    fn join_reorder_right_join_not_swapped() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(scan("big", 5000))
            .with_node(scan("small", 5))
            .with_node(join_with_type("j", "big", "small", JoinType::Right));

        let result = JoinReorderRule.apply(&plan);
        assert!(result.is_none(), "right join must not be reordered");
    }

    #[test]
    fn join_reorder_semi_join_not_swapped() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(scan("big", 5000))
            .with_node(scan("small", 5))
            .with_node(join_with_type("j", "big", "small", JoinType::Semi));

        let result = JoinReorderRule.apply(&plan);
        assert!(result.is_none(), "semi join must not be reordered");
    }

    #[test]
    fn join_reorder_anti_join_not_swapped() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(scan("big", 5000))
            .with_node(scan("small", 5))
            .with_node(join_with_type("j", "big", "small", JoinType::Anti));

        let result = JoinReorderRule.apply(&plan);
        assert!(result.is_none(), "anti join must not be reordered");
    }

    #[test]
    fn join_reorder_full_join_not_swapped() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(scan("big", 5000))
            .with_node(scan("small", 5))
            .with_node(join_with_type("j", "big", "small", JoinType::Full));

        let result = JoinReorderRule.apply(&plan);
        assert!(result.is_none(), "full join must not be reordered");
    }

    // ── Multi-join plan ───────────────────────────────────────────────────────

    #[test]
    fn join_reorder_reorders_multiple_joins_in_one_pass() {
        // Three tables: a=100, b=10000, c=50
        // joins: j1 = (a JOIN b), j2 = (j1 JOIN c)
        // After rule:
        //   j1: a(100) < b(10000) → already correct (no swap in j1)
        //   j2: j1 has an estimated_rows from join estimate; c=50
        //       If j1.estimated_rows > 50 then c goes to left
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(scan("a", 100))
            .with_node(scan("b", 10_000))
            .with_node(inner_join("j1", "a", "b"))
            .with_node(scan("c", 50))
            .with_node(inner_join("j2", "j1", "c"));

        // j1 has no estimated_rows set → no swap in j2 based on j1
        // c(50) on right, j1(unknown) on left → right has a known estimate (50)
        // left has unknown estimate → left_rows = u64::MAX → right < left → swap
        let result = JoinReorderRule.apply(&plan).expect("should swap j2");
        let j2 = result.nodes().iter().find(|n| n.id() == "j2").unwrap();
        // c (right, rows=50) should have moved to left since j1 has no estimate (→ u64::MAX)
        assert_eq!(j2.inputs()[0], "c");
        assert_eq!(j2.inputs()[1], "j1");
    }

    #[test]
    fn join_reorder_result_plan_is_valid() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(scan("big", 5000))
            .with_node(scan("small", 5))
            .with_node(inner_join("j", "big", "small"));

        let result = JoinReorderRule.apply(&plan).expect("should swap");
        result.validate().expect("reordered plan must be valid");
    }

    #[test]
    fn join_reorder_only_right_estimate_treated_as_smaller() {
        // left has no estimate (→ u64::MAX), right has estimate 100
        // u64::MAX vs 100 → right is much smaller → swap
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(scan_no_estimate("a"))
            .with_node(scan("b", 100))
            .with_node(inner_join("j", "a", "b"));

        let result = JoinReorderRule.apply(&plan).expect("should swap");
        let j = result.nodes().iter().find(|n| n.id() == "j").unwrap();
        assert_eq!(j.inputs()[0], "b");
        assert_eq!(j.inputs()[1], "a");
    }
}
