#![forbid(unsafe_code)]

//! Query optimizer traits and infrastructure for Krishiv.
//!
//! This crate defines the rule-based optimizer framework used by both the
//! logical and physical planning pipelines, as well as the AQE (Adaptive
//! Query Execution) extension traits that operate on runtime statistics
//! collected during stage execution.

use krishiv_plan::{LogicalPlan, PhysicalPlan};

// ── Cost model ────────────────────────────────────────────────────────────────

/// Estimated cost of executing a plan.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Cost {
    /// Estimated CPU time in nanoseconds.
    pub cpu_nanos: u64,
    /// Estimated peak memory in bytes.
    pub memory_bytes: u64,
    /// Estimated bytes transferred over the network.
    pub network_bytes: u64,
}

/// Runtime statistics collected by an executor stage.
///
/// These are fed back into AQE rules so the optimizer can re-plan in-flight.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeStats {
    /// Number of input rows processed.
    pub input_rows: u64,
    /// Number of output rows produced.
    pub output_rows: u64,
    /// Actual CPU time consumed in nanoseconds.
    pub cpu_nanos: u64,
    /// Peak memory used in bytes.
    pub memory_bytes: u64,
    /// Bytes spilled to disk.
    pub spill_bytes: u64,
}

// ── Optimizer traits ──────────────────────────────────────────────────────────

/// Estimates the cost of executing a [`LogicalPlan`].
pub trait CostModel: Send + Sync {
    /// Return an estimated [`Cost`] for the given logical plan.
    fn estimate(&self, plan: &LogicalPlan) -> Cost;
}

/// A rule that transforms a [`LogicalPlan`] into a (possibly better) one.
pub trait OptimizerRule: Send + Sync {
    /// Short, stable rule name used in explain and diagnostics output.
    fn name(&self) -> &str;

    /// Apply the rule to `plan` and return the (possibly unchanged) result.
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan;
}

/// An Adaptive Query Execution rule that re-plans based on [`RuntimeStats`].
///
/// AQE rules receive the current [`PhysicalPlan`] together with per-stage
/// runtime statistics and may return a re-optimised physical plan.
pub trait AqeRule: Send + Sync {
    /// Short, stable rule name used in explain and diagnostics output.
    fn name(&self) -> &str;

    /// Apply the AQE rule given collected [`RuntimeStats`] for each stage.
    fn apply(&self, plan: PhysicalPlan, stats: &[RuntimeStats]) -> PhysicalPlan;
}

/// A rule that applies streaming-specific rewrites to a [`LogicalPlan`].
pub trait StreamRule: Send + Sync {
    /// Short, stable rule name used in explain and diagnostics output.
    fn name(&self) -> &str;

    /// Apply the streaming rewrite to `plan`.
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan;
}

/// A rule that detects skewed (hot) partitions from [`RuntimeStats`].
///
/// Returns the indices of partitions whose row count or resource usage
/// significantly exceeds the average, signalling that the coordinator should
/// split or re-balance those partitions.
pub trait SkewRule: Send + Sync {
    /// Short, stable rule name used in explain and diagnostics output.
    fn name(&self) -> &str;

    /// Return the indices of hot partitions detected in `stats`.
    fn detect_hot_partitions(&self, stats: &[RuntimeStats]) -> Vec<usize>;
}

// ── Optimizer ─────────────────────────────────────────────────────────────────

/// The result of running the optimizer over a logical plan.
#[derive(Debug, Clone)]
pub struct OptimizeResult {
    /// The (possibly rewritten) logical plan.
    pub plan: LogicalPlan,
    /// Names of the rules that fired and changed the plan, in application order.
    pub applied_rules: Vec<String>,
}

impl OptimizeResult {
    /// Return a human-readable summary of which rules fired.
    pub fn describe(&self) -> String {
        if self.applied_rules.is_empty() {
            return "optimizer: no rules applied".to_string();
        }
        let rules = self.applied_rules.join(", ");
        format!("optimizer applied: {rules}")
    }
}

/// Rule-based optimizer for Krishiv logical plans.
///
/// Rules are applied in the order they were added. Each rule receives the plan
/// produced by the previous rule. If a rule does not change the plan it should
/// return the input unchanged; the optimizer detects changes via [`PartialEq`]
/// and only records a rule in [`OptimizeResult::applied_rules`] when it
/// actually modifies the plan.
pub struct Optimizer {
    rules: Vec<Box<dyn OptimizerRule>>,
}

impl Optimizer {
    /// Create an optimizer with no rules.
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Append a rule to the optimizer pipeline.
    pub fn add_rule(&mut self, rule: Box<dyn OptimizerRule>) {
        self.rules.push(rule);
    }

    /// Run all rules in order and return the final plan together with the list
    /// of rules that produced a visible change.
    pub fn optimize(&self, plan: LogicalPlan) -> OptimizeResult {
        let mut current = plan;
        let mut applied_rules = Vec::new();

        for rule in &self.rules {
            let before = current.clone();
            current = rule.apply(current);
            if current != before {
                applied_rules.push(rule.name().to_string());
            }
        }

        OptimizeResult {
            plan: current,
            applied_rules,
        }
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

// ── ThresholdSkewRule ─────────────────────────────────────────────────────────

/// Detects hot partitions whose `input_rows` exceeds `threshold × median_rows`.
pub struct ThresholdSkewRule {
    threshold: f64,
}

impl ThresholdSkewRule {
    /// Create a rule that flags partitions exceeding `threshold × median` input rows.
    ///
    /// Typical value: 2.0 (flag anything more than 2× the median).
    pub fn new(threshold: f64) -> Self {
        Self { threshold }
    }

    fn median_rows(stats: &[RuntimeStats]) -> f64 {
        if stats.is_empty() {
            return 0.0;
        }
        let mut rows: Vec<u64> = stats.iter().map(|s| s.input_rows).collect();
        rows.sort_unstable();
        let mid = (rows.len() - 1) / 2;
        rows[mid] as f64
    }
}

impl SkewRule for ThresholdSkewRule {
    fn name(&self) -> &str {
        "threshold-skew"
    }

    fn detect_hot_partitions(&self, stats: &[RuntimeStats]) -> Vec<usize> {
        if stats.is_empty() {
            return Vec::new();
        }
        let median = Self::median_rows(stats);
        if median == 0.0 {
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

// ── CoalesceRule ──────────────────────────────────────────────────────────────

/// Advice returned by the coalesce rule: which partition indices should be merged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoalesceAdvice {
    /// Groups of partition indices to merge. Each inner `Vec` is one merged partition.
    pub groups: Vec<Vec<usize>>,
}

/// Merges partitions whose `memory_bytes` falls below `min_partition_bytes`.
pub struct CoalesceRule {
    min_partition_bytes: u64,
}

impl CoalesceRule {
    /// Create a new `CoalesceRule` with the given minimum partition byte threshold.
    pub fn new(min_partition_bytes: u64) -> Self {
        Self {
            min_partition_bytes,
        }
    }

    /// Compute coalesce advice from per-partition stats, without modifying the plan.
    ///
    /// Groups consecutive partitions whose `memory_bytes < min_partition_bytes`
    /// into single groups. Non-small partitions are singleton groups.
    ///
    /// Example: `[small, small, big, small]` → `[[0,1], [2], [3]]`
    pub fn advise(&self, stats: &[RuntimeStats]) -> CoalesceAdvice {
        if stats.is_empty() {
            return CoalesceAdvice { groups: Vec::new() };
        }

        let mut groups: Vec<Vec<usize>> = Vec::new();
        let mut current_small: Vec<usize> = Vec::new();

        for (i, s) in stats.iter().enumerate() {
            if s.memory_bytes < self.min_partition_bytes {
                current_small.push(i);
            } else {
                if !current_small.is_empty() {
                    groups.push(current_small.clone());
                    current_small.clear();
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

    /// Returns the plan unchanged; coalescing is advisory (see `advise`).
    fn apply(&self, plan: PhysicalPlan, _stats: &[RuntimeStats]) -> PhysicalPlan {
        plan
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use krishiv_plan::{ExecutionKind, LogicalPlan, PlanNode};

    use super::{Optimizer, OptimizerRule, RuntimeStats};

    fn empty_plan() -> LogicalPlan {
        LogicalPlan::new("test", ExecutionKind::Batch)
    }

    fn plan_with_node() -> LogicalPlan {
        LogicalPlan::new("test", ExecutionKind::Batch).with_node(PlanNode::new(
            "scan",
            "scan t",
            ExecutionKind::Batch,
        ))
    }

    // ── no-rules optimizer ────────────────────────────────────────────────

    #[test]
    fn optimizer_no_rules_is_noop() {
        let optimizer = Optimizer::new();
        let plan = plan_with_node();
        let result = optimizer.optimize(plan.clone());

        assert_eq!(result.plan, plan);
        assert!(result.applied_rules.is_empty());
    }

    #[test]
    fn optimizer_default_is_noop() {
        let optimizer = Optimizer::default();
        let plan = empty_plan();
        let result = optimizer.optimize(plan.clone());

        assert_eq!(result.plan, plan);
        assert!(result.applied_rules.is_empty());
    }

    // ── rules that do not change the plan ─────────────────────────────────

    struct NoOpRule;

    impl OptimizerRule for NoOpRule {
        fn name(&self) -> &str {
            "no-op"
        }

        fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
            plan
        }
    }

    #[test]
    fn optimizer_noop_rule_produces_empty_applied_rules() {
        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(NoOpRule));

        let plan = plan_with_node();
        let result = optimizer.optimize(plan.clone());

        assert_eq!(result.plan, plan);
        assert!(
            result.applied_rules.is_empty(),
            "no-op rule must not appear in applied_rules"
        );
    }

    // ── rules that change the plan ────────────────────────────────────────

    struct AddNodeRule;

    impl OptimizerRule for AddNodeRule {
        fn name(&self) -> &str {
            "add-node"
        }

        fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
            plan.with_node(PlanNode::new("extra", "extra node", ExecutionKind::Batch))
        }
    }

    #[test]
    fn optimizer_rule_that_changes_plan_is_recorded() {
        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(AddNodeRule));

        let result = optimizer.optimize(empty_plan());

        assert_eq!(result.applied_rules, vec!["add-node"]);
        assert_eq!(result.plan.nodes().len(), 1);
    }

    #[test]
    fn optimizer_multiple_rules_only_records_changed_ones() {
        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(NoOpRule));
        optimizer.add_rule(Box::new(AddNodeRule));
        optimizer.add_rule(Box::new(NoOpRule));

        let result = optimizer.optimize(empty_plan());

        assert_eq!(result.applied_rules, vec!["add-node"]);
    }

    // ── OptimizeResult::describe ──────────────────────────────────────────

    #[test]
    fn optimize_result_describe_no_rules() {
        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(NoOpRule));

        let result = optimizer.optimize(empty_plan());
        assert_eq!(result.describe(), "optimizer: no rules applied");
    }

    #[test]
    fn optimize_result_describe_with_rules() {
        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(AddNodeRule));

        let result = optimizer.optimize(empty_plan());
        assert_eq!(result.describe(), "optimizer applied: add-node");
    }

    #[test]
    fn optimize_result_describe_multiple_applied_rules() {
        struct AnotherRule;
        impl OptimizerRule for AnotherRule {
            fn name(&self) -> &str {
                "another-rule"
            }
            fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
                plan.with_node(PlanNode::new("x", "x", ExecutionKind::Batch))
            }
        }

        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(AddNodeRule));
        optimizer.add_rule(Box::new(AnotherRule));

        let result = optimizer.optimize(empty_plan());
        assert!(result.describe().contains("add-node"));
        assert!(result.describe().contains("another-rule"));
    }

    // ── RuntimeStats ─────────────────────────────────────────────────────

    #[test]
    fn runtime_stats_default_is_zero() {
        let stats = RuntimeStats::default();
        assert_eq!(stats.input_rows, 0);
        assert_eq!(stats.output_rows, 0);
        assert_eq!(stats.cpu_nanos, 0);
        assert_eq!(stats.memory_bytes, 0);
        assert_eq!(stats.spill_bytes, 0);
    }

    // ── ThresholdSkewRule ─────────────────────────────────────────────────

    use super::{CoalesceRule, SkewRule, ThresholdSkewRule};

    fn make_stats_with_rows(input_rows: &[u64]) -> Vec<RuntimeStats> {
        input_rows
            .iter()
            .map(|&r| RuntimeStats {
                input_rows: r,
                ..Default::default()
            })
            .collect()
    }

    fn make_stats_with_memory(memory_bytes: &[u64]) -> Vec<RuntimeStats> {
        memory_bytes
            .iter()
            .map(|&m| RuntimeStats {
                memory_bytes: m,
                ..Default::default()
            })
            .collect()
    }

    #[test]
    fn skew_rule_empty_stats_no_hot_partitions() {
        let rule = ThresholdSkewRule::new(2.0);
        assert!(rule.detect_hot_partitions(&[]).is_empty());
    }

    #[test]
    fn skew_rule_all_equal_no_hot_partitions() {
        let stats = make_stats_with_rows(&[100, 100, 100]);
        let rule = ThresholdSkewRule::new(2.0);
        assert!(rule.detect_hot_partitions(&stats).is_empty());
    }

    #[test]
    fn skew_rule_one_hot_partition_detected() {
        // partitions: 10, 10, 100 — median is 10, threshold=2.0 → 100 > 20 → hot
        let stats = make_stats_with_rows(&[10, 10, 100]);
        let rule = ThresholdSkewRule::new(2.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert_eq!(hot, vec![2]);
    }

    #[test]
    fn skew_rule_threshold_boundary_not_flagged() {
        // median=10, threshold=2.0, value=20 → 20 is NOT strictly greater than 2*10=20
        let stats = make_stats_with_rows(&[10, 10, 20]);
        let rule = ThresholdSkewRule::new(2.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert!(hot.is_empty(), "exact boundary should not be flagged");
    }

    // ── CoalesceRule ──────────────────────────────────────────────────────

    #[test]
    fn coalesce_all_small_in_one_group() {
        let stats = make_stats_with_memory(&[100, 200, 50]);
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&stats);
        assert_eq!(advice.groups, vec![vec![0, 1, 2]]);
    }

    #[test]
    fn coalesce_all_large_singleton_groups() {
        let stats = make_stats_with_memory(&[2000, 3000, 5000]);
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&stats);
        assert_eq!(advice.groups, vec![vec![0], vec![1], vec![2]]);
    }

    #[test]
    fn coalesce_mixed_groups_correctly() {
        // [small, small, big, small] → [[0,1], [2], [3]]
        let stats = make_stats_with_memory(&[100, 200, 5000, 300]);
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&stats);
        assert_eq!(advice.groups, vec![vec![0, 1], vec![2], vec![3]]);
    }

    #[test]
    fn coalesce_empty_stats_empty_groups() {
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&[]);
        assert_eq!(advice.groups, Vec::<Vec<usize>>::new());
    }
}
