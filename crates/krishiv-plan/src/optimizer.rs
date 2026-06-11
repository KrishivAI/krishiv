#![forbid(unsafe_code)]

//! Query optimizer traits and infrastructure for Krishiv.
//!
//! This crate defines the rule-based optimizer framework used by both the
//! logical and physical planning pipelines, as well as the AQE (Adaptive
//! Query Execution) extension traits that operate on runtime statistics
//! collected during stage execution.

mod auto_partition;
mod broadcast;
mod coalesce;
mod predicate_pushdown;
mod small_file;

pub use auto_partition::AutoPartitionRule;
pub use broadcast::{BroadcastAutoRule, DEFAULT_BROADCAST_THRESHOLD_ROWS};
pub use coalesce::{CoalesceAdvice, CoalesceRule};
pub use predicate_pushdown::PredicatePushdownRule;
pub use small_file::{FileStats, SmallFilePlanner, SplitPlanAdvice};

use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::{ExecutionKind, LogicalPlan, NodeOp, PhysicalPlan, PlanError};

/// Result type for logical and adaptive optimizer pipelines.
pub type OptimizerResult<T> = Result<T, OptimizerError>;

/// Errors produced while validating or executing optimizer rules.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OptimizerError {
    /// The optimizer received a malformed input plan.
    #[error("invalid {optimizer} optimizer input: {source}")]
    InvalidInput {
        optimizer: &'static str,
        #[source]
        source: PlanError,
    },
    /// A rule returned a malformed output plan.
    #[error("{optimizer} optimizer rule '{rule}' produced an invalid plan: {source}")]
    InvalidRuleOutput {
        optimizer: &'static str,
        rule: String,
        #[source]
        source: PlanError,
    },
    /// A rule panicked while processing a plan.
    #[error("{optimizer} optimizer rule '{rule}' panicked: {message}")]
    RulePanicked {
        optimizer: &'static str,
        rule: String,
        message: String,
    },
}

// ── Cost model ────────────────────────────────────────────────────────────────

/// Estimated cost of executing a plan.
#[non_exhaustive]
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
#[non_exhaustive]
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
    /// Actual bytes written to the shuffle store (Arrow IPC / Parquet on disk).
    ///
    /// When non-zero, AQE rules prefer this over `memory_bytes` for partition
    /// sizing, because shuffle output is compressed/serialized and therefore a
    /// more accurate proxy for network and disk cost. `memory_bytes` is the
    /// peak in-memory footprint, which can be 2–4× larger than the wire size.
    /// Zero means the value was not collected (older task builds or non-shuffle
    /// tasks), and the rule falls back to `memory_bytes`.
    pub serialized_bytes: u64,
}

// ── Optimizer traits ──────────────────────────────────────────────────────────

/// Estimates the cost of executing a [`LogicalPlan`].
pub trait CostModel: Send + Sync {
    /// Return an estimated [`Cost`] for the given logical plan.
    fn estimate(&self, plan: &LogicalPlan) -> Cost;
}

/// Static, row-count-aware cost model for logical plans.
///
/// Walks every node in the plan and accumulates cost estimates based on
/// operator type and the node's `estimated_rows` field.  When `estimated_rows`
/// is `None` a conservative default of 10 000 rows is assumed.
///
/// ## Per-node coefficients
///
/// | Operator     | CPU (ns/row) | Memory (B/row) | Network (B/row) |
/// |--------------|:------------:|:--------------:|:---------------:|
/// | Scan         | 10           | 64             | 0               |
/// | Filter       | 5            | 0              | 0               |
/// | Project      | 2            | 0              | 0               |
/// | Aggregate    | 50           | 200            | 0               |
/// | Join         | 100          | 100            | 0               |
/// | Exchange     | 20           | 0              | 200             |
/// | Other/Window | 15           | 64             | 0               |
///
/// These figures are deliberately simple and tunable; their absolute values
/// are less important than their relative ordering (Aggregate > Join > …).
pub struct StaticCostModel;

impl CostModel for StaticCostModel {
    fn estimate(&self, plan: &LogicalPlan) -> Cost {
        const DEFAULT_ROWS: u64 = 10_000;
        let mut cpu_nanos: u64 = 0;
        let mut memory_bytes: u64 = 0;
        let mut network_bytes: u64 = 0;

        for node in plan.nodes() {
            let rows = node.estimated_rows().unwrap_or(DEFAULT_ROWS);
            match node.op() {
                Some(NodeOp::Scan { .. }) => {
                    cpu_nanos = cpu_nanos.saturating_add(rows.saturating_mul(10));
                    memory_bytes = memory_bytes.saturating_add(rows.saturating_mul(64));
                }
                Some(NodeOp::Filter { .. }) => {
                    cpu_nanos = cpu_nanos.saturating_add(rows.saturating_mul(5));
                }
                Some(NodeOp::Project { .. }) => {
                    cpu_nanos = cpu_nanos.saturating_add(rows.saturating_mul(2));
                }
                Some(NodeOp::Aggregate { .. }) => {
                    cpu_nanos = cpu_nanos.saturating_add(rows.saturating_mul(50));
                    memory_bytes = memory_bytes.saturating_add(rows.saturating_mul(200));
                }
                Some(NodeOp::Join { .. }) => {
                    cpu_nanos = cpu_nanos.saturating_add(rows.saturating_mul(100));
                    memory_bytes = memory_bytes.saturating_add(rows.saturating_mul(100));
                }
                Some(NodeOp::Exchange { .. }) => {
                    cpu_nanos = cpu_nanos.saturating_add(rows.saturating_mul(20));
                    network_bytes = network_bytes.saturating_add(rows.saturating_mul(200));
                }
                _ => {
                    cpu_nanos = cpu_nanos.saturating_add(rows.saturating_mul(15));
                    memory_bytes = memory_bytes.saturating_add(rows.saturating_mul(64));
                }
            }
        }

        Cost {
            cpu_nanos,
            memory_bytes,
            network_bytes,
        }
    }
}

/// A rule that transforms a [`LogicalPlan`] into a (possibly better) one.
///
/// P2.4: `apply` returns `Option<LogicalPlan>` — `None` means the plan is
/// unchanged, allowing [`Optimizer`] to skip the clone-and-compare cycle
/// and to record only rules that actually fired.
pub trait OptimizerRule: Send + Sync {
    /// Short, stable rule name used in explain and diagnostics output.
    fn name(&self) -> &str;

    /// Apply the rule to `plan`.
    ///
    /// Return `Some(new_plan)` when the rule rewrites the plan, or `None` when
    /// the plan is unchanged.  Returning `None` is more efficient than returning
    /// a clone of the original plan unchanged.
    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan>;
}

/// An Adaptive Query Execution rule that re-plans based on [`RuntimeStats`].
///
/// AQE rules receive the current [`PhysicalPlan`] together with per-stage
/// runtime statistics and may return a re-optimised physical plan.
pub trait AqeRule: Send + Sync {
    /// Short, stable rule name used in explain and diagnostics output.
    fn name(&self) -> &str;

    /// Apply the AQE rule given collected [`RuntimeStats`] for each stage.
    ///
    /// Return `Some(new_plan)` when the rule rewrites the plan, or `None` when
    /// the plan is unchanged.  The rule borrows the plan; clone it internally
    /// only when a rewrite is needed so non-firing rules pay no clone cost.
    fn apply(&self, plan: &PhysicalPlan, stats: &[RuntimeStats]) -> Option<PhysicalPlan>;
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
    ///
    /// P2.4: rules signal no-change by returning `None`, avoiding an O(rules ×
    /// plan_size) clone-per-rule cycle.
    pub fn optimize(&self, plan: LogicalPlan) -> OptimizerResult<OptimizeResult> {
        plan.validate()
            .map_err(|source| OptimizerError::InvalidInput {
                optimizer: "logical",
                source,
            })?;
        let mut current = plan;
        let mut applied_rules = Vec::new();

        for rule in &self.rules {
            let rule_name = rule.name().to_string();
            let outcome =
                catch_unwind(AssertUnwindSafe(|| rule.apply(&current))).map_err(|payload| {
                    OptimizerError::RulePanicked {
                        optimizer: "logical",
                        rule: rule_name.clone(),
                        message: krishiv_common::panic_payload_to_string(&*payload),
                    }
                })?;
            if let Some(new_plan) = outcome {
                if new_plan.name() != current.name() || new_plan.kind() != current.kind() {
                    return Err(OptimizerError::InvalidRuleOutput {
                        optimizer: "logical",
                        rule: rule_name,
                        source: PlanError::Validation(String::from(
                            "logical optimizer rules must preserve plan name and execution kind",
                        )),
                    });
                }
                new_plan
                    .validate()
                    .map_err(|source| OptimizerError::InvalidRuleOutput {
                        optimizer: "logical",
                        rule: rule_name.clone(),
                        source,
                    })?;
                if new_plan != current {
                    applied_rules.push(rule_name);
                    current = new_plan;
                }
            }
        }

        Ok(OptimizeResult {
            plan: current,
            applied_rules,
        })
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

    /// P1.16: For even-length arrays, average the two middle values.
    fn median_rows(stats: &[RuntimeStats]) -> f64 {
        if stats.is_empty() {
            return 0.0;
        }
        let mut rows: Vec<u64> = stats.iter().map(|s| s.input_rows).collect();
        rows.sort_unstable();
        let n = rows.len();
        let mid = n / 2;
        if n.is_multiple_of(2) {
            (rows[mid - 1] as f64 + rows[mid] as f64) / 2.0
        } else {
            rows[mid] as f64
        }
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
        stats
            .iter()
            .enumerate()
            .filter(|(_, s)| s.input_rows as f64 > self.threshold * median)
            .map(|(i, _)| i)
            .collect()
    }
}

// ── StreamingAqeGuard ─────────────────────────────────────────────────────────

/// Guards streaming plans from AQE rules that would change partition count.
///
/// Stateful streaming stages use keyed-distribution routing: the same key must
/// always map to the same executor task for the entire job lifetime.  AQE
/// coalescing and repartitioning would change the partition count mid-job,
/// orphaning all in-flight state.
///
/// Place this rule first in any AQE pipeline that includes coalescing or
/// repartitioning rules.  When the plan carries `ExecutionKind::Streaming`,
/// all subsequent AQE rules that affect partitioning must be skipped.
///
/// Usage:
/// ```
/// use krishiv_plan::optimizer::{AqeOptimizer, CoalesceRule, StreamingAqeGuard};
/// let mut aqe = AqeOptimizer::new();
/// aqe.add_guarded_rule(Box::new(CoalesceRule::new(64 * 1024 * 1024)));
/// ```
pub struct StreamingAqeGuard;

impl StreamingAqeGuard {
    /// Returns `true` if the plan contains any streaming node that must not be
    /// subject to AQE partition-count changes.
    ///
    /// P3.18: Walk the plan tree recursively so that hybrid batch/streaming
    /// plans are also detected.  A plan is considered streaming if either its
    /// top-level `ExecutionKind` is `Streaming` or any of its nodes carries
    /// `ExecutionKind::Streaming`.
    pub fn plan_is_streaming(plan: &PhysicalPlan) -> bool {
        plan.kind() == ExecutionKind::Streaming
            || plan
                .nodes()
                .iter()
                .any(|node| node.kind() == ExecutionKind::Streaming)
    }
}

/// AQE optimizer that automatically skips partition-changing rules for
/// streaming plans.
///
/// Rules added via [`add_guarded_rule`](AqeOptimizer::add_guarded_rule) are
/// not applied when [`StreamingAqeGuard::plan_is_streaming`] returns `true`.
/// Rules added via [`add_rule`](AqeOptimizer::add_rule) always run regardless
/// of execution kind — use this for rules that are safe on streaming plans
/// (e.g., pure statistics collection).
pub struct AqeOptimizer {
    /// Rules that run on all plans, including streaming.
    always_rules: Vec<Box<dyn AqeRule>>,
    /// Rules that are skipped for streaming plans.
    guarded_rules: Vec<Box<dyn AqeRule>>,
    /// Cost model for cold-start estimation when `RuntimeStats` are absent.
    ///
    /// When `stats` passed to `apply` is empty (first execution cycle), the
    /// optimizer uses this model to estimate memory cost from the logical plan
    /// and synthesises a single `RuntimeStats` entry so that `AutoPartitionRule`
    /// can propose an initial partition count rather than defaulting to the plan's
    /// current value.  Defaults to [`StaticCostModel`].
    cost_model: std::sync::Arc<dyn CostModel>,
}

impl AqeOptimizer {
    /// Create an empty AQE optimizer backed by [`StaticCostModel`].
    pub fn new() -> Self {
        Self {
            always_rules: Vec::new(),
            guarded_rules: Vec::new(),
            cost_model: std::sync::Arc::new(StaticCostModel),
        }
    }

    /// Replace the default cost model with a custom implementation.
    pub fn with_cost_model(mut self, model: std::sync::Arc<dyn CostModel>) -> Self {
        self.cost_model = model;
        self
    }

    /// Add a rule that always runs, including on streaming plans.
    pub fn add_rule(&mut self, rule: Box<dyn AqeRule>) {
        self.always_rules.push(rule);
    }

    /// Add a rule that is skipped when the plan is a streaming plan.
    ///
    /// Use this for coalescing, repartitioning, and any other AQE rule that
    /// changes partition count or assignment.
    pub fn add_guarded_rule(&mut self, rule: Box<dyn AqeRule>) {
        self.guarded_rules.push(rule);
    }

    /// Apply all applicable rules given per-stage runtime statistics.
    ///
    /// When `stats` is empty the cost model is used to synthesise a single
    /// `RuntimeStats` entry (from the logical plan cost estimate) so that rules
    /// that need size information can still make a first-pass decision.
    ///
    /// Returns the (possibly rewritten) plan and the names of rules that fired.
    pub fn apply(
        &self,
        plan: PhysicalPlan,
        stats: &[RuntimeStats],
    ) -> OptimizerResult<(PhysicalPlan, Vec<String>)> {
        plan.validate()
            .map_err(|source| OptimizerError::InvalidInput {
                optimizer: "AQE",
                source,
            })?;
        let input_is_streaming = StreamingAqeGuard::plan_is_streaming(&plan);
        let mut current = plan;
        let mut applied = Vec::new();

        // When no runtime stats are available (cold start), synthesise a single
        // RuntimeStats entry from the cost model estimate so rules that need
        // size information can propose an initial partition count.
        //
        // PhysicalPlan does not carry a back-reference to the original
        // LogicalPlan, but its PlanNodes expose the same `estimated_rows()`
        // and `op()` accessors that StaticCostModel uses.  We build a
        // ephemeral LogicalPlan that mirrors the physical nodes so the cost
        // model can walk them without requiring a separate logical plan to be
        // threaded through the call stack.
        let cost_synthesised_stats: Vec<RuntimeStats>;
        let effective_stats = if stats.is_empty() && !input_is_streaming {
            let mut lplan = crate::LogicalPlan::new(current.name(), current.kind());
            for node in current.nodes() {
                lplan.add_node(node.clone());
            }
            let cost = self.cost_model.estimate(&lplan);
            cost_synthesised_stats = vec![RuntimeStats {
                memory_bytes: cost.memory_bytes,
                cpu_nanos: cost.cpu_nanos,
                ..Default::default()
            }];
            &cost_synthesised_stats[..]
        } else {
            stats
        };

        for rule in &self.always_rules {
            let rule_name = rule.name().to_string();
            let outcome = catch_unwind(AssertUnwindSafe(|| rule.apply(&current, effective_stats))).map_err(
                |payload| OptimizerError::RulePanicked {
                    optimizer: "AQE",
                    rule: rule_name.clone(),
                    message: krishiv_common::panic_payload_to_string(&*payload),
                },
            )?;
            if let Some(new_plan) = outcome {
                if new_plan.name() != current.name() || new_plan.kind() != current.kind() {
                    return Err(OptimizerError::InvalidRuleOutput {
                        optimizer: "AQE",
                        rule: rule_name,
                        source: PlanError::Validation(String::from(
                            "AQE rules must preserve plan name and execution kind",
                        )),
                    });
                }
                new_plan
                    .validate()
                    .map_err(|source| OptimizerError::InvalidRuleOutput {
                        optimizer: "AQE",
                        rule: rule_name.clone(),
                        source,
                    })?;
                if new_plan != current {
                    applied.push(rule_name);
                    current = new_plan;
                }
            }
        }

        if !input_is_streaming && !StreamingAqeGuard::plan_is_streaming(&current) {
            for rule in &self.guarded_rules {
                let rule_name = rule.name().to_string();
                let outcome = catch_unwind(AssertUnwindSafe(|| rule.apply(&current, effective_stats)))
                    .map_err(|payload| OptimizerError::RulePanicked {
                        optimizer: "AQE",
                        rule: rule_name.clone(),
                        message: krishiv_common::panic_payload_to_string(&*payload),
                    })?;
                if let Some(new_plan) = outcome {
                    if new_plan.name() != current.name() || new_plan.kind() != current.kind() {
                        return Err(OptimizerError::InvalidRuleOutput {
                            optimizer: "AQE",
                            rule: rule_name,
                            source: PlanError::Validation(String::from(
                                "AQE rules must preserve plan name and execution kind",
                            )),
                        });
                    }
                    new_plan
                        .validate()
                        .map_err(|source| OptimizerError::InvalidRuleOutput {
                            optimizer: "AQE",
                            rule: rule_name.clone(),
                            source,
                        })?;
                    if new_plan != current {
                        applied.push(rule_name);
                        current = new_plan;
                    }
                }
            }
        }

        Ok((current, applied))
    }
}

impl Default for AqeOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

pub fn default_logical_optimizer() -> Optimizer {
    let mut optimizer = Optimizer::new();
    optimizer.add_rule(Box::new(BroadcastAutoRule::new(
        DEFAULT_BROADCAST_THRESHOLD_ROWS,
    )));
    optimizer.add_rule(Box::new(PredicatePushdownRule));
    optimizer
}

/// Default AQE optimizer with guarded coalescing and the streaming guard.
///
/// Includes `CoalesceRule` as a guarded rule (skipped for streaming plans).
/// Rules that require runtime statistics will be no-ops until stats feed
/// is wired (see `AqeOptimizer::apply`).
pub fn default_aqe_optimizer() -> AqeOptimizer {
    let mut optimizer = AqeOptimizer::new();
    optimizer.add_guarded_rule(Box::new(AutoPartitionRule::new(64)));
    optimizer.add_guarded_rule(Box::new(CoalesceRule::new(64 * 1024 * 1024)));
    optimizer
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::{ExecutionKind, FieldType, LogicalPlan, NodeOp, PhysicalPlan, PlanNode};

    use super::{
        AqeOptimizer, AqeRule, AutoPartitionRule, CoalesceAdvice, CoalesceRule, Cost, Optimizer,
        OptimizerError, OptimizerRule, RuntimeStats, SmallFilePlanner, SplitPlanAdvice,
        StreamingAqeGuard, default_logical_optimizer,
    };
    use crate::Partitioning;

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
        let result = optimizer.optimize(plan.clone()).expect("optimize");

        assert_eq!(result.plan, plan);
        assert!(result.applied_rules.is_empty());
    }

    #[test]
    fn optimizer_default_is_noop() {
        let optimizer = Optimizer::default();
        let plan = empty_plan();
        let result = optimizer.optimize(plan.clone()).expect("optimize");

        assert_eq!(result.plan, plan);
        assert!(result.applied_rules.is_empty());
    }

    // ── rules that do not change the plan ─────────────────────────────────

    struct NoOpRule;

    impl OptimizerRule for NoOpRule {
        fn name(&self) -> &str {
            "no-op"
        }

        fn apply(&self, _plan: &LogicalPlan) -> Option<LogicalPlan> {
            None
        }
    }

    #[test]
    fn optimizer_noop_rule_produces_empty_applied_rules() {
        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(NoOpRule));

        let plan = plan_with_node();
        let result = optimizer.optimize(plan.clone()).expect("optimize");

        assert_eq!(result.plan, plan);
        assert!(
            result.applied_rules.is_empty(),
            "no-op rule must not appear in applied_rules"
        );
    }

    #[test]
    fn optimizer_rejects_invalid_input_plan() {
        let optimizer = Optimizer::new();
        let invalid = LogicalPlan::new("invalid", ExecutionKind::Batch).with_node(
            PlanNode::new("sink", "sink", ExecutionKind::Batch).with_inputs(["missing"]),
        );

        let error = optimizer.optimize(invalid).expect_err("invalid input");

        assert!(matches!(
            error,
            OptimizerError::InvalidInput {
                optimizer: "logical",
                ..
            }
        ));
    }

    #[test]
    fn optimizer_rejects_invalid_rule_output() {
        struct InvalidOutputRule;
        impl OptimizerRule for InvalidOutputRule {
            fn name(&self) -> &str {
                "invalid-output"
            }

            fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
                Some(plan.clone().with_node(PlanNode::new(
                    "scan",
                    "duplicate",
                    ExecutionKind::Batch,
                )))
            }
        }

        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(InvalidOutputRule));

        let error = optimizer
            .optimize(plan_with_node())
            .expect_err("invalid rule output");

        assert!(matches!(
            error,
            OptimizerError::InvalidRuleOutput {
                optimizer: "logical",
                ref rule,
                ..
            } if rule == "invalid-output"
        ));
    }

    #[test]
    fn optimizer_contains_rule_panics() {
        struct PanickingRule;
        impl OptimizerRule for PanickingRule {
            fn name(&self) -> &str {
                "panicking"
            }

            fn apply(&self, _plan: &LogicalPlan) -> Option<LogicalPlan> {
                panic!("rule failed")
            }
        }

        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(PanickingRule));

        let error = optimizer
            .optimize(plan_with_node())
            .expect_err("panic must be contained");

        assert!(matches!(
            error,
            OptimizerError::RulePanicked {
                optimizer: "logical",
                ref rule,
                ref message,
            } if rule == "panicking" && message == "rule failed"
        ));
    }

    #[test]
    fn optimizer_rejects_rule_that_changes_plan_identity() {
        struct RenameRule;
        impl OptimizerRule for RenameRule {
            fn name(&self) -> &str {
                "rename"
            }

            fn apply(&self, _plan: &LogicalPlan) -> Option<LogicalPlan> {
                Some(LogicalPlan::new("renamed", ExecutionKind::Batch))
            }
        }

        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(RenameRule));

        let error = optimizer
            .optimize(empty_plan())
            .expect_err("identity changes must fail");

        assert!(matches!(
            error,
            OptimizerError::InvalidRuleOutput { ref rule, .. } if rule == "rename"
        ));
        assert!(error.to_string().contains("preserve plan name"));
    }

    #[test]
    fn optimizer_ignores_some_unchanged_plan() {
        struct CloneRule;
        impl OptimizerRule for CloneRule {
            fn name(&self) -> &str {
                "clone"
            }

            fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
                Some(plan.clone())
            }
        }

        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(CloneRule));
        let result = optimizer.optimize(plan_with_node()).expect("optimize");

        assert!(result.applied_rules.is_empty());
    }

    // ── rules that change the plan ────────────────────────────────────────

    struct AddNodeRule;

    impl OptimizerRule for AddNodeRule {
        fn name(&self) -> &str {
            "add-node"
        }

        fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
            Some(
                plan.clone()
                    .with_node(PlanNode::new("extra", "extra node", ExecutionKind::Batch)),
            )
        }
    }

    #[test]
    fn optimizer_rule_that_changes_plan_is_recorded() {
        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(AddNodeRule));

        let result = optimizer.optimize(empty_plan()).expect("optimize");

        assert_eq!(result.applied_rules, vec!["add-node"]);
        assert_eq!(result.plan.nodes().len(), 1);
    }

    #[test]
    fn optimizer_multiple_rules_only_records_changed_ones() {
        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(NoOpRule));
        optimizer.add_rule(Box::new(AddNodeRule));
        optimizer.add_rule(Box::new(NoOpRule));

        let result = optimizer.optimize(empty_plan()).expect("optimize");

        assert_eq!(result.applied_rules, vec!["add-node"]);
    }

    // ── OptimizeResult::describe ──────────────────────────────────────────

    #[test]
    fn optimize_result_describe_no_rules() {
        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(NoOpRule));

        let result = optimizer.optimize(empty_plan()).expect("optimize");
        assert_eq!(result.describe(), "optimizer: no rules applied");
    }

    #[test]
    fn optimize_result_describe_with_rules() {
        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(AddNodeRule));

        let result = optimizer.optimize(empty_plan()).expect("optimize");
        assert_eq!(result.describe(), "optimizer applied: add-node");
    }

    #[test]
    fn optimize_result_describe_multiple_applied_rules() {
        struct AnotherRule;
        impl OptimizerRule for AnotherRule {
            fn name(&self) -> &str {
                "another-rule"
            }
            fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
                Some(
                    plan.clone()
                        .with_node(PlanNode::new("x", "x", ExecutionKind::Batch)),
                )
            }
        }

        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(AddNodeRule));
        optimizer.add_rule(Box::new(AnotherRule));

        let result = optimizer.optimize(empty_plan()).expect("optimize");
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

    use super::{SkewRule, ThresholdSkewRule};

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

    #[test]
    fn skew_rule_even_median_handles_u64_max() {
        let stats = make_stats_with_rows(&[u64::MAX, u64::MAX]);
        let rule = ThresholdSkewRule::new(2.0);

        assert!(rule.detect_hot_partitions(&stats).is_empty());
    }

    // P1.16 — median fix for even-length arrays
    #[test]
    fn skew_rule_median_even_length_averages_two_middle_values() {
        // sorted: [10, 20, 30, 100], median = (20+30)/2 = 25
        // threshold=2.0 → hot when rows > 50; only 100 qualifies
        let stats = make_stats_with_rows(&[10, 100, 20, 30]);
        let rule = ThresholdSkewRule::new(2.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert_eq!(hot, vec![1], "only the 100-row partition should be hot");
    }

    // ── CoalesceRule ──────────────────────────────────────────────────────

    #[test]
    fn coalesce_all_small_in_one_group() {
        // After sort by memory_bytes: [2(50), 0(100), 1(200)] → all small → one group.
        let stats = make_stats_with_memory(&[100, 200, 50]);
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&stats);
        assert_eq!(advice.groups, vec![vec![2, 0, 1]]);
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
        // After sort: [0(100), 1(200), 3(300), 2(5000)].
        // Smalls (100+200+300=600 ≤ 128 MiB target) coalesce into one group;
        // big 2(5000) is a singleton. This is 2 groups vs the old 3.
        let stats = make_stats_with_memory(&[100, 200, 5000, 300]);
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&stats);
        assert_eq!(advice.groups, vec![vec![0, 1, 3], vec![2]]);
    }

    #[test]
    fn coalesce_empty_stats_empty_groups() {
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&[]);
        assert_eq!(advice.groups, Vec::<Vec<usize>>::new());
    }

    #[test]
    fn coalesce_rule_apply_reduces_200_small_partitions_to_le_10() {
        // 200 partitions each with 1 byte — all below the 128 MiB threshold.
        let stats: Vec<RuntimeStats> = (0..200)
            .map(|_| RuntimeStats {
                memory_bytes: 1,
                ..RuntimeStats::default()
            })
            .collect();
        let plan = PhysicalPlan::new("test-plan", ExecutionKind::Batch);
        let rule = CoalesceRule::new(128 * 1024 * 1024); // 128 MiB
        let result = rule.apply(&plan, &stats).expect("coalesce should fire");
        let coalesced = result
            .coalesced_partition_count()
            .expect("CoalesceRule must set coalesced_partition_count");
        // All 200 partitions are small so they collapse into one group.
        assert!(
            coalesced <= 10,
            "expected ≤ 10 partitions after coalescing, got {coalesced}"
        );
    }

    #[test]
    fn coalesce_rule_apply_does_not_stamp_when_no_coalescing_needed() {
        // All partitions are large — no coalescing.
        let stats: Vec<RuntimeStats> = (0..5)
            .map(|_| RuntimeStats {
                memory_bytes: 256 * 1024 * 1024, // 256 MiB each
                ..RuntimeStats::default()
            })
            .collect();
        let plan = PhysicalPlan::new("big-plan", ExecutionKind::Batch);
        let rule = CoalesceRule::new(128 * 1024 * 1024);
        let result = rule.apply(&plan, &stats);
        assert!(result.is_none(), "no coalescing should return None");
    }

    #[test]
    fn coalesce_rule_connects_before_terminal_sink_and_is_idempotent() {
        let plan = PhysicalPlan::new("sink-plan", ExecutionKind::Batch)
            .with_node(PlanNode::new("scan", "scan", ExecutionKind::Batch))
            .with_node(
                PlanNode::new("sink", "sink", ExecutionKind::Batch)
                    .with_inputs(["scan"])
                    .with_op(NodeOp::Sink {
                        format: "memory".to_string(),
                    }),
            );
        let stats = vec![
            RuntimeStats {
                memory_bytes: 10,
                ..RuntimeStats::default()
            },
            RuntimeStats {
                memory_bytes: 10,
                ..RuntimeStats::default()
            },
        ];
        let rule = CoalesceRule::new(100).with_target_partition_bytes(100);

        let first = rule.apply(&plan, &stats).expect("first rewrite");
        first.validate().expect("valid first rewrite");
        let coalesce = first
            .nodes()
            .iter()
            .find(|node| matches!(node.op(), Some(NodeOp::CoalescePartitions { .. })))
            .expect("coalesce node");
        let sink = first
            .nodes()
            .iter()
            .find(|node| node.id() == "sink")
            .expect("sink node");
        assert_eq!(coalesce.inputs(), &["scan"]);
        assert_eq!(sink.inputs(), &[coalesce.id()]);
        assert_eq!(first.coalesced_partition_count(), Some(1));

        let second = rule.apply(&first.clone(), &stats).expect("second rewrite");
        second.validate().expect("valid second rewrite");
        assert_eq!(second.nodes().len(), first.nodes().len());
        assert_eq!(
            second
                .nodes()
                .iter()
                .filter(|node| matches!(node.op(), Some(NodeOp::CoalescePartitions { .. })))
                .count(),
            1
        );
    }

    #[test]
    fn coalesce_rule_is_intrinsically_disabled_for_streaming() {
        let plan = PhysicalPlan::new("stream", ExecutionKind::Streaming);
        let stats = vec![
            RuntimeStats {
                memory_bytes: 1,
                ..RuntimeStats::default()
            },
            RuntimeStats {
                memory_bytes: 1,
                ..RuntimeStats::default()
            },
        ];

        assert!(CoalesceRule::new(100).apply(&plan, &stats).is_none());
    }

    // ── SmallFilePlanner ──────────────────────────────────────────────────

    use super::FileStats;

    fn make_file(path: &str, size_bytes: u64) -> FileStats {
        FileStats {
            path: path.to_owned(),
            size_bytes,
        }
    }

    #[test]
    fn small_file_planner_groups_small_files() {
        let files = vec![
            make_file("a.parquet", 100),
            make_file("b.parquet", 100),
            make_file("c.parquet", 100),
        ];
        let planner = SmallFilePlanner::new(250);
        let advice = planner.plan(&files);
        assert_eq!(
            advice.task_groups,
            vec![
                vec!["a.parquet".to_owned(), "b.parquet".to_owned()],
                vec!["c.parquet".to_owned()],
            ]
        );
    }

    #[test]
    fn small_file_planner_each_large_file_own_task() {
        let files = vec![
            make_file("big1.parquet", 1000),
            make_file("big2.parquet", 2000),
        ];
        let planner = SmallFilePlanner::new(500);
        let advice = planner.plan(&files);
        assert_eq!(
            advice.task_groups,
            vec![
                vec!["big1.parquet".to_owned()],
                vec!["big2.parquet".to_owned()],
            ]
        );
    }

    #[test]
    fn small_file_planner_empty_input() {
        let planner = SmallFilePlanner::new(1000);
        let advice = planner.plan(&[]);
        assert_eq!(advice.task_groups, Vec::<Vec<String>>::new());
    }

    #[test]
    fn small_file_planner_all_fit_in_one_task() {
        let files = vec![
            make_file("x.parquet", 50),
            make_file("y.parquet", 50),
            make_file("z.parquet", 50),
        ];
        let planner = SmallFilePlanner::new(1000);
        let advice = planner.plan(&files);
        assert_eq!(
            advice.task_groups,
            vec![vec![
                "x.parquet".to_owned(),
                "y.parquet".to_owned(),
                "z.parquet".to_owned()
            ]]
        );
    }

    // ── StreamingAqeGuard ─────────────────────────────────────────────────

    fn batch_plan() -> PhysicalPlan {
        PhysicalPlan::new("batch-plan", ExecutionKind::Batch)
    }

    fn streaming_plan() -> PhysicalPlan {
        PhysicalPlan::new("streaming-plan", ExecutionKind::Streaming)
    }

    fn stats_small(n: usize) -> Vec<RuntimeStats> {
        (0..n)
            .map(|_| RuntimeStats {
                memory_bytes: 100,
                ..Default::default()
            })
            .collect()
    }

    #[test]
    fn streaming_guard_detects_streaming_plan() {
        assert!(StreamingAqeGuard::plan_is_streaming(&streaming_plan()));
        assert!(!StreamingAqeGuard::plan_is_streaming(&batch_plan()));
    }

    #[test]
    fn aqe_optimizer_applies_guarded_rules_to_batch() {
        let mut aqe = AqeOptimizer::new();
        aqe.add_guarded_rule(Box::new(CoalesceRule::new(1)));

        let stats = stats_small(2);
        let (_, batch_fired) = aqe.apply(batch_plan(), &stats).expect("aqe");
        let (_, stream_fired) = aqe.apply(streaming_plan(), &stats).expect("aqe");

        assert!(
            batch_fired.is_empty(),
            "advisory-only rule never appears as fired"
        );
        assert!(
            stream_fired.is_empty(),
            "streaming plan: guard skipped rule correctly"
        );
    }

    #[test]
    fn aqe_optimizer_always_rules_run_for_streaming() {
        let mut aqe = AqeOptimizer::new();
        aqe.add_guarded_rule(Box::new(CoalesceRule::new(1)));
        let plan = streaming_plan();
        let stats = stats_small(3);
        let (returned_plan, _) = aqe.apply(plan.clone(), &stats).expect("aqe");
        assert_eq!(returned_plan, plan);
    }

    // ── S5.1: CoalesceRule reduces 200 small partitions to ≤ 10 ──────────

    /// S5.1 test: 200 partitions × 1 MiB each → total 200 MiB.
    ///
    /// With `target_partition_bytes = 128 MiB`:
    ///   target_partitions = ceil(200 MiB / 128 MiB) = 2
    ///
    /// All 200 partitions are "small" (1 MiB < min_partition_bytes = 128 MiB),
    /// so `advise()` returns a single group of all 200 indices, giving
    /// `groups.len() = 1 < original_count = 200`.
    ///
    /// `CoalesceRule::apply` must insert a `CoalescePartitions` node and the
    /// resulting plan must have `target_partitions ≤ 10`.
    #[test]
    fn coalesce_rule_reduces_200_small_partitions() {
        use crate::NodeOp;

        use crate::optimizer::AqeRule;

        const PARTITIONS: usize = 200;
        const ONE_MIB: u64 = 1_048_576; // 1 MiB per partition

        // Build stats: 200 partitions, each 1 MiB — all below the 128 MiB threshold.
        let stats: Vec<RuntimeStats> = (0..PARTITIONS)
            .map(|_| RuntimeStats {
                memory_bytes: ONE_MIB,
                ..Default::default()
            })
            .collect();

        let rule = CoalesceRule::new(ONE_MIB * 2) // min = 2 MiB → all 200 are small
            .with_target_partition_bytes(134_217_728); // target = 128 MiB

        let plan = PhysicalPlan::new("big-job", ExecutionKind::Batch);
        let rewritten = AqeRule::apply(&rule, &plan, &stats).expect("coalesce should fire");

        // The plan must have had a CoalescePartitions node appended.
        let coalesce_node = rewritten
            .nodes()
            .iter()
            .find(|n: &&crate::PlanNode| matches!(n.op(), Some(NodeOp::CoalescePartitions { .. })));

        assert!(
            coalesce_node.is_some(),
            "expected a CoalescePartitions node to be inserted"
        );

        // Extract target_partitions from the node and verify it is ≤ 10.
        if let Some(NodeOp::CoalescePartitions { target_partitions }) =
            coalesce_node.and_then(|n: &crate::PlanNode| n.op())
        {
            assert!(
                *target_partitions <= 10,
                "expected target_partitions ≤ 10, got {target_partitions}"
            );
        }
    }

    /// Verify that `CoalesceRule::apply` is a no-op when all partitions are
    /// already large enough (no coalescing benefit).
    #[test]
    fn coalesce_rule_noop_when_partitions_are_large() {
        use crate::optimizer::AqeRule;

        const ONE_GIB: u64 = 1_073_741_824;

        // Each partition is 1 GiB — well above any threshold.
        let stats: Vec<RuntimeStats> = (0..4)
            .map(|_| RuntimeStats {
                memory_bytes: ONE_GIB,
                ..Default::default()
            })
            .collect();

        let rule = CoalesceRule::new(1_048_576); // min = 1 MiB
        let plan = PhysicalPlan::new("large-job", ExecutionKind::Batch);
        let _plan_clone = plan.clone();
        let rewritten = AqeRule::apply(&rule, &plan, &stats);

        // No coalescing: plan must be returned unchanged (None).
        assert!(
            rewritten.is_none(),
            "plan must be None when no partitions are small"
        );
    }

    // ── Logical rule test helpers ─────────────────────────────────────────

    fn scan_with_schema(
        id: &str,
        table: &str,
        schema_fields: &[(&str, crate::FieldType)],
    ) -> PlanNode {
        let schema = crate::PlanSchema::new(
            schema_fields
                .iter()
                .map(|(name, ft)| crate::SchemaField::new(*name, ft.clone()))
                .collect(),
        );
        PlanNode::new(id, format!("scan {table}"), ExecutionKind::Batch)
            .with_op(NodeOp::Scan {
                table: table.to_string(),
                filters: vec![],
            })
            .with_output_schema(schema)
    }

    fn filter_node(id: &str, inputs: &[&str], predicate: &str) -> PlanNode {
        PlanNode::new(id, format!("filter: {predicate}"), ExecutionKind::Batch)
            .with_inputs(inputs.iter().map(|s| s.to_string()))
            .with_op(NodeOp::Filter {
                predicate: predicate.to_string(),
            })
    }

    fn project_node(id: &str, inputs: &[&str], columns: &[&str]) -> PlanNode {
        PlanNode::new(id, "project", ExecutionKind::Batch)
            .with_inputs(inputs.iter().map(|s| s.to_string()))
            .with_op(NodeOp::Project {
                columns: columns.iter().map(|s| s.to_string()).collect(),
            })
    }

    // ── PredicatePushdownRule ──────────────────────────────────────────────

    use super::PredicatePushdownRule;

    #[test]
    fn predicate_pushdown_simple_filter_on_scan() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema(
                "s",
                "orders",
                &[("id", FieldType::Int64), ("amount", FieldType::Float64)],
            ))
            .with_node(filter_node("f", &["s"], "amount > 100"));

        let result = PredicatePushdownRule.apply(&plan).unwrap();

        // Filter should be removed, scan should have pushed filter
        assert!(
            !result.nodes().iter().any(|n| n.id() == "f"),
            "filter node should be removed"
        );
        let scan = result.nodes().iter().find(|n| n.id() == "s").unwrap();
        if let Some(NodeOp::Scan { filters, .. }) = scan.op() {
            assert_eq!(filters, &["amount > 100"]);
        } else {
            panic!("expected Scan node");
        }
    }

    #[test]
    fn predicate_pushdown_partial_pushdown() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema(
                "s",
                "orders",
                &[("id", FieldType::Int64), ("amount", FieldType::Float64)],
            ))
            // id is a scan column, status is not in scan's schema
            .with_node(filter_node("f", &["s"], "id > 0 AND status = 'active'"));

        let result = PredicatePushdownRule.apply(&plan).unwrap();

        // Filter should remain with only the non-pushable conjunct
        let filter = result.nodes().iter().find(|n| n.id() == "f").unwrap();
        if let Some(NodeOp::Filter { predicate }) = filter.op() {
            assert_eq!(predicate, "status = 'active'");
        } else {
            panic!("expected Filter node");
        }

        // Scan should have the pushable conjunct
        let scan = result.nodes().iter().find(|n| n.id() == "s").unwrap();
        if let Some(NodeOp::Scan { filters, .. }) = scan.op() {
            assert_eq!(filters, &["id > 0"]);
        } else {
            panic!("expected Scan node");
        }
    }

    #[test]
    fn predicate_pushdown_noop_when_predicate_not_scan_columns() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema("s", "orders", &[("id", FieldType::Int64)]))
            .with_node(filter_node("f", &["s"], "status = 'active'"));

        let result = PredicatePushdownRule.apply(&plan);
        assert!(result.is_none(), "no columns match → no change");
    }

    #[test]
    fn predicate_pushdown_noop_when_filter_not_over_scan() {
        // Filter above a project (not directly above a scan) should not change
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema("s", "t", &[("x", FieldType::Int32)]))
            .with_node(project_node("p", &["s"], &["x"]))
            .with_node(filter_node("f", &["p"], "x > 0"));

        let result = PredicatePushdownRule.apply(&plan);
        assert!(result.is_none(), "filter above project → no pushdown");
    }

    #[test]
    fn predicate_pushdown_empty_predicate_noop() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema("s", "t", &[("x", FieldType::Int32)]))
            .with_node(filter_node("f", &["s"], ""));

        let result = PredicatePushdownRule.apply(&plan);
        assert!(result.is_none(), "empty predicate → no change");
    }

    #[test]
    fn predicate_pushdown_qualified_column_match() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema(
                "s",
                "orders",
                &[("id", FieldType::Int64), ("amount", FieldType::Float64)],
            ))
            .with_node(filter_node(
                "f",
                &["s"],
                "orders.id = 5 AND orders.amount > 100",
            ));

        let result = PredicatePushdownRule.apply(&plan).unwrap();

        assert!(
            !result.nodes().iter().any(|n| n.id() == "f"),
            "filter should be fully pushed"
        );
        let scan = result.nodes().iter().find(|n| n.id() == "s").unwrap();
        if let Some(NodeOp::Scan { filters, .. }) = scan.op() {
            assert_eq!(filters.len(), 2);
            assert!(filters.contains(&"orders.id = 5".to_string()));
            assert!(filters.contains(&"orders.amount > 100".to_string()));
        } else {
            panic!("expected Scan node");
        }
    }

    #[test]
    fn predicate_pushdown_does_not_guess_table_aliases() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema("s", "orders", &[("id", FieldType::Int64)]))
            .with_node(filter_node("f", &["s"], "o.id = 5"));

        assert!(PredicatePushdownRule.apply(&plan).is_none());
    }

    #[test]
    fn predicate_pushdown_rewires_downstream_inputs() {
        // scan → filter → project: after pushdown, project should reference scan
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema("s", "t", &[("x", FieldType::Int32)]))
            .with_node(filter_node("f", &["s"], "x > 0"))
            .with_node(project_node("p", &["f"], &["x"]));

        let result = PredicatePushdownRule.apply(&plan).unwrap();

        // Filter should be gone
        assert!(
            !result.nodes().iter().any(|n| n.id() == "f"),
            "filter should be removed"
        );
        // Project should now reference scan directly
        let project = result.nodes().iter().find(|n| n.id() == "p").unwrap();
        assert!(
            project.inputs().contains(&"s".to_string()),
            "project should now reference the scan node directly"
        );
    }

    // ── Cost struct ──────────────────────────────────────────────────────────

    #[test]
    fn cost_default_is_all_zeros() {
        let cost = Cost::default();
        assert_eq!(cost.cpu_nanos, 0);
        assert_eq!(cost.memory_bytes, 0);
        assert_eq!(cost.network_bytes, 0);
    }

    #[test]
    fn cost_equality() {
        let a = Cost {
            cpu_nanos: 100,
            memory_bytes: 200,
            network_bytes: 300,
        };
        let b = Cost {
            cpu_nanos: 100,
            memory_bytes: 200,
            network_bytes: 300,
        };
        let c = Cost {
            cpu_nanos: 999,
            memory_bytes: 200,
            network_bytes: 300,
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn cost_clone_produces_equal_value() {
        let original = Cost {
            cpu_nanos: 42,
            memory_bytes: 1024,
            network_bytes: 512,
        };
        let cloned = original.clone();
        assert_eq!(original, cloned);
    }

    #[test]
    fn cost_debug_format() {
        let cost = Cost {
            cpu_nanos: 42,
            memory_bytes: 100,
            network_bytes: 200,
        };
        let debug = format!("{cost:?}");
        assert!(debug.contains("42"));
        assert!(debug.contains("100"));
        assert!(debug.contains("200"));
    }

    // ── RuntimeStats additional tests ───────────────────────────────────────

    #[test]
    fn runtime_stats_custom_values() {
        let stats = RuntimeStats {
            input_rows: 1000,
            output_rows: 500,
            cpu_nanos: 1_000_000,
            memory_bytes: 1024 * 1024,
            spill_bytes: 4096,
            serialized_bytes: 512 * 1024,
        };
        assert_eq!(stats.input_rows, 1000);
        assert_eq!(stats.output_rows, 500);
        assert_eq!(stats.cpu_nanos, 1_000_000);
        assert_eq!(stats.memory_bytes, 1024 * 1024);
        assert_eq!(stats.spill_bytes, 4096);
        assert_eq!(stats.serialized_bytes, 512 * 1024);
    }

    #[test]
    fn runtime_stats_equality() {
        let a = RuntimeStats {
            input_rows: 10,
            output_rows: 5,
            cpu_nanos: 100,
            memory_bytes: 200,
            spill_bytes: 0,
            serialized_bytes: 0,
        };
        let b = RuntimeStats {
            input_rows: 10,
            output_rows: 5,
            cpu_nanos: 100,
            memory_bytes: 200,
            spill_bytes: 0,
            serialized_bytes: 0,
        };
        let c = RuntimeStats {
            input_rows: 10,
            output_rows: 5,
            cpu_nanos: 100,
            memory_bytes: 999,
            spill_bytes: 0,
            serialized_bytes: 0,
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn runtime_stats_clone() {
        let original = RuntimeStats {
            input_rows: 42,
            output_rows: 41,
            cpu_nanos: 99,
            memory_bytes: 88,
            spill_bytes: 77,
            serialized_bytes: 66,
        };
        let cloned = original.clone();
        assert_eq!(original, cloned);
    }

    #[test]
    fn auto_partition_rule_prefers_serialized_bytes_over_memory_bytes() {
        // A plan with a Hash exchange at 4 buckets.
        let plan = PhysicalPlan::new("p", ExecutionKind::Batch).with_node(
            PlanNode::new("xchg", "exchange", ExecutionKind::Batch).with_partitioning(
                Partitioning::Hash {
                    keys: vec!["k".into()],
                    buckets: 4,
                },
            ),
        );

        // 200 MiB in memory but only 50 MiB serialized.
        // With target=128 MiB:
        //   serialized path: ceil(50/128) = 1 → target=1
        //   memory path:     ceil(200/128) = 2 → target=2
        // The rule must use serialized_bytes and produce 1 bucket.
        let stats = vec![RuntimeStats {
            memory_bytes: 200 * 1024 * 1024,
            serialized_bytes: 50 * 1024 * 1024,
            ..Default::default()
        }];

        let rule = AutoPartitionRule::new(64).with_target_partition_bytes(128 * 1024 * 1024);
        let result = rule.apply(&plan.clone(), &stats).expect("rule must fire");

        let buckets = result
            .nodes()
            .iter()
            .find_map(|n| {
                if let Partitioning::Hash { buckets, .. } = n.partitioning() {
                    Some(*buckets)
                } else {
                    None
                }
            })
            .expect("exchange node");

        assert_eq!(
            buckets, 1,
            "serialized_bytes=50 MiB / target=128 MiB → 1 bucket, not memory-driven 2"
        );
    }

    #[test]
    fn coalesce_rule_prefers_serialized_bytes_for_small_partition_detection() {
        // Three partitions: small in memory but large when serialized (e.g.
        // incompressible binary blobs). serialized_bytes must govern the
        // "is small?" threshold, not memory_bytes.
        let rule = CoalesceRule::new(100); // threshold = 100 bytes
        let stats = vec![
            // memory=50 (< 100), but serialized=500 (≥ 100) → big
            RuntimeStats {
                memory_bytes: 50,
                serialized_bytes: 500,
                ..Default::default()
            },
            // memory=50 (< 100), serialized=0 → fall back to memory → small
            RuntimeStats {
                memory_bytes: 50,
                serialized_bytes: 0,
                ..Default::default()
            },
        ];
        let advice = rule.advise(&stats);
        // Partition 0 must be a singleton (big by serialized_bytes).
        // Partition 1 must be in its own group too (small — but only 1 partition,
        // so no coalescing benefit). The groups should be [[1], [0]] after sort
        // (sorted by effective_bytes: 0 → 50 via fallback, 500).
        assert_eq!(advice.groups.len(), 2, "each partition in its own group");
    }

    // ── ThresholdSkewRule additional tests ──────────────────────────────────

    #[test]
    fn skew_rule_single_partition_never_hot() {
        let stats = make_stats_with_rows(&[1000]);
        let rule = ThresholdSkewRule::new(2.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert!(
            hot.is_empty(),
            "single partition cannot be hot relative to itself"
        );
    }

    #[test]
    fn skew_rule_all_zero_rows() {
        let stats = make_stats_with_rows(&[0, 0, 0]);
        let rule = ThresholdSkewRule::new(2.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert!(hot.is_empty(), "all-zero rows produce no hot partitions");
    }

    #[test]
    fn skew_rule_threshold_zero_any_nonzero_is_hot() {
        let stats = make_stats_with_rows(&[0, 5, 0]);
        let rule = ThresholdSkewRule::new(0.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert_eq!(
            hot,
            vec![1],
            "threshold=0 should flag any non-zero partition"
        );
    }

    #[test]
    fn skew_rule_threshold_zero_all_zero_nothing_hot() {
        let stats = make_stats_with_rows(&[0, 0, 0]);
        let rule = ThresholdSkewRule::new(0.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert!(
            hot.is_empty(),
            "all-zero rows with threshold=0 should produce no hot partitions"
        );
    }

    #[test]
    fn skew_rule_very_large_threshold_nothing_hot() {
        let stats = make_stats_with_rows(&[10, 20, 30]);
        let rule = ThresholdSkewRule::new(100.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert!(
            hot.is_empty(),
            "very large threshold should not flag anything"
        );
    }

    #[test]
    fn skew_rule_two_partitions_never_hot_at_threshold_2() {
        // With 2 partitions [a, b] sorted, median = (a+b)/2.
        // b > 2*(a+b)/2 = a+b means b > a+b, i.e. 0 > a, impossible for u64.
        let stats = make_stats_with_rows(&[1, 1000]);
        let rule = ThresholdSkewRule::new(2.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert!(
            hot.is_empty(),
            "with 2 partitions and threshold=2.0, no partition can be hot"
        );
    }

    #[test]
    fn skew_rule_three_partitions_two_hot() {
        // sorted: [10, 10, 100], median=10, threshold=2.0 → 100 > 20 → hot
        let stats = make_stats_with_rows(&[100, 10, 10]);
        let rule = ThresholdSkewRule::new(2.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert_eq!(hot, vec![0]); // 100 is at index 0 in original
    }

    #[test]
    fn skew_rule_name() {
        let rule = ThresholdSkewRule::new(2.0);
        assert_eq!(rule.name(), "threshold-skew");
    }

    #[test]
    fn skew_rule_single_nonzero_partition() {
        // [50] — median=50, 50 > 2*50=100? No.
        let stats = make_stats_with_rows(&[50]);
        let rule = ThresholdSkewRule::new(2.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert!(hot.is_empty());
    }

    #[test]
    fn skew_rule_many_identical_partitions() {
        let stats = make_stats_with_rows(&[100; 10]);
        let rule = ThresholdSkewRule::new(2.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert!(
            hot.is_empty(),
            "all identical partitions should produce no hot"
        );
    }

    #[test]
    fn skew_rule_odd_length_median() {
        // sorted: [10, 20, 30], median = 20
        // threshold=1.5 → hot when rows > 30
        // 30 is NOT strictly > 30, so no hot
        let stats = make_stats_with_rows(&[30, 10, 20]);
        let rule = ThresholdSkewRule::new(1.5);
        let hot = rule.detect_hot_partitions(&stats);
        assert!(hot.is_empty());
    }

    #[test]
    fn skew_rule_odd_length_median_with_hot() {
        // sorted: [10, 20, 100], median = 20
        // threshold=2.0 → hot when rows > 40 → 100 qualifies
        let stats = make_stats_with_rows(&[10, 100, 20]);
        let rule = ThresholdSkewRule::new(2.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert_eq!(hot, vec![1]);
    }

    #[test]
    fn skew_rule_even_length_median_averaging() {
        // sorted: [10, 20, 30, 40], median = (20+30)/2 = 25
        // threshold=2.0 → hot when rows > 50 → none qualify
        let stats = make_stats_with_rows(&[40, 10, 30, 20]);
        let rule = ThresholdSkewRule::new(2.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert!(hot.is_empty());
    }

    #[test]
    fn skew_rule_even_length_median_with_hot() {
        // sorted: [10, 20, 30, 200], median = (20+30)/2 = 25
        // threshold=2.0 → hot when rows > 50 → 200 qualifies
        let stats = make_stats_with_rows(&[200, 10, 30, 20]);
        let rule = ThresholdSkewRule::new(2.0);
        let hot = rule.detect_hot_partitions(&stats);
        assert_eq!(hot, vec![0]);
    }

    // ── CoalesceRule additional tests ───────────────────────────────────────

    #[test]
    fn coalesce_rule_target_partition_bytes_getter() {
        let rule = CoalesceRule::new(1000);
        assert_eq!(rule.target_partition_bytes(), 134_217_728); // default 128 MiB
    }

    #[test]
    fn coalesce_rule_with_target_partition_bytes() {
        let rule = CoalesceRule::new(1000).with_target_partition_bytes(256 * 1024 * 1024);
        assert_eq!(rule.target_partition_bytes(), 256 * 1024 * 1024);
    }

    #[test]
    fn coalesce_rule_boundary_memory_equals_threshold() {
        // memory_bytes == min_partition_bytes → NOT small (< is strict)
        let stats = make_stats_with_memory(&[1000, 1000, 1000]);
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&stats);
        assert_eq!(advice.groups, vec![vec![0], vec![1], vec![2]]);
    }

    #[test]
    fn coalesce_rule_boundary_memory_one_less_than_threshold() {
        // After sort: [0(999), 2(999), 1(1001)].
        // Both 999-byte partitions are small (< 1000 threshold) and coalesce;
        // 1001-byte partition is big and stays singleton.
        let stats = make_stats_with_memory(&[999, 1001, 999]);
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&stats);
        assert_eq!(advice.groups, vec![vec![0, 2], vec![1]]);
    }

    #[test]
    fn coalesce_rule_consecutive_smalls_grouped() {
        // After sort: [0(100), 4(100), 1(200), 5(200), 2(300), 3(5000)].
        // All five small partitions (100+100+200+200+300=900 bytes) fit under
        // the 128 MiB target, so they coalesce into one group. 3(5000) is big.
        let stats = make_stats_with_memory(&[100, 200, 300, 5000, 100, 200]);
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&stats);
        assert_eq!(advice.groups, vec![vec![0, 4, 1, 5, 2], vec![3]]);
    }

    #[test]
    fn coalesce_rule_all_small_one_group() {
        let stats = make_stats_with_memory(&[1, 2, 3, 4, 5]);
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&stats);
        assert_eq!(advice.groups, vec![vec![0, 1, 2, 3, 4]]);
    }

    #[test]
    fn coalesce_rule_all_big_singleton_groups() {
        let stats = make_stats_with_memory(&[5000, 6000, 7000]);
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&stats);
        assert_eq!(advice.groups, vec![vec![0], vec![1], vec![2]]);
    }

    #[test]
    fn coalesce_rule_single_partition() {
        let stats = make_stats_with_memory(&[100]);
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&stats);
        assert_eq!(advice.groups, vec![vec![0]]);
    }

    #[test]
    fn coalesce_rule_apply_empty_stats() {
        let rule = CoalesceRule::new(1000);
        let plan = PhysicalPlan::new("test", ExecutionKind::Batch);
        let result = rule.apply(&plan, &[]);
        assert!(result.is_none());
    }

    #[test]
    fn coalesce_rule_apply_single_partition() {
        let stats = make_stats_with_memory(&[100]);
        let rule = CoalesceRule::new(1000);
        let plan = PhysicalPlan::new("test", ExecutionKind::Batch);
        let result = rule.apply(&plan, &stats);
        assert!(
            result.is_none(),
            "single partition should not trigger coalescing"
        );
    }

    #[test]
    fn coalesce_rule_apply_two_partitions_one_small_one_big() {
        let stats = make_stats_with_memory(&[100, 5000]);
        let rule = CoalesceRule::new(1000);
        let plan = PhysicalPlan::new("test", ExecutionKind::Batch);
        let result = rule.apply(&plan, &stats);
        assert!(
            result.is_none(),
            "2 groups from 2 partitions → no coalescing"
        );
    }

    #[test]
    fn coalesce_rule_apply_two_partitions_both_small() {
        let stats = make_stats_with_memory(&[100, 200]);
        let rule = CoalesceRule::new(1000);
        let plan = PhysicalPlan::new("test", ExecutionKind::Batch);
        let result = rule.apply(&plan, &stats);
        assert!(result.is_some(), "2 small partitions should coalesce");
        let result = result.unwrap();
        assert_eq!(result.coalesced_partition_count(), Some(1));
    }

    #[test]
    fn coalesce_rule_apply_stamps_coalesce_node() {
        use crate::NodeOp;
        let stats = make_stats_with_memory(&[100, 200, 300]);
        let rule = CoalesceRule::new(1000);
        let plan = PhysicalPlan::new("test", ExecutionKind::Batch);
        let result = rule.apply(&plan, &stats).unwrap();
        let coalesce_node = result
            .nodes()
            .iter()
            .find(|n| matches!(n.op(), Some(NodeOp::CoalescePartitions { .. })));
        assert!(
            coalesce_node.is_some(),
            "expected a CoalescePartitions node"
        );
    }

    #[test]
    fn coalesce_rule_apply_not_stamped_when_no_coalescing() {
        let stats = make_stats_with_memory(&[5000, 6000]);
        let rule = CoalesceRule::new(1000);
        let plan = PhysicalPlan::new("test", ExecutionKind::Batch);
        let result = rule.apply(&plan, &stats);
        assert!(result.is_none());
    }

    #[test]
    fn coalesce_rule_advise_name() {
        let rule = CoalesceRule::new(1000);
        assert_eq!(rule.name(), "coalesce-small-partitions");
    }

    #[test]
    fn coalesce_rule_target_partitions_from_stats_zero_total() {
        // All memory_bytes=0 → total=0 → returns 1
        let stats = make_stats_with_memory(&[0, 0, 0]);
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&stats);
        // All zeros are < 1000, so one group
        assert_eq!(advice.groups, vec![vec![0, 1, 2]]);
    }

    #[test]
    fn coalesce_rule_min_partition_bytes_zero_nothing_small() {
        // min_partition_bytes=0 → memory_bytes < 0 is never true for u64
        let stats = make_stats_with_memory(&[100, 200, 300]);
        let rule = CoalesceRule::new(0);
        let advice = rule.advise(&stats);
        assert_eq!(advice.groups, vec![vec![0], vec![1], vec![2]]);
    }

    #[test]
    fn coalesce_rule_min_partition_bytes_max_all_small() {
        let stats = make_stats_with_memory(&[100, 200, 300]);
        let rule = CoalesceRule::new(u64::MAX);
        let advice = rule.advise(&stats);
        assert_eq!(advice.groups, vec![vec![0, 1, 2]]);
    }

    #[test]
    fn coalesce_rule_apply_empty_partition_list_advise() {
        let rule = CoalesceRule::new(1000);
        let advice = rule.advise(&[]);
        assert!(advice.groups.is_empty());
    }

    // ── SmallFilePlanner additional tests ───────────────────────────────────

    #[test]
    fn small_file_planner_single_file() {
        let files = vec![make_file("only.parquet", 500)];
        let planner = SmallFilePlanner::new(1000);
        let advice = planner.plan(&files);
        assert_eq!(advice.task_groups, vec![vec!["only.parquet".to_owned()]]);
    }

    #[test]
    fn small_file_planner_single_large_file() {
        let files = vec![make_file("huge.parquet", 10_000)];
        let planner = SmallFilePlanner::new(1000);
        let advice = planner.plan(&files);
        assert_eq!(advice.task_groups, vec![vec!["huge.parquet".to_owned()]]);
    }

    #[test]
    fn small_file_planner_exact_fit() {
        let files = vec![make_file("a.parquet", 500), make_file("b.parquet", 500)];
        let planner = SmallFilePlanner::new(1000);
        let advice = planner.plan(&files);
        // 500 + 500 = 1000, NOT > 1000, so they stay together
        assert_eq!(
            advice.task_groups,
            vec![vec!["a.parquet".to_owned(), "b.parquet".to_owned()]]
        );
    }

    #[test]
    fn small_file_planner_just_over_fit() {
        let files = vec![make_file("a.parquet", 500), make_file("b.parquet", 501)];
        let planner = SmallFilePlanner::new(1000);
        let advice = planner.plan(&files);
        // 500 + 501 = 1001 > 1000, so split
        assert_eq!(
            advice.task_groups,
            vec![vec!["a.parquet".to_owned()], vec!["b.parquet".to_owned()],]
        );
    }

    #[test]
    fn small_file_planner_target_bytes_zero() {
        let files = vec![make_file("a.parquet", 100), make_file("b.parquet", 200)];
        let planner = SmallFilePlanner::new(0);
        let advice = planner.plan(&files);
        // Each file gets its own group
        assert_eq!(
            advice.task_groups,
            vec![vec!["a.parquet".to_owned()], vec!["b.parquet".to_owned()],]
        );
    }

    #[test]
    fn small_file_planner_many_tiny_files() {
        let files: Vec<FileStats> = (0..50)
            .map(|i| make_file(&format!("file_{i}.parquet"), 10))
            .collect();
        let planner = SmallFilePlanner::new(100);
        let advice = planner.plan(&files);
        assert_eq!(advice.task_groups.len(), 5);
        for group in &advice.task_groups {
            assert_eq!(group.len(), 10);
        }
    }

    #[test]
    fn small_file_planner_mixed_sizes() {
        let files = vec![
            make_file("tiny.parquet", 10),
            make_file("small.parquet", 50),
            make_file("big.parquet", 200),
            make_file("tiny2.parquet", 10),
        ];
        let planner = SmallFilePlanner::new(100);
        let advice = planner.plan(&files);
        assert_eq!(
            advice.task_groups,
            vec![
                vec!["tiny.parquet".to_owned(), "small.parquet".to_owned()],
                vec!["big.parquet".to_owned()],
                vec!["tiny2.parquet".to_owned()],
            ]
        );
    }

    #[test]
    fn small_file_planner_zero_byte_files() {
        let files = vec![
            make_file("empty1.parquet", 0),
            make_file("empty2.parquet", 0),
            make_file("empty3.parquet", 0),
        ];
        let planner = SmallFilePlanner::new(1000);
        let advice = planner.plan(&files);
        assert_eq!(
            advice.task_groups,
            vec![vec![
                "empty1.parquet".to_owned(),
                "empty2.parquet".to_owned(),
                "empty3.parquet".to_owned(),
            ]]
        );
    }

    #[test]
    fn small_file_planner_handles_u64_size_overflow() {
        let files = vec![
            make_file("max.parquet", u64::MAX),
            make_file("one.parquet", 1),
        ];

        let advice = SmallFilePlanner::new(u64::MAX).plan(&files);

        assert_eq!(
            advice.task_groups,
            vec![
                vec!["max.parquet".to_string()],
                vec!["one.parquet".to_string()]
            ]
        );
    }

    #[test]
    fn small_file_planner_zero_byte_files_target_zero() {
        let files = vec![make_file("e1.parquet", 0), make_file("e2.parquet", 0)];
        let planner = SmallFilePlanner::new(0);
        let advice = planner.plan(&files);
        // 0 + 0 = 0, NOT > 0, so they stay together
        assert_eq!(
            advice.task_groups,
            vec![vec!["e1.parquet".to_owned(), "e2.parquet".to_owned()]]
        );
    }

    #[test]
    fn small_file_planner_target_bytes_one() {
        let files = vec![make_file("a.parquet", 1), make_file("b.parquet", 1)];
        let planner = SmallFilePlanner::new(1);
        let advice = planner.plan(&files);
        // 1 + 1 = 2 > 1, so split
        assert_eq!(
            advice.task_groups,
            vec![vec!["a.parquet".to_owned()], vec!["b.parquet".to_owned()],]
        );
    }

    #[test]
    fn small_file_planner_large_files_each_own_task() {
        let files: Vec<FileStats> = (0..10)
            .map(|i| make_file(&format!("big_{i}.parquet"), 1_000_000))
            .collect();
        let planner = SmallFilePlanner::new(100);
        let advice = planner.plan(&files);
        assert_eq!(advice.task_groups.len(), 10);
        for group in &advice.task_groups {
            assert_eq!(group.len(), 1);
        }
    }

    // ── AqeOptimizer additional tests ──────────────────────────────────────

    struct AlwaysFireRule;

    impl AqeRule for AlwaysFireRule {
        fn name(&self) -> &str {
            "always-fire"
        }
        fn apply(&self, plan: &PhysicalPlan, _stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
            let node_id = format!("extra-{}", plan.nodes().len());
            Some(
                plan.clone()
                    .with_node(PlanNode::new(node_id, "extra", ExecutionKind::Batch)),
            )
        }
    }

    struct NeverFireRule;

    impl AqeRule for NeverFireRule {
        fn name(&self) -> &str {
            "never-fire"
        }
        fn apply(&self, _plan: &PhysicalPlan, _stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
            None
        }
    }

    #[test]
    fn aqe_optimizer_empty_no_rules() {
        let aqe = AqeOptimizer::new();
        let plan = batch_plan();
        let (result, applied) = aqe.apply(plan.clone(), &[]).expect("aqe");
        assert_eq!(result, plan);
        assert!(applied.is_empty());
    }

    #[test]
    fn aqe_optimizer_empty_default() {
        let aqe = AqeOptimizer::default();
        let plan = batch_plan();
        let (result, applied) = aqe.apply(plan.clone(), &[]).expect("aqe");
        assert_eq!(result, plan);
        assert!(applied.is_empty());
    }

    #[test]
    fn aqe_optimizer_always_rules_fired_recorded() {
        let mut aqe = AqeOptimizer::new();
        aqe.add_rule(Box::new(AlwaysFireRule));
        let plan = batch_plan();
        let (result, applied) = aqe.apply(plan, &[]).expect("aqe");
        assert_eq!(applied, vec!["always-fire"]);
        assert!(!result.nodes().is_empty());
    }

    #[test]
    fn aqe_optimizer_guarded_rules_fired_on_batch() {
        let mut aqe = AqeOptimizer::new();
        aqe.add_guarded_rule(Box::new(AlwaysFireRule));
        let stats = stats_small(2);
        let (_, applied) = aqe.apply(batch_plan(), &stats).expect("aqe");
        assert_eq!(applied, vec!["always-fire"]);
    }

    #[test]
    fn aqe_optimizer_guarded_rules_skipped_on_streaming() {
        let mut aqe = AqeOptimizer::new();
        aqe.add_guarded_rule(Box::new(AlwaysFireRule));
        let stats = stats_small(2);
        let (_, applied) = aqe.apply(streaming_plan(), &stats).expect("aqe");
        assert!(
            applied.is_empty(),
            "guarded rules should be skipped for streaming"
        );
    }

    #[test]
    fn aqe_optimizer_mixed_rules_batch() {
        let mut aqe = AqeOptimizer::new();
        aqe.add_rule(Box::new(AlwaysFireRule));
        aqe.add_guarded_rule(Box::new(AlwaysFireRule));
        let stats = stats_small(2);
        let (_, applied) = aqe.apply(batch_plan(), &stats).expect("aqe");
        assert_eq!(applied, vec!["always-fire", "always-fire"]);
    }

    #[test]
    fn aqe_optimizer_mixed_rules_streaming() {
        let mut aqe = AqeOptimizer::new();
        aqe.add_rule(Box::new(AlwaysFireRule));
        aqe.add_guarded_rule(Box::new(AlwaysFireRule));
        let stats = stats_small(2);
        let (_, applied) = aqe.apply(streaming_plan(), &stats).expect("aqe");
        assert_eq!(applied, vec!["always-fire"]);
    }

    #[test]
    fn aqe_optimizer_never_fire_rules() {
        let mut aqe = AqeOptimizer::new();
        aqe.add_rule(Box::new(NeverFireRule));
        aqe.add_guarded_rule(Box::new(NeverFireRule));
        let stats = stats_small(2);
        let (_, applied) = aqe.apply(batch_plan(), &stats).expect("aqe");
        assert!(applied.is_empty());
    }

    #[test]
    fn aqe_optimizer_rejects_invalid_input_plan() {
        let aqe = AqeOptimizer::new();
        let invalid = PhysicalPlan::new("invalid", ExecutionKind::Batch).with_node(
            PlanNode::new("sink", "sink", ExecutionKind::Batch).with_inputs(["missing"]),
        );

        let error = aqe.apply(invalid, &[]).expect_err("invalid input");

        assert!(matches!(
            error,
            OptimizerError::InvalidInput {
                optimizer: "AQE",
                ..
            }
        ));
    }

    #[test]
    fn aqe_optimizer_rejects_invalid_rule_output() {
        struct InvalidAqeRule;
        impl AqeRule for InvalidAqeRule {
            fn name(&self) -> &str {
                "invalid-aqe"
            }

            fn apply(&self, plan: &PhysicalPlan, _stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
                Some(
                    plan.clone().with_node(
                        PlanNode::new("dangling", "dangling", ExecutionKind::Batch)
                            .with_inputs(["missing"]),
                    ),
                )
            }
        }

        let mut aqe = AqeOptimizer::new();
        aqe.add_rule(Box::new(InvalidAqeRule));

        let error = aqe
            .apply(batch_plan(), &[])
            .expect_err("invalid rule output");

        assert!(matches!(
            error,
            OptimizerError::InvalidRuleOutput {
                optimizer: "AQE",
                ref rule,
                ..
            } if rule == "invalid-aqe"
        ));
    }

    #[test]
    fn aqe_optimizer_contains_rule_panics() {
        struct PanickingAqeRule;
        impl AqeRule for PanickingAqeRule {
            fn name(&self) -> &str {
                "panicking-aqe"
            }

            fn apply(&self, _plan: &PhysicalPlan, _stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
                panic!("aqe rule failed")
            }
        }

        let mut aqe = AqeOptimizer::new();
        aqe.add_rule(Box::new(PanickingAqeRule));

        let error = aqe
            .apply(batch_plan(), &[])
            .expect_err("panic must be contained");

        assert!(matches!(
            error,
            OptimizerError::RulePanicked {
                optimizer: "AQE",
                ref rule,
                ref message,
            } if rule == "panicking-aqe" && message == "aqe rule failed"
        ));
    }

    #[test]
    fn aqe_optimizer_streaming_plan_detected_via_node() {
        let plan = PhysicalPlan::new("hybrid", ExecutionKind::Batch).with_node(PlanNode::new(
            "stream-node",
            "source",
            ExecutionKind::Streaming,
        ));
        assert!(StreamingAqeGuard::plan_is_streaming(&plan));
    }

    #[test]
    fn aqe_optimizer_batch_plan_with_batch_nodes_not_streaming() {
        let plan = PhysicalPlan::new("batch", ExecutionKind::Batch).with_node(PlanNode::new(
            "n1",
            "node1",
            ExecutionKind::Batch,
        ));
        assert!(!StreamingAqeGuard::plan_is_streaming(&plan));
    }

    #[test]
    fn aqe_optimizer_multiple_guarded_rules_all_skipped_on_streaming() {
        let mut aqe = AqeOptimizer::new();
        aqe.add_guarded_rule(Box::new(AlwaysFireRule));
        aqe.add_guarded_rule(Box::new(AlwaysFireRule));
        aqe.add_guarded_rule(Box::new(AlwaysFireRule));
        let stats = stats_small(2);
        let (_, applied) = aqe.apply(streaming_plan(), &stats).expect("aqe");
        assert!(applied.is_empty());
    }

    // ── StreamingAqeGuard ──────────────────────────────────────────────────

    #[test]
    fn streaming_guard_empty_plan_not_streaming() {
        let plan = PhysicalPlan::new("empty", ExecutionKind::Batch);
        assert!(!StreamingAqeGuard::plan_is_streaming(&plan));
    }

    #[test]
    fn streaming_guard_streaming_plan_is_streaming() {
        let plan = PhysicalPlan::new("stream", ExecutionKind::Streaming);
        assert!(StreamingAqeGuard::plan_is_streaming(&plan));
    }

    #[test]
    fn streaming_guard_batch_plan_with_streaming_node() {
        let plan = PhysicalPlan::new("batch", ExecutionKind::Batch).with_node(PlanNode::new(
            "s",
            "source",
            ExecutionKind::Streaming,
        ));
        assert!(StreamingAqeGuard::plan_is_streaming(&plan));
    }

    #[test]
    fn streaming_guard_streaming_plan_with_batch_node() {
        let plan = PhysicalPlan::new("stream", ExecutionKind::Streaming).with_node(PlanNode::new(
            "b",
            "batch-node",
            ExecutionKind::Batch,
        ));
        assert!(StreamingAqeGuard::plan_is_streaming(&plan));
    }

    #[test]
    fn streaming_guard_batch_plan_with_multiple_batch_nodes() {
        let plan = PhysicalPlan::new("batch", ExecutionKind::Batch)
            .with_node(PlanNode::new("n1", "a", ExecutionKind::Batch))
            .with_node(PlanNode::new("n2", "b", ExecutionKind::Batch));
        assert!(!StreamingAqeGuard::plan_is_streaming(&plan));
    }

    // ── PredicatePushdownRule additional tests ──────────────────────────────

    #[test]
    fn predicate_pushdown_all_conjuncts_pushable() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema(
                "s",
                "t",
                &[("a", FieldType::Int32), ("b", FieldType::Int64)],
            ))
            .with_node(filter_node("f", &["s"], "a > 0 AND b < 100"));

        let result = PredicatePushdownRule.apply(&plan).unwrap();
        assert!(!result.nodes().iter().any(|n| n.id() == "f"));
        let scan = result.nodes().iter().find(|n| n.id() == "s").unwrap();
        if let Some(NodeOp::Scan { filters, .. }) = scan.op() {
            assert_eq!(filters.len(), 2);
            assert!(filters.contains(&"a > 0".to_string()));
            assert!(filters.contains(&"b < 100".to_string()));
        } else {
            panic!("expected Scan node");
        }
    }

    #[test]
    fn predicate_pushdown_multiple_filters_on_different_scans() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema("s1", "t1", &[("a", FieldType::Int32)]))
            .with_node(scan_with_schema("s2", "t2", &[("b", FieldType::Int64)]))
            .with_node(filter_node("f1", &["s1"], "a > 0"))
            .with_node(filter_node("f2", &["s2"], "b < 100"));

        let result = PredicatePushdownRule.apply(&plan).unwrap();
        assert!(!result.nodes().iter().any(|n| n.id() == "f1"));
        assert!(!result.nodes().iter().any(|n| n.id() == "f2"));

        let scan1 = result.nodes().iter().find(|n| n.id() == "s1").unwrap();
        let scan2 = result.nodes().iter().find(|n| n.id() == "s2").unwrap();
        if let Some(NodeOp::Scan { filters, .. }) = scan1.op() {
            assert_eq!(filters, &["a > 0"]);
        }
        if let Some(NodeOp::Scan { filters, .. }) = scan2.op() {
            assert_eq!(filters, &["b < 100"]);
        }
    }

    #[test]
    fn predicate_pushdown_noop_when_no_filter_nodes() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch).with_node(scan_with_schema(
            "s",
            "t",
            &[("a", FieldType::Int32)],
        ));

        let result = PredicatePushdownRule.apply(&plan);
        assert!(result.is_none());
    }

    #[test]
    fn predicate_pushdown_name() {
        assert_eq!(PredicatePushdownRule.name(), "predicate-pushdown");
    }

    #[test]
    fn predicate_pushdown_only_sql_keywords() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema("s", "t", &[("a", FieldType::Int32)]))
            .with_node(filter_node("f", &["s"], "AND OR NOT"));

        let result = PredicatePushdownRule.apply(&plan);
        assert!(result.is_none());
    }

    #[test]
    fn predicate_pushdown_numbers_only() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema("s", "t", &[("a", FieldType::Int32)]))
            .with_node(filter_node("f", &["s"], "123 > 456"));

        let result = PredicatePushdownRule.apply(&plan);
        assert!(result.is_none());
    }

    #[test]
    fn predicate_pushdown_dot_qualified_column() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema("s", "t", &[("id", FieldType::Int64)]))
            .with_node(filter_node("f", &["s"], "t.id = 1"));

        let result = PredicatePushdownRule.apply(&plan).unwrap();
        assert!(!result.nodes().iter().any(|n| n.id() == "f"));
        let scan = result.nodes().iter().find(|n| n.id() == "s").unwrap();
        if let Some(NodeOp::Scan { filters, .. }) = scan.op() {
            assert_eq!(filters, &["t.id = 1"]);
        }
    }

    #[test]
    fn predicate_pushdown_empty_plan() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch);
        let result = PredicatePushdownRule.apply(&plan);
        assert!(result.is_none());
    }

    #[test]
    fn predicate_pushdown_filter_on_non_scan_input() {
        // Filter above an aggregate (not a scan)
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema("s", "t", &[("a", FieldType::Int32)]))
            .with_node(
                PlanNode::new("agg", "aggregate", ExecutionKind::Batch)
                    .with_inputs(vec!["s".to_string()])
                    .with_op(NodeOp::Aggregate {
                        group_keys: vec!["a".to_string()],
                    }),
            )
            .with_node(filter_node("f", &["agg"], "a > 0"));

        let result = PredicatePushdownRule.apply(&plan);
        assert!(result.is_none(), "filter above aggregate → no pushdown");
    }

    #[test]
    fn predicate_pushdown_preserves_existing_scan_filters() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node({
                let schema =
                    crate::PlanSchema::new(vec![crate::SchemaField::new("a", FieldType::Int32)]);
                PlanNode::new("s", "scan t", ExecutionKind::Batch)
                    .with_op(NodeOp::Scan {
                        table: "t".to_string(),
                        filters: vec!["existing_filter = 1".to_string()],
                    })
                    .with_output_schema(schema)
            })
            .with_node(filter_node("f", &["s"], "a > 0"));

        let result = PredicatePushdownRule.apply(&plan).unwrap();
        let scan = result.nodes().iter().find(|n| n.id() == "s").unwrap();
        if let Some(NodeOp::Scan { filters, .. }) = scan.op() {
            assert!(filters.contains(&"existing_filter = 1".to_string()));
            assert!(filters.contains(&"a > 0".to_string()));
            assert_eq!(filters.len(), 2);
        }
    }

    // ── default_logical_optimizer ───────────────────────────────────────────

    #[test]
    fn default_logical_optimizer_applies_only_semantics_preserving_rules() {
        let optimizer = default_logical_optimizer();
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema(
                "s",
                "t",
                &[("a", FieldType::Int32), ("b", FieldType::Int64)],
            ))
            .with_node(filter_node("f", &["s"], "a > 0"))
            .with_node(project_node("p", &["f"], &["a", "a", "b"]));

        let result = optimizer.optimize(plan).expect("optimize");
        assert!(!result.applied_rules.is_empty());
        assert!(
            result
                .applied_rules
                .contains(&"predicate-pushdown".to_string())
        );
        let project = result
            .plan
            .nodes()
            .iter()
            .find(|node| node.id() == "p")
            .expect("project");
        assert!(matches!(
            project.op(),
            Some(NodeOp::Project { columns }) if columns == &["a", "a", "b"]
        ));
    }

    #[test]
    fn default_logical_optimizer_empty_plan_noop() {
        let optimizer = default_logical_optimizer();
        let plan = LogicalPlan::new("test", ExecutionKind::Batch);
        let result = optimizer.optimize(plan.clone()).expect("optimize");
        assert_eq!(result.plan, plan);
        assert!(result.applied_rules.is_empty());
    }

    // ── CoalesceAdvice ─────────────────────────────────────────────────────

    #[test]
    fn coalesce_advice_default() {
        let advice = CoalesceAdvice { groups: Vec::new() };
        assert!(advice.groups.is_empty());
    }

    #[test]
    fn coalesce_advice_clone() {
        let advice = CoalesceAdvice {
            groups: vec![vec![0, 1], vec![2]],
        };
        let cloned = advice.clone();
        assert_eq!(advice, cloned);
    }

    #[test]
    fn coalesce_advice_equality() {
        let a = CoalesceAdvice {
            groups: vec![vec![0, 1]],
        };
        let b = CoalesceAdvice {
            groups: vec![vec![0, 1]],
        };
        let c = CoalesceAdvice {
            groups: vec![vec![1, 0]],
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn coalesce_advice_debug() {
        let advice = CoalesceAdvice {
            groups: vec![vec![0, 1]],
        };
        let debug = format!("{advice:?}");
        assert!(debug.contains("CoalesceAdvice"));
    }

    // ── SplitPlanAdvice ────────────────────────────────────────────────────

    #[test]
    fn split_plan_advice_default() {
        let advice = SplitPlanAdvice {
            task_groups: Vec::new(),
        };
        assert!(advice.task_groups.is_empty());
    }

    #[test]
    fn split_plan_advice_clone() {
        let advice = SplitPlanAdvice {
            task_groups: vec![vec!["a.parquet".to_owned()]],
        };
        let cloned = advice.clone();
        assert_eq!(advice, cloned);
    }

    // ── FileStats ──────────────────────────────────────────────────────────

    #[test]
    fn file_stats_equality() {
        let a = FileStats {
            path: "a.parquet".to_owned(),
            size_bytes: 100,
        };
        let b = FileStats {
            path: "a.parquet".to_owned(),
            size_bytes: 100,
        };
        let c = FileStats {
            path: "b.parquet".to_owned(),
            size_bytes: 100,
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn file_stats_debug() {
        let fs = FileStats {
            path: "test.parquet".to_owned(),
            size_bytes: 42,
        };
        let debug = format!("{fs:?}");
        assert!(debug.contains("test.parquet"));
        assert!(debug.contains("42"));
    }

    // ── Optimizer additional tests ──────────────────────────────────────────

    #[test]
    fn optimizer_rules_applied_in_order() {
        struct FirstRule;
        impl OptimizerRule for FirstRule {
            fn name(&self) -> &str {
                "first"
            }
            fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
                Some(
                    plan.clone()
                        .with_node(PlanNode::new("first", "first", ExecutionKind::Batch)),
                )
            }
        }

        struct SecondRule;
        impl OptimizerRule for SecondRule {
            fn name(&self) -> &str {
                "second"
            }
            fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
                assert!(
                    plan.nodes().iter().any(|n| n.id() == "first"),
                    "second rule should see first rule's node"
                );
                Some(plan.clone().with_node(PlanNode::new(
                    "second",
                    "second",
                    ExecutionKind::Batch,
                )))
            }
        }

        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(FirstRule));
        optimizer.add_rule(Box::new(SecondRule));

        let result = optimizer.optimize(empty_plan()).expect("optimize");
        assert_eq!(result.applied_rules, vec!["first", "second"]);
        assert_eq!(result.plan.nodes().len(), 2);
    }

    #[test]
    fn optimizer_many_rules_all_noop() {
        let mut optimizer = Optimizer::new();
        for _ in 0..100 {
            optimizer.add_rule(Box::new(NoOpRule));
        }
        let plan = plan_with_node();
        let result = optimizer.optimize(plan.clone()).expect("optimize");
        assert_eq!(result.plan, plan);
        assert!(result.applied_rules.is_empty());
    }

    #[test]
    fn optimize_result_describe_exact_format() {
        struct TestRule;
        impl OptimizerRule for TestRule {
            fn name(&self) -> &str {
                "test-rule"
            }
            fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
                Some(
                    plan.clone()
                        .with_node(PlanNode::new("n", "n", ExecutionKind::Batch)),
                )
            }
        }

        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(TestRule));

        let result = optimizer.optimize(empty_plan()).expect("optimize");
        assert_eq!(result.describe(), "optimizer applied: test-rule");
    }

    #[test]
    fn optimize_result_describe_empty() {
        let optimizer = Optimizer::new();
        let result = optimizer.optimize(empty_plan()).expect("optimize");
        assert_eq!(result.describe(), "optimizer: no rules applied");
    }

    // ── predicate pushdown through Join ────────────────────────────────────────

    fn join_node(id: &str, left: &str, right: &str) -> PlanNode {
        PlanNode::new(id, "join", ExecutionKind::Batch)
            .with_inputs(vec![left.to_string(), right.to_string()])
            .with_op(NodeOp::Join {
                join_type: crate::JoinType::Inner,
            })
    }

    #[test]
    fn predicate_pushdown_through_join_pushes_single_side_predicate() {
        // Filter(ts > 0) above Join(scan_users, scan_orders):
        // `ts` belongs only to scan_users → predicate pushed to scan_users, not scan_orders.
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema(
                "scan-users",
                "users",
                &[("user_id", FieldType::Utf8), ("ts", FieldType::Int64)],
            ))
            .with_node(scan_with_schema(
                "scan-orders",
                "orders",
                &[("order_id", FieldType::Int64), ("amount", FieldType::Int64)],
            ))
            .with_node(join_node("join", "scan-users", "scan-orders"))
            .with_node(filter_node("filter", &["join"], "ts > 0"));

        let result = PredicatePushdownRule.apply(&plan).unwrap();

        let users_scan = result
            .nodes()
            .iter()
            .find(|n| n.id() == "scan-users")
            .unwrap();
        if let Some(NodeOp::Scan { filters, .. }) = users_scan.op() {
            assert!(
                !filters.is_empty(),
                "predicate must be pushed into scan-users"
            );
            assert!(
                filters.iter().any(|f| f.contains("ts")),
                "pushed filter must reference ts"
            );
        } else {
            panic!("scan-users must have NodeOp::Scan");
        }

        let orders_scan = result
            .nodes()
            .iter()
            .find(|n| n.id() == "scan-orders")
            .unwrap();
        if let Some(NodeOp::Scan { filters, .. }) = orders_scan.op() {
            assert!(
                filters.is_empty(),
                "scan-orders must not receive ts predicate"
            );
        }
    }

    #[test]
    fn predicate_pushdown_through_join_removes_fully_owned_predicates() {
        // Filter(ts > 0 AND order_id > 100): ts on left, order_id on right.
        // Both single-side conjuncts are pushed into their respective scans.
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema("su", "users", &[("ts", FieldType::Int64)]))
            .with_node(scan_with_schema(
                "so",
                "orders",
                &[("order_id", FieldType::Int64)],
            ))
            .with_node(join_node("j", "su", "so"))
            .with_node(filter_node("f", &["j"], "ts > 0 AND order_id > 100"));

        let result = PredicatePushdownRule.apply(&plan).unwrap();

        let users = result.nodes().iter().find(|n| n.id() == "su").unwrap();
        let orders = result.nodes().iter().find(|n| n.id() == "so").unwrap();

        if let Some(NodeOp::Scan { filters, .. }) = users.op() {
            assert!(
                filters.iter().any(|f| f.contains("ts")),
                "ts predicate must be pushed into users scan"
            );
        }
        if let Some(NodeOp::Scan { filters, .. }) = orders.op() {
            assert!(
                filters.iter().any(|f| f.contains("order_id")),
                "order_id predicate must be pushed into orders scan"
            );
        }
        assert!(
            result.nodes().iter().all(|node| node.id() != "f"),
            "filter must be removed after every conjunct is pushed exactly once"
        );
    }

    #[test]
    fn predicate_pushdown_keeps_ambiguous_join_column_in_filter() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema(
                "left",
                "left_table",
                &[("id", FieldType::Int64)],
            ))
            .with_node(scan_with_schema(
                "right",
                "right_table",
                &[("id", FieldType::Int64)],
            ))
            .with_node(join_node("join", "left", "right"))
            .with_node(filter_node("filter", &["join"], "id > 0"));

        assert!(
            PredicatePushdownRule.apply(&plan).is_none(),
            "an unqualified column present on both join sides is not safe to push"
        );
    }

    #[test]
    fn predicate_pushdown_does_not_cross_outer_join() {
        let join = PlanNode::new("join", "left join", ExecutionKind::Batch)
            .with_inputs(["left", "right"])
            .with_op(NodeOp::Join {
                join_type: crate::JoinType::Left,
            });
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema(
                "left",
                "left_table",
                &[("left_id", FieldType::Int64)],
            ))
            .with_node(scan_with_schema(
                "right",
                "right_table",
                &[("right_id", FieldType::Int64)],
            ))
            .with_node(join)
            .with_node(filter_node("filter", &["join"], "right_id > 0"));

        assert!(
            PredicatePushdownRule.apply(&plan).is_none(),
            "pushing a post-join predicate through an outer join can change semantics"
        );
    }

    // ── StaticCostModel ───────────────────────────────────────────────────────

    use super::StaticCostModel;
    use crate::optimizer::CostModel;

    #[test]
    fn static_cost_model_empty_plan_is_zero() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch);
        let cost = StaticCostModel.estimate(&plan);
        assert_eq!(cost.cpu_nanos, 0);
        assert_eq!(cost.memory_bytes, 0);
        assert_eq!(cost.network_bytes, 0);
    }

    #[test]
    fn static_cost_model_scan_uses_estimated_rows() {
        let node = PlanNode::new("s1", "scan t", ExecutionKind::Batch)
            .with_estimated_rows(Some(1_000))
            .with_op(NodeOp::Scan {
                table: "t".into(),
                filters: vec![],
            });
        let plan = LogicalPlan::new("test", ExecutionKind::Batch).with_node(node);
        let cost = StaticCostModel.estimate(&plan);
        assert_eq!(cost.cpu_nanos, 1_000 * 10);
        assert_eq!(cost.memory_bytes, 1_000 * 64);
        assert_eq!(cost.network_bytes, 0);
    }

    #[test]
    fn static_cost_model_exchange_charges_network() {
        let node = PlanNode::new("e1", "exchange", ExecutionKind::Batch)
            .with_estimated_rows(Some(500))
            .with_op(NodeOp::Exchange {
                partitioning: crate::Partitioning::Hash {
                    keys: vec!["id".into()],
                    buckets: 4,
                },
            });
        let plan = LogicalPlan::new("test", ExecutionKind::Batch).with_node(node);
        let cost = StaticCostModel.estimate(&plan);
        assert_eq!(cost.network_bytes, 500 * 200);
        assert_eq!(cost.memory_bytes, 0);
    }

    #[test]
    fn static_cost_model_aggregate_uses_default_rows_when_unknown() {
        let node = PlanNode::new("a1", "agg", ExecutionKind::Batch).with_op(NodeOp::Aggregate {
            group_keys: vec!["k".into()],
        });
        let plan = LogicalPlan::new("test", ExecutionKind::Batch).with_node(node);
        let cost = StaticCostModel.estimate(&plan);
        // default = 10_000 rows
        assert_eq!(cost.cpu_nanos, 10_000 * 50);
        assert_eq!(cost.memory_bytes, 10_000 * 200);
    }

    #[test]
    fn static_cost_model_multi_node_plan_accumulates() {
        let scan = PlanNode::new("s1", "scan t", ExecutionKind::Batch)
            .with_estimated_rows(Some(1_000))
            .with_op(NodeOp::Scan {
                table: "t".into(),
                filters: vec![],
            });
        let agg = PlanNode::new("a1", "agg", ExecutionKind::Batch)
            .with_estimated_rows(Some(100))
            .with_op(NodeOp::Aggregate {
                group_keys: vec!["k".into()],
            });
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan)
            .with_node(agg);
        let cost = StaticCostModel.estimate(&plan);
        assert_eq!(cost.cpu_nanos, 1_000 * 10 + 100 * 50);
        assert_eq!(cost.memory_bytes, 1_000 * 64 + 100 * 200);
        assert_eq!(cost.network_bytes, 0);
    }
}
