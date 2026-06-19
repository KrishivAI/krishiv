#![forbid(unsafe_code)]

//! Query optimizer traits and infrastructure for Krishiv.
//!
//! This crate defines the rule-based optimizer framework used by both the
//! logical and physical planning pipelines, as well as the AQE (Adaptive
//! Query Execution) extension traits that operate on runtime statistics
//! collected during stage execution.

mod auto_partition;
mod broadcast;
mod broadcast_runtime;
mod coalesce;
mod join_reorder;
mod predicate_pushdown;
mod small_file;

#[cfg(test)]
mod optimizer_tests;

pub use auto_partition::AutoPartitionRule;
pub use broadcast::{BroadcastAutoRule, DEFAULT_BROADCAST_THRESHOLD_ROWS};
pub use broadcast_runtime::{BroadcastRuntimeRule, DEFAULT_MAX_BROADCAST_BYTES};
pub use coalesce::{CoalesceAdvice, CoalesceRule};
pub use join_reorder::JoinReorderRule;
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

        if !input_is_streaming && !StreamingAqeGuard::plan_is_streaming(&current) {
            for rule in &self.guarded_rules {
                let rule_name = rule.name().to_string();
                let outcome =
                    catch_unwind(AssertUnwindSafe(|| rule.apply(&current, effective_stats)))
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
    // 1. Push filters into scans so that estimated_rows on scan nodes reflect
    //    the actual filtered size before join ordering kicks in.
    optimizer.add_rule(Box::new(PredicatePushdownRule));
    // 2. Mark small scan nodes as broadcast-eligible (uses estimated_rows).
    optimizer.add_rule(Box::new(BroadcastAutoRule::new(
        DEFAULT_BROADCAST_THRESHOLD_ROWS,
    )));
    // 3. Reorder commutative join inputs so the smaller table is on the left,
    //    minimising intermediate result sizes in left-deep join trees.
    optimizer.add_rule(Box::new(JoinReorderRule));
    optimizer
}

/// Default AQE optimizer with guarded coalescing and the streaming guard.
///
/// Includes `BroadcastRuntimeRule`, `AutoPartitionRule`, and `CoalesceRule`
/// as guarded rules (skipped for streaming plans).  Rules that require
/// runtime statistics will be no-ops until stats feed is wired (see
/// `AqeOptimizer::apply`).
pub fn default_aqe_optimizer() -> AqeOptimizer {
    let mut optimizer = AqeOptimizer::new();
    // 1. Promote/demote broadcast joins from observed sizes before bucket
    //    counts are re-derived, so AutoPartitionRule sees the final exchange
    //    shape (a promoted Broadcast node is no longer a Hash/RoundRobin
    //    candidate for bucket stamping).
    optimizer.add_guarded_rule(Box::new(BroadcastRuntimeRule::new(
        DEFAULT_MAX_BROADCAST_BYTES,
    )));
    optimizer.add_guarded_rule(Box::new(AutoPartitionRule::new(64)));
    optimizer.add_guarded_rule(Box::new(CoalesceRule::new(64 * 1024 * 1024)));
    optimizer
}
