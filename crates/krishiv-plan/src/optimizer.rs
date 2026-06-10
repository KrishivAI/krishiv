#![forbid(unsafe_code)]

//! Query optimizer traits and infrastructure for Krishiv.
//!
//! This crate defines the rule-based optimizer framework used by both the
//! logical and physical planning pipelines, as well as the AQE (Adaptive
//! Query Execution) extension traits that operate on runtime statistics
//! collected during stage execution.

use std::any::Any;
use std::collections::HashSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::{ExecutionKind, LogicalPlan, NodeOp, Partitioning, PhysicalPlan, PlanError, PlanNode};

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
    /// the plan is unchanged.
    fn apply(&self, plan: PhysicalPlan, stats: &[RuntimeStats]) -> Option<PhysicalPlan>;
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
                        message: panic_payload_message(payload),
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

fn panic_payload_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
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

// ── CoalesceRule ──────────────────────────────────────────────────────────────

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
}

/// Default target partition size: 128 MiB.
const DEFAULT_TARGET_PARTITION_BYTES: u64 = krishiv_common::partition::TARGET_BYTES_PER_PARTITION;

impl CoalesceRule {
    /// Create a new `CoalesceRule` with the given minimum partition byte threshold.
    ///
    /// Uses the default `target_partition_bytes` of 128 MiB.
    pub fn new(min_partition_bytes: u64) -> Self {
        Self {
            min_partition_bytes,
            target_partition_bytes: DEFAULT_TARGET_PARTITION_BYTES,
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
            let s = &stats[i];
            if s.serialized_bytes > 0 { s.serialized_bytes } else { s.memory_bytes }
        });

        let mut groups: Vec<Vec<usize>> = Vec::new();
        let mut current_small: Vec<usize> = Vec::new();
        let mut current_small_bytes = 0u128;
        let target_bytes = u128::from(self.target_partition_bytes.max(1));

        for i in order {
            let s = &stats[i];
            // Prefer serialized_bytes over memory_bytes for the same reason as
            // AutoPartitionRule: shuffle output is compressed and a better
            // proxy for actual partition cost than peak in-memory footprint.
            let effective_bytes = if s.serialized_bytes > 0 { s.serialized_bytes } else { s.memory_bytes };
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
    fn apply(&self, plan: PhysicalPlan, stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
        if stats.is_empty() || StreamingAqeGuard::plan_is_streaming(&plan) {
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
            let terminal = &plan.nodes()[terminal_index];
            if matches!(terminal.op(), Some(NodeOp::CoalescePartitions { .. })) {
                return Some(terminal_index);
            }
            if matches!(terminal.op(), Some(NodeOp::Sink { .. })) && terminal.inputs().len() == 1 {
                let input_id = &terminal.inputs()[0];
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
        {
            coalesce_inputs.push(plan.nodes()[terminal_index].id().to_string());
        }
        rewritten.add_node(
            PlanNode::new(coalesce_id, label, plan.kind())
                .with_inputs(coalesce_inputs)
                .with_op(NodeOp::CoalescePartitions { target_partitions }),
        );
        Some(rewritten.with_coalesced_partition_count(target_partitions))
    }
}

// ── AutoPartitionRule ───────────────────────────────────────────────────────────

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

    fn apply(&self, plan: PhysicalPlan, stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
        // When an explicit shuffle_partitions override is set on the plan
        // (via SET shuffle.partitions = N or SessionBuilder), use it as the
        // target bucket count regardless of stats.  Stats may be empty on the
        // first execution and that's fine — the override is a user intent.
        if let Some(override_buckets) = plan.shuffle_partitions() {
            return self.apply_override(plan, override_buckets);
        }

        if stats.is_empty() || StreamingAqeGuard::plan_is_streaming(&plan) {
            return None;
        }

        // Sum the best available size metric across all partitions.
        // Prefer serialized_bytes (shuffle wire size) over memory_bytes (peak
        // in-memory) because shuffle output is compressed/serialized and thus
        // a more accurate proxy for partition cost. Fall back to memory_bytes
        // when serialized_bytes is zero (non-shuffle tasks or older executors).
        let total_bytes: u64 = stats
            .iter()
            .map(|s| if s.serialized_bytes > 0 { s.serialized_bytes } else { s.memory_bytes })
            .sum();
        if total_bytes == 0 {
            return None;
        }

        // Compute target partition count.
        let target =
            u64::from(self.max_buckets).min(
                (total_bytes + self.target_partition_bytes - 1) / self.target_partition_bytes,
            );
        let target = target.max(1) as u32;

        self.stamp_target(plan, target)
    }
}

impl AutoPartitionRule {
    /// Apply the rule with an explicit override bucket count.
    /// Skips streaming plans, but does not require runtime stats.
    fn apply_override(&self, plan: PhysicalPlan, target: u32) -> Option<PhysicalPlan> {
        if StreamingAqeGuard::plan_is_streaming(&plan) {
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
    fn stamp_target(&self, plan: PhysicalPlan, target: u32) -> Option<PhysicalPlan> {
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

        let mut plan = plan;
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
                    node.set_partitioning(Partitioning::RoundRobin {
                        buckets: target,
                    });
                }
                _ => {}
            }
        }

        tracing::debug!(
            rule = "auto-partition",
            target,
            "AutoPartitionRule applied"
        );

        Some(plan)
    }
}

// ── SmallFilePlanner ──────────────────────────────────────────────────────────

/// Per-file metadata used by [`SmallFilePlanner`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStats {
    pub path: String,
    pub size_bytes: u64,
}

/// Advice produced by [`SmallFilePlanner`]: a list of scan groups where each
/// group of file paths should be handled by a single executor task.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitPlanAdvice {
    /// Each inner `Vec` is one task's worth of files.
    pub task_groups: Vec<Vec<String>>,
}

/// Plans scan parallelism for a set of files.
///
/// When individual files are smaller than `target_bytes`, multiple files are
/// grouped into a single task so each task processes roughly `target_bytes` of
/// data. Files larger than `target_bytes` each get their own task (splitting
/// within a file is not yet supported).
pub struct SmallFilePlanner {
    target_bytes: u64,
}

impl SmallFilePlanner {
    /// Create a planner with the given target bytes per task.
    pub fn new(target_bytes: u64) -> Self {
        Self { target_bytes }
    }

    /// Produce a scan plan for the given file list.
    ///
    /// Files are grouped greedily: accumulate until the next file would push the
    /// group over `target_bytes`, then start a new group. This ensures each
    /// group is at most `target_bytes + max_single_file_bytes`.
    pub fn plan(&self, files: &[FileStats]) -> SplitPlanAdvice {
        if files.is_empty() {
            return SplitPlanAdvice {
                task_groups: Vec::new(),
            };
        }

        let mut groups: Vec<Vec<String>> = Vec::new();
        let mut current: Vec<String> = Vec::new();
        let mut current_bytes = 0u128;
        let target_bytes = u128::from(self.target_bytes);

        for file in files {
            let file_bytes = u128::from(file.size_bytes);
            if !current.is_empty() && current_bytes + file_bytes > target_bytes {
                groups.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            current.push(file.path.clone());
            current_bytes += file_bytes;
        }
        if !current.is_empty() {
            groups.push(current);
        }

        SplitPlanAdvice {
            task_groups: groups,
        }
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
}

impl AqeOptimizer {
    /// Create an empty AQE optimizer.
    pub fn new() -> Self {
        Self {
            always_rules: Vec::new(),
            guarded_rules: Vec::new(),
        }
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

        for rule in &self.always_rules {
            let rule_name = rule.name().to_string();
            let outcome = catch_unwind(AssertUnwindSafe(|| rule.apply(current.clone(), stats)))
                .map_err(|payload| OptimizerError::RulePanicked {
                    optimizer: "AQE",
                    rule: rule_name.clone(),
                    message: panic_payload_message(payload),
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
                let outcome = catch_unwind(AssertUnwindSafe(|| rule.apply(current.clone(), stats)))
                    .map_err(|payload| OptimizerError::RulePanicked {
                        optimizer: "AQE",
                        rule: rule_name.clone(),
                        message: panic_payload_message(payload),
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

// ── Logical optimizer rules ─────────────────────────────────────────────────

/// Push `Filter` predicates down into `TableScan` nodes.
///
/// Walks the logical plan looking for `Filter` nodes and decomposes each
/// filter's predicate into AND-conjuncts. Conjuncts that reference only
/// columns present in one scan's output schema are pushed into that scan
/// node's `filters` list. If all conjuncts are pushed the `Filter` node is
/// removed; remaining cross-join conjuncts stay in place.
///
/// Two patterns are handled:
/// - **Filter-above-Scan**: filter's direct input is a scan.
/// - **Filter-above-Join**: filter sits above a join; each conjunct is tested
///   against the left and right scan inputs independently and pushed as far
///   down as it can go. Cross-join predicates (referencing both sides) remain
///   in the filter.
/// Default threshold for auto-broadcast: tables with estimated rows below
/// this value are candidates for broadcast join.  ~1M rows ≈ 100 MiB at 100
/// bytes/row.
const DEFAULT_BROADCAST_THRESHOLD_ROWS: u64 = 1_000_000;

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
                && node.estimated_rows().map_or(false, |r| r <= self.max_rows);

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

pub struct PredicatePushdownRule;

impl OptimizerRule for PredicatePushdownRule {
    fn name(&self) -> &str {
        "predicate-pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let nodes = plan.nodes().to_vec();
        let id_to_idx: std::collections::HashMap<&str, usize> =
            nodes.iter().enumerate().map(|(i, n)| (n.id(), i)).collect();

        // Collect pushdown candidates: filter nodes whose input is a scan.
        struct FilterPushdown {
            filter_idx: usize,
            scan_pushes: Vec<(usize, Vec<String>)>,
            remaining: Vec<String>,
        }

        let mut pushdowns: Vec<FilterPushdown> = Vec::new();

        for (i, node) in nodes.iter().enumerate() {
            let predicate = match node.op() {
                Some(NodeOp::Filter { predicate }) => predicate.clone(),
                _ => continue,
            };

            // Collect all scan nodes reachable in one or two hops from this
            // filter. One hop covers Filter-above-Scan; two hops covers
            // Filter-above-Join-above-Scan so each side of the join can
            // independently receive the conjuncts that belong to it.
            let direct_inputs: Vec<usize> = node
                .inputs()
                .iter()
                .filter_map(|input_id| id_to_idx.get(input_id.as_str()).copied())
                .collect();

            let mut scan_indices: Vec<usize> = direct_inputs
                .iter()
                .copied()
                .filter(|&idx| matches!(nodes[idx].op(), Some(NodeOp::Scan { .. })))
                .collect();

            // Filter-above-Join: descend through join nodes to collect
            // both left and right scan inputs for per-side pushdown.
            for join_idx in direct_inputs.iter().copied().filter(|&idx| {
                matches!(
                    nodes[idx].op(),
                    Some(NodeOp::Join {
                        join_type: crate::JoinType::Inner
                    })
                )
            }) {
                for child_id in nodes[join_idx].inputs() {
                    if let Some(&child_idx) = id_to_idx.get(child_id.as_str()) {
                        if matches!(nodes[child_idx].op(), Some(NodeOp::Scan { .. })) {
                            scan_indices.push(child_idx);
                        }
                    }
                }
            }
            scan_indices.sort_unstable();
            scan_indices.dedup();

            if scan_indices.is_empty() {
                continue;
            }

            // C5: Use sqlparser to split predicate conjuncts properly
            // instead of naively splitting on the literal string " AND ".
            let conjuncts = split_predicate_conjuncts(&predicate);

            if conjuncts.is_empty() {
                continue;
            }

            let scan_contracts = scan_indices
                .iter()
                .map(|&scan_idx| {
                    let scan_node = &nodes[scan_idx];
                    let columns = scan_node
                        .output_schema()
                        .fields()
                        .iter()
                        .map(|field| field.name())
                        .collect::<Vec<_>>();
                    let table = match scan_node.op() {
                        Some(NodeOp::Scan { table, .. }) => table.as_str(),
                        _ => "",
                    };
                    (scan_idx, table, columns)
                })
                .collect::<Vec<_>>();
            let mut scan_pushes = std::collections::HashMap::<usize, Vec<String>>::new();
            let mut remaining = Vec::new();

            for conjunct in conjuncts {
                let columns = extract_column_refs(&conjunct);
                let matching_scans = scan_contracts
                    .iter()
                    .filter_map(|(scan_idx, table, scan_columns)| {
                        (!columns.is_empty()
                            && columns
                                .iter()
                                .all(|column| column_belongs_to_scan(column, table, scan_columns)))
                        .then_some(*scan_idx)
                    })
                    .collect::<Vec<_>>();
                if let [scan_idx] = matching_scans.as_slice() {
                    scan_pushes.entry(*scan_idx).or_default().push(conjunct);
                } else {
                    remaining.push(conjunct);
                }
            }

            if !scan_pushes.is_empty() {
                let mut scan_pushes = scan_pushes.into_iter().collect::<Vec<_>>();
                scan_pushes.sort_by_key(|(scan_idx, _)| *scan_idx);
                pushdowns.push(FilterPushdown {
                    filter_idx: i,
                    scan_pushes,
                    remaining,
                });
            }
        }

        if pushdowns.is_empty() {
            return None;
        }

        let mut new_nodes = nodes.clone();
        let mut to_remove: Vec<usize> = Vec::new();

        for pd in &pushdowns {
            for (scan_idx, pushable) in &pd.scan_pushes {
                if let Some(NodeOp::Scan { table, filters }) = new_nodes[*scan_idx].op() {
                    let mut new_filters = filters.clone();
                    new_filters.extend(pushable.iter().cloned());
                    new_nodes[*scan_idx] = new_nodes[*scan_idx].clone().with_op(NodeOp::Scan {
                        table: table.clone(),
                        filters: new_filters,
                    });
                }
            }

            if pd.remaining.is_empty() {
                to_remove.push(pd.filter_idx);
            } else {
                new_nodes[pd.filter_idx] =
                    new_nodes[pd.filter_idx].clone().with_op(NodeOp::Filter {
                        predicate: pd.remaining.join(" AND "),
                    });
            }
        }

        // Remove filter nodes and rewire downstream node inputs.
        for &idx in to_remove.iter().rev() {
            let filter_id = new_nodes[idx].id().to_string();
            let filter_inputs: Vec<String> = new_nodes[idx].inputs().to_vec();
            new_nodes.remove(idx);

            for node in &mut new_nodes {
                let inputs: Vec<String> = node.inputs().to_vec();
                if inputs.contains(&filter_id) {
                    let new_inputs: Vec<String> = inputs
                        .iter()
                        .flat_map(|input| {
                            if input == &filter_id {
                                filter_inputs.clone()
                            } else {
                                vec![input.clone()]
                            }
                        })
                        .collect();
                    *node = node.clone().with_inputs(new_inputs);
                }
            }
        }

        let mut out = LogicalPlan::new(plan.name(), plan.kind());
        for node in new_nodes {
            out.add_node(node);
        }
        Some(out)
    }
}

/// Extract likely column-name identifiers from a predicate expression string.
///
/// Skips string literals and function names, retaining unquoted and quoted
/// identifier paths such as `column` and `table.column`.
fn extract_column_refs(predicate: &str) -> Vec<String> {
    const SQL_KEYWORDS: &[&str] = &[
        "AND", "OR", "NOT", "IN", "IS", "NULL", "TRUE", "FALSE", "WHERE", "SELECT", "FROM", "AS",
        "ON", "BETWEEN", "LIKE", "EXISTS", "HAVING", "GROUP", "ORDER", "BY", "ASC", "DESC",
        "LIMIT", "OFFSET", "DISTINCT", "ALL", "ANY", "SOME", "CASE", "WHEN", "THEN", "ELSE", "END",
        "CAST",
    ];

    let chars = predicate.char_indices().collect::<Vec<_>>();
    let mut refs = Vec::new();
    let mut cursor = 0usize;
    while cursor < chars.len() {
        let (_, ch) = chars[cursor];
        if ch == '\'' {
            cursor += 1;
            while cursor < chars.len() {
                if chars[cursor].1 == '\'' {
                    if cursor + 1 < chars.len() && chars[cursor + 1].1 == '\'' {
                        cursor += 2;
                        continue;
                    }
                    cursor += 1;
                    break;
                }
                cursor += 1;
            }
            continue;
        }
        if ch == '"' || ch == '`' {
            let quote = ch;
            let start = chars[cursor].0 + ch.len_utf8();
            cursor += 1;
            while cursor < chars.len() && chars[cursor].1 != quote {
                cursor += 1;
            }
            let end = chars
                .get(cursor)
                .map_or(predicate.len(), |(offset, _)| *offset);
            if end > start {
                refs.push(predicate[start..end].to_string());
            }
            cursor = cursor.saturating_add(1);
            continue;
        }
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = chars[cursor].0;
            cursor += 1;
            while cursor < chars.len()
                && (chars[cursor].1.is_ascii_alphanumeric()
                    || chars[cursor].1 == '_'
                    || chars[cursor].1 == '.')
            {
                cursor += 1;
            }
            let end = chars
                .get(cursor)
                .map_or(predicate.len(), |(offset, _)| *offset);
            let token = &predicate[start..end];
            let next_non_whitespace = chars[cursor..]
                .iter()
                .find_map(|(_, next)| (!next.is_whitespace()).then_some(*next));
            if next_non_whitespace != Some('(')
                && !SQL_KEYWORDS.contains(&token.to_uppercase().as_str())
                && !refs.iter().any(|existing| existing == token)
            {
                refs.push(token.to_string());
            }
            continue;
        }
        cursor += 1;
    }
    refs
}

/// C5: Split a SQL predicate string into conjuncts using sqlparser for correct
/// AND splitting.  Respects quoted strings, nested expressions, etc.
fn split_predicate_conjuncts(predicate: &str) -> Vec<String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let expression = predicate
        .strip_prefix("WHERE ")
        .or_else(|| predicate.strip_prefix("where "))
        .unwrap_or(predicate);
    let statement = format!("SELECT * FROM __krishiv_predicate WHERE {expression}");
    let Ok(mut stmts) = Parser::parse_sql(&dialect, &statement) else {
        return Vec::new();
    };
    let Some(stmt) = stmts.pop() else {
        return vec![predicate.to_string()];
    };
    // Extract the expression and split on top-level AND.
    let sqlparser::ast::Statement::Query(query) = stmt else {
        return vec![predicate.to_string()];
    };
    let Some(select_body) = query.body.as_select() else {
        return vec![predicate.to_string()];
    };
    let Some(selection) = &select_body.selection else {
        return vec![predicate.to_string()];
    };
    collect_binary_conjuncts(selection, "AND")
}

/// Recursively collect top-level conjuncts from a binary expression tree.
fn collect_binary_conjuncts(expr: &sqlparser::ast::Expr, op: &str) -> Vec<String> {
    match expr {
        sqlparser::ast::Expr::BinaryOp {
            left,
            op: bin_op,
            right,
        } if bin_op.to_string().to_uppercase() == op => {
            let mut left_conjuncts = collect_binary_conjuncts(left, op);
            let right_conjuncts = collect_binary_conjuncts(right, op);
            left_conjuncts.extend(right_conjuncts);
            left_conjuncts
        }
        other => {
            vec![other.to_string()]
        }
    }
}

/// Check whether `col` (possibly qualified like `"t.id"`) belongs to `scan_table`
/// with the given column names.  C5: When a column reference has an explicit
/// qualifier, require an exact case-insensitive table match. Aliases are not
/// represented in `PlanNode`, so guessing them would permit unsafe pushdown.
fn column_belongs_to_scan(col: &str, scan_table: &str, scan_columns: &[&str]) -> bool {
    if let Some(dot_pos) = col.rfind('.') {
        let qualifier = &col[..dot_pos];
        let unqualified = &col[dot_pos + 1..];
        if !qualifier.is_empty() {
            let scan_lower = scan_table.to_ascii_lowercase();
            let qual_lower = qualifier.to_ascii_lowercase();
            if qual_lower == scan_lower {
                return scan_columns.contains(&unqualified);
            }
            // Reject qualification that doesn't match this table at all.
            return false;
        }
        return scan_columns.contains(&unqualified);
    }
    scan_columns.contains(&col)
}

/// Default logical optimizer with semantics-preserving rules enabled.
pub fn default_logical_optimizer() -> Optimizer {
    let mut optimizer = Optimizer::new();
    optimizer.add_rule(Box::new(BroadcastAutoRule::new(DEFAULT_BROADCAST_THRESHOLD_ROWS)));
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
        let result = rule.apply(plan, &stats).expect("coalesce should fire");
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
        let result = rule.apply(plan, &stats);
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

        let first = rule.apply(plan, &stats).expect("first rewrite");
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

        let second = rule.apply(first.clone(), &stats).expect("second rewrite");
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

        assert!(CoalesceRule::new(100).apply(plan, &stats).is_none());
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
        let rewritten = AqeRule::apply(&rule, plan, &stats).expect("coalesce should fire");

        // The plan must have had a CoalescePartitions node appended.
        let coalesce_node = rewritten
            .nodes()
            .iter()
            .find(|n: &&crate::PlanNode| {
                matches!(n.op(), Some(NodeOp::CoalescePartitions { .. }))
            });

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
        let rewritten = AqeRule::apply(&rule, plan, &stats);

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
            PlanNode::new("xchg", "exchange", ExecutionKind::Batch)
                .with_partitioning(Partitioning::Hash {
                    keys: vec!["k".into()],
                    buckets: 4,
                }),
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
        let result = rule.apply(plan.clone(), &stats).expect("rule must fire");

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
            RuntimeStats { memory_bytes: 50, serialized_bytes: 500, ..Default::default() },
            // memory=50 (< 100), serialized=0 → fall back to memory → small
            RuntimeStats { memory_bytes: 50, serialized_bytes: 0, ..Default::default() },
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
        let result = rule.apply(plan, &[]);
        assert!(result.is_none());
    }

    #[test]
    fn coalesce_rule_apply_single_partition() {
        let stats = make_stats_with_memory(&[100]);
        let rule = CoalesceRule::new(1000);
        let plan = PhysicalPlan::new("test", ExecutionKind::Batch);
        let result = rule.apply(plan, &stats);
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
        let result = rule.apply(plan, &stats);
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
        let result = rule.apply(plan, &stats);
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
        let result = rule.apply(plan, &stats).unwrap();
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
        let result = rule.apply(plan, &stats);
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
        fn apply(&self, plan: PhysicalPlan, _stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
            let node_id = format!("extra-{}", plan.nodes().len());
            Some(plan.with_node(PlanNode::new(node_id, "extra", ExecutionKind::Batch)))
        }
    }

    struct NeverFireRule;

    impl AqeRule for NeverFireRule {
        fn name(&self) -> &str {
            "never-fire"
        }
        fn apply(&self, _plan: PhysicalPlan, _stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
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

            fn apply(&self, plan: PhysicalPlan, _stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
                Some(
                    plan.with_node(
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

            fn apply(&self, _plan: PhysicalPlan, _stats: &[RuntimeStats]) -> Option<PhysicalPlan> {
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
                let schema = crate::PlanSchema::new(vec![crate::SchemaField::new(
                    "a",
                    FieldType::Int32,
                )]);
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
}
