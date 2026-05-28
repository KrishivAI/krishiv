#![forbid(unsafe_code)]

//! Query optimizer traits and infrastructure for Krishiv.
//!
//! This crate defines the rule-based optimizer framework used by both the
//! logical and physical planning pipelines, as well as the AQE (Adaptive
//! Query Execution) extension traits that operate on runtime statistics
//! collected during stage execution.

use krishiv_plan::{ExecutionKind, LogicalPlan, NodeOp, PhysicalPlan, PlanNode};

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

/// A rule that applies streaming-specific rewrites to a [`LogicalPlan`].
// P3.19: StreamRule has no implementations in this workspace; kept for forward
// compatibility but suppressed from dead-code warnings.
#[allow(dead_code)]
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
    ///
    /// P2.4: rules signal no-change by returning `None`, avoiding an O(rules ×
    /// plan_size) clone-per-rule cycle.
    pub fn optimize(&self, plan: LogicalPlan) -> OptimizeResult {
        let mut current = plan;
        let mut applied_rules = Vec::new();

        for rule in &self.rules {
            if let Some(new_plan) = rule.apply(&current) {
                applied_rules.push(rule.name().to_string());
                current = new_plan;
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
            (rows[mid - 1] + rows[mid]) as f64 / 2.0
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
const DEFAULT_TARGET_PARTITION_BYTES: u64 = 134_217_728;

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

    /// Compute the target partition count based on total bytes and `target_partition_bytes`.
    ///
    /// Returns `ceil(total_bytes / target_partition_bytes)`, with a minimum of 1.
    fn target_partitions_from_stats(&self, stats: &[RuntimeStats]) -> usize {
        let total_bytes: u64 = stats.iter().map(|s| s.memory_bytes).sum();
        if total_bytes == 0 || self.target_partition_bytes == 0 {
            return 1;
        }
        // ceiling division
        total_bytes.div_ceil(self.target_partition_bytes).max(1) as usize
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
        if stats.is_empty() {
            return None;
        }
        let advice = self.advise(stats);
        let original_count = stats.len();

        if advice.groups.len() >= original_count || original_count == 0 {
            return None;
        }

        let target_partitions = self.target_partitions_from_stats(stats);

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

        let coalesce_node = PlanNode::new(
            "coalesce",
            format!("CoalescePartitions({original_count} → {target_partitions})"),
            ExecutionKind::Batch,
        )
        .with_op(NodeOp::CoalescePartitions { target_partitions });

        Some(
            plan.with_node(coalesce_node)
                .with_coalesced_partition_count(advice.groups.len()),
        )
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
        let mut current_bytes: u64 = 0;

        for file in files {
            if !current.is_empty() && current_bytes + file.size_bytes > self.target_bytes {
                groups.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            current.push(file.path.clone());
            current_bytes += file.size_bytes;
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
/// use krishiv_optimizer::{AqeOptimizer, CoalesceRule, StreamingAqeGuard};
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
    pub fn apply(&self, plan: PhysicalPlan, stats: &[RuntimeStats]) -> (PhysicalPlan, Vec<String>) {
        let is_streaming = StreamingAqeGuard::plan_is_streaming(&plan);
        let mut current = plan;
        let mut applied = Vec::new();

        for rule in &self.always_rules {
            if let Some(new_plan) = rule.apply(current.clone(), stats) {
                applied.push(rule.name().to_string());
                current = new_plan;
            }
        }

        if !is_streaming {
            for rule in &self.guarded_rules {
                if let Some(new_plan) = rule.apply(current.clone(), stats) {
                    applied.push(rule.name().to_string());
                    current = new_plan;
                }
            }
        }

        (current, applied)
    }
}

impl Default for AqeOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

// ── Production optimizer rules (P2-3) ───────────────────────────────────────────

/// Remove duplicate column names from `Project` nodes while preserving original order.
pub struct ProjectionPruningRule;

impl OptimizerRule for ProjectionPruningRule {
    fn name(&self) -> &str {
        "projection-pruning"
    }

    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let mut nodes: Vec<PlanNode> = plan.nodes().to_vec();
        let mut changed = false;
        for node in &mut nodes {
            if let Some(NodeOp::Project { columns }) = node.op() {
                let original_len = columns.len();
                let mut seen = std::collections::HashSet::new();
                let mut pruned = columns.clone();
                pruned.retain(|c| seen.insert(c.clone()));
                if pruned.len() != original_len {
                    changed = true;
                    *node = node.clone().with_op(NodeOp::Project { columns: pruned });
                }
            }
        }
        changed.then(|| {
            let mut out = LogicalPlan::new(plan.name(), plan.kind());
            for node in nodes {
                out.add_node(node);
            }
            out
        })
    }
}

/// Push `Filter` predicates down into `TableScan` nodes.
///
/// Walks the logical plan looking for `Filter` nodes whose direct input is a
/// `Scan` node. The filter's predicate is decomposed into AND-conjuncts, and
/// any conjuncts that reference only columns present in the scan's output
/// schema are pushed into the scan node's `filters` list. If all conjuncts
/// are pushed, the `Filter` node is removed from the plan; remaining conjuncts
/// stay in place.
///
/// Pushdown through join nodes is not yet implemented — only the
/// Filter-above-Scan pattern is handled.
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
        struct Pushdown {
            filter_idx: usize,
            scan_idx: usize,
            pushable: Vec<String>,
            remaining: Vec<String>,
        }

        let mut pushdowns: Vec<Pushdown> = Vec::new();

        for (i, node) in nodes.iter().enumerate() {
            let predicate = match node.op() {
                Some(NodeOp::Filter { predicate }) => predicate.clone(),
                _ => continue,
            };

            let scan_info = node
                .inputs()
                .iter()
                .filter_map(|input_id| id_to_idx.get(input_id.as_str()).copied())
                .find(|&idx| matches!(nodes[idx].op(), Some(NodeOp::Scan { .. })));

            let Some(scan_idx) = scan_info else {
                continue;
            };
            let scan_node = &nodes[scan_idx];

            let conjuncts: Vec<&str> = predicate
                .split(" AND ")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();

            if conjuncts.is_empty() {
                continue;
            }

            let scan_columns: Vec<&str> = scan_node
                .output_schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect();

            let mut pushable: Vec<String> = Vec::new();
            let mut remaining: Vec<String> = Vec::new();

            for conjunct in &conjuncts {
                let cols = extract_column_refs(conjunct);
                let can_push = !cols.is_empty()
                    && cols
                        .iter()
                        .all(|c| column_belongs_to_scan(c, &scan_columns));

                if can_push {
                    pushable.push(conjunct.to_string());
                } else {
                    remaining.push(conjunct.to_string());
                }
            }

            if !pushable.is_empty() {
                pushdowns.push(Pushdown {
                    filter_idx: i,
                    scan_idx,
                    pushable,
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
            // Push conjuncts into the scan node's filters list.
            if let Some(NodeOp::Scan { table, filters }) = new_nodes[pd.scan_idx].op() {
                let mut new_filters = filters.clone();
                new_filters.extend(pd.pushable.iter().cloned());
                new_nodes[pd.scan_idx] = new_nodes[pd.scan_idx].clone().with_op(NodeOp::Scan {
                    table: table.clone(),
                    filters: new_filters,
                });
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
/// Splits on non-alphanumeric characters (except `_` and `.`) and keeps tokens
/// that contain at least one ASCII letter and are not SQL reserved words.
fn extract_column_refs(predicate: &str) -> Vec<String> {
    const SQL_KEYWORDS: &[&str] = &[
        "AND", "OR", "NOT", "IN", "IS", "NULL", "TRUE", "FALSE", "WHERE", "SELECT", "FROM", "AS",
        "ON", "BETWEEN", "LIKE", "EXISTS", "HAVING", "GROUP", "ORDER", "BY", "ASC", "DESC",
        "LIMIT", "OFFSET", "DISTINCT", "ALL", "ANY", "SOME", "CASE", "WHEN", "THEN", "ELSE", "END",
        "CAST",
    ];

    let mut refs: Vec<String> = predicate
        .split(|c: char| !c.is_alphanumeric() && c != '_' && c != '.')
        .filter(|w| !w.is_empty())
        .filter(|w| w.chars().any(|c| c.is_ascii_alphabetic()))
        .filter(|w| !SQL_KEYWORDS.contains(&w.to_uppercase().as_str()))
        .map(|w| w.to_string())
        .collect();

    refs.dedup();
    refs
}

/// Check whether `col` (possibly qualified like `"t.id"`) matches any of the
/// scan's column names, either by full match or by unqualified suffix.
fn column_belongs_to_scan(col: &str, scan_columns: &[&str]) -> bool {
    if scan_columns.contains(&col) {
        return true;
    }
    if let Some(dot_pos) = col.rfind('.') {
        let unqualified = &col[dot_pos + 1..];
        return scan_columns.contains(&unqualified);
    }
    false
}

/// Drop no-op `Project` nodes that select zero columns.
pub struct EmptyProjectionRemovalRule;

impl OptimizerRule for EmptyProjectionRemovalRule {
    fn name(&self) -> &str {
        "empty-projection-removal"
    }

    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let mut nodes: Vec<PlanNode> = plan.nodes().to_vec();
        let before = nodes.len();
        nodes.retain(|node| {
            !matches!(
                node.op(),
                Some(NodeOp::Project { columns }) if columns.is_empty()
            )
        });
        if nodes.len() == before {
            return None;
        }
        let mut out = LogicalPlan::new(plan.name(), plan.kind());
        for node in nodes {
            out.add_node(node);
        }
        Some(out)
    }
}

/// Default logical optimizer with production rules enabled.
pub fn default_logical_optimizer() -> Optimizer {
    let mut optimizer = Optimizer::new();
    optimizer.add_rule(Box::new(ProjectionPruningRule));
    optimizer.add_rule(Box::new(PredicatePushdownRule));
    optimizer.add_rule(Box::new(EmptyProjectionRemovalRule));
    optimizer
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use krishiv_plan::{ExecutionKind, FieldType, LogicalPlan, NodeOp, PhysicalPlan, PlanNode};

    use super::{
        AqeOptimizer, AqeRule, CoalesceRule, Optimizer, OptimizerRule, RuntimeStats,
        StreamingAqeGuard,
    };

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

        fn apply(&self, _plan: &LogicalPlan) -> Option<LogicalPlan> {
            None
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
        assert!(
            result.is_none(),
            "no coalescing should return None"
        );
    }

    // ── SmallFilePlanner ──────────────────────────────────────────────────

    use super::{FileStats, SmallFilePlanner};

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
        let (_, batch_fired) = aqe.apply(batch_plan(), &stats);
        let (_, stream_fired) = aqe.apply(streaming_plan(), &stats);

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
        let (returned_plan, _) = aqe.apply(plan.clone(), &stats);
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
        use krishiv_plan::NodeOp;

        use crate::AqeRule;

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
            .find(|n: &&krishiv_plan::PlanNode| {
                matches!(n.op(), Some(NodeOp::CoalescePartitions { .. }))
            });

        assert!(
            coalesce_node.is_some(),
            "expected a CoalescePartitions node to be inserted"
        );

        // Extract target_partitions from the node and verify it is ≤ 10.
        if let Some(NodeOp::CoalescePartitions { target_partitions }) =
            coalesce_node.and_then(|n: &krishiv_plan::PlanNode| n.op())
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
        use crate::AqeRule;

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
        let plan_clone = plan.clone();
        let rewritten = AqeRule::apply(&rule, plan, &stats);

        // No coalescing: plan must be returned unchanged (None).
        assert!(
            rewritten.is_none(),
            "plan must be None when no partitions are small"
        );
    }

    // ── ProjectionPruningRule ─────────────────────────────────────────────

    use super::ProjectionPruningRule;

    fn scan_with_schema(
        id: &str,
        table: &str,
        schema_fields: &[(&str, krishiv_plan::FieldType)],
    ) -> PlanNode {
        let schema = krishiv_plan::PlanSchema::new(
            schema_fields
                .iter()
                .map(|(name, ft)| krishiv_plan::SchemaField::new(*name, ft.clone()))
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

    #[test]
    fn projection_pruning_preserves_order() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema(
                "s",
                "t",
                &[("a", FieldType::Int32), ("b", FieldType::Utf8)],
            ))
            .with_node(project_node("p", &["s"], &["b", "a", "b", "a"]));

        let result = ProjectionPruningRule.apply(&plan).unwrap();

        let project_node = result.nodes().iter().find(|n| n.id() == "p").unwrap();
        if let Some(NodeOp::Project { columns }) = project_node.op() {
            // Should be ["b", "a"] — first occurrence order preserved, not sorted to ["a", "b"]
            assert_eq!(columns, &["b", "a"]);
        } else {
            panic!("expected Project node");
        }
    }

    #[test]
    fn projection_pruning_noop_when_no_duplicates() {
        let plan = LogicalPlan::new("test", ExecutionKind::Batch)
            .with_node(scan_with_schema(
                "s",
                "t",
                &[("a", FieldType::Int32), ("b", FieldType::Utf8)],
            ))
            .with_node(project_node("p", &["s"], &["a", "b"]));

        let result = ProjectionPruningRule.apply(&plan);
        assert!(result.is_none(), "no duplicates → no change");
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
            // Qualified column "o.id" should match scan column "id" via unqualified suffix
            .with_node(filter_node("f", &["s"], "o.id = 5 AND o.amount > 100"));

        let result = PredicatePushdownRule.apply(&plan).unwrap();

        assert!(
            !result.nodes().iter().any(|n| n.id() == "f"),
            "filter should be fully pushed"
        );
        let scan = result.nodes().iter().find(|n| n.id() == "s").unwrap();
        if let Some(NodeOp::Scan { filters, .. }) = scan.op() {
            assert_eq!(filters.len(), 2);
            assert!(filters.contains(&"o.id = 5".to_string()));
            assert!(filters.contains(&"o.amount > 100".to_string()));
        } else {
            panic!("expected Scan node");
        }
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
}
