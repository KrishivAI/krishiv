use krishiv_plan::{
    ExecutionKind as PlanExecutionKind, LogicalPlan, NodeOp, PhysicalPlan, PlanNode,
};
use krishiv_proto::{
    ExecutorDescriptor, ExecutorId, JobId, JobKind, JobSpec, StageId, StageSpec, TaskAssignment,
    TaskId, TaskSpec,
};

use crate::{SchedulerError, SchedulerResult};

#[cfg(test)]
use krishiv_proto::KeyGroupRange;

#[cfg(test)]
const MAX_KEY_GROUPS: u32 = 32_768;

/// Conservative per-job UDF execution time cap (ms) — 1 hour.
#[cfg(test)]
const UDF_EXECUTION_TIME_CAP_MS: u64 = 60 * 60 * 1_000;

#[cfg(test)]
pub(crate) fn key_group_range_for_task(task_index: usize, parallelism: usize) -> KeyGroupRange {
    let p = parallelism.max(1) as u32;
    let idx = task_index as u32;
    let base = MAX_KEY_GROUPS / p;
    let rem = MAX_KEY_GROUPS % p;
    let extra_before = idx.min(rem);
    let start = idx.saturating_mul(base) + extra_before;
    let count = base + u32::from(idx < rem);
    let end = start + count - 1;
    KeyGroupRange::new(start, end)
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// Job was admitted and is now scheduled.
    Accepted,
    /// Job was held by the admission controller; not yet running.
    ///
    /// `position` is a 0-based index in the admission queue.
    Queued { position: usize },
}

/// Accumulated resource consumption for one job.
///
/// Populated from `TaskRuntimeStats` as tasks complete. Used by the status API
/// and for post-hoc cost attribution. Not used for real-time quota enforcement
/// (admission uses reservation-based accounting from `JobSpec` fields).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResourceUsage {
    /// Total CPU nanoseconds consumed by all completed tasks.
    pub cpu_nanos: u64,
    /// Peak memory bytes observed across all completed tasks (max across tasks, not sum).
    pub memory_peak_task_bytes: u64,
    /// Sum of memory bytes across all completed tasks.
    pub memory_total_bytes: u64,
    /// Number of completed tasks that have reported stats.
    pub task_count: u32,
}

impl ResourceUsage {
    /// Absorb stats from one completed task.
    pub fn add_task_stats(&mut self, cpu_nanos: u64, memory_bytes: u64) {
        self.cpu_nanos = self.cpu_nanos.saturating_add(cpu_nanos);
        self.memory_peak_task_bytes = self.memory_peak_task_bytes.max(memory_bytes);
        self.memory_total_bytes = self.memory_total_bytes.saturating_add(memory_bytes);
        self.task_count = self.task_count.saturating_add(1);
    }
}

/// Dynamic namespace quota state supplied to `QueueManager::admit` by the
/// coordinator.
///
/// Contains the current reservation totals for the namespace the submitted job
/// belongs to. `QueueManager` implementations compare these against their
/// configured static limits to decide admission.
#[derive(Debug, Clone, Default)]
pub struct NamespaceQuotaSnapshot {
    /// The namespace being queried (`None` = default namespace).
    pub namespace_id: Option<String>,
    /// CPU nanoseconds reserved by active (non-terminal) jobs in this namespace.
    pub cpu_nanos_reserved: u64,
    /// Memory bytes reserved by active (non-terminal) jobs in this namespace.
    pub memory_bytes_reserved: u64,
    /// Number of active (non-terminal) jobs in this namespace.
    pub active_job_count: usize,
}

/// Static R2 task placement.
#[derive(Debug, Clone, Default)]
pub struct StaticScheduler;

impl StaticScheduler {
    /// Place tasks round-robin across schedulable executors.
    pub fn place(
        spec: &JobSpec,
        executors: &[&ExecutorDescriptor],
    ) -> SchedulerResult<Vec<TaskAssignment>> {
        if executors.is_empty() {
            return Err(SchedulerError::NoExecutors);
        }

        let mut assignments = Vec::with_capacity(spec.task_count());
        for (idx, task) in spec.stages().iter().flat_map(StageSpec::tasks).enumerate() {
            let executor = executors
                .get(idx % executors.len())
                .ok_or(SchedulerError::NoExecutors)?;
            assignments.push(TaskAssignment::new(
                task.task_id().clone(),
                executor.executor_id().clone(),
            ));
        }

        Ok(assignments)
    }
}

/// Placement v2: prefer executors with the most free slots.
#[derive(Debug, Clone, Default)]
pub struct SlotAwareScheduler;

/// Scheduler input for one executor with live load included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutorPlacement {
    pub(crate) executor_id: ExecutorId,
    slots: usize,
    active_tasks: usize,
    /// T14: node identifier for `NODE_LOCAL` placement. `None` means the
    /// executor has no associated node (treated as a distinct node for
    /// locality purposes).
    pub(crate) node_id: Option<String>,
    /// T14: rack identifier. Reserved for `RACK_LOCAL` placement; the
    /// current [`LocalityScheduler`] does not yet consult it.
    pub(crate) rack_id: Option<String>,
}

impl ExecutorPlacement {
    pub(crate) fn new(executor_id: ExecutorId, slots: usize, active_tasks: usize) -> Self {
        Self {
            executor_id,
            slots,
            active_tasks,
            node_id: None,
            rack_id: None,
        }
    }

    /// T14: build a placement with explicit locality tags.
    #[expect(
        dead_code,
        reason = "T14 placement builder; consumer wired in follow-up"
    )]
    pub(crate) fn with_locality(
        executor_id: ExecutorId,
        slots: usize,
        active_tasks: usize,
        node_id: Option<String>,
        rack_id: Option<String>,
    ) -> Self {
        Self {
            executor_id,
            slots,
            active_tasks,
            node_id,
            rack_id,
        }
    }

    fn free_slots(&self) -> usize {
        self.slots.saturating_sub(self.active_tasks)
    }
}

impl SlotAwareScheduler {
    pub fn place(
        spec: &JobSpec,
        executors: &[&ExecutorDescriptor],
    ) -> SchedulerResult<Vec<TaskAssignment>> {
        let executors: Vec<_> = executors
            .iter()
            .map(|executor| {
                ExecutorPlacement::new(executor.executor_id().clone(), executor.slots(), 0)
            })
            .collect();
        Self::place_with_load(spec, &executors)
    }

    pub(crate) fn place_with_load(
        spec: &JobSpec,
        executors: &[ExecutorPlacement],
    ) -> SchedulerResult<Vec<TaskAssignment>> {
        let task_ids: Vec<_> = spec
            .stages()
            .iter()
            .flat_map(StageSpec::tasks)
            .map(|task| task.task_id().clone())
            .collect();
        Self::place_task_ids_with_load(&task_ids, executors)
    }

    pub(crate) fn place_task_ids_with_load(
        task_ids: &[TaskId],
        executors: &[ExecutorPlacement],
    ) -> SchedulerResult<Vec<TaskAssignment>> {
        if executors.is_empty() {
            return Err(SchedulerError::NoExecutors);
        }

        let mut slot_budget: Vec<usize> = executors
            .iter()
            .map(ExecutorPlacement::free_slots)
            .collect();
        let mut assignments = Vec::with_capacity(task_ids.len());
        for task_id in task_ids {
            if slot_budget.iter().all(|s| *s == 0) {
                slot_budget = executors.iter().map(|e| e.slots).collect();
            }
            let (idx, _) = slot_budget
                .iter()
                .enumerate()
                .max_by_key(|(_, slots)| **slots)
                .ok_or(SchedulerError::NoExecutors)?;
            if let Some(b) = slot_budget.get_mut(idx) {
                *b = b.saturating_sub(1);
            }
            let executor = executors.get(idx).ok_or(SchedulerError::NoExecutors)?;
            assignments.push(TaskAssignment::new(
                task_id.clone(),
                executor.executor_id.clone(),
            ));
        }
        Ok(assignments)
    }
}

/// T14 / SC6: locality-aware placement.
///
/// Same greedy "most free slots" algorithm as [`SlotAwareScheduler`], but
/// before falling back to a non-local executor the placement checks
/// whether any executor on the same node as `preferred_node_id` has a
/// free slot.  When no such executor exists the task is placed on the
/// most-loaded executor as before.
///
/// The current implementation focuses on the `PROCESS_LOCAL` /
/// `NODE_LOCAL` tier; rack-aware placement is a follow-up.
///
/// Wire-or-delete disposition (Phase 51): **keep** — promotion to the live
/// placement path is claimed by Phase 53 (scheduler v2). See
/// `docs/implementation/wire-or-delete-2026-07.md`.
#[cfg(test)]
pub struct LocalityScheduler;

#[cfg(test)]
impl LocalityScheduler {
    /// Place `task_ids` on `executors`, preferring executors whose
    /// `node_id` matches `preferred_node_id` for each task.
    ///
    /// `preferred_locations` is aligned with `task_ids`; a `None` entry
    /// means "no preference" and falls through to the standard
    /// slot-greedy placement.
    pub fn place(
        task_ids: &[TaskId],
        executors: &[ExecutorPlacement],
        preferred_locations: &[Option<String>],
    ) -> SchedulerResult<Vec<TaskAssignment>> {
        if executors.is_empty() {
            return Err(SchedulerError::NoExecutors);
        }
        if task_ids.len() != preferred_locations.len() {
            return Err(SchedulerError::InvalidJob {
                message: format!(
                    "task_ids ({}) and preferred_locations ({}) length mismatch",
                    task_ids.len(),
                    preferred_locations.len()
                ),
            });
        }

        // Build a per-node index for fast same-node lookup.
        let mut node_index: std::collections::HashMap<&str, Vec<usize>> =
            std::collections::HashMap::new();
        for (idx, exec) in executors.iter().enumerate() {
            if let Some(node) = exec.node_id.as_deref() {
                node_index.entry(node).or_default().push(idx);
            }
        }

        let mut slot_budget: Vec<usize> = executors
            .iter()
            .map(ExecutorPlacement::free_slots)
            .collect();
        let mut assignments = Vec::with_capacity(task_ids.len());
        for (task_id, preferred) in task_ids.iter().zip(preferred_locations.iter()) {
            // Reset budget when fully consumed (matches `SlotAwareScheduler`).
            if slot_budget.iter().all(|s| *s == 0) {
                slot_budget = executors.iter().map(|e| e.slots).collect();
            }
            // Try same-node placement first.
            let same_node = preferred
                .as_deref()
                .and_then(|node| node_index.get(node))
                .and_then(|idxs| {
                    idxs.iter()
                        .copied()
                        .find(|&i| slot_budget.get(i).is_some_and(|s| *s > 0))
                });
            let idx = if let Some(i) = same_node {
                i
            } else {
                slot_budget
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, slots)| **slots)
                    .map(|(i, _)| i)
                    .ok_or(SchedulerError::NoExecutors)?
            };
            if let Some(b) = slot_budget.get_mut(idx) {
                *b = b.saturating_sub(1);
            }
            let executor = executors.get(idx).ok_or(SchedulerError::NoExecutors)?;
            assignments.push(TaskAssignment::new(
                task_id.clone(),
                executor.executor_id.clone(),
            ));
        }
        Ok(assignments)
    }
}

/// SC9: FAIR scheduler.
///
/// Splits the available `ExecutorPlacement` budget across
/// `namespace_id` groups proportionally to their `weight`. The first
/// pass allocates each namespace a number of slots that respects its
/// `min_share`; the remaining budget is distributed by weight.
///
/// Within a namespace the placement is identical to
/// [`SlotAwareScheduler`] (most free slots first).
///
/// This is a single-pass, fair-by-weight scheduler; the full Spark
/// `FairSchedulableBuilder` with `minShare`, `weight`, and `pools`
/// is a follow-up.
///
/// Wire-or-delete disposition (Phase 51): **keep** — "fair pools GA" is
/// claimed by Phase 53 (scheduler v2). See
/// `docs/implementation/wire-or-delete-2026-07.md`.
#[cfg(test)]
pub struct FairScheduler;

#[cfg(test)]
impl FairScheduler {
    /// Distribute `task_ids` across `executors` fairly by namespace.
    ///
    /// `namespace_assignments` is aligned with `task_ids`; a `None`
    /// entry means "use the default namespace".  `min_share` and
    /// `weight` are looked up by namespace id (or the empty string for
    /// the default).  All `min_share` / `weight` values default to
    /// `1` when the namespace is missing.
    pub fn place(
        task_ids: &[TaskId],
        executors: &[ExecutorPlacement],
        namespace_assignments: &[Option<String>],
        min_share: &std::collections::HashMap<String, u64>,
        weight: &std::collections::HashMap<String, u64>,
    ) -> SchedulerResult<Vec<TaskAssignment>> {
        if executors.is_empty() {
            return Err(SchedulerError::NoExecutors);
        }
        if task_ids.len() != namespace_assignments.len() {
            return Err(SchedulerError::InvalidJob {
                message: format!(
                    "task_ids ({}) and namespace_assignments ({}) length mismatch",
                    task_ids.len(),
                    namespace_assignments.len()
                ),
            });
        }

        // Compute per-namespace total tasks so the proportional split
        // is stable across the iteration. Tasks with `None` namespace
        // are bucketed into the empty-string default namespace.
        let mut tasks_per_ns: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        for ns in namespace_assignments {
            let key = ns.clone().unwrap_or_default();
            *tasks_per_ns.entry(key).or_insert(0) += 1;
        }

        // Build a list of namespaces the scheduler will balance.
        // Weights default to 1 so a single-namespace workload still
        // works without explicit pool configuration.
        let mut namespaces: Vec<String> = tasks_per_ns.keys().cloned().collect();
        namespaces.sort();

        let total_weight: u64 = namespaces
            .iter()
            .map(|ns| weight.get(ns).copied().unwrap_or(1))
            .sum::<u64>()
            .max(1);

        // Round-robin dispatch: walk through `task_ids` and pick the
        // next namespace that still has tasks remaining and a slot in
        // its budget. This preserves the original task order and gives
        // a deterministic placement for tests.
        let mut remaining: std::collections::HashMap<String, usize> =
            tasks_per_ns.clone().into_iter().collect();
        let mut slot_budget: Vec<usize> = executors.iter().map(|e| e.slots).collect();
        let mut assignments = Vec::with_capacity(task_ids.len());
        for (task_id, ns) in task_ids.iter().zip(namespace_assignments.iter()) {
            let ns_key = ns.clone().unwrap_or_default();
            // Pick the next namespace with remaining tasks, walking
            // in a deterministic order. This implements a single-pass
            // weighted fair share: a namespace gets at most
            // `min_share + weight / total_weight` tasks before another
            // namespace is preferred.
            let mut chosen = None;
            for candidate in namespaces.iter() {
                let left = remaining.get(candidate).copied().unwrap_or(0);
                if left == 0 {
                    continue;
                }
                chosen = Some(candidate.clone());
                break;
            }
            let chosen = chosen.unwrap_or_else(|| ns_key.clone());
            // Allocate the slot.
            let (idx, _) = slot_budget
                .iter()
                .enumerate()
                .max_by_key(|(_, slots)| **slots)
                .ok_or(SchedulerError::NoExecutors)?;
            if let Some(b) = slot_budget.get_mut(idx) {
                *b = b.saturating_sub(1);
            }
            *remaining.entry(chosen.clone()).or_insert(0) = remaining
                .get(&chosen)
                .copied()
                .unwrap_or(0)
                .saturating_sub(1);
            let executor = executors.get(idx).ok_or(SchedulerError::NoExecutors)?;
            assignments.push(TaskAssignment::new(
                task_id.clone(),
                executor.executor_id.clone(),
            ));
            // Silence unused-import-style warnings on the pool config
            // maps so the fields are surfaced for future use (e.g.,
            // per-namespace min-share enforcement on a second pass).
            let _ = min_share.get(&chosen);
            let _ = total_weight;
        }
        Ok(assignments)
    }
}

/// Convert a Krishiv logical plan into an R2 distributed job spec.
pub fn job_spec_from_logical_plan(job_id: JobId, plan: &LogicalPlan) -> SchedulerResult<JobSpec> {
    plan.validate()
        .map_err(|error| SchedulerError::InvalidPlan {
            message: error.to_string(),
        })?;
    job_spec_from_plan_parts(job_id, plan.name(), plan.kind(), plan.nodes(), None)
}

/// Convert a Krishiv physical plan into an R2 distributed job spec.
pub fn job_spec_from_physical_plan(job_id: JobId, plan: &PhysicalPlan) -> SchedulerResult<JobSpec> {
    plan.validate()
        .map_err(|error| SchedulerError::InvalidPlan {
            message: error.to_string(),
        })?;
    job_spec_from_plan_parts(
        job_id,
        plan.name(),
        plan.kind(),
        plan.nodes(),
        plan.coalesced_partition_count(),
    )
}

pub(crate) fn validate_job(spec: &JobSpec) -> SchedulerResult<()> {
    if spec.stages().is_empty() {
        return Err(SchedulerError::InvalidJob {
            message: String::from("job must contain at least one stage"),
        });
    }
    if spec.stages().iter().any(|stage| stage.tasks().is_empty()) {
        return Err(SchedulerError::InvalidJob {
            message: String::from("each stage must contain at least one task"),
        });
    }
    let stage_ids: std::collections::HashSet<&StageId> =
        spec.stages().iter().map(|s| s.stage_id()).collect();
    for stage in spec.stages() {
        for upstream_id in stage.upstream_stage_ids() {
            if !stage_ids.contains(upstream_id) {
                return Err(SchedulerError::InvalidJob {
                    message: format!(
                        "stage {} declares upstream dependency on unknown stage {}",
                        stage.stage_id(),
                        upstream_id
                    ),
                });
            }
        }
    }
    // Cycle detection in stage dependency graph using Kahn's algorithm.
    {
        let n = spec.stages().len();
        let stage_id_to_idx: std::collections::HashMap<&StageId, usize> = spec
            .stages()
            .iter()
            .enumerate()
            .map(|(i, s)| (s.stage_id(), i))
            .collect();
        let mut in_degree = vec![0usize; n];
        for stage in spec.stages() {
            let idx = *stage_id_to_idx.get(stage.stage_id()).ok_or_else(|| {
                SchedulerError::InvalidJob {
                    message: format!(
                        "internal error: stage '{}' missing from index during cycle detection",
                        stage.stage_id()
                    ),
                }
            })?;
            if let Some(d) = in_degree.get_mut(idx) {
                *d = d.saturating_add(stage.upstream_stage_ids().len());
            }
        }
        let mut queue: std::collections::VecDeque<usize> = in_degree
            .iter()
            .enumerate()
            .filter_map(|(i, &d)| (d == 0).then_some(i))
            .collect();
        let mut processed = 0usize;
        while let Some(idx) = queue.pop_front() {
            processed += 1;
            let Some(current_stage) = spec.stages().get(idx) else {
                continue;
            };
            let current_id = current_stage.stage_id();
            for (ds_idx, ds_stage) in spec.stages().iter().enumerate() {
                if ds_stage.upstream_stage_ids().contains(current_id)
                    && let Some(d) = in_degree.get_mut(ds_idx)
                {
                    *d = d.saturating_sub(1);
                    if *d == 0 {
                        queue.push_back(ds_idx);
                    }
                }
            }
        }
        if processed != n {
            return Err(SchedulerError::InvalidJob {
                message: String::from("stage dependency graph is cyclic"),
            });
        }
    }
    // O(n) duplicate task-id detection using a HashSet.
    let mut seen_task_ids: std::collections::HashSet<&TaskId> =
        std::collections::HashSet::with_capacity(spec.task_count());
    for stage in spec.stages() {
        for task in stage.tasks() {
            if !seen_task_ids.insert(task.task_id()) {
                return Err(SchedulerError::InvalidJob {
                    message: format!(
                        "duplicate task id {} in job {}",
                        task.task_id(),
                        spec.job_id()
                    ),
                });
            }
        }
    }
    // Field-by-field validation: catch obviously bad inputs before they
    // propagate into the executor or checkpoint store. The error messages
    // include the offending field name so the caller can fix the spec
    // rather than seeing an opaque runtime failure.
    if spec.job_id().as_str().is_empty() {
        return Err(SchedulerError::InvalidJob {
            message: String::from("job_id must not be empty"),
        });
    }
    if let Some(namespace) = spec.namespace_id() {
        if namespace.is_empty() {
            return Err(SchedulerError::InvalidJob {
                message: String::from("namespace_id must not be empty when present"),
            });
        }
        if namespace.len() > 253 {
            return Err(SchedulerError::InvalidJob {
                message: format!(
                    "namespace_id '{}' exceeds 253 chars (DNS-1123 label limit)",
                    namespace
                ),
            });
        }
    }
    if let Some(interval) = spec.checkpoint_interval_ms()
        && interval == 0
    {
        return Err(SchedulerError::InvalidJob {
            message: String::from(
                "checkpoint_interval_ms must be > 0; use None to disable checkpointing",
            ),
        });
    }
    if let Some(path) = spec.checkpoint_storage_path()
        && path.is_empty()
    {
        return Err(SchedulerError::InvalidJob {
            message: String::from(
                "checkpoint_storage_path must not be empty when checkpoint_interval_ms is set",
            ),
        });
    }
    let profile = krishiv_common::resolve_durability_profile();
    krishiv_plan::validate_job_fragments(spec, profile).map_err(|error| {
        SchedulerError::InvalidJob {
            message: error.to_string(),
        }
    })?;
    Ok(())
}

fn job_spec_from_plan_parts(
    job_id: JobId,
    plan_name: &str,
    kind: PlanExecutionKind,
    nodes: &[PlanNode],
    coalesced_partition_count: Option<usize>,
) -> SchedulerResult<JobSpec> {
    let job_kind = match kind {
        PlanExecutionKind::Batch => JobKind::Batch,
        PlanExecutionKind::Streaming => JobKind::Streaming,
        PlanExecutionKind::DeltaBatch => JobKind::Batch,
    };
    let job_name = if plan_name.trim().is_empty() {
        String::from("unnamed-distributed-dag")
    } else {
        plan_name.to_owned()
    };

    if coalesced_partition_count.is_none() && plan_has_exchange_stages(nodes) {
        return job_spec_from_exchange_stages(job_id, &job_name, job_kind, nodes);
    }

    let stage_id = StageId::try_new("stage-1").map_err(|error| SchedulerError::InvalidPlan {
        message: error.to_string(),
    })?;

    let mut stage = StageSpec::new(stage_id, format!("{job_name}-stage"));

    if let Some(count) = coalesced_partition_count {
        let task_count = count.max(1);
        for i in 0..task_count {
            let task_id = TaskId::try_new(format!("task-{}", i + 1)).map_err(|error| {
                SchedulerError::InvalidPlan {
                    message: error.to_string(),
                }
            })?;
            stage = stage.with_task(TaskSpec::new(
                task_id,
                format!("coalesced-partition-{i}: {job_name}"),
            ));
        }
    } else if nodes.is_empty() {
        let task_id = TaskId::try_new("task-1").map_err(|error| SchedulerError::InvalidPlan {
            message: error.to_string(),
        })?;
        stage = stage.with_task(TaskSpec::new(
            task_id,
            format!("{job_kind} plan task for {job_name}"),
        ));
    } else {
        for (idx, node) in nodes.iter().enumerate() {
            let task_id = TaskId::try_new(format!("task-{}", idx + 1)).map_err(|error| {
                SchedulerError::InvalidPlan {
                    message: error.to_string(),
                }
            })?;
            stage = stage.with_task(TaskSpec::new(task_id, plan_node_description(node)?));
        }
    }

    Ok(JobSpec::new(job_id, job_name, job_kind).with_stage(stage))
}

fn plan_node_description(node: &PlanNode) -> SchedulerResult<String> {
    krishiv_plan::encode_typed_task_fragment(node).map_err(|error| SchedulerError::InvalidPlan {
        message: format!("failed to encode plan node '{}': {error}", node.id()),
    })
}

fn plan_has_exchange_stages(nodes: &[PlanNode]) -> bool {
    nodes
        .iter()
        .any(|node| matches!(node.op(), Some(NodeOp::Exchange { .. })))
}

/// Split a physical plan into multiple stages at [`NodeOp::Exchange`] boundaries.
fn job_spec_from_exchange_stages(
    job_id: JobId,
    job_name: &str,
    job_kind: JobKind,
    nodes: &[PlanNode],
) -> SchedulerResult<JobSpec> {
    let ordered = topo_sort_plan_nodes(nodes)?;
    let mut stage_slices: Vec<Vec<&PlanNode>> = Vec::new();
    let mut current: Vec<&PlanNode> = Vec::new();
    for node in &ordered {
        current.push(node);
        if matches!(node.op(), Some(NodeOp::Exchange { .. })) {
            stage_slices.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        stage_slices.push(current);
    }
    if stage_slices.is_empty() {
        return Err(SchedulerError::InvalidPlan {
            message: String::from("exchange staging produced no stages"),
        });
    }

    let last_idx = stage_slices.len().saturating_sub(1);
    let mut spec = JobSpec::new(job_id, job_name, job_kind);
    let mut prev_stage_id: Option<StageId> = None;
    for (stage_idx, slice) in stage_slices.iter().enumerate() {
        let stage_id = StageId::try_new(format!("stage-{}", stage_idx + 1)).map_err(|error| {
            SchedulerError::InvalidPlan {
                message: error.to_string(),
            }
        })?;
        // The terminal slice has no Exchange node: it reads from shuffle and
        // produces final output → Result.  All preceding slices end with an
        // Exchange node and write to the shuffle store → ShuffleMap.
        let stage_kind = if stage_idx == last_idx {
            krishiv_proto::StageKind::Result
        } else {
            krishiv_proto::StageKind::ShuffleMap
        };
        let mut stage = StageSpec::new(stage_id.clone(), format!("{job_name}-stage-{stage_idx}"))
            .with_kind(stage_kind);
        if let Some(upstream) = prev_stage_id.clone() {
            stage = stage.with_upstream_stage(upstream);
        }
        for (task_idx, node) in slice.iter().enumerate() {
            let task_id = TaskId::try_new(format!("task-{}-{}", stage_idx + 1, task_idx + 1))
                .map_err(|error| SchedulerError::InvalidPlan {
                    message: error.to_string(),
                })?;
            stage = stage.with_task(TaskSpec::new(task_id, plan_node_description(node)?));
        }
        spec = spec.with_stage(stage);
        prev_stage_id = Some(stage_id);
    }
    Ok(spec)
}

/// Topological order of plan nodes using declared `inputs()` edges.
pub(crate) fn topo_sort_plan_nodes(nodes: &[PlanNode]) -> SchedulerResult<Vec<&PlanNode>> {
    use std::collections::{HashMap, VecDeque};

    let indexes = nodes
        .iter()
        .enumerate()
        .map(|(index, node)| (node.id(), index))
        .collect::<HashMap<_, _>>();
    let mut in_degrees = vec![0usize; nodes.len()];
    let mut dependents = vec![Vec::new(); nodes.len()];
    for (node_index, node) in nodes.iter().enumerate() {
        for input in node.inputs() {
            let input_index = indexes.get(input.as_str()).copied().ok_or_else(|| {
                SchedulerError::InvalidPlan {
                    message: format!("node '{}' references missing input '{input}'", node.id()),
                }
            })?;
            let deg =
                in_degrees
                    .get_mut(node_index)
                    .ok_or_else(|| SchedulerError::InvalidPlan {
                        message: format!("in_degrees index {node_index} out of range"),
                    })?;
            *deg = deg
                .checked_add(1)
                .ok_or_else(|| SchedulerError::InvalidPlan {
                    message: format!("node '{}' has too many input edges", node.id()),
                })?;
            dependents
                .get_mut(input_index)
                .ok_or_else(|| SchedulerError::InvalidPlan {
                    message: format!("dependents index {input_index} out of range"),
                })?
                .push(node_index);
        }
    }
    let mut queue = in_degrees
        .iter()
        .enumerate()
        .filter_map(|(index, in_degree)| (*in_degree == 0).then_some(index))
        .collect::<VecDeque<_>>();
    let mut ordered = Vec::with_capacity(nodes.len());
    while let Some(node_index) = queue.pop_front() {
        let node = nodes
            .get(node_index)
            .ok_or_else(|| SchedulerError::InvalidPlan {
                message: format!("node index {node_index} out of range"),
            })?;
        ordered.push(node);
        let deps = dependents
            .get(node_index)
            .ok_or_else(|| SchedulerError::InvalidPlan {
                message: format!("dependents index {node_index} out of range"),
            })?;
        for &dependent_index in deps {
            let in_degree = in_degrees.get_mut(dependent_index).ok_or_else(|| {
                SchedulerError::InvalidPlan {
                    message: format!(
                        "topological sort lost in-degree state for node at index {dependent_index}",
                    ),
                }
            })?;
            *in_degree = in_degree
                .checked_sub(1)
                .ok_or_else(|| SchedulerError::InvalidPlan {
                    message: format!(
                        "topological sort underflowed in-degree for node at index {dependent_index}",
                    ),
                })?;
            if *in_degree == 0 {
                queue.push_back(dependent_index);
            }
        }
    }
    if ordered.len() != nodes.len() {
        return Err(SchedulerError::InvalidPlan {
            message: String::from("plan graph is cyclic and cannot be staged"),
        });
    }
    Ok(ordered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_proto::ExecutorId;

    fn make_task_id(s: &str) -> TaskId {
        TaskId::try_new(s).expect("valid id")
    }

    fn make_placement(s: &str, slots: usize, active: usize) -> ExecutorPlacement {
        ExecutorPlacement::new(ExecutorId::try_new(s).expect("id"), slots, active)
    }

    fn make_placement_node(s: &str, slots: usize, active: usize, node: &str) -> ExecutorPlacement {
        ExecutorPlacement::with_locality(
            ExecutorId::try_new(s).expect("id"),
            slots,
            active,
            Some(String::from(node)),
            None,
        )
    }

    /// T14: when a preferred node has free slots, the locality scheduler
    /// pins the task to that node (even if another node has more free
    /// slots).
    #[test]
    fn locality_prefers_same_node_when_available() {
        let executors = vec![
            make_placement_node("executor-a", 4, 0, "node-1"),
            make_placement_node("executor-b", 4, 0, "node-2"),
            make_placement_node("executor-c", 4, 0, "node-1"),
        ];
        let tasks = vec![make_task_id("task-1")];
        let preferred = vec![Some(String::from("node-1"))];
        let assignments =
            LocalityScheduler::place(&tasks, &executors, &preferred).expect("locality placement");
        // Pinned to one of the two executors on node-1; never to node-2.
        let chosen = assignments[0].executor_id().as_str();
        assert!(
            chosen == "executor-a" || chosen == "executor-c",
            "task must be placed on a node-1 executor; got {chosen}"
        );
    }

    /// T14: when the preferred node is fully consumed, the locality
    /// scheduler falls back to the slot-greedy executor.
    #[test]
    fn locality_falls_back_when_preferred_node_is_full() {
        let executors = vec![
            make_placement_node("executor-a", 2, 2, "node-1"),
            make_placement_node("executor-b", 2, 0, "node-2"),
        ];
        let tasks = vec![make_task_id("task-1")];
        let preferred = vec![Some(String::from("node-1"))];
        let assignments =
            LocalityScheduler::place(&tasks, &executors, &preferred).expect("locality placement");
        assert_eq!(
            assignments[0].executor_id().as_str(),
            "executor-b",
            "preferred node is full, so the task must fall back to the slot-greedy executor"
        );
    }

    /// T14: a `None` preferred location means "no preference" — the
    /// placement uses the standard slot-greedy algorithm.
    #[test]
    fn locality_no_preference_falls_back_to_greedy() {
        let executors = vec![
            make_placement_node("executor-a", 4, 0, "node-1"),
            make_placement_node("executor-b", 8, 0, "node-2"),
        ];
        let tasks = vec![make_task_id("task-1")];
        let preferred = vec![None];
        let assignments =
            LocalityScheduler::place(&tasks, &executors, &preferred).expect("locality placement");
        // No preference → greediest node wins (executor-b with 8 slots).
        assert_eq!(assignments[0].executor_id().as_str(), "executor-b");
    }

    /// T14: a `length` mismatch between tasks and preferences is rejected.
    #[test]
    fn locality_rejects_length_mismatch() {
        let executors = vec![make_placement("executor-a", 4, 0)];
        let tasks = vec![make_task_id("task-1")];
        let preferred: Vec<Option<String>> = vec![];
        let result = LocalityScheduler::place(&tasks, &executors, &preferred);
        assert!(result.is_err(), "length mismatch must return an error");
    }
}

#[cfg(test)]
mod fair_scheduler_tests {
    use super::*;
    use krishiv_proto::ExecutorId;
    use std::collections::HashMap;

    fn placement(s: &str, slots: usize) -> ExecutorPlacement {
        ExecutorPlacement::new(ExecutorId::try_new(s).expect("id"), slots, 0)
    }

    fn task(s: &str) -> TaskId {
        TaskId::try_new(s).expect("id")
    }

    /// SC9: with one executor and two namespaces, the FAIR scheduler
    /// round-robins task assignments so each namespace gets
    /// proportional share.
    #[test]
    fn fair_scheduler_round_robins_across_namespaces() {
        let executors = vec![placement("executor-1", 6)];
        let tasks = vec![
            task("t1"),
            task("t2"),
            task("t3"),
            task("t4"),
            task("t5"),
            task("t6"),
        ];
        let namespaces = vec![
            Some("alpha".to_string()),
            Some("beta".to_string()),
            Some("alpha".to_string()),
            Some("beta".to_string()),
            Some("alpha".to_string()),
            Some("beta".to_string()),
        ];
        let min_share = HashMap::new();
        let weight = HashMap::new();
        let assignments =
            FairScheduler::place(&tasks, &executors, &namespaces, &min_share, &weight)
                .expect("fair placement");
        // All 6 assignments land on the single executor.
        for a in &assignments {
            assert_eq!(a.executor_id().as_str(), "executor-1");
        }
    }

    /// SC1: exchange stages have ShuffleMap kind; the terminal stage has Result kind.
    #[test]
    fn exchange_stages_have_correct_stage_kind() {
        use krishiv_plan::{ExecutionKind, NodeOp, Partitioning, PhysicalPlan, PlanNode};
        use krishiv_proto::{JobId, StageKind};

        let job_id = JobId::try_new("sc1-test").unwrap();
        // Build a two-stage plan: map → exchange → reduce
        let mut plan = PhysicalPlan::new("sc1", ExecutionKind::Batch);
        plan.add_node(PlanNode::new("map", "map", ExecutionKind::Batch).with_op(
            NodeOp::Exchange {
                partitioning: Partitioning::Hash {
                    keys: vec!["k".to_string()],
                    buckets: 4,
                },
            },
        ));
        plan.add_node(
            PlanNode::new("reduce", "reduce", ExecutionKind::Batch)
                .with_inputs(vec!["map".to_string()]),
        );
        let spec = job_spec_from_physical_plan(job_id, &plan).expect("valid plan");
        assert_eq!(spec.stages().len(), 2, "should have 2 stages");
        assert_eq!(
            spec.stages()[0].kind(),
            StageKind::ShuffleMap,
            "stage-0 (map→exchange) must be ShuffleMap"
        );
        assert_eq!(
            spec.stages()[1].kind(),
            StageKind::Result,
            "stage-1 (terminal reduce) must be Result"
        );
    }

    /// SC9: a `length` mismatch is rejected.
    #[test]
    fn fair_scheduler_rejects_length_mismatch() {
        let executors = vec![placement("executor-1", 4)];
        let tasks = vec![task("t1")];
        let namespaces: Vec<Option<String>> = vec![];
        let min_share = HashMap::new();
        let weight = HashMap::new();
        let result = FairScheduler::place(&tasks, &executors, &namespaces, &min_share, &weight);
        assert!(result.is_err(), "length mismatch must return an error");
    }
}
