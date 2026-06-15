use std::collections::HashMap;

use krishiv_plan::{
    ExecutionKind as PlanExecutionKind, LogicalPlan, NodeOp, PhysicalPlan, PlanNode,
};
use krishiv_proto::{
    AttemptId, ConnectorCapabilityFlags, ExecutorDescriptor, ExecutorId, ExecutorTaskAssignment,
    InputPartition, InputPartitionDescriptor, JobId, JobKind, JobSpec, JobState, KeyGroupRange,
    LeaseGeneration, MissingShufflePartition, OutputContract, OutputContractKind, PlanFragment,
    StageId, StageSpec, StageState, StreamingTaskState, TaskAssignment, TaskAttemptRef, TaskId,
    TaskOutputMetadata, TaskSpec, TaskState, TaskStatusUpdate,
};
use krishiv_shuffle::{ShuffleMetadata, ShufflePath};

use crate::{ExecutorHeartbeatAge, SchedulerError, SchedulerResult, TaskUpdateOutcome};

const MAX_KEY_GROUPS: u32 = 32_768;

/// Conservative per-job UDF execution time cap (ms) — 1 hour.
const UDF_EXECUTION_TIME_CAP_MS: u64 = 60 * 60 * 1_000;

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
            let executor = executors[idx % executors.len()];
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

impl SlotAwareScheduler {
    pub fn place(
        spec: &JobSpec,
        executors: &[&ExecutorDescriptor],
    ) -> SchedulerResult<Vec<TaskAssignment>> {
        if executors.is_empty() {
            return Err(SchedulerError::NoExecutors);
        }

        let mut slot_budget: Vec<usize> = executors.iter().map(|e| e.slots()).collect();
        let mut assignments = Vec::with_capacity(spec.task_count());
        for task in spec.stages().iter().flat_map(StageSpec::tasks) {
            if slot_budget.iter().all(|s| *s == 0) {
                slot_budget = executors.iter().map(|e| e.slots()).collect();
            }
            let (idx, _) = slot_budget
                .iter()
                .enumerate()
                .max_by_key(|(_, slots)| **slots)
                .ok_or(SchedulerError::NoExecutors)?;
            slot_budget[idx] = slot_budget[idx].saturating_sub(1);
            assignments.push(TaskAssignment::new(
                task.task_id().clone(),
                executors[idx].executor_id().clone(),
            ));
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
            let idx = *stage_id_to_idx
                .get(stage.stage_id())
                .expect("stage just indexed");
            in_degree[idx] = in_degree[idx].saturating_add(stage.upstream_stage_ids().len());
        }
        let mut queue: std::collections::VecDeque<usize> = in_degree
            .iter()
            .enumerate()
            .filter_map(|(i, &d)| (d == 0).then_some(i))
            .collect();
        let mut processed = 0usize;
        while let Some(idx) = queue.pop_front() {
            processed += 1;
            let current_id = spec.stages()[idx].stage_id();
            for (ds_idx, ds_stage) in spec.stages().iter().enumerate() {
                if ds_stage.upstream_stage_ids().contains(current_id) {
                    in_degree[ds_idx] = in_degree[ds_idx].saturating_sub(1);
                    if in_degree[ds_idx] == 0 {
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

    let mut spec = JobSpec::new(job_id, job_name, job_kind);
    let mut prev_stage_id: Option<StageId> = None;
    for (stage_idx, slice) in stage_slices.iter().enumerate() {
        let stage_id = StageId::try_new(format!("stage-{}", stage_idx + 1)).map_err(|error| {
            SchedulerError::InvalidPlan {
                message: error.to_string(),
            }
        })?;
        let mut stage = StageSpec::new(stage_id.clone(), format!("{job_name}-stage-{stage_idx}"));
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
            in_degrees[node_index] = in_degrees[node_index].checked_add(1).ok_or_else(|| {
                SchedulerError::InvalidPlan {
                    message: format!("node '{}' has too many input edges", node.id()),
                }
            })?;
            dependents[input_index].push(node_index);
        }
    }
    let mut queue = in_degrees
        .iter()
        .enumerate()
        .filter_map(|(index, in_degree)| (*in_degree == 0).then_some(index))
        .collect::<VecDeque<_>>();
    let mut ordered = Vec::with_capacity(nodes.len());
    while let Some(node_index) = queue.pop_front() {
        ordered.push(&nodes[node_index]);
        for &dependent_index in &dependents[node_index] {
            let in_degree =
                in_degrees
                    .get_mut(dependent_index)
                    .ok_or_else(|| SchedulerError::InvalidPlan {
                        message: format!(
                            "topological sort lost in-degree state for node '{}'",
                            nodes[dependent_index].id()
                        ),
                    })?;
            *in_degree = in_degree
                .checked_sub(1)
                .ok_or_else(|| SchedulerError::InvalidPlan {
                    message: format!(
                        "topological sort underflowed in-degree for node '{}'",
                        nodes[dependent_index].id()
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

