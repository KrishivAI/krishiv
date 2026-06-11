use std::collections::HashMap;

use krishiv_plan::{
    ExecutionKind as PlanExecutionKind, LogicalPlan, NodeOp, PhysicalPlan, PlanNode,
};
use krishiv_proto::{
    AttemptId, ConnectorCapabilityFlags, ExecutorDescriptor, ExecutorId, ExecutorTaskAssignment,
    InputPartition, InputPartitionDescriptor, JobId, JobKind, JobSpec, JobState, KeyGroupRange,
    LeaseGeneration, OutputContract, OutputContractKind, PlanFragment, StageId, StageSpec,
    StageState, StreamingTaskState, TaskAssignment, TaskAttemptRef, TaskId, TaskOutputMetadata,
    TaskSpec, TaskState, TaskStatusUpdate,
};
use krishiv_shuffle::{ShuffleMetadata, ShufflePath};

use crate::{ExecutorHeartbeatAge, SchedulerError, SchedulerResult, TaskUpdateOutcome};

const MAX_KEY_GROUPS: u32 = 32_768;

/// Conservative per-job UDF execution time cap (ms) — 1 hour.
const UDF_EXECUTION_TIME_CAP_MS: u64 = 60 * 60 * 1_000;

fn key_group_range_for_task(task_index: usize, parallelism: usize) -> KeyGroupRange {
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

/// Result of a `Coordinator::submit_job` call.
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

/// Job record owned by the active coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRecord {
    pub(crate) spec: JobSpec,
    pub(crate) state: JobState,
    pub(crate) max_stage_retries: u32,
    pub(crate) stages: Vec<StageRecord>,
    /// Shuffle partition availability metadata per producing stage.
    /// Updated when tasks report ShufflePartitionOutput in TaskOutputMetadata.
    pub(crate) shuffle_output: HashMap<StageId, ShuffleMetadata>,
    /// Accumulated resource consumption from completed tasks.
    pub(crate) resource_usage: ResourceUsage,
}

impl JobRecord {
    /// Conservative per-job execution time cap (ms) for sandboxed UDFs.
    pub fn udf_execution_time_cap_ms(&self) -> Option<u64> {
        Some(UDF_EXECUTION_TIME_CAP_MS)
    }

    /// Memory budget (bytes) for sandboxed UDF execution for this job, taken
    /// directly from the JobSpec (the same value used for admission/quota).
    pub fn udf_memory_limit_bytes(&self) -> Option<u64> {
        self.spec.memory_limit_bytes()
    }

    pub(crate) fn from_spec(spec: JobSpec, max_stage_retries: u32) -> Self {
        let stages = spec
            .stages()
            .iter()
            .cloned()
            .map(StageRecord::from_spec)
            .collect();
        Self {
            spec,
            state: JobState::Accepted,
            max_stage_retries,
            stages,
            shuffle_output: HashMap::new(),
            resource_usage: ResourceUsage::default(),
        }
    }

    /// Accumulated resource consumption reported by completed tasks.
    pub fn resource_usage(&self) -> &ResourceUsage {
        &self.resource_usage
    }

    /// Job id.
    pub fn job_id(&self) -> &JobId {
        self.spec.job_id()
    }

    /// Job state.
    pub fn state(&self) -> JobState {
        self.state
    }

    /// Stage records.
    pub fn stages(&self) -> &[StageRecord] {
        &self.stages
    }

    /// Mutable access to stages (used by circuit breaker for re-assignment).
    pub(crate) fn stages_mut(&mut self) -> &mut [StageRecord] {
        &mut self.stages
    }

    pub(crate) fn apply_assignments(&mut self, assignments: Vec<TaskAssignment>) {
        self.state = JobState::Running;
        let job_id_str = self.job_id().to_string();
        for stage in &mut self.stages {
            stage.state = StageState::Scheduling;
            let stage_id_str = stage.stage_id().to_string();
            for task in &mut stage.tasks {
                if let Some(assignment) = assignments
                    .iter()
                    .find(|assignment| assignment.task_id() == task.task_id())
                {
                    task.assigned_executor = Some(assignment.executor_id().clone());
                    task.state = TaskState::Assigned;
                }
            }
        }
    }

    pub(crate) fn launch_assigned_task_assignments(
        &mut self,
        executor_leases: &[(ExecutorId, LeaseGeneration)],
        batch_sql_tables: Option<&[crate::batch_sql::BatchSqlTable]>,
        inline_partitions: Option<&[krishiv_proto::InputPartition]>,
        task_inline_partitions: Option<
            &std::collections::HashMap<TaskId, Vec<krishiv_proto::InputPartition>>,
        >,
        // When the coordinator has detected hot-key skew for this job, this
        // override replaces `ShuffleWriteConfig.num_partitions` at launch time
        // so newly-assigned shuffle-write tasks spread data more evenly.
        skew_partition_override: Option<u32>,
    ) -> SchedulerResult<Vec<ExecutorTaskAssignment>> {
        let mut assignments = Vec::new();
        self.state = JobState::Running;

        // Build a HashSet once for O(1) upstream-ready checks instead of
        // O(stages²) Vec::contains per stage in the outer loop.
        let succeeded_stage_ids: std::collections::HashSet<StageId> = self
            .stages
            .iter()
            .filter(|s| s.state == StageState::Succeeded)
            .map(|s| s.stage_id().clone())
            .collect();

        for stage in &mut self.stages {
            let stage_id = stage.stage_id().clone();
            let stage_parallelism = stage.tasks.len();

            // Skip stages whose upstream shuffle dependencies are not yet complete.
            let upstream_ready = stage
                .spec
                .upstream_stage_ids()
                .iter()
                .all(|up| succeeded_stage_ids.contains(up));
            if !upstream_ready {
                continue;
            }

            for (task_index, task) in stage.tasks.iter_mut().enumerate() {
                if task.state == TaskState::Assigned && !task.launch_in_flight {
                    let task_body =
                        krishiv_plan::TypedTaskFragment::decode_or_legacy(task.spec.description())
                            .body;
                    if task_body.starts_with("stream:loop:")
                        && inline_partitions.is_none()
                        && task_inline_partitions.is_none()
                    {
                        // A continuous loop task is input-driven. Keep its
                        // executor ownership assigned but do not launch an
                        // empty cycle during registration or scheduler ticks.
                        continue;
                    }
                    let executor_id = task.assigned_executor.clone().ok_or_else(|| {
                        SchedulerError::InvalidJob {
                            message: format!(
                                "task {} is assigned without an executor",
                                task.task_id()
                            ),
                        }
                    })?;

                    let lease_generation = executor_leases
                        .iter()
                        .find_map(|(known_executor, lease_generation)| {
                            (known_executor == &executor_id).then_some(*lease_generation)
                        })
                        .ok_or_else(|| SchedulerError::UnknownExecutor {
                            executor_id: executor_id.clone(),
                        })?;

                    task.attempt = task.attempt.saturating_add(1);
                    task.launch_in_flight = true;
                    let attempt_id = AttemptId::try_new(task.attempt).map_err(|error| {
                        SchedulerError::InvalidJob {
                            message: error.to_string(),
                        }
                    })?;
                    let task_description = task.spec.description().to_owned();
                    let task_timeout_secs = task.spec.task_timeout_secs();
                    let input_partitions = if let Some(tables) = batch_sql_tables {
                        tables
                            .iter()
                            .enumerate()
                            .map(|(idx, table)| {
                                InputPartition::new(format!("parquet-{idx}"), String::new())
                                    .with_descriptor(InputPartitionDescriptor::LocalParquet {
                                        table_name: table.table_name.clone(),
                                        path: table.path.to_string_lossy().into_owned(),
                                    })
                            })
                            .collect()
                    } else if let Some(parts_by_task) = task_inline_partitions {
                        parts_by_task.get(task.task_id()).cloned().ok_or_else(|| {
                            SchedulerError::InvalidJob {
                                message: format!(
                                    "task {} has no registered task-scoped input partition",
                                    task.task_id()
                                ),
                            }
                        })?
                    } else if let Some(parts) = inline_partitions {
                        parts.to_vec()
                    } else {
                        vec![InputPartition::new(
                            task.task_id().as_str(),
                            task_description.clone(),
                        )]
                    };
                    let mut assignment = ExecutorTaskAssignment::new(
                        TaskAttemptRef::new(
                            self.spec.job_id().clone(),
                            stage_id.clone(),
                            task.task_id().clone(),
                            attempt_id,
                        ),
                        executor_id,
                        lease_generation,
                        PlanFragment::new(task_description.clone()).with_streaming(
                            krishiv_plan::execution_kind_from_fragment(&task_description)
                                == PlanExecutionKind::Streaming,
                        ),
                        OutputContract::new(
                            OutputContractKind::InlineRecordBatches,
                            format!("inline result for {}", task.task_id()),
                        ),
                    )
                    .with_input_partitions(input_partitions)
                    .with_key_group_range(key_group_range_for_task(task_index, stage_parallelism));
                    if task_body.starts_with("stream:loop:") {
                        assignment = assignment.with_requires_reattach(true);
                    }
                    if let Some(secs) = task_timeout_secs {
                        assignment = assignment.with_task_timeout_secs(secs);
                    }
                    if let Some(nanos) = self.spec.cpu_limit_nanos() {
                        assignment = assignment.with_cpu_limit_nanos(nanos);
                    }
                    if let Some(bytes) = self.spec.memory_limit_bytes() {
                        assignment = assignment.with_memory_limit_bytes(bytes);
                    }
                    // Propagate typed shuffle configs from the task spec to the
                    // assignment. Without this the executor never sees the
                    // ShuffleWriteConfig / ShuffleReadConfig attached at job
                    // submission time — they stayed on the TaskSpec but were
                    // never forwarded to ExecutorTaskAssignment.
                    if let Some(write_cfg) = task.spec.shuffle_write() {
                        let effective_num_partitions = skew_partition_override
                            .map(|n| n as usize)
                            .unwrap_or(write_cfg.num_partitions)
                            .max(1);
                        assignment =
                            assignment.with_shuffle_write(krishiv_proto::ShuffleWriteConfig {
                                num_partitions: effective_num_partitions,
                                ..write_cfg.clone()
                            });
                    }
                    if let Some(read_cfg) = task.spec.shuffle_read() {
                        assignment = assignment.with_shuffle_read(read_cfg.clone());
                    }
                    assignments.push(assignment);
                }
            }
            if stage
                .tasks
                .iter()
                .any(|task| task.state == TaskState::Running)
            {
                stage.state = StageState::Running;
            }
        }
        Ok(assignments)
    }

    pub(crate) fn apply_task_update(
        &mut self,
        update: TaskStatusUpdate,
    ) -> SchedulerResult<TaskUpdateOutcome> {
        use krishiv_proto::ShufflePartitionOutput;
        let stage_id = update.stage_id().clone();
        let shuffle_partitions: Vec<ShufflePartitionOutput> = update
            .output_metadata()
            .map(|m| m.shuffle_partitions().to_vec())
            .unwrap_or_default();

        // Capture stats before consuming the update.
        let runtime_stats = update
            .output_metadata()
            .and_then(|m| m.runtime_stats())
            .map(|s| (s.cpu_nanos, s.memory_bytes));

        let stage = self
            .stages
            .iter_mut()
            .find(|stage| stage.stage_id() == &stage_id)
            .ok_or_else(|| SchedulerError::UnknownStage {
                stage_id: stage_id.clone(),
            })?;

        let outcome = stage.apply_task_update(update, self.max_stage_retries)?;

        // Accumulate resource stats from successfully-completed tasks.
        if outcome != TaskUpdateOutcome::Duplicate
            && let Some((cpu_nanos, memory_bytes)) = runtime_stats
        {
            self.resource_usage.add_task_stats(cpu_nanos, memory_bytes);
        }

        // If the task succeeded with shuffle output, record partition availability.
        if outcome != TaskUpdateOutcome::Duplicate && !shuffle_partitions.is_empty() {
            let meta = self.shuffle_output.entry(stage_id.clone()).or_default();
            for p in &shuffle_partitions {
                let path = ShufflePath {
                    job_id: self.spec.job_id().as_str().to_owned(),
                    stage_id: stage_id.as_str().to_owned(),
                    partition_id: p.partition_id,
                };
                meta.mark_available(&path);
            }
        }

        self.refresh_state();
        Ok(outcome)
    }

    pub(crate) fn cancel(&mut self) {
        self.state = JobState::Cancelled;
        for stage in &mut self.stages {
            stage.cancel();
        }
    }

    pub fn retry_count(&self) -> usize {
        self.stages
            .iter()
            .map(|stage| stage.retry_count() as usize)
            .sum()
    }

    pub fn failed_task_count(&self) -> usize {
        self.stages
            .iter()
            .flat_map(StageRecord::tasks)
            .filter(|task| task.state() == TaskState::Failed)
            .count()
    }

    pub fn running_task_count(&self) -> usize {
        self.stages
            .iter()
            .flat_map(StageRecord::tasks)
            .filter(|task| task.state() == TaskState::Running)
            .count()
    }

    pub(crate) fn refresh_state(&mut self) {
        if self
            .stages
            .iter()
            .any(|stage| stage.state == StageState::Failed)
        {
            self.state = JobState::Failed;
            return;
        }
        // Streaming jobs never enter Succeeded while running — they run until
        // explicitly stopped or failed. Only batch jobs transition to Succeeded.
        if self.spec.kind() != JobKind::Streaming
            && self
                .stages
                .iter()
                .all(|stage| stage.state == StageState::Succeeded)
        {
            self.state = JobState::Succeeded;
        } else {
            self.state = JobState::Running;
        }
    }

    pub(crate) fn snapshot(&self) -> JobSnapshot {
        let mut task_count = 0;
        let mut assigned_task_count = 0;
        let mut running_task_count = 0;
        let mut succeeded_task_count = 0;
        let mut failed_task_count = 0;

        for task in self.stages.iter().flat_map(StageRecord::tasks) {
            task_count += 1;
            match task.state() {
                TaskState::Assigned => assigned_task_count += 1,
                TaskState::Running => running_task_count += 1,
                TaskState::Succeeded => succeeded_task_count += 1,
                TaskState::Failed => failed_task_count += 1,
                TaskState::Pending | TaskState::Retrying | TaskState::Cancelled => {}
            }
        }

        JobSnapshot {
            job_id: self.spec.job_id().clone(),
            kind: self.spec.kind(),
            state: self.state,
            stage_count: self.stages.len(),
            task_count,
            assigned_task_count,
            running_task_count,
            succeeded_task_count,
            failed_task_count,
            priority: self.spec.priority(),
            namespace_id: self.spec.namespace_id().map(str::to_owned),
            resource_usage: self.resource_usage.clone(),
        }
    }

    pub(crate) fn detail_snapshot(&self) -> JobDetailSnapshot {
        JobDetailSnapshot {
            job: self.snapshot(),
            stages: self.stages.iter().map(StageRecord::snapshot).collect(),
        }
    }

    /// Total number of shuffle partitions marked Available across all stages.
    pub fn shuffle_partitions_available_count(&self) -> usize {
        self.shuffle_output
            .values()
            .map(ShuffleMetadata::available_count)
            .sum()
    }

    /// Total shuffle bytes written across all stages (sum of partition size_bytes
    /// as recorded by executor TaskOutputMetadata).
    pub fn shuffle_bytes_written(&self) -> u64 {
        self.stages
            .iter()
            .flat_map(StageRecord::tasks)
            .filter_map(|t| t.output_metadata.as_ref())
            .flat_map(|m| m.shuffle_partitions())
            .map(|p| p.size_bytes)
            .sum()
    }
}

/// Stage record owned by a job coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageRecord {
    pub(crate) spec: StageSpec,
    pub(crate) state: StageState,
    pub(crate) retry_count: u32,
    pub(crate) tasks: Vec<TaskRecord>,
}

impl StageRecord {
    pub(crate) fn from_spec(spec: StageSpec) -> Self {
        let tasks = spec
            .tasks()
            .iter()
            .cloned()
            .map(TaskRecord::from_spec)
            .collect();
        Self {
            spec,
            state: StageState::Pending,
            retry_count: 0,
            tasks,
        }
    }

    /// Stage id.
    pub fn stage_id(&self) -> &StageId {
        self.spec.stage_id()
    }

    /// Stage state.
    pub fn state(&self) -> StageState {
        self.state
    }

    /// Task records.
    pub fn tasks(&self) -> &[TaskRecord] {
        &self.tasks
    }

    /// Mutable task records (used by the streaming re-attach state update path).
    pub(crate) fn tasks_mut(&mut self) -> &mut [TaskRecord] {
        &mut self.tasks
    }

    /// Number of stage-level retries already scheduled.
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    pub(crate) fn apply_task_update(
        &mut self,
        update: TaskStatusUpdate,
        max_stage_retries: u32,
    ) -> SchedulerResult<TaskUpdateOutcome> {
        let max_task_attempts = self.spec.max_task_attempts();

        let task_idx = self
            .tasks
            .iter()
            .position(|task| task.task_id() == update.task_id())
            .ok_or_else(|| SchedulerError::UnknownTask {
                task_id: update.task_id().clone(),
            })?;

        let outcome = self.tasks[task_idx].apply_status_update(&update)?;
        if outcome == TaskUpdateOutcome::Duplicate {
            return Ok(outcome);
        }

        if update.state() == TaskState::Failed {
            // Try per-task retry first: if this specific task still has attempts remaining,
            // reset only that task to Pending rather than retrying the entire stage
            // (which would reset even succeeded tasks).
            let task = &mut self.tasks[task_idx];
            task.failure_count = task.failure_count.saturating_add(1);
            let task_failure_count = task.failure_count;

            if task_failure_count < max_task_attempts {
                let task = &mut self.tasks[task_idx];
                task.state = TaskState::Pending;
                task.assigned_executor = None;
                task.launch_in_flight = false;
                self.refresh_state();
                return Ok(TaskUpdateOutcome::Applied);
            }

            // Per-task attempts exhausted — fall back to whole-stage retry if configured.
            if self.retry_count < max_stage_retries {
                self.retry_stage();
                return Ok(TaskUpdateOutcome::Applied);
            }
        }

        self.refresh_state();
        Ok(TaskUpdateOutcome::Applied)
    }

    pub(crate) fn cancel(&mut self) {
        self.state = StageState::Cancelled;
        for task in &mut self.tasks {
            if !task.state().is_terminal() {
                task.cancel();
            }
        }
    }

    fn retry_stage(&mut self) {
        self.retry_count = self.retry_count.saturating_add(1);
        self.state = StageState::Retrying;

        // Reset to Pending so the scheduler can re-queue and re-assign.
        // Assigned would bypass placement logic on the next schedule pass.
        for task in &mut self.tasks {
            task.state = TaskState::Pending;
            task.assigned_executor = None;
            task.launch_in_flight = false;
        }
    }

    pub(crate) fn refresh_state(&mut self) {
        let mut all_succeeded = true;
        let mut any_failed = false;
        let mut any_running = false;
        let mut any_assigned = false;

        for task in &self.tasks {
            match task.state {
                TaskState::Succeeded => {}
                TaskState::Failed => {
                    all_succeeded = false;
                    any_failed = true;
                }
                TaskState::Running => {
                    all_succeeded = false;
                    any_running = true;
                }
                TaskState::Assigned => {
                    all_succeeded = false;
                    any_assigned = true;
                }
                _ => {
                    all_succeeded = false;
                }
            }
        }

        self.state = if all_succeeded {
            StageState::Succeeded
        } else if any_failed {
            StageState::Failed
        } else if any_running {
            StageState::Running
        } else if any_assigned {
            StageState::Scheduling
        } else {
            StageState::Pending
        };
    }

    pub(crate) fn snapshot(&self) -> StageSnapshot {
        StageSnapshot {
            stage_id: self.spec.stage_id().clone(),
            state: self.state,
            retry_count: self.retry_count,
            task_count: self.tasks.len(),
            tasks: self.tasks.iter().map(TaskRecord::snapshot).collect(),
        }
    }
}

/// Task record owned by a job coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRecord {
    pub(crate) spec: TaskSpec,
    pub(crate) state: TaskState,
    pub(crate) assigned_executor: Option<ExecutorId>,
    pub(crate) attempt: u32,
    pub(crate) launch_in_flight: bool,
    pub(crate) output_metadata: Option<TaskOutputMetadata>,
    pub(crate) last_failure_reason: Option<String>,
    /// How many times this specific task has failed; drives per-task retry budget.
    pub(crate) failure_count: u32,
    /// Number of times this task's executor was lost (marked Lost/timeout).
    /// Incremented each time `reset_running_tasks_for_lost_executor` rescheduled
    /// this task; distinct from `failure_count` (which tracks task-reported failures).
    pub(crate) executor_loss_count: u32,
    /// Last event-time watermark reported by the executor for this streaming task.
    /// `None` for batch tasks or streaming tasks that have not yet heartbeated.
    pub(crate) last_watermark_ms: Option<i64>,
    /// Last committed source offset reported by the executor for this streaming task.
    /// Connector-specific encoding; `None` for batch tasks.
    pub(crate) last_source_offset: Option<Vec<u8>>,
    /// Wall-clock timestamp (ms since UNIX epoch) when the task most recently
    /// Wall-clock ms when the task most recently entered Running state.
    /// Used by stall detection to identify hung tasks.
    pub(crate) assigned_at_ms: Option<u64>,
}

impl TaskRecord {
    pub(crate) fn from_spec(spec: TaskSpec) -> Self {
        Self {
            spec,
            state: TaskState::Pending,
            assigned_executor: None,
            attempt: 0,
            launch_in_flight: false,
            output_metadata: None,
            last_failure_reason: None,
            failure_count: 0,
            executor_loss_count: 0,
            last_watermark_ms: None,
            last_source_offset: None,
            assigned_at_ms: None,
        }
    }

    /// Task id.
    pub fn task_id(&self) -> &TaskId {
        self.spec.task_id()
    }

    /// Task state.
    pub fn state(&self) -> TaskState {
        self.state
    }

    /// Assigned executor, if any.
    pub fn assigned_executor(&self) -> Option<&ExecutorId> {
        self.assigned_executor.as_ref()
    }

    /// Current attempt number.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub(crate) fn launch_in_flight(&self) -> bool {
        self.launch_in_flight
    }

    pub(crate) fn clear_launch_in_flight(&mut self) {
        self.launch_in_flight = false;
    }

    /// Last reported output metadata.
    pub fn output_metadata(&self) -> Option<&TaskOutputMetadata> {
        self.output_metadata.as_ref()
    }

    /// Last failure reason reported by the executor, if any.
    pub fn last_failure_reason(&self) -> Option<&str> {
        self.last_failure_reason.as_deref()
    }

    /// Last event-time watermark reported by this streaming task's executor (milliseconds since epoch).
    pub fn last_watermark_ms(&self) -> Option<i64> {
        self.last_watermark_ms
    }

    /// Last committed source offset reported by this streaming task's executor.
    pub fn last_source_offset(&self) -> Option<&[u8]> {
        self.last_source_offset.as_deref()
    }

    /// Apply streaming task state received from an executor heartbeat.
    ///
    /// Called by the re-attach protocol to update the coordinator's view of the
    /// task's progress without re-submitting the job.
    pub(crate) fn apply_streaming_state(&mut self, state: &StreamingTaskState) {
        self.last_watermark_ms = Some(state.watermark_ms as i64);
        if !state.source_offset.is_empty() {
            self.last_source_offset = Some(state.source_offset.clone());
        }
    }

    pub(crate) fn cancel(&mut self) {
        self.state = TaskState::Cancelled;
        self.launch_in_flight = false;
    }

    pub(crate) fn apply_status_update(
        &mut self,
        update: &TaskStatusUpdate,
    ) -> SchedulerResult<TaskUpdateOutcome> {
        if self.attempt == 0 {
            return Err(SchedulerError::InvalidJob {
                message: format!(
                    "task {} received status update at attempt 0 — task was never launched",
                    self.task_id()
                ),
            });
        }

        if update.attempt() != self.attempt {
            return Err(SchedulerError::StaleTaskAttempt {
                task_id: self.task_id().clone(),
                expected: self.attempt,
                received: update.attempt(),
            });
        }

        if self.assigned_executor.as_ref() != Some(update.executor_id()) {
            return Err(SchedulerError::StaleTaskAttempt {
                task_id: self.task_id().clone(),
                expected: self.attempt,
                received: update.attempt(),
            });
        }

        if self.state == update.state() {
            return Ok(TaskUpdateOutcome::Duplicate);
        }

        if self.state.is_terminal()
            || (self.state != TaskState::Running
                && self.state != TaskState::Assigned
                && update.state() != TaskState::Running)
        {
            return Err(SchedulerError::StaleTaskAttempt {
                task_id: self.task_id().clone(),
                expected: self.attempt,
                received: update.attempt(),
            });
        }

        self.state = update.state();
        self.launch_in_flight = false;
        self.assigned_executor = Some(update.executor_id().clone());
        self.attempt = update.attempt();
        if let Some(output_metadata) = update.output_metadata() {
            self.output_metadata = Some(output_metadata.clone());
        }
        if self.state == TaskState::Failed {
            self.last_failure_reason = update.message().map(ToOwned::to_owned);
        }
        // Record assignment time when the task starts running so the stall-detection loop
        // can identify hung tasks.
        if self.state == TaskState::Running {
            self.assigned_at_ms =
                Some(u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0));
        } else if self.state.is_terminal() {
            self.assigned_at_ms = None;
        }
        Ok(TaskUpdateOutcome::Applied)
    }

    pub(crate) fn snapshot(&self) -> TaskSnapshot {
        TaskSnapshot {
            task_id: self.spec.task_id().clone(),
            state: self.state,
            assigned_executor: self.assigned_executor.clone(),
            attempt: self.attempt,
            output_metadata: self.output_metadata.clone(),
            last_failure_reason: self.last_failure_reason.clone(),
            failure_count: self.failure_count,
            source_capabilities: self.spec.source_capabilities.clone(),
            sink_capabilities: self.spec.sink_capabilities.clone(),
            last_watermark_ms: self.last_watermark_ms,
            last_source_offset: self.last_source_offset.clone(),
        }
    }
}

/// Job status summary for CLI/UI use in later R2 slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSnapshot {
    pub(crate) job_id: JobId,
    pub(crate) kind: JobKind,
    pub(crate) state: JobState,
    pub(crate) stage_count: usize,
    pub(crate) task_count: usize,
    pub(crate) assigned_task_count: usize,
    pub(crate) running_task_count: usize,
    pub(crate) succeeded_task_count: usize,
    pub(crate) failed_task_count: usize,
    /// Scheduling priority (0 = lowest, 255 = highest).
    pub(crate) priority: u8,
    /// Governance namespace, if set.
    pub(crate) namespace_id: Option<String>,
    /// Accumulated resource consumption from completed tasks.
    pub(crate) resource_usage: ResourceUsage,
}

impl JobSnapshot {
    /// Job id.
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// Job kind.
    pub fn kind(&self) -> JobKind {
        self.kind
    }

    /// Job state.
    pub fn state(&self) -> JobState {
        self.state
    }

    /// Number of stages.
    pub fn stage_count(&self) -> usize {
        self.stage_count
    }

    /// Number of tasks.
    pub fn task_count(&self) -> usize {
        self.task_count
    }

    /// Number of assigned tasks.
    pub fn assigned_task_count(&self) -> usize {
        self.assigned_task_count
    }

    /// Number of running tasks.
    pub fn running_task_count(&self) -> usize {
        self.running_task_count
    }

    /// Number of succeeded tasks.
    pub fn succeeded_task_count(&self) -> usize {
        self.succeeded_task_count
    }

    /// Number of failed tasks.
    pub fn failed_task_count(&self) -> usize {
        self.failed_task_count
    }

    /// Scheduling priority.
    pub fn priority(&self) -> u8 {
        self.priority
    }

    /// Governance namespace.
    pub fn namespace_id(&self) -> Option<&str> {
        self.namespace_id.as_deref()
    }

    /// Accumulated resource consumption.
    pub fn resource_usage(&self) -> &ResourceUsage {
        &self.resource_usage
    }
}

/// Detailed job status for CLI/UI use in later R2 slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobDetailSnapshot {
    pub(crate) job: JobSnapshot,
    pub(crate) stages: Vec<StageSnapshot>,
}

impl JobDetailSnapshot {
    /// Job summary.
    pub fn job(&self) -> &JobSnapshot {
        &self.job
    }

    /// Stage summaries.
    pub fn stages(&self) -> &[StageSnapshot] {
        &self.stages
    }
}

/// Stage status summary for CLI/UI use in later R2 slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageSnapshot {
    pub(crate) stage_id: StageId,
    pub(crate) state: StageState,
    pub(crate) retry_count: u32,
    pub(crate) task_count: usize,
    pub(crate) tasks: Vec<TaskSnapshot>,
}

impl StageSnapshot {
    /// Stage id.
    pub fn stage_id(&self) -> &StageId {
        &self.stage_id
    }

    /// Stage state.
    pub fn state(&self) -> StageState {
        self.state
    }

    /// Number of stage-level retries already scheduled.
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    /// Number of tasks in this stage.
    pub fn task_count(&self) -> usize {
        self.task_count
    }

    /// Task summaries.
    pub fn tasks(&self) -> &[TaskSnapshot] {
        &self.tasks
    }
}

/// Task status summary for CLI/UI use in later R2 slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSnapshot {
    pub(crate) task_id: TaskId,
    pub(crate) state: TaskState,
    pub(crate) assigned_executor: Option<ExecutorId>,
    pub(crate) attempt: u32,
    pub(crate) output_metadata: Option<TaskOutputMetadata>,
    pub(crate) last_failure_reason: Option<String>,
    pub(crate) failure_count: u32,
    /// Capability flags declared by the source connector for this task, if known.
    pub source_capabilities: Option<ConnectorCapabilityFlags>,
    /// Capability flags declared by the sink connector for this task, if known.
    pub sink_capabilities: Option<ConnectorCapabilityFlags>,
    /// Last event-time watermark reported by this streaming task's executor (ms since epoch).
    pub last_watermark_ms: Option<i64>,
    /// Last committed source offset reported by this streaming task's executor.
    pub last_source_offset: Option<Vec<u8>>,
}

impl TaskSnapshot {
    /// Task id.
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Task state.
    pub fn state(&self) -> TaskState {
        self.state
    }

    /// Assigned executor, if any.
    pub fn assigned_executor(&self) -> Option<&ExecutorId> {
        self.assigned_executor.as_ref()
    }

    /// Current attempt number.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Last reported output metadata.
    pub fn output_metadata(&self) -> Option<&TaskOutputMetadata> {
        self.output_metadata.as_ref()
    }

    /// Last failure reason reported by the executor, if any.
    pub fn last_failure_reason(&self) -> Option<&str> {
        self.last_failure_reason.as_deref()
    }

    /// Number of times this specific task has failed (for observability).
    pub fn failure_count(&self) -> u32 {
        self.failure_count
    }

    /// Last event-time watermark reported by this streaming task (ms since epoch).
    pub fn last_watermark_ms(&self) -> Option<i64> {
        self.last_watermark_ms
    }

    /// Last committed source offset reported by this streaming task.
    pub fn last_source_offset(&self) -> Option<&[u8]> {
        self.last_source_offset.as_deref()
    }
}

/// Basic R3.1 scheduler/executor stability metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StabilityMetrics {
    pub(crate) heartbeat_ages: Vec<ExecutorHeartbeatAge>,
    pub(crate) retry_count: usize,
    pub(crate) running_task_count: usize,
    pub(crate) failed_assignments: usize,
    /// Total shuffle partitions currently marked Available across all active jobs.
    pub shuffle_partitions_available: usize,
    /// Total shuffle bytes written across all active jobs.
    pub shuffle_bytes_written: u64,
}

impl StabilityMetrics {
    /// Zero-valued metrics for use when the coordinator lock is unavailable.
    pub fn empty() -> Self {
        Self {
            heartbeat_ages: Vec::new(),
            retry_count: 0,
            running_task_count: 0,
            failed_assignments: 0,
            shuffle_partitions_available: 0,
            shuffle_bytes_written: 0,
        }
    }

    /// Heartbeat age per executor.
    pub fn heartbeat_ages(&self) -> &[ExecutorHeartbeatAge] {
        &self.heartbeat_ages
    }

    /// Total stage retry count.
    pub fn retry_count(&self) -> usize {
        self.retry_count
    }

    /// Currently running task count.
    pub fn running_task_count(&self) -> usize {
        self.running_task_count
    }

    /// Failed assignment count.
    pub fn failed_assignments(&self) -> usize {
        self.failed_assignments
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
    if let Some(interval) = spec.checkpoint_interval_ms() && interval == 0 {
        return Err(SchedulerError::InvalidJob {
            message: String::from(
                "checkpoint_interval_ms must be > 0; use None to disable checkpointing",
            ),
        });
    }
    if let Some(path) = spec.checkpoint_storage_path() && path.is_empty() {
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
fn topo_sort_plan_nodes(nodes: &[PlanNode]) -> SchedulerResult<Vec<&PlanNode>> {
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

#[cfg(test)]
mod exchange_stage_tests {
    use super::*;
    use krishiv_plan::{ExecutionKind, Partitioning, PlanNode, TypedTaskFragment};
    use krishiv_proto::{InputPartition, JobId};

    #[test]
    fn physical_plan_with_exchange_produces_multi_stage_job() {
        let scan = PlanNode::new("scan", "scan", ExecutionKind::Batch).with_op(NodeOp::Scan {
            table: String::from("t"),
            filters: vec![],
        });
        let exchange = PlanNode::new("ex", "exchange", ExecutionKind::Batch)
            .with_inputs(["scan"])
            .with_op(NodeOp::Exchange {
                partitioning: Partitioning::Hash {
                    keys: vec![String::from("k")],
                    buckets: 2,
                },
            });
        let agg = PlanNode::new("agg", "aggregate", ExecutionKind::Batch)
            .with_inputs(["ex"])
            .with_op(NodeOp::Aggregate {
                group_keys: vec![String::from("k")],
            });
        let plan = PhysicalPlan::new("exchange-plan", ExecutionKind::Batch)
            .with_node(scan)
            .with_node(exchange)
            .with_node(agg);
        let job_id = JobId::try_new("job-exchange-test").unwrap();
        let spec = job_spec_from_physical_plan(job_id, &plan).unwrap();
        assert_eq!(spec.stages().len(), 2);
        assert_eq!(
            spec.stages()[1].upstream_stage_ids().len(),
            1,
            "downstream stage must declare upstream dependency"
        );
    }

    #[test]
    fn physical_plan_conversion_rejects_invalid_graph() {
        let plan = PhysicalPlan::new("invalid", ExecutionKind::Batch).with_node(
            PlanNode::new("sink", "sink", ExecutionKind::Batch).with_inputs(["missing"]),
        );
        let job_id = JobId::try_new("job-invalid-plan").unwrap();

        let error = job_spec_from_physical_plan(job_id, &plan).expect_err("invalid graph");

        assert!(matches!(error, SchedulerError::InvalidPlan { .. }));
        assert!(error.to_string().contains("missing input 'missing'"));
    }

    #[test]
    fn topological_sort_handles_duplicate_edges() {
        let nodes = vec![
            PlanNode::new("scan", "scan", ExecutionKind::Batch),
            PlanNode::new("self-join", "self join", ExecutionKind::Batch)
                .with_inputs(["scan", "scan"])
                .with_op(NodeOp::Join {
                    join_type: krishiv_plan::JoinType::Inner,
                }),
        ];

        let ordered = topo_sort_plan_nodes(&nodes).expect("topological order");

        assert_eq!(
            ordered.iter().map(|node| node.id()).collect::<Vec<_>>(),
            vec!["scan", "self-join"]
        );
    }

    #[test]
    fn key_group_ranges_split_stage_parallelism() {
        let first = key_group_range_for_task(0, 4);
        let second = key_group_range_for_task(1, 4);
        let last = key_group_range_for_task(3, 4);

        assert_eq!((first.start(), first.end()), (0, 8191));
        assert_eq!((second.start(), second.end()), (8192, 16383));
        assert_eq!((last.start(), last.end()), (24576, 32767));
    }

    #[test]
    fn typed_continuous_loop_assignment_requires_reattach() {
        let job_id = JobId::try_new("continuous-assignment-job").unwrap();
        let stage_id = StageId::try_new("continuous-stage").unwrap();
        let task_id = TaskId::try_new("continuous-task").unwrap();
        let executor_id = ExecutorId::try_new("continuous-executor").unwrap();
        let fragment = TypedTaskFragment::new(
            ExecutionKind::Streaming,
            "stream:loop:continuous-assignment-job|\
             stream:tw:key=key:time=ts:win=10000:lag=0:agg=count",
        )
        .encode()
        .unwrap();
        let spec = JobSpec::new(job_id, "continuous", JobKind::Streaming).with_stage(
            StageSpec::new(stage_id, "continuous-stage")
                .with_task(TaskSpec::new(task_id.clone(), fragment)),
        );
        let mut job = JobRecord::from_spec(spec, 1);
        job.apply_assignments(vec![TaskAssignment::new(task_id, executor_id.clone())]);
        let input = vec![InputPartition::new("cycle-input", "inline")];

        let assignments = job
            .launch_assigned_task_assignments(
                &[(executor_id, LeaseGeneration::initial())],
                None,
                Some(&input),
                None,
                None,
            )
            .unwrap();

        assert_eq!(assignments.len(), 1);
        assert!(assignments[0].requires_reattach());
    }

    #[test]
    fn task_scoped_inputs_are_bound_to_the_matching_task() {
        let job_id = JobId::try_new("task-scoped-input-job").unwrap();
        let stage_id = StageId::try_new("task-scoped-stage").unwrap();
        let task_a = TaskId::try_new("task-a").unwrap();
        let task_b = TaskId::try_new("task-b").unwrap();
        let executor_a = ExecutorId::try_new("executor-a").unwrap();
        let executor_b = ExecutorId::try_new("executor-b").unwrap();
        let spec = JobSpec::new(job_id, "task-scoped", JobKind::Batch).with_stage(
            StageSpec::new(stage_id, "stage")
                .with_task(TaskSpec::new(task_a.clone(), "window:a"))
                .with_task(TaskSpec::new(task_b.clone(), "window:b")),
        );
        let mut job = JobRecord::from_spec(spec, 1);
        job.apply_assignments(vec![
            TaskAssignment::new(task_a.clone(), executor_a.clone()),
            TaskAssignment::new(task_b.clone(), executor_b.clone()),
        ]);
        let task_inputs = std::collections::HashMap::from([
            (
                task_a.clone(),
                vec![InputPartition::new("input-a", "partition-a")],
            ),
            (
                task_b.clone(),
                vec![InputPartition::new("input-b", "partition-b")],
            ),
        ]);

        let assignments = job
            .launch_assigned_task_assignments(
                &[
                    (executor_a, LeaseGeneration::initial()),
                    (executor_b, LeaseGeneration::initial()),
                ],
                None,
                None,
                Some(&task_inputs),
                None,
            )
            .unwrap();
        assert_eq!(assignments.len(), 2);
        for assignment in assignments {
            let expected_partition = if assignment.task_id() == &task_a {
                "input-a"
            } else {
                assert_eq!(assignment.task_id(), &task_b);
                "input-b"
            };
            assert_eq!(assignment.input_partitions().len(), 1);
            assert_eq!(
                assignment.input_partitions()[0].partition_id(),
                expected_partition
            );
        }
    }
}
