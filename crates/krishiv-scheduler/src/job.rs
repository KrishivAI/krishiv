use std::collections::HashMap;

use krishiv_plan::{ExecutionKind as PlanExecutionKind, LogicalPlan, PhysicalPlan, PlanNode};
use krishiv_proto::{
    AttemptId, ConnectorCapabilityFlags, ExecutorDescriptor, ExecutorId, ExecutorTaskAssignment,
    InputPartition, JobId, JobKind, JobSpec, JobState, LeaseGeneration, OutputContract,
    OutputContractKind, PlanFragment, StageId, StageSpec, StageState, StreamingTaskState,
    TaskAssignment, TaskAttemptRef, TaskId, TaskOutputMetadata, TaskSpec, TaskState,
    TaskStatusUpdate,
};
use krishiv_shuffle::{ShuffleMetadata, ShufflePath};

use crate::{
    ExecutorHeartbeatAge, SchedulerError, SchedulerResult, TaskUpdateOutcome,
};

/// Result of a `Coordinator::submit_job` call.
///
/// R7.1 introduces `Queued` when admission control cannot immediately place the
/// job.  All current callers receive `Accepted` because `InMemoryQueueManager`
/// always admits.  Code that discards the outcome (`.unwrap()`, `?`) requires
/// no change; code that pattern-matches must handle both variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// Job was admitted and is now scheduled.
    Accepted,
    /// Job was held by the admission controller; not yet running.
    ///
    /// `position` is a 0-based index in the admission queue.
    Queued { position: usize },
}

// ── R7.1 Resource governance types ───────────────────────────────────────────

/// Accumulated resource consumption for one job.
///
/// Populated from `TaskRuntimeStats` as tasks complete. Used by the status API
/// and for post-hoc cost attribution. Not used for real-time quota enforcement
/// (admission uses reservation-based accounting from `JobSpec` fields).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResourceUsage {
    /// Total CPU nanoseconds consumed by all completed tasks.
    pub cpu_nanos: u64,
    /// Peak memory bytes observed across all completed tasks.
    pub memory_peak_bytes: u64,
    /// Number of completed tasks that have reported stats.
    pub task_count: u32,
}

impl ResourceUsage {
    /// Empty usage.
    pub fn zero() -> Self {
        Self::default()
    }

    /// Absorb stats from one completed task.
    pub fn add_task_stats(&mut self, cpu_nanos: u64, memory_bytes: u64) {
        self.cpu_nanos = self.cpu_nanos.saturating_add(cpu_nanos);
        self.memory_peak_bytes = self.memory_peak_bytes.max(memory_bytes);
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
    ///
    /// P2.5: Accepts borrowed descriptors so callers avoid cloning the full
    /// descriptor vec on every `submit_job` call.
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
            resource_usage: ResourceUsage::zero(),
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

    pub(crate) fn apply_assignments(&mut self, assignments: Vec<TaskAssignment>) {
        self.state = JobState::Running;
        for stage in &mut self.stages {
            stage.state = StageState::Scheduling;
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
    ) -> SchedulerResult<Vec<ExecutorTaskAssignment>> {
        let mut assignments = Vec::new();
        self.state = JobState::Running;

        // P2.10: Build a HashSet once for O(1) upstream-ready checks instead of
        // O(stages²) Vec::contains per stage in the outer loop.
        // Clone the IDs so the set owns its data and does not borrow self.stages.
        let succeeded_stage_ids: std::collections::HashSet<StageId> = self
            .stages
            .iter()
            .filter(|s| s.state == StageState::Succeeded)
            .map(|s| s.stage_id().clone())
            .collect();

        for stage in &mut self.stages {
            let stage_id = stage.stage_id().clone();

            // Skip stages whose upstream shuffle dependencies are not yet complete.
            let upstream_ready = stage
                .spec
                .upstream_stage_ids()
                .iter()
                .all(|up| succeeded_stage_ids.contains(up));
            if !upstream_ready {
                continue;
            }

            for task in &mut stage.tasks {
                if task.state == TaskState::Assigned {
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

                    task.state = TaskState::Running;
                    task.attempt = task.attempt.saturating_add(1);
                    let attempt_id = AttemptId::try_new(task.attempt).map_err(|error| {
                        SchedulerError::InvalidJob {
                            message: error.to_string(),
                        }
                    })?;
                    let task_description = task.spec.description().to_owned();
                    let task_timeout_secs = task.spec.task_timeout_secs();
                    let mut assignment = ExecutorTaskAssignment::new(
                        TaskAttemptRef::new(
                            self.spec.job_id().clone(),
                            stage_id.clone(),
                            task.task_id().clone(),
                            attempt_id,
                        ),
                        executor_id,
                        lease_generation,
                        PlanFragment::new(task_description.clone()),
                        OutputContract::new(
                            OutputContractKind::InlineRecordBatches,
                            format!("inline result for {}", task.task_id()),
                        ),
                    )
                    .with_input_partitions(vec![InputPartition::new(
                        task.task_id().as_str(),
                        task_description,
                    )]);
                    if let Some(secs) = task_timeout_secs {
                        assignment = assignment.with_task_timeout_secs(secs);
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
        if !shuffle_partitions.is_empty() {
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
        let task = self
            .tasks
            .iter_mut()
            .find(|task| task.task_id() == update.task_id())
            .ok_or_else(|| SchedulerError::UnknownTask {
                task_id: update.task_id().clone(),
            })?;

        let outcome = task.apply_status_update(&update)?;
        if outcome == TaskUpdateOutcome::Duplicate {
            return Ok(outcome);
        }

        if update.state() == TaskState::Failed && self.retry_count < max_stage_retries {
            self.retry_stage();
            return Ok(TaskUpdateOutcome::Applied);
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

        // P1.24: Always reset to Pending so the scheduler can re-queue and re-assign.
        // Using Assigned here would bypass the placement logic on the next schedule pass.
        for task in &mut self.tasks {
            task.state = TaskState::Pending;
            task.assigned_executor = None;
        }
    }

    /// P2.7: Single-pass over tasks instead of four separate iterator passes.
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
    pub(crate) output_metadata: Option<TaskOutputMetadata>,
    pub(crate) last_failure_reason: Option<String>,
    /// Last event-time watermark reported by the executor for this streaming task.
    /// `None` for batch tasks or streaming tasks that have not yet heartbeated.
    pub(crate) last_watermark_ms: Option<i64>,
    /// Last committed source offset reported by the executor for this streaming task.
    /// Connector-specific encoding; `None` for batch tasks.
    pub(crate) last_source_offset: Option<Vec<u8>>,
}

impl TaskRecord {
    pub(crate) fn from_spec(spec: TaskSpec) -> Self {
        Self {
            spec,
            state: TaskState::Pending,
            assigned_executor: None,
            attempt: 0,
            output_metadata: None,
            last_failure_reason: None,
            last_watermark_ms: None,
            last_source_offset: None,
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
    }

    pub(crate) fn apply_status_update(
        &mut self,
        update: &TaskStatusUpdate,
    ) -> SchedulerResult<TaskUpdateOutcome> {
        if update.attempt() != self.attempt {
            return Err(SchedulerError::StaleTaskAttempt {
                task_id: self.task_id().clone(),
                expected: self.attempt,
                received: update.attempt(),
            });
        }

        if self.attempt == 0 {
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
            || (self.state != TaskState::Running && update.state() != TaskState::Running)
        {
            return Err(SchedulerError::StaleTaskAttempt {
                task_id: self.task_id().clone(),
                expected: self.attempt,
                received: update.attempt(),
            });
        }

        self.state = update.state();
        self.assigned_executor = Some(update.executor_id().clone());
        self.attempt = update.attempt();
        if let Some(output_metadata) = update.output_metadata() {
            self.output_metadata = Some(output_metadata.clone());
        }
        if self.state == TaskState::Failed {
            self.last_failure_reason = update.message().map(ToOwned::to_owned);
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
    job_spec_from_plan_parts(job_id, plan.name(), plan.kind(), plan.nodes())
}

/// Convert a Krishiv physical plan into an R2 distributed job spec.
pub fn job_spec_from_physical_plan(job_id: JobId, plan: &PhysicalPlan) -> SchedulerResult<JobSpec> {
    job_spec_from_plan_parts(job_id, plan.name(), plan.kind(), plan.nodes())
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
    // P0.19: O(n) duplicate task-id detection using a HashSet instead of O(n²) Vec scan.
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
    Ok(())
}

fn job_spec_from_plan_parts(
    job_id: JobId,
    plan_name: &str,
    kind: PlanExecutionKind,
    nodes: &[PlanNode],
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
    let stage_id = StageId::try_new("stage-1").map_err(|error| SchedulerError::InvalidPlan {
        message: error.to_string(),
    })?;

    let mut stage = StageSpec::new(stage_id, format!("{job_name}-stage"));
    if nodes.is_empty() {
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
            stage = stage.with_task(TaskSpec::new(task_id, plan_node_description(node)));
        }
    }

    Ok(JobSpec::new(job_id, job_name, job_kind).with_stage(stage))
}

fn plan_node_description(node: &PlanNode) -> String {
    if node.inputs().is_empty() {
        format!("{} [{}] {}", node.id(), node.kind(), node.label())
    } else {
        format!(
            "{} [{}] {} <- {}",
            node.id(),
            node.kind(),
            node.label(),
            node.inputs().join(", ")
        )
    }
}
