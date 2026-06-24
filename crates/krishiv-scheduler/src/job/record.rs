use std::collections::HashMap;

use krishiv_plan::ExecutionKind as PlanExecutionKind;
use krishiv_proto::{
    AttemptId, ExecutorId, ExecutorTaskAssignment, InputPartition, InputPartitionDescriptor, JobId,
    JobKind, JobSpec, JobState, KeyGroupRange, LeaseGeneration, MissingShufflePartition,
    OutputContract, OutputContractKind, PlanFragment, StageId, StageSpec, StageState,
    StreamingTaskState, TaskAssignment, TaskAttemptRef, TaskId, TaskOutputMetadata, TaskSpec,
    TaskState, TaskStatusUpdate,
};
use krishiv_shuffle::{ShuffleMetadata, ShufflePath};

use crate::{SchedulerError, SchedulerResult, TaskUpdateOutcome};

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

use super::scheduler::ResourceUsage;
use super::snapshot::{JobDetailSnapshot, JobSnapshot, StageSnapshot, TaskSnapshot};

/// Job record owned by the active coordinator.
#[derive(Debug, Clone, PartialEq)]
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

    pub(crate) fn mark_queued(&mut self) {
        self.state = JobState::Queued;
        for stage in &mut self.stages {
            stage.state = StageState::Pending;
            for task in stage.tasks_mut() {
                task.state = TaskState::Pending;
                task.assigned_executor = None;
                task.clear_launch_in_flight();
            }
        }
    }

    pub(crate) fn mark_admitted(&mut self) {
        if self.state == JobState::Queued {
            self.state = JobState::Accepted;
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
        if self.state == JobState::Queued {
            return;
        }
        self.state = JobState::Running;
        let _job_id_str = self.job_id().to_string();
        for stage in &mut self.stages {
            stage.state = StageState::Scheduling;
            let _stage_id_str = stage.stage_id().to_string();
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
        if self.state == JobState::Queued {
            return Ok(assignments);
        }
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
                        match task.spec.sink_contract() {
                            // Terminal write tasks carry a sink contract on
                            // their spec (Phase 2.3 distributed writes); the
                            // executor stages output and the coordinator
                            // publishes it on job success.
                            Some(contract) => {
                                OutputContract::new(OutputContractKind::Sink, contract)
                            }
                            None => OutputContract::new(
                                OutputContractKind::InlineRecordBatches,
                                format!("inline result for {}", task.task_id()),
                            ),
                        },
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
            shuffle_bytes_written: self.shuffle_bytes_written(),
            shuffle_partitions_available: self.shuffle_partitions_available_count(),
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

    /// Invalidate shuffle partitions owned by a lost executor.
    ///
    /// When an executor is lost, the shuffle data it served (via Arrow Flight)
    /// is no longer accessible.  This method scans all Succeeded tasks whose
    /// `assigned_executor` matches `executor_id`, marks their non-inline shuffle
    /// partitions as Failed in `shuffle_output`, resets those tasks to Pending
    /// so they are re-scheduled, and refreshes the affected stage + job state.
    ///
    /// Returns `true` if any tasks were affected.
    pub(crate) fn invalidate_executor_shuffle_partitions(
        &mut self,
        executor_id: &ExecutorId,
    ) -> bool {
        let job_id_str = self.spec.job_id().as_str().to_owned();
        let mut affected = false;

        for stage in &mut self.stages {
            let stage_id_str = stage.spec.stage_id().as_str().to_owned();
            let mut stage_affected = false;

            // Collect shuffle paths to invalidate without holding a mutable borrow
            // on both stage.tasks and self.shuffle_output simultaneously.
            let mut paths_to_invalidate: Vec<ShufflePath> = Vec::new();

            for task in &mut stage.tasks {
                if task.state != TaskState::Succeeded {
                    continue;
                }
                if task.assigned_executor.as_ref() != Some(executor_id) {
                    continue;
                }
                let Some(meta) = &task.output_metadata else {
                    continue;
                };
                let remote_partitions: Vec<ShufflePath> = meta
                    .shuffle_partitions()
                    .iter()
                    .filter(|p| !p.flight_endpoint.is_empty())
                    .map(|p| ShufflePath {
                        job_id: job_id_str.clone(),
                        stage_id: stage_id_str.clone(),
                        partition_id: p.partition_id,
                    })
                    .collect();

                if remote_partitions.is_empty() {
                    continue;
                }

                paths_to_invalidate.extend(remote_partitions);
                task.state = TaskState::Pending;
                task.assigned_executor = None;
                task.launch_in_flight = false;
                stage_affected = true;
                affected = true;
            }

            if stage_affected {
                // Mark the collected paths as Failed in the metadata registry.
                if let Ok(stage_key) = krishiv_proto::StageId::try_new(&stage_id_str) {
                    let meta_entry = self.shuffle_output.entry(stage_key).or_default();
                    for path in &paths_to_invalidate {
                        meta_entry.mark_failed(path, "executor lost".to_owned());
                    }
                }
                stage.refresh_state();
            }
        }

        if affected {
            self.refresh_state();
        }

        affected
    }

    /// Invalidate specific shuffle partitions reported missing by a consumer task.
    ///
    /// When a consumer task reports `MissingShufflePartition` entries in its `Failed`
    /// status update, the producing tasks that wrote those partitions must be re-run.
    /// This method marks the named (stage_id, partition_id) paths as Failed and resets
    /// their owning tasks to Pending so they are re-scheduled.
    ///
    /// Returns `true` if any tasks were affected.
    pub(crate) fn invalidate_specific_shuffle_partitions(
        &mut self,
        missing: &[MissingShufflePartition],
    ) -> bool {
        if missing.is_empty() {
            return false;
        }
        let job_id_str = self.spec.job_id().as_str().to_owned();
        let mut affected = false;

        for stage in &mut self.stages {
            let stage_id = stage.spec.stage_id().clone();
            // Collect which partition_ids in this stage are reported missing.
            let missing_ids: Vec<u32> = missing
                .iter()
                .filter(|m| m.stage_id() == &stage_id)
                .map(|m| m.partition_id())
                .collect();
            if missing_ids.is_empty() {
                continue;
            }

            let mut stage_affected = false;
            let mut paths_to_invalidate: Vec<ShufflePath> = Vec::new();

            for task in &mut stage.tasks {
                if task.state != TaskState::Succeeded {
                    continue;
                }
                let Some(meta) = &task.output_metadata else {
                    continue;
                };
                let affected_partitions: Vec<ShufflePath> = meta
                    .shuffle_partitions()
                    .iter()
                    .filter(|p| missing_ids.contains(&p.partition_id))
                    .map(|p| ShufflePath {
                        job_id: job_id_str.clone(),
                        stage_id: stage_id.as_str().to_owned(),
                        partition_id: p.partition_id,
                    })
                    .collect();

                if affected_partitions.is_empty() {
                    continue;
                }

                paths_to_invalidate.extend(affected_partitions);
                task.state = TaskState::Pending;
                task.assigned_executor = None;
                task.launch_in_flight = false;
                stage_affected = true;
                affected = true;
            }

            if stage_affected {
                let meta_entry = self.shuffle_output.entry(stage_id).or_default();
                for path in &paths_to_invalidate {
                    meta_entry.mark_failed(
                        path,
                        "shuffle partition missing on consumer fetch".to_owned(),
                    );
                }
                stage.refresh_state();
            }
        }

        if affected {
            self.refresh_state();
        }

        affected
    }

    /// Collect per-task serialized shuffle bytes for a completed stage.
    ///
    /// Called after a shuffle stage succeeds to gather AQE re-optimization inputs.
    /// Returns a `Vec<RuntimeStats>` with one entry per task (order matches tasks).
    pub(crate) fn collect_stage_runtime_stats(
        &self,
        stage_id: &StageId,
    ) -> Vec<krishiv_plan::optimizer::RuntimeStats> {
        let Some(stage) = self.stages.iter().find(|s| s.stage_id() == stage_id) else {
            return Vec::new();
        };
        let mut result = Vec::new();
        for t in stage
            .tasks
            .iter()
            .filter(|t| t.state == TaskState::Succeeded)
        {
            let meta = t.output_metadata.as_ref();
            let partitions = meta.map(|m| m.shuffle_partitions()).unwrap_or(&[]);
            if !partitions.is_empty() {
                // One RuntimeStats per shuffle partition: CoalesceRule needs
                // per-partition granularity to decide how many groups to form.
                for p in partitions {
                    let mut ps = krishiv_plan::optimizer::RuntimeStats::default();
                    ps.serialized_bytes = p.size_bytes;
                    result.push(ps);
                }
            } else {
                // No shuffle partitions: use aggregate task stats (non-shuffle stage).
                let rs = meta.and_then(|m| m.runtime_stats());
                let mut ps = krishiv_plan::optimizer::RuntimeStats::default();
                ps.input_rows = rs.map_or(0, |s| s.input_rows);
                ps.output_rows = rs.map_or(0, |s| s.output_rows);
                ps.cpu_nanos = rs.map_or(0, |s| s.cpu_nanos);
                ps.memory_bytes = rs.map_or(0, |s| s.memory_bytes);
                ps.spill_bytes = rs.map_or(0, |s| s.spill_bytes);
                ps.serialized_bytes = rs.map_or(0, |s| s.serialized_bytes);
                result.push(ps);
            }
        }
        result
    }
}

/// Stage record owned by a job coordinator.
#[derive(Debug, Clone, PartialEq)]
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
        let shuffle_bytes_written: u64 = self
            .tasks
            .iter()
            .filter_map(|t| t.output_metadata.as_ref())
            .flat_map(|m| m.shuffle_partitions())
            .map(|p| p.size_bytes)
            .sum();
        let shuffle_partitions_available: usize = self
            .tasks
            .iter()
            .filter(|t| t.state == TaskState::Succeeded)
            .filter_map(|t| t.output_metadata.as_ref())
            .map(|m| m.shuffle_partitions().len())
            .sum();
        StageSnapshot {
            stage_id: self.spec.stage_id().clone(),
            state: self.state,
            retry_count: self.retry_count,
            task_count: self.tasks.len(),
            tasks: self.tasks.iter().map(TaskRecord::snapshot).collect(),
            shuffle_bytes_written,
            shuffle_partitions_available,
        }
    }
}

/// Task record owned by a job coordinator.
#[derive(Debug, Clone, PartialEq)]
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
    /// Wall-clock ms when the task most recently entered Running state.
    /// Used by stall detection to identify hung tasks.
    pub(crate) assigned_at_ms: Option<u64>,
    /// Wall-clock ms of the most recent progress signal (heartbeat, output
    /// metadata, or streaming progress report). Falls back to `assigned_at_ms`
    /// when no progress has been reported. Used by stall detection so that
    /// long-running tasks that are actively making progress are not killed.
    pub(crate) last_progress_ms: Option<u64>,
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
            last_progress_ms: None,
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
        self.last_watermark_ms = Some(state.watermark_ms);
        if !state.source_offset.is_empty() {
            self.last_source_offset = Some(state.source_offset.clone());
        }
        // Executor heartbeat confirms this task is alive — refresh the progress
        // timestamp so stall detection does not kill long-windowing tasks that
        // are accumulating data without yet emitting output rows.
        self.last_progress_ms =
            Some(u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0));
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
        // can identify hung tasks. Initialize last_progress_ms to the same value.
        if self.state == TaskState::Running {
            let now_ms = u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0);
            self.assigned_at_ms = Some(now_ms);
            self.last_progress_ms = Some(now_ms);
        } else if self.state.is_terminal() {
            self.assigned_at_ms = None;
            self.last_progress_ms = None;
        } else if update.output_metadata().is_some() || update.message().is_some() {
            // Non-terminal status updates (output metadata, progress messages)
            // refresh the progress timestamp so stall detection doesn't kill
            // long-running tasks that are actively producing output.
            self.last_progress_ms =
                Some(u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0));
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
