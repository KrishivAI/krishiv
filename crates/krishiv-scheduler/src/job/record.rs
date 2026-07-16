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

/// Generous retry budget for a consumer task that fails on a *missing upstream
/// shuffle partition* (FetchFailed). Such failures are upstream data loss, not
/// the task's fault, and a single executor loss can surface as several
/// sequential fetch failures (one per lost producer). The productive recovery
/// path is bounded by `max_shuffle_regen` (the job fails cleanly on durable
/// loss); this only backstops a degenerate report loop so it never counts
/// against the ordinary (default 1) task-attempt budget.
const MISSING_SHUFFLE_MAX_ATTEMPTS: u32 = 30;

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
    /// Phase 53: base delay for exponential backoff between task retry
    /// attempts (doubles per failure, capped at `retry_backoff_cap_ms`).
    pub(crate) retry_backoff_base_ms: u64,
    /// Phase 53: upper bound for the retry backoff delay.
    pub(crate) retry_backoff_cap_ms: u64,
    pub(crate) stages: Vec<StageRecord>,
    /// Shuffle partition availability metadata per producing stage.
    /// Updated when tasks report ShufflePartitionOutput in TaskOutputMetadata.
    pub(crate) shuffle_output: HashMap<StageId, ShuffleMetadata>,
    /// Accumulated resource consumption from completed tasks.
    pub(crate) resource_usage: ResourceUsage,
    /// Phase 58: how many shuffle-partition regeneration cycles this job has
    /// triggered (a consumer reporting missing upstream output → re-running the
    /// producing map tasks). Bounded by
    /// `CoordinatorConfig::max_shuffle_regen_attempts` so a persistently-lost
    /// producer fails the job instead of looping forever. Transient in-memory
    /// counter — reset on coordinator restart, which itself re-plans from
    /// durable metadata.
    pub(crate) shuffle_regen_total: u32,
}

/// Phase 58: result of attempting shuffle-partition regeneration for a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ShuffleRegenOutcome {
    /// No succeeded producer matched the reported missing partitions — nothing
    /// to regenerate (e.g. a stale/duplicate report).
    NoneAffected,
    /// Producing map tasks were reset to Pending for re-execution.
    Regenerated,
    /// The job has regenerated shuffle output too many times; the caller must
    /// fail it as unrecoverable rather than loop.
    BudgetExhausted { attempts: u32, limit: u32 },
}

/// Does a consumer's missing-partition report name this task's shuffle output?
///
/// Reports arrive in two addressing forms:
///  * the **coordinator stage id** (`dist-sN` / native stages) from the legacy
///    `shuffle-write:` path — the owning task is then found through its
///    registered output metadata (which partition ids it produced);
///  * the **`sN.mM` shuffle sub-stage key** the dfplan reader embeds
///    (`collect_missing_shuffle_partitions` on the executor). That key IS
///    the map task's own `ShuffleWriteConfig.stage_id`
///    (`distributed_batch.rs::stage_specs_from_plan`), so it names exactly one
///    task and every partition of the key is that task's output — no metadata
///    lookup needed (or possible: the coordinator stage id never equals it).
fn missing_report_addresses_task(
    m: &MissingShufflePartition,
    stage_id: &StageId,
    task: &TaskRecord,
) -> bool {
    if m.stage_id() == stage_id {
        return task.output_metadata.as_ref().is_some_and(|meta| {
            meta.shuffle_partitions()
                .iter()
                .any(|p| p.partition_id == m.partition_id())
        });
    }
    task.spec
        .shuffle_write()
        .is_some_and(|sw| &sw.stage_id == m.stage_id())
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
            retry_backoff_base_ms: 1_000,
            retry_backoff_cap_ms: 30_000,
            stages,
            shuffle_output: HashMap::new(),
            resource_usage: ResourceUsage::default(),
            shuffle_regen_total: 0,
        }
    }

    /// Phase 53: override the task retry backoff policy (base doubles per
    /// failure, capped).
    pub(crate) fn set_retry_backoff(&mut self, base_ms: u64, cap_ms: u64) {
        self.retry_backoff_base_ms = base_ms.max(1);
        self.retry_backoff_cap_ms = cap_ms.max(base_ms);
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
            let _stage_id_str = stage.stage_id().to_string();
            let mut stage_received_assignment = false;
            for task in &mut stage.tasks {
                if let Some(assignment) = assignments
                    .iter()
                    .find(|assignment| assignment.task_id() == task.task_id())
                {
                    task.assigned_executor = Some(assignment.executor_id().clone());
                    task.state = TaskState::Assigned;
                    task.pending_since_ms = None;
                    task.retry_backoff_until_ms = None;
                    stage_received_assignment = true;
                }
            }
            // Only stages that actually received an assignment move to
            // Scheduling, and never out of a terminal state. The previous
            // unconditional stomp demoted SUCCEEDED upstream stages whenever
            // any other stage's tasks were (re)assigned later (Phase 53
            // eager backlog drains, Phase 54 AQE rewrites), which made the
            // launch loop's upstream-ready check fail forever — downstream
            // tasks assigned after upstream success could never launch.
            if stage_received_assignment
                && !matches!(stage.state, StageState::Succeeded | StageState::Failed)
            {
                stage.state = StageState::Scheduling;
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

        // Shuffle locations for dfplan tasks (Phase 52): where each stage's
        // map tasks reported their shuffle output. Downstream dfplan tasks
        // receive these as ShuffleFlight input partitions so executors can
        // fetch partitions written on other executors; an empty endpoint
        // means the writer's local (in-process) store and is not forwarded.
        // Keyed by the map task's ShuffleWriteConfig stage id — the
        // `shuffle_stage_key` sub-stage wire contract.
        let mut shuffle_location_inputs: std::collections::HashMap<StageId, Vec<InputPartition>> =
            std::collections::HashMap::new();
        for stage in &self.stages {
            let mut locations = Vec::new();
            for task in &stage.tasks {
                let Some(write_cfg) = task.spec.shuffle_write() else {
                    continue;
                };
                let Some(meta) = &task.output_metadata else {
                    continue;
                };
                for p in meta.shuffle_partitions() {
                    if p.flight_endpoint.is_empty() {
                        continue;
                    }
                    locations.push(InputPartition::typed(
                        format!("shuffle-{}-p{}", write_cfg.stage_id, p.partition_id),
                        InputPartitionDescriptor::ShuffleFlight {
                            table_name: String::from("__dfplan_shuffle"),
                            flight_endpoint: p.flight_endpoint.clone(),
                            job_id: self.spec.job_id().clone(),
                            upstream_stage_id: write_cfg.stage_id.clone(),
                            partition_id: p.partition_id,
                        },
                    ));
                }
            }
            if !locations.is_empty() {
                shuffle_location_inputs.insert(stage.stage_id().clone(), locations);
            }
        }

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

                    let lease_generation = match executor_leases.iter().find_map(
                        |(known_executor, lease_generation)| {
                            (known_executor == &executor_id).then_some(*lease_generation)
                        },
                    ) {
                        Some(generation) => generation,
                        None => {
                            // The executor this task is assigned to is no longer
                            // an eligible launch target — it was either
                            // circuit-broken (over the failure threshold and
                            // filtered from `executor_leases` before this call)
                            // or is no longer registered (lost). Reset the task
                            // to Pending so the next assignment round re-places
                            // it on a healthy executor, rather than aborting the
                            // whole job's launch with `UnknownExecutor` and
                            // livelocking on the stale assignment. Observed on a
                            // 3-node cluster: an early executor kill left one map
                            // task pinned to a filtered executor and the launch
                            // loop spun (`unknown executor: …`) until the job hit
                            // its batch-SQL timeout.
                            tracing::warn!(
                                job_id = %self.spec.job_id(),
                                task_id = %task.task_id(),
                                executor_id = %executor_id,
                                "assigned executor is no longer an eligible launch target; resetting task to Pending for re-assignment"
                            );
                            task.state = TaskState::Pending;
                            task.assigned_executor = None;
                            task.launch_in_flight = false;
                            continue;
                        }
                    };

                    task.attempt = task.attempt.saturating_add(1);
                    task.launch_in_flight = true;
                    let attempt_id = AttemptId::try_new(task.attempt).map_err(|error| {
                        SchedulerError::InvalidJob {
                            message: error.to_string(),
                        }
                    })?;
                    let task_description = task.spec.description().to_owned();
                    let task_timeout_secs = task.spec.task_timeout_secs();
                    let input_partitions =
                        if krishiv_sql::distributed_plan::is_dfplan_body(&task_body) {
                            // dfplan tasks carry scans inside the encoded plan;
                            // their only inputs are the upstream shuffle
                            // locations (empty for map stages and for
                            // single-executor jobs — local store reads).
                            stage
                                .spec
                                .upstream_stage_ids()
                                .iter()
                                .filter_map(|up| shuffle_location_inputs.get(up))
                                .flatten()
                                .cloned()
                                .collect()
                        } else if let Some(tables) = batch_sql_tables {
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
                    if task_body.starts_with("stream:loop:")
                        || task_body.starts_with("stream:rloop:")
                    {
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
                        // dfplan stages bake the reduce-side partition count
                        // into the encoded plan (ShuffleReadExec), so the skew
                        // override must never resize them: map output written
                        // beyond that count would silently be dropped.
                        let skew_override =
                            if krishiv_sql::distributed_plan::is_dfplan_body(&task_body) {
                                None
                            } else {
                                skew_partition_override
                            };
                        let effective_num_partitions = skew_override
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

        let outcome = stage.apply_task_update(
            update,
            self.max_stage_retries,
            self.retry_backoff_base_ms,
            self.retry_backoff_cap_ms,
        )?;

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
    /// Missing reports arrive in TWO addressing forms (see
    /// [`missing_report_addresses_task`]); matching only the coordinator
    /// stage-id form left every dfplan-path report unmatched — the consumer
    /// refailed forever against a partition nobody would regenerate (live
    /// wedge, Phase 58 chaos gate, 2026-07-16).
    pub(crate) fn invalidate_specific_shuffle_partitions(
        &mut self,
        missing: &[MissingShufflePartition],
        max_regen: u32,
    ) -> ShuffleRegenOutcome {
        if missing.is_empty() {
            return ShuffleRegenOutcome::NoneAffected;
        }

        // Detection pre-pass: does any *succeeded* producer actually own a
        // reported-missing partition? A stale or duplicate consumer report can
        // reference partitions that were already re-produced or never existed —
        // that must not consume the regeneration budget or fail the job.
        let mut any_match = false;
        'detect: for stage in &self.stages {
            let stage_id = stage.spec.stage_id();
            for task in &stage.tasks {
                if task.state != TaskState::Succeeded {
                    continue;
                }
                if missing
                    .iter()
                    .any(|m| missing_report_addresses_task(m, stage_id, task))
                {
                    any_match = true;
                    break 'detect;
                }
            }
        }
        if !any_match {
            return ShuffleRegenOutcome::NoneAffected;
        }

        // Phase 58: enforce the regeneration budget BEFORE mutating so a
        // persistently-lost producer fails the job instead of looping forever.
        if self.shuffle_regen_total >= max_regen {
            return ShuffleRegenOutcome::BudgetExhausted {
                attempts: self.shuffle_regen_total,
                limit: max_regen,
            };
        }
        self.shuffle_regen_total += 1;

        let job_id_str = self.spec.job_id().as_str().to_owned();
        let mut affected = false;

        for stage in &mut self.stages {
            let stage_id = stage.spec.stage_id().clone();
            let mut stage_affected = false;
            let mut paths_to_invalidate: Vec<ShufflePath> = Vec::new();

            for task in &mut stage.tasks {
                if task.state != TaskState::Succeeded {
                    continue;
                }
                let addressed: Vec<&MissingShufflePartition> = missing
                    .iter()
                    .filter(|m| missing_report_addresses_task(m, &stage_id, task))
                    .collect();
                if addressed.is_empty() {
                    continue;
                }

                // Record failed paths under the addressing key the consumer
                // used (coordinator stage id or sub-stage key), so the entry
                // that served the fetch is the one marked. The re-run producer
                // re-registers under the same key (replace-on-write) either way.
                for m in &addressed {
                    paths_to_invalidate.push(ShufflePath {
                        job_id: job_id_str.clone(),
                        stage_id: m.stage_id().as_str().to_owned(),
                        partition_id: m.partition_id(),
                    });
                }
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

        ShuffleRegenOutcome::Regenerated
    }

    /// Phase 53: preferred placement node per stage, derived from where the
    /// upstream stages' shuffle output actually lives.
    ///
    /// For each stage with upstream dependencies, finds the executor host
    /// holding the largest share of upstream shuffle bytes (Succeeded
    /// upstream tasks, weighted by `ShufflePartitionOutput::size_bytes`;
    /// weight 1 when sizes are unreported). Stages without upstreams (scans)
    /// get no preference. Returns `stage_id → preferred host`.
    pub(crate) fn preferred_nodes_by_stage(
        &self,
        executor_hosts: &HashMap<ExecutorId, String>,
    ) -> HashMap<StageId, String> {
        let mut result = HashMap::new();
        for stage in &self.stages {
            let upstreams = stage.spec.upstream_stage_ids();
            if upstreams.is_empty() {
                continue;
            }
            let mut bytes_by_host: HashMap<&str, u64> = HashMap::new();
            for upstream in self
                .stages
                .iter()
                .filter(|s| upstreams.contains(s.stage_id()))
            {
                for task in upstream.tasks() {
                    if task.state() != TaskState::Succeeded {
                        continue;
                    }
                    let Some(host) = task
                        .assigned_executor()
                        .and_then(|eid| executor_hosts.get(eid))
                    else {
                        continue;
                    };
                    let weight: u64 = task
                        .output_metadata()
                        .map(|m| {
                            m.shuffle_partitions()
                                .iter()
                                .map(|p| p.size_bytes.max(1))
                                .sum()
                        })
                        .filter(|&w: &u64| w > 0)
                        .unwrap_or(1);
                    *bytes_by_host.entry(host.as_str()).or_insert(0) += weight;
                }
            }
            if let Some((host, _)) = bytes_by_host
                .into_iter()
                .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(a.0)))
            {
                result.insert(stage.stage_id().clone(), host.to_owned());
            }
        }
        result
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
    /// Phase 54 AQE: replace this stage's tasks wholesale.
    ///
    /// Callers must have verified that no task has been launched (Pending or
    /// Assigned-but-unlaunched only) — replacement drops the old records.
    pub(crate) fn replace_tasks(&mut self, tasks: Vec<TaskRecord>) {
        self.tasks = tasks;
        self.refresh_state();
    }

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
        retry_backoff_base_ms: u64,
        retry_backoff_cap_ms: u64,
    ) -> SchedulerResult<TaskUpdateOutcome> {
        let max_task_attempts = self.spec.max_task_attempts();

        let task_idx = self
            .tasks
            .iter()
            .position(|task| task.task_id() == update.task_id())
            .ok_or_else(|| SchedulerError::UnknownTask {
                task_id: update.task_id().clone(),
            })?;

        let outcome = self
            .tasks
            .get_mut(task_idx)
            .ok_or_else(|| SchedulerError::UnknownTask {
                task_id: update.task_id().clone(),
            })?
            .apply_status_update(&update)?;
        if outcome == TaskUpdateOutcome::Duplicate {
            return Ok(outcome);
        }

        if update.state() == TaskState::Failed {
            // Try per-task retry first: if this specific task still has attempts remaining,
            // reset only that task to Pending rather than retrying the entire stage
            // (which would reset even succeeded tasks).
            let task = self
                .tasks
                .get_mut(task_idx)
                .ok_or_else(|| SchedulerError::UnknownTask {
                    task_id: update.task_id().clone(),
                })?;
            task.failure_count = task.failure_count.saturating_add(1);
            let task_failure_count = task.failure_count;

            // FetchFailed semantics (Spark parity): a consumer that failed
            // because an upstream shuffle partition is unavailable is not at
            // fault — its producer is re-queued by the caller's
            // missing-partition branch and regenerated under the separate
            // `max_shuffle_regen` budget. Give such failures a generous retry
            // budget so a multi-producer executor loss (which surfaces as
            // several *sequential* fetch failures, one lost producer at a
            // time) still converges, instead of the default
            // `max_task_attempts = 1` failing the whole job after the first
            // lost producer. The productive path is bounded by
            // `max_shuffle_regen` (the job fails cleanly on durable loss);
            // this cap only backstops a pathological no-op report loop.
            let missing_shuffle = !update.missing_shuffle_partitions().is_empty();
            let effective_attempts = if missing_shuffle {
                max_task_attempts.max(MISSING_SHUFFLE_MAX_ATTEMPTS)
            } else {
                max_task_attempts
            };

            if task_failure_count < effective_attempts {
                let task =
                    self.tasks
                        .get_mut(task_idx)
                        .ok_or_else(|| SchedulerError::UnknownTask {
                            task_id: update.task_id().clone(),
                        })?;
                task.state = TaskState::Pending;
                task.assigned_executor = None;
                task.launch_in_flight = false;
                if missing_shuffle {
                    // The consumer's upstream-ready gate already holds it until
                    // the regenerated producer re-succeeds, so an extra failure
                    // backoff would only delay recovery.
                    task.retry_backoff_until_ms = None;
                } else {
                    // Phase 53: exponential backoff before re-assignment —
                    // base * 2^(failures-1), capped. Failure-driven retries only;
                    // executor-loss and speculation resets stay immediate.
                    let exp = task_failure_count.saturating_sub(1).min(16);
                    let delay_ms = retry_backoff_base_ms
                        .saturating_mul(1u64 << exp)
                        .min(retry_backoff_cap_ms);
                    let now_ms =
                        u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0);
                    task.retry_backoff_until_ms = Some(now_ms.saturating_add(delay_ms));
                }
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
    /// Wall-clock duration in ms from task assignment to successful completion.
    /// Set when the task transitions to `Succeeded`; `None` otherwise.
    /// Used by speculative execution to compute the median completed task
    /// duration for a stage without requiring `assigned_at_ms` (which is
    /// cleared on task completion).
    pub(crate) completed_duration_ms: Option<u64>,
    /// Phase 53: wall-clock ms before which a failure-retried task must not
    /// be re-assigned (exponential backoff on task-reported failures).
    /// Transient — not persisted; a coordinator restart resets the backoff.
    pub(crate) retry_backoff_until_ms: Option<u64>,
    /// Phase 53 delay scheduling: wall-clock ms when this task was first
    /// considered for assignment while Pending. Anchors the locality-wait
    /// budget; cleared on assignment.
    pub(crate) pending_since_ms: Option<u64>,
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
            completed_duration_ms: None,
            retry_backoff_until_ms: None,
            pending_since_ms: None,
        }
    }

    /// Task id.
    pub fn task_id(&self) -> &TaskId {
        self.spec.task_id()
    }

    /// Task description (the typed fragment body for the typed-fragment
    /// envelope, or the raw plan description for legacy fragments). Used
    /// by the coordinator to derive a stable `OperatorId` for the task
    /// — the description survives task retries because it is derived from
    /// the plan, not the runtime task id.
    pub fn description(&self) -> &str {
        self.spec.description()
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
        } else if self.state == TaskState::Succeeded {
            let now_ms = u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0);
            // Capture the wall-clock duration so speculative execution can
            // compute the median completed task time for this stage without
            // needing assigned_at_ms (which is cleared below).
            self.completed_duration_ms = self.assigned_at_ms.map(|s| now_ms.saturating_sub(s));
            self.assigned_at_ms = None;
            self.last_progress_ms = None;
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

#[cfg(test)]
mod shuffle_regen_tests {
    use super::*;
    use krishiv_proto::{
        JobId, JobKind, JobSpec, MissingShufflePartition, ShufflePartitionOutput, StageId,
        StageSpec, TaskId, TaskOutputMetadata, TaskSpec,
    };

    /// Mark the single producer task Succeeded with one shuffle partition (id 0)
    /// so it is a valid regeneration target.
    fn succeed_producer(job: &mut JobRecord) {
        let meta = TaskOutputMetadata::new("shuffle", 10, 1, 1).with_shuffle_partitions(vec![
            ShufflePartitionOutput::new(0, 1024, "http://producer-host:9000"),
        ]);
        let task = &mut job.stages[0].tasks[0];
        task.state = TaskState::Succeeded;
        task.output_metadata = Some(meta);
        job.stages[0].refresh_state();
        job.refresh_state();
    }

    fn producer_job() -> (JobRecord, StageId) {
        let stage_id = StageId::try_new("stage-0").unwrap();
        let spec = JobSpec::new(
            JobId::try_new("regen-job").unwrap(),
            "regen-test",
            JobKind::Batch,
        )
        .with_stage(
            StageSpec::new(stage_id.clone(), "write stage")
                .with_task(TaskSpec::new(TaskId::try_new("task-0").unwrap(), "shuffle-write")),
        );
        let mut job = JobRecord::from_spec(spec, 4);
        succeed_producer(&mut job);
        (job, stage_id)
    }

    /// Phase 58/53: a task assigned to an executor that is no longer an eligible
    /// launch target — circuit-broken (filtered from the leases) or lost
    /// (unregistered) — must be reset to Pending for re-assignment, NOT abort
    /// the whole job's launch with `UnknownExecutor`. The old behaviour
    /// livelocked: the launch loop re-hit the stale assignment every cycle
    /// (`unknown executor: …`) until the job timed out (observed on a 3-node
    /// cluster when an executor was killed early, before it produced output).
    #[test]
    fn assigned_task_on_vanished_executor_resets_instead_of_erroring() {
        use krishiv_proto::{ExecutorId, JobState, LeaseGeneration};

        let stage_id = StageId::try_new("stage-0").unwrap();
        let spec = JobSpec::new(
            JobId::try_new("livelock-job").unwrap(),
            "livelock-test",
            JobKind::Batch,
        )
        .with_stage(
            StageSpec::new(stage_id, "map stage")
                .with_task(TaskSpec::new(TaskId::try_new("task-0").unwrap(), "map:body")),
        );
        let mut job = JobRecord::from_spec(spec, 0);
        job.state = JobState::Running;

        // Pin the task to an executor, as the assign phase would.
        let gone = ExecutorId::try_new("exec-gone").unwrap();
        {
            let task = &mut job.stages[0].tasks[0];
            task.state = TaskState::Assigned;
            task.assigned_executor = Some(gone.clone());
            task.launch_in_flight = false;
        }

        // Launch with a lease set that does NOT contain the assigned executor
        // (it was circuit-broken / lost). Must return Ok (nothing launches),
        // and the task must be reset to Pending with its executor cleared so
        // the next assignment round re-places it on the healthy executor.
        let healthy = ExecutorId::try_new("exec-healthy").unwrap();
        let leases = vec![(healthy, LeaseGeneration::initial())];
        let assignments = job
            .launch_assigned_task_assignments(&leases, None, None, None, None)
            .expect("a vanished assigned executor must not error the job's launch");
        assert!(
            assignments.is_empty(),
            "the pinned task must not launch onto the vanished executor"
        );
        let task = &job.stages[0].tasks[0];
        assert_eq!(
            task.state,
            TaskState::Pending,
            "the orphaned task must be reset to Pending for re-assignment"
        );
        assert!(
            task.assigned_executor.is_none(),
            "the stale executor assignment must be cleared"
        );
        assert!(!task.launch_in_flight);
    }

    /// Phase 58: shuffle regeneration is bounded — the first `limit` cycles
    /// re-run the producer, the next one reports `BudgetExhausted` so the caller
    /// fails the job instead of looping forever. A report that matches no
    /// succeeded producer consumes no budget.
    #[test]
    fn shuffle_regeneration_is_bounded_then_exhausts() {
        let (mut job, stage_id) = producer_job();
        let missing = vec![MissingShufflePartition::new(stage_id, 0)];

        // A stale report referencing a stage that owns no matching partition
        // must not consume the regeneration budget.
        let bogus = vec![MissingShufflePartition::new(
            StageId::try_new("stage-nonexistent").unwrap(),
            0,
        )];
        assert_eq!(
            job.invalidate_specific_shuffle_partitions(&bogus, 2),
            ShuffleRegenOutcome::NoneAffected
        );
        assert_eq!(job.shuffle_regen_total, 0);

        // Two real regenerations are within budget (limit = 2). Each resets the
        // producer to Pending, so re-succeed it before the next consumer failure.
        assert_eq!(
            job.invalidate_specific_shuffle_partitions(&missing, 2),
            ShuffleRegenOutcome::Regenerated
        );
        assert_eq!(job.stages[0].tasks[0].state, TaskState::Pending);
        succeed_producer(&mut job);

        assert_eq!(
            job.invalidate_specific_shuffle_partitions(&missing, 2),
            ShuffleRegenOutcome::Regenerated
        );
        succeed_producer(&mut job);

        // The third real regeneration exceeds the budget → the caller must fail.
        assert_eq!(
            job.invalidate_specific_shuffle_partitions(&missing, 2),
            ShuffleRegenOutcome::BudgetExhausted {
                attempts: 2,
                limit: 2,
            }
        );
        // On exhaustion the producer is NOT reset — it stays as it was.
        assert_eq!(job.stages[0].tasks[0].state, TaskState::Succeeded);
    }

    /// Phase 58 (live chaos-gate wedge, 2026-07-16): a dfplan consumer reports
    /// a missing partition by the `sN.mM` shuffle sub-stage key — the producing
    /// map task's own `ShuffleWriteConfig.stage_id` — never by the coordinator
    /// stage id (`dist-sN`). The report must still resolve to that task and
    /// regenerate it; matching only the coordinator form made every dfplan
    /// report a no-op, so the reduce refailed forever into the same missing
    /// partition while its failures circuit-broke both executors.
    #[test]
    fn dfplan_substage_key_report_regenerates_producer() {
        use krishiv_proto::ShuffleWriteConfig;

        let stage_id = StageId::try_new("dist-s0").unwrap();
        let spec = JobSpec::new(
            JobId::try_new("dfplan-regen-job").unwrap(),
            "dfplan-regen-test",
            JobKind::Batch,
        )
        .with_stage(
            StageSpec::new(stage_id, "map stage").with_task(
                TaskSpec::new(TaskId::try_new("dist-s0-t2").unwrap(), "dfplan:v1:body")
                    .with_shuffle_write(ShuffleWriteConfig {
                        stage_id: StageId::try_new("s0.m2").unwrap(),
                        num_partitions: 4,
                        key_columns: Vec::new(),
                        lease_token: 0,
                    }),
            ),
        );
        let mut job = JobRecord::from_spec(spec, 4);
        succeed_producer(&mut job);

        // The sub-stage key alone names the owning task: the reported
        // partition (3) is deliberately absent from the producer's registered
        // metadata (which only lists partition 0).
        let missing = vec![MissingShufflePartition::new(
            StageId::try_new("s0.m2").unwrap(),
            3,
        )];
        assert_eq!(
            job.invalidate_specific_shuffle_partitions(&missing, 2),
            ShuffleRegenOutcome::Regenerated
        );
        assert_eq!(
            job.stages[0].tasks[0].state,
            TaskState::Pending,
            "the sub-stage-keyed producer must be reset for re-execution"
        );
        assert!(job.stages[0].tasks[0].assigned_executor.is_none());
    }

    /// Phase 58 (FetchFailed semantics): a consumer that fails on a *missing
    /// upstream shuffle partition* must be re-queued under a generous budget
    /// rather than the default `max_task_attempts = 1`, so a multi-producer
    /// executor loss (several sequential fetch failures) still converges. An
    /// ordinary failure keeps the strict budget.
    #[test]
    fn missing_shuffle_failure_does_not_exhaust_default_task_budget() {
        use krishiv_proto::{ExecutorId, TaskStatusUpdate};

        let stage_id = StageId::try_new("dist-s1").unwrap();
        let mut stage = StageRecord::from_spec(
            StageSpec::new(stage_id.clone(), "reduce")
                .with_task(TaskSpec::new(TaskId::try_new("r0").unwrap(), "dfplan:body")),
        );
        let exec = ExecutorId::try_new("exec-x").unwrap();
        let job_id = JobId::try_new("fetchfail-job").unwrap();
        let task_id = TaskId::try_new("r0").unwrap();
        let missing = vec![MissingShufflePartition::new(
            StageId::try_new("s0.m2").unwrap(),
            3,
        )];

        // Six sequential missing-shuffle failures (as a multi-producer loss
        // would surface) — every one must re-queue the consumer, not fail it.
        for round in 1..=6u32 {
            let t = &mut stage.tasks[0];
            t.attempt = round;
            t.state = TaskState::Assigned;
            t.assigned_executor = Some(exec.clone());
            let upd = TaskStatusUpdate::new(
                job_id.clone(),
                stage_id.clone(),
                task_id.clone(),
                exec.clone(),
                TaskState::Failed,
                round,
            )
            .with_missing_shuffle_partitions(missing.clone());
            // max_stage_retries = 0: isolate the per-task budget.
            stage.apply_task_update(upd, 0, 0, 0).unwrap();
            assert_eq!(
                stage.tasks[0].state,
                TaskState::Pending,
                "missing-shuffle failure #{round} must re-queue the consumer"
            );
        }

        // Contrast: an ordinary failure under max_task_attempts = 1 (with no
        // stage retries) must NOT retry — it stays Failed.
        let t = &mut stage.tasks[0];
        t.attempt = 7;
        t.state = TaskState::Assigned;
        t.assigned_executor = Some(exec.clone());
        let upd = TaskStatusUpdate::new(job_id, stage_id, task_id, exec, TaskState::Failed, 7);
        stage.apply_task_update(upd, 0, 0, 0).unwrap();
        assert_eq!(
            stage.tasks[0].state,
            TaskState::Failed,
            "an ordinary failure under max_task_attempts=1 must not retry"
        );
    }
}
