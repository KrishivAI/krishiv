use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_proto::{
    CheckpointAckRequest, CheckpointAckResponse, CoordinatorExecutorService,
    ExecutorTaskAssignment, FencingToken, InitiateCheckpointRequest, JobId,
    MissingShufflePartition, TaskAttemptRef, TaskId, TaskOutputMetadata, TaskState,
    TaskStatusRequest, TaskStatusResponse,
};
use krishiv_sql::SqlEngine;
use krishiv_state::checkpoint::CheckpointStorage;

use crate::{
    ExecutorAssignmentInbox, ExecutorError, ExecutorResult, SharedBarrierAckRegistry,
    SharedBarrierInjector, SharedKeyGroupRanges,
    fragment::{batch::execute_batch_fragment, streaming::execute_streaming_fragment},
};

use super::partition::{
    DEFAULT_BATCH_TASK_TIMEOUT_SECS, MAX_CHECKPOINT_ACK_RETRIES,
    collect_missing_shuffle_partitions, default_streaming_task_timeout_secs,
    format_failure_message,
};
use super::task_output::{
    CheckpointStateHandle, ExecutorTaskOutput, ExecutorTaskRunReport, RestoredJobCheckpoint,
    ShuffleContext, apply_snapshots_to_state, kafka_offsets_from_source_records,
};
use super::task_runner::{
    ContinuousJobDrainer, NoOpProgressCallback, SharedProgressCallback, StreamingProgressSnapshot,
    TaskRunner,
};

/// Minimal R3.1 stage-local task runner skeleton.
#[derive(Clone)]
pub struct ExecutorTaskRunner {
    pub(crate) inbox: ExecutorAssignmentInbox,
    pub(crate) shuffle: Option<ShuffleContext>,
    pub(crate) inmem_shuffle: Option<std::sync::Arc<krishiv_shuffle::ShuffleBackend>>,
    /// Shared SQL engine — one instance per runner rather than per-fragment.
    pub(crate) sql_engine: Arc<SqlEngine>,
    /// Per-task checkpoint state keyed by task id.
    pub(crate) checkpoint_runners: Arc<DashMap<TaskId, Arc<Mutex<TaskRunner>>>>,
    /// Attempts currently executing on this executor (P1-19).
    pub(crate) running_attempts: Option<Arc<DashMap<String, TaskAttemptRef>>>,
    /// Optional continuous streaming drain hook (in-process cluster).
    pub(crate) continuous_drainer: Option<Arc<dyn ContinuousJobDrainer>>,
    /// Per-job stateful `ContinuousWindowExecutor` instances for `stream:loop:` fragments (GAP-6).
    ///
    /// Keyed by job-id string.  The executor is created on first use and
    /// reused across drain cycles so that partial window state (e.g. an open
    /// tumbling window that has not yet reached its watermark) accumulates
    /// correctly across multiple invocations of the same `stream:loop:` task.
    ///
    /// `Arc<Mutex<…>>` because the runner is cloned between tasks but all
    /// clones must share the same stateful executor for a given job.
    pub(crate) loop_executors:
        Arc<DashMap<String, Arc<std::sync::Mutex<krishiv_dataflow::ContinuousWindowExecutor>>>>,
    /// Per-job pending input batches for distributed `push_continuous_input` gRPC.
    ///
    /// The gRPC service appends decoded batches here; `execute_loop_fragment` drains
    /// them (as a fallback when `continuous_drainer` is absent) so the same executor
    /// state is shared with the network path.
    pub(crate) continuous_inputs: Arc<DashMap<String, Vec<RecordBatch>>>,
    /// Per-job `StreamingPartitionAdvisor` instances (EMA-based bucket advisor).
    ///
    /// Keyed by job-id string. Accumulates the EMA of observed input-batch byte
    /// sizes across streaming cycles and recommends a bucket count that tracks
    /// actual data volume. All runner clones share the same advisor per job.
    pub(crate) streaming_advisors:
        Arc<DashMap<String, Arc<std::sync::Mutex<krishiv_dataflow::StreamingPartitionAdvisor>>>>,
    /// Live executor lease generation, shared with the heartbeat loop.
    /// Used to stamp checkpoint-fanout RPCs without round-tripping through
    /// the gRPC service (B10).  Defaults to `LeaseGeneration::initial()`.
    pub(crate) live_lease: crate::grpc_client::SharedLeaseGeneration,

    /// Shared barrier injector fed by the gRPC `BarrierService`.  Barriers
    /// enqueued here are drained by the runner loop and trigger checkpoint
    /// initiation.
    pub(crate) barrier_injector: Option<SharedBarrierInjector>,
    /// Completion registry wired to the barrier gRPC stream for deferred acks.
    pub(crate) barrier_ack_registry: Option<SharedBarrierAckRegistry>,
    pub(crate) key_group_ranges: Option<SharedKeyGroupRanges>,

    /// Most-recently observed coordinator fencing token, cached from real gRPC
    /// `InitiateCheckpointRequest` messages.  `drain_pending_barriers` stamps
    /// locally-injected barriers with this token so the coordinator does not
    /// reject acks from stale-token barriers after a leadership election.
    pub(crate) cached_coordinator_fencing_token: Arc<AtomicU64>,

    /// Per-source `rows_per_second` throttle limits received from the
    /// coordinator heartbeat response (R7.2).
    ///
    /// The heartbeat loop writes into this table via
    /// `SourceThrottleTable::apply()`; source operators read from it via
    /// `SourceThrottleTable::check_and_log()` or `active_limit()`.
    /// Clone is cheap — all runner clones share the same underlying `DashMap`.
    pub source_throttle_limits: crate::source_throttle::SourceThrottleTable,

    /// Optional streaming progress callback (GAP-OB-04).
    ///
    /// When set, streaming operators report intermediate progress (watermark,
    /// rows emitted, state size) via this callback. The heartbeat loop wires
    /// this to forward snapshots to the coordinator for metrics exposure.
    pub(crate) progress_callback: SharedProgressCallback,

    /// Auxiliary cache of `"table_name:path"` keys for callers that reuse this
    /// runner's shared SQL engine outside task-local execution.
    ///
    /// Batch task execution creates a fresh SQL engine per assignment so UDF
    /// limits and table registrations stay scoped to that task; those paths
    /// intentionally do not consult this cache.
    pub(crate) registered_parquet_cache: Arc<DashMap<String, ()>>,

    /// Root directory for durable window operator state (single-node-durable
    /// and distributed-durable profiles). Each continuous job gets a sub-directory
    /// `<state_dir>/<job_id>/`. `None` → ephemeral (dev-local) state.
    pub(crate) state_dir: Option<std::path::PathBuf>,

    /// This executor's own ID, used when synthesising checkpoint-ack assignments
    /// so the coordinator can correlate them with the registered executor.
    /// `None` in unit-test runners where no real executor identity is needed.
    pub(crate) own_executor_id: Option<krishiv_proto::ExecutorId>,

    /// Restored job checkpoints pending application to lazily created loop
    /// executors.  Inserted when a `RestoreFromCheckpointCommand` arrives
    /// before the job's `ContinuousWindowExecutor` exists (fresh process);
    /// consumed by `execute_loop_fragment` at executor creation.
    pub(crate) pending_restores: Arc<DashMap<String, RestoredJobCheckpoint>>,

    /// Restored Kafka offsets per job, consumed by source pipelines at source
    /// construction to seek the broker consumer to the checkpointed position.
    pub kafka_restore_offsets: Arc<DashMap<String, Vec<krishiv_connectors::kafka::KafkaOffset>>>,

    /// Transactional-sink registry driven by the checkpoint lifecycle:
    /// `pre_commit` at the barrier, `commit_through` on completion
    /// notifications, `restore_to` on restore directives.
    pub transaction_log: crate::transactions::TwoPhaseSinkRegistry,

    /// CO5: connector registry for opening sources/sinks by kind instead of
    /// hard-coding concrete types in fragment execution paths. Defaults to
    /// [`krishiv_connectors::registry::default_registry()`] so existing
    /// single-type paths work unchanged.
    pub connector_registry: Arc<krishiv_connectors::ConnectorRegistry>,

    /// E4.4: Optional handle to the coordinator's queryable-state registry.
    ///
    /// When set, streaming `stream:loop:` drain cycles publish a snapshot of
    /// the window operator's state after each batch so external callers can
    /// perform point lookups via the REST API without stopping the job.
    pub(crate) queryable_state: Option<Arc<krishiv_state::QueryableStateStore>>,
}

impl fmt::Debug for ExecutorTaskRunner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutorTaskRunner")
            .field("inbox", &self.inbox)
            .field("shuffle", &self.shuffle)
            .field("loop_executors", &self.loop_executors.len())
            .field(
                "inmem_shuffle",
                &self
                    .inmem_shuffle
                    .as_ref()
                    .map(|_| "<InMemoryShuffleStore>"),
            )
            .field("sql_engine", &self.sql_engine)
            .field(
                "running_attempts",
                &self
                    .running_attempts
                    .as_ref()
                    .map(|map| map.len())
                    .unwrap_or(0),
            )
            .field("live_lease", &self.live_lease.get())
            .field(
                "cached_coordinator_fencing_token",
                &self
                    .cached_coordinator_fencing_token
                    .load(Ordering::Relaxed),
            )
            .field("source_throttle_limits", &self.source_throttle_limits.len())
            .finish()
    }
}

impl ExecutorTaskRunner {
    /// Create a runner over an executor assignment inbox.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self {
            inbox,
            shuffle: None,
            inmem_shuffle: None,
            sql_engine: Arc::new(SqlEngine::new()),
            checkpoint_runners: Arc::new(DashMap::new()),
            running_attempts: None,
            continuous_drainer: None,
            loop_executors: Arc::new(DashMap::new()),
            continuous_inputs: Arc::new(DashMap::new()),
            streaming_advisors: Arc::new(DashMap::new()),
            live_lease: crate::grpc_client::SharedLeaseGeneration::new(
                krishiv_proto::LeaseGeneration::initial(),
            ),
            barrier_injector: None,
            barrier_ack_registry: None,
            key_group_ranges: None,
            cached_coordinator_fencing_token: Arc::new(AtomicU64::new(
                FencingToken::initial().as_u64(),
            )),
            source_throttle_limits: crate::source_throttle::SourceThrottleTable::new(),
            progress_callback: Arc::new(NoOpProgressCallback),
            registered_parquet_cache: Arc::new(DashMap::new()),
            state_dir: None,
            own_executor_id: None,
            pending_restores: Arc::new(DashMap::new()),
            kafka_restore_offsets: Arc::new(DashMap::new()),
            transaction_log: crate::transactions::TwoPhaseSinkRegistry::new(),
            connector_registry: Arc::new(krishiv_connectors::registry::default_registry()),
            queryable_state: None,
        }
    }

    /// Attach the coordinator's queryable-state registry.
    ///
    /// When set, each `stream:loop:` drain cycle publishes a state snapshot so
    /// external callers can query live window state via the REST API.
    pub fn with_queryable_state(mut self, store: Arc<krishiv_state::QueryableStateStore>) -> Self {
        self.queryable_state = Some(store);
        self
    }

    /// Set the root directory for durable window operator state.
    ///
    /// When set, continuous window operators use `RocksDbStateBackend::open(state_dir/job_id/)`
    /// instead of `ephemeral()`, making state survive executor restarts.
    /// Corresponds to the `single-node-durable` and `distributed-durable` profiles.
    pub fn with_state_dir(mut self, dir: std::path::PathBuf) -> Self {
        self.state_dir = Some(dir);
        self
    }

    /// Attach a shared lease handle so checkpoint-fanout RPCs stamp the live
    /// executor lease rather than `LeaseGeneration::initial()` (B10).
    pub fn with_live_lease(mut self, lease: crate::grpc_client::SharedLeaseGeneration) -> Self {
        self.live_lease = lease;
        self
    }

    /// CO5: replace the default connector registry with a custom one.
    ///
    /// Use this to register additional source/sink drivers or override
    /// defaults before the runner starts executing fragments.
    pub fn with_connector_registry(
        mut self,
        registry: krishiv_connectors::ConnectorRegistry,
    ) -> Self {
        self.connector_registry = Arc::new(registry);
        self
    }

    /// Access the shared SQL engine.
    pub fn sql_engine(&self) -> &Arc<SqlEngine> {
        &self.sql_engine
    }

    /// Shared loop executor map for wiring with the gRPC service.
    pub fn shared_loop_executors(
        &self,
    ) -> Arc<DashMap<String, Arc<std::sync::Mutex<krishiv_dataflow::ContinuousWindowExecutor>>>>
    {
        Arc::clone(&self.loop_executors)
    }

    /// Shared continuous input buffer for wiring with the gRPC service.
    pub fn shared_continuous_inputs(&self) -> Arc<DashMap<String, Vec<RecordBatch>>> {
        Arc::clone(&self.continuous_inputs)
    }

    /// Replace the loop executor map with a pre-allocated shared instance.
    ///
    /// Use this builder method in the executor CLI to ensure the gRPC service
    /// and the task runner share the same `ContinuousWindowExecutor` instances.
    pub fn with_shared_loop_executors(
        mut self,
        executors: Arc<
            DashMap<String, Arc<std::sync::Mutex<krishiv_dataflow::ContinuousWindowExecutor>>>,
        >,
    ) -> Self {
        self.loop_executors = executors;
        self
    }

    /// Replace the continuous inputs map with a pre-allocated shared instance.
    pub fn with_shared_continuous_inputs(
        mut self,
        inputs: Arc<DashMap<String, Vec<RecordBatch>>>,
    ) -> Self {
        self.continuous_inputs = inputs;
        self
    }

    /// Access the registered-parquet cache (keyed by `"table_name:path"`).
    ///
    /// This cache is only valid for callers that also reuse the same SQL engine
    /// instance. Task-local SQL execution registers its own parquet inputs.
    pub fn registered_parquet_cache(&self) -> &Arc<dashmap::DashMap<String, ()>> {
        &self.registered_parquet_cache
    }

    /// Inject a pre-existing parquet cache for code paths that reuse a shared
    /// SQL engine instance.
    ///
    /// By default each [`ExecutorTaskRunner`] creates a fresh private cache.
    pub fn with_shared_parquet_cache(mut self, cache: Arc<dashmap::DashMap<String, ()>>) -> Self {
        self.registered_parquet_cache = cache;
        self
    }

    /// Attach a shared barrier injector so barriers received via gRPC are
    /// consumed by the runner loop and trigger checkpoint initiation.
    pub fn with_barrier_injector(mut self, injector: SharedBarrierInjector) -> Self {
        self.barrier_injector = Some(injector);
        self
    }

    /// Attach the barrier ack completion registry shared with the gRPC service.
    pub fn with_barrier_ack_registry(mut self, registry: SharedBarrierAckRegistry) -> Self {
        self.barrier_ack_registry = Some(registry);
        self
    }

    /// Attach the key-group range registry used by the barrier service.
    pub fn with_key_group_ranges(mut self, ranges: SharedKeyGroupRanges) -> Self {
        self.key_group_ranges = Some(ranges);
        self
    }

    /// Attach a pre-created `SourceThrottleTable` so the heartbeat loop and
    /// runner tasks share the same limit map (R7.2 backpressure credit wiring).
    pub fn with_source_throttle_table(
        mut self,
        table: crate::source_throttle::SourceThrottleTable,
    ) -> Self {
        self.source_throttle_limits = table;
        self
    }

    /// Wire local input draining for stateful `stream:loop:` fragments.
    pub fn with_continuous_drainer(mut self, drainer: Arc<dyn ContinuousJobDrainer>) -> Self {
        self.continuous_drainer = Some(drainer);
        self
    }

    /// Track running attempts for coordinator heartbeats (P1-19).
    pub fn with_running_attempts(
        mut self,
        running_attempts: Arc<DashMap<String, TaskAttemptRef>>,
    ) -> Self {
        self.running_attempts = Some(running_attempts);
        self
    }

    /// Set the executor's own identity so checkpoint-ack RPCs carry the real
    /// executor ID rather than a synthetic placeholder.
    pub fn with_executor_id(mut self, id: krishiv_proto::ExecutorId) -> Self {
        self.own_executor_id = Some(id);
        self
    }

    /// Attach a custom SQL engine (including one pre-configured with UDF limits for a job).
    ///
    /// Preferred pattern for job-aware task execution (Track E):
    ///   let limits = /* from JCP or JobRecord for the task's job */;
    ///   let engine = Arc::new(SqlEngine::new().with_udf_limits(limits));
    ///   runner = ExecutorTaskRunner::new(inbox).with_sql_engine(engine);
    ///
    /// This is the concrete execution-path wiring for sandboxed UDF enforcement
    /// on real tasks belonging to a job with resource limits.
    pub fn with_sql_engine(mut self, engine: Arc<SqlEngine>) -> Self {
        self.sql_engine = engine;
        self
    }

    /// Configure UDF resource limits directly on the runner.
    /// This creates a SqlEngine with the given limits for sandbox enforcement
    /// during task execution.
    pub fn with_udf_limits(mut self, limits: krishiv_plan::udf::ResourceLimits) -> Self {
        let engine = Arc::new(SqlEngine::new().with_udf_limits(limits));
        self.sql_engine = engine;
        self
    }

    /// Attach a streaming progress callback (GAP-OB-04).
    ///
    /// Streaming operators call this periodically to report watermark advance,
    /// row throughput, and state size. The heartbeat loop wires this to forward
    /// snapshots to the coordinator for metrics exposure and structured logs.
    pub fn with_progress_callback(mut self, callback: SharedProgressCallback) -> Self {
        self.progress_callback = callback;
        self
    }

    /// Report a streaming progress snapshot via the configured callback.
    ///
    /// Safe to call from any slot — the callback is `Send + Sync`.
    pub fn report_streaming_progress(&self, snapshot: &StreamingProgressSnapshot) {
        self.progress_callback.on_progress(snapshot);
    }

    /// Attach a shuffle context so this runner can handle `shuffle-write:` fragments.
    pub fn with_shuffle(mut self, ctx: ShuffleContext) -> Self {
        self.shuffle = Some(ctx);
        self
    }

    /// Attach an in-memory shuffle store for R4a typed shuffle write/read tasks.
    pub fn with_inmem_shuffle(
        mut self,
        store: std::sync::Arc<krishiv_shuffle::ShuffleBackend>,
    ) -> Self {
        self.inmem_shuffle = Some(store);
        self
    }

    /// Assignment inbox consumed by this runner.
    pub fn inbox(&self) -> &ExecutorAssignmentInbox {
        &self.inbox
    }

    /// Consume and run one queued assignment, if present.
    pub async fn run_next_with<S>(
        &self,
        coordinator: &S,
    ) -> Result<Option<ExecutorTaskRunReport>, tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        let Some(assignment) = self
            .inbox
            .pop_next()
            .map_err(|error| tonic::Status::internal(error.to_string()))?
        else {
            return Ok(None);
        };

        self.run_assignment_with(assignment, coordinator)
            .await
            .map(Some)
    }

    /// Run a specific assignment through the skeleton lifecycle.
    pub async fn run_assignment_with<S>(
        &self,
        assignment: ExecutorTaskAssignment,
        coordinator: &S,
    ) -> Result<ExecutorTaskRunReport, tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        let running = self
            .send_task_status(
                &assignment,
                TaskState::Running,
                "executor accepted assignment",
                coordinator,
                None,
                Vec::new(),
            )
            .await?;
        crate::fragment::common::ensure_status_accepted_or_duplicate(
            running.disposition(),
            TaskState::Running,
        )?;

        if let Some(running_map) = &self.running_attempts {
            let attempt = TaskAttemptRef::new(
                assignment.job_id().clone(),
                assignment.stage_id().clone(),
                assignment.task_id().clone(),
                assignment.attempt_id(),
            );
            running_map.insert(assignment.task_id().as_str().to_string(), attempt);
        }
        if let Some(ranges) = &self.key_group_ranges {
            ranges.set(
                assignment.task_id().as_str().to_string(),
                assignment.key_group_range(),
            );
        }

        // If a CancelTask RPC arrived while this task was queued, finish here
        // instead of starting execution.
        if self
            .inbox
            .is_task_cancelled(assignment.task_id())
            .map_err(|error| tonic::Status::internal(error.to_string()))?
        {
            self.clear_running_attempt(&assignment);
            let _ = self.inbox.clear_cancelled_task(assignment.task_id());
            let cancelled = self
                .send_task_status(
                    &assignment,
                    TaskState::Cancelled,
                    "task cancelled by coordinator request",
                    coordinator,
                    None,
                    Vec::new(),
                )
                .await?;
            return Ok(ExecutorTaskRunReport::new(
                assignment,
                ExecutorTaskOutput::cancelled(),
                running.disposition(),
                cancelled.disposition(),
            ));
        }

        let model = crate::ExecutionModel::from_plan_fragment(assignment.plan_fragment())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let fragment_body = crate::fragment::common::task_fragment_body(
            assignment.plan_fragment().description().trim(),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let is_continuous_cycle = fragment_body.starts_with("stream:loop:");

        // Build resource limits from assignment (propagated from job spec).
        let udf_limits = krishiv_plan::udf::ResourceLimits {
            max_memory_bytes: assignment.memory_limit_bytes(),
            max_execution_time_ms: assignment.cpu_limit_nanos().map(|n| n / 1_000_000),
        };
        // Shared memory budget for all operators within this task.
        let memory_budget =
            krishiv_common::MemoryBudget::from_limit(assignment.memory_limit_bytes());

        let execute_result = match model {
            crate::ExecutionModel::Batch => {
                // Batch tasks respect task_timeout_secs: they are expected to
                // complete in bounded time.  A default of 1 hour guards against
                // hung tasks that would otherwise block the stage forever.
                let timeout_secs = assignment
                    .task_timeout_secs()
                    .unwrap_or(DEFAULT_BATCH_TASK_TIMEOUT_SECS);
                match tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_secs),
                    execute_batch_fragment(self, &assignment, udf_limits, memory_budget),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_elapsed) => Err(ExecutorError::InvalidAssignment {
                        message: format!("task timed out after {} seconds", timeout_secs),
                    }),
                }
            }
            crate::ExecutionModel::Streaming => {
                // Streaming tasks run a bounded-window loop. A safety timeout
                // prevents deadlocked operators from blocking indefinitely (R6).
                // Callers that need longer execution windows should set
                // task_timeout_secs explicitly in the task spec.
                let timeout_secs = assignment
                    .task_timeout_secs()
                    .unwrap_or_else(default_streaming_task_timeout_secs);
                match tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_secs),
                    execute_streaming_fragment(
                        self,
                        &assignment,
                        udf_limits.clone(),
                        memory_budget,
                    ),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_elapsed) => Err(crate::ExecutorError::InvalidAssignment {
                        message: format!(
                            "streaming task timed out after {timeout_secs}s; \
                             set task_timeout_secs in the task spec to allow longer execution"
                        ),
                    }),
                }
            }
            crate::ExecutionModel::DeltaBatch => {
                // Stateless IVM tick (coordinator-authoritative): the executor
                // runs one tick on a transient flow and returns each view's
                // full output. No per-job state is retained on the executor.
                let timeout_secs = assignment
                    .task_timeout_secs()
                    .unwrap_or(DEFAULT_BATCH_TASK_TIMEOUT_SECS);
                let fragment_body = fragment_body.to_string();
                match tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_secs),
                    crate::fragment::ivm::execute_ivm_fragment(&fragment_body),
                )
                .await
                {
                    Ok(Ok((summary, blob))) => {
                        tracing::debug!(
                            active_views = summary.active_views,
                            total_output_rows = summary.total_output_rows,
                            "IVM delta:step tick completed on executor"
                        );
                        Ok(crate::runner::task_output::ExecutorTaskOutput::ivm_step(
                            summary.active_views,
                            summary.total_output_rows,
                        )
                        .with_ivm_output(blob))
                    }
                    Ok(Err(e)) => Err(ExecutorError::InvalidAssignment {
                        message: format!("IVM step failed: {e}"),
                    }),
                    Err(_) => Err(ExecutorError::InvalidAssignment {
                        message: format!("IVM step timed out after {timeout_secs}s"),
                    }),
                }
            }
        };

        let output = match execute_result {
            Ok(output) => output,
            Err(error) => {
                let error_text = error.to_string();
                let message =
                    format_failure_message(assignment.plan_fragment().description(), &error_text);
                // Detect missing upstream shuffle partitions so the coordinator can
                // re-queue the producing task instead of only retrying this consumer.
                let missing = collect_missing_shuffle_partitions(&error);
                let failed = self
                    .send_task_status(
                        &assignment,
                        TaskState::Failed,
                        message,
                        coordinator,
                        None,
                        missing,
                    )
                    .await?;
                crate::fragment::common::ensure_status_accepted_or_duplicate(
                    failed.disposition(),
                    TaskState::Failed,
                )?;
                // Clear the running attempt AFTER the terminal status is reported
                // to the coordinator, matching the success path ordering. This
                // ensures the task remains visible to checkpoint fanout until its
                // terminal status is durably reported.
                self.clear_running_attempt(&assignment);
                return Err(tonic::Status::internal(error_text));
            }
        };

        let typed_requires_reattach = assignment.requires_reattach();

        // A stream:loop assignment is one bounded input cycle over retained
        // operator state. Report Succeeded so the coordinator can durably
        // collect this cycle's output while keeping the logical job active for
        // the next push. Other reattachable streaming operators remain Running
        // continuously.
        let terminal_state = if is_continuous_cycle {
            TaskState::Succeeded
        } else if model == crate::ExecutionModel::Streaming && typed_requires_reattach {
            TaskState::Running
        } else {
            TaskState::Succeeded
        };
        let terminal_message = if is_continuous_cycle {
            "continuous input cycle completed"
        } else if terminal_state == TaskState::Running {
            "streaming operator active"
        } else {
            "executor completed stage-local fragment"
        };

        let terminal = self
            .send_task_status(
                &assignment,
                terminal_state,
                terminal_message,
                coordinator,
                Some(output.to_task_output_metadata()),
                Vec::new(),
            )
            .await?;
        crate::fragment::common::ensure_status_accepted_or_duplicate(
            terminal.disposition(),
            terminal_state,
        )?;

        if terminal_state == TaskState::Succeeded {
            self.clear_running_attempt(&assignment);
        }

        Ok(ExecutorTaskRunReport::new(
            assignment,
            output,
            running.disposition(),
            terminal.disposition(),
        ))
    }

    /// Execute a batch (terminal) stage fragment.
    ///
    /// All R1–R4 fragment kinds route through here.  The function collects
    /// output and returns it so the caller can report `TaskState::Succeeded`.
    #[cfg(test)]
    pub(crate) async fn execute_batch_fragment(
        &self,
        assignment: &ExecutorTaskAssignment,
    ) -> ExecutorResult<ExecutorTaskOutput> {
        execute_batch_fragment(
            self,
            assignment,
            krishiv_plan::udf::ResourceLimits::default(),
            krishiv_common::MemoryBudget::unlimited(),
        )
        .await
    }

    /// Execute a streaming (continuous) stage fragment.
    #[cfg(test)]
    pub(crate) async fn execute_streaming_fragment(
        &self,
        assignment: &ExecutorTaskAssignment,
    ) -> ExecutorResult<ExecutorTaskOutput> {
        execute_streaming_fragment(
            self,
            assignment,
            krishiv_plan::udf::ResourceLimits::default(),
            krishiv_common::MemoryBudget::unlimited(),
        )
        .await
    }

    pub(crate) async fn send_task_status<S>(
        &self,
        assignment: &ExecutorTaskAssignment,
        state: TaskState,
        message: impl Into<String>,
        coordinator: &S,
        output_metadata: Option<TaskOutputMetadata>,
        missing_partitions: Vec<MissingShufflePartition>,
    ) -> Result<TaskStatusResponse, tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        let ids = TaskAttemptRef::new(
            assignment.job_id().clone(),
            assignment.stage_id().clone(),
            assignment.task_id().clone(),
            assignment.attempt_id(),
        );
        let message = message.into();
        let mut request = TaskStatusRequest::new(
            ids,
            assignment.executor_id().clone(),
            assignment.lease_generation(),
            state,
        )
        .with_message(message);
        if let Some(output_metadata) = output_metadata {
            request = request.with_output_metadata(output_metadata);
        }
        if !missing_partitions.is_empty() {
            request = request.with_missing_shuffle_partitions(missing_partitions);
        }

        const MAX_RETRIES: u8 = 3;
        let mut attempt = 0;
        loop {
            let result = coordinator
                .task_status(tonic::Request::new(request.clone()))
                .await
                .map(tonic::Response::into_inner);

            match result {
                Ok(response) => return Ok(response),
                Err(e) => {
                    let is_retryable = matches!(
                        e.code(),
                        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
                    );
                    if is_retryable && attempt < MAX_RETRIES - 1 {
                        attempt += 1;
                        let backoff_ms = 100u64 * (1u64 << attempt);
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn send_checkpoint_ack_with_retries<S>(
        &self,
        ack: CheckpointAckRequest,
        coordinator: S,
    ) -> Result<CheckpointAckResponse, tonic::Status>
    where
        S: CoordinatorExecutorService + Clone + 'static,
    {
        let mut attempt = 0;
        loop {
            let result = coordinator
                .clone()
                .checkpoint_ack(tonic::Request::new(ack.clone()))
                .await
                .map(tonic::Response::into_inner);

            match result {
                Ok(response) => return Ok(response),
                Err(error) => {
                    let is_retryable = matches!(
                        error.code(),
                        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
                    );
                    if is_retryable && attempt < MAX_CHECKPOINT_ACK_RETRIES - 1 {
                        attempt += 1;
                        let backoff_ms = 100u64 * (1u64 << attempt);
                        tracing::warn!(
                            job_id = %ack.job_id,
                            task_id = %ack.task_id,
                            epoch = ack.epoch,
                            attempt,
                            error = %error,
                            "checkpoint ack delivery failed transiently; retrying"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    return Err(error);
                }
            }
        }
    }

    fn clear_running_attempt(&self, assignment: &ExecutorTaskAssignment) {
        if let Some(running_map) = &self.running_attempts {
            running_map.remove(assignment.task_id().as_str());
        }
        if let Some(ranges) = &self.key_group_ranges {
            ranges.remove(assignment.task_id().as_str());
        }
    }

    /// Effective checkpoint-state source for a job: the stateful loop executor
    /// when one exists, otherwise the supplied generic backend.
    pub fn checkpoint_state_for_job(
        &self,
        job_id: &str,
        fallback: CheckpointStateHandle,
    ) -> CheckpointStateHandle {
        match self.loop_executors.get(job_id) {
            Some(entry) => CheckpointStateHandle::ContinuousWindow(Arc::clone(entry.value())),
            None => fallback,
        }
    }

    /// Handle a checkpoint initiation request and deliver the ack to the coordinator (P1-17).
    pub async fn initiate_checkpoint_and_deliver_ack<S>(
        &self,
        assignment: &ExecutorTaskAssignment,
        req: InitiateCheckpointRequest,
        state: CheckpointStateHandle,
        storage: Arc<dyn CheckpointStorage>,
        coordinator: S,
    ) -> Result<CheckpointAckResponse, tonic::Status>
    where
        S: CoordinatorExecutorService + Clone + 'static,
    {
        // Get-or-create the Arc<Mutex<TaskRunner>>; the entry stays in the DashMap
        // throughout the blocking I/O so concurrent barriers for the same task_id
        // find the existing Arc rather than creating a fresh TaskRunner with
        // last_acked_epoch=0 and producing phantom acks.
        let task_id = assignment.task_id().clone();
        let runner_arc = self
            .checkpoint_runners
            .entry(task_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(TaskRunner::new(task_id.clone()))))
            .clone();
        // DashMap shard lock is now fully released; runner_arc keeps state alive.

        let ack = tokio::task::spawn_blocking(move || {
            let mut runner = runner_arc
                .lock()
                .map_err(|_| ExecutorError::LocalExecution {
                    message: "checkpoint runner lock poisoned".into(),
                })?;
            runner.handle_initiate_checkpoint(req, &state, storage.as_ref())
        })
        .await
        .map_err(|error| tonic::Status::internal(error.to_string()))?
        .map_err(|error| tonic::Status::internal(error.to_string()))?;

        self.send_checkpoint_ack_with_retries(ack, coordinator)
            .await
    }

    /// Fan out checkpoint initiation to all known task runners for a job (heartbeat path).
    ///
    /// Uses the real `running_attempts` map to source actual executor and
    /// stage identifiers — previously this code synthesized fake ids that
    /// the coordinator could not correlate (B10).
    pub async fn initiate_checkpoint_for_job<S>(
        &self,
        req: &InitiateCheckpointRequest,
        fallback_state: CheckpointStateHandle,
        storage: Arc<dyn CheckpointStorage>,
        coordinator: S,
    ) -> Result<(), tonic::Status>
    where
        S: CoordinatorExecutorService + Clone + 'static,
    {
        use krishiv_proto::{
            ExecutorTaskAssignment, OutputContract, OutputContractKind, PlanFragment,
            TaskAttemptRef,
        };
        // Cache the live coordinator fencing token so that `drain_pending_barriers`
        // (which cannot receive the token from the barrier proto itself) uses the
        // most-recently seen token instead of always stamping FencingToken::initial().
        self.cached_coordinator_fencing_token
            .store(req.fencing_token.as_u64(), Ordering::SeqCst);
        let running_attempts = self.running_attempts.as_ref().ok_or_else(|| {
            tonic::Status::failed_precondition(
                "executor checkpoint fanout requires running attempt tracking",
            )
        })?;

        let attempts: Vec<TaskAttemptRef> = running_attempts
            .iter()
            .filter_map(|entry| {
                let attempt = entry.value();
                (attempt.job_id() == &req.job_id).then(|| attempt.clone())
            })
            .collect();

        if attempts.is_empty() {
            return Err(tonic::Status::failed_precondition(format!(
                "no running attempts for checkpoint job {}",
                req.job_id
            )));
        }

        // Transactional sinks: durably stage the open buffer under this epoch
        // BEFORE any state snapshot or ack.  A committed checkpoint must have
        // its sink output prepared; failing here fails the whole barrier (no
        // ack is sent, the epoch aborts on timeout, and processing continues).
        let job_id_str = req.job_id.as_str().to_owned();
        if self.transaction_log.has_job(&job_id_str) {
            let log = self.transaction_log.clone();
            let epoch = req.epoch;
            tokio::task::spawn_blocking(move || log.pre_commit(&job_id_str, epoch))
                .await
                .map_err(|error| tonic::Status::internal(error.to_string()))?
                .map_err(|error| {
                    tonic::Status::internal(format!(
                        "transactional sink pre-commit failed for epoch {}: {error}",
                        req.epoch
                    ))
                })?;
        }

        // Continuous window jobs snapshot the per-job loop executor — the
        // generic backend would persist vacuous state for them.
        let state = self.checkpoint_state_for_job(req.job_id.as_str(), fallback_state);

        // Fan out concurrently: each attempt has a distinct task_id (and thus a
        // distinct `checkpoint_runners` entry / TaskRunner mutex), so the
        // per-task snapshot I/O + ack RPCs are independent. Driving them in
        // parallel keeps barrier latency from scaling linearly with the number
        // of this executor's tasks for the job.
        use futures::stream::{FuturesUnordered, StreamExt as _};
        let mut acks = FuturesUnordered::new();
        for attempt in attempts {
            let task_id = attempt.task_id().clone();
            let stage_id = attempt.stage_id().clone();
            let attempt_id = attempt.attempt_id();
            let executor_id = self
                .own_executor_id
                .clone()
                .ok_or_else(|| tonic::Status::internal("executor id not set on runner"))?;
            let ids = TaskAttemptRef::new(req.job_id.clone(), stage_id, task_id, attempt_id);
            let assignment = ExecutorTaskAssignment::new(
                ids,
                executor_id,
                self.live_lease.get(),
                PlanFragment::new("checkpoint"),
                OutputContract::new(OutputContractKind::InlineRecordBatches, "checkpoint"),
            );

            let req = req.clone();
            let state = state.clone();
            let storage = Arc::clone(&storage);
            let coordinator = coordinator.clone();
            acks.push(async move {
                let task_id = assignment.task_id().clone();
                let result = self
                    .initiate_checkpoint_and_deliver_ack(
                        &assignment,
                        req,
                        state,
                        storage,
                        coordinator,
                    )
                    .await;
                (task_id, result)
            });
        }
        let mut failed_tasks: Vec<String> = Vec::new();
        while let Some((task_id, result)) = acks.next().await {
            if let Err(error) = result {
                tracing::warn!(task_id = %task_id, error = %error, "checkpoint acknowledgement failed");
                failed_tasks.push(task_id.to_string());
            }
        }
        if !failed_tasks.is_empty() {
            return Err(tonic::Status::internal(format!(
                "checkpoint acks failed for tasks: {}",
                failed_tasks.join(", ")
            )));
        }
        Ok(())
    }

    /// Apply a `CheckpointCompleteCommand`: commit transactional-sink output
    /// prepared at or before the committed epoch.
    ///
    /// Completion notifications are best-effort.  A failed or missed commit
    /// here is repaired by the next completion notification (commit-through
    /// covers earlier epochs) or by restore (recover-and-commit), so errors
    /// are logged rather than escalated.
    pub async fn handle_checkpoint_complete(&self, cmd: &krishiv_proto::CheckpointCompleteCommand) {
        self.cached_coordinator_fencing_token
            .store(cmd.fencing_token.as_u64(), Ordering::SeqCst);
        let log = self.transaction_log.clone();
        let job_id = cmd.job_id.as_str().to_owned();
        let epoch = cmd.epoch;
        let result = tokio::task::spawn_blocking(move || log.commit_through(&job_id, epoch)).await;
        match result {
            Ok(Ok(0)) => {}
            Ok(Ok(committed)) => tracing::info!(
                job_id = %cmd.job_id,
                epoch = cmd.epoch,
                committed,
                "committed transactional sink output for completed checkpoint"
            ),
            Ok(Err(error)) => tracing::error!(
                job_id = %cmd.job_id,
                epoch = cmd.epoch,
                error = %error,
                "transactional sink commit failed; retried on the next completion or restore"
            ),
            Err(join_error) => tracing::error!(
                job_id = %cmd.job_id,
                epoch = cmd.epoch,
                error = %join_error,
                "transactional sink commit task panicked"
            ),
        }
    }

    /// Apply a `RestoreFromCheckpointCommand`: reload operator state, re-seed
    /// source offsets, and reconcile transactional sinks for one job.
    ///
    /// Blocking storage I/O — call via `spawn_blocking` from async contexts.
    pub fn restore_job_from_checkpoint(
        &self,
        cmd: &krishiv_proto::RestoreFromCheckpointCommand,
        fallback_state: &CheckpointStateHandle,
        storage: &dyn CheckpointStorage,
    ) -> ExecutorResult<()> {
        use krishiv_state::checkpoint::read_epoch_metadata;

        let job_id = cmd.job_id.as_str();
        let restore_err = |message: String| ExecutorError::LocalExecution { message };

        let metadata = read_epoch_metadata(storage, job_id, cmd.epoch)
            .map_err(|e| restore_err(format!("restore read metadata for {job_id}: {e}")))?
            .ok_or_else(|| {
                restore_err(format!(
                    "restore failed for job {job_id}: checkpoint epoch {} not found in \
                     executor checkpoint storage",
                    cmd.epoch
                ))
            })?;
        metadata
            .validate()
            .map_err(|e| restore_err(format!("restore metadata invalid for {job_id}: {e}")))?;
        if metadata.job_id != job_id || metadata.epoch != cmd.epoch {
            return Err(restore_err(format!(
                "restore metadata mismatch for job {job_id} epoch {}: metadata has job {} epoch {}",
                cmd.epoch, metadata.job_id, metadata.epoch
            )));
        }

        self.cached_coordinator_fencing_token
            .store(cmd.fencing_token.as_u64(), Ordering::SeqCst);

        // Transactional sinks: commit output covered by the restored
        // checkpoint, abort everything after it.  A failure here must fail
        // the restore — proceeding would silently drop committed-checkpoint
        // sink output or leak post-checkpoint output.
        let (committed, aborted) = self
            .transaction_log
            .restore_to(job_id, cmd.epoch)
            .map_err(|e| restore_err(format!("transactional sink restore for {job_id}: {e}")))?;
        if committed > 0 || aborted > 0 {
            tracing::info!(
                job_id,
                epoch = cmd.epoch,
                committed,
                aborted,
                "reconciled transactional sink output during restore"
            );
        }

        // Source offsets: stash restored Kafka positions for the pipelines to
        // seek at (re)construction.
        let kafka_offsets = kafka_offsets_from_source_records(&metadata.source_offsets);
        if kafka_offsets.is_empty() {
            self.kafka_restore_offsets.remove(job_id);
        } else {
            self.kafka_restore_offsets
                .insert(job_id.to_owned(), kafka_offsets);
        }

        // Operator state: read every snapshot referenced by the checkpoint.
        let mut snapshots = Vec::with_capacity(metadata.operator_snapshots.len());
        for snap_ref in &metadata.operator_snapshots {
            let bytes = storage
                .read_bytes(&snap_ref.snapshot_path)
                .map_err(|e| restore_err(format!("restore read {}: {e}", snap_ref.snapshot_path)))?
                .ok_or_else(|| {
                    restore_err(format!(
                        "restore failed for job {job_id}: snapshot {} referenced by \
                         checkpoint epoch {} is missing",
                        snap_ref.snapshot_path, cmd.epoch
                    ))
                })?;
            snapshots.push(bytes);
        }

        if let Some(loop_exec) = self
            .loop_executors
            .get(job_id)
            .map(|e| Arc::clone(e.value()))
        {
            // Live loop executor: roll its window state back now.
            apply_snapshots_to_state(
                &CheckpointStateHandle::ContinuousWindow(loop_exec),
                &snapshots,
            )
            .map_err(|e| restore_err(format!("restore window state for {job_id}: {e}")))?;
            self.pending_restores.remove(job_id);
        } else {
            // No loop executor yet (fresh process): stash the snapshots so the
            // executor created by the first `stream:loop:` fragment is seeded
            // before its first drain.
            self.pending_restores.insert(
                job_id.to_owned(),
                RestoredJobCheckpoint {
                    epoch: cmd.epoch,
                    fencing_token: cmd.fencing_token.as_u64(),
                    snapshots: snapshots.clone(),
                },
            );
        }

        // Generic backend: clear this job's operator namespaces and merge the
        // restored entries so non-window stateful tasks roll back too.
        if let CheckpointStateHandle::Backend(backend) = fallback_state {
            let mut guard = backend
                .lock()
                .map_err(|e| restore_err(format!("state backend lock poisoned: {e}")))?;
            let namespaces = guard
                .list_namespaces()
                .map_err(|e| restore_err(format!("restore list namespaces: {e}")))?;
            for ns in namespaces {
                if metadata
                    .operator_snapshots
                    .iter()
                    .any(|s| s.operator_id == ns.operator_id())
                {
                    guard
                        .clear_namespace(&ns)
                        .map_err(|e| restore_err(format!("restore clear namespace: {e}")))?;
                }
            }
            for bytes in &snapshots {
                let entries = krishiv_state::decode_snapshot_entries(bytes)
                    .map_err(|e| restore_err(format!("restore decode snapshot: {e}")))?;
                let batch: Vec<(&str, &str, &[u8], &[u8])> = entries
                    .iter()
                    .map(|(op, name, key, value)| {
                        (op.as_str(), name.as_str(), key.as_slice(), value.as_slice())
                    })
                    .collect();
                if !batch.is_empty() {
                    guard
                        .put_batch(&batch)
                        .map_err(|e| restore_err(format!("restore merge into backend: {e}")))?;
                }
            }
        }

        // Reset per-task checkpoint progress so pre-rollback barriers are
        // rejected as stale and the next epoch acks cleanly.
        if let Some(running) = &self.running_attempts {
            for entry in running.iter() {
                if entry.value().job_id() != &cmd.job_id {
                    continue;
                }
                let task_id = entry.value().task_id().clone();
                self.checkpoint_runners
                    .entry(task_id.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(TaskRunner::new(task_id))))
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .apply_restored_epoch(cmd.epoch);
            }
        }

        tracing::info!(
            job_id,
            epoch = cmd.epoch,
            snapshots = snapshots.len(),
            "restored job state from checkpoint"
        );
        Ok(())
    }

    /// Key-group range this executor covers for `job_id`, derived from the
    /// ranges registered at task assignment.  Defaults to the full range when
    /// no hosted task registered one (single-node deployments).
    fn key_group_range_for_job(&self, job_id: &str) -> krishiv_proto::KeyGroupRange {
        let (Some(running), Some(ranges)) = (&self.running_attempts, &self.key_group_ranges) else {
            return krishiv_proto::KeyGroupRange::full();
        };
        let mut bounds: Option<(u32, u32)> = None;
        for entry in running.iter() {
            if entry.value().job_id().as_str() != job_id {
                continue;
            }
            if let Some(range) = ranges.get(entry.value().task_id().as_str()) {
                bounds = Some(match bounds {
                    None => (range.start(), range.end()),
                    Some((start, end)) => (start.min(range.start()), end.max(range.end())),
                });
            }
        }
        match bounds {
            Some((start, end)) => krishiv_proto::KeyGroupRange::try_new(start, end)
                .unwrap_or_else(|_| krishiv_proto::KeyGroupRange::full()),
            None => krishiv_proto::KeyGroupRange::full(),
        }
    }

    /// Drain all pending barriers from the shared injector and initiate
    /// checkpoints for each one.  Called from the runner loop in `cli.rs`.
    pub async fn drain_pending_barriers<S>(
        &self,
        fallback_state: CheckpointStateHandle,
        storage: Arc<dyn CheckpointStorage>,
        coordinator: S,
    ) where
        S: CoordinatorExecutorService + Clone + 'static,
    {
        let Some(ref injector) = self.barrier_injector else {
            return;
        };
        while let Some(barrier) = injector.next_barrier() {
            let Ok(job_id) = JobId::try_new(&barrier.job_id) else {
                continue;
            };
            // Use the most-recently observed coordinator fencing token so the
            // ack is not rejected after a leadership election.
            let raw_token = self.cached_coordinator_fencing_token.load(Ordering::SeqCst);
            let fencing_token = if raw_token == 0 {
                // No checkpoint received yet; no prior leader exists.
                FencingToken::initial()
            } else {
                // Defensively handle unexpected fencing token values rather than
                // panicking (M4). If the token is invalid, fall back to initial.
                FencingToken::try_new(raw_token).unwrap_or_else(|_| {
                    tracing::warn!(
                        raw_token,
                        "cached fencing token rejected by validation; falling back to initial"
                    );
                    FencingToken::initial()
                })
            };
            let req = InitiateCheckpointRequest {
                job_id,
                epoch: barrier.epoch,
                fencing_token,
            };

            let s_clone = Arc::clone(&storage);
            let coord_clone = coordinator.clone();
            if let Err(e) = self
                .initiate_checkpoint_for_job(&req, fallback_state.clone(), s_clone, coord_clone)
                .await
            {
                tracing::warn!(error = %e, "barrier checkpoint failed");
            } else if let Some(registry) = &self.barrier_ack_registry {
                use crate::barrier_transport::BarrierAckCompletion;
                let range = self.key_group_range_for_job(&barrier.job_id);
                registry.complete(
                    &barrier.job_id,
                    barrier.epoch,
                    BarrierAckCompletion {
                        checkpoint_uri: format!(
                            "checkpoint://{}/{}",
                            barrier.job_id, barrier.checkpoint_id
                        ),
                        key_group_range_start: range.start(),
                        key_group_range_end: range.end(),
                    },
                );
            }
        }
    }
}
