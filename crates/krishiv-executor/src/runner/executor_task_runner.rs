use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_proto::{
    CheckpointAckRequest, CheckpointAckResponse, CheckpointAlignment, CoordinatorExecutorService,
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
    RestoredSourceOffset, ShuffleContext, apply_snapshots_to_state,
    kafka_offsets_from_source_records, restored_source_offsets_from_records,
};
use super::task_runner::{
    ContinuousJobDrainer, NoOpProgressCallback, SharedProgressCallback, StreamingProgressSnapshot,
    TaskRunner,
};

pub(crate) type SharedContinuousConnectorSources =
    Arc<DashMap<String, Arc<tokio::sync::Mutex<Box<dyn krishiv_connectors::DynSource>>>>>;

use crate::erased;

/// Shared per-job egress buffers for run-loop (`stream:rloop:`) jobs.
///
/// The run-loop appends emitted windows here; `drain_continuous_output`
/// serves and clears them. Bounded (drop-oldest) — durable consumption goes
/// through the transactional sink or queryable state, per the DUR-5 contract.
pub type SharedContinuousOutputs = Arc<DashMap<String, Vec<RecordBatch>>>;

/// Shared per-buffer-key wakeup notifies for pushed continuous input.
///
/// `push_continuous_input` notifies the key it appended under so a run-loop
/// blocked in its idle wait wakes within microseconds instead of the fallback
/// tick (the embedded loop's `data_notify` discipline, promoted).
pub type SharedContinuousNotify = Arc<DashMap<String, Arc<tokio::sync::Notify>>>;

/// Which stateful operator a running task's barrier snapshots must capture.
///
/// Bound by the run-loop / stateful join fragments at start; consulted by the
/// checkpoint fanout so each subtask snapshots ITS operator instead of a
/// per-job singleton (the H-6 fix carried through the checkpoint path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskStateBinding {
    /// `loop_executors` key of a continuous window executor.
    Window(String),
    /// `join_executors` key of a continuous two-input window join operator.
    Join(String),
}

/// Dyn-erased coordinator client so long-lived fragments (the run-loop) can
/// drive barrier checkpoints without the generic `S: CoordinatorExecutorService`
/// parameter infecting fragment signatures.
#[derive(Clone)]
pub struct SharedCoordinatorClient(pub Arc<dyn CoordinatorExecutorService>);

impl std::fmt::Debug for SharedCoordinatorClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SharedCoordinatorClient")
    }
}

#[tonic::async_trait]
impl CoordinatorExecutorService for SharedCoordinatorClient {
    async fn register_executor(
        &self,
        request: tonic::Request<krishiv_proto::RegisterExecutorRequest>,
    ) -> Result<tonic::Response<krishiv_proto::RegisterExecutorResponse>, tonic::Status> {
        self.0.register_executor(request).await
    }

    async fn deregister_executor(
        &self,
        request: tonic::Request<krishiv_proto::DeregisterExecutorRequest>,
    ) -> Result<tonic::Response<krishiv_proto::DeregisterExecutorResponse>, tonic::Status> {
        self.0.deregister_executor(request).await
    }

    async fn executor_heartbeat(
        &self,
        request: tonic::Request<krishiv_proto::ExecutorHeartbeatRequest>,
    ) -> Result<tonic::Response<krishiv_proto::ExecutorHeartbeatResponse>, tonic::Status> {
        self.0.executor_heartbeat(request).await
    }

    async fn task_status(
        &self,
        request: tonic::Request<TaskStatusRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        self.0.task_status(request).await
    }

    async fn checkpoint_ack(
        &self,
        request: tonic::Request<CheckpointAckRequest>,
    ) -> Result<tonic::Response<CheckpointAckResponse>, tonic::Status> {
        self.0.checkpoint_ack(request).await
    }

    async fn push_task_result(
        &self,
        request: tonic::Request<krishiv_proto::services::TaskResultChunkStream>,
    ) -> Result<tonic::Response<krishiv_proto::PushTaskResultResponse>, tonic::Status> {
        self.0.push_task_result(request).await
    }
}

/// Everything a long-lived run-loop fragment needs to drive barrier
/// checkpoints from inside its own iteration boundary (Leg C of Phase 55):
/// the generic state fallback, checkpoint storage, and a coordinator client.
/// Wired by the executor CLI where all three live.
#[derive(Clone)]
pub struct RunLoopBarrierContext {
    pub state: CheckpointStateHandle,
    pub storage: Arc<dyn CheckpointStorage>,
    pub coordinator: SharedCoordinatorClient,
}

impl fmt::Debug for RunLoopBarrierContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunLoopBarrierContext")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

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
    /// Per-job/partition connector sources for `stream:loop:` registry inputs.
    ///
    /// These source instances retain connector-owned cursor state across
    /// continuous input cycles. Restore evicts entries for the job so the next
    /// cycle opens a fresh source and applies the restored checkpoint offset.
    pub(crate) continuous_connector_sources: SharedContinuousConnectorSources,
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

    /// Generic connector-encoded source offsets per job, consumed by connector
    /// source construction before the first restored read.
    pub source_restore_offsets: Arc<DashMap<String, Vec<RestoredSourceOffset>>>,

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

    /// Phase 55: per-job egress buffers for run-loop jobs (bounded; served by
    /// `drain_continuous_output` so results never park in coordinator memory).
    pub(crate) continuous_outputs: SharedContinuousOutputs,

    /// Phase 55: per-buffer-key input notifies shared with the gRPC service —
    /// the run-loop's µs-scale wakeup on pushed input.
    pub(crate) continuous_input_notify: SharedContinuousNotify,

    /// Phase 55: which stateful operator each running task's barrier snapshot
    /// must capture (per-subtask window executors, continuous join operators).
    pub(crate) task_state_bindings: Arc<DashMap<String, TaskStateBinding>>,

    /// Phase 55: per-job stateful two-input join operators (`window-join:`
    /// fragments retain state across cycles — closes G5/#88 state loss).
    pub(crate) join_executors:
        Arc<DashMap<String, Arc<Mutex<krishiv_dataflow::WatermarkWindowJoinOperator>>>>,

    /// Phase 55: credit-gated executor→executor exchange for keyed run-loop
    /// parallelism. All runner clones share the peer channel map.
    pub(crate) stream_exchange: crate::stream_exchange::StreamExchange,

    /// This executor's advertised task endpoint, used to short-circuit
    /// exchange deliveries to co-located peer subtasks.
    pub(crate) own_task_endpoint: Option<String>,

    /// Phase 55: barrier-drive context for run-loop fragments (state fallback,
    /// checkpoint storage, and coordinator client). `None` outside the CLI
    /// runtime — the slot loop in `cli.rs` then remains the only barrier
    /// drainer, as before.
    pub(crate) barrier_context: Option<RunLoopBarrierContext>,

    /// Phase 56: job → declared run-loop parallelism, registered by each
    /// subtask at start. Restore uses it as the key-group redistribution
    /// target so a process hosting a SUBSET of subtasks still routes by the
    /// job-wide ranges.
    pub(crate) rloop_parallelism: Arc<DashMap<String, usize>>,

    /// Phase 57 (AUD-6): executor-resident IVM flows, keyed by IVM job id.
    /// State attaches once; every tick afterwards feeds deltas into the same
    /// warm flow (cached ctx + compiled plans + operator accumulators) and
    /// returns output deltas.
    pub(crate) ivm_flows: crate::fragment::ivm::ResidentIvmFlows,
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
            sql_engine: Arc::new(
                SqlEngine::new()
                    .with_target_parallelism(krishiv_sql::default_parallelism_from_env()),
            ),
            checkpoint_runners: Arc::new(DashMap::new()),
            running_attempts: None,
            continuous_drainer: None,
            loop_executors: Arc::new(DashMap::new()),
            continuous_inputs: Arc::new(DashMap::new()),
            continuous_connector_sources: Arc::new(DashMap::new()),
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
            source_restore_offsets: Arc::new(DashMap::new()),
            transaction_log: crate::transactions::TwoPhaseSinkRegistry::new(),
            connector_registry: Arc::new(krishiv_connectors::registry::default_registry()),
            queryable_state: None,
            continuous_outputs: Arc::new(DashMap::new()),
            continuous_input_notify: Arc::new(DashMap::new()),
            task_state_bindings: Arc::new(DashMap::new()),
            join_executors: Arc::new(DashMap::new()),
            stream_exchange: crate::stream_exchange::StreamExchange::default(),
            own_task_endpoint: None,
            barrier_context: None,
            rloop_parallelism: Arc::new(DashMap::new()),
            ivm_flows: Arc::new(DashMap::new()),
        }
    }

    /// Share pre-allocated run-loop egress buffers with the gRPC service.
    pub fn with_shared_continuous_outputs(mut self, outputs: SharedContinuousOutputs) -> Self {
        self.continuous_outputs = outputs;
        self
    }

    /// Share pre-allocated input notifies with the gRPC service.
    pub fn with_shared_continuous_notify(mut self, notify: SharedContinuousNotify) -> Self {
        self.continuous_input_notify = notify;
        self
    }

    /// Shared run-loop egress buffers for wiring with the gRPC service.
    pub fn shared_continuous_outputs(&self) -> SharedContinuousOutputs {
        Arc::clone(&self.continuous_outputs)
    }

    /// Shared input notifies for wiring with the gRPC service.
    pub fn shared_continuous_notify(&self) -> SharedContinuousNotify {
        Arc::clone(&self.continuous_input_notify)
    }

    /// Record this executor's advertised task endpoint so exchange deliveries
    /// to co-located peer subtasks short-circuit through shared memory.
    pub fn with_own_task_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.own_task_endpoint = Some(endpoint.into());
        self
    }

    /// Attach the barrier-drive context so run-loop fragments align barriers
    /// at their own iteration boundaries (Phase 55 Leg C).
    pub fn with_barrier_context(mut self, context: RunLoopBarrierContext) -> Self {
        self.barrier_context = Some(context);
        self
    }

    /// Wake-up handle for one continuous-input buffer key, creating it on
    /// first use. `push_continuous_input` notifies this handle after append.
    pub(crate) fn notify_handle(&self, key: &str) -> Arc<tokio::sync::Notify> {
        Arc::clone(
            self.continuous_input_notify
                .entry(key.to_owned())
                .or_default()
                .value(),
        )
    }

    /// Notify any run-loop waiting on `key` that input arrived.
    pub(crate) fn notify_continuous_input(&self, key: &str) {
        if let Some(notify) = self.continuous_input_notify.get(key) {
            notify.notify_waiters();
        }
    }

    /// Drain pending barriers through the attached [`RunLoopBarrierContext`].
    /// Returns the number of barriers processed (0 when no context is wired —
    /// the CLI slot loop then remains the only barrier drainer).
    pub(crate) async fn drain_barriers_via_context(&self) -> usize {
        let Some(ctx) = self.barrier_context.clone() else {
            return 0;
        };
        self.drain_pending_barriers(ctx.state, ctx.storage, ctx.coordinator)
            .await
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

    /// Shared connector source cache for long-lived stream-loop registry inputs.
    pub(crate) fn shared_continuous_connector_sources(&self) -> SharedContinuousConnectorSources {
        Arc::clone(&self.continuous_connector_sources)
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

    fn clear_continuous_connector_sources_for_job(&self, job_id: &str) {
        // Cycle-model sources key by `{job}|…`; run-loop subtasks key by
        // `{job}#<subtask>|…` — clear both families on restore/teardown.
        let cycle_prefix = format!("{job_id}|");
        let rloop_prefix = format!("{job_id}#");
        let keys: Vec<String> = self
            .continuous_connector_sources
            .iter()
            .filter_map(|entry| {
                (entry.key().starts_with(&cycle_prefix) || entry.key().starts_with(&rloop_prefix))
                    .then(|| entry.key().clone())
            })
            .collect();
        for key in keys {
            self.continuous_connector_sources.remove(&key);
        }
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

    /// Register the platform Iceberg REST catalog from `KRISHIV_ICEBERG_REST_*`
    /// on the shared SQL engine (once, at startup), so governed
    /// `catalog.namespace.table` references resolve during local-stage fragment
    /// execution — the coordinator-mode catalog gap for gateway SELECTs. Returns
    /// whether a catalog was registered. Call after `with_udf_limits`, which
    /// replaces the engine.
    pub async fn register_catalog_from_env(&self) -> Result<bool, String> {
        self.sql_engine
            .register_iceberg_rest_catalog_from_env()
            .await
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
    pub async fn run_next_with(
        &self,
        coordinator: &dyn CoordinatorExecutorService,
    ) -> Result<Option<ExecutorTaskRunReport>, tonic::Status> {
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
    pub async fn run_assignment_with(
        &self,
        assignment: ExecutorTaskAssignment,
        coordinator: &dyn CoordinatorExecutorService,
    ) -> Result<ExecutorTaskRunReport, tonic::Status> {
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
        let is_run_loop =
            fragment_body.starts_with(crate::fragment::run_loop::STREAM_RLOOP_PREFIX);

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
                    erased(execute_batch_fragment(
                        self,
                        &assignment,
                        udf_limits,
                        memory_budget,
                    )),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_elapsed) => Err(ExecutorError::InvalidAssignment {
                        message: format!("task timed out after {} seconds", timeout_secs),
                    }),
                }
            }
            crate::ExecutionModel::Streaming if is_run_loop => {
                // Phase 55 run-loop: the task IS a long-lived loop that exits
                // only on cancellation — a wall-clock timeout would kill a
                // healthy streaming job, so none applies.
                erased(execute_streaming_fragment(
                    self,
                    &assignment,
                    udf_limits.clone(),
                    memory_budget,
                ))
                .await
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
                    erased(execute_streaming_fragment(
                        self,
                        &assignment,
                        udf_limits.clone(),
                        memory_budget,
                    )),
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
                // Phase 57 (AUD-6): resident-protocol fragments run against the
                // executor's persistent per-job flow (attach/tick/ckpt/detach).
                // The legacy `delta:step:` stateless tick is kept for
                // rolling-upgrade compatibility with older coordinators.
                let timeout_secs = assignment
                    .task_timeout_secs()
                    .unwrap_or(DEFAULT_BATCH_TASK_TIMEOUT_SECS);
                let fragment_body = fragment_body.to_string();
                let is_resident =
                    crate::fragment::ivm::is_resident_ivm_fragment(&fragment_body);
                let ivm_future = erased(async {
                    if is_resident {
                        crate::fragment::ivm::execute_resident_ivm_fragment(
                            &self.ivm_flows,
                            &fragment_body,
                        )
                        .await
                    } else {
                        crate::fragment::ivm::execute_ivm_fragment(&fragment_body).await
                    }
                });
                match tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_secs),
                    ivm_future,
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
        let run_loop_cancelled = is_run_loop
            && output.kind() == crate::runner::ExecutorTaskOutputKind::Cancelled;
        let terminal_state = if run_loop_cancelled {
            // A run-loop returns only when cancelled: report Cancelled so the
            // coordinator's teardown observes the stop instead of a phantom
            // success.
            TaskState::Cancelled
        } else if is_continuous_cycle {
            TaskState::Succeeded
        } else if model == crate::ExecutionModel::Streaming && typed_requires_reattach {
            TaskState::Running
        } else {
            TaskState::Succeeded
        };
        let terminal_message = if run_loop_cancelled {
            "run-loop task cancelled"
        } else if is_continuous_cycle {
            "continuous input cycle completed"
        } else if terminal_state == TaskState::Running {
            "streaming operator active"
        } else {
            "executor completed stage-local fragment"
        };

        // Phase 2.10: a disk-spooled result must reach the coordinator BEFORE
        // the terminal status report that announces it — the coordinator
        // claims the spool while applying the update. Push failure fails the
        // task (silently dropping the result would corrupt the job output).
        if terminal_state == TaskState::Succeeded
            && let Some(spool) = output.spooled_result()
        {
            let ids = TaskAttemptRef::new(
                assignment.job_id().clone(),
                assignment.stage_id().clone(),
                assignment.task_id().clone(),
                assignment.attempt_id(),
            );
            let chunks = crate::runner::result_spool::spool_chunk_stream(
                ids,
                spool.path().to_path_buf(),
                spool.total_bytes(),
            );
            let push_result = coordinator
                .push_task_result(tonic::Request::new(chunks))
                .await;
            if let Err(error) = push_result {
                let message = format!("spooled result delivery failed: {error}");
                let failed = self
                    .send_task_status(
                        &assignment,
                        TaskState::Failed,
                        message.clone(),
                        coordinator,
                        None,
                        Vec::new(),
                    )
                    .await?;
                crate::fragment::common::ensure_status_accepted_or_duplicate(
                    failed.disposition(),
                    TaskState::Failed,
                )?;
                self.clear_running_attempt(&assignment);
                return Err(tonic::Status::internal(message));
            }
        }

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

        if matches!(terminal_state, TaskState::Succeeded | TaskState::Cancelled) {
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

    pub(crate) async fn send_task_status(
        &self,
        assignment: &ExecutorTaskAssignment,
        state: TaskState,
        message: impl Into<String>,
        coordinator: &dyn CoordinatorExecutorService,
        output_metadata: Option<TaskOutputMetadata>,
        missing_partitions: Vec<MissingShufflePartition>,
    ) -> Result<TaskStatusResponse, tonic::Status> {
        let ids = TaskAttemptRef::new(
            assignment.job_id().clone(),
            assignment.stage_id().clone(),
            assignment.task_id().clone(),
            assignment.attempt_id(),
        );
        let message = message.into();
        // Stamp the freshest lease we hold: the assignment embeds the lease at
        // push time, but a heartbeat-timeout eviction + re-register bumps the
        // registry's generation while the task keeps running — reporting the
        // frozen assignment lease then gets fenced as stale and aborts healthy
        // work (B10 already made checkpoint-fanout use the live lease). `max`
        // keeps the assignment stamp when the shared handle was never attached
        // (it defaults to `initial()`); cross-process zombie fencing is
        // unaffected — a zombie's live lease is behind the registry either way.
        // A task-status channel can remain pinned to a coordinator that has
        // just demoted. The standby rejects mutations, while the heartbeat
        // loop invalidates this channel and re-registers against the new
        // leader. Keep the assignment in the runner until that convergence
        // completes; dropping it here would leave the coordinator's Assigned
        // task permanently wedged. Rebuild the request every time so a lease
        // advanced by the heartbeat loop is picked up by the next attempt.
        const MAX_RETRIES: u8 = 60;
        let mut attempt = 0u8;
        loop {
            let lease = assignment.lease_generation().max(self.live_lease.get());
            let mut request =
                TaskStatusRequest::new(ids.clone(), assignment.executor_id().clone(), lease, state)
                    .with_message(message.clone());
            if let Some(output_metadata) = &output_metadata {
                request = request.with_output_metadata(output_metadata.clone());
            }
            if !missing_partitions.is_empty() {
                request = request.with_missing_shuffle_partitions(missing_partitions.clone());
            }

            let result = coordinator
                .task_status(tonic::Request::new(request))
                .await
                .map(tonic::Response::into_inner);

            match result {
                Ok(response)
                    if matches!(
                        response.disposition(),
                        krishiv_proto::TransportDisposition::StaleLease
                            | krishiv_proto::TransportDisposition::UnknownExecutor
                    ) && attempt < MAX_RETRIES - 1 =>
                {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Ok(response) => return Ok(response),
                Err(e) => {
                    let is_retryable = matches!(
                        e.code(),
                        tonic::Code::Unavailable
                            | tonic::Code::DeadlineExceeded
                            | tonic::Code::FailedPrecondition
                    );
                    if is_retryable && attempt < MAX_RETRIES - 1 {
                        attempt += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn send_checkpoint_ack_with_retries(
        &self,
        ack: CheckpointAckRequest,
        coordinator: SharedCoordinatorClient,
    ) -> Result<CheckpointAckResponse, tonic::Status> {
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

    /// Effective checkpoint-state source for one running task attempt.
    ///
    /// Phase 55: run-loop subtasks and stateful join tasks bind their own
    /// operator (`task_state_bindings`), so a barrier snapshot on a
    /// parallelism-N job captures each subtask's key-group slice instead of a
    /// per-job singleton. Falls back to the job-level lookup (cycle model)
    /// and then to the generic backend.
    pub fn checkpoint_state_for_task(
        &self,
        job_id: &str,
        task_id: &str,
        fallback: CheckpointStateHandle,
    ) -> CheckpointStateHandle {
        if let Some(binding) = self.task_state_bindings.get(task_id) {
            match binding.value() {
                TaskStateBinding::Window(key) => {
                    if let Some(entry) = self.loop_executors.get(key) {
                        return CheckpointStateHandle::ContinuousWindow(Arc::clone(entry.value()));
                    }
                }
                TaskStateBinding::Join(key) => {
                    if let Some(entry) = self.join_executors.get(key) {
                        return CheckpointStateHandle::WindowJoin(Arc::clone(entry.value()));
                    }
                }
            }
        }
        self.checkpoint_state_for_job(job_id, fallback)
    }

    /// Handle a checkpoint initiation request and deliver the ack to the coordinator (P1-17).
    pub async fn initiate_checkpoint_and_deliver_ack(
        &self,
        assignment: &ExecutorTaskAssignment,
        req: InitiateCheckpointRequest,
        state: CheckpointStateHandle,
        storage: Arc<dyn CheckpointStorage>,
        coordinator: SharedCoordinatorClient,
        sink_transactions: Vec<krishiv_proto::SinkTransactionRef>,
    ) -> Result<CheckpointAckResponse, tonic::Status> {
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

        let epoch = req.epoch;
        let mut ack = tokio::task::spawn_blocking(move || {
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

        // DUR-2 reporting: attach the job's prepared-sink transactions so the
        // coordinator persists them in the checkpoint metadata and can drive
        // commit-or-abort on recovery. Only stamp on a real (non-stale) ack —
        // a stale ack carries `last_acked_epoch` and must not claim to have
        // prepared this epoch's sink output. The coordinator dedups by
        // `prepare_path`, so stamping the same refs on every task attempt's ack
        // is safe.
        if ack.epoch == epoch {
            ack.sink_transactions = sink_transactions;
        }

        self.send_checkpoint_ack_with_retries(ack, coordinator)
            .await
    }

    /// Fan out checkpoint initiation to all known task runners for a job (heartbeat path).
    ///
    /// Uses the real `running_attempts` map to source actual executor and
    /// stage identifiers — previously this code synthesized fake ids that
    /// the coordinator could not correlate (B10).
    pub async fn initiate_checkpoint_for_job(
        &self,
        req: &InitiateCheckpointRequest,
        fallback_state: CheckpointStateHandle,
        storage: Arc<dyn CheckpointStorage>,
        coordinator: SharedCoordinatorClient,
    ) -> Result<(), tonic::Status> {
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
        //
        // DUR-2: after staging, read back the prepared-transaction refs (each
        // with its durable prepare path) so every task attempt's ack can carry
        // them to the coordinator. Without this the checkpoint metadata records
        // no sink transactions and recovery cannot commit-or-abort them.
        let job_id_str = req.job_id.as_str().to_owned();
        let mut sink_transactions: Vec<krishiv_proto::SinkTransactionRef> = Vec::new();
        if self.transaction_log.has_job(&job_id_str) {
            let log = self.transaction_log.clone();
            let epoch = req.epoch;
            let job_for_blocking = job_id_str.clone();
            let refs = tokio::task::spawn_blocking(move || {
                log.pre_commit(&job_for_blocking, epoch)?;
                log.prepared_refs(&job_for_blocking)
            })
            .await
            .map_err(|error| tonic::Status::internal(error.to_string()))?
            .map_err(|error| {
                tonic::Status::internal(format!(
                    "transactional sink pre-commit failed for epoch {}: {error}",
                    req.epoch
                ))
            })?;
            sink_transactions = refs
                .into_iter()
                .map(|r| krishiv_proto::SinkTransactionRef {
                    // The durable path is the recovery identity; it is also the
                    // coordinator's dedup key, so distinct staged files stay
                    // distinct while duplicate reports collapse.
                    sink_id: r.prepare_path.clone(),
                    epoch: r.epoch,
                    prepare_path: r.prepare_path,
                    committed: false,
                })
                .collect();
        }

        // Continuous window jobs snapshot their stateful operator — the
        // generic backend would persist vacuous state for them. Resolution is
        // per-attempt (Phase 55): a parallelism-N run-loop job snapshots each
        // subtask's own key-group slice via `task_state_bindings`.
        let state_for_attempt = |task_id: &TaskId| {
            self.checkpoint_state_for_task(
                req.job_id.as_str(),
                task_id.as_str(),
                fallback_state.clone(),
            )
        };

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
            let state = state_for_attempt(assignment.task_id());
            let storage = Arc::clone(&storage);
            let coordinator = coordinator.clone();
            let sink_transactions = sink_transactions.clone();
            acks.push(async move {
                let task_id = assignment.task_id().clone();
                let result = self
                    .initiate_checkpoint_and_deliver_ack(
                        &assignment,
                        req,
                        state,
                        storage,
                        coordinator,
                        sink_transactions,
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

        // DUR-2: `restore_to` above reconciles the in-memory prepared log,
        // which covers a coordinator-only restart. On an executor crash that
        // log was empty, so drive the coordinator's durable recovery plan —
        // each ref reconstructs its prepared transaction from the durable
        // prepare path and commits (covered by the restored epoch) or aborts
        // (after it). `finalize_prepared` is idempotent, so running it after
        // `restore_to` never double-commits.
        let commit_paths: Vec<String> = cmd
            .sink_commit
            .iter()
            .map(|s| s.prepare_path.clone())
            .collect();
        let abort_paths: Vec<String> = cmd
            .sink_abort
            .iter()
            .map(|s| s.prepare_path.clone())
            .collect();
        let (rec_committed, rec_aborted) = self
            .transaction_log
            .recover_prepared_refs(job_id, &commit_paths, &abort_paths)
            .map_err(|e| restore_err(format!("DUR-2 durable sink recovery for {job_id}: {e}")))?;
        if rec_committed > 0 || rec_aborted > 0 {
            tracing::info!(
                job_id,
                epoch = cmd.epoch,
                rec_committed,
                rec_aborted,
                "recovered prepared-sink transactions from durable checkpoint refs (DUR-2)"
            );
        }

        // Source offsets: stash generic connector-encoded offsets for any
        // connector source and keep the existing Kafka compatibility cache.
        let source_offsets = restored_source_offsets_from_records(&metadata.source_offsets);
        if source_offsets.is_empty() {
            self.source_restore_offsets.remove(job_id);
        } else {
            self.source_restore_offsets
                .insert(job_id.to_owned(), source_offsets);
        }
        self.clear_continuous_connector_sources_for_job(job_id);

        let kafka_offsets = kafka_offsets_from_source_records(&metadata.source_offsets);
        if kafka_offsets.is_empty() {
            self.kafka_restore_offsets.remove(job_id);
        } else {
            self.kafka_restore_offsets
                .insert(job_id.to_owned(), kafka_offsets);
        }

        // Operator state: read every snapshot referenced by the checkpoint,
        // then materialize incremental SST pointer blobs into portable
        // snapshots (Phase 56) so all downstream restore paths keep working
        // on one format.
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
        let snapshots =
            crate::runner::task_runner::materialize_portable_snapshots(snapshots, storage)?;

        // Run-loop subtask executors key by `{job}#<subtask>`. With exactly
        // one local subtask the restore applies directly; with several, the
        // per-subtask snapshot↔key-group redistribution is Phase 56 scope
        // (rescaling) — stash the snapshots and surface the gap loudly
        // instead of merging sibling key-groups into the wrong operator.
        let rloop_prefix = format!("{job_id}#");
        let rloop_execs: Vec<_> = self
            .loop_executors
            .iter()
            .filter(|e| e.key().starts_with(&rloop_prefix))
            .map(|e| Arc::clone(e.value()))
            .collect();
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
        } else if rloop_execs.len() == 1 {
            let Some(loop_exec) = rloop_execs.into_iter().next() else {
                unreachable!("len checked above");
            };
            apply_snapshots_to_state(
                &CheckpointStateHandle::ContinuousWindow(loop_exec),
                &snapshots,
            )
            .map_err(|e| restore_err(format!("restore run-loop state for {job_id}: {e}")))?;
            self.pending_restores.remove(job_id);
        } else if rloop_execs.len() > 1 {
            // Phase 56: key-group redistribution — every checkpointed entry
            // is routed to the subtask that owns its key group at the JOB's
            // parallelism, then each locally hosted subtask loads its share.
            // Cross-node rescale requires shared checkpoint storage so every
            // executor sees the full snapshot set (the Flink model).
            let parallelism = self
                .rloop_parallelism
                .get(job_id)
                .map(|entry| *entry.value())
                .unwrap_or(rloop_execs.len());
            let redistributed = krishiv_state::redistribute_snapshots(
                &snapshots,
                parallelism as u32,
                krishiv_state::EntryRouting::WindowGroupKey,
            )
            .map_err(|e| {
                restore_err(format!("key-group redistribution for {job_id}: {e}"))
            })?;
            let mut applied = 0usize;
            for entry in self.loop_executors.iter() {
                let Some(subtask) = entry
                    .key()
                    .strip_prefix(&rloop_prefix)
                    .and_then(|suffix| suffix.parse::<usize>().ok())
                else {
                    continue;
                };
                let Some(share) = redistributed.get(subtask) else {
                    return Err(restore_err(format!(
                        "restore for {job_id}: subtask {subtask} outside redistribution \
                         range {parallelism}"
                    )));
                };
                apply_snapshots_to_state(
                    &CheckpointStateHandle::ContinuousWindow(Arc::clone(entry.value())),
                    std::slice::from_ref(share),
                )
                .map_err(|e| {
                    restore_err(format!(
                        "restore run-loop subtask {subtask} state for {job_id}: {e}"
                    ))
                })?;
                applied += 1;
            }
            tracing::info!(
                job_id,
                epoch = cmd.epoch,
                parallelism,
                local_subtasks = applied,
                "restored run-loop job via key-group redistribution"
            );
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
    /// checkpoints for each one.  Called from the runner loop in `cli.rs` and
    /// from run-loop iteration boundaries (Phase 55 Leg C). Returns the
    /// number of barriers processed.
    pub async fn drain_pending_barriers(
        &self,
        fallback_state: CheckpointStateHandle,
        storage: Arc<dyn CheckpointStorage>,
        coordinator: SharedCoordinatorClient,
    ) -> usize {
        let Some(ref injector) = self.barrier_injector else {
            return 0;
        };
        let mut processed = 0usize;
        while let Some(barrier) = injector.next_barrier() {
            processed = processed.saturating_add(1);
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
                alignment: CheckpointAlignment::default(),
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
        processed
    }
}
