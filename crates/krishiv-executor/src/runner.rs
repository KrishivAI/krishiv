//! Task runner types: `TaskRunner`, `ExecutorTaskRunner`, `ExecutorTaskRunReport`,
//! `ExecutorTaskOutput`, `ExecutorTaskOutputKind`, `ShuffleContext`, `LocalParquetPartition`.

use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_checkpoint::{CheckpointStorage, snapshot_path};
use krishiv_proto::{
    CheckpointAckRequest, CheckpointAckResponse, CheckpointSourceOffset,
    CoordinatorExecutorService, ExecutorTaskAssignment, FencingToken, InitiateCheckpointRequest,
    InputPartitionDescriptor, JobId, TaskAttemptRef, TaskId, TaskOutputMetadata, TaskRuntimeStats,
    TaskState, TaskStatusRequest, TaskStatusResponse, TransportDisposition,
};
use krishiv_sql::SqlEngine;
use krishiv_state::StateBackend;

use crate::{
    ExecutorAssignmentInbox, ExecutorError, ExecutorResult, SharedBarrierInjector,
    fragment::{batch::execute_batch_fragment, streaming::execute_streaming_fragment},
};

/// Maximum bytes used in the failure message sent to the coordinator.  Larger
/// messages are truncated with `…` so they cannot blow past gRPC payload limits.
pub(crate) const TASK_FAILURE_MESSAGE_MAX_BYTES: usize = 4096;

/// Format an executor-side failure into a coordinator-visible message that
/// includes the fragment description and the underlying error text.  Truncates
/// at [`TASK_FAILURE_MESSAGE_MAX_BYTES`] so we cannot ship arbitrarily large
/// strings through `task_status` RPCs.
pub(crate) fn format_failure_message(fragment: &str, error: &str) -> String {
    let mut buf = String::with_capacity(fragment.len() + error.len() + 32);
    buf.push_str("executor failed fragment '");
    buf.push_str(fragment.trim());
    buf.push_str("': ");
    buf.push_str(error.trim());
    if buf.len() > TASK_FAILURE_MESSAGE_MAX_BYTES {
        let mut end = TASK_FAILURE_MESSAGE_MAX_BYTES.saturating_sub(1);
        while !buf.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        buf.truncate(end);
        buf.push('…');
    }
    buf
}

pub(crate) const LOCAL_PARQUET_PARTITION_PREFIX: &str = "local-parquet:";
pub(crate) const CONNECTOR_PARQUET_PARTITION_PREFIX: &str = "connector-parquet:";
pub(crate) const OBJECT_PARQUET_PARTITION_PREFIX: &str = "object-parquet:";
pub(crate) const OBJECT_PARQUET_SINK_PREFIX: &str = "object-parquet-sink:";
#[cfg(feature = "kafka")]
pub(crate) const MEMORY_KAFKA_PARTITION_PREFIX: &str = "memory-kafka:";
#[cfg(feature = "kafka")]
pub(crate) const PARQUET_SINK_PREFIX: &str = "parquet-sink:";
#[cfg(feature = "kafka")]
pub(crate) const KAFKA_TO_PARQUET_FRAGMENT: &str = "connector-pipeline:kafka-to-parquet";
pub(crate) const SHUFFLE_WRITE_PREFIX: &str = "shuffle-write:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalParquetPartition {
    pub(crate) table_name: String,
    pub(crate) path: PathBuf,
}

impl LocalParquetPartition {
    pub(crate) fn parse(partition: &krishiv_proto::InputPartition) -> ExecutorResult<Option<Self>> {
        let (table_name, path) = match partition.descriptor() {
            Some(InputPartitionDescriptor::LocalParquet { table_name, path }) => {
                (table_name.as_str(), path.as_str())
            }
            Some(_) => return Ok(None),
            None => {
                let descriptor = partition.description().trim();
                let Some(payload) = descriptor.strip_prefix(LOCAL_PARQUET_PARTITION_PREFIX) else {
                    return Ok(None);
                };
                payload
                    .split_once(':')
                    .ok_or_else(|| ExecutorError::InvalidAssignment {
                        message: format!(
                            "input partition {} must use local-parquet:<table>:<path>",
                            partition.partition_id()
                        ),
                    })?
            }
        };
        let table_name = table_name.trim();
        let path = path.trim();
        if table_name.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "input partition {} has an empty local Parquet table name",
                    partition.partition_id()
                ),
            });
        }
        if path.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "input partition {} has an empty local Parquet path",
                    partition.partition_id()
                ),
            });
        }

        Ok(Some(Self {
            table_name: table_name.to_owned(),
            path: PathBuf::from(path),
        }))
    }

    pub(crate) fn table_name(&self) -> &str {
        &self.table_name
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

/// Result of one executor-side task runner pass.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutorTaskRunReport {
    assignment: ExecutorTaskAssignment,
    output: ExecutorTaskOutput,
    running_disposition: TransportDisposition,
    terminal_disposition: TransportDisposition,
}

impl ExecutorTaskRunReport {
    pub(crate) fn new(
        assignment: ExecutorTaskAssignment,
        output: ExecutorTaskOutput,
        running_disposition: TransportDisposition,
        terminal_disposition: TransportDisposition,
    ) -> Self {
        Self {
            assignment,
            output,
            running_disposition,
            terminal_disposition,
        }
    }

    /// Assignment processed by this runner pass.
    pub fn assignment(&self) -> &ExecutorTaskAssignment {
        &self.assignment
    }

    /// Local output metadata produced by this runner pass.
    pub fn output(&self) -> &ExecutorTaskOutput {
        &self.output
    }

    /// Coordinator response to the `Running` status update.
    pub fn running_disposition(&self) -> TransportDisposition {
        self.running_disposition
    }

    /// Coordinator response to the terminal status update.
    pub fn terminal_disposition(&self) -> TransportDisposition {
        self.terminal_disposition
    }
}

/// Encode record batches as Arrow IPC stream bytes for coordinator inline results.
fn encode_record_batches_ipc(batches: &[RecordBatch]) -> Result<Vec<Vec<u8>>, String> {
    use arrow::ipc::writer::StreamWriter;

    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut buf, &schema).map_err(|e| format!("ipc writer: {e}"))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| format!("ipc write batch: {e}"))?;
        }
        writer.finish().map_err(|e| format!("ipc finish: {e}"))?;
    }
    Ok(vec![buf])
}

/// Local executor output metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutorTaskOutput {
    pub(crate) kind: ExecutorTaskOutputKind,
    pub(crate) row_count: usize,
    pub(crate) batch_count: usize,
    pub(crate) column_count: usize,
    pub(crate) shuffle_partitions: Vec<krishiv_proto::ShufflePartitionOutput>,
    pub(crate) runtime_stats: Option<TaskRuntimeStats>,
    /// Record batches produced by streaming window operators (in-process / local path).
    pub(crate) record_batches: Vec<RecordBatch>,
    /// GAP-2: Maximum event-time watermark (in milliseconds) reached by this
    /// streaming window task.  `None` for batch and non-window tasks.
    ///
    /// The coordinator propagates this to downstream stage scheduling so that
    /// a pipeline fan-out knows the global low watermark across all executor
    /// tasks and can safely emit late-data decisions.
    pub(crate) watermark_ms: Option<i64>,
}

impl ExecutorTaskOutput {
    pub(crate) fn sql(row_count: usize, batch_count: usize, column_count: usize) -> Self {
        Self {
            kind: ExecutorTaskOutputKind::Sql,
            row_count,
            batch_count,
            column_count,
            shuffle_partitions: Vec::new(),
            runtime_stats: None,
            record_batches: Vec::new(),
            watermark_ms: None,
        }
    }

    #[cfg(feature = "kafka")]
    pub(crate) fn connector_pipeline(
        row_count: usize,
        batch_count: usize,
        column_count: usize,
    ) -> Self {
        Self {
            kind: ExecutorTaskOutputKind::ConnectorPipeline,
            row_count,
            batch_count,
            column_count,
            shuffle_partitions: Vec::new(),
            runtime_stats: None,
            record_batches: Vec::new(),
            watermark_ms: None,
        }
    }

    pub(crate) fn placeholder() -> Self {
        Self {
            kind: ExecutorTaskOutputKind::Placeholder,
            row_count: 0,
            batch_count: 0,
            column_count: 0,
            shuffle_partitions: Vec::new(),
            runtime_stats: None,
            record_batches: Vec::new(),
            watermark_ms: None,
        }
    }

    pub(crate) fn cancelled() -> Self {
        Self {
            kind: ExecutorTaskOutputKind::Cancelled,
            row_count: 0,
            batch_count: 0,
            column_count: 0,
            shuffle_partitions: Vec::new(),
            runtime_stats: None,
            record_batches: Vec::new(),
            watermark_ms: None,
        }
    }

    pub(crate) fn shuffle_write(
        row_count: usize,
        partitions: Vec<krishiv_proto::ShufflePartitionOutput>,
    ) -> Self {
        Self {
            kind: ExecutorTaskOutputKind::ShuffleWrite,
            row_count,
            batch_count: partitions.len(),
            column_count: 0,
            shuffle_partitions: partitions,
            runtime_stats: None,
            record_batches: Vec::new(),
            watermark_ms: None,
        }
    }

    /// Output from a R5.1 streaming window aggregation task.
    pub(crate) fn streaming_window(
        row_count: usize,
        batch_count: usize,
        column_count: usize,
        record_batches: Vec<RecordBatch>,
    ) -> Self {
        Self {
            kind: ExecutorTaskOutputKind::StreamingWindow,
            row_count,
            batch_count,
            column_count,
            shuffle_partitions: Vec::new(),
            runtime_stats: None,
            record_batches,
            watermark_ms: None,
        }
    }

    /// Batches produced by this task (streaming window or SQL).
    pub fn record_batches(&self) -> &[RecordBatch] {
        &self.record_batches
    }

    pub(crate) fn with_runtime_stats(mut self, stats: TaskRuntimeStats) -> Self {
        self.runtime_stats = Some(stats);
        self
    }

    pub(crate) fn with_record_batches(mut self, batches: Vec<RecordBatch>) -> Self {
        self.record_batches = batches;
        self
    }

    /// Attach the maximum event-time watermark reached by this streaming task.
    ///
    /// Must be set for `StreamingWindow` outputs so that the coordinator can
    /// track global low-watermark across all tasks and propagate it downstream.
    pub(crate) fn with_watermark_ms(mut self, watermark_ms: i64) -> Self {
        self.watermark_ms = Some(watermark_ms);
        self
    }

    /// Maximum event-time watermark reached by this streaming window task, if any.
    pub fn watermark_ms(&self) -> Option<i64> {
        self.watermark_ms
    }

    /// Output kind.
    pub fn kind(&self) -> ExecutorTaskOutputKind {
        self.kind
    }

    /// Number of rows produced locally.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Number of Arrow record batches produced locally.
    pub fn batch_count(&self) -> usize {
        self.batch_count
    }

    /// Number of columns in the local output schema.
    pub fn column_count(&self) -> usize {
        self.column_count
    }

    /// Convert to coordinator-visible lightweight metadata.
    pub fn to_task_output_metadata(&self) -> TaskOutputMetadata {
        let mut meta = TaskOutputMetadata::new(
            self.kind.as_str(),
            self.row_count as u64,
            self.batch_count as u64,
            self.column_count as u64,
        );
        if !self.shuffle_partitions.is_empty() {
            meta = meta.with_shuffle_partitions(self.shuffle_partitions.clone());
        }
        if let Some(stats) = &self.runtime_stats {
            meta = meta.with_runtime_stats(stats.clone());
        }
        if !self.record_batches.is_empty()
            && let Ok(ipc) = encode_record_batches_ipc(&self.record_batches)
        {
            meta = meta.with_inline_record_batch_ipc(ipc);
        }
        // GAP-2: Propagate watermark so the coordinator can track global low-watermark
        // across all executor tasks for downstream stage scheduling.
        if let Some(wm) = self.watermark_ms {
            meta = meta.with_watermark_ms(wm);
        }
        meta
    }

    /// Shuffle partition outputs produced by this task (empty for non-shuffle tasks).
    pub fn shuffle_partitions(&self) -> &[krishiv_proto::ShufflePartitionOutput] {
        &self.shuffle_partitions
    }
}

/// Local executor output kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorTaskOutputKind {
    /// Real SQL fragment executed through the Krishiv SQL/DataFusion seam.
    Sql,
    /// Connector-to-connector pipeline executed by the task runner.
    ConnectorPipeline,
    /// Placeholder path for non-SQL fragments while R3.1 is still bootstrapping.
    Placeholder,
    /// Task was cancelled before execution started.
    Cancelled,
    /// Shuffle write: hash-partitioned batches written to the local shuffle store.
    ShuffleWrite,
    /// R5.1 streaming tumbling-window aggregation output.
    StreamingWindow,
}

impl ExecutorTaskOutputKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::ConnectorPipeline => "connector_pipeline",
            Self::Placeholder => "placeholder",
            Self::Cancelled => "cancelled",
            Self::ShuffleWrite => "shuffle_write",
            Self::StreamingWindow => "streaming_window",
        }
    }
}

/// Shuffle store context held by the task runner.
///
/// When present, `shuffle-write:` fragments can write hash-partitioned output to
/// the local store and report `ShufflePartitionOutput` back to the coordinator.
#[derive(Clone)]
pub struct ShuffleContext {
    pub store: std::sync::Arc<krishiv_shuffle::LocalDiskShuffleStore>,
    /// `<host>:<port>` of this executor's Arrow IPC flight server.
    pub flight_endpoint: String,
}

impl fmt::Debug for ShuffleContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShuffleContext")
            .field("flight_endpoint", &self.flight_endpoint)
            .finish()
    }
}

// ── R6 CheckpointState ────────────────────────────────────────────────────────

/// Per-task checkpoint state for executor-side checkpoint participation (R6).
///
/// Tracks the last acked epoch, operator/task identity, and source offset so the
/// executor can correctly handle `InitiateCheckpointRequest` messages.
#[derive(Debug, Clone)]
pub struct TaskRunner {
    /// Last checkpoint epoch that this task acked (0 = none acked yet).
    pub last_acked_epoch: u64,
    /// Operator identifier for this task: defaults to `"operator-<task_id>"`.
    pub operator_id: String,
    /// Task identifier.
    pub task_id: TaskId,
    /// Last Kafka offset processed.  `-1` if this is not a Kafka source task.
    pub kafka_source_offset: i64,
}

impl TaskRunner {
    /// Create a new `TaskRunner` for `task_id`.
    pub fn new(task_id: TaskId) -> Self {
        let operator_id = format!("operator-{}", task_id.as_str());
        Self {
            last_acked_epoch: 0,
            operator_id,
            task_id,
            kafka_source_offset: -1,
        }
    }

    /// Set the Kafka source offset (for source tasks).
    pub fn with_kafka_offset(mut self, offset: i64) -> Self {
        self.kafka_source_offset = offset;
        self
    }

    /// Handle a `InitiateCheckpointRequest`.
    ///
    /// 1. Rejects stale epochs (epoch <= last_acked_epoch).
    /// 2. Takes a snapshot via `state_backend.snapshot()`.
    /// 3. Writes the snapshot to `storage`.
    /// 4. Returns a `CheckpointAckRequest` with source offsets and snapshot path.
    /// 5. Updates `last_acked_epoch`.
    pub fn handle_initiate_checkpoint(
        &mut self,
        req: InitiateCheckpointRequest,
        state_backend: &dyn StateBackend,
        storage: &(impl CheckpointStorage + ?Sized),
    ) -> ExecutorResult<CheckpointAckRequest> {
        // Stale epoch: return an ack that signals the stale condition via epoch.
        if req.epoch <= self.last_acked_epoch {
            return Ok(CheckpointAckRequest {
                job_id: req.job_id,
                operator_id: self.operator_id.clone(),
                task_id: self.task_id.clone(),
                epoch: self.last_acked_epoch, // signal: stale
                fencing_token: req.fencing_token,
                source_offsets: vec![],
                snapshot_path: None,
            });
        }

        // Take a state snapshot (EXE-1: fail-closed — do not ack a new epoch on error).
        let snapshot_bytes = match state_backend.snapshot() {
            Ok(bytes) => bytes,
            Err(krishiv_state::StateError::SnapshotUnsupported { .. }) => Vec::new(),
            Err(_) => {
                return Err(ExecutorError::LocalExecution {
                    message: format!(
                        "checkpoint snapshot failed for task {} at epoch {}",
                        self.task_id, req.epoch
                    ),
                });
            }
        };

        // Write snapshot if non-empty; suppress phantom path on write failure.
        let snap_path = if !snapshot_bytes.is_empty() {
            let path = snapshot_path(
                req.job_id.as_str(),
                req.epoch,
                &self.operator_id,
                self.task_id.as_str(),
            );
            // `storage` may be `?Sized`, so we cannot pass it to the
            // `&dyn CheckpointStorage`-accepting helper.  Call the trait
            // method directly using the same `snapshot_path` layout.
            storage
                .write_bytes(&path, &snapshot_bytes)
                .map_err(|error| ExecutorError::LocalExecution {
                    message: format!(
                        "checkpoint snapshot write failed for task {} at epoch {}: {error}",
                        self.task_id, req.epoch
                    ),
                })?;
            Some(path)
        } else {
            None
        };

        // Build source offsets.
        let source_offsets = if self.kafka_source_offset >= 0 {
            vec![CheckpointSourceOffset {
                partition_id: format!("kafka-{}", self.task_id.as_str()),
                offset: self.kafka_source_offset,
            }]
        } else {
            vec![]
        };

        self.last_acked_epoch = req.epoch;

        Ok(CheckpointAckRequest {
            job_id: req.job_id,
            operator_id: self.operator_id.clone(),
            task_id: self.task_id.clone(),
            epoch: req.epoch,
            fencing_token: req.fencing_token,
            source_offsets,
            snapshot_path: snap_path,
        })
    }
}

/// Drains output from a long-running continuous streaming job (R5.2).
pub trait ContinuousJobDrainer: Send + Sync {
    /// Process pending input for `job_id` and return newly emitted batches.
    fn drain_job(&self, job_id: &str) -> Result<Vec<RecordBatch>, String>;
}

/// Minimal R3.1 stage-local task runner skeleton.
#[derive(Clone)]
pub struct ExecutorTaskRunner {
    pub(crate) inbox: ExecutorAssignmentInbox,
    pub(crate) shuffle: Option<ShuffleContext>,
    pub(crate) inmem_shuffle: Option<std::sync::Arc<krishiv_shuffle::InMemoryShuffleStore>>,
    /// Shared SQL engine — one instance per runner rather than per-fragment.
    pub(crate) sql_engine: Arc<SqlEngine>,
    /// Per-task checkpoint state keyed by task id.
    pub(crate) checkpoint_runners: Arc<DashMap<TaskId, TaskRunner>>,
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
        Arc<DashMap<String, Arc<std::sync::Mutex<krishiv_exec::ContinuousWindowExecutor>>>>,
    /// Live executor lease generation, shared with the heartbeat loop.
    /// Used to stamp checkpoint-fanout RPCs without round-tripping through
    /// the gRPC service (B10).  Defaults to `LeaseGeneration::initial()`.
    pub(crate) live_lease: crate::grpc_client::SharedLeaseGeneration,

    /// Shared barrier injector fed by the gRPC `BarrierService`.  Barriers
    /// enqueued here are drained by the runner loop and trigger checkpoint
    /// initiation.
    pub(crate) barrier_injector: Option<SharedBarrierInjector>,

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
            .field(
                "source_throttle_limits",
                &self.source_throttle_limits.len(),
            )
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
            live_lease: crate::grpc_client::SharedLeaseGeneration::new(
                krishiv_proto::LeaseGeneration::initial(),
            ),
            barrier_injector: None,
            cached_coordinator_fencing_token: Arc::new(AtomicU64::new(
                FencingToken::initial().as_u64(),
            )),
            source_throttle_limits: crate::source_throttle::SourceThrottleTable::new(),
        }
    }

    /// Attach a shared lease handle so checkpoint-fanout RPCs stamp the live
    /// executor lease rather than `LeaseGeneration::initial()` (B10).
    pub fn with_live_lease(mut self, lease: crate::grpc_client::SharedLeaseGeneration) -> Self {
        self.live_lease = lease;
        self
    }

    /// Attach a shared barrier injector so barriers received via gRPC are
    /// consumed by the runner loop and trigger checkpoint initiation.
    pub fn with_barrier_injector(mut self, injector: SharedBarrierInjector) -> Self {
        self.barrier_injector = Some(injector);
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

    /// Wire continuous streaming drain for `stream:continuous:` fragments.
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

    /// Attach a custom SQL engine. Useful for tests or policy-wrapped engines.
    pub fn with_sql_engine(mut self, engine: Arc<SqlEngine>) -> Self {
        self.sql_engine = engine;
        self
    }

    /// Attach a shuffle context so this runner can handle `shuffle-write:` fragments.
    pub fn with_shuffle(mut self, ctx: ShuffleContext) -> Self {
        self.shuffle = Some(ctx);
        self
    }

    /// Attach an in-memory shuffle store for R4a typed shuffle write/read tasks.
    pub fn with_inmem_shuffle(
        mut self,
        store: std::sync::Arc<krishiv_shuffle::InMemoryShuffleStore>,
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
                )
                .await?;
            return Ok(ExecutorTaskRunReport::new(
                assignment,
                ExecutorTaskOutput::cancelled(),
                running.disposition(),
                cancelled.disposition(),
            ));
        }

        let model =
            crate::ExecutionModel::from_fragment(assignment.plan_fragment().description().trim());

        let execute_result = match model {
            crate::ExecutionModel::Batch => {
                // Batch tasks respect task_timeout_secs: they are expected to
                // complete in bounded time.
                if let Some(timeout_secs) = assignment.task_timeout_secs() {
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(timeout_secs),
                        execute_batch_fragment(self, &assignment),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_elapsed) => Err(ExecutorError::InvalidAssignment {
                            message: format!("task timed out after {} seconds", timeout_secs),
                        }),
                    }
                } else {
                    execute_batch_fragment(self, &assignment).await
                }
            }
            crate::ExecutionModel::Streaming => {
                // Streaming tasks run an unbounded loop; task_timeout_secs is
                // intentionally ignored.  The real continuous operator loop
                // arrives in R5.1; until then return a clear not-implemented
                // error so R5 can replace this branch without touching call
                // sites.
                execute_streaming_fragment(self, &assignment).await
            }
        };

        let output = match execute_result {
            Ok(output) => output,
            Err(error) => {
                self.clear_running_attempt(&assignment);
                let error_text = error.to_string();
                let message =
                    format_failure_message(assignment.plan_fragment().description(), &error_text);
                let failed = self
                    .send_task_status(&assignment, TaskState::Failed, message, coordinator, None)
                    .await?;
                crate::fragment::common::ensure_status_accepted_or_duplicate(
                    failed.disposition(),
                    TaskState::Failed,
                )?;
                return Err(tonic::Status::internal(error_text));
            }
        };

        let fragment = assignment.plan_fragment().description().trim();
        // GAP-6: stream:loop: fragments complete each drain cycle and report
        // Succeeded so the coordinator sees the windowed output.  Future drain
        // cycles are triggered by re-assigning the task.
        let terminal_streaming_task = model == crate::ExecutionModel::Streaming
            && (fragment.starts_with("stream:continuous:")
                || fragment.starts_with("stream:loop:")
                || krishiv_plan::window::parse_stream_fragment(fragment).is_ok());
        let terminal_state =
            if model == crate::ExecutionModel::Streaming && !terminal_streaming_task {
                TaskState::Running
            } else {
                TaskState::Succeeded
            };
        let terminal_message = if terminal_state == TaskState::Running {
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
    #[allow(dead_code)]
    pub(crate) async fn execute_batch_fragment(
        &self,
        assignment: &ExecutorTaskAssignment,
    ) -> ExecutorResult<ExecutorTaskOutput> {
        execute_batch_fragment(self, assignment).await
    }

    /// Execute a streaming (continuous) stage fragment.
    #[allow(dead_code)]
    pub(crate) async fn execute_streaming_fragment(
        &self,
        assignment: &ExecutorTaskAssignment,
    ) -> ExecutorResult<ExecutorTaskOutput> {
        execute_streaming_fragment(self, assignment).await
    }

    pub(crate) async fn send_task_status<S>(
        &self,
        assignment: &ExecutorTaskAssignment,
        state: TaskState,
        message: impl Into<String>,
        coordinator: &S,
        output_metadata: Option<TaskOutputMetadata>,
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

        coordinator
            .task_status(tonic::Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }

    fn clear_running_attempt(&self, assignment: &ExecutorTaskAssignment) {
        if let Some(running_map) = &self.running_attempts {
            running_map.remove(assignment.task_id().as_str());
        }
    }

    /// Handle a checkpoint initiation request and deliver the ack to the coordinator (P1-17).
    pub async fn initiate_checkpoint_and_deliver_ack<S>(
        &self,
        assignment: &ExecutorTaskAssignment,
        req: InitiateCheckpointRequest,
        state_backend: &dyn StateBackend,
        storage: &(impl CheckpointStorage + ?Sized),
        coordinator: &S,
    ) -> Result<CheckpointAckResponse, tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        let mut checkpoint_runner = self
            .checkpoint_runners
            .entry(assignment.task_id().clone())
            .or_insert_with(|| TaskRunner::new(assignment.task_id().clone()));
        let ack = match tokio::runtime::Handle::try_current() {
            Ok(handle)
                if matches!(
                    handle.runtime_flavor(),
                    tokio::runtime::RuntimeFlavor::MultiThread
                ) =>
            {
                tokio::task::block_in_place(|| {
                    checkpoint_runner.handle_initiate_checkpoint(req, state_backend, storage)
                })
            }
            _ => checkpoint_runner.handle_initiate_checkpoint(req, state_backend, storage),
        }
        .map_err(|error| tonic::Status::internal(error.to_string()))?;
        drop(checkpoint_runner);
        coordinator
            .checkpoint_ack(tonic::Request::new(ack))
            .await
            .map(tonic::Response::into_inner)
    }

    /// Fan out checkpoint initiation to all known task runners for a job (heartbeat path).
    ///
    /// Uses the real `running_attempts` map to source actual executor and
    /// stage identifiers — previously this code synthesized fake ids that
    /// the coordinator could not correlate (B10).
    pub async fn initiate_checkpoint_for_job<S>(
        &self,
        req: &InitiateCheckpointRequest,
        state_backend: &dyn StateBackend,
        storage: &(impl CheckpointStorage + ?Sized),
        coordinator: &S,
    ) -> Result<(), tonic::Status>
    where
        S: CoordinatorExecutorService,
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
        for entry in self.checkpoint_runners.iter() {
            let task_id = entry.key().clone();
            // Prefer the real attempt ref from running_attempts.  Fall back to
            // a stage-0 / initial-attempt synthesis ONLY when no real ref is
            // available (e.g. the task completed before the barrier arrived).
            let (stage_id, attempt_id, executor_id_opt) = self
                .running_attempts
                .as_ref()
                .and_then(|map| {
                    map.get(task_id.as_str()).map(|attempt| {
                        (
                            attempt.stage_id().clone(),
                            attempt.attempt_id(),
                            None::<krishiv_proto::ExecutorId>,
                        )
                    })
                })
                .unwrap_or_else(|| {
                    let stage = krishiv_proto::StageId::try_new("s0")
                        .or_else(|_| krishiv_proto::StageId::try_new("stage"))
                        .expect("stage id");
                    (stage, krishiv_proto::AttemptId::initial(), None)
                });
            let _ = executor_id_opt; // (kept for symmetry; coordinator looks up by task_id)
            let executor_id =
                krishiv_proto::ExecutorId::try_new("exec").expect("'exec' is a valid executor id");
            let ids = TaskAttemptRef::new(req.job_id.clone(), stage_id, task_id, attempt_id);
            let assignment = ExecutorTaskAssignment::new(
                ids,
                executor_id,
                self.live_lease.get(),
                PlanFragment::new("checkpoint"),
                OutputContract::new(OutputContractKind::InlineRecordBatches, "checkpoint"),
            );
            if let Err(error) = self
                .initiate_checkpoint_and_deliver_ack(
                    &assignment,
                    req.clone(),
                    state_backend,
                    storage,
                    coordinator,
                )
                .await
            {
                tracing::warn!(task_id = %assignment.task_id(), error = %error, "checkpoint acknowledgement failed");
            }
        }
        Ok(())
    }

    /// Drain all pending barriers from the shared injector and initiate
    /// checkpoints for each one.  Called from the runner loop in `cli.rs`.
    pub async fn drain_pending_barriers<S>(
        &self,
        state_backend: &dyn StateBackend,
        storage: &(impl CheckpointStorage + ?Sized),
        coordinator: &S,
    ) where
        S: CoordinatorExecutorService,
    {
        let Some(ref injector) = self.barrier_injector else {
            return;
        };
        while let Some(barrier) = injector.next_barrier() {
            let Ok(job_id) = JobId::try_new(&barrier.job_id) else {
                continue;
            };
            // Use the most-recently observed coordinator fencing token so the
            // ack is not rejected after a leadership election.  Falls back to
            // FencingToken::initial() only before any real checkpoint request
            // has been received (which is safe: no prior leader exists yet).
            let raw_token = self
                .cached_coordinator_fencing_token
                .load(Ordering::SeqCst);
            let fencing_token = FencingToken::try_new(raw_token.max(1))
                .unwrap_or_else(|_| FencingToken::initial());
            let req = InitiateCheckpointRequest {
                job_id,
                epoch: barrier.epoch,
                fencing_token,
            };
            if let Err(e) = self
                .initiate_checkpoint_for_job(&req, state_backend, storage, coordinator)
                .await
            {
                tracing::warn!(error = %e, "barrier checkpoint failed");
            }
        }
    }
}

pub(crate) fn parse_local_parquet_partitions(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<Vec<LocalParquetPartition>> {
    let mut table_names = BTreeSet::new();
    let mut parsed = Vec::new();
    for partition in partitions {
        let Some(local_partition) = LocalParquetPartition::parse(partition)? else {
            continue;
        };
        if !table_names.insert(local_partition.table_name().to_owned()) {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "duplicate local Parquet table name {} in assigned input partitions",
                    local_partition.table_name()
                ),
            });
        }
        parsed.push(local_partition);
    }
    Ok(parsed)
}
