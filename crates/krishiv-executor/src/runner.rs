//! Task runner types: `TaskRunner`, `ExecutorTaskRunner`, `ExecutorTaskRunReport`,
//! `ExecutorTaskOutput`, `ExecutorTaskOutputKind`, `ShuffleContext`, `LocalParquetPartition`.

use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_checkpoint::{CheckpointStorage, snapshot_path, write_operator_snapshot};
use krishiv_proto::{
    CheckpointAckRequest, CheckpointAckResponse, CheckpointSourceOffset,
    CoordinatorExecutorService, ExecutorTaskAssignment, InitiateCheckpointRequest,
    InputPartitionDescriptor, TaskAttemptRef, TaskId, TaskOutputMetadata, TaskRuntimeStats,
    TaskState, TaskStatusRequest, TaskStatusResponse, TransportDisposition,
};
use krishiv_sql::SqlEngine;
use krishiv_state::StateBackend;

use crate::{
    ExecutorAssignmentInbox, ExecutorError, ExecutorResult,
    fragment::{batch::execute_batch_fragment, streaming::execute_streaming_fragment},
};

pub(crate) const LOCAL_PARQUET_PARTITION_PREFIX: &str = "local-parquet:";
pub(crate) const CONNECTOR_PARQUET_PARTITION_PREFIX: &str = "connector-parquet:";
pub(crate) const OBJECT_PARQUET_PARTITION_PREFIX: &str = "object-parquet:";
pub(crate) const OBJECT_PARQUET_SINK_PREFIX: &str = "object-parquet-sink:";
pub(crate) const MEMORY_KAFKA_PARTITION_PREFIX: &str = "memory-kafka:";
pub(crate) const PARQUET_SINK_PREFIX: &str = "parquet-sink:";
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
        }
    }

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
        storage: &impl CheckpointStorage,
    ) -> CheckpointAckRequest {
        // Stale epoch: return an ack that signals the stale condition via epoch.
        if req.epoch <= self.last_acked_epoch {
            return CheckpointAckRequest {
                job_id: req.job_id,
                operator_id: self.operator_id.clone(),
                task_id: self.task_id.clone(),
                epoch: self.last_acked_epoch, // signal: stale
                fencing_token: req.fencing_token,
                source_offsets: vec![],
                snapshot_path: None,
            };
        }

        // Take a state snapshot (EXE-1: fail-closed — do not ack a new epoch on error).
        let snapshot_bytes = match state_backend.snapshot() {
            Ok(bytes) => bytes,
            Err(krishiv_state::StateError::SnapshotUnsupported { .. }) => Vec::new(),
            Err(_) => {
                return CheckpointAckRequest {
                    job_id: req.job_id,
                    operator_id: self.operator_id.clone(),
                    task_id: self.task_id.clone(),
                    epoch: self.last_acked_epoch,
                    fencing_token: req.fencing_token,
                    source_offsets: vec![],
                    snapshot_path: None,
                };
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
            match write_operator_snapshot(
                storage,
                req.job_id.as_str(),
                req.epoch,
                &self.operator_id,
                self.task_id.as_str(),
                &snapshot_bytes,
            ) {
                Ok(()) => Some(path),
                Err(_) => None,
            }
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

        CheckpointAckRequest {
            job_id: req.job_id,
            operator_id: self.operator_id.clone(),
            task_id: self.task_id.clone(),
            epoch: req.epoch,
            fencing_token: req.fencing_token,
            source_offsets,
            snapshot_path: snap_path,
        }
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
}

impl fmt::Debug for ExecutorTaskRunner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutorTaskRunner")
            .field("inbox", &self.inbox)
            .field("shuffle", &self.shuffle)
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
        }
    }

    /// Wire continuous streaming drain for `stream:continuous:` fragments.
    pub fn with_continuous_drainer(
        mut self,
        drainer: Arc<dyn ContinuousJobDrainer>,
    ) -> Self {
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
                let failed = self
                    .send_task_status(
                        &assignment,
                        TaskState::Failed,
                        "executor failed assignment before DataFusion execution",
                        coordinator,
                        None,
                    )
                    .await?;
                crate::fragment::common::ensure_status_accepted_or_duplicate(
                    failed.disposition(),
                    TaskState::Failed,
                )?;
                return Err(tonic::Status::internal(error.to_string()));
            }
        };

        let terminal = self
            .send_task_status(
                &assignment,
                TaskState::Succeeded,
                "executor completed stage-local fragment",
                coordinator,
                Some(output.to_task_output_metadata()),
            )
            .await?;
        crate::fragment::common::ensure_status_accepted_or_duplicate(
            terminal.disposition(),
            TaskState::Succeeded,
        )?;

        self.clear_running_attempt(&assignment);

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
        message: &'static str,
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
        storage: &impl CheckpointStorage,
        coordinator: &S,
    ) -> Result<CheckpointAckResponse, tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        let mut checkpoint_runner = self
            .checkpoint_runners
            .entry(assignment.task_id().clone())
            .or_insert_with(|| TaskRunner::new(assignment.task_id().clone()))
            .clone();
        let ack = checkpoint_runner.handle_initiate_checkpoint(req, state_backend, storage);
        self.checkpoint_runners
            .insert(assignment.task_id().clone(), checkpoint_runner);
        coordinator
            .checkpoint_ack(tonic::Request::new(ack))
            .await
            .map(tonic::Response::into_inner)
    }

    /// Fan out checkpoint initiation to all known task runners for a job (heartbeat path).
    pub async fn initiate_checkpoint_for_job<S>(
        &self,
        req: &InitiateCheckpointRequest,
        state_backend: &dyn StateBackend,
        storage: &impl CheckpointStorage,
        coordinator: &S,
    ) -> Result<(), tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        use krishiv_proto::{
            ExecutorId, ExecutorTaskAssignment, LeaseGeneration, OutputContract,
            OutputContractKind, PlanFragment, TaskAttemptRef,
        };
        for entry in self.checkpoint_runners.iter() {
            let task_id = entry.key().clone();
            let stage_id = krishiv_proto::StageId::try_new("checkpoint")
                .or_else(|_| krishiv_proto::StageId::try_new("s0"))
                .expect("stage id");
            let ids = TaskAttemptRef::new(
                req.job_id.clone(),
                stage_id,
                task_id,
                krishiv_proto::AttemptId::initial(),
            );
            let assignment = ExecutorTaskAssignment::new(
                ids,
                ExecutorId::try_new("exec-checkpoint").unwrap_or_else(|_| {
                    ExecutorId::try_new("exec").expect("exec id")
                }),
                LeaseGeneration::initial(),
                PlanFragment::new("checkpoint"),
                OutputContract::new(OutputContractKind::InlineRecordBatches, "checkpoint"),
            );
            let _ = self
                .initiate_checkpoint_and_deliver_ack(
                    &assignment,
                    req.clone(),
                    state_backend,
                    storage,
                    coordinator,
                )
                .await;
        }
        Ok(())
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
