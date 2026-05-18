#![forbid(unsafe_code)]

//! R3.1 executor process skeleton.
//!
//! This crate owns executor-side process configuration, versioned
//! coordinator/executor transport requests, and the first networked gRPC client
//! path. The task runner executes the first narrow local SQL fragments through
//! the Krishiv SQL/DataFusion seam and returns lightweight output metadata.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use krishiv_proto::{
    CoordinatorExecutorService, DeregisterExecutorRequest, DeregisterExecutorResponse,
    ExecutorDescriptor, ExecutorHeartbeatRequest, ExecutorHeartbeatResponse, ExecutorId,
    ExecutorState, ExecutorTaskAssignment, ExecutorTaskService, LeaseGeneration,
    RegisterExecutorRequest, RegisterExecutorResponse, TaskAttemptRef, TaskCancellationRequest,
    TaskOutputMetadata, TaskState, TaskStatusRequest, TaskStatusResponse, TransportDisposition,
    TransportVersion, wire,
};
use krishiv_sql::SqlEngine;

/// Executor crate result alias.
pub type ExecutorResult<T> = Result<T, ExecutorError>;

/// Executor transport result alias.
pub type ExecutorTransportResult<T> = Result<T, ExecutorTransportError>;

/// Executor configuration or startup error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorError {
    /// Executor id failed validation.
    InvalidExecutorId { message: String },
    /// Task slots must be greater than zero.
    InvalidSlots,
    /// Coordinator endpoint cannot be empty.
    EmptyCoordinatorEndpoint,
    /// The executor assignment inbox lock was poisoned.
    AssignmentInboxPoisoned,
    /// A received task assignment cannot be executed.
    InvalidAssignment { message: String },
    /// Local stage fragment execution failed.
    LocalExecution { message: String },
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidExecutorId { message } => write!(f, "invalid executor id: {message}"),
            Self::InvalidSlots => f.write_str("task slots must be greater than zero"),
            Self::EmptyCoordinatorEndpoint => f.write_str("coordinator endpoint cannot be empty"),
            Self::AssignmentInboxPoisoned => f.write_str("executor assignment inbox is poisoned"),
            Self::InvalidAssignment { message } => write!(f, "invalid task assignment: {message}"),
            Self::LocalExecution { message } => {
                write!(f, "local stage fragment execution failed: {message}")
            }
        }
    }
}

impl Error for ExecutorError {}

/// In-memory receiver queue for task assignments delivered to an executor.
#[derive(Debug, Clone, Default)]
pub struct ExecutorAssignmentInbox {
    assignments: Arc<RwLock<Vec<ExecutorTaskAssignment>>>,
    cancelled_tasks: Arc<RwLock<BTreeSet<krishiv_proto::TaskId>>>,
}

impl ExecutorAssignmentInbox {
    /// Create an empty assignment inbox.
    pub fn new() -> Self {
        Self::default()
    }

    /// Store one received assignment.
    pub fn push(&self, assignment: ExecutorTaskAssignment) -> ExecutorResult<()> {
        self.assignments
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .push(assignment);
        Ok(())
    }

    /// Remove the next received assignment in FIFO order.
    pub fn pop_next(&self) -> ExecutorResult<Option<ExecutorTaskAssignment>> {
        let mut assignments = self
            .assignments
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?;
        if assignments.is_empty() {
            Ok(None)
        } else {
            Ok(Some(assignments.remove(0)))
        }
    }

    /// Cancel and remove queued assignments for a task id.
    ///
    /// Also marks the task id as cancelled so the runner can skip execution even
    /// if the task has already been popped from the queue.
    pub fn cancel_task(&self, task_id: &krishiv_proto::TaskId) -> ExecutorResult<bool> {
        let mut assignments = self
            .assignments
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?;
        let before = assignments.len();
        assignments.retain(|assignment| assignment.task_id() != task_id);
        let removed = assignments.len() != before;
        drop(assignments);
        self.cancelled_tasks
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .insert(task_id.clone());
        Ok(removed)
    }

    /// Whether a task id has been cancelled.
    pub fn is_task_cancelled(&self, task_id: &krishiv_proto::TaskId) -> ExecutorResult<bool> {
        Ok(self
            .cancelled_tasks
            .read()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .contains(task_id))
    }

    /// Remove a task id from the cancelled set after the runner has handled it.
    pub fn clear_cancelled_task(&self, task_id: &krishiv_proto::TaskId) -> ExecutorResult<()> {
        self.cancelled_tasks
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .remove(task_id);
        Ok(())
    }

    /// Snapshot all received assignments.
    pub fn assignments(&self) -> ExecutorResult<Vec<ExecutorTaskAssignment>> {
        Ok(self
            .assignments
            .read()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .clone())
    }

    /// Number of assignments received so far.
    pub fn len(&self) -> ExecutorResult<usize> {
        Ok(self
            .assignments
            .read()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .len())
    }

    /// Whether the inbox is empty.
    pub fn is_empty(&self) -> ExecutorResult<bool> {
        Ok(self.len()? == 0)
    }
}

const LOCAL_PARQUET_PARTITION_PREFIX: &str = "local-parquet:";
const CONNECTOR_PARQUET_PARTITION_PREFIX: &str = "connector-parquet:";
const OBJECT_PARQUET_PARTITION_PREFIX: &str = "object-parquet:";
const OBJECT_PARQUET_SINK_PREFIX: &str = "object-parquet-sink:";
const MEMORY_KAFKA_PARTITION_PREFIX: &str = "memory-kafka:";
const PARQUET_SINK_PREFIX: &str = "parquet-sink:";
const KAFKA_TO_PARQUET_FRAGMENT: &str = "connector-pipeline:kafka-to-parquet";

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalParquetPartition {
    table_name: String,
    path: PathBuf,
}

impl LocalParquetPartition {
    fn parse(partition: &krishiv_proto::InputPartition) -> ExecutorResult<Self> {
        let descriptor = partition.description().trim();
        let payload = descriptor.strip_prefix(LOCAL_PARQUET_PARTITION_PREFIX).ok_or_else(|| {
            ExecutorError::InvalidAssignment {
                message: format!(
                    "input partition {} must use local-parquet:<table>:<path> for SQL fragments with assigned inputs",
                    partition.partition_id()
                ),
            }
        })?;
        let (table_name, path) =
            payload
                .split_once(':')
                .ok_or_else(|| ExecutorError::InvalidAssignment {
                    message: format!(
                        "input partition {} must use local-parquet:<table>:<path>",
                        partition.partition_id()
                    ),
                })?;
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

        Ok(Self {
            table_name: table_name.to_owned(),
            path: PathBuf::from(path),
        })
    }

    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

/// Result of one executor-side task runner pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorTaskRunReport {
    assignment: ExecutorTaskAssignment,
    output: ExecutorTaskOutput,
    running_disposition: TransportDisposition,
    terminal_disposition: TransportDisposition,
}

impl ExecutorTaskRunReport {
    fn new(
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorTaskOutput {
    kind: ExecutorTaskOutputKind,
    row_count: usize,
    batch_count: usize,
    column_count: usize,
}

impl ExecutorTaskOutput {
    fn sql(row_count: usize, batch_count: usize, column_count: usize) -> Self {
        Self {
            kind: ExecutorTaskOutputKind::Sql,
            row_count,
            batch_count,
            column_count,
        }
    }

    fn connector_pipeline(row_count: usize, batch_count: usize, column_count: usize) -> Self {
        Self {
            kind: ExecutorTaskOutputKind::ConnectorPipeline,
            row_count,
            batch_count,
            column_count,
        }
    }

    fn placeholder() -> Self {
        Self {
            kind: ExecutorTaskOutputKind::Placeholder,
            row_count: 0,
            batch_count: 0,
            column_count: 0,
        }
    }

    fn cancelled() -> Self {
        Self {
            kind: ExecutorTaskOutputKind::Cancelled,
            row_count: 0,
            batch_count: 0,
            column_count: 0,
        }
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
        TaskOutputMetadata::new(
            self.kind.as_str(),
            self.row_count as u64,
            self.batch_count as u64,
            self.column_count as u64,
        )
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
}

impl ExecutorTaskOutputKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::ConnectorPipeline => "connector_pipeline",
            Self::Placeholder => "placeholder",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Minimal R3.1 stage-local task runner skeleton.
#[derive(Debug, Clone)]
pub struct ExecutorTaskRunner {
    inbox: ExecutorAssignmentInbox,
}

impl ExecutorTaskRunner {
    /// Create a runner over an executor assignment inbox.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self { inbox }
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
        ensure_status_accepted_or_duplicate(running.disposition(), TaskState::Running)?;

        // If a CancelTask RPC arrived while this task was queued, finish here
        // instead of starting execution.
        if self
            .inbox
            .is_task_cancelled(assignment.task_id())
            .map_err(|error| tonic::Status::internal(error.to_string()))?
        {
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

        let execute_result = if let Some(timeout_secs) = assignment.task_timeout_secs() {
            match tokio::time::timeout(
                std::time::Duration::from_secs(timeout_secs),
                self.execute_stage_fragment(&assignment),
            )
            .await
            {
                Ok(result) => result,
                Err(_elapsed) => Err(ExecutorError::InvalidAssignment {
                    message: format!("task timed out after {} seconds", timeout_secs),
                }),
            }
        } else {
            self.execute_stage_fragment(&assignment).await
        };

        let output = match execute_result {
            Ok(output) => output,
            Err(error) => {
                let failed = self
                    .send_task_status(
                        &assignment,
                        TaskState::Failed,
                        "executor failed assignment before DataFusion execution",
                        coordinator,
                        None,
                    )
                    .await?;
                ensure_status_accepted_or_duplicate(failed.disposition(), TaskState::Failed)?;
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
        ensure_status_accepted_or_duplicate(terminal.disposition(), TaskState::Succeeded)?;

        Ok(ExecutorTaskRunReport::new(
            assignment,
            output,
            running.disposition(),
            terminal.disposition(),
        ))
    }

    async fn execute_stage_fragment(
        &self,
        assignment: &ExecutorTaskAssignment,
    ) -> ExecutorResult<ExecutorTaskOutput> {
        let fragment = assignment.plan_fragment().description().trim();
        if fragment.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: String::from("plan fragment description cannot be empty"),
            });
        }
        if assignment.output_contract().description().trim().is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: String::from("output contract description cannot be empty"),
            });
        }

        if fragment == KAFKA_TO_PARQUET_FRAGMENT {
            return execute_kafka_to_parquet_pipeline(assignment).await;
        }

        if let Some(query) = sql_query_from_fragment(fragment) {
            let engine = SqlEngine::new();
            for partition in parse_local_parquet_partitions(assignment.input_partitions())? {
                engine
                    .register_parquet(partition.table_name(), partition.path())
                    .await
                    .map_err(|error| ExecutorError::LocalExecution {
                        message: error.to_string(),
                    })?;
            }
            for (table_name, batches) in
                read_connector_parquet_partitions(assignment.input_partitions()).await?
            {
                engine
                    .register_record_batches(&table_name, batches)
                    .await
                    .map_err(|error| ExecutorError::LocalExecution {
                        message: error.to_string(),
                    })?;
            }
            for (table_name, batches) in
                read_object_parquet_partitions(assignment.input_partitions()).await?
            {
                engine
                    .register_record_batches(&table_name, batches)
                    .await
                    .map_err(|error| ExecutorError::LocalExecution {
                        message: error.to_string(),
                    })?;
            }

            let dataframe =
                engine
                    .sql(query)
                    .await
                    .map_err(|error| ExecutorError::LocalExecution {
                        message: error.to_string(),
                    })?;
            let batches =
                dataframe
                    .collect()
                    .await
                    .map_err(|error| ExecutorError::LocalExecution {
                        message: error.to_string(),
                    })?;
            if assignment.output_contract().kind() == krishiv_proto::OutputContractKind::Sink
                && assignment
                    .output_contract()
                    .description()
                    .trim()
                    .starts_with(OBJECT_PARQUET_SINK_PREFIX)
            {
                write_object_parquet_sink(assignment.output_contract().description(), &batches)
                    .await?;
            }
            let row_count = batches.iter().map(|batch| batch.num_rows()).sum();
            let column_count = batches.first().map_or(0, |batch| batch.num_columns());
            return Ok(ExecutorTaskOutput::sql(
                row_count,
                batches.len(),
                column_count,
            ));
        }

        Ok(ExecutorTaskOutput::placeholder())
    }

    async fn send_task_status<S>(
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
}

fn parse_local_parquet_partitions(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<Vec<LocalParquetPartition>> {
    if !partitions.iter().any(|partition| {
        partition
            .description()
            .trim()
            .starts_with(LOCAL_PARQUET_PARTITION_PREFIX)
    }) {
        return Ok(Vec::new());
    }

    let mut table_names = BTreeSet::new();
    let mut parsed = Vec::with_capacity(partitions.len());
    for partition in partitions {
        let local_partition = LocalParquetPartition::parse(partition)?;
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

/// Read all batches from `connector-parquet:<path>` input partitions via `ParquetSource`.
///
/// Returns a list of `(table_name, batches)` pairs — one per `connector-parquet:` partition.
/// The table name is derived from the path's filename stem (without extension).
/// Partitions that do not start with the `connector-parquet:` prefix are skipped.
async fn read_connector_parquet_partitions(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<Vec<(String, Vec<arrow::record_batch::RecordBatch>)>> {
    use krishiv_connectors::{Source, parquet::ParquetSource};

    let mut result = Vec::new();
    for partition in partitions {
        let desc = partition.description().trim();
        let path_str = match desc.strip_prefix(CONNECTOR_PARQUET_PARTITION_PREFIX) {
            Some(p) => p.trim(),
            None => continue,
        };
        if path_str.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "input partition {} has an empty path in connector-parquet descriptor",
                    partition.partition_id()
                ),
            });
        }
        let path = std::path::Path::new(path_str);
        // Derive a table name from the filename stem.
        let table_name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("connector_table")
            .to_owned();

        let mut source = ParquetSource::open(path).map_err(|e| ExecutorError::LocalExecution {
            message: format!("connector-parquet open failed for '{path_str}': {e}"),
        })?;
        let mut batches = Vec::new();
        loop {
            match source
                .read_batch()
                .await
                .map_err(|e| ExecutorError::LocalExecution {
                    message: format!("connector-parquet read failed: {e}"),
                })? {
                Some(batch) => batches.push(batch),
                None => break,
            }
        }
        result.push((table_name, batches));
    }
    Ok(result)
}

/// Read all batches from `object-parquet:<table>:<base_dir>:<object_path>` partitions.
///
/// This is the deterministic S3-compatible executor path for R3: tests use
/// `object_store::local::LocalFileSystem`, while production object-store
/// credentials and provider-specific URLs remain behind the connector boundary.
async fn read_object_parquet_partitions(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<Vec<(String, Vec<arrow::record_batch::RecordBatch>)>> {
    use std::sync::Arc;

    use krishiv_connectors::{Source, s3::S3Source};
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectPath;

    let mut result = Vec::new();
    for partition in partitions {
        let desc = partition.description().trim();
        let Some(payload) = desc.strip_prefix(OBJECT_PARQUET_PARTITION_PREFIX) else {
            continue;
        };
        let (table_name, base_dir, object_path) = parse_object_parquet_descriptor(
            partition.partition_id(),
            payload,
            "object-parquet:<table>:<base_dir>:<object_path>",
        )?;
        let store = Arc::new(
            LocalFileSystem::new_with_prefix(&base_dir).map_err(|error| {
                ExecutorError::LocalExecution {
                    message: format!(
                        "failed to open object store prefix '{}': {error}",
                        base_dir.display()
                    ),
                }
            })?,
        );
        let mut source = S3Source::open(store, ObjectPath::from(object_path.clone()))
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!("object-parquet open failed for '{object_path}': {error}"),
            })?;
        let mut batches = Vec::new();
        while let Some(batch) =
            source
                .read_batch()
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: format!("object-parquet read failed: {error}"),
                })?
        {
            batches.push(batch);
        }
        result.push((table_name, batches));
    }
    Ok(result)
}

async fn write_object_parquet_sink(
    description: &str,
    batches: &[arrow::record_batch::RecordBatch],
) -> ExecutorResult<()> {
    use std::sync::Arc;

    use krishiv_connectors::{Sink, s3::S3Sink};
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectPath;

    let payload = description
        .trim()
        .strip_prefix(OBJECT_PARQUET_SINK_PREFIX)
        .ok_or_else(|| ExecutorError::InvalidAssignment {
            message: format!(
                "object sink must use {OBJECT_PARQUET_SINK_PREFIX}<base_dir>:<object_path>"
            ),
        })?;
    let (base_dir, object_path) =
        payload
            .split_once(':')
            .ok_or_else(|| ExecutorError::InvalidAssignment {
                message: format!(
                    "object sink must use {OBJECT_PARQUET_SINK_PREFIX}<base_dir>:<object_path>"
                ),
            })?;
    let base_dir = base_dir.trim();
    let object_path = object_path.trim();
    if base_dir.is_empty() || object_path.is_empty() {
        return Err(ExecutorError::InvalidAssignment {
            message: String::from("object sink base_dir and object_path cannot be empty"),
        });
    }

    let store = Arc::new(LocalFileSystem::new_with_prefix(base_dir).map_err(|error| {
        ExecutorError::LocalExecution {
            message: format!("failed to open object store prefix '{base_dir}': {error}"),
        }
    })?);
    let mut sink = S3Sink::new(store, ObjectPath::from(object_path));
    for batch in batches {
        sink.write_batch(batch.clone())
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!("object-parquet sink write failed: {error}"),
            })?;
    }
    sink.flush()
        .await
        .map_err(|error| ExecutorError::LocalExecution {
            message: format!("object-parquet sink flush failed: {error}"),
        })
}

fn parse_object_parquet_descriptor(
    partition_id: &str,
    payload: &str,
    expected: &str,
) -> ExecutorResult<(String, PathBuf, String)> {
    let parts: Vec<&str> = payload.splitn(3, ':').collect();
    if parts.len() != 3 {
        return Err(ExecutorError::InvalidAssignment {
            message: format!("input partition {partition_id} must use {expected}"),
        });
    }
    let table_name = parts[0].trim();
    let base_dir = parts[1].trim();
    let object_path = parts[2].trim();
    if table_name.is_empty() || base_dir.is_empty() || object_path.is_empty() {
        return Err(ExecutorError::InvalidAssignment {
            message: format!("input partition {partition_id} has an empty object-parquet field"),
        });
    }
    Ok((
        table_name.to_owned(),
        PathBuf::from(base_dir),
        object_path.to_owned(),
    ))
}

async fn execute_kafka_to_parquet_pipeline(
    assignment: &ExecutorTaskAssignment,
) -> ExecutorResult<ExecutorTaskOutput> {
    use krishiv_connectors::kafka::{
        InMemoryKafkaOffsetCommitter, InMemoryKafkaSource, KafkaOffset,
    };
    use krishiv_connectors::parquet::ParquetSink;
    use krishiv_connectors::{PostWriteOffsetCommitProtocol, Source};

    let (topic, partition, start_offset, batch) =
        parse_memory_kafka_partition(assignment.input_partitions())?;
    let sink_path = parse_parquet_sink_path(assignment.output_contract().description())?;
    let mut source = InMemoryKafkaSource::new(topic, partition, start_offset, vec![batch]);
    let mut sink =
        ParquetSink::create(&sink_path).map_err(|error| ExecutorError::LocalExecution {
            message: format!(
                "parquet sink create failed for '{}': {error}",
                sink_path.display()
            ),
        })?;
    let mut committer = InMemoryKafkaOffsetCommitter::new();

    let mut row_count = 0usize;
    let mut batch_count = 0usize;
    let mut column_count = 0usize;
    while let Some(batch) =
        source
            .read_batch()
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!("memory Kafka source read failed: {error}"),
            })?
    {
        row_count += batch.num_rows();
        batch_count += 1;
        column_count = batch.num_columns();
        let offset = source
            .current_offset()
            .and_then(|offset| offset.downcast::<KafkaOffset>().ok())
            .map(|offset| *offset)
            .ok_or_else(|| ExecutorError::LocalExecution {
                message: String::from("memory Kafka source did not expose a KafkaOffset"),
            })?;

        PostWriteOffsetCommitProtocol::write_flush_commit(&mut sink, &mut committer, batch, offset)
            .await
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!("Kafka-to-Parquet post-write commit failed: {error}"),
            })?;
    }

    if committer.committed_offsets().is_empty() && row_count > 0 {
        return Err(ExecutorError::LocalExecution {
            message: String::from("Kafka-to-Parquet pipeline wrote rows without committing offset"),
        });
    }

    Ok(ExecutorTaskOutput::connector_pipeline(
        row_count,
        batch_count,
        column_count,
    ))
}

fn parse_parquet_sink_path(description: &str) -> ExecutorResult<PathBuf> {
    let path = description
        .trim()
        .strip_prefix(PARQUET_SINK_PREFIX)
        .ok_or_else(|| ExecutorError::InvalidAssignment {
            message: format!(
                "Kafka-to-Parquet output contract must use {PARQUET_SINK_PREFIX}<path>"
            ),
        })?
        .trim();
    if path.is_empty() {
        return Err(ExecutorError::InvalidAssignment {
            message: String::from("Kafka-to-Parquet output path cannot be empty"),
        });
    }
    Ok(PathBuf::from(path))
}

fn parse_memory_kafka_partition(
    partitions: &[krishiv_proto::InputPartition],
) -> ExecutorResult<(String, i32, i64, arrow::record_batch::RecordBatch)> {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    let mut parsed = None;
    for partition in partitions {
        let desc = partition.description().trim();
        let Some(payload) = desc.strip_prefix(MEMORY_KAFKA_PARTITION_PREFIX) else {
            continue;
        };
        if parsed.is_some() {
            return Err(ExecutorError::InvalidAssignment {
                message: String::from(
                    "Kafka-to-Parquet pipeline accepts exactly one memory-kafka partition",
                ),
            });
        }
        let parts: Vec<&str> = payload.splitn(4, ':').collect();
        if parts.len() != 4 {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "input partition {} must use memory-kafka:<topic>:<partition>:<start_offset>:<id=value,...>",
                    partition.partition_id()
                ),
            });
        }
        let topic = parts[0].trim();
        if topic.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: String::from("memory-kafka topic cannot be empty"),
            });
        }
        let kafka_partition =
            parts[1]
                .trim()
                .parse::<i32>()
                .map_err(|error| ExecutorError::InvalidAssignment {
                    message: format!("invalid memory-kafka partition id: {error}"),
                })?;
        let start_offset =
            parts[2]
                .trim()
                .parse::<i64>()
                .map_err(|error| ExecutorError::InvalidAssignment {
                    message: format!("invalid memory-kafka start offset: {error}"),
                })?;
        let records = parts[3].trim();
        if records.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: String::from("memory-kafka records cannot be empty"),
            });
        }

        let mut ids = Vec::new();
        let mut values = Vec::new();
        for record in records.split(',') {
            let (id, value) =
                record
                    .trim()
                    .split_once('=')
                    .ok_or_else(|| ExecutorError::InvalidAssignment {
                        message: format!(
                            "invalid memory-kafka record '{record}', expected id=value"
                        ),
                    })?;
            ids.push(id.trim().parse::<i64>().map_err(|error| {
                ExecutorError::InvalidAssignment {
                    message: format!("invalid memory-kafka record id '{id}': {error}"),
                }
            })?);
            values.push(value.trim().to_owned());
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let value_refs: Vec<&str> = values.iter().map(String::as_str).collect();
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(value_refs)),
            ],
        )
        .map_err(|error| ExecutorError::LocalExecution {
            message: format!("failed to build memory-kafka record batch: {error}"),
        })?;
        parsed = Some((topic.to_owned(), kafka_partition, start_offset, batch));
    }

    parsed.ok_or_else(|| ExecutorError::InvalidAssignment {
        message: format!(
            "Kafka-to-Parquet pipeline requires one {MEMORY_KAFKA_PARTITION_PREFIX}<topic>:<partition>:<start_offset>:<records> input partition"
        ),
    })
}

fn sql_query_from_fragment(fragment: &str) -> Option<&str> {
    let (_, query) = fragment.split_once("sql:")?;
    let query = query.trim();
    (!query.is_empty()).then_some(query)
}

fn ensure_status_accepted_or_duplicate(
    disposition: TransportDisposition,
    state: TaskState,
) -> Result<(), tonic::Status> {
    match disposition {
        TransportDisposition::Accepted | TransportDisposition::Duplicate => Ok(()),
        _ => Err(tonic::Status::failed_precondition(format!(
            "coordinator returned {disposition} for {state} status"
        ))),
    }
}

/// Executor-side task assignment service backed by an in-memory inbox.
#[derive(Debug, Clone)]
pub struct ExecutorTaskInboxService {
    inbox: ExecutorAssignmentInbox,
}

impl ExecutorTaskInboxService {
    /// Create a task assignment service.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self { inbox }
    }

    /// Assignment inbox backing this service.
    pub fn inbox(&self) -> &ExecutorAssignmentInbox {
        &self.inbox
    }
}

#[tonic::async_trait]
impl ExecutorTaskService for ExecutorTaskInboxService {
    async fn assign_task(
        &self,
        request: tonic::Request<ExecutorTaskAssignment>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let assignment = request.into_inner();
        if !TransportVersion::CURRENT.is_compatible_with(assignment.version()) {
            return Err(tonic::Status::invalid_argument(format!(
                "unsupported executor task transport version {}; current version is {}",
                assignment.version(),
                TransportVersion::CURRENT
            )));
        }

        self.inbox
            .push(assignment)
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        Ok(tonic::Response::new(TaskStatusResponse::new(
            TransportDisposition::Accepted,
        )))
    }

    async fn cancel_task(
        &self,
        request: tonic::Request<TaskCancellationRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let request = request.into_inner();
        if !TransportVersion::CURRENT.is_compatible_with(request.version()) {
            return Err(tonic::Status::invalid_argument(format!(
                "unsupported executor task transport version {}; current version is {}",
                request.version(),
                TransportVersion::CURRENT
            )));
        }
        let removed = self
            .inbox
            .cancel_task(request.task_id())
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        let response = if removed {
            TaskStatusResponse::new(TransportDisposition::Accepted)
        } else {
            TaskStatusResponse::new(TransportDisposition::UnknownTask)
                .with_message("task is not queued on this executor")
        };
        Ok(tonic::Response::new(response))
    }
}

/// Networked gRPC adapter for executor-side task assignment calls.
#[derive(Debug, Clone)]
pub struct ExecutorTaskGrpcService {
    inner: ExecutorTaskInboxService,
}

impl ExecutorTaskGrpcService {
    /// Create a networked executor task service.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self {
            inner: ExecutorTaskInboxService::new(inbox),
        }
    }

    /// Assignment inbox backing this service.
    pub fn inbox(&self) -> &ExecutorAssignmentInbox {
        self.inner.inbox()
    }
}

#[tonic::async_trait]
impl wire::v1::executor_task_server::ExecutorTask for ExecutorTaskGrpcService {
    async fn assign_task(
        &self,
        request: tonic::Request<wire::v1::ExecutorTaskAssignment>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        let request = wire::executor_task_assignment_from_wire(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let response = self
            .inner
            .assign_task(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::task_status_response_to_wire(
            response,
        )))
    }

    async fn cancel_task(
        &self,
        request: tonic::Request<wire::v1::TaskCancellationRequest>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        let request = wire::task_cancellation_request_from_wire(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let response = self
            .inner
            .cancel_task(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::task_status_response_to_wire(
            response,
        )))
    }
}

/// Build the generated tonic server around an executor task inbox.
pub fn executor_task_grpc_server(
    inbox: ExecutorAssignmentInbox,
) -> wire::v1::executor_task_server::ExecutorTaskServer<ExecutorTaskGrpcService> {
    wire::v1::executor_task_server::ExecutorTaskServer::new(ExecutorTaskGrpcService::new(inbox))
}

/// Serve the executor task-assignment gRPC API on a socket address.
pub async fn serve_executor_task_grpc(
    addr: SocketAddr,
    inbox: ExecutorAssignmentInbox,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(executor_task_grpc_server(inbox))
        .serve(addr)
        .await
}

/// Serve the executor task-assignment gRPC API on an already-bound listener.
pub async fn serve_executor_task_grpc_with_listener(
    listener: tokio::net::TcpListener,
    inbox: ExecutorAssignmentInbox,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(executor_task_grpc_server(inbox))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await
}

/// Network transport error raised by the executor gRPC client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorTransportError {
    /// The gRPC channel could not be created or used.
    Transport { message: String },
    /// The coordinator returned a gRPC status error.
    Status { message: String },
    /// A protobuf response could not be converted to a Krishiv contract.
    Wire { message: String },
}

impl fmt::Display for ExecutorTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport { message } => write!(f, "executor transport failed: {message}"),
            Self::Status { message } => write!(f, "coordinator rejected transport call: {message}"),
            Self::Wire { message } => write!(f, "invalid coordinator wire response: {message}"),
        }
    }
}

impl Error for ExecutorTransportError {}

impl From<tonic::transport::Error> for ExecutorTransportError {
    fn from(value: tonic::transport::Error) -> Self {
        Self::Transport {
            message: value.to_string(),
        }
    }
}

impl From<tonic::Status> for ExecutorTransportError {
    fn from(value: tonic::Status) -> Self {
        Self::Status {
            message: value.to_string(),
        }
    }
}

impl From<wire::WireError> for ExecutorTransportError {
    fn from(value: wire::WireError) -> Self {
        Self::Wire {
            message: value.to_string(),
        }
    }
}

/// R3.1 executor startup configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorConfig {
    executor_id: ExecutorId,
    host: String,
    slots: usize,
    coordinator_endpoint: String,
    lease_generation: LeaseGeneration,
}

impl ExecutorConfig {
    /// Create executor configuration.
    pub fn new(
        executor_id: impl Into<String>,
        host: impl Into<String>,
        slots: usize,
        coordinator_endpoint: impl Into<String>,
    ) -> ExecutorResult<Self> {
        if slots == 0 {
            return Err(ExecutorError::InvalidSlots);
        }

        let coordinator_endpoint = coordinator_endpoint.into();
        if coordinator_endpoint.trim().is_empty() {
            return Err(ExecutorError::EmptyCoordinatorEndpoint);
        }

        let executor_id =
            ExecutorId::try_new(executor_id).map_err(|error| ExecutorError::InvalidExecutorId {
                message: error.to_string(),
            })?;

        Ok(Self {
            executor_id,
            host: host.into(),
            slots,
            coordinator_endpoint,
            lease_generation: LeaseGeneration::initial(),
        })
    }

    /// Executor id.
    pub fn executor_id(&self) -> &ExecutorId {
        &self.executor_id
    }

    /// Host or pod name advertised by the executor.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Advertised task slots.
    pub fn slots(&self) -> usize {
        self.slots
    }

    /// Coordinator endpoint the executor will connect to in a later R3.1 slice.
    pub fn coordinator_endpoint(&self) -> &str {
        &self.coordinator_endpoint
    }

    /// Current executor lease generation.
    pub fn lease_generation(&self) -> LeaseGeneration {
        self.lease_generation
    }

    /// Build an executor descriptor for registration.
    pub fn descriptor(&self) -> ExecutorDescriptor {
        ExecutorDescriptor::new(self.executor_id.clone(), self.host.clone(), self.slots)
    }
}

/// Minimal executor runtime facade for the R3.1 bootstrap slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorRuntime {
    config: ExecutorConfig,
}

impl ExecutorRuntime {
    /// Create an executor runtime.
    pub fn new(config: ExecutorConfig) -> Self {
        Self { config }
    }

    /// Runtime configuration.
    pub fn config(&self) -> &ExecutorConfig {
        &self.config
    }

    /// Build the versioned registration request this executor will send.
    pub fn registration_request(&self) -> RegisterExecutorRequest {
        RegisterExecutorRequest::new(self.config.descriptor())
    }

    /// Register this executor through a tonic-shaped coordinator service.
    pub async fn register_with<S>(
        &self,
        service: &S,
    ) -> Result<RegisterExecutorResponse, tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        service
            .register_executor(tonic::Request::new(self.registration_request()))
            .await
            .map(tonic::Response::into_inner)
    }

    /// Build a deregistration request for this executor.
    pub fn deregistration_request(&self) -> DeregisterExecutorRequest {
        DeregisterExecutorRequest::new(
            self.config.executor_id.clone(),
            self.config.lease_generation,
        )
        .with_reason("executor graceful shutdown")
    }

    /// Deregister this executor through a tonic-shaped coordinator service.
    pub async fn deregister_with<S>(
        &self,
        service: &S,
    ) -> Result<DeregisterExecutorResponse, tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        service
            .deregister_executor(tonic::Request::new(self.deregistration_request()))
            .await
            .map(tonic::Response::into_inner)
    }

    /// Build an empty healthy heartbeat request for this executor.
    pub fn heartbeat_request(&self) -> ExecutorHeartbeatRequest {
        ExecutorHeartbeatRequest::new(
            self.config.executor_id.clone(),
            self.config.lease_generation,
            ExecutorState::Healthy,
        )
    }

    /// Send a heartbeat through a tonic-shaped coordinator service.
    pub async fn heartbeat_with<S>(
        &self,
        service: &S,
    ) -> Result<ExecutorHeartbeatResponse, tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        service
            .executor_heartbeat(tonic::Request::new(self.heartbeat_request()))
            .await
            .map(tonic::Response::into_inner)
    }

    /// Register this executor through a networked coordinator gRPC endpoint.
    pub async fn register_with_grpc_endpoint(
        &self,
    ) -> ExecutorTransportResult<RegisterExecutorResponse> {
        let mut client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
            self.config.coordinator_endpoint.clone(),
        )
        .await?;
        let request = wire::register_executor_request_to_wire(self.registration_request());
        let response = client.register_executor(request).await?.into_inner();
        Ok(wire::register_executor_response_from_wire(response)?)
    }

    /// Deregister this executor through a networked coordinator gRPC endpoint.
    pub async fn deregister_with_grpc_endpoint(
        &self,
    ) -> ExecutorTransportResult<DeregisterExecutorResponse> {
        let mut client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
            self.config.coordinator_endpoint.clone(),
        )
        .await?;
        let request = wire::deregister_executor_request_to_wire(self.deregistration_request());
        let response = client.deregister_executor(request).await?.into_inner();
        Ok(wire::deregister_executor_response_from_wire(response)?)
    }

    /// Send one healthy heartbeat through a networked coordinator gRPC endpoint.
    pub async fn heartbeat_with_grpc_endpoint(
        &self,
    ) -> ExecutorTransportResult<ExecutorHeartbeatResponse> {
        let mut client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
            self.config.coordinator_endpoint.clone(),
        )
        .await?;
        let request = wire::executor_heartbeat_request_to_wire(self.heartbeat_request());
        let response = client.executor_heartbeat(request).await?.into_inner();
        Ok(wire::executor_heartbeat_response_from_wire(response)?)
    }

    /// Register once and immediately send one heartbeat over gRPC.
    pub async fn register_and_heartbeat_once(
        &self,
    ) -> ExecutorTransportResult<(RegisterExecutorResponse, ExecutorHeartbeatResponse)> {
        let mut client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
            self.config.coordinator_endpoint.clone(),
        )
        .await?;

        let registration = client
            .register_executor(wire::register_executor_request_to_wire(
                self.registration_request(),
            ))
            .await?
            .into_inner();
        let registration = wire::register_executor_response_from_wire(registration)?;

        let heartbeat = client
            .executor_heartbeat(wire::executor_heartbeat_request_to_wire(
                self.heartbeat_request(),
            ))
            .await?
            .into_inner();
        let heartbeat = wire::executor_heartbeat_response_from_wire(heartbeat)?;

        Ok((registration, heartbeat))
    }

    /// Human-readable startup summary for the binary.
    pub fn startup_summary(&self) -> String {
        format!(
            "Krishiv executor {} ready for transport {} at {} with {} slot(s)",
            self.config.executor_id(),
            TransportVersion::CURRENT,
            self.config.coordinator_endpoint(),
            self.config.slots()
        )
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use tempfile::tempdir;

    use krishiv_proto::{
        AttemptId, CoordinatorExecutorService, CoordinatorId, DeregisterExecutorRequest,
        DeregisterExecutorResponse, ExecutorHeartbeatRequest, ExecutorHeartbeatResponse,
        ExecutorId, ExecutorState, ExecutorTaskAssignment, ExecutorTaskService, InputPartition,
        JobId, JobKind, JobSpec, JobState, LeaseGeneration, OutputContract, OutputContractKind,
        PlanFragment, RegisterExecutorRequest, RegisterExecutorResponse, StageId, StageSpec,
        TaskAttemptRef, TaskCancellationRequest, TaskId, TaskSpec, TaskStatusRequest,
        TaskStatusResponse, TransportDisposition, TransportVersion, wire,
    };
    use krishiv_scheduler::{
        Coordinator, CoordinatorExecutorTonicService, SharedCoordinator,
        serve_coordinator_executor_grpc_with_listener,
    };

    use super::{
        ExecutorAssignmentInbox, ExecutorConfig, ExecutorError, ExecutorRuntime,
        ExecutorTaskInboxService, ExecutorTaskOutputKind, ExecutorTaskRunner,
        serve_executor_task_grpc_with_listener,
    };

    struct AcceptingCoordinatorService;

    #[tonic::async_trait]
    impl CoordinatorExecutorService for AcceptingCoordinatorService {
        async fn register_executor(
            &self,
            request: tonic::Request<RegisterExecutorRequest>,
        ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
            let request = request.into_inner();
            Ok(tonic::Response::new(RegisterExecutorResponse::new(
                request.descriptor().executor_id().clone(),
                LeaseGeneration::initial(),
                TransportDisposition::Accepted,
            )))
        }

        async fn deregister_executor(
            &self,
            request: tonic::Request<DeregisterExecutorRequest>,
        ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status> {
            let request = request.into_inner();
            Ok(tonic::Response::new(DeregisterExecutorResponse::new(
                request.executor_id().clone(),
                request.lease_generation(),
                TransportDisposition::Accepted,
            )))
        }

        async fn executor_heartbeat(
            &self,
            request: tonic::Request<ExecutorHeartbeatRequest>,
        ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status> {
            Ok(tonic::Response::new(ExecutorHeartbeatResponse::new(
                request.into_inner().lease_generation(),
                TransportDisposition::Accepted,
            )))
        }

        async fn task_status(
            &self,
            _request: tonic::Request<TaskStatusRequest>,
        ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
            Ok(tonic::Response::new(TaskStatusResponse::new(
                TransportDisposition::Accepted,
            )))
        }
    }

    #[derive(Debug, Clone)]
    struct NetworkCoordinatorService {
        endpoint: String,
    }

    impl NetworkCoordinatorService {
        fn new(endpoint: impl Into<String>) -> Self {
            Self {
                endpoint: endpoint.into(),
            }
        }
    }

    #[tonic::async_trait]
    impl CoordinatorExecutorService for NetworkCoordinatorService {
        async fn register_executor(
            &self,
            request: tonic::Request<RegisterExecutorRequest>,
        ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
            let mut client =
                wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
                    self.endpoint.clone(),
                )
                .await
                .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
            let response = client
                .register_executor(wire::register_executor_request_to_wire(
                    request.into_inner(),
                ))
                .await?
                .into_inner();
            let response = wire::register_executor_response_from_wire(response)
                .map_err(|error| tonic::Status::internal(error.to_string()))?;
            Ok(tonic::Response::new(response))
        }

        async fn deregister_executor(
            &self,
            request: tonic::Request<DeregisterExecutorRequest>,
        ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status> {
            let mut client =
                wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
                    self.endpoint.clone(),
                )
                .await
                .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
            let response = client
                .deregister_executor(wire::deregister_executor_request_to_wire(
                    request.into_inner(),
                ))
                .await?
                .into_inner();
            let response = wire::deregister_executor_response_from_wire(response)
                .map_err(|error| tonic::Status::internal(error.to_string()))?;
            Ok(tonic::Response::new(response))
        }

        async fn executor_heartbeat(
            &self,
            request: tonic::Request<ExecutorHeartbeatRequest>,
        ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status> {
            let mut client =
                wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
                    self.endpoint.clone(),
                )
                .await
                .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
            let response = client
                .executor_heartbeat(wire::executor_heartbeat_request_to_wire(
                    request.into_inner(),
                ))
                .await?
                .into_inner();
            let response = wire::executor_heartbeat_response_from_wire(response)
                .map_err(|error| tonic::Status::internal(error.to_string()))?;
            Ok(tonic::Response::new(response))
        }

        async fn task_status(
            &self,
            request: tonic::Request<TaskStatusRequest>,
        ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
            let mut client =
                wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
                    self.endpoint.clone(),
                )
                .await
                .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
            let response = client
                .task_status(wire::task_status_request_to_wire(request.into_inner()))
                .await?
                .into_inner();
            let response = wire::task_status_response_from_wire(response)
                .map_err(|error| tonic::Status::internal(error.to_string()))?;
            Ok(tonic::Response::new(response))
        }
    }

    #[test]
    fn config_rejects_invalid_values() {
        assert!(matches!(
            ExecutorConfig::new("exec-1", "host", 0, "http://coordinator"),
            Err(ExecutorError::InvalidSlots)
        ));
        assert!(matches!(
            ExecutorConfig::new("exec-1", "host", 1, " "),
            Err(ExecutorError::EmptyCoordinatorEndpoint)
        ));
    }

    #[test]
    fn runtime_builds_versioned_registration_request() {
        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-1", "pod-a", 2, "http://coordinator").unwrap(),
        );
        let request = runtime.registration_request();

        assert_eq!(request.version(), TransportVersion::CURRENT);
        assert_eq!(request.descriptor().executor_id().as_str(), "exec-1");
        assert_eq!(request.descriptor().slots(), 2);
    }

    #[test]
    fn runtime_builds_heartbeat_with_initial_lease() {
        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-1", "pod-a", 1, "http://coordinator").unwrap(),
        );
        let heartbeat = runtime.heartbeat_request();

        assert_eq!(heartbeat.state(), ExecutorState::Healthy);
        assert_eq!(heartbeat.lease_generation(), LeaseGeneration::initial());
        assert!(heartbeat.running_attempts().is_empty());
    }

    #[tokio::test]
    async fn runtime_registers_and_heartbeats_through_service_boundary() {
        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-1", "pod-a", 1, "http://coordinator").unwrap(),
        );
        let service = AcceptingCoordinatorService;

        let registration = runtime.register_with(&service).await.unwrap();
        let heartbeat = runtime.heartbeat_with(&service).await.unwrap();

        assert_eq!(registration.disposition(), TransportDisposition::Accepted);
        assert_eq!(heartbeat.disposition(), TransportDisposition::Accepted);
    }

    #[tokio::test]
    async fn deregister_via_grpc_endpoint_transitions_executor_to_removed() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-dereg-exec").unwrap(),
        ));
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping gRPC deregister test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind test gRPC listener: {error}"),
        };
        let addr = listener.local_addr().unwrap();
        let server_shared = shared.clone();
        let server = tokio::spawn(async move {
            serve_coordinator_executor_grpc_with_listener(listener, server_shared)
                .await
                .unwrap();
        });

        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-dereg-test", "pod-dereg", 1, format!("http://{addr}"))
                .unwrap(),
        );

        runtime.register_with_grpc_endpoint().await.unwrap();
        let dereg = runtime.deregister_with_grpc_endpoint().await.unwrap();
        assert_eq!(dereg.disposition(), TransportDisposition::Accepted);

        {
            let coordinator = shared.read().unwrap();
            let snapshot = coordinator
                .executor_snapshots()
                .into_iter()
                .find(|s| s.executor_id().as_str() == "exec-dereg-test")
                .expect("executor should still be in registry after deregister");
            assert_eq!(snapshot.state(), ExecutorState::Removed);
        }

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn task_runner_reports_cancelled_when_inbox_cancel_received() {
        let inbox = ExecutorAssignmentInbox::new();
        let runner = ExecutorTaskRunner::new(inbox.clone());

        let assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-cancel").unwrap(),
                StageId::try_new("stage-1").unwrap(),
                TaskId::try_new("task-cancel-1").unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("sql: select 1"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline"),
        );

        inbox.cancel_task(assignment.task_id()).unwrap();
        assert!(inbox.is_task_cancelled(assignment.task_id()).unwrap());

        let service = AcceptingCoordinatorService;
        let report = runner
            .run_assignment_with(assignment, &service)
            .await
            .unwrap();

        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Cancelled);
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );
    }

    #[tokio::test]
    async fn task_inbox_service_accepts_assignment() {
        let inbox = ExecutorAssignmentInbox::new();
        let service = ExecutorTaskInboxService::new(inbox.clone());

        let response = service
            .assign_task(tonic::Request::new(demo_assignment("task-1")))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::Accepted);
        assert_eq!(inbox.len().unwrap(), 1);
        let assignments = inbox.assignments().unwrap();
        assert_eq!(assignments[0].task_id().as_str(), "task-1");
        assert_eq!(
            assignments[0].lease_generation(),
            LeaseGeneration::initial()
        );
    }

    #[tokio::test]
    async fn task_assignment_flows_over_network_to_executor_inbox() {
        let inbox = ExecutorAssignmentInbox::new();
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping executor task gRPC test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind executor task listener: {error}"),
        };
        let addr = listener.local_addr().unwrap();
        let server_inbox = inbox.clone();
        let server = tokio::spawn(async move {
            serve_executor_task_grpc_with_listener(listener, server_inbox)
                .await
                .unwrap();
        });

        let mut client =
            wire::v1::executor_task_client::ExecutorTaskClient::connect(format!("http://{addr}"))
                .await
                .unwrap();
        let response = client
            .assign_task(wire::executor_task_assignment_to_wire(demo_assignment(
                "task-network-1",
            )))
            .await
            .unwrap()
            .into_inner();
        let response = wire::task_status_response_from_wire(response).unwrap();

        assert_eq!(response.disposition(), TransportDisposition::Accepted);
        assert_eq!(inbox.len().unwrap(), 1);
        assert_eq!(
            inbox.assignments().unwrap()[0].task_id().as_str(),
            "task-network-1"
        );

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn task_runner_reports_running_and_success_to_scheduler() {
        let executor_id = ExecutorId::try_new("exec-runner-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let inbox = ExecutorAssignmentInbox::new();
        let job_id = JobId::try_new("job-runner-1").unwrap();

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-runner",
                    1,
                ))
                .unwrap();
            coordinator
                .submit_job(single_task_job(job_id.clone()))
                .unwrap();
            let mut assignments = coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap();
            inbox.push(assignments.remove(0)).unwrap();
        }

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&service).await.unwrap().unwrap();

        assert_eq!(report.assignment().job_id(), &job_id);
        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
        assert_eq!(report.output().row_count(), 1);
        assert_eq!(report.output().batch_count(), 1);
        assert_eq!(report.output().column_count(), 1);
        assert!(matches!(
            report.running_disposition(),
            TransportDisposition::Accepted | TransportDisposition::Duplicate
        ));
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );
        assert!(inbox.is_empty().unwrap());

        let coordinator = shared.read().unwrap();
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
        assert_eq!(snapshot.succeeded_task_count(), 1);
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let metadata = detail.stages()[0].tasks()[0].output_metadata().unwrap();
        assert_eq!(metadata.output_kind(), "sql");
        assert_eq!(metadata.row_count(), 1);
    }

    #[tokio::test]
    async fn runtime_deregisters_through_service_boundary() {
        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-1", "pod-a", 1, "http://coordinator").unwrap(),
        );
        let response = runtime
            .deregister_with(&AcceptingCoordinatorService)
            .await
            .unwrap();

        assert_eq!(response.executor_id(), runtime.config().executor_id());
        assert_eq!(response.disposition(), TransportDisposition::Accepted);
    }

    #[tokio::test]
    async fn task_inbox_service_cancels_queued_assignment() {
        let inbox = ExecutorAssignmentInbox::new();
        let service = ExecutorTaskInboxService::new(inbox.clone());
        let assignment = demo_assignment("task-cancel-1");
        let cancel = TaskCancellationRequest::new(TaskAttemptRef::new(
            assignment.job_id().clone(),
            assignment.stage_id().clone(),
            assignment.task_id().clone(),
            assignment.attempt_id(),
        ));

        service
            .assign_task(tonic::Request::new(assignment))
            .await
            .unwrap();
        let response = service
            .cancel_task(tonic::Request::new(cancel))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::Accepted);
        assert!(inbox.is_empty().unwrap());
    }

    #[test]
    fn local_parquet_partition_descriptors_are_validated() {
        let partition = InputPartition::new("part-1", "local-parquet:people:/tmp/people.parquet");
        let parsed = super::parse_local_parquet_partitions(&[partition]).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].table_name(), "people");
        assert_eq!(
            parsed[0].path(),
            std::path::Path::new("/tmp/people.parquet")
        );

        let duplicate = super::parse_local_parquet_partitions(&[
            InputPartition::new("part-1", "local-parquet:people:/tmp/people-1.parquet"),
            InputPartition::new("part-2", "local-parquet:people:/tmp/people-2.parquet"),
        ])
        .unwrap_err();
        assert!(
            duplicate
                .to_string()
                .contains("duplicate local Parquet table name")
        );

        let malformed = super::parse_local_parquet_partitions(&[
            InputPartition::new("part-1", "local-parquet:people:/tmp/people.parquet"),
            InputPartition::new("part-2", "not-a-local-parquet-descriptor"),
        ])
        .unwrap_err();
        assert!(
            malformed
                .to_string()
                .contains("local-parquet:<table>:<path>")
        );
    }

    #[tokio::test]
    async fn task_runner_executes_local_parquet_partition_sql() {
        let temp = tempdir().unwrap();
        let parquet_path = temp.path().join("people.parquet");
        write_people_parquet(&parquet_path);

        let executor_id = ExecutorId::try_new("exec-parquet-runner-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-parquet-runner-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let inbox = ExecutorAssignmentInbox::new();
        let job_id = JobId::try_new("job-parquet-runner-1").unwrap();

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-parquet-runner",
                    1,
                ))
                .unwrap();
            coordinator
                .submit_job(parquet_scan_job(job_id.clone()))
                .unwrap();
            let launched = coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap()
                .remove(0);
            inbox
                .push(local_parquet_assignment(launched, &parquet_path))
                .unwrap();
        }

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&service).await.unwrap().unwrap();

        assert_eq!(report.assignment().job_id(), &job_id);
        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
        assert_eq!(report.output().row_count(), 2);
        assert_eq!(report.output().batch_count(), 1);
        assert_eq!(report.output().column_count(), 2);
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );
        assert!(inbox.is_empty().unwrap());

        let coordinator = shared.read().unwrap();
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
        assert_eq!(snapshot.succeeded_task_count(), 1);
    }

    #[tokio::test]
    async fn select_one_assignment_flows_over_grpc_and_reports_output_metadata() {
        let executor_id = ExecutorId::try_new("exec-network-runner-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-network-runner-1").unwrap(),
        ));
        let coordinator_listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping coordinator gRPC test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind coordinator gRPC listener: {error}"),
        };
        let coordinator_addr = coordinator_listener.local_addr().unwrap();
        let coordinator_shared = shared.clone();
        let coordinator_server = tokio::spawn(async move {
            serve_coordinator_executor_grpc_with_listener(coordinator_listener, coordinator_shared)
                .await
                .unwrap();
        });

        let inbox = ExecutorAssignmentInbox::new();
        let executor_listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping executor task gRPC test because loopback sockets are denied");
                coordinator_server.abort();
                let _ = coordinator_server.await;
                return;
            }
            Err(error) => panic!("failed to bind executor task gRPC listener: {error}"),
        };
        let executor_addr = executor_listener.local_addr().unwrap();
        let executor_inbox = inbox.clone();
        let executor_server = tokio::spawn(async move {
            serve_executor_task_grpc_with_listener(executor_listener, executor_inbox)
                .await
                .unwrap();
        });

        let coordinator = NetworkCoordinatorService::new(format!("http://{coordinator_addr}"));
        let registration = coordinator
            .register_executor(tonic::Request::new(RegisterExecutorRequest::new(
                krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-network-runner",
                    1,
                ),
            )))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(registration.disposition(), TransportDisposition::Accepted);
        let heartbeat = coordinator
            .executor_heartbeat(tonic::Request::new(ExecutorHeartbeatRequest::new(
                executor_id.clone(),
                registration.lease_generation(),
                ExecutorState::Healthy,
            )))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(heartbeat.disposition(), TransportDisposition::Accepted);

        let job_id = JobId::try_new("job-network-runner-1").unwrap();
        let assignment = {
            let mut scheduler = shared.write().unwrap();
            scheduler
                .submit_job(single_task_job(job_id.clone()))
                .unwrap();
            scheduler
                .launch_assigned_task_assignments(&job_id)
                .unwrap()
                .remove(0)
        };

        let mut executor_client = wire::v1::executor_task_client::ExecutorTaskClient::connect(
            format!("http://{executor_addr}"),
        )
        .await
        .unwrap();
        let assign_response = executor_client
            .assign_task(wire::executor_task_assignment_to_wire(assignment))
            .await
            .unwrap()
            .into_inner();
        let assign_response = wire::task_status_response_from_wire(assign_response).unwrap();
        assert_eq!(
            assign_response.disposition(),
            TransportDisposition::Accepted
        );

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&coordinator).await.unwrap().unwrap();

        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
        assert_eq!(report.output().row_count(), 1);
        assert_eq!(report.output().batch_count(), 1);
        assert_eq!(report.output().column_count(), 1);
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );
        assert!(inbox.is_empty().unwrap());

        {
            let scheduler = shared.read().unwrap();
            let snapshot = scheduler.job_snapshot(&job_id).unwrap();
            assert_eq!(snapshot.state(), JobState::Succeeded);
            assert_eq!(snapshot.succeeded_task_count(), 1);
        }

        executor_server.abort();
        let _ = executor_server.await;
        coordinator_server.abort();
        let _ = coordinator_server.await;
    }

    #[tokio::test]
    async fn local_parquet_assignment_flows_over_grpc_and_reports_output_metadata() {
        let temp = tempdir().unwrap();
        let parquet_path = temp.path().join("people.parquet");
        write_people_parquet(&parquet_path);

        let executor_id = ExecutorId::try_new("exec-network-parquet-runner-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-network-parquet-runner-1").unwrap(),
        ));
        let coordinator_listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping coordinator gRPC test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind coordinator gRPC listener: {error}"),
        };
        let coordinator_addr = coordinator_listener.local_addr().unwrap();
        let coordinator_shared = shared.clone();
        let coordinator_server = tokio::spawn(async move {
            serve_coordinator_executor_grpc_with_listener(coordinator_listener, coordinator_shared)
                .await
                .unwrap();
        });

        let inbox = ExecutorAssignmentInbox::new();
        let executor_listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping executor task gRPC test because loopback sockets are denied");
                coordinator_server.abort();
                let _ = coordinator_server.await;
                return;
            }
            Err(error) => panic!("failed to bind executor task gRPC listener: {error}"),
        };
        let executor_addr = executor_listener.local_addr().unwrap();
        let executor_inbox = inbox.clone();
        let executor_server = tokio::spawn(async move {
            serve_executor_task_grpc_with_listener(executor_listener, executor_inbox)
                .await
                .unwrap();
        });

        let coordinator = NetworkCoordinatorService::new(format!("http://{coordinator_addr}"));
        let registration = coordinator
            .register_executor(tonic::Request::new(RegisterExecutorRequest::new(
                krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-network-parquet-runner",
                    1,
                ),
            )))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(registration.disposition(), TransportDisposition::Accepted);
        let heartbeat = coordinator
            .executor_heartbeat(tonic::Request::new(ExecutorHeartbeatRequest::new(
                executor_id.clone(),
                registration.lease_generation(),
                ExecutorState::Healthy,
            )))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(heartbeat.disposition(), TransportDisposition::Accepted);

        let job_id = JobId::try_new("job-network-parquet-runner-1").unwrap();
        let assignment = {
            let mut scheduler = shared.write().unwrap();
            scheduler
                .submit_job(parquet_scan_job(job_id.clone()))
                .unwrap();
            let launched = scheduler
                .launch_assigned_task_assignments(&job_id)
                .unwrap()
                .remove(0);
            local_parquet_assignment(launched, &parquet_path)
        };

        let mut executor_client = wire::v1::executor_task_client::ExecutorTaskClient::connect(
            format!("http://{executor_addr}"),
        )
        .await
        .unwrap();
        let assign_response = executor_client
            .assign_task(wire::executor_task_assignment_to_wire(assignment))
            .await
            .unwrap()
            .into_inner();
        let assign_response = wire::task_status_response_from_wire(assign_response).unwrap();
        assert_eq!(
            assign_response.disposition(),
            TransportDisposition::Accepted
        );

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&coordinator).await.unwrap().unwrap();

        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
        assert_eq!(report.output().row_count(), 2);
        assert_eq!(report.output().batch_count(), 1);
        assert_eq!(report.output().column_count(), 2);
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );
        assert!(inbox.is_empty().unwrap());

        {
            let scheduler = shared.read().unwrap();
            let snapshot = scheduler.job_snapshot(&job_id).unwrap();
            assert_eq!(snapshot.state(), JobState::Succeeded);
            assert_eq!(snapshot.succeeded_task_count(), 1);
        }

        executor_server.abort();
        let _ = executor_server.await;
        coordinator_server.abort();
        let _ = coordinator_server.await;
    }

    fn demo_assignment(task_id: &str) -> ExecutorTaskAssignment {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new(task_id).unwrap(),
            AttemptId::initial(),
        );

        ExecutorTaskAssignment::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("scan parquet partition"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline result"),
        )
        .with_input_partitions(vec![InputPartition::new("part-1", "first split")])
    }

    fn single_task_job(job_id: JobId) -> JobSpec {
        JobSpec::new(job_id, "runner smoke", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "single stage").with_task(
                TaskSpec::new(TaskId::try_new("task-1").unwrap(), "sql: select 1 as value"),
            ),
        )
    }

    fn parquet_scan_job(job_id: JobId) -> JobSpec {
        JobSpec::new(job_id, "parquet runner smoke", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "single stage").with_task(
                TaskSpec::new(
                    TaskId::try_new("task-1").unwrap(),
                    "sql: select id, name from people where id > 1 order by id",
                ),
            ),
        )
    }

    fn local_parquet_assignment(
        launched: ExecutorTaskAssignment,
        parquet_path: &std::path::Path,
    ) -> ExecutorTaskAssignment {
        ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                launched.job_id().clone(),
                launched.stage_id().clone(),
                launched.task_id().clone(),
                launched.attempt_id(),
            ),
            launched.executor_id().clone(),
            launched.lease_generation(),
            PlanFragment::new("sql: select id, name from people where id > 1 order by id"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline result"),
        )
        .with_input_partitions(vec![InputPartition::new(
            "people-part-1",
            format!("local-parquet:people:{}", parquet_path.display()),
        )])
    }

    fn write_people_parquet(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["ada", "grace", "katherine"])),
            ],
        )
        .unwrap_or_else(|error| panic!("unexpected record batch error: {error}"));
        let file = File::create(path)
            .unwrap_or_else(|error| panic!("unexpected parquet file error: {error}"));
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .unwrap_or_else(|error| panic!("unexpected parquet writer error: {error}"));
        writer
            .write(&batch)
            .unwrap_or_else(|error| panic!("unexpected parquet write error: {error}"));
        writer
            .close()
            .unwrap_or_else(|error| panic!("unexpected parquet close error: {error}"));
    }

    #[tokio::test]
    async fn executor_runs_parquet_task_via_connector_source() {
        let temp = tempdir().unwrap();
        let parquet_path = temp.path().join("people.parquet");
        write_people_parquet(&parquet_path);

        let executor_id = ExecutorId::try_new("exec-connector-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-connector-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let inbox = ExecutorAssignmentInbox::new();
        let job_id = JobId::try_new("job-connector-1").unwrap();

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-connector",
                    1,
                ))
                .unwrap();
            coordinator
                .submit_job(parquet_scan_job(job_id.clone()))
                .unwrap();
            let launched = coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap()
                .remove(0);

            // Use connector-parquet: prefix instead of local-parquet:
            let assignment = ExecutorTaskAssignment::new(
                TaskAttemptRef::new(
                    launched.job_id().clone(),
                    launched.stage_id().clone(),
                    launched.task_id().clone(),
                    launched.attempt_id(),
                ),
                launched.executor_id().clone(),
                launched.lease_generation(),
                PlanFragment::new("sql: select id, name from people where id > 1 order by id"),
                OutputContract::new(OutputContractKind::InlineRecordBatches, "inline result"),
            )
            .with_input_partitions(vec![InputPartition::new(
                "people-connector-part-1",
                format!("connector-parquet:{}", parquet_path.display()),
            )]);
            inbox.push(assignment).unwrap();
        }

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&service).await.unwrap().unwrap();

        assert_eq!(report.assignment().job_id(), &job_id);
        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
        assert_eq!(report.output().row_count(), 2, "expected 2 rows (id > 1)");
        assert_eq!(report.output().batch_count(), 1);
        assert_eq!(report.output().column_count(), 2);
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );
        assert!(inbox.is_empty().unwrap());

        let coordinator = shared.read().unwrap();
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
        assert_eq!(snapshot.succeeded_task_count(), 1);
    }

    #[tokio::test]
    async fn executor_reads_object_parquet_source_and_writes_object_sink() {
        use krishiv_connectors::Source;
        use krishiv_connectors::parquet::ParquetSource;

        let temp = tempdir().unwrap();
        let object_root = temp.path().join("object-store");
        std::fs::create_dir_all(&object_root).unwrap();
        let input_path = object_root.join("input/people.parquet");
        std::fs::create_dir_all(input_path.parent().unwrap()).unwrap();
        write_people_parquet(&input_path);

        let executor_id = ExecutorId::try_new("exec-object-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-object-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let inbox = ExecutorAssignmentInbox::new();
        let job_id = JobId::try_new("job-object-1").unwrap();

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-object",
                    1,
                ))
                .unwrap();
            coordinator
                .submit_job(parquet_scan_job(job_id.clone()))
                .unwrap();
            let launched = coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap()
                .remove(0);

            let assignment = ExecutorTaskAssignment::new(
                TaskAttemptRef::new(
                    launched.job_id().clone(),
                    launched.stage_id().clone(),
                    launched.task_id().clone(),
                    launched.attempt_id(),
                ),
                launched.executor_id().clone(),
                launched.lease_generation(),
                PlanFragment::new("sql: select id, name from people where id > 1 order by id"),
                OutputContract::new(
                    OutputContractKind::Sink,
                    format!(
                        "object-parquet-sink:{}:output/filtered.parquet",
                        object_root.display()
                    ),
                ),
            )
            .with_input_partitions(vec![InputPartition::new(
                "people-object-part-1",
                format!(
                    "object-parquet:people:{}:input/people.parquet",
                    object_root.display()
                ),
            )]);
            inbox.push(assignment).unwrap();
        }

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&service).await.unwrap().unwrap();
        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
        assert_eq!(report.output().row_count(), 2);
        assert_eq!(report.output().column_count(), 2);

        let output_path = object_root.join("output/filtered.parquet");
        let mut source = ParquetSource::open(&output_path).unwrap();
        let batch = source.read_batch().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(source.read_batch().await.unwrap().is_none());

        let coordinator = shared.read().unwrap();
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
    }

    #[tokio::test]
    async fn executor_runs_kafka_to_parquet_pipeline_on_real_runner() {
        use krishiv_connectors::Source;
        use krishiv_connectors::parquet::ParquetSource;

        let temp = tempdir().unwrap();
        let output_path = temp.path().join("events.parquet");

        let executor_id = ExecutorId::try_new("exec-kafka-pipeline-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-kafka-pipeline-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let inbox = ExecutorAssignmentInbox::new();
        let job_id = JobId::try_new("job-kafka-pipeline-1").unwrap();

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-kafka-pipeline",
                    1,
                ))
                .unwrap();
            coordinator
                .submit_job(single_task_job(job_id.clone()))
                .unwrap();
            let launched = coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap()
                .remove(0);

            let assignment = ExecutorTaskAssignment::new(
                TaskAttemptRef::new(
                    launched.job_id().clone(),
                    launched.stage_id().clone(),
                    launched.task_id().clone(),
                    launched.attempt_id(),
                ),
                launched.executor_id().clone(),
                launched.lease_generation(),
                PlanFragment::new(super::KAFKA_TO_PARQUET_FRAGMENT),
                OutputContract::new(
                    OutputContractKind::Sink,
                    format!("parquet-sink:{}", output_path.display()),
                ),
            )
            .with_input_partitions(vec![InputPartition::new(
                "events-partition-0",
                "memory-kafka:events:0:5:1=created,2=updated,3=deleted",
            )]);
            inbox.push(assignment).unwrap();
        }

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&service).await.unwrap().unwrap();

        assert_eq!(report.assignment().job_id(), &job_id);
        assert_eq!(
            report.output().kind(),
            ExecutorTaskOutputKind::ConnectorPipeline
        );
        assert_eq!(report.output().row_count(), 3);
        assert_eq!(report.output().batch_count(), 1);
        assert_eq!(report.output().column_count(), 2);
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );

        let mut source = ParquetSource::open(&output_path).unwrap();
        let batch = source.read_batch().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
        assert!(source.read_batch().await.unwrap().is_none());

        let coordinator = shared.read().unwrap();
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
        assert_eq!(snapshot.succeeded_task_count(), 1);
    }

    #[tokio::test]
    async fn executor_rejects_kafka_to_parquet_without_parquet_sink_contract() {
        let assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-bad-pipeline").unwrap(),
                StageId::try_new("stage-1").unwrap(),
                TaskId::try_new("task-1").unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-bad-pipeline").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new(super::KAFKA_TO_PARQUET_FRAGMENT),
            OutputContract::new(OutputContractKind::Sink, "inline result"),
        )
        .with_input_partitions(vec![InputPartition::new(
            "events-partition-0",
            "memory-kafka:events:0:0:1=created",
        )]);
        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
        let err = runner
            .execute_stage_fragment(&assignment)
            .await
            .unwrap_err();
        match err {
            ExecutorError::InvalidAssignment { message } => {
                assert!(message.contains("parquet-sink:"));
            }
            other => panic!("expected InvalidAssignment, got {other}"),
        }
    }

    #[tokio::test]
    async fn assignment_lease_generation_rejects_stale_shuffle_write() {
        use krishiv_shuffle::{
            InMemoryShuffleStore, PartitionId, ShufflePartition, ShuffleStore, StoreError,
        };

        let stale_assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-shuffle-lease").unwrap(),
                StageId::try_new("stage-1").unwrap(),
                TaskId::try_new("task-1").unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-zombie").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("sql: select 1"),
            OutputContract::new(OutputContractKind::Shuffle, "shuffle partition"),
        );
        let fresh_assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                stale_assignment.job_id().clone(),
                stale_assignment.stage_id().clone(),
                stale_assignment.task_id().clone(),
                stale_assignment.attempt_id().next(),
            ),
            ExecutorId::try_new("exec-replacement").unwrap(),
            stale_assignment.lease_generation().next(),
            PlanFragment::new("sql: select 1"),
            OutputContract::new(OutputContractKind::Shuffle, "shuffle partition"),
        );

        let store = InMemoryShuffleStore::new();
        let id = PartitionId {
            job_id: fresh_assignment.job_id().to_string(),
            stage_id: fresh_assignment.stage_id().to_string(),
            partition: 0,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let partition = ShufflePartition {
            id: id.clone(),
            schema,
            batches: vec![batch],
        };

        store
            .register_partition_lease(id.clone(), fresh_assignment.lease_generation().as_u64())
            .await
            .unwrap();

        let err = store
            .write_partition(
                partition.clone(),
                stale_assignment.lease_generation().as_u64(),
            )
            .await
            .unwrap_err();

        match err {
            StoreError::StaleLeaseToken { expected, actual } => {
                assert_eq!(expected, fresh_assignment.lease_generation().as_u64());
                assert_eq!(actual, stale_assignment.lease_generation().as_u64());
            }
            other => panic!("expected StaleLeaseToken, got {other}"),
        }
        assert!(store.read_partition(&id).await.unwrap().is_none());

        store
            .write_partition(partition, fresh_assignment.lease_generation().as_u64())
            .await
            .unwrap();
        let stored = store.read_partition(&id).await.unwrap().unwrap();
        assert_eq!(stored.batches[0].num_rows(), 1);
    }
}
