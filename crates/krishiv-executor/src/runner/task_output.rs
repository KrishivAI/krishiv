use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use krishiv_proto::{
    ExecutorTaskAssignment, TaskOutputMetadata, TaskRuntimeStats, TransportDisposition,
};

use krishiv_state::StateBackend;

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
    let schema = batches
        .first()
        .ok_or_else(|| "empty batch list".to_string())?
        .schema();
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
    /// Hot-key reports from `HeavyHittersTracker` observed during shuffle write.
    pub(crate) hot_key_reports: Vec<krishiv_proto::HeartbeatHotKeyReport>,
    /// Staged sink files (relative to the sink base_dir) written under the
    /// Phase 2.3 staged commit protocol. Empty for non-sink and legacy
    /// direct-write sink tasks.
    pub(crate) sink_staged_files: Vec<String>,
    /// EMA-based partition bucket recommendation from `StreamingPartitionAdvisor`.
    /// `None` for non-streaming tasks; `Some(n)` when the advisor has observed
    /// enough data to suggest a bucket count for the next streaming cycle.
    pub(crate) advisory_buckets: Option<u32>,
    /// E1.3: Backpressure signal from downstream operators.  `None` means no
    /// credit accounting was active (legacy / batch tasks); a signal value
    /// lets the coordinator decide whether to schedule the next streaming cycle
    /// immediately or wait for downstream to drain.
    pub(crate) backpressure: krishiv_common::BackpressureSignal,
    /// Coordinator-authoritative IVM tick output: a framed `name → RecordBatch`
    /// map of each view's full materialized output (via `encode_batch_map`).
    /// `None` for non-IVM tasks; `Some(bytes)` for `IvmStep` tasks. Travelled
    /// to the coordinator through the existing `inline_record_batch_ipc` channel
    /// as a single raw blob (not decoded as Arrow IPC — the coordinator unpacks
    /// it via `decode_batch_map`).
    pub(crate) ivm_output: Option<Vec<u8>>,
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
            hot_key_reports: Vec::new(),
            sink_staged_files: Vec::new(),
            advisory_buckets: None,
            backpressure: krishiv_common::BackpressureSignal::None,
            ivm_output: None,
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
            hot_key_reports: Vec::new(),
            sink_staged_files: Vec::new(),
            advisory_buckets: None,
            backpressure: krishiv_common::BackpressureSignal::None,
            ivm_output: None,
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
            hot_key_reports: Vec::new(),
            sink_staged_files: Vec::new(),
            advisory_buckets: None,
            backpressure: krishiv_common::BackpressureSignal::None,
            ivm_output: None,
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
            hot_key_reports: Vec::new(),
            sink_staged_files: Vec::new(),
            advisory_buckets: None,
            backpressure: krishiv_common::BackpressureSignal::None,
            ivm_output: None,
        }
    }

    /// Output from a streaming window aggregation task (tumbling, sliding, or session).
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
            hot_key_reports: Vec::new(),
            sink_staged_files: Vec::new(),
            advisory_buckets: None,
            backpressure: krishiv_common::BackpressureSignal::None,
            ivm_output: None,
        }
    }

    pub(crate) fn ivm_step(active_views: usize, total_output_rows: usize) -> Self {
        Self {
            kind: ExecutorTaskOutputKind::IvmStep,
            row_count: total_output_rows,
            batch_count: active_views,
            column_count: 0,
            shuffle_partitions: Vec::new(),
            runtime_stats: None,
            record_batches: Vec::new(),
            watermark_ms: None,
            hot_key_reports: Vec::new(),
            sink_staged_files: Vec::new(),
            advisory_buckets: None,
            backpressure: krishiv_common::BackpressureSignal::None,
            ivm_output: None,
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

    /// Attach the framed view-output blob for a coordinator-authoritative
    /// IVM tick (produced by `execute_ivm_fragment`).
    pub(crate) fn with_ivm_output(mut self, blob: Option<Vec<u8>>) -> Self {
        self.ivm_output = blob;
        self
    }

    /// Attach staged sink file paths (Phase 2.3 staged commit protocol).
    pub(crate) fn with_sink_staged_files(mut self, files: Vec<String>) -> Self {
        self.sink_staged_files = files;
        self
    }

    /// Staged sink files written by this task (empty for non-staged tasks).
    pub fn sink_staged_files(&self) -> &[String] {
        &self.sink_staged_files
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

    /// Attach the EMA-derived partition bucket recommendation from
    /// `StreamingPartitionAdvisor` for the next streaming cycle.
    pub(crate) fn with_advisory_buckets(mut self, buckets: u32) -> Self {
        self.advisory_buckets = Some(buckets);
        self
    }

    /// EMA-based bucket count recommendation for the next streaming cycle, if any.
    pub fn advisory_buckets(&self) -> Option<u32> {
        self.advisory_buckets
    }

    /// E1.3: Downstream backpressure signal observed during this task execution.
    pub fn backpressure(&self) -> krishiv_common::BackpressureSignal {
        self.backpressure
    }

    /// E1.3: Set the backpressure signal (called by streaming fragment executors).
    #[allow(dead_code)]
    pub(crate) fn with_backpressure(mut self, signal: krishiv_common::BackpressureSignal) -> Self {
        self.backpressure = signal;
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
        if !self.record_batches.is_empty() {
            match encode_record_batches_ipc(&self.record_batches) {
                Ok(ipc) => {
                    meta = meta.with_inline_record_batch_ipc(ipc);
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        batch_count = self.record_batches.len(),
                        "failed to encode inline record batches for task output"
                    );
                }
            }
        }
        // GAP-2: Propagate watermark so the coordinator can track global low-watermark
        // across all executor tasks for downstream stage scheduling.
        if let Some(wm) = self.watermark_ms {
            meta = meta.with_watermark_ms(wm);
        }
        if !self.hot_key_reports.is_empty() {
            meta = meta.with_hot_key_reports(self.hot_key_reports.clone());
        }
        if !self.sink_staged_files.is_empty() {
            meta = meta.with_sink_staged_files(self.sink_staged_files.clone());
        }
        // Coordinator-authoritative IVM: carry the framed view-output blob as a
        // single raw entry in the inline-record-batch channel. The coordinator
        // reads it via take_job_inline_results and unpacks with decode_batch_map
        // (it is NOT Arrow IPC and must not be passed through the SQL decoder).
        if let Some(blob) = &self.ivm_output {
            meta = meta.with_inline_record_batch_ipc(vec![blob.clone()]);
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
    /// Task was cancelled before execution started.
    Cancelled,
    /// Shuffle write: hash-partitioned batches written to the local shuffle store.
    ShuffleWrite,
    /// Streaming window aggregation output (tumbling, sliding, or session).
    StreamingWindow,
    /// One bounded IVM tick completed (DeltaBatch model).
    IvmStep,
}

impl ExecutorTaskOutputKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::ConnectorPipeline => "connector_pipeline",
            Self::Cancelled => "cancelled",
            Self::ShuffleWrite => "shuffle_write",
            Self::StreamingWindow => "streaming_window",
            Self::IvmStep => "ivm_step",
        }
    }
}

/// Shuffle store context held by the task runner.
///
/// When present, `shuffle-write:` fragments can write hash-partitioned output to
/// the local store and report `ShufflePartitionOutput` back to the coordinator.
#[derive(Clone)]
pub struct ShuffleContext {
    pub store: std::sync::Arc<krishiv_shuffle::ShuffleBackend>,
    pub local_dir: PathBuf,
    pub flight_endpoint: String,
    /// When the External Shuffle Service is running in-process, sort-shuffle
    /// writers register their output here so the ESS HTTP server can serve
    /// partition-level range reads without a separate registration RPC.
    pub ess_index: Option<krishiv_shuffle::SortShuffleIndex>,
    /// T12: optional push-shuffle store — when set, each partition's IPC bytes
    /// are also pushed here so reduce-side tasks can read without a Flight hop.
    pub push_store: Option<std::sync::Arc<krishiv_shuffle::PushShuffleStore>>,
}

impl fmt::Debug for ShuffleContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShuffleContext")
            .field("flight_endpoint", &self.flight_endpoint)
            .field("has_ess_index", &self.ess_index.is_some())
            .finish()
    }
}

// ── R6 CheckpointState ────────────────────────────────────────────────────────

/// Typed access to the state a task snapshots at a checkpoint barrier and
/// reloads at restore.
///
/// Continuous window jobs keep their operator state inside the per-job
/// [`ContinuousWindowExecutor`] — snapshotting the executor-wide generic
/// backend for them would persist vacuous state (the window operators never
/// write to it).  This enum makes the selection explicit and typed instead of
/// hiding it behind a partially implemented `StateBackend` adapter.
#[derive(Clone)]
pub enum CheckpointStateHandle {
    /// Generic keyed state backend shared by non-window stateful tasks.
    Backend(Arc<std::sync::Mutex<Box<dyn StateBackend>>>),
    /// Stateful continuous window executor owned by a `stream:loop:` job.
    ContinuousWindow(Arc<std::sync::Mutex<krishiv_dataflow::ContinuousWindowExecutor>>),
}

impl fmt::Debug for CheckpointStateHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(_) => f.write_str("CheckpointStateHandle::Backend"),
            Self::ContinuousWindow(_) => f.write_str("CheckpointStateHandle::ContinuousWindow"),
        }
    }
}

impl CheckpointStateHandle {
    /// Wrap a concrete state backend.
    pub fn from_backend(backend: impl StateBackend + 'static) -> Self {
        Self::Backend(Arc::new(std::sync::Mutex::new(Box::new(backend))))
    }

    /// Take a portable snapshot of the underlying state.
    pub fn snapshot(&self) -> krishiv_state::StateResult<Vec<u8>> {
        match self {
            Self::Backend(backend) => backend
                .lock()
                .map_err(|e| krishiv_state::StateError::LockPoisoned {
                    message: e.to_string(),
                })?
                .snapshot(),
            Self::ContinuousWindow(exec) => exec
                .lock()
                .map_err(|e| krishiv_state::StateError::LockPoisoned {
                    message: e.to_string(),
                })?
                .snapshot()
                .map_err(|e| krishiv_state::StateError::BackendUnavailable {
                    message: format!("continuous window snapshot: {e}"),
                    source: None,
                }),
        }
    }

    /// Replace the underlying state with `bytes` (a snapshot produced by
    /// [`Self::snapshot`]).  Pass the canonical empty snapshot to clear.
    pub fn load_snapshot(&self, bytes: &[u8]) -> krishiv_state::StateResult<()> {
        match self {
            Self::Backend(backend) => backend
                .lock()
                .map_err(|e| krishiv_state::StateError::LockPoisoned {
                    message: e.to_string(),
                })?
                .load_snapshot(bytes),
            Self::ContinuousWindow(exec) => exec
                .lock()
                .map_err(|e| krishiv_state::StateError::LockPoisoned {
                    message: e.to_string(),
                })?
                .restore_from_snapshot(bytes)
                .map_err(|e| krishiv_state::StateError::BackendUnavailable {
                    message: format!("continuous window restore: {e}"),
                    source: None,
                }),
        }
    }

    /// Merge `bytes` additively into the underlying state.
    pub fn merge_snapshot(&self, bytes: &[u8]) -> krishiv_state::StateResult<()> {
        match self {
            Self::Backend(backend) => {
                let entries = krishiv_state::decode_snapshot_entries(bytes)?;
                let mut guard =
                    backend
                        .lock()
                        .map_err(|e| krishiv_state::StateError::LockPoisoned {
                            message: e.to_string(),
                        })?;
                let batch: Vec<(&str, &str, &[u8], &[u8])> = entries
                    .iter()
                    .map(|(op, name, key, value)| {
                        (op.as_str(), name.as_str(), key.as_slice(), value.as_slice())
                    })
                    .collect();
                guard.put_batch(&batch)
            }
            Self::ContinuousWindow(exec) => exec
                .lock()
                .map_err(|e| krishiv_state::StateError::LockPoisoned {
                    message: e.to_string(),
                })?
                .merge_snapshot(bytes)
                .map_err(|e| krishiv_state::StateError::BackendUnavailable {
                    message: format!("continuous window merge restore: {e}"),
                    source: None,
                }),
        }
    }
}

/// Restore directive applied (or pending application) on this executor for a
/// job, recorded from a `RestoreFromCheckpointCommand`.
///
/// Snapshot bytes are read from checkpoint storage when the command arrives so
/// a lazily created loop executor can be seeded without re-reading storage.
#[derive(Debug, Clone)]
pub struct RestoredJobCheckpoint {
    pub epoch: u64,
    pub fencing_token: u64,
    /// Operator snapshot bytes from the restored checkpoint, in metadata order.
    pub snapshots: Vec<Vec<u8>>,
}

/// Connector-encoded source offset restored from checkpoint metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredSourceOffset {
    /// Checkpoint partition/source identifier as stored in metadata.
    pub partition_id: String,
    /// Best-effort source identifier derived from the partition id.
    pub source_id: Option<String>,
    /// Legacy numeric offset retained for compatibility/status.
    pub legacy_offset: i64,
    /// Connector-encoded exact offset bytes.
    pub encoded_offset: Vec<u8>,
}

impl RestoredSourceOffset {
    fn from_record(record: &krishiv_state::checkpoint::SourceOffsetRecord) -> Option<Self> {
        if record.encoded_offset.is_empty() {
            return None;
        }
        let source_id = record
            .partition_id
            .rsplit_once('-')
            .map(|(source, _)| source.to_owned())
            .filter(|source| !source.is_empty());
        Some(Self {
            partition_id: record.partition_id.clone(),
            source_id,
            legacy_offset: record.offset,
            encoded_offset: record.encoded_offset.clone(),
        })
    }

    /// Return true when this offset belongs to the connector source being opened.
    pub fn matches_source(&self, partition_id: &str, table_name: &str) -> bool {
        self.partition_id == partition_id
            || self.partition_id == table_name
            || self.source_id.as_deref() == Some(table_name)
    }
}

/// Extract generic connector-encoded source offsets from checkpoint metadata.
pub fn restored_source_offsets_from_records(
    records: &[krishiv_state::checkpoint::SourceOffsetRecord],
) -> Vec<RestoredSourceOffset> {
    records
        .iter()
        .filter_map(RestoredSourceOffset::from_record)
        .collect()
}

/// Apply restored snapshot bytes to a state handle: the first non-empty
/// snapshot replaces the state, the rest merge additively.  With no snapshots
/// the state is cleared — the job had no state at the restored checkpoint.
pub(crate) fn apply_snapshots_to_state(
    state: &CheckpointStateHandle,
    snapshots: &[Vec<u8>],
) -> krishiv_state::StateResult<()> {
    let mut non_empty = snapshots.iter().filter(|bytes| !bytes.is_empty());
    match non_empty.next() {
        None => state.load_snapshot(&krishiv_state::encode_snapshot_entries(&[])),
        Some(first) => {
            state.load_snapshot(first)?;
            for rest in non_empty {
                state.merge_snapshot(rest)?;
            }
            Ok(())
        }
    }
}

/// Parse Kafka offsets out of checkpoint source-offset records.
///
/// Checkpoint acks encode each Kafka partition as
/// `kafka-{topic}-{partition}`; the partition index is always the final
/// `-`-separated segment, so `rsplit_once` recovers topics that themselves
/// contain `-`.
pub fn kafka_offsets_from_source_records(
    records: &[krishiv_state::checkpoint::SourceOffsetRecord],
) -> Vec<krishiv_connectors::kafka::KafkaOffset> {
    use krishiv_connectors::Offset;

    let mut offsets = Vec::new();
    for record in records {
        let Some(rest) = record.partition_id.strip_prefix("kafka-") else {
            continue;
        };
        let Some((topic, partition)) = rest.rsplit_once('-') else {
            continue;
        };
        let Ok(partition) = partition.parse::<i32>() else {
            continue;
        };
        if topic.is_empty() {
            continue;
        }
        let fallback = krishiv_connectors::kafka::KafkaOffset {
            topic: topic.to_owned(),
            partition,
            offset: record.offset,
        };
        let offset = if record.encoded_offset.is_empty() {
            fallback
        } else {
            krishiv_connectors::kafka::KafkaOffset::decode(&record.encoded_offset)
                .unwrap_or(fallback)
        };
        offsets.push(offset);
    }
    offsets.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
    offsets
}
