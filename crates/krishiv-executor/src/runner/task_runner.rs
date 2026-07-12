use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use krishiv_connectors::Offset;
use krishiv_proto::{
    CheckpointAckRequest, CheckpointSourceOffset, InitiateCheckpointRequest, TaskId,
};
use krishiv_state::checkpoint::{CheckpointStorage, snapshot_path};

use crate::{ExecutorError, ExecutorResult};

use super::task_output::CheckpointStateHandle;

/// Per-task checkpoint state for executor-side checkpoint participation (R6).
///
/// Tracks the last acked epoch, operator/task identity, and source offsets so the
/// executor can correctly handle `InitiateCheckpointRequest` messages.
#[derive(Debug, Clone)]
pub struct TaskRunner {
    /// Last checkpoint epoch that this task acked (0 = none acked yet).
    pub last_acked_epoch: u64,
    /// Operator identifier for this task: defaults to `"operator-<task_id>"`.
    pub operator_id: String,
    /// Task identifier.
    pub task_id: TaskId,
    /// Generic connector source offsets for checkpoint. Empty for tasks whose
    /// sources do not expose checkpoint-capable encoded offsets.
    pub source_offsets: Vec<CheckpointSourceOffset>,
    /// Per-partition Kafka source offsets for checkpoint. Retained as a
    /// compatibility path while Kafka source execution still owns typed Kafka
    /// offsets directly.
    pub kafka_source_offsets: Vec<krishiv_connectors::kafka::KafkaOffset>,
}

impl TaskRunner {
    /// Create a new `TaskRunner` for `task_id`.
    pub fn new(task_id: TaskId) -> Self {
        let operator_id = format!("operator-{}", task_id.as_str());
        Self {
            last_acked_epoch: 0,
            operator_id,
            task_id,
            source_offsets: Vec::new(),
            kafka_source_offsets: Vec::new(),
        }
    }

    /// Set generic connector source offsets for checkpoint.
    /// Insert or replace one source offset by partition id (run-loop path:
    /// offsets accumulate incrementally per owned split instead of being
    /// replaced wholesale each cycle).
    pub fn upsert_source_offset(&mut self, offset: CheckpointSourceOffset) {
        match self
            .source_offsets
            .iter_mut()
            .find(|existing| existing.partition_id == offset.partition_id)
        {
            Some(existing) => *existing = offset,
            None => self.source_offsets.push(offset),
        }
    }

    pub fn with_source_offsets(mut self, offsets: Vec<CheckpointSourceOffset>) -> Self {
        self.source_offsets = offsets;
        self
    }

    /// Set the per-partition Kafka source offsets (for Kafka source tasks).
    pub fn with_kafka_source_offsets(
        mut self,
        offsets: Vec<krishiv_connectors::kafka::KafkaOffset>,
    ) -> Self {
        self.kafka_source_offsets = offsets;
        self
    }

    /// Handle a `InitiateCheckpointRequest`.
    ///
    /// 1. Rejects stale epochs (epoch <= last_acked_epoch).
    /// 2. Takes a snapshot via `state.snapshot()`.
    /// 3. Writes the snapshot to `storage`.
    /// 4. Returns a `CheckpointAckRequest` with source offsets and snapshot path.
    /// 5. Updates `last_acked_epoch`.
    pub fn handle_initiate_checkpoint(
        &mut self,
        req: InitiateCheckpointRequest,
        state: &CheckpointStateHandle,
        storage: &(impl CheckpointStorage + ?Sized),
    ) -> ExecutorResult<CheckpointAckRequest> {
        // Stale epoch: return an ack that signals the stale condition via epoch.
        if req.epoch <= self.last_acked_epoch {
            let operator_id = krishiv_proto::OperatorId::try_new(self.operator_id.clone())
                .map_err(|_| ExecutorError::LocalExecution {
                    message: format!("operator_id is empty for task {}", self.task_id),
                })?;
            return Ok(CheckpointAckRequest {
                job_id: req.job_id,
                operator_id,
                task_id: self.task_id.clone(),
                epoch: self.last_acked_epoch, // signal: stale
                fencing_token: req.fencing_token,
                source_offsets: vec![],
                snapshot_path: None,
                unaligned_buffers: Vec::new(),
                sink_transactions: Vec::new(),
            });
        }

        // Take a state snapshot with retry for transient I/O errors (R9).
        // SnapshotUnsupported is permanent (stateless operator) and skipped.
        // Other errors are retried up to 3 times with 200 ms back-off.
        let snapshot_bytes = {
            let mut last_err = None;
            let mut result = None;
            for attempt in 0u8..3 {
                if attempt > 0 {
                    std::thread::sleep(std::time::Duration::from_millis(200));
                }
                match state.snapshot() {
                    Ok(bytes) => {
                        result = Some(bytes);
                        break;
                    }
                    Err(krishiv_state::StateError::SnapshotUnsupported { .. }) => {
                        result = Some(Vec::new());
                        break;
                    }
                    Err(e) => {
                        last_err = Some(e);
                    }
                }
            }
            match result {
                Some(bytes) => bytes,
                None => {
                    return Err(ExecutorError::LocalExecution {
                        message: format!(
                            "checkpoint snapshot failed after 3 attempts for task {} at epoch {}: {}",
                            self.task_id,
                            req.epoch,
                            last_err.map(|e| e.to_string()).unwrap_or_default()
                        ),
                    });
                }
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

        // Build source offsets. Generic connector offsets are already in the
        // checkpoint wire shape; Kafka offsets are appended through the legacy
        // compatibility cache until Kafka source execution moves fully to the
        // generic path.
        let mut source_offsets = self.source_offsets.clone();
        let kafka_source_offsets: Vec<CheckpointSourceOffset> = self
            .kafka_source_offsets
            .iter()
            .map(|ko| {
                Ok(CheckpointSourceOffset {
                    partition_id: krishiv_proto::PartitionId::try_new(format!(
                        "kafka-{}-{}",
                        ko.topic, ko.partition
                    ))
                    .map_err(|_| ExecutorError::LocalExecution {
                        message: format!(
                            "partition_id is empty for topic={} partition={}",
                            ko.topic, ko.partition
                        ),
                    })?,
                    offset: ko.offset,
                    encoded_offset: ko.encode(),
                })
            })
            .collect::<Result<Vec<_>, ExecutorError>>()?;
        source_offsets.extend(kafka_source_offsets);

        self.last_acked_epoch = req.epoch;

        let operator_id =
            krishiv_proto::OperatorId::try_new(self.operator_id.clone()).map_err(|_| {
                ExecutorError::LocalExecution {
                    message: format!("operator_id is empty for task {}", self.task_id),
                }
            })?;

        Ok(CheckpointAckRequest {
            job_id: req.job_id,
            operator_id,
            task_id: self.task_id.clone(),
            epoch: req.epoch,
            fencing_token: req.fencing_token,
            source_offsets,
            snapshot_path: snap_path,
            unaligned_buffers: Vec::new(),
            sink_transactions: Vec::new(),
        })
    }

    /// Reset this task's checkpoint progress to a restored epoch.
    ///
    /// After a global rollback the coordinator resumes epochs from
    /// `restored_epoch + 1`; seeding `last_acked_epoch` here makes the runner
    /// reject any straggler barrier for a pre-rollback epoch as stale.
    /// Source offsets are cleared — the rewound source re-populates them on
    /// its first post-restore read, and the authoritative restored offsets flow
    /// through the runner-level source restore tables.
    pub fn apply_restored_epoch(&mut self, restored_epoch: u64) {
        self.last_acked_epoch = restored_epoch;
        self.source_offsets.clear();
        self.kafka_source_offsets.clear();
    }
}

/// Drains output from a long-running continuous streaming job.
pub trait ContinuousJobDrainer: Send + Sync {
    /// Process pending input for `job_id` and return newly emitted batches.
    fn drain_job(&self, job_id: &str) -> Result<Vec<RecordBatch>, String>;
}

// ── Streaming progress snapshot (GAP-OB-04) ──────────────────────────────

/// Periodic streaming progress report emitted by a continuous streaming task.
///
/// Unlike the terminal `TaskState::Succeeded` transition (which only fires once
/// at task completion), these snapshots provide intermediate observability into
/// watermark progress, row throughput, and state size while the task is running.
#[derive(Debug, Clone)]
pub struct StreamingProgressSnapshot {
    /// Task that produced this snapshot.
    pub task_id: String,
    /// Job that owns this task.
    pub job_id: String,
    /// Current event-time watermark in milliseconds since epoch.
    pub watermark_ms: i64,
    /// Total rows emitted since task start.
    pub rows_emitted: u64,
    /// Total batches emitted since task start.
    pub batches_emitted: u64,
    /// Approximate state backend byte size.
    pub state_bytes: u64,
    /// Current source offset (connector-specific encoding).
    pub source_offset: Option<Vec<u8>>,
    /// Wall-clock timestamp of this snapshot (ms since epoch).
    pub timestamp_ms: u64,
}

/// Callback invoked by streaming operators to report intermediate progress.
///
/// Each call receives an immutable reference to a [`StreamingProgressSnapshot`].
/// Implementations are free to forward the snapshot to metrics, heartbeat
/// channels, or structured logs.
pub trait StreamingProgressCallback: Send + Sync {
    fn on_progress(&self, snapshot: &StreamingProgressSnapshot);
}

/// Thread-safe boxed wrapper for progress callbacks.
pub type SharedProgressCallback = Arc<dyn StreamingProgressCallback>;

/// Default no-op callback for when progress reporting is not configured.
pub(crate) struct NoOpProgressCallback;

impl StreamingProgressCallback for NoOpProgressCallback {
    fn on_progress(&self, _snapshot: &StreamingProgressSnapshot) {}
}
