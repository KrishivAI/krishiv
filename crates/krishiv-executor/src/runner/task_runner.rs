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
    /// Phase 56: incremental SST checkpointer for RocksDB-backed window
    /// state, created lazily on the first incremental epoch. `None` until
    /// then and for tasks whose state is not RocksDB-backed.
    pub(crate) incremental_checkpointer: Option<krishiv_state::RocksDbIncrementalCheckpointer>,
    /// Phase 56 (SH7): bytes currently reserved in the unified arbiter's
    /// State region for this task's operator state (updated per checkpoint
    /// from observed snapshot/SST size; best-effort accounting).
    pub(crate) state_region_reserved: u64,
}

/// Marker prefix for an incremental-checkpoint pointer blob written at the
/// standard `state.bin` path (Phase 56). The blob body after the prefix is
/// the storage path of the [`krishiv_state::SstEpochManifest`] JSON. Writing
/// a pointer at the standard layout keeps the coordinator commit/manifest
/// path completely unchanged; restore materializes the SSTs back into a
/// portable snapshot before applying.
pub(crate) const INCREMENTAL_SNAPSHOT_MARKER: &[u8] = b"INCR-SST-V1|";

/// Whether incremental checkpoints are enabled
/// (`KRISHIV_INCREMENTAL_CHECKPOINTS`, default true).
pub(crate) fn incremental_checkpoints_enabled() -> bool {
    match std::env::var("KRISHIV_INCREMENTAL_CHECKPOINTS") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
    }
}

/// Every Nth epoch takes a FULL portable snapshot even in incremental mode
/// (`KRISHIV_FULL_SNAPSHOT_EVERY`, default 8) — bounds the SST manifest
/// chain and keeps rescale/savepoint materialization cheap.
pub(crate) fn full_snapshot_every() -> u64 {
    std::env::var("KRISHIV_FULL_SNAPSHOT_EVERY")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(8)
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
            incremental_checkpointer: None,
            state_region_reserved: 0,
        }
    }

    /// Phase 56 (SH7): reconcile this task's State-region reservation with
    /// the size observed at the latest checkpoint. Best-effort — a denied
    /// reservation is logged, not fatal (the state itself lives on disk;
    /// this accounting exists so Execution/Shuffle stop overcommitting
    /// against resident state).
    fn record_state_region_usage(&mut self, observed_bytes: u64) {
        let Some(manager) = crate::fragment::common::executor_unified_memory() else {
            return;
        };
        use krishiv_common::MemoryRegion;
        if self.state_region_reserved > 0 {
            manager.release(MemoryRegion::State, self.state_region_reserved);
            self.state_region_reserved = 0;
        }
        if observed_bytes > 0 {
            if manager.try_reserve(MemoryRegion::State, observed_bytes) {
                self.state_region_reserved = observed_bytes;
            } else {
                tracing::warn!(
                    task_id = %self.task_id,
                    observed_bytes,
                    "unified State region cannot cover observed operator state; \
                     executor memory is overcommitted"
                );
            }
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
        storage: &dyn CheckpointStorage,
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

        // Phase 56: RocksDB-backed window state checkpoints SST deltas
        // instead of a full portable snapshot — bytes written per epoch
        // scale with change rate, not state size. A full snapshot is still
        // taken every `full_snapshot_every()` epochs (bounds the manifest
        // chain; keeps rescale/savepoint materialization cheap) and whenever
        // the state is not RocksDB-backed.
        if incremental_checkpoints_enabled()
            && !req.epoch.is_multiple_of(full_snapshot_every())
            && let Some(ack) = self.try_incremental_checkpoint(&req, state, storage)?
        {
            self.last_acked_epoch = req.epoch;
            return Ok(ack);
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
        self.record_state_region_usage(snapshot_bytes.len() as u64);

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

    /// Attempt an incremental SST checkpoint for RocksDB-backed continuous
    /// window state. Returns `Ok(None)` when the state is not eligible
    /// (non-window handle, in-memory backend, operator not yet initialised)
    /// so the caller falls back to the full portable snapshot.
    fn try_incremental_checkpoint(
        &mut self,
        req: &InitiateCheckpointRequest,
        state: &CheckpointStateHandle,
        storage: &dyn CheckpointStorage,
    ) -> ExecutorResult<Option<CheckpointAckRequest>> {
        let CheckpointStateHandle::ContinuousWindow(exec) = state else {
            return Ok(None);
        };
        let storage_prefix = format!(
            "{}/incr/{}/{}",
            req.job_id.as_str(),
            self.operator_id,
            self.task_id.as_str()
        );
        let manifest = {
            let mut guard = exec.lock().map_err(|_| ExecutorError::LocalExecution {
                message: format!(
                    "incremental checkpoint: window executor lock poisoned for task {}",
                    self.task_id
                ),
            })?;
            // Persist live window panes into the backend FIRST — the SST
            // checkpoint captures the backend's on-disk state.
            guard
                .checkpoint()
                .map_err(|e| ExecutorError::LocalExecution {
                    message: format!("incremental checkpoint persist panes: {e}"),
                })?;
            if self.incremental_checkpointer.is_none() {
                // Lazily create the checkpointer only when the backend turns
                // out to be RocksDB (checked below via with_rocksdb_state).
                let work_dir = std::env::temp_dir()
                    .join("krishiv-incr-ckpt")
                    .join(req.job_id.as_str())
                    .join(self.task_id.as_str());
                self.incremental_checkpointer = Some(
                    krishiv_state::RocksDbIncrementalCheckpointer::new(work_dir).map_err(|e| {
                        ExecutorError::LocalExecution {
                            message: format!("incremental checkpointer init: {e}"),
                        }
                    })?,
                );
            }
            let Some(checkpointer) = self.incremental_checkpointer.as_mut() else {
                return Ok(None);
            };
            match guard.with_rocksdb_state(|backend| {
                checkpointer.take_checkpoint(backend, req.epoch, storage, &storage_prefix)
            }) {
                None => return Ok(None), // not RocksDB-backed → full snapshot
                Some(result) => result.map_err(|e| ExecutorError::LocalExecution {
                    message: format!(
                        "incremental checkpoint failed for task {} epoch {}: {e}",
                        self.task_id, req.epoch
                    ),
                })?,
            }
        };

        // Pointer blob at the standard snapshot layout: the coordinator's
        // commit/manifest verification reads it like any snapshot; restore
        // detects the marker and materializes the SSTs.
        let mut pointer = INCREMENTAL_SNAPSHOT_MARKER.to_vec();
        pointer.extend_from_slice(manifest.manifest_storage_path.as_bytes());
        let path = snapshot_path(
            req.job_id.as_str(),
            req.epoch,
            &self.operator_id,
            self.task_id.as_str(),
        );
        storage
            .write_bytes(&path, &pointer)
            .map_err(|error| ExecutorError::LocalExecution {
                message: format!(
                    "incremental checkpoint pointer write failed for task {} epoch {}: {error}",
                    self.task_id, req.epoch
                ),
            })?;
        tracing::debug!(
            task_id = %self.task_id,
            epoch = req.epoch,
            sst_files = manifest.sst_count(),
            sst_bytes = manifest.total_sst_bytes(),
            "incremental SST checkpoint taken"
        );
        self.record_state_region_usage(manifest.total_sst_bytes());

        let mut source_offsets = self.source_offsets.clone();
        source_offsets.extend(self.kafka_checkpoint_offsets()?);
        let operator_id =
            krishiv_proto::OperatorId::try_new(self.operator_id.clone()).map_err(|_| {
                ExecutorError::LocalExecution {
                    message: format!("operator_id is empty for task {}", self.task_id),
                }
            })?;
        Ok(Some(CheckpointAckRequest {
            job_id: req.job_id.clone(),
            operator_id,
            task_id: self.task_id.clone(),
            epoch: req.epoch,
            fencing_token: req.fencing_token,
            source_offsets,
            snapshot_path: Some(path),
            unaligned_buffers: Vec::new(),
            sink_transactions: Vec::new(),
        }))
    }

    /// Kafka compatibility offsets in checkpoint wire shape.
    fn kafka_checkpoint_offsets(&self) -> ExecutorResult<Vec<CheckpointSourceOffset>> {
        self.kafka_source_offsets
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
            .collect()
    }

    /// Reset this task's checkpoint progress to a restored epoch.
    ///
    /// (See also [`materialize_portable_snapshots`] for the restore-side
    /// counterpart of the incremental checkpoint pointer blobs.)
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

/// Phase 56: convert checkpoint snapshot blobs into PORTABLE snapshots.
///
/// Full snapshots pass through unchanged. Incremental pointer blobs
/// ([`INCREMENTAL_SNAPSHOT_MARKER`]) are materialized: the SST manifest is
/// loaded from storage, the RocksDB directory is reconstructed in a scratch
/// dir, opened, and serialized to the portable entry format — so every
/// downstream consumer (window restore, key-group redistribution, generic
/// backend merge) keeps working on one snapshot format.
pub(crate) fn materialize_portable_snapshots(
    snapshots: Vec<Vec<u8>>,
    storage: &dyn CheckpointStorage,
) -> ExecutorResult<Vec<Vec<u8>>> {
    use krishiv_state::StateBackend as _;
    let mut out = Vec::with_capacity(snapshots.len());
    for (index, blob) in snapshots.into_iter().enumerate() {
        let Some(manifest_path) = blob
            .strip_prefix(INCREMENTAL_SNAPSHOT_MARKER)
            .map(|rest| String::from_utf8_lossy(rest).into_owned())
        else {
            out.push(blob);
            continue;
        };
        let manifest_bytes = storage
            .read_bytes(&manifest_path)
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("incremental restore: read manifest {manifest_path}: {e}"),
            })?
            .ok_or_else(|| ExecutorError::LocalExecution {
                message: format!(
                    "incremental restore: SST manifest {manifest_path} missing from storage"
                ),
            })?;
        let manifest: krishiv_state::SstEpochManifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("incremental restore: parse manifest {manifest_path}: {e}"),
            })?;
        let scratch = std::env::temp_dir().join(format!(
            "krishiv-incr-restore-{}-{}-{}",
            std::process::id(),
            manifest.epoch,
            index
        ));
        let _ = std::fs::remove_dir_all(&scratch);
        krishiv_state::RocksDbIncrementalCheckpointer::restore_checkpoint(
            &manifest, storage, &scratch,
        )
        .map_err(|e| ExecutorError::LocalExecution {
            message: format!("incremental restore: rebuild RocksDB dir: {e}"),
        })?;
        let portable = {
            let backend = krishiv_state::RocksDbStateBackend::open(&scratch).map_err(|e| {
                ExecutorError::LocalExecution {
                    message: format!("incremental restore: open rebuilt RocksDB: {e}"),
                }
            })?;
            backend
                .snapshot()
                .map_err(|e| ExecutorError::LocalExecution {
                    message: format!("incremental restore: portable snapshot: {e}"),
                })?
        };
        let _ = std::fs::remove_dir_all(&scratch);
        out.push(portable);
    }
    Ok(out)
}
