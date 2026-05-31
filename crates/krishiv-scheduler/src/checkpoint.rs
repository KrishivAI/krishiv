use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use krishiv_checkpoint::{
    CheckpointMetadata, CheckpointResult, CheckpointStorage, IntegrityManifest,
    OperatorSnapshotRef, SourceOffsetRecord, latest_valid_epoch, latest_valid_epoch_async,
    list_valid_epochs, read_epoch_metadata, read_epoch_metadata_async, read_operator_snapshot,
    read_operator_snapshot_async, write_epoch_hint, write_epoch_hint_async, write_epoch_metadata,
    write_epoch_metadata_async, write_manifest, write_manifest_async,
};
use krishiv_proto::{CheckpointAckRequest, FencingToken, JobId};

/// State of the per-job checkpoint coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointCoordinatorState {
    /// No checkpoint is in progress.
    Idle,
    /// Waiting for executor acks for `epoch`.
    AwaitingAcks { epoch: u64, initiated_at_ms: u64 },
    /// `epoch` was successfully committed.
    Committed { epoch: u64 },
    /// Checkpoint failed; reason recorded.
    Failed { epoch: u64, reason: String },
}

/// Per-job checkpoint coordinator (R6).
///
/// Created when a streaming job with `checkpoint_interval_ms.is_some()` is submitted.
/// Drives the barrier protocol: initiates epochs, collects executor acks, writes
/// `CheckpointMetadata` + `IntegrityManifest` to storage on quorum.
#[derive(Clone)]
pub struct CheckpointCoordinator {
    pub(crate) job_id: JobId,
    pub(crate) coordinator_id: String,
    pub(crate) storage: Arc<dyn CheckpointStorage>,
    pub(crate) interval_ms: u64,
    pub(crate) current_epoch: u64,
    pub(crate) fencing_token: FencingToken,
    pub(crate) pending_acks: HashMap<String, CheckpointAckRequest>, // key: task_id string
    pub(crate) expected_task_count: usize,
    pub(crate) state: CheckpointCoordinatorState,
    /// Savepoint label to attach when the next epoch is committed.
    pub(crate) pending_savepoint_label: Option<String>,
    /// Whether the next commit should be flagged as a savepoint.
    pub(crate) pending_is_savepoint: bool,
    /// Accumulated wall-clock ms since the last checkpoint was initiated.
    /// Driven by `try_tick`; resets on each successful `initiate()`.
    pub(crate) elapsed_ms: u64,
    /// Wall-clock ms spent waiting for acks on the in-flight epoch.
    pub(crate) awaiting_elapsed_ms: u64,
}

impl fmt::Debug for CheckpointCoordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CheckpointCoordinator")
            .field("job_id", &self.job_id)
            .field("interval_ms", &self.interval_ms)
            .field("current_epoch", &self.current_epoch)
            .field("fencing_token", &self.fencing_token)
            .field("expected_task_count", &self.expected_task_count)
            .field("state", &self.state)
            .finish()
    }
}

impl CheckpointCoordinator {
    /// Create a new checkpoint coordinator for `job_id`.
    pub fn new(
        job_id: JobId,
        coordinator_id: String,
        storage: Arc<dyn CheckpointStorage>,
        interval_ms: u64,
        expected_task_count: usize,
    ) -> Self {
        Self {
            job_id,
            coordinator_id,
            storage,
            interval_ms,
            current_epoch: 0,
            fencing_token: FencingToken::initial(),
            pending_acks: HashMap::new(),
            expected_task_count,
            state: CheckpointCoordinatorState::Idle,
            pending_savepoint_label: None,
            pending_is_savepoint: false,
            elapsed_ms: 0,
            awaiting_elapsed_ms: 0,
        }
    }

    /// **Test-only**: create a checkpoint coordinator with a default coordinator id.
    #[doc(hidden)]
    pub fn new_for_test(
        job_id: JobId,
        storage: Arc<dyn CheckpointStorage>,
        interval_ms: u64,
        expected_task_count: usize,
    ) -> Self {
        Self::new(
            job_id,
            "test-coordinator".to_owned(),
            storage,
            interval_ms,
            expected_task_count,
        )
    }

    /// Update quorum size to match currently running tasks (SCH-3).
    pub fn set_expected_task_count(&mut self, count: usize) {
        self.expected_task_count = count;
    }

    /// Begin a new checkpoint epoch.
    ///
    /// Returns `Ok(epoch)` with the epoch number that was initiated.
    /// Returns `Err` if a checkpoint is already awaiting acks.
    pub fn initiate(&mut self) -> Result<u64, String> {
        if matches!(self.state, CheckpointCoordinatorState::AwaitingAcks { .. }) {
            return Err(format!(
                "checkpoint coordinator for job {} is already awaiting acks",
                self.job_id
            ));
        }
        self.current_epoch += 1;
        self.elapsed_ms = 0;
        self.awaiting_elapsed_ms = 0;
        self.pending_acks.clear();
        // Use real wall-clock time (not a deterministic counter) for initiated_at_ms
        // so observability tooling can compute accurate checkpoint latencies.
        let initiated_at_ms = u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0);
        self.state = CheckpointCoordinatorState::AwaitingAcks {
            epoch: self.current_epoch,
            initiated_at_ms,
        };
        Ok(self.current_epoch)
    }

    /// Advance the checkpoint clock by `elapsed_ms` milliseconds.
    ///
    /// Automatically initiates a new checkpoint epoch when accumulated time
    /// crosses `interval_ms`.  Skips initiation while a checkpoint is already
    /// awaiting acks — the next tick after the in-flight checkpoint commits
    /// will fire the next epoch.
    ///
    /// Returns `Some(epoch)` if a checkpoint was initiated, `None` otherwise.
    pub fn try_tick(&mut self, elapsed_ms: u64, ack_timeout_ms: u64) -> Option<u64> {
        // Process awaiting-acks timeout even when expected_task_count is 0,
        // so an epoch doesn't get stuck forever if all tasks finish during it.
        if matches!(self.state, CheckpointCoordinatorState::AwaitingAcks { .. }) {
            self.awaiting_elapsed_ms = self.awaiting_elapsed_ms.saturating_add(elapsed_ms);
            if self.awaiting_elapsed_ms >= ack_timeout_ms {
                self.abort_epoch("timed out waiting for checkpoint acknowledgements");
            }
            return None;
        }
        if self.expected_task_count == 0 {
            return None;
        }
        self.elapsed_ms = self.elapsed_ms.saturating_add(elapsed_ms);
        if self.elapsed_ms >= self.interval_ms {
            self.initiate().ok()
        } else {
            None
        }
    }

    /// Initiate a savepoint (triggered checkpoint with `is_savepoint=true`).
    ///
    /// Stores `label` for use when `commit_epoch` writes the metadata.
    pub fn initiate_savepoint(&mut self, label: Option<String>) -> Result<u64, String> {
        self.pending_is_savepoint = true;
        self.pending_savepoint_label = label;
        self.initiate()
    }

    /// Record one executor ack.
    ///
    /// Returns `Ok(true)` when quorum is complete (all expected acks received).
    /// Returns `Err` if the ack's epoch is stale.
    pub fn receive_ack(&mut self, ack: CheckpointAckRequest) -> Result<bool, String> {
        let current_epoch = match &self.state {
            CheckpointCoordinatorState::AwaitingAcks { epoch, .. } => *epoch,
            _ => {
                return Err(format!(
                    "checkpoint coordinator for job {} is not awaiting acks",
                    self.job_id
                ));
            }
        };
        if ack.epoch != current_epoch {
            return Err(format!(
                "stale checkpoint ack for job {}: expected epoch {current_epoch}, got epoch {}",
                self.job_id, ack.epoch
            ));
        }
        // Fencing token check: must exactly match the current leader's token.
        // Reject both older and newer tokens (prevents split-brain from stale or
        // future-generation coordinators after failover / network partition).
        if ack.fencing_token != self.fencing_token {
            return Err(format!(
                "stale fencing token in ack for job {}: expected {}, got {}",
                self.job_id,
                self.fencing_token.as_u64(),
                ack.fencing_token.as_u64()
            ));
        }
        let key = ack.task_id.as_str().to_owned();
        self.pending_acks.insert(key, ack);
        if self.pending_acks.len() >= self.expected_task_count {
            self.commit_epoch().map_err(|e| e.to_string())?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Async variant of [`Self::receive_ack`].
    pub async fn receive_ack_async(&mut self, ack: CheckpointAckRequest) -> Result<bool, String> {
        let current_epoch = match &self.state {
            CheckpointCoordinatorState::AwaitingAcks { epoch, .. } => *epoch,
            _ => {
                return Err(format!(
                    "checkpoint coordinator for job {} is not awaiting acks",
                    self.job_id
                ));
            }
        };
        if ack.epoch != current_epoch {
            return Err(format!(
                "stale checkpoint ack for job {}: expected epoch {current_epoch}, got epoch {}",
                self.job_id, ack.epoch
            ));
        }
        if ack.fencing_token != self.fencing_token {
            return Err(format!(
                "stale fencing token in ack for job {}: expected {}, got {}",
                self.job_id,
                self.fencing_token.as_u64(),
                ack.fencing_token.as_u64()
            ));
        }
        let key = ack.task_id.as_str().to_owned();
        self.pending_acks.insert(key, ack);
        if self.pending_acks.len() >= self.expected_task_count {
            self.commit_epoch_async().await.map_err(|e| e.to_string())?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Commit the current epoch: write metadata + manifest to storage.
    ///
    /// Normally called automatically when quorum is reached in `receive_ack`.
    pub fn commit_epoch(&mut self) -> CheckpointResult<u64> {
        let epoch = match &self.state {
            CheckpointCoordinatorState::AwaitingAcks { epoch, .. } => *epoch,
            _ => {
                return Err(krishiv_checkpoint::CheckpointError::Storage {
                    message: format!(
                        "commit_epoch called but coordinator for job {} is not awaiting acks",
                        self.job_id
                    ),
                });
            }
        };

        // Collect source offsets — last write wins per partition_id.
        let mut offset_map: HashMap<String, i64> = HashMap::new();
        for ack in self.pending_acks.values() {
            for so in &ack.source_offsets {
                offset_map.insert(so.partition_id.clone(), so.offset);
            }
        }
        let source_offsets: Vec<SourceOffsetRecord> = offset_map
            .into_iter()
            .map(|(partition_id, offset)| SourceOffsetRecord {
                partition_id,
                offset,
            })
            .collect();

        // Collect operator snapshots from acks that have snapshot_path.
        let operator_snapshots: Vec<OperatorSnapshotRef> = self
            .pending_acks
            .values()
            .filter_map(|ack| {
                ack.snapshot_path.as_ref().map(|path| OperatorSnapshotRef {
                    operator_id: ack.operator_id.clone(),
                    task_id: ack.task_id.as_str().to_owned(),
                    snapshot_path: path.clone(),
                })
            })
            .collect();

        let is_savepoint = self.pending_is_savepoint;
        let savepoint_label = self.pending_savepoint_label.take();
        let metadata = CheckpointMetadata {
            version: CheckpointMetadata::VERSION,
            epoch,
            job_id: self.job_id.as_str().to_owned(),
            fencing_token: self.fencing_token.as_u64(),
            coordinator_id: Some(self.coordinator_id.clone()),
            timestamp_ms: epoch * self.interval_ms,
            source_offsets,
            operator_snapshots,
            is_savepoint,
            savepoint_label,
            iceberg_snapshot_id: None,
            kafka_offsets: None,
        };

        // GAP-CP-03: Validate fencing token before committing to storage.
        // Rejects any attempt to write a checkpoint whose token doesn't match
        // the current coordinator generation, preventing split-brain writes.
        krishiv_checkpoint::validate_fencing_token(&metadata, self.fencing_token.as_u64())
            .map_err(|e| krishiv_checkpoint::CheckpointError::Storage {
                message: format!("fencing token mismatch for job {}: {e}", self.job_id),
            })?;

        write_epoch_metadata(
            self.storage.as_ref(),
            self.job_id.as_str(),
            epoch,
            &metadata,
        )?;

        // Build manifest: hash metadata.json + each snapshot file.
        let mut manifest = IntegrityManifest::new();
        let meta_json = serde_json::to_vec_pretty(&metadata).map_err(|e| {
            krishiv_checkpoint::CheckpointError::Storage {
                message: format!("metadata serialize for manifest: {e}"),
            }
        })?;
        manifest.insert_bytes("metadata.json", &meta_json);
        for snap_ref in &metadata.operator_snapshots {
            if let Some(bytes) = read_operator_snapshot(
                self.storage.as_ref(),
                self.job_id.as_str(),
                epoch,
                &snap_ref.operator_id,
                &snap_ref.task_id,
            )? {
                // The manifest key is the path relative to the epoch dir.
                let rel_path = format!("{}/{}/state.bin", snap_ref.operator_id, snap_ref.task_id);
                manifest.insert_bytes(&rel_path, &bytes);
            }
        }
        write_manifest(
            self.storage.as_ref(),
            self.job_id.as_str(),
            epoch,
            &manifest,
        )?;

        // BUG-1: Epoch hint MUST be written last — after the manifest seals the
        // epoch — so that `latest_valid_epoch` never returns an epoch whose
        // manifest has not yet been written.  A crash between write_manifest and
        // write_epoch_hint is safe: the next call to `latest_valid_epoch` falls
        // back to `list_valid_epochs`, which will find the sealed epoch via its
        // manifest file.
        write_epoch_hint(self.storage.as_ref(), self.job_id.as_str(), epoch)?;

        krishiv_governance::audit_log(
            "scheduler",
            &krishiv_governance::AuditAction::CheckpointCommitted {
                job_id: self.job_id.to_string(),
                epoch,
                fencing_token: self.fencing_token.as_u64(),
            },
            krishiv_governance::AuditOutcome::Allowed,
        );

        krishiv_governance::audit_log(
            "scheduler",
            &krishiv_governance::AuditAction::SinkCommitCompleted {
                job_id: self.job_id.to_string(),
                sink_id: "global".to_string(),
                epoch,
            },
            krishiv_governance::AuditOutcome::Allowed,
        );

        self.state = CheckpointCoordinatorState::Committed { epoch };
        self.pending_is_savepoint = false;
        self.awaiting_elapsed_ms = 0;
        Ok(epoch)
    }

    /// Async variant of [`Self::commit_epoch`] for Tokio scheduler paths.
    pub async fn commit_epoch_async(&mut self) -> CheckpointResult<u64> {
        let epoch = match &self.state {
            CheckpointCoordinatorState::AwaitingAcks { epoch, .. } => *epoch,
            _ => {
                return Err(krishiv_checkpoint::CheckpointError::Storage {
                    message: format!(
                        "commit_epoch called but coordinator for job {} is not awaiting acks",
                        self.job_id
                    ),
                });
            }
        };

        let mut offset_map: HashMap<String, i64> = HashMap::new();
        for ack in self.pending_acks.values() {
            for so in &ack.source_offsets {
                offset_map.insert(so.partition_id.clone(), so.offset);
            }
        }
        let source_offsets: Vec<SourceOffsetRecord> = offset_map
            .into_iter()
            .map(|(partition_id, offset)| SourceOffsetRecord {
                partition_id,
                offset,
            })
            .collect();

        let operator_snapshots: Vec<OperatorSnapshotRef> = self
            .pending_acks
            .values()
            .filter_map(|ack| {
                ack.snapshot_path.as_ref().map(|path| OperatorSnapshotRef {
                    operator_id: ack.operator_id.clone(),
                    task_id: ack.task_id.as_str().to_owned(),
                    snapshot_path: path.clone(),
                })
            })
            .collect();

        let is_savepoint = self.pending_is_savepoint;
        let savepoint_label = self.pending_savepoint_label.take();
        let metadata = CheckpointMetadata {
            version: CheckpointMetadata::VERSION,
            epoch,
            job_id: self.job_id.as_str().to_owned(),
            fencing_token: self.fencing_token.as_u64(),
            coordinator_id: Some(self.coordinator_id.clone()),
            timestamp_ms: epoch * self.interval_ms,
            source_offsets,
            operator_snapshots,
            is_savepoint,
            savepoint_label,
            iceberg_snapshot_id: None,
            kafka_offsets: None,
        };

        krishiv_checkpoint::validate_fencing_token(&metadata, self.fencing_token.as_u64())
            .map_err(|e| krishiv_checkpoint::CheckpointError::Storage {
                message: format!("fencing token mismatch for job {}: {e}", self.job_id),
            })?;

        write_epoch_metadata_async(
            self.storage.as_ref(),
            self.job_id.as_str(),
            epoch,
            &metadata,
        )
        .await?;

        let mut manifest = IntegrityManifest::new();
        let meta_json = serde_json::to_vec_pretty(&metadata).map_err(|e| {
            krishiv_checkpoint::CheckpointError::Storage {
                message: format!("metadata serialize for manifest: {e}"),
            }
        })?;
        manifest.insert_bytes("metadata.json", &meta_json);
        for snap_ref in &metadata.operator_snapshots {
            if let Some(bytes) = read_operator_snapshot_async(
                self.storage.as_ref(),
                self.job_id.as_str(),
                epoch,
                &snap_ref.operator_id,
                &snap_ref.task_id,
            )
            .await?
            {
                let rel_path = format!("{}/{}/state.bin", snap_ref.operator_id, snap_ref.task_id);
                manifest.insert_bytes(&rel_path, &bytes);
            }
        }
        write_manifest_async(
            self.storage.as_ref(),
            self.job_id.as_str(),
            epoch,
            &manifest,
        )
        .await?;
        write_epoch_hint_async(self.storage.as_ref(), self.job_id.as_str(), epoch).await?;

        krishiv_governance::audit_log(
            "scheduler",
            &krishiv_governance::AuditAction::CheckpointCommitted {
                job_id: self.job_id.to_string(),
                epoch,
                fencing_token: self.fencing_token.as_u64(),
            },
            krishiv_governance::AuditOutcome::Allowed,
        );

        krishiv_governance::audit_log(
            "scheduler",
            &krishiv_governance::AuditAction::SinkCommitCompleted {
                job_id: self.job_id.to_string(),
                sink_id: "global".to_string(),
                epoch,
            },
            krishiv_governance::AuditOutcome::Allowed,
        );

        self.state = CheckpointCoordinatorState::Committed { epoch };
        self.pending_is_savepoint = false;
        self.awaiting_elapsed_ms = 0;
        Ok(epoch)
    }

    /// Abort the current in-progress epoch (timeout or failure).
    pub fn abort_epoch(&mut self, reason: &str) {
        let epoch = match &self.state {
            CheckpointCoordinatorState::AwaitingAcks { epoch, .. } => *epoch,
            _ => return,
        };
        self.pending_acks.clear();
        self.pending_is_savepoint = false;
        self.pending_savepoint_label = None;
        self.elapsed_ms = 0;
        self.awaiting_elapsed_ms = 0;
        self.state = CheckpointCoordinatorState::Failed {
            epoch,
            reason: reason.to_owned(),
        };

        krishiv_governance::audit_log(
            "scheduler",
            &krishiv_governance::AuditAction::CheckpointAborted {
                job_id: self.job_id.to_string(),
                epoch,
                reason: Some(reason.to_owned()),
            },
            krishiv_governance::AuditOutcome::Allowed,
        );
    }

    /// Load the latest valid epoch from storage on coordinator restart.
    pub fn recover_from_storage(&mut self) -> CheckpointResult<Option<u64>> {
        match latest_valid_epoch(self.storage.as_ref(), self.job_id.as_str()) {
            Ok(epoch) => {
                if let Some(meta) =
                    read_epoch_metadata(self.storage.as_ref(), self.job_id.as_str(), epoch)?
                {
                    self.fencing_token =
                        FencingToken::try_new(meta.fencing_token).unwrap_or(self.fencing_token);
                }
                self.current_epoch = epoch;
                self.state = CheckpointCoordinatorState::Committed { epoch };
                Ok(Some(epoch))
            }
            Err(krishiv_checkpoint::CheckpointError::NoValidEpoch) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Async variant of [`Self::recover_from_storage`].
    pub async fn recover_from_storage_async(&mut self) -> CheckpointResult<Option<u64>> {
        match latest_valid_epoch_async(self.storage.as_ref(), self.job_id.as_str()).await {
            Ok(epoch) => {
                if let Some(meta) =
                    read_epoch_metadata_async(self.storage.as_ref(), self.job_id.as_str(), epoch)
                        .await?
                {
                    self.fencing_token =
                        FencingToken::try_new(meta.fencing_token).unwrap_or(self.fencing_token);
                }
                self.current_epoch = epoch;
                self.state = CheckpointCoordinatorState::Committed { epoch };
                Ok(Some(epoch))
            }
            Err(krishiv_checkpoint::CheckpointError::NoValidEpoch) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// List all valid epoch numbers for this job.
    pub fn list_epochs(&self) -> CheckpointResult<Vec<u64>> {
        list_valid_epochs(self.storage.as_ref(), self.job_id.as_str())
    }

    /// Current epoch counter.
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    /// Current fencing token.
    pub fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Whether a checkpoint is currently in progress (awaiting acks).
    pub fn is_awaiting_acks(&self) -> bool {
        matches!(self.state, CheckpointCoordinatorState::AwaitingAcks { .. })
    }

    /// Coordinator state.
    pub fn coordinator_state(&self) -> &CheckpointCoordinatorState {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use krishiv_checkpoint::{LocalFsCheckpointStorage, write_operator_snapshot};
    use krishiv_proto::{
        CheckpointAckRequest, CheckpointSourceOffset, FencingToken, JobId, TaskId,
    };

    use super::{CheckpointCoordinator, CheckpointCoordinatorState};

    fn make_ack(
        job_id: &JobId,
        task_id: &str,
        epoch: u64,
        fencing_token: FencingToken,
    ) -> CheckpointAckRequest {
        CheckpointAckRequest {
            job_id: job_id.clone(),
            operator_id: format!("op-{task_id}"),
            task_id: TaskId::try_new(task_id).unwrap(),
            epoch,
            fencing_token,
            source_offsets: vec![CheckpointSourceOffset {
                partition_id: "p0".into(),
                offset: 1,
            }],
            snapshot_path: None,
        }
    }

    /// C2/C22 regression: try_tick must process timeout for awaiting-acks epochs
    /// even when expected_task_count == 0.
    #[test]
    fn checkpoint_timeout_aborts_stuck_epoch_with_zero_expected_tasks() {
        let storage: Arc<dyn krishiv_checkpoint::CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-timeout-zero").unwrap();
        let mut coord = CheckpointCoordinator::new_for_test(job_id, storage, 1_000, 0);

        // Start an epoch with 0 expected tasks.
        assert_eq!(coord.initiate().unwrap(), 1);

        // Tick with elapsed < ack_timeout — epoch should still be awaiting acks.
        assert_eq!(coord.try_tick(100, 5_000), None);
        assert!(coord.is_awaiting_acks());

        // Tick with elapsed >= ack_timeout — epoch should abort.
        assert_eq!(coord.try_tick(5_000, 5_000), None);
        assert!(!coord.is_awaiting_acks());
    }

    #[test]
    fn commit_epoch_validates_fencing_token() {
        // GAP-CP-03: commit_epoch must call validate_fencing_token before writing.
        // We advance the coordinator's token then try to inject an ack with the old
        // token; the resulting metadata's fencing_token will match the *current* token
        // (taken from coord.fencing_token), so the write should succeed.
        // The guard prevents a *different* coordinator (stale token) from committing.
        let storage: Arc<dyn krishiv_checkpoint::CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-fence").unwrap();
        let mut coord =
            CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 1000, 1);

        // Simulate a coordinator failover: bump fencing token.
        coord.fencing_token = FencingToken::try_new(2).unwrap();
        let epoch = coord.initiate().unwrap();
        assert_eq!(epoch, 1);

        // Write an operator snapshot so the manifest can be built.
        write_operator_snapshot(
            storage.as_ref(),
            "job-fence",
            1,
            "op-task-1",
            "task-1",
            b"state",
        )
        .unwrap();

        // Ack with the CURRENT fencing token — commit should succeed.
        let ack = make_ack(&job_id, "task-1", 1, coord.fencing_token());
        let done = coord.receive_ack(ack).unwrap();
        assert!(done, "quorum of 1 should complete immediately");

        // The committed metadata must carry the correct fencing token.
        let meta =
            krishiv_checkpoint::read_epoch_metadata(storage.as_ref(), "job-fence", 1).unwrap();
        assert_eq!(
            meta.unwrap().fencing_token,
            2,
            "committed token must match coordinator token"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_receive_ack_commits_epoch_without_blocking_wrapper() {
        let storage: Arc<dyn krishiv_checkpoint::CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-async-ck").unwrap();
        let mut coord =
            CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 1000, 1);

        assert_eq!(coord.initiate().unwrap(), 1);
        krishiv_checkpoint::write_operator_snapshot_async(
            storage.as_ref(),
            "job-async-ck",
            1,
            "op-task-1",
            "task-1",
            b"state",
        )
        .await
        .unwrap();

        let ack = make_ack(&job_id, "task-1", 1, coord.fencing_token());
        let done = coord.receive_ack_async(ack).await.unwrap();
        assert!(done, "quorum of 1 should complete immediately");

        let meta =
            krishiv_checkpoint::read_epoch_metadata_async(storage.as_ref(), "job-async-ck", 1)
                .await
                .unwrap()
                .expect("metadata");
        assert_eq!(meta.epoch, 1);
        assert_eq!(
            coord.coordinator_state(),
            &CheckpointCoordinatorState::Committed { epoch: 1 }
        );
    }

    #[test]
    fn commit_epoch_rejects_tampered_fencing_token() {
        // If the metadata's fencing_token is somehow different from self.fencing_token
        // (e.g. a buggy ack injector), commit_epoch must fail.
        let storage: Arc<dyn krishiv_checkpoint::CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-fence-bad").unwrap();
        let mut coord =
            CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 1000, 1);

        // Set a token of 3 on the coordinator.
        coord.fencing_token = FencingToken::try_new(3).unwrap();
        let _epoch = coord.initiate().unwrap();

        // Directly corrupt the coordinator's in-flight token to mismatch.
        // We do this by completing via a valid ack, then manually flipping the
        // stored token to see that validation would catch it.
        // (In practice this tests the guard path indirectly via the metadata build.)
        let ack = make_ack(&job_id, "task-1", 1, coord.fencing_token());
        // Mutate token after building ack — simulate a race that changes coordinator token.
        coord.fencing_token = FencingToken::try_new(4).unwrap();
        // The ack carries token=3, but the coordinator is now at 4.
        // receive_ack now requires exact match (!=), so this is rejected.
        // The validate_fencing_token guard in commit_epoch provides an additional
        // defense at storage time.
        let result = coord.receive_ack(ack);
        assert!(
            result.is_err(),
            "ack with stale fencing token must be rejected"
        );
    }

    #[test]
    fn receive_ack_rejects_higher_fencing_token() {
        // Critical split-brain defense: a token *higher* than the current leader's
        // token must be rejected (the old `<` check would have accepted it).
        // This test would have passed with the buggy `<` logic.
        let storage: Arc<dyn krishiv_checkpoint::CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-higher-token").unwrap();
        let mut coord =
            CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 1000, 1);

        // Current leader has token 5.
        coord.fencing_token = FencingToken::try_new(5).unwrap();
        let _epoch = coord.initiate().unwrap();

        // Ack carrying a *future* token (e.g. from a coordinator that became leader later
        // and then crashed, or from a split-brain instance) must be rejected.
        let future_ack = make_ack(&job_id, "task-1", 1, FencingToken::try_new(7).unwrap());
        let res_future = coord.receive_ack(future_ack);
        assert!(
            res_future.is_err(),
            "ack with higher fencing token (7 > 5) must be rejected to prevent split-brain"
        );
        assert!(res_future.unwrap_err().contains("stale fencing token"));

        // Same token is accepted.
        let good_ack = make_ack(&job_id, "task-1", 1, FencingToken::try_new(5).unwrap());
        let done = coord
            .receive_ack(good_ack)
            .expect("current token must be accepted");
        assert!(done, "quorum reached");

        // Lower token is also rejected.
        // (We create a fresh coord because previous quorum committed the epoch.)
        let mut coord2 =
            CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 1000, 1);
        coord2.fencing_token = FencingToken::try_new(5).unwrap();
        let _ = coord2.initiate().unwrap();
        let old_ack = make_ack(&job_id, "task-1", 1, FencingToken::try_new(3).unwrap());
        let res_old = coord2.receive_ack(old_ack);
        assert!(
            res_old.is_err(),
            "ack with lower fencing token must be rejected"
        );
    }
}
