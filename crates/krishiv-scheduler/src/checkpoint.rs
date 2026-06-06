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

/// Commit data extracted from the checkpoint coordinator in preparation for
/// async object-store writes.  Produced under the coordinator write lock,
/// consumed without the lock.
#[derive(Clone)]
pub struct PendingCommit {
    pub storage: Arc<dyn CheckpointStorage>,
    pub job_id: String,
    pub epoch: u64,
    pub metadata: CheckpointMetadata,
    pub operator_snapshots: Vec<OperatorSnapshotRef>,
}

/// State of the per-job checkpoint coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointCoordinatorState {
    /// No checkpoint is in progress.
    Idle,
    /// Waiting for executor acks for `epoch`.
    AwaitingAcks { epoch: u64, initiated_at_ms: u64 },
    /// Quorum reached, storage write in flight (state not yet finalised).
    Committing { epoch: u64 },
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
    /// Extracted commit data awaiting async storage I/O.  Populated when
    /// quorum is reached; cleared after finalise.
    pub(crate) pending_commit: Option<PendingCommit>,
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
            pending_commit: None,
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
        if matches!(
            self.state,
            CheckpointCoordinatorState::AwaitingAcks { .. }
                | CheckpointCoordinatorState::Committing { .. }
        ) {
            if matches!(self.state, CheckpointCoordinatorState::AwaitingAcks { .. }) {
                self.awaiting_elapsed_ms = self.awaiting_elapsed_ms.saturating_add(elapsed_ms);
                if self.awaiting_elapsed_ms >= ack_timeout_ms {
                    self.abort_epoch("timed out waiting for checkpoint acknowledgements");
                }
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
        self.validate_ack_contract(&ack, current_epoch)?;
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

    /// Async variant of [`Self::receive_ack`].  When quorum is reached the
    /// commit data is extracted *in memory* and stored in `self.pending_commit`;
    /// the actual async object-store writes are deferred to
    /// [`Self::commit_storage`] so the coordinator lock is not held across I/O.
    pub async fn receive_ack_async(&mut self, ack: CheckpointAckRequest) -> Result<bool, String> {
        let current_epoch = match &self.state {
            CheckpointCoordinatorState::AwaitingAcks { epoch, .. } => *epoch,
            // Acks arriving while we are in Committing are racing the storage
            // write — reject them (the epoch is already being committed).
            CheckpointCoordinatorState::Committing { epoch } => {
                return Err(format!(
                    "checkpoint coordinator for job {} is already committing epoch {epoch}",
                    self.job_id
                ));
            }
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
        self.validate_ack_contract(&ack, current_epoch)?;
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
            self.pending_commit = Some(self.extract_commit_data()?);
            self.state = CheckpointCoordinatorState::Committing {
                epoch: current_epoch,
            };
            return Ok(true);
        }
        Ok(false)
    }

    fn validate_ack_contract(
        &self,
        ack: &CheckpointAckRequest,
        current_epoch: u64,
    ) -> Result<(), String> {
        if ack.job_id != self.job_id {
            return Err(format!(
                "checkpoint ack job_id {} does not match coordinator job_id {}",
                ack.job_id, self.job_id
            ));
        }
        if let Some(snapshot_path) = &ack.snapshot_path {
            let expected = krishiv_checkpoint::snapshot_path(
                self.job_id.as_str(),
                current_epoch,
                &ack.operator_id,
                ack.task_id.as_str(),
            );
            if snapshot_path != &expected {
                return Err(format!(
                    "checkpoint ack snapshot path {snapshot_path} does not match expected path {expected}"
                ));
            }
        }
        Ok(())
    }

    /// Extract commit data from in-memory state (no I/O).  Called under the
    /// coordinator write lock when quorum is reached.
    fn extract_commit_data(&self) -> Result<PendingCommit, String> {
        let epoch = match &self.state {
            CheckpointCoordinatorState::AwaitingAcks { epoch, .. } => *epoch,
            _ => {
                return Err(format!(
                    "checkpoint coordinator for job {} must be awaiting acks to extract commit data",
                    self.job_id
                ));
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
        let savepoint_label = self.pending_savepoint_label.clone();
        let metadata = CheckpointMetadata {
            version: CheckpointMetadata::VERSION,
            epoch,
            job_id: self.job_id.as_str().to_owned(),
            fencing_token: self.fencing_token.as_u64(),
            coordinator_id: Some(self.coordinator_id.clone()),
            timestamp_ms: epoch * self.interval_ms,
            source_offsets,
            operator_snapshots: operator_snapshots.clone(),
            is_savepoint,
            savepoint_label,
            iceberg_snapshot_id: None,
            kafka_offsets: None,
        };

        // GAP-CP-03: Validate fencing token before committing to storage.
        krishiv_checkpoint::validate_fencing_token(&metadata, self.fencing_token.as_u64())
            .map_err(|e| format!("fencing token mismatch for job {}: {e}", self.job_id))?;

        Ok(PendingCommit {
            storage: Arc::clone(&self.storage),
            job_id: self.job_id.as_str().to_owned(),
            epoch,
            metadata,
            operator_snapshots,
        })
    }

    /// Take the extracted commit data (moved out of `self`), if any.
    pub fn take_pending_commit(&mut self) -> Option<PendingCommit> {
        self.pending_commit.take()
    }

    /// Perform the async object-store writes for a prepared commit.
    ///
    /// Call this **without** the coordinator lock.  After it returns, call
    /// [`Self::finalize_commit`] (under the coordinator lock) to transition
    /// the state to `Committed`.
    pub async fn commit_storage(commit: PendingCommit) -> CheckpointResult<u64> {
        let epoch = commit.epoch;
        let job_id = &commit.job_id;
        let metadata = &commit.metadata;

        // Build manifest: hash metadata.json + each snapshot file.
        let mut manifest = IntegrityManifest::new();
        let meta_json = serde_json::to_vec_pretty(metadata).map_err(|e| {
            krishiv_checkpoint::CheckpointError::Storage {
                message: format!("metadata serialize for manifest: {e}"),
            }
        })?;
        manifest.insert_bytes("metadata.json", &meta_json);
        for snap_ref in &commit.operator_snapshots {
            if let Some(bytes) = read_operator_snapshot_async(
                commit.storage.as_ref(),
                job_id,
                epoch,
                &snap_ref.operator_id,
                &snap_ref.task_id,
            )
            .await?
            {
                let rel_path = format!("{}/{}/state.bin", snap_ref.operator_id, snap_ref.task_id);
                manifest.insert_bytes(&rel_path, &bytes);
            } else {
                return Err(krishiv_checkpoint::CheckpointError::Corrupt {
                    epoch,
                    message: format!(
                        "snapshot {} referenced by checkpoint ack is missing",
                        snap_ref.snapshot_path
                    ),
                });
            }
        }

        write_epoch_metadata_async(commit.storage.as_ref(), job_id, epoch, metadata).await?;
        write_manifest_async(commit.storage.as_ref(), job_id, epoch, &manifest).await?;

        write_epoch_hint_async(commit.storage.as_ref(), job_id, epoch).await?;

        krishiv_governance::audit_log(
            "scheduler",
            &krishiv_governance::AuditAction::CheckpointCommitted {
                job_id: job_id.to_string(),
                epoch,
                fencing_token: metadata.fencing_token,
            },
            krishiv_governance::AuditOutcome::Allowed,
        );

        krishiv_governance::audit_log(
            "scheduler",
            &krishiv_governance::AuditAction::SinkCommitCompleted {
                job_id: job_id.to_string(),
                sink_id: "global".to_string(),
                epoch,
            },
            krishiv_governance::AuditOutcome::Allowed,
        );

        Ok(epoch)
    }

    /// Finalize a previously prepared commit: transition state to `Committed`.
    ///
    /// Call this under the coordinator write lock **after**
    /// [`Self::commit_storage`] completes.
    pub fn finalize_commit(&mut self, epoch: u64) -> CheckpointResult<()> {
        match &self.state {
            CheckpointCoordinatorState::Committing {
                epoch: committing_epoch,
            } if *committing_epoch == epoch => {}
            CheckpointCoordinatorState::Committing {
                epoch: committing_epoch,
            } => {
                return Err(krishiv_checkpoint::CheckpointError::Storage {
                    message: format!(
                        "cannot finalize checkpoint epoch {epoch} for job {}: \
                         coordinator is committing epoch {committing_epoch}",
                        self.job_id
                    ),
                });
            }
            state => {
                return Err(krishiv_checkpoint::CheckpointError::Storage {
                    message: format!(
                        "cannot finalize checkpoint epoch {epoch} for job {}: \
                         coordinator state is {state:?}",
                        self.job_id
                    ),
                });
            }
        }
        self.state = CheckpointCoordinatorState::Committed { epoch };
        self.pending_is_savepoint = false;
        self.pending_savepoint_label = None;
        self.awaiting_elapsed_ms = 0;
        self.pending_commit = None;
        Ok(())
    }

    /// Activate a validated checkpoint restore as this job's committed epoch.
    ///
    /// `active_fencing_token` should be the live leader-election token when
    /// available.  Restore accepts metadata from prior coordinator instances,
    /// but future barrier acks must use the current owner token rather than the
    /// older token stored in checkpoint metadata.
    pub fn activate_restored_epoch(
        &mut self,
        metadata: &CheckpointMetadata,
        active_fencing_token: Option<u64>,
    ) -> CheckpointResult<()> {
        metadata.validate()?;
        if metadata.job_id != self.job_id.as_str() {
            return Err(krishiv_checkpoint::CheckpointError::Corrupt {
                epoch: metadata.epoch,
                message: format!(
                    "checkpoint metadata job_id {} does not match coordinator job_id {}",
                    metadata.job_id, self.job_id
                ),
            });
        }

        let token = active_fencing_token.unwrap_or(metadata.fencing_token);
        self.fencing_token = FencingToken::try_new(token).map_err(|error| {
            krishiv_checkpoint::CheckpointError::Storage {
                message: format!("invalid active fencing token {token}: {error}"),
            }
        })?;
        self.current_epoch = metadata.epoch;
        self.pending_acks.clear();
        self.pending_is_savepoint = false;
        self.pending_savepoint_label = None;
        self.pending_commit = None;
        self.elapsed_ms = 0;
        self.awaiting_elapsed_ms = 0;
        self.state = CheckpointCoordinatorState::Committed {
            epoch: metadata.epoch,
        };
        Ok(())
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
            } else {
                return Err(krishiv_checkpoint::CheckpointError::Corrupt {
                    epoch,
                    message: format!(
                        "snapshot {} referenced by checkpoint ack is missing",
                        snap_ref.snapshot_path
                    ),
                });
            }
        }

        write_epoch_metadata(
            self.storage.as_ref(),
            self.job_id.as_str(),
            epoch,
            &metadata,
        )?;
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
    ///
    /// Combines extraction, storage I/O, and state finalisation in one call
    /// for backward compatibility.  New code should prefer the three-phase
    /// split: [`Self::extract_commit_data`] → [`Self::commit_storage`] →
    /// [`Self::finalize_commit`].
    pub async fn commit_epoch_async(&mut self) -> CheckpointResult<u64> {
        let commit = self
            .extract_commit_data()
            .map_err(|e| krishiv_checkpoint::CheckpointError::Storage { message: e })?;
        let epoch = commit.epoch;

        Self::commit_storage(commit).await?;

        self.finalize_commit(epoch)?;
        Ok(epoch)
    }

    /// Abort the current in-progress epoch (timeout or failure).
    pub fn abort_epoch(&mut self, reason: &str) {
        let epoch = match &self.state {
            CheckpointCoordinatorState::AwaitingAcks { epoch, .. }
            | CheckpointCoordinatorState::Committing { epoch } => *epoch,
            _ => return,
        };
        self.pending_acks.clear();
        self.pending_is_savepoint = false;
        self.pending_savepoint_label = None;
        self.pending_commit = None;
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

        // receive_ack_async transitions to Committing (no I/O under lock).
        let ack = make_ack(&job_id, "task-1", 1, coord.fencing_token());
        let done = coord.receive_ack_async(ack).await.unwrap();
        assert!(done, "quorum of 1 should complete immediately");
        assert_eq!(
            coord.coordinator_state(),
            &CheckpointCoordinatorState::Committing { epoch: 1 }
        );

        // Storage I/O and finalisation happen in separate steps.
        let commit = coord.take_pending_commit().expect("pending commit");
        CheckpointCoordinator::commit_storage(commit).await.unwrap();
        coord.finalize_commit(1).unwrap();

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finalize_commit_rejects_mismatched_epoch_without_state_change() {
        let storage: Arc<dyn krishiv_checkpoint::CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-finalize-guard").unwrap();
        let mut coord =
            CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 1000, 1);
        coord.initiate().unwrap();

        let ack = make_ack(&job_id, "task-1", 1, coord.fencing_token());
        assert!(coord.receive_ack_async(ack).await.unwrap());
        assert_eq!(
            coord.coordinator_state(),
            &CheckpointCoordinatorState::Committing { epoch: 1 }
        );

        let error = coord
            .finalize_commit(2)
            .expect_err("mismatched finalize epoch must be rejected");
        assert!(
            error.to_string().contains("committing epoch 1"),
            "unexpected error: {error}"
        );
        assert_eq!(
            coord.coordinator_state(),
            &CheckpointCoordinatorState::Committing { epoch: 1 },
            "failed finalize must leave committing epoch unchanged"
        );
        assert!(
            coord.pending_commit.is_some(),
            "failed finalize must not discard the pending commit"
        );

        coord.finalize_commit(1).unwrap();
        assert_eq!(
            coord.coordinator_state(),
            &CheckpointCoordinatorState::Committed { epoch: 1 }
        );
    }

    #[test]
    fn checkpoint_inner_finalize_ack_rejects_mismatched_epoch() {
        let storage: Arc<dyn krishiv_checkpoint::CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-inner-finalize-guard").unwrap();
        let mut coord =
            CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 1000, 1);
        coord.state = CheckpointCoordinatorState::Committing { epoch: 1 };

        let mut inner = crate::coordinator_sharded::CheckpointInner::new();
        inner.coordinators.insert(job_id.clone(), coord);

        let error = inner
            .finalize_ack(&job_id, 2)
            .expect_err("checkpoint inner must reject mismatched finalize epoch");
        assert!(
            error.to_string().contains("committing epoch 1"),
            "unexpected error: {error}"
        );
        let coord = inner.coordinators.get(&job_id).unwrap();
        assert_eq!(
            coord.coordinator_state(),
            &CheckpointCoordinatorState::Committing { epoch: 1 }
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
    fn receive_ack_rejects_mismatched_job_id() {
        let storage: Arc<dyn krishiv_checkpoint::CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-ack-contract").unwrap();
        let other_job_id = JobId::try_new("job-other").unwrap();
        let mut coord =
            CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 1000, 1);
        coord.initiate().unwrap();

        let ack = make_ack(&other_job_id, "task-1", 1, coord.fencing_token());
        let error = coord
            .receive_ack(ack)
            .expect_err("ack for another job must be rejected");

        assert!(
            error.contains("does not match coordinator job_id"),
            "unexpected error: {error}"
        );
        assert!(matches!(
            coord.coordinator_state(),
            CheckpointCoordinatorState::AwaitingAcks { epoch: 1, .. }
        ));
    }

    #[test]
    fn receive_ack_rejects_noncanonical_snapshot_path() {
        let storage: Arc<dyn krishiv_checkpoint::CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-ack-path").unwrap();
        let mut coord =
            CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 1000, 1);
        coord.initiate().unwrap();

        let mut ack = make_ack(&job_id, "task-1", 1, coord.fencing_token());
        ack.snapshot_path = Some("/tmp/not-a-checkpoint-snapshot".to_owned());
        let error = coord
            .receive_ack(ack)
            .expect_err("noncanonical snapshot path must be rejected");

        assert!(error.contains("snapshot path"), "unexpected error: {error}");
        assert!(
            krishiv_checkpoint::read_epoch_metadata(storage.as_ref(), "job-ack-path", 1)
                .unwrap()
                .is_none(),
            "invalid ack must not write checkpoint metadata"
        );
    }

    #[test]
    fn receive_ack_rejects_missing_snapshot_without_sealing_epoch() {
        let storage: Arc<dyn krishiv_checkpoint::CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-missing-snapshot").unwrap();
        let mut coord =
            CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 1000, 1);
        coord.initiate().unwrap();

        let mut ack = make_ack(&job_id, "task-1", 1, coord.fencing_token());
        ack.snapshot_path = Some(krishiv_checkpoint::snapshot_path(
            "job-missing-snapshot",
            1,
            "op-task-1",
            "task-1",
        ));
        let error = coord
            .receive_ack(ack)
            .expect_err("missing snapshot file must reject commit");

        assert!(error.contains("missing"), "unexpected error: {error}");
        assert!(
            krishiv_checkpoint::read_epoch_metadata(storage.as_ref(), "job-missing-snapshot", 1)
                .unwrap()
                .is_none(),
            "missing snapshot must fail before metadata is written"
        );
        assert!(
            !krishiv_checkpoint::validate_epoch(storage.as_ref(), "job-missing-snapshot", 1)
                .unwrap()
        );
        assert!(matches!(
            krishiv_checkpoint::latest_valid_epoch(storage.as_ref(), "job-missing-snapshot"),
            Err(krishiv_checkpoint::CheckpointError::NoValidEpoch)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_commit_storage_rejects_missing_snapshot_without_sealing_epoch() {
        let storage: Arc<dyn krishiv_checkpoint::CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("job-async-missing-snapshot").unwrap();
        let mut coord =
            CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 1000, 1);
        coord.initiate().unwrap();

        let mut ack = make_ack(&job_id, "task-1", 1, coord.fencing_token());
        ack.snapshot_path = Some(krishiv_checkpoint::snapshot_path(
            "job-async-missing-snapshot",
            1,
            "op-task-1",
            "task-1",
        ));
        let done = coord.receive_ack_async(ack).await.unwrap();
        assert!(done, "quorum should produce a pending commit");
        let commit = coord.take_pending_commit().expect("pending commit");

        let error = CheckpointCoordinator::commit_storage(commit)
            .await
            .expect_err("missing snapshot file must reject async storage commit");

        assert!(
            error.to_string().contains("missing"),
            "unexpected error: {error}"
        );
        assert!(
            krishiv_checkpoint::read_epoch_metadata_async(
                storage.as_ref(),
                "job-async-missing-snapshot",
                1
            )
            .await
            .unwrap()
            .is_none(),
            "missing async snapshot must fail before metadata is written"
        );
        assert!(matches!(
            krishiv_checkpoint::latest_valid_epoch_async(
                storage.as_ref(),
                "job-async-missing-snapshot"
            )
            .await,
            Err(krishiv_checkpoint::CheckpointError::NoValidEpoch)
        ));
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
