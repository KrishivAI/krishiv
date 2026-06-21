use super::*;

/// Maximum entries in checkpoint_notify_sent before old entries are evicted.
const MAX_CHECKPOINT_NOTIFY_ENTRIES: usize = 10_000;

/// Evict the oldest entries from a sent-tracking set once it exceeds the cap.
fn prune_sent_set(set: &mut indexmap::IndexSet<(JobId, ExecutorId, u64)>) {
    while set.len() > MAX_CHECKPOINT_NOTIFY_ENTRIES {
        let Some(oldest) = set.get_index(0).cloned() else {
            break;
        };
        set.shift_remove(&oldest);
    }
}

impl Coordinator {
    /// Route a checkpoint ack to the correct per-job coordinator.
    pub fn handle_checkpoint_ack(&mut self, ack: CheckpointAckRequest) -> CheckpointAckResponse {
        let commit_start = std::time::Instant::now();
        let (res, post_commit) = self.handle_checkpoint_ack_deferred(ack);
        if let Some((job_id, epoch)) = post_commit {
            self.on_checkpoint_epoch_committed(&job_id, epoch);
            let elapsed_secs = commit_start.elapsed().as_secs_f64();
            krishiv_metrics::global_metrics()
                .observe_checkpoint_commit_duration("post_commit", elapsed_secs);
        }
        res
    }

    /// In-memory checkpoint ack processing without the post-commit FS I/O.
    ///
    /// Returns `(response, Option<(job_id, epoch)>)` — when the second element
    /// is `Some`, the caller must invoke [`Self::on_checkpoint_epoch_committed`]
    /// **outside** any coordinator lock to avoid blocking heartbeats/submissions
    /// on filesystem I/O (savepoint preservation, stop-with-savepoint).
    ///
    /// This is the lock-safe entry point for async callers (barrier dispatch,
    /// gRPC) that cannot afford to hold the write lock during FS I/O.
    pub fn handle_checkpoint_ack_deferred(
        &mut self,
        ack: CheckpointAckRequest,
    ) -> (CheckpointAckResponse, Option<(JobId, u64)>) {
        tracing::debug!(
            job_id = %ack.job_id,
            epoch = ack.epoch,
            fencing_token = ack.fencing_token.as_u64(),
            "handling checkpoint ack (deferred)"
        );

        let job_id = ack.job_id.clone();

        let (res, post_commit) = match self.ckpt.coordinators.get_mut(&job_id) {
            None => (CheckpointAckResponse::JobNotFound, None),
            Some(coord) => {
                let coordinator_token = coord.fencing_token();
                if ack.fencing_token.as_u64() != coordinator_token.as_u64() {
                    return (
                        CheckpointAckResponse::StaleFencingToken {
                            current_token: coordinator_token.as_u64(),
                        },
                        None,
                    );
                }

                let current_epoch = coord.current_epoch();
                match coord.receive_ack(ack.clone()) {
                    Ok(true) => {
                        self.clear_checkpoint_notify_for_epoch(&job_id, ack.epoch);
                        CHECKPOINT_EPOCHS_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
                        record_checkpoint_epoch(job_id.as_str(), ack.epoch);
                        krishiv_metrics::global_metrics().inc_checkpoint_committed(job_id.as_str());
                        // Defer post-commit FS I/O to the caller.
                        (CheckpointAckResponse::Accepted, Some((job_id, ack.epoch)))
                    }
                    // Ack accepted but quorum not yet reached — this is NOT
                    // stale. Return Accepted so barrier fanout doesn't log
                    // spurious rejections for N-1 of N acks. Matches the async
                    // path (coordinator_sharded.rs handle_ack Ok(false)).
                    Ok(false) => (CheckpointAckResponse::Accepted, None),
                    Err(_) => (CheckpointAckResponse::StaleEpoch { current_epoch }, None),
                }
            }
        };

        self.exec.notify.notify_waiters();

        (res, post_commit)
    }

    /// Post-commit processing for a durably committed checkpoint epoch.
    ///
    /// 1. **Savepoint preservation**: epochs flagged `is_savepoint` are copied
    ///    into the durable `savepoints/` area, excluded from checkpoint
    ///    pruning/GC.  A copy failure is logged and leaves the committed epoch
    ///    intact — the savepoint can be re-created from it while it remains in
    ///    the active chain.
    /// 2. **Stop-with-savepoint**: when this epoch is the pending stop target
    ///    and the savepoint copy succeeded, the job is cancelled.  The cancel
    ///    is gated on the durable copy so a stop can never discard state.
    ///
    /// Called on both the sync and the async (gRPC three-phase) commit paths.
    pub fn on_checkpoint_epoch_committed(&mut self, job_id: &JobId, epoch: u64) {
        let mut savepoint_preserved = false;
        if let Some(coord) = self.ckpt.coordinators.get(job_id) {
            let storage = Arc::clone(coord.storage());
            match read_epoch_metadata(storage.as_ref(), job_id.as_str(), epoch) {
                Ok(Some(meta)) if meta.is_savepoint => {
                    match krishiv_state::checkpoint::create_savepoint_at_epoch(
                        storage.as_ref(),
                        job_id.as_str(),
                        epoch,
                        meta.savepoint_label.as_deref(),
                    ) {
                        Ok(_) => {
                            savepoint_preserved = true;
                            tracing::info!(
                                job_id = %job_id,
                                epoch,
                                label = meta.savepoint_label.as_deref().unwrap_or(""),
                                "savepoint epoch preserved in durable savepoints area"
                            );
                        }
                        Err(error) => {
                            tracing::error!(
                                job_id = %job_id,
                                epoch,
                                error = %error,
                                "failed to copy committed savepoint epoch into the \
                                 savepoints area; the epoch remains restorable from \
                                 the active checkpoint chain"
                            );
                        }
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::error!(
                        job_id = %job_id,
                        epoch,
                        error = %error,
                        "cannot read committed epoch metadata for savepoint check"
                    );
                }
            }
        }

        if self.ckpt.pending_stop_after_savepoint.get(job_id) == Some(&epoch) {
            if savepoint_preserved {
                self.ckpt.pending_stop_after_savepoint.remove(job_id);
                match self.cancel_job(job_id) {
                    Ok(()) => {
                        tracing::info!(
                            job_id = %job_id,
                            epoch,
                            "job stopped after savepoint epoch committed"
                        );
                    }
                    Err(error) => {
                        tracing::error!(
                            job_id = %job_id,
                            epoch,
                            error = %error,
                            "savepoint committed but job cancellation failed; \
                             retry the stop via cancel"
                        );
                    }
                }
            } else {
                self.ckpt.pending_stop_after_savepoint.remove(job_id);
                tracing::error!(
                    job_id = %job_id,
                    epoch,
                    "stop-with-savepoint aborted: savepoint epoch committed but \
                     durable preservation failed; job continues running"
                );
            }
        }
    }

    /// Async variant of [`Self::handle_checkpoint_ack`] for gRPC paths.
    ///
    /// Returns `(response, Some(pending_commit))` when quorum is reached and
    /// the caller should perform async storage writes without the coordinator
    /// lock before calling [`CheckpointCoordinator::finalize_commit`].
    pub async fn handle_checkpoint_ack_async(
        &mut self,
        ack: CheckpointAckRequest,
    ) -> (
        CheckpointAckResponse,
        Option<crate::checkpoint::PendingCommit>,
    ) {
        tracing::debug!(
            job_id = %ack.job_id,
            epoch = ack.epoch,
            fencing_token = ack.fencing_token.as_u64(),
            "handling checkpoint ack"
        );

        let job_id = ack.job_id.clone();

        let (res, pending) = match self.ckpt.coordinators.get_mut(&job_id) {
            None => (CheckpointAckResponse::JobNotFound, None),
            Some(coord) => {
                let coordinator_token = coord.fencing_token();
                if ack.fencing_token.as_u64() != coordinator_token.as_u64() {
                    return (
                        CheckpointAckResponse::StaleFencingToken {
                            current_token: coordinator_token.as_u64(),
                        },
                        None,
                    );
                }

                let is_quorum = coord.receive_ack_async(ack.clone()).await;
                // Release the borrow on `self.ckpt.coordinators` before
                // calling self.* methods.
                let (is_quorum, pending) = match is_quorum {
                    Ok(true) => (true, coord.take_pending_commit()),
                    Ok(false) => (false, None),
                    Err(_) => (false, None),
                };
                // coord borrow released — safe to call self.* now.
                if is_quorum {
                    self.clear_checkpoint_notify_for_epoch(&job_id, ack.epoch);
                    CHECKPOINT_EPOCHS_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
                    record_checkpoint_epoch(job_id.as_str(), ack.epoch);
                    krishiv_metrics::global_metrics().inc_checkpoint_committed(job_id.as_str());
                    (CheckpointAckResponse::Accepted, pending)
                } else {
                    // Ack accepted but quorum not yet reached — not stale.
                    // Matches the async sharded path (coordinator_sharded.rs).
                    (CheckpointAckResponse::Accepted, None)
                }
            }
        };

        self.exec.notify.notify_waiters();

        (res, pending)
    }

    /// Initiate a savepoint for a streaming job.
    ///
    /// Returns the savepoint epoch number.  Fails if no `CheckpointCoordinator`
    /// exists for this job (i.e. the job was not submitted with checkpoint config).
    pub fn savepoint_job(&mut self, job_id: &JobId, label: Option<String>) -> SchedulerResult<u64> {
        self.ensure_active()?;
        let running = self.running_task_count_for_job(job_id);
        match self.ckpt.coordinators.get_mut(job_id) {
            None => Err(SchedulerError::InvalidJob {
                message: format!(
                    "no checkpoint coordinator for job {job_id}; job must be streaming with checkpoint config"
                ),
            }),
            Some(coord) => {
                coord.set_expected_task_count(running.max(1));
                coord
                    .initiate_savepoint(label)
                    .map_err(|e| SchedulerError::InvalidJob { message: e })
            }
        }
    }

    /// Trigger a savepoint and stop the job once the savepoint epoch commits.
    ///
    /// The savepoint barrier flows through the job like a normal checkpoint;
    /// when all tasks ack and the epoch is durably committed *and* copied into
    /// the savepoints area, [`Self::on_checkpoint_epoch_committed`] cancels
    /// the job.  Returns the savepoint epoch.
    pub fn stop_job_with_savepoint(
        &mut self,
        job_id: &JobId,
        label: Option<String>,
    ) -> SchedulerResult<u64> {
        let epoch = self.savepoint_job(job_id, label)?;
        self.ckpt
            .pending_stop_after_savepoint
            .insert(job_id.clone(), epoch);
        Ok(epoch)
    }

    /// Restore a job from an immutable savepoint.
    ///
    /// Copies the savepoint epoch back into the active checkpoint chain
    /// (validating its manifest and fencing token), then activates the
    /// restore exactly like a checkpoint restore — including the executor
    /// restore directives.
    pub fn restore_job_from_savepoint(
        &mut self,
        job_id: &JobId,
        savepoint_epoch: u64,
        storage_path: &str,
        leader_fencing_token: Option<u64>,
    ) -> SchedulerResult<CheckpointMetadata> {
        let storage = Self::open_checkpoint_storage(storage_path)?;
        let current_token = leader_fencing_token
            .or_else(|| {
                self.ckpt
                    .coordinators
                    .get(job_id)
                    .map(|coord| coord.fencing_token().as_u64())
            })
            .ok_or_else(|| SchedulerError::InvalidJob {
                message: format!(
                    "cannot restore job {job_id} from savepoint: no fencing token available"
                ),
            })?;
        krishiv_state::checkpoint::restore_savepoint(
            storage.as_ref(),
            job_id.as_str(),
            savepoint_epoch,
            current_token,
        )
        .map_err(|e| SchedulerError::InvalidJob {
            message: format!(
                "cannot restore job {job_id} from savepoint epoch {savepoint_epoch}: {e}"
            ),
        })?;
        self.activate_job_restore_from_checkpoint_with_fencing(
            job_id,
            savepoint_epoch,
            storage_path,
            leader_fencing_token,
        )
    }

    /// List all valid checkpoint epochs for a job.
    pub fn list_job_checkpoints(&self, job_id: &JobId) -> SchedulerResult<Vec<u64>> {
        match self.ckpt.coordinators.get(job_id) {
            None => Ok(vec![]),
            Some(coord) => coord.list_epochs().map_err(|e| SchedulerError::InvalidJob {
                message: e.to_string(),
            }),
        }
    }

    /// Read and validate checkpoint metadata for `epoch` from `storage_path`.
    ///
    /// Returns the validated `CheckpointMetadata` so the caller can inspect
    /// source offsets and operator snapshots before resubmitting tasks.
    /// Rejects mismatched parallelism if the job is already tracked.
    pub fn restore_job_from_checkpoint(
        &self,
        job_id: &JobId,
        epoch: u64,
        storage_path: &str,
    ) -> SchedulerResult<CheckpointMetadata> {
        self.restore_job_from_checkpoint_with_fencing(job_id, epoch, storage_path, None)
    }

    /// Same as [`Self::restore_job_from_checkpoint`] but accepts an explicit
    /// current fencing token from the live leader-election backend.
    ///
    /// Distributed deployments MUST pass the live token (A8): when the
    /// in-memory `checkpoint_coordinators` map has not yet been rebuilt after
    /// a restart, this is the only place where stale-epoch restores can be
    /// rejected.
    pub fn restore_job_from_checkpoint_with_fencing(
        &self,
        job_id: &JobId,
        epoch: u64,
        storage_path: &str,
        leader_fencing_token: Option<u64>,
    ) -> SchedulerResult<CheckpointMetadata> {
        let metadata =
            self.validate_restore_metadata(job_id, epoch, storage_path, leader_fencing_token)?;
        // Parallelism check: read-only restores reject mismatched task counts;
        // the activating restore path redistributes state by key group instead.
        if let Ok(detail) = self.job_detail_snapshot(job_id) {
            let current_tasks = detail.job().task_count();
            let snapshot_tasks = metadata.operator_snapshots.len();
            if snapshot_tasks > 0 && current_tasks != snapshot_tasks {
                return Err(SchedulerError::InvalidJob {
                    message: format!(
                        "cannot restore job {job_id}: checkpoint has {snapshot_tasks} operator snapshots \
                         but job has {current_tasks} tasks; use the activating restore path which \
                         redistributes keyed state across the new parallelism"
                    ),
                });
            }
        }
        Ok(metadata)
    }

    /// Validate checkpoint metadata for restore without the parallelism check.
    fn validate_restore_metadata(
        &self,
        job_id: &JobId,
        epoch: u64,
        storage_path: &str,
        leader_fencing_token: Option<u64>,
    ) -> SchedulerResult<CheckpointMetadata> {
        let storage = Self::open_checkpoint_storage(storage_path)?;

        let meta = read_epoch_metadata(storage.as_ref(), job_id.as_str(), epoch).map_err(|e| {
            SchedulerError::InvalidJob {
                message: format!("cannot read checkpoint epoch {epoch}: {e}"),
            }
        })?;

        let meta = meta.ok_or_else(|| SchedulerError::InvalidJob {
            message: format!("checkpoint epoch {epoch} not found for job {job_id}"),
        })?;

        meta.validate().map_err(|e| SchedulerError::InvalidJob {
            message: format!("checkpoint epoch {epoch} metadata is incompatible: {e}"),
        })?;
        if meta.job_id != job_id.as_str() || meta.epoch != epoch {
            return Err(SchedulerError::InvalidJob {
                message: format!(
                    "checkpoint epoch {epoch} metadata mismatch for requested job {job_id}: \
                     metadata has job {} epoch {}",
                    meta.job_id, meta.epoch
                ),
            });
        }

        let epoch_is_valid =
            validate_epoch(storage.as_ref(), job_id.as_str(), epoch).map_err(|e| {
                SchedulerError::InvalidJob {
                    message: format!("checkpoint epoch {epoch} failed integrity check: {e}"),
                }
            })?;
        if !epoch_is_valid {
            return Err(SchedulerError::InvalidJob {
                message: format!("checkpoint epoch {epoch} failed integrity check"),
            });
        }

        // GAP-CK-01 / A8: prefer the in-memory checkpoint coordinator's token
        // when no live leader token was supplied.  A live leader-election
        // token is authoritative for gRPC/admin restore after failover; using
        // an older in-memory token would reject valid older checkpoints or
        // activate future work under the wrong owner generation.
        let token = leader_fencing_token.or_else(|| {
            self.ckpt
                .coordinators
                .get(job_id)
                .map(|coord| coord.fencing_token().as_u64())
        });
        if let Some(current_token) = token {
            if current_token == 0 {
                return Err(SchedulerError::InvalidJob {
                    message: format!(
                        "restore rejected for job {job_id}: fencing token is zero (no leader)"
                    ),
                });
            }
            validate_fencing_token_for_restore(&meta, current_token).map_err(|e| {
                SchedulerError::InvalidJob {
                    message: format!("restore rejected for job {job_id}: {e}"),
                }
            })?;
        } else if self.durability_profile.spec().requires_fencing
            || krishiv_common::profile_requires_fail_closed_metadata(self.durability_profile)
        {
            return Err(SchedulerError::InvalidJob {
                message: format!(
                    "restore rejected for job {job_id}: no live fencing token available (A8)"
                ),
            });
        } else {
            tracing::warn!(
                job_id = %job_id,
                epoch = epoch,
                "restoring checkpoint without fencing token validation; \
                 caller did not supply a leader token (A8)"
            );
        }

        Ok(meta)
    }

    /// Validate and activate a checkpoint restore for an already tracked job.
    ///
    /// This is the mutating counterpart to
    /// [`Self::restore_job_from_checkpoint_with_fencing`].  It updates the
    /// active checkpoint pointer and the in-memory checkpoint coordinator, and
    /// prunes later active checkpoint epochs so a subsequent restart cannot
    /// resurrect abandoned post-restore state by scanning storage.
    pub fn activate_job_restore_from_checkpoint_with_fencing(
        &mut self,
        job_id: &JobId,
        epoch: u64,
        storage_path: &str,
        leader_fencing_token: Option<u64>,
    ) -> SchedulerResult<CheckpointMetadata> {
        self.ensure_active()?;
        let metadata =
            self.validate_restore_metadata(job_id, epoch, storage_path, leader_fencing_token)?;

        let coord = self.ckpt.coordinators.get(job_id).ok_or_else(|| {
            SchedulerError::InvalidJob {
                message: format!(
                    "cannot activate restore for job {job_id}: no checkpoint coordinator is registered"
                ),
            }
        })?;
        if matches!(
            coord.coordinator_state(),
            CheckpointCoordinatorState::AwaitingAcks { .. }
                | CheckpointCoordinatorState::Committing { .. }
        ) {
            return Err(SchedulerError::InvalidJob {
                message: format!(
                    "cannot activate restore for job {job_id}: checkpoint epoch is in flight"
                ),
            });
        }

        // Rescale detection: when the job's current task set differs from the
        // checkpoint's snapshot set, redistribute keyed state by key group.
        let current_task_ids: Vec<TaskId> = self
            .job_detail_snapshot(job_id)
            .map(|detail| {
                let mut ids: Vec<TaskId> = detail
                    .stages()
                    .iter()
                    .flat_map(|stage| stage.tasks())
                    .map(|task| task.task_id().clone())
                    .collect();
                ids.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));
                ids
            })
            .unwrap_or_default();
        let needs_rescale = !metadata.operator_snapshots.is_empty()
            && !current_task_ids.is_empty()
            && metadata.operator_snapshots.len() != current_task_ids.len();

        let storage = Self::open_checkpoint_storage(storage_path)?;
        let valid_epochs =
            krishiv_state::checkpoint::list_valid_epochs(storage.as_ref(), job_id.as_str())
                .map_err(|e| SchedulerError::InvalidJob {
                    message: format!("cannot list checkpoint epochs for job {job_id}: {e}"),
                })?;
        for future_epoch in valid_epochs
            .into_iter()
            .filter(|candidate| *candidate > epoch)
        {
            krishiv_state::checkpoint::delete_epoch(
                storage.as_ref(),
                job_id.as_str(),
                future_epoch,
            )
            .map_err(|e| SchedulerError::InvalidJob {
                message: format!(
                    "cannot prune checkpoint epoch {future_epoch} after restoring job {job_id} \
                         to epoch {epoch}: {e}"
                ),
            })?;
        }

        let active_token = leader_fencing_token.unwrap_or(metadata.fencing_token);
        let (metadata, restored_epoch) = if needs_rescale {
            let rescaled = self.write_rescaled_epoch(
                storage.as_ref(),
                job_id,
                &metadata,
                &current_task_ids,
                active_token,
            )?;
            let rescaled_epoch = rescaled.epoch;
            (rescaled, rescaled_epoch)
        } else {
            krishiv_state::checkpoint::write_epoch_hint(storage.as_ref(), job_id.as_str(), epoch)
                .map_err(|e| SchedulerError::InvalidJob {
                message: format!("cannot activate checkpoint epoch {epoch} for job {job_id}: {e}"),
            })?;
            (metadata, epoch)
        };

        let coord = self.ckpt.coordinators.get_mut(job_id).ok_or_else(|| {
            SchedulerError::InvalidJob {
                message: format!(
                    "cannot activate restore for job {job_id}: no checkpoint coordinator is registered"
                ),
            }
        })?;
        coord
            .activate_restored_epoch(&metadata, leader_fencing_token)
            .map_err(|e| SchedulerError::InvalidJob {
                message: format!(
                    "cannot activate checkpoint epoch {restored_epoch} for job {job_id}: {e}"
                ),
            })?;
        let directive = RestoreDirective {
            epoch: restored_epoch,
            fencing_token: coord.fencing_token().as_u64(),
        };
        self.ckpt.notify_sent.retain(|(jid, _, _)| jid != job_id);
        self.ckpt.barrier_sent.retain(|(jid, _)| jid != job_id);
        // Every executor with tasks in this job must reload state and source
        // offsets from the restored epoch.
        self.set_restore_directive(job_id, directive);
        self.exec.notify.notify_waiters();

        Ok(metadata)
    }

    /// Redistribute a checkpoint's operator state across the job's current
    /// task set and seal it as a new epoch (`source epoch + 1`).
    ///
    /// Every old snapshot is decoded, its entries routed by key group to the
    /// new tasks (window-operator group keys are extracted from the state-key
    /// layout; watermarks are broadcast at the minimum), and the result is
    /// written, manifested, and hinted exactly like a committed checkpoint.
    /// Source offsets carry over unchanged — rescaling repartitions state, not
    /// source positions.
    fn write_rescaled_epoch(
        &self,
        storage: &dyn CheckpointStorage,
        job_id: &JobId,
        source: &CheckpointMetadata,
        current_task_ids: &[TaskId],
        active_fencing_token: u64,
    ) -> SchedulerResult<CheckpointMetadata> {
        use krishiv_state::checkpoint::{
            IntegrityManifest, OperatorSnapshotRef, write_epoch_hint, write_epoch_metadata,
            write_manifest, write_operator_snapshot,
        };

        let invalid = |message: String| SchedulerError::InvalidJob { message };

        // Read every old snapshot referenced by the source metadata.
        let mut old_snapshots = Vec::with_capacity(source.operator_snapshots.len());
        for snap in &source.operator_snapshots {
            let bytes = storage
                .read_bytes(&snap.snapshot_path)
                .map_err(|e| invalid(format!("rescale read {}: {e}", snap.snapshot_path)))?
                .ok_or_else(|| {
                    invalid(format!(
                        "rescale source snapshot {} is missing",
                        snap.snapshot_path
                    ))
                })?;
            old_snapshots.push(bytes);
        }

        let new_parallelism = u32::try_from(current_task_ids.len()).map_err(|_| {
            invalid(format!(
                "rescale target parallelism {} exceeds u32",
                current_task_ids.len()
            ))
        })?;
        let redistributed = krishiv_state::redistribute_snapshots(
            &old_snapshots,
            new_parallelism,
            krishiv_state::EntryRouting::WindowGroupKey,
        )
        .map_err(|e| invalid(format!("rescale redistribution for job {job_id}: {e}")))?;

        let rescaled_epoch = source.epoch.checked_add(1).ok_or_else(|| {
            invalid(format!(
                "rescale epoch overflow from epoch {}",
                source.epoch
            ))
        })?;

        // Write the redistributed per-task snapshots under the rescaled epoch.
        let mut new_refs: Vec<OperatorSnapshotRef> = Vec::new();
        let mut manifest = IntegrityManifest::new();
        for (task_id, bytes) in current_task_ids.iter().zip(redistributed.iter()) {
            if bytes.is_empty() {
                continue;
            }
            let operator_id = format!("operator-{}", task_id.as_str());
            write_operator_snapshot(
                storage,
                job_id.as_str(),
                rescaled_epoch,
                &operator_id,
                task_id.as_str(),
                bytes,
            )
            .map_err(|e| invalid(format!("rescale snapshot write for task {task_id}: {e}")))?;
            manifest.insert_bytes(
                format!("{}/{}/state.bin", operator_id, task_id.as_str()),
                bytes,
            );
            new_refs.push(OperatorSnapshotRef {
                operator_id: operator_id.clone(),
                task_id: task_id.as_str().to_owned(),
                snapshot_path: krishiv_state::checkpoint::snapshot_path(
                    job_id.as_str(),
                    rescaled_epoch,
                    &operator_id,
                    task_id.as_str(),
                ),
            });
        }

        let metadata = CheckpointMetadata {
            version: CheckpointMetadata::VERSION,
            epoch: rescaled_epoch,
            job_id: job_id.as_str().to_owned(),
            fencing_token: active_fencing_token,
            coordinator_id: Some(self.coordinator_id.as_str().to_owned()),
            timestamp_ms: u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0),
            source_offsets: source.source_offsets.clone(),
            operator_snapshots: new_refs,
            is_savepoint: false,
            savepoint_label: None,
            iceberg_snapshot_id: source.iceberg_snapshot_id,
            kafka_offsets: source.kafka_offsets.clone(),
        };
        let meta_json = serde_json::to_vec_pretty(&metadata)
            .map_err(|e| invalid(format!("rescale metadata serialize: {e}")))?;
        manifest.insert_bytes("metadata.json", &meta_json);

        write_epoch_metadata(storage, job_id.as_str(), rescaled_epoch, &metadata)
            .map_err(|e| invalid(format!("rescale metadata write: {e}")))?;
        write_manifest(storage, job_id.as_str(), rescaled_epoch, &manifest)
            .map_err(|e| invalid(format!("rescale manifest write: {e}")))?;
        // Hint last: seals the rescaled epoch (same ordering as commit_epoch).
        write_epoch_hint(storage, job_id.as_str(), rescaled_epoch)
            .map_err(|e| invalid(format!("rescale epoch hint write: {e}")))?;

        tracing::info!(
            job_id = %job_id,
            source_epoch = source.epoch,
            rescaled_epoch,
            old_parallelism = source.operator_snapshots.len(),
            new_parallelism = current_task_ids.len(),
            "redistributed checkpoint state across new parallelism"
        );

        Ok(metadata)
    }

    /// Record a restore directive for a job and reset its delivery tracking so
    /// every executor with tasks in the job receives it.
    pub(crate) fn set_restore_directive(&mut self, job_id: &JobId, directive: RestoreDirective) {
        self.ckpt
            .restore_directives
            .insert(job_id.clone(), directive);
        self.ckpt
            .restore_notify_sent
            .retain(|(jid, _, _)| jid != job_id);
        // Re-deliver the completion signal for epochs at or before the restore
        // point: an executor that restored may hold prepared transactions for
        // the restored epoch that must be committed (recover-and-commit).
        self.ckpt
            .checkpoint_complete_sent
            .retain(|(jid, _, _)| jid != job_id);
        self.exec.notify.notify_waiters();
    }

    /// Active restore directive for a job, if any.
    pub fn restore_directive(&self, job_id: &JobId) -> Option<RestoreDirective> {
        self.ckpt.restore_directives.get(job_id).copied()
    }

    // ── R6a: Out-of-band barrier trigger ──────────────────────────────────────

    /// Initiate a new checkpoint epoch for a streaming job and return one
    /// `InitiateCheckpointRequest` per currently running task.
    ///
    /// The caller is responsible for delivering each request to its executor
    /// (via gRPC or in-process simulation). Executors respond by calling
    /// `handle_checkpoint_ack()` on this coordinator.
    ///
    /// Returns `Err` if the job has no checkpoint coordinator (not a streaming
    /// job with checkpoint config) or if a checkpoint is already in flight.
    pub fn trigger_checkpoint_for_job(
        &mut self,
        job_id: &JobId,
    ) -> SchedulerResult<Vec<InitiateCheckpointRequest>> {
        // Validate job exists first.
        drop(self.find_job(job_id)?);

        let running = self.running_task_count_for_job(job_id);
        let coord =
            self.ckpt
                .coordinators
                .get_mut(job_id)
                .ok_or_else(|| SchedulerError::InvalidJob {
                    message: format!(
                        "no checkpoint coordinator for job {job_id}; \
                     job must be streaming with checkpoint_interval_ms set"
                    ),
                })?;
        coord.set_expected_task_count(running);

        let epoch = coord
            .initiate()
            .map_err(|msg| SchedulerError::InvalidJob { message: msg })?;
        let fencing_token = coord.fencing_token();

        // One broadcast request covers all executors for the job — the
        // coordinator doesn't need per-task granularity for the barrier trigger.
        // The executor processes the request once per running task internally.
        Ok(vec![InitiateCheckpointRequest {
            job_id: job_id.clone(),
            epoch,
            fencing_token,
        }])
    }

    /// Read-only access to the checkpoint coordinator for a specific job.
    pub fn checkpoint_coordinator(&self, job_id: &JobId) -> Option<&CheckpointCoordinator> {
        self.ckpt.coordinators.get(job_id)
    }

    /// Mirror all checkpoint-control state FROM a `CheckpointInner` back into
    /// the embedded `self.ckpt` (inner→outer full replace, in-process ack path).
    ///
    /// Preserves `self.ckpt.notify` so callers of `self.exec.notify.notify_waiters()`
    /// continue to use the correct Notify handle; all 7 data fields are replaced.
    pub(crate) fn apply_checkpoint_inner_sync(
        &mut self,
        inner: &crate::coordinator_sharded::CheckpointInner,
    ) {
        // Preserve the embedded notify handle; replace only data fields.
        let notify = self.ckpt.notify.clone();
        self.ckpt.coordinators.clone_from(&inner.coordinators);
        self.ckpt.notify_sent.clone_from(&inner.notify_sent);
        self.ckpt.barrier_sent.clone_from(&inner.barrier_sent);
        self.ckpt
            .checkpoint_complete_sent
            .clone_from(&inner.checkpoint_complete_sent);
        self.ckpt
            .restore_directives
            .clone_from(&inner.restore_directives);
        self.ckpt
            .restore_notify_sent
            .clone_from(&inner.restore_notify_sent);
        self.ckpt
            .pending_stop_after_savepoint
            .clone_from(&inner.pending_stop_after_savepoint);
        self.ckpt.notify = notify;
    }

    /// Mutable access to the checkpoint coordinator for a specific job.
    pub fn checkpoint_coordinator_mut(
        &mut self,
        job_id: &JobId,
    ) -> Option<&mut CheckpointCoordinator> {
        self.ckpt.coordinators.get_mut(job_id)
    }

    pub fn pending_initiate_checkpoints_for_executor(
        &mut self,
        executor_id: &ExecutorId,
    ) -> Vec<InitiateCheckpointCommand> {
        let mut out = Vec::new();
        for (job_id, coord) in &self.ckpt.coordinators {
            let epoch = match &coord.state {
                CheckpointCoordinatorState::AwaitingAcks { epoch, .. } => *epoch,
                _ => continue,
            };
            if !self.executor_has_running_task_in_job(executor_id, job_id) {
                continue;
            }
            let key = (job_id.clone(), executor_id.clone(), epoch);
            if self.ckpt.notify_sent.contains(&key) {
                continue;
            }
            out.push(InitiateCheckpointCommand {
                job_id: job_id.clone(),
                epoch,
                fencing_token: coord.fencing_token,
            });
            self.ckpt.notify_sent.insert(key);
            prune_sent_set(&mut self.ckpt.notify_sent);
        }
        out
    }

    /// Checkpoint-complete notifications to deliver to `executor_id` in its
    /// next heartbeat response.
    ///
    /// Emitted once per (job, executor, committed epoch) while the job's
    /// checkpoint coordinator rests on `Committed { epoch }`.  Missing the
    /// window before the next epoch initiates is safe: transactional sinks
    /// commit all prepared output at or before the *next* completed epoch
    /// (commit-through semantics), so completion is eventually delivered as
    /// long as checkpoints keep committing.
    pub fn pending_checkpoint_complete_for_executor(
        &mut self,
        executor_id: &ExecutorId,
    ) -> Vec<krishiv_proto::CheckpointCompleteCommand> {
        let mut out = Vec::new();
        for (job_id, coord) in &self.ckpt.coordinators {
            let Some(epoch) = coord.committed_epoch() else {
                continue;
            };
            if epoch == 0 || !self.executor_has_running_task_in_job(executor_id, job_id) {
                continue;
            }
            let key = (job_id.clone(), executor_id.clone(), epoch);
            if self.ckpt.checkpoint_complete_sent.contains(&key) {
                continue;
            }
            out.push(krishiv_proto::CheckpointCompleteCommand {
                job_id: job_id.clone(),
                epoch,
                fencing_token: coord.fencing_token,
            });
            self.ckpt.checkpoint_complete_sent.insert(key);
            prune_sent_set(&mut self.ckpt.checkpoint_complete_sent);
        }
        out
    }

    /// Restore directives to deliver to `executor_id` in its next heartbeat
    /// response.
    ///
    /// Targets executors with running *or* assigned tasks in the job so a
    /// reassigned executor can reload state before (or between) task cycles.
    pub fn pending_restore_commands_for_executor(
        &mut self,
        executor_id: &ExecutorId,
    ) -> Vec<krishiv_proto::RestoreFromCheckpointCommand> {
        let mut out = Vec::new();
        let directives: Vec<(JobId, RestoreDirective)> = self
            .ckpt
            .restore_directives
            .iter()
            .map(|(job_id, directive)| (job_id.clone(), *directive))
            .collect();
        for (job_id, directive) in directives {
            if !self.executor_has_active_task_in_job(executor_id, &job_id) {
                continue;
            }
            let key = (job_id.clone(), executor_id.clone(), directive.epoch);
            if self.ckpt.restore_notify_sent.contains(&key) {
                continue;
            }
            let Ok(fencing_token) = krishiv_proto::FencingToken::try_new(directive.fencing_token)
            else {
                tracing::error!(
                    job_id = %job_id,
                    epoch = directive.epoch,
                    "restore directive carries a zero fencing token; dropping"
                );
                continue;
            };
            out.push(krishiv_proto::RestoreFromCheckpointCommand {
                job_id: job_id.clone(),
                epoch: directive.epoch,
                fencing_token,
            });
            self.ckpt.restore_notify_sent.insert(key);
            prune_sent_set(&mut self.ckpt.restore_notify_sent);
        }
        out
    }

    /// Whether `executor_id` owns a running or assigned task in `job_id`.
    pub(crate) fn executor_has_active_task_in_job(
        &self,
        executor_id: &ExecutorId,
        job_id: &JobId,
    ) -> bool {
        self.job_coordinators
            .get(job_id)
            .map(|jc| jc.read_record())
            .is_some_and(|job| {
                job.stages.iter().any(|stage| {
                    stage.tasks().iter().any(|task| {
                        matches!(task.state(), TaskState::Running | TaskState::Assigned)
                            && task.assigned_executor() == Some(executor_id)
                    })
                })
            })
    }

    pub(crate) fn clear_checkpoint_notify_for_epoch(&mut self, job_id: &JobId, epoch: u64) {
        self.ckpt
            .notify_sent
            .retain(|(jid, _, ep)| jid != job_id || ep != &epoch);
    }
}
