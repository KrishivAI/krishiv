use super::*;

/// Maximum entries in checkpoint_notify_sent before old entries are evicted.
const MAX_CHECKPOINT_NOTIFY_ENTRIES: usize = 10_000;

impl Coordinator {
    /// Route a checkpoint ack to the correct per-job coordinator.
    pub fn handle_checkpoint_ack(&mut self, ack: CheckpointAckRequest) -> CheckpointAckResponse {
        tracing::debug!(
            job_id = %ack.job_id,
            epoch = ack.epoch,
            fencing_token = ack.fencing_token.as_u64(),
            "handling checkpoint ack"
        );

        let job_id = ack.job_id.clone();

        let res = match self.checkpoint_coordinators.get_mut(&job_id) {
            None => CheckpointAckResponse::JobNotFound,
            Some(coord) => {
                let coordinator_token = coord.fencing_token();
                if ack.fencing_token.as_u64() != coordinator_token.as_u64() {
                    return CheckpointAckResponse::StaleFencingToken {
                        current_token: coordinator_token.as_u64(),
                    };
                }

                let current_epoch = coord.current_epoch();
                match coord.receive_ack(ack.clone()) {
                    Ok(true) => {
                        self.clear_checkpoint_notify_for_epoch(&job_id, ack.epoch);
                        CHECKPOINT_EPOCHS_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
                        record_checkpoint_epoch(job_id.as_str(), ack.epoch);
                        krishiv_metrics::global_metrics().inc_checkpoint_committed(job_id.as_str());
                        CheckpointAckResponse::Accepted
                    }
                    Ok(false) => CheckpointAckResponse::StaleEpoch { current_epoch },
                    Err(_) => CheckpointAckResponse::StaleEpoch { current_epoch },
                }
            }
        };

        self.notify.notify_waiters();

        res
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

        let (res, pending) = match self.checkpoint_coordinators.get_mut(&job_id) {
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
                let is_quorum = coord.receive_ack_async(ack.clone()).await;
                // Release the borrow on `self.checkpoint_coordinators` before
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
                    (CheckpointAckResponse::StaleEpoch { current_epoch }, None)
                }
            }
        };

        self.notify.notify_waiters();

        (res, pending)
    }

    /// Initiate a savepoint for a streaming job.
    ///
    /// Returns the savepoint epoch number.  Fails if no `CheckpointCoordinator`
    /// exists for this job (i.e. the job was not submitted with checkpoint config).
    pub fn savepoint_job(&mut self, job_id: &JobId, label: Option<String>) -> SchedulerResult<u64> {
        self.ensure_active()?;
        let running = self.running_task_count_for_job(job_id);
        let res = match self.checkpoint_coordinators.get_mut(job_id) {
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
        };

        if res.is_ok() {
            krishiv_plan::governance::audit_log(
                "scheduler",
                &krishiv_plan::governance::AuditAction::SavepointCreated {
                    job_id: job_id.to_string(),
                },
                krishiv_plan::governance::AuditOutcome::Allowed,
            );
        }
        res
    }

    /// List all valid checkpoint epochs for a job.
    pub fn list_job_checkpoints(&self, job_id: &JobId) -> SchedulerResult<Vec<u64>> {
        match self.checkpoint_coordinators.get(job_id) {
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
            self.checkpoint_coordinators
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

        // Parallelism check: if the job is already tracked, reject mismatched task count.
        if let Ok(detail) = self.job_detail_snapshot(job_id) {
            let current_tasks = detail.job().task_count();
            let snapshot_tasks = meta.operator_snapshots.len();
            if snapshot_tasks > 0 && current_tasks != snapshot_tasks {
                return Err(SchedulerError::InvalidJob {
                    message: format!(
                        "cannot restore job {job_id}: checkpoint has {snapshot_tasks} operator snapshots \
                         but job has {current_tasks} tasks; rescaling requires a savepoint + resubmit with matching parallelism"
                    ),
                });
            }
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
        let metadata = self.restore_job_from_checkpoint_with_fencing(
            job_id,
            epoch,
            storage_path,
            leader_fencing_token,
        )?;

        let coord = self.checkpoint_coordinators.get(job_id).ok_or_else(|| {
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

        let storage = Self::open_checkpoint_storage(storage_path)?;
        let valid_epochs = krishiv_state::checkpoint::list_valid_epochs(storage.as_ref(), job_id.as_str())
            .map_err(|e| SchedulerError::InvalidJob {
                message: format!("cannot list checkpoint epochs for job {job_id}: {e}"),
            })?;
        for future_epoch in valid_epochs
            .into_iter()
            .filter(|candidate| *candidate > epoch)
        {
            krishiv_state::checkpoint::delete_epoch(storage.as_ref(), job_id.as_str(), future_epoch)
                .map_err(|e| SchedulerError::InvalidJob {
                    message: format!(
                        "cannot prune checkpoint epoch {future_epoch} after restoring job {job_id} \
                         to epoch {epoch}: {e}"
                    ),
                })?;
        }
        krishiv_state::checkpoint::write_epoch_hint(storage.as_ref(), job_id.as_str(), epoch).map_err(
            |e| SchedulerError::InvalidJob {
                message: format!("cannot activate checkpoint epoch {epoch} for job {job_id}: {e}"),
            },
        )?;

        let coord = self.checkpoint_coordinators.get_mut(job_id).ok_or_else(|| {
            SchedulerError::InvalidJob {
                message: format!(
                    "cannot activate restore for job {job_id}: no checkpoint coordinator is registered"
                ),
            }
        })?;
        coord
            .activate_restored_epoch(&metadata, leader_fencing_token)
            .map_err(|e| SchedulerError::InvalidJob {
                message: format!("cannot activate checkpoint epoch {epoch} for job {job_id}: {e}"),
            })?;
        self.checkpoint_notify_sent
            .retain(|(jid, _, _)| jid != job_id);
        self.barrier_dispatch_sent.retain(|(jid, _)| jid != job_id);
        self.notify.notify_waiters();

        krishiv_plan::governance::audit_log(
            "scheduler",
            &krishiv_plan::governance::AuditAction::SavepointRestored {
                job_id: job_id.to_string(),
                epoch,
            },
            krishiv_plan::governance::AuditOutcome::Allowed,
        );

        Ok(metadata)
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
        let coord = self
            .checkpoint_coordinators
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
        self.checkpoint_coordinators.get(job_id)
    }

    /// Snapshot checkpoint-owned state for initializing a sharded checkpoint inner.
    pub fn checkpoint_inner_parts(
        &self,
    ) -> (
        std::collections::HashMap<JobId, CheckpointCoordinator>,
        indexmap::IndexSet<(JobId, ExecutorId, u64)>,
        std::collections::HashSet<(JobId, u64)>,
    ) {
        (
            self.checkpoint_coordinators.clone(),
            self.checkpoint_notify_sent.clone(),
            self.barrier_dispatch_sent.clone(),
        )
    }

    /// Mutable access to the checkpoint coordinator for a specific job.
    pub fn checkpoint_coordinator_mut(
        &mut self,
        job_id: &JobId,
    ) -> Option<&mut CheckpointCoordinator> {
        self.checkpoint_coordinators.get_mut(job_id)
    }

    pub fn pending_initiate_checkpoints_for_executor(
        &mut self,
        executor_id: &ExecutorId,
    ) -> Vec<InitiateCheckpointCommand> {
        let mut out = Vec::new();
        for (job_id, coord) in &self.checkpoint_coordinators {
            let epoch = match &coord.state {
                CheckpointCoordinatorState::AwaitingAcks { epoch, .. } => *epoch,
                _ => continue,
            };
            if !self.executor_has_running_task_in_job(executor_id, job_id) {
                continue;
            }
            let key = (job_id.clone(), executor_id.clone(), epoch);
            if self.checkpoint_notify_sent.contains(&key) {
                continue;
            }
            out.push(InitiateCheckpointCommand {
                job_id: job_id.clone(),
                epoch,
                fencing_token: coord.fencing_token,
            });
            self.checkpoint_notify_sent.insert(key);
            while self.checkpoint_notify_sent.len() > MAX_CHECKPOINT_NOTIFY_ENTRIES {
                let Some(oldest) = self.checkpoint_notify_sent.get_index(0).cloned() else {
                    break;
                };
                self.checkpoint_notify_sent.shift_remove(&oldest);
            }
        }
        out
    }

    pub(crate) fn clear_checkpoint_notify_for_epoch(&mut self, job_id: &JobId, epoch: u64) {
        self.checkpoint_notify_sent
            .retain(|(jid, _, ep)| jid != job_id || ep != &epoch);
    }
}
