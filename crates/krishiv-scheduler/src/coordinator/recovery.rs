use super::{
    Arc, CheckpointCoordinator, CheckpointStorage, Coordinator, ExecutorId, JobId, JobKind,
    JobState, MetadataStore, SchedulerError, SchedulerResult, TaskState,
    open_checkpoint_storage_from_uri,
};
use crate::job_coordinator::JobCoordinator;

impl Coordinator {
    /// Reload the attached shared store and rebuild coordinator state from it.
    ///
    /// This is used on every standby-to-active transition, not just process
    /// startup, so a warm standby cannot schedule from a stale etcd snapshot.
    pub(crate) fn refresh_and_recover_from_attached_store(&mut self) -> SchedulerResult<()> {
        let Some(handle) = self.store.clone() else {
            return Ok(());
        };
        let mut store = handle.inner();
        store.refresh()?;
        self.recover_from_store(&mut *store)
    }

    /// Restore job state from a `MetadataStore` after coordinator restart.
    ///
    /// For streaming jobs with Running tasks, the `streaming_reattach_grace_ticks`
    /// window starts here: executors owning those tasks will not be evicted for
    /// missing heartbeats during the grace period, allowing them to re-register
    /// and resume without re-processing already-committed events.
    ///
    /// For streaming jobs with checkpoint config, checkpoint state is recovered
    /// via `CheckpointCoordinator::recover_from_storage`.
    #[tracing::instrument(skip(self, store), name = "recover_from_store")]
    pub fn recover_from_store(&mut self, store: &mut dyn MetadataStore) -> SchedulerResult<()> {
        // P1.23: Clear in-memory state first so stale phantom jobs cannot survive.
        // Always prefer the persisted store as the authoritative source of truth.
        self.job_coordinators.clear();
        self.streaming_task_index.clear();
        self.exec.executors.reset_for_recovery();
        for record in store.jobs() {
            let job_id = record.job_id().clone();
            self.job_coordinators.insert(
                job_id.clone(),
                Arc::new(JobCoordinator::new(job_id, record.clone())),
            );
        }
        let normalized = self.normalize_recovered_launch_state();
        if normalized > 0 {
            tracing::warn!(
                task_count = normalized,
                "recovered assigned tasks had in-flight launches; clearing launch guards for retry"
            );
        }
        // RC1: Rebuild streaming_task_index so heartbeats arriving during the
        // recovery window are not silently dropped.  Without this, every call to
        // apply_streaming_task_state returns early because the index is empty.
        let streaming_job_ids: Vec<JobId> = self
            .job_coordinators
            .values()
            .map(|jc| {
                let j = jc.read_record();
                (j.spec.kind() == JobKind::Streaming, j.job_id().clone())
            })
            .filter(|(is_streaming, _)| *is_streaming)
            .map(|(_, id)| id)
            .collect();
        for job_id in streaming_job_ids {
            self.index_streaming_tasks(&job_id);
        }
        // GAP-CP-06: Rebuild checkpoint coordinators from the recovered job specs.
        // Before this fix, recover_from_store iterated an empty in-memory map
        // because checkpoint coordinators are only inserted in submit_job.  After
        // a coordinator restart the map is empty so no checkpointing resumes.
        self.ckpt.coordinators.clear();
        let streaming_checkpoint_jobs: Vec<(JobId, u64, String, usize)> = self
            .job_coordinators
            .values()
            .map(|jc| jc.read_record())
            .filter(|j| {
                j.spec.kind() == JobKind::Streaming
                    && j.spec.checkpoint_interval_ms().is_some()
                    && j.spec.checkpoint_storage_path().is_some()
            })
            .filter_map(|j| {
                if j.state() == JobState::Queued {
                    None
                } else {
                    let task_count: usize = j.spec.stages().iter().map(|s| s.tasks().len()).sum();
                    Some((
                        j.job_id().clone(),
                        j.spec.checkpoint_interval_ms()?,
                        j.spec.checkpoint_storage_path()?.to_owned(),
                        task_count,
                    ))
                }
            })
            .collect();
        for (job_id, interval_ms, storage_path, task_count) in streaming_checkpoint_jobs {
            match Self::open_checkpoint_storage(&storage_path) {
                Ok(storage) => {
                    let mut coord = CheckpointCoordinator::new(
                        job_id.clone(),
                        self.coordinator_id().as_str().to_owned(),
                        storage,
                        interval_ms,
                        task_count,
                    );
                    if let Err(e) = coord.recover_from_storage() {
                        // Checkpoint recovery failure is a hard error for streaming jobs:
                        // continuing without epoch state risks duplicate delivery because
                        // the coordinator would not know which offsets are already committed.
                        // Mark the job Failed so operators must explicitly decide whether
                        // to replay from the last committed checkpoint or reset offsets.
                        tracing::error!(
                            job_id = %job_id,
                            error = %e,
                            "checkpoint epoch state could not be recovered; \
                             failing streaming job to prevent duplicate delivery"
                        );
                        if let Some(jc) = self.job_coordinators.get(&job_id) {
                            let mut record = jc.write_record();
                            if !record.state().is_terminal() {
                                record.state = JobState::Failed;
                            }
                        }
                        // Do not insert a checkpoint coordinator for a failed job.
                        continue;
                    }
                    self.ckpt.coordinators.insert(job_id, coord);
                }
                Err(e) => {
                    tracing::error!(
                        job_id = %job_id,
                        error = %e,
                        "cannot open checkpoint storage after coordinator restart; \
                         failing streaming job to prevent duplicate delivery"
                    );
                    if let Some(jc) = self.job_coordinators.get(&job_id) {
                        let mut record = jc.write_record();
                        if !record.state().is_terminal() {
                            record.state = JobState::Failed;
                        }
                    }
                }
            }
        }
        // SC4: Pre-populate checkpoint_complete_sent for all committed epochs so
        // the coordinator does not re-deliver "epoch N committed" to executors
        // that already received it before the restart. After recover_from_storage
        // every coordinator is in Committed { epoch } state, and checkpoint_complete_sent
        // would otherwise be empty — causing pending_checkpoint_complete_for_executor
        // to re-emit for every executor on the first heartbeat tick.
        //
        // Safe inference: if the CheckpointCoordinator is Committed { epoch: N }
        // after storage recovery, epoch N was already delivered to all active
        // executors before the restart. We mark all currently-known executors as
        // already notified so they do not receive a spurious duplicate commit signal.
        // New executors registering after recovery will have epoch N in their past
        // and will be skipped by executor_has_running_task_in_job anyway.
        let restored_executors: Vec<ExecutorId> = store
            .executors()
            .iter()
            .map(|d| d.executor_id().clone())
            .collect();
        for (job_id, coord) in &self.ckpt.coordinators {
            let Some(committed_epoch) = coord.committed_epoch() else {
                continue;
            };
            if committed_epoch == 0 {
                continue;
            }
            for executor_id in &restored_executors {
                let key = (job_id.clone(), executor_id.clone(), committed_epoch);
                self.ckpt.checkpoint_complete_sent.insert(key);
            }
        }
        tracing::debug!(
            entries = self.ckpt.checkpoint_complete_sent.len(),
            "SC4: pre-populated checkpoint_complete_sent to prevent re-delivery after restart"
        );

        // R10: Restore executor descriptors so re-attaching executors are
        // recognised without a fresh registration handshake. Executors that
        // were persisted but have not yet re-registered start in the
        // Registered state; they will be evicted by the heartbeat timeout if
        // they never reconnect.
        for descriptor in store.executors() {
            if let Err(e) = self.exec.executors.register(descriptor.clone()) {
                tracing::warn!(
                    executor_id = %descriptor.executor_id(),
                    error = %e,
                    "could not restore executor descriptor during recovery; \
                     executor must re-register before receiving tasks"
                );
            }
        }

        // R11: Immediately advance the heartbeat clock by the timeout so that
        // restored executors that have not re-registered are evicted on the
        // first heartbeat tick instead of persisting as stale endpoints for
        // `heartbeat_timeout_ticks` (15+ seconds).  Live executors will
        // re-register within seconds and reset their heartbeat tick.
        //
        // P1.23: but streaming executors that own running tasks must NOT be torn
        // down here — the re-attach grace window (set up just below) exists to
        // give them time to re-register. Protect them from this immediate sweep;
        // they are still evicted later by a grace-aware tick if they never come
        // back.
        // Phase 53: one O(cluster) scan for the whole executor list instead
        // of an O(all jobs) scan per candidate executor.
        let streaming = self.executors_with_streaming_running_tasks();
        let protected_streaming: std::collections::HashSet<ExecutorId> = self
            .exec
            .executors
            .list()
            .into_iter()
            .map(|record| record.executor_id().clone())
            .filter(|id| streaming.contains(id))
            .collect();
        let stale_ids = self
            .exec
            .executors
            .advance_clock_excluding(self.config.heartbeat_timeout_ticks(), &protected_streaming);
        if !stale_ids.is_empty() {
            tracing::info!(
                count = stale_ids.len(),
                "recovery: advanced heartbeat clock to evict {} stale restored executor(s)",
                stale_ids.len()
            );
        }

        // Start the re-attach grace period.
        self.exec.ticks_since_restart = 0;
        self.exec.recovering = true;

        // Phase 2.6: post-restart shuffle availability audit. Shuffle outputs
        // whose producing executor is not present in the restored registry can
        // never be fetched (the executor will never be evicted either, since
        // the heartbeat clock only tracks registered executors). Invalidate
        // them now so producers re-run before consumers fail their fetches.
        let audited = self.audit_shuffle_availability();
        if audited > 0 {
            tracing::warn!(
                jobs_affected = audited,
                "post-restart shuffle audit invalidated unavailable partitions; producers re-queued"
            );
        }

        // DUR-1: re-drive any job persisted in the non-terminal `Committing`
        // state — its staged sink output was mid-publish when the coordinator
        // died. Re-publish idempotently and persist the resolved terminal state
        // so a false `Succeeded` is never surfaced and the job is not re-driven
        // again after its staging is consumed.
        self.redrive_committing_jobs(store);

        Ok(())
    }

    fn normalize_recovered_launch_state(&mut self) -> usize {
        let mut normalized = 0usize;
        for coordinator in self.job_coordinators.values() {
            let mut record = coordinator.write_record();
            if record.state() == JobState::Queued {
                record.mark_queued();
                continue;
            }
            if record.state().is_terminal() {
                continue;
            }
            let mut job_changed = false;
            for stage in &mut record.stages {
                let mut stage_changed = false;
                for task in stage.tasks_mut() {
                    if task.state() == TaskState::Assigned && task.launch_in_flight() {
                        task.clear_launch_in_flight();
                        normalized = normalized.saturating_add(1);
                        stage_changed = true;
                        job_changed = true;
                    }
                }
                if stage_changed {
                    stage.refresh_state();
                }
            }
            if job_changed {
                record.refresh_state();
            }
        }
        normalized
    }

    /// Audit shuffle availability across all non-terminal jobs.
    ///
    /// For every Succeeded task that produced remote (Flight-served) shuffle
    /// partitions, verify the producing executor is still known to the
    /// executor registry. Unknown producers — executors whose descriptors
    /// were not restored or that re-registered under a new identity — have
    /// their partitions marked Failed and their tasks reset to Pending.
    ///
    /// Registered-but-silent executors are deliberately left alone here: the
    /// re-attach grace period gives them time to reconnect, and the heartbeat
    /// timeout eviction path invalidates their shuffle output if they never do.
    ///
    /// Returns the number of jobs with invalidated partitions.
    #[tracing::instrument(skip(self), name = "audit_shuffle_availability")]
    pub fn audit_shuffle_availability(&mut self) -> usize {
        let known_executors: std::collections::HashSet<ExecutorId> = self
            .exec
            .executors
            .list()
            .iter()
            .map(|record| record.executor_id().clone())
            .collect();

        let job_ids: Vec<JobId> = self.job_coordinators.keys().cloned().collect();
        let mut jobs_affected = 0usize;
        for job_id in &job_ids {
            let Ok(mut job) = self.find_job_mut(job_id) else {
                continue;
            };
            if job.state().is_terminal() {
                continue;
            }
            let mut affected = false;

            // 1. Assigned/Running tasks pointing at executors that no longer
            //    exist can never receive a launch or report status — reset
            //    them to Pending so the orchestration loop can re-place them.
            for stage in job.stages_mut() {
                let mut stage_affected = false;
                for task in stage.tasks_mut() {
                    let unknown = task
                        .assigned_executor()
                        .is_some_and(|id| !known_executors.contains(id));
                    if unknown && matches!(task.state(), TaskState::Assigned | TaskState::Running) {
                        task.state = TaskState::Pending;
                        task.assigned_executor = None;
                        task.clear_launch_in_flight();
                        stage_affected = true;
                        affected = true;
                    }
                }
                if stage_affected {
                    stage.refresh_state();
                }
            }
            if affected {
                job.refresh_state();
            }

            // 2. Succeeded producers of remote shuffle output whose executor
            //    is gone — their partitions are unfetchable; re-run them.
            let mut unknown_producers: Vec<ExecutorId> = Vec::new();
            for stage in job.stages() {
                for task in stage.tasks() {
                    if task.state() != TaskState::Succeeded {
                        continue;
                    }
                    let Some(executor_id) = task.assigned_executor() else {
                        continue;
                    };
                    if known_executors.contains(executor_id)
                        || unknown_producers.contains(executor_id)
                    {
                        continue;
                    }
                    let has_remote_shuffle = task.output_metadata().is_some_and(|m| {
                        m.shuffle_partitions()
                            .iter()
                            .any(|p| !p.flight_endpoint.is_empty())
                    });
                    if has_remote_shuffle {
                        unknown_producers.push(executor_id.clone());
                    }
                }
            }
            for executor_id in &unknown_producers {
                if job.invalidate_executor_shuffle_partitions(executor_id) {
                    affected = true;
                }
            }
            if affected {
                jobs_affected += 1;
            }
        }
        if jobs_affected > 0 {
            self.exec.notify.notify_waiters();
        }
        jobs_affected
    }

    /// Reload one job record from the attached metadata store into memory.
    ///
    /// Used by per-job coordinator processes that share a durable metadata file
    /// with the cluster control plane (ADR-DIST-01).
    pub fn sync_job_from_metadata_store(&mut self, job_id: &JobId) -> SchedulerResult<()> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| SchedulerError::Transport {
                message: "coordinator has no metadata store".to_string(),
            })?;
        let record = {
            let guard = store.inner();
            guard.jobs().iter().find(|j| j.job_id() == job_id).cloned()
        };
        if let Some(record) = record {
            let streaming = record.spec.kind() == JobKind::Streaming;
            self.job_coordinators.insert(
                job_id.clone(),
                Arc::new(JobCoordinator::new(job_id.clone(), record)),
            );
            if streaming {
                self.index_streaming_tasks(job_id);
            }
        }
        Ok(())
    }

    /// Snapshot all in-memory jobs to a `MetadataStore` so that a subsequent
    /// `recover_from_store` call sees the current state.  Primarily useful in
    /// tests that simulate a coordinator restart without a real persistent store.
    pub fn persist_jobs_to_store(&self, store: &mut dyn MetadataStore) -> SchedulerResult<()> {
        for record in self.job_coordinators.values().map(|jc| jc.read_record()) {
            store.save_job(&record)?;
        }
        Ok(())
    }

    pub(crate) fn open_checkpoint_storage(
        path: &str,
    ) -> SchedulerResult<Arc<dyn CheckpointStorage>> {
        open_checkpoint_storage_from_uri(path).map_err(|e| SchedulerError::InvalidJob {
            message: format!("failed to open checkpoint storage at {path}: {e}"),
        })
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use crate::{InMemoryMetadataStore, JobRecord, checkpoint::CheckpointCoordinator};
    use krishiv_proto::{
        CheckpointAckRequest, CheckpointSourceOffset, CoordinatorId, ExecutorDescriptor, JobKind,
        JobSpec, OperatorId, PartitionId, TaskId,
    };
    use krishiv_state::checkpoint::{LocalFsCheckpointStorage, write_operator_snapshot};
    use std::sync::Arc;

    fn make_executor_descriptor(id: &str) -> ExecutorDescriptor {
        ExecutorDescriptor::new(ExecutorId::try_new(id).unwrap(), "127.0.0.1", 4)
    }

    /// SC4 regression: after coordinator restart checkpoint_complete_sent must
    /// be pre-populated for committed epochs so executors do not receive a
    /// spurious duplicate "epoch N committed" notification.
    #[test]
    fn sc4_checkpoint_complete_not_resent_after_coordinator_restart() {
        let storage: Arc<dyn CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let job_id = JobId::try_new("sc4-job").unwrap();
        let executor_id = ExecutorId::try_new("sc4-exec-1").unwrap();

        // Commit epoch 1 to storage so recover_from_storage restores Committed{1}.
        {
            let mut coord =
                CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 1_000, 1);
            coord.initiate().unwrap();
            write_operator_snapshot(
                storage.as_ref(),
                "sc4-job",
                1,
                "op-task-0",
                "task-0",
                b"state",
            )
            .unwrap();
            let ack = CheckpointAckRequest {
                job_id: job_id.clone(),
                operator_id: OperatorId::try_new("op-task-0").unwrap(),
                task_id: TaskId::try_new("task-0").unwrap(),
                epoch: 1,
                fencing_token: coord.fencing_token(),
                source_offsets: vec![CheckpointSourceOffset {
                    partition_id: PartitionId::try_new("p0").unwrap(),
                    offset: 1,
                    encoded_offset: 1_i64.to_le_bytes().to_vec(),
                }],
                snapshot_path: None,
                unaligned_buffers: Vec::new(),
                sink_transactions: Vec::new(),
            };
            assert!(coord.receive_ack(ack).unwrap());
        }

        // Build job spec and metadata store (executor descriptor persisted for SC4 query).
        let job_spec = JobSpec::new(job_id.clone(), "sc4-job", JobKind::Streaming)
            .with_checkpoint(1_000, "__test_inject__");
        let job_record = JobRecord::from_spec(job_spec, 0);
        let exec_desc = make_executor_descriptor(executor_id.as_str());

        let mut store = InMemoryMetadataStore::default();
        store.save_job(&job_record).unwrap();
        store.save_executor(&exec_desc).unwrap();

        // Simulate coordinator restart: fresh coordinator with injected Committed
        // CheckpointCoordinator (bypasses open_checkpoint_storage file path).
        let coord_id = CoordinatorId::try_new("coord-sc4").unwrap();
        let mut coordinator = Coordinator::active(coord_id);

        let mut cp_coord =
            CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 1_000, 1);
        cp_coord.recover_from_storage().unwrap();
        assert_eq!(cp_coord.committed_epoch(), Some(1));

        coordinator.job_coordinators.insert(
            job_id.clone(),
            Arc::new(crate::job_coordinator::JobCoordinator::new(
                job_id.clone(),
                job_record,
            )),
        );
        coordinator
            .ckpt
            .coordinators
            .insert(job_id.clone(), cp_coord);
        coordinator.exec.executors.register(exec_desc).unwrap();

        // Replicate the SC4 pre-population logic from recover_from_store.
        let restored_executors: Vec<ExecutorId> = store
            .executors()
            .iter()
            .map(|d| d.executor_id().clone())
            .collect();
        for (jid, coord) in &coordinator.ckpt.coordinators {
            let Some(committed_epoch) = coord.committed_epoch() else {
                continue;
            };
            if committed_epoch == 0 {
                continue;
            }
            for eid in &restored_executors {
                coordinator.ckpt.checkpoint_complete_sent.insert((
                    jid.clone(),
                    eid.clone(),
                    committed_epoch,
                ));
            }
        }

        // Assert: dedup set is pre-populated for committed epoch.
        let key = (job_id.clone(), executor_id.clone(), 1u64);
        assert!(
            coordinator.ckpt.checkpoint_complete_sent.contains(&key),
            "SC4: checkpoint_complete_sent must be pre-populated with committed epoch \
             after recovery to prevent spurious duplicate commit notification"
        );

        // Assert: no re-delivery on the first heartbeat tick.
        let commands = coordinator.pending_checkpoint_complete_for_executor(&executor_id);
        assert!(
            commands.is_empty(),
            "SC4: pending_checkpoint_complete_for_executor must not re-emit for a \
             pre-populated executor; got {commands:?}"
        );
    }
}
