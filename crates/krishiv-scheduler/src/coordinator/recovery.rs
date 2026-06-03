use super::*;
use crate::job_coordinator::JobCoordinator;

impl Coordinator {
    /// Restore job state from a `MetadataStore` after coordinator restart.
    ///
    /// For streaming jobs with Running tasks, the `streaming_reattach_grace_ticks`
    /// window starts here: executors owning those tasks will not be evicted for
    /// missing heartbeats during the grace period, allowing them to re-register
    /// and resume without re-processing already-committed events.
    ///
    /// For streaming jobs with checkpoint config, checkpoint state is recovered
    /// via `CheckpointCoordinator::recover_from_storage`.
    pub fn recover_from_store(&mut self, store: &dyn MetadataStore) -> SchedulerResult<()> {
        // P1.23: Clear in-memory state first so stale phantom jobs cannot survive.
        // Always prefer the persisted store as the authoritative source of truth.
        self.job_coordinators.clear();
        self.streaming_task_index.clear();
        for record in store.jobs() {
            let job_id = record.job_id().clone();
            self.job_coordinators.insert(
                job_id.clone(),
                Arc::new(JobCoordinator::new(job_id, record.clone())),
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
        self.checkpoint_coordinators.clear();
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
                let task_count: usize = j.spec.stages().iter().map(|s| s.tasks().len()).sum();
                Some((
                    j.job_id().clone(),
                    j.spec.checkpoint_interval_ms()?,
                    j.spec.checkpoint_storage_path()?.to_owned(),
                    task_count,
                ))
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
                        tracing::warn!(
                            job_id = %job_id,
                            error = %e,
                            "checkpoint epoch state could not be recovered; \
                             job will checkpoint from scratch (possible re-processing from last committed offset)"
                        );
                    }
                    self.checkpoint_coordinators.insert(job_id, coord);
                }
                Err(e) => {
                    tracing::warn!(
                        job_id = %job_id,
                        error = %e,
                        "cannot restore checkpoint coordinator (storage unavailable); job will checkpoint from scratch"
                    );
                }
            }
        }
        // R10: Restore executor descriptors so re-attaching executors are
        // recognised without a fresh registration handshake. Executors that
        // were persisted but have not yet re-registered start in the
        // Registered state; they will be evicted by the heartbeat timeout if
        // they never reconnect.
        for descriptor in store.executors() {
            if let Err(e) = self.executors.register(descriptor.clone()) {
                tracing::warn!(
                    executor_id = %descriptor.executor_id(),
                    error = %e,
                    "could not restore executor descriptor during recovery; \
                     executor must re-register before receiving tasks"
                );
            }
        }

        // Start the re-attach grace period.
        self.ticks_since_restart = 0;
        self.recovering = true;
        Ok(())
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
                Arc::new(JobCoordinator::new(job_id.clone(), record.clone())),
            );
            // Track B (two-tier): keep the JCP surface consistent when a dedicated
            // per-job coordinator syncs a single job record from shared metadata.
            if !self.job_coordinators.contains_key(job_id) {
                let jcp = crate::job_coordinator::JobCoordinator::new(job_id.clone(), record);
                self.job_coordinators.insert(job_id.clone(), Arc::new(jcp));
            }
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
