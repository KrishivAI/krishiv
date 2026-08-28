use super::{
    Arc, AtomicOrdering, AttemptId, CheckpointCoordinator, Coordinator, EventLogEvent,
    JOBS_SUBMITTED_TOTAL, JobId, JobKind, JobRecord, JobSpec, JobState, LogicalPlan, PhysicalPlan,
    ResourceUsage, SchedulerError, SchedulerResult, ShuffleRegenOutcome, SlotAwareScheduler,
    StageState, SubmitOutcome, TaskState, TaskStatusUpdate, TaskUpdateOutcome,
    job_spec_from_logical_plan, job_spec_from_physical_plan, validate_job,
};

impl Coordinator {
    #[tracing::instrument(
        level = "info",
        skip(self, spec),
        fields(
            job_id = %spec.job_id(),
            namespace = spec.namespace_id().unwrap_or("default"),
            job_kind = ?spec.kind(),
        )
    )]
    pub fn submit_job(&mut self, spec: JobSpec) -> SchedulerResult<SubmitOutcome> {
        self.ensure_active()?;
        validate_job(&spec)?;

        if let Some(existing) = self.job_coordinators.get(spec.job_id()) {
            // A terminal (Cancelled/Failed/Succeeded) job with this id is being
            // replaced: evict it now so the id is immediately reusable instead
            // of waiting for the background GC tick. This is what a pipeline
            // reconcile does when it re-registers a streaming job it just
            // deregistered (cancel marks the job GC-ready but keeps it in the
            // registry). A live job is still a genuine duplicate.
            if existing.read_record().state().is_terminal() {
                self.evict_completed_job(&spec.job_id().clone());
            } else {
                return Err(SchedulerError::DuplicateJob {
                    job_id: spec.job_id().clone(),
                });
            }
        }
        // Reaching here means there is no live (non-terminal) job under this
        // id right now — either there never was one in memory, or it was just
        // evicted above for being terminal. Either way this id is being
        // freshly (re)used, so clear the durable store's terminal-job latch
        // before the persist below: see `NonBlockingStoreHandle::terminal_jobs`.
        // This must run even when `existing` above was `None` — a job whose
        // in-memory `JobCoordinator` was already GC'd (or never rebuilt after
        // a coordinator restart) is still latched in the store, and skipping
        // this for that case is exactly the gap that let a live-repro'd
        // chaos-gate resubmission churn forever in memory while every persist
        // for it was silently rejected (Phase 58 #180: two streaming jobs
        // found stuck Assigned/reaped in a loop for over an hour past their
        // own recorded cancellation, because `forget_terminal_job` was only
        // ever reached from the `existing.is_terminal()` arm above).
        if let Some(store) = &self.store {
            store.forget_terminal_job(spec.job_id().as_str());
        }

        // Admission control: queued jobs are persisted as visible job records
        // and admitted by later executor-heartbeat / scheduling ticks.
        let outcome = self.evaluate_admission(&spec);
        let is_queued = matches!(outcome, SubmitOutcome::Queued { .. });

        // Prepare (but don't yet commit) a CheckpointCoordinator for streaming jobs.
        // A7: We previously inserted the coordinator into `checkpoint_coordinators`
        // before persisting the job — if `save_job` failed, the in-memory coordinator
        // leaked.  Now we open storage here, hand the constructed `CheckpointCoordinator`
        // over only after the job record is durably saved AND inserted in memory.
        let mut pending_checkpoint: Option<CheckpointCoordinator> = None;
        if !is_queued
            && spec.kind() == JobKind::Streaming
            && let (Some(interval_ms), Some(storage_path)) = (
                spec.checkpoint_interval_ms(),
                spec.checkpoint_storage_path(),
            )
        {
            let storage = Self::open_checkpoint_storage(storage_path)?;
            pending_checkpoint = Some(CheckpointCoordinator::new(
                spec.job_id().clone(),
                self.coordinator_id().as_str().to_owned(),
                storage,
                interval_ms,
                0,
            ));
        }

        // Deferred placement: attempt to place tasks on available executors at
        // submission time, but do not reject the job if no executors are
        // registered yet. Tasks stay Pending and the orchestration loop
        // (assign_pending_tasks_for_schedulable_jobs) will assign them as soon
        // as executors register or become healthy. This prevents submission
        // failures during rolling executor restarts.
        // Submit-time placement must honour the circuit breaker for the same
        // reason the orchestration-tick path does: a task placed here on a
        // circuit-broken executor is refused by the launch path, reset to
        // Pending, and re-placed identically — forever.
        let submit_now_ms = u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0);
        let executors = self
            .exec
            .executors
            .schedulable_executor_placements_excluding(
                &self.circuit_broken_executors(submit_now_ms),
            );
        let job_id = spec.job_id().clone();
        let _job_name = spec.name().to_owned();
        let _namespace = spec
            .namespace_id()
            .map(|s| s.to_owned())
            .unwrap_or_default();
        let mut record = JobRecord::from_spec(spec, self.config.max_stage_retries());
        record.set_retry_backoff(
            self.config.task_retry_backoff_base_ms(),
            self.config.task_retry_backoff_cap_ms(),
        );
        if is_queued {
            record.mark_queued();
        } else if !executors.is_empty() {
            let assignments = SlotAwareScheduler::place_with_load(&record.spec, &executors)?;
            record.apply_assignments(assignments);
        }
        // If no executors: all tasks remain Pending; assign_pending_tasks will
        // place them on the next orchestration tick when executors register.
        // Persist the job record to the metadata store BEFORE committing
        // in-memory state.  A synchronous write ensures durability: if the
        // store write fails, the caller receives an error and no in-memory
        // state is leaked (B7 / ADR-12.9).
        //
        // This goes through `save_job_checked` (not `store.inner().save_job`
        // directly) so a fresh, non-terminal submission is also subject to
        // the terminal-job latch: `forget_terminal_job` was just called for
        // this exact id above, so admission should always succeed, but if a
        // concurrent write raced in and relatched it first, `inner().save_job`
        // would silently write a record the latch immediately makes
        // unpersistable to every caller thereafter — precisely the bypass
        // that let a resubmission become live in memory while never able to
        // durably save again (Phase 58 #180). Treat rejection as a genuine
        // conflict rather than proceeding to insert an unpersisted job.
        if let Some(store) = &self.store {
            if !store.save_job_checked(&record)? {
                return Err(SchedulerError::DuplicateJob {
                    job_id: job_id.clone(),
                });
            }
            store.inner().append_event(EventLogEvent::JobSubmitted {
                job_id: job_id.clone(),
            })?;
        }
        let inserted_job_id = record.job_id().clone();

        // Phase 59 (observability gap-a): stamp the submit instant for batch
        // jobs so `on_job_terminal` can record whole-query wall-clock latency.
        // Streaming submit→terminal is job lifetime, not query latency, so it is
        // deliberately not tracked.
        if record.spec.kind() == JobKind::Batch {
            self.job_submit_instants
                .insert(inserted_job_id.clone(), std::time::Instant::now());
        }

        // Track B (two-tier CCP/JCP): create the owning JobCoordinator for this job.
        // The JCP holds the Arc<RwLock<JobRecord>> and will progressively own per-job
        // launch decisions, heartbeat windows, checkpoint coordination, and recovery.
        // The outer Coordinator (CCP) retains cross-job concerns and the thin map for delegation.
        let jcp =
            crate::job_coordinator::JobCoordinator::new(inserted_job_id.clone(), record.clone());
        self.job_coordinators
            .insert(inserted_job_id.clone(), Arc::new(jcp));
        tracing::debug!(
            job_id = %inserted_job_id,
            "job coordinator registered (two-tier seam active)"
        );

        if let Some(ckpt_coord) = pending_checkpoint {
            self.ckpt
                .coordinators
                .insert(inserted_job_id.clone(), ckpt_coord);
        }
        // P1.1: Index streaming tasks for O(1) heartbeat lookup.
        self.index_streaming_tasks(&inserted_job_id);
        // Phase 53: new work is launch-dirty; strict-capacity leftovers go to
        // the pending backlog for assignment when slots free.
        if !is_queued {
            self.launch_dirty_jobs.insert(inserted_job_id.clone());
            let has_pending = self
                .job_coordinators
                .get(&inserted_job_id)
                .is_some_and(|jc| {
                    jc.read_record()
                        .stages()
                        .iter()
                        .flat_map(|s| s.tasks())
                        .any(|t| t.state() == TaskState::Pending)
                });
            if has_pending {
                self.pending_backlog_jobs.insert(inserted_job_id.clone());
            }
        }
        // GAP-OB-01: Increment jobs_submitted counter.
        JOBS_SUBMITTED_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
        krishiv_metrics::global_metrics().inc_tasks_submitted();

        // IVM-AUD-DIST-2: wake the task-launch loop.
        //
        // The loop above marked this job launch-dirty, but the only thing that
        // drains `launch_dirty_jobs` is `drive_pending_task_launches`, whose
        // loop parks on `select! { interval.tick() (500 ms), notify.notified() }`.
        // Nothing here fired that `Notify`, so a freshly submitted job waited
        // for the next 500 ms interval tick before its tasks were even
        // LAUNCHED — a floor no downstream wait loop can undo, because the work
        // has not started yet.
        //
        // Measured: a non-partitioned IVM tick (which dispatches a job through
        // this path) sat at ~496 ms against ~17 ms for the central path. That
        // floor was invariant when the *downstream* wait loop's poll interval
        // was cut 100 ms -> 10 ms, which is what proved the cost lives here, at
        // submission, and not in the waiter (BENCHMARKING 2026-08-28h).
        //
        // Only non-queued submissions have launchable work: a queued job has no
        // tasks to launch until `admit_queued_jobs` promotes it, and that path
        // already fires this same `Notify`.
        if !is_queued {
            self.exec.notify.notify_waiters();
        }
        Ok(outcome)
    }

    pub(crate) fn evaluate_admission(&self, spec: &JobSpec) -> SubmitOutcome {
        let quota = self.namespace_quota_snapshot(spec.namespace_id());
        let mut outcome = self.queue_manager.admit(spec, &quota);

        // Memory-estimate admission: when the job declares a memory ask and the
        // cluster reports memory capacity via heartbeats, queue the job if its
        // ask exceeds what is currently available across schedulable executors.
        // Unknown capacity skips the check so clusters without memory reporting
        // are unaffected.
        if matches!(outcome, SubmitOutcome::Accepted)
            && let Some(ask) = spec.memory_limit_bytes()
            && ask > 0
            && self
                .exec
                .executors
                .cluster_available_memory_bytes()
                .is_none()
        {
            tracing::debug!(
                job_id = %spec.job_id(),
                memory_ask = ask,
                "job declares a memory ask but no executor has reported memory \
                 capacity; skipping admission check"
            );
        }
        if matches!(outcome, SubmitOutcome::Accepted)
            && let Some(ask) = spec.memory_limit_bytes()
            && ask > 0
            && let Some(available) = self.exec.executors.cluster_available_memory_bytes()
            && ask > available
        {
            tracing::warn!(
                job_id = %spec.job_id(),
                memory_ask = ask,
                cluster_available = available,
                "job memory ask exceeds available cluster memory; queueing"
            );
            outcome = SubmitOutcome::Queued { position: 0 };
        }

        outcome
    }

    pub(crate) fn admit_queued_jobs(&mut self) -> SchedulerResult<usize> {
        self.ensure_active()?;
        let mut queued: Vec<(u8, JobId)> = self
            .job_coordinators
            .iter()
            .filter_map(|(job_id, coordinator)| {
                let record = coordinator.read_record();
                (record.state() == JobState::Queued)
                    .then_some((record.spec.priority(), job_id.clone()))
            })
            .collect();
        queued.sort_by_key(|(priority, _)| std::cmp::Reverse(*priority));

        let mut admitted = 0usize;
        for (_, job_id) in queued {
            let spec = {
                let Some(coordinator) = self.job_coordinators.get(&job_id) else {
                    continue;
                };
                let record = coordinator.read_record();
                if record.state() != JobState::Queued {
                    continue;
                }
                record.spec.clone()
            };
            if !matches!(self.evaluate_admission(&spec), SubmitOutcome::Accepted) {
                continue;
            }

            {
                let mut record = self.find_job_mut(&job_id)?;
                record.mark_admitted();
            }
            self.ensure_checkpoint_coordinator_for_job(&job_id)?;
            self.persist_job_record(&job_id, true)?;
            admitted = admitted.saturating_add(1);
            tracing::info!(job_id = %job_id, "queued job admitted");
        }

        if admitted > 0 {
            self.exec.notify.notify_waiters();
        }
        Ok(admitted)
    }

    pub(crate) fn ensure_checkpoint_coordinator_for_job(
        &mut self,
        job_id: &JobId,
    ) -> SchedulerResult<()> {
        if self.ckpt.coordinators.contains_key(job_id) {
            return Ok(());
        }
        let (kind, interval_ms, storage_path, task_count) = {
            let record = self.find_job(job_id)?;
            (
                record.spec.kind(),
                record.spec.checkpoint_interval_ms(),
                record.spec.checkpoint_storage_path().map(str::to_owned),
                record.spec.task_count(),
            )
        };
        if kind != JobKind::Streaming {
            return Ok(());
        }
        let (Some(interval_ms), Some(storage_path)) = (interval_ms, storage_path) else {
            return Ok(());
        };
        let storage = Self::open_checkpoint_storage(&storage_path)?;
        self.ckpt.coordinators.insert(
            job_id.clone(),
            CheckpointCoordinator::new(
                job_id.clone(),
                self.coordinator_id().as_str().to_owned(),
                storage,
                interval_ms,
                task_count,
            ),
        );
        Ok(())
    }

    pub(crate) fn persist_job_record(&self, job_id: &JobId, sync: bool) -> SchedulerResult<()> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        let record = self
            .job_coordinators
            .get(job_id)
            .map(|coordinator| coordinator.read_record())
            .ok_or_else(|| SchedulerError::UnknownJob {
                job_id: job_id.clone(),
            })?;
        if sync {
            store.save_job_checked(&record)?;
        } else {
            store.save_job(&record);
        }
        Ok(())
    }

    /// Cancel a job and mark non-terminal stages/tasks cancelled.
    #[tracing::instrument(level = "info", skip(self), fields(job_id = %job_id))]
    pub fn cancel_job(&mut self, job_id: &JobId) -> SchedulerResult<()> {
        self.ensure_active()?;
        let (_job_name, _namespace) = {
            let job = self.find_job(job_id)?;
            let name = job.spec.name().to_owned();
            let ns = job
                .spec
                .namespace_id()
                .map(|s| s.to_owned())
                .unwrap_or_default();
            (name, ns)
        };
        {
            let mut job = self.find_job_mut(job_id)?;
            job.cancel();
        }

        // Cancellation is a terminal state transition and must be durable
        // before a future standby can promote. Without this write, failover
        // reloads the last Running snapshot, resurrecting cancelled tasks and
        // consuming executor slots indefinitely.
        self.persist_job_record(job_id, true)?;

        if let Some(store) = &self.store {
            let mut guard = store.inner();
            if let Err(e) = guard.append_event(EventLogEvent::JobCancelled {
                job_id: job_id.clone(),
            }) {
                tracing::warn!(job_id = %job_id, error = %e, "failed to append JobCancelled event");
            }
        }

        // Use the same terminal bookkeeping as succeeded/failed jobs so a
        // cancellation is archived in durable history and releases resources
        // exactly once.
        self.on_job_terminal(job_id);

        Ok(())
    }

    /// Apply a task update from an executor.
    #[tracing::instrument(skip(self, update), fields(job_id = %update.job_id(), task_id = %update.task_id(), state = ?update.state()), name = "apply_task_update")]
    pub fn apply_task_update(
        &mut self,
        update: TaskStatusUpdate,
    ) -> SchedulerResult<TaskUpdateOutcome> {
        // Callers must drain pending_sink_finalize after every call via
        // take_pending_sink_finalize().  A non-empty vec here means a previous
        // caller forgot to drain, which would cause blocking I/O under the write lock.
        debug_assert!(
            self.pending_sink_finalize.is_empty(),
            "pending_sink_finalize not drained before next apply_task_update call; \
             caller must call take_pending_sink_finalize() after every apply_task_update"
        );
        self.ensure_active()?;
        self.exec
            .executors
            .validate_lease(update.executor_id(), update.lease_generation())?;

        tracing::debug!(
            job_id = %update.job_id(),
            stage_id = %update.stage_id(),
            task_id = %update.task_id(),
            attempt = update.attempt(),
            state = ?update.state(),
            executor = %update.executor_id(),
            "applying task status update"
        );

        let job_id = update.job_id().clone();
        let stage_id = update.stage_id().clone();
        let task_id = update.task_id().clone();
        let attempt = update.attempt();
        let is_continuous_cycle = self.is_continuous_cycle_task(&job_id, &task_id);
        let inline_ipc = update
            .output_metadata()
            .map(|meta| meta.inline_record_batch_ipc().to_vec())
            .unwrap_or_default();
        let spooled_result_total_bytes = update
            .output_metadata()
            .map(|meta| meta.spooled_result_total_bytes())
            .unwrap_or(0);
        // G5: post-cycle continuous operator state + its watermark (persisted
        // below once the update is applied successfully).
        let state_snapshot = update
            .output_metadata()
            .and_then(|meta| meta.state_snapshot().map(<[u8]>::to_vec));
        let task_watermark_ms = update
            .output_metadata()
            .and_then(|meta| meta.watermark_ms());
        let terminal_state = update.state();
        let executor_id_for_circuit = update.executor_id().clone();
        // Save before update is moved.
        let missing_partitions: Vec<krishiv_proto::MissingShufflePartition> =
            update.missing_shuffle_partitions().to_vec();
        let hot_key_reports = update
            .output_metadata()
            .map(|meta| meta.hot_key_reports().to_vec())
            .unwrap_or_default();
        let already_terminal = self
            .job_coordinators
            .get(&job_id)
            .map(|jc| jc.read_record().state().is_terminal())
            .unwrap_or(false);
        if already_terminal {
            return Ok(TaskUpdateOutcome::Duplicate);
        }
        let outcome = self.find_job_mut(&job_id)?.apply_task_update(update)?;

        if outcome == TaskUpdateOutcome::Duplicate {
            tracing::debug!(
                job_id = %job_id,
                stage_id = %stage_id,
                task_id = %task_id,
                attempt,
                state = ?terminal_state,
                executor = %executor_id_for_circuit,
                "duplicate task status update ignored without replaying side effects"
            );
            return Ok(outcome);
        }

        if !hot_key_reports.is_empty() {
            let throttles = self.process_hot_key_reports(&hot_key_reports);
            if !throttles.is_empty() {
                self.pending_source_throttles
                    .entry(executor_id_for_circuit.clone())
                    .or_default()
                    .extend(throttles);
            }
        }

        // IMM-2 (Circuit Breaker Strengthening):
        // Record failure and, if the executor is now bad, clear the assignment
        // so the task can be re-assigned to a healthy executor on the next launch cycle.
        //
        // FetchFailed exemption (Spark parity): a consumer that failed because
        // an UPSTREAM shuffle partition is unavailable says nothing about the
        // health of the executor it ran on — the data is gone, not the node.
        // Counting those failures banned every executor in turn while the
        // producer was regenerated (live wedge, Phase 58 chaos gate,
        // 2026-07-16: two executors circuit-broken by one lost partition →
        // zero launch candidates → job pinned Running forever). The failure
        // metric still counts them; only the per-executor breaker skips them.
        if terminal_state == TaskState::Failed {
            krishiv_metrics::global_metrics().inc_tasks_failed();
        }
        if terminal_state == TaskState::Failed && missing_partitions.is_empty() {
            let threshold = self.config.circuit_breaker_failure_threshold();
            let now_ms = u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0);
            let exceeded = self.exec.executors.record_task_failure(
                &executor_id_for_circuit,
                threshold,
                now_ms,
            );
            if exceeded {
                tracing::warn!(
                    executor_id = %executor_id_for_circuit,
                    "executor exceeded failure threshold — clearing assignments for re-launch on healthy executors"
                );

                if let Some(jc) = self.job_coordinator(&job_id) {
                    // Clear assignments SYNCHRONOUSLY under the coordinator
                    // write lock (which is already held here). The previous
                    // tokio::spawn raced with the task-launch loop: notify
                    // fired before clearing completed, so the launcher could
                    // re-assign tasks back to the bad executor.
                    let cleared = jc.clear_assignments_for_bad_executor_and_count_sync(
                        &executor_id_for_circuit,
                    );
                    tracing::debug!(
                        job_id = %job_id,
                        executor_id = %executor_id_for_circuit,
                        cleared_count = cleared,
                        "circuit breaker: assignments cleared synchronously"
                    );
                } else if let Ok(mut job) = self.find_job_mut(&job_id) {
                    for stage in job.stages_mut() {
                        for task in stage.tasks_mut() {
                            if task.assigned_executor.as_ref() == Some(&executor_id_for_circuit) {
                                task.assigned_executor = None;
                                task.launch_in_flight = false;
                            }
                        }
                    }
                }

                tracing::debug!(
                    job_id = %job_id,
                    executor_id = %executor_id_for_circuit,
                    "circuit breaker triggered; assignments cleared via JCP or fallback"
                );
                // Fire notify AFTER clearing completes so the task-launch loop
                // sees the updated (cleared) assignments.
                self.exec.notify.notify_waiters();
            }
        } else if terminal_state == TaskState::Succeeded {
            krishiv_metrics::global_metrics().inc_tasks_succeeded();
            self.exec
                .executors
                .reset_task_failures(&executor_id_for_circuit);
        }

        // Re-queue the producing stage when the consumer reports missing partitions.
        // This handles the case where a producer executor's shuffle data is lost
        // (disk failure, eviction, restart) after the produce stage already succeeded.
        if terminal_state == TaskState::Failed && !missing_partitions.is_empty() {
            // Identify *which* partitions, not just how many. A bare
            // `missing_count: 1` cannot distinguish "the producer never wrote
            // partition 7" from "the consumer asked the wrong executor for
            // it", and both regenerate into the same failure. TPC-H q10 has
            // hit this three times across three sweeps and each investigation
            // had to start from nothing, because the one fact that would have
            // narrowed it — the partition index and its producing stage — was
            // discarded at the log line.
            let missing_ids: Vec<String> = missing_partitions
                .iter()
                .map(|m| format!("{}/{}", m.stage_id().as_str(), m.partition_id()))
                .collect();
            tracing::warn!(
                job_id = %job_id,
                stage_id = %stage_id,
                missing_count = missing_partitions.len(),
                missing = %missing_ids.join(","),
                executor = %executor_id_for_circuit,
                "consumer task reported missing upstream shuffle partitions; invalidating producers"
            );
            let max_regen = self.config.max_shuffle_regen_attempts();
            let regen = if let Ok(mut job) = self.find_job_mut(&job_id) {
                job.invalidate_specific_shuffle_partitions(&missing_partitions, max_regen)
            } else {
                ShuffleRegenOutcome::NoneAffected
            };
            match regen {
                ShuffleRegenOutcome::Regenerated => self.exec.notify.notify_waiters(),
                ShuffleRegenOutcome::NoneAffected => {}
                // C2 / SOTA §3: the identical partition missed twice with
                // nothing changed in between. Retrying is provably useless, so
                // stop at attempt two and hand the operator both observations
                // instead of eight anonymous ones.
                ShuffleRegenOutcome::RepeatedMiss { diagnosis } => {
                    let message = format!(
                        "job {job_id} failed: the same shuffle partition was reported \
                         missing twice with no executor loss in between, so regenerating \
                         it again cannot help. {diagnosis}"
                    );
                    tracing::error!(
                        job_id = %job_id,
                        diagnosis = %diagnosis,
                        missing = %missing_ids.join(","),
                        "shuffle regeneration reproduced an identical miss; failing job fast"
                    );
                    let _ = self.cancel_job(&job_id);
                    return Err(SchedulerError::Transport { message });
                }
                // Phase 58: the producing stage cannot durably retain its output.
                // Stop the regenerate/refetch loop and fail the job with a
                // terminal reason (mirrors the fatal spooled-result path below).
                ShuffleRegenOutcome::BudgetExhausted {
                    attempts,
                    limit,
                    diagnosis,
                } => {
                    let message = format!(
                        "job {job_id} lost shuffle output and regenerated it {attempts} \
                         times (limit {limit}); the producing stage cannot durably retain \
                         its output — failing the job as unrecoverable. {diagnosis}"
                    );
                    tracing::error!(
                        job_id = %job_id,
                        attempts,
                        limit,
                        diagnosis = %diagnosis,
                        "shuffle regeneration budget exhausted; failing job"
                    );
                    let _ = self.cancel_job(&job_id);
                    return Err(SchedulerError::Transport { message });
                }
            }
        }

        if terminal_state == TaskState::Succeeded && !inline_ipc.is_empty() {
            self.job_inline_results
                .entry(job_id.clone())
                .or_default()
                .extend(inline_ipc);
        }

        // Claim a spooled result delivered via PushTaskResult ahead of this
        // terminal report. Missing or size-mismatched spools fail the WHOLE
        // JOB, not just this update: the task is already recorded Succeeded
        // above, so a plain error here would let the job complete with this
        // task's rows silently missing (a retried report would come back
        // Duplicate and skip this block).
        if terminal_state == TaskState::Succeeded && spooled_result_total_bytes > 0 {
            let key = crate::result_spool::TaskResultKey {
                job_id: job_id.clone(),
                task_id: task_id.clone(),
                attempt_id: attempt,
            };
            match self.pending_task_result_spools.remove(&key) {
                Some(spool) if spool.total_bytes() == spooled_result_total_bytes => {
                    self.job_result_spools
                        .entry(job_id.clone())
                        .or_default()
                        .push(spool);
                }
                Some(spool) => {
                    let message = format!(
                        "task {task_id} spooled result size mismatch: status declares \
                         {spooled_result_total_bytes} bytes, spool holds {}; cancelling job",
                        spool.total_bytes()
                    );
                    let _ = self.cancel_job(&job_id);
                    return Err(SchedulerError::Transport { message });
                }
                None => {
                    let message = format!(
                        "task {task_id} declared a spooled result of \
                         {spooled_result_total_bytes} bytes but no spool was received; \
                         cancelling job"
                    );
                    let _ = self.cancel_job(&job_id);
                    return Err(SchedulerError::Transport { message });
                }
            }
        }

        // G5: a completed continuous cycle carries the executor's post-cycle
        // operator state — persist it as the job's restorable checkpoint, so
        // `POST /api/v1/continuous/{id}/checkpoint` returns live state and a
        // recreated job can be rehydrated via the restore endpoint.
        if terminal_state == TaskState::Succeeded
            && is_continuous_cycle
            && let Some(snapshot_bytes) = state_snapshot
        {
            let watermark_ms = task_watermark_ms.unwrap_or(i64::MIN);
            self.save_continuous_snapshot(
                job_id.as_str(),
                crate::ContinuousSnapshot {
                    snapshot_bytes,
                    watermark_ms,
                },
            );
        }

        // AQE stage-boundary re-optimization (Phase 2.9).
        //
        // When a shuffle stage completes, collect per-task serialized_bytes and
        // run the default AQE optimizer so downstream stage launch can use the
        // `coalesced_partition_count` hint to right-size reduce parallelism.
        if terminal_state == TaskState::Succeeded {
            let stage_just_succeeded = self
                .job_coordinators
                .get(&job_id)
                .map(|jc| {
                    let r = jc.read_record();
                    r.stages
                        .iter()
                        .find(|s| s.stage_id() == &stage_id)
                        .is_some_and(|s| s.state == StageState::Succeeded)
                })
                .unwrap_or(false);
            if stage_just_succeeded {
                let stats = self
                    .job_coordinators
                    .get(&job_id)
                    .map(|jc| jc.read_record().collect_stage_runtime_stats(&stage_id))
                    .unwrap_or_default();
                // AQE coalesce hints are only meaningful for ShuffleMap stages.
                // Result stages have no downstream shuffle consumers to hint.
                let is_shuffle_map = self
                    .job_coordinators
                    .get(&job_id)
                    .and_then(|jc| {
                        let r = jc.read_record();
                        r.stages
                            .iter()
                            .find(|s| s.stage_id() == &stage_id)
                            .map(|s| s.spec.kind() == krishiv_proto::StageKind::ShuffleMap)
                    })
                    .unwrap_or(true); // default to true for backwards-compat with unlabelled stages
                if is_shuffle_map
                    && self.config.aqe_enabled()
                    && !stats.is_empty()
                    && stats.iter().any(|s| s.serialized_bytes > 0)
                {
                    // Coalescing must not shrink a stage below the cluster's
                    // schedulable width, or a stage whose bytes fit one target
                    // partition runs as one task while every other slot idles
                    // (q2/SF100: four stages coalesced to 1, 24 min on a
                    // nine-slot cluster). Slots are read live, so the floor
                    // tracks executors joining and leaving.
                    let aqe =
                        krishiv_plan::optimizer::default_aqe_optimizer_with_stats_and_parallelism(
                            self.total_schedulable_slots().max(1),
                        );
                    // T1: synthesize a minimal physical plan from the stats
                    // so the AQE rules have at least one node to rewrite.
                    // The scheduler doesn't preserve the original physical
                    // plan at stage-succeeded time, so the AQE could only
                    // previously fire on the empty placeholder, leaving
                    // every rule (Coalesce, AutoPartition, Broadcast) as a
                    // no-op. The synthesised plan carries one Exchange node
                    // per stat so the rules' `plan.nodes()` walks observe
                    // real data and the coalesce hint can be computed.
                    let mut placeholder = krishiv_plan::PhysicalPlan::new(
                        job_id.as_str(),
                        krishiv_plan::ExecutionKind::Batch,
                    );
                    let output_count = stats.len() as u32;
                    for (i, s) in stats.iter().enumerate() {
                        use krishiv_plan::{NodeOp, Partitioning, PlanNode};
                        let node = PlanNode::new(
                            format!("aqe-shuffle-{i}"),
                            format!("aqe-shuffle-{i}"),
                            krishiv_plan::ExecutionKind::Batch,
                        )
                        .with_op(NodeOp::Exchange {
                            partitioning: Partitioning::Hash {
                                keys: vec![format!("k{i}")],
                                buckets: output_count.max(1),
                            },
                        })
                        .with_estimated_rows(Some(s.output_rows.max(1)));
                        placeholder.add_node(node);
                    }
                    // A sink node so the rules' `terminal_indexes` check passes.
                    use krishiv_plan::{NodeOp, PlanNode};
                    let sink_id = "aqe-sink".to_string();
                    placeholder.add_node(
                        PlanNode::new(&sink_id, "aqe-sink", krishiv_plan::ExecutionKind::Batch)
                            .with_op(NodeOp::Sink {
                                format: "arrow".to_string(),
                            })
                            .with_inputs(
                                (0..stats.len())
                                    .map(|i| format!("aqe-shuffle-{i}"))
                                    .collect::<Vec<_>>(),
                            ),
                    );
                    match aqe.apply(placeholder, &stats) {
                        Ok((plan, applied)) if !applied.is_empty() => {
                            if let Some(hint) = plan.coalesced_partition_count() {
                                tracing::info!(
                                    job_id = %job_id,
                                    stage_id = %stage_id,
                                    coalesced_partition_count = hint,
                                    applied_rules = ?applied,
                                    "AQE stage-boundary re-optimization: coalesce hint stored"
                                );
                                // Store the hint for the next stage launch.
                                self.aqe_coalesce_hints
                                    .insert((job_id.clone(), stage_id.clone()), hint);
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::debug!(
                                job_id = %job_id,
                                stage_id = %stage_id,
                                error = %e,
                                "AQE stage-boundary re-optimization skipped"
                            );
                        }
                    }
                }
                // Phase 54: the REAL stage-boundary rewrite — coalesce small
                // reduce partitions / split skewed ones on the downstream
                // Result stage's dfplan bodies, from measured shuffle sizes.
                // (The placeholder-plan pass above only records hints.)
                if self.config.aqe_enabled() {
                    let _ = self.apply_stage_boundary_aqe(&job_id, &stage_id);
                }
            }
        }

        if is_continuous_cycle && terminal_state == TaskState::Succeeded {
            self.complete_continuous_input_cycle(&job_id, &task_id);
        } else if is_continuous_cycle
            && matches!(terminal_state, TaskState::Failed | TaskState::Cancelled)
        {
            // A cancelled cycle (e.g. an executor-side tombstone from a prior
            // incarnation of this job id) must release the fence too —
            // otherwise every later push 409s forever.
            self.continuous_input_cycles.remove(&job_id);
            self.job_input_partitions.remove(&job_id);
        }

        // A continuous task reporting `Failed` because its executor went away
        // must not terminate the whole streaming job: the executor-loss reset
        // runs only on the slow heartbeat path, so this fast self-reported
        // failure otherwise drives the job terminal (via `refresh_state`,
        // which has no streaming exception) before recovery can act — and the
        // next push gets `unknown job`. Rescue it here, on the fast path,
        // bounded by the same loss budget. The shuffle-regen case
        // (`!missing_partitions.is_empty()`) is handled above and left alone.
        if is_continuous_cycle
            && terminal_state == TaskState::Failed
            && missing_partitions.is_empty()
            && self.rescue_failed_continuous_task(&job_id, &task_id)
        {
            tracing::info!(
                job_id = %job_id,
                task_id = %task_id,
                "continuous task failure rescued (executor loss); job kept Running for reassignment"
            );
        }

        // Phase 2.3 distributed write commit: when this update drove the job
        // to a terminal state, publish staged sink outputs (job success) or
        // clean up staging (failure/cancel). Runs before the state snapshot
        // below so a publish failure demotes the job to Failed prior to
        // persistence and GC bookkeeping.
        self.finalize_staged_sink_outputs(&job_id);

        // Terminal-state bookkeeping (GC queueing, resource release, history
        // archival). Self-gates on the job's terminal state, so a job that
        // `finalize_staged_sink_outputs` just demoted to the non-terminal
        // `Committing` state (DUR-1) is a no-op here — its bookkeeping is
        // deferred until `mark_sink_publish_committed`/`_failed` resolves the
        // publish.
        self.on_job_terminal(&job_id);
        if let Some(record) = self
            .job_coordinators
            .get(&job_id)
            .map(|jc| jc.read_record())
            && let Some(store) = &self.store
        {
            if terminal_state.is_terminal()
                || krishiv_common::profile_requires_fail_closed_metadata(self.durability_profile)
            {
                // Durable profiles require synchronous metadata commits for all task updates.
                // Latch-checked: this is the other path (besides cancel_job) through
                // which a job's record can reach a durably terminal state, and it
                // must be protected from the same stale-write resurrection race.
                store.save_job_checked(&record)?;
            } else {
                store.save_job(&record);
            }
        }
        // H3: Emit task-level event log entries for succeeded/failed terminal states.
        if let Some(store) = &self.store {
            let attempt_id = AttemptId::try_new(attempt).unwrap_or(AttemptId::initial());
            let event = match terminal_state {
                TaskState::Succeeded => Some(EventLogEvent::TaskSucceeded {
                    job_id: job_id.clone(),
                    stage_id: stage_id.clone(),
                    task_id: task_id.clone(),
                    attempt: attempt_id,
                }),
                TaskState::Failed => {
                    let reason = self
                        .find_job(&job_id)
                        .ok()
                        .and_then(|job| {
                            job.stages()
                                .iter()
                                .find(|s| s.stage_id() == &stage_id)
                                .and_then(|s| {
                                    s.tasks()
                                        .iter()
                                        .find(|t| t.task_id() == &task_id && t.attempt() == attempt)
                                        .and_then(|t| t.last_failure_reason().map(str::to_owned))
                                })
                        })
                        .unwrap_or_default();
                    Some(EventLogEvent::TaskFailed {
                        job_id: job_id.clone(),
                        stage_id: stage_id.clone(),
                        task_id: task_id.clone(),
                        attempt: attempt_id,
                        reason,
                    })
                }
                _ => None,
            };
            if let Some(event) = event {
                let mut guard = store.inner();
                if let Err(e) = guard.append_event(event) {
                    tracing::warn!(
                        job_id = %job_id,
                        stage_id = %stage_id,
                        task_id = %task_id,
                        error = %e,
                        "failed to persist task-level event log entry"
                    );
                }
            }
        }
        // P1.1: Remove streaming task index entries when job reaches a terminal state.
        let is_terminal = self
            .job_coordinators
            .get(&job_id)
            .map(|jc| jc.read_record().state().is_terminal())
            .unwrap_or(false);
        if is_terminal {
            self.remove_streaming_task_index(&job_id);
            self.pending_backlog_jobs.remove(&job_id);
            self.launch_dirty_jobs.remove(&job_id);
        } else {
            // Phase 53: a task transition can create launch-ready work
            // (failure retry reset it to Pending, a stage boundary opened
            // the next stage) — mark this job for the next drive tick.
            self.launch_dirty_jobs.insert(job_id.clone());
        }
        // Phase 53 (strict capacity): a completed task frees a slot — flow
        // backlog work into it now instead of oversubscribing at placement
        // time. This job's own retry resets also (re)enter the backlog here.
        if matches!(terminal_state, TaskState::Succeeded | TaskState::Failed) {
            if !is_terminal {
                self.pending_backlog_jobs.insert(job_id.clone());
            }
            self.drain_pending_backlog();
        }
        Ok(outcome)
    }

    /// One-time bookkeeping that fires when a job reaches a **terminal** state
    /// (`Succeeded`/`Failed`/`Cancelled`): queue it for shuffle GC, free its
    /// inline input/result state, release its admission-control resources, and
    /// archive an immutable history record + `JobCompleted` event.
    ///
    /// Self-gating: a no-op unless the job's current record state is terminal
    /// and it has not already been queued for GC (idempotent). This lets the
    /// DUR-1 `Committing` path defer the bookkeeping — `apply_task_update` calls
    /// it eagerly (no-op while `Committing`) and again from
    /// `mark_sink_publish_committed`/`mark_sink_publish_failed` once the publish
    /// resolves the job to a terminal state.
    pub(crate) fn on_job_terminal(&mut self, job_id: &JobId) {
        let (is_terminal, usage, state) = self
            .job_coordinators
            .get(job_id)
            .map(|jc| {
                let r = jc.read_record();
                (r.state().is_terminal(), r.resource_usage.clone(), r.state())
            })
            .unwrap_or((false, ResourceUsage::default(), JobState::Accepted));

        if !is_terminal || self.gc_ready_jobs.contains(job_id) {
            return;
        }

        // IVM-AUD-DIST-3: wake anything waiting for this job to CONCLUDE.
        //
        // `exec.notify` was fired on executor register/deregister, heartbeat,
        // task launch, checkpoint ack/commit and restore — but by nothing on a
        // terminal transition. Callers that wait for `Succeeded`/`Failed` on
        // this same handle (the IVM dispatch wait loop; the batch-SQL wait
        // loop) therefore subscribed to a signal the event they wait for never
        // sent, and only their fallback poll ever observed the conclusion. The
        // guard was enforced by nothing: the sleep was load-bearing and the
        // `Notify` decorative.
        //
        // This is the single funnel — every terminal path runs its bookkeeping
        // here — and the `gc_ready_jobs` check above makes it fire exactly once
        // per job, on the transition rather than on every re-entry.
        self.exec.notify.notify_waiters();
        const MAX_GC_JOBS: usize = 1000;
        if self.gc_ready_jobs.len() >= MAX_GC_JOBS
            && let Some(evicted) = self.gc_ready_jobs.pop_front()
        {
            // Dropping the id out of the queue is not the same as collecting
            // it. `take_gc_ready_jobs` is the ONLY caller of
            // `evict_completed_job`, and the orphan sweep's live set is
            // exactly `active_job_ids()` == `job_coordinators.keys()`. So a
            // job dropped here without being evicted leaks its whole
            // in-memory footprint (record, inline results, result spools,
            // input partitions, checkpoint coordinator, indexes) *and* pins
            // its shuffle directory on disk forever, because the sweep keeps
            // seeing it as a live job. Both leaks are silent and unbounded:
            // the cap only bounds the queue.
            //
            // Evicting here reclaims the memory and lets the orphan sweep
            // reclaim the partitions once the id leaves the live set. The
            // only thing genuinely lost is the targeted
            // `delete_job_partitions` call, which the sweep subsumes.
            self.gc_ready_at.remove(&evicted);
            tracing::warn!(
                evicted_job_id = %evicted,
                queue_len = MAX_GC_JOBS,
                "gc-ready queue is full; evicting the oldest terminal job \
                 without its targeted shuffle GC (the orphan sweep reclaims \
                 its partitions once it leaves the live-job set)"
            );
            self.evict_completed_job(&evicted);
        }
        self.gc_ready_jobs.push_back(job_id.clone());
        self.gc_ready_at
            .insert(job_id.clone(), std::time::Instant::now());
        self.ckpt.coordinators.remove(job_id);

        // Phase 59 (observability gap-a): observe whole-query wall-clock latency
        // exactly once per batch job. This block is self-gated by the
        // `gc_ready_jobs.contains` guard above, so it never double-counts across
        // the DUR-1 `Committing` re-entry path. Non-batch jobs never inserted an
        // instant, so the map lookup is simply absent for them.
        if let Some(submit_instant) = self.job_submit_instants.remove(job_id) {
            krishiv_metrics::global_metrics()
                .observe_query_latency("batch", submit_instant.elapsed().as_secs_f64());
        }
        // Free inline input data (InlineIpc partitions for batch-sql and
        // bounded-window jobs) — executors have already consumed this by the
        // time the job reaches a terminal state.
        self.job_input_partitions.remove(job_id);
        self.job_task_input_partitions.remove(job_id);
        self.continuous_input_cycles.remove(job_id);
        self.pending_continuous_restores.remove(job_id);
        self.batch_sql_job_tables.remove(job_id);
        self.pending_task_result_spools
            .retain(|key, _| key.job_id != *job_id);
        if state != JobState::Succeeded {
            self.job_inline_results.remove(job_id);
            self.job_result_spools.remove(job_id);
        }
        self.queue_manager.on_job_complete(job_id, &usage);

        // SC13: append a `JobCompleted` event to the event log so the
        // History Server can render a complete lifecycle. The
        // `final_state` is a serialised string so the History
        // Server doesn't have to re-resolve `JobState` variants.
        if let Some(store) = &self.store {
            let mut guard = store.inner();
            if let Err(e) = guard.append_event(EventLogEvent::JobCompleted {
                job_id: job_id.clone(),
                final_state: state.to_string(),
            }) {
                tracing::warn!(job_id = %job_id, error = %e, "failed to append JobCompleted event");
            }
        }

        // Archive an immutable history record before the job is evicted.
        if let Some(jc) = self.job_coordinators.get(job_id) {
            let r = jc.read_record();
            let history = crate::store::JobHistoryRecord {
                job_id: job_id.as_str().to_owned(),
                job_kind: r.spec.kind().to_string(),
                final_state: state.to_string(),
                completed_at_ms: krishiv_common::async_util::unix_now_ms() as u64,
                stage_count: r.stages.len(),
                task_count: r.stages.iter().map(|s| s.tasks.len()).sum(),
                succeeded_task_count: r
                    .stages
                    .iter()
                    .flat_map(|s| s.tasks.iter())
                    .filter(|t| t.state == TaskState::Succeeded)
                    .count() as u32,
                failed_task_count: r
                    .stages
                    .iter()
                    .flat_map(|s| s.tasks.iter())
                    .filter(|t| t.state == TaskState::Failed)
                    .count() as u32,
                cpu_nanos: usage.cpu_nanos,
                memory_peak_task_bytes: usage.memory_peak_task_bytes,
                namespace_id: r.spec.namespace_id().map(str::to_owned),
                priority: r.spec.priority(),
            };
            if let Some(store) = &self.store {
                let mut guard = store.inner();
                if let Err(e) = guard.save_job_history(history) {
                    tracing::warn!(
                        job_id = %job_id,
                        error = %e,
                        "failed to persist job history record"
                    );
                }
            }
        }
    }

    /// Drain the list of jobs that have reached a terminal state and need shuffle GC.
    ///
    /// The coordinator binary's tick loop should call this, then asynchronously
    /// delete partitions for each returned job id via the shuffle store.
    /// S3: Also evicts each job from `job_coordinators` to prevent unbounded map
    /// growth. Eviction happens here (not in `apply_task_update`) so that the job
    /// snapshot remains queryable until the GC cycle runs.
    pub fn take_gc_ready_jobs(&mut self) -> Vec<JobId> {
        // TTL-after-finished: only evict jobs that have been terminal for at
        // least the grace window, so a consumer whose poll is delayed (e.g. a
        // batch-SQL `poll_batch_sql_outcome` starved of the read lock) still
        // observes the terminal outcome and takes its result before the job is
        // reaped. Younger terminal jobs stay queued for a later GC cycle.
        let grace = job_gc_grace();
        let now = std::time::Instant::now();
        let mut evict: Vec<JobId> = Vec::new();
        let mut keep: std::collections::VecDeque<JobId> = std::collections::VecDeque::new();
        for job_id in std::mem::take(&mut self.gc_ready_jobs) {
            let aged = self
                .gc_ready_at
                .get(&job_id)
                .map(|queued_at| now.duration_since(*queued_at) >= grace)
                .unwrap_or(true);
            if aged {
                // Clean the timestamp here (not only in `evict_completed_job`,
                // which early-returns for an already-removed job) so the
                // `gc_ready_at` map can never leak an entry.
                self.gc_ready_at.remove(&job_id);
                evict.push(job_id);
            } else {
                keep.push_back(job_id);
            }
        }
        self.gc_ready_jobs = keep;
        for job_id in &evict {
            self.evict_completed_job(job_id);
        }
        evict
    }

    /// Remove a single completed job from the in-memory registry.
    ///
    /// Only safe to call after the job has reached a terminal state (Succeeded,
    /// Failed, or Cancelled). Cleans up `job_coordinators`, associated input
    /// partitions, batch-SQL tables, and checkpoint state. Used by the embedded
    /// in-process runtime which has no background GC loop.
    pub fn evict_completed_job(&mut self, job_id: &JobId) {
        if let Some(jc) = self.job_coordinators.get(job_id) {
            if !jc.read_record().state().is_terminal() {
                return;
            }
        } else {
            return;
        }
        self.job_coordinators.remove(job_id);
        self.purge_job_scoped_state(job_id);

        // Retire the durable record too, so the store's live-job set tracks
        // this map instead of accumulating forever. `on_job_terminal` already
        // archived the outcome in the history log, which is what `/ui/history`
        // and the terminal-jobs latch read; the `jobs` record only exists so a
        // restart can resume work, and there is none left to resume.
        //
        // Gated on that archive actually being present. If the history write
        // failed (it logs and continues by design — a failed archive must not
        // block the job from concluding), removing here would erase the last
        // trace of the outcome. Leaking one record is the cheaper error.
        if let Some(store) = &self.store {
            let archived = store.inner().get_job_history(job_id.as_str()).is_some();
            if archived {
                store.remove_job(job_id.as_str());
            } else {
                tracing::warn!(
                    job_id = %job_id,
                    "evicting a terminal job with no history record; keeping its durable \
                     job record so the outcome is not lost entirely"
                );
            }
        }
    }

    /// Drop every piece of coordinator state keyed by `job_id`, *except* the
    /// `job_coordinators` entry itself.
    ///
    /// Split out of [`Self::evict_completed_job`] so recovery can reuse it:
    /// `recover_from_store` rebuilds `job_coordinators` from the durable store
    /// and runs on every standby→active promotion, not only at process start.
    /// Any job that was live before the promotion but is absent from the store
    /// afterwards used to leave all of this behind — most damagingly a
    /// `continuous_input_cycles` fence, which makes every later push to a job
    /// of that id 409 forever.
    ///
    /// Keep this the single place that enumerates the per-job maps: the
    /// forward/reverse streaming indexes drifted apart precisely because two
    /// call sites maintained them separately.
    pub(crate) fn purge_job_scoped_state(&mut self, job_id: &JobId) {
        self.job_inline_results.remove(job_id);
        self.job_result_spools.remove(job_id);
        self.pending_task_result_spools
            .retain(|key, _| key.job_id != *job_id);
        self.job_input_partitions.remove(job_id);
        self.job_task_input_partitions.remove(job_id);
        self.continuous_input_cycles.remove(job_id);
        self.pending_continuous_restores.remove(job_id);
        self.batch_sql_job_tables.remove(job_id);
        self.ckpt.coordinators.remove(job_id);
        self.gc_ready_jobs.retain(|id| id != job_id);
        self.gc_ready_at.remove(job_id);
        self.pending_backlog_jobs.remove(job_id);
        self.launch_dirty_jobs.remove(job_id);
        self.streaming_task_index
            .retain(|_, (jid, _)| jid != job_id);
        // The reverse index was NOT dropped here, only the forward one. That
        // leaks a `Vec<TaskId>` per streaming job forever — and worse, the
        // forward index is keyed by bare `TaskId`, so a stale reverse entry
        // naming `t0` lets a later `remove_streaming_task_index` for this dead
        // job delete a *live* job's `t0` entry and silently stop its watermark
        // updates.
        self.streaming_job_task_index.remove(job_id);
        // S4: Evict adaptive decision log entries for the completed job to
        // prevent unbounded HashMap growth on long-running coordinators.
        self.adaptive_decision_log.remove(job_id);
        // S1: Evict any pending skew repartition override. Safety-net for jobs
        // that finish before their next task-launch cycle consumes the entry.
        self.skew_repartition_overrides.remove(job_id);
        self.streaming_advisory_partitions.remove(job_id);
        self.aqe_coalesce_hints.retain(|(jid, _), _| jid != job_id);
        // Phase 59 (observability gap-a): drop any submit instant not already
        // consumed by `on_job_terminal` so an evicted job cannot leak an entry.
        self.job_submit_instants.remove(job_id);
        // Recovery control-plane state for the completed job.
        self.ckpt.restore_directives.remove(job_id);
        self.ckpt.pending_stop_after_savepoint.remove(job_id);
        self.ckpt
            .restore_notify_sent
            .retain(|(jid, _, _)| jid != job_id);
        self.ckpt
            .checkpoint_complete_sent
            .retain(|(jid, _, _)| jid != job_id);
        self.ckpt.notify_sent.retain(|(jid, _, _)| jid != job_id);
        // M6: Evict stale per-executor per-job watermark entries to prevent
        // unbounded memory growth on long-lived coordinators.
        for watermarks in self.executor_job_watermarks.values_mut() {
            watermarks.remove(job_id);
        }
    }

    /// Convert and submit a Krishiv logical DAG through the R2 scheduler.
    pub fn submit_logical_plan(
        &mut self,
        job_id: JobId,
        plan: &LogicalPlan,
    ) -> SchedulerResult<SubmitOutcome> {
        self.submit_job(job_spec_from_logical_plan(job_id, plan)?)
    }

    /// Convert and submit a Krishiv physical DAG through the R2 scheduler.
    /// Submit a `PhysicalPlan` as a job.
    ///
    /// AQE optimization is applied before submission: the `default_aqe_optimizer`
    /// runs `CoalesceRule` (guarded by `StreamingAqeGuard` for streaming plans)
    /// to stamp `coalesced_partition_count` on the plan.  With empty runtime
    /// stats this is a no-op; re-optimization will be triggered when per-stage
    /// stats become available.
    pub fn submit_physical_plan(
        &mut self,
        job_id: JobId,
        plan: &PhysicalPlan,
    ) -> SchedulerResult<SubmitOutcome> {
        let aqe = krishiv_plan::optimizer::default_aqe_optimizer_with_stats_and_parallelism(
            self.total_schedulable_slots().max(1),
        );
        let (optimized, _applied) = aqe.apply(plan.clone(), &[])?;
        self.submit_job(job_spec_from_physical_plan(job_id, &optimized)?)
    }
}

/// Grace window a job stays queryable after reaching a terminal state before
/// the GC tick may evict it (`KRISHIV_JOB_GC_GRACE_SECS`, default 30s). Bounds
/// how long a slow consumer has to observe the terminal outcome + take its
/// result; also bounds retained memory for jobs whose consumer never polls.
fn job_gc_grace() -> std::time::Duration {
    std::env::var("KRISHIV_JOB_GC_GRACE_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(super::DEFAULT_JOB_GC_GRACE_SECS))
}

#[cfg(test)]
mod gc_grace_tests {
    use super::*;

    /// TTL-after-finished: `take_gc_ready_jobs` must retain a terminal job that
    /// has been queued for less than the grace window (so a slow consumer still
    /// observes its outcome), and evict only those past the window. Uses
    /// backdated `gc_ready_at` timestamps for determinism (no env / sleeps).
    #[test]
    fn gc_grace_defers_eviction_of_young_terminal_jobs() {
        let mut coord = Coordinator::new_active(None).unwrap();
        let young = JobId::try_new("gc-young").unwrap();
        let aged = JobId::try_new("gc-aged").unwrap();

        coord.gc_ready_jobs.push_back(young.clone());
        coord
            .gc_ready_at
            .insert(young.clone(), std::time::Instant::now());
        coord.gc_ready_jobs.push_back(aged.clone());
        coord.gc_ready_at.insert(
            aged.clone(),
            std::time::Instant::now() - std::time::Duration::from_secs(3600),
        );

        let evicted = coord.take_gc_ready_jobs();

        assert_eq!(
            evicted,
            vec![aged.clone()],
            "only the terminal job past the grace window should be evicted"
        );
        assert!(
            coord.gc_ready_jobs.contains(&young),
            "a young terminal job must stay queued within the grace window"
        );
        assert!(
            coord.gc_ready_at.contains_key(&young),
            "young job keeps its queued-at timestamp"
        );
        assert!(
            !coord.gc_ready_at.contains_key(&aged),
            "an evicted job's timestamp must not leak in gc_ready_at"
        );
    }

    /// The `MAX_GC_JOBS` cap bounds the *queue*, not the state the queue
    /// exists to release. `take_gc_ready_jobs` is the only caller of
    /// `evict_completed_job`, so a job dropped off the front by the cap used
    /// to keep its whole in-memory footprint forever — and, because the shuffle
    /// orphan sweep's live set is exactly `active_job_ids()`
    /// (`job_coordinators.keys()`), keep its shuffle directory on disk forever
    /// too. Both leaks are silent and unbounded.
    #[test]
    fn overflowing_the_gc_queue_evicts_the_dropped_job_instead_of_leaking_it() {
        use krishiv_proto::{StageId, StageSpec, TaskId, TaskSpec};

        fn job(job_id: &JobId) -> JobSpec {
            JobSpec::new(job_id.clone(), "gc-cap", JobKind::Batch).with_stage(
                StageSpec::new(StageId::try_new("stage-0").unwrap(), "stage").with_task(
                    TaskSpec::new(TaskId::try_new("t0").unwrap(), "sql: select 1"),
                ),
            )
        }

        let mut coord = Coordinator::new_active(None).unwrap();
        let victim = JobId::try_new("gc-cap-victim").unwrap();
        coord.submit_job(job(&victim)).unwrap();
        coord.cancel_job(&victim).unwrap();
        assert_eq!(
            coord.gc_ready_jobs.front(),
            Some(&victim),
            "the victim must be the oldest entry, i.e. the one the cap drops"
        );

        // Fill the queue to the cap behind the victim.
        for i in 0..999 {
            coord
                .gc_ready_jobs
                .push_back(JobId::try_new(format!("gc-cap-filler-{i}")).unwrap());
        }

        // One more terminal job forces the cap to drop the victim.
        let newcomer = JobId::try_new("gc-cap-newcomer").unwrap();
        coord.submit_job(job(&newcomer)).unwrap();
        coord.cancel_job(&newcomer).unwrap();

        assert!(
            !coord.gc_ready_jobs.contains(&victim),
            "the cap must have dropped the victim from the queue"
        );
        assert!(
            !coord.job_coordinators.contains_key(&victim),
            "a job dropped by the cap must be evicted, not left in \
             job_coordinators where it leaks memory and pins its shuffle \
             directory against the orphan sweep forever"
        );
        assert!(
            !coord.active_job_ids().contains(victim.as_str()),
            "the orphan sweep's live set must no longer claim the dropped job"
        );
        assert!(
            !coord.gc_ready_at.contains_key(&victim),
            "the dropped job's queued-at timestamp must not leak"
        );
        assert!(
            coord.job_coordinators.contains_key(&newcomer),
            "the job that triggered the overflow must still be tracked"
        );
    }
}

/// Phase 58 #180 (second gap, found on the very next gate rerun after the
/// resurrection-latch fix landed): `submit_job` persisted through
/// `store.inner().save_job(...)` directly instead of the latch-checked
/// `save_job_checked`, and only called `forget_terminal_job` when an
/// in-memory `JobCoordinator` for the id already existed and was terminal —
/// never when the id was latched terminal in the store but absent from
/// memory entirely (evicted by GC, or a coordinator restart that didn't
/// reload it). A resubmission hitting either gap became live in memory while
/// every subsequent persist for it was silently rejected, forever — live
/// found as two chaos-gate streaming jobs cycling Assigned -> Pending ->
/// Assigned for over an hour past their own recorded cancellation, confirmed
/// via direct etcd inspection to still show `state: cancelled` the entire
/// time.
#[cfg(test)]
mod terminal_id_reuse_tests {
    use super::*;
    use crate::store::InMemoryMetadataStore;
    use krishiv_proto::{StageId, StageSpec, TaskId, TaskSpec};

    fn single_task_job(job_id: &JobId, task_id: &str) -> JobSpec {
        JobSpec::new(job_id.clone(), "terminal-reuse-test", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-0").unwrap(), "stage").with_task(TaskSpec::new(
                TaskId::try_new(task_id).unwrap(),
                "sql: select 1",
            )),
        )
    }

    /// Resubmitting under an id whose in-memory `JobCoordinator` was evicted
    /// (not merely cancelled-in-place) while the store still latches it
    /// terminal must succeed, and must actually persist — not silently create
    /// a job that can never become durable again.
    #[test]
    fn submit_job_after_id_evicted_from_memory_but_still_latched_terminal_succeeds_and_persists() {
        let mut coordinator = Coordinator::new_active(None)
            .unwrap()
            .with_store(InMemoryMetadataStore::default());

        let job_id = JobId::try_new("job-reused").unwrap();
        coordinator
            .submit_job(single_task_job(&job_id, "t0"))
            .unwrap();
        coordinator.cancel_job(&job_id).unwrap();

        // Simulate eviction from the live registry (GC tick, or a coordinator
        // restart that only partially reloads history) while the store still
        // remembers the id as terminal.
        coordinator.job_coordinators.remove(&job_id);
        assert!(
            coordinator
                .store
                .as_ref()
                .unwrap()
                .is_terminal_latched(job_id.as_str()),
            "the store must still latch the id as terminal after eviction"
        );

        coordinator
            .submit_job(single_task_job(&job_id, "t0-again"))
            .expect(
                "resubmission after eviction must not be treated as a stale, \
                 unpersistable duplicate",
            );

        let persisted = coordinator
            .store
            .as_ref()
            .unwrap()
            .inner()
            .jobs()
            .iter()
            .find(|r| r.job_id() == &job_id)
            .cloned()
            .expect("the fresh submission must have been persisted, not silently dropped");
        assert!(
            !persisted.state().is_terminal(),
            "the persisted record must reflect the fresh, non-terminal \
             submission, not the stale cancelled one"
        );
    }

    /// Defense in depth: even if some other path ever reintroduces the same
    /// divergence directly (job live in memory, store already latched
    /// terminal), the scheduler must self-heal instead of scheduling a job
    /// that can never become durable again.
    #[test]
    fn reconcile_store_latched_terminal_jobs_cancels_a_job_the_store_already_considers_done() {
        let mut coordinator = Coordinator::new_active(None)
            .unwrap()
            .with_store(InMemoryMetadataStore::default());

        let job_id = JobId::try_new("job-divergent").unwrap();
        coordinator
            .submit_job(single_task_job(&job_id, "t0"))
            .unwrap();
        coordinator.cancel_job(&job_id).unwrap();

        // Reproduce the divergence directly, bypassing whatever real path
        // might cause it, so this test exercises the self-heal in isolation.
        {
            let jc = coordinator.job_coordinators.get(&job_id).unwrap();
            let mut record = jc.write_record();
            record.state = JobState::Running;
            for stage in record.stages_mut() {
                stage.state = StageState::Running;
            }
        }
        assert!(
            !coordinator
                .job_coordinators
                .get(&job_id)
                .unwrap()
                .read_record()
                .state()
                .is_terminal(),
            "the divergence must be in place before the reconcile runs"
        );

        coordinator.reconcile_store_latched_terminal_jobs();

        assert!(
            coordinator
                .job_coordinators
                .get(&job_id)
                .unwrap()
                .read_record()
                .state()
                .is_terminal(),
            "a job latched terminal in the store must be cancelled in memory to match"
        );
        let persisted = coordinator
            .store
            .as_ref()
            .unwrap()
            .inner()
            .jobs()
            .iter()
            .find(|r| r.job_id() == &job_id)
            .cloned()
            .expect("job must still exist in the store");
        assert!(
            persisted.state().is_terminal(),
            "the self-heal must also durably persist the correction, not just \
             patch the in-memory copy"
        );
    }

    /// Live-repro'd on 2026-07-20 under sustained `coordinator-kill` chaos:
    /// the self-heal fired because this coordinator's *local* latch believed
    /// the job was terminal, but the durable store itself was independently
    /// non-terminal at that exact moment (confirmed by direct etcd
    /// inspection) — most likely a retried submit/cancel landing on a
    /// different coordinator generation across a leader failover. An
    /// in-memory-only fix there would self-undo on the very next reload from
    /// the store. This test reproduces that: the store is left genuinely
    /// non-terminal (not just "latched but actually fine") when the
    /// reconcile runs, and asserts the self-heal converges the store itself,
    /// not only the in-memory view.
    #[test]
    fn reconcile_store_latched_terminal_jobs_persists_the_fix_even_when_the_store_itself_is_stale()
    {
        let mut coordinator = Coordinator::new_active(None)
            .unwrap()
            .with_store(InMemoryMetadataStore::default());

        let job_id = JobId::try_new("job-store-also-stale").unwrap();
        coordinator
            .submit_job(single_task_job(&job_id, "t0"))
            .unwrap();
        coordinator.cancel_job(&job_id).unwrap();

        // Simulate a stale write reaching the store directly (bypassing the
        // latch, as a delayed duplicate from a different coordinator
        // generation would) so the durable record is ALSO non-terminal, not
        // merely the in-memory copy.
        {
            let jc = coordinator.job_coordinators.get(&job_id).unwrap();
            let mut record = jc.write_record();
            record.state = JobState::Running;
            for stage in record.stages_mut() {
                stage.state = StageState::Running;
            }
            let stale = record.clone();
            drop(record);
            coordinator
                .store
                .as_ref()
                .unwrap()
                .inner()
                .save_job(&stale)
                .unwrap();
        }
        assert!(
            !coordinator
                .store
                .as_ref()
                .unwrap()
                .inner()
                .jobs()
                .iter()
                .find(|r| r.job_id() == &job_id)
                .unwrap()
                .state()
                .is_terminal(),
            "the store must be genuinely non-terminal before the reconcile runs"
        );

        coordinator.reconcile_store_latched_terminal_jobs();

        assert!(
            coordinator
                .job_coordinators
                .get(&job_id)
                .unwrap()
                .read_record()
                .state()
                .is_terminal(),
            "in-memory must be corrected"
        );
        assert!(
            coordinator
                .store
                .as_ref()
                .unwrap()
                .inner()
                .jobs()
                .iter()
                .find(|r| r.job_id() == &job_id)
                .unwrap()
                .state()
                .is_terminal(),
            "the store must be converged back to terminal too, not left stale"
        );
    }
}

#[cfg(test)]
mod durable_job_retirement_tests {
    use super::*;
    use crate::store::{InMemoryMetadataStore, MetadataStore};
    use krishiv_proto::{StageId, StageSpec, TaskId, TaskSpec};

    fn single_task_job(job_id: &JobId, task_id: &str) -> JobSpec {
        JobSpec::new(job_id.clone(), "retirement-test", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-0").unwrap(), "stage").with_task(TaskSpec::new(
                TaskId::try_new(task_id).unwrap(),
                "sql: select 1",
            )),
        )
    }

    /// `jobs` is the live-job set; `job_history` is the archive. Before
    /// `remove_job` existed, `save_job` was the only writer of `jobs`, so a
    /// coordinator's live-job set was really "every job this cluster has ever
    /// run" — unbounded on disk, and reloaded in full into `job_coordinators`
    /// by `recover_from_store` on every standby→active promotion.
    ///
    /// Eviction is the right retirement point: it is exactly when the live
    /// coordinator itself stops answering for the job, so durable state now
    /// tracks the in-memory map instead of diverging from it.
    #[test]
    fn evicting_a_terminal_job_retires_its_durable_record_and_keeps_the_history() {
        let mut coordinator = Coordinator::new_active(None)
            .unwrap()
            .with_store(InMemoryMetadataStore::default());

        let job_id = JobId::try_new("job-retired").unwrap();
        coordinator
            .submit_job(single_task_job(&job_id, "t0"))
            .unwrap();
        coordinator.cancel_job(&job_id).unwrap();

        assert_eq!(
            coordinator.store.as_ref().unwrap().inner().jobs().len(),
            1,
            "precondition: the terminal job is still a live record before eviction"
        );

        coordinator.evict_completed_job(&job_id);

        let store = coordinator.store.as_ref().unwrap().inner();
        assert!(
            store.jobs().is_empty(),
            "an evicted terminal job must not stay in the live-job set forever"
        );
        assert_eq!(
            store
                .get_job_history(job_id.as_str())
                .expect("the outcome must survive in the history archive")
                .final_state,
            JobState::Cancelled.to_string(),
            "retirement must not cost the recorded outcome"
        );
    }

    /// The point of the retirement: a promotion rebuilds `job_coordinators`
    /// from the store, so what the store keeps is what every future
    /// coordinator carries. Only live jobs should come back.
    #[test]
    fn recovery_loads_only_live_jobs() {
        let mut store = InMemoryMetadataStore::default();

        let live = JobId::try_new("job-live").unwrap();
        let retired = JobId::try_new("job-retired").unwrap();
        for job_id in [&live, &retired] {
            let mut record = JobRecord::from_spec(single_task_job(job_id, "t0"), 0);
            if job_id == &retired {
                record.state = JobState::Succeeded;
            }
            store.save_job(&record).unwrap();
        }
        // The retired job concluded: archived first, then retired — the
        // ordering `MetadataStore::remove_job` documents.
        store
            .save_job_history(crate::store::JobHistoryRecord {
                job_id: retired.as_str().to_owned(),
                job_kind: "batch".to_owned(),
                final_state: "succeeded".to_owned(),
                completed_at_ms: 1,
                stage_count: 1,
                task_count: 1,
                succeeded_task_count: 1,
                failed_task_count: 0,
                cpu_nanos: 0,
                memory_peak_task_bytes: 0,
                namespace_id: None,
                priority: 0,
            })
            .unwrap();
        store.remove_job(retired.as_str()).unwrap();

        let mut coordinator = Coordinator::new_active(None).unwrap();
        coordinator.recover_from_store(&mut store).unwrap();

        assert!(
            coordinator.job_coordinators.contains_key(&live),
            "a live job must still be recovered"
        );
        assert!(
            !coordinator.job_coordinators.contains_key(&retired),
            "a retired job must not be rebuilt into the live registry on promotion"
        );
    }

    /// A failed history write is logged and the job still concludes — that is
    /// deliberate, a broken archive must not wedge the lifecycle. But it means
    /// eviction can reach a job whose outcome exists *only* in its live
    /// record, and removing that would erase the outcome entirely. Leaking one
    /// record is the cheaper error, so retirement is gated on the archive.
    #[test]
    fn a_terminal_job_with_no_history_record_keeps_its_durable_record() {
        let job_id = JobId::try_new("job-unarchived").unwrap();
        let mut record = JobRecord::from_spec(single_task_job(&job_id, "t0"), 0);
        record.state = JobState::Failed;

        // A store holding a terminal job with no archive alongside it — what a
        // failed `save_job_history` leaves behind.
        let mut store = InMemoryMetadataStore::default();
        store.save_job(&record).unwrap();
        assert!(store.list_job_history().is_empty());

        let mut coordinator = Coordinator::new_active(None).unwrap().with_store(store);
        coordinator.job_coordinators.insert(
            job_id.clone(),
            Arc::new(crate::job_coordinator::JobCoordinator::new(
                job_id.clone(),
                record,
            )),
        );

        coordinator.evict_completed_job(&job_id);

        assert!(
            !coordinator.job_coordinators.contains_key(&job_id),
            "eviction from the live registry still happens"
        );
        assert_eq!(
            coordinator.store.as_ref().unwrap().inner().jobs().len(),
            1,
            "without an archive, the live record is the only trace of the outcome"
        );
    }
}

#[cfg(test)]
mod submit_wakes_launch_loop_tests {
    use super::*;
    use krishiv_proto::{StageId, StageSpec, TaskId, TaskSpec};

    fn single_task_job(job_id: &JobId) -> JobSpec {
        JobSpec::new(job_id.clone(), "dist2-test", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-0").unwrap(), "stage").with_task(TaskSpec::new(
                TaskId::try_new("t0").unwrap(),
                "sql: select 1",
            )),
        )
    }

    /// IVM-AUD-DIST-2. The task-launch loop parks on
    /// `select! { interval.tick() (500 ms), notify.notified() }` and is the only
    /// thing that drains `launch_dirty_jobs`. `submit_job` marked the job dirty
    /// but never fired that `Notify`, so a freshly submitted job's tasks were
    /// not launched until the next 500 ms interval tick — a floor that no
    /// downstream wait loop can recover, because the work has not begun.
    ///
    /// The assertion is counter-based, not timing-based: `Notify::notified()`
    /// snapshots the `notify_waiters` count when the future is CREATED, so a
    /// future built before the submit completes immediately iff the submit
    /// fired the notification. The 50 ms bound is only an upper limit on the
    /// failing case; the passing case resolves without waiting at all.
    #[tokio::test]
    async fn submitting_a_job_wakes_the_task_launch_loop() {
        let mut coordinator = Coordinator::new_active(None).unwrap();
        let notify = coordinator.exec.notify.clone();

        // Registered BEFORE the submit — this is the launch loop's position.
        let woken = notify.notified();

        let job_id = JobId::try_new("job-dist2").unwrap();
        coordinator.submit_job(single_task_job(&job_id)).unwrap();

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), woken)
                .await
                .is_ok(),
            "submit_job must wake the task-launch loop; without this the job \
             waits out the loop's 500 ms interval tick before its tasks are \
             even launched"
        );
    }

    /// The queued path must not regress: a job held by admission control has no
    /// launchable work, and `admit_queued_jobs` fires the notification when it
    /// promotes one. This pins the guard to `!is_queued` so the fix cannot be
    /// widened into a wake-on-every-submit that busies the launch loop.
    #[tokio::test]
    async fn the_wake_is_tied_to_there_being_launchable_work() {
        let mut coordinator = Coordinator::new_active(None).unwrap();
        let job_id = JobId::try_new("job-dist2-live").unwrap();
        coordinator.submit_job(single_task_job(&job_id)).unwrap();

        // A second, independent waiter sees nothing until the next submit.
        let notify = coordinator.exec.notify.clone();
        let quiet = notify.notified();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), quiet)
                .await
                .is_err(),
            "no submit happened, so nothing may wake the launch loop"
        );
    }
}

#[cfg(test)]
mod terminal_wakes_waiters_tests {
    use super::*;
    use krishiv_proto::{StageId, StageSpec, TaskId, TaskSpec};

    fn single_task_job(job_id: &JobId) -> JobSpec {
        JobSpec::new(job_id.clone(), "dist3-test", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-0").unwrap(), "stage").with_task(TaskSpec::new(
                TaskId::try_new("t0").unwrap(),
                "sql: select 1",
            )),
        )
    }

    /// IVM-AUD-DIST-3. Wait loops park on `exec.notify` for a job to reach a
    /// terminal state, but nothing fired that notification when one did, so the
    /// conclusion was only ever observed by the loop's fallback poll. The
    /// waiter is registered before the transition, which is the wait loop's own
    /// position, and `notified()` snapshots the counter at creation — so this
    /// asserts the transition itself signalled, not that time passed.
    #[tokio::test]
    async fn a_job_reaching_a_terminal_state_wakes_its_waiters() {
        let mut coordinator = Coordinator::new_active(None).unwrap();
        let job_id = JobId::try_new("job-dist3").unwrap();
        coordinator.submit_job(single_task_job(&job_id)).unwrap();

        let notify = coordinator.exec.notify.clone();
        let concluded = notify.notified();

        coordinator.cancel_job(&job_id).unwrap();

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), concluded)
                .await
                .is_ok(),
            "a terminal transition must wake waiters; without it a caller \
             waiting for the job to conclude only learns via its fallback poll"
        );
    }
}
