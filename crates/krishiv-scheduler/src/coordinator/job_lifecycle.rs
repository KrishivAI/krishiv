use super::{
    Arc, AtomicOrdering, AttemptId, CheckpointCoordinator, Coordinator, EventLogEvent,
    JOBS_SUBMITTED_TOTAL, JobId, JobKind, JobRecord, JobSpec, JobState, LogicalPlan, PhysicalPlan,
    ResourceUsage, SchedulerError, SchedulerResult, SlotAwareScheduler, StageState, SubmitOutcome,
    TaskState, TaskStatusUpdate, TaskUpdateOutcome, job_spec_from_logical_plan,
    job_spec_from_physical_plan, validate_job,
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
                let job_id = spec.job_id().clone();
                self.evict_completed_job(&job_id);
            } else {
                return Err(SchedulerError::DuplicateJob {
                    job_id: spec.job_id().clone(),
                });
            }
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
        let executors = self.exec.executors.schedulable_executor_placements();
        let job_id = spec.job_id().clone();
        let _job_name = spec.name().to_owned();
        let _namespace = spec
            .namespace_id()
            .map(|s| s.to_owned())
            .unwrap_or_default();
        let mut record = JobRecord::from_spec(spec, self.config.max_stage_retries());
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
        if let Some(store) = &self.store {
            let mut guard = store.inner();
            guard.save_job(&record)?;
            guard.append_event(EventLogEvent::JobSubmitted {
                job_id: job_id.clone(),
            })?;
        }
        let inserted_job_id = record.job_id().clone();

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
        // GAP-OB-01: Increment jobs_submitted counter.
        JOBS_SUBMITTED_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
        krishiv_metrics::global_metrics().inc_tasks_submitted();
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
            let mut guard = store.inner();
            guard.save_job(&record)?;
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

        if let Some(store) = &self.store {
            let mut guard = store.inner();
            if let Err(e) = guard.append_event(EventLogEvent::JobCancelled {
                job_id: job_id.clone(),
            }) {
                tracing::warn!(job_id = %job_id, error = %e, "failed to append JobCancelled event");
            }
        }

        if !self.gc_ready_jobs.contains(job_id) {
            const MAX_GC_JOBS: usize = 1000;
            if self.gc_ready_jobs.len() >= MAX_GC_JOBS {
                self.gc_ready_jobs.pop_front();
            }
            self.gc_ready_jobs.push_back(job_id.clone());
        }
        self.ckpt.coordinators.remove(job_id);
        self.job_inline_results.remove(job_id);
        self.job_result_spools.remove(job_id);
        self.pending_task_result_spools
            .retain(|key, _| key.job_id != *job_id);
        self.job_input_partitions.remove(job_id);
        self.job_task_input_partitions.remove(job_id);
        self.continuous_input_cycles.remove(job_id);
        self.pending_continuous_restores.remove(job_id);
        self.batch_sql_job_tables.remove(job_id);

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
        if terminal_state == TaskState::Failed {
            krishiv_metrics::global_metrics().inc_tasks_failed();

            let threshold = self.config.circuit_breaker_failure_threshold();
            let exceeded = self
                .exec
                .executors
                .record_task_failure(&executor_id_for_circuit, threshold);
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
            tracing::warn!(
                job_id = %job_id,
                stage_id = %stage_id,
                missing_count = missing_partitions.len(),
                "consumer task reported missing upstream shuffle partitions; invalidating producers"
            );
            let producers_affected = if let Ok(mut job) = self.find_job_mut(&job_id) {
                job.invalidate_specific_shuffle_partitions(&missing_partitions)
            } else {
                false
            };
            if producers_affected {
                self.exec.notify.notify_waiters();
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
                    && !stats.is_empty()
                    && stats.iter().any(|s| s.serialized_bytes > 0)
                {
                    let aqe = krishiv_plan::optimizer::default_aqe_optimizer();
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

        // Phase 2.3 distributed write commit: when this update drove the job
        // to a terminal state, publish staged sink outputs (job success) or
        // clean up staging (failure/cancel). Runs before the state snapshot
        // below so a publish failure demotes the job to Failed prior to
        // persistence and GC bookkeeping.
        self.finalize_staged_sink_outputs(&job_id);

        // Snapshot the job's current state and resource usage after the update.
        let (is_terminal, usage, state, _job_name, _namespace) = self
            .job_coordinators
            .get(&job_id)
            .map(|jc| {
                let r = jc.read_record();
                (
                    r.state().is_terminal(),
                    r.resource_usage.clone(),
                    r.state(),
                    r.spec.name().to_owned(),
                    r.spec
                        .namespace_id()
                        .map(|s| s.to_owned())
                        .unwrap_or_default(),
                )
            })
            .unwrap_or((
                false,
                ResourceUsage::default(),
                JobState::Accepted,
                String::new(),
                String::new(),
            ));

        if is_terminal && !self.gc_ready_jobs.contains(&job_id) {
            const MAX_GC_JOBS: usize = 1000;
            if self.gc_ready_jobs.len() >= MAX_GC_JOBS {
                self.gc_ready_jobs.pop_front();
            }
            self.gc_ready_jobs.push_back(job_id.clone());
            self.ckpt.coordinators.remove(&job_id);
            // Free inline input data (InlineIpc partitions for batch-sql and
            // bounded-window jobs) — executors have already consumed this by the
            // time the job reaches a terminal state.
            self.job_input_partitions.remove(&job_id);
            self.job_task_input_partitions.remove(&job_id);
            self.continuous_input_cycles.remove(&job_id);
            self.pending_continuous_restores.remove(&job_id);
            self.batch_sql_job_tables.remove(&job_id);
            self.pending_task_result_spools
                .retain(|key, _| key.job_id != job_id);
            if state != JobState::Succeeded {
                self.job_inline_results.remove(&job_id);
                self.job_result_spools.remove(&job_id);
            }
            self.queue_manager.on_job_complete(&job_id, &usage);

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
            if let Some(jc) = self.job_coordinators.get(&job_id) {
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
                let mut guard = store.inner();
                guard.save_job(&record)?;
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
        }
        Ok(outcome)
    }

    /// Drain the list of jobs that have reached a terminal state and need shuffle GC.
    ///
    /// The coordinator binary's tick loop should call this, then asynchronously
    /// delete partitions for each returned job id via the shuffle store.
    /// S3: Also evicts each job from `job_coordinators` to prevent unbounded map
    /// growth. Eviction happens here (not in `apply_task_update`) so that the job
    /// snapshot remains queryable until the GC cycle runs.
    pub fn take_gc_ready_jobs(&mut self) -> Vec<JobId> {
        let jobs: Vec<JobId> = std::mem::take(&mut self.gc_ready_jobs)
            .into_iter()
            .collect();
        for job_id in &jobs {
            self.evict_completed_job(job_id);
        }
        jobs
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
        self.streaming_task_index
            .retain(|_, (jid, _)| jid != job_id);
        // S4: Evict adaptive decision log entries for the completed job to
        // prevent unbounded HashMap growth on long-running coordinators.
        self.adaptive_decision_log.remove(job_id);
        // S1: Evict any pending skew repartition override. Safety-net for jobs
        // that finish before their next task-launch cycle consumes the entry.
        self.skew_repartition_overrides.remove(job_id);
        self.streaming_advisory_partitions.remove(job_id);
        self.aqe_coalesce_hints.retain(|(jid, _), _| jid != job_id);
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
        let aqe = krishiv_plan::optimizer::default_aqe_optimizer();
        let (optimized, _applied) = aqe.apply(plan.clone(), &[])?;
        self.submit_job(job_spec_from_physical_plan(job_id, &optimized)?)
    }
}
