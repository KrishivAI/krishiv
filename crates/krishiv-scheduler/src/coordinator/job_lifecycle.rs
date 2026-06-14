use super::*;

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

        if self.job_coordinators.contains_key(spec.job_id()) {
            return Err(SchedulerError::DuplicateJob {
                job_id: spec.job_id().clone(),
            });
        }

        // Admission control: compute live quota snapshot then ask the queue manager.
        let quota = self.namespace_quota_snapshot(spec.namespace_id());
        let mut outcome = self.queue_manager.admit(&spec, &quota);

        // Memory-estimate admission: when the job declares a memory ask and the
        // cluster reports memory capacity via heartbeats, queue the job if its
        // ask exceeds what is currently available across schedulable executors.
        // Unknown capacity (no executor reports a memory limit) skips the check
        // so clusters without memory reporting are unaffected.
        if matches!(outcome, SubmitOutcome::Accepted)
            && let Some(ask) = spec.memory_limit_bytes()
            && ask > 0
            && let Some(available) = self.executors.cluster_available_memory_bytes()
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

        if let SubmitOutcome::Queued { .. } = &outcome {
            if krishiv_common::profile_requires_fail_closed_metadata(self.durability_profile) {
                return Err(SchedulerError::InvalidJob {
                    message: format!(
                        "job {} was queued by admission control but durable profiles require \
                         immediate admission; increase quota or reduce concurrent load",
                        spec.job_id()
                    ),
                });
            }
            return Ok(outcome);
        }

        // Prepare (but don't yet commit) a CheckpointCoordinator for streaming jobs.
        // A7: We previously inserted the coordinator into `checkpoint_coordinators`
        // before persisting the job — if `save_job` failed, the in-memory coordinator
        // leaked.  Now we open storage here, hand the constructed `CheckpointCoordinator`
        // over only after the job record is durably saved AND inserted in memory.
        let mut pending_checkpoint: Option<CheckpointCoordinator> = None;
        if spec.kind() == JobKind::Streaming
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
        let executors = self.executors.schedulable_executors();
        let job_id = spec.job_id().clone();
        let _job_name = spec.name().to_owned();
        let _namespace = spec
            .namespace_id()
            .map(|s| s.to_owned())
            .unwrap_or_default();
        let mut record = JobRecord::from_spec(spec, self.config.max_stage_retries());
        if !executors.is_empty() {
            let assignments = SlotAwareScheduler::place(&record.spec, &executors)?;
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
            self.checkpoint_coordinators
                .insert(inserted_job_id.clone(), ckpt_coord);
        }
        // P1.1: Index streaming tasks for O(1) heartbeat lookup.
        self.index_streaming_tasks(&inserted_job_id);
        // GAP-OB-01: Increment jobs_submitted counter.
        JOBS_SUBMITTED_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
        krishiv_metrics::global_metrics().inc_tasks_submitted();
        Ok(SubmitOutcome::Accepted)
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

        if !self.gc_ready_jobs.contains(job_id) {
            const MAX_GC_JOBS: usize = 1000;
            if self.gc_ready_jobs.len() >= MAX_GC_JOBS {
                self.gc_ready_jobs.pop_front();
            }
            self.gc_ready_jobs.push_back(job_id.clone());
        }
        self.checkpoint_coordinators.remove(job_id);
        self.job_inline_results.remove(job_id);
        self.job_input_partitions.remove(job_id);
        self.job_task_input_partitions.remove(job_id);
        self.continuous_input_cycles.remove(job_id);
        self.batch_sql_job_tables.remove(job_id);

        Ok(())
    }

    /// Apply a task update from an executor.
    pub fn apply_task_update(
        &mut self,
        update: TaskStatusUpdate,
    ) -> SchedulerResult<TaskUpdateOutcome> {
        self.ensure_active()?;
        self.executors
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
        let terminal_state = update.state();
        let executor_id_for_circuit = update.executor_id().clone();
        // Save before update is moved.
        let missing_partitions: Vec<krishiv_proto::MissingShufflePartition> =
            update.missing_shuffle_partitions().to_vec();
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

        // IMM-2 (Circuit Breaker Strengthening):
        // Record failure and, if the executor is now bad, clear the assignment
        // so the task can be re-assigned to a healthy executor on the next launch cycle.
        if terminal_state == TaskState::Failed {
            krishiv_metrics::global_metrics().inc_tasks_failed();

            let threshold = self.config.circuit_breaker_failure_threshold();
            let exceeded = self
                .executors
                .record_task_failure(&executor_id_for_circuit, threshold);
            if exceeded {
                tracing::warn!(
                    executor_id = %executor_id_for_circuit,
                    "executor exceeded failure threshold — clearing assignments for re-launch on healthy executors"
                );

                if let Some(jc) = self.job_coordinator(&job_id) {
                    let jc = jc.clone();
                    let eid = executor_id_for_circuit.clone();
                    tokio::spawn(async move {
                        let _ = jc.clear_assignments_for_bad_executor_and_count(&eid).await;
                    });
                } else {
                    if let Ok(mut job) = self.find_job_mut(&job_id) {
                        for stage in job.stages_mut() {
                            for task in stage.tasks_mut() {
                                if task.assigned_executor.as_ref() == Some(&executor_id_for_circuit)
                                {
                                    task.assigned_executor = None;
                                    task.launch_in_flight = false;
                                }
                            }
                        }
                    }
                }

                tracing::debug!(
                    job_id = %job_id,
                    executor_id = %executor_id_for_circuit,
                    "circuit breaker triggered; assignments cleared via JCP or fallback"
                );
                self.notify.notify_waiters();
            }
        } else if terminal_state == TaskState::Succeeded {
            krishiv_metrics::global_metrics().inc_tasks_succeeded();
            self.executors.reset_task_failures(&executor_id_for_circuit);
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
                self.notify.notify_waiters();
            }
        }

        if terminal_state == TaskState::Succeeded && !inline_ipc.is_empty() {
            self.job_inline_results
                .entry(job_id.clone())
                .or_default()
                .extend(inline_ipc);
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
                if !stats.is_empty() && stats.iter().any(|s| s.serialized_bytes > 0) {
                    let aqe = krishiv_plan::optimizer::default_aqe_optimizer();
                    let placeholder = krishiv_plan::PhysicalPlan::new(
                        job_id.as_str(),
                        krishiv_plan::ExecutionKind::Batch,
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
        } else if is_continuous_cycle && terminal_state == TaskState::Failed {
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
            self.checkpoint_coordinators.remove(&job_id);
            // Free inline input data (InlineIpc partitions for batch-sql and
            // bounded-window jobs) — executors have already consumed this by the
            // time the job reaches a terminal state.
            self.job_input_partitions.remove(&job_id);
            self.job_task_input_partitions.remove(&job_id);
            self.continuous_input_cycles.remove(&job_id);
            self.batch_sql_job_tables.remove(&job_id);
            if state != JobState::Succeeded {
                self.job_inline_results.remove(&job_id);
            }
            self.queue_manager.on_job_complete(&job_id, &usage);

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
                    let _ = guard.save_job_history(history);
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
        self.job_input_partitions.remove(job_id);
        self.job_task_input_partitions.remove(job_id);
        self.continuous_input_cycles.remove(job_id);
        self.batch_sql_job_tables.remove(job_id);
        self.checkpoint_coordinators.remove(job_id);
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
        self.restore_directives.remove(job_id);
        self.pending_stop_after_savepoint.remove(job_id);
        self.restore_notify_sent.retain(|(jid, _, _)| jid != job_id);
        self.checkpoint_complete_sent
            .retain(|(jid, _, _)| jid != job_id);
        self.checkpoint_notify_sent
            .retain(|(jid, _, _)| jid != job_id);
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
