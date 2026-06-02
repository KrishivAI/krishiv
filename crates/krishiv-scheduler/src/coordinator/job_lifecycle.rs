use super::*;

impl Coordinator {
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
        let outcome = self.queue_manager.admit(&spec, &quota);
        if let SubmitOutcome::Queued { .. } = &outcome {
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
        let job_name = spec.name().to_owned();
        let namespace = spec
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
        krishiv_governance::audit_log(
            "scheduler",
            &krishiv_governance::AuditAction::JobSubmitted {
                job_id: inserted_job_id.to_string(),
            },
            krishiv_governance::AuditOutcome::Allowed,
        );

        // GAP-OB-06: Emit OpenLineage START event.
        // Only spawn when a Tokio runtime is active (production); skip in
        // synchronous test contexts — the event is advisory, not critical.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _j_id = inserted_job_id.to_string();
            handle.spawn(async move {
                let event = krishiv_governance::new_run_event(
                    krishiv_governance::RunEventType::Start,
                    job_name,
                    namespace,
                    vec![],
                    vec![],
                );
                krishiv_governance::emit_lineage_event(event).await;
            });
        }

        Ok(SubmitOutcome::Accepted)
    }

    /// Cancel a job and mark non-terminal stages/tasks cancelled.
    pub fn cancel_job(&mut self, job_id: &JobId) -> SchedulerResult<()> {
        self.ensure_active()?;
        let (job_name, namespace) = {
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

        krishiv_governance::audit_log(
            "scheduler",
            &krishiv_governance::AuditAction::JobCancelled {
                job_id: job_id.to_string(),
            },
            krishiv_governance::AuditOutcome::Allowed,
        );

        if !self.gc_ready_jobs.contains(job_id) {
            const MAX_GC_JOBS: usize = 1000;
            if self.gc_ready_jobs.len() >= MAX_GC_JOBS {
                self.gc_ready_jobs.remove(0);
            }
            self.gc_ready_jobs.push(job_id.clone());
        }
        self.checkpoint_coordinators.remove(job_id);
        self.job_input_partitions.remove(job_id);
        self.batch_sql_job_tables.remove(job_id);

        // Emit OpenLineage FAIL event for job cancellation (Phase 3 M5 / GAP-OB-06)
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let event = krishiv_governance::new_run_event(
                    krishiv_governance::RunEventType::Fail,
                    job_name,
                    namespace,
                    vec![],
                    vec![],
                );
                krishiv_governance::emit_lineage_event(event).await;
            });
        }
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
        let inline_ipc = update
            .output_metadata()
            .map(|meta| meta.inline_record_batch_ipc().to_vec())
            .unwrap_or_default();
        let terminal_state = update.state();
        let executor_id_for_circuit = update.executor_id().clone();
        let outcome = self.find_job_mut(&job_id)?.apply_task_update(update)?;

        // IMM-2 (Circuit Breaker Strengthening):
        // Record failure and, if the executor is now bad, clear the assignment
        // so the task can be re-assigned to a healthy executor on the next launch cycle.
        if terminal_state == TaskState::Failed {
            krishiv_governance::audit_log(
                "scheduler",
                &krishiv_governance::AuditAction::TaskFailed {
                    job_id: job_id.to_string(),
                    stage_id: stage_id.to_string(),
                    task_id: task_id.to_string(),
                    attempt_id: attempt,
                },
                krishiv_governance::AuditOutcome::Allowed,
            );

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
            self.executors.reset_task_failures(&executor_id_for_circuit);
        }

        if terminal_state == TaskState::Succeeded && !inline_ipc.is_empty() {
            self.job_inline_results
                .entry(job_id.clone())
                .or_default()
                .extend(inline_ipc);
        }

        // Snapshot the job's current state and resource usage after the update.
        let (is_terminal, usage, state, job_name, namespace) = self
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
                ResourceUsage::zero(),
                JobState::Accepted,
                String::new(),
                String::new(),
            ));

        if is_terminal && !self.gc_ready_jobs.contains(&job_id) {
            const MAX_GC_JOBS: usize = 1000;
            if self.gc_ready_jobs.len() >= MAX_GC_JOBS {
                self.gc_ready_jobs.remove(0);
            }
            self.gc_ready_jobs.push(job_id.clone());
            self.checkpoint_coordinators.remove(&job_id);
            // Free inline input data (InlineIpc partitions for batch-sql and
            // bounded-window jobs) — executors have already consumed this by the
            // time the job reaches a terminal state.
            self.job_input_partitions.remove(&job_id);
            self.batch_sql_job_tables.remove(&job_id);
            self.queue_manager.on_job_complete(&job_id, &usage);

            // Emit OpenLineage COMPLETE/FAIL events (Phase 3 M5 / GAP-OB-06)
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let event_type = match state {
                    JobState::Succeeded => krishiv_governance::RunEventType::Complete,
                    _ => krishiv_governance::RunEventType::Fail,
                };
                handle.spawn(async move {
                    let event = krishiv_governance::new_run_event(
                        event_type,
                        job_name,
                        namespace,
                        vec![],
                        vec![],
                    );
                    krishiv_governance::emit_lineage_event(event).await;
                });
            }
        }
        if let Some(record) = self
            .job_coordinators
            .get(&job_id)
            .map(|jc| jc.read_record())
            && let Some(store) = &self.store
        {
            if terminal_state.is_terminal() {
                // Synchronous durable commit for critical task state transitions
                let mut guard = store.inner();
                guard.save_job(&record)?;
            } else {
                // Non-blocking fire-and-forget: enqueue the save to background task.
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
    pub fn take_gc_ready_jobs(&mut self) -> Vec<JobId> {
        std::mem::take(&mut self.gc_ready_jobs)
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
        let aqe = krishiv_optimizer::default_aqe_optimizer();
        let (optimized, _applied) = aqe.apply(plan.clone(), &[]);
        self.submit_job(job_spec_from_physical_plan(job_id, &optimized)?)
    }
}
