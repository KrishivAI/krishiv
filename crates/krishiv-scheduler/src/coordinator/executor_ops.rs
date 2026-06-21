use super::*;

impl Coordinator {
    /// Register an executor with the active coordinator.
    #[tracing::instrument(
        level = "info",
        skip(self, descriptor),
        fields(executor_id = %descriptor.executor_id(), host = %descriptor.host(), slots = descriptor.slots())
    )]
    pub fn register_executor(
        &mut self,
        descriptor: ExecutorDescriptor,
    ) -> SchedulerResult<LeaseGeneration> {
        self.ensure_active()?;
        // Persist before admitting into the in-memory registry so a metadata
        // failure does not leave a worker accepted only until process restart.
        if let Some(ref store) = self.store {
            store.save_executor(&descriptor);
        }
        let res = self.exec.executors.register(descriptor.clone());
        if res.is_ok() {
            self.assign_pending_tasks_for_schedulable_jobs();
            self.exec.notify.notify_waiters();
        }
        res
    }

    /// Deregister an executor with a valid lease generation.
    pub fn deregister_executor(
        &mut self,
        executor_id: &ExecutorId,
        lease_generation: LeaseGeneration,
    ) -> SchedulerResult<LeaseGeneration> {
        self.ensure_active()?;
        let res = self.exec.executors.deregister(executor_id, lease_generation);
        if res.is_ok() {
            // Evict the executor's gRPC channel so stale TCP connections
            // do not leak (Phase 1.3).
            if let Ok(record) = self.exec.executors.find_executor(executor_id)
                && let Some(endpoint) = record.descriptor().task_endpoint()
            {
                self.executor_channels.remove(endpoint);
            }
            // R10: Remove the persisted descriptor — clean deregister means the
            // executor won't be auto-restored on next coordinator restart.
            if let Some(ref store) = self.store {
                store.remove_executor(executor_id);
            }
            self.exec.notify.notify_waiters();
        }
        res
    }

    /// Apply an executor heartbeat.
    ///
    /// For streaming executors re-attaching after a coordinator restart, the heartbeat may
    /// include `streaming_task_states`. These are applied to the matching task records so
    /// the coordinator tracks the executor's current watermark and source offset without
    /// re-submitting the job from scratch.
    ///
    /// Returns throttle commands to forward back to the executor (R7.2 Group C).
    pub fn executor_heartbeat(
        &mut self,
        heartbeat: ExecutorHeartbeat,
    ) -> SchedulerResult<ExecutorHeartbeatEffects> {
        self.ensure_active()?;
        let executor_id = heartbeat.executor_id().clone();
        let fallback_lease = heartbeat.lease_generation();
        let streaming_states: Vec<StreamingTaskState> = heartbeat.streaming_task_states().to_vec();
        let hot_key_reports = heartbeat.hot_key_reports().to_vec();
        let streaming_progress: Vec<StreamingProgressReport> =
            heartbeat.streaming_progress().to_vec();
        self.exec.executors.heartbeat(heartbeat)?;

        // Wire executor slot usage to the global metrics gauge so the
        // Prometheus krishiv_executor_slots_used metric reflects live state.
        if let Ok(exec_rec) = self.exec.executors.find_executor(&executor_id) {
            let slots_used = exec_rec.running_tasks().len() as u64;
            krishiv_metrics::global_metrics()
                .set_executor_slots_used(executor_id.as_str(), slots_used);
        }

        self.assign_pending_tasks_for_schedulable_jobs();
        for state in &streaming_states {
            self.apply_streaming_task_state(state);
        }
        // R7.2 Group D: process hot-key reports and record adaptive decisions.
        let mut source_throttles = self.process_hot_key_reports(&hot_key_reports);
        if let Some(pending) = self.pending_source_throttles.remove(&executor_id) {
            source_throttles.extend(pending);
        }
        // Record streaming progress for observability (watermark, throughput, state size).
        for report in &streaming_progress {
            self.record_streaming_progress(report);
        }
        let checkpoint_commands = self.pending_initiate_checkpoints_for_executor(&executor_id);
        // Restore directives must precede new checkpoint work on the executor:
        // the executor processes restores before initiate commands, so command
        // ordering here only affects the same-response case which the executor
        // handles explicitly.
        let restore_commands = self.pending_restore_commands_for_executor(&executor_id);
        let checkpoint_complete_commands =
            self.pending_checkpoint_complete_for_executor(&executor_id);
        let lease_generation = self
            .exec
            .executors
            .find_executor(&executor_id)
            .map(|e| e.lease_generation())
            .unwrap_or(fallback_lease);

        self.exec.notify.notify_waiters();

        Ok(ExecutorHeartbeatEffects {
            source_throttles,
            checkpoint_commands,
            checkpoint_complete_commands,
            restore_commands,
            lease_generation,
        })
    }

    /// Record adaptive decisions for incoming hot-key reports and return throttle
    /// commands to send back to the executor.
    ///
    /// For each hot key whose `heat_score` exceeds `HOT_KEY_HEAT_THRESHOLD`, an
    /// `AdaptiveDecisionLog` entry is recorded AND a `ThrottleDecision` is returned
    /// so the executor can immediately reduce the source's ingestion rate.
    ///
    /// The throttle rate is set to `(1.0 - heat_score) * base_rows_per_second`
    /// (floor: 1 row/s) so hotter keys receive more aggressive throttling.
    ///
    /// If `disable_hot_key_splitting` is set the decision is logged with
    /// `applied = false` and no throttle command is emitted.
    pub(crate) fn process_hot_key_reports(
        &mut self,
        reports: &[HeartbeatHotKeyReport],
    ) -> Vec<crate::adaptive::ThrottleDecision> {
        const HOT_KEY_HEAT_THRESHOLD: f64 = 0.3;
        let base_rows_per_second = self.adaptive_override.hot_key_base_rows_per_second;

        if reports.is_empty() {
            return Vec::new();
        }
        let now_ms = u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0);
        let mut throttles = Vec::new();

        for report in reports {
            let job_id = report.job_id.clone();
            let is_hot = report.heat_score >= HOT_KEY_HEAT_THRESHOLD;
            let applied = is_hot && !self.adaptive_override.disable_hot_key_splitting;
            let log = AdaptiveDecisionLog {
                timestamp_ms: now_ms,
                kind: AdaptiveDecisionKind::HotKeySplit,
                affected_job_id: job_id.clone(),
                details: format!(
                    "hot key '{}' heat={:.3} estimated_count={} max_error={}",
                    report.key, report.heat_score, report.estimated_count, report.max_error
                ),
                applied,
            };
            let log_bucket = self
                .adaptive_decision_log
                .entry(job_id.clone())
                .or_default();
            const MAX_LOG_PER_JOB: usize = 100;
            if log_bucket.len() >= MAX_LOG_PER_JOB {
                log_bucket.pop_front(); // O(1) with VecDeque
            }
            log_bucket.push_back(log);

            if applied {
                // Clamp heat_score to [0, 1] to prevent invalid calculations from NaN or out-of-range values.
                let heat = report.heat_score.clamp(0.0_f64, 1.0_f64);
                // Throttle the source proportional to its heat score.
                let reduced_rate = ((1.0 - heat) * base_rows_per_second as f64).max(1.0) as u64;
                throttles.push(crate::adaptive::ThrottleDecision {
                    source_id: report.source_id.clone(),
                    rows_per_second: Some(reduced_rate),
                });
                tracing::info!(
                    source_id = %report.source_id,
                    heat_score = report.heat_score,
                    throttle_rate = reduced_rate,
                    "hot-key throttle applied"
                );
                // S1: Mark the job for round-robin repartitioning on the next
                // task batch. This spreads hot-key data evenly across all
                // available executor slots rather than concentrating it on the
                // bucket that hashes to the hot key.
                //
                // SAFETY: Never apply to streaming jobs. Streaming stages use
                // keyed partitioning — every record for a given key must reach
                // the same executor task for the lifetime of the job. Changing
                // the partition scheme mid-stream would scatter state for the
                // same key across multiple tasks, producing incorrect window
                // aggregation results. For streaming hot keys the only safe
                // mitigation is source throttling (already applied above).
                let is_streaming = self
                    .job_coordinators
                    .get(&job_id)
                    .map(|jc| jc.read_record().spec.kind() == JobKind::Streaming)
                    .unwrap_or(false);
                if !is_streaming {
                    let buckets = self.exec.executors.list().len().max(2) as u32;
                    self.skew_repartition_overrides
                        .insert(job_id.clone(), buckets);
                } else {
                    tracing::debug!(
                        job_id = %job_id,
                        key = %report.key,
                        "hot-key repartition override skipped for streaming job \
                         (keyed state must stay pinned to its assigned task)"
                    );
                }
            }
        }
        throttles
    }

    /// Record the EMA-derived advisory partition count for a streaming job.
    ///
    /// Called by the in-process runtime after each streaming task cycle to
    /// propagate the `StreamingPartitionAdvisor` recommendation.  The stored
    /// value is used by `launch_assigned_task_assignments` to choose the number
    /// of tasks for the next cycle.  A new observation replaces the previous
    /// one — only the latest advisory matters.
    pub fn record_streaming_advisory_buckets(&mut self, job_id: &JobId, buckets: u32) {
        if buckets > 0 {
            self.streaming_advisory_partitions
                .insert(job_id.clone(), buckets);
        }
    }

    /// Return the current advisory partition count for a streaming job, if any.
    pub fn streaming_advisory_partitions(&self, job_id: &JobId) -> Option<u32> {
        self.streaming_advisory_partitions.get(job_id).copied()
    }

    /// Mark an executor lost and release its running task assignments for retry.
    pub fn mark_executor_lost(&mut self, executor_id: &ExecutorId) -> SchedulerResult<()> {
        self.ensure_active()?;
        self.prune_executor_channel(executor_id);
        self.exec.executors.mark_lost(executor_id)?;
        self.handle_executor_loss_for_checkpoints(executor_id);
        self.reset_running_tasks_for_lost_executor(executor_id);
        krishiv_metrics::global_metrics().inc_executor_lost();
        Ok(())
    }

    /// Checkpoint-protocol reaction to an executor loss.
    ///
    /// For every checkpointed streaming job with running tasks on the lost
    /// executor:
    ///
    /// 1. An in-flight `AwaitingAcks` epoch is aborted immediately — the lost
    ///    executor can never ack it, and waiting for the full ack timeout only
    ///    delays recovery.  Epochs already in `Committing` continue: quorum was
    ///    reached and the storage write must run to completion.
    /// 2. A [`RestoreDirective`] is set to the last committed epoch (global
    ///    rollback).  All executors of the job — including survivors — must
    ///    reload state from that epoch, because rewound sources re-deliver the
    ///    post-checkpoint data and surviving state would double-count it.
    ///
    /// Must be called *before* `reset_running_tasks_for_lost_executor`, while
    /// task→executor assignments still identify the affected jobs.
    pub(crate) fn handle_executor_loss_for_checkpoints(&mut self, lost_id: &ExecutorId) {
        let affected_jobs: Vec<JobId> = self
            .ckpt
            .coordinators
            .keys()
            .filter(|job_id| self.executor_has_running_task_in_job(lost_id, job_id))
            .cloned()
            .collect();

        for job_id in affected_jobs {
            let Some(coord) = self.ckpt.coordinators.get_mut(&job_id) else {
                continue;
            };
            if let CheckpointCoordinatorState::AwaitingAcks { epoch, .. } = coord.state {
                coord.abort_epoch(&format!("executor {lost_id} lost during epoch {epoch}"));
                self.ckpt.notify_sent
                    .retain(|(jid, _, e)| jid != &job_id || *e != epoch);
                self.ckpt.barrier_sent
                    .retain(|(jid, e)| jid != &job_id || *e != epoch);
                tracing::warn!(
                    job_id = %job_id,
                    epoch,
                    executor_id = %lost_id,
                    "aborted in-flight checkpoint epoch after executor loss"
                );
            }

            let Some(coord) = self.ckpt.coordinators.get(&job_id) else {
                continue;
            };
            // The rollback target is the last DURABLY committed epoch from
            // storage — the in-memory state machine no longer rests on it
            // after the abort above.
            let committed = match krishiv_state::checkpoint::latest_valid_epoch(
                coord.storage().as_ref(),
                job_id.as_str(),
            ) {
                Ok(epoch) => Some(epoch),
                Err(krishiv_state::checkpoint::CheckpointError::NoValidEpoch) => None,
                Err(error) => {
                    tracing::error!(
                        job_id = %job_id,
                        error = %error,
                        "cannot determine last committed epoch after executor loss; \
                         no rollback directive will be issued"
                    );
                    None
                }
            };
            match committed {
                Some(epoch) => {
                    self.set_restore_directive(
                        &job_id,
                        RestoreDirective {
                            epoch,
                            fencing_token: coord.fencing_token().as_u64(),
                        },
                    );
                    tracing::warn!(
                        job_id = %job_id,
                        epoch,
                        executor_id = %lost_id,
                        "executor loss in checkpointed streaming job: \
                         directing global rollback to last committed epoch"
                    );
                }
                None => {
                    tracing::warn!(
                        job_id = %job_id,
                        executor_id = %lost_id,
                        "executor loss in checkpointed streaming job with no \
                         committed epoch; tasks restart from their source origin"
                    );
                }
            }
        }
    }

    fn prune_executor_channel(&mut self, executor_id: &ExecutorId) {
        if let Ok(record) = self.exec.executors.find_executor(executor_id)
            && let Some(endpoint) = record.descriptor().task_endpoint()
        {
            self.executor_channels.remove(endpoint);
        }
    }

    /// Advance the deterministic heartbeat clock and mark timed-out executors lost.
    ///
    /// Tasks previously assigned to lost executors are reset to `Assigned` so they
    /// will be relaunched on the next `launch_assigned_task_assignments` call.
    ///
    /// During the streaming re-attach grace period after a coordinator restart,
    /// executors that own Running tasks in streaming jobs are not evicted even if
    /// they have missed heartbeats. This gives them time to re-register without
    /// forcing a full streaming job re-run.
    pub fn advance_heartbeat_clock(&mut self, ticks: u64) -> SchedulerResult<Vec<ExecutorId>> {
        self.ensure_active()?;
        // Advance the restart tick counter.
        self.exec.ticks_since_restart = self.exec.ticks_since_restart.saturating_add(ticks);

        let in_grace_period = self.exec.recovering
            && self.exec.ticks_since_restart <= self.config.streaming_reattach_grace_ticks();

        let lost = self.exec.executors.advance_clock(ticks);
        let mut evicted: Vec<ExecutorId> = Vec::new();
        for lost_id in &lost {
            // During the re-attach grace period, skip evicting executors that own
            // Running tasks in streaming jobs so they can re-register.
            if in_grace_period && self.executor_has_streaming_running_tasks(lost_id) {
                continue;
            }
            self.handle_executor_loss_for_checkpoints(lost_id);
            self.reset_running_tasks_for_lost_executor(lost_id);
            self.prune_executor_channel(lost_id);
            evicted.push(lost_id.clone());
        }

        // Drive per-job checkpoint interval timers (SCH-3: quorum = running tasks).
        let elapsed_ms = ticks.saturating_mul(self.config.tick_period_ms());
        let job_ids: Vec<JobId> = self.ckpt.coordinators.keys().cloned().collect();
        for job_id in &job_ids {
            let running = self.running_task_count_for_job(job_id);

            // Capture the awaiting epoch BEFORE ticking so we can detect a
            // timeout-triggered abort (GAP-5).  An abort transitions the state
            // from AwaitingAcks → Failed; if that happens we must clean up
            // checkpoint_notify_sent and barrier_dispatch_sent entries for the
            // aborted epoch so they don't accumulate forever and block future
            // checkpoint rounds.
            let pre_tick_awaiting: Option<u64> =
                self.ckpt.coordinators.get(job_id).and_then(|c| {
                    if let CheckpointCoordinatorState::AwaitingAcks { epoch, .. } = &c.state {
                        Some(*epoch)
                    } else {
                        None
                    }
                });

            if let Some(coord) = self.ckpt.coordinators.get_mut(job_id) {
                coord.set_expected_task_count(running);
                coord.try_tick(elapsed_ms, self.config.checkpoint_ack_timeout_ms());
            }

            // GAP-5: if try_tick aborted an in-flight epoch, remove all stale
            // tracking entries that referenced that epoch.
            //
            // Without this cleanup:
            //   - checkpoint_notify_sent retains (job_id, executor_id, epoch) for
            //     every executor that was notified; since the epoch number is never
            //     reused those entries would live until the coordinator shuts down.
            //   - barrier_dispatch_sent retains (job_id, epoch); again the epoch is
            //     unique so the entry is harmless for correctness but wastes memory.
            if let Some(aborted_epoch) = pre_tick_awaiting {
                let was_aborted = self
                    .ckpt
                    .coordinators
                    .get(job_id)
                    .is_some_and(|c| matches!(c.state, CheckpointCoordinatorState::Failed { .. }));
                if was_aborted {
                    self.ckpt.notify_sent
                        .retain(|(jid, _, e)| jid != job_id || *e != aborted_epoch);
                    self.ckpt.barrier_sent
                        .retain(|(jid, e)| jid != job_id || *e != aborted_epoch);
                    // A stop-with-savepoint waiting on the aborted epoch can
                    // never fire; drop it so the operator can retry the stop.
                    if self.ckpt.pending_stop_after_savepoint.get(job_id) == Some(&aborted_epoch) {
                        self.ckpt.pending_stop_after_savepoint.remove(job_id);
                        tracing::warn!(
                            job_id = %job_id,
                            epoch = aborted_epoch,
                            "stop-with-savepoint cancelled: savepoint epoch timed out"
                        );
                    }
                    tracing::warn!(
                        job_id = %job_id,
                        epoch = aborted_epoch,
                        "checkpoint epoch aborted by ack timeout; \
                         cleaned up stale notify and barrier-dispatch tracking entries"
                    );
                }
            }
        }

        Ok(evicted)
    }

    /// Count tasks in `Running` state for a job (checkpoint quorum size).
    ///
    /// D3: Previously this included `Assigned` tasks too, which over-counted
    /// the expected quorum and caused barrier rounds to time out waiting for
    /// acks from tasks that hadn't started yet.  When the new task transitions
    /// to `Running` via heartbeat, the coordinator can re-tick to include it
    /// in the next epoch.
    pub(crate) fn running_task_count_for_job(&self, job_id: &JobId) -> usize {
        self.job_coordinators
            .get(job_id)
            .map(|jc| jc.read_record())
            .map_or(0, |job| {
                job.stages
                    .iter()
                    .flat_map(|stage| stage.tasks())
                    .filter(|task| matches!(task.state(), TaskState::Running))
                    .count()
            })
    }

    pub(crate) fn executor_has_running_task_in_job(
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
                        task.state() == TaskState::Running
                            && task.assigned_executor() == Some(executor_id)
                    })
                })
            })
    }

    pub(crate) fn reset_running_tasks_for_lost_executor(&mut self, lost_id: &ExecutorId) {
        const MAX_EXECUTOR_LOSSES_BEFORE_FAIL: u32 = 5;

        let mut jobs_to_reassign = Vec::new();
        for (job_id, job_arc) in &self.job_coordinators {
            let mut job = job_arc.write_record();
            let mut job_affected = false;
            for stage in &mut job.stages {
                let mut stage_affected = false;
                for task in &mut stage.tasks {
                    if task.assigned_executor.as_ref() == Some(lost_id)
                        && (task.state == TaskState::Running
                            || (task.state == TaskState::Assigned && task.launch_in_flight()))
                    {
                        task.executor_loss_count = task.executor_loss_count.saturating_add(1);
                        task.assigned_executor = None;
                        task.clear_launch_in_flight();
                        if task.executor_loss_count >= MAX_EXECUTOR_LOSSES_BEFORE_FAIL {
                            task.state = TaskState::Failed;
                            task.last_failure_reason = Some(format!(
                                "executor lost {} consecutive times (max {}); task permanently failed",
                                task.executor_loss_count, MAX_EXECUTOR_LOSSES_BEFORE_FAIL
                            ));
                            tracing::warn!(
                                task_id = %task.task_id(),
                                executor_loss_count = task.executor_loss_count,
                                "task failed after too many executor losses"
                            );
                        } else {
                            task.state = TaskState::Pending;
                        }
                        stage_affected = true;
                        job_affected = true;
                    }
                }
                if stage_affected {
                    stage.refresh_state();
                }
            }
            if job_affected {
                job.refresh_state();
            }

            // Invalidate shuffle partitions produced by the lost executor.
            // Succeeded tasks that wrote shuffle data to the executor's Flight
            // server can no longer be read — reset them to Pending so they are
            // re-executed on a healthy executor.
            if job.invalidate_executor_shuffle_partitions(lost_id) {
                tracing::info!(
                    executor_id = %lost_id,
                    job_id = %job_id,
                    "shuffle partitions invalidated for lost executor; affected tasks reset to Pending"
                );
                job_affected = true;
            }

            if job_affected {
                jobs_to_reassign.push(job_id.clone());
            }
        }
        for job_id in jobs_to_reassign {
            if let Err(error) = self.assign_pending_tasks(&job_id) {
                tracing::warn!(job_id = %job_id, error = %error, "failed to reassign tasks after executor loss");
            }
        }
    }
}

impl Coordinator {
    fn assign_pending_tasks_for_schedulable_jobs(&mut self) {
        if let Err(error) = self.admit_queued_jobs() {
            tracing::warn!(error = %error, "failed to admit queued jobs");
        }
        let job_ids: Vec<JobId> = self
            .job_coordinators
            .iter()
            .filter_map(|(job_id, job_coordinator)| {
                let state = job_coordinator.read_record().state();
                if state.is_terminal() || state == JobState::Queued {
                    None
                } else {
                    Some(job_id.clone())
                }
            })
            .collect();

        for job_id in job_ids {
            match self.assign_pending_tasks(&job_id) {
                Ok(0) | Err(SchedulerError::NoExecutors) => {}
                Ok(count) => {
                    tracing::debug!(
                        job_id = %job_id,
                        task_count = count,
                        "assigned pending tasks after executor registration"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        job_id = %job_id,
                        error = %error,
                        "failed to assign pending tasks after executor registration"
                    );
                }
            }
        }
    }
}
