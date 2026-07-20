use super::{
    Coordinator, ExecutorId, JobId, JobKind, StreamingProgressReport, StreamingTaskState, TaskState,
};

impl Coordinator {
    /// Update a task record's last-known watermark and source offset from executor-reported state.
    ///
    /// P1.1: Uses `streaming_task_index` for O(1) lookup instead of O(jobs×stages×tasks) scan.
    pub(crate) fn apply_streaming_task_state(&mut self, state: &StreamingTaskState) {
        let (job_id, stage_id) = match self.streaming_task_index.get(&state.task_id) {
            Some(entry) => (entry.0.clone(), entry.1.clone()),
            None => return,
        };
        if let Some(mut job) = self
            .job_coordinators
            .get(&job_id)
            .map(|jc| jc.write_record())
            && let Some(stage) = job.stages.iter_mut().find(|s| s.stage_id() == &stage_id)
        {
            for task in stage.tasks_mut() {
                if task.task_id() == &state.task_id {
                    task.apply_streaming_state(state);
                    return;
                }
            }
        }
    }

    /// Record a streaming progress report from an executor heartbeat.
    ///
    /// These reports carry watermark, throughput, and state-size data for
    /// continuous streaming tasks. The data is logged at debug level for
    /// observability and can be consumed by external monitoring.
    pub(crate) fn record_streaming_progress(&mut self, report: &StreamingProgressReport) {
        tracing::debug!(
            job_id = %report.job_id,
            task_id = %report.task_id,
            watermark_ms = report.watermark_ms,
            rows_emitted = report.rows_emitted,
            batches_emitted = report.batches_emitted,
            state_bytes = report.state_bytes,
            timestamp_ms = report.timestamp_ms,
            "streaming progress",
        );

        // Wire incoming executor streaming progress reports to the global metrics registry (Phase 3 H3 / GAP-OB-04)
        let metrics = krishiv_metrics::global_metrics();
        metrics.set_watermark_ms(report.job_id.as_str(), report.watermark_ms);
        // Note: report.rows_emitted is a cumulative counter representing newly emitted rows since task start.
        // We use set_streaming_rows to set the absolute cumulative updates correctly without double-counting.
        metrics.set_streaming_rows(
            report.job_id.as_str(),
            report.task_id.as_str(),
            report.rows_emitted,
        );
        metrics.set_state_bytes(report.job_id.as_str(), report.state_bytes);
    }

    /// Populate `streaming_task_index` for all tasks in a job after assignment.
    ///
    /// Called after `apply_assignments` so that streaming heartbeats can use the O(1) index.
    /// Also populates the reverse index for O(tasks_per_job) cleanup.
    pub(crate) fn index_streaming_tasks(&mut self, job_id: &JobId) {
        let job = match self.job_coordinators.get(job_id).map(|jc| jc.read_record()) {
            Some(j) => j,
            None => return,
        };
        let mut job_task_ids = Vec::new();
        for stage in &job.stages {
            let stage_id = stage.stage_id().clone();
            for task in stage.tasks() {
                let task_id = task.task_id().clone();
                self.streaming_task_index
                    .insert(task_id.clone(), (job_id.clone(), stage_id.clone()));
                job_task_ids.push(task_id);
            }
        }
        if !job_task_ids.is_empty() {
            self.streaming_job_task_index
                .insert(job_id.clone(), job_task_ids);
        }
    }

    /// Remove `streaming_task_index` entries for a completed/failed/cancelled job.
    /// Uses the reverse index for O(tasks_per_job) lookup instead of O(total_tasks) scan.
    pub(crate) fn remove_streaming_task_index(&mut self, job_id: &JobId) {
        if let Some(task_ids) = self.streaming_job_task_index.remove(job_id) {
            for tid in task_ids {
                self.streaming_task_index.remove(&tid);
            }
        }
    }

    /// Phase 53 (audit §3b): executors owning at least one Running streaming
    /// task (or a completed continuous `stream:loop` identity), computed in
    /// one O(cluster state) scan for a whole executor list — this replaced
    /// the per-executor `executor_has_streaming_running_tasks` check, which
    /// was O(all jobs) per candidate on recovery paths.
    pub(crate) fn executors_with_streaming_running_tasks(
        &self,
    ) -> std::collections::HashSet<ExecutorId> {
        let profile = self.durability_profile;
        let mut set = std::collections::HashSet::new();
        for jc in self.job_coordinators.values() {
            let job = jc.read_record();
            if job.spec.kind() != JobKind::Streaming {
                continue;
            }
            for stage in &job.stages {
                for task in stage.tasks() {
                    let Some(eid) = task.assigned_executor() else {
                        continue;
                    };
                    if set.contains(eid) {
                        continue;
                    }
                    let counts = task.state() == TaskState::Running
                        || (task.state() == TaskState::Succeeded
                            && krishiv_plan::task_body_for_profile(
                                task.spec.description(),
                                profile,
                            )
                            .is_ok_and(|body| body.starts_with("stream:loop:")));
                    if counts {
                        set.insert(eid.clone());
                    }
                }
            }
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_proto::{
        CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorState, JobSpec, StageId,
        StageSpec, TaskId, TaskSpec,
    };

    fn two_task_streaming_job(job_id: &JobId) -> JobSpec {
        JobSpec::new(job_id.clone(), "streaming-job", JobKind::Streaming).with_stage(
            StageSpec::new(StageId::try_new("stage-0").unwrap(), "stage")
                .with_task(TaskSpec::new(
                    TaskId::try_new("t0").unwrap(),
                    "stream:tw",
                ))
                .with_task(TaskSpec::new(
                    TaskId::try_new("t1").unwrap(),
                    "stream:tw",
                )),
        )
    }

    #[test]
    fn index_streaming_tasks_then_remove_clears_both_indexes() {
        let mut coord = Coordinator::active(CoordinatorId::try_new("stream-idx").unwrap());
        let job_id = JobId::try_new("job-idx").unwrap();
        coord.submit_job(two_task_streaming_job(&job_id)).unwrap();

        coord.index_streaming_tasks(&job_id);
        assert_eq!(
            coord.streaming_task_index.len(),
            2,
            "both tasks must be indexed for O(1) lookup"
        );
        assert!(
            coord
                .streaming_task_index
                .contains_key(&TaskId::try_new("t0").unwrap())
        );
        assert_eq!(
            coord.streaming_job_task_index.get(&job_id).map(Vec::len),
            Some(2)
        );

        coord.remove_streaming_task_index(&job_id);
        assert!(
            coord.streaming_task_index.is_empty(),
            "removing the job must clear every indexed task, not just the reverse entry"
        );
        assert!(!coord.streaming_job_task_index.contains_key(&job_id));
    }

    #[test]
    fn apply_streaming_task_state_updates_the_indexed_task() {
        let mut coord = Coordinator::active(CoordinatorId::try_new("stream-apply").unwrap());
        let job_id = JobId::try_new("job-apply").unwrap();
        coord.submit_job(two_task_streaming_job(&job_id)).unwrap();
        coord.index_streaming_tasks(&job_id);

        let task_id = TaskId::try_new("t0").unwrap();
        coord.apply_streaming_task_state(&StreamingTaskState::new(
            task_id.clone(),
            42_000,
            b"offset-7".to_vec(),
        ));

        let record = coord.job_coordinators.get(&job_id).unwrap().read_record();
        let task = record
            .stages()
            .iter()
            .flat_map(|s| s.tasks())
            .find(|t| t.task_id() == &task_id)
            .unwrap();
        assert_eq!(task.last_watermark_ms(), Some(42_000));
        assert_eq!(task.last_source_offset(), Some(b"offset-7".as_slice()));
    }

    #[test]
    fn apply_streaming_task_state_is_a_noop_for_an_unindexed_task() {
        // `submit_job` always indexes its own tasks (job_lifecycle.rs calls
        // `index_streaming_tasks` unconditionally), so the only way to
        // observe a genuinely unindexed task is one that was never
        // submitted at all — a stale/unknown report arriving after the
        // index believes nothing about this id.
        let mut coord = Coordinator::active(CoordinatorId::try_new("stream-noop").unwrap());
        let job_id = JobId::try_new("job-noop").unwrap();
        coord.submit_job(two_task_streaming_job(&job_id)).unwrap();

        // Must not panic even though "no-such-task" is not in any job.
        coord.apply_streaming_task_state(&StreamingTaskState::new(
            TaskId::try_new("no-such-task").unwrap(),
            1,
            Vec::new(),
        ));

        // And the real, indexed tasks must be completely unaffected.
        let record = coord.job_coordinators.get(&job_id).unwrap().read_record();
        for task in record.stages().iter().flat_map(|s| s.tasks()) {
            assert_eq!(task.last_watermark_ms(), None);
        }
    }

    #[test]
    fn executors_with_streaming_running_tasks_excludes_batch_jobs_and_unassigned_tasks() {
        let mut coord = Coordinator::active(CoordinatorId::try_new("stream-running").unwrap());
        let exec_id = krishiv_proto::ExecutorId::try_new("exec-running").unwrap();
        coord
            .register_executor(ExecutorDescriptor::new(exec_id.clone(), "host", 2))
            .unwrap();
        coord
            .executor_heartbeat(ExecutorHeartbeat::new(exec_id.clone(), ExecutorState::Healthy))
            .unwrap();

        let streaming_job = JobId::try_new("job-streaming").unwrap();
        coord
            .submit_job(two_task_streaming_job(&streaming_job))
            .unwrap();
        {
            let mut record = coord.find_job_mut(&streaming_job).unwrap();
            record.stages[0].tasks[0].state = TaskState::Running;
            record.stages[0].tasks[0].assigned_executor = Some(exec_id.clone());
            // t1 stays unassigned/Pending — must not contribute to the set.
        }

        let batch_job = JobId::try_new("job-batch").unwrap();
        coord
            .submit_job(
                JobSpec::new(batch_job.clone(), "batch-job", JobKind::Batch).with_stage(
                    StageSpec::new(StageId::try_new("stage-0").unwrap(), "stage").with_task(
                        TaskSpec::new(TaskId::try_new("bt0").unwrap(), "sql: select 1"),
                    ),
                ),
            )
            .unwrap();
        {
            let mut record = coord.find_job_mut(&batch_job).unwrap();
            record.stages[0].tasks[0].state = TaskState::Running;
            record.stages[0].tasks[0].assigned_executor = Some(exec_id.clone());
        }

        let running = coord.executors_with_streaming_running_tasks();
        assert_eq!(
            running,
            std::collections::HashSet::from([exec_id]),
            "only the executor running the streaming job's Running task counts, \
             not the batch job's Running task nor the streaming job's unassigned task"
        );
    }
}
