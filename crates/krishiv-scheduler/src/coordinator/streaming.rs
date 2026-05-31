use super::*;

impl Coordinator {
    /// Update a task record's last-known watermark and source offset from executor-reported state.
    ///
    /// P1.1: Uses `streaming_task_index` for O(1) lookup instead of O(jobs×stages×tasks) scan.
    pub(crate) fn apply_streaming_task_state(&mut self, state: &StreamingTaskState) {
        let (job_id, stage_id) = match self.streaming_task_index.get(&state.task_id) {
            Some(entry) => (entry.0.clone(), entry.1.clone()),
            None => return,
        };
        if let Some(job) = self.jobs.get_mut(&job_id)
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
        metrics.set_watermark_ms(&report.job_id, report.watermark_ms);
        // Note: report.rows_emitted is a cumulative counter representing newly emitted rows since task start.
        // We use set_streaming_rows to set the absolute cumulative updates correctly without double-counting.
        metrics.set_streaming_rows(&report.job_id, &report.task_id, report.rows_emitted);
        metrics.set_state_bytes(&report.job_id, report.state_bytes);
    }

    /// Populate `streaming_task_index` for all tasks in a job after assignment.
    ///
    /// Called after `apply_assignments` so that streaming heartbeats can use the O(1) index.
    /// Also populates the reverse index for O(tasks_per_job) cleanup.
    pub(crate) fn index_streaming_tasks(&mut self, job_id: &JobId) {
        let job = match self.jobs.get(job_id) {
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

    /// Returns true if the executor owns at least one Running task in a streaming job.
    pub(crate) fn executor_has_streaming_running_tasks(&self, executor_id: &ExecutorId) -> bool {
        self.jobs.values().any(|job| {
            job.spec.kind() == JobKind::Streaming
                && job.stages.iter().any(|stage| {
                    stage.tasks().iter().any(|task| {
                        task.state() == TaskState::Running
                            && task.assigned_executor() == Some(executor_id)
                    })
                })
        })
    }
}
