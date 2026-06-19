use super::*;

impl Coordinator {
    /// Snapshot one job.
    pub fn job_snapshot(&self, job_id: &JobId) -> SchedulerResult<JobSnapshot> {
        self.job_coordinators
            .get(job_id)
            .map(|jc| jc.read_record().snapshot())
            .ok_or_else(|| SchedulerError::UnknownJob {
                job_id: job_id.clone(),
            })
    }

    /// Snapshot one job with stage and task detail.
    pub fn job_detail_snapshot(&self, job_id: &JobId) -> SchedulerResult<JobDetailSnapshot> {
        self.job_coordinators
            .get(job_id)
            .map(|jc| jc.read_record().detail_snapshot())
            .ok_or_else(|| SchedulerError::UnknownJob {
                job_id: job_id.clone(),
            })
    }

    /// Snapshot all known jobs.
    pub fn job_snapshots(&self) -> Vec<JobSnapshot> {
        self.job_coordinators
            .values()
            .map(|jc| jc.read_record().snapshot())
            .collect()
    }

    /// Snapshot all known executors.
    pub fn executor_snapshots(&self) -> Vec<ExecutorRecord> {
        self.executors.list()
    }

    /// Basic scheduler/executor stability metrics.
    ///
    /// P2.6: Single-pass over jobs/stages/tasks instead of six separate iterations.
    pub fn stability_metrics(&self) -> StabilityMetrics {
        let mut failed_assignments: usize = 0;
        let mut retry_count: usize = 0;
        let mut running_task_count: usize = 0;
        let mut shuffle_partitions_available: usize = 0;
        let mut shuffle_bytes_written: u64 = 0;

        for job in self.job_coordinators.values().map(|jc| jc.read_record()) {
            // Stage retry counts.
            for stage in job.stages() {
                retry_count = retry_count.saturating_add(stage.retry_count() as usize);
            }
            // Per-task counters and shuffle partition output bytes.
            for stage in job.stages() {
                for task in stage.tasks() {
                    match task.state() {
                        TaskState::Failed => failed_assignments += 1,
                        TaskState::Running => running_task_count += 1,
                        _ => {}
                    }
                    if let Some(meta) = task.output_metadata() {
                        for p in meta.shuffle_partitions() {
                            shuffle_bytes_written =
                                shuffle_bytes_written.saturating_add(p.size_bytes);
                        }
                    }
                }
            }
            // Shuffle partition availability from the job's shuffle_output map.
            shuffle_partitions_available = shuffle_partitions_available
                .saturating_add(job.shuffle_partitions_available_count());
        }

        StabilityMetrics {
            heartbeat_ages: self.executors.heartbeat_ages(),
            failed_assignments,
            retry_count,
            running_task_count,
            shuffle_partitions_available,
            shuffle_bytes_written,
        }
    }

    /// Compute the current reservation totals for the given namespace.
    ///
    /// Walks active (non-terminal) jobs and sums their `cpu_limit_nanos` and
    /// `memory_limit_bytes` reservations. The returned snapshot is passed to
    /// `QueueManager::admit` so quota enforcement is stateless in the queue
    /// manager itself.
    pub fn namespace_quota_snapshot(&self, namespace_id: Option<&str>) -> NamespaceQuotaSnapshot {
        let mut snap = NamespaceQuotaSnapshot {
            namespace_id: namespace_id.map(str::to_owned),
            ..Default::default()
        };
        for job in self.job_coordinators.values().map(|jc| jc.read_record()) {
            if job.state().is_terminal() {
                continue;
            }
            if job.spec.namespace_id() != namespace_id {
                continue;
            }
            snap.active_job_count += 1;
            snap.cpu_nanos_reserved = snap
                .cpu_nanos_reserved
                .saturating_add(job.spec.cpu_limit_nanos().unwrap_or(0));
            snap.memory_bytes_reserved = snap
                .memory_bytes_reserved
                .saturating_add(job.spec.memory_limit_bytes().unwrap_or(0));
        }
        snap
    }

    /// Take inline Arrow IPC result batches for a completed job.
    pub fn take_job_inline_results(&mut self, job_id: &JobId) -> Option<Vec<Vec<u8>>> {
        self.job_inline_results.remove(job_id)
    }

    /// Track B (two-tier): Returns the JobCoordinator for a job if present.
    /// This is the seam for delegating per-job decisions (launch, recovery, heartbeat windows).
    pub fn job_coordinator(
        &self,
        job_id: &JobId,
    ) -> Option<Arc<crate::job_coordinator::JobCoordinator>> {
        self.job_coordinators.get(job_id).cloned()
    }

    /// Track E large completion step: Returns UDF resource limits for a job using JCP-owned accessors.
    /// Returns (time_cap_ms, memory_bytes). Callers can build a real ResourceLimits from this.
    pub async fn job_udf_resource_limits(&self, job_id: &JobId) -> (Option<u64>, Option<u64>) {
        if let Some(jc) = self.job_coordinator(job_id) {
            return jc.udf_resource_limits().await;
        }
        // Transitional fallback
        (Some(60 * 60 * 1000), None)
    }

    /// Return the adaptive decision log for a job, or an empty vec if there
    /// are no decisions for this job.  R7.2 Group H.
    pub fn adaptive_decision_log(&self, job_id: &JobId) -> Vec<&AdaptiveDecisionLog> {
        self.adaptive_decision_log
            .get(job_id)
            .map(|v| v.iter().collect())
            .unwrap_or_default()
    }

    /// List all archived terminal-job history records (most-recent first).
    ///
    /// Returns an empty vec when no metadata store is attached or the backend
    /// does not persist history (e.g. etcd).
    pub fn list_job_history(&self) -> Vec<crate::store::JobHistoryRecord> {
        self.store
            .as_ref()
            .map(|s| s.inner().list_job_history())
            .unwrap_or_default()
    }

    /// Look up a single archived job by id.
    pub fn get_job_history(&self, job_id: &str) -> Option<crate::store::JobHistoryRecord> {
        self.store
            .as_ref()
            .and_then(|s| s.inner().get_job_history(job_id))
    }
}
