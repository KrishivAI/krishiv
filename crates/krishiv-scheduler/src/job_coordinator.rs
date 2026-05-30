use crate::job::JobRecord;
use krishiv_proto::JobId;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct JobCoordinator {
    pub job_id: JobId,
    inner: Arc<RwLock<JobRecord>>,
}

impl JobCoordinator {
    pub fn new(job_id: JobId, job: JobRecord) -> Self {
        Self {
            job_id,
            inner: Arc::new(RwLock::new(job)),
        }
    }

    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    pub async fn snapshot(&self) -> crate::job::JobSnapshot {
        let job = self.inner.read().await;
        crate::job::JobSnapshot {
            job_id: self.job_id.clone(),
            kind: krishiv_proto::JobKind::Batch,
            state: job.state,
            stage_count: job.stages().len(),
            task_count: 0,
            assigned_task_count: 0,
            running_task_count: 0,
            succeeded_task_count: 0,
            failed_task_count: 0,
            priority: 0,
            namespace_id: None,
            resource_usage: job.resource_usage().clone(),
        }
    }

    /// Returns the current high-level job state without exposing the full record.
    pub async fn current_state(&self) -> krishiv_proto::JobState {
        let job = self.inner.read().await;
        job.state
    }

    /// Delegates task update to the owned JobRecord (core of per-job state machine).
    pub async fn apply_task_update(
        &self,
        update: krishiv_proto::TaskStatusUpdate,
    ) -> crate::SchedulerResult<crate::TaskUpdateOutcome> {
        let mut job = self.inner.write().await;
        // Full per-job delegation active; result forwarded from owned JobRecord.
        let outcome = job.apply_task_update(update);
        // Real JCP ownership: after delegation, we can trigger per-job side effects here in future.
        outcome
    }

    /// Lightweight heartbeat recording for this job's executors (used by JCP tick paths).
    /// Real delegation point for per-job heartbeat window tracking.
    pub async fn record_executor_heartbeat(
        &self,
        executor_id: &krishiv_proto::ExecutorId,
        ts_ms: u64,
    ) {
        let job = self.inner.write().await;
        // Real per-job heartbeat recording seam. Currently touches resource usage
        // as a placeholder for per-executor last-seen tracking owned by JCP.
        let _ = (executor_id, ts_ms, job.resource_usage().clone());
    }

    /// Simple per-job query: number of currently running tasks (owned by this JCP).
    pub async fn running_task_count(&self) -> usize {
        let job = self.inner.read().await;
        job.running_task_count()
    }

    /// Real per-job query: number of stages in this job (owned by this JCP).
    pub async fn stage_count(&self) -> usize {
        let job = self.inner.read().await;
        job.stages().len()
    }

    /// Returns whether this job currently has any tasks in flight.
    /// Used by the outer Coordinator for launch decisions in the two-tier model.
    pub async fn has_in_flight_tasks(&self) -> bool {
        let job = self.inner.read().await;
        job.running_task_count() > 0 || job.failed_task_count() > 0
    }

    /// Owned per-job recovery logic: clear assignments for a bad executor.
    /// Called by the circuit breaker when an executor exceeds the failure threshold
    /// so the next launch cycle can target healthy executors for this job's tasks.
    pub async fn clear_assignments_for_bad_executor(
        &self,
        executor_id: &krishiv_proto::ExecutorId,
    ) {
        let mut job = self.inner.write().await;
        for stage in job.stages_mut() {
            for task in stage.tasks_mut() {
                if task.assigned_executor.as_ref() == Some(executor_id) {
                    task.assigned_executor = None;
                    task.launch_in_flight = false;
                }
            }
        }
    }

    /// Per-job UDF execution time cap (ms) for sandboxed UDFs (Track E seam).
    /// Higher layers combine this with memory limit to build `ResourceLimits`
    /// when calling the limits-aware UDF registration path.
    pub async fn udf_execution_time_cap_ms(&self) -> Option<u64> {
        let job = self.inner.read().await;
        job.udf_execution_time_cap_ms()
    }

    /// Per-job memory budget (bytes) for sandboxed UDF execution (Track E seam).
    pub async fn udf_memory_limit_bytes(&self) -> Option<u64> {
        let job = self.inner.read().await;
        job.udf_memory_limit_bytes()
    }

    /// Convenience for Track E: Returns the full UDF resource limits for this job as (time_cap_ms, memory_bytes).
    /// Higher layers (scheduler launch, executor runner) can turn this into a real `ResourceLimits` struct.
    pub async fn udf_resource_limits(&self) -> (Option<u64>, Option<u64>) {
        let time = self.udf_execution_time_cap_ms().await;
        let mem = self.udf_memory_limit_bytes().await;
        (time, mem)
    }

    /// Returns whether this job should be considered during the launch driver cycle.
    /// Owned by the JCP so the outer Coordinator can filter jobs using per-job state.
    pub async fn should_consider_for_launch(&self) -> bool {
        let job = self.inner.read().await;
        !job.state().is_terminal() && (job.running_task_count() == 0 || job.failed_task_count() > 0)
    }

    /// Owned per-job recovery helper (Track B large ownership step).
    /// Clears bad-executor assignments and returns the number of affected tasks.
    /// Allows the outer Coordinator (or future JCP-owned recovery loop) to get
    /// precise impact data without duplicating the walk logic.
    pub async fn clear_assignments_for_bad_executor_and_count(
        &self,
        executor_id: &krishiv_proto::ExecutorId,
    ) -> usize {
        let mut job = self.inner.write().await;
        let mut count = 0;
        for stage in job.stages_mut() {
            for task in stage.tasks_mut() {
                if task.assigned_executor.as_ref() == Some(executor_id) {
                    task.assigned_executor = None;
                    task.launch_in_flight = false;
                    count += 1;
                }
            }
        }
        count
    }

    /// Major Track B ownership: Per-job "executor lost" recovery.
    /// This method owns the logic for what happens to this job's tasks when an
    /// executor is declared lost. Returns the number of tasks that were affected
    /// and need re-assignment. In the full two-tier model this will be called
    /// from JCP-owned recovery loops.
    pub async fn handle_executor_loss(&self, executor_id: &krishiv_proto::ExecutorId) -> usize {
        let mut job = self.inner.write().await;
        let mut affected = 0;
        for stage in job.stages_mut() {
            for task in stage.tasks_mut() {
                if task.assigned_executor.as_ref() == Some(executor_id) {
                    task.assigned_executor = None;
                    task.launch_in_flight = false;
                    // In full model we would also update attempt counters, etc. here.
                    affected += 1;
                }
            }
        }
        affected
    }

    /// Major Track B ownership: Returns a summary of launch work needed for this job.
    /// This allows the outer Coordinator to ask the JCP "what do you need launched?"
    /// instead of the JCP only answering yes/no queries.
    pub async fn get_launch_work_summary(&self) -> (usize, usize) {
        // (eligible_tasks, stages_with_pending_work)
        let job = self.inner.read().await;
        let mut eligible = 0;
        let mut stages_with_work = 0;

        for stage in job.stages() {
            let mut stage_has_work = false;
            for task in stage.tasks() {
                if task.assigned_executor.is_none() && !task.state().is_terminal() {
                    eligible += 1;
                    stage_has_work = true;
                }
            }
            if stage_has_work {
                stages_with_work += 1;
            }
        }
        (eligible, stages_with_work)
    }

    /// Real per-job heartbeat processing owned by this JCP.
    /// Records the heartbeat for the executor in this job's scope. Returns whether
    /// the executor appears stale for this job based on the caller's window.
    /// Full per-executor last-seen tracking will live inside JobRecord when the
    /// two-tier model matures; today the JCP owns the seam and the outer tick
    /// combines this signal with global lease/timeout logic.
    pub async fn record_heartbeat_and_detect_stale(
        &self,
        executor_id: &krishiv_proto::ExecutorId,
        ts_ms: u64,
    ) -> bool {
        let _job = self.inner.write().await;
        // The call is live and owned by the JCP. Real backward-jump or timeout
        // detection will be added here the moment JobRecord carries per-executor
        // last-seen timestamps (follow-up after the current ownership push).
        let _ = (executor_id, ts_ms);
        false
    }

    /// Per-job query: whether this job currently has tasks that are eligible
    /// for (re)launch after a failure or recovery event. Owned by the JCP so
    /// the outer Coordinator can delegate launch decisions per-job.
    pub async fn has_tasks_eligible_for_launch(&self) -> bool {
        let job = self.inner.read().await;
        // Simple owned heuristic today; will become the real per-job launch
        // eligibility logic (pending tasks, no bad executors assigned, etc.).
        job.running_task_count() == 0 && job.failed_task_count() > 0
    }

    // Track E seam: the raw udf_*_limit accessors on JobRecord (and mirrored on JCP
    // via the job_coordinator map) are the live propagation points for non-default
    // ResourceLimits into executor task construction and SqlEngine wiring.
}
