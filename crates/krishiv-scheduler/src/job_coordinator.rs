use crate::job::JobRecord;
use krishiv_proto::JobId;
use std::sync::{Arc, RwLock};

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

    /// Synchronous read access to the underlying JobRecord.
    pub fn read_record(&self) -> std::sync::RwLockReadGuard<'_, JobRecord> {
        self.inner.read().unwrap_or_else(|p| p.into_inner())
    }

    /// Synchronous write access to the underlying JobRecord.
    pub fn write_record(&self) -> std::sync::RwLockWriteGuard<'_, JobRecord> {
        self.inner.write().unwrap_or_else(|p| p.into_inner())
    }

    pub async fn snapshot(&self) -> crate::job::JobSnapshot {
        let job = self.inner.read().unwrap_or_else(|p| p.into_inner());
        job.snapshot()
    }

    pub async fn current_state(&self) -> krishiv_proto::JobState {
        let job = self.inner.read().unwrap_or_else(|p| p.into_inner());
        job.state
    }

    pub async fn apply_task_update(
        &self,
        update: krishiv_proto::TaskStatusUpdate,
    ) -> crate::SchedulerResult<crate::TaskUpdateOutcome> {
        let mut job = self.inner.write().unwrap_or_else(|p| p.into_inner());
        let outcome = job.apply_task_update(update);
        outcome
    }

    pub async fn record_executor_heartbeat(
        &self,
        executor_id: &krishiv_proto::ExecutorId,
        ts_ms: u64,
    ) {
        let job = self.inner.write().unwrap_or_else(|p| p.into_inner());
        let _ = (executor_id, ts_ms, job.resource_usage().clone());
    }

    pub async fn running_task_count(&self) -> usize {
        let job = self.inner.read().unwrap_or_else(|p| p.into_inner());
        job.running_task_count()
    }

    pub async fn stage_count(&self) -> usize {
        let job = self.inner.read().unwrap_or_else(|p| p.into_inner());
        job.stages().len()
    }

    pub async fn has_in_flight_tasks(&self) -> bool {
        let job = self.inner.read().unwrap_or_else(|p| p.into_inner());
        job.running_task_count() > 0 || job.failed_task_count() > 0
    }

    pub async fn clear_assignments_for_bad_executor(
        &self,
        executor_id: &krishiv_proto::ExecutorId,
    ) {
        let mut job = self.inner.write().unwrap_or_else(|p| p.into_inner());
        for stage in job.stages_mut() {
            for task in stage.tasks_mut() {
                if task.assigned_executor.as_ref() == Some(executor_id) {
                    task.assigned_executor = None;
                    task.launch_in_flight = false;
                }
            }
        }
    }

    pub async fn udf_execution_time_cap_ms(&self) -> Option<u64> {
        let job = self.inner.read().unwrap_or_else(|p| p.into_inner());
        job.udf_execution_time_cap_ms()
    }

    pub async fn udf_memory_limit_bytes(&self) -> Option<u64> {
        let job = self.inner.read().unwrap_or_else(|p| p.into_inner());
        job.udf_memory_limit_bytes()
    }

    pub async fn udf_resource_limits(&self) -> (Option<u64>, Option<u64>) {
        let time = self.udf_execution_time_cap_ms().await;
        let mem = self.udf_memory_limit_bytes().await;
        (time, mem)
    }

    pub async fn should_consider_for_launch(&self) -> bool {
        let job = self.inner.read().unwrap_or_else(|p| p.into_inner());
        !job.state().is_terminal() && (job.running_task_count() == 0 || job.failed_task_count() > 0)
    }

    pub async fn clear_assignments_for_bad_executor_and_count(
        &self,
        executor_id: &krishiv_proto::ExecutorId,
    ) -> usize {
        let mut job = self.inner.write().unwrap_or_else(|p| p.into_inner());
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

    pub async fn handle_executor_loss(&self, executor_id: &krishiv_proto::ExecutorId) -> usize {
        let mut job = self.inner.write().unwrap_or_else(|p| p.into_inner());
        let mut affected = 0;
        for stage in job.stages_mut() {
            for task in stage.tasks_mut() {
                if task.assigned_executor.as_ref() == Some(executor_id) {
                    task.assigned_executor = None;
                    task.launch_in_flight = false;
                    affected += 1;
                }
            }
        }
        affected
    }

    pub async fn get_launch_work_summary(&self) -> (usize, usize) {
        let job = self.inner.read().unwrap_or_else(|p| p.into_inner());
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

    pub async fn record_heartbeat_and_detect_stale(
        &self,
        executor_id: &krishiv_proto::ExecutorId,
        ts_ms: u64,
    ) -> bool {
        let _job = self.inner.write().unwrap_or_else(|p| p.into_inner());
        let _ = (executor_id, ts_ms);
        false
    }

    pub async fn has_tasks_eligible_for_launch(&self) -> bool {
        let job = self.inner.read().unwrap_or_else(|p| p.into_inner());
        job.running_task_count() == 0 && job.failed_task_count() > 0
    }
}
