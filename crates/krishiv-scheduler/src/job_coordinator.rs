use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::job::JobRecord;
use krishiv_proto::{ExecutorId, JobId, TaskState};

/// Default heartbeat staleness threshold in milliseconds (30 seconds).
const DEFAULT_HEARTBEAT_STALE_MS: u64 = 30_000;

pub struct JobCoordinator {
    pub job_id: JobId,
    inner: Arc<RwLock<JobRecord>>,
    /// Last heartbeat timestamp per executor (ms since epoch).
    heartbeat_timestamps: Mutex<HashMap<ExecutorId, u64>>,
    /// Heartbeat staleness threshold in milliseconds.
    stale_threshold_ms: u64,
}

impl JobCoordinator {
    pub fn new(job_id: JobId, job: JobRecord) -> Self {
        Self {
            job_id,
            inner: Arc::new(RwLock::new(job)),
            heartbeat_timestamps: Mutex::new(HashMap::new()),
            stale_threshold_ms: DEFAULT_HEARTBEAT_STALE_MS,
        }
    }

    pub fn with_stale_threshold(mut self, threshold_ms: u64) -> Self {
        self.stale_threshold_ms = threshold_ms;
        self
    }

    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    pub fn read_record(&self) -> std::sync::RwLockReadGuard<'_, JobRecord> {
        self.inner.read().unwrap_or_else(|p| p.into_inner())
    }

    pub fn write_record(&self) -> std::sync::RwLockWriteGuard<'_, JobRecord> {
        self.inner.write().unwrap_or_else(|p| p.into_inner())
    }

    pub async fn snapshot(&self) -> crate::job::JobSnapshot {
        self.inner.read().unwrap_or_else(|p| p.into_inner()).snapshot()
    }

    pub async fn current_state(&self) -> krishiv_proto::JobState {
        self.inner.read().unwrap_or_else(|p| p.into_inner()).state
    }

    pub async fn apply_task_update(
        &self,
        update: krishiv_proto::TaskStatusUpdate,
    ) -> crate::SchedulerResult<crate::TaskUpdateOutcome> {
        self.inner.write().unwrap_or_else(|p| p.into_inner()).apply_task_update(update)
    }

    /// Record a heartbeat from an executor and store its timestamp.
    pub fn record_executor_heartbeat(
        &self,
        executor_id: &ExecutorId,
        ts_ms: u64,
    ) {
        self.heartbeat_timestamps
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(executor_id.clone(), ts_ms);
    }

    /// Record a heartbeat and detect whether the executor is stale
    /// (no heartbeat received within `stale_threshold_ms`).
    pub fn record_heartbeat_and_detect_stale(
        &self,
        executor_id: &ExecutorId,
        ts_ms: u64,
    ) -> bool {
        let mut timestamps = self
            .heartbeat_timestamps
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let stale = timestamps
            .get(executor_id)
            .map(|prev| ts_ms.saturating_sub(*prev) > self.stale_threshold_ms)
            .unwrap_or(true);
        timestamps.insert(executor_id.clone(), ts_ms);
        stale
    }

    pub async fn running_task_count(&self) -> usize {
        self.inner.read().unwrap_or_else(|p| p.into_inner()).running_task_count()
    }

    pub async fn stage_count(&self) -> usize {
        self.inner.read().unwrap_or_else(|p| p.into_inner()).stages().len()
    }

    pub async fn has_in_flight_tasks(&self) -> bool {
        let job = self.inner.read().unwrap_or_else(|p| p.into_inner());
        job.running_task_count() > 0 || job.failed_task_count() > 0
    }

    pub async fn clear_assignments_for_bad_executor(
        &self,
        executor_id: &ExecutorId,
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

    pub async fn clear_assignments_for_bad_executor_and_count(
        &self,
        executor_id: &ExecutorId,
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

    pub async fn handle_executor_loss(&self, executor_id: &ExecutorId) -> usize {
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
        self.heartbeat_timestamps
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(executor_id);
        affected
    }

    pub async fn has_tasks_eligible_for_launch(&self) -> bool {
        let job = self.inner.read().unwrap_or_else(|p| p.into_inner());
        job.stages().iter().any(|stage| {
            stage.tasks().iter().any(|task| {
                task.assigned_executor.is_some()
                    && task.state() != TaskState::Running
                    && task.state() != TaskState::Succeeded
                    && !task.launch_in_flight
            })
        })
    }

    pub async fn should_consider_for_launch(&self) -> bool {
        let job = self.inner.read().unwrap_or_else(|p| p.into_inner());
        if job.state().is_terminal() {
            return false;
        }
        job.stages().iter().any(|stage| {
            stage.tasks().iter().any(|task| {
                task.assigned_executor.is_some()
                    && !task.launch_in_flight
                    && !matches!(task.state(), TaskState::Succeeded | TaskState::Failed | TaskState::Cancelled)
            })
        })
    }

    pub async fn get_launch_work_summary(&self) -> (usize, usize) {
        let job = self.inner.read().unwrap_or_else(|p| p.into_inner());
        let mut eligible = 0;
        let mut stages_with_work = 0;

        for stage in job.stages() {
            let mut stage_has_work = false;
            for task in stage.tasks() {
                if task.assigned_executor.is_some()
                    && !task.launch_in_flight
                    && !task.state().is_terminal()
                {
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

    pub async fn udf_execution_time_cap_ms(&self) -> Option<u64> {
        self.inner.read().unwrap_or_else(|p| p.into_inner()).udf_execution_time_cap_ms()
    }

    pub async fn udf_memory_limit_bytes(&self) -> Option<u64> {
        self.inner.read().unwrap_or_else(|p| p.into_inner()).udf_memory_limit_bytes()
    }

    pub async fn udf_resource_limits(&self) -> (Option<u64>, Option<u64>) {
        let time = self.udf_execution_time_cap_ms().await;
        let mem = self.udf_memory_limit_bytes().await;
        (time, mem)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::JobRecord;
    use krishiv_proto::{JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec};

    fn make_job_coordinator(job_id: &str, task_count: usize) -> JobCoordinator {
        let jid = JobId::try_new(job_id).unwrap();
        let mut spec = JobSpec::new(jid.clone(), format!("test-{job_id}"), JobKind::Streaming);
        let mut stage = StageSpec::new(StageId::try_new("stage-0").unwrap(), "stage");
        for i in 0..task_count {
            stage = stage.with_task(TaskSpec::new(
                TaskId::try_new(format!("task-{i}")).unwrap(),
                "stream:tw",
            ));
        }
        spec = spec.with_stage(stage);
        let record = JobRecord::from_spec(spec, 3);
        JobCoordinator::new(jid, record)
    }

    #[test]
    fn record_executor_heartbeat_stores_timestamp() {
        let jc = make_job_coordinator("hb-test", 1);
        let executor_id = ExecutorId::try_new("exec-1").unwrap();

        // Initially no heartbeat, so detect_stale returns true.
        let stale = jc.record_heartbeat_and_detect_stale(&executor_id, 100);
        assert!(stale, "first heartbeat should report stale (no prior timestamp)");

        // Second heartbeat immediately — should NOT be stale.
        let stale = jc.record_heartbeat_and_detect_stale(&executor_id, 101);
        assert!(!stale, "immediate second heartbeat should not be stale");
    }

    #[test]
    fn record_heartbeat_detects_stale_after_timeout() {
        let jc = make_job_coordinator("stale-test", 2)
            .with_stale_threshold(100); // 100ms threshold

        let executor_id = ExecutorId::try_new("exec-stale").unwrap();

        // First heartbeat at t=0.
        let _ = jc.record_heartbeat_and_detect_stale(&executor_id, 0);

        // Second heartbeat at t=50ms — within threshold.
        let stale = jc.record_heartbeat_and_detect_stale(&executor_id, 50);
        assert!(!stale);

        // Third heartbeat at t=200ms — exceeds 100ms threshold.
        let stale = jc.record_heartbeat_and_detect_stale(&executor_id, 200);
        assert!(stale, "heartbeat after 200ms with 100ms threshold should be stale");
    }

    #[test]
    fn has_tasks_eligible_for_launch_returns_false_when_none_assigned() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let jc = make_job_coordinator("launch-test", 2);
        rt.block_on(async {
            // No tasks assigned yet — nothing eligible.
            assert!(!jc.has_tasks_eligible_for_launch().await);
        });
    }

    #[test]
    fn handle_executor_loss_clears_assignments_and_removes_heartbeat() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let jc = make_job_coordinator("loss-test", 2);
        let executor_id = ExecutorId::try_new("exec-loss").unwrap();

        // Record a heartbeat so there's an entry to clean up.
        jc.record_executor_heartbeat(&executor_id, 100);

        assert!(jc
            .heartbeat_timestamps
            .lock()
            .unwrap()
            .contains_key(&executor_id));

        rt.block_on(async {
            let affected = jc.handle_executor_loss(&executor_id).await;
            assert_eq!(affected, 0);
        });
        assert!(!jc
            .heartbeat_timestamps
            .lock()
            .unwrap()
            .contains_key(&executor_id));
    }

    #[test]
    fn should_consider_for_launch_ignores_terminal_jobs() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let jc = make_job_coordinator("terminal-test", 1);
        jc.write_record().state = crate::job::JobState::Succeeded;
        rt.block_on(async {
            assert!(!jc.should_consider_for_launch().await);
        });
    }
}
