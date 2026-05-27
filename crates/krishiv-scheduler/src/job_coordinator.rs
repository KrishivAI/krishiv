//! Per-job coordinator facade (ADR-DIST-01).
//!
//! A [`JobCoordinator`] scopes mutations and reads to a single [`JobId`] while
//! sharing the cluster executor registry and metadata store on the underlying
//! [`super::SharedCoordinator`].

use krishiv_proto::{JobId, JobKind, JobSpec};

use crate::{
    Coordinator, JobSnapshot, SchedulerError, SchedulerResult, SharedCoordinator, SubmitOutcome,
};

/// One active coordinator view for a single distributed job.
#[derive(Debug, Clone)]
pub struct JobCoordinator {
    job_id: JobId,
    cluster: SharedCoordinator,
}

impl JobCoordinator {
    pub fn new(job_id: JobId, cluster: SharedCoordinator) -> Self {
        Self { job_id, cluster }
    }

    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    pub fn cluster(&self) -> &SharedCoordinator {
        &self.cluster
    }

    fn ensure_job(&self, spec: &JobSpec) -> SchedulerResult<()> {
        if spec.job_id() != &self.job_id {
            return Err(SchedulerError::InvalidJob {
                message: format!(
                    "job coordinator {} rejects foreign job {}",
                    self.job_id,
                    spec.job_id()
                ),
            });
        }
        Ok(())
    }

    pub fn submit_job(&self, spec: JobSpec) -> SchedulerResult<SubmitOutcome> {
        self.ensure_job(&spec)?;
        self.cluster.blocking_write().submit_job(spec)
    }

    pub fn cancel_job(&self) -> SchedulerResult<()> {
        self.cluster.blocking_write().cancel_job(&self.job_id)
    }

    pub fn job_snapshot(&self) -> SchedulerResult<JobSnapshot> {
        self.cluster.blocking_read().job_snapshot(&self.job_id)
    }

    pub fn job_kind(&self) -> SchedulerResult<JobKind> {
        Ok(self.job_snapshot()?.kind())
    }

    /// Single-shot scoped tick: launch any newly assigned tasks for this
    /// job's id only.  Does NOT advance the heartbeat clock — that is the
    /// CCP's responsibility, doing it here would double-count ticks (A4).
    pub fn coordinator_tick(&self) -> SchedulerResult<()> {
        if self.cluster.blocking_read().jobs.contains_key(&self.job_id) {
            self.cluster
                .blocking_write()
                .launch_assigned_task_assignments(&self.job_id)?;
        }
        Ok(())
    }

    /// Spawn job-scoped task launch and dispatch loops.  Returns abort
    /// handles so the caller can stop the loops on demotion / job
    /// completion.
    ///
    /// The heartbeat clock is NOT advanced here — see [`Self::coordinator_tick`].
    /// CCP's `spawn_orchestration_loops` already ticks the global clock.
    #[must_use]
    pub fn spawn_job_orchestration_loops_with_handles(self) -> Vec<tokio::task::AbortHandle> {
        let mut handles = Vec::with_capacity(1);
        let job_id = self.job_id;
        let cluster = self.cluster;
        let launch_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let job_ids = {
                    let coord = cluster.read().await;
                    if coord.job_snapshot(&job_id).is_ok() {
                        vec![job_id.clone()]
                    } else {
                        continue;
                    }
                };
                for jid in job_ids {
                    let targets = {
                        let mut coord = cluster.write().await;
                        match coord.launch_assigned_task_assignments(&jid) {
                            Ok(assignments) => {
                                match coord.resolve_assignment_targets(assignments) {
                                    Ok(targets) => targets,
                                    Err(error) => {
                                        tracing::warn!(%jid, %error, "resolve assignment targets failed");
                                        continue;
                                    }
                                }
                            }
                            Err(error) => {
                                let text = error.to_string();
                                if !text.contains("InactiveCoordinator") {
                                    tracing::warn!(%jid, error = %text, "launch assignments failed");
                                }
                                continue;
                            }
                        }
                    };
                    let channels = {
                        let coord = cluster.read().await;
                        coord.executor_channels.clone()
                    };
                    if let Err(error) =
                        Coordinator::deliver_assignment_targets_with_channels(channels, targets)
                            .await
                    {
                        tracing::warn!(%jid, %error, "job coordinator task launch failed");
                    }
                }
            }
        });
        handles.push(launch_task.abort_handle());
        handles
    }

    /// Same as [`Self::spawn_job_orchestration_loops_with_handles`] but
    /// discards the abort handles — kept for source compatibility.
    pub fn spawn_job_orchestration_loops(self) {
        let _ = self.spawn_job_orchestration_loops_with_handles();
    }
}

#[cfg(test)]
mod tests {
    use krishiv_proto::{
        CoordinatorId, JobId, JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec,
    };

    use super::*;
    use crate::{Coordinator, ExecutorDescriptor, ExecutorId, SharedCoordinator};

    fn single_task_job(job_id: JobId) -> JobSpec {
        let stage = StageSpec::new(StageId::try_new("s1").unwrap(), "stage")
            .with_task(TaskSpec::new(TaskId::try_new("t1").unwrap(), "task"));
        JobSpec::new(job_id, "single-task", JobKind::Batch).with_stage(stage)
    }

    #[test]
    fn job_coordinator_rejects_foreign_job_submit() {
        let coord_id = CoordinatorId::try_new("ccp").unwrap();
        let mut coord = Coordinator::active(coord_id);
        let exec_id = ExecutorId::try_new("e1").unwrap();
        coord
            .register_executor(ExecutorDescriptor::new(exec_id, "host", 2))
            .unwrap();
        let shared = SharedCoordinator::new(coord);
        let job_a = JobId::try_new("job-a").unwrap();
        let job_b = JobId::try_new("job-b").unwrap();
        let jcp = JobCoordinator::new(job_a, shared);
        let stage = StageSpec::new(StageId::try_new("s1").unwrap(), "stage")
            .with_task(TaskSpec::new(TaskId::try_new("t1").unwrap(), "task"));
        let foreign = JobSpec::new(job_b, "foreign", JobKind::Batch).with_stage(stage);
        assert!(matches!(
            jcp.submit_job(foreign),
            Err(SchedulerError::InvalidJob { .. })
        ));
    }

    /// C1 regression: coordinator_tick must not deadlock when launch_assigned_task_assignments
    /// takes the write lock while another thread holds the read lock.
    #[test]
    fn coordinator_tick_does_not_deadlock_with_concurrent_read() {
        let coord_id = CoordinatorId::try_new("ccp-dl").unwrap();
        let mut coord = Coordinator::active(coord_id);
        let exec_id = ExecutorId::try_new("e1-dl").unwrap();
        coord
            .register_executor(ExecutorDescriptor::new(exec_id, "host", 1))
            .unwrap();
        let job_id = JobId::try_new("job-dl").unwrap();
        coord.submit_job(single_task_job(job_id)).unwrap();
        let shared = SharedCoordinator::new(coord);

        let jcp = JobCoordinator::new(JobId::try_new("jcp-dl").unwrap(), shared.clone());

        // Read lock (e.g. from job_snapshots) + coordinated_tick must not deadlock.
        let _guard = shared.blocking_read();
        jcp.coordinator_tick().unwrap();
        drop(_guard);
    }
}
