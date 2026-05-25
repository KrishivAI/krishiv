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
        self.cluster
            .write()
            .map_err(|_| SchedulerError::Transport {
                message: "coordinator lock poisoned".to_string(),
            })?
            .submit_job(spec)
    }

    pub fn cancel_job(&self) -> SchedulerResult<()> {
        self.cluster
            .write()
            .map_err(|_| SchedulerError::Transport {
                message: "coordinator lock poisoned".to_string(),
            })?
            .cancel_job(&self.job_id)
    }

    pub fn job_snapshot(&self) -> SchedulerResult<JobSnapshot> {
        self.cluster
            .read()
            .map_err(|_| SchedulerError::Transport {
                message: "coordinator lock poisoned".to_string(),
            })?
            .job_snapshot(&self.job_id)
    }

    pub fn job_kind(&self) -> SchedulerResult<JobKind> {
        Ok(self.job_snapshot()?.kind())
    }

    pub fn coordinator_tick(&self) -> SchedulerResult<()> {
        let mut coord = self
            .cluster
            .write()
            .map_err(|_| SchedulerError::Transport {
                message: "coordinator lock poisoned".to_string(),
            })?;
        if self.job_snapshot().is_err() {
            return Ok(());
        }
        coord.advance_heartbeat_clock(1)?;
        coord.launch_assigned_task_assignments(&self.job_id)?;
        Ok(())
    }

    pub fn spawn_job_orchestration_loops(self) {
        let job_id = self.job_id.clone();
        let cluster = self.cluster.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                let tick = (|| {
                    let mut coord = cluster.write().map_err(|_| SchedulerError::Transport {
                        message: "coordinator lock poisoned".to_string(),
                    })?;
                    if coord.job_snapshot(&job_id).is_err() {
                        return Ok(());
                    }
                    coord.advance_heartbeat_clock(1)?;
                    coord.launch_assigned_task_assignments(&job_id)?;
                    Ok::<(), SchedulerError>(())
                })();
                if let Err(error) = tick {
                    tracing::warn!(%job_id, %error, "job coordinator heartbeat tick failed");
                }
            }
        });
        let job_id = self.job_id;
        let cluster = self.cluster;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                interval.tick().await;
                let job_ids = match cluster.read() {
                    Ok(coord) if coord.job_snapshot(&job_id).is_ok() => vec![job_id.clone()],
                    Ok(_) => continue,
                    Err(_) => continue,
                };
                for jid in job_ids {
                    let targets = match cluster.write() {
                        Ok(mut coord) => match coord.launch_assigned_task_assignments(&jid) {
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
                                tracing::warn!(%jid, %error, "launch assignments failed");
                                continue;
                            }
                        },
                        Err(_) => continue,
                    };
                    let channels = match cluster.read() {
                        Ok(coord) => coord.executor_channels.clone(),
                        Err(_) => continue,
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
    }
}

#[cfg(test)]
mod tests {
    use krishiv_proto::{
        CoordinatorId, JobId, JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec,
    };

    use super::*;
    use crate::{Coordinator, ExecutorDescriptor, ExecutorId};

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
}
