//! Cluster control plane (CCP) — admission, executor registry, and job coordinator lifecycle.
//!
//! See ADR-DIST-01. The CCP owns the shared [`super::SharedCoordinator`] executor
//! registry and metadata store. Each distributed job is driven by a
//! [`super::JobCoordinator`] scoped to one [`krishiv_proto::JobId`].

use std::sync::Arc;

use krishiv_proto::{CoordinatorId, JobId, JobSpec};

use crate::{
    Coordinator, JobCoordinator, LeaderElection, SchedulerError, SchedulerResult,
    SharedCoordinator, SingleNodeElection, SubmitOutcome,
};

/// Cluster-level coordinator runtime (one active CCP per cell).
#[derive(Clone)]
pub struct ClusterControlPlane {
    coordinator_id: CoordinatorId,
    shared: SharedCoordinator,
    leader: Arc<SingleNodeLeader>,
}

/// Leader election wrapper used by the CCP process.
#[derive(Debug)]
pub struct SingleNodeLeader {
    inner: SingleNodeElection,
    fencing_token: std::sync::atomic::AtomicU64,
}

impl Default for SingleNodeLeader {
    fn default() -> Self {
        Self::new()
    }
}

impl SingleNodeLeader {
    pub fn new() -> Self {
        Self {
            inner: SingleNodeElection,
            fencing_token: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub fn fencing_token(&self) -> u64 {
        self.fencing_token.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn bump_fencing_token(&self) -> u64 {
        self.fencing_token
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .saturating_add(1)
    }
}

impl LeaderElection for SingleNodeLeader {
    fn is_leader(&self) -> bool {
        self.inner.is_leader()
    }

    async fn try_acquire(&self) -> bool {
        let acquired = self.inner.try_acquire().await;
        if acquired {
            let _ = self.bump_fencing_token();
        }
        acquired
    }

    async fn renew(&self) -> bool {
        self.inner.renew().await
    }

    fn fencing_token(&self) -> u64 {
        self.fencing_token()
    }
}

impl ClusterControlPlane {
    pub fn new(coordinator: Coordinator) -> Self {
        let coordinator_id = coordinator.coordinator_id().clone();
        Self {
            coordinator_id,
            shared: SharedCoordinator::new(coordinator),
            leader: Arc::new(SingleNodeLeader::new()),
        }
    }

    pub fn from_shared(coordinator_id: CoordinatorId, shared: SharedCoordinator) -> Self {
        Self {
            coordinator_id,
            shared,
            leader: Arc::new(SingleNodeLeader::new()),
        }
    }

    pub fn coordinator_id(&self) -> &CoordinatorId {
        &self.coordinator_id
    }

    pub fn shared_coordinator(&self) -> &SharedCoordinator {
        &self.shared
    }

    pub fn leader(&self) -> &Arc<SingleNodeLeader> {
        &self.leader
    }

    pub fn is_active(&self) -> bool {
        self.shared
            .read()
            .map(|c| c.state() == krishiv_proto::CoordinatorState::Active)
            .unwrap_or(false)
    }

    pub fn promote_to_active(&self) -> SchedulerResult<()> {
        self.shared
            .write()
            .map_err(|_| SchedulerError::Transport {
                message: "coordinator lock poisoned".to_string(),
            })?
            .promote_to_active();
        Ok(())
    }

    pub fn demote_to_standby(&self) -> SchedulerResult<()> {
        self.shared
            .write()
            .map_err(|_| SchedulerError::Transport {
                message: "coordinator lock poisoned".to_string(),
            })?
            .demote_to_standby();
        Ok(())
    }

    pub fn submit_job(&self, spec: JobSpec) -> SchedulerResult<SubmitOutcome> {
        if !self.leader.is_leader() {
            let state = self
                .shared
                .read()
                .map(|c| c.state())
                .unwrap_or(krishiv_proto::CoordinatorState::Standby);
            return Err(SchedulerError::InactiveCoordinator {
                coordinator_id: self.coordinator_id.clone(),
                state,
            });
        }
        self.shared
            .write()
            .map_err(|_| SchedulerError::Transport {
                message: "coordinator lock poisoned".to_string(),
            })?
            .submit_job(spec)
    }

    pub fn job_coordinator(&self, job_id: JobId) -> JobCoordinator {
        JobCoordinator::new(job_id, self.shared.clone())
    }

    pub fn spawn_orchestration_loops(&self) {
        self.shared.spawn_orchestration_loops();
    }

    pub async fn run_leader_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            if self.leader.try_acquire().await {
                let _ = self.promote_to_active();
            } else if self.leader.is_leader() {
                if !self.leader.renew().await {
                    let _ = self.demote_to_standby();
                }
            } else {
                let _ = self.demote_to_standby();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use krishiv_proto::{
        CoordinatorId, ExecutorDescriptor, ExecutorId, JobId, JobKind, JobSpec, StageId, StageSpec,
        TaskId, TaskSpec,
    };

    use super::*;

    #[test]
    fn ccp_submit_and_job_coordinator_scope() {
        let id = CoordinatorId::try_new("ccp").unwrap();
        let mut coord = Coordinator::active(id);
        let exec_id = ExecutorId::try_new("exec-1").unwrap();
        coord
            .register_executor(ExecutorDescriptor::new(exec_id, "host", 2))
            .unwrap();
        let ccp = ClusterControlPlane::new(coord);
        let job_id = JobId::try_new("job-1").unwrap();
        let stage = StageSpec::new(StageId::try_new("s1").unwrap(), "stage")
            .with_task(TaskSpec::new(TaskId::try_new("t1").unwrap(), "task"));
        let spec = JobSpec::new(job_id.clone(), "demo", JobKind::Batch).with_stage(stage);
        ccp.submit_job(spec).unwrap();
        let jcp = ccp.job_coordinator(job_id);
        assert_eq!(
            jcp.job_snapshot().unwrap().job_id(),
            &JobId::try_new("job-1").unwrap()
        );
    }
}
