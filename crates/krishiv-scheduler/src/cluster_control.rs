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

/// Trait-object handle for any [`LeaderElection`] backing the CCP.  The
/// inherent constructors below default to [`SingleNodeLeader`] which is
/// suitable for embedded/single-node deployments.  Bare-metal HA and
/// Kubernetes deployments inject `K8sLeaseElection` via
/// [`ClusterControlPlane::with_leader`] (A1).
pub type SharedLeader = Arc<dyn LeaderElection + Send + Sync>;

/// Cluster-level coordinator runtime (one active CCP per cell).
#[derive(Clone)]
pub struct ClusterControlPlane {
    coordinator_id: CoordinatorId,
    shared: SharedCoordinator,
    leader: SharedLeader,
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
        self.fencing_token
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn bump_fencing_token(&self) -> u64 {
        self.fencing_token
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1)
    }
}

#[async_trait::async_trait]
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
        SingleNodeLeader::fencing_token(self)
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

    /// Build a CCP that uses a caller-supplied [`LeaderElection`] (A1).
    ///
    /// Distributed deployments inject their own implementation here: `EtcdLeaseElection`
    /// on bare metal (`--leader-backend etcd`) and `K8sLeaseElection` in Kubernetes.
    /// Embedded and single-node continue to use the default [`SingleNodeLeader`].
    pub fn from_shared_with_leader(
        coordinator_id: CoordinatorId,
        shared: SharedCoordinator,
        leader: SharedLeader,
    ) -> Self {
        Self {
            coordinator_id,
            shared,
            leader,
        }
    }

    /// Replace the leader-election backend on an already-built CCP (test helper).
    #[must_use]
    pub fn with_leader(mut self, leader: SharedLeader) -> Self {
        self.leader = leader;
        self
    }

    pub fn coordinator_id(&self) -> &CoordinatorId {
        &self.coordinator_id
    }

    pub fn shared_coordinator(&self) -> &SharedCoordinator {
        &self.shared
    }

    /// Erased leader handle (A1) — callers should use this in preference to
    /// the legacy [`single_node_leader`] accessor whenever they need to read
    /// the fencing token or `is_leader()` flag without caring which backend
    /// is in use.
    pub fn leader(&self) -> &SharedLeader {
        &self.leader
    }

    /// Live fencing token from whichever leader-election backend is wired in (A1).
    pub fn fencing_token(&self) -> u64 {
        self.leader.fencing_token()
    }

    /// True when this process currently owns the leader lease.
    pub fn is_leader(&self) -> bool {
        self.leader.is_leader()
    }

    pub async fn is_active(&self) -> bool {
        self.shared.read().await.state() == krishiv_proto::CoordinatorState::Active
    }

    pub async fn promote_to_active(&self) -> SchedulerResult<()> {
        self.shared.write().await.promote_to_active();
        Ok(())
    }

    pub async fn demote_to_standby(&self) -> SchedulerResult<()> {
        self.shared.write().await.demote_to_standby();
        Ok(())
    }

    pub async fn submit_job(&self, spec: JobSpec) -> SchedulerResult<SubmitOutcome> {
        if !self.leader.is_leader() {
            let state = self.shared.read().await.state();
            return Err(SchedulerError::InactiveCoordinator {
                coordinator_id: self.coordinator_id.clone(),
                state,
            });
        }
        self.shared.submit_job(spec).await
    }

    pub fn job_coordinator(&self, job_id: JobId) -> JobCoordinator {
        // Two-tier seam: JCP owns its JobRecord. For early callers we construct
        // a minimal placeholder record (real attachment happens at submit_job time).
        let spec = JobSpec::new(job_id.clone(), "jcp-seam", krishiv_proto::JobKind::Batch);
        let record = crate::job::JobRecord::from_spec(spec, 3);
        JobCoordinator::new(job_id, record)
    }

    /// Spawn orchestration loops only when we currently hold leadership.
    /// Returns [`OrchestratorHandles`] so callers can stop the loops on demotion.
    pub fn spawn_orchestration_loops(&self) -> crate::OrchestratorHandles {
        self.shared.spawn_orchestration_loops()
    }

    /// Run the leader election loop.  Eagerly attempts acquisition before the
    /// first tick so there is no Active/Standby ambiguity at startup (A2).
    ///
    /// Promotion installs orchestration loops; demotion aborts them (E3).
    /// The loop runs forever until the task is aborted.
    pub async fn run_leader_loop(self: Arc<Self>) {
        let mut orchestration_handles: Option<crate::OrchestratorHandles> = None;

        if self.leader.try_acquire().await {
            self.shared
                .sync_leader_fencing_token(self.fencing_token());
            if let Err(e) = self.promote_to_active().await {
                tracing::error!(error = %e, "failed to promote to active");
            }
            orchestration_handles = Some(self.spawn_orchestration_loops());
        } else if let Err(e) = self.demote_to_standby().await {
            tracing::error!(error = %e, "failed to demote to standby after failed acquisition");
        }

        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if self.leader.try_acquire().await {
                self.shared
                    .sync_leader_fencing_token(self.fencing_token());
                if let Err(e) = self.promote_to_active().await {
                    tracing::error!(error = %e, "failed to promote to active");
                }
                if orchestration_handles.is_none() {
                    orchestration_handles = Some(self.spawn_orchestration_loops());
                }
            } else if self.leader.is_leader() {
                if self.leader.renew().await {
                    self.shared
                        .sync_leader_fencing_token(self.fencing_token());
                } else {
                    if let Err(e) = self.demote_to_standby().await {
                        tracing::error!(error = %e, "failed to demote to standby");
                    }
                    if let Some(handles) = orchestration_handles.take() {
                        handles.shutdown();
                    }
                }
            } else {
                if let Err(e) = self.demote_to_standby().await {
                    tracing::error!(error = %e, "failed to demote to standby");
                }
                if let Some(handles) = orchestration_handles.take() {
                    handles.shutdown();
                }
            }
        }
    }
}

#[cfg(test)]
#[cfg(test)]
mod tests {
    use krishiv_proto::{
        CoordinatorId, ExecutorDescriptor, ExecutorId, JobId, JobKind, JobSpec, StageId, StageSpec,
        TaskId, TaskSpec,
    };

    use super::*;

    #[derive(Debug)]
    struct NeverLeader;

    #[async_trait::async_trait]
    impl LeaderElection for NeverLeader {
        fn is_leader(&self) -> bool {
            false
        }

        async fn try_acquire(&self) -> bool {
            false
        }

        async fn renew(&self) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn ccp_submit_and_job_coordinator_scope() {
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
        ccp.submit_job(spec).await.unwrap();
        let jcp = ccp.job_coordinator(job_id);
        assert_eq!(jcp.job_id(), &JobId::try_new("job-1").unwrap());
    }

    #[tokio::test]
    async fn leader_loop_demotes_active_coordinator_when_acquire_fails() {
        let id = CoordinatorId::try_new("ccp-standby").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(id.clone()));
        let ccp = Arc::new(ClusterControlPlane::from_shared_with_leader(
            id,
            shared.clone(),
            Arc::new(NeverLeader),
        ));

        let ccp_loop = Arc::clone(&ccp);
        let handle = tokio::spawn(async move {
            ccp_loop.run_leader_loop().await;
        });

        tokio::time::timeout(std::time::Duration::from_millis(100), async {
            loop {
                if shared.read().await.state() == krishiv_proto::CoordinatorState::Standby {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        handle.abort();
        let _ = handle.await;
        assert_eq!(
            shared.read().await.state(),
            krishiv_proto::CoordinatorState::Standby
        );
    }
}
