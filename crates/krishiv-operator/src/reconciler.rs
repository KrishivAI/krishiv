//! Reconciliation.

use std::fmt;
use std::sync::Arc;

use krishiv_proto::{
    CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, JobId,
    JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec,
};
use krishiv_scheduler::{
    Coordinator, JobCoordinator, JobSnapshot, SchedulerError, SharedCoordinator,
};

use crate::constants::{API_GROUP, API_VERSION, KIND};
use crate::crd::job::{
    ConditionStatus, JobCondition, KrishivJobPhase, KrishivJobResource, KrishivJobStatus,
    TaskStatusCounters,
};
use crate::error::{OperatorError, OperatorResult};
use crate::pod_failure::ExecutorPodLaunchFailure;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileAction {
    /// Resource was converted and submitted to the active coordinator.
    Submitted,
    /// Existing scheduler job was observed and status was refreshed.
    Observed,
    /// Resource is accepted but no scheduler executor can place it yet.
    WaitingForExecutors,
    /// Krishiv finalizer was added to the resource so deletion can be tracked.
    FinalizerAdded,
    /// Resource is being deleted; scheduler job was cancelled and finalizer was removed.
    FinalizerRemoved,
    /// Executor pod launch failed before the scheduler could run the job.
    ExecutorPodLaunchFailed,
}

/// Reconcile result including the status a live controller would patch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileOutcome {
    action: ReconcileAction,
    status: KrishivJobStatus,
}

impl ReconcileOutcome {
    /// Action performed or planned.
    pub fn action(&self) -> ReconcileAction {
        self.action
    }

    /// Status to write to the `KrishivJob/status` subresource.
    pub fn status(&self) -> &KrishivJobStatus {
        &self.status
    }
}

/// Optional executor registered at controller startup for the R2 bootstrap path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapExecutor {
    /// Executor id.
    pub executor_id: String,
    /// Host or pod label used in scheduler snapshots.
    pub host: String,
    /// Static task slots.
    pub slots: usize,
}

impl BootstrapExecutor {
    /// Create a bootstrap executor descriptor.
    pub fn new(executor_id: impl Into<String>, host: impl Into<String>, slots: usize) -> Self {
        Self {
            executor_id: executor_id.into(),
            host: host.into(),
            slots,
        }
    }
}

/// R2 in-process reconciler used before the live Kubernetes watcher exists.
#[derive(Debug, Clone)]
pub struct KrishivJobReconciler {
    coordinator_id: CoordinatorId,
    /// Tracks jobs that already have a dedicated in-process JCP loop.
    dedicated_loops: Arc<std::sync::Mutex<std::collections::HashSet<JobId>>>,
}
impl KrishivJobReconciler {
    /// Create a reconciler for one active coordinator.
    pub fn new(coordinator_id: CoordinatorId) -> Self {
        Self {
            coordinator_id,
            dedicated_loops: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        }
    }

    /// Start a per-job orchestration loop when `dedicatedCoordinator` is enabled.
    pub fn ensure_dedicated_job_loop(
        &self,
        cluster: &SharedCoordinator,
        job_id: &JobId,
        enabled: bool,
    ) {
        if !enabled {
            return;
        }
        let Ok(mut guard) = self.dedicated_loops.lock() else {
            return;
        };
        if !guard.insert(job_id.clone()) {
            return;
        }
        drop(guard);
        JobCoordinator::new(job_id.clone(), cluster.clone()).spawn_job_orchestration_loops();
    }

    /// Active coordinator id used in status patches.
    pub fn coordinator_id(&self) -> &CoordinatorId {
        &self.coordinator_id
    }

    /// Reconcile one `KrishivJob` resource against an in-process coordinator.
    pub fn reconcile(
        &self,
        coordinator: &mut Coordinator,
        resource: &KrishivJobResource,
    ) -> OperatorResult<ReconcileOutcome> {
        self.reconcile_with_executor_pod_failure(coordinator, resource, None)
    }

    /// Reconcile one `KrishivJob` while considering observed executor pod launch failures.
    pub fn reconcile_with_executor_pod_failure(
        &self,
        coordinator: &mut Coordinator,
        resource: &KrishivJobResource,
        pod_failure: Option<ExecutorPodLaunchFailure>,
    ) -> OperatorResult<ReconcileOutcome> {
        // Finalizer lifecycle: add on first observe; remove on deletion after cleanup.
        if resource.metadata.is_being_deleted() {
            // Resource is being deleted — cancel the scheduler job if it exists and
            // signal that the finalizer should be stripped so Kubernetes can proceed.
            let job_id = scheduler_job_id(resource)?;
            let _ = coordinator.cancel_job(&job_id); // best-effort: ignore unknown job
            let status = match coordinator.job_snapshot(&job_id) {
                Ok(snapshot) => status_from_snapshot(resource, &self.coordinator_id, &snapshot),
                Err(_) => accepted_waiting_for_executors(resource, &self.coordinator_id),
            };
            return Ok(ReconcileOutcome {
                action: ReconcileAction::FinalizerRemoved,
                status,
            });
        }

        if let Some(failure) = pod_failure {
            if let Some(executor_id) = failure.executor_id.as_ref() {
                let _ = coordinator.mark_executor_lost(executor_id);
            }
            return Ok(ReconcileOutcome {
                action: ReconcileAction::ExecutorPodLaunchFailed,
                status: executor_pod_launch_failed_status(resource, &self.coordinator_id, failure),
            });
        }

        if !resource.metadata.has_finalizer() {
            // Resource does not yet have our finalizer — request it be added.
            let status = accepted_waiting_for_executors(resource, &self.coordinator_id);
            return Ok(ReconcileOutcome {
                action: ReconcileAction::FinalizerAdded,
                status,
            });
        }

        validate_resource(resource)?;
        let job_id = scheduler_job_id(resource)?;

        match coordinator.job_snapshot(&job_id) {
            Ok(snapshot) => {
                let status = status_from_snapshot(resource, &self.coordinator_id, &snapshot);
                Ok(ReconcileOutcome {
                    action: ReconcileAction::Observed,
                    status,
                })
            }
            Err(SchedulerError::UnknownJob { .. }) => {
                let job = job_spec_from_resource(resource)?;
                match coordinator.submit_job(job) {
                    Ok(_) => {
                        let snapshot = coordinator.job_snapshot(&job_id)?;
                        let status =
                            status_from_snapshot(resource, &self.coordinator_id, &snapshot);
                        Ok(ReconcileOutcome {
                            action: ReconcileAction::Submitted,
                            status,
                        })
                    }
                    Err(SchedulerError::NoExecutors) => Ok(ReconcileOutcome {
                        action: ReconcileAction::WaitingForExecutors,
                        status: accepted_waiting_for_executors(resource, &self.coordinator_id),
                    }),
                    Err(error) => Err(error.into()),
                }
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// Convert a `KrishivJob` resource into an R2 scheduler job.
pub fn job_spec_from_resource(resource: &KrishivJobResource) -> OperatorResult<JobSpec> {
    validate_resource(resource)?;
    let job_id = scheduler_job_id(resource)?;
    let stage_id = StageId::try_new("stage-1").map_err(invalid_id)?;
    let mut stage = StageSpec::new(stage_id, format!("{}-stage", resource.metadata.name));

    for task_idx in 1..=resource.spec.tasks {
        let task_id = TaskId::try_new(format!("task-{task_idx}")).map_err(invalid_id)?;
        stage = stage.with_task(TaskSpec::new(task_id, task_description(resource, task_idx)));
    }

    Ok(JobSpec::new(
        job_id,
        resource.metadata.name.clone(),
        JobKind::from(resource.spec.mode),
    )
    .with_stage(stage))
}

/// Build a deterministic local coordinator with one healthy executor for tests.
pub fn demo_coordinator(
    coordinator_id: CoordinatorId,
    slots: usize,
) -> OperatorResult<Coordinator> {
    let executor_id = ExecutorId::try_new("exec-operator-demo").map_err(invalid_id)?;
    let mut coordinator = Coordinator::active(coordinator_id);
    coordinator.register_executor(ExecutorDescriptor::new(
        executor_id.clone(),
        "operator-demo-executor",
        slots,
    ))?;
    coordinator.executor_heartbeat(ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy))?;
    Ok(coordinator)
}

fn validate_resource(resource: &KrishivJobResource) -> OperatorResult<()> {
    if resource.api_version != format!("{API_GROUP}/{API_VERSION}") {
        return Err(OperatorError::InvalidResource {
            message: format!(
                "unsupported apiVersion {}; expected {API_GROUP}/{API_VERSION}",
                resource.api_version
            ),
        });
    }
    if resource.kind != KIND {
        return Err(OperatorError::InvalidResource {
            message: format!("unsupported kind {}; expected {KIND}", resource.kind),
        });
    }
    if resource.metadata.name.trim().is_empty() {
        return Err(OperatorError::InvalidResource {
            message: String::from("metadata.name cannot be empty"),
        });
    }
    if resource.spec.image.trim().is_empty() {
        return Err(OperatorError::InvalidResource {
            message: String::from("spec.image cannot be empty"),
        });
    }
    if resource.spec.tasks == 0 {
        return Err(OperatorError::InvalidResource {
            message: String::from("spec.tasks must be greater than zero"),
        });
    }
    if resource.spec.parallelism == Some(0) {
        return Err(OperatorError::InvalidResource {
            message: String::from("spec.parallelism must be greater than zero when set"),
        });
    }
    Ok(())
}

pub(crate) fn scheduler_job_id(resource: &KrishivJobResource) -> OperatorResult<JobId> {
    JobId::try_new(resource.scheduler_job_id()).map_err(invalid_id)
}

fn invalid_id(error: impl fmt::Display) -> OperatorError {
    OperatorError::InvalidResource {
        message: error.to_string(),
    }
}

fn task_description(resource: &KrishivJobResource, task_idx: usize) -> String {
    let args = if resource.spec.args.is_empty() {
        String::from("no args")
    } else {
        resource.spec.args.join(" ")
    };

    format!(
        "{} task {task_idx} using image {} with {args}",
        JobKind::from(resource.spec.mode),
        resource.spec.image
    )
}

fn status_from_snapshot(
    resource: &KrishivJobResource,
    coordinator_id: &CoordinatorId,
    snapshot: &JobSnapshot,
) -> KrishivJobStatus {
    let condition = JobCondition::new(
        "Scheduled",
        ConditionStatus::True,
        "SchedulerObserved",
        format!(
            "scheduler job {} is {}",
            snapshot.job_id(),
            snapshot.state()
        ),
    );

    KrishivJobStatus {
        phase: KrishivJobPhase::from(snapshot.state()),
        coordinator: Some(coordinator_id.to_string()),
        observed_generation: resource.metadata.generation,
        stages: snapshot.stage_count(),
        tasks: TaskStatusCounters {
            assigned: snapshot.assigned_task_count(),
            running: snapshot.running_task_count(),
            succeeded: snapshot.succeeded_task_count(),
            failed: snapshot.failed_task_count(),
        },
        conditions: vec![condition],
    }
}

fn accepted_waiting_for_executors(
    resource: &KrishivJobResource,
    coordinator_id: &CoordinatorId,
) -> KrishivJobStatus {
    KrishivJobStatus {
        phase: KrishivJobPhase::Accepted,
        coordinator: Some(coordinator_id.to_string()),
        observed_generation: resource.metadata.generation,
        stages: 0,
        tasks: TaskStatusCounters::default(),
        conditions: vec![JobCondition::new(
            "Scheduled",
            ConditionStatus::False,
            "NoExecutors",
            "no healthy executors are available for static R2 placement",
        )],
    }
}

fn executor_pod_launch_failed_status(
    resource: &KrishivJobResource,
    coordinator_id: &CoordinatorId,
    failure: ExecutorPodLaunchFailure,
) -> KrishivJobStatus {
    KrishivJobStatus {
        phase: KrishivJobPhase::Failed,
        coordinator: Some(coordinator_id.to_string()),
        observed_generation: resource.metadata.generation,
        stages: 0,
        tasks: TaskStatusCounters::default(),
        conditions: vec![JobCondition::new(
            "ExecutorPodReady",
            ConditionStatus::False,
            failure.reason,
            failure.message,
        )],
    }
}
