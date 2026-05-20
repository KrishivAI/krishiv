#![forbid(unsafe_code)]

//! R2 Kubernetes operator reconciliation foundation.
//!
//! This crate starts the controller path without binding Krishiv to a live
//! Kubernetes client yet. It owns typed `KrishivJob` resource models,
//! conversion into scheduler jobs, and status patch planning over the existing
//! in-process R2 coordinator.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use futures::StreamExt;
use krishiv_proto::{
    CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, JobId,
    JobKind, JobSpec, JobState, StageId, StageSpec, TaskId, TaskSpec,
};
use krishiv_scheduler::{
    Coordinator, JobSnapshot, LeaderElection, NamespaceQuotaSnapshot, QueueManager, QuotaPolicy,
    ResourceUsage, SchedulerError, SharedCoordinator, SubmitOutcome,
};
use kube::Client;
use kube::api::{Api, Patch, PatchParams};
use kube::core::{ApiResource, DynamicObject, GroupVersionKind};
use kube::runtime::watcher::{self, Event as WatchEvent};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Krishiv Kubernetes API group.
pub const API_GROUP: &str = "krishiv.io";

/// KrishivJob API version owned by R2.
pub const API_VERSION: &str = "v1alpha1";

/// KrishivJob Kubernetes kind.
pub const KIND: &str = "KrishivJob";

/// R2 finalizer name reserved for future cleanup.
pub const FINALIZER: &str = "krishiv.io/job-finalizer";

/// Pod label used to associate an executor pod with a scheduler executor id.
pub const EXECUTOR_ID_LABEL: &str = "krishiv.io/executor-id";

/// Operator result alias.
pub type OperatorResult<T> = Result<T, OperatorError>;

/// Operator and reconciliation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorError {
    /// Resource validation failed before scheduling.
    InvalidResource { message: String },
    /// Scheduler operation failed.
    Scheduler(SchedulerError),
    /// Kubernetes client or runtime operation failed.
    Kubernetes { message: String },
    /// Serialization or deserialization failed.
    Serialization { message: String },
    /// Shared coordinator lock was poisoned.
    CoordinatorLockPoisoned,
}

impl fmt::Display for OperatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidResource { message } => write!(f, "invalid KrishivJob: {message}"),
            Self::Scheduler(error) => write!(f, "{error}"),
            Self::Kubernetes { message } => write!(f, "kubernetes operation failed: {message}"),
            Self::Serialization { message } => write!(f, "serialization failed: {message}"),
            Self::CoordinatorLockPoisoned => f.write_str("shared coordinator lock was poisoned"),
        }
    }
}

impl Error for OperatorError {}

impl From<SchedulerError> for OperatorError {
    fn from(value: SchedulerError) -> Self {
        Self::Scheduler(value)
    }
}

impl From<kube::Error> for OperatorError {
    fn from(value: kube::Error) -> Self {
        Self::Kubernetes {
            message: value.to_string(),
        }
    }
}

impl From<watcher::Error> for OperatorError {
    fn from(value: watcher::Error) -> Self {
        Self::Kubernetes {
            message: value.to_string(),
        }
    }
}

impl From<serde_json::Error> for OperatorError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization {
            message: value.to_string(),
        }
    }
}

/// Minimal Kubernetes object metadata needed by the R2 reconciler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMeta {
    /// Resource name.
    pub name: String,
    /// Resource namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Kubernetes generation observed by the controller.
    #[serde(default)]
    pub generation: i64,
    /// Resource labels.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// Resource finalizers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub finalizers: Vec<String>,
    /// Non-null when the resource has been deleted but finalizers are still present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletion_timestamp: Option<String>,
}

impl ObjectMeta {
    /// Create metadata for a namespaced resource.
    pub fn namespaced(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace: Some(namespace.into()),
            generation: 1,
            labels: BTreeMap::new(),
            finalizers: Vec::new(),
            deletion_timestamp: None,
        }
    }

    /// Namespace used for scheduler identity.
    pub fn namespace_or_default(&self) -> &str {
        self.namespace.as_deref().unwrap_or("default")
    }

    /// URL-safe R2 scheduler job id for this namespaced resource.
    pub fn scheduler_job_id(&self) -> String {
        format!("{}.{}", self.namespace_or_default(), self.name)
    }

    /// Whether the Krishiv finalizer is present on this resource.
    pub fn has_finalizer(&self) -> bool {
        self.finalizers.iter().any(|f| f == FINALIZER)
    }

    /// Whether the resource has been deleted (deletion timestamp is set).
    pub fn is_being_deleted(&self) -> bool {
        self.deletion_timestamp.is_some()
    }
}

/// `KrishivJob` custom resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KrishivJobResource {
    /// Kubernetes API version.
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    /// Kubernetes resource kind.
    pub kind: String,
    /// Resource metadata.
    pub metadata: ObjectMeta,
    /// Desired job state.
    pub spec: KrishivJobSpec,
    /// Observed job status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<KrishivJobStatus>,
}

impl KrishivJobResource {
    /// Create a new R2 `KrishivJob` resource.
    pub fn new(metadata: ObjectMeta, spec: KrishivJobSpec) -> Self {
        Self {
            api_version: format!("{API_GROUP}/{API_VERSION}"),
            kind: KIND.to_owned(),
            metadata,
            spec,
            status: None,
        }
    }

    /// Scheduler job id derived from metadata.
    pub fn scheduler_job_id(&self) -> String {
        self.metadata.scheduler_job_id()
    }
}

/// Desired execution mode in a `KrishivJob`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KrishivJobMode {
    /// Bounded batch job.
    Batch,
    /// Early R2 streaming job with R1-level stream semantics.
    Streaming,
}

impl From<KrishivJobMode> for JobKind {
    fn from(value: KrishivJobMode) -> Self {
        match value {
            KrishivJobMode::Batch => Self::Batch,
            KrishivJobMode::Streaming => Self::Streaming,
        }
    }
}

/// Kubernetes restart policy accepted by the R2 CRD.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RestartPolicy {
    /// Do not restart failed pods.
    #[default]
    Never,
    /// Restart on failure.
    OnFailure,
}

/// Desired `KrishivJob` spec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KrishivJobSpec {
    /// Bounded batch or early R2 streaming execution.
    pub mode: KrishivJobMode,
    /// Container image used by executors for this job.
    pub image: String,
    /// Number of static tasks for the R2 scheduler.
    pub tasks: usize,
    /// Maximum executor task parallelism requested by the job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<usize>,
    /// Optional container entrypoint override.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entrypoint: Vec<String>,
    /// Optional container arguments.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Restart policy.
    #[serde(default, rename = "restartPolicy")]
    pub restart_policy: RestartPolicy,
    /// Optional labels propagated to future runtime objects.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

impl KrishivJobSpec {
    /// Create a new job spec.
    pub fn new(mode: KrishivJobMode, image: impl Into<String>, tasks: usize) -> Self {
        Self {
            mode,
            image: image.into(),
            tasks,
            parallelism: None,
            entrypoint: Vec::new(),
            args: Vec::new(),
            restart_policy: RestartPolicy::Never,
            labels: BTreeMap::new(),
        }
    }

    /// Effective task parallelism requested by the job.
    pub fn effective_parallelism(&self) -> usize {
        self.parallelism.unwrap_or(self.tasks)
    }
}

/// Observed `KrishivJob` phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KrishivJobPhase {
    /// Resource was accepted by the controller.
    Accepted,
    /// Resource is being planned.
    Planning,
    /// Job is running.
    Running,
    /// Job succeeded.
    Succeeded,
    /// Job failed.
    Failed,
    /// Job was cancelled.
    Cancelled,
}

impl From<JobState> for KrishivJobPhase {
    fn from(value: JobState) -> Self {
        match value {
            JobState::Accepted => Self::Accepted,
            JobState::Planning => Self::Planning,
            JobState::Running => Self::Running,
            JobState::Succeeded => Self::Succeeded,
            JobState::Failed => Self::Failed,
            JobState::Cancelled => Self::Cancelled,
        }
    }
}

/// Task counters stored under `.status.tasks`.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskStatusCounters {
    /// Assigned task count.
    pub assigned: usize,
    /// Running task count.
    pub running: usize,
    /// Succeeded task count.
    pub succeeded: usize,
    /// Failed task count.
    pub failed: usize,
}

/// Kubernetes condition status values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConditionStatus {
    /// Condition is true.
    True,
    /// Condition is false.
    False,
    /// Condition is unknown.
    Unknown,
}

/// Kubernetes-style condition stored under `.status.conditions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCondition {
    /// Condition type.
    #[serde(rename = "type")]
    pub condition_type: String,
    /// Condition status.
    pub status: ConditionStatus,
    /// Machine-readable reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Human-readable message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Last transition timestamp. R2 tests keep this unset for determinism.
    #[serde(
        default,
        rename = "lastTransitionTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_transition_time: Option<String>,
}

impl JobCondition {
    fn new(
        condition_type: impl Into<String>,
        status: ConditionStatus,
        reason: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            condition_type: condition_type.into(),
            status,
            reason: Some(reason.into()),
            message: Some(message.into()),
            last_transition_time: None,
        }
    }
}

/// Observed `KrishivJob` status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KrishivJobStatus {
    /// High-level job phase.
    pub phase: KrishivJobPhase,
    /// Active coordinator id that observed the status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coordinator: Option<String>,
    /// Kubernetes generation observed by the controller.
    #[serde(rename = "observedGeneration")]
    pub observed_generation: i64,
    /// Number of scheduler stages.
    pub stages: usize,
    /// Task state counters.
    pub tasks: TaskStatusCounters,
    /// Kubernetes-style status conditions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<JobCondition>,
}

/// Reconcile action performed or planned by the R2 operator.
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

/// Classified executor pod launch failure detected by the operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorPodLaunchFailure {
    /// Scheduler executor id associated with the failed pod, when known.
    pub executor_id: Option<ExecutorId>,
    /// Machine-readable reason, suitable for a status condition.
    pub reason: String,
    /// Human-readable message from pod status/container waiting state.
    pub message: String,
}

impl ExecutorPodLaunchFailure {
    fn new(reason: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            executor_id: None,
            reason: reason.into(),
            message: message.into(),
        }
    }

    fn with_executor_id(mut self, executor_id: ExecutorId) -> Self {
        self.executor_id = Some(executor_id);
        self
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

/// Live Kubernetes controller configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KubernetesControllerConfig {
    /// Namespace to watch. `None` watches all namespaces.
    namespace: Option<String>,
    /// Active R2 coordinator id.
    coordinator_id: CoordinatorId,
    /// Optional label selector for the watcher.
    label_selector: Option<String>,
    /// Optional field selector for the watcher.
    field_selector: Option<String>,
    /// Optional bootstrap executor for R2 static scheduling.
    bootstrap_executor: Option<BootstrapExecutor>,
}

/// Runtime state owned by the live R2 Kubernetes controller process.
#[derive(Debug, Clone)]
pub struct KubernetesControllerRuntime {
    coordinator: SharedCoordinator,
    reconciler: KrishivJobReconciler,
}

impl KubernetesControllerRuntime {
    /// Create an active coordinator runtime from controller config.
    pub fn new(config: &KubernetesControllerConfig) -> OperatorResult<Self> {
        let mut coordinator = Coordinator::active(config.coordinator_id.clone());
        if let Some(executor) = &config.bootstrap_executor {
            register_bootstrap_executor(&mut coordinator, executor)?;
        }

        Ok(Self {
            coordinator: SharedCoordinator::new(coordinator),
            reconciler: KrishivJobReconciler::new(config.coordinator_id.clone()),
        })
    }

    /// Shared coordinator handle used by the controller and status server.
    pub fn coordinator(&self) -> SharedCoordinator {
        self.coordinator.clone()
    }

    /// Reconciler bound to the active coordinator id.
    pub fn reconciler(&self) -> &KrishivJobReconciler {
        &self.reconciler
    }
}

impl KubernetesControllerConfig {
    /// Create a config for one namespace.
    pub fn namespaced(namespace: impl Into<String>, coordinator_id: CoordinatorId) -> Self {
        Self {
            namespace: Some(namespace.into()),
            coordinator_id,
            label_selector: None,
            field_selector: None,
            bootstrap_executor: None,
        }
    }

    /// Create a config for all namespaces.
    pub fn all_namespaces(coordinator_id: CoordinatorId) -> Self {
        Self {
            namespace: None,
            coordinator_id,
            label_selector: None,
            field_selector: None,
            bootstrap_executor: None,
        }
    }

    /// Namespace being watched, if scoped.
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    /// Coordinator id used by the live controller.
    pub fn coordinator_id(&self) -> &CoordinatorId {
        &self.coordinator_id
    }

    /// Add a Kubernetes label selector.
    #[must_use]
    pub fn with_label_selector(mut self, selector: impl Into<String>) -> Self {
        self.label_selector = Some(selector.into());
        self
    }

    /// Add a Kubernetes field selector.
    #[must_use]
    pub fn with_field_selector(mut self, selector: impl Into<String>) -> Self {
        self.field_selector = Some(selector.into());
        self
    }

    /// Register a bootstrap executor when the controller starts.
    #[must_use]
    pub fn with_bootstrap_executor(mut self, executor: BootstrapExecutor) -> Self {
        self.bootstrap_executor = Some(executor);
        self
    }
}

/// One live Kubernetes reconciliation report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KubernetesReconcileReport {
    /// Namespace containing the reconciled resource.
    pub namespace: String,
    /// Resource name.
    pub name: String,
    /// Reconciler action.
    pub action: ReconcileAction,
    /// Status patched to Kubernetes.
    pub status: KrishivJobStatus,
}

/// Run the live Kubernetes controller using in-cluster config or local kubeconfig.
pub async fn run_kubernetes_controller(config: KubernetesControllerConfig) -> OperatorResult<()> {
    let client = Client::try_default().await?;
    run_kubernetes_controller_with_client(client, config).await
}

/// Run the live Kubernetes controller with an explicit Kubernetes client.
pub async fn run_kubernetes_controller_with_client(
    client: Client,
    config: KubernetesControllerConfig,
) -> OperatorResult<()> {
    let runtime = KubernetesControllerRuntime::new(&config)?;
    run_kubernetes_controller_runtime_with_client(client, config, runtime).await
}

/// Run the live Kubernetes controller with an explicit shared runtime.
pub async fn run_kubernetes_controller_runtime_with_client(
    client: Client,
    config: KubernetesControllerConfig,
    runtime: KubernetesControllerRuntime,
) -> OperatorResult<()> {
    let jobs = krishivjob_api(client, config.namespace())?;
    let watcher_config = watcher_config(&config);

    let mut events = watcher::watcher(jobs.clone(), watcher_config).boxed();
    while let Some(event) = events.next().await {
        match event? {
            WatchEvent::Apply(object) | WatchEvent::InitApply(object) => {
                reconcile_dynamic_object_with_runtime(&jobs, &runtime, object).await?;
            }
            WatchEvent::Delete(_) | WatchEvent::Init | WatchEvent::InitDone => {}
        }
    }

    Ok(())
}

/// Reconcile one Kubernetes dynamic object using a shared controller runtime.
pub async fn reconcile_dynamic_object_with_runtime(
    jobs: &Api<DynamicObject>,
    runtime: &KubernetesControllerRuntime,
    object: DynamicObject,
) -> OperatorResult<KubernetesReconcileReport> {
    let resource = resource_from_dynamic_object(&object)?;
    let outcome = {
        let mut coordinator = runtime
            .coordinator
            .write()
            .map_err(|_| OperatorError::CoordinatorLockPoisoned)?;
        runtime.reconciler.reconcile(&mut coordinator, &resource)?
    };
    patch_krishivjob_status(jobs, &resource, outcome.status()).await?;

    Ok(KubernetesReconcileReport {
        namespace: resource.metadata.namespace_or_default().to_owned(),
        name: resource.metadata.name,
        action: outcome.action(),
        status: outcome.status().clone(),
    })
}

/// Reconcile one Kubernetes dynamic object and patch its status.
pub async fn reconcile_dynamic_object(
    jobs: &Api<DynamicObject>,
    reconciler: &KrishivJobReconciler,
    coordinator: &mut Coordinator,
    object: DynamicObject,
) -> OperatorResult<KubernetesReconcileReport> {
    let resource = resource_from_dynamic_object(&object)?;
    let outcome = reconciler.reconcile(coordinator, &resource)?;
    patch_krishivjob_status(jobs, &resource, outcome.status()).await?;

    Ok(KubernetesReconcileReport {
        namespace: resource.metadata.namespace_or_default().to_owned(),
        name: resource.metadata.name,
        action: outcome.action(),
        status: outcome.status().clone(),
    })
}

/// Convert a Kubernetes dynamic object into a typed `KrishivJobResource`.
pub fn resource_from_dynamic_object(object: &DynamicObject) -> OperatorResult<KrishivJobResource> {
    let value = serde_json::to_value(object)?;
    let mut resource: KrishivJobResource = serde_json::from_value(value)?;
    if resource.api_version.is_empty() {
        resource.api_version = format!("{API_GROUP}/{API_VERSION}");
    }
    if resource.kind.is_empty() {
        resource.kind = KIND.to_owned();
    }
    Ok(resource)
}

/// Patch the `KrishivJob/status` subresource.
pub async fn patch_krishivjob_status(
    jobs: &Api<DynamicObject>,
    resource: &KrishivJobResource,
    status: &KrishivJobStatus,
) -> OperatorResult<()> {
    let params = PatchParams::default();
    let patch = status_patch(status);
    jobs.patch_status(&resource.metadata.name, &params, &Patch::Merge(&patch))
        .await?;
    Ok(())
}

/// Build the Kubernetes status merge patch.
pub fn status_patch(status: &KrishivJobStatus) -> Value {
    json!({ "status": status })
}

/// API resource descriptor for `krishivjobs.krishiv.io`.
pub fn krishivjob_api_resource() -> ApiResource {
    let gvk = GroupVersionKind::gvk(API_GROUP, API_VERSION, KIND);
    ApiResource::from_gvk_with_plural(&gvk, "krishivjobs")
}

/// Kubernetes API handle for `KrishivJob` dynamic objects.
pub fn krishivjob_api(
    client: Client,
    namespace: Option<&str>,
) -> OperatorResult<Api<DynamicObject>> {
    let api_resource = krishivjob_api_resource();
    Ok(match namespace {
        Some(namespace) => Api::namespaced_with(client, namespace, &api_resource),
        None => Api::all_with(client, &api_resource),
    })
}

/// R2 in-process reconciler used before the live Kubernetes watcher exists.
#[derive(Debug, Clone)]
pub struct KrishivJobReconciler {
    coordinator_id: CoordinatorId,
}

fn watcher_config(config: &KubernetesControllerConfig) -> watcher::Config {
    watcher::Config {
        label_selector: config.label_selector.clone(),
        field_selector: config.field_selector.clone(),
        ..Default::default()
    }
}

fn register_bootstrap_executor(
    coordinator: &mut Coordinator,
    executor: &BootstrapExecutor,
) -> OperatorResult<()> {
    if executor.slots == 0 {
        return Err(OperatorError::InvalidResource {
            message: String::from("bootstrap executor slots must be greater than zero"),
        });
    }

    let executor_id = ExecutorId::try_new(executor.executor_id.clone()).map_err(invalid_id)?;
    coordinator.register_executor(ExecutorDescriptor::new(
        executor_id.clone(),
        executor.host.clone(),
        executor.slots,
    ))?;
    coordinator.executor_heartbeat(ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy))?;
    Ok(())
}

impl KrishivJobReconciler {
    /// Create a reconciler for one active coordinator.
    pub fn new(coordinator_id: CoordinatorId) -> Self {
        Self { coordinator_id }
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

fn scheduler_job_id(resource: &KrishivJobResource) -> OperatorResult<JobId> {
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

/// Detect executor pod launch failures from a Kubernetes Pod-like JSON status object.
///
/// The live controller can feed this helper with pod objects selected for a
/// `KrishivJob`. Tests use JSON fixtures so detection remains deterministic
/// without a Kubernetes API server.
pub fn detect_executor_pod_launch_failure(pod: &Value) -> Option<ExecutorPodLaunchFailure> {
    let executor_id = executor_id_from_pod(pod);
    let status = pod.get("status")?;
    let phase = status.get("phase").and_then(Value::as_str);
    let reason = status.get("reason").and_then(Value::as_str);
    let message = status.get("message").and_then(Value::as_str);
    if phase == Some("Failed") {
        return Some(with_optional_executor_id(
            ExecutorPodLaunchFailure::new(
                reason.unwrap_or("PodFailed"),
                message.unwrap_or("executor pod failed before task launch"),
            ),
            executor_id,
        ));
    }
    if reason == Some("Unschedulable") {
        return Some(with_optional_executor_id(
            ExecutorPodLaunchFailure::new(
                "Unschedulable",
                message.unwrap_or("executor pod is unschedulable"),
            ),
            executor_id,
        ));
    }

    for status in status
        .get("containerStatuses")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let waiting = status.get("state").and_then(|state| state.get("waiting"));
        if let Some(waiting) = waiting {
            let reason = waiting
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("ContainerWaiting");
            if matches!(
                reason,
                "ImagePullBackOff"
                    | "ErrImagePull"
                    | "CrashLoopBackOff"
                    | "CreateContainerConfigError"
                    | "CreateContainerError"
            ) {
                return Some(with_optional_executor_id(
                    ExecutorPodLaunchFailure::new(
                        reason,
                        waiting
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("executor container failed to launch"),
                    ),
                    executor_id,
                ));
            }
        }
    }

    None
}

fn executor_id_from_pod(pod: &Value) -> Option<ExecutorId> {
    pod.get("metadata")
        .and_then(|metadata| metadata.get("labels"))
        .and_then(|labels| labels.get(EXECUTOR_ID_LABEL))
        .and_then(Value::as_str)
        .and_then(|value| ExecutorId::try_new(value).ok())
}

fn with_optional_executor_id(
    failure: ExecutorPodLaunchFailure,
    executor_id: Option<ExecutorId>,
) -> ExecutorPodLaunchFailure {
    match executor_id {
        Some(executor_id) => failure.with_executor_id(executor_id),
        None => failure,
    }
}

// ── R7.1: KrishivQueue CRD and CrdQueueManager ───────────────────────────────

/// Kind string for the `KrishivQueue` CRD.
pub const QUEUE_KIND: &str = "KrishivQueue";

/// `KrishivQueue` Kubernetes resource spec.
///
/// Defines quota limits for a governance namespace.  The
/// `CrdQueueManager` reads live `KrishivQueue` objects to derive the
/// `QuotaPolicy` applied to each namespace at admission time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KrishivQueueSpec {
    /// Governance namespace name.  Must match `JobSpec::namespace_id`.
    pub namespace: String,
    /// Maximum CPU nanoseconds reserved simultaneously (`None` = unlimited).
    #[serde(default)]
    pub cpu_nanos_limit: Option<u64>,
    /// Maximum memory bytes reserved simultaneously (`None` = unlimited).
    #[serde(default)]
    pub memory_bytes_limit: Option<u64>,
    /// Maximum concurrent active jobs (`None` = unlimited).
    #[serde(default)]
    pub max_concurrent_jobs: Option<usize>,
    /// Scheduling priority band (0 = lowest, 255 = highest; default 128).
    #[serde(default = "default_priority")]
    pub priority: u8,
}

fn default_priority() -> u8 {
    128
}

/// Status subresource for a `KrishivQueue`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KrishivQueueStatus {
    /// Number of active jobs currently admitted to this namespace.
    #[serde(default)]
    pub active_job_count: usize,
    /// CPU nanoseconds currently reserved in this namespace.
    #[serde(default)]
    pub cpu_nanos_reserved: u64,
    /// Memory bytes currently reserved in this namespace.
    #[serde(default)]
    pub memory_bytes_reserved: u64,
}

/// Typed `KrishivQueue` Kubernetes resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KrishivQueue {
    pub spec: KrishivQueueSpec,
    #[serde(default)]
    pub status: KrishivQueueStatus,
}

impl KrishivQueue {
    /// Derive a `QuotaPolicy` from this queue's spec.
    pub fn quota_policy(&self) -> QuotaPolicy {
        QuotaPolicy {
            cpu_nanos_limit: self.spec.cpu_nanos_limit,
            memory_bytes_limit: self.spec.memory_bytes_limit,
            max_concurrent_jobs: self.spec.max_concurrent_jobs,
        }
    }
}

/// Kubernetes CRD-backed queue manager.
///
/// Built from a snapshot of `KrishivQueue` CRD objects read from the API
/// server.  In production the operator refreshes this at each reconcile loop;
/// the `QueueManager` trait itself is stateless.
///
/// Lives in `krishiv-operator` so it is the only crate allowed to call
/// Kubernetes APIs (Kubernetes isolation rule).
#[derive(Debug)]
pub struct CrdQueueManager {
    /// Policy per namespace derived from `KrishivQueue` CRD objects.
    namespace_policies: std::collections::HashMap<String, QuotaPolicy>,
    /// Default policy for namespaces without a `KrishivQueue` object.
    default_policy: QuotaPolicy,
}

impl CrdQueueManager {
    /// Build from a list of `KrishivQueue` resources.
    pub fn from_queues(queues: impl IntoIterator<Item = KrishivQueue>) -> Self {
        let namespace_policies = queues
            .into_iter()
            .map(|q| (q.spec.namespace.clone(), q.quota_policy()))
            .collect();
        Self {
            namespace_policies,
            default_policy: QuotaPolicy::default(),
        }
    }

    /// Build with an explicit default policy applied to unmatched namespaces.
    pub fn with_default(
        queues: impl IntoIterator<Item = KrishivQueue>,
        default_policy: QuotaPolicy,
    ) -> Self {
        let mut mgr = Self::from_queues(queues);
        mgr.default_policy = default_policy;
        mgr
    }
}

impl QueueManager for CrdQueueManager {
    fn admit(
        &self,
        spec: &krishiv_proto::JobSpec,
        quota: &NamespaceQuotaSnapshot,
    ) -> SubmitOutcome {
        let policy = spec
            .namespace_id()
            .and_then(|ns| self.namespace_policies.get(ns))
            .unwrap_or(&self.default_policy);

        if let Some(limit) = policy.max_concurrent_jobs {
            if quota.active_job_count >= limit {
                return SubmitOutcome::Queued {
                    position: quota.active_job_count.saturating_sub(limit),
                };
            }
        }
        if let Some(limit) = policy.cpu_nanos_limit {
            if quota
                .cpu_nanos_reserved
                .saturating_add(spec.cpu_limit_nanos().unwrap_or(0))
                > limit
            {
                return SubmitOutcome::Queued { position: 0 };
            }
        }
        if let Some(limit) = policy.memory_bytes_limit {
            if quota
                .memory_bytes_reserved
                .saturating_add(spec.memory_limit_bytes().unwrap_or(0))
                > limit
            {
                return SubmitOutcome::Queued { position: 0 };
            }
        }
        SubmitOutcome::Accepted
    }

    fn on_job_complete(&self, _job_id: &krishiv_proto::JobId, _usage: &ResourceUsage) {}
}

// ── K8s Lease-backed leader election ─────────────────────────────────────────

/// Kubernetes `coordination.k8s.io/v1` Lease-backed leader election.
///
/// In production this communicates with the Kubernetes API using the `kube`
/// client.  The R9 implementation provides the structural contract and a
/// simulated in-process lease for testing; live K8s API calls are wired in R10.
///
/// Lease duration: configurable (default 15 s, matching K8s controller-manager).
/// Renewal interval: every `lease_duration_s / 3` seconds.
/// Fencing: each successful `try_acquire` increments the fencing token so stale
/// coordinators are rejected at [`validate_fencing_token`] call sites.
///
/// [`validate_fencing_token`]: krishiv_checkpoint::validate_fencing_token
#[derive(Debug)]
pub struct K8sLeaseElection {
    lease_name: String,
    namespace: String,
    holder_identity: String,
    lease_duration_s: u64,
    state: std::sync::Mutex<K8sLeaseState>,
}

#[derive(Debug)]
struct K8sLeaseState {
    is_leader: bool,
    fencing_token: u64,
}

impl K8sLeaseElection {
    /// Create a new election handle.
    ///
    /// `lease_name` — the K8s Lease object name (typically the job id or coordinator id).
    /// `namespace` — the K8s namespace containing the Lease.
    /// `holder_identity` — unique coordinator identity (pod name / hostname).
    pub fn new(
        lease_name: impl Into<String>,
        namespace: impl Into<String>,
        holder_identity: impl Into<String>,
    ) -> Self {
        Self {
            lease_name: lease_name.into(),
            namespace: namespace.into(),
            holder_identity: holder_identity.into(),
            lease_duration_s: 15,
            state: std::sync::Mutex::new(K8sLeaseState {
                is_leader: false,
                fencing_token: 0,
            }),
        }
    }

    /// Set the lease duration in seconds (default: 15).
    #[must_use]
    pub fn with_lease_duration(mut self, secs: u64) -> Self {
        self.lease_duration_s = secs;
        self
    }

    /// Lease name.
    pub fn lease_name(&self) -> &str {
        &self.lease_name
    }

    /// Namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Holder identity.
    pub fn holder_identity(&self) -> &str {
        &self.holder_identity
    }

    /// Configured lease duration in seconds.
    pub fn lease_duration_s(&self) -> u64 {
        self.lease_duration_s
    }
}

impl LeaderElection for K8sLeaseElection {
    fn is_leader(&self) -> bool {
        self.state.lock().unwrap().is_leader
    }

    fn try_acquire(&self) -> bool {
        let mut s = self.state.lock().unwrap();
        // Simulate a successful acquire: increment the fencing token and mark
        // this node as leader. In production this would POST/PUT the K8s Lease
        // object with optimistic concurrency and only set `is_leader = true`
        // when the API server confirms the write.
        s.fencing_token += 1;
        s.is_leader = true;
        true
    }

    fn renew(&self) -> bool {
        // Simulate a successful renewal (lease not expired, no competing holder).
        // In production this would PATCH the Lease renewTime and fail if another
        // holder has taken over (resourceVersion conflict → return false).
        self.state.lock().unwrap().is_leader
    }

    fn release(&self) {
        self.state.lock().unwrap().is_leader = false;
    }

    fn fencing_token(&self) -> u64 {
        self.state.lock().unwrap().fencing_token
    }
}

#[cfg(test)]
mod tests {
    use krishiv_proto::{
        CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, JobId,
        JobState, TaskState, TaskStatusUpdate,
    };

    use super::{
        BootstrapExecutor, ConditionStatus, EXECUTOR_ID_LABEL, ExecutorPodLaunchFailure,
        K8sLeaseElection, KrishivJobMode, KrishivJobPhase, KrishivJobReconciler,
        KrishivJobResource, KrishivJobSpec, KubernetesControllerConfig,
        KubernetesControllerRuntime, ObjectMeta, OperatorError, ReconcileAction, demo_coordinator,
        detect_executor_pod_launch_failure, job_spec_from_resource, krishivjob_api_resource,
        resource_from_dynamic_object, status_patch,
    };
    use krishiv_scheduler::{Coordinator, LeaderElection as _};
    use kube::core::DynamicObject;
    use serde_json::json;

    #[test]
    fn builds_scheduler_job_spec_from_batch_resource() {
        let resource = sample_resource();

        let job = job_spec_from_resource(&resource).unwrap();

        assert_eq!(job.job_id().as_str(), "krishiv-system.sample-batch");
        assert_eq!(job.name(), "sample-batch");
        assert_eq!(job.kind().to_string(), "batch");
        assert_eq!(job.task_count(), 2);
        assert!(job.stages()[0].tasks()[0].description().contains("select"));
    }

    #[test]
    fn rejects_invalid_resource_before_scheduling() {
        let mut resource = sample_resource();
        resource.spec.image = String::from(" ");

        let error = job_spec_from_resource(&resource).unwrap_err();

        assert!(matches!(error, OperatorError::InvalidResource { .. }));
        assert!(error.to_string().contains("spec.image"));
    }

    #[test]
    fn reconcile_waits_for_executors_without_failing_resource() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = Coordinator::active(coordinator_id);

        let outcome = reconciler
            .reconcile(&mut coordinator, &sample_resource())
            .unwrap();

        assert_eq!(outcome.action(), ReconcileAction::WaitingForExecutors);
        assert_eq!(outcome.status().phase, KrishivJobPhase::Accepted);
        assert_eq!(
            outcome.status().conditions[0].status,
            ConditionStatus::False
        );
        assert_eq!(
            outcome.status().conditions[0].reason.as_deref(),
            Some("NoExecutors")
        );
    }

    #[test]
    fn reconcile_submits_job_when_executor_is_available() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();
        let resource = sample_resource();

        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();

        assert_eq!(outcome.action(), ReconcileAction::Submitted);
        assert_eq!(outcome.status().phase, KrishivJobPhase::Running);
        assert_eq!(outcome.status().stages, 1);
        assert_eq!(outcome.status().tasks.assigned, 2);
    }

    #[test]
    fn reconcile_observes_existing_job_on_second_pass() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();
        let resource = sample_resource();

        reconciler.reconcile(&mut coordinator, &resource).unwrap();
        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();

        assert_eq!(outcome.action(), ReconcileAction::Observed);
        assert_eq!(outcome.status().tasks.assigned, 2);
    }

    #[test]
    fn reconcile_status_tracks_running_tasks_after_launch() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();
        let resource = sample_resource();

        reconciler.reconcile(&mut coordinator, &resource).unwrap();
        let job_id = resource.scheduler_job_id();
        coordinator
            .launch_assigned_tasks(&krishiv_proto::JobId::try_new(job_id).unwrap())
            .unwrap();
        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();

        assert_eq!(outcome.action(), ReconcileAction::Observed);
        assert_eq!(outcome.status().tasks.assigned, 0);
        assert_eq!(outcome.status().tasks.running, 2);
    }

    #[test]
    fn reconcile_status_tracks_succeeded_job() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();
        let resource = sample_resource();

        reconciler.reconcile(&mut coordinator, &resource).unwrap();
        let job_id = krishiv_proto::JobId::try_new(resource.scheduler_job_id()).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let stage_id = detail.stages()[0].stage_id().clone();
        let executor_id = ExecutorId::try_new("exec-operator-demo").unwrap();

        for task in detail.stages()[0].tasks() {
            coordinator
                .apply_task_update(TaskStatusUpdate::new(
                    job_id.clone(),
                    stage_id.clone(),
                    task.task_id().clone(),
                    executor_id.clone(),
                    TaskState::Succeeded,
                    1,
                ))
                .unwrap();
        }
        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();

        assert_eq!(outcome.status().phase, KrishivJobPhase::Succeeded);
        assert_eq!(outcome.status().tasks.succeeded, 2);
    }

    #[test]
    fn streaming_resource_maps_to_streaming_job() {
        let mut resource = sample_resource();
        resource.metadata.name = String::from("sample-stream");
        resource.spec.mode = KrishivJobMode::Streaming;
        resource.spec.tasks = 1;

        let job = job_spec_from_resource(&resource).unwrap();

        assert_eq!(job.kind().to_string(), "streaming");
        assert_eq!(job.task_count(), 1);
    }

    #[test]
    fn krishivjob_api_resource_declares_explicit_plural() {
        let resource = krishivjob_api_resource();

        assert_eq!(resource.group, "krishiv.io");
        assert_eq!(resource.version, "v1alpha1");
        assert_eq!(resource.kind, "KrishivJob");
        assert_eq!(resource.plural, "krishivjobs");
    }

    #[test]
    fn converts_dynamic_object_into_typed_resource() {
        let api_resource = krishivjob_api_resource();
        let mut object = DynamicObject::new("sample-batch", &api_resource)
            .within("krishiv-system")
            .data(json!({
                "spec": {
                    "mode": "batch",
                    "image": "ghcr.io/krishiv/krishiv:dev",
                    "tasks": 2,
                    "parallelism": 2,
                    "restartPolicy": "Never"
                }
            }));
        object.metadata.generation = Some(7);

        let resource = resource_from_dynamic_object(&object).unwrap();

        assert_eq!(resource.metadata.name, "sample-batch");
        assert_eq!(
            resource.metadata.namespace.as_deref(),
            Some("krishiv-system")
        );
        assert_eq!(resource.metadata.generation, 7);
        assert_eq!(resource.spec.tasks, 2);
    }

    #[test]
    fn status_patch_wraps_status_subresource() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();
        let resource = sample_resource();

        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();
        let patch = status_patch(outcome.status());

        assert_eq!(patch["status"]["phase"], "Running");
        assert_eq!(patch["status"]["coordinator"], "coord-1");
        assert_eq!(patch["status"]["tasks"]["assigned"], 2);
    }

    #[test]
    fn controller_config_can_scope_or_watch_all_namespaces() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let namespaced =
            KubernetesControllerConfig::namespaced("krishiv-system", coordinator_id.clone());
        let all = KubernetesControllerConfig::all_namespaces(coordinator_id)
            .with_label_selector("app.kubernetes.io/name=krishiv")
            .with_bootstrap_executor(BootstrapExecutor::new("exec-1", "executor", 2));

        assert_eq!(namespaced.namespace(), Some("krishiv-system"));
        assert_eq!(all.namespace(), None);
        assert_eq!(all.coordinator_id().as_str(), "coord-1");
    }

    #[test]
    fn controller_runtime_shares_bootstrap_coordinator_state() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let config = KubernetesControllerConfig::namespaced("krishiv-system", coordinator_id)
            .with_bootstrap_executor(BootstrapExecutor::new("exec-1", "executor", 2));

        let runtime = KubernetesControllerRuntime::new(&config).unwrap();
        let shared = runtime.coordinator();
        let coordinator = shared.read().unwrap();

        assert_eq!(coordinator.coordinator_id().as_str(), "coord-1");
        assert_eq!(coordinator.executor_snapshots().len(), 1);
        assert_eq!(
            coordinator.executor_snapshots()[0].state(),
            ExecutorState::Healthy
        );
        assert_eq!(runtime.reconciler().coordinator_id().as_str(), "coord-1");
    }

    #[test]
    fn demo_coordinator_registers_one_healthy_executor() {
        let coordinator = demo_coordinator(CoordinatorId::try_new("coord-1").unwrap(), 2).unwrap();

        assert_eq!(coordinator.executor_snapshots().len(), 1);
        assert_eq!(
            coordinator.executor_snapshots()[0].state(),
            ExecutorState::Healthy
        );
    }

    #[test]
    fn manual_executor_registration_can_place_job() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let mut coordinator = Coordinator::active(coordinator_id.clone());
        let executor_id = ExecutorId::try_new("exec-1").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(
                executor_id.clone(),
                "executor-pod",
                1,
            ))
            .unwrap();
        coordinator
            .executor_heartbeat(ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy))
            .unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id);

        let outcome = reconciler
            .reconcile(&mut coordinator, &sample_resource())
            .unwrap();

        assert_eq!(outcome.action(), ReconcileAction::Submitted);
    }

    fn sample_resource() -> KrishivJobResource {
        let mut spec = KrishivJobSpec::new(KrishivJobMode::Batch, "ghcr.io/krishiv/krishiv:dev", 2);
        spec.parallelism = Some(2);
        spec.args = vec![
            String::from("sql"),
            String::from("--query"),
            String::from("select 1 as value"),
        ];

        // Resources observed in the reconcile loop already have the finalizer registered.
        let mut meta = ObjectMeta::namespaced("krishiv-system", "sample-batch");
        meta.finalizers = vec![super::FINALIZER.to_string()];

        KrishivJobResource::new(meta, spec)
    }

    #[test]
    fn reconcile_adds_finalizer_on_first_observe() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = Coordinator::active(coordinator_id);

        // Resource without a finalizer triggers FinalizerAdded.
        let mut resource = sample_resource();
        resource.metadata.finalizers.clear();

        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();

        assert_eq!(outcome.action(), ReconcileAction::FinalizerAdded);
    }

    #[test]
    fn reconcile_removes_finalizer_on_deletion() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();

        // Submit the job first so there is something to clean up.
        reconciler
            .reconcile(&mut coordinator, &sample_resource())
            .unwrap();

        // Simulate the resource being deleted (deletion timestamp set).
        let mut resource = sample_resource();
        resource.metadata.deletion_timestamp = Some(String::from("2026-05-18T00:00:00Z"));

        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();

        assert_eq!(outcome.action(), ReconcileAction::FinalizerRemoved);
    }

    #[test]
    fn reconcile_delete_calls_cancel_job_before_removing_finalizer() {
        let coordinator_id = CoordinatorId::try_new("coord-1").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();

        // Submit the job so there is a live scheduler job to cancel.
        reconciler
            .reconcile(&mut coordinator, &sample_resource())
            .unwrap();

        // Confirm job was submitted and is not yet cancelled.
        let job_id = JobId::try_new("krishiv-system.sample-batch").unwrap();
        let snapshot_before = coordinator.job_snapshot(&job_id).unwrap();
        assert_ne!(snapshot_before.state(), JobState::Cancelled);

        // Simulate the resource being deleted.
        let mut resource = sample_resource();
        resource.metadata.deletion_timestamp = Some(String::from("2026-05-18T00:00:00Z"));

        let outcome = reconciler.reconcile(&mut coordinator, &resource).unwrap();

        assert_eq!(outcome.action(), ReconcileAction::FinalizerRemoved);

        // After reconcile on deletion the scheduler job must be cancelled.
        let snapshot_after = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot_after.state(), JobState::Cancelled);
    }

    // --- Slice 7: Operator restart idempotency ---

    #[test]
    fn operator_restart_does_not_duplicate_scheduler_jobs() {
        let coordinator_id = CoordinatorId::try_new("coord-idem").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = demo_coordinator(coordinator_id, 2).unwrap();

        // Resource without a finalizer: first reconcile returns FinalizerAdded.
        let mut resource = sample_resource();
        resource.metadata.finalizers.clear();

        let outcome1 = reconciler.reconcile(&mut coordinator, &resource).unwrap();
        assert_eq!(outcome1.action(), ReconcileAction::FinalizerAdded);
        // No scheduler job created yet.
        assert_eq!(coordinator.job_snapshots().len(), 0);

        // Second reconcile with the finalizer present: returns Submitted.
        let resource_with_finalizer = sample_resource(); // has the finalizer
        let outcome2 = reconciler
            .reconcile(&mut coordinator, &resource_with_finalizer)
            .unwrap();
        assert_eq!(outcome2.action(), ReconcileAction::Submitted);
        assert_eq!(coordinator.job_snapshots().len(), 1);

        // Third reconcile: job already exists, returns Observed — NOT a duplicate submit.
        let outcome3 = reconciler
            .reconcile(&mut coordinator, &resource_with_finalizer)
            .unwrap();
        assert_eq!(outcome3.action(), ReconcileAction::Observed);
        // Still exactly 1 scheduler job — no duplication.
        assert_eq!(coordinator.job_snapshots().len(), 1);
    }

    #[test]
    fn detects_executor_image_pull_failure_from_pod_status() {
        let pod = json!({
            "status": {
                "phase": "Pending",
                "containerStatuses": [{
                    "name": "executor",
                    "state": {
                        "waiting": {
                            "reason": "ImagePullBackOff",
                            "message": "failed to pull image"
                        }
                    }
                }]
            }
        });

        let failure = detect_executor_pod_launch_failure(&pod).unwrap();

        assert_eq!(failure.reason, "ImagePullBackOff");
        assert!(failure.message.contains("failed to pull"));
    }

    #[test]
    fn detects_executor_id_label_on_pod_launch_failure() {
        let pod = json!({
            "metadata": {
                "labels": {
                    EXECUTOR_ID_LABEL: "exec-pod-fail"
                }
            },
            "status": {
                "phase": "Pending",
                "containerStatuses": [{
                    "name": "executor",
                    "state": {
                        "waiting": {
                            "reason": "CreateContainerError",
                            "message": "container could not start"
                        }
                    }
                }]
            }
        });

        let failure = detect_executor_pod_launch_failure(&pod).unwrap();

        assert_eq!(
            failure.executor_id.as_ref().map(ExecutorId::as_str),
            Some("exec-pod-fail")
        );
        assert_eq!(failure.reason, "CreateContainerError");
    }

    #[test]
    fn reconcile_reports_executor_pod_launch_failure_status() {
        let coordinator_id = CoordinatorId::try_new("coord-pod-fail").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = Coordinator::active(coordinator_id);

        let outcome = reconciler
            .reconcile_with_executor_pod_failure(
                &mut coordinator,
                &sample_resource(),
                Some(ExecutorPodLaunchFailure::new(
                    "Unschedulable",
                    "0/3 nodes are available",
                )),
            )
            .unwrap();

        assert_eq!(outcome.action(), ReconcileAction::ExecutorPodLaunchFailed);
        assert_eq!(outcome.status().phase, KrishivJobPhase::Failed);
        assert_eq!(
            outcome.status().conditions[0].condition_type,
            "ExecutorPodReady"
        );
        assert_eq!(
            outcome.status().conditions[0].reason.as_deref(),
            Some("Unschedulable")
        );
    }

    #[test]
    fn reconcile_executor_pod_launch_failure_marks_executor_lost_and_requeues_task() {
        let coordinator_id = CoordinatorId::try_new("coord-pod-requeue").unwrap();
        let reconciler = KrishivJobReconciler::new(coordinator_id.clone());
        let mut coordinator = Coordinator::active(coordinator_id);
        let executor_id = ExecutorId::try_new("exec-launch-fail").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();
        coordinator
            .executor_heartbeat(ExecutorHeartbeat::new(
                executor_id.clone(),
                ExecutorState::Healthy,
            ))
            .unwrap();

        let mut resource = sample_resource();
        resource.spec.tasks = 1;
        let job_id = JobId::try_new(resource.scheduler_job_id()).unwrap();
        coordinator
            .submit_job(job_spec_from_resource(&resource).unwrap())
            .unwrap();
        let assignments = coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap();
        let assignment = assignments.first().unwrap();
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    assignment.job_id().clone(),
                    assignment.stage_id().clone(),
                    assignment.task_id().clone(),
                    executor_id.clone(),
                    TaskState::Running,
                    assignment.attempt_id().as_u32(),
                )
                .with_lease_generation(assignment.lease_generation()),
            )
            .unwrap();

        let outcome = reconciler
            .reconcile_with_executor_pod_failure(
                &mut coordinator,
                &resource,
                Some(
                    ExecutorPodLaunchFailure::new("ImagePullBackOff", "failed to pull image")
                        .with_executor_id(executor_id.clone()),
                ),
            )
            .unwrap();

        assert_eq!(outcome.action(), ReconcileAction::ExecutorPodLaunchFailed);
        assert_eq!(
            coordinator.executor_snapshots()[0].state(),
            ExecutorState::Lost
        );
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        assert_eq!(detail.stages()[0].tasks()[0].state(), TaskState::Assigned);
    }

    // ── R7.1 CrdQueueManager tests ───────────────────────────────────────────

    use super::{CrdQueueManager, KrishivQueue, KrishivQueueSpec, KrishivQueueStatus};
    use krishiv_scheduler::{NamespaceQuotaSnapshot, QueueManager, SubmitOutcome};

    fn make_queue(namespace: &str, max_jobs: usize) -> KrishivQueue {
        KrishivQueue {
            spec: KrishivQueueSpec {
                namespace: namespace.to_owned(),
                cpu_nanos_limit: None,
                memory_bytes_limit: None,
                max_concurrent_jobs: Some(max_jobs),
                priority: 128,
            },
            status: KrishivQueueStatus::default(),
        }
    }

    #[test]
    fn crd_queue_manager_admits_within_limit() {
        let mgr = CrdQueueManager::from_queues([make_queue("team-a", 3)]);
        let spec = JobId::try_new("j").unwrap();
        let job_spec = krishiv_proto::JobSpec::new(spec, "test", krishiv_proto::JobKind::Batch)
            .with_namespace("team-a");
        let quota = NamespaceQuotaSnapshot {
            namespace_id: Some("team-a".to_owned()),
            active_job_count: 2,
            ..Default::default()
        };
        assert_eq!(mgr.admit(&job_spec, &quota), SubmitOutcome::Accepted);
    }

    #[test]
    fn crd_queue_manager_queues_when_namespace_limit_reached() {
        let mgr = CrdQueueManager::from_queues([make_queue("team-b", 1)]);
        let job_spec = krishiv_proto::JobSpec::new(
            JobId::try_new("j2").unwrap(),
            "test",
            krishiv_proto::JobKind::Batch,
        )
        .with_namespace("team-b");
        let quota = NamespaceQuotaSnapshot {
            namespace_id: Some("team-b".to_owned()),
            active_job_count: 1,
            ..Default::default()
        };
        assert!(matches!(
            mgr.admit(&job_spec, &quota),
            SubmitOutcome::Queued { .. }
        ));
    }

    #[test]
    fn crd_queue_manager_admits_unknown_namespace_with_default_policy() {
        let mgr = CrdQueueManager::from_queues([make_queue("team-c", 1)]);
        let job_spec = krishiv_proto::JobSpec::new(
            JobId::try_new("j3").unwrap(),
            "test",
            krishiv_proto::JobKind::Batch,
        );
        // No namespace set — default policy has no limits.
        let quota = NamespaceQuotaSnapshot::default();
        assert_eq!(mgr.admit(&job_spec, &quota), SubmitOutcome::Accepted);
    }

    #[test]
    fn krishiv_queue_derives_correct_quota_policy() {
        let q = make_queue("eng", 5);
        let policy = q.quota_policy();
        assert_eq!(policy.max_concurrent_jobs, Some(5));
        assert!(policy.cpu_nanos_limit.is_none());
    }

    // ── K8sLeaseElection failover tests ───────────────────────────────────

    #[test]
    fn k8s_lease_election_initially_not_leader() {
        let election = K8sLeaseElection::new("job-1", "default", "pod-a");
        assert!(!election.is_leader());
        assert_eq!(election.fencing_token(), 0);
    }

    #[test]
    fn k8s_lease_election_try_acquire_succeeds() {
        let election = K8sLeaseElection::new("job-1", "default", "pod-a");
        assert!(election.try_acquire());
        assert!(election.is_leader());
        assert_eq!(election.fencing_token(), 1);
    }

    #[test]
    fn k8s_lease_election_fencing_token_increments_on_each_acquire() {
        let election = K8sLeaseElection::new("job-1", "default", "pod-a");
        election.try_acquire();
        election.release();
        election.try_acquire();
        assert_eq!(election.fencing_token(), 2);
    }

    #[test]
    fn k8s_lease_election_release_clears_leader() {
        let election = K8sLeaseElection::new("job-1", "default", "pod-a");
        election.try_acquire();
        assert!(election.is_leader());
        election.release();
        assert!(!election.is_leader());
    }

    #[test]
    fn k8s_lease_election_renew_while_leader() {
        let election = K8sLeaseElection::new("job-1", "default", "pod-a");
        election.try_acquire();
        assert!(election.renew());
    }

    #[test]
    fn k8s_lease_election_renew_after_release_fails() {
        let election = K8sLeaseElection::new("job-1", "default", "pod-a");
        election.try_acquire();
        election.release();
        assert!(!election.renew());
    }

    #[test]
    fn failover_stale_coordinator_checkpoint_rejected() {
        // Coordinator A acquires at token=1, commits epoch 1.
        // Coordinator B takes over at token=2.
        // Coordinator A tries to commit epoch 2 with its old token=1 → rejected.
        use krishiv_checkpoint::{CheckpointError, CheckpointMetadata, validate_fencing_token};

        let coord_a = K8sLeaseElection::new("job-failover", "default", "pod-a");
        coord_a.try_acquire(); // token = 1

        let mut meta_epoch1 = CheckpointMetadata {
            version: CheckpointMetadata::VERSION,
            epoch: 1,
            job_id: "job-failover".to_owned(),
            fencing_token: coord_a.fencing_token(), // 1
            timestamp_ms: 0,
            source_offsets: vec![],
            operator_snapshots: vec![],
            is_savepoint: false,
            savepoint_label: None,
        };
        // Epoch 1 commit succeeds (current token = 1, meta token = 1).
        assert!(validate_fencing_token(&meta_epoch1, coord_a.fencing_token()).is_ok());

        // Coordinator A loses the lease; Coordinator B acquires (token = 2).
        coord_a.release();
        let coord_b = K8sLeaseElection::new("job-failover", "default", "pod-b");
        coord_b.try_acquire(); // token = 1 (fresh election handle)
        // Simulate that the global fencing token is now 2 (B's acquire follows A's).
        coord_b.try_acquire(); // token = 2

        // Coordinator A tries to commit epoch 2 with its stale token = 1.
        meta_epoch1.epoch = 2;
        meta_epoch1.fencing_token = 1; // A's old token
        let result = validate_fencing_token(&meta_epoch1, coord_b.fencing_token()); // current=2
        assert!(
            matches!(
                result,
                Err(CheckpointError::StaleFencingToken {
                    stored: 1,
                    current: 2
                })
            ),
            "expected StaleFencingToken, got: {result:?}"
        );
    }
}
