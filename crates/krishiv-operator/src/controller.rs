//! Controller runtime.
use futures::StreamExt;
use krishiv_proto::{
    CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState,
};
use krishiv_scheduler::{Coordinator, SharedCoordinator};
use kube::Client;
use kube::api::{Api, ListParams};
use kube::core::DynamicObject;
use kube::runtime::watcher::{self, Event as WatchEvent};

use crate::dynamic::{krishivjob_api, resource_from_dynamic_object};
use crate::dynamic::{
    patch_krishivjob_finalizer, patch_krishivjob_status, remove_krishivjob_finalizer,
};
use crate::error::{OperatorError, OperatorResult};
use crate::pod_manager::PodLifecycleManager;
use crate::reconciler::*;
use crate::status::KrishivJobStatus;
use krishiv_scheduler::SchedulerError;

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
    /// Coordinator gRPC endpoint injected into executor pod env vars.
    ///
    /// Defaults to `http://krishiv-coordinator:9090` if not set.
    coordinator_endpoint: String,
    /// Optional durable metadata store path (Redb).
    metadata_path: Option<std::path::PathBuf>,
    /// Durability profile for coordinator metadata writes.
    durability_profile: krishiv_common::DurabilityProfile,
}

/// Runtime state owned by the live R2 Kubernetes controller process.
#[derive(Clone)]
pub struct KubernetesControllerRuntime {
    coordinator: SharedCoordinator,
    reconciler: KrishivJobReconciler,
    /// Pod lifecycle manager — `None` when running without a Kubernetes client
    /// (e.g. in unit tests that use the in-memory reconciler only).
    pod_manager: Option<PodLifecycleManager>,
}

impl std::fmt::Debug for KubernetesControllerRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubernetesControllerRuntime")
            .field("coordinator", &self.coordinator)
            .field("reconciler", &self.reconciler)
            .field("pod_manager", &self.pod_manager.is_some())
            .finish()
    }
}

impl KubernetesControllerRuntime {
    /// Create an active coordinator runtime from controller config.
    pub fn new(config: &KubernetesControllerConfig) -> OperatorResult<Self> {
        let mut coordinator = Coordinator::active(config.coordinator_id.clone())
            .with_durability_profile(config.durability_profile);
        if let Some(executor) = &config.bootstrap_executor {
            register_bootstrap_executor(&mut coordinator, executor)?;
        }

        if let Some(path) = &config.metadata_path {
            let store = krishiv_scheduler::RocksDbMetadataStore::open(path).map_err(|error| {
                OperatorError::InvalidResource {
                    message: format!(
                        "failed to open metadata store at {}: {error}",
                        path.display()
                    ),
                }
            })?;
            coordinator
                .recover_from_store(&store)
                .map_err(OperatorError::from)?;
            let fail_closed =
                krishiv_common::profile_requires_fail_closed_metadata(config.durability_profile);
            coordinator = coordinator.with_store_fail_closed(store, fail_closed);
        }

        let shared =
            SharedCoordinator::new(coordinator).with_durability_profile(config.durability_profile);
        if krishiv_common::profile_requires_fail_closed_metadata(config.durability_profile) {
            shared.sync_leader_fencing_token(1);
        }

        Ok(Self {
            coordinator: shared,
            reconciler: KrishivJobReconciler::new(config.coordinator_id.clone()),
            pod_manager: None,
        })
    }

    /// Attach a pod lifecycle manager — called by the live controller before
    /// starting the watch loop so that submitted jobs get executor pods.
    pub fn with_pod_manager(mut self, manager: PodLifecycleManager) -> Self {
        self.pod_manager = Some(manager);
        self
    }

    /// Shared coordinator handle used by the controller and status server.
    pub fn coordinator(&self) -> SharedCoordinator {
        self.coordinator.clone()
    }

    /// Reconciler bound to the active coordinator id.
    pub fn reconciler(&self) -> &KrishivJobReconciler {
        &self.reconciler
    }

    /// Pod lifecycle manager, if one has been configured.
    pub fn pod_manager(&self) -> Option<&PodLifecycleManager> {
        self.pod_manager.as_ref()
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
            coordinator_endpoint: "http://krishiv-coordinator:9090".to_owned(),
            metadata_path: std::env::var("KRISHIV_METADATA_PATH")
                .ok()
                .map(std::path::PathBuf::from),
            durability_profile: krishiv_common::resolve_durability_profile(),
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
            coordinator_endpoint: "http://krishiv-coordinator:9090".to_owned(),
            metadata_path: std::env::var("KRISHIV_METADATA_PATH")
                .ok()
                .map(std::path::PathBuf::from),
            durability_profile: krishiv_common::resolve_durability_profile(),
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

    /// gRPC endpoint injected into executor pod environment variables.
    pub fn coordinator_endpoint(&self) -> &str {
        &self.coordinator_endpoint
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

    /// Override the coordinator endpoint injected into executor pods.
    #[must_use]
    pub fn with_coordinator_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.coordinator_endpoint = endpoint.into();
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
    let pod_manager = PodLifecycleManager::new(client.clone(), config.coordinator_endpoint());
    let runtime = KubernetesControllerRuntime::new(&config)?.with_pod_manager(pod_manager);
    // Non-HA path: no external leader-election loop manages orchestration, so
    // we own the handles here and shut them down when the watcher exits.
    let _orchestration = runtime.coordinator.spawn_orchestration_loops();
    run_kubernetes_controller_runtime_with_client(client, config, runtime).await
}

/// Run the live Kubernetes controller with an explicit shared runtime.
///
/// Orchestration loops are **not** started here; the caller is responsible for
/// managing them.  In the non-HA path, `run_kubernetes_controller_with_client`
/// owns the handles.  In the HA path, `spawn_coordinator_leader_election` in
/// `main.rs` starts and stops loops tied to lease ownership.
pub async fn run_kubernetes_controller_runtime_with_client(
    client: Client,
    config: KubernetesControllerConfig,
    runtime: KubernetesControllerRuntime,
) -> OperatorResult<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let jobs = krishivjob_api(client, config.namespace())?;
    let watcher_config = watcher_config(&config);

    let status_jobs = jobs.clone();
    let status_runtime = runtime.clone();
    let mut status_shutdown_rx = shutdown_rx.clone();
    let status_handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    match status_jobs.list(&ListParams::default()).await {
                        Ok(resources) => {
                            for object in resources {
                                if let Err(error) = reconcile_dynamic_object_with_runtime(
                                    &status_jobs,
                                    &status_runtime,
                                    object,
                                ).await {
                                    tracing::warn!(error = %error, "periodic KrishivJob status reconcile failed");
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(error = %error, "failed to list KrishivJob resources for status reconcile");
                        }
                    }
                }
                _ = status_shutdown_rx.changed() => { if *status_shutdown_rx.borrow() { return; } }
            }
        }
    });

    let mut events = watcher::watcher(jobs.clone(), watcher_config).boxed();
    while let Some(event) = events.next().await {
        match event? {
            WatchEvent::Apply(object) | WatchEvent::InitApply(object) => {
                if let Err(e) = reconcile_dynamic_object_with_runtime(&jobs, &runtime, object).await
                {
                    if matches!(
                        e,
                        OperatorError::Scheduler(SchedulerError::InactiveCoordinator { .. })
                    ) {
                        tracing::debug!(
                            "skipping reconcile because coordinator is inactive (standby mode)"
                        );
                    } else {
                        return Err(e);
                    }
                }
            }
            WatchEvent::Delete(_) | WatchEvent::Init | WatchEvent::InitDone => {}
        }
    }

    let _ = shutdown_tx.send(true);
    status_handle.abort();

    Ok(())
}

/// Reconcile one Kubernetes dynamic object using a shared controller runtime.
///
/// # Pod lifecycle
///
/// - `ReconcileAction::Submitted`: executor Pods are created so the scheduler
///   gets healthy executors to assign tasks to (BUG-2 fix).
/// - `ReconcileAction::FinalizerRemoved`: executor Pods are deleted after the
///   scheduler job is cancelled.
pub async fn reconcile_dynamic_object_with_runtime(
    jobs: &Api<DynamicObject>,
    runtime: &KubernetesControllerRuntime,
    object: DynamicObject,
) -> OperatorResult<KubernetesReconcileReport> {
    let resource = resource_from_dynamic_object(&object)?;
    let mut outcome = {
        let mut coordinator = runtime.coordinator.write().await;
        runtime.reconciler.reconcile(&mut coordinator, &resource)?
    };
    let job_id = crate::reconciler::scheduler_job_id(&resource).ok();

    let remove_finalizer = outcome.action() == ReconcileAction::FinalizerRemoved;
    match outcome.action() {
        ReconcileAction::FinalizerAdded => {
            patch_krishivjob_finalizer(jobs, &resource).await?;
        }
        ReconcileAction::Submitted => {
            if let Some(ref job_id) = job_id {
                // BUG-2: Create executor Pods so the scheduler has executors to
                // assign tasks to.  Without pods, submitted jobs stay permanently
                // in the WaitingForExecutors state.
                if let Some(pod_manager) = runtime.pod_manager() {
                    match pod_manager.create_executor_pods(&resource).await {
                        Ok(executor_ids) => {
                            tracing::info!(
                                job_id = %job_id,
                                pod_count = executor_ids.len(),
                                "created executor pods for submitted job"
                            );
                            if let Some(failure) = pod_manager
                                .detect_executor_pod_launch_failure(&resource)
                                .await
                            {
                                let mut coordinator = runtime.coordinator.write().await;
                                outcome = runtime.reconciler.reconcile_with_executor_pod_failure(
                                    &mut coordinator,
                                    &resource,
                                    Some(failure),
                                )?;
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                job_id = %job_id,
                                error = %err,
                                "executor pod creation failed; job will remain WaitingForExecutors"
                            );
                        }
                    }
                }
                runtime
                    .reconciler
                    .ensure_dedicated_job_loop(job_id, resource.spec.dedicated_coordinator);
                if resource.spec.dedicated_coordinator {
                    tracing::info!(
                        job_id = %job_id,
                        "dedicated in-process JCP bookkeeping enabled"
                    );
                }
            }
        }
        ReconcileAction::FinalizerRemoved => {
            // Clean up executor Pods after the scheduler job is cancelled so
            // orphaned pods don't linger in the namespace.
            if let Some(pod_manager) = runtime.pod_manager()
                && let Err(err) = pod_manager.delete_executor_pods(&resource).await
            {
                tracing::warn!(
                    job = resource.metadata.name,
                    error = %err,
                    "executor pod deletion failed (best-effort)"
                );
            }
        }
        _ => {}
    }

    patch_krishivjob_status(jobs, &resource, outcome.status()).await?;
    if remove_finalizer {
        remove_krishivjob_finalizer(jobs, &resource).await?;
    }

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
    if outcome.action() == ReconcileAction::FinalizerAdded {
        patch_krishivjob_finalizer(jobs, &resource).await?;
    }
    patch_krishivjob_status(jobs, &resource, outcome.status()).await?;
    if outcome.action() == ReconcileAction::FinalizerRemoved {
        remove_krishivjob_finalizer(jobs, &resource).await?;
    }

    Ok(KubernetesReconcileReport {
        namespace: resource.metadata.namespace_or_default().to_owned(),
        name: resource.metadata.name,
        action: outcome.action(),
        status: outcome.status().clone(),
    })
}

fn watcher_config(config: &KubernetesControllerConfig) -> kube::runtime::watcher::Config {
    kube::runtime::watcher::Config {
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

    let executor_id = ExecutorId::try_new(executor.executor_id.clone()).map_err(|e| {
        OperatorError::InvalidResource {
            message: e.to_string(),
        }
    })?;
    coordinator.register_executor(ExecutorDescriptor::new(
        executor_id.clone(),
        executor.host.clone(),
        executor.slots,
    ))?;
    coordinator.executor_heartbeat(ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy))?;
    Ok(())
}
