//! Controller runtime.

use crate::jcp_pod;
use futures::StreamExt;
use krishiv_proto::{
    CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState,
};
use krishiv_scheduler::{Coordinator, SharedCoordinator};
use kube::Client;
use kube::api::Api;
use kube::core::DynamicObject;
use kube::runtime::watcher::{self, Event as WatchEvent};

use crate::dynamic::patch_krishivjob_status;
use crate::dynamic::{krishivjob_api, resource_from_dynamic_object};
use crate::error::{OperatorError, OperatorResult};
use crate::reconciler::*;
use crate::status::KrishivJobStatus;

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
    let tick_coordinator = runtime.coordinator.clone();
    let tick_period_ms = {
        let coord = tick_coordinator.read().await;
        coord.config().tick_period_ms()
    };
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(tick_period_ms));
        loop {
            ticker.tick().await;
            let mut coord = tick_coordinator.write().await;
            if let Err(e) = coord.coordinator_tick() {
                tracing::warn!(error = %e, "embedded coordinator tick failed");
            }
        }
    });

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
    let (outcome, job_id) = {
        let mut coordinator = runtime.coordinator.write().await;
        let job_id = crate::reconciler::scheduler_job_id(&resource).ok();
        let outcome = runtime.reconciler.reconcile(&mut coordinator, &resource)?;
        (outcome, job_id)
    };
    if matches!(outcome.action(), ReconcileAction::Submitted)
        && let Some(job_id) = job_id
    {
        runtime.reconciler.ensure_dedicated_job_loop(
            &runtime.coordinator,
            &job_id,
            resource.spec.dedicated_coordinator,
        );
        if resource.spec.dedicated_coordinator {
            tracing::info!(
                job_id = %job_id,
                jcp_pod = %jcp_pod::jcp_pod_name(&job_id),
                "dedicated JCP orchestration enabled (see k8s/manifests/jcp-pod-template.yaml)"
            );
        }
    }
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
