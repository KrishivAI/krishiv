#![forbid(unsafe_code)]

//! Public facade for `krishiv-operator`.

pub mod constants;
#[cfg(feature = "k8s")]
pub mod controller;
pub mod crd;
#[cfg(feature = "k8s")]
pub mod dynamic;
pub mod error;
pub mod jcp_pod;
#[cfg(feature = "k8s")]
pub mod lease;
pub mod pod_failure;
pub mod queue_manager;
pub mod reconciler;
pub mod status;

#[cfg(all(test, feature = "k8s"))]
mod tests;

pub use constants::{API_GROUP, API_VERSION, EXECUTOR_ID_LABEL, FIELD_MANAGER, FINALIZER, KIND};
#[cfg(feature = "k8s")]
pub use controller::{
    KubernetesControllerConfig, KubernetesControllerRuntime, KubernetesReconcileReport,
    reconcile_dynamic_object, reconcile_dynamic_object_with_runtime, run_kubernetes_controller,
    run_kubernetes_controller_runtime_with_client, run_kubernetes_controller_with_client,
};
pub use crd::job::{KrishivJobMode, KrishivJobResource, KrishivJobSpec, ObjectMeta, RestartPolicy};
#[cfg(feature = "k8s")]
pub use dynamic::{krishivjob_api, krishivjob_api_resource, resource_from_dynamic_object};
pub use error::{OperatorError, OperatorResult};
#[cfg(feature = "k8s")]
pub use lease::K8sLeaseElection;
pub use pod_failure::{ExecutorPodLaunchFailure, detect_executor_pod_launch_failure};
pub use queue_manager::{
    CrdQueueManager, KrishivQueue, KrishivQueueSpec, KrishivQueueStatus, QUEUE_KIND,
};
pub use reconciler::{
    BootstrapExecutor, KrishivJobReconciler, ReconcileAction, ReconcileOutcome, demo_coordinator,
    job_spec_from_resource,
};
pub use status::{
    ConditionStatus, JobCondition, KrishivJobPhase, KrishivJobStatus, TaskStatusCounters,
    patch_krishivjob_finalizer, patch_krishivjob_status, status_patch,
};
