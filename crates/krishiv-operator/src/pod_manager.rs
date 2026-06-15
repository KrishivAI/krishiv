//! Kubernetes executor Pod lifecycle management (BUG-2).
//!
//! The reconciler submits a `KrishivJob` to the in-process scheduler, but the
//! scheduler has no executors registered in Kubernetes mode — jobs sit in
//! `WaitingForExecutors` forever.  This module creates a fleet of executor Pods
//! when a job is first submitted and deletes them when the job is finalised.
//!
//! # Pod naming
//!
//! Each executor pod is named `{job}-exec-{idx}` where `idx` is `0..parallelism`.
//! The pod carries:
//! - Label `krishiv.io/job: {job_name}` for selector-based listing.
//! - Label `krishiv.io/executor-id: {executor_id}` matching the id registered
//!   with the scheduler.
//! - Owner reference pointing to the `KrishivJob` resource so that Kubernetes
//!   garbage-collects pods when the job object is deleted (belt-and-suspenders
//!   alongside the explicit delete in [`PodLifecycleManager::delete_executor_pods`]).

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{
    Container, EnvVar, EnvVarSource, ObjectFieldSelector, Pod, PodSpec, PodTemplateSpec,
    SecretKeySelector,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::Client;
use kube::api::{Api, DeleteParams, ObjectMeta as KubeObjectMeta, PostParams};
use tracing::{info, warn};

use crate::constants::{API_GROUP, API_VERSION, EXECUTOR_ID_LABEL, KIND};
use crate::crd::job::KrishivJobResource;
use crate::error::{OperatorError, OperatorResult};

/// Label applied to every managed executor Pod.
pub const JOB_LABEL: &str = "krishiv.io/job";
/// Label propagated to executors so the scheduler can identify them.
const EXECUTOR_IDX_LABEL: &str = "krishiv.io/executor-idx";
/// Heartbeat interval (seconds) injected into every executor pod.
const EXECUTOR_HEARTBEAT_INTERVAL_SECS: &str = "1";
/// Environment variable passed to the executor process with the coordinator gRPC endpoint.
const ENV_COORDINATOR_ENDPOINT: &str = "KRISHIV_COORDINATOR_ENDPOINT";
/// Environment variable passed to the executor process with its unique executor id.
const ENV_EXECUTOR_ID: &str = "KRISHIV_EXECUTOR_ID";
/// Environment variable that tells the executor which job it is attached to.
const ENV_JOB_ID: &str = "KRISHIV_JOB_ID";
/// Number of task slots advertised by each executor Pod.
const ENV_TASK_SLOTS: &str = "KRISHIV_TASK_SLOTS";
/// Pod IP used by executors when advertising task/barrier gRPC endpoints.
const ENV_POD_IP: &str = "POD_IP";
/// Opt-in fail-closed switch for executor task-control gRPC auth.
const ENV_REQUIRE_EXECUTOR_TASK_AUTH: &str = "KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH";
/// Bearer token used by executors when calling coordinator gRPC.
const ENV_COORDINATOR_BEARER_TOKEN: &str = "KRISHIV_COORDINATOR_BEARER_TOKEN";
/// Bearer token consumed by executor task-control gRPC auth.
const ENV_EXECUTOR_TASK_BEARER_TOKEN: &str = "KRISHIV_EXECUTOR_TASK_BEARER_TOKEN";
/// Secret name used for the coordinator gRPC bearer token.
const ENV_COORDINATOR_AUTH_SECRET_NAME: &str = "KRISHIV_COORDINATOR_AUTH_SECRET_NAME";
/// Secret key used for the coordinator gRPC bearer token.
const ENV_COORDINATOR_AUTH_SECRET_KEY: &str = "KRISHIV_COORDINATOR_AUTH_SECRET_KEY";
/// Secret name used for the executor task-control bearer token.
const ENV_EXECUTOR_TASK_AUTH_SECRET_NAME: &str = "KRISHIV_EXECUTOR_TASK_AUTH_SECRET_NAME";
/// Secret key used for the executor task-control bearer token.
const ENV_EXECUTOR_TASK_AUTH_SECRET_KEY: &str = "KRISHIV_EXECUTOR_TASK_AUTH_SECRET_KEY";
const DEFAULT_COORDINATOR_AUTH_SECRET_NAME: &str = "krishiv-coordinator-auth";
const DEFAULT_COORDINATOR_AUTH_SECRET_KEY: &str = "token";
const DEFAULT_EXECUTOR_TASK_AUTH_SECRET_NAME: &str = "krishiv-executor-task-auth";
const DEFAULT_EXECUTOR_TASK_AUTH_SECRET_KEY: &str = "token";

/// Explicit allow-list of environment variable names that may be injected from
/// `KrishivJobResource.spec.args` into executor Pods.  All other keys are
/// silently ignored to prevent arbitrary env-var injection (security hardening).
const ALLOWED_EXECUTOR_ENV_VARS: &[&str] = &[
    "KRISHIV_HEARTBEAT_INTERVAL_SECS",
    "KRISHIV_HTTP_ADDR",
    "KRISHIV_TASK_GRPC_ADDR",
    "KRISHIV_BARRIER_GRPC_ADDR",
    "KRISHIV_SHUFFLE_DIR",
    "KRISHIV_SHUFFLE_URI",
    "KRISHIV_STATE_DIR",
    "KRISHIV_CHECKPOINT_STORAGE",
    "KRISHIV_DURABILITY_PROFILE",
    "KAFKA_BOOTSTRAP_SERVERS",
];

/// Manages the lifecycle of executor Pods spawned for a `KrishivJob`.
#[derive(Clone)]
pub struct PodLifecycleManager {
    client: Client,
    /// gRPC endpoint of the coordinator (e.g. `http://krishiv-coordinator:9090`).
    coordinator_endpoint: String,
}

impl PodLifecycleManager {
    pub fn new(client: Client, coordinator_endpoint: impl Into<String>) -> Self {
        Self {
            client,
            coordinator_endpoint: coordinator_endpoint.into(),
        }
    }

    /// Create `parallelism` executor Pods for `resource` and return the executor
    /// ids registered with each pod.
    ///
    /// Pod creation is best-effort: a pod that already exists is left untouched
    /// (`409 Conflict` is ignored).  Any other creation error is propagated.
    pub async fn create_executor_pods(
        &self,
        resource: &KrishivJobResource,
    ) -> OperatorResult<Vec<String>> {
        let namespace = resource.metadata.namespace_or_default();
        let job_name = &resource.metadata.name;
        let parallelism = resource.spec.effective_parallelism();
        let job_id = resource.metadata.scheduler_job_id();

        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let mut executor_ids = Vec::with_capacity(parallelism);

        for idx in 0..parallelism {
            let executor_id = format!("{job_name}-exec-{idx}");
            let pod_name = format!("{job_name}-exec-{idx}");

            let pod = self.build_pod(resource, &pod_name, &executor_id, idx, &job_id);

            match pods.create(&PostParams::default(), &pod).await {
                Ok(_) => {
                    info!(
                        job = job_name,
                        pod = pod_name,
                        executor_id,
                        "created executor pod"
                    );
                    executor_ids.push(executor_id);
                }
                Err(kube::Error::Api(err)) if err.code == 409 => {
                    info!(
                        job = job_name,
                        pod = pod_name,
                        "executor pod already exists, skipping"
                    );
                    executor_ids.push(executor_id);
                }
                Err(err) => {
                    return Err(OperatorError::Kubernetes {
                        message: format!(
                            "failed to create executor pod {pod_name} for job {job_name}: {err}"
                        ),
                    });
                }
            }
        }

        Ok(executor_ids)
    }

    /// Inspect executor pods for a job and return the first classified launch failure.
    pub async fn detect_executor_pod_launch_failure(
        &self,
        resource: &KrishivJobResource,
    ) -> Option<crate::pod_failure::ExecutorPodLaunchFailure> {
        let namespace = resource.metadata.namespace_or_default();
        let job_name = &resource.metadata.name;
        let parallelism = resource.spec.effective_parallelism();
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);

        for idx in 0..parallelism {
            let pod_name = format!("{job_name}-exec-{idx}");
            let pod = match pods.get(&pod_name).await {
                Ok(pod) => pod,
                Err(kube::Error::Api(err)) if err.code == 404 => continue,
                Err(err) => {
                    warn!(
                        job = job_name,
                        pod = pod_name,
                        error = %err,
                        "failed to read executor pod status during launch-failure detection"
                    );
                    continue;
                }
            };
            let value = match serde_json::to_value(&pod) {
                Ok(value) => value,
                Err(err) => {
                    warn!(
                        job = job_name,
                        pod = pod_name,
                        error = %err,
                        "failed to serialize executor pod for launch-failure detection"
                    );
                    continue;
                }
            };
            if let Some(failure) = crate::pod_failure::detect_executor_pod_launch_failure(&value) {
                return Some(failure);
            }
        }
        None
    }

    /// Delete all executor Pods associated with `resource`.
    ///
    /// Pods are selected by the `krishiv.io/job` label.  Missing pods (`404`)
    /// are ignored — this method is idempotent.
    pub async fn delete_executor_pods(&self, resource: &KrishivJobResource) -> OperatorResult<()> {
        let namespace = resource.metadata.namespace_or_default();
        let job_name = &resource.metadata.name;
        let parallelism = resource.spec.effective_parallelism();
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);

        for idx in 0..parallelism {
            let pod_name = format!("{job_name}-exec-{idx}");
            match pods.delete(&pod_name, &DeleteParams::default()).await {
                Ok(_) => {
                    info!(job = job_name, pod = pod_name, "deleted executor pod");
                }
                Err(kube::Error::Api(err)) if err.code == 404 => {
                    // Already gone — not an error.
                }
                Err(err) => {
                    warn!(
                        job = job_name,
                        pod = pod_name,
                        error = %err,
                        "failed to delete executor pod (best-effort)"
                    );
                }
            }
        }
        Ok(())
    }

    pub(crate) fn build_pod(
        &self,
        resource: &KrishivJobResource,
        pod_name: &str,
        executor_id: &str,
        idx: usize,
        job_id: &str,
    ) -> Pod {
        build_executor_pod(
            resource,
            pod_name,
            executor_id,
            idx,
            job_id,
            &self.coordinator_endpoint,
        )
    }
}

/// Build an executor Pod for a `KrishivJobResource` without a live kube client.
///
/// Extracted from `PodLifecycleManager::build_pod` so it can be tested without
/// constructing a `kube::Client`.
pub(crate) fn build_executor_pod(
    resource: &KrishivJobResource,
    pod_name: &str,
    executor_id: &str,
    idx: usize,
    job_id: &str,
    coordinator_endpoint: &str,
) -> Pod {
    let job_name = &resource.metadata.name;
    let namespace = resource.metadata.namespace_or_default().to_owned();

    let mut labels: BTreeMap<String, String> = resource.spec.labels.clone();
    labels.insert(JOB_LABEL.to_owned(), job_name.clone());
    labels.insert(EXECUTOR_IDX_LABEL.to_owned(), idx.to_string());
    labels.insert(EXECUTOR_ID_LABEL.to_owned(), executor_id.to_owned());

    // Owner reference — Kubernetes will GC the pod when the KrishivJob is deleted.
    // Only set owner references when a UID is available; an empty UID causes
    // the Kubernetes API to reject the owner reference.
    let owner_refs = resource
        .metadata
        .uid
        .as_deref()
        .map(|uid| {
            vec![OwnerReference {
                api_version: format!("{API_GROUP}/{API_VERSION}"),
                kind: KIND.to_owned(),
                name: job_name.clone(),
                uid: uid.to_owned(),
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]
        })
        .unwrap_or_default();

    let mut env_vars = vec![
        pod_ip_env_var(),
        EnvVar {
            name: ENV_COORDINATOR_ENDPOINT.to_owned(),
            value: Some(coordinator_endpoint.to_owned()),
            ..Default::default()
        },
        EnvVar {
            name: ENV_EXECUTOR_ID.to_owned(),
            value: Some(executor_id.to_owned()),
            ..Default::default()
        },
        EnvVar {
            name: ENV_JOB_ID.to_owned(),
            value: Some(job_id.to_owned()),
            ..Default::default()
        },
        EnvVar {
            name: ENV_TASK_SLOTS.to_owned(),
            value: Some("1".to_owned()),
            ..Default::default()
        },
    ];

    // Allow the job spec to inject additional environment variables via args
    // that follow the KEY=VALUE convention (executor interprets them on start).
    // Only keys listed in ALLOWED_EXECUTOR_ENV_VARS are forwarded; everything
    // else is silently ignored to prevent arbitrary env-var injection.
    for arg in &resource.spec.args {
        if let Some((k, v)) = arg.split_once('=')
            && !k.is_empty()
            && ALLOWED_EXECUTOR_ENV_VARS.contains(&k)
        {
            env_vars.push(EnvVar {
                name: k.to_owned(),
                value: Some(v.to_owned()),
                ..Default::default()
            });
        }
    }
    env_vars.push(executor_task_auth_required_env_var());
    env_vars.push(coordinator_bearer_token_env_var());
    env_vars.push(executor_task_bearer_token_env_var());

    let container = Container {
        name: "executor".to_owned(),
        image: Some(resource.spec.image.clone()),
        command: if resource.spec.entrypoint.is_empty() {
            None
        } else {
            Some(resource.spec.entrypoint.clone())
        },
        args: Some(executor_args(coordinator_endpoint)),
        env: Some(env_vars),
        ..Default::default()
    };

    // Map the CRD restart policy to the Kubernetes Pod restart policy.
    let restart_policy = match resource.spec.restart_policy {
        crate::crd::job::RestartPolicy::Never => "Never".to_owned(),
        crate::crd::job::RestartPolicy::OnFailure => "OnFailure".to_owned(),
    };

    Pod {
        metadata: KubeObjectMeta {
            name: Some(pod_name.to_owned()),
            namespace: Some(namespace),
            labels: Some(labels),
            owner_references: Some(owner_refs),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![container],
            restart_policy: Some(restart_policy),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Build a `PodTemplateSpec` from a `KrishivJobResource` without a live kube client.
///
/// Used by the admission webhook and dry-run tooling where a full
/// `PodLifecycleManager` is not available.  The template uses index 0 for the
/// executor-id label since a concrete index is not known at template-build time;
/// callers that create multiple pods should use `build_executor_pod` directly.
pub fn build_executor_pod_template(
    resource: &KrishivJobResource,
    coordinator_endpoint: &str,
) -> PodTemplateSpec {
    let job_name = &resource.metadata.name;
    let executor_id = format!("{job_name}-exec-0");
    let job_id = resource.metadata.scheduler_job_id();

    let mut labels: BTreeMap<String, String> = resource.spec.labels.clone();
    labels.insert(JOB_LABEL.to_owned(), job_name.clone());
    labels.insert(EXECUTOR_ID_LABEL.to_owned(), executor_id.clone());

    let container = Container {
        name: "executor".to_owned(),
        image: Some(resource.spec.image.clone()),
        command: if resource.spec.entrypoint.is_empty() {
            None
        } else {
            Some(resource.spec.entrypoint.clone())
        },
        args: Some(executor_args(coordinator_endpoint)),
        env: Some(vec![
            pod_ip_env_var(),
            EnvVar {
                name: ENV_COORDINATOR_ENDPOINT.to_owned(),
                value: Some(coordinator_endpoint.to_owned()),
                ..Default::default()
            },
            EnvVar {
                name: ENV_EXECUTOR_ID.to_owned(),
                value: Some(executor_id),
                ..Default::default()
            },
            EnvVar {
                name: ENV_JOB_ID.to_owned(),
                value: Some(job_id),
                ..Default::default()
            },
            EnvVar {
                name: ENV_TASK_SLOTS.to_owned(),
                value: Some("1".to_owned()),
                ..Default::default()
            },
            executor_task_auth_required_env_var(),
            coordinator_bearer_token_env_var(),
            executor_task_bearer_token_env_var(),
        ]),
        ..Default::default()
    };

    PodTemplateSpec {
        metadata: Some(KubeObjectMeta {
            labels: Some(labels),
            ..Default::default()
        }),
        spec: Some(PodSpec {
            containers: vec![container],
            restart_policy: Some(match resource.spec.restart_policy {
                crate::crd::job::RestartPolicy::Never => "Never".to_owned(),
                crate::crd::job::RestartPolicy::OnFailure => "OnFailure".to_owned(),
            }),
            ..Default::default()
        }),
    }
}

fn executor_args(coordinator_endpoint: &str) -> Vec<String> {
    vec![
        "executor".to_owned(),
        "--coordinator".to_owned(),
        coordinator_endpoint.to_owned(),
        "--connect".to_owned(),
        "--heartbeat-interval-secs".to_owned(),
        EXECUTOR_HEARTBEAT_INTERVAL_SECS.to_owned(),
    ]
}

fn pod_ip_env_var() -> EnvVar {
    EnvVar {
        name: ENV_POD_IP.to_owned(),
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                api_version: Some("v1".to_owned()),
                field_path: "status.podIP".to_owned(),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn executor_task_auth_required_env_var() -> EnvVar {
    EnvVar {
        name: ENV_REQUIRE_EXECUTOR_TASK_AUTH.to_owned(),
        value: Some("true".to_owned()),
        ..Default::default()
    }
}

fn coordinator_bearer_token_env_var() -> EnvVar {
    secret_env_var(
        ENV_COORDINATOR_BEARER_TOKEN,
        coordinator_auth_secret_name(),
        coordinator_auth_secret_key(),
    )
}

fn executor_task_bearer_token_env_var() -> EnvVar {
    secret_env_var(
        ENV_EXECUTOR_TASK_BEARER_TOKEN,
        executor_task_auth_secret_name(),
        executor_task_auth_secret_key(),
    )
}

fn secret_env_var(name: &str, secret_name: String, secret_key: String) -> EnvVar {
    EnvVar {
        name: name.to_owned(),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: secret_name,
                key: secret_key,
                optional: Some(false),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn coordinator_auth_secret_name() -> String {
    std::env::var(ENV_COORDINATOR_AUTH_SECRET_NAME)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_COORDINATOR_AUTH_SECRET_NAME.to_owned())
}

fn coordinator_auth_secret_key() -> String {
    std::env::var(ENV_COORDINATOR_AUTH_SECRET_KEY)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_COORDINATOR_AUTH_SECRET_KEY.to_owned())
}

fn executor_task_auth_secret_name() -> String {
    std::env::var(ENV_EXECUTOR_TASK_AUTH_SECRET_NAME)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_EXECUTOR_TASK_AUTH_SECRET_NAME.to_owned())
}

fn executor_task_auth_secret_key() -> String {
    std::env::var(ENV_EXECUTOR_TASK_AUTH_SECRET_KEY)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_EXECUTOR_TASK_AUTH_SECRET_KEY.to_owned())
}
