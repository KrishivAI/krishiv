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
    Container, EnvVar, Pod, PodSpec, PodTemplateSpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{Api, DeleteParams, ObjectMeta as KubeObjectMeta, PostParams};
use kube::Client;
use tracing::{info, warn};

use crate::constants::{API_GROUP, API_VERSION, EXECUTOR_ID_LABEL, KIND};
use crate::crd::job::KrishivJobResource;
use crate::error::{OperatorError, OperatorResult};

/// Label applied to every managed executor Pod.
pub const JOB_LABEL: &str = "krishiv.io/job";
/// Label propagated to executors so the scheduler can identify them.
const EXECUTOR_IDX_LABEL: &str = "krishiv.io/executor-idx";
/// Environment variable passed to the executor process with the coordinator gRPC endpoint.
const ENV_COORDINATOR_ENDPOINT: &str = "KRISHIV_COORDINATOR_ENDPOINT";
/// Environment variable passed to the executor process with its unique executor id.
const ENV_EXECUTOR_ID: &str = "KRISHIV_EXECUTOR_ID";
/// Environment variable that tells the executor which job it is attached to.
const ENV_JOB_ID: &str = "KRISHIV_JOB_ID";
/// Number of task slots advertised by each executor Pod.
const ENV_TASK_SLOTS: &str = "KRISHIV_TASK_SLOTS";

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
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);

        let mut executor_ids = Vec::with_capacity(parallelism);
        for idx in 0..parallelism {
            let executor_id = format!("{job_name}-exec-{idx}");
            let pod_name = format!("{job_name}-exec-{idx}");
            let job_id = resource.metadata.scheduler_job_id();

            let pod = self.build_pod(
                resource,
                &pod_name,
                &executor_id,
                idx,
                &job_id,
            );

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
                    // Pod already exists — idempotent, still track the executor id.
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

    /// Delete all executor Pods associated with `resource`.
    ///
    /// Pods are selected by the `krishiv.io/job` label.  Missing pods (`404`)
    /// are ignored — this method is idempotent.
    pub async fn delete_executor_pods(
        &self,
        resource: &KrishivJobResource,
    ) -> OperatorResult<()> {
        let namespace = resource.metadata.namespace_or_default();
        let job_name = &resource.metadata.name;
        let parallelism = resource.spec.effective_parallelism();
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);

        for idx in 0..parallelism {
            let pod_name = format!("{job_name}-exec-{idx}");
            match pods
                .delete(&pod_name, &DeleteParams::default())
                .await
            {
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

    fn build_pod(
        &self,
        resource: &KrishivJobResource,
        pod_name: &str,
        executor_id: &str,
        idx: usize,
        job_id: &str,
    ) -> Pod {
        let job_name = &resource.metadata.name;
        let namespace = resource.metadata.namespace_or_default().to_owned();

        let mut labels: BTreeMap<String, String> = resource.spec.labels.clone();
        labels.insert(JOB_LABEL.to_owned(), job_name.clone());
        labels.insert(EXECUTOR_IDX_LABEL.to_owned(), idx.to_string());
        labels.insert(EXECUTOR_ID_LABEL.to_owned(), executor_id.to_owned());

        // Owner reference — Kubernetes will GC the pod when the KrishivJob is deleted.
        let owner_refs = resource
            .metadata
            .generation
            .checked_abs()
            .map(|_| {
                vec![OwnerReference {
                    api_version: format!("{API_GROUP}/{API_VERSION}"),
                    kind: KIND.to_owned(),
                    name: job_name.clone(),
                    uid: String::new(), // UID not available in our CRD mock; leave empty.
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]
            })
            .unwrap_or_default();

        let mut env_vars = vec![
            EnvVar {
                name: ENV_COORDINATOR_ENDPOINT.to_owned(),
                value: Some(self.coordinator_endpoint.clone()),
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
        // (Full env-var injection would require a CRD schema extension; this is
        // a minimal path that avoids a breaking change to the CRD spec.)
        for arg in &resource.spec.args {
            if let Some((k, v)) = arg.split_once('=')
                && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                && !k.is_empty()
            {
                env_vars.push(EnvVar {
                    name: k.to_owned(),
                    value: Some(v.to_owned()),
                    ..Default::default()
                });
            }
        }

        let container = Container {
            name: "executor".to_owned(),
            image: Some(resource.spec.image.clone()),
            command: if resource.spec.entrypoint.is_empty() {
                None
            } else {
                Some(resource.spec.entrypoint.clone())
            },
            args: if resource.spec.args.is_empty() {
                None
            } else {
                Some(resource.spec.args.clone())
            },
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
}

/// Build a `PodTemplateSpec` from a `KrishivJobResource` without a live kube client.
///
/// Used by the admission webhook and dry-run tooling where a full
/// `PodLifecycleManager` is not available.
pub fn build_executor_pod_template(
    resource: &KrishivJobResource,
    coordinator_endpoint: &str,
) -> PodTemplateSpec {
    let mut labels: BTreeMap<String, String> = resource.spec.labels.clone();
    labels.insert(JOB_LABEL.to_owned(), resource.metadata.name.clone());

    let container = Container {
        name: "executor".to_owned(),
        image: Some(resource.spec.image.clone()),
        command: if resource.spec.entrypoint.is_empty() {
            None
        } else {
            Some(resource.spec.entrypoint.clone())
        },
        args: if resource.spec.args.is_empty() {
            None
        } else {
            Some(resource.spec.args.clone())
        },
        env: Some(vec![
            EnvVar {
                name: ENV_COORDINATOR_ENDPOINT.to_owned(),
                value: Some(coordinator_endpoint.to_owned()),
                ..Default::default()
            },
            EnvVar {
                name: ENV_JOB_ID.to_owned(),
                value: Some(resource.metadata.scheduler_job_id()),
                ..Default::default()
            },
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
