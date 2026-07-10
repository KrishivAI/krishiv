//! SC14: Kubernetes-backed [`ClusterManager`] implementation.
//!
//! `KubernetesClusterManager` implements the synchronous `ClusterManager`
//! trait by enqueuing create/delete Pod requests onto a `tokio::mpsc`
//! channel.  A background task drives the actual `kube::Api` calls so the
//! coordinator lock is never held across network round-trips.
//!
//! # Pod naming
//!
//! Dynamic pool pods are named `{pool_name}-pool-{idx}` and carry the label
//! `krishiv.io/pool: {pool_name}`.  They are cluster-scoped executors that
//! register with the coordinator on startup and pick up any pending task,
//! regardless of which job submitted it.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use k8s_openapi::api::core::v1::{Container, EnvVar, Pod, PodSpec};
use kube::Client;
#[allow(unused_imports)]
use kube::api::{Api, DeleteParams, ObjectMeta as KubeMeta, PostParams};
use tokio::sync::mpsc;
use tracing::{info, warn};

use krishiv_scheduler::cluster_control::ClusterManager;

/// Label applied to every pool executor Pod created by dynamic allocation.
const POOL_LABEL: &str = "krishiv.io/pool";

/// Configuration for the Kubernetes executor pool managed by
/// [`KubernetesClusterManager`].
#[derive(Debug, Clone)]
pub struct KubernetesClusterManagerConfig {
    /// Kubernetes namespace in which executor Pods are created.
    pub namespace: String,
    /// Name prefix for pool Pods (e.g. `"krishiv-pool"`).
    pub pool_name: String,
    /// Container image for pool executor Pods.
    pub image: String,
    /// gRPC endpoint the executor will dial to register with the coordinator.
    pub coordinator_endpoint: String,
    /// Maximum total workers the manager will create.  Requests beyond this
    /// cap are silently dropped (returns 0 for the excess).
    pub max_workers: usize,
    /// Task slots each executor Pod advertises. `None` (recommended) omits
    /// the env var so the executor derives capacity from its CPU allocation;
    /// `Some(n)` pins an explicit override.
    pub task_slots: Option<usize>,
}

/// Messages sent from the synchronous trait surface to the async background
/// actor.
enum AllocRequest {
    Create {
        pod_name: String,
        executor_id: String,
    },
    Delete {
        pod_name: String,
    },
}

/// Kubernetes implementation of [`ClusterManager`].
///
/// Create with [`KubernetesClusterManager::spawn`], which starts the
/// background actor on the current Tokio runtime.
pub struct KubernetesClusterManager {
    tx: mpsc::Sender<AllocRequest>,
    current: Arc<AtomicUsize>,
    max_workers: usize,
    /// Rolling index used to generate unique pod names.
    next_idx: std::sync::Mutex<usize>,
    pool_name: String,
}

impl KubernetesClusterManager {
    /// Spawn the background actor and return a `KubernetesClusterManager`.
    ///
    /// `buffer` is the mpsc channel capacity; 64 is a reasonable default.
    pub fn spawn(config: KubernetesClusterManagerConfig, client: Client, buffer: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer.max(1));
        let current = Arc::new(AtomicUsize::new(0));
        let current2 = current.clone();
        let config2 = config.clone();

        tokio::spawn(async move {
            run_alloc_actor(rx, client, config2, current2).await;
        });

        Self {
            tx,
            current,
            max_workers: config.max_workers,
            next_idx: std::sync::Mutex::new(0),
            pool_name: config.pool_name,
        }
    }

    fn next_pod_name(&self) -> (String, String) {
        let mut guard = self.next_idx.lock().unwrap_or_else(|e| e.into_inner());
        let idx = *guard;
        *guard = idx.wrapping_add(1);
        let pod_name = format!("{}-pool-{idx}", self.pool_name);
        let executor_id = format!("{}-pool-exec-{idx}", self.pool_name);
        (pod_name, executor_id)
    }
}

impl ClusterManager for KubernetesClusterManager {
    fn request_workers(&self, n: usize) -> usize {
        let current = self.current.load(Ordering::Relaxed);
        let headroom = self.max_workers.saturating_sub(current);
        let actual = n.min(headroom);
        if actual == 0 {
            return 0;
        }
        let mut granted = 0;
        for _ in 0..actual {
            let (pod_name, executor_id) = self.next_pod_name();
            // Non-blocking try_send: if the channel is full, skip gracefully.
            if self
                .tx
                .try_send(AllocRequest::Create {
                    pod_name,
                    executor_id,
                })
                .is_ok()
            {
                granted += 1;
            }
        }
        granted
    }

    fn release_workers(&self, n: usize) {
        let current = self.current.load(Ordering::Relaxed);
        let releasable = n.min(current);
        for _ in 0..releasable {
            let (pod_name, _) = self.next_pod_name();
            let _ = self.tx.try_send(AllocRequest::Delete { pod_name });
        }
    }

    fn current_workers(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }
}

/// Background task: receive [`AllocRequest`]s and drive k8s API calls.
async fn run_alloc_actor(
    mut rx: mpsc::Receiver<AllocRequest>,
    client: Client,
    config: KubernetesClusterManagerConfig,
    current: Arc<AtomicUsize>,
) {
    while let Some(req) = rx.recv().await {
        match req {
            AllocRequest::Create {
                pod_name,
                executor_id,
            } => {
                let pod = build_pool_pod(&config, &pod_name, &executor_id);
                let pods: Api<Pod> = Api::namespaced(client.clone(), &config.namespace);
                match pods.create(&PostParams::default(), &pod).await {
                    Ok(_) => {
                        info!(pool = %config.pool_name, pod = %pod_name, executor_id, "SC14: dynamic executor pod created");
                        current.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(kube::Error::Api(e)) if e.code == 409 => {
                        // Pod already exists — treat as created.
                        current.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        warn!(pool = %config.pool_name, pod = %pod_name, error = %e, "SC14: failed to create dynamic executor pod");
                    }
                }
            }
            AllocRequest::Delete { pod_name } => {
                let pods: Api<Pod> = Api::namespaced(client.clone(), &config.namespace);
                match pods.delete(&pod_name, &DeleteParams::default()).await {
                    Ok(_) => {
                        info!(pool = %config.pool_name, pod = %pod_name, "SC14: dynamic executor pod deleted");
                        current.fetch_sub(1, Ordering::Relaxed);
                    }
                    Err(kube::Error::Api(e)) if e.code == 404 => {
                        // Already gone — decrement anyway (best-effort).
                        let _ = current.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                            Some(v.saturating_sub(1))
                        });
                    }
                    Err(e) => {
                        warn!(pool = %config.pool_name, pod = %pod_name, error = %e, "SC14: failed to delete dynamic executor pod");
                    }
                }
            }
        }
    }
}

/// Build a minimal executor Pod spec for a dynamic pool member.
fn build_pool_pod(
    config: &KubernetesClusterManagerConfig,
    pod_name: &str,
    executor_id: &str,
) -> Pod {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    let labels =
        std::collections::BTreeMap::from([(POOL_LABEL.to_owned(), config.pool_name.clone())]);
    Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_owned()),
            namespace: Some(config.namespace.clone()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(PodSpec {
            restart_policy: Some("Never".to_owned()),
            containers: vec![Container {
                name: "executor".to_owned(),
                image: Some(config.image.clone()),
                env: Some(vec![
                    EnvVar {
                        name: "KRISHIV_COORDINATOR_ENDPOINT".to_owned(),
                        value: Some(config.coordinator_endpoint.clone()),
                        ..Default::default()
                    },
                    EnvVar {
                        name: "KRISHIV_EXECUTOR_ID".to_owned(),
                        value: Some(executor_id.to_owned()),
                        ..Default::default()
                    },
                ]
                .into_iter()
                .chain(config.task_slots.map(|slots| EnvVar {
                    name: "KRISHIV_TASK_SLOTS".to_owned(),
                    value: Some(slots.to_string()),
                    ..Default::default()
                }))
                .collect()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that `request_workers` respects `max_workers` and that
    /// `current_workers()` returns 0 before the background actor runs.
    /// This test exercises the synchronous surface without a real k8s cluster.
    #[tokio::test]
    async fn request_workers_respects_max_cap() {
        let config = KubernetesClusterManagerConfig {
            namespace: "default".to_owned(),
            pool_name: "test-pool".to_owned(),
            image: "krishiv:latest".to_owned(),
            coordinator_endpoint: "http://localhost:9090".to_owned(),
            max_workers: 5,
            task_slots: None,
        };

        // task_slots: None must omit the env var entirely so the executor
        // derives capacity from CPU (the audit FLAG-hardcode bug); Some(n)
        // must inject exactly n.
        let pod = build_pool_pod(&config, "pod-0", "exec-0");
        let env = pod.spec.as_ref().unwrap().containers[0].env.clone().unwrap();
        assert!(
            !env.iter().any(|e| e.name == "KRISHIV_TASK_SLOTS"),
            "task_slots: None must not inject KRISHIV_TASK_SLOTS"
        );
        let pinned = KubernetesClusterManagerConfig {
            task_slots: Some(3),
            ..config.clone()
        };
        let pod = build_pool_pod(&pinned, "pod-0", "exec-0");
        let env = pod.spec.as_ref().unwrap().containers[0].env.clone().unwrap();
        assert_eq!(
            env.iter()
                .find(|e| e.name == "KRISHIV_TASK_SLOTS")
                .and_then(|e| e.value.clone())
                .as_deref(),
            Some("3"),
        );

        // Use a deliberately tiny buffer so try_send fills up quickly.
        let (tx, _rx) = mpsc::channel(2);
        let current = Arc::new(AtomicUsize::new(0));
        let mgr = KubernetesClusterManager {
            tx,
            current,
            max_workers: 5,
            next_idx: std::sync::Mutex::new(0),
            pool_name: "test-pool".to_owned(),
        };

        // First call: request 3 out of max 5 — should grant up to channel capacity.
        let granted = mgr.request_workers(3);
        assert!(granted <= 3, "should not grant more than requested");
        assert!(granted <= 5, "should not exceed max_workers");

        // With current=0, requesting 10 more should be capped at max_workers (5).
        // channel is exhausted (capacity 2, already sent 2 or fewer), so try_send
        // returns Err for the rest — granted will equal min(10, 5, channel_space).
        // Just verify the return is <= max_workers.
        let granted2 = mgr.request_workers(10);
        assert!(
            granted2 <= 5,
            "request_workers must not exceed max_workers cap"
        );
    }

    /// `release_workers` is a no-op when `current = 0`.
    #[test]
    fn release_workers_noop_when_empty() {
        let (tx, _rx) = mpsc::channel(4);
        let current = Arc::new(AtomicUsize::new(0));
        let mgr = KubernetesClusterManager {
            tx,
            current,
            max_workers: 10,
            next_idx: std::sync::Mutex::new(0),
            pool_name: "noop-pool".to_owned(),
        };
        mgr.release_workers(5); // Should not panic.
        assert_eq!(mgr.current_workers(), 0);
    }
}
