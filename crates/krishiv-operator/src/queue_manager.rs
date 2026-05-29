//! Queue CRD manager.

use krishiv_scheduler::{
    NamespaceQuotaSnapshot, QueueManager, QuotaPolicy, ResourceUsage, SubmitOutcome,
};
use serde::{Deserialize, Serialize};

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

        if let Some(limit) = policy.max_concurrent_jobs
            && quota.active_job_count >= limit
        {
            return SubmitOutcome::Queued {
                position: quota.active_job_count.saturating_sub(limit),
            };
        }
        if let Some(limit) = policy.cpu_nanos_limit
            && quota
                .cpu_nanos_reserved
                .saturating_add(spec.cpu_limit_nanos().unwrap_or(0))
                > limit
        {
            return SubmitOutcome::Queued {
                position: quota.active_job_count.saturating_add(1),
            };
        }
        if let Some(limit) = policy.memory_bytes_limit
            && quota
                .memory_bytes_reserved
                .saturating_add(spec.memory_limit_bytes().unwrap_or(0))
                > limit
        {
            return SubmitOutcome::Queued {
                position: quota.active_job_count.saturating_add(1),
            };
        }
        SubmitOutcome::Accepted
    }

    fn on_job_complete(&self, _job_id: &krishiv_proto::JobId, _usage: &ResourceUsage) {}
}
