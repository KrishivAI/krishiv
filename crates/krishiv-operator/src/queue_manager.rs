//! Queue CRD definitions for the krishiv operator.

use serde::{Deserialize, Serialize};

/// Kind string for the `KrishivQueue` CRD.
pub const QUEUE_KIND: &str = "KrishivQueue";

/// `KrishivQueue` Kubernetes resource spec.
///
/// Defines resource limits for a namespace. Read by the operator reconciler
/// to drive admission policy decisions.
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
    /// Reserved for future priority-based queue ordering; not yet read by `admit()`.
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
