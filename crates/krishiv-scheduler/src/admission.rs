use std::fmt;

use krishiv_proto::JobSpec;

use crate::{NamespaceQuotaSnapshot, SubmitOutcome};

/// Admission decision returned by a `QueueManager`.
///
/// Receives the static `JobSpec` and a live `NamespaceQuotaSnapshot` from the
/// coordinator. Implementations compare the spec's resource requests against
/// the snapshot's current reservations and their own configured limits.
pub trait QueueManager: Send + Sync + fmt::Debug {
    /// Return whether `spec` may enter the scheduler immediately.
    ///
    /// `quota` contains the live reservation totals for the job's namespace.
    fn admit(&self, spec: &JobSpec, quota: &NamespaceQuotaSnapshot) -> SubmitOutcome;

    /// Notify the queue manager when a job reaches a terminal state.
    ///
    /// The default is a no-op.
    fn on_job_complete(&self, _job_id: &krishiv_proto::JobId, _usage: &crate::ResourceUsage) {}
}

/// Always-admit queue manager for embedded and test contexts.
///
/// Every job is immediately accepted regardless of quota snapshot values.
#[derive(Debug, Default, Clone)]
pub struct InMemoryQueueManager;

impl QueueManager for InMemoryQueueManager {
    fn admit(&self, _spec: &JobSpec, _quota: &NamespaceQuotaSnapshot) -> SubmitOutcome {
        SubmitOutcome::Accepted
    }
}

/// Environment variable: maximum concurrent active jobs per namespace.
pub const NAMESPACE_MAX_ACTIVE_JOBS_ENV: &str = "KRISHIV_NAMESPACE_MAX_ACTIVE_JOBS";

/// Environment variable: maximum total CPU nanoseconds reserved per namespace.
pub const NAMESPACE_MAX_CPU_NANOS_ENV: &str = "KRISHIV_NAMESPACE_MAX_CPU_NANOS";

/// Environment variable: maximum total memory bytes reserved per namespace.
pub const NAMESPACE_MAX_MEMORY_BYTES_ENV: &str = "KRISHIV_NAMESPACE_MAX_MEMORY_BYTES";

/// Per-namespace resource-quota admission controller.
///
/// Enforces three independent limits — active job count, total CPU reservation,
/// and total memory reservation — for the namespace a submitted job belongs to.
/// When admission is denied the decision is `Queued { position: 0 }`, signalling
/// that the caller should retry once capacity is freed.
///
/// Limits are set at construction time or read from environment variables via
/// [`NamespaceQuotaQueueManager::from_env`].  `None` for any limit means that
/// dimension is unconstrained.
#[derive(Debug, Clone)]
pub struct NamespaceQuotaQueueManager {
    /// Maximum number of simultaneously active (non-terminal) jobs per namespace.
    max_active_jobs: Option<usize>,
    /// Maximum total CPU nanoseconds reserved by active jobs in a namespace.
    max_cpu_nanos: Option<u64>,
    /// Maximum total memory bytes reserved by active jobs in a namespace.
    max_memory_bytes: Option<u64>,
}

impl NamespaceQuotaQueueManager {
    /// Create a quota manager with explicit limits.  Pass `None` for an unconstrained dimension.
    pub fn new(
        max_active_jobs: Option<usize>,
        max_cpu_nanos: Option<u64>,
        max_memory_bytes: Option<u64>,
    ) -> Self {
        Self {
            max_active_jobs,
            max_cpu_nanos,
            max_memory_bytes,
        }
    }

    /// Read limits from the standard environment variables.
    ///
    /// - `KRISHIV_NAMESPACE_MAX_ACTIVE_JOBS` → `max_active_jobs`
    /// - `KRISHIV_NAMESPACE_MAX_CPU_NANOS`   → `max_cpu_nanos`
    /// - `KRISHIV_NAMESPACE_MAX_MEMORY_BYTES`→ `max_memory_bytes`
    ///
    /// Unset or unparseable values leave the corresponding limit as `None` (unconstrained).
    pub fn from_env() -> Self {
        let max_active_jobs = std::env::var(NAMESPACE_MAX_ACTIVE_JOBS_ENV)
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0);
        let max_cpu_nanos = std::env::var(NAMESPACE_MAX_CPU_NANOS_ENV)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n > 0);
        let max_memory_bytes = std::env::var(NAMESPACE_MAX_MEMORY_BYTES_ENV)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n > 0);
        Self {
            max_active_jobs,
            max_cpu_nanos,
            max_memory_bytes,
        }
    }
}

impl QueueManager for NamespaceQuotaQueueManager {
    fn admit(&self, spec: &JobSpec, quota: &NamespaceQuotaSnapshot) -> SubmitOutcome {
        if let Some(max_jobs) = self.max_active_jobs
            && quota.active_job_count >= max_jobs
        {
            return SubmitOutcome::Queued { position: 0 };
        }
        if let Some(max_cpu) = self.max_cpu_nanos {
            let would_be = quota
                .cpu_nanos_reserved
                .saturating_add(spec.cpu_limit_nanos().unwrap_or(0));
            if would_be > max_cpu {
                return SubmitOutcome::Queued { position: 0 };
            }
        }
        if let Some(max_mem) = self.max_memory_bytes {
            let would_be = quota
                .memory_bytes_reserved
                .saturating_add(spec.memory_limit_bytes().unwrap_or(0));
            if would_be > max_mem {
                return SubmitOutcome::Queued { position: 0 };
            }
        }
        SubmitOutcome::Accepted
    }
}
