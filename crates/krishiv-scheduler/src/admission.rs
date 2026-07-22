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

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_proto::{JobId, JobKind};

    fn job(cpu_nanos: Option<u64>, memory_bytes: Option<u64>) -> JobSpec {
        let mut spec = JobSpec::new(JobId::try_new("job-adm").unwrap(), "adm", JobKind::Batch);
        if let Some(c) = cpu_nanos {
            spec = spec.with_cpu_limit_nanos(c);
        }
        if let Some(m) = memory_bytes {
            spec = spec.with_memory_limit_bytes(m);
        }
        spec
    }

    fn snapshot(active: usize, cpu: u64, mem: u64) -> NamespaceQuotaSnapshot {
        NamespaceQuotaSnapshot {
            active_job_count: active,
            cpu_nanos_reserved: cpu,
            memory_bytes_reserved: mem,
            ..Default::default()
        }
    }

    #[test]
    fn in_memory_manager_always_accepts_regardless_of_quota() {
        let qm = InMemoryQueueManager;
        // Even an absurdly loaded namespace snapshot is admitted.
        let outcome = qm.admit(&job(Some(u64::MAX), Some(u64::MAX)), &snapshot(9999, u64::MAX, u64::MAX));
        assert_eq!(outcome, SubmitOutcome::Accepted);
    }

    #[test]
    fn unconstrained_quota_manager_admits_everything() {
        let qm = NamespaceQuotaQueueManager::new(None, None, None);
        assert_eq!(
            qm.admit(&job(Some(1_000), Some(1_000)), &snapshot(1_000, 1_000_000, 1_000_000)),
            SubmitOutcome::Accepted
        );
    }

    #[test]
    fn active_job_limit_queues_at_capacity_and_admits_below() {
        let qm = NamespaceQuotaQueueManager::new(Some(2), None, None);
        // Below limit → accepted.
        assert_eq!(qm.admit(&job(None, None), &snapshot(1, 0, 0)), SubmitOutcome::Accepted);
        // At limit → queued (>= is the boundary).
        assert_eq!(
            qm.admit(&job(None, None), &snapshot(2, 0, 0)),
            SubmitOutcome::Queued { position: 0 }
        );
        // Above limit → queued.
        assert_eq!(
            qm.admit(&job(None, None), &snapshot(3, 0, 0)),
            SubmitOutcome::Queued { position: 0 }
        );
    }

    #[test]
    fn cpu_limit_uses_would_be_reservation() {
        let qm = NamespaceQuotaQueueManager::new(None, Some(100), None);
        // reserved 60 + this job's 40 == 100, not > 100 → accepted.
        assert_eq!(qm.admit(&job(Some(40), None), &snapshot(0, 60, 0)), SubmitOutcome::Accepted);
        // reserved 60 + 41 == 101 > 100 → queued.
        assert_eq!(
            qm.admit(&job(Some(41), None), &snapshot(0, 60, 0)),
            SubmitOutcome::Queued { position: 0 }
        );
        // A job with no declared cpu limit contributes 0.
        assert_eq!(qm.admit(&job(None, None), &snapshot(0, 100, 0)), SubmitOutcome::Accepted);
    }

    #[test]
    fn memory_limit_uses_would_be_reservation() {
        let qm = NamespaceQuotaQueueManager::new(None, None, Some(1_000));
        assert_eq!(qm.admit(&job(None, Some(500)), &snapshot(0, 0, 500)), SubmitOutcome::Accepted);
        assert_eq!(
            qm.admit(&job(None, Some(501)), &snapshot(0, 0, 500)),
            SubmitOutcome::Queued { position: 0 }
        );
    }

    #[test]
    fn saturating_add_never_overflows() {
        let qm = NamespaceQuotaQueueManager::new(None, Some(u64::MAX), Some(u64::MAX));
        // reserved MAX + job MAX would overflow; saturating_add caps at MAX which
        // is NOT > MAX, so this is admitted rather than panicking.
        let outcome = qm.admit(&job(Some(u64::MAX), Some(u64::MAX)), &snapshot(0, u64::MAX, u64::MAX));
        assert_eq!(outcome, SubmitOutcome::Accepted);
    }

    #[test]
    fn first_tripped_limit_queues_even_if_others_pass() {
        // Only the active-job limit is exceeded; cpu/mem are fine.
        let qm = NamespaceQuotaQueueManager::new(Some(1), Some(u64::MAX), Some(u64::MAX));
        assert_eq!(
            qm.admit(&job(Some(1), Some(1)), &snapshot(5, 0, 0)),
            SubmitOutcome::Queued { position: 0 }
        );
    }

    #[test]
    fn from_env_without_vars_is_unconstrained() {
        // The three quota env vars are not set in the test process → all limits
        // None → behaves like the always-admit path.
        let qm = NamespaceQuotaQueueManager::from_env();
        assert_eq!(
            qm.admit(&job(Some(1_000), Some(1_000)), &snapshot(1_000, u64::MAX / 2, u64::MAX / 2)),
            SubmitOutcome::Accepted
        );
    }

    #[test]
    fn on_job_complete_default_is_a_noop() {
        // The default trait method must not panic and returns nothing.
        let qm = NamespaceQuotaQueueManager::new(Some(1), None, None);
        qm.on_job_complete(&JobId::try_new("done").unwrap(), &crate::ResourceUsage::default());
    }
}
