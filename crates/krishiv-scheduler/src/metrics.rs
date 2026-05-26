//! Scheduler hot-path metrics.

use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

// ── GAP-OB-01: Scheduler hot-path metrics counters ──────────────────────────
//
// Simple process-local atomic counters exposed via `scheduler_metrics()`.
// Prometheus / OTLP export can scrape these via the metrics HTTP endpoint.

/// Total number of jobs accepted by `submit_job` since process start.
pub static JOBS_SUBMITTED_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

/// Total number of checkpoint epochs initiated since process start.
pub static CHECKPOINT_EPOCHS_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

/// Total number of task assignments launched since process start.
pub static TASKS_ASSIGNED_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

/// Snapshot of scheduler-level metrics counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerMetrics {
    pub jobs_submitted_total: u64,
    pub checkpoint_epochs_total: u64,
    pub tasks_assigned_total: u64,
}

/// Read the current scheduler metrics snapshot.
pub fn scheduler_metrics() -> SchedulerMetrics {
    SchedulerMetrics {
        jobs_submitted_total: JOBS_SUBMITTED_TOTAL.load(AtomicOrdering::Relaxed),
        checkpoint_epochs_total: CHECKPOINT_EPOCHS_TOTAL.load(AtomicOrdering::Relaxed),
        tasks_assigned_total: TASKS_ASSIGNED_TOTAL.load(AtomicOrdering::Relaxed),
    }
}
