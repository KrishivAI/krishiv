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

/// Current checkpoint epoch gauge — updated every time a checkpoint is committed.
///
/// This is a process-global gauge (not per-job) that tracks the last committed
/// epoch across all jobs.  Per-job tracking requires a `DashMap` or similar,
/// which is beyond the scope of this lightweight atomic-counter module.
pub static CHECKPOINT_EPOCH_GAUGE: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

/// Rolling sum of task assignment latency in milliseconds.
///
/// Divide by `TASKS_ASSIGNED_TOTAL` to obtain the rolling average.
/// Updated whenever `TASKS_ASSIGNED_TOTAL` is incremented via
/// [`record_task_assignment_duration_ms`].
pub static TASK_ASSIGNMENT_DURATION_MS_SUM: LazyLock<AtomicU64> =
    LazyLock::new(|| AtomicU64::new(0));

/// Record the completion of one checkpoint epoch for a job.
///
/// Updates [`CHECKPOINT_EPOCH_GAUGE`] to the maximum of the stored value and
/// `epoch` (monotonic — a newly committed epoch is always ≥ any prior epoch).
pub fn record_checkpoint_epoch(_job_id: &str, epoch: u64) {
    // Monotonic update: only advance the gauge, never retreat it.
    let mut current = CHECKPOINT_EPOCH_GAUGE.load(AtomicOrdering::Relaxed);
    loop {
        if epoch <= current {
            break;
        }
        match CHECKPOINT_EPOCH_GAUGE.compare_exchange_weak(
            current,
            epoch,
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

/// Record the latency of one task assignment round-trip in milliseconds.
///
/// Updates [`TASK_ASSIGNMENT_DURATION_MS_SUM`].  The rolling average is
/// `TASK_ASSIGNMENT_DURATION_MS_SUM / TASKS_ASSIGNED_TOTAL`.
pub fn record_task_assignment_duration_ms(duration_ms: u64) {
    TASK_ASSIGNMENT_DURATION_MS_SUM.fetch_add(duration_ms, AtomicOrdering::Relaxed);
}

/// Snapshot of scheduler-level metrics counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerMetrics {
    pub jobs_submitted_total: u64,
    pub checkpoint_epochs_total: u64,
    pub tasks_assigned_total: u64,
    /// Last committed checkpoint epoch across all jobs (monotonically increasing).
    pub checkpoint_epoch_gauge: u64,
    /// Rolling sum of task assignment latency in milliseconds.
    pub task_assignment_duration_ms_sum: u64,
}

/// Read the current scheduler metrics snapshot.
pub fn scheduler_metrics() -> SchedulerMetrics {
    SchedulerMetrics {
        jobs_submitted_total: JOBS_SUBMITTED_TOTAL.load(AtomicOrdering::Relaxed),
        checkpoint_epochs_total: CHECKPOINT_EPOCHS_TOTAL.load(AtomicOrdering::Relaxed),
        tasks_assigned_total: TASKS_ASSIGNED_TOTAL.load(AtomicOrdering::Relaxed),
        checkpoint_epoch_gauge: CHECKPOINT_EPOCH_GAUGE.load(AtomicOrdering::Relaxed),
        task_assignment_duration_ms_sum: TASK_ASSIGNMENT_DURATION_MS_SUM
            .load(AtomicOrdering::Relaxed),
    }
}

/// Render the current metrics snapshot as a Prometheus-compatible text format.
///
/// Each counter is emitted as a `# TYPE counter` + metric line.
pub fn render_prometheus_metrics() -> String {
    let m = scheduler_metrics();
    format!(
        "# HELP krishiv_jobs_submitted_total Total jobs accepted by coordinator since process start.\n\
         # TYPE krishiv_jobs_submitted_total counter\n\
         krishiv_jobs_submitted_total {jobs}\n\
         # HELP krishiv_checkpoint_epochs_total Total checkpoint epochs initiated since process start.\n\
         # TYPE krishiv_checkpoint_epochs_total counter\n\
         krishiv_checkpoint_epochs_total {epochs}\n\
         # HELP krishiv_tasks_assigned_total Total task assignments launched since process start.\n\
         # TYPE krishiv_tasks_assigned_total counter\n\
         krishiv_tasks_assigned_total {tasks}\n\
         # HELP krishiv_checkpoint_epoch_gauge Last committed checkpoint epoch (monotonically increasing).\n\
         # TYPE krishiv_checkpoint_epoch_gauge gauge\n\
         krishiv_checkpoint_epoch_gauge {epoch_gauge}\n\
         # HELP krishiv_task_assignment_duration_ms_sum Rolling sum of task assignment latency (ms).\n\
         # TYPE krishiv_task_assignment_duration_ms_sum counter\n\
         krishiv_task_assignment_duration_ms_sum {duration_sum}\n",
        jobs = m.jobs_submitted_total,
        epochs = m.checkpoint_epochs_total,
        tasks = m.tasks_assigned_total,
        epoch_gauge = m.checkpoint_epoch_gauge,
        duration_sum = m.task_assignment_duration_ms_sum,
    )
}
