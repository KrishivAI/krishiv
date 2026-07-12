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

// ── Phase 53: scheduling observability ───────────────────────────────────────

/// Tasks placed on their preferred node (NODE_LOCAL tier).
pub static PLACEMENTS_NODE_LOCAL_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
/// Tasks placed on their preferred rack (RACK_LOCAL tier).
pub static PLACEMENTS_RACK_LOCAL_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
/// Tasks placed with no locality match (ANY tier).
pub static PLACEMENTS_ANY_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
/// Tasks deferred by delay scheduling (waiting for a local slot).
pub static PLACEMENTS_DEFERRED_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
/// Straggler tasks detected by the speculation pass.
pub static SPECULATION_DETECTED_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
/// Straggler originals cancelled + re-queued by the speculation pass.
pub static SPECULATION_PREEMPTED_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
/// Phase 54: stages whose reduce tasks were coalesced by the AQE pass.
pub static AQE_STAGES_COALESCED_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
/// Phase 54: reduce tasks eliminated by AQE partition coalescing.
pub static AQE_TASKS_COALESCED_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
/// Phase 54: skewed reduce partitions split into map-range sub-tasks.
pub static AQE_SKEW_SPLITS_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

/// Record per-tier locality counts from one placement round.
pub fn record_locality_tier_counts(
    node_local: usize,
    rack_local: usize,
    any: usize,
    deferred: usize,
) {
    PLACEMENTS_NODE_LOCAL_TOTAL.fetch_add(node_local as u64, AtomicOrdering::Relaxed);
    PLACEMENTS_RACK_LOCAL_TOTAL.fetch_add(rack_local as u64, AtomicOrdering::Relaxed);
    PLACEMENTS_ANY_TOTAL.fetch_add(any as u64, AtomicOrdering::Relaxed);
    PLACEMENTS_DEFERRED_TOTAL.fetch_add(deferred as u64, AtomicOrdering::Relaxed);
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
    /// Phase 53: per-tier placement counters + speculation outcomes.
    pub placements_node_local_total: u64,
    pub placements_rack_local_total: u64,
    pub placements_any_total: u64,
    pub placements_deferred_total: u64,
    pub speculation_detected_total: u64,
    pub speculation_preempted_total: u64,
    /// Phase 54: AQE stage-boundary re-optimization outcomes.
    pub aqe_stages_coalesced_total: u64,
    pub aqe_tasks_coalesced_total: u64,
    pub aqe_skew_splits_total: u64,
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
        placements_node_local_total: PLACEMENTS_NODE_LOCAL_TOTAL.load(AtomicOrdering::Relaxed),
        placements_rack_local_total: PLACEMENTS_RACK_LOCAL_TOTAL.load(AtomicOrdering::Relaxed),
        placements_any_total: PLACEMENTS_ANY_TOTAL.load(AtomicOrdering::Relaxed),
        placements_deferred_total: PLACEMENTS_DEFERRED_TOTAL.load(AtomicOrdering::Relaxed),
        speculation_detected_total: SPECULATION_DETECTED_TOTAL.load(AtomicOrdering::Relaxed),
        speculation_preempted_total: SPECULATION_PREEMPTED_TOTAL.load(AtomicOrdering::Relaxed),
        aqe_stages_coalesced_total: AQE_STAGES_COALESCED_TOTAL.load(AtomicOrdering::Relaxed),
        aqe_tasks_coalesced_total: AQE_TASKS_COALESCED_TOTAL.load(AtomicOrdering::Relaxed),
        aqe_skew_splits_total: AQE_SKEW_SPLITS_TOTAL.load(AtomicOrdering::Relaxed),
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
         krishiv_task_assignment_duration_ms_sum {duration_sum}\n\
         # HELP krishiv_placements_total Task placements by locality tier.\n\
         # TYPE krishiv_placements_total counter\n\
         krishiv_placements_total{{tier=\"node_local\"}} {node_local}\n\
         krishiv_placements_total{{tier=\"rack_local\"}} {rack_local}\n\
         krishiv_placements_total{{tier=\"any\"}} {any}\n\
         krishiv_placements_total{{tier=\"deferred\"}} {deferred}\n\
         # HELP krishiv_speculation_detected_total Straggler tasks detected by the speculation pass.\n\
         # TYPE krishiv_speculation_detected_total counter\n\
         krishiv_speculation_detected_total {spec_detected}\n\
         # HELP krishiv_speculation_preempted_total Straggler originals cancelled and re-queued.\n\
         # TYPE krishiv_speculation_preempted_total counter\n\
         krishiv_speculation_preempted_total {spec_preempted}\n\
         # HELP krishiv_aqe_stages_coalesced_total Stages whose reduce tasks were coalesced by AQE.\n\
         # TYPE krishiv_aqe_stages_coalesced_total counter\n\
         krishiv_aqe_stages_coalesced_total {aqe_stages}\n\
         # HELP krishiv_aqe_tasks_coalesced_total Reduce tasks eliminated by AQE partition coalescing.\n\
         # TYPE krishiv_aqe_tasks_coalesced_total counter\n\
         krishiv_aqe_tasks_coalesced_total {aqe_tasks}\n\
         # HELP krishiv_aqe_skew_splits_total Skewed reduce partitions split into map-range sub-tasks.\n\
         # TYPE krishiv_aqe_skew_splits_total counter\n\
         krishiv_aqe_skew_splits_total {aqe_skews}\n",
        jobs = m.jobs_submitted_total,
        epochs = m.checkpoint_epochs_total,
        tasks = m.tasks_assigned_total,
        epoch_gauge = m.checkpoint_epoch_gauge,
        duration_sum = m.task_assignment_duration_ms_sum,
        node_local = m.placements_node_local_total,
        rack_local = m.placements_rack_local_total,
        any = m.placements_any_total,
        deferred = m.placements_deferred_total,
        spec_detected = m.speculation_detected_total,
        spec_preempted = m.speculation_preempted_total,
        aqe_stages = m.aqe_stages_coalesced_total,
        aqe_tasks = m.aqe_tasks_coalesced_total,
        aqe_skews = m.aqe_skew_splits_total,
    )
}
