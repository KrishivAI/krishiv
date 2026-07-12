use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

// Process metrics rendered in Prometheus text exposition format.

/// Default latency histogram bucket upper bounds, in seconds.
const LATENCY_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// µs-resolution bucket bounds (in seconds) for the streaming ingest→emit
/// record-latency histogram (Phase 55 exit gate). The shared
/// [`LATENCY_BUCKETS`] bottom out at 5 ms — too coarse to resolve the
/// continuous loop's sub-millisecond target — so this family carries its own
/// per-metric bucket set: 50 µs → 1 s.
const STREAM_RECORD_LATENCY_BUCKETS: &[f64] = &[
    0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
    0.5, 1.0,
];

/// Escape a Prometheus label value per the text exposition format: `\`, `"`,
/// and `\n` must be escaped so a malicious or unusual label cannot break the
/// exposition format or inject metric lines.
fn escape_label_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// Thread-safe OpenTelemetry-aligned latency histogram.
#[derive(Debug)]
pub struct KrishivHistogram {
    buckets: &'static [f64],
    counts: Vec<AtomicU64>,
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl Default for KrishivHistogram {
    fn default() -> Self {
        let counts = (0..=LATENCY_BUCKETS.len())
            .map(|_| AtomicU64::new(0))
            .collect();
        Self {
            buckets: LATENCY_BUCKETS,
            counts,
            sum_micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
}

impl KrishivHistogram {
    /// Create a histogram with a custom static bucket set.
    ///
    /// Use for metric families whose latency scale the shared
    /// [`LATENCY_BUCKETS`] cannot resolve (e.g. the µs-scale streaming
    /// record-latency histogram).
    pub fn with_buckets(buckets: &'static [f64]) -> Self {
        let counts = (0..=buckets.len()).map(|_| AtomicU64::new(0)).collect();
        Self {
            buckets,
            counts,
            sum_micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Bucket upper bounds (seconds) this histogram observes into.
    pub fn bucket_bounds(&self) -> &'static [f64] {
        self.buckets
    }

    /// Approximate quantile from the recorded bucket counts (linear
    /// interpolation inside the winning bucket; the overflow bucket reports
    /// its lower bound). Returns `None` when nothing has been recorded.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        let (count, _, counts, _) = self.snapshot();
        if count == 0 {
            return None;
        }
        let rank = (q.clamp(0.0, 1.0) * count as f64).ceil().max(1.0) as u64;
        let mut cumulative = 0u64;
        for (i, c) in counts.iter().enumerate() {
            cumulative += c;
            if cumulative >= rank {
                return Some(match self.buckets.get(i) {
                    Some(&upper) => upper,
                    // Overflow bucket: report the largest finite bound.
                    None => self.buckets.last().copied().unwrap_or(f64::INFINITY),
                });
            }
        }
        self.buckets.last().copied()
    }

    /// Record a duration observation in seconds.
    ///
    /// Non-finite or negative durations are dropped: the sum is stored as
    /// unsigned microseconds and could not represent them, and recording them
    /// would corrupt the count/bucket tallies relative to the sum.
    pub fn observe(&self, value_secs: f64) {
        if !value_secs.is_finite() || value_secs < 0.0 {
            return;
        }
        let micros = (value_secs * 1_000_000.0) as u64;
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        let mut bucket_idx = self.buckets.len();
        for (i, &bucket) in self.buckets.iter().enumerate() {
            if value_secs <= bucket {
                bucket_idx = i;
                break;
            }
        }
        if let Some(counter) = self.counts.get(bucket_idx) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Snapshot the current count, sum, per-bucket counts, and bucket count.
    pub fn snapshot(&self) -> (u64, f64, Vec<u64>, u64) {
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let mut counts = Vec::with_capacity(self.counts.len());
        for c in &self.counts {
            counts.push(c.load(Ordering::Relaxed));
        }
        (count, sum, counts, self.buckets.len() as u64)
    }
}

/// OpenTelemetry-aligned counters/histograms for Krishiv runtime observability.
#[derive(Debug, Default)]
pub struct KrishivMetrics {
    tasks_submitted: AtomicU64,
    tasks_running: AtomicU64,
    tasks_succeeded: AtomicU64,
    tasks_failed: AtomicU64,
    executor_lost: AtomicU64,
    shuffle_bytes_written: AtomicU64,
    /// T19: total rows written to shuffle output (counter).
    shuffle_records_written: AtomicU64,
    /// T19: total bytes read from shuffle input (counter).
    shuffle_read_bytes: AtomicU64,
    /// T19: total rows read from shuffle input (counter).
    shuffle_read_records: AtomicU64,
    /// T19: total time spent on shuffle write paths in microseconds (counter).
    shuffle_write_time_us: AtomicU64,
    /// T19: total time spent on shuffle read paths in microseconds (counter).
    shuffle_read_time_us: AtomicU64,
    /// T19: local shuffle blocks fetched (counter).
    shuffle_local_blocks_fetched: AtomicU64,
    /// T19: remote shuffle blocks fetched (counter).
    shuffle_remote_blocks_fetched: AtomicU64,
    /// T19: time spent waiting for shuffle fetches to return in microseconds (counter).
    shuffle_fetch_wait_time_us: AtomicU64,
    /// Total bytes spilled to local disk by memory-bounded operators (counter).
    spill_bytes_total: AtomicU64,
    /// Total spill events / spill files written (counter).
    spill_files_total: AtomicU64,
    job_queue_depth: AtomicU64,
    /// Peak memory observed per operator kind (gauge, keyed by operator label).
    operator_memory_bytes: dashmap::DashMap<String, AtomicU64>,
    /// Current committed checkpoint epoch per job_id (gauge, keyed by job_id).
    checkpoint_epoch: dashmap::DashMap<String, AtomicU64>,
    /// Global low watermark in milliseconds (gauge, keyed by job_id).
    watermark_ms: dashmap::DashMap<String, AtomicI64>,
    /// Checkpoint epochs committed/aborted (counter, keyed by job_id).
    checkpoint_epochs: dashmap::DashMap<String, CheckpointEpochCounters>,
    /// Task attempts per (job_id, stage_id) — submitted/succeeded/failed/retrying.
    task_attempts: dashmap::DashMap<String, TaskAttemptCounters>,
    /// Executor slots used per executor (gauge).
    executor_slots_used: dashmap::DashMap<String, AtomicU64>,
    /// Source offset lag (broker_offset - consumer_offset) per (job_id, source_id).
    source_offset_lag: dashmap::DashMap<String, AtomicI64>,
    /// Streaming rows emitted per (job_id, task_id) (counter).
    streaming_rows: dashmap::DashMap<String, AtomicU64>,
    /// State backend key count per job_id (gauge).
    state_key_count: dashmap::DashMap<String, AtomicU64>,
    /// State backend byte size per job_id (gauge).
    state_bytes: dashmap::DashMap<String, AtomicU64>,
    /// Shuffle partitions per (job_id, stage_id) — pending/available/failed.
    shuffle_partitions: dashmap::DashMap<String, ShufflePartitionCounters>,
    /// Latency histogram for gRPC call durations (labeled by path/method).
    grpc_call_duration: dashmap::DashMap<String, KrishivHistogram>,
    /// Latency histogram for checkpoint commit phases (labeled by phase).
    checkpoint_commit_duration: dashmap::DashMap<String, KrishivHistogram>,
    // ── Streaming metrics (Phase 7) ─────────────────────────────────────────
    /// Source read latency histogram (labeled by source_id).
    source_read_duration: dashmap::DashMap<String, KrishivHistogram>,
    /// Output buffer flush reason counter (labeled by reason: "rows"|"bytes"|"time").
    output_buffer_flushes: dashmap::DashMap<String, AtomicU64>,
    /// Checkpoint alignment time histogram (labeled by alignment mode).
    checkpoint_alignment_duration: dashmap::DashMap<String, KrishivHistogram>,
    /// Unaligned in-flight bytes gauge (labeled by job_id).
    unaligned_in_flight_bytes: dashmap::DashMap<String, AtomicU64>,
    /// Checkpoint upload time histogram (labeled by job_id).
    checkpoint_upload_duration: dashmap::DashMap<String, KrishivHistogram>,
    /// Restore time histogram (labeled by job_id).
    restore_duration: dashmap::DashMap<String, KrishivHistogram>,
    /// State cache hit counter (labeled by job_id).
    state_cache_hits: dashmap::DashMap<String, AtomicU64>,
    /// State cache miss counter (labeled by job_id).
    state_cache_misses: dashmap::DashMap<String, AtomicU64>,
    /// Object-store request count (labeled by operation: "get"|"put"|"delete"|"list").
    object_store_requests: dashmap::DashMap<String, AtomicU64>,
    /// Sink prepare duration histogram (labeled by sink_id).
    sink_prepare_duration: dashmap::DashMap<String, KrishivHistogram>,
    /// Sink commit duration histogram (labeled by sink_id).
    sink_commit_duration: dashmap::DashMap<String, KrishivHistogram>,
    /// Sink abort duration histogram (labeled by sink_id).
    sink_abort_duration: dashmap::DashMap<String, KrishivHistogram>,
    /// Backpressure duration in microseconds (labeled by job_id).
    backpressure_duration_us: dashmap::DashMap<String, AtomicU64>,
    /// Streaming ingest→emit record latency (labeled by job_id) — µs-scale
    /// buckets ([`STREAM_RECORD_LATENCY_BUCKETS`]), Phase 55 exit-gate
    /// instrument. Measures source-read to operator-emit inside the engine.
    stream_record_latency: dashmap::DashMap<String, KrishivHistogram>,
}

#[derive(Debug, Default)]
struct CheckpointEpochCounters {
    committed: AtomicU64,
    aborted: AtomicU64,
    failed: AtomicU64,
}

#[derive(Debug, Default)]
struct TaskAttemptCounters {
    submitted: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    retrying: AtomicU64,
}

#[derive(Debug, Default)]
struct ShufflePartitionCounters {
    pending: AtomicU64,
    available: AtomicU64,
    failed: AtomicU64,
}

static GLOBAL_METRICS: OnceLock<KrishivMetrics> = OnceLock::new();

/// Process-wide metrics registry (lazy-initialized).
pub fn global_metrics() -> &'static KrishivMetrics {
    GLOBAL_METRICS.get_or_init(KrishivMetrics::default)
}

impl KrishivMetrics {
    // Global (unlabeled) counters/gauges

    /// Record a submitted task.
    pub fn inc_tasks_submitted(&self) {
        self.tasks_submitted.fetch_add(1, Ordering::Relaxed);
    }

    /// Set the current running task gauge.
    pub fn set_tasks_running(&self, count: u64) {
        self.tasks_running.store(count, Ordering::Relaxed);
    }

    /// Record a succeeded task.
    pub fn inc_tasks_succeeded(&self) {
        self.tasks_succeeded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed task.
    pub fn inc_tasks_failed(&self) {
        self.tasks_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an executor heartbeat timeout (executor marked lost).
    pub fn inc_executor_lost(&self) {
        self.executor_lost.fetch_add(1, Ordering::Relaxed);
    }

    /// Add shuffle bytes written.
    pub fn add_shuffle_bytes_written(&self, bytes: u64) {
        self.shuffle_bytes_written
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// T19: increment the rows-written counter.
    pub fn add_shuffle_records_written(&self, rows: u64) {
        self.shuffle_records_written
            .fetch_add(rows, Ordering::Relaxed);
    }

    /// T19: increment the shuffle-read bytes counter.
    pub fn add_shuffle_read_bytes(&self, bytes: u64) {
        self.shuffle_read_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// T19: increment the shuffle-read rows counter.
    pub fn add_shuffle_read_records(&self, rows: u64) {
        self.shuffle_read_records.fetch_add(rows, Ordering::Relaxed);
    }

    /// T19: increment the shuffle-write time counter (microseconds).
    pub fn add_shuffle_write_time_us(&self, us: u64) {
        self.shuffle_write_time_us.fetch_add(us, Ordering::Relaxed);
    }

    /// T19: increment the shuffle-read time counter (microseconds).
    pub fn add_shuffle_read_time_us(&self, us: u64) {
        self.shuffle_read_time_us.fetch_add(us, Ordering::Relaxed);
    }

    /// T19: increment the local-blocks-fetched counter.
    pub fn add_shuffle_local_blocks_fetched(&self, count: u64) {
        self.shuffle_local_blocks_fetched
            .fetch_add(count, Ordering::Relaxed);
    }

    /// T19: increment the remote-blocks-fetched counter.
    pub fn add_shuffle_remote_blocks_fetched(&self, count: u64) {
        self.shuffle_remote_blocks_fetched
            .fetch_add(count, Ordering::Relaxed);
    }

    /// T19: increment the fetch-wait time counter (microseconds).
    pub fn add_shuffle_fetch_wait_time_us(&self, us: u64) {
        self.shuffle_fetch_wait_time_us
            .fetch_add(us, Ordering::Relaxed);
    }

    /// Record a spill to local disk: total bytes written plus the number of
    /// spill events (roughly one per spill file).
    pub fn record_spill(&self, bytes: u64, files: u64) {
        self.spill_bytes_total.fetch_add(bytes, Ordering::Relaxed);
        self.spill_files_total.fetch_add(files, Ordering::Relaxed);
    }

    /// Current tasks running.
    pub fn tasks_running(&self) -> u64 {
        self.tasks_running.load(Ordering::Relaxed)
    }

    /// Total tasks submitted.
    pub fn tasks_submitted(&self) -> u64 {
        self.tasks_submitted.load(Ordering::Relaxed)
    }

    /// Total tasks succeeded.
    pub fn tasks_succeeded(&self) -> u64 {
        self.tasks_succeeded.load(Ordering::Relaxed)
    }

    /// Total tasks failed.
    pub fn tasks_failed(&self) -> u64 {
        self.tasks_failed.load(Ordering::Relaxed)
    }

    /// Total executor lost events.
    pub fn executor_lost(&self) -> u64 {
        self.executor_lost.load(Ordering::Relaxed)
    }

    /// Total shuffle bytes written.
    pub fn shuffle_bytes_written(&self) -> u64 {
        self.shuffle_bytes_written.load(Ordering::Relaxed)
    }

    /// Current job queue depth.
    pub fn job_queue_depth(&self) -> u64 {
        self.job_queue_depth.load(Ordering::Relaxed)
    }

    /// Number of jobs with active watermark entries.
    pub fn watermark_entry_count(&self) -> usize {
        self.watermark_ms.len()
    }

    /// Number of jobs with active state-key entries.
    pub fn state_key_entry_count(&self) -> usize {
        self.state_key_count.len()
    }

    /// Total bytes spilled to disk so far.
    pub fn spill_bytes_total(&self) -> u64 {
        self.spill_bytes_total.load(Ordering::Relaxed)
    }

    /// Total spill events so far.
    pub fn spill_files_total(&self) -> u64 {
        self.spill_files_total.load(Ordering::Relaxed)
    }

    /// Record the peak memory observed for an operator kind (gauge).
    ///
    /// Keeps the maximum value seen per operator label so the gauge reflects
    /// the high-water mark across all tasks in this process.
    pub fn record_operator_memory(&self, operator: &str, bytes: u64) {
        let entry = self
            .operator_memory_bytes
            .entry(operator.to_string())
            .or_default();
        entry.fetch_max(bytes, Ordering::Relaxed);
    }

    /// Peak memory recorded for an operator kind, if any.
    pub fn operator_memory(&self, operator: &str) -> Option<u64> {
        self.operator_memory_bytes
            .get(operator)
            .map(|v| v.load(Ordering::Relaxed))
    }

    /// Set job queue depth gauge.
    pub fn set_job_queue_depth(&self, depth: u64) {
        self.job_queue_depth.store(depth, Ordering::Relaxed);
    }

    // Labeled per-job checkpoint metrics

    /// Set the current committed checkpoint epoch gauge for a job.
    pub fn set_checkpoint_epoch(&self, job_id: &str, epoch: u64) {
        self.checkpoint_epoch
            .entry(job_id.to_string())
            .or_default()
            .store(epoch, Ordering::Relaxed);
    }

    /// Record a committed checkpoint epoch.
    pub fn inc_checkpoint_committed(&self, job_id: &str) {
        self.checkpoint_epochs
            .entry(job_id.to_string())
            .or_default()
            .committed
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record an aborted checkpoint epoch.
    pub fn inc_checkpoint_aborted(&self, job_id: &str) {
        self.checkpoint_epochs
            .entry(job_id.to_string())
            .or_default()
            .aborted
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed checkpoint epoch.
    pub fn inc_checkpoint_failed(&self, job_id: &str) {
        self.checkpoint_epochs
            .entry(job_id.to_string())
            .or_default()
            .failed
            .fetch_add(1, Ordering::Relaxed);
    }

    // Labeled per-job watermark / offset metrics

    /// Set the current global low watermark (ms) for a streaming job.
    pub fn set_watermark_ms(&self, job_id: &str, watermark_ms: i64) {
        self.watermark_ms
            .entry(job_id.to_string())
            .or_default()
            .store(watermark_ms, Ordering::Relaxed);
    }

    /// Set the source offset lag for a specific source partition.
    /// Positive values mean the source is behind; negative/zero means caught up.
    pub fn set_source_offset_lag(&self, job_id: &str, source_id: &str, lag: i64) {
        let key = format!("{job_id}:{source_id}");
        self.source_offset_lag
            .entry(key)
            .or_default()
            .store(lag, Ordering::Relaxed);
    }

    // Labeled per-job/stage task attempt counters

    /// Record a task attempt submission for a given job and stage.
    pub fn inc_task_attempt_submitted(&self, job_id: &str, stage_id: &str) {
        let key = format!("{job_id}:{stage_id}");
        self.task_attempts
            .entry(key)
            .or_default()
            .submitted
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a task attempt that succeeded.
    pub fn inc_task_attempt_succeeded(&self, job_id: &str, stage_id: &str) {
        let key = format!("{job_id}:{stage_id}");
        self.task_attempts
            .entry(key)
            .or_default()
            .succeeded
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a task attempt that failed.
    pub fn inc_task_attempt_failed(&self, job_id: &str, stage_id: &str) {
        let key = format!("{job_id}:{stage_id}");
        self.task_attempts
            .entry(key)
            .or_default()
            .failed
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a task currently retrying.
    pub fn inc_task_attempt_retrying(&self, job_id: &str, stage_id: &str) {
        let key = format!("{job_id}:{stage_id}");
        self.task_attempts
            .entry(key)
            .or_default()
            .retrying
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Remove all per-job task attempt counters when a job is cleaned up.
    pub fn remove_task_attempt_counters(&self, job_id: &str) {
        let prefix = format!("{job_id}:");
        self.task_attempts.retain(|k, _| !k.starts_with(&prefix));
    }

    // Executor slot gauges

    /// Set the number of slots currently used on an executor.
    pub fn set_executor_slots_used(&self, executor_id: &str, slots: u64) {
        self.executor_slots_used
            .entry(executor_id.to_string())
            .or_default()
            .store(slots, Ordering::Relaxed);
    }

    // Streaming rows counter

    /// Add rows emitted by a streaming task.
    pub fn add_streaming_rows(&self, job_id: &str, task_id: &str, rows: u64) {
        let key = format!("{job_id}:{task_id}");
        self.streaming_rows
            .entry(key)
            .or_default()
            .fetch_add(rows, Ordering::Relaxed);
    }

    /// Set absolute cumulative rows emitted by a streaming task.
    pub fn set_streaming_rows(&self, job_id: &str, task_id: &str, rows: u64) {
        let key = format!("{job_id}:{task_id}");
        self.streaming_rows
            .entry(key)
            .or_default()
            .store(rows, Ordering::Relaxed);
    }

    // State backend gauges

    /// Set the key count for a state backend.
    pub fn set_state_key_count(&self, job_id: &str, count: u64) {
        self.state_key_count
            .entry(job_id.to_string())
            .or_default()
            .store(count, Ordering::Relaxed);
    }

    /// Set the byte size for a state backend.
    pub fn set_state_bytes(&self, job_id: &str, bytes: u64) {
        self.state_bytes
            .entry(job_id.to_string())
            .or_default()
            .store(bytes, Ordering::Relaxed);
    }

    // Shuffle partition progress gauges

    /// Set shuffle partition counts for a (job_id, stage_id) pair.
    pub fn set_shuffle_partitions(
        &self,
        job_id: &str,
        stage_id: &str,
        pending: u64,
        available: u64,
        failed: u64,
    ) {
        let key = format!("{job_id}:{stage_id}");
        let entry = self.shuffle_partitions.entry(key).or_default();
        entry.pending.store(pending, Ordering::Relaxed);
        entry.available.store(available, Ordering::Relaxed);
        entry.failed.store(failed, Ordering::Relaxed);
    }

    /// Remove per-job shuffle partition counters when a job is cleaned up.
    pub fn remove_shuffle_partition_counters(&self, job_id: &str) {
        let prefix = format!("{job_id}:");
        self.shuffle_partitions
            .retain(|k, _| !k.starts_with(&prefix));
    }

    /// Remove all per-job metrics for a completed/cancelled job.
    pub fn remove_job(&self, job_id: &str) {
        self.checkpoint_epoch.remove(job_id);
        self.watermark_ms.remove(job_id);
        self.checkpoint_epochs.remove(job_id);
        self.state_key_count.remove(job_id);
        self.state_bytes.remove(job_id);
        self.remove_task_attempt_counters(job_id);
        self.remove_shuffle_partition_counters(job_id);
        let prefix = format!("{job_id}:");
        self.source_offset_lag
            .retain(|k, _| !k.starts_with(&prefix));
        self.streaming_rows.retain(|k, _| !k.starts_with(&prefix));
        self.operator_memory_bytes
            .retain(|k, _| !k.starts_with(&prefix));
        // Streaming metrics cleanup
        self.unaligned_in_flight_bytes.remove(job_id);
        self.state_cache_hits.remove(job_id);
        self.state_cache_misses.remove(job_id);
        self.backpressure_duration_us.remove(job_id);
        self.checkpoint_upload_duration.remove(job_id);
        self.restore_duration.remove(job_id);
        self.source_read_duration.remove(job_id);
        self.stream_record_latency.remove(job_id);
        self.checkpoint_alignment_duration.remove(job_id);
        self.output_buffer_flushes.clear();
        self.sink_prepare_duration.clear();
        self.sink_commit_duration.clear();
        self.sink_abort_duration.clear();
        self.object_store_requests.clear();
    }

    // Duration observation histograms

    /// Record a gRPC call duration in seconds.
    pub fn observe_grpc_duration(&self, path: &str, duration_secs: f64) {
        self.grpc_call_duration
            .entry(path.to_string())
            .or_default()
            .observe(duration_secs);
    }

    /// Record a checkpoint commit duration in seconds.
    pub fn observe_checkpoint_commit_duration(&self, phase: &str, duration_secs: f64) {
        self.checkpoint_commit_duration
            .entry(phase.to_string())
            .or_default()
            .observe(duration_secs);
    }

    // ── Streaming metrics (Phase 7) ─────────────────────────────────────────

    /// Record source read latency in seconds.
    pub fn observe_source_read_duration(&self, source_id: &str, duration_secs: f64) {
        self.source_read_duration
            .entry(source_id.to_string())
            .or_default()
            .observe(duration_secs);
    }

    /// Record one ingest→emit record latency observation for a streaming job
    /// (seconds; µs-resolution buckets — Phase 55 exit-gate instrument).
    pub fn observe_stream_record_latency(&self, job_id: &str, duration_secs: f64) {
        self.stream_record_latency
            .entry(job_id.to_string())
            .or_insert_with(|| KrishivHistogram::with_buckets(STREAM_RECORD_LATENCY_BUCKETS))
            .observe(duration_secs);
    }

    /// Approximate quantile of the ingest→emit latency for a job (seconds).
    /// `None` when the job has no recorded observations.
    pub fn stream_record_latency_quantile(&self, job_id: &str, q: f64) -> Option<f64> {
        self.stream_record_latency
            .get(job_id)
            .and_then(|h| h.quantile(q))
    }

    /// Record an output buffer flush with a reason.
    pub fn inc_output_buffer_flush(&self, reason: &str) {
        self.output_buffer_flushes
            .entry(reason.to_string())
            .or_default()
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record checkpoint alignment time in seconds.
    pub fn observe_checkpoint_alignment_duration(&self, alignment: &str, duration_secs: f64) {
        self.checkpoint_alignment_duration
            .entry(alignment.to_string())
            .or_default()
            .observe(duration_secs);
    }

    /// Set unaligned in-flight bytes for a job.
    pub fn set_unaligned_in_flight_bytes(&self, job_id: &str, bytes: u64) {
        self.unaligned_in_flight_bytes
            .entry(job_id.to_string())
            .or_default()
            .store(bytes, Ordering::Relaxed);
    }

    /// Record checkpoint upload duration in seconds.
    pub fn observe_checkpoint_upload_duration(&self, job_id: &str, duration_secs: f64) {
        self.checkpoint_upload_duration
            .entry(job_id.to_string())
            .or_default()
            .observe(duration_secs);
    }

    /// Record restore duration in seconds.
    pub fn observe_restore_duration(&self, job_id: &str, duration_secs: f64) {
        self.restore_duration
            .entry(job_id.to_string())
            .or_default()
            .observe(duration_secs);
    }

    /// Increment state cache hit count.
    pub fn inc_state_cache_hit(&self, job_id: &str) {
        self.state_cache_hits
            .entry(job_id.to_string())
            .or_default()
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increment state cache miss count.
    pub fn inc_state_cache_miss(&self, job_id: &str) {
        self.state_cache_misses
            .entry(job_id.to_string())
            .or_default()
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increment object-store request count.
    pub fn inc_object_store_request(&self, operation: &str) {
        self.object_store_requests
            .entry(operation.to_string())
            .or_default()
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record sink prepare duration in seconds.
    pub fn observe_sink_prepare_duration(&self, sink_id: &str, duration_secs: f64) {
        self.sink_prepare_duration
            .entry(sink_id.to_string())
            .or_default()
            .observe(duration_secs);
    }

    /// Record sink commit duration in seconds.
    pub fn observe_sink_commit_duration(&self, sink_id: &str, duration_secs: f64) {
        self.sink_commit_duration
            .entry(sink_id.to_string())
            .or_default()
            .observe(duration_secs);
    }

    /// Record sink abort duration in seconds.
    pub fn observe_sink_abort_duration(&self, sink_id: &str, duration_secs: f64) {
        self.sink_abort_duration
            .entry(sink_id.to_string())
            .or_default()
            .observe(duration_secs);
    }

    /// Add backpressure duration in microseconds.
    pub fn add_backpressure_duration_us(&self, job_id: &str, us: u64) {
        self.backpressure_duration_us
            .entry(job_id.to_string())
            .or_default()
            .fetch_add(us, Ordering::Relaxed);
    }

    // Prometheus rendering

    /// Render Prometheus exposition format for Krishiv counters/gauges.
    ///
    /// Emits valid Prometheus text format: exactly one `# HELP` and `# TYPE` line
    /// per metric family, followed by all labeled samples for that family.  Label
    /// values are escaped per the exposition format specification.
    pub fn render_prometheus(&self) -> String {
        self.render_prometheus_inner().unwrap_or_default()
    }

    fn render_prometheus_inner(&self) -> Result<String, std::fmt::Error> {
        let mut out = String::with_capacity(8192);

        // Global (unlabeled) metrics

        let submitted = self.tasks_submitted.load(Ordering::Relaxed);
        let succeeded = self.tasks_succeeded.load(Ordering::Relaxed);
        let failed = self.tasks_failed.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_tasks_total Tasks submitted to the coordinator"
        )?;
        writeln!(out, "# TYPE krishiv_tasks_total counter")?;
        writeln!(
            out,
            "krishiv_tasks_total{{status=\"submitted\"}} {submitted}"
        )?;
        writeln!(
            out,
            "krishiv_tasks_total{{status=\"succeeded\"}} {succeeded}"
        )?;
        writeln!(out, "krishiv_tasks_total{{status=\"failed\"}} {failed}")?;

        let running = self.tasks_running.load(Ordering::Relaxed);
        writeln!(out, "# HELP krishiv_tasks_running Currently running tasks")?;
        writeln!(out, "# TYPE krishiv_tasks_running gauge")?;
        writeln!(out, "krishiv_tasks_running {running}")?;

        let executor_lost = self.executor_lost.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_executor_lost_total Executors marked lost (heartbeat timeout)"
        )?;
        writeln!(out, "# TYPE krishiv_executor_lost_total counter")?;
        writeln!(out, "krishiv_executor_lost_total {executor_lost}")?;

        let shuffle_bytes = self.shuffle_bytes_written.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_shuffle_bytes_written_total Shuffle bytes written"
        )?;
        writeln!(out, "# TYPE krishiv_shuffle_bytes_written_total counter")?;
        writeln!(out, "krishiv_shuffle_bytes_written_total {shuffle_bytes}")?;

        // T19: emit the rest of the shuffle metrics so the Prometheus
        // scrape reflects the same shape Spark's metric system exposes.
        let shuffle_records = self.shuffle_records_written.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_shuffle_records_written_total Shuffle rows written"
        )?;
        writeln!(out, "# TYPE krishiv_shuffle_records_written_total counter")?;
        writeln!(
            out,
            "krishiv_shuffle_records_written_total {shuffle_records}"
        )?;

        let shuffle_read_bytes = self.shuffle_read_bytes.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_shuffle_read_bytes_total Shuffle bytes read"
        )?;
        writeln!(out, "# TYPE krishiv_shuffle_read_bytes_total counter")?;
        writeln!(out, "krishiv_shuffle_read_bytes_total {shuffle_read_bytes}")?;

        let shuffle_read_records = self.shuffle_read_records.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_shuffle_read_records_total Shuffle rows read"
        )?;
        writeln!(out, "# TYPE krishiv_shuffle_read_records_total counter")?;
        writeln!(
            out,
            "krishiv_shuffle_read_records_total {shuffle_read_records}"
        )?;

        let shuffle_write_time_us = self.shuffle_write_time_us.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_shuffle_write_time_us_total Shuffle write time (us)"
        )?;
        writeln!(out, "# TYPE krishiv_shuffle_write_time_us_total counter")?;
        writeln!(
            out,
            "krishiv_shuffle_write_time_us_total {shuffle_write_time_us}"
        )?;

        let shuffle_read_time_us = self.shuffle_read_time_us.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_shuffle_read_time_us_total Shuffle read time (us)"
        )?;
        writeln!(out, "# TYPE krishiv_shuffle_read_time_us_total counter")?;
        writeln!(
            out,
            "krishiv_shuffle_read_time_us_total {shuffle_read_time_us}"
        )?;

        let local_blocks = self.shuffle_local_blocks_fetched.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_shuffle_local_blocks_fetched_total Local shuffle blocks fetched"
        )?;
        writeln!(
            out,
            "# TYPE krishiv_shuffle_local_blocks_fetched_total counter"
        )?;
        writeln!(
            out,
            "krishiv_shuffle_local_blocks_fetched_total {local_blocks}"
        )?;

        let remote_blocks = self.shuffle_remote_blocks_fetched.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_shuffle_remote_blocks_fetched_total Remote shuffle blocks fetched"
        )?;
        writeln!(
            out,
            "# TYPE krishiv_shuffle_remote_blocks_fetched_total counter"
        )?;
        writeln!(
            out,
            "krishiv_shuffle_remote_blocks_fetched_total {remote_blocks}"
        )?;

        let fetch_wait_us = self.shuffle_fetch_wait_time_us.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_shuffle_fetch_wait_time_us_total Shuffle fetch wait time (us)"
        )?;
        writeln!(
            out,
            "# TYPE krishiv_shuffle_fetch_wait_time_us_total counter"
        )?;
        writeln!(
            out,
            "krishiv_shuffle_fetch_wait_time_us_total {fetch_wait_us}"
        )?;

        let queue_depth = self.job_queue_depth.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_job_queue_depth Pending jobs in admission queue"
        )?;
        writeln!(out, "# TYPE krishiv_job_queue_depth gauge")?;
        writeln!(out, "krishiv_job_queue_depth {queue_depth}")?;

        let spill_bytes = self.spill_bytes_total.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_spill_bytes_total Bytes spilled to local disk"
        )?;
        writeln!(out, "# TYPE krishiv_spill_bytes_total counter")?;
        writeln!(out, "krishiv_spill_bytes_total {spill_bytes}")?;

        let spill_files = self.spill_files_total.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_spill_files_total Spill events (spill files written)"
        )?;
        writeln!(out, "# TYPE krishiv_spill_files_total counter")?;
        writeln!(out, "krishiv_spill_files_total {spill_files}")?;

        // Labeled per-operator memory gauge

        let op_mem_entries: BTreeMap<String, u64> = self
            .operator_memory_bytes
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !op_mem_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_operator_memory_bytes Peak memory observed per operator kind"
            )?;
            writeln!(out, "# TYPE krishiv_operator_memory_bytes gauge")?;
            for (operator, bytes) in &op_mem_entries {
                writeln!(
                    out,
                    "krishiv_operator_memory_bytes{{operator=\"{}\"}} {bytes}",
                    escape_label_value(operator)
                )?;
            }
        }

        // Labeled checkpoint epoch gauge — always emit the family so alerting
        // rules have a stable baseline even when no job has registered an epoch.

        let epoch_entries: BTreeMap<String, u64> = self
            .checkpoint_epoch
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        writeln!(
            out,
            "# HELP krishiv_checkpoint_epoch Current committed checkpoint epoch per job"
        )?;
        writeln!(out, "# TYPE krishiv_checkpoint_epoch gauge")?;
        for (job_id, epoch) in &epoch_entries {
            writeln!(
                out,
                "krishiv_checkpoint_epoch{{job_id=\"{}\"}} {epoch}",
                escape_label_value(job_id)
            )?;
        }

        // Labeled watermark gauge

        let wm_entries: BTreeMap<String, i64> = self
            .watermark_ms
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !wm_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_watermark_ms Current global low watermark per streaming job"
            )?;
            writeln!(out, "# TYPE krishiv_watermark_ms gauge")?;
            for (job_id, wm) in &wm_entries {
                writeln!(
                    out,
                    "krishiv_watermark_ms{{job_id=\"{}\"}} {wm}",
                    escape_label_value(job_id)
                )?;
            }
        }

        // Labeled checkpoint epoch counters

        let cp_counter_entries: BTreeMap<String, (u64, u64, u64)> = self
            .checkpoint_epochs
            .iter()
            .map(|e| {
                let v = e.value();
                (
                    e.key().clone(),
                    (
                        v.committed.load(Ordering::Relaxed),
                        v.aborted.load(Ordering::Relaxed),
                        v.failed.load(Ordering::Relaxed),
                    ),
                )
            })
            .collect();
        if !cp_counter_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_checkpoint_epochs_total Checkpoint epochs committed/aborted/failed per job"
            )?;
            writeln!(out, "# TYPE krishiv_checkpoint_epochs_total counter")?;
            for (job_id, (committed, aborted, failed_cp)) in &cp_counter_entries {
                let job = escape_label_value(job_id);
                writeln!(
                    out,
                    "krishiv_checkpoint_epochs_total{{job_id=\"{job}\",status=\"committed\"}} {committed}"
                )?;
                writeln!(
                    out,
                    "krishiv_checkpoint_epochs_total{{job_id=\"{job}\",status=\"aborted\"}} {aborted}"
                )?;
                writeln!(
                    out,
                    "krishiv_checkpoint_epochs_total{{job_id=\"{job}\",status=\"failed\"}} {failed_cp}"
                )?;
            }
        }

        // Labeled task attempt counters

        let ta_entries: BTreeMap<String, (u64, u64, u64, u64)> = self
            .task_attempts
            .iter()
            .map(|e| {
                let v = e.value();
                (
                    e.key().clone(),
                    (
                        v.submitted.load(Ordering::Relaxed),
                        v.succeeded.load(Ordering::Relaxed),
                        v.failed.load(Ordering::Relaxed),
                        v.retrying.load(Ordering::Relaxed),
                    ),
                )
            })
            .collect();
        if !ta_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_task_attempts_total Task attempts per job and stage"
            )?;
            writeln!(out, "# TYPE krishiv_task_attempts_total counter")?;
            for (key, (submitted_ta, succeeded_ta, failed_ta, retrying)) in &ta_entries {
                // key is "job_id:stage_id"
                let (job_id, stage_id) = key.split_once(':').unwrap_or((key.as_str(), ""));
                let job = escape_label_value(job_id);
                let stage = escape_label_value(stage_id);
                writeln!(
                    out,
                    "krishiv_task_attempts_total{{job_id=\"{job}\",stage_id=\"{stage}\",status=\"submitted\"}} {submitted_ta}"
                )?;
                writeln!(
                    out,
                    "krishiv_task_attempts_total{{job_id=\"{job}\",stage_id=\"{stage}\",status=\"succeeded\"}} {succeeded_ta}"
                )?;
                writeln!(
                    out,
                    "krishiv_task_attempts_total{{job_id=\"{job}\",stage_id=\"{stage}\",status=\"failed\"}} {failed_ta}"
                )?;
                writeln!(
                    out,
                    "krishiv_task_attempts_total{{job_id=\"{job}\",stage_id=\"{stage}\",status=\"retrying\"}} {retrying}"
                )?;
            }
        }

        // Labeled executor slots gauge

        let es_entries: BTreeMap<String, u64> = self
            .executor_slots_used
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !es_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_executor_slots_used Task slots in use per executor"
            )?;
            writeln!(out, "# TYPE krishiv_executor_slots_used gauge")?;
            for (executor_id, slots) in &es_entries {
                writeln!(
                    out,
                    "krishiv_executor_slots_used{{executor_id=\"{}\"}} {slots}",
                    escape_label_value(executor_id)
                )?;
            }
        }

        // Labeled source offset lag gauge

        let lag_entries: BTreeMap<String, i64> = self
            .source_offset_lag
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !lag_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_source_offset_lag Source offset lag per job and source"
            )?;
            writeln!(out, "# TYPE krishiv_source_offset_lag gauge")?;
            for (key, lag) in &lag_entries {
                let (job_id, source_id) = key.split_once(':').unwrap_or((key.as_str(), ""));
                writeln!(
                    out,
                    "krishiv_source_offset_lag{{job_id=\"{}\",source_id=\"{}\"}} {lag}",
                    escape_label_value(job_id),
                    escape_label_value(source_id)
                )?;
            }
        }

        // Labeled streaming rows counter

        let sr_entries: BTreeMap<String, u64> = self
            .streaming_rows
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !sr_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_streaming_rows_emitted_total Rows emitted by streaming tasks"
            )?;
            writeln!(out, "# TYPE krishiv_streaming_rows_emitted_total counter")?;
            for (key, rows) in &sr_entries {
                let (job_id, task_id) = key.split_once(':').unwrap_or((key.as_str(), ""));
                writeln!(
                    out,
                    "krishiv_streaming_rows_emitted_total{{job_id=\"{}\",task_id=\"{}\"}} {rows}",
                    escape_label_value(job_id),
                    escape_label_value(task_id)
                )?;
            }
        }

        // Labeled state backend gauges

        let sk_entries: BTreeMap<String, u64> = self
            .state_key_count
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        let sb_entries: BTreeMap<String, u64> = self
            .state_bytes
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !sk_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_state_key_count Key count per state backend"
            )?;
            writeln!(out, "# TYPE krishiv_state_key_count gauge")?;
            for (job_id, count) in &sk_entries {
                writeln!(
                    out,
                    "krishiv_state_key_count{{job_id=\"{}\"}} {count}",
                    escape_label_value(job_id)
                )?;
            }
        }
        if !sb_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_state_bytes Byte size per state backend"
            )?;
            writeln!(out, "# TYPE krishiv_state_bytes gauge")?;
            for (job_id, bytes) in &sb_entries {
                writeln!(
                    out,
                    "krishiv_state_bytes{{job_id=\"{}\"}} {bytes}",
                    escape_label_value(job_id)
                )?;
            }
        }

        // Labeled shuffle partition gauges

        let sp_entries: BTreeMap<String, (u64, u64, u64)> = self
            .shuffle_partitions
            .iter()
            .map(|e| {
                let v = e.value();
                (
                    e.key().clone(),
                    (
                        v.pending.load(Ordering::Relaxed),
                        v.available.load(Ordering::Relaxed),
                        v.failed.load(Ordering::Relaxed),
                    ),
                )
            })
            .collect();
        if !sp_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_shuffle_partitions Shuffle partition counts per job and stage"
            )?;
            writeln!(out, "# TYPE krishiv_shuffle_partitions gauge")?;
            for (key, (pending, available, failed_sp)) in &sp_entries {
                let (job_id, stage_id) = key.split_once(':').unwrap_or((key.as_str(), ""));
                let job = escape_label_value(job_id);
                let stage = escape_label_value(stage_id);
                writeln!(
                    out,
                    "krishiv_shuffle_partitions{{job_id=\"{job}\",stage_id=\"{stage}\",state=\"pending\"}} {pending}"
                )?;
                writeln!(
                    out,
                    "krishiv_shuffle_partitions{{job_id=\"{job}\",stage_id=\"{stage}\",state=\"available\"}} {available}"
                )?;
                writeln!(
                    out,
                    "krishiv_shuffle_partitions{{job_id=\"{job}\",stage_id=\"{stage}\",state=\"failed\"}} {failed_sp}"
                )?;
            }
        }

        // Latency histograms

        render_histogram(
            &mut out,
            "krishiv_grpc_call_duration_seconds",
            "gRPC call duration in seconds",
            "path",
            &self.grpc_call_duration,
        )?;
        render_histogram(
            &mut out,
            "krishiv_checkpoint_commit_duration_seconds",
            "Checkpoint commit duration per phase in seconds",
            "phase",
            &self.checkpoint_commit_duration,
        )?;

        // ── Streaming metrics (Phase 7) ───────────────────────────────────

        render_histogram(
            &mut out,
            "krishiv_source_read_duration_seconds",
            "Source read latency in seconds",
            "source_id",
            &self.source_read_duration,
        )?;

        render_histogram(
            &mut out,
            "krishiv_stream_record_latency_seconds",
            "Streaming ingest-to-emit record latency in seconds (µs-resolution buckets)",
            "job_id",
            &self.stream_record_latency,
        )?;

        let flush_entries: BTreeMap<String, u64> = self
            .output_buffer_flushes
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !flush_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_output_buffer_flushes_total Output buffer flush count by reason"
            )?;
            writeln!(out, "# TYPE krishiv_output_buffer_flushes_total counter")?;
            for (reason, count) in &flush_entries {
                writeln!(
                    out,
                    "krishiv_output_buffer_flushes_total{{reason=\"{}\"}} {count}",
                    escape_label_value(reason)
                )?;
            }
        }

        render_histogram(
            &mut out,
            "krishiv_checkpoint_alignment_duration_seconds",
            "Checkpoint alignment time in seconds",
            "alignment",
            &self.checkpoint_alignment_duration,
        )?;

        let unaligned_entries: BTreeMap<String, u64> = self
            .unaligned_in_flight_bytes
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !unaligned_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_unaligned_in_flight_bytes Unaligned checkpoint in-flight bytes"
            )?;
            writeln!(out, "# TYPE krishiv_unaligned_in_flight_bytes gauge")?;
            for (job, bytes) in &unaligned_entries {
                writeln!(
                    out,
                    "krishiv_unaligned_in_flight_bytes{{job_id=\"{}\"}} {bytes}",
                    escape_label_value(job)
                )?;
            }
        }

        render_histogram(
            &mut out,
            "krishiv_checkpoint_upload_duration_seconds",
            "Checkpoint upload duration in seconds",
            "job_id",
            &self.checkpoint_upload_duration,
        )?;

        render_histogram(
            &mut out,
            "krishiv_restore_duration_seconds",
            "Restore duration in seconds",
            "job_id",
            &self.restore_duration,
        )?;

        let cache_hits: BTreeMap<String, u64> = self
            .state_cache_hits
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !cache_hits.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_state_cache_hits_total State cache hit count"
            )?;
            writeln!(out, "# TYPE krishiv_state_cache_hits_total counter")?;
            for (job, count) in &cache_hits {
                writeln!(
                    out,
                    "krishiv_state_cache_hits_total{{job_id=\"{}\"}} {count}",
                    escape_label_value(job)
                )?;
            }
        }

        let cache_misses: BTreeMap<String, u64> = self
            .state_cache_misses
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !cache_misses.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_state_cache_misses_total State cache miss count"
            )?;
            writeln!(out, "# TYPE krishiv_state_cache_misses_total counter")?;
            for (job, count) in &cache_misses {
                writeln!(
                    out,
                    "krishiv_state_cache_misses_total{{job_id=\"{}\"}} {count}",
                    escape_label_value(job)
                )?;
            }
        }

        let os_entries: BTreeMap<String, u64> = self
            .object_store_requests
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !os_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_object_store_requests_total Object store request count"
            )?;
            writeln!(out, "# TYPE krishiv_object_store_requests_total counter")?;
            for (op, count) in &os_entries {
                writeln!(
                    out,
                    "krishiv_object_store_requests_total{{operation=\"{}\"}} {count}",
                    escape_label_value(op)
                )?;
            }
        }

        render_histogram(
            &mut out,
            "krishiv_sink_prepare_duration_seconds",
            "Sink prepare duration in seconds",
            "sink_id",
            &self.sink_prepare_duration,
        )?;
        render_histogram(
            &mut out,
            "krishiv_sink_commit_duration_seconds",
            "Sink commit duration in seconds",
            "sink_id",
            &self.sink_commit_duration,
        )?;
        render_histogram(
            &mut out,
            "krishiv_sink_abort_duration_seconds",
            "Sink abort duration in seconds",
            "sink_id",
            &self.sink_abort_duration,
        )?;

        let bp_entries: BTreeMap<String, u64> = self
            .backpressure_duration_us
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !bp_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_backpressure_duration_us_total Backpressure duration in microseconds"
            )?;
            writeln!(out, "# TYPE krishiv_backpressure_duration_us_total counter")?;
            for (job, us) in &bp_entries {
                writeln!(
                    out,
                    "krishiv_backpressure_duration_us_total{{job_id=\"{}\"}} {us}",
                    escape_label_value(job)
                )?;
            }
        }

        Ok(out)
    }
}

/// Render a labeled histogram family in Prometheus text format.
///
/// Each entry's per-bucket counts are stored non-cumulatively and accumulated
/// here into the cumulative bucket counts required by the exposition format.
fn render_histogram(
    out: &mut String,
    metric: &str,
    help: &str,
    label: &str,
    map: &dashmap::DashMap<String, KrishivHistogram>,
) -> Result<(), std::fmt::Error> {
    let mut entries = BTreeMap::new();
    for entry in map.iter() {
        let key = entry.key().clone();
        let (count, sum, counts, _) = entry.value().snapshot();
        // Per-metric bucket sets (e.g. the µs-scale stream-record-latency
        // family) carry their own bounds; the default family shares
        // LATENCY_BUCKETS.
        let bounds = entry.value().bucket_bounds();
        entries.insert(key, (count, sum, counts, bounds));
    }
    if entries.is_empty() {
        return Ok(());
    }
    writeln!(out, "# HELP {metric} {help}")?;
    writeln!(out, "# TYPE {metric} histogram")?;
    for (label_value, (count, sum, counts, bounds)) in &entries {
        let value = escape_label_value(label_value);
        writeln!(out, "{metric}_sum{{{label}=\"{value}\"}} {:.6}", sum)?;
        writeln!(out, "{metric}_count{{{label}=\"{value}\"}} {count}")?;
        let mut cumulative = 0u64;
        for (i, &bucket) in bounds.iter().enumerate() {
            cumulative += counts.get(i).copied().unwrap_or(0);
            writeln!(
                out,
                "{metric}_bucket{{{label}=\"{value}\",le=\"{bucket}\"}} {cumulative}"
            )?;
        }
        cumulative += counts.get(bounds.len()).copied().unwrap_or(0);
        writeln!(
            out,
            "{metric}_bucket{{{label}=\"{value}\",le=\"+Inf\"}} {cumulative}"
        )?;
    }
    Ok(())
}

// W3C tracestate propagation

/// Returns the W3C `tracestate` header value for the currently active `tracing` span,
/// or `None` when no span is active or no tracestate is set.
pub fn current_tracestate() -> Option<String> {
    use opentelemetry::trace::TraceContextExt as _;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    let ctx = tracing::Span::current().context();
    let span_ref = ctx.span();
    let span_ctx = span_ref.span_context();
    if !span_ctx.is_valid() {
        return None;
    }
    let state = span_ctx.trace_state();
    if state.header().is_empty() {
        None
    } else {
        Some(state.header().to_string())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn histogram_drops_negative_and_non_finite_durations() {
        let h = KrishivHistogram::default();
        h.observe(-1.0);
        h.observe(f64::NAN);
        h.observe(f64::INFINITY);
        let (count, sum, counts, _) = h.snapshot();
        assert_eq!(count, 0, "negative/NaN/inf observations must be dropped");
        assert_eq!(sum, 0.0);
        assert!(counts.iter().all(|c| *c == 0));
    }

    #[test]
    fn histogram_records_finite_positive_durations() {
        let h = KrishivHistogram::default();
        h.observe(0.003);
        h.observe(0.3);
        let (count, sum, _, _) = h.snapshot();
        assert_eq!(count, 2);
        assert!((sum - 0.303).abs() < 1e-6);
    }

    #[test]
    fn escape_label_value_escapes_special_characters() {
        assert_eq!(escape_label_value("plain"), "plain");
        // backslash -> \\
        assert_eq!(escape_label_value("a\\b"), "a\\\\b");
        // double quote -> \"
        assert_eq!(escape_label_value("a\"b"), "a\\\"b");
        // newline -> \n
        assert_eq!(escape_label_value("a\nb"), "a\\nb");
    }

    #[test]
    fn render_prometheus_escapes_job_id_with_special_characters() {
        let m = KrishivMetrics::default();
        m.set_checkpoint_epoch("job\"evil", 3);
        let body = m.render_prometheus();
        assert!(
            body.contains(r#"krishiv_checkpoint_epoch{job_id="job\"evil"} 3"#),
            "label value with a quote must be escaped, got: {body}"
        );
    }

    #[test]
    fn render_prometheus_escapes_newline_in_labeled_metric() {
        let m = KrishivMetrics::default();
        m.set_executor_slots_used("exec\ninject", 1);
        let body = m.render_prometheus();
        assert!(
            body.contains(r#"krishiv_executor_slots_used{executor_id="exec\ninject"} 1"#),
            "newline in label value must be escaped, got: {body}"
        );
        // The rendered body must not contain a literal newline inside the label.
        assert!(!body.contains("exec\ninject"));
    }

    #[test]
    fn render_prometheus_emits_checkpoint_epoch_family_when_empty() {
        let m = KrishivMetrics::default();
        let body = m.render_prometheus();
        assert!(body.contains("# HELP krishiv_checkpoint_epoch"));
        assert!(body.contains("# TYPE krishiv_checkpoint_epoch gauge"));
    }
}

/// T19: the new shuffle metric methods increment the right counters
/// and the Prometheus scrape surfaces the new fields.
#[test]
fn shuffle_metrics_increment_and_render() {
    let m = KrishivMetrics::default();
    m.add_shuffle_bytes_written(1024);
    m.add_shuffle_records_written(8);
    m.add_shuffle_read_bytes(512);
    m.add_shuffle_read_records(4);
    m.add_shuffle_write_time_us(1234);
    m.add_shuffle_read_time_us(567);
    m.add_shuffle_local_blocks_fetched(2);
    m.add_shuffle_remote_blocks_fetched(1);
    m.add_shuffle_fetch_wait_time_us(890);
    let body = m.render_prometheus();
    for required in [
        "krishiv_shuffle_bytes_written_total 1024",
        "krishiv_shuffle_records_written_total 8",
        "krishiv_shuffle_read_bytes_total 512",
        "krishiv_shuffle_read_records_total 4",
        "krishiv_shuffle_write_time_us_total 1234",
        "krishiv_shuffle_read_time_us_total 567",
        "krishiv_shuffle_local_blocks_fetched_total 2",
        "krishiv_shuffle_remote_blocks_fetched_total 1",
        "krishiv_shuffle_fetch_wait_time_us_total 890",
    ] {
        assert!(
            body.contains(required),
            "Prometheus output missing {required}; body:\n{body}"
        );
    }
}
