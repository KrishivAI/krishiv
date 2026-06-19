use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

// Process metrics rendered in Prometheus text exposition format.

/// Default latency histogram bucket upper bounds, in seconds.
const LATENCY_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
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
        self.counts[bucket_idx].fetch_add(1, Ordering::Relaxed);
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

    // Prometheus rendering

    /// Render Prometheus exposition format for Krishiv counters/gauges.
    ///
    /// Emits valid Prometheus text format: exactly one `# HELP` and `# TYPE` line
    /// per metric family, followed by all labeled samples for that family.  Label
    /// values are escaped per the exposition format specification.
    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(8192);

        // Global (unlabeled) metrics

        let submitted = self.tasks_submitted.load(Ordering::Relaxed);
        let succeeded = self.tasks_succeeded.load(Ordering::Relaxed);
        let failed = self.tasks_failed.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_tasks_total Tasks submitted to the coordinator"
        )
        .unwrap();
        writeln!(out, "# TYPE krishiv_tasks_total counter").unwrap();
        writeln!(
            out,
            "krishiv_tasks_total{{status=\"submitted\"}} {submitted}"
        )
        .unwrap();
        writeln!(
            out,
            "krishiv_tasks_total{{status=\"succeeded\"}} {succeeded}"
        )
        .unwrap();
        writeln!(out, "krishiv_tasks_total{{status=\"failed\"}} {failed}").unwrap();

        let running = self.tasks_running.load(Ordering::Relaxed);
        writeln!(out, "# HELP krishiv_tasks_running Currently running tasks").unwrap();
        writeln!(out, "# TYPE krishiv_tasks_running gauge").unwrap();
        writeln!(out, "krishiv_tasks_running {running}").unwrap();

        let executor_lost = self.executor_lost.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_executor_lost_total Executors marked lost (heartbeat timeout)"
        )
        .unwrap();
        writeln!(out, "# TYPE krishiv_executor_lost_total counter").unwrap();
        writeln!(out, "krishiv_executor_lost_total {executor_lost}").unwrap();

        let shuffle_bytes = self.shuffle_bytes_written.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_shuffle_bytes_written_total Shuffle bytes written"
        )
        .unwrap();
        writeln!(out, "# TYPE krishiv_shuffle_bytes_written_total counter").unwrap();
        writeln!(out, "krishiv_shuffle_bytes_written_total {shuffle_bytes}").unwrap();

        let queue_depth = self.job_queue_depth.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_job_queue_depth Pending jobs in admission queue"
        )
        .unwrap();
        writeln!(out, "# TYPE krishiv_job_queue_depth gauge").unwrap();
        writeln!(out, "krishiv_job_queue_depth {queue_depth}").unwrap();

        let spill_bytes = self.spill_bytes_total.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_spill_bytes_total Bytes spilled to local disk"
        )
        .unwrap();
        writeln!(out, "# TYPE krishiv_spill_bytes_total counter").unwrap();
        writeln!(out, "krishiv_spill_bytes_total {spill_bytes}").unwrap();

        let spill_files = self.spill_files_total.load(Ordering::Relaxed);
        writeln!(
            out,
            "# HELP krishiv_spill_files_total Spill events (spill files written)"
        )
        .unwrap();
        writeln!(out, "# TYPE krishiv_spill_files_total counter").unwrap();
        writeln!(out, "krishiv_spill_files_total {spill_files}").unwrap();

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
            )
            .unwrap();
            writeln!(out, "# TYPE krishiv_operator_memory_bytes gauge").unwrap();
            for (operator, bytes) in &op_mem_entries {
                writeln!(
                    out,
                    "krishiv_operator_memory_bytes{{operator=\"{}\"}} {bytes}",
                    escape_label_value(operator)
                )
                .unwrap();
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
        )
        .unwrap();
        writeln!(out, "# TYPE krishiv_checkpoint_epoch gauge").unwrap();
        for (job_id, epoch) in &epoch_entries {
            writeln!(
                out,
                "krishiv_checkpoint_epoch{{job_id=\"{}\"}} {epoch}",
                escape_label_value(job_id)
            )
            .unwrap();
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
            )
            .unwrap();
            writeln!(out, "# TYPE krishiv_watermark_ms gauge").unwrap();
            for (job_id, wm) in &wm_entries {
                writeln!(
                    out,
                    "krishiv_watermark_ms{{job_id=\"{}\"}} {wm}",
                    escape_label_value(job_id)
                )
                .unwrap();
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
            )
            .unwrap();
            writeln!(out, "# TYPE krishiv_checkpoint_epochs_total counter").unwrap();
            for (job_id, (committed, aborted, failed_cp)) in &cp_counter_entries {
                let job = escape_label_value(job_id);
                writeln!(
                    out,
                    "krishiv_checkpoint_epochs_total{{job_id=\"{job}\",status=\"committed\"}} {committed}"
                )
                .unwrap();
                writeln!(
                    out,
                    "krishiv_checkpoint_epochs_total{{job_id=\"{job}\",status=\"aborted\"}} {aborted}"
                )
                .unwrap();
                writeln!(
                    out,
                    "krishiv_checkpoint_epochs_total{{job_id=\"{job}\",status=\"failed\"}} {failed_cp}"
                )
                .unwrap();
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
            )
            .unwrap();
            writeln!(out, "# TYPE krishiv_task_attempts_total counter").unwrap();
            for (key, (submitted_ta, succeeded_ta, failed_ta, retrying)) in &ta_entries {
                // key is "job_id:stage_id"
                let (job_id, stage_id) = key.split_once(':').unwrap_or((key.as_str(), ""));
                let job = escape_label_value(job_id);
                let stage = escape_label_value(stage_id);
                writeln!(
                    out,
                    "krishiv_task_attempts_total{{job_id=\"{job}\",stage_id=\"{stage}\",status=\"submitted\"}} {submitted_ta}"
                )
                .unwrap();
                writeln!(
                    out,
                    "krishiv_task_attempts_total{{job_id=\"{job}\",stage_id=\"{stage}\",status=\"succeeded\"}} {succeeded_ta}"
                )
                .unwrap();
                writeln!(
                    out,
                    "krishiv_task_attempts_total{{job_id=\"{job}\",stage_id=\"{stage}\",status=\"failed\"}} {failed_ta}"
                )
                .unwrap();
                writeln!(
                    out,
                    "krishiv_task_attempts_total{{job_id=\"{job}\",stage_id=\"{stage}\",status=\"retrying\"}} {retrying}"
                )
                .unwrap();
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
            )
            .unwrap();
            writeln!(out, "# TYPE krishiv_executor_slots_used gauge").unwrap();
            for (executor_id, slots) in &es_entries {
                writeln!(
                    out,
                    "krishiv_executor_slots_used{{executor_id=\"{}\"}} {slots}",
                    escape_label_value(executor_id)
                )
                .unwrap();
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
            )
            .unwrap();
            writeln!(out, "# TYPE krishiv_source_offset_lag gauge").unwrap();
            for (key, lag) in &lag_entries {
                let (job_id, source_id) = key.split_once(':').unwrap_or((key.as_str(), ""));
                writeln!(
                    out,
                    "krishiv_source_offset_lag{{job_id=\"{}\",source_id=\"{}\"}} {lag}",
                    escape_label_value(job_id),
                    escape_label_value(source_id)
                )
                .unwrap();
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
            )
            .unwrap();
            writeln!(out, "# TYPE krishiv_streaming_rows_emitted_total counter").unwrap();
            for (key, rows) in &sr_entries {
                let (job_id, task_id) = key.split_once(':').unwrap_or((key.as_str(), ""));
                writeln!(
                    out,
                    "krishiv_streaming_rows_emitted_total{{job_id=\"{}\",task_id=\"{}\"}} {rows}",
                    escape_label_value(job_id),
                    escape_label_value(task_id)
                )
                .unwrap();
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
            )
            .unwrap();
            writeln!(out, "# TYPE krishiv_state_key_count gauge").unwrap();
            for (job_id, count) in &sk_entries {
                writeln!(
                    out,
                    "krishiv_state_key_count{{job_id=\"{}\"}} {count}",
                    escape_label_value(job_id)
                )
                .unwrap();
            }
        }
        if !sb_entries.is_empty() {
            writeln!(
                out,
                "# HELP krishiv_state_bytes Byte size per state backend"
            )
            .unwrap();
            writeln!(out, "# TYPE krishiv_state_bytes gauge").unwrap();
            for (job_id, bytes) in &sb_entries {
                writeln!(
                    out,
                    "krishiv_state_bytes{{job_id=\"{}\"}} {bytes}",
                    escape_label_value(job_id)
                )
                .unwrap();
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
            )
            .unwrap();
            writeln!(out, "# TYPE krishiv_shuffle_partitions gauge").unwrap();
            for (key, (pending, available, failed_sp)) in &sp_entries {
                let (job_id, stage_id) = key.split_once(':').unwrap_or((key.as_str(), ""));
                let job = escape_label_value(job_id);
                let stage = escape_label_value(stage_id);
                writeln!(
                    out,
                    "krishiv_shuffle_partitions{{job_id=\"{job}\",stage_id=\"{stage}\",state=\"pending\"}} {pending}"
                )
                .unwrap();
                writeln!(
                    out,
                    "krishiv_shuffle_partitions{{job_id=\"{job}\",stage_id=\"{stage}\",state=\"available\"}} {available}"
                )
                .unwrap();
                writeln!(
                    out,
                    "krishiv_shuffle_partitions{{job_id=\"{job}\",stage_id=\"{stage}\",state=\"failed\"}} {failed_sp}"
                )
                .unwrap();
            }
        }

        // Latency histograms

        render_histogram(
            &mut out,
            "krishiv_grpc_call_duration_seconds",
            "gRPC call duration in seconds",
            "path",
            &self.grpc_call_duration,
        );
        render_histogram(
            &mut out,
            "krishiv_checkpoint_commit_duration_seconds",
            "Checkpoint commit duration per phase in seconds",
            "phase",
            &self.checkpoint_commit_duration,
        );

        out
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
) {
    let mut entries = BTreeMap::new();
    for entry in map.iter() {
        let key = entry.key().clone();
        let (count, sum, counts, _) = entry.value().snapshot();
        entries.insert(key, (count, sum, counts));
    }
    if entries.is_empty() {
        return;
    }
    writeln!(out, "# HELP {metric} {help}").unwrap();
    writeln!(out, "# TYPE {metric} histogram").unwrap();
    for (label_value, (count, sum, counts)) in &entries {
        let value = escape_label_value(label_value);
        writeln!(out, "{metric}_sum{{{label}=\"{value}\"}} {:.6}", sum).unwrap();
        writeln!(out, "{metric}_count{{{label}=\"{value}\"}} {count}").unwrap();
        let mut cumulative = 0u64;
        for (i, &bucket) in LATENCY_BUCKETS.iter().enumerate() {
            cumulative += counts.get(i).copied().unwrap_or(0);
            writeln!(
                out,
                "{metric}_bucket{{{label}=\"{value}\",le=\"{bucket}\"}} {cumulative}"
            )
            .unwrap();
        }
        cumulative += counts.get(LATENCY_BUCKETS.len()).copied().unwrap_or(0);
        writeln!(
            out,
            "{metric}_bucket{{{label}=\"{value}\",le=\"+Inf\"}} {cumulative}"
        )
        .unwrap();
    }
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
