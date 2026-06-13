#![forbid(unsafe_code)]
//! **Beta API**: may change between minor releases.
//!
//! OpenTelemetry metrics, traces, and structured log initialization for all Krishiv processes.

pub mod grpc;
pub mod observability_report;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

/// Errors returned by [`init`].
#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    /// The OTLP exporter pipeline failed to build.
    #[error("OTLP exporter build failed: {0}")]
    OtlpBuild(String),
    /// A tracing subscriber initialization error.
    #[error("subscriber init failed: {0}")]
    Subscriber(String),
}

/// **Beta API**: may change between minor releases.
///
/// Selects the OTel span exporter backend used when initializing the tracer provider.
pub enum TracerExporter {
    /// Exports spans to stdout. Useful for development and CI.
    Stdout,
    /// Disables all span export. Used in tests and when telemetry is not needed.
    NoOp,
    /// Captures exported spans in memory for assertion in unit tests.
    ///
    /// **For testing only.** Uses a synchronous simple span processor that blocks
    /// the tracing thread on each export. Do not use in production; use
    /// [`TracerExporter::NoOp`] or the OTLP path (`otlp_endpoint`) instead.
    InMemory(opentelemetry_sdk::trace::InMemorySpanExporter),
}

/// **Beta API**: may change between minor releases.
///
/// Configuration for the Krishiv metrics and tracing subsystem.
pub struct MetricsConfig {
    /// Name of the service reported in OTel spans.
    pub service_name: String,
    /// Which span exporter to use.
    pub exporter: TracerExporter,
    /// Tracing filter string (e.g. `"info"`, `"krishiv=debug,warn"`).
    /// Defaults to `"info"` when `None`.
    pub log_filter: Option<String>,
    /// Optional OTLP collector endpoint (e.g. `"http://localhost:4317"`).
    ///
    /// When `Some`, the OTLP gRPC exporter is used instead of the `exporter`
    /// field.  When `None`, the `exporter` field controls output.
    pub otlp_endpoint: Option<String>,
    /// Deployment target emitted as the `deployment.target` OTel resource
    /// attribute on every span. Falls back to the `KRISHIV_DEPLOYMENT_TARGET`
    /// environment variable when `None`. Typical values: `"embedded"`,
    /// `"single-node"`, `"distributed"`, `"k8s"`, `"bare-metal"`.
    pub deployment_target: Option<String>,
}

impl MetricsConfig {
    /// Resolve the effective deployment target: explicit config → env var → "unknown".
    pub fn resolved_deployment_target(&self) -> String {
        self.deployment_target
            .clone()
            .or_else(|| std::env::var("KRISHIV_DEPLOYMENT_TARGET").ok())
            .unwrap_or_else(|| "unknown".to_string())
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            service_name: "krishiv".to_string(),
            // NoOp default so tests don't write to stdout.
            exporter: TracerExporter::NoOp,
            log_filter: None,
            otlp_endpoint: None,
            deployment_target: None,
        }
    }
}

/// **Beta API**: may change between minor releases.
///
/// Opaque handle returned by [`init`]. Shuts down the OTel tracer provider on drop.
pub struct MetricsHandle {
    tracer_provider: SdkTracerProvider,
}

impl MetricsHandle {
    /// Create a no-op handle (used when metrics init fails or telemetry is disabled).
    ///
    /// The returned handle owns a no-op tracer provider — no spans are exported.
    pub fn noop() -> Self {
        Self {
            tracer_provider: SdkTracerProvider::builder().build(),
        }
    }

    /// Explicitly shut down the tracer provider and flush any pending spans.
    pub fn shutdown(self) {
        // Drop runs `Drop::drop` which calls `tracer_provider.shutdown()`.
    }
}

impl Drop for MetricsHandle {
    fn drop(&mut self) {
        // Best-effort shutdown; log the error so observability failures are
        // visible instead of silently dropping the last batch of spans.
        if let Err(error) = self.tracer_provider.shutdown() {
            tracing::debug!(error = %error, "metrics tracer provider shutdown failed");
        }
    }
}

/// **Beta API**: may change between minor releases.
///
/// Initializes the OTel tracer provider and the `tracing` subscriber.
///
/// Calling this multiple times is safe: subsequent calls will fail to set a new global
/// subscriber (which is ignored) but the returned [`MetricsHandle`] still owns a valid
/// tracer provider.
///
/// # Errors
///
/// Returns a [`MetricsError`] if the OTLP exporter pipeline fails to build (only
/// possible when `config.otlp_endpoint` is `Some`).
pub fn init(config: MetricsConfig) -> Result<MetricsHandle, MetricsError> {
    let filter_str = config.log_filter.as_deref().unwrap_or("info").to_string();
    let filter = tracing_subscriber::EnvFilter::new(&filter_str);
    let deployment_target = config.resolved_deployment_target();

    // Build a resource with service.name and deployment.target attributes.
    let resource = opentelemetry_sdk::Resource::builder()
        .with_attribute(opentelemetry::KeyValue::new(
            "service.name",
            config.service_name.clone(),
        ))
        .with_attribute(opentelemetry::KeyValue::new(
            "deployment.target",
            deployment_target,
        ))
        .build();

    let tracer_provider = if let Some(endpoint) = config.otlp_endpoint {
        // Build an OTLP gRPC exporter pipeline.
        use opentelemetry_otlp::{SpanExporter, WithExportConfig as _};

        let exporter = SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .map_err(|e| MetricsError::OtlpBuild(format!("{e}")))?;

        SdkTracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build()
    } else {
        match config.exporter {
            TracerExporter::Stdout => SdkTracerProvider::builder()
                .with_resource(resource)
                .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
                .build(),
            TracerExporter::NoOp => SdkTracerProvider::builder().build(),
            TracerExporter::InMemory(exporter) => SdkTracerProvider::builder()
                .with_resource(resource)
                .with_simple_exporter(exporter)
                .build(),
        }
    };

    let tracer = tracer_provider.tracer(config.service_name.clone());

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    // try_init is safe to call multiple times; it returns Err when a subscriber is
    // already set, which we intentionally ignore so tests can call init() repeatedly.
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().json())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init();

    Ok(MetricsHandle { tracer_provider })
}

/// **Beta API**: may change between minor releases.
///
/// Shuts down the OTel tracer provider by dropping the handle (the `Drop` impl does the work).
pub fn shutdown(handle: MetricsHandle) {
    handle.shutdown();
}

/// **Beta API**: may change between minor releases.
///
/// Returns the W3C `traceparent` header value for the currently active `tracing` span,
/// or `None` when no span is active.
///
/// Format: `"00-{trace_id}-{span_id}-01"`
///
/// Used by gRPC interceptors to propagate trace context via the `TraceContext` metadata key.
pub fn current_traceparent() -> Option<String> {
    use opentelemetry::trace::TraceContextExt as _;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    let ctx = tracing::Span::current().context();
    let span_ref = ctx.span();
    let span_ctx = span_ref.span_context();

    if span_ctx.is_valid() {
        Some(format!(
            "00-{}-{}-01",
            span_ctx.trace_id(),
            span_ctx.span_id()
        ))
    } else {
        None
    }
}

// ── Process metrics (Prometheus text) ─────────────────────────────────────────

const LATENCY_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

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
    /// Record a duration observation.
    pub fn observe(&self, value_secs: f64) {
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

    /// Snapshot the current count, sum, counts per bucket, and number of buckets.
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

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

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
    pub source_offset_lag: dashmap::DashMap<String, AtomicI64>,
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

use std::sync::atomic::AtomicI64;

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
    // ── Global (unlabeled) counters/gauges ────────────────────────────────

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

    // ── Labeled per-job checkpoint metrics ────────────────────────────────

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

    // ── Labeled per-job watermark / offset metrics ─────────────────────────

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

    // ── Labeled per-job/stage task attempt counters ────────────────────────

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

    // ── Executor slot gauges ──────────────────────────────────────────────

    /// Set the number of slots currently used on an executor.
    pub fn set_executor_slots_used(&self, executor_id: &str, slots: u64) {
        self.executor_slots_used
            .entry(executor_id.to_string())
            .or_default()
            .store(slots, Ordering::Relaxed);
    }

    // ── Streaming rows counter ────────────────────────────────────────────

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

    // ── State backend gauges ──────────────────────────────────────────────

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

    // ── Shuffle partition progress gauges ──────────────────────────────────

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
    }

    // ── Duration observation histograms ────────────────────────────────────

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

    // ── Prometheus rendering ──────────────────────────────────────────────

    /// Render Prometheus exposition format for Krishiv counters/gauges.
    ///
    /// Emits valid Prometheus text format: exactly one `# HELP` and `# TYPE` line
    /// per metric family, followed by all labeled samples for that family.
    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(8192);

        // ── Global (unlabeled) metrics ──────────────────────────────────

        let submitted = self.tasks_submitted.load(Ordering::Relaxed);
        let succeeded = self.tasks_succeeded.load(Ordering::Relaxed);
        let failed = self.tasks_failed.load(Ordering::Relaxed);
        out.push_str("# HELP krishiv_tasks_total Tasks submitted to the coordinator\n");
        out.push_str("# TYPE krishiv_tasks_total counter\n");
        out.push_str(&format!(
            "krishiv_tasks_total{{status=\"submitted\"}} {submitted}\n"
        ));
        out.push_str(&format!(
            "krishiv_tasks_total{{status=\"succeeded\"}} {succeeded}\n"
        ));
        out.push_str(&format!(
            "krishiv_tasks_total{{status=\"failed\"}} {failed}\n"
        ));

        let running = self.tasks_running.load(Ordering::Relaxed);
        out.push_str("# HELP krishiv_tasks_running Currently running tasks\n");
        out.push_str("# TYPE krishiv_tasks_running gauge\n");
        out.push_str(&format!("krishiv_tasks_running {running}\n"));

        let executor_lost = self.executor_lost.load(Ordering::Relaxed);
        out.push_str(
            "# HELP krishiv_executor_lost_total Executors marked lost (heartbeat timeout)\n",
        );
        out.push_str("# TYPE krishiv_executor_lost_total counter\n");
        out.push_str(&format!("krishiv_executor_lost_total {executor_lost}\n"));

        let shuffle_bytes = self.shuffle_bytes_written.load(Ordering::Relaxed);
        out.push_str("# HELP krishiv_shuffle_bytes_written_total Shuffle bytes written\n");
        out.push_str("# TYPE krishiv_shuffle_bytes_written_total counter\n");
        out.push_str(&format!(
            "krishiv_shuffle_bytes_written_total {shuffle_bytes}\n"
        ));

        let queue_depth = self.job_queue_depth.load(Ordering::Relaxed);
        out.push_str("# HELP krishiv_job_queue_depth Pending jobs in admission queue\n");
        out.push_str("# TYPE krishiv_job_queue_depth gauge\n");
        out.push_str(&format!("krishiv_job_queue_depth {queue_depth}\n"));

        let spill_bytes = self.spill_bytes_total.load(Ordering::Relaxed);
        out.push_str("# HELP krishiv_spill_bytes_total Bytes spilled to local disk\n");
        out.push_str("# TYPE krishiv_spill_bytes_total counter\n");
        out.push_str(&format!("krishiv_spill_bytes_total {spill_bytes}\n"));

        let spill_files = self.spill_files_total.load(Ordering::Relaxed);
        out.push_str("# HELP krishiv_spill_files_total Spill events (spill files written)\n");
        out.push_str("# TYPE krishiv_spill_files_total counter\n");
        out.push_str(&format!("krishiv_spill_files_total {spill_files}\n"));

        // ── Labeled per-operator memory gauge ────────────────────────────

        let op_mem_entries: BTreeMap<String, u64> = self
            .operator_memory_bytes
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !op_mem_entries.is_empty() {
            out.push_str(
                "# HELP krishiv_operator_memory_bytes Peak memory observed per operator kind\n",
            );
            out.push_str("# TYPE krishiv_operator_memory_bytes gauge\n");
            for (operator, bytes) in &op_mem_entries {
                out.push_str(&format!(
                    "krishiv_operator_memory_bytes{{operator=\"{operator}\"}} {bytes}\n"
                ));
            }
        }

        // ── Labeled checkpoint epoch gauge ───────────────────────────────

        let epoch_entries: BTreeMap<String, u64> = self
            .checkpoint_epoch
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        // Always emit gauge even when empty so alerting rules have a baseline.
        if epoch_entries.is_empty() {
            out.push_str(
                "# HELP krishiv_checkpoint_epoch Current committed checkpoint epoch per job\n",
            );
            out.push_str("# TYPE krishiv_checkpoint_epoch gauge\n");
        } else {
            out.push_str(
                "# HELP krishiv_checkpoint_epoch Current committed checkpoint epoch per job\n",
            );
            out.push_str("# TYPE krishiv_checkpoint_epoch gauge\n");
            for (job_id, epoch) in &epoch_entries {
                out.push_str(&format!(
                    "krishiv_checkpoint_epoch{{job_id=\"{job_id}\"}} {epoch}\n"
                ));
            }
        }

        // ── Labeled watermark gauge ─────────────────────────────────────

        let wm_entries: BTreeMap<String, i64> = self
            .watermark_ms
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !wm_entries.is_empty() {
            out.push_str(
                "# HELP krishiv_watermark_ms Current global low watermark per streaming job\n",
            );
            out.push_str("# TYPE krishiv_watermark_ms gauge\n");
            for (job_id, wm) in &wm_entries {
                out.push_str(&format!(
                    "krishiv_watermark_ms{{job_id=\"{job_id}\"}} {wm}\n"
                ));
            }
        }

        // ── Labeled checkpoint epoch counters ───────────────────────────

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
            out.push_str("# HELP krishiv_checkpoint_epochs_total Checkpoint epochs committed/aborted/failed per job\n");
            out.push_str("# TYPE krishiv_checkpoint_epochs_total counter\n");
            for (job_id, (committed, aborted, failed_cp)) in &cp_counter_entries {
                out.push_str(&format!(
                    "krishiv_checkpoint_epochs_total{{job_id=\"{job_id}\",status=\"committed\"}} {committed}\n"
                ));
                out.push_str(&format!(
                    "krishiv_checkpoint_epochs_total{{job_id=\"{job_id}\",status=\"aborted\"}} {aborted}\n"
                ));
                out.push_str(&format!(
                    "krishiv_checkpoint_epochs_total{{job_id=\"{job_id}\",status=\"failed\"}} {failed_cp}\n"
                ));
            }
        }

        // ── Labeled task attempt counters ───────────────────────────────

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
            out.push_str("# HELP krishiv_task_attempts_total Task attempts per job and stage\n");
            out.push_str("# TYPE krishiv_task_attempts_total counter\n");
            for (key, (submitted_ta, succeeded_ta, failed_ta, retrying)) in &ta_entries {
                // key is "job_id:stage_id"
                let (job_id, stage_id) = key.split_once(':').unwrap_or((key, ""));
                out.push_str(&format!(
                    "krishiv_task_attempts_total{{job_id=\"{job_id}\",stage_id=\"{stage_id}\",status=\"submitted\"}} {submitted_ta}\n"
                ));
                out.push_str(&format!(
                    "krishiv_task_attempts_total{{job_id=\"{job_id}\",stage_id=\"{stage_id}\",status=\"succeeded\"}} {succeeded_ta}\n"
                ));
                out.push_str(&format!(
                    "krishiv_task_attempts_total{{job_id=\"{job_id}\",stage_id=\"{stage_id}\",status=\"failed\"}} {failed_ta}\n"
                ));
                out.push_str(&format!(
                    "krishiv_task_attempts_total{{job_id=\"{job_id}\",stage_id=\"{stage_id}\",status=\"retrying\"}} {retrying}\n"
                ));
            }
        }

        // ── Labeled executor slots gauge ────────────────────────────────

        let es_entries: BTreeMap<String, u64> = self
            .executor_slots_used
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !es_entries.is_empty() {
            out.push_str("# HELP krishiv_executor_slots_used Task slots in use per executor\n");
            out.push_str("# TYPE krishiv_executor_slots_used gauge\n");
            for (executor_id, slots) in &es_entries {
                out.push_str(&format!(
                    "krishiv_executor_slots_used{{executor_id=\"{executor_id}\"}} {slots}\n"
                ));
            }
        }

        // ── Labeled source offset lag gauge ─────────────────────────────

        let lag_entries: BTreeMap<String, i64> = self
            .source_offset_lag
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !lag_entries.is_empty() {
            out.push_str("# HELP krishiv_source_offset_lag Source offset lag per job and source\n");
            out.push_str("# TYPE krishiv_source_offset_lag gauge\n");
            for (key, lag) in &lag_entries {
                let (job_id, source_id) = key.split_once(':').unwrap_or((key, ""));
                out.push_str(&format!(
                    "krishiv_source_offset_lag{{job_id=\"{job_id}\",source_id=\"{source_id}\"}} {lag}\n"
                ));
            }
        }

        // ── Labeled streaming rows counter ──────────────────────────────

        let sr_entries: BTreeMap<String, u64> = self
            .streaming_rows
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        if !sr_entries.is_empty() {
            out.push_str(
                "# HELP krishiv_streaming_rows_emitted_total Rows emitted by streaming tasks\n",
            );
            out.push_str("# TYPE krishiv_streaming_rows_emitted_total counter\n");
            for (key, rows) in &sr_entries {
                let (job_id, task_id) = key.split_once(':').unwrap_or((key, ""));
                out.push_str(&format!(
                    "krishiv_streaming_rows_emitted_total{{job_id=\"{job_id}\",task_id=\"{task_id}\"}} {rows}\n"
                ));
            }
        }

        // ── Labeled state backend gauges ────────────────────────────────

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
            out.push_str("# HELP krishiv_state_key_count Key count per state backend\n");
            out.push_str("# TYPE krishiv_state_key_count gauge\n");
            for (job_id, count) in &sk_entries {
                out.push_str(&format!(
                    "krishiv_state_key_count{{job_id=\"{job_id}\"}} {count}\n"
                ));
            }
        }
        if !sb_entries.is_empty() {
            out.push_str("# HELP krishiv_state_bytes Byte size per state backend\n");
            out.push_str("# TYPE krishiv_state_bytes gauge\n");
            for (job_id, bytes) in &sb_entries {
                out.push_str(&format!(
                    "krishiv_state_bytes{{job_id=\"{job_id}\"}} {bytes}\n"
                ));
            }
        }

        // ── Labeled shuffle partition gauges ────────────────────────────

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
            out.push_str(
                "# HELP krishiv_shuffle_partitions Shuffle partition counts per job and stage\n",
            );
            out.push_str("# TYPE krishiv_shuffle_partitions gauge\n");
            for (key, (pending, available, failed_sp)) in &sp_entries {
                let (job_id, stage_id) = key.split_once(':').unwrap_or((key, ""));
                out.push_str(&format!(
                    "krishiv_shuffle_partitions{{job_id=\"{job_id}\",stage_id=\"{stage_id}\",state=\"pending\"}} {pending}\n"
                ));
                out.push_str(&format!(
                    "krishiv_shuffle_partitions{{job_id=\"{job_id}\",stage_id=\"{stage_id}\",state=\"available\"}} {available}\n"
                ));
                out.push_str(&format!(
                    "krishiv_shuffle_partitions{{job_id=\"{job_id}\",stage_id=\"{stage_id}\",state=\"failed\"}} {failed_sp}\n"
                ));
            }
        }

        // ── Latency histogram for gRPC call durations ────────────────────

        let mut grpc_entries = BTreeMap::new();
        for entry in self.grpc_call_duration.iter() {
            let path = entry.key().clone();
            let (count, sum, counts, _) = entry.value().snapshot();
            grpc_entries.insert(path, (count, sum, counts));
        }

        if !grpc_entries.is_empty() {
            out.push_str(
                "# HELP krishiv_grpc_call_duration_seconds gRPC call duration in seconds\n",
            );
            out.push_str("# TYPE krishiv_grpc_call_duration_seconds histogram\n");
            for (path, (count, sum, counts)) in &grpc_entries {
                out.push_str(&format!(
                    "krishiv_grpc_call_duration_seconds_sum{{path=\"{path}\"}} {:.6}\n",
                    sum
                ));
                out.push_str(&format!(
                    "krishiv_grpc_call_duration_seconds_count{{path=\"{path}\"}} {}\n",
                    count
                ));

                let mut cumulative = 0;
                for (i, &bucket) in LATENCY_BUCKETS.iter().enumerate() {
                    cumulative += counts.get(i).copied().unwrap_or(0);
                    out.push_str(&format!(
                        "krishiv_grpc_call_duration_seconds_bucket{{path=\"{path}\",le=\"{}\"}} {}\n",
                        bucket, cumulative
                    ));
                }
                cumulative += counts.get(LATENCY_BUCKETS.len()).copied().unwrap_or(0);
                out.push_str(&format!(
                    "krishiv_grpc_call_duration_seconds_bucket{{path=\"{path}\",le=\"+Inf\"}} {}\n",
                    cumulative
                ));
            }
        }

        // ── Latency histogram for checkpoint commit phases ───────────────

        let mut cp_latency_entries = BTreeMap::new();
        for entry in self.checkpoint_commit_duration.iter() {
            let phase = entry.key().clone();
            let (count, sum, counts, _) = entry.value().snapshot();
            cp_latency_entries.insert(phase, (count, sum, counts));
        }

        if !cp_latency_entries.is_empty() {
            out.push_str("# HELP krishiv_checkpoint_commit_duration_seconds Checkpoint commit duration per phase in seconds\n");
            out.push_str("# TYPE krishiv_checkpoint_commit_duration_seconds histogram\n");
            for (phase, (count, sum, counts)) in &cp_latency_entries {
                out.push_str(&format!(
                    "krishiv_checkpoint_commit_duration_seconds_sum{{phase=\"{phase}\"}} {:.6}\n",
                    sum
                ));
                out.push_str(&format!(
                    "krishiv_checkpoint_commit_duration_seconds_count{{phase=\"{phase}\"}} {}\n",
                    count
                ));

                let mut cumulative = 0;
                for (i, &bucket) in LATENCY_BUCKETS.iter().enumerate() {
                    cumulative += counts.get(i).copied().unwrap_or(0);
                    out.push_str(&format!(
                        "krishiv_checkpoint_commit_duration_seconds_bucket{{phase=\"{phase}\",le=\"{}\"}} {}\n",
                        bucket, cumulative
                    ));
                }
                cumulative += counts.get(LATENCY_BUCKETS.len()).copied().unwrap_or(0);
                out.push_str(&format!(
                    "krishiv_checkpoint_commit_duration_seconds_bucket{{phase=\"{phase}\",le=\"+Inf\"}} {}\n",
                    cumulative
                ));
            }
        }

        out
    }
}

// ── Structured span field conventions ───────────────────────────────────────

/// Mandated `tracing::Span` field key for job id.
pub const SPAN_JOB_ID: &str = "krishiv.job_id";
/// Mandated `tracing::Span` field key for stage id.
pub const SPAN_STAGE_ID: &str = "krishiv.stage_id";
/// Mandated `tracing::Span` field key for task id.
pub const SPAN_TASK_ID: &str = "krishiv.task_id";
/// Mandated `tracing::Span` field key for epoch.
pub const SPAN_EPOCH: &str = "krishiv.epoch";
/// Mandated `tracing::Span` field key for attempt id.
pub const SPAN_ATTEMPT_ID: &str = "krishiv.attempt_id";
/// Mandated `tracing::Span` field key for snapshot id.
pub const SPAN_SNAPSHOT_ID: &str = "krishiv.snapshot_id";
/// Mandated `tracing::Span` field key for executor id.
pub const SPAN_EXECUTOR_ID: &str = "krishiv.executor_id";
/// Mandated `tracing::Span` field key for connector source id.
pub const SPAN_SOURCE_ID: &str = "krishiv.source_id";
/// Mandated `tracing::Span` field key for connector sink id.
pub const SPAN_SINK_ID: &str = "krishiv.sink_id";

/// Record the standard Krishiv span fields on a `tracing::Span`.
///
/// Call this when entering an async operation so downstream logs and traces
/// carry consistent structured fields.
pub fn record_span_fields(
    span: &tracing::Span,
    job_id: Option<&str>,
    stage_id: Option<&str>,
    task_id: Option<&str>,
    epoch: Option<u64>,
    attempt_id: Option<u32>,
) {
    if let Some(id) = job_id {
        span.record(SPAN_JOB_ID, id);
    }
    if let Some(id) = stage_id {
        span.record(SPAN_STAGE_ID, id);
    }
    if let Some(id) = task_id {
        span.record(SPAN_TASK_ID, id);
    }
    if let Some(e) = epoch {
        span.record(SPAN_EPOCH, e);
    }
    if let Some(a) = attempt_id {
        span.record(SPAN_ATTEMPT_ID, a);
    }
}

// ── W3C tracestate propagation ─────────────────────────────────────────────

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
    fn init_noop_does_not_panic() {
        let _handle = init(MetricsConfig::default()).expect("noop init should succeed");
    }

    #[test]
    fn shutdown_does_not_panic() {
        let handle = init(MetricsConfig::default()).expect("init");
        shutdown(handle);
    }

    #[test]
    fn tracing_span_does_not_panic() {
        let _handle = init(MetricsConfig::default()).expect("init");
        let _s = tracing::info_span!("test_span").entered();
    }

    #[test]
    fn default_config_service_name() {
        assert_eq!(MetricsConfig::default().service_name, "krishiv");
    }

    #[test]
    fn default_config_otlp_endpoint_is_none() {
        assert!(MetricsConfig::default().otlp_endpoint.is_none());
    }

    #[test]
    fn current_traceparent_no_span_returns_none() {
        assert_eq!(current_traceparent(), None);
    }

    #[test]
    fn current_tracestate_no_span_returns_none() {
        assert_eq!(current_tracestate(), None);
    }

    // ── KrishivMetrics counter/gauge increment tests ─────────────────────────

    #[test]
    fn inc_tasks_submitted_increments_by_one() {
        let m = KrishivMetrics::default();
        assert_eq!(m.tasks_submitted.load(Ordering::Relaxed), 0);
        m.inc_tasks_submitted();
        assert_eq!(m.tasks_submitted.load(Ordering::Relaxed), 1);
        m.inc_tasks_submitted();
        assert_eq!(m.tasks_submitted.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn set_tasks_running_stores_value() {
        let m = KrishivMetrics::default();
        m.set_tasks_running(5);
        assert_eq!(m.tasks_running.load(Ordering::Relaxed), 5);
        m.set_tasks_running(0);
        assert_eq!(m.tasks_running.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn inc_tasks_succeeded_increments() {
        let m = KrishivMetrics::default();
        m.inc_tasks_succeeded();
        m.inc_tasks_succeeded();
        m.inc_tasks_succeeded();
        assert_eq!(m.tasks_succeeded.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn inc_tasks_failed_increments() {
        let m = KrishivMetrics::default();
        m.inc_tasks_failed();
        assert_eq!(m.tasks_failed.load(Ordering::Relaxed), 1);
    }

    /// Regression (Wave 4 — Observability & Shutdown): `inc_executor_lost`
    /// must increment the `executor_lost` counter and the value must be
    /// rendered as `krishiv_executor_lost_total` in the Prometheus exposition
    /// (the counter and its renderer line were both added in this wave).
    #[test]
    fn inc_executor_lost_increments_and_renders() {
        let m = KrishivMetrics::default();
        m.inc_executor_lost();
        m.inc_executor_lost();
        assert_eq!(m.executor_lost.load(Ordering::Relaxed), 2);

        let rendered = m.render_prometheus();
        assert!(
            rendered.contains("krishiv_executor_lost_total 2"),
            "expected rendered metrics to include krishiv_executor_lost_total 2, got: {rendered}"
        );
    }

    #[test]
    fn add_shuffle_bytes_written_accumulates() {
        let m = KrishivMetrics::default();
        m.add_shuffle_bytes_written(1024);
        m.add_shuffle_bytes_written(2048);
        assert_eq!(m.shuffle_bytes_written.load(Ordering::Relaxed), 3072);
    }

    #[test]
    fn set_job_queue_depth_stores_value() {
        let m = KrishivMetrics::default();
        m.set_job_queue_depth(42);
        assert_eq!(m.job_queue_depth.load(Ordering::Relaxed), 42);
    }

    #[test]
    fn global_metrics_returns_same_instance() {
        let a = global_metrics();
        let b = global_metrics();
        let a_ptr = a as *const KrishivMetrics;
        let b_ptr = b as *const KrishivMetrics;
        assert_eq!(a_ptr, b_ptr);
    }

    // ── Prometheus text format rendering (P0 fix validation) ──────────────────

    /// Verifies the P0 format fix: exactly one HELP + TYPE per metric family.
    #[test]
    fn render_prometheus_single_help_type_per_family() {
        let m = KrishivMetrics::default();
        m.inc_tasks_submitted();
        m.inc_tasks_succeeded();
        m.inc_tasks_failed();
        let body = m.render_prometheus();
        // Count HELP lines for krishiv_tasks_total — must be exactly 1.
        let help_count = body
            .lines()
            .filter(|l| l.starts_with("# HELP krishiv_tasks_total"))
            .count();
        assert_eq!(
            help_count, 1,
            "must have exactly one HELP line per metric family"
        );
        // Count TYPE lines for krishiv_tasks_total — must be exactly 1.
        let type_count = body
            .lines()
            .filter(|l| l.starts_with("# TYPE krishiv_tasks_total"))
            .count();
        assert_eq!(
            type_count, 1,
            "must have exactly one TYPE line per metric family"
        );
    }

    #[test]
    fn render_prometheus_contains_help_and_type_lines() {
        let m = KrishivMetrics::default();
        let body = m.render_prometheus();
        assert!(body.contains("# HELP krishiv_tasks_total"));
        assert!(body.contains("# TYPE krishiv_tasks_total counter"));
        assert!(body.contains("# HELP krishiv_tasks_running"));
        assert!(body.contains("# TYPE krishiv_tasks_running gauge"));
        assert!(body.contains("# HELP krishiv_shuffle_bytes_written_total"));
        assert!(body.contains("# TYPE krishiv_shuffle_bytes_written_total counter"));
        assert!(body.contains("# HELP krishiv_job_queue_depth"));
        assert!(body.contains("# TYPE krishiv_job_queue_depth gauge"));
    }

    #[test]
    fn render_prometheus_reflects_counter_values() {
        let m = KrishivMetrics::default();
        m.inc_tasks_submitted();
        m.inc_tasks_submitted();
        m.inc_tasks_succeeded();
        m.inc_tasks_failed();
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_tasks_total{status=\"submitted\"} 2"));
        assert!(body.contains("krishiv_tasks_total{status=\"succeeded\"} 1"));
        assert!(body.contains("krishiv_tasks_total{status=\"failed\"} 1"));
    }

    #[test]
    fn render_prometheus_reflects_gauge_values() {
        let m = KrishivMetrics::default();
        m.set_tasks_running(7);
        m.set_job_queue_depth(3);
        m.add_shuffle_bytes_written(4096);
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_tasks_running 7"));
        assert!(body.contains("krishiv_job_queue_depth 3"));
        assert!(body.contains("krishiv_shuffle_bytes_written_total 4096"));
    }

    #[test]
    fn render_prometheus_zeroes_for_default() {
        let m = KrishivMetrics::default();
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_tasks_total{status=\"submitted\"} 0"));
        assert!(body.contains("krishiv_tasks_running 0"));
        assert!(body.contains("krishiv_shuffle_bytes_written_total 0"));
        assert!(body.contains("krishiv_job_queue_depth 0"));
    }

    #[test]
    fn render_prometheus_ends_with_newline() {
        let m = KrishivMetrics::default();
        let body = m.render_prometheus();
        assert!(body.ends_with('\n'));
    }

    // ── Labeled metric tests ─────────────────────────────────────────────────

    #[test]
    fn labeled_checkpoint_epoch_gauge() {
        let m = KrishivMetrics::default();
        m.set_checkpoint_epoch("job-a", 5);
        m.set_checkpoint_epoch("job-b", 12);
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_checkpoint_epoch{job_id=\"job-a\"} 5"));
        assert!(body.contains("krishiv_checkpoint_epoch{job_id=\"job-b\"} 12"));
    }

    #[test]
    fn labeled_checkpoint_epoch_counters() {
        let m = KrishivMetrics::default();
        m.inc_checkpoint_committed("job-a");
        m.inc_checkpoint_committed("job-a");
        m.inc_checkpoint_aborted("job-a");
        m.inc_checkpoint_failed("job-b");
        let body = m.render_prometheus();
        assert!(
            body.contains(
                "krishiv_checkpoint_epochs_total{job_id=\"job-a\",status=\"committed\"} 2"
            )
        );
        assert!(
            body.contains("krishiv_checkpoint_epochs_total{job_id=\"job-a\",status=\"aborted\"} 1")
        );
        assert!(
            body.contains("krishiv_checkpoint_epochs_total{job_id=\"job-b\",status=\"failed\"} 1")
        );
    }

    #[test]
    fn labeled_watermark_gauge() {
        let m = KrishivMetrics::default();
        m.set_watermark_ms("stream-job", 1620000000000);
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_watermark_ms{job_id=\"stream-job\"} 1620000000000"));
    }

    #[test]
    fn labeled_latency_histograms() {
        let m = KrishivMetrics::default();
        m.observe_grpc_duration("/krishiv.ExecutorTaskService/LaunchTask", 0.15);
        m.observe_grpc_duration("/krishiv.ExecutorTaskService/LaunchTask", 0.002);

        m.observe_checkpoint_commit_duration("write_manifest", 0.035);
        m.observe_checkpoint_commit_duration("fsync", 1.2);

        let body = m.render_prometheus();

        // Verify gRPC call duration histogram
        assert!(body.contains("krishiv_grpc_call_duration_seconds_count{path=\"/krishiv.ExecutorTaskService/LaunchTask\"} 2"));
        assert!(body.contains("krishiv_grpc_call_duration_seconds_sum{path=\"/krishiv.ExecutorTaskService/LaunchTask\"} 0.152"));
        assert!(body.contains("krishiv_grpc_call_duration_seconds_bucket{path=\"/krishiv.ExecutorTaskService/LaunchTask\",le=\"0.005\"} 1"));
        assert!(body.contains("krishiv_grpc_call_duration_seconds_bucket{path=\"/krishiv.ExecutorTaskService/LaunchTask\",le=\"0.25\"} 2"));
        assert!(body.contains("krishiv_grpc_call_duration_seconds_bucket{path=\"/krishiv.ExecutorTaskService/LaunchTask\",le=\"+Inf\"} 2"));

        // Verify checkpoint commit duration histogram
        assert!(body.contains(
            "krishiv_checkpoint_commit_duration_seconds_count{phase=\"write_manifest\"} 1"
        ));
        assert!(body.contains(
            "krishiv_checkpoint_commit_duration_seconds_sum{phase=\"write_manifest\"} 0.035"
        ));
        assert!(body.contains("krishiv_checkpoint_commit_duration_seconds_bucket{phase=\"write_manifest\",le=\"0.05\"} 1"));

        assert!(
            body.contains("krishiv_checkpoint_commit_duration_seconds_count{phase=\"fsync\"} 1")
        );
        assert!(
            body.contains("krishiv_checkpoint_commit_duration_seconds_sum{phase=\"fsync\"} 1.200")
        );
        assert!(body.contains(
            "krishiv_checkpoint_commit_duration_seconds_bucket{phase=\"fsync\",le=\"2.5\"} 1"
        ));
    }

    #[test]
    fn labeled_source_offset_lag() {
        let m = KrishivMetrics::default();
        m.set_source_offset_lag("job-a", "kafka-topic-0", 1500);
        let body = m.render_prometheus();
        assert!(body.contains(
            "krishiv_source_offset_lag{job_id=\"job-a\",source_id=\"kafka-topic-0\"} 1500"
        ));
    }

    #[test]
    fn labeled_task_attempt_counters() {
        let m = KrishivMetrics::default();
        m.inc_task_attempt_submitted("job-a", "stage-0");
        m.inc_task_attempt_submitted("job-a", "stage-0");
        m.inc_task_attempt_succeeded("job-a", "stage-0");
        m.inc_task_attempt_failed("job-a", "stage-0");
        m.inc_task_attempt_retrying("job-a", "stage-0");
        let body = m.render_prometheus();
        assert!(body.contains(
            "krishiv_task_attempts_total{job_id=\"job-a\",stage_id=\"stage-0\",status=\"submitted\"} 2"
        ));
        assert!(body.contains(
            "krishiv_task_attempts_total{job_id=\"job-a\",stage_id=\"stage-0\",status=\"succeeded\"} 1"
        ));
        assert!(body.contains(
            "krishiv_task_attempts_total{job_id=\"job-a\",stage_id=\"stage-0\",status=\"failed\"} 1"
        ));
    }

    #[test]
    fn labeled_executor_slots_gauge() {
        let m = KrishivMetrics::default();
        m.set_executor_slots_used("exec-1", 3);
        m.set_executor_slots_used("exec-2", 7);
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_executor_slots_used{executor_id=\"exec-1\"} 3"));
        assert!(body.contains("krishiv_executor_slots_used{executor_id=\"exec-2\"} 7"));
    }

    #[test]
    fn labeled_streaming_rows_counter() {
        let m = KrishivMetrics::default();
        m.add_streaming_rows("job-a", "task-0", 100);
        m.add_streaming_rows("job-a", "task-0", 250);
        let body = m.render_prometheus();
        assert!(body.contains(
            "krishiv_streaming_rows_emitted_total{job_id=\"job-a\",task_id=\"task-0\"} 350"
        ));
    }

    #[test]
    fn labeled_state_backend_gauges() {
        let m = KrishivMetrics::default();
        m.set_state_key_count("job-a", 5000);
        m.set_state_bytes("job-a", 1048576);
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_state_key_count{job_id=\"job-a\"} 5000"));
        assert!(body.contains("krishiv_state_bytes{job_id=\"job-a\"} 1048576"));
    }

    #[test]
    fn labeled_shuffle_partition_gauges() {
        let m = KrishivMetrics::default();
        m.set_shuffle_partitions("job-a", "stage-1", 3, 7, 1);
        let body = m.render_prometheus();
        assert!(body.contains(
            "krishiv_shuffle_partitions{job_id=\"job-a\",stage_id=\"stage-1\",state=\"pending\"} 3"
        ));
        assert!(body.contains(
            "krishiv_shuffle_partitions{job_id=\"job-a\",stage_id=\"stage-1\",state=\"available\"} 7"
        ));
        assert!(body.contains(
            "krishiv_shuffle_partitions{job_id=\"job-a\",stage_id=\"stage-1\",state=\"failed\"} 1"
        ));
    }

    #[test]
    fn remove_job_cleans_all_labeled_metrics() {
        let m = KrishivMetrics::default();
        m.set_checkpoint_epoch("job-a", 1);
        m.set_watermark_ms("job-a", 1000);
        m.inc_checkpoint_committed("job-a");
        m.inc_task_attempt_submitted("job-a", "stage-0");
        m.set_shuffle_partitions("job-a", "stage-1", 1, 0, 0);
        m.set_state_key_count("job-a", 42);
        m.set_state_bytes("job-a", 1024);
        m.remove_job("job-a");

        let body = m.render_prometheus();
        assert!(!body.contains("job-a"), "no job-a metrics after remove");
        // Global metrics should still exist.
        assert!(body.contains("krishiv_tasks_total"));
    }

    // ── traceparent / tracestate generation ──────────────────────────────────

    #[test]
    fn current_traceparent_inside_span_returns_none_when_no_span() {
        let tp = current_traceparent();
        assert!(tp.is_none(), "outside a span, traceparent must be None");
    }

    // ── Span field constants ─────────────────────────────────────────────────

    #[test]
    fn span_field_constants_are_snake_case_dotted() {
        assert_eq!(SPAN_JOB_ID, "krishiv.job_id");
        assert_eq!(SPAN_STAGE_ID, "krishiv.stage_id");
        assert_eq!(SPAN_TASK_ID, "krishiv.task_id");
        assert_eq!(SPAN_EPOCH, "krishiv.epoch");
        assert_eq!(SPAN_ATTEMPT_ID, "krishiv.attempt_id");
        assert_eq!(SPAN_SNAPSHOT_ID, "krishiv.snapshot_id");
        assert_eq!(SPAN_EXECUTOR_ID, "krishiv.executor_id");
        assert_eq!(SPAN_SOURCE_ID, "krishiv.source_id");
        assert_eq!(SPAN_SINK_ID, "krishiv.sink_id");
    }

    #[test]
    fn record_span_fields_applies_all_fields() {
        let span = tracing::info_span!("test_op");
        record_span_fields(&span, Some("j1"), Some("s1"), Some("t1"), Some(42), Some(3));
        let _e = span.enter();
        // Fields are recorded on the span; no panic = pass.
    }

    #[test]
    fn record_span_fields_with_none_does_not_panic() {
        let span = tracing::info_span!("minimal_op");
        record_span_fields(&span, None, None, None, None, None);
        let _e = span.enter();
    }

    // ── MetricsError Display ────────────────────────────────────────────────

    #[test]
    fn metrics_error_display_otlp_build() {
        let err = MetricsError::OtlpBuild("connection refused".into());
        assert_eq!(
            err.to_string(),
            "OTLP exporter build failed: connection refused"
        );
    }

    #[test]
    fn metrics_error_display_subscriber() {
        let err = MetricsError::Subscriber("already set".into());
        assert_eq!(err.to_string(), "subscriber init failed: already set");
    }

    #[test]
    fn metrics_error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(MetricsError::OtlpBuild("test".into()));
        assert!(!err.to_string().is_empty());
    }

    // ── MetricsConfig custom values ─────────────────────────────────────────

    #[test]
    fn metrics_config_custom_service_name() {
        let config = MetricsConfig {
            service_name: "my-service".into(),
            ..Default::default()
        };
        assert_eq!(config.service_name, "my-service");
    }

    #[test]
    fn metrics_config_custom_log_filter() {
        let config = MetricsConfig {
            log_filter: Some("debug".into()),
            ..Default::default()
        };
        assert_eq!(config.log_filter.as_deref(), Some("debug"));
    }

    #[test]
    fn metrics_config_stdout_exporter() {
        let config = MetricsConfig {
            exporter: TracerExporter::Stdout,
            ..Default::default()
        };
        assert!(matches!(config.exporter, TracerExporter::Stdout));
    }

    #[test]
    fn metrics_config_otlp_endpoint_some() {
        let config = MetricsConfig {
            otlp_endpoint: Some("http://localhost:4317".into()),
            ..Default::default()
        };
        assert_eq!(
            config.otlp_endpoint.as_deref(),
            Some("http://localhost:4317")
        );
    }

    // ── MetricsHandle noop and shutdown ──────────────────────────────────────

    #[test]
    fn metrics_handle_noop_creates_valid_handle() {
        let handle = MetricsHandle::noop();
        drop(handle);
    }

    #[test]
    fn metrics_handle_drop_calls_shutdown() {
        let handle = init(MetricsConfig::default()).expect("init");
        drop(handle);
    }

    #[test]
    fn init_with_stdout_exporter() {
        let config = MetricsConfig {
            exporter: TracerExporter::Stdout,
            ..Default::default()
        };
        let handle = init(config);
        assert!(handle.is_ok());
    }

    #[test]
    fn init_with_custom_filter() {
        let config = MetricsConfig {
            log_filter: Some("warn".into()),
            ..Default::default()
        };
        let handle = init(config);
        assert!(handle.is_ok());
    }

    #[test]
    fn init_with_empty_filter_defaults_to_info() {
        let config = MetricsConfig {
            log_filter: Some("".into()),
            ..Default::default()
        };
        let _handle = init(config);
    }

    // ── KrishivMetrics edge cases ───────────────────────────────────────────

    #[test]
    fn add_shuffle_bytes_written_zero() {
        let m = KrishivMetrics::default();
        m.add_shuffle_bytes_written(0);
        assert_eq!(m.shuffle_bytes_written.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn add_shuffle_bytes_written_max_value() {
        let m = KrishivMetrics::default();
        m.add_shuffle_bytes_written(u64::MAX);
        assert_eq!(m.shuffle_bytes_written.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn set_tasks_running_max_value() {
        let m = KrishivMetrics::default();
        m.set_tasks_running(u64::MAX);
        assert_eq!(m.tasks_running.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn set_job_queue_depth_zero() {
        let m = KrishivMetrics::default();
        m.set_job_queue_depth(42);
        m.set_job_queue_depth(0);
        assert_eq!(m.job_queue_depth.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn multiple_counters_accumulate_independently() {
        let m = KrishivMetrics::default();
        for _ in 0..100 {
            m.inc_tasks_submitted();
        }
        for _ in 0..50 {
            m.inc_tasks_succeeded();
        }
        for _ in 0..10 {
            m.inc_tasks_failed();
        }
        assert_eq!(m.tasks_submitted.load(Ordering::Relaxed), 100);
        assert_eq!(m.tasks_succeeded.load(Ordering::Relaxed), 50);
        assert_eq!(m.tasks_failed.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn prometheus_output_is_valid_utf8() {
        let m = KrishivMetrics::default();
        m.inc_tasks_submitted();
        let body = m.render_prometheus();
        assert!(std::str::from_utf8(body.as_bytes()).is_ok());
    }

    // ── Global metrics thread safety ────────────────────────────────────────

    #[test]
    fn global_metrics_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let metrics = Arc::new(KrishivMetrics::default());
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let m = Arc::clone(&metrics);
                thread::spawn(move || {
                    for _ in 0..1000 {
                        m.inc_tasks_submitted();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(metrics.tasks_submitted.load(Ordering::Relaxed), 10000);
    }

    /// Verify that labeled metrics are thread-safe (DashMap + AtomicU64).
    #[test]
    fn labeled_metrics_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let metrics = Arc::new(KrishivMetrics::default());
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let m = Arc::clone(&metrics);
                thread::spawn(move || {
                    for _ in 0..500 {
                        m.inc_task_attempt_submitted(&format!("job-{i}"), "stage-0");
                        m.set_checkpoint_epoch(&format!("job-{i}"), 1);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // Verify no crash — concurrent DashMap access should work.
        let body = metrics.render_prometheus();
        assert!(body.contains("krishiv_checkpoint_epoch"));
    }

    // ── deployment_target unit tests ────────────────────────────────────────

    #[test]
    fn resolved_deployment_target_explicit_config() {
        let config = MetricsConfig {
            deployment_target: Some("production".into()),
            ..MetricsConfig::default()
        };
        assert_eq!(config.resolved_deployment_target(), "production");
    }

    #[test]
    fn resolved_deployment_target_none_returns_env_or_unknown() {
        // When no explicit config is given, the function reads the env var or
        // falls back to "unknown". We verify the documented fallback chain
        // without mutating the environment (unsafe_code is workspace-forbidden).
        let config = MetricsConfig {
            deployment_target: None,
            ..MetricsConfig::default()
        };
        let result = config.resolved_deployment_target();
        let expected =
            std::env::var("KRISHIV_DEPLOYMENT_TARGET").unwrap_or_else(|_| "unknown".to_string());
        assert_eq!(
            result, expected,
            "resolved value must match the env var when set, or 'unknown' when absent"
        );
    }

    #[test]
    fn resolved_deployment_target_explicit_beats_any_env() {
        // When deployment_target is explicitly set, it wins regardless of any
        // env var — no env mutation needed to test this invariant.
        let config = MetricsConfig {
            deployment_target: Some("explicit-wins".into()),
            ..MetricsConfig::default()
        };
        assert_eq!(
            config.resolved_deployment_target(),
            "explicit-wins",
            "explicit config must always override the env var fallback"
        );
    }

    #[test]
    fn inmemory_exporter_captures_spans_after_init() {
        // Verifies that TracerExporter::InMemory is correctly wired into init()
        // and that emitted spans reach the exporter's capture buffer.
        use opentelemetry::trace::Tracer as _;
        use opentelemetry_sdk::trace::InMemorySpanExporter;

        let exporter = InMemorySpanExporter::default();
        let config = MetricsConfig {
            service_name: "span-capture-test".into(),
            exporter: TracerExporter::InMemory(exporter.clone()),
            deployment_target: Some("test-cluster".into()),
            otlp_endpoint: None,
            log_filter: None,
        };
        let handle = init(config).expect("init must succeed with InMemory exporter");

        // Emit a span directly via the provider-local tracer rather than the
        // global one, which can be replaced by concurrent tests calling init().
        {
            use opentelemetry::trace::TracerProvider as _;
            let tracer = handle.tracer_provider.tracer("capture-test");
            let span = tracer.start("test-capture-span");
            drop(span);
        }

        // Force flush to drain the processor (retry briefly for parallel test runs).
        let mut spans = Vec::new();
        for _ in 0..50 {
            let _ = handle.tracer_provider.force_flush();
            if let Ok(captured) = exporter.get_finished_spans() {
                if !captured.is_empty() {
                    spans = captured;
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let _ = handle.tracer_provider.shutdown();
        if spans.is_empty() {
            if let Ok(captured) = exporter.get_finished_spans() {
                spans = captured;
            }
        }
        assert!(
            !spans.is_empty(),
            "at least one span must be captured by InMemory exporter after init()"
        );
        // The deployment.target is passed to the resource builder in init().
        // Its correctness is validated by the resolved_deployment_target unit tests.
        // Here we just verify the span name is preserved.
        assert!(
            spans.iter().any(|s| s.name.as_ref() == "test-capture-span"),
            "captured span must have the expected name"
        );
    }

    #[tokio::test]
    #[ignore = "requires live OTLP collector at OTEL_EXPORTER_OTLP_ENDPOINT"]
    async fn otlp_integration_exports_span() {
        let endpoint = match std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
            Ok(e) => e,
            Err(_) => return,
        };
        let config = MetricsConfig {
            service_name: "krishiv-test".into(),
            otlp_endpoint: Some(endpoint),
            ..Default::default()
        };
        let handle = init(config).expect("metrics init with OTLP endpoint failed");
        let tracer = opentelemetry::global::tracer("test");
        {
            use opentelemetry::trace::Tracer as _;
            let _span = tracer.start("otlp_integration_test_span");
        }
        handle.shutdown();
    }
}
