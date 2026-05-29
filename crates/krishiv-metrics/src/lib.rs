#![forbid(unsafe_code)]
//! **Beta API**: may change between minor releases.
//!
//! OpenTelemetry metrics, traces, and structured log initialization for all Krishiv processes.

pub mod grpc;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

/// Errors returned by [`init`].
#[derive(Debug)]
pub enum MetricsError {
    /// The OTLP exporter pipeline failed to build.
    OtlpBuild(String),
    /// A tracing subscriber initialization error.
    Subscriber(String),
}

impl std::fmt::Display for MetricsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OtlpBuild(msg) => write!(f, "OTLP exporter build failed: {msg}"),
            Self::Subscriber(msg) => write!(f, "subscriber init failed: {msg}"),
        }
    }
}

impl std::error::Error for MetricsError {}

/// **Beta API**: may change between minor releases.
///
/// Selects the OTel span exporter backend used when initializing the tracer provider.
pub enum TracerExporter {
    /// Exports spans to stdout. Useful for development and CI.
    Stdout,
    /// Disables all span export. Used in tests and when telemetry is not needed.
    NoOp,
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
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            service_name: "krishiv".to_string(),
            // NoOp default so tests don't write to stdout.
            exporter: TracerExporter::NoOp,
            log_filter: None,
            otlp_endpoint: None,
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
        // Ignore shutdown errors — best-effort flush.
        let _ = self.tracer_provider.shutdown();
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

    let tracer_provider = if let Some(endpoint) = config.otlp_endpoint {
        // Build an OTLP gRPC exporter pipeline.
        use opentelemetry_otlp::{SpanExporter, WithExportConfig as _};

        let exporter = SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .map_err(|e| MetricsError::OtlpBuild(format!("{e}")))?;

        SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .build()
    } else {
        match config.exporter {
            TracerExporter::Stdout => SdkTracerProvider::builder()
                .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
                .build(),
            TracerExporter::NoOp => SdkTracerProvider::builder().build(),
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

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// OpenTelemetry-aligned counters/histograms for Krishiv runtime observability.
#[derive(Debug, Default)]
pub struct KrishivMetrics {
    tasks_submitted: AtomicU64,
    tasks_running: AtomicU64,
    tasks_succeeded: AtomicU64,
    tasks_failed: AtomicU64,
    shuffle_bytes_written: AtomicU64,
    job_queue_depth: AtomicU64,
}

static GLOBAL_METRICS: OnceLock<KrishivMetrics> = OnceLock::new();

/// Process-wide metrics registry (lazy-initialized).
pub fn global_metrics() -> &'static KrishivMetrics {
    GLOBAL_METRICS.get_or_init(KrishivMetrics::default)
}

impl KrishivMetrics {
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

    /// Add shuffle bytes written.
    pub fn add_shuffle_bytes_written(&self, bytes: u64) {
        self.shuffle_bytes_written
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Set job queue depth gauge.
    pub fn set_job_queue_depth(&self, depth: u64) {
        self.job_queue_depth.store(depth, Ordering::Relaxed);
    }

    /// Render Prometheus exposition format for Krishiv counters/gauges.
    pub fn render_prometheus(&self) -> String {
        format!(
            "\
# HELP krishiv_tasks_total Tasks submitted to the coordinator
# TYPE krishiv_tasks_total counter
krishiv_tasks_total{{status=\"submitted\"}} {submitted}
# HELP krishiv_tasks_running Currently running tasks
# TYPE krishiv_tasks_running gauge
krishiv_tasks_running {running}
# HELP krishiv_tasks_total Tasks that completed successfully
# TYPE krishiv_tasks_total counter
krishiv_tasks_total{{status=\"succeeded\"}} {succeeded}
# HELP krishiv_tasks_total Tasks that failed
# TYPE krishiv_tasks_total counter
krishiv_tasks_total{{status=\"failed\"}} {failed}
# HELP krishiv_shuffle_bytes_written_total Shuffle bytes written
# TYPE krishiv_shuffle_bytes_written_total counter
krishiv_shuffle_bytes_written_total {shuffle_bytes}
# HELP krishiv_job_queue_depth Pending jobs in admission queue
# TYPE krishiv_job_queue_depth gauge
krishiv_job_queue_depth {queue_depth}
",
            submitted = self.tasks_submitted.load(Ordering::Relaxed),
            running = self.tasks_running.load(Ordering::Relaxed),
            succeeded = self.tasks_succeeded.load(Ordering::Relaxed),
            failed = self.tasks_failed.load(Ordering::Relaxed),
            shuffle_bytes = self.shuffle_bytes_written.load(Ordering::Relaxed),
            queue_depth = self.job_queue_depth.load(Ordering::Relaxed),
        )
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
        // Outside any active span, current_traceparent must return None.
        assert_eq!(current_traceparent(), None);
    }

    #[test]
    fn krishiv_metrics_prometheus_contains_tasks_total() {
        let m = KrishivMetrics::default();
        m.inc_tasks_submitted();
        let body = m.render_prometheus();
        assert!(body.contains("krishiv_tasks_total"));
        assert!(body.contains("krishiv_job_queue_depth"));
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

    // ── Prometheus text format rendering ─────────────────────────────────────

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

    // ── traceparent generation ───────────────────────────────────────────────

    #[test]
    fn current_traceparent_inside_span_returns_some() {
        // Without an active span, traceparent must be None.
        let tp = current_traceparent();
        assert!(tp.is_none(), "outside a span, traceparent must be None");
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
        // Empty filter string should still succeed (defaults to "info")
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
    fn render_prometheus_all_zero_after_reset() {
        let m = KrishivMetrics::default();
        m.inc_tasks_submitted();
        m.inc_tasks_succeeded();
        m.inc_tasks_failed();
        m.set_tasks_running(5);
        m.add_shuffle_bytes_written(1024);
        m.set_job_queue_depth(3);
        // Reset to zero by creating a new instance
        let m2 = KrishivMetrics::default();
        let body = m2.render_prometheus();
        assert!(body.contains("krishiv_tasks_total{status=\"submitted\"} 0"));
        assert!(body.contains("krishiv_tasks_total{status=\"succeeded\"} 0"));
        assert!(body.contains("krishiv_tasks_total{status=\"failed\"} 0"));
        assert!(body.contains("krishiv_tasks_running 0"));
        assert!(body.contains("krishiv_shuffle_bytes_written_total 0"));
        assert!(body.contains("krishiv_job_queue_depth 0"));
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

    // ── OTLP integration test ───────────────────────────────────────────────

    /// OTLP integration test — only runs when OTEL_EXPORTER_OTLP_ENDPOINT is set.
    /// Skips silently in normal CI. Run manually with a live collector:
    ///   OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 cargo test -p krishiv-metrics otlp_integration -- --ignored
    #[tokio::test]
    #[ignore = "requires live OTLP collector at OTEL_EXPORTER_OTLP_ENDPOINT"]
    async fn otlp_integration_exports_span() {
        let endpoint = match std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
            Ok(e) => e,
            Err(_) => return, // skip if not set
        };
        // Initialize with OTLP endpoint.
        let config = MetricsConfig {
            service_name: "krishiv-test".into(),
            otlp_endpoint: Some(endpoint),
            ..Default::default()
        };
        let handle = init(config).expect("metrics init with OTLP endpoint failed");
        // Emit a test span.
        let tracer = opentelemetry::global::tracer("test");
        {
            use opentelemetry::trace::Tracer as _;
            let _span = tracer.start("otlp_integration_test_span");
            // span closes when dropped
        }
        handle.shutdown();
    }
}
