#![forbid(unsafe_code)]
//! **Beta API**: may change between minor releases.
//!
//! OpenTelemetry metrics, traces, and structured log initialization for all Krishiv processes.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

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
/// Returns an error string if the OTLP exporter pipeline fails to build (only
/// possible when `config.otlp_endpoint` is `Some`).
pub fn init(config: MetricsConfig) -> Result<MetricsHandle, String> {
    let filter_str = config.log_filter.as_deref().unwrap_or("info").to_string();

    let filter = tracing_subscriber::EnvFilter::new(&filter_str);

    let tracer_provider = if let Some(endpoint) = config.otlp_endpoint {
        // Build an OTLP gRPC exporter pipeline.
        use opentelemetry_otlp::{SpanExporter, WithExportConfig as _};

        let exporter = SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .map_err(|e| format!("OTLP exporter build failed: {e}"))?;

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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

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
