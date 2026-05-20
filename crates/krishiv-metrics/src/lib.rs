#![forbid(unsafe_code)]
//! **Beta API**: may change between minor releases.
//!
//! OpenTelemetry metrics, traces, and structured log initialization for all Krishiv processes.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::TracerProvider as SdkTracerProvider;
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
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            service_name: "krishiv".to_string(),
            // NoOp default so tests don't write to stdout.
            exporter: TracerExporter::NoOp,
            log_filter: None,
        }
    }
}

/// **Beta API**: may change between minor releases.
///
/// Opaque handle returned by [`init`]. Shuts down the OTel tracer provider on drop.
pub struct MetricsHandle {
    tracer_provider: SdkTracerProvider,
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
pub fn init(config: MetricsConfig) -> MetricsHandle {
    let filter_str = config
        .log_filter
        .as_deref()
        .unwrap_or("info")
        .to_string();

    let filter = tracing_subscriber::EnvFilter::new(&filter_str);

    let tracer_provider = match config.exporter {
        TracerExporter::Stdout => SdkTracerProvider::builder()
            .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
            .build(),
        TracerExporter::NoOp => SdkTracerProvider::builder().build(),
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

    MetricsHandle { tracer_provider }
}

/// **Beta API**: may change between minor releases.
///
/// Shuts down the OTel tracer provider by dropping the handle (the `Drop` impl does the work).
pub fn shutdown(handle: MetricsHandle) {
    drop(handle);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_noop_does_not_panic() {
        let _handle = init(MetricsConfig::default());
    }

    #[test]
    fn shutdown_does_not_panic() {
        let handle = init(MetricsConfig::default());
        shutdown(handle);
    }

    #[test]
    fn tracing_span_does_not_panic() {
        let _handle = init(MetricsConfig::default());
        let _s = tracing::info_span!("test_span").entered();
    }

    #[test]
    fn default_config_service_name() {
        assert_eq!(MetricsConfig::default().service_name, "krishiv");
    }

    #[test]
    fn current_traceparent_no_span_returns_none() {
        // Outside any active span, current_traceparent must return None.
        assert_eq!(current_traceparent(), None);
    }
}
