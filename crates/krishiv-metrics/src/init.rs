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

