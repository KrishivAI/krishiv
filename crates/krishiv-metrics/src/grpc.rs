//! tonic interceptors for W3C `traceparent` and `tracestate` propagation, and
//! a tower [`GrpcDurationLayer`] that calls
//! [`KrishivMetrics::observe_grpc_duration`] for every completed RPC.
//!
//! # Client side
//!
//! Use [`inject_trace_context`] when building a tonic stub:
//! ```ignore
//! let client = CoordinatorExecutorClient::with_interceptor(channel, inject_trace_context);
//! ```
//!
//! # Server side
//!
//! Use [`extract_trace_context`] when registering a service:
//! ```ignore
//! Server::builder()
//!     .add_service(tonic::service::interceptor::InterceptedService::new(svc, extract_trace_context))
//!     .serve(addr)
//!     .await?;
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tower::{Layer, Service};

/// Tonic client interceptor: reads `current_traceparent()` and `current_tracestate()`
/// and inserts them as `"traceparent"` and `"tracestate"` metadata keys on every
/// outgoing request.
///
/// When no span is active the request is forwarded unchanged.
pub fn inject_trace_context(
    mut req: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    if let Some(value) = crate::current_traceparent()
        && let Ok(meta_val) = tonic::metadata::MetadataValue::try_from(value.as_str())
    {
        req.metadata_mut().insert("traceparent", meta_val);
    }
    if let Some(value) = crate::current_tracestate()
        && let Ok(meta_val) = tonic::metadata::MetadataValue::try_from(value.as_str())
    {
        req.metadata_mut().insert("tracestate", meta_val);
    }
    Ok(req)
}

struct MetadataExtractor<'a>(&'a tonic::metadata::MetadataMap);

impl<'a> Extractor for MetadataExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|k| match k {
                tonic::metadata::KeyRef::Ascii(key) => key.as_str(),
                tonic::metadata::KeyRef::Binary(key) => key.as_str(),
            })
            .collect()
    }
}

/// Tonic server interceptor: reads `"traceparent"` and `"tracestate"` from request
/// metadata and stores the decoded [`opentelemetry::Context`] in request
/// extensions under the key [`RemoteSpanContext`].
///
/// `Context` is `Clone + Send + 'static`, so it survives `tokio::spawn`
/// boundaries.  Each async handler that creates spans should re-attach it:
///
/// ```ignore
/// if let Some(parent_cx) = req.extensions().get::<RemoteSpanContext>() {
///     let _guard = parent_cx.0.clone().attach();
///     // spans created here will be children of the remote parent
/// }
/// ```
///
/// The `ContextGuard` returned by `attach()` is `!Send`; callers must hold it
/// only within the synchronous extent of the span they create.
///
/// When the headers are absent or malformed the request is forwarded unchanged.
pub fn extract_trace_context(
    mut req: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    let propagator = TraceContextPropagator::new();
    let parent_cx = propagator.extract(&MetadataExtractor(req.metadata()));
    // Store the context (Send + Clone) in extensions so async handlers can
    // re-attach it after tokio::spawn boundaries.  Attaching here would only
    // cover the synchronous interceptor stack, not the spawned handler task.
    req.extensions_mut().insert(RemoteSpanContext(parent_cx));
    Ok(req)
}

/// Wrapper for the decoded remote OTel [`opentelemetry::Context`] stored in
/// tonic request extensions by [`extract_trace_context`].
#[derive(Clone, Debug)]
pub struct RemoteSpanContext(pub opentelemetry::Context);

// Internal-error surfacing (Phase 59 error taxonomy, gap-d)

/// Per-process error-reference seed, derived once from wall-clock time so that
/// references minted by different daemon runs do not collide in a shared log
/// index. No new dependency: `SystemTime` is enough entropy for a correlation
/// token that is never security-sensitive.
fn error_ref_seed() -> u64 {
    static SEED: OnceLock<u64> = OnceLock::new();
    *SEED.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
            // Mix so two daemons started in the same millisecond still differ.
            ^ (std::process::id() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
    })
}

/// Monotonic per-process counter distinguishing individual internal errors.
static ERROR_REF_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Classify an internal error for client surfacing (Phase 59 error taxonomy).
///
/// gRPC handlers must not return `Status::internal(err.to_string())`: the raw
/// error can leak table names, file paths, backend addresses, or SQL fragments
/// to an unauthenticated caller. Instead, log the full error server-side at
/// ERROR with a short correlation `error_ref`, and return an **opaque** status
/// carrying only that reference. An operator greps the logs for the ref; the
/// client learns nothing about engine internals.
///
/// ```ignore
/// .map_err(|e| internal_status("commit checkpoint", &e))?
/// ```
pub fn internal_status(context: &str, error: &dyn std::fmt::Display) -> tonic::Status {
    let n = ERROR_REF_COUNTER.fetch_add(1, Ordering::Relaxed);
    let error_ref = format!(
        "{:08x}-{:06x}",
        error_ref_seed() & 0xFFFF_FFFF,
        n & 0xFF_FFFF
    );
    // Full detail stays server-side, keyed by the ref the client receives.
    tracing::error!(error_ref = %error_ref, context, error = %error, "internal error");
    tonic::Status::internal(format!(
        "internal error (ref {error_ref}); contact the operator with this reference"
    ))
}

// GrpcDurationLayer

/// Tower layer that records per-RPC call duration via
/// [`crate::global_metrics().observe_grpc_duration`].
///
/// The gRPC method path is extracted from `http::Request::uri().path()`.
/// Apply as:
///
/// ```ignore
/// tonic::transport::Server::builder()
///     .layer(krishiv_metrics::grpc::GrpcDurationLayer)
///     .add_service(...)
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcDurationLayer;

impl<S> Layer<S> for GrpcDurationLayer {
    type Service = GrpcDurationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcDurationService { inner }
    }
}

/// Service wrapper produced by [`GrpcDurationLayer`].
#[derive(Clone, Debug)]
pub struct GrpcDurationService<S> {
    inner: S,
}

impl<S, B> Service<http::Request<B>> for GrpcDurationService<S>
where
    S: Service<http::Request<B>> + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let path = req.uri().path().to_string();
        let start = std::time::Instant::now();
        let fut = self.inner.call(req);
        Box::pin(async move {
            let result = fut.await;
            let elapsed = start.elapsed().as_secs_f64();
            crate::global_metrics().observe_grpc_duration(&path, elapsed);
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inject_trace_context_with_no_span_passes_through() {
        let req = tonic::Request::new(());
        let result = inject_trace_context(req);
        assert!(result.is_ok());
    }

    #[test]
    fn extract_trace_context_with_no_header_passes_through() {
        let req = tonic::Request::new(());
        let result = extract_trace_context(req);
        assert!(result.is_ok());
    }

    #[test]
    fn extract_trace_context_with_valid_header() {
        let mut req = tonic::Request::new(());
        req.metadata_mut().insert(
            "traceparent",
            tonic::metadata::MetadataValue::from_static("00-abc123-def456-01"),
        );
        let result = extract_trace_context(req);
        assert!(result.is_ok());
    }

    #[test]
    fn extract_trace_context_with_empty_header() {
        let mut req = tonic::Request::new(());
        req.metadata_mut().insert(
            "traceparent",
            tonic::metadata::MetadataValue::from_static(""),
        );
        let result = extract_trace_context(req);
        assert!(result.is_ok());
    }

    #[test]
    fn inject_trace_context_preserves_request() {
        let req = tonic::Request::new(());
        let result = inject_trace_context(req);
        assert!(result.is_ok());
        // Verify request can be consumed
        let _req = result.unwrap();
    }

    #[test]
    fn extract_trace_context_preserves_request() {
        let req = tonic::Request::new(());
        let result = extract_trace_context(req);
        assert!(result.is_ok());
        // Verify request can be consumed
        let _req = result.unwrap();
    }

    #[test]
    fn internal_status_does_not_leak_error_detail() {
        let secret = "table reference.payroll at s3://internal-bucket/secret/path.parquet";
        let status = internal_status("scan table", &secret);
        assert_eq!(status.code(), tonic::Code::Internal);
        let msg = status.message();
        // The opaque client-facing message must not echo any internal detail.
        assert!(!msg.contains("payroll"), "leaked table name: {msg}");
        assert!(!msg.contains("s3://"), "leaked backend path: {msg}");
        assert!(!msg.contains("secret"), "leaked path fragment: {msg}");
        // …but it must carry a correlation ref the operator can grep.
        assert!(msg.contains("ref "), "missing correlation ref: {msg}");
    }

    #[test]
    fn internal_status_refs_are_unique_per_call() {
        let a = internal_status("ctx", &"boom");
        let b = internal_status("ctx", &"boom");
        assert_ne!(
            a.message(),
            b.message(),
            "each internal error must get a distinct correlation ref"
        );
    }
}
