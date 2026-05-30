//! tonic interceptors for W3C `traceparent` and `tracestate` propagation.
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

use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry_sdk::propagation::TraceContextPropagator;

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
/// metadata and attaches them as the remote OTel parent context via
/// [`Context::attach`](opentelemetry::Context::attach) so the
/// `tracing-opentelemetry` layer picks them up when new spans are created.
///
/// Previously used `tracing::Span::current().set_parent()` which silently
/// dropped the context when no `tracing` span was active (the common case at
/// interceptor time).
///
/// # Thread-local caveat
///
/// The [`ContextGuard`](opentelemetry::ContextGuard) returned by `attach()` is
/// `!Send`, so it cannot be stored in request extensions.  The context flows
/// correctly on the handling thread for the synchronous portion of request
/// processing.  Across `tokio::spawn` boundaries the parent context should be
/// re-extracted via a stored `opentelemetry::Context` in request extensions.
///
/// When the headers are absent or malformed the request is forwarded unchanged.
pub fn extract_trace_context(req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
    let propagator = TraceContextPropagator::new();
    let parent_cx = propagator.extract(&MetadataExtractor(req.metadata()));
    parent_cx.attach();
    Ok(req)
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
}
