//! tonic interceptors for W3C `traceparent` propagation.
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
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Tonic client interceptor: reads `current_traceparent()` and inserts it as
/// the `"traceparent"` metadata key on every outgoing request.
///
/// When no span is active the request is forwarded unchanged.
pub fn inject_trace_context(
    mut req: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    if let Some(value) = crate::current_traceparent() {
        if let Ok(meta_val) = tonic::metadata::MetadataValue::try_from(value.as_str()) {
            req.metadata_mut().insert("traceparent", meta_val);
        }
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

/// Tonic server interceptor: reads `"traceparent"` from request metadata and
/// sets it as the parent span context so downstream spans inherit the W3C trace
/// context.
///
/// When the header is absent or malformed the request is forwarded unchanged.
pub fn extract_trace_context(req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
    let propagator = TraceContextPropagator::new();
    let parent_ctx = propagator.extract(&MetadataExtractor(req.metadata()));
    let _ = tracing::Span::current().set_parent(parent_ctx);
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
