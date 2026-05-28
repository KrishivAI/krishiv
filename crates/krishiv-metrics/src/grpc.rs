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

/// Tonic server interceptor: reads `"traceparent"` from request metadata and
/// records it on the current `tracing` span so downstream spans inherit the
/// W3C trace context.
///
/// When the header is absent or malformed the request is forwarded unchanged.
pub fn extract_trace_context(
    req: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    if let Some(val) = req
        .metadata()
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
    {
        tracing::Span::current().record("traceparent", val);
    }
    Ok(req)
}
