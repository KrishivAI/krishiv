//! Pooled gRPC client for coordinator RPCs.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use krishiv_proto::{LeaseGeneration, wire};
use tokio::sync::Mutex;
use tonic::transport::Channel;

pub const COORDINATOR_BEARER_TOKEN_ENV: &str = "KRISHIV_COORDINATOR_BEARER_TOKEN";

/// Shared, atomically-updated lease generation handle.  The executor binary
/// owns one of these for the entire process; every component that sends a
/// coordinator RPC reads the live generation from it before transmitting so
/// retries after a lease bump cannot ship a stale lease.
#[derive(Debug, Clone)]
pub struct SharedLeaseGeneration {
    inner: Arc<AtomicU64>,
}

impl SharedLeaseGeneration {
    pub fn new(initial: LeaseGeneration) -> Self {
        Self {
            inner: Arc::new(AtomicU64::new(initial.as_u64())),
        }
    }

    pub fn get(&self) -> LeaseGeneration {
        let raw = self.inner.load(Ordering::Acquire);
        // `LeaseGeneration::try_new` rejects 0. A coordinator that sends a
        // 0-lease indicates "uninitialized" / "fence lost" — surface this as
        // `LeaseGeneration::initial()` (the most conservative valid value)
        // rather than silently coercing 0 → 1, which would let a fenced-off
        // coordinator continue making progress.
        match LeaseGeneration::try_new(raw) {
            Ok(g) => g,
            Err(_) => {
                tracing::warn!(
                    raw = raw,
                    "SharedLeaseGeneration::get observed a zero raw value; \
                     coercing to LeaseGeneration::initial() as a safe default"
                );
                LeaseGeneration::initial()
            }
        }
    }

    pub fn set(&self, lease: LeaseGeneration) {
        let new_val = lease.as_u64();
        // Monotonic: never go backwards.  fetch_max returns the previous value.
        self.inner.fetch_max(new_val, Ordering::Release);
    }
}

/// Type alias for the intercepted coordinator executor client.
type InterceptedCoordinatorClient =
    wire::v1::coordinator_executor_client::CoordinatorExecutorClient<
        tonic::service::interceptor::InterceptedService<
            Channel,
            fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
        >,
    >;

/// Build a [`tonic::transport::ClientTlsConfig`] from env vars.
///
/// When `KRISHIV_CA_CERT` is set the PEM is loaded as the trusted CA bundle,
/// which enables verification of the coordinator's server certificate.
/// When the env var is absent the function returns `None` (plaintext).
///
/// Build a [`tonic::transport::ClientTlsConfig`] from env vars.
///
/// When `KRISHIV_CA_CERT` is set the PEM is loaded as the trusted CA bundle,
/// which enables verification of the coordinator's server certificate.
/// When the env var is absent the function returns `None` (plaintext).
/// Returns `None` and logs an error if the cert file cannot be read.
fn client_tls_config_from_env() -> Option<tonic::transport::ClientTlsConfig> {
    if let Ok(ca_path) = std::env::var("KRISHIV_CA_CERT") {
        let ca_pem = match std::fs::read(&ca_path) {
            Ok(pem) => pem,
            Err(e) => {
                tracing::error!(
                    ca_path = %ca_path,
                    error = %e,
                    "KRISHIV_CA_CERT: cannot read CA certificate file; \
                     TLS will not be enabled"
                );
                return None;
            }
        };
        let ca = tonic::transport::Certificate::from_pem(ca_pem);
        return Some(tonic::transport::ClientTlsConfig::new().ca_certificate(ca));
    }
    None
}

fn configured_coordinator_bearer_token() -> Option<String> {
    std::env::var(COORDINATOR_BEARER_TOKEN_ENV)
        .ok()
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
}

pub(crate) fn inject_coordinator_bearer_token(
    mut req: tonic::Request<()>,
    token: &str,
) -> Result<tonic::Request<()>, tonic::Status> {
    let header = format!("Bearer {}", token.trim());
    let value = tonic::metadata::MetadataValue::try_from(header.as_str()).map_err(|_| {
        tonic::Status::internal(format!(
            "{COORDINATOR_BEARER_TOKEN_ENV} contains characters that are invalid for gRPC metadata"
        ))
    })?;
    req.metadata_mut().insert("authorization", value);
    Ok(req)
}

fn inject_coordinator_request_context(
    req: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    let req = krishiv_metrics::grpc::inject_trace_context(req)?;
    if let Some(token) = configured_coordinator_bearer_token() {
        inject_coordinator_bearer_token(req, &token)
    } else {
        Ok(req)
    }
}

/// Reuses one coordinator gRPC channel across RPCs and stamps the live lease
/// onto every outgoing executor-originated request.
#[derive(Debug, Clone)]
pub struct CoordinatorGrpcPool {
    endpoint: String,
    client: Arc<Mutex<Option<InterceptedCoordinatorClient>>>,
    lease: SharedLeaseGeneration,
}

impl CoordinatorGrpcPool {
    pub fn new(endpoint: impl Into<String>, lease_generation: LeaseGeneration) -> Self {
        Self {
            endpoint: endpoint.into(),
            client: Arc::new(Mutex::new(None)),
            lease: SharedLeaseGeneration::new(lease_generation),
        }
    }

    /// Build a pool that shares its lease atomic with the caller (executor binary).
    pub fn with_shared_lease(endpoint: impl Into<String>, lease: SharedLeaseGeneration) -> Self {
        Self {
            endpoint: endpoint.into(),
            client: Arc::new(Mutex::new(None)),
            lease,
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn lease_handle(&self) -> SharedLeaseGeneration {
        self.lease.clone()
    }

    pub async fn client(&self) -> Result<InterceptedCoordinatorClient, tonic::transport::Error> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }
        // Connect while holding the lock — prevents concurrent duplicate connects.
        let mut endpoint = tonic::transport::Endpoint::from_shared(self.endpoint.clone())?
            .connect_timeout(std::time::Duration::from_secs(10))
            .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
            .http2_keep_alive_interval(std::time::Duration::from_secs(15))
            .keep_alive_timeout(std::time::Duration::from_secs(20))
            .keep_alive_while_idle(true);
        if let Some(tls) = client_tls_config_from_env() {
            endpoint = endpoint.tls_config(tls)?;
        }
        let channel = endpoint.connect().await?;
        let max = krishiv_proto::max_grpc_message_bytes();
        let client =
            wire::v1::coordinator_executor_client::CoordinatorExecutorClient::with_interceptor(
                channel,
                inject_coordinator_request_context
                    as fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
            )
            .max_decoding_message_size(max)
            .max_encoding_message_size(max);
        *guard = Some(client.clone());
        Ok(client)
    }

    /// Drop the cached client so the next call reconnects (used after stale-lease errors).
    pub async fn invalidate(&self) {
        let mut guard = self.client.lock().await;
        *guard = None;
    }

    pub fn lease_generation(&self) -> LeaseGeneration {
        self.lease.get()
    }

    pub fn set_lease_generation(&self, lease: LeaseGeneration) {
        self.lease.set(lease);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inject_coordinator_bearer_token_adds_authorization_metadata() {
        let request =
            inject_coordinator_bearer_token(tonic::Request::new(()), " coord-secret ").unwrap();

        let auth = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok());
        assert_eq!(auth, Some("Bearer coord-secret"));
    }
}
