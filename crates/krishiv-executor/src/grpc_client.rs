//! Pooled gRPC client for coordinator RPCs (GAP-C3, B7).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use krishiv_proto::{LeaseGeneration, wire};
use tokio::sync::Mutex;
use tonic::transport::Channel;

/// Shared, atomically-updated lease generation handle.  The executor binary
/// owns one of these for the entire process; every component that sends a
/// coordinator RPC reads the live generation from it before transmitting so
/// retries after a lease bump cannot ship a stale lease (B7/B8/F1).
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
        // `LeaseGeneration::try_new` rejects 0 — `SharedLeaseGeneration` is
        // initialized via `LeaseGeneration::initial()` (=1) and only ever
        // monotonically increased, so this should always succeed.
        LeaseGeneration::try_new(raw.max(1)).unwrap_or_else(|_| LeaseGeneration::initial())
    }

    pub fn set(&self, lease: LeaseGeneration) {
        let new_val = lease.as_u64();
        // Monotonic: never go backwards.  fetch_max returns the previous value.
        self.inner.fetch_max(new_val, Ordering::Release);
    }
}

/// Type alias for the intercepted coordinator executor client.
type InterceptedCoordinatorClient = wire::v1::coordinator_executor_client::CoordinatorExecutorClient<
    tonic::service::interceptor::InterceptedService<
        Channel,
        fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
    >,
>;

/// Reuses one coordinator gRPC channel across RPCs and stamps the live lease
/// onto every outgoing executor-originated request.
#[derive(Clone)]
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
        // Double-check pattern: lock, check, unlock, connect, re-lock, store.
        {
            let guard = self.client.lock().await;
            if let Some(client) = guard.as_ref() {
                return Ok(client.clone());
            }
        }
        // Lock released — connect without holding the mutex.
        let channel = tonic::transport::Endpoint::from_shared(self.endpoint.clone())?
            .connect_timeout(std::time::Duration::from_secs(10))
            .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
            .http2_keep_alive_interval(std::time::Duration::from_secs(15))
            .keep_alive_timeout(std::time::Duration::from_secs(20))
            .keep_alive_while_idle(true)
            .connect()
            .await?;
        let client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::with_interceptor(
            channel,
            krishiv_metrics::grpc::inject_trace_context
                as fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
        );
        // Re-lock and store if still empty (another task may have connected first).
        let mut guard = self.client.lock().await;
        if let Some(existing) = guard.as_ref() {
            return Ok(existing.clone());
        }
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
