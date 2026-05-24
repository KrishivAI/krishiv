//! Pooled gRPC client for coordinator RPCs (GAP-C3).

use std::sync::Arc;

use krishiv_proto::{LeaseGeneration, wire};
use tokio::sync::Mutex;
use tonic::transport::Channel;

/// Reuses one coordinator gRPC channel across RPCs.
#[derive(Clone)]
pub struct CoordinatorGrpcPool {
    endpoint: String,
    client: Arc<
        Mutex<Option<wire::v1::coordinator_executor_client::CoordinatorExecutorClient<Channel>>>,
    >,
    lease: Arc<tokio::sync::RwLock<LeaseGeneration>>,
}

impl CoordinatorGrpcPool {
    pub fn new(endpoint: impl Into<String>, lease_generation: LeaseGeneration) -> Self {
        Self {
            endpoint: endpoint.into(),
            client: Arc::new(Mutex::new(None)),
            lease: Arc::new(tokio::sync::RwLock::new(lease_generation)),
        }
    }

    pub async fn client(
        &self,
    ) -> Result<
        wire::v1::coordinator_executor_client::CoordinatorExecutorClient<Channel>,
        tonic::transport::Error,
    > {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }
        let client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
            self.endpoint.clone(),
        )
        .await?;
        *guard = Some(client.clone());
        Ok(client)
    }

    pub async fn lease_generation(&self) -> LeaseGeneration {
        *self.lease.read().await
    }

    pub async fn set_lease_generation(&self, lease: LeaseGeneration) {
        *self.lease.write().await = lease;
    }
}
