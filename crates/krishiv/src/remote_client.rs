//! Stub gRPC client for remote coordinator dispatch.
//!
//! All async methods use `connect_lazy` so no TCP handshake happens during
//! tests or CLI startup — the connection is deferred to the first actual RPC.

use tonic::transport::Channel;

/// Error type for remote coordinator calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteClientError(pub String);

impl std::fmt::Display for RemoteClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "remote coordinator error: {}", self.0)
    }
}

impl std::error::Error for RemoteClientError {}

impl From<tonic::transport::Error> for RemoteClientError {
    fn from(e: tonic::transport::Error) -> Self {
        Self(format!("transport error: {e}"))
    }
}

impl From<tonic::Status> for RemoteClientError {
    fn from(s: tonic::Status) -> Self {
        Self(format!("rpc status {}: {}", s.code(), s.message()))
    }
}

/// A single checkpoint epoch returned by `list_checkpoints`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteCheckpointEpoch {
    pub epoch: u64,
    pub kind: String,
    pub label: Option<String>,
}

/// A single state snapshot returned by `inspect_state`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteStateSnapshot {
    pub task_id: String,
    pub snapshot_path: String,
}

/// gRPC client for a remote coordinator.
///
/// Connection is lazy — no TCP is opened until the first RPC is sent.
pub struct RemoteCoordinatorClient {
    url: String,
    channel: Option<Channel>,
}

impl RemoteCoordinatorClient {
    /// Create a client pointing at `url` (e.g. `http://coordinator:7070`).
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            channel: None,
        }
    }

    fn channel(&mut self) -> Result<&Channel, RemoteClientError> {
        if self.channel.is_none() {
            let endpoint = self
                .url
                .parse::<tonic::transport::Endpoint>()
                .map_err(|e| RemoteClientError(e.to_string()))?;
            self.channel = Some(endpoint.connect_lazy());
        }
        Ok(self.channel.as_ref().unwrap())
    }

    /// Trigger a savepoint for the given job on the remote coordinator.
    pub async fn trigger_savepoint(&mut self, _job_id: &str) -> Result<(), RemoteClientError> {
        let _ch = self.channel()?;
        Ok(())
    }

    /// Request a restore from a specific checkpoint epoch on the remote coordinator.
    pub async fn restore(&mut self, _job_id: &str, _epoch: u64) -> Result<(), RemoteClientError> {
        let _ch = self.channel()?;
        Ok(())
    }

    /// List checkpoint epochs for a job on the remote coordinator.
    pub async fn list_checkpoints(
        &mut self,
        _job_id: &str,
    ) -> Result<Vec<RemoteCheckpointEpoch>, RemoteClientError> {
        let _ch = self.channel()?;
        Ok(Vec::new())
    }

    /// Inspect operator state snapshots for a job on the remote coordinator.
    pub async fn inspect_state(
        &mut self,
        _job_id: &str,
        _operator_id: &str,
    ) -> Result<Vec<RemoteStateSnapshot>, RemoteClientError> {
        let _ch = self.channel()?;
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_client_new_stores_url() {
        let client = RemoteCoordinatorClient::new("http://coord:7070");
        assert_eq!(client.url, "http://coord:7070");
        assert!(client.channel.is_none(), "channel must be lazy");
    }

    #[test]
    fn remote_client_error_display() {
        let e = RemoteClientError("connection refused".to_string());
        assert!(e.to_string().contains("connection refused"));
    }

    #[tokio::test]
    async fn trigger_savepoint_stub_returns_ok() {
        let mut client = RemoteCoordinatorClient::new("http://localhost:9999");
        let result = client.trigger_savepoint("job-1").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn list_checkpoints_stub_returns_empty() {
        let mut client = RemoteCoordinatorClient::new("http://localhost:9999");
        let result = client.list_checkpoints("job-1").await;
        assert_eq!(result.unwrap(), Vec::new());
    }

    #[tokio::test]
    async fn inspect_state_stub_returns_empty() {
        let mut client = RemoteCoordinatorClient::new("http://localhost:9999");
        let result = client.inspect_state("job-1", "op-1").await;
        assert_eq!(result.unwrap(), Vec::new());
    }

    #[tokio::test]
    async fn restore_stub_returns_ok() {
        let mut client = RemoteCoordinatorClient::new("http://localhost:9999");
        let result = client.restore("job-1", 42).await;
        assert!(result.is_ok());
    }
}
