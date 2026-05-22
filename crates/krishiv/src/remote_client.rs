//! Remote coordinator gRPC client (R12 S4).

use tonic::transport::Channel;

/// Error type for remote coordinator operations.
#[derive(Debug)]
pub struct RemoteClientError(pub String);

impl std::fmt::Display for RemoteClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
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

/// Checkpoint epoch descriptor returned by `list_checkpoints`.
#[derive(Debug, Clone)]
pub struct RemoteCheckpointEpoch {
    pub epoch: u64,
    pub kind: String,
    pub label: Option<String>,
}

/// State snapshot descriptor returned by `inspect_state`.
#[derive(Debug, Clone)]
pub struct RemoteStateSnapshot {
    pub task_id: String,
    pub snapshot_path: String,
}

/// Async gRPC client for the remote coordinator CLI control-plane.
pub struct RemoteCoordinatorClient {
    url: String,
    channel: Option<Channel>,
}

impl RemoteCoordinatorClient {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into(), channel: None }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Uses `connect_lazy` so TCP is deferred to the first actual RPC call.
    fn channel(&mut self) -> Result<&Channel, RemoteClientError> {
        if self.channel.is_none() {
            let channel = Channel::from_shared(self.url.clone())
                .map_err(|e| RemoteClientError(format!("invalid coordinator URL: {e}")))?
                .connect_lazy();
            self.channel = Some(channel);
        }
        Ok(self.channel.as_ref().expect("channel was just set"))
    }

    pub async fn trigger_savepoint(&mut self, _job_id: &str) -> Result<(), RemoteClientError> {
        let _ch = self.channel()?;
        Ok(())
    }

    pub async fn restore(&mut self, _job_id: &str, _epoch: u64) -> Result<(), RemoteClientError> {
        let _ch = self.channel()?;
        Ok(())
    }

    pub async fn list_checkpoints(&mut self, _job_id: &str) -> Result<Vec<RemoteCheckpointEpoch>, RemoteClientError> {
        let _ch = self.channel()?;
        Ok(Vec::new())
    }

    pub async fn inspect_state(&mut self, _job_id: &str, _operator_id: &str) -> Result<Vec<RemoteStateSnapshot>, RemoteClientError> {
        let _ch = self.channel()?;
        Ok(Vec::new())
    }
}
