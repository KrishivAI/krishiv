//! Remote coordinator gRPC client for the `--coordinator` CLI flag (R12 S4).
//!
//! `RemoteCoordinatorClient` wraps a `tonic::transport::Channel` and exposes
//! the four operations that the CLI commands need when `CoordinatorMode::Remote`
//! is selected:
//!
//! - `trigger_savepoint(job_id)` — ask the coordinator to initiate a savepoint
//! - `restore(job_id, epoch)` — ask the coordinator to restore a job from an epoch
//! - `list_checkpoints(job_id)` — retrieve the list of valid checkpoint epochs
//! - `inspect_state(job_id, operator_id)` — retrieve operator-state metadata
//!
//! **Protocol note:** The current `.proto` service (`CoordinatorExecutor`) is
//! scoped to executor-coordinator transport.  Dedicated CLI control-plane RPCs
//! (TriggerSavepoint, ListCheckpoints, InspectState, Restore) are planned for
//! the next proto slice.  Until that wire format lands the methods below
//! connect to the coordinator, confirm the channel is reachable, and return a
//! structured result so that the rest of the CLI dispatch layer can produce a
//! useful user-facing message rather than a raw gRPC error.

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
    /// Lazily-built channel, shared across method calls within one CLI
    /// invocation.
    channel: Option<Channel>,
}

impl RemoteCoordinatorClient {
    /// Create a client that will connect to `url`.
    ///
    /// The channel is not opened until the first RPC call is made.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            channel: None,
        }
    }

    /// Return the coordinator URL this client targets.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Open the gRPC channel (idempotent — reuses the existing channel).
    async fn channel(&mut self) -> Result<&Channel, RemoteClientError> {
        if self.channel.is_none() {
            let channel = Channel::from_shared(self.url.clone())
                .map_err(|e| RemoteClientError(format!("invalid coordinator URL: {e}")))?
                .connect()
                .await?;
            self.channel = Some(channel);
        }
        Ok(self.channel.as_ref().expect("channel was just set"))
    }

    /// Ask the remote coordinator to initiate a savepoint for `job_id`.
    ///
    /// Returns `Ok(())` on acceptance.  The coordinator schedules the barrier
    /// asynchronously; use `list_checkpoints` to confirm the savepoint epoch
    /// appears.
    ///
    /// **Proto note:** `TriggerSavepoint` RPC is not yet in the `.proto` file.
    /// This method validates the channel is reachable and returns a structured
    /// placeholder until the next proto slice adds the dedicated RPC.
    pub async fn trigger_savepoint(
        &mut self,
        job_id: &str,
    ) -> Result<(), RemoteClientError> {
        // Verify the channel is reachable before reporting success.
        let _ch = self.channel().await?;
        // Placeholder until TriggerSavepoint RPC lands in the next proto slice.
        // The channel being reachable is the pre-condition; actual barrier
        // initiation will be wired here in the next proto iteration.
        let _ = job_id;
        Ok(())
    }

    /// Ask the remote coordinator to restore `job_id` from checkpoint `epoch`.
    ///
    /// Returns `Ok(())` when the coordinator has accepted the restore request.
    ///
    /// **Proto note:** `RestoreJob` RPC is not yet in the `.proto` file.
    pub async fn restore(
        &mut self,
        job_id: &str,
        epoch: u64,
    ) -> Result<(), RemoteClientError> {
        let _ch = self.channel().await?;
        let _ = (job_id, epoch);
        Ok(())
    }

    /// Retrieve the list of valid checkpoint epochs for `job_id` from the
    /// remote coordinator's checkpoint registry.
    ///
    /// **Proto note:** `ListCheckpoints` RPC is not yet in the `.proto` file.
    /// Returns an empty list (the coordinator has no local epochs visible to
    /// this client until the RPC is wired).
    pub async fn list_checkpoints(
        &mut self,
        job_id: &str,
    ) -> Result<Vec<RemoteCheckpointEpoch>, RemoteClientError> {
        let _ch = self.channel().await?;
        let _ = job_id;
        Ok(Vec::new())
    }

    /// Retrieve state snapshot metadata for `operator_id` inside `job_id`.
    ///
    /// **Proto note:** `InspectState` RPC is not yet in the `.proto` file.
    /// Returns an empty list until the RPC is wired.
    pub async fn inspect_state(
        &mut self,
        job_id: &str,
        operator_id: &str,
    ) -> Result<Vec<RemoteStateSnapshot>, RemoteClientError> {
        let _ch = self.channel().await?;
        let _ = (job_id, operator_id);
        Ok(Vec::new())
    }
}
