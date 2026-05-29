//! gRPC client for remote coordinator management RPCs (GAP-RT-04).
//!
//! Uses `connect_lazy` so no TCP handshake happens during tests or CLI startup.
//! All methods proxy to the generated `CoordinatorManagementClient`.

use krishiv_proto::wire::v1::coordinator_management_client::CoordinatorManagementClient;
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

/// gRPC client for a remote coordinator management service.
///
/// Connection is lazy — no TCP is opened until the first RPC is sent.
pub struct RemoteCoordinatorClient {
    url: String,
    client: Option<CoordinatorManagementClient<Channel>>,
}

impl RemoteCoordinatorClient {
    /// Create a client pointing at `url` (e.g. `http://coordinator:7070`).
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            client: None,
        }
    }

    fn client(&mut self) -> Result<&mut CoordinatorManagementClient<Channel>, RemoteClientError> {
        if self.client.is_none() {
            let endpoint = self
                .url
                .parse::<tonic::transport::Endpoint>()
                .map_err(|e| RemoteClientError(e.to_string()))?;
            let channel = endpoint.connect_lazy();
            self.client = Some(CoordinatorManagementClient::new(channel));
        }
        self.client
            .as_mut()
            .ok_or_else(|| RemoteClientError("failed to initialize gRPC client".into()))
    }

    /// Trigger a savepoint for the given job on the remote coordinator.
    pub async fn trigger_savepoint(&mut self, job_id: &str) -> Result<(), RemoteClientError> {
        let req = krishiv_proto::wire::v1::TriggerSavepointRequest {
            job_id: job_id.to_owned(),
            label: String::new(),
        };
        self.client()?
            .trigger_savepoint(tonic::Request::new(req))
            .await
            .map_err(RemoteClientError::from)?;
        Ok(())
    }

    /// Trigger a named savepoint for the given job on the remote coordinator.
    pub async fn trigger_savepoint_with_label(
        &mut self,
        job_id: &str,
        label: &str,
    ) -> Result<u64, RemoteClientError> {
        let req = krishiv_proto::wire::v1::TriggerSavepointRequest {
            job_id: job_id.to_owned(),
            label: label.to_owned(),
        };
        let resp = self
            .client()?
            .trigger_savepoint(tonic::Request::new(req))
            .await
            .map_err(RemoteClientError::from)?;
        Ok(resp.into_inner().epoch)
    }

    /// Request a restore from a specific checkpoint epoch on the remote coordinator.
    pub async fn restore(
        &mut self,
        job_id: &str,
        epoch: u64,
        storage_path: &str,
    ) -> Result<(), RemoteClientError> {
        let req = krishiv_proto::wire::v1::RestoreJobRequest {
            job_id: job_id.to_owned(),
            epoch,
            storage_path: storage_path.to_owned(),
        };
        let resp = self
            .client()?
            .restore_job(tonic::Request::new(req))
            .await
            .map_err(RemoteClientError::from)?;
        let inner = resp.into_inner();
        if !inner.accepted {
            return Err(RemoteClientError(inner.message));
        }
        Ok(())
    }

    /// List checkpoint epochs for a job on the remote coordinator.
    pub async fn list_checkpoints(
        &mut self,
        job_id: &str,
    ) -> Result<Vec<RemoteCheckpointEpoch>, RemoteClientError> {
        let req = krishiv_proto::wire::v1::ListCheckpointsRequest {
            job_id: job_id.to_owned(),
        };
        let resp = self
            .client()?
            .list_checkpoints(tonic::Request::new(req))
            .await
            .map_err(RemoteClientError::from)?;
        let epochs = resp
            .into_inner()
            .epochs
            .into_iter()
            .map(|e| RemoteCheckpointEpoch {
                epoch: e.epoch,
                kind: if e.is_savepoint {
                    "savepoint".to_owned()
                } else {
                    "checkpoint".to_owned()
                },
                label: if e.savepoint_label.is_empty() {
                    None
                } else {
                    Some(e.savepoint_label)
                },
            })
            .collect();
        Ok(epochs)
    }

    /// Inspect operator state snapshots for a job on the remote coordinator.
    pub async fn inspect_state(
        &mut self,
        job_id: &str,
        operator_id: &str,
    ) -> Result<Vec<RemoteStateSnapshot>, RemoteClientError> {
        let req = krishiv_proto::wire::v1::InspectStateRequest {
            job_id: job_id.to_owned(),
            operator_id: operator_id.to_owned(),
        };
        let resp = self
            .client()?
            .inspect_state(tonic::Request::new(req))
            .await
            .map_err(RemoteClientError::from)?;
        let snapshots = resp
            .into_inner()
            .snapshots
            .into_iter()
            .map(|s| RemoteStateSnapshot {
                task_id: s.task_id,
                snapshot_path: s.snapshot_path,
            })
            .collect();
        Ok(snapshots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_client_new_stores_url() {
        let client = RemoteCoordinatorClient::new("http://coord:7070");
        assert_eq!(client.url, "http://coord:7070");
        assert!(client.client.is_none(), "channel must be lazy");
    }

    #[test]
    fn remote_client_error_display() {
        let e = RemoteClientError("connection refused".to_string());
        assert!(e.to_string().contains("connection refused"));
    }

    #[test]
    fn remote_client_error_from_status() {
        let status = tonic::Status::not_found("job not found");
        let e = RemoteClientError::from(status);
        assert!(e.to_string().contains("not_found") || e.to_string().contains("job not found"));
    }
}
