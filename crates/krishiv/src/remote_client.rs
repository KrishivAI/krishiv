//! gRPC client for remote coordinator management RPCs (GAP-RT-04).
//!
//! Uses `connect_lazy` so no TCP handshake happens during tests or CLI startup.
//! All methods proxy to the generated `CoordinatorManagementClient`.

use krishiv_proto::wire::v1::coordinator_management_client::CoordinatorManagementClient;
use tonic::transport::Channel;

const COORDINATOR_BEARER_TOKEN_ENV: &str = "KRISHIV_COORDINATOR_BEARER_TOKEN";

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
            let endpoint = endpoint_for(
                &self.url,
                krishiv_executor::grpc_client::client_tls_config_from_env(),
            )?;
            let channel = endpoint.connect_lazy();
            self.client = Some(CoordinatorManagementClient::new(channel));
        }
        self.client
            .as_mut()
            .ok_or_else(|| RemoteClientError("failed to initialize gRPC client".into()))
    }

    fn request<T>(&self, message: T) -> Result<tonic::Request<T>, RemoteClientError> {
        let mut request = tonic::Request::new(message);
        if let Some(token) = configured_coordinator_bearer_token() {
            check_token_transport(&self.url, true)?;
            inject_coordinator_bearer_token(&mut request, &token)?;
        }
        Ok(request)
    }

    /// Trigger a savepoint for the given job on the remote coordinator.
    pub async fn trigger_savepoint(
        &mut self,
        job_id: &str,
        label: Option<&str>,
    ) -> Result<(), RemoteClientError> {
        let req = krishiv_proto::wire::v1::TriggerSavepointRequest {
            job_id: job_id.to_owned(),
            label: label.unwrap_or_default().to_owned(),
            stop: false,
        };
        let request = self.request(req)?;
        self.client()?
            .trigger_savepoint(request)
            .await
            .map_err(RemoteClientError::from)?;
        Ok(())
    }

    /// Request a restore from a specific checkpoint epoch on the remote coordinator.
    pub async fn restore(
        &mut self,
        job_id: &str,
        epoch: u64,
        storage_path: &str,
        from_savepoint: bool,
    ) -> Result<(), RemoteClientError> {
        let req = krishiv_proto::wire::v1::RestoreJobRequest {
            job_id: job_id.to_owned(),
            epoch,
            storage_path: storage_path.to_owned(),
            from_savepoint,
        };
        let request = self.request(req)?;
        let resp = self
            .client()?
            .restore_job(request)
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
        let request = self.request(req)?;
        let resp = self
            .client()?
            .list_checkpoints(request)
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
        let request = self.request(req)?;
        let resp = self
            .client()?
            .inspect_state(request)
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

/// Build the channel endpoint for `url`, with TLS for `https://`.
///
/// `url.parse::<Endpoint>()` never configures TLS, so every `https://`
/// coordinator failed and only plaintext ones worked. The trust root comes
/// from `KRISHIV_CA_CERT`, as for executors.
fn endpoint_for(
    url: &str,
    tls: Option<tonic::transport::ClientTlsConfig>,
) -> Result<tonic::transport::Endpoint, RemoteClientError> {
    let endpoint = tonic::transport::Endpoint::from_shared(url.to_owned())
        .map_err(|e| RemoteClientError(e.to_string()))?;
    if !url
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("https://")
    {
        return Ok(endpoint);
    }
    let tls = tls.ok_or_else(|| {
        RemoteClientError(format!(
            "{url} uses TLS: set KRISHIV_CA_CERT to the PEM bundle that signs the \
             coordinator's certificate"
        ))
    })?;
    endpoint
        .tls_config(tls)
        .map_err(|e| RemoteClientError(format!("TLS configuration for {url}: {e}")))
}

/// Refuse to put the bearer token on a plaintext connection that leaves this
/// machine.
fn check_token_transport(url: &str, has_token: bool) -> Result<(), RemoteClientError> {
    let lower = url.trim().to_ascii_lowercase();
    let Some(rest) = lower.strip_prefix("http://") else {
        return Ok(());
    };
    let authority = rest.split('/').next().unwrap_or("");
    let host = if let Some(v6) = authority.strip_prefix('[') {
        v6.split(']').next().unwrap_or("")
    } else {
        authority
            .rsplit_once(':')
            .map_or(authority, |(host, _port)| host)
    };
    let loopback = matches!(host, "localhost" | "127.0.0.1" | "::1");
    if has_token && !loopback {
        return Err(RemoteClientError(format!(
            "refusing to send {COORDINATOR_BEARER_TOKEN_ENV} in cleartext to {url}; use an \
             https:// URL (with KRISHIV_CA_CERT)"
        )));
    }
    Ok(())
}

fn configured_coordinator_bearer_token() -> Option<String> {
    std::env::var(COORDINATOR_BEARER_TOKEN_ENV)
        .ok()
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
}

fn inject_coordinator_bearer_token<T>(
    request: &mut tonic::Request<T>,
    token: &str,
) -> Result<(), RemoteClientError> {
    let header = format!("Bearer {}", token.trim());
    let value = tonic::metadata::MetadataValue::try_from(header.as_str()).map_err(|_| {
        RemoteClientError(format!(
            "{COORDINATOR_BEARER_TOKEN_ENV} contains characters that are invalid for gRPC metadata"
        ))
    })?;
    request.metadata_mut().insert("authorization", value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// M6: `https://` must configure TLS (it used to fail with tonic's
    /// "HttpsUriWithoutTlsSupport"), and the bearer token must never be sent
    /// in cleartext to a coordinator on another host.
    #[test]
    fn https_needs_a_trust_root_and_tokens_never_go_out_in_cleartext() {
        // No KRISHIV_CA_CERT in the test environment: https is refused with a
        // message naming what to set, instead of an opaque transport error.
        let err = endpoint_for("https://coordinator:7070", None).unwrap_err();
        assert!(err.to_string().contains("KRISHIV_CA_CERT"), "{err}");

        assert!(check_token_transport("http://coordinator:7070", true).is_err());
        assert!(check_token_transport("http://127.0.0.1:7070", true).is_ok());
        assert!(check_token_transport("http://localhost:7070", true).is_ok());
        assert!(check_token_transport("https://coordinator:7070", true).is_ok());
        assert!(check_token_transport("http://coordinator:7070", false).is_ok());
    }

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

    #[test]
    fn inject_coordinator_bearer_token_adds_authorization_metadata() {
        let mut request = tonic::Request::new(());

        inject_coordinator_bearer_token(&mut request, " coord-secret ").unwrap();

        let auth = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok());
        assert_eq!(auth, Some("Bearer coord-secret"));
    }
}
