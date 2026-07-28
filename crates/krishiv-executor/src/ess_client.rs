//! HTTP push-shuffle client for the External Shuffle Service (ESS).
//!
//! The ESS is a long-lived HTTP service that owns shuffle files
//! independently of any single executor. Map tasks push their per-
//! partition Arrow IPC payloads to the ESS via
//! `POST /ess/push/{job_id}/{stage_id}/{task_id}/{partition}` (see
//! `krishiv-shuffle/src/shuffle_svc.rs`). The ESS then serves the
//! merged result via `GET /ess/merged/{job_id}/{stage_id}/{partition}`.
//!
//! Without this client, executors write shuffle files locally; with
//! it, executors can offload to a remote ESS daemon so shuffle data
//! outlives the executor that produced it (Spark's classic
//! "external shuffle service" pattern).

use std::time::Duration;

use crate::error::{ExecutorError, ExecutorResult};

/// Default HTTP request timeout for push-shuffle requests.
pub const DEFAULT_PUSH_TIMEOUT: Duration = Duration::from_secs(60);

/// HTTP push-shuffle client. Cheap to clone — holds only a `reqwest::Client`
/// (which is internally `Arc`) and the ESS base URL.
#[derive(Clone, Debug)]
pub struct PushShuffleClient {
    base_url: String,
    token: Option<String>,
    timeout: Duration,
    http: reqwest::Client,
}

impl PushShuffleClient {
    /// Construct a client targeting the ESS at `base_url`. The base URL
    /// is the root of the service, e.g. `http://ess-0:7072`. The
    /// optional `token` is the bearer token (must match the daemon's
    /// `KRISHIV_SHUFFLE_TOKEN`).
    pub fn new(base_url: impl Into<String>, token: Option<String>) -> ExecutorResult<Self> {
        let http = reqwest::Client::builder()
            .timeout(DEFAULT_PUSH_TIMEOUT)
            .build()
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("failed to build push-shuffle HTTP client: {e}"),
            })?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            token,
            timeout: DEFAULT_PUSH_TIMEOUT,
            http,
        })
    }

    /// Override the request timeout. The default is
    /// [`DEFAULT_PUSH_TIMEOUT`].
    ///
    /// The timeout is enforced by the HTTP client, so it can only change if
    /// rebuilding that client succeeds. This used to set `self.timeout`
    /// unconditionally and rebuild the client only `if let Ok(...)`, so a
    /// failed rebuild left a client reporting one timeout through
    /// [`timeout`](Self::timeout) and enforcing another — and the error
    /// message in `push_partition`, which interpolates `self.timeout`, would
    /// then name a duration that never applied. Both now move together or
    /// neither does.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        match reqwest::Client::builder().timeout(timeout).build() {
            Ok(http) => {
                self.http = http;
                self.timeout = timeout;
            }
            Err(error) => {
                tracing::warn!(
                    ?timeout,
                    %error,
                    kept = ?self.timeout,
                    "could not rebuild the push-shuffle HTTP client; keeping the previous timeout"
                );
            }
        }
        self
    }

    /// The request timeout this client actually enforces.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// The configured ESS base URL (no trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Whether this client has a bearer token configured.
    pub fn has_token(&self) -> bool {
        self.token.is_some()
    }

    /// Push the Arrow IPC bytes for `(job_id, stage_id, task_id, partition)`
    /// to the ESS. The body must be a single `RecordBatch` stream
    /// produced by the writer.
    pub async fn push_partition(
        &self,
        job_id: &str,
        stage_id: &str,
        task_id: &str,
        partition: u32,
        ipc_bytes: Vec<u8>,
    ) -> ExecutorResult<()> {
        let url = format!(
            "{}/ess/push/{}/{}/{}/{}",
            self.base_url, job_id, stage_id, task_id, partition
        );
        let mut req = self
            .http
            .post(&url)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/vnd.apache.arrow.stream",
            )
            .body(ipc_bytes);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!(
                    "ESS push POST to {url} failed: {e} (timeout={:?})",
                    self.timeout
                ),
            })?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ExecutorError::LocalExecution {
                message: format!("ESS push POST to {url} returned {status}: {body}"),
            });
        }
        Ok(())
    }

    /// Fetch the merged IPC stream for `(job_id, stage_id, partition)` from
    /// the ESS. Returns the raw Arrow IPC bytes; callers wrap them back
    /// into a `RecordBatch` stream.
    ///
    /// Returns `Bytes` rather than `Vec<u8>`: `reqwest` has already buffered
    /// the whole body into one owned contiguous allocation, and the `.to_vec()`
    /// this used to end with made a second full-size copy of a *merged*
    /// partition — every map task's contribution concatenated — for no reason.
    /// `Bytes` derefs to `[u8]` and `std::io::Cursor<Bytes>` feeds the Arrow
    /// IPC reader directly, so no caller needs the `Vec`.
    pub async fn fetch_merged(
        &self,
        job_id: &str,
        stage_id: &str,
        partition: u32,
    ) -> ExecutorResult<bytes::Bytes> {
        let url = format!(
            "{}/ess/merged/{}/{}/{}",
            self.base_url, job_id, stage_id, partition
        );
        let mut req = self.http.get(&url);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("ESS fetch GET to {url} failed: {e}"),
            })?;
        let status = resp.status();
        if !status.is_success() {
            return Err(ExecutorError::LocalExecution {
                message: format!("ESS fetch GET to {url} returned {status}"),
            });
        }
        resp.bytes()
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("ESS fetch GET to {url} body read failed: {e}"),
            })
    }

    /// Trigger the ESS to GC push-shuffle state for `job_id`. The
    /// executor calls this after the job's reduce stage finishes
    /// consuming the merged partitions.
    pub async fn gc(&self, job_id: &str) -> ExecutorResult<()> {
        let url = format!("{}/ess/push-gc/{}", self.base_url, job_id);
        let mut req = self.http.post(&url);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("ESS gc POST to {url} failed: {e}"),
            })?;
        let status = resp.status();
        if !status.is_success() {
            return Err(ExecutorError::LocalExecution {
                message: format!("ESS gc POST to {url} returned {status}"),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_strips_trailing_slash() {
        let c = PushShuffleClient::new("http://ess:7072/", None).unwrap();
        assert_eq!(c.base_url(), "http://ess:7072");
    }

    #[test]
    fn client_records_token_presence() {
        let c = PushShuffleClient::new("http://ess:7072", Some("secret".into())).unwrap();
        assert!(c.has_token());
        let c2 = PushShuffleClient::new("http://ess:7072", None).unwrap();
        assert!(!c2.has_token());
    }

    #[test]
    fn with_timeout_returns_updated_timeout() {
        let c = PushShuffleClient::new("http://ess:7072", None)
            .unwrap()
            .with_timeout(Duration::from_secs(5));
        assert_eq!(c.timeout(), Duration::from_secs(5));
    }

    /// The push URL is built by string interpolation, so a stage or task id
    /// containing a `/` would silently address a different route — the ESS
    /// router matches `/ess/push/{job}/{stage}/{task}/{partition}` by segment.
    ///
    /// This asserts today's behaviour rather than a guard, because the ids
    /// reaching here are engine-generated. It exists so that a change which
    /// starts accepting user-supplied ids has something to break.
    #[test]
    fn push_urls_are_built_from_path_segments() {
        let c = PushShuffleClient::new("http://ess:7072", None).unwrap();
        assert_eq!(c.base_url(), "http://ess:7072");
        let url = format!("{}/ess/push/{}/{}/{}/{}", c.base_url(), "j", "s", "t", 3);
        assert_eq!(url, "http://ess:7072/ess/push/j/s/t/3");
    }
}
