//! Cross-region federation layer for Krishiv distributed compute.
//!
//! Provides `RegionId`, `RoutingPolicy`, the `FederationClient` trait,
//! `SingleRegionFederationClient` (local short-circuit), and
//! `GlobalCoordinator` (multi-region round-robin / affinity routing).

use std::collections::HashMap;

use async_trait::async_trait;

// ── RegionId ──────────────────────────────────────────────────────────────────

/// Opaque identifier for a deployment region (e.g. `"us-east-1"`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RegionId(String);

impl RegionId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RegionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ── RoutingPolicy ─────────────────────────────────────────────────────────────

/// How the `GlobalCoordinator` selects a region for a new task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingPolicy {
    /// Distribute tasks evenly across all available regions.
    RoundRobin,
    /// Always route to the first region in the map (useful for single-region deployments).
    Primary,
}

// ── FederationClient ──────────────────────────────────────────────────────────

/// Trait implemented by both the local short-circuit and the remote gRPC
/// federation client (P3-26: async methods for non-blocking I/O).
#[async_trait]
pub trait FederationClient: Send + Sync {
    /// Submit a job to this region.  Returns an opaque remote job-id string.
    async fn submit_job(&self, job_id: &str, spec_json: &str) -> FederationResult<String>;

    /// Query job status from this region.
    async fn job_status(&self, remote_job_id: &str) -> FederationResult<JobStatusResponse>;

    /// Cancel a job in this region.
    async fn cancel_job(&self, remote_job_id: &str) -> FederationResult<()>;
}

// ── FederationResult / FederationError ────────────────────────────────────────

pub type FederationResult<T> = Result<T, FederationError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FederationError(pub String);

impl std::fmt::Display for FederationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FederationError {}

// ── JobStatusResponse ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobStatusResponse {
    pub remote_job_id: String,
    pub state: String,
}

// ── RemoteFederationClient ────────────────────────────────────────────────────

/// gRPC/HTTP federation client that forwards to a regional coordinator (WS-11 / ADR-19.1).
#[derive(Debug, Clone)]
pub struct RemoteFederationClient {
    pub region: RegionId,
    pub coordinator_url: String,
    http: reqwest::Client,
}

impl RemoteFederationClient {
    pub fn new(region: RegionId, coordinator_url: impl Into<String>) -> Self {
        Self {
            region,
            coordinator_url: coordinator_url.into(),
            http: reqwest::Client::new(),
        }
    }

    fn federation_url(&self, path: &str) -> String {
        format!(
            "{}/federation/v1{}",
            self.coordinator_url.trim_end_matches('/'),
            path
        )
    }
}

#[derive(serde::Serialize)]
struct SubmitJobBody<'a> {
    job_id: &'a str,
    spec_json: &'a str,
}

#[derive(serde::Deserialize)]
struct SubmitJobResponse {
    remote_job_id: String,
}

#[derive(serde::Deserialize)]
struct JobStatusBody {
    remote_job_id: String,
    state: String,
}

#[async_trait]
impl FederationClient for RemoteFederationClient {
    async fn submit_job(&self, job_id: &str, spec_json: &str) -> FederationResult<String> {
        let resp = self
            .http
            .post(self.federation_url("/jobs"))
            .json(&SubmitJobBody { job_id, spec_json })
            .send()
            .await
            .map_err(|e| FederationError(format!("submit_job HTTP: {e}")))?;
        if !resp.status().is_success() {
            return Err(FederationError(format!(
                "submit_job HTTP {}",
                resp.status()
            )));
        }
        let body: SubmitJobResponse = resp
            .json()
            .await
            .map_err(|e| FederationError(format!("submit_job decode: {e}")))?;
        Ok(body.remote_job_id)
    }

    async fn job_status(&self, remote_job_id: &str) -> FederationResult<JobStatusResponse> {
        let url = self.federation_url(&format!("/jobs/{remote_job_id}"));
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| FederationError(format!("job_status HTTP: {e}")))?;
        if !resp.status().is_success() {
            return Err(FederationError(format!("job_status HTTP {}", resp.status())));
        }
        let body: JobStatusBody = resp
            .json()
            .await
            .map_err(|e| FederationError(format!("job_status decode: {e}")))?;
        Ok(JobStatusResponse {
            remote_job_id: body.remote_job_id,
            state: body.state,
        })
    }

    async fn cancel_job(&self, remote_job_id: &str) -> FederationResult<()> {
        let url = self.federation_url(&format!("/jobs/{remote_job_id}/cancel"));
        let resp = self
            .http
            .post(url)
            .send()
            .await
            .map_err(|e| FederationError(format!("cancel_job HTTP: {e}")))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(FederationError(format!("cancel_job HTTP {}", resp.status())))
        }
    }
}

// ── SingleRegionFederationClient ──────────────────────────────────────────────

/// In-process short-circuit client used when Krishiv runs in a single region.
/// Operations are no-ops that always succeed so the `GlobalCoordinator` code
/// path can be exercised without a real remote endpoint.
#[derive(Debug, Clone)]
pub struct SingleRegionFederationClient {
    pub region: RegionId,
    pub coordinator_url: String,
}

impl SingleRegionFederationClient {
    pub fn new(region: RegionId, coordinator_url: impl Into<String>) -> Self {
        Self {
            region,
            coordinator_url: coordinator_url.into(),
        }
    }
}

#[async_trait]
impl FederationClient for SingleRegionFederationClient {
    async fn submit_job(&self, job_id: &str, _spec_json: &str) -> FederationResult<String> {
        tracing::debug!(region = %self.region, job_id, "SingleRegionFederationClient: submit_job (no-op)");
        Ok(job_id.to_owned())
    }

    async fn job_status(&self, remote_job_id: &str) -> FederationResult<JobStatusResponse> {
        tracing::debug!(region = %self.region, remote_job_id, "SingleRegionFederationClient: job_status (no-op)");
        Ok(JobStatusResponse {
            remote_job_id: remote_job_id.to_owned(),
            state: "Running".to_owned(),
        })
    }

    async fn cancel_job(&self, remote_job_id: &str) -> FederationResult<()> {
        tracing::debug!(region = %self.region, remote_job_id, "SingleRegionFederationClient: cancel_job (no-op)");
        Ok(())
    }
}

// ── RegionEntry ───────────────────────────────────────────────────────────────

struct RegionEntry {
    coordinator_url: String,
    client: Box<dyn FederationClient>,
}

// ── GlobalCoordinator ─────────────────────────────────────────────────────────

/// Routes tasks across multiple regions using a configurable `RoutingPolicy`.
pub struct GlobalCoordinator {
    regions: HashMap<RegionId, RegionEntry>,
    region_order: Vec<RegionId>,
    policy: RoutingPolicy,
    round_robin_idx: std::sync::atomic::AtomicUsize,
}

impl GlobalCoordinator {
    /// Build a `GlobalCoordinator` from a list of `(RegionId, coordinator_url, client)` tuples.
    pub fn new(
        entries: Vec<(RegionId, String, Box<dyn FederationClient>)>,
        policy: RoutingPolicy,
    ) -> Self {
        let mut regions: HashMap<RegionId, RegionEntry> = HashMap::new();
        let mut region_order: Vec<RegionId> = Vec::new();
        for (region, url, client) in entries {
            region_order.push(region.clone());
            regions.insert(
                region,
                RegionEntry {
                    coordinator_url: url,
                    client,
                },
            );
        }
        region_order.sort_by(|a, b| a.0.cmp(&b.0));
        Self {
            regions,
            region_order,
            policy,
            round_robin_idx: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Select a region for the given job_id and return the coordinator URL.
    pub fn route_task(&self, _job_id: &str) -> FederationResult<&str> {
        if self.region_order.is_empty() {
            return Err(FederationError("no regions configured".to_owned()));
        }
        let region = match self.policy {
            RoutingPolicy::Primary => &self.region_order[0],
            RoutingPolicy::RoundRobin => {
                let idx = self
                    .round_robin_idx
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    % self.region_order.len();
                &self.region_order[idx]
            }
        };
        let entry = self
            .regions
            .get(region)
            .ok_or_else(|| FederationError(format!("region {region} not found")))?;
        Ok(entry.coordinator_url.as_str())
    }

    /// Return the `FederationClient` for the region that `route_task` would select.
    pub fn route_client(&self, _job_id: &str) -> FederationResult<&dyn FederationClient> {
        if self.region_order.is_empty() {
            return Err(FederationError("no regions configured".to_owned()));
        }
        let region = match self.policy {
            RoutingPolicy::Primary => &self.region_order[0],
            RoutingPolicy::RoundRobin => {
                let idx = self
                    .round_robin_idx
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    % self.region_order.len();
                &self.region_order[idx]
            }
        };
        let entry = self
            .regions
            .get(region)
            .ok_or_else(|| FederationError(format!("region {region} not found")))?;
        Ok(entry.client.as_ref())
    }

    pub fn region_count(&self) -> usize {
        self.regions.len()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(region: &str, url: &str) -> (RegionId, String, Box<dyn FederationClient>) {
        let r = RegionId::new(region);
        let c: Box<dyn FederationClient> =
            Box::new(SingleRegionFederationClient::new(r.clone(), url));
        (r, url.to_owned(), c)
    }

    #[test]
    fn global_coordinator_round_robin_routes_to_configured_url() {
        let gc = GlobalCoordinator::new(
            vec![make_entry("us-east-1", "http://coord-east:7070")],
            RoutingPolicy::RoundRobin,
        );
        let url = gc.route_task("job-abc").unwrap();
        assert_eq!(url, "http://coord-east:7070");
    }

    #[test]
    fn global_coordinator_primary_always_returns_first_region() {
        let gc = GlobalCoordinator::new(
            vec![
                make_entry("eu-west-1", "http://coord-eu:7070"),
                make_entry("us-east-1", "http://coord-us:7070"),
            ],
            RoutingPolicy::Primary,
        );
        let url = gc.route_task("job-x").unwrap();
        assert_eq!(url, "http://coord-eu:7070");
        let url2 = gc.route_task("job-y").unwrap();
        assert_eq!(url2, "http://coord-eu:7070");
    }

    #[test]
    fn global_coordinator_empty_returns_error() {
        let gc = GlobalCoordinator::new(vec![], RoutingPolicy::RoundRobin);
        assert!(gc.route_task("job-z").is_err());
    }

    #[tokio::test]
    async fn single_region_client_submit_returns_job_id() {
        let client =
            SingleRegionFederationClient::new(RegionId::new("us-east-1"), "http://localhost:7070");
        let result = client.submit_job("job-123", "{}").await.unwrap();
        assert_eq!(result, "job-123");
    }

    #[tokio::test]
    async fn remote_client_requires_live_coordinator() {
        let client = RemoteFederationClient::new(
            RegionId::new("us-west-2"),
            "http://127.0.0.1:1",
        );
        assert!(client.submit_job("job-1", "{}").await.is_err());
    }

    #[tokio::test]
    async fn single_region_client_status_returns_running() {
        let client =
            SingleRegionFederationClient::new(RegionId::new("us-east-1"), "http://localhost:7070");
        let status = client.job_status("job-123").await.unwrap();
        assert_eq!(status.state, "Running");
    }
}
