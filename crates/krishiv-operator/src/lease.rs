//! K8s lease election.

use std::fmt;

use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use krishiv_scheduler::LeaderElection;
use kube::api::{Api, ObjectMeta as KubeObjectMeta, Patch, PatchParams, PostParams};

///
/// In production this communicates with the Kubernetes API using the `kube`
/// client.  When `client` is `Some`, all lease operations use live K8s API
/// calls with optimistic concurrency via `resourceVersion`.  When `client` is
/// `None` the implementation falls back to a simulated in-process lease, which
/// is used by unit tests so they can run without a real cluster.
///
/// Lease duration: configurable (default 15 s, matching K8s controller-manager).
/// Renewal interval: every `lease_duration_s / 3` seconds.
/// Fencing: each successful `try_acquire` increments the fencing token so stale
/// coordinators are rejected at [`validate_fencing_token`] call sites.
///
/// [`validate_fencing_token`]: krishiv_checkpoint::validate_fencing_token
pub struct K8sLeaseElection {
    lease_name: String,
    namespace: String,
    holder_identity: String,
    lease_duration_s: u64,
    /// Live K8s client.  `None` → simulation mode (unit tests).
    client: Option<kube::Client>,
    state: std::sync::Mutex<K8sLeaseState>,
}

impl fmt::Debug for K8sLeaseElection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("K8sLeaseElection")
            .field("lease_name", &self.lease_name)
            .field("namespace", &self.namespace)
            .field("holder_identity", &self.holder_identity)
            .field("lease_duration_s", &self.lease_duration_s)
            .field("client", &self.client.as_ref().map(|_| "<kube::Client>"))
            .field("state", &self.state)
            .finish()
    }
}

#[derive(Debug)]
struct K8sLeaseState {
    is_leader: bool,
    fencing_token: u64,
    /// Wall-clock time of the last successful acquire or renew.
    /// Used to auto-evict stale `is_leader = true` state when the renewal loop
    /// dies without calling `release()`.
    last_renewed_at: Option<std::time::Instant>,
}

impl K8sLeaseElection {
    /// Create a new election handle in **simulation mode** (no K8s client).
    ///
    /// `lease_name` — the K8s Lease object name (typically the job id or coordinator id).
    /// `namespace` — the K8s namespace containing the Lease.
    /// `holder_identity` — unique coordinator identity (pod name / hostname).
    pub fn new(
        lease_name: impl Into<String>,
        namespace: impl Into<String>,
        holder_identity: impl Into<String>,
    ) -> Self {
        Self {
            lease_name: lease_name.into(),
            namespace: namespace.into(),
            holder_identity: holder_identity.into(),
            lease_duration_s: 15,
            client: None,
            state: std::sync::Mutex::new(K8sLeaseState {
                is_leader: false,
                fencing_token: 0,
                last_renewed_at: None,
            }),
        }
    }

    /// Attach a live `kube::Client` to enable real K8s Lease API calls.
    ///
    /// When a client is present, `try_acquire`, `renew`, and `release` all
    /// issue actual HTTP requests to the Kubernetes API server.
    #[must_use]
    pub fn with_kube_client(mut self, client: kube::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Set the lease duration in seconds (default: 15).
    #[must_use]
    pub fn with_lease_duration(mut self, secs: u64) -> Self {
        self.lease_duration_s = secs;
        self
    }

    /// Lease name.
    pub fn lease_name(&self) -> &str {
        &self.lease_name
    }

    /// Namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Holder identity.
    pub fn holder_identity(&self) -> &str {
        &self.holder_identity
    }

    /// Configured lease duration in seconds.
    pub fn lease_duration_s(&self) -> u64 {
        self.lease_duration_s
    }

    // ── Live K8s helpers ──────────────────────────────────────────────────────

    /// Returns a namespaced `Api<Lease>` using the stored client.
    fn lease_api(&self, client: &kube::Client) -> Api<Lease> {
        Api::namespaced(client.clone(), &self.namespace)
    }

    /// Current UTC time as a `MicroTime`.
    fn now_micro() -> MicroTime {
        MicroTime(k8s_openapi::jiff::Timestamp::now())
    }

    /// Try to acquire the lease via live K8s API calls.
    ///
    /// Returns `true` and increments the fencing token when the API server
    /// confirms the write.  Returns `false` on any conflict or error.
    async fn k8s_try_acquire(&self, client: &kube::Client) -> bool {
        let api = self.lease_api(client);
        let now = Self::now_micro();

        match api.get_opt(&self.lease_name).await {
            Err(e) => {
                tracing::warn!(
                    lease = %self.lease_name,
                    error = %e,
                    "k8s_lease: GET failed during try_acquire"
                );
                false
            }
            Ok(None) => {
                // Lease does not exist — create it.
                let lease = Lease {
                    metadata: KubeObjectMeta {
                        name: Some(self.lease_name.clone()),
                        namespace: Some(self.namespace.clone()),
                        ..Default::default()
                    },
                    spec: Some(k8s_openapi::api::coordination::v1::LeaseSpec {
                        holder_identity: Some(self.holder_identity.clone()),
                        lease_duration_seconds: Some(self.lease_duration_s as i32),
                        acquire_time: Some(now.clone()),
                        renew_time: Some(now),
                        lease_transitions: Some(1),
                        ..Default::default()
                    }),
                };
                match api.create(&PostParams::default(), &lease).await {
                    Ok(_) => {
                        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
                        s.fencing_token += 1;
                        s.is_leader = true;
                        s.last_renewed_at = Some(std::time::Instant::now());
                        true
                    }
                    Err(e) => {
                        tracing::warn!(
                            lease = %self.lease_name,
                            error = %e,
                            "k8s_lease: POST failed during try_acquire"
                        );
                        false
                    }
                }
            }
            Ok(Some(existing)) => {
                // Check if we already hold the lease or if it has expired.
                let holder = existing
                    .spec
                    .as_ref()
                    .and_then(|s| s.holder_identity.as_deref())
                    .unwrap_or("");
                let renew_time = existing
                    .spec
                    .as_ref()
                    .and_then(|s| s.renew_time.as_ref())
                    .map(|t| t.0.as_second());
                let duration = existing
                    .spec
                    .as_ref()
                    .and_then(|s| s.lease_duration_seconds)
                    .unwrap_or(self.lease_duration_s as i32) as i64;
                let now_ts = k8s_openapi::jiff::Timestamp::now().as_second();
                let is_ours = holder == self.holder_identity;
                let is_expired = renew_time.map(|rt| rt + duration < now_ts).unwrap_or(true);

                if !is_ours && !is_expired {
                    // Another holder owns a live lease — we cannot take over.
                    return false;
                }

                // Patch the existing lease (optimistic concurrency via resourceVersion).
                // S7: Fail when resourceVersion is None or empty — an empty version
                // makes optimistic-concurrency an unconditional write (split-brain).
                let Some(resource_version) = existing
                    .metadata
                    .resource_version
                    .clone()
                    .filter(|v| !v.is_empty())
                else {
                    tracing::warn!(
                        lease = %self.lease_name,
                        "k8s_lease: missing resourceVersion during try_acquire — cannot patch safely"
                    );
                    return false;
                };
                let patch_value = serde_json::json!({
                    "apiVersion": "coordination.k8s.io/v1",
                    "kind": "Lease",
                    "metadata": {
                        "name": self.lease_name,
                        "namespace": self.namespace,
                        "resourceVersion": resource_version,
                    },
                    "spec": {
                        "holderIdentity": self.holder_identity,
                        "leaseDurationSeconds": self.lease_duration_s as i32,
                        "renewTime": now,
                    }
                });
                let patch = Patch::Merge(patch_value);
                match api
                    .patch(&self.lease_name, &PatchParams::default(), &patch)
                    .await
                {
                    Ok(_) => {
                        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
                        s.fencing_token += 1;
                        s.is_leader = true;
                        s.last_renewed_at = Some(std::time::Instant::now());
                        true
                    }
                    Err(kube::Error::Api(ref ae)) if ae.code == 409 => {
                        tracing::warn!(
                            lease = %self.lease_name,
                            "k8s_lease: PATCH conflict (409) during try_acquire — another holder won the race"
                        );
                        false
                    }
                    Err(e) => {
                        tracing::warn!(
                            lease = %self.lease_name,
                            error = %e,
                            "k8s_lease: PATCH failed during try_acquire"
                        );
                        false
                    }
                }
            }
        }
    }

    /// Renew the lease via live K8s API calls.
    ///
    /// Returns `true` when renewTime is updated successfully.  Returns `false`
    /// if another holder has taken over or on any API error.
    async fn k8s_renew(&self, client: &kube::Client) -> bool {
        let api = self.lease_api(client);

        let existing = match api.get_opt(&self.lease_name).await {
            Ok(Some(l)) => l,
            Ok(None) => {
                tracing::warn!(lease = %self.lease_name, "k8s_lease: lease not found during renew");
                self.state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .is_leader = false;
                return false;
            }
            Err(e) => {
                tracing::warn!(lease = %self.lease_name, error = %e, "k8s_lease: GET failed during renew");
                self.state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .is_leader = false;
                return false;
            }
        };

        let holder = existing
            .spec
            .as_ref()
            .and_then(|s| s.holder_identity.as_deref())
            .unwrap_or("");
        if holder != self.holder_identity {
            tracing::warn!(
                lease = %self.lease_name,
                current_holder = %holder,
                our_identity = %self.holder_identity,
                "k8s_lease: holderIdentity mismatch during renew — lost leadership"
            );
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_leader = false;
            return false;
        }

        // S7: Fail when resourceVersion is None or empty.
        let Some(resource_version) = existing
            .metadata
            .resource_version
            .clone()
            .filter(|v| !v.is_empty())
        else {
            tracing::warn!(
                lease = %self.lease_name,
                "k8s_lease: missing resourceVersion during renew — cannot patch safely"
            );
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_leader = false;
            return false;
        };
        let now = Self::now_micro();
        let patch_value = serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": self.lease_name,
                "namespace": self.namespace,
                "resourceVersion": resource_version,
            },
            "spec": {
                "renewTime": now,
            }
        });
        let patch = Patch::Merge(patch_value);
        match api
            .patch(&self.lease_name, &PatchParams::default(), &patch)
            .await
        {
            Ok(_) => {
                self.state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .last_renewed_at = Some(std::time::Instant::now());
                true
            }
            Err(kube::Error::Api(ref ae)) if ae.code == 409 => {
                tracing::warn!(
                    lease = %self.lease_name,
                    "k8s_lease: PATCH conflict (409) during renew — lost leadership"
                );
                self.state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .is_leader = false;
                false
            }
            Err(e) => {
                tracing::warn!(
                    lease = %self.lease_name,
                    error = %e,
                    "k8s_lease: PATCH failed during renew"
                );
                self.state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .is_leader = false;
                false
            }
        }
    }

    /// Release the lease via live K8s API calls.
    async fn k8s_release(&self, client: &kube::Client) {
        let api = self.lease_api(client);
        let patch_value = serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": self.lease_name,
                "namespace": self.namespace,
            },
            "spec": {
                "holderIdentity": "",
            }
        });
        let patch = Patch::Merge(patch_value);
        if let Err(e) = api
            .patch(&self.lease_name, &PatchParams::default(), &patch)
            .await
        {
            tracing::warn!(
                lease = %self.lease_name,
                error = %e,
                "k8s_lease: PATCH failed during release (ignoring)"
            );
        }
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_leader = false;
    }
}

#[async_trait::async_trait]
impl LeaderElection for K8sLeaseElection {
    fn is_leader(&self) -> bool {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if s.is_leader {
            let expired = s
                .last_renewed_at
                .is_none_or(|t| t.elapsed().as_secs() > self.lease_duration_s);
            if expired {
                s.is_leader = false;
            }
        }
        s.is_leader
    }

    /// Attempt to acquire the leader lease.
    ///
    /// When a `kube::Client` is present, issues a live K8s Lease API call using
    /// `.await` directly — no `block_on` that would panic inside a Tokio runtime.
    async fn try_acquire(&self) -> bool {
        if let Some(ref client) = self.client {
            self.k8s_try_acquire(client).await
        } else {
            if krishiv_common::is_production_mode() {
                tracing::error!(
                    "K8s lease election simulation mode is forbidden in production; \
                     attach a kube client via with_kube_client()"
                );
                return false;
            }
            // Simulation mode: increment fencing token and mark as leader.
            let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
            s.fencing_token += 1;
            s.is_leader = true;
            s.last_renewed_at = Some(std::time::Instant::now());
            true
        }
    }

    /// Renew the current leader lease.
    ///
    /// Uses `.await` instead of `block_on` so the call is safe inside any async
    /// executor context (ADR-R12-02 Option B fix).
    async fn renew(&self) -> bool {
        if let Some(ref client) = self.client {
            self.k8s_renew(client).await
        } else {
            // Simulation: renewal succeeds as long as we are still marked leader.
            let is_leader = self
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_leader;
            if is_leader {
                self.state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .last_renewed_at = Some(std::time::Instant::now());
            }
            is_leader
        }
    }

    /// Release the leader lease voluntarily (graceful shutdown).
    ///
    /// Uses `.await` instead of `block_on` (ADR-R12-02 Option B fix).
    async fn release(&self) {
        if let Some(ref client) = self.client {
            self.k8s_release(client).await;
        } else {
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_leader = false;
        }
    }

    fn fencing_token(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .fencing_token
    }
}
