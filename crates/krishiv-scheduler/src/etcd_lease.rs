//! etcd v3 lease election for bare-metal cluster control plane HA.

use std::fmt;
use std::sync::Mutex;
use std::time::Instant;

use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, PutOptions, Txn, TxnOp};

use crate::LeaderElection;

/// Default leader key for the cluster control plane.
pub const DEFAULT_CCP_LEADER_KEY: &str = "/krishiv/ccp/leader";

#[derive(Debug)]
struct EtcdLeaseState {
    is_leader: bool,
    fencing_token: u64,
    lease_id: i64,
    last_renewed_at: Option<Instant>,
}

/// Leader election backed by an etcd v3 lease on a single coordination key.
///
/// When `client` is `None`, runs in **simulation mode** (unit tests, no etcd process).
pub struct EtcdLeaseElection {
    lease_key: String,
    holder_identity: String,
    lease_duration_s: u64,
    client: Option<tokio::sync::Mutex<Client>>,
    state: Mutex<EtcdLeaseState>,
}

impl fmt::Debug for EtcdLeaseElection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EtcdLeaseElection")
            .field("lease_key", &self.lease_key)
            .field("holder_identity", &self.holder_identity)
            .field("lease_duration_s", &self.lease_duration_s)
            .field("client", &self.client.as_ref().map(|_| "<etcd::Client>"))
            .field("state", &self.state)
            .finish()
    }
}

fn put_with_lease(key: &[u8], value: &[u8], lease_id: i64) -> TxnOp {
    TxnOp::put(
        key,
        value,
        Some(PutOptions::new().with_lease(lease_id)),
    )
}

impl EtcdLeaseElection {
    /// Simulation mode (no live etcd client).
    pub fn new(
        lease_key: impl Into<String>,
        holder_identity: impl Into<String>,
        lease_duration_s: u64,
    ) -> Self {
        Self {
            lease_key: lease_key.into(),
            holder_identity: holder_identity.into(),
            lease_duration_s: lease_duration_s.max(1),
            client: None,
            state: Mutex::new(EtcdLeaseState {
                is_leader: false,
                fencing_token: 0,
                lease_id: 0,
                last_renewed_at: None,
            }),
        }
    }

    /// Connect to etcd and return an election handle ready for live API calls.
    pub async fn connect(
        endpoints: Vec<String>,
        lease_key: impl Into<String>,
        holder_identity: impl Into<String>,
        lease_duration_s: u64,
    ) -> Result<Self, String> {
        let endpoints: Vec<String> = endpoints
            .into_iter()
            .map(|e| normalize_etcd_endpoint(&e))
            .collect();
        if endpoints.is_empty() {
            return Err(String::from("etcd endpoints list is empty"));
        }
        let client = Client::connect(&endpoints, None)
            .await
            .map_err(|e| format!("etcd connect failed: {e}"))?;
        Ok(Self {
            lease_key: lease_key.into(),
            holder_identity: holder_identity.into(),
            lease_duration_s: lease_duration_s.max(1),
            client: Some(tokio::sync::Mutex::new(client)),
            state: Mutex::new(EtcdLeaseState {
                is_leader: false,
                fencing_token: 0,
                lease_id: 0,
                last_renewed_at: None,
            }),
        })
    }

    pub fn lease_key(&self) -> &str {
        &self.lease_key
    }

    pub fn holder_identity(&self) -> &str {
        &self.holder_identity
    }

    pub fn lease_duration_s(&self) -> u64 {
        self.lease_duration_s
    }

    fn mark_leader(&self, lease_id: i64, bump_fence: bool) {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if bump_fence {
            s.fencing_token = s.fencing_token.saturating_add(1);
        }
        s.is_leader = true;
        s.lease_id = lease_id;
        s.last_renewed_at = Some(Instant::now());
    }

    fn clear_leader(&self) {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        s.is_leader = false;
        s.lease_id = 0;
    }

    async fn etcd_try_acquire(&self, client: &mut Client) -> bool {
        let renew_existing = {
            let s = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if s.is_leader && s.lease_id != 0 {
                Some(s.lease_id)
            } else {
                None
            }
        };
        if let Some(lease_id) = renew_existing {
            if self.etcd_ping_lease(client, lease_id).await {
                self.state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .last_renewed_at = Some(Instant::now());
                return true;
            }
            self.clear_leader();
        }

        let lease_id = match client
            .lease_grant(self.lease_duration_s as i64, None)
            .await
        {
            Ok(resp) => resp.id(),
            Err(error) => {
                tracing::warn!(
                    key = %self.lease_key,
                    %error,
                    "etcd_lease: lease_grant failed during try_acquire"
                );
                self.clear_leader();
                return false;
            }
        };

        let key = self.lease_key.as_bytes();
        let holder = self.holder_identity.as_bytes();

        let create_txn = Txn::new()
            .when([Compare::create_revision(key, CompareOp::Equal, 0)])
            .and_then([put_with_lease(key, holder, lease_id)]);
        match client.txn(create_txn).await {
            Ok(resp) if resp.succeeded() => {
                self.mark_leader(lease_id, true);
                return true;
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(
                    key = %self.lease_key,
                    %error,
                    "etcd_lease: create txn failed during try_acquire"
                );
                let _ = client.lease_revoke(lease_id).await;
                self.clear_leader();
                return false;
            }
        }

        let existing = match client.get(key, None).await {
            Ok(resp) => resp.kvs().first().cloned(),
            Err(error) => {
                tracing::warn!(
                    key = %self.lease_key,
                    %error,
                    "etcd_lease: GET failed during try_acquire"
                );
                let _ = client.lease_revoke(lease_id).await;
                self.clear_leader();
                return false;
            }
        };

        let Some(kv) = existing else {
            let _ = client.lease_revoke(lease_id).await;
            self.clear_leader();
            return false;
        };

        let current_holder = String::from_utf8_lossy(kv.value());
        let is_ours = current_holder == self.holder_identity;
        let lease_alive = kv.lease() != 0
            && self.etcd_lease_alive(client, kv.lease()).await;

        if !is_ours && lease_alive {
            let _ = client.lease_revoke(lease_id).await;
            self.clear_leader();
            return false;
        }

        let mod_revision = kv.mod_revision();
        let takeover_txn = Txn::new()
            .when([Compare::mod_revision(key, CompareOp::Equal, mod_revision)])
            .and_then([put_with_lease(key, holder, lease_id)]);
        match client.txn(takeover_txn).await {
            Ok(resp) if resp.succeeded() => {
                self.mark_leader(lease_id, true);
                true
            }
            Ok(_) => {
                let _ = client.lease_revoke(lease_id).await;
                self.clear_leader();
                false
            }
            Err(error) => {
                tracing::warn!(
                    key = %self.lease_key,
                    %error,
                    "etcd_lease: takeover txn failed during try_acquire"
                );
                let _ = client.lease_revoke(lease_id).await;
                self.clear_leader();
                false
            }
        }
    }

    async fn etcd_lease_alive(&self, client: &mut Client, lease_id: i64) -> bool {
        match client.lease_time_to_live(lease_id, None).await {
            Ok(resp) => resp.ttl() > 0,
            Err(_) => false,
        }
    }

    async fn etcd_ping_lease(&self, client: &mut Client, lease_id: i64) -> bool {
        match client.lease_keep_alive(lease_id).await {
            Ok((mut keeper, mut stream)) => {
                let ping = keeper.keep_alive().await.is_ok();
                if ping {
                    use futures::StreamExt;
                    let _ = stream.next().await;
                }
                ping
            }
            Err(error) => {
                tracing::warn!(
                    key = %self.lease_key,
                    lease_id,
                    %error,
                    "etcd_lease: keep_alive failed"
                );
                false
            }
        }
    }

    async fn etcd_renew(&self, client: &mut Client) -> bool {
        let lease_id = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .lease_id;
        if lease_id == 0 {
            self.clear_leader();
            return false;
        }

        let key = self.lease_key.as_bytes();
        let holder = match client.get(key, None).await {
            Ok(resp) => resp
                .kvs()
                .first()
                .map(|kv| String::from_utf8_lossy(kv.value()).into_owned()),
            Err(error) => {
                tracing::warn!(
                    key = %self.lease_key,
                    %error,
                    "etcd_lease: GET failed during renew"
                );
                self.clear_leader();
                return false;
            }
        };

        if holder.as_deref() != Some(self.holder_identity.as_str()) {
            tracing::warn!(
                key = %self.lease_key,
                current_holder = ?holder,
                our_identity = %self.holder_identity,
                "etcd_lease: holder mismatch during renew"
            );
            self.clear_leader();
            return false;
        }

        if self.etcd_ping_lease(client, lease_id).await {
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .last_renewed_at = Some(Instant::now());
            true
        } else {
            self.clear_leader();
            false
        }
    }

    async fn etcd_release(&self, client: &mut Client) {
        let lease_id = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .lease_id;
        self.clear_leader();
        if lease_id != 0 {
            if let Err(error) = client.lease_revoke(lease_id).await {
                tracing::warn!(
                    key = %self.lease_key,
                    lease_id,
                    %error,
                    "etcd_lease: lease_revoke failed during release"
                );
            }
        }
    }
}

fn normalize_etcd_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
}

#[async_trait]
impl LeaderElection for EtcdLeaseElection {
    fn is_leader(&self) -> bool {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if s.is_leader {
            let expired = s.last_renewed_at.is_none_or(|t| {
                t.elapsed().as_secs() > self.lease_duration_s.saturating_mul(2)
            });
            if expired {
                s.is_leader = false;
            }
        }
        s.is_leader
    }

    async fn try_acquire(&self) -> bool {
        if let Some(client) = &self.client {
            let mut guard = client.lock().await;
            return self.etcd_try_acquire(&mut guard).await;
        }
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        s.fencing_token = s.fencing_token.saturating_add(1);
        s.is_leader = true;
        s.lease_id = 1;
        s.last_renewed_at = Some(Instant::now());
        true
    }

    async fn renew(&self) -> bool {
        if let Some(client) = &self.client {
            let mut guard = client.lock().await;
            return self.etcd_renew(&mut guard).await;
        }
        let is_leader = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_leader;
        if is_leader {
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .last_renewed_at = Some(Instant::now());
        }
        is_leader
    }

    async fn release(&self) {
        if let Some(client) = &self.client {
            let mut guard = client.lock().await;
            self.etcd_release(&mut guard).await;
            return;
        }
        self.clear_leader();
    }

    fn fencing_token(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .fencing_token
    }
}

impl fmt::Display for EtcdLeaseElection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EtcdLeaseElection(key={}, holder={})",
            self.lease_key, self.holder_identity
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn simulation_acquire_increments_fencing_token() {
        let election = EtcdLeaseElection::new(DEFAULT_CCP_LEADER_KEY, "node-a", 15);
        assert!(!election.is_leader());
        assert_eq!(election.fencing_token(), 0);
        assert!(election.try_acquire().await);
        assert!(election.is_leader());
        assert_eq!(election.fencing_token(), 1);
        assert!(election.renew().await);
        election.release().await;
        assert!(!election.is_leader());
    }

    #[tokio::test]
    async fn simulation_second_acquire_bumps_fence() {
        let election = EtcdLeaseElection::new(DEFAULT_CCP_LEADER_KEY, "node-a", 15);
        assert!(election.try_acquire().await);
        election.release().await;
        assert!(election.try_acquire().await);
        assert_eq!(election.fencing_token(), 2);
    }

    #[test]
    fn normalize_endpoint_adds_http_scheme() {
        assert_eq!(
            normalize_etcd_endpoint("127.0.0.1:2379"),
            "http://127.0.0.1:2379"
        );
        assert_eq!(
            normalize_etcd_endpoint("http://etcd:2379"),
            "http://etcd:2379"
        );
    }
}
