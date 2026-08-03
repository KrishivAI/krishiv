//! Leader election abstraction.

// ── Leader election abstraction ───────────────────────────────────────────────

/// `SingleNodeElection` is the embedded/single-node implementation.
/// `K8sLeaseElection` in `krishiv-operator` implements this for Kubernetes HA.
/// `EtcdLeaseElection` in this crate (`feature = "etcd"`) implements bare-metal CCP HA.
///
/// The trait is `#[async_trait]` so `Arc<dyn LeaderElection>` works for
/// runtime injection (A1).  The boxed-future overhead is negligible at the
/// rate of one election tick per few seconds.
#[async_trait::async_trait]
pub trait LeaderElection: Send + Sync {
    /// Whether this node currently holds the leader lease.
    fn is_leader(&self) -> bool;

    /// Attempt to acquire the leader lease. Returns `true` if acquired.
    ///
    /// Default: always succeeds (single-node behaviour).
    async fn try_acquire(&self) -> bool {
        self.is_leader()
    }

    /// Renew the current leader lease. Returns `true` if the renewal succeeded.
    ///
    /// A `false` result means another node has taken the lease — this node must
    /// stop acting as leader immediately and reject any pending checkpoint writes.
    ///
    /// Default: returns `is_leader()` (single-node behaviour).
    async fn renew(&self) -> bool {
        self.is_leader()
    }

    /// Release the leader lease voluntarily (graceful shutdown).
    ///
    /// Default: no-op.
    async fn release(&self) {}

    /// How long since this node last successfully renewed its lease, if it
    /// believes it holds one.
    ///
    /// **This is a health signal, not a leadership predicate.** `is_leader()`
    /// deliberately stays a plain flag: safety comes from the fencing token,
    /// which every checkpoint ack validates and which etcd revisions make
    /// monotonic across restarts — not from a clock. Making `is_leader()`
    /// time-aware would buy no safety and would self-demote a healthy leader
    /// through an ordinary GC pause, flapping the cluster.
    ///
    /// What it does catch is the renew loop *not running at all* — task
    /// panicked, starved, or wedged — which this codebase has seen: a
    /// coordinator frozen for minutes while `/healthz` on a dedicated liveness
    /// thread kept answering. Wire this to readiness so the orchestrator
    /// restarts the pod. Same split as Kubernetes' own `leaderelection`:
    /// `IsLeader()` is a flag, and a separate `Check(maxTolerableExpiredLease)`
    /// drives the probe.
    ///
    /// `None` means "not leader, or no renewal recorded" — never a staleness
    /// claim. Default: `None` (single-node has no lease to go stale).
    fn renewal_age(&self) -> Option<std::time::Duration> {
        None
    }

    /// Monotonically increasing fencing token for this lease holder.
    ///
    /// Must be stored in every `CheckpointMetadata` committed by this
    /// coordinator. A checkpoint whose `fencing_token` is less than the current
    /// token must be rejected.
    ///
    /// Default: returns `0` (single-node — no competing coordinators).
    fn fencing_token(&self) -> u64 {
        0
    }

    /// Lease TTL in seconds.
    ///
    /// The leader loop uses this to compute a safe renew interval
    /// (`lease_duration / 3`) so the coordinator renews well before the lease
    /// expires, minimizing the split-brain window where a stale coordinator
    /// remains Active after lease expiry.
    ///
    /// Default: returns `15` (matching the default `leader_lease_duration_s`).
    fn lease_duration_s(&self) -> u64 {
        15
    }
}

/// No-op leader election that always reports this node as the leader.
#[derive(Debug, Default)]
pub struct SingleNodeElection;

impl LeaderElection for SingleNodeElection {
    fn is_leader(&self) -> bool {
        true
    }
}
