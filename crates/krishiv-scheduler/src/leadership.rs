//! Leader election abstraction.

// ── Leader election abstraction ───────────────────────────────────────────────

/// `SingleNodeElection` is the embedded/single-node implementation.
/// `K8sLeaseElection` in `krishiv-operator` implements this for Kubernetes HA.
/// Bare-metal HA backed by external etcd is selected through the same trait.
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
}

/// No-op leader election that always reports this node as the leader.
#[derive(Debug, Default)]
pub struct SingleNodeElection;

impl LeaderElection for SingleNodeElection {
    fn is_leader(&self) -> bool {
        true
    }
}
