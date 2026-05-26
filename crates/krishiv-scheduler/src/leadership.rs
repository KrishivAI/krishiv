//! Leader election abstraction.

// ── Leader election abstraction ───────────────────────────────────────────────

/// `SingleNodeElection` is the embedded/single-node implementation.
/// `K8sLeaseElection` in `krishiv-operator` implements this for Kubernetes HA.
/// Bare-metal HA backed by external etcd is deferred post-R9.
///
/// # ADR-R12-02 (Option B — AFIT)
/// The three mutating methods use `async fn` (AFIT, stable since Rust 1.75).
/// This eliminates the `block_on` anti-pattern in `K8sLeaseElection`, which
/// panics when called from inside an async Tokio runtime context.
///
/// `dyn LeaderElection` is not used anywhere in this codebase, so auto-trait
/// bounds on the returned futures (the lint `async_fn_in_trait` warns about)
/// are not a concern.
#[allow(async_fn_in_trait)]
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
