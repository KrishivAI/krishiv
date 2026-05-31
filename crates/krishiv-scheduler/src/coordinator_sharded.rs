//! Inner locks (ExecutorInner, CheckpointInner) are the long-term primary source
//! of truth for executor registry and checkpoint coordinator state. The outer
//! Coordinator maintains a snapshot view for convenience. The dual sync dance
//! is transitional; hot paths should migrate to direct inner access + Notify
//! signaling to eliminate block_on and reduce lock contention.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::checkpoint::CheckpointCoordinator;
use crate::heartbeat::ExecutorRegistry;
use krishiv_proto::{CoordinatorState, ExecutorId};
use tokio::sync::Notify;

/// Executor-facing state guarded by a dedicated `RwLock`.
#[derive(Clone, Debug)]
pub(crate) struct ExecutorInner {
    pub executors: ExecutorRegistry,
    pub state: CoordinatorState,
    pub ticks_since_restart: u64,
    pub recovering: bool,
    /// Notify used to wake waiters when executor or state changes occur.
    /// Enables future removal of periodic block_on-based sync.
    pub notify: Arc<Notify>,
}

/// Checkpoint-facing state guarded by a dedicated `RwLock`.
#[derive(Clone, Debug)]
pub(crate) struct CheckpointInner {
    pub coordinators: HashMap<krishiv_proto::JobId, CheckpointCoordinator>,
    pub notify_sent: HashSet<(krishiv_proto::JobId, ExecutorId, u64)>,
    pub barrier_sent: HashSet<(krishiv_proto::JobId, u64)>,
    /// Notify for checkpoint-related state changes (acks, epoch advances).
    pub notify: Arc<Notify>,
}

impl CheckpointInner {
    pub fn new() -> Self {
        Self {
            coordinators: HashMap::new(),
            notify_sent: HashSet::new(),
            barrier_sent: HashSet::new(),
            notify: Arc::new(Notify::new()),
        }
    }
}

/// The sync helper functions below are transitional. Hot paths should prefer
/// the bypass fast-path methods on SharedCoordinator that operate directly on
/// the inner locks. The long-term goal is for ExecutorInner/CheckpointInner
/// (plus Notify) to be the sole source of truth.

/// Synchronise checkpoint state FROM the inner lock INTO the Coordinator fields.
#[allow(dead_code)]
pub(crate) fn sync_checkpoint_from_inner(
    inner: &CheckpointInner,
    dest_coordinators: &mut HashMap<krishiv_proto::JobId, CheckpointCoordinator>,
    dest_notify: &mut HashSet<(krishiv_proto::JobId, ExecutorId, u64)>,
    dest_barrier: &mut HashSet<(krishiv_proto::JobId, u64)>,
) {
    dest_coordinators.clone_from(&inner.coordinators);
    dest_notify.clone_from(&inner.notify_sent);
    dest_barrier.clone_from(&inner.barrier_sent);
}

/// Synchronise checkpoint state FROM the Coordinator fields INTO the inner lock.
#[allow(dead_code)]
pub(crate) fn sync_checkpoint_to_inner(
    src_coordinators: &HashMap<krishiv_proto::JobId, CheckpointCoordinator>,
    src_notify: &HashSet<(krishiv_proto::JobId, ExecutorId, u64)>,
    src_barrier: &HashSet<(krishiv_proto::JobId, u64)>,
    inner: &mut CheckpointInner,
) {
    inner.coordinators.clone_from(src_coordinators);
    inner.notify_sent.clone_from(src_notify);
    inner.barrier_sent.clone_from(src_barrier);
}
