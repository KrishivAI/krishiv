use std::fmt;

use krishiv_proto::JobSpec;

use crate::{NamespaceQuotaSnapshot, SubmitOutcome};

/// Admission decision returned by a `QueueManager`.
///
/// Receives the static `JobSpec` and a live `NamespaceQuotaSnapshot` from the
/// coordinator. Implementations compare the spec's resource requests against
/// the snapshot's current reservations and their own configured limits.
pub trait QueueManager: Send + Sync + fmt::Debug {
    /// Return whether `spec` may enter the scheduler immediately.
    ///
    /// `quota` contains the live reservation totals for the job's namespace.
    fn admit(&self, spec: &JobSpec, quota: &NamespaceQuotaSnapshot) -> SubmitOutcome;

    /// Notify the queue manager when a job reaches a terminal state.
    ///
    /// The default is a no-op.
    fn on_job_complete(&self, _job_id: &krishiv_proto::JobId, _usage: &crate::ResourceUsage) {}
}

/// Always-admit queue manager for embedded and test contexts.
///
/// Every job is immediately accepted regardless of quota snapshot values.
#[derive(Debug, Default, Clone)]
pub struct InMemoryQueueManager;

impl QueueManager for InMemoryQueueManager {
    fn admit(&self, _spec: &JobSpec, _quota: &NamespaceQuotaSnapshot) -> SubmitOutcome {
        SubmitOutcome::Accepted
    }
}
