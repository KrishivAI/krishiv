//! Per-job transactional-sink registry for checkpoint-aligned two-phase commit.
//!
//! Pipelines that write through a [`TransactionalSinkParticipant`] register it
//! here keyed by job id.  The checkpoint lifecycle then drives it:
//!
//! - barrier (`initiate_checkpoint_for_job`): [`TwoPhaseSinkRegistry::pre_commit`]
//!   stages the open buffer under the barrier epoch *before* the checkpoint ack;
//! - `CheckpointCompleteCommand`: [`TwoPhaseSinkRegistry::commit_through`]
//!   makes prepared output at or before the committed epoch visible;
//! - `RestoreFromCheckpointCommand`: [`TwoPhaseSinkRegistry::restore_to`]
//!   commits prepared output covered by the restored checkpoint and aborts
//!   everything after it (recover-and-commit / recover-and-abort).

use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use krishiv_connectors::{ConnectorResult, TransactionalSinkParticipant};

/// Shared handle to one registered participant.
pub type SharedSinkParticipant = Arc<Mutex<dyn TransactionalSinkParticipant>>;

/// Registry of transactional-sink participants keyed by job id.
///
/// Clone is cheap — all clones share the same underlying map.
#[derive(Clone, Default)]
pub struct TwoPhaseSinkRegistry {
    inner: Arc<DashMap<String, Vec<SharedSinkParticipant>>>,
}

impl std::fmt::Debug for TwoPhaseSinkRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwoPhaseSinkRegistry")
            .field("jobs", &self.inner.len())
            .finish()
    }
}

impl TwoPhaseSinkRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a participant for `job_id` and return the shared handle the
    /// pipeline stages batches through.
    pub fn register(
        &self,
        job_id: &str,
        participant: impl TransactionalSinkParticipant + 'static,
    ) -> SharedSinkParticipant {
        let shared: SharedSinkParticipant = Arc::new(Mutex::new(participant));
        self.inner
            .entry(job_id.to_owned())
            .or_default()
            .push(Arc::clone(&shared));
        shared
    }

    /// Get-or-create a participant for `job_id`.
    ///
    /// Pipelines that run in repeated bounded cycles use this so the same
    /// transaction log accumulates across cycles of one job.  The first call
    /// builds the participant via `init`; later calls return the first
    /// registered participant.
    pub fn get_or_register<F, P>(
        &self,
        job_id: &str,
        init: F,
    ) -> ConnectorResult<SharedSinkParticipant>
    where
        F: FnOnce() -> ConnectorResult<P>,
        P: TransactionalSinkParticipant + 'static,
    {
        if let Some(existing) = self.inner.get(job_id)
            && let Some(first) = existing.first()
        {
            return Ok(Arc::clone(first));
        }
        let participant = init()?;
        Ok(self.register(job_id, participant))
    }

    /// Whether any participant is registered for `job_id`.
    pub fn has_job(&self, job_id: &str) -> bool {
        self.inner.get(job_id).is_some_and(|v| !v.is_empty())
    }

    /// Barrier: durably prepare every participant's open buffer under `epoch`.
    ///
    /// Fails fast on the first error — the caller must not ack the checkpoint
    /// when sink staging failed, otherwise a committed checkpoint would lack
    /// its sink output.
    pub fn pre_commit(&self, job_id: &str, epoch: u64) -> ConnectorResult<()> {
        let Some(participants) = self.inner.get(job_id) else {
            return Ok(());
        };
        for participant in participants.iter() {
            participant
                .lock()
                .map_err(|_| krishiv_connectors::ConnectorError::Protocol {
                    message: format!(
                        "transactional sink lock poisoned for job {job_id}; \
                         sink state is unreliable — restart the job"
                    ),
                })?
                .pre_commit(epoch)?;
        }
        Ok(())
    }

    /// Commit every prepared transaction at or before `epoch` across the
    /// job's participants.  Returns the number of committed staged writes.
    pub fn commit_through(&self, job_id: &str, epoch: u64) -> ConnectorResult<usize> {
        let Some(participants) = self.inner.get(job_id) else {
            return Ok(0);
        };
        let mut committed = 0usize;
        for participant in participants.iter() {
            committed += participant
                .lock()
                .map_err(|_| krishiv_connectors::ConnectorError::Protocol {
                    message: format!("transactional sink lock poisoned for job {job_id}"),
                })?
                .commit_through(epoch)?;
        }
        Ok(committed)
    }

    /// Restore to `epoch`: commit prepared transactions covered by the
    /// restored checkpoint, then abort everything after it.
    ///
    /// Returns `(committed, aborted)` staged-write counts.
    pub fn restore_to(&self, job_id: &str, epoch: u64) -> ConnectorResult<(usize, usize)> {
        let Some(participants) = self.inner.get(job_id) else {
            return Ok((0, 0));
        };
        let mut committed = 0usize;
        let mut aborted = 0usize;
        for participant in participants.iter() {
            let mut guard =
                participant
                    .lock()
                    .map_err(|_| krishiv_connectors::ConnectorError::Protocol {
                        message: format!("transactional sink lock poisoned for job {job_id}"),
                    })?;
            committed += guard.commit_through(epoch)?;
            aborted += guard.abort_after(epoch)?;
        }
        Ok((committed, aborted))
    }

    /// Drop every participant registered for `job_id` (job eviction).
    pub fn remove_job(&self, job_id: &str) {
        self.inner.remove(job_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_connectors::{EpochTransactionLog, InMemoryTwoPhaseCommitSink};

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
    }

    #[test]
    fn registry_lifecycle_commit_on_complete() {
        let registry = TwoPhaseSinkRegistry::new();
        let participant = registry
            .get_or_register("job-a", || {
                Ok(EpochTransactionLog::new(InMemoryTwoPhaseCommitSink::new()))
            })
            .unwrap();

        participant.lock().unwrap().stage(&batch()).unwrap();
        registry.pre_commit("job-a", 1).unwrap();
        assert_eq!(participant.lock().unwrap().prepared_epochs(), vec![1]);

        // Complete notification commits the prepared epoch.
        assert_eq!(registry.commit_through("job-a", 1).unwrap(), 1);
        assert!(participant.lock().unwrap().prepared_epochs().is_empty());
    }

    #[test]
    fn registry_restore_commits_covered_and_aborts_after() {
        let registry = TwoPhaseSinkRegistry::new();
        let participant = registry
            .get_or_register("job-b", || {
                Ok(EpochTransactionLog::new(InMemoryTwoPhaseCommitSink::new()))
            })
            .unwrap();

        participant.lock().unwrap().stage(&batch()).unwrap();
        registry.pre_commit("job-b", 1).unwrap();
        participant.lock().unwrap().stage(&batch()).unwrap();
        registry.pre_commit("job-b", 2).unwrap();

        let (committed, aborted) = registry.restore_to("job-b", 1).unwrap();
        assert_eq!((committed, aborted), (1, 1));
    }

    #[test]
    fn registry_get_or_register_reuses_participant_across_cycles() {
        let registry = TwoPhaseSinkRegistry::new();
        let first = registry
            .get_or_register("job-c", || {
                Ok(EpochTransactionLog::new(InMemoryTwoPhaseCommitSink::new()))
            })
            .unwrap();
        first.lock().unwrap().stage(&batch()).unwrap();

        let second = registry
            .get_or_register("job-c", || {
                Ok(EpochTransactionLog::new(InMemoryTwoPhaseCommitSink::new()))
            })
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second), "same participant reused");
        assert_eq!(second.lock().unwrap().open_rows(), 3);
    }

    #[test]
    fn registry_unknown_job_is_noop() {
        let registry = TwoPhaseSinkRegistry::new();
        registry.pre_commit("nope", 1).unwrap();
        assert_eq!(registry.commit_through("nope", 1).unwrap(), 0);
        assert_eq!(registry.restore_to("nope", 1).unwrap(), (0, 0));
    }
}
