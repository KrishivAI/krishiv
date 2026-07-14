//! Per-job transactional-sink registry for checkpoint-aligned two-phase commit.
//!
//! Pipelines that write through a [`TransactionalSinkParticipant`] register it
//! here keyed by job id.  Two lifecycles drive it:
//!
//! **Barrier-driven** (long-running stream tasks with a checkpoint coordinator):
//!
//! - barrier (`initiate_checkpoint_for_job`): [`TwoPhaseSinkRegistry::pre_commit`]
//!   stages the open buffer under the barrier epoch *before* the checkpoint ack;
//! - `CheckpointCompleteCommand`: [`TwoPhaseSinkRegistry::commit_through`]
//!   makes prepared output at or before the committed epoch visible;
//! - `RestoreFromCheckpointCommand`: [`TwoPhaseSinkRegistry::restore_to`]
//!   commits prepared output covered by the restored checkpoint and aborts
//!   everything after it (recover-and-commit / recover-and-abort).
//!
//! **Cycle-aligned** (continuous `stream:loop` jobs): each completed cycle is
//! its own epoch, prepared and committed at cycle end via
//! [`TwoPhaseSinkRegistry::commit_cycle`].  Continuous tasks are transient
//! (one task per pushed cycle), so no checkpoint coordinator ever targets
//! them with barriers — and prepared state is process-local, so deferring the
//! commit to a later heartbeat would only widen the crash window without
//! adding durability.  Exactly-once end to end comes from the source-offset
//! protocol: offsets ride in the committed Iceberg snapshot's summary, and a
//! feeder must consult the table's committed offsets before redelivering.

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
    /// Last cycle-aligned epoch handed out per job (see [`Self::commit_cycle`]).
    /// Process-local by design: a fresh process restarts at 1, which is safe
    /// because epoch monotonicity is only enforced against in-memory prepared
    /// state and committed snapshots carry source offsets, not epochs, as
    /// their recovery contract.
    cycle_epochs: Arc<DashMap<String, u64>>,
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

    /// DUR-2 reporting: the prepared-but-unfinalized sink transactions for
    /// `job_id`, to be recorded in the checkpoint ack so the coordinator can
    /// persist them in the checkpoint metadata and drive recovery after a
    /// crash. Empty when no participant is registered or none staged durable
    /// output.
    pub fn prepared_refs(
        &self,
        job_id: &str,
    ) -> ConnectorResult<Vec<krishiv_connectors::PreparedSinkRef>> {
        let Some(participants) = self.inner.get(job_id) else {
            return Ok(Vec::new());
        };
        let mut refs = Vec::new();
        for participant in participants.iter() {
            let guard = participant
                .lock()
                .map_err(|_| krishiv_connectors::ConnectorError::Protocol {
                    message: format!("transactional sink lock poisoned for job {job_id}"),
                })?;
            refs.extend(guard.prepared_refs());
        }
        Ok(refs)
    }

    /// DUR-2 durable recovery: finalize prepared-sink transactions listed in a
    /// `RestoreFromCheckpointCommand` by reconstructing them from their durable
    /// prepare paths, rather than from the in-memory prepared log.
    ///
    /// Runs alongside [`Self::restore_to`]: `restore_to` covers the case where
    /// only the coordinator restarted (the executor's in-memory log is intact);
    /// this covers the executor-crash case where that log was lost, so the
    /// prepared transactions must be re-derived from the durable checkpoint
    /// refs. `finalize_prepared` is idempotent, so running both is safe.
    ///
    /// A no-op (with a warning) when no participant is registered for the job
    /// yet — the task-recreation path registers one and reconciles then.
    /// Returns `(committed, aborted)` counts.
    pub fn recover_prepared_refs(
        &self,
        job_id: &str,
        commit_paths: &[String],
        abort_paths: &[String],
    ) -> ConnectorResult<(usize, usize)> {
        if commit_paths.is_empty() && abort_paths.is_empty() {
            return Ok((0, 0));
        }
        let Some(participants) = self.inner.get(job_id) else {
            tracing::warn!(
                job_id,
                commit = commit_paths.len(),
                abort = abort_paths.len(),
                "DUR-2 recovery refs received but no sink participant is registered yet; \
                 deferring to task re-creation"
            );
            return Ok((0, 0));
        };
        let Some(first) = participants.first() else {
            return Ok((0, 0));
        };
        let mut guard = first
            .lock()
            .map_err(|_| krishiv_connectors::ConnectorError::Protocol {
                message: format!("transactional sink lock poisoned for job {job_id}"),
            })?;
        for path in commit_paths {
            guard.finalize_prepared(path, true)?;
        }
        for path in abort_paths {
            guard.finalize_prepared(path, false)?;
        }
        Ok((commit_paths.len(), abort_paths.len()))
    }

    /// Cycle-aligned commit for continuous `stream:loop` jobs: prepare and
    /// commit everything staged during the cycle that just completed as one
    /// epoch.  Returns the committed epoch, or `None` when the job has no
    /// registered participant.
    ///
    /// An error means the cycle's output did NOT durably commit — the caller
    /// must fail the cycle so the coordinator never persists a snapshot that
    /// claims the cycle happened (the feeder then redelivers).
    pub fn commit_cycle(&self, job_id: &str) -> ConnectorResult<Option<u64>> {
        if !self.has_job(job_id) {
            return Ok(None);
        }
        let epoch = {
            let mut entry = self.cycle_epochs.entry(job_id.to_owned()).or_insert(0);
            *entry += 1;
            *entry
        };
        self.pre_commit(job_id, epoch)?;
        self.commit_through(job_id, epoch)?;
        Ok(Some(epoch))
    }

    /// Drop every participant registered for `job_id` (job eviction).
    pub fn remove_job(&self, job_id: &str) {
        self.inner.remove(job_id);
        self.cycle_epochs.remove(job_id);
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
    fn commit_cycle_commits_staged_output_under_monotonic_epochs() {
        let registry = TwoPhaseSinkRegistry::new();
        let participant = registry
            .get_or_register("job-d", || {
                Ok(EpochTransactionLog::new(InMemoryTwoPhaseCommitSink::new()))
            })
            .unwrap();

        participant.lock().unwrap().stage(&batch()).unwrap();
        assert_eq!(registry.commit_cycle("job-d").unwrap(), Some(1));
        assert!(participant.lock().unwrap().prepared_epochs().is_empty());

        participant.lock().unwrap().stage(&batch()).unwrap();
        assert_eq!(registry.commit_cycle("job-d").unwrap(), Some(2));

        // No participant registered: explicit None, not an error.
        assert_eq!(registry.commit_cycle("job-none").unwrap(), None);

        // Eviction resets the cycle-epoch counter with the participants.
        registry.remove_job("job-d");
        assert!(!registry.has_job("job-d"));
    }

    #[test]
    fn registry_unknown_job_is_noop() {
        let registry = TwoPhaseSinkRegistry::new();
        registry.pre_commit("nope", 1).unwrap();
        assert_eq!(registry.commit_through("nope", 1).unwrap(), 0);
        assert_eq!(registry.restore_to("nope", 1).unwrap(), (0, 0));
    }

    #[test]
    fn recover_prepared_refs_noop_on_empty_and_unknown() {
        let registry = TwoPhaseSinkRegistry::new();
        // Empty ref lists: nothing to do, no participant lookup needed.
        assert_eq!(
            registry.recover_prepared_refs("job-x", &[], &[]).unwrap(),
            (0, 0)
        );
        // Non-empty refs but no participant registered yet: warn + defer, but
        // must not error (task re-creation reconciles).
        assert_eq!(
            registry
                .recover_prepared_refs("job-x", &[String::from("/tmp/a.parquet.tmp")], &[])
                .unwrap(),
            (0, 0)
        );
    }

    #[test]
    fn recover_prepared_refs_drives_durable_finalize_after_crash() {
        use krishiv_connectors::LocalParquetTwoPhaseCommitSink;

        let dir = tempfile::tempdir().unwrap();

        // Pre-crash executor: stage two epochs through a durable parquet sink.
        // pre_commit writes `.tmp` staging files that survive a crash.
        let pre_crash = TwoPhaseSinkRegistry::new();
        let participant = pre_crash
            .get_or_register("job-recover", || {
                Ok(EpochTransactionLog::new(LocalParquetTwoPhaseCommitSink::new(
                    dir.path(),
                )))
            })
            .unwrap();
        participant.lock().unwrap().stage(&batch()).unwrap();
        pre_crash.pre_commit("job-recover", 1).unwrap();
        participant.lock().unwrap().stage(&batch()).unwrap();
        pre_crash.pre_commit("job-recover", 2).unwrap();

        // The durable prepare paths the coordinator would carry in the
        // checkpoint refs: the two `.tmp` staging files now on disk.
        let mut tmp_paths: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| {
                let p = e.unwrap().path();
                (p.extension().and_then(|x| x.to_str()) == Some("tmp"))
                    .then(|| p.to_string_lossy().into_owned())
            })
            .collect();
        tmp_paths.sort();
        assert_eq!(tmp_paths.len(), 2, "two staged .tmp files exist pre-crash");

        // Crash: the in-memory prepared log is gone. A fresh executor registers
        // a brand-new participant on the same durable directory — its in-memory
        // log is empty, so `restore_to` alone would silently lose the staged
        // output. The DUR-2 durable drive must finalize from the refs instead.
        let post_crash = TwoPhaseSinkRegistry::new();
        let _fresh = post_crash
            .get_or_register("job-recover", || {
                Ok(EpochTransactionLog::new(LocalParquetTwoPhaseCommitSink::new(
                    dir.path(),
                )))
            })
            .unwrap();
        // restore_to on the fresh (empty) log is a genuine no-op here.
        assert_eq!(post_crash.restore_to("job-recover", 1).unwrap(), (0, 0));

        // Restored epoch = 1: commit epoch-1's transaction, abort epoch-2's.
        let (committed, aborted) = post_crash
            .recover_prepared_refs(
                "job-recover",
                std::slice::from_ref(&tmp_paths[0]),
                std::slice::from_ref(&tmp_paths[1]),
            )
            .unwrap();
        assert_eq!((committed, aborted), (1, 1));

        // The committed epoch's `.tmp` became a final `.parquet`; the aborted
        // one was removed. No `.tmp` files should remain.
        let remaining: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            remaining.iter().any(|n| n.ends_with(".parquet")),
            "committed transaction produced a final parquet file: {remaining:?}"
        );
        assert!(
            !remaining.iter().any(|n| n.ends_with(".tmp")),
            "no staging files remain after recovery: {remaining:?}"
        );

        // Idempotent: re-running recovery must not error or double-commit.
        let (c2, a2) = post_crash
            .recover_prepared_refs(
                "job-recover",
                std::slice::from_ref(&tmp_paths[0]),
                std::slice::from_ref(&tmp_paths[1]),
            )
            .unwrap();
        assert_eq!((c2, a2), (1, 1), "recovery is idempotent");
    }
}
