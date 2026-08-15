//! In-memory transactional Kafka sink for certification tests.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;

use crate::{ConnectorCapabilities, ConnectorError, ConnectorResult, TwoPhaseCommitSink};

/// Deterministic Kafka transaction id: `{job_id}/{partition_id}/{epoch}`.
pub fn transaction_id(job_id: &str, partition_id: u32, epoch: u64) -> String {
    format!("{job_id}/{partition_id}/{epoch}")
}

/// Handle for a staged Kafka transaction batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KafkaTxnHandle {
    id: u64,
}

/// Transactional sink backed by in-memory topic simulation for certification.
///
/// **Not broker-backed.** Do not use in `single-node-durable` or
/// `distributed-durable` profiles; call [`Self::new_for_profile`] which rejects
/// durable profiles.
#[derive(Debug)]
pub struct TransactionalKafkaSink {
    job_id: String,
    partition_id: u32,
    epoch: u64,
    next_handle: u64,
    staged: BTreeMap<u64, RecordBatch>,
    committed: Vec<RecordBatch>,
    fenced_epochs: Vec<u64>,
    /// Set by [`Self::fence_zombie`]; a fenced sink rejects all further
    /// `prepare`/`commit` calls, matching real Kafka producer fencing.
    fenced: bool,
}

impl TransactionalKafkaSink {
    pub fn new(job_id: impl Into<String>, partition_id: u32, epoch: u64) -> Self {
        Self {
            job_id: job_id.into(),
            partition_id,
            epoch,
            next_handle: 0,
            staged: BTreeMap::new(),
            committed: Vec::new(),
            fenced_epochs: Vec::new(),
            fenced: false,
        }
    }

    /// Create a sink only when simulation is permitted for the durability profile.
    pub fn new_for_profile(
        profile: krishiv_common::DurabilityProfile,
        job_id: impl Into<String>,
        partition_id: u32,
        epoch: u64,
    ) -> ConnectorResult<Self> {
        if krishiv_common::forbids_simulation_connectors(profile) {
            return Err(ConnectorError::Config {
                message: "TransactionalKafkaSink is an in-memory simulator and cannot be used in \
                          durable profiles; wire a broker-backed rdkafka transactional producer"
                    .into(),
            });
        }
        Ok(Self::new(job_id, partition_id, epoch))
    }

    pub fn txn_id(&self) -> String {
        transaction_id(&self.job_id, self.partition_id, self.epoch)
    }

    pub fn fence_zombie(&mut self, previous_epoch: u64) {
        self.fenced_epochs.push(previous_epoch);
        self.staged.clear();
        self.fenced = true;
    }

    fn fenced_error(&self, op: &str) -> ConnectorError {
        ConnectorError::Kafka {
            message: format!(
                "producer fenced: {op} rejected for zombie transaction {}",
                self.txn_id()
            ),
            retriable: false,
        }
    }

    pub fn committed_batches(&self) -> &[RecordBatch] {
        &self.committed
    }

    /// Kafka source config for exactly-once: `isolation.level=read_committed`.
    pub fn source_config_read_committed() -> Vec<(&'static str, &'static str)> {
        vec![("isolation.level", "read_committed")]
    }
}

impl TwoPhaseCommitSink for TransactionalKafkaSink {
    type Handle = KafkaTxnHandle;

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new().with_two_phase_commit()
    }

    fn prepare(&mut self, epoch: u64, batch: &RecordBatch) -> ConnectorResult<Self::Handle> {
        if self.fenced {
            return Err(self.fenced_error("prepare"));
        }
        if epoch != self.epoch {
            return Err(ConnectorError::Kafka {
                message: format!("epoch mismatch: expected {}", self.epoch),
                retriable: false,
            });
        }
        let id = self.next_handle;
        self.next_handle += 1;
        self.staged.insert(id, batch.clone());
        Ok(KafkaTxnHandle { id })
    }

    fn commit(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        if self.fenced {
            return Err(self.fenced_error("commit"));
        }
        match self.staged.remove(&handle.id) {
            Some(batch) => {
                self.committed.push(batch);
                Ok(())
            }
            // An unknown or cleared handle has nothing staged to commit;
            // returning Ok would silently drop the batch.
            None => Err(ConnectorError::Kafka {
                message: format!(
                    "commit of unknown or cleared transaction handle {} in {}",
                    handle.id,
                    self.txn_id()
                ),
                retriable: false,
            }),
        }
    }

    fn abort(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        self.staged.remove(&handle.id);
        Ok(())
    }
}

/// Shared registry for fencing zombie transactions on coordinator recovery.
#[derive(Default, Clone)]
pub struct TransactionalKafkaRegistry {
    inner: Arc<Mutex<BTreeMap<String, TransactionalKafkaSink>>>,
}

impl TransactionalKafkaRegistry {
    pub fn register(&self, sink: TransactionalKafkaSink) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(sink.txn_id(), sink);
    }

    pub fn fence_previous_epoch(&self, job_id: &str, partition_id: u32, previous_epoch: u64) {
        let id = transaction_id(job_id, partition_id, previous_epoch);
        if let Some(sink) = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&id)
        {
            sink.fence_zombie(previous_epoch);
        }
    }

    /// Run `f` against a registered sink, if present. Lets tests exercise a
    /// registered sink (e.g. verify a fenced zombie rejects operations).
    pub fn with_sink_mut<R>(
        &self,
        txn_id: &str,
        f: impl FnOnce(&mut TransactionalKafkaSink) -> R,
    ) -> Option<R> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(txn_id)
            .map(f)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn single_binary_batch(payload: &[u8]) -> RecordBatch {
        use arrow::array::BinaryArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as ArrowArc;

        let schema = ArrowArc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Binary,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![ArrowArc::new(BinaryArray::from(vec![payload])) as _],
        )
        .expect("batch")
    }

    #[test]
    fn kafka_exactly_once_prepare_commit() {
        let mut sink = TransactionalKafkaSink::new("job", 0, 1);
        let batch = single_binary_batch(b"v");
        let h = sink.prepare(1, &batch).unwrap();
        sink.commit(h).unwrap();
        assert_eq!(sink.committed_batches().len(), 1);
    }

    #[test]
    fn kafka_recovery_fences_zombie_epoch() {
        let reg = TransactionalKafkaRegistry::default();
        let mut old = TransactionalKafkaSink::new("job", 0, 1);
        let h = old.prepare(1, &single_binary_batch(b"v")).unwrap();
        let old_id = old.txn_id();
        reg.register(old);
        reg.fence_previous_epoch("job", 0, 1);
        let mut new = TransactionalKafkaSink::new("job", 0, 2);
        let h2 = new.prepare(2, &single_binary_batch(b"v2")).unwrap();
        new.commit(h2).unwrap();
        assert_eq!(new.committed_batches().len(), 1);
        // The fenced zombie must reject further operations: a post-fence
        // prepare, and a commit of its pre-fence (cleared) handle.
        reg.with_sink_mut(&old_id, |zombie| {
            assert!(
                zombie.prepare(1, &single_binary_batch(b"z")).is_err(),
                "a fenced zombie must not stage new batches"
            );
            assert!(
                zombie.commit(h).is_err(),
                "committing a fenced handle must fail, not silently succeed"
            );
            assert!(zombie.committed_batches().is_empty());
        })
        .expect("zombie sink is registered");
    }

    /// Even without fencing, committing a handle that was never staged (or
    /// was already cleared) must error rather than silently succeed.
    #[test]
    fn kafka_commit_of_unknown_handle_errors() {
        let mut sink = TransactionalKafkaSink::new("job", 0, 1);
        let h = sink.prepare(1, &single_binary_batch(b"v")).unwrap();
        sink.abort(h).unwrap();
        assert!(
            sink.commit(h).is_err(),
            "commit after abort has nothing staged and must error"
        );
        assert!(sink.committed_batches().is_empty());
    }
}
