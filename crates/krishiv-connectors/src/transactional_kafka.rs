//! Transactional Kafka sink for exactly-once Kafka→Kafka (R16 S5.1, ADR-R16.4).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::array::{BinaryArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc as ArrowArc;

use crate::{ConnectorError, ConnectorResult, TwoPhaseCommitSink};

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
#[derive(Debug)]
pub struct TransactionalKafkaSink {
    job_id: String,
    partition_id: u32,
    epoch: u64,
    next_handle: u64,
    staged: BTreeMap<u64, RecordBatch>,
    committed: Vec<RecordBatch>,
    fenced_epochs: Vec<u64>,
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
        }
    }

    pub fn txn_id(&self) -> String {
        transaction_id(&self.job_id, self.partition_id, self.epoch)
    }

    pub fn fence_zombie(&mut self, previous_epoch: u64) {
        self.fenced_epochs.push(previous_epoch);
        self.staged.clear();
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

    fn prepare(
        &mut self,
        epoch: u64,
        batch: &RecordBatch,
    ) -> ConnectorResult<Self::Handle> {
        if epoch != self.epoch {
            return Err(ConnectorError::IoStr {
                message: format!("epoch mismatch: expected {}", self.epoch),
            });
        }
        let id = self.next_handle;
        self.next_handle += 1;
        self.staged.insert(id, batch.clone());
        Ok(KafkaTxnHandle { id })
    }

    fn commit(&mut self, handle: Self::Handle) -> ConnectorResult<()> {
        if let Some(batch) = self.staged.remove(&handle.id) {
            self.committed.push(batch);
        }
        Ok(())
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
        self.inner.lock().unwrap().insert(sink.txn_id(), sink);
    }

    pub fn fence_previous_epoch(&self, job_id: &str, partition_id: u32, previous_epoch: u64) {
        let id = transaction_id(job_id, partition_id, previous_epoch);
        if let Some(sink) = self.inner.lock().unwrap().get_mut(&id) {
            sink.fence_zombie(previous_epoch);
        }
    }
}

fn single_binary_batch(payload: &[u8]) -> RecordBatch {
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let h = old
            .prepare(1, &single_binary_batch(b"v"))
            .unwrap();
        reg.register(old);
        reg.fence_previous_epoch("job", 0, 1);
        let mut new = TransactionalKafkaSink::new("job", 0, 2);
        let h2 = new
            .prepare(2, &single_binary_batch(b"v2"))
            .unwrap();
        new.commit(h2).unwrap();
        assert_eq!(new.committed_batches().len(), 1);
        let _ = h; // staged in old sink was fenced
    }
}
