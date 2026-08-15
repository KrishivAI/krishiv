//! In-memory transactional producer for tests and rdkafka wrapper.

use std::collections::BTreeMap;

use arrow::record_batch::RecordBatch;

use crate::ConnectorError;

/// Tracks committed Kafka offsets per topic-partition for exactly-once pipelines.
#[derive(Debug, Clone, Default)]
pub struct TransactionalBatchMetadata {
    pub committed_offsets: BTreeMap<String, i64>,
}

impl TransactionalBatchMetadata {
    pub fn record(&mut self, topic_partition: impl Into<String>, offset: i64) {
        self.committed_offsets
            .insert(topic_partition.into(), offset);
    }
}

/// In-memory transactional session used by tests and embedded pipelines.
///
/// Offsets staged inside an open transaction (via [`Self::stage_offset`]) only
/// become visible in `metadata.committed_offsets` when the transaction is
/// committed; [`Self::abort_transaction`] drops them.
#[derive(Debug, Default)]
pub struct InMemoryTransactionalProducer {
    active: bool,
    in_transaction: bool,
    pending_offsets: BTreeMap<String, i64>,
    pub metadata: TransactionalBatchMetadata,
}

impl InMemoryTransactionalProducer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn init_transactions(&mut self) -> Result<(), ConnectorError> {
        self.active = true;
        Ok(())
    }

    pub fn begin_transaction(&mut self) -> Result<(), ConnectorError> {
        if !self.active {
            return Err(ConnectorError::Kafka {
                message: "call init_transactions first".into(),
                retriable: false,
            });
        }
        self.in_transaction = true;
        self.pending_offsets.clear();
        Ok(())
    }

    /// Stage a per-topic-partition offset inside the open transaction.
    ///
    /// The offset becomes durable only when [`Self::commit_transaction`]
    /// succeeds — the coupling that lets callers commit source offsets
    /// atomically with (i.e. strictly after) a downstream two-phase commit.
    pub fn stage_offset(
        &mut self,
        topic_partition: impl Into<String>,
        offset: i64,
    ) -> Result<(), ConnectorError> {
        if !self.in_transaction {
            return Err(ConnectorError::Kafka {
                message: "call begin_transaction first".into(),
                retriable: false,
            });
        }
        self.pending_offsets.insert(topic_partition.into(), offset);
        Ok(())
    }

    pub fn commit_transaction(
        &mut self,
        offsets: BTreeMap<String, i64>,
    ) -> Result<(), ConnectorError> {
        self.metadata.committed_offsets = offsets;
        self.metadata
            .committed_offsets
            .extend(std::mem::take(&mut self.pending_offsets));
        self.in_transaction = false;
        Ok(())
    }

    /// Abort the open transaction, discarding any staged (uncommitted) offsets.
    pub fn abort_transaction(&mut self) -> Result<(), ConnectorError> {
        self.pending_offsets.clear();
        self.in_transaction = false;
        Ok(())
    }

    /// Convenience that begins, stages one offset, and **auto-commits** the
    /// transaction in a single call. Callers that need offset commits coupled
    /// to an external two-phase commit must use the explicit
    /// `begin_transaction` / `stage_offset` / `commit_transaction` sequence
    /// instead — this method commits immediately.
    pub fn write_batch_with_offsets(
        &mut self,
        batch: &RecordBatch,
        topic_partition: &str,
        offset: i64,
    ) -> Result<BTreeMap<String, i64>, ConnectorError> {
        self.begin_transaction()?;
        let _ = batch;
        let mut map = self.metadata.committed_offsets.clone();
        map.insert(topic_partition.to_string(), offset);
        self.commit_transaction(map.clone())?;
        Ok(map)
    }
}

#[cfg(feature = "kafka")]
pub mod rdkafka_txn {
    use super::ConnectorError;
    use rdkafka::ClientConfig;
    use rdkafka::producer::{FutureProducer, Producer};
    use rdkafka::util::Timeout;

    /// rdkafka transactional producer wrapper.
    pub struct RdkafkaTransactionalProducer {
        producer: FutureProducer,
        transactional_id: String,
    }

    impl RdkafkaTransactionalProducer {
        pub fn new(
            bootstrap_servers: &str,
            transactional_id: impl Into<String>,
        ) -> Result<Self, ConnectorError> {
            let transactional_id = transactional_id.into();
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", bootstrap_servers)
                .set("transactional.id", &transactional_id)
                .set("enable.idempotence", "true")
                .create()
                .map_err(|e| ConnectorError::Kafka {
                    message: e.to_string(),
                    retriable: true,
                })?;
            producer
                .init_transactions(Timeout::After(std::time::Duration::from_secs(30)))
                .map_err(|e| ConnectorError::Kafka {
                    message: e.to_string(),
                    retriable: true,
                })?;
            Ok(Self {
                producer,
                transactional_id,
            })
        }

        pub fn begin(&self) -> Result<(), ConnectorError> {
            self.producer
                .begin_transaction()
                .map_err(|e| ConnectorError::Kafka {
                    message: e.to_string(),
                    retriable: true,
                })
        }

        pub fn commit(&self) -> Result<(), ConnectorError> {
            self.producer
                .commit_transaction(Timeout::After(std::time::Duration::from_secs(30)))
                .map_err(|e| ConnectorError::Kafka {
                    message: e.to_string(),
                    retriable: true,
                })
        }

        pub fn abort(&self) -> Result<(), ConnectorError> {
            self.producer
                .abort_transaction(Timeout::After(std::time::Duration::from_secs(30)))
                .map_err(|e| ConnectorError::Kafka {
                    message: e.to_string(),
                    retriable: true,
                })
        }

        pub fn transactional_id(&self) -> &str {
            &self.transactional_id
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn transactional_in_memory_roundtrip() {
        let mut prod = InMemoryTransactionalProducer::new();
        prod.init_transactions().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap();
        let offsets = prod
            .write_batch_with_offsets(&batch, "orders-0", 99)
            .unwrap();
        assert_eq!(offsets.get("orders-0"), Some(&99));
        assert_eq!(prod.metadata.committed_offsets.get("orders-0"), Some(&99));
    }

    /// Aborting an open transaction must drop its staged offsets: committed
    /// state stays at the last committed transaction, and a later commit does
    /// not resurrect the aborted offsets.
    #[test]
    fn transactional_abort_drops_uncommitted_offsets() {
        let mut prod = InMemoryTransactionalProducer::new();
        prod.init_transactions().unwrap();

        prod.begin_transaction().unwrap();
        prod.stage_offset("orders-0", 10).unwrap();
        prod.commit_transaction(BTreeMap::new()).unwrap();
        assert_eq!(prod.metadata.committed_offsets.get("orders-0"), Some(&10));

        prod.begin_transaction().unwrap();
        prod.stage_offset("orders-0", 20).unwrap();
        prod.abort_transaction().unwrap();
        assert_eq!(
            prod.metadata.committed_offsets.get("orders-0"),
            Some(&10),
            "abort must not advance committed offsets"
        );

        // Staging outside a transaction is rejected.
        assert!(prod.stage_offset("orders-0", 30).is_err());

        // A subsequent commit does not resurrect the aborted offset.
        prod.begin_transaction().unwrap();
        prod.commit_transaction(prod.metadata.committed_offsets.clone())
            .unwrap();
        assert_eq!(prod.metadata.committed_offsets.get("orders-0"), Some(&10));
    }
}
