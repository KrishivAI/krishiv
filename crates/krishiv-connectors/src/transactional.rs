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
#[derive(Debug, Default)]
pub struct InMemoryTransactionalProducer {
    active: bool,
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
        Ok(())
    }

    pub fn commit_transaction(
        &mut self,
        offsets: BTreeMap<String, i64>,
    ) -> Result<(), ConnectorError> {
        self.metadata.committed_offsets = offsets;
        Ok(())
    }

    pub fn abort_transaction(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

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
    use super::*;
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
    }
}
