//! Kafka source and sink stubs.
//!
//! These stubs define the connector contracts and capability flags for
//! Kafka-backed data pipelines.  The actual Kafka broker connection is gated
//! behind a `kafka-runtime` feature that does not exist yet; all data methods
//! return [`ConnectorError::Unsupported`] until that feature is enabled.

use std::any::Any;

use arrow::record_batch::RecordBatch;

use crate::{
    ConnectorCapabilities, ConnectorConfig, ConnectorError, ConnectorResult, OffsetCommitter, Sink,
    Source,
};

// ---------------------------------------------------------------------------
// KafkaOffset
// ---------------------------------------------------------------------------

/// A Kafka topic-partition offset: the durable cursor into a Kafka partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaOffset {
    /// The topic name.
    pub topic: String,
    /// The partition index.
    pub partition: i32,
    /// The offset within the partition.
    pub offset: i64,
}

impl crate::Offset for KafkaOffset {
    /// Encode as: `topic_len:u32 LE | topic_bytes | partition:i32 LE | offset:i64 LE`.
    fn encode(&self) -> Vec<u8> {
        let topic_bytes = self.topic.as_bytes();
        let topic_len = topic_bytes.len() as u32;
        let mut out = Vec::with_capacity(4 + topic_bytes.len() + 4 + 8);
        out.extend_from_slice(&topic_len.to_le_bytes());
        out.extend_from_slice(topic_bytes);
        out.extend_from_slice(&self.partition.to_le_bytes());
        out.extend_from_slice(&self.offset.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> ConnectorResult<Self> {
        if bytes.len() < 4 {
            return Err(ConnectorError::Io {
                message: "KafkaOffset decode: buffer too short for topic_len".into(),
            });
        }
        let topic_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let topic_end = 4 + topic_len;
        if bytes.len() < topic_end + 4 + 8 {
            return Err(ConnectorError::Io {
                message: "KafkaOffset decode: buffer too short for topic + partition + offset"
                    .into(),
            });
        }
        let topic = std::str::from_utf8(&bytes[4..topic_end])
            .map_err(|e| ConnectorError::Io {
                message: format!("KafkaOffset decode: invalid UTF-8 in topic: {e}"),
            })?
            .to_string();
        let partition = i32::from_le_bytes(bytes[topic_end..topic_end + 4].try_into().unwrap());
        let offset_start = topic_end + 4;
        let offset = i64::from_le_bytes(bytes[offset_start..offset_start + 8].try_into().unwrap());
        Ok(KafkaOffset {
            topic,
            partition,
            offset,
        })
    }
}

// ---------------------------------------------------------------------------
// KafkaConfig
// ---------------------------------------------------------------------------

/// Validated Kafka connector configuration.
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    /// Comma-separated list of `host:port` bootstrap server addresses.
    pub bootstrap_servers: String,
    /// The Kafka topic to read from or write to.
    pub topic: String,
    /// The consumer group id used for offset management.
    pub group_id: String,
}

impl KafkaConfig {
    /// Validate and extract a `KafkaConfig` from a [`ConnectorConfig`].
    ///
    /// Required properties: `bootstrap.servers`, `topic`.
    /// Optional: `group.id` (defaults to `"krishiv-default"`).
    pub fn from_config(config: &ConnectorConfig) -> ConnectorResult<Self> {
        Ok(Self {
            bootstrap_servers: config.required("bootstrap.servers")?.to_string(),
            topic: config.required("topic")?.to_string(),
            group_id: config
                .get("group.id")
                .unwrap_or("krishiv-default")
                .to_string(),
        })
    }
}

// ---------------------------------------------------------------------------
// KafkaSource
// ---------------------------------------------------------------------------

/// A Kafka source stub.
///
/// Capabilities: unbounded + rewindable (consumer group offset allows seek to
/// any committed offset).
///
/// All data methods return [`ConnectorError::Unsupported`] until the
/// `kafka-runtime` feature is enabled and a real broker connection is wired.
pub struct KafkaSource {
    config: KafkaConfig,
}

impl KafkaSource {
    /// Create a new `KafkaSource` from a validated config.
    pub fn new(config: KafkaConfig) -> Self {
        Self { config }
    }

    /// Return the config this source was created with.
    pub fn config(&self) -> &KafkaConfig {
        &self.config
    }
}

impl Source for KafkaSource {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_unbounded()
            .with_rewindable()
    }

    async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        Err(ConnectorError::Unsupported {
            message: "Kafka broker connection not available in stub; enable kafka-runtime feature"
                .into(),
        })
    }

    fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
        None
    }
}

// ---------------------------------------------------------------------------
// KafkaSink
// ---------------------------------------------------------------------------

/// A Kafka sink stub.
///
/// Capabilities: unbounded + transactional (Kafka supports transactional
/// producers for exactly-once delivery when combined with idempotent
/// producers and consumer group offset management).
///
/// Post-write offset commit protocol: the Kafka consumer group offset is
/// committed only after the corresponding output batch has been durably written
/// to the downstream sink.  If the executor crashes between the output write and
/// the offset commit, the source will reprocess that batch on reassignment,
/// providing at-least-once delivery.
///
/// All data methods return [`ConnectorError::Unsupported`] until the
/// `kafka-runtime` feature is enabled.
pub struct KafkaSink {
    config: KafkaConfig,
}

impl KafkaSink {
    /// Create a new `KafkaSink` from a validated config.
    pub fn new(config: KafkaConfig) -> Self {
        Self { config }
    }

    /// Return the config this sink was created with.
    pub fn config(&self) -> &KafkaConfig {
        &self.config
    }
}

impl Sink for KafkaSink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_unbounded()
            .with_transactional()
    }

    async fn write_batch(&mut self, _batch: RecordBatch) -> ConnectorResult<()> {
        Err(ConnectorError::Unsupported {
            message: "Kafka broker connection not available in stub; enable kafka-runtime feature"
                .into(),
        })
    }

    async fn flush(&mut self) -> ConnectorResult<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// In-memory Kafka test harness
// ---------------------------------------------------------------------------

/// Deterministic in-memory Kafka-like source used by executor and connector
/// certification tests until the real broker-backed runtime is added.
///
/// The source emits pre-built Arrow batches in order. `current_offset()` returns
/// the next offset to commit after the most recent successful read, mirroring
/// Kafka's convention that committed offsets point to the next message to read.
pub struct InMemoryKafkaSource {
    topic: String,
    partition: i32,
    next_offset: i64,
    batches: Vec<RecordBatch>,
    cursor: usize,
}

impl InMemoryKafkaSource {
    /// Create a deterministic source from Arrow batches and a starting offset.
    pub fn new(
        topic: impl Into<String>,
        partition: i32,
        start_offset: i64,
        batches: Vec<RecordBatch>,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            next_offset: start_offset,
            batches,
            cursor: 0,
        }
    }

    /// Return the next Kafka-style offset to commit/read from.
    pub fn next_offset(&self) -> KafkaOffset {
        KafkaOffset {
            topic: self.topic.clone(),
            partition: self.partition,
            offset: self.next_offset,
        }
    }
}

impl Source for InMemoryKafkaSource {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_bounded()
            .with_rewindable()
    }

    async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        if self.cursor >= self.batches.len() {
            return Ok(None);
        }
        let batch = self.batches[self.cursor].clone();
        self.cursor += 1;
        self.next_offset += batch.num_rows() as i64;
        Ok(Some(batch))
    }

    fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
        Some(Box::new(self.next_offset()))
    }
}

/// In-memory commit log for Kafka offsets.
#[derive(Debug, Default, Clone)]
pub struct InMemoryKafkaOffsetCommitter {
    committed: Vec<KafkaOffset>,
}

impl InMemoryKafkaOffsetCommitter {
    /// Create an empty commit log.
    pub fn new() -> Self {
        Self::default()
    }

    /// All offsets committed so far, in commit order.
    pub fn committed_offsets(&self) -> &[KafkaOffset] {
        &self.committed
    }

    /// Last committed offset, if any.
    pub fn last_committed_offset(&self) -> Option<&KafkaOffset> {
        self.committed.last()
    }
}

impl OffsetCommitter<KafkaOffset> for InMemoryKafkaOffsetCommitter {
    async fn commit_offset(&mut self, offset: KafkaOffset) -> ConnectorResult<()> {
        self.committed.push(offset);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectorConfig, Offset};

    // -----------------------------------------------------------------------
    // KafkaOffset
    // -----------------------------------------------------------------------

    #[test]
    fn kafka_offset_encode_decode_roundtrip() {
        let original = KafkaOffset {
            topic: "events".to_string(),
            partition: 3,
            offset: 1_234_567,
        };
        let encoded = original.encode();
        let decoded = KafkaOffset::decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    // -----------------------------------------------------------------------
    // KafkaConfig validation
    // -----------------------------------------------------------------------

    #[test]
    fn kafka_config_requires_bootstrap_servers() {
        let config = ConnectorConfig::new("my-kafka", "kafka").with_property("topic", "events");
        let err = KafkaConfig::from_config(&config).unwrap_err();
        match err {
            ConnectorError::Config { message } => {
                assert!(
                    message.contains("bootstrap.servers"),
                    "expected 'bootstrap.servers' in: {message}"
                );
            }
            other => panic!("expected Config error, got: {other}"),
        }
    }

    #[test]
    fn kafka_config_requires_topic() {
        let config = ConnectorConfig::new("my-kafka", "kafka")
            .with_property("bootstrap.servers", "localhost:9092");
        let err = KafkaConfig::from_config(&config).unwrap_err();
        match err {
            ConnectorError::Config { message } => {
                assert!(message.contains("topic"), "expected 'topic' in: {message}");
            }
            other => panic!("expected Config error, got: {other}"),
        }
    }

    // -----------------------------------------------------------------------
    // KafkaSource capabilities
    // -----------------------------------------------------------------------

    #[test]
    fn kafka_source_reports_unbounded_and_rewindable() {
        let config = KafkaConfig {
            bootstrap_servers: "localhost:9092".into(),
            topic: "events".into(),
            group_id: "test-group".into(),
        };
        let source = KafkaSource::new(config);
        let caps = source.capabilities();
        assert!(caps.is_unbounded());
        assert!(caps.is_rewindable());
        assert!(!caps.is_bounded());
        assert!(!caps.is_transactional());
        assert!(!caps.is_idempotent());
    }

    // -----------------------------------------------------------------------
    // KafkaSink capabilities
    // -----------------------------------------------------------------------

    #[test]
    fn kafka_sink_reports_unbounded_and_transactional() {
        let config = KafkaConfig {
            bootstrap_servers: "localhost:9092".into(),
            topic: "events".into(),
            group_id: "test-group".into(),
        };
        let sink = KafkaSink::new(config);
        let caps = sink.capabilities();
        assert!(caps.is_unbounded());
        assert!(caps.is_transactional());
        assert!(!caps.is_bounded());
        assert!(!caps.is_rewindable());
        assert!(!caps.is_idempotent());
    }

    // -----------------------------------------------------------------------
    // Unsupported stubs
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn kafka_source_read_batch_returns_unsupported() {
        let config = KafkaConfig {
            bootstrap_servers: "localhost:9092".into(),
            topic: "events".into(),
            group_id: "test-group".into(),
        };
        let mut source = KafkaSource::new(config);
        let err = source.read_batch().await.unwrap_err();
        match err {
            ConnectorError::Unsupported { .. } => {}
            other => panic!("expected Unsupported, got: {other}"),
        }
    }

    #[tokio::test]
    async fn kafka_sink_write_batch_returns_unsupported() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let config = KafkaConfig {
            bootstrap_servers: "localhost:9092".into(),
            topic: "events".into(),
            group_id: "test-group".into(),
        };
        let mut sink = KafkaSink::new(config);

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();

        let err = sink.write_batch(batch).await.unwrap_err();
        match err {
            ConnectorError::Unsupported { .. } => {}
            other => panic!("expected Unsupported, got: {other}"),
        }
    }

    #[tokio::test]
    async fn in_memory_kafka_source_advances_current_offset_after_read() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        let mut source = InMemoryKafkaSource::new("events", 2, 10, vec![batch]);

        let read = source.read_batch().await.unwrap().unwrap();
        assert_eq!(read.num_rows(), 3);
        let offset = source
            .current_offset()
            .unwrap()
            .downcast::<KafkaOffset>()
            .unwrap();
        assert_eq!(
            *offset,
            KafkaOffset {
                topic: "events".into(),
                partition: 2,
                offset: 13,
            }
        );
        assert!(source.read_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn in_memory_kafka_offset_committer_records_commits_in_order() {
        use crate::OffsetCommitter;

        let mut committer = InMemoryKafkaOffsetCommitter::new();
        committer
            .commit_offset(KafkaOffset {
                topic: "events".into(),
                partition: 0,
                offset: 1,
            })
            .await
            .unwrap();
        committer
            .commit_offset(KafkaOffset {
                topic: "events".into(),
                partition: 0,
                offset: 4,
            })
            .await
            .unwrap();

        assert_eq!(committer.committed_offsets().len(), 2);
        assert_eq!(committer.last_committed_offset().unwrap().offset, 4);
    }
}
