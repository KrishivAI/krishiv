//! Apache Pulsar streaming source that consumes messages from a Pulsar topic.
//! # Arrow schema
//!
//! | Column            | Arrow type  | Notes                             |
//! |-------------------|-------------|-----------------------------------|
//! | `topic`           | `Utf8`      | Origin topic                      |
//! | `partition_key`   | `Utf8`      | nullable — absent for many topics |
//! | `publish_time_ms` | `Int64`     | epoch ms; 0 if metadata missing   |
//! | `data`            | `Binary`    | Raw message payload bytes         |
//!
//! # Usage
//!
//! ```no_run
//! # #[cfg(feature = "pulsar")]
//! # async fn example() -> anyhow::Result<()> {
//! use krishiv_connectors::pulsar_connector::{PulsarConfig, PulsarSource};
//! use futures::StreamExt;
//!
//! let cfg = PulsarConfig::new("pulsar://localhost:6650", "persistent://public/default/events");
//! let mut src = PulsarSource::connect(cfg).await?;
//! // Collect up to 100 messages into one batch.
//! if let Some(batch) = src.next_batch(100).await? {
//!     println!("{} rows", batch.num_rows());
//! }
//! # Ok(())
//! # }
//! ```

use std::any::Any;
use std::sync::Arc;

use arrow::array::{BinaryBuilder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use pulsar::{Consumer, DeserializeMessage, Payload, Pulsar, SubType, TokioExecutor};

use crate::capabilities::ConnectorCapabilities;
use crate::error::{ConnectorError, ConnectorResult};
use crate::source::Source;

// ── Schema ────────────────────────────────────────────────────────────────────

/// Fixed Arrow schema for Pulsar messages.
pub fn pulsar_arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("topic", DataType::Utf8, false),
        Field::new("partition_key", DataType::Utf8, true),
        Field::new("publish_time_ms", DataType::Int64, false),
        Field::new("data", DataType::Binary, false),
    ]))
}

// ── Raw-bytes deserialization shim ────────────────────────────────────────────

/// A simple marker type for receiving raw bytes from Pulsar without
/// deserializing them.
pub struct RawBytes(pub Vec<u8>);

impl DeserializeMessage for RawBytes {
    type Output = Result<RawBytes, pulsar::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        Ok(RawBytes(payload.data.clone()))
    }
}

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for a single-topic Pulsar consumer.
#[derive(Debug, Clone)]
pub struct PulsarConfig {
    /// Pulsar broker URL (e.g. `"pulsar://localhost:6650"`).
    pub broker_url: String,
    /// Topic to subscribe to.
    pub topic: String,
    /// Consumer subscription name.
    pub subscription: String,
    /// Subscription type (default: `Exclusive`).
    pub sub_type: SubType,
    /// Maximum messages per `read_batch` call (default: 500).
    pub batch_size: usize,
}

impl PulsarConfig {
    /// Create a config with sensible defaults.
    pub fn new(broker_url: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            broker_url: broker_url.into(),
            topic: topic.into(),
            subscription: "krishiv-default".into(),
            sub_type: SubType::Exclusive,
            batch_size: 500,
        }
    }

    pub fn with_subscription(mut self, s: impl Into<String>) -> Self {
        self.subscription = s.into();
        self
    }

    pub fn with_sub_type(mut self, st: SubType) -> Self {
        self.sub_type = st;
        self
    }

    pub fn with_batch_size(mut self, n: usize) -> Self {
        self.batch_size = n.max(1);
        self
    }
}

// ── Source ────────────────────────────────────────────────────────────────────

/// Reads messages from a Pulsar topic as Arrow [`RecordBatch`] values.
///
/// Messages are consumed lazily. Each call to
/// [`next_batch`][PulsarSource::next_batch] polls the consumer for up to
/// `max_messages` messages and converts them to a batch.
///
/// Returns `Ok(None)` if no messages are available within the first poll.
pub struct PulsarSource {
    consumer: Consumer<RawBytes, TokioExecutor>,
    schema: SchemaRef,
    batch_size: usize,
}

impl PulsarSource {
    /// Connect to Pulsar and subscribe to the configured topic.
    pub async fn connect(config: PulsarConfig) -> ConnectorResult<Self> {
        let pulsar_client = Pulsar::builder(&config.broker_url, TokioExecutor)
            .build()
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

        let consumer: Consumer<RawBytes, TokioExecutor> = pulsar_client
            .consumer()
            .with_topic(&config.topic)
            .with_subscription(&config.subscription)
            .with_subscription_type(config.sub_type)
            .build()
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

        Ok(Self {
            consumer,
            schema: pulsar_arrow_schema(),
            batch_size: config.batch_size,
        })
    }

    /// Arrow schema for Pulsar message batches.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Poll for up to `max_messages` Pulsar messages and convert them to a
    /// single [`RecordBatch`].
    ///
    /// Returns `Ok(None)` when no message is ready immediately.
    pub async fn next_batch(
        &mut self,
        max_messages: usize,
    ) -> ConnectorResult<Option<RecordBatch>> {
        let mut topic_col = StringBuilder::new();
        let mut key_col = StringBuilder::new();
        let mut ts_col = Int64Builder::new();
        let mut data_col = BinaryBuilder::new();
        let mut count = 0usize;

        while count < max_messages {
            match self.consumer.try_next().await {
                Ok(Some(msg)) => {
                    topic_col.append_value(&msg.topic);
                    match &msg.payload.metadata.partition_key {
                        Some(k) => key_col.append_value(k),
                        None => key_col.append_null(),
                    }
                    ts_col.append_value(msg.payload.metadata.publish_time as i64);
                    data_col.append_value(&msg.payload.data);
                    count += 1;
                    // Ack the message so the subscription cursor advances.
                    // Without this, on restart every un-acked message is
                    // re-delivered from subscription start.
                    if let Err(e) = self.consumer.ack(&msg).await {
                        tracing::warn!(error = %e, "pulsar ack failed; will retry on next batch");
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    return Err(ConnectorError::Io(std::io::Error::other(e.to_string())));
                }
            }
        }

        if count == 0 {
            return Ok(None);
        }

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(topic_col.finish()),
                Arc::new(key_col.finish()),
                Arc::new(ts_col.finish()),
                Arc::new(data_col.finish()),
            ],
        )
        .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

        Ok(Some(batch))
    }

    /// Connector capabilities: unbounded streaming source.
    pub fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::default().with_unbounded()
    }
}

// ── Source trait impl ─────────────────────────────────────────────────────────

impl Source for PulsarSource {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_unbounded()
            .with_checkpoint()
    }

    async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        self.next_batch(self.batch_size).await
    }

    fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
        // Pulsar consumer position is broker-managed via subscription.
        // Offsets are implicitly committed on ack; no client-side cursor needed.
        None
    }
}

// ── Message batch builder (testable without a live broker) ───────────────────

/// Batch data from pre-collected Pulsar message tuples `(topic, key, ts_ms, data)`.
///
/// Used in tests to verify schema and column layout without a live broker.
pub fn messages_to_batch(
    schema: &SchemaRef,
    messages: &[(&str, Option<&str>, i64, &[u8])],
) -> ConnectorResult<RecordBatch> {
    let mut topic_col = StringBuilder::with_capacity(messages.len(), messages.len() * 32);
    let mut key_col = StringBuilder::with_capacity(messages.len(), messages.len() * 16);
    let mut ts_col = Int64Builder::with_capacity(messages.len());
    let mut data_col = BinaryBuilder::with_capacity(messages.len(), messages.len() * 64);

    for (topic, key, ts_ms, data) in messages {
        topic_col.append_value(topic);
        match key {
            Some(k) => key_col.append_value(k),
            None => key_col.append_null(),
        }
        ts_col.append_value(*ts_ms);
        data_col.append_value(*data);
    }

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(topic_col.finish()),
            Arc::new(key_col.finish()),
            Arc::new(ts_col.finish()),
            Arc::new(data_col.finish()),
        ],
    )
    .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pulsar_schema_has_four_columns() {
        let schema = pulsar_arrow_schema();
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(
            schema.field_with_name("topic").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("partition_key").unwrap().data_type(),
            &DataType::Utf8
        );
        assert!(
            schema
                .field_with_name("partition_key")
                .unwrap()
                .is_nullable()
        );
        assert_eq!(
            schema
                .field_with_name("publish_time_ms")
                .unwrap()
                .data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("data").unwrap().data_type(),
            &DataType::Binary
        );
    }

    #[test]
    fn messages_to_batch_converts_correctly() {
        let schema = pulsar_arrow_schema();
        let msgs = vec![
            (
                "persistent://public/default/events",
                Some("key-a"),
                1_000_000i64,
                b"hello".as_ref(),
            ),
            (
                "persistent://public/default/events",
                None,
                2_000_000i64,
                b"world".as_ref(),
            ),
        ];
        let batch = messages_to_batch(&schema, &msgs).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);

        use arrow::array::{Array, StringArray};
        let topics = batch
            .column_by_name("topic")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(topics.value(0), "persistent://public/default/events");

        let keys = batch
            .column_by_name("partition_key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(keys.value(0), "key-a");
        assert!(keys.is_null(1));
    }

    #[test]
    fn empty_messages_produces_empty_batch() {
        let schema = pulsar_arrow_schema();
        let batch = messages_to_batch(&schema, &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn config_defaults() {
        let cfg = PulsarConfig::new("pulsar://localhost:6650", "test-topic");
        assert_eq!(cfg.broker_url, "pulsar://localhost:6650");
        assert_eq!(cfg.topic, "test-topic");
        assert_eq!(cfg.subscription, "krishiv-default");
        assert!(matches!(cfg.sub_type, SubType::Exclusive));
    }

    #[test]
    fn config_with_subscription() {
        let cfg = PulsarConfig::new("pulsar://localhost:6650", "t")
            .with_subscription("my-sub")
            .with_sub_type(SubType::Shared);
        assert_eq!(cfg.subscription, "my-sub");
        assert!(matches!(cfg.sub_type, SubType::Shared));
    }

    #[test]
    fn capabilities_unbounded() {
        let caps = ConnectorCapabilities::default().with_unbounded();
        assert!(!caps.is_bounded());
    }

    #[test]
    fn source_capabilities_include_checkpoint() {
        let caps = ConnectorCapabilities::new()
            .with_unbounded()
            .with_checkpoint();
        assert!(caps.is_checkpoint_capable());
        assert!(!caps.is_bounded());
    }

    #[test]
    fn config_with_batch_size() {
        let cfg = PulsarConfig::new("pulsar://localhost:6650", "topic").with_batch_size(200);
        assert_eq!(cfg.batch_size, 200);
    }

    #[test]
    fn config_batch_size_clamped_to_minimum_one() {
        let cfg = PulsarConfig::new("pulsar://localhost:6650", "topic").with_batch_size(0);
        assert_eq!(cfg.batch_size, 1);
    }
}
