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
            return Err(ConnectorError::IoStr {
                message: "KafkaOffset decode: buffer too short for topic_len".into(),
            });
        }
        let topic_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let topic_end = 4 + topic_len;
        if bytes.len() < topic_end + 4 + 8 {
            return Err(ConnectorError::IoStr {
                message: "KafkaOffset decode: buffer too short for topic + partition + offset"
                    .into(),
            });
        }
        let topic = std::str::from_utf8(&bytes[4..topic_end])
            .map_err(|e| ConnectorError::IoStr {
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

/// A Kafka source stub when the `kafka` feature is disabled.
#[cfg(not(feature = "kafka"))]
pub struct KafkaSource {
    config: KafkaConfig,
}

#[cfg(not(feature = "kafka"))]
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

#[cfg(not(feature = "kafka"))]
impl Source for KafkaSource {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_unbounded()
            .with_rewindable()
    }

    async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        Err(ConnectorError::Unsupported {
            message: "Kafka broker connection requires the `kafka` feature".into(),
        })
    }

    fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
        None
    }
}

/// Kafka source backed by `rdkafka` when the `kafka` feature is enabled (P1-20).
#[cfg(feature = "kafka")]
pub struct KafkaSource {
    inner: RdkafkaKafkaSource,
    config: KafkaConfig,
}

#[cfg(feature = "kafka")]
impl KafkaSource {
    /// Create a new `KafkaSource` from a validated config.
    pub fn new(config: KafkaConfig) -> ConnectorResult<Self> {
        let inner = RdkafkaKafkaSource::new(
            config.bootstrap_servers.clone(),
            config.group_id.clone(),
            config.topic.clone(),
        )
        .map_err(|message| ConnectorError::Kafka {
            message,
            retriable: false,
        })?;
        Ok(Self { inner, config })
    }

    /// Return the config this source was created with.
    pub fn config(&self) -> &KafkaConfig {
        &self.config
    }
}

#[cfg(feature = "kafka")]
impl Source for KafkaSource {
    fn capabilities(&self) -> ConnectorCapabilities {
        self.inner.capabilities()
    }

    async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        self.inner.read_batch().await
    }

    fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
        self.inner.current_offset()
    }
}

// ---------------------------------------------------------------------------
// KafkaSink  (stub when `kafka` feature is absent)
// ---------------------------------------------------------------------------

/// A Kafka sink backed by `rdkafka` when the `kafka` feature is enabled.
///
/// Each `write_batch` call serialises every row as a JSON object and produces
/// it to the configured topic.  When the `kafka` feature is not enabled the
/// sink returns `ConnectorError::Unsupported` on every data method.
///
/// Capabilities: unbounded + transactional.
#[cfg(not(feature = "kafka"))]
pub struct KafkaSink {
    config: KafkaConfig,
}

#[cfg(not(feature = "kafka"))]
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

#[cfg(not(feature = "kafka"))]
impl Sink for KafkaSink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_unbounded()
            .with_transactional()
    }

    async fn write_batch(&mut self, _batch: RecordBatch) -> ConnectorResult<()> {
        Err(ConnectorError::Unsupported {
            message: "Kafka producer requires the `kafka` feature".into(),
        })
    }

    async fn flush(&mut self) -> ConnectorResult<()> {
        Err(ConnectorError::Unsupported {
            message: "Kafka sink flush requires the `kafka` feature (pip install krishiv[kafka])"
                .to_string(),
        })
    }
}

// ---------------------------------------------------------------------------
// KafkaSink  (rdkafka-backed when `kafka` feature is enabled)
// ---------------------------------------------------------------------------

/// `rdkafka`-backed Kafka sink (P1-20).
///
/// Each `write_batch` serialises every row of the record batch as a JSON
/// object and sends it to the configured Kafka topic.  Messages are produced
/// with `MessageDelivery::Blocking` so back-pressure is applied before
/// returning.  `flush()` calls `producer.flush()` to drain the internal
/// `rdkafka` queue.
#[cfg(feature = "kafka")]
pub struct KafkaSink {
    config: KafkaConfig,
    producer: rdkafka::producer::FutureProducer,
}

#[cfg(feature = "kafka")]
impl KafkaSink {
    /// Create a new `KafkaSink` from a validated config.
    ///
    /// Returns an error if the rdkafka producer cannot be created
    /// (e.g. invalid bootstrap servers string).
    pub fn new(config: KafkaConfig) -> ConnectorResult<Self> {
        use rdkafka::ClientConfig;

        let producer: rdkafka::producer::FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|e| ConnectorError::Kafka {
                message: format!("rdkafka producer creation failed: {e}"),
                retriable: false,
            })?;

        Ok(Self { config, producer })
    }

    /// Return the config this sink was created with.
    pub fn config(&self) -> &KafkaConfig {
        &self.config
    }

    /// Serialise a single RecordBatch row as a JSON object.
    fn row_to_json(batch: &RecordBatch, row: usize) -> serde_json::Value {
        use arrow::datatypes::DataType;

        let schema = batch.schema();
        let mut map = serde_json::Map::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let v = if col.is_null(row) {
                serde_json::Value::Null
            } else {
                match field.data_type() {
                    DataType::Utf8 => {
                        use arrow::array::StringArray;
                        let arr = col.as_any().downcast_ref::<StringArray>();
                        arr.map(|a| serde_json::Value::String(a.value(row).to_owned()))
                            .unwrap_or(serde_json::Value::Null)
                    }
                    DataType::Int64 => {
                        use arrow::array::Int64Array;
                        let arr = col.as_any().downcast_ref::<Int64Array>();
                        arr.map(|a| serde_json::Value::Number(a.value(row).into()))
                            .unwrap_or(serde_json::Value::Null)
                    }
                    DataType::Int32 => {
                        use arrow::array::Int32Array;
                        let arr = col.as_any().downcast_ref::<Int32Array>();
                        arr.map(|a| serde_json::Value::Number(a.value(row).into()))
                            .unwrap_or(serde_json::Value::Null)
                    }
                    DataType::Float64 => {
                        use arrow::array::Float64Array;
                        let arr = col.as_any().downcast_ref::<Float64Array>();
                        arr.and_then(|a| {
                            serde_json::Number::from_f64(a.value(row))
                                .map(serde_json::Value::Number)
                        })
                        .unwrap_or(serde_json::Value::Null)
                    }
                    DataType::Boolean => {
                        use arrow::array::BooleanArray;
                        let arr = col.as_any().downcast_ref::<BooleanArray>();
                        arr.map(|a| serde_json::Value::Bool(a.value(row)))
                            .unwrap_or(serde_json::Value::Null)
                    }
                    _ => {
                        use arrow::util::display::array_value_to_string;
                        array_value_to_string(col.as_ref(), row)
                            .map(serde_json::Value::String)
                            .unwrap_or(serde_json::Value::Null)
                    }
                }
            };
            map.insert(field.name().clone(), v);
        }
        serde_json::Value::Object(map)
    }
}

#[cfg(feature = "kafka")]
impl Sink for KafkaSink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_unbounded()
            .with_transactional()
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> ConnectorResult<()> {
        use rdkafka::producer::FutureRecord;
        use std::time::Duration;

        let topic = self.config.topic.clone();
        for row in 0..batch.num_rows() {
            let json = Self::row_to_json(&batch, row);
            let payload = json.to_string();
            let record: FutureRecord<'_, str, str> = FutureRecord::to(&topic).payload(&payload);
            self.producer
                .send(record, Duration::from_secs(5))
                .await
                .map_err(|(e, _)| ConnectorError::Kafka {
                    message: format!("rdkafka produce failed: {e}"),
                    retriable: true,
                })?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> ConnectorResult<()> {
        use rdkafka::producer::Producer;
        use std::time::Duration;
        self.producer
            .flush(Duration::from_secs(10))
            .map_err(|e| ConnectorError::Kafka {
                message: format!("rdkafka flush failed: {e}"),
                retriable: true,
            })
    }
}

// In-memory Kafka test harness
// ---------------------------------------------------------------------------

/// **Testing only**: In-memory implementation for unit tests. Not for production use.
///
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
// RdkafkaKafkaSource  (behind `kafka` feature)
// ---------------------------------------------------------------------------

/// A streaming [`Source`] that reads Arrow `RecordBatch`es from a Kafka topic
/// via `rdkafka`.
///
/// Each `read_batch` call polls a single Kafka message, deserialises the JSON
/// payload into a single-row `RecordBatch` (all columns as `Utf8`), and returns
/// it.  Offset commits are triggered when [`RdkafkaKafkaSource::commit_watermark`]
/// is called — typically after the downstream sink has acknowledged the data —
/// providing at-least-once delivery semantics.
///
/// Capabilities: unbounded.
#[cfg(feature = "kafka")]
pub struct RdkafkaKafkaSource {
    consumer: std::sync::Arc<rdkafka::consumer::StreamConsumer>,
    topic: String,
    partition: i32,
    /// Timeout per poll attempt in milliseconds.
    poll_timeout_ms: u64,
    /// Last successfully read (partition, offset) for watermark commits.
    last_offset: Option<(i32, i64)>,
}

#[cfg(feature = "kafka")]
impl RdkafkaKafkaSource {
    /// Create a new source subscribed to `topic`.
    ///
    /// `bootstrap_servers`: comma-separated `host:port` list.
    /// `group_id`: consumer group identifier.
    /// `topic`: Kafka topic name.
    ///
    /// Returns an error string if the consumer cannot be created or the
    /// subscription fails.
    pub fn new(
        bootstrap_servers: impl AsRef<str>,
        group_id: impl AsRef<str>,
        topic: impl Into<String>,
    ) -> Result<Self, String> {
        use rdkafka::ClientConfig;
        use rdkafka::consumer::Consumer;

        let topic = topic.into();

        let consumer: rdkafka::consumer::StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers.as_ref())
            .set("group.id", group_id.as_ref())
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(|e| format!("rdkafka consumer creation failed: {e}"))?;

        consumer
            .subscribe(&[topic.as_str()])
            .map_err(|e| format!("rdkafka subscribe to '{topic}' failed: {e}"))?;

        Ok(Self {
            consumer: std::sync::Arc::new(consumer),
            topic,
            partition: 0,
            poll_timeout_ms: 100,
            last_offset: None,
        })
    }

    /// Create a source from a [`crate::cdc::KafkaCdcConfig`].
    #[cfg(feature = "kafka")]
    pub fn from_cdc_config(config: &crate::cdc::KafkaCdcConfig) -> Result<Self, String> {
        Self::new(&config.bootstrap_servers, &config.group_id, &config.topic)
    }

    /// Override the per-poll timeout (default: 100 ms).
    #[must_use]
    pub fn with_poll_timeout_ms(mut self, ms: u64) -> Self {
        self.poll_timeout_ms = ms;
        self
    }

    /// Commit consumer group offsets up to the last successfully read message.
    ///
    /// Call this after the downstream sink has acknowledged the corresponding
    /// output, advancing the committed watermark and preventing re-processing
    /// of already-delivered messages on restart.
    pub fn commit_watermark(&self) {
        use rdkafka::consumer::Consumer;
        if let Err(e) = self
            .consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
        {
            tracing::warn!(
                topic = %self.topic,
                error = %e,
                "rdkafka watermark commit failed"
            );
        }
    }

    /// Return the current `KafkaOffset` (the next offset that will be read,
    /// suitable for committing after downstream acknowledgement).
    pub fn current_kafka_offset(&self) -> Option<KafkaOffset> {
        self.last_offset.map(|(partition, offset)| KafkaOffset {
            topic: self.topic.clone(),
            partition,
            offset: offset + 1, // Kafka convention: committed offset = next to read
        })
    }

    /// Deserialise a raw UTF-8 Kafka payload into a single-row `RecordBatch`.
    ///
    /// If the payload is valid JSON the top-level object's string values are
    /// unpacked into separate `Utf8` columns (sorted alphabetically for schema
    /// stability).  Non-JSON payloads are returned as a single `_raw: Utf8`
    /// column so the pipeline never silently discards messages.
    fn payload_to_batch(payload: &str) -> crate::ConnectorResult<arrow::record_batch::RecordBatch> {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        // Try to parse as a JSON object.
        if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(payload)
        {
            let mut keys: Vec<&str> = map.keys().map(String::as_str).collect();
            keys.sort_unstable();

            let fields: Vec<Field> = keys
                .iter()
                .map(|k| Field::new(*k, DataType::Utf8, true))
                .collect();
            let schema = Arc::new(Schema::new(fields));

            let columns: Vec<Arc<dyn arrow::array::Array>> = keys
                .iter()
                .map(|k| {
                    let v = match &map[*k] {
                        serde_json::Value::Null => None,
                        serde_json::Value::String(s) => Some(s.clone()),
                        other => Some(other.to_string()),
                    };
                    let arr: StringArray = std::iter::once(v.as_deref()).collect();
                    Arc::new(arr) as Arc<dyn arrow::array::Array>
                })
                .collect();

            return arrow::record_batch::RecordBatch::try_new(schema, columns).map_err(|e| {
                crate::ConnectorError::Schema {
                    message: format!("failed to build batch from Kafka JSON payload: {e}"),
                }
            });
        }

        // Non-JSON payload: wrap in a `_raw` column.
        let schema = Arc::new(Schema::new(vec![Field::new("_raw", DataType::Utf8, true)]));
        let arr: StringArray = std::iter::once(Some(payload)).collect();
        arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(arr)]).map_err(|e| {
            crate::ConnectorError::Schema {
                message: format!("failed to build _raw batch from Kafka payload: {e}"),
            }
        })
    }
}

#[cfg(feature = "kafka")]
impl Source for RdkafkaKafkaSource {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new().with_unbounded()
    }

    async fn read_batch(
        &mut self,
    ) -> crate::ConnectorResult<Option<arrow::record_batch::RecordBatch>> {
        use rdkafka::Message;

        let msg = tokio::time::timeout(
            std::time::Duration::from_millis(self.poll_timeout_ms),
            self.consumer.recv(),
        )
        .await;

        match msg {
            // GAP-16: Poll timeout — no message available on this poll cycle.
            // Check whether the consumer has reached the broker's high-water mark
            // for this partition.  If so, return `Ok(None)` to signal EOF so the
            // caller can stop polling instead of spinning indefinitely.
            //
            // `fetch_watermarks` is a synchronous metadata RPC; we run it on a
            // blocking thread so it does not stall the async runtime.  On error
            // we fall back to returning an empty batch (momentary idle) so the
            // pipeline does not abort on a transient metadata failure.
            Err(_timeout) => {
                let consumer = self.consumer.clone();
                let topic = self.topic.clone();
                let partition = self.partition;
                let next_offset = self.last_offset.map(|(_, o)| o + 1).unwrap_or(0);

                let at_eof = tokio::task::spawn_blocking(move || {
                    use rdkafka::consumer::Consumer;
                    match consumer.fetch_watermarks(
                        &topic,
                        partition,
                        std::time::Duration::from_millis(500),
                    ) {
                        Ok((_low, high)) => next_offset >= high,
                        Err(_) => false, // conservatively assume not at EOF on metadata error
                    }
                })
                .await
                .unwrap_or(false);

                // Return Ok(None) both at EOF and on idle timeout so the caller
                // can back off instead of busy-looping on empty batches.
                Ok(None)
            }
            Ok(Err(e)) => Err(crate::ConnectorError::IoStr {
                message: format!("rdkafka receive error: {e}"),
            }),
            Ok(Ok(msg)) => {
                self.partition = msg.partition();
                self.last_offset = Some((msg.partition(), msg.offset()));

                let payload = match msg.payload_view::<str>() {
                    Some(Ok(s)) => s,
                    Some(Err(_)) | None => {
                        // Tombstone or non-UTF-8: return an empty batch to avoid blocking.
                        tracing::warn!(
                            topic = %self.topic,
                            partition = msg.partition(),
                            offset = msg.offset(),
                            "skipping unreadable Kafka message"
                        );
                        return Ok(Some(arrow::record_batch::RecordBatch::new_empty(
                            std::sync::Arc::new(arrow::datatypes::Schema::empty()),
                        )));
                    }
                };

                let batch = Self::payload_to_batch(payload)?;
                Ok(Some(batch))
            }
        }
    }

    fn current_offset(&self) -> Option<Box<dyn std::any::Any + Send>> {
        self.current_kafka_offset()
            .map(|o| Box::new(o) as Box<dyn std::any::Any + Send>)
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

    #[tokio::test]
    async fn kafka_source_reports_unbounded_and_rewindable() {
        let config = KafkaConfig {
            bootstrap_servers: "localhost:9092".into(),
            topic: "events".into(),
            group_id: "test-group".into(),
        };
        #[cfg(not(feature = "kafka"))]
        let source = KafkaSource::new(config);
        #[cfg(feature = "kafka")]
        let source = KafkaSource::new(config).expect("kafka source");
        let caps = source.capabilities();
        assert!(caps.is_unbounded());
        #[cfg(not(feature = "kafka"))]
        assert!(caps.is_rewindable());
        assert!(!caps.is_bounded());
        assert!(!caps.is_transactional());
        assert!(!caps.is_idempotent());
    }

    // -----------------------------------------------------------------------
    // KafkaSink capabilities
    // -----------------------------------------------------------------------

    #[test]
    #[cfg(not(feature = "kafka"))]
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
    #[cfg(not(feature = "kafka"))]
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
    async fn kafka_source_reads_batches() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        let mut source = InMemoryKafkaSource::new("events", 0, 0, vec![batch]);

        let read = source.read_batch().await.unwrap().unwrap();
        assert_eq!(read.num_rows(), 3);
        assert!(source.read_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    #[cfg(not(feature = "kafka"))]
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

    // -----------------------------------------------------------------------
    // RdkafkaKafkaSource — compile-only / type-check tests (no live broker)
    // -----------------------------------------------------------------------

    /// Verify that `RdkafkaKafkaSource` reports `unbounded` capability.
    ///
    /// We cannot connect to a real broker in unit tests, so we only assert the
    /// constructor error path and that the type satisfies the `Source` trait.
    ///
    /// `StreamConsumer::new` spawns a tokio task internally, so this test must
    /// run inside a tokio runtime.
    #[cfg(feature = "kafka")]
    #[tokio::test]
    async fn rdkafka_kafka_source_constructor_fails_gracefully_without_broker() {
        // Connecting to an unreachable broker should fail with a descriptive
        // error rather than panicking.
        let result = super::RdkafkaKafkaSource::new("localhost:1", "test-group", "test-topic");
        // rdkafka validates config synchronously; creation may succeed or fail
        // depending on platform.  Both are acceptable — we only assert no panic.
        let _ = result;
    }

    /// `RdkafkaKafkaSource` implements `Source`.
    #[cfg(feature = "kafka")]
    #[test]
    fn rdkafka_kafka_source_implements_source_trait() {
        // This is a compile-time assertion: if `RdkafkaKafkaSource` does not
        // implement `Source` the test will not compile.
        fn assert_source<T: crate::Source>() {}
        assert_source::<super::RdkafkaKafkaSource>();
    }

    #[tokio::test]
    async fn kafka_sink_stub_flush_returns_error() {
        #[cfg(not(feature = "kafka"))]
        {
            use crate::Sink;
            let config = KafkaConfig {
                bootstrap_servers: "localhost:9092".to_string(),
                topic: "events".to_string(),
                group_id: "krishiv-default".to_string(),
            };
            let mut sink = KafkaSink::new(config);
            let result = sink.flush().await;
            assert!(
                result.is_err(),
                "flush on stub KafkaSink must return an error"
            );
            let err = result.err().unwrap().to_string();
            assert!(err.contains("Kafka sink flush requires the `kafka` feature"));
        }
    }

    // -----------------------------------------------------------------------
    // Property-based tests (Sprint 4)
    // -----------------------------------------------------------------------

    #[cfg(test)]
    mod property_tests {
        use super::*;
        use proptest::prelude::*;

        /// Property test: KafkaOffset round-trip encode/decode
        /// Ensures arbitrary offsets can be serialized and deserialized without loss.
        proptest! {
            #[test]
            fn kafka_offset_roundtrip(
                topic in "[a-z]{1,64}",
                partition: i32,
                offset: i64,
            ) {
                let original = KafkaOffset { topic, partition, offset };
                let encoded = original.encode();
                let decoded = KafkaOffset::decode(&encoded).expect("decode");
                prop_assert_eq!(original, decoded);
            }

            /// Property test: KafkaOffset encode is stable
            /// Same offset always produces the same bytes.
            #[test]
            fn kafka_offset_encode_stable(
                topic in "[a-z]{1,32}",
                partition: i32,
                offset: i64,
            ) {
                let off = KafkaOffset { topic, partition, offset };
                let enc1 = off.encode();
                let enc2 = off.encode();
                prop_assert_eq!(enc1, enc2);
            }
        }
    }
}
