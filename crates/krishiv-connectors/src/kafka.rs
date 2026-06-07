//! Kafka source and sink connectors.
//!
//! Broker-backed implementations are gated behind the `kafka` feature. Without
//! that feature, constructors remain available for configuration validation but
//! data methods return [`ConnectorError::Unsupported`] and advertise no runtime
//! capabilities.

use std::any::Any;

use arrow::record_batch::RecordBatch;

use crate::{
    CheckpointSource, ConnectorCapabilities, ConnectorConfig, ConnectorError, ConnectorResult,
    OffsetCommitter, Sink, Source,
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
            return Err(ConnectorError::Offset {
                message: "KafkaOffset decode: buffer too short for topic_len".into(),
            });
        }
        let topic_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let topic_end = 4usize
            .checked_add(topic_len)
            .ok_or_else(|| ConnectorError::Offset {
                message: "KafkaOffset decode: topic length overflow".into(),
            })?;
        let expected_len = topic_end
            .checked_add(12)
            .ok_or_else(|| ConnectorError::Offset {
                message: "KafkaOffset decode: encoded length overflow".into(),
            })?;
        if bytes.len() != expected_len {
            return Err(ConnectorError::Offset {
                message: format!(
                    "KafkaOffset decode: expected {expected_len} bytes from topic length, got {}",
                    bytes.len()
                ),
            });
        }
        let topic = std::str::from_utf8(&bytes[4..topic_end])
            .map_err(|e| ConnectorError::Offset {
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
    pub bootstrap_servers: String,
    pub topic: String,
    pub group_id: String,
    pub auto_commit_interval_ms: Option<u64>,
    pub security_protocol: Option<String>,
    pub ssl_ca_location: Option<String>,
    pub ssl_certificate_location: Option<String>,
    pub ssl_key_location: Option<String>,
    pub ssl_key_password: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub sasl_mechanisms: Option<String>,
    pub enable_idempotence: Option<bool>,
    pub transactional_id: Option<String>,
}

impl KafkaConfig {
    /// Validate and extract a `KafkaConfig` from a [`ConnectorConfig`].
    ///
    /// Required properties: `bootstrap.servers`, `topic`.
    /// Optional: `group.id` (defaults to `"krishiv-default"`),
    ///           `auto.commit.interval.ms` (enables auto-commit when present).
    pub fn from_config(config: &ConnectorConfig) -> ConnectorResult<Self> {
        Ok(Self {
            bootstrap_servers: config.required("bootstrap.servers")?.to_string(),
            topic: config.required("topic")?.to_string(),
            group_id: config
                .get("group.id")
                .unwrap_or("krishiv-default")
                .to_string(),
            auto_commit_interval_ms: config
                .get("auto.commit.interval.ms")
                .and_then(|s| s.parse().ok()),
            security_protocol: config.get("security.protocol").map(|s| s.to_string()),
            ssl_ca_location: config.get("ssl.ca.location").map(|s| s.to_string()),
            ssl_certificate_location: config
                .get("ssl.certificate.location")
                .map(|s| s.to_string()),
            ssl_key_location: config.get("ssl.key.location").map(|s| s.to_string()),
            ssl_key_password: config.get("ssl.key.password").map(|s| s.to_string()),
            sasl_username: config.get("sasl.username").map(|s| s.to_string()),
            sasl_password: config.get("sasl.password").map(|s| s.to_string()),
            sasl_mechanisms: config.get("sasl.mechanisms").map(|s| s.to_string()),
            enable_idempotence: config
                .get("enable.idempotence")
                .and_then(|s| s.parse().ok()),
            transactional_id: config.get("transactional.id").map(|s| s.to_string()),
        })
    }

    /// Enable rdkafka auto-commit with the given interval.
    #[must_use]
    pub fn with_auto_commit(mut self, interval_ms: u64) -> Self {
        self.auto_commit_interval_ms = Some(interval_ms);
        self
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
            config.auto_commit_interval_ms,
            Some(&config),
        )
        .map_err(|message| ConnectorError::Kafka {
            message,
            retriable: false,
        })?;
        Ok(Self { inner, config })
    }

    /// Commit the current watermark offset for all partitions read so far.
    /// No-op if no messages have been read yet.
    pub fn commit_current_offset(&self) {
        self.inner.commit_offsets();
    }

    /// All per-partition offsets read so far (suitable for checkpoint storage).
    pub fn all_current_offsets(&self) -> Vec<KafkaOffset> {
        self.inner.all_current_offsets()
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
    pub fn new(config: KafkaConfig) -> ConnectorResult<Self> {
        use rdkafka::ClientConfig;

        let mut client = ClientConfig::new();
        client
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("message.timeout.ms", "5000");
        apply_kafka_security_config(&mut client, &config);
        apply_kafka_transactional_config(&mut client, &config);

        let producer: rdkafka::producer::FutureProducer =
            client.create().map_err(|e| ConnectorError::Kafka {
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
        ConnectorCapabilities::new().with_unbounded()
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
    start_offset: i64,
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
            start_offset,
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
            .with_checkpoint()
    }

    async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
        if self.cursor >= self.batches.len() {
            return Ok(None);
        }
        let batch = self.batches[self.cursor].clone();
        let row_count = i64::try_from(batch.num_rows()).map_err(|_| ConnectorError::Offset {
            message: "in-memory Kafka batch row count exceeds i64".into(),
        })?;
        let next_offset =
            self.next_offset
                .checked_add(row_count)
                .ok_or_else(|| ConnectorError::Offset {
                    message: "in-memory Kafka offset overflow".into(),
                })?;
        self.cursor += 1;
        self.next_offset = next_offset;
        Ok(Some(batch))
    }

    fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
        Some(Box::new(self.next_offset()))
    }

    fn reset(&mut self) {
        self.cursor = 0;
        self.next_offset = self.start_offset;
    }
}

impl CheckpointSource for InMemoryKafkaSource {
    type Offset = KafkaOffset;

    fn checkpoint_offset(&self) -> ConnectorResult<Self::Offset> {
        Ok(self.next_offset())
    }

    fn restore_offset(&mut self, offset: &Self::Offset) -> ConnectorResult<()> {
        if offset.topic != self.topic || offset.partition != self.partition {
            return Err(ConnectorError::Offset {
                message: format!(
                    "Kafka offset belongs to {}/{}, expected {}/{}",
                    offset.topic, offset.partition, self.topic, self.partition
                ),
            });
        }
        if offset.offset < self.start_offset {
            return Err(ConnectorError::Offset {
                message: format!(
                    "Kafka offset {} is before configured start offset {}",
                    offset.offset, self.start_offset
                ),
            });
        }

        let mut candidate = self.start_offset;
        if offset.offset == candidate {
            self.cursor = 0;
            self.next_offset = candidate;
            return Ok(());
        }
        for (index, batch) in self.batches.iter().enumerate() {
            let row_count =
                i64::try_from(batch.num_rows()).map_err(|_| ConnectorError::Offset {
                    message: "in-memory Kafka batch row count exceeds i64".into(),
                })?;
            candidate = candidate
                .checked_add(row_count)
                .ok_or_else(|| ConnectorError::Offset {
                    message: "in-memory Kafka offset overflow while restoring".into(),
                })?;
            if offset.offset == candidate {
                self.cursor = index + 1;
                self.next_offset = candidate;
                return Ok(());
            }
            if offset.offset < candidate {
                return Err(ConnectorError::Offset {
                    message: format!(
                        "Kafka offset {} is not a batch boundary for {}/{}",
                        offset.offset, self.topic, self.partition
                    ),
                });
            }
        }

        Err(ConnectorError::Offset {
            message: format!(
                "Kafka offset {} is past final available offset {} for {}/{}",
                offset.offset, candidate, self.topic, self.partition
            ),
        })
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
    /// Timeout per poll attempt in milliseconds.
    poll_timeout_ms: u64,
    /// Latest offset read per partition — tracks all partitions, not just the last one.
    partition_offsets: std::collections::HashMap<i32, i64>,
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
    /// Create a new source subscribed to `topic`.
    ///
    /// `auto_commit_interval_ms`: `None` = manual commit; `Some(ms)` enables
    /// rdkafka auto-commit every `ms` milliseconds (at-least-once delivery).
    pub fn new(
        bootstrap_servers: impl AsRef<str>,
        group_id: impl AsRef<str>,
        topic: impl Into<String>,
        auto_commit_interval_ms: Option<u64>,
        kafka_cfg: Option<&KafkaConfig>,
    ) -> Result<Self, String> {
        use rdkafka::ClientConfig;
        use rdkafka::consumer::Consumer;

        let topic = topic.into();
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", bootstrap_servers.as_ref())
            .set("group.id", group_id.as_ref())
            .set("auto.offset.reset", "earliest");

        if let Some(kc) = kafka_cfg {
            apply_kafka_security_config(&mut cfg, kc);
        }

        match auto_commit_interval_ms {
            Some(ms) => {
                // At-least-once risk: auto-commit fires on a timer that is
                // independent of the distributed checkpoint cycle. If the
                // executor crashes after auto-commit but before the state
                // snapshot, messages committed here will be skipped on restart.
                // Manual commit is required for checkpoint-aligned recovery, but
                // it is not sufficient for exactly-once until broker partition
                // seek/restore is wired through CheckpointSource.
                tracing::warn!(
                    topic = %topic,
                    interval_ms = ms,
                    "Kafka auto-commit enabled: at-least-once delivery only. \
                     For checkpoint-aligned recovery, set auto_commit_interval_ms=None \
                     and commit only after each durable state checkpoint."
                );
                cfg.set("enable.auto.commit", "true")
                    .set("auto.commit.interval.ms", ms.to_string());
            }
            None => {
                cfg.set("enable.auto.commit", "false");
            }
        }

        let consumer: rdkafka::consumer::StreamConsumer = cfg
            .create()
            .map_err(|e| format!("rdkafka consumer creation failed: {e}"))?;

        consumer
            .subscribe(&[topic.as_str()])
            .map_err(|e| format!("rdkafka subscribe to '{topic}' failed: {e}"))?;

        Ok(Self {
            consumer: std::sync::Arc::new(consumer),
            topic,
            poll_timeout_ms: 100,
            partition_offsets: std::collections::HashMap::new(),
        })
    }

    /// Create a source from a [`crate::cdc::KafkaCdcConfig`] (manual commit mode).
    #[cfg(feature = "kafka")]
    pub fn from_cdc_config(config: &crate::cdc::KafkaCdcConfig) -> Result<Self, String> {
        Self::new(
            &config.bootstrap_servers,
            &config.group_id,
            &config.topic,
            None,
            None,
        )
    }

    /// Override the per-poll timeout (default: 100 ms).
    #[must_use]
    pub fn with_poll_timeout_ms(mut self, ms: u64) -> Self {
        self.poll_timeout_ms = ms;
        self
    }

    /// Commit consumer group offsets up to the last successfully read message.
    ///
    /// **Must only be called after the downstream state has been durably
    /// persisted** (e.g. after `state_backend.snapshot()` succeeds). Calling
    /// before the snapshot creates a window where the committed offset advances
    /// past unsnapshotted state — messages would be skipped on restart (C7).
    ///
    /// Manual commit alone is not an exactly-once certification. This broker
    /// source intentionally does not advertise checkpoint capability until
    /// partition assignment and seek-based restore implement [`CheckpointSource`].
    pub fn commit_offsets(&self) {
        use rdkafka::consumer::Consumer;
        if let Err(e) = self
            .consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Sync)
        {
            tracing::warn!(
                topic = %self.topic,
                error = %e,
                "rdkafka offset commit failed"
            );
        }
    }

    /// Deprecated alias for [`commit_offsets`].
    #[deprecated(since = "0.1.0", note = "use commit_offsets() instead")]
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

    /// Latest committed offset for the most recently read partition.
    /// For multi-partition topics use [`all_current_offsets`] instead.
    pub fn current_kafka_offset(&self) -> Option<KafkaOffset> {
        self.partition_offsets
            .iter()
            .max_by_key(|&(_, offset)| offset)
            .map(|(&partition, &offset)| KafkaOffset {
                topic: self.topic.clone(),
                partition,
                offset: offset + 1, // Kafka convention: committed offset = next to read
            })
    }

    /// Partition IDs currently assigned to this consumer by the broker's group
    /// coordinator (C6).
    ///
    /// rdkafka handles partition assignment automatically via the consumer group
    /// rebalance protocol — each partition in the topic is assigned to exactly
    /// one consumer in the group, preventing duplicate reads across executors.
    /// This accessor exposes which partitions this instance currently owns,
    /// derived from the set of partitions from which messages have been received.
    /// Partitions are added on first message receipt and are never removed
    /// (offsets remain valid for offset tracking across restarts).
    pub fn assigned_partitions(&self) -> Vec<i32> {
        let mut partitions: Vec<i32> = self.partition_offsets.keys().copied().collect();
        partitions.sort_unstable();
        partitions
    }

    /// All per-partition offsets read so far — correct for multi-partition topics.
    /// Each entry is the *next* offset to read (last_read + 1).
    pub fn all_current_offsets(&self) -> Vec<KafkaOffset> {
        self.partition_offsets
            .iter()
            .map(|(&partition, &offset)| KafkaOffset {
                topic: self.topic.clone(),
                partition,
                offset: offset + 1,
            })
            .collect()
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
            // Poll timeout — no message ready on this cycle; caller retries.
            Err(_timeout) => Ok(None),
            Ok(Err(e)) => Err(crate::ConnectorError::Kafka {
                message: format!("rdkafka receive error: {e}"),
                retriable: true,
            }),
            Ok(Ok(msg)) => {
                // C6: Track offset per partition. Log when a new partition is
                // first assigned — this happens after a consumer group rebalance
                // and surfaces which partitions each executor instance owns.
                let new_partition = !self.partition_offsets.contains_key(&msg.partition());
                self.partition_offsets.insert(msg.partition(), msg.offset());
                if new_partition {
                    tracing::info!(
                        topic = %self.topic,
                        partition = msg.partition(),
                        "Kafka partition assigned to this consumer via group rebalance"
                    );
                }

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
        // Returns all per-partition offsets boxed as Vec<KafkaOffset>.
        // Single-partition callers may downcast to KafkaOffset via current_kafka_offset().
        let offsets = self.all_current_offsets();
        if offsets.is_empty() {
            None
        } else {
            Some(Box::new(offsets) as Box<dyn std::any::Any + Send>)
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(feature = "kafka")]
fn apply_kafka_security_config(cfg: &mut rdkafka::ClientConfig, config: &KafkaConfig) {
    if let Some(ref proto) = config.security_protocol {
        cfg.set("security.protocol", proto);
    }
    if let Some(ref ca) = config.ssl_ca_location {
        cfg.set("ssl.ca.location", ca);
    }
    if let Some(ref cert) = config.ssl_certificate_location {
        cfg.set("ssl.certificate.location", cert);
    }
    if let Some(ref key) = config.ssl_key_location {
        cfg.set("ssl.key.location", key);
    }
    if let Some(ref pass) = config.ssl_key_password {
        cfg.set("ssl.key.password", pass);
    }
    if let Some(ref user) = config.sasl_username {
        cfg.set("sasl.username", user);
    }
    if let Some(ref pass) = config.sasl_password {
        cfg.set("sasl.password", pass);
    }
    if let Some(ref mech) = config.sasl_mechanisms {
        cfg.set("sasl.mechanisms", mech);
    }
}

#[cfg(feature = "kafka")]
fn apply_kafka_transactional_config(cfg: &mut rdkafka::ClientConfig, config: &KafkaConfig) {
    if config.enable_idempotence.unwrap_or(false) {
        cfg.set("enable.idempotence", "true");
    }
    if let Some(ref tid) = config.transactional_id {
        cfg.set("enable.idempotence", "true");
        cfg.set("transactional.id", tid);
        tracing::info!(
            transactional_id = %tid,
            "kafka producer configured for exactly-once transactions"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectorConfig, Offset};

    fn test_kafka_config() -> KafkaConfig {
        KafkaConfig {
            bootstrap_servers: "localhost:9092".into(),
            topic: "events".into(),
            group_id: "test-group".into(),
            auto_commit_interval_ms: None,
            security_protocol: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_key_password: None,
            sasl_username: None,
            sasl_password: None,
            sasl_mechanisms: None,
            enable_idempotence: None,
            transactional_id: None,
        }
    }

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

    #[test]
    fn kafka_offset_decode_rejects_noncanonical_length() {
        let mut encoded = KafkaOffset {
            topic: "events".to_string(),
            partition: 3,
            offset: 42,
        }
        .encode();
        encoded.push(0);
        let error =
            KafkaOffset::decode(&encoded).expect_err("trailing offset bytes must be rejected");
        assert!(matches!(error, ConnectorError::Offset { .. }));
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
        let config = test_kafka_config();
        #[cfg(not(feature = "kafka"))]
        let source = KafkaSource::new(config);
        #[cfg(feature = "kafka")]
        let source = KafkaSource::new(config).expect("kafka source");
        let caps = source.capabilities();
        #[cfg(feature = "kafka")]
        assert!(caps.is_unbounded());
        #[cfg(not(feature = "kafka"))]
        assert!(!caps.is_unbounded());
        assert!(!caps.is_rewindable());
        assert!(!caps.is_bounded());
        assert!(!caps.is_transactional());
        assert!(!caps.is_idempotent());
        assert!(
            !caps.is_checkpoint_capable(),
            "broker Kafka must not claim checkpoint restore before seek is implemented"
        );
    }

    // -----------------------------------------------------------------------
    // KafkaSink capabilities
    // -----------------------------------------------------------------------

    #[test]
    #[cfg(not(feature = "kafka"))]
    fn kafka_sink_stub_reports_no_runtime_capabilities() {
        let config = test_kafka_config();
        let sink = KafkaSink::new(config);
        let caps = sink.capabilities();
        assert!(!caps.is_unbounded());
        assert!(!caps.is_transactional());
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
        let config = test_kafka_config();
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

        let config = test_kafka_config();
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
        let mut source = InMemoryKafkaSource::new("events", 2, 10, vec![batch.clone()]);
        assert!(source.capabilities().is_checkpoint_capable());

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

        let mut rewind_source = InMemoryKafkaSource::new("events", 2, 10, vec![batch]);
        crate::CertificationSuite::run_rewind_test::<KafkaOffset>(&mut rewind_source)
            .await
            .expect("in-memory Kafka source must restore cursor and starting offset");
    }

    #[tokio::test]
    async fn in_memory_kafka_source_restores_typed_checkpoint_offsets() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let first =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();
        let second =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![3, 4, 5]))]).unwrap();
        let mut source = InMemoryKafkaSource::new("events", 2, 10, vec![first, second]);

        crate::CertificationSuite::run_checkpoint_restore_test(&mut source)
            .await
            .expect("in-memory Kafka source must restore exact batch boundaries");
    }

    #[test]
    fn in_memory_kafka_source_rejects_non_boundary_checkpoint_offset() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        let mut source = InMemoryKafkaSource::new("events", 2, 10, vec![batch]);

        let error = source
            .restore_offset(&KafkaOffset {
                topic: "events".into(),
                partition: 2,
                offset: 11,
            })
            .expect_err("offset inside a batch must not be accepted");
        assert!(matches!(error, ConnectorError::Offset { .. }));
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
        let result =
            super::RdkafkaKafkaSource::new("localhost:1", "test-group", "test-topic", None, None);
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
            let config = test_kafka_config();
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

        // Property test: KafkaOffset round-trip encode/decode.
        // Ensures arbitrary offsets can be serialized and deserialized without loss.
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

            // Property test: KafkaOffset encode is stable.
            // Same offset always produces the same bytes.
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
