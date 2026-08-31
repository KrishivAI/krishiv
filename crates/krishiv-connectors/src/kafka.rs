//! Kafka source/sink with feature-gated rdkafka backend, offset types, and in-memory test harness.
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
        let topic_len = u32::try_from(topic_bytes.len()).unwrap_or(u32::MAX);
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
        let topic_len = u32::from_le_bytes(
            bytes
                .get(..4)
                .ok_or_else(|| ConnectorError::Offset {
                    message: "KafkaOffset decode: slice length mismatch for topic_len".into(),
                })?
                .try_into()
                .map_err(|_| ConnectorError::Offset {
                    message: "KafkaOffset decode: slice length mismatch for topic_len".into(),
                })?,
        ) as usize;
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
        let topic =
            std::str::from_utf8(
                bytes
                    .get(4..topic_end)
                    .ok_or_else(|| ConnectorError::Offset {
                        message: "KafkaOffset decode: topic slice out of range".into(),
                    })?,
            )
            .map_err(|e| ConnectorError::Offset {
                message: format!("KafkaOffset decode: invalid UTF-8 in topic: {e}"),
            })?
            .to_string();
        let partition = i32::from_le_bytes(
            bytes
                .get(topic_end..topic_end + 4)
                .ok_or_else(|| ConnectorError::Offset {
                    message: "KafkaOffset decode: slice length mismatch for partition".into(),
                })?
                .try_into()
                .map_err(|_| ConnectorError::Offset {
                    message: "KafkaOffset decode: slice length mismatch for partition".into(),
                })?,
        );
        let offset_start = topic_end + 4;
        let offset = i64::from_le_bytes(
            bytes
                .get(offset_start..offset_start + 8)
                .ok_or_else(|| ConnectorError::Offset {
                    message: "KafkaOffset decode: slice length mismatch for offset".into(),
                })?
                .try_into()
                .map_err(|_| ConnectorError::Offset {
                    message: "KafkaOffset decode: slice length mismatch for offset".into(),
                })?,
        );
        Ok(KafkaOffset {
            topic,
            partition,
            offset,
        })
    }
}

// ---------------------------------------------------------------------------
// MultiKafkaOffset
// ---------------------------------------------------------------------------

/// Multi-partition Kafka checkpoint: one [`KafkaOffset`] per assigned partition.
///
/// Encoding: `u32 count (LE) | for each entry: u32 item_len (LE) | KafkaOffset bytes`.
/// The length-prefix per entry lets the decoder slice correctly without assuming
/// a fixed size (topic length is variable).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MultiKafkaOffset {
    pub offsets: Vec<KafkaOffset>,
}

impl MultiKafkaOffset {
    /// Create from a list of per-partition offsets.
    pub fn new(offsets: Vec<KafkaOffset>) -> Self {
        Self { offsets }
    }

    /// True when no partitions have been checkpointed yet.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }
}

impl crate::Offset for MultiKafkaOffset {
    fn encode(&self) -> Vec<u8> {
        let count = u32::try_from(self.offsets.len()).unwrap_or(u32::MAX);
        let mut out = Vec::new();
        out.extend_from_slice(&count.to_le_bytes());
        for ko in &self.offsets {
            let item_bytes = ko.encode();
            let item_len = u32::try_from(item_bytes.len()).unwrap_or(u32::MAX);
            out.extend_from_slice(&item_len.to_le_bytes());
            out.extend_from_slice(&item_bytes);
        }
        out
    }

    fn decode(bytes: &[u8]) -> ConnectorResult<Self> {
        if bytes.len() < 4 {
            return Err(ConnectorError::Offset {
                message: "MultiKafkaOffset decode: buffer too short for count".into(),
            });
        }
        let count = u32::from_le_bytes(
            bytes
                .get(..4)
                .ok_or_else(|| ConnectorError::Offset {
                    message: "MultiKafkaOffset decode: slice length mismatch for count".into(),
                })?
                .try_into()
                .map_err(|_| ConnectorError::Offset {
                    message: "MultiKafkaOffset decode: slice length mismatch for count".into(),
                })?,
        ) as usize;
        let mut pos = 4usize;
        let mut offsets = Vec::with_capacity(count);
        for i in 0..count {
            if pos + 4 > bytes.len() {
                return Err(ConnectorError::Offset {
                    message: format!(
                        "MultiKafkaOffset decode: truncated before entry {i} length prefix"
                    ),
                });
            }
            let item_len = u32::from_le_bytes(
                bytes
                    .get(pos..pos + 4)
                    .ok_or_else(|| ConnectorError::Offset {
                        message: format!(
                            "MultiKafkaOffset decode: slice length mismatch for entry {i} length"
                        ),
                    })?
                    .try_into()
                    .map_err(|_| ConnectorError::Offset {
                        message: format!(
                            "MultiKafkaOffset decode: slice length mismatch for entry {i} length"
                        ),
                    })?,
            ) as usize;
            pos += 4;
            if pos + item_len > bytes.len() {
                return Err(ConnectorError::Offset {
                    message: format!(
                        "MultiKafkaOffset decode: entry {i} claims {item_len} bytes but only {} remain",
                        bytes.len() - pos
                    ),
                });
            }
            let ko = KafkaOffset::decode(bytes.get(pos..pos + item_len).ok_or_else(|| {
                ConnectorError::Offset {
                    message: format!("MultiKafkaOffset decode: entry {i} slice out of range"),
                }
            })?)?;
            pos += item_len;
            offsets.push(ko);
        }
        if pos != bytes.len() {
            return Err(ConnectorError::Offset {
                message: format!(
                    "MultiKafkaOffset decode: {} trailing bytes after {count} entries",
                    bytes.len() - pos
                ),
            });
        }
        Ok(MultiKafkaOffset { offsets })
    }
}

// ---------------------------------------------------------------------------
// KafkaConfig
// ---------------------------------------------------------------------------

/// Validated Kafka connector configuration.
#[derive(Clone)]
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
    /// Typed-decode column declaration (Phase 65): a JSON array of
    /// `{"name": …, "type": …}` in the engine's landing vocabulary
    /// (`Int64`, `Float64`, `Utf8`, `Boolean`, `Timestamp`, …). When set,
    /// JSON payloads decode through arrow-json against this schema —
    /// typed columns instead of everything-as-Utf8. A payload the typed
    /// decoder cannot handle falls the WHOLE batch back to the untyped
    /// path (messages are never lost to a decode upgrade).
    pub decode_columns: Option<String>,
}

impl std::fmt::Debug for KafkaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaConfig")
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("topic", &self.topic)
            .field("group_id", &self.group_id)
            .field("auto_commit_interval_ms", &self.auto_commit_interval_ms)
            .field("security_protocol", &self.security_protocol)
            .field("ssl_ca_location", &self.ssl_ca_location)
            .field("ssl_certificate_location", &self.ssl_certificate_location)
            .field("ssl_key_location", &self.ssl_key_location)
            .field(
                "ssl_key_password",
                &self.ssl_key_password.as_ref().map(|_| "<redacted>"),
            )
            .field("sasl_username", &self.sasl_username)
            .field(
                "sasl_password",
                &self.sasl_password.as_ref().map(|_| "<redacted>"),
            )
            .field("sasl_mechanisms", &self.sasl_mechanisms)
            .field("enable_idempotence", &self.enable_idempotence)
            .field("transactional_id", &self.transactional_id)
            .field("decode_columns", &self.decode_columns.is_some())
            .finish()
    }
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
            decode_columns: config.get("decode.columns").map(|s| s.to_string()),
        })
    }

    /// Enable rdkafka auto-commit with the given interval.
    #[must_use]
    pub fn with_auto_commit(mut self, interval_ms: u64) -> Self {
        self.auto_commit_interval_ms = Some(interval_ms);
        self
    }
}

/// Render an Arrow schema into a `decode.columns` declaration —
/// the inverse of [`decode_schema_from_columns`]. Returns `None` when any
/// field has no vocabulary mapping; the caller then simply leaves typed
/// decode off and the untyped fold applies (never a hard failure).
pub fn decode_columns_from_schema(schema: &arrow::datatypes::Schema) -> Option<String> {
    use arrow::datatypes::DataType;
    let mut cols = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let type_name = match field.data_type() {
            DataType::Int8 => "Int8",
            DataType::Int16 => "Int16",
            DataType::Int32 => "Int32",
            DataType::Int64 => "Int64",
            DataType::Float32 => "Float32",
            DataType::Float64 => "Float64",
            DataType::Utf8 | DataType::LargeUtf8 => "Utf8",
            DataType::Boolean => "Boolean",
            DataType::Date32 => "Date32",
            DataType::Timestamp(_, _) => "Timestamp",
            _ => return None,
        };
        cols.push(serde_json::json!({ "name": field.name(), "type": type_name }));
    }
    serde_json::to_string(&cols).ok()
}

/// Parse a `decode.columns` declaration into an Arrow schema.
///
/// The vocabulary is the engine's landing set — the same names the DDL and
/// pipeline manifests use — mapped to Arrow types. All columns nullable:
/// a Kafka payload owes nothing, and NULL-on-absent matches the untyped
/// fold's behavior.
pub fn decode_schema_from_columns(
    json: &str,
) -> Result<std::sync::Arc<arrow::datatypes::Schema>, String> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    let cols: Vec<serde_json::Value> =
        serde_json::from_str(json).map_err(|e| format!("decode.columns is not JSON: {e}"))?;
    let mut fields = Vec::with_capacity(cols.len());
    for col in &cols {
        let name = col
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or("decode.columns entries need a name")?;
        let type_name = col
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or("decode.columns entries need a type")?;
        let data_type = match type_name {
            "Int8" => DataType::Int8,
            "Int16" => DataType::Int16,
            "Int32" => DataType::Int32,
            "Int64" => DataType::Int64,
            "Float32" => DataType::Float32,
            "Float64" => DataType::Float64,
            "Utf8" | "LargeUtf8" => DataType::Utf8,
            "Boolean" => DataType::Boolean,
            "Date32" => DataType::Date32,
            t if t.starts_with("Timestamp") => DataType::Timestamp(TimeUnit::Millisecond, None),
            other => return Err(format!("decode.columns: no mapping for type '{other}'")),
        };
        fields.push(Field::new(name, data_type, true));
    }
    Ok(std::sync::Arc::new(Schema::new(fields)))
}

// ---------------------------------------------------------------------------
// KafkaSource
// ---------------------------------------------------------------------------

/// The one message both feature-absent arms return, so the remedy is stated
/// identically wherever a lean build meets a Kafka table.
#[cfg(not(feature = "kafka"))]
const KAFKA_FEATURE_ABSENT: &str =
    "Kafka support is not compiled into this build: rebuild with the `kafka` feature";

/// A Kafka source stub when the `kafka` feature is disabled.
#[cfg(not(feature = "kafka"))]
pub struct KafkaSource {
    config: KafkaConfig,
}

#[cfg(not(feature = "kafka"))]
impl KafkaSource {
    /// Fail closed: without the `kafka` feature there is no broker client, so
    /// the error is raised at construction rather than as an empty stream at
    /// read time.
    ///
    /// The signature mirrors the rdkafka-backed arm exactly. It has to: these
    /// stubs exist so dependents compile against one API regardless of the
    /// feature, and when they silently drifted (this returned `Self`, the real
    /// one `ConnectorResult<Self>`) nothing caught it, because the whole module
    /// was gated on the feature being ON and this arm never compiled.
    pub fn new(_config: KafkaConfig) -> ConnectorResult<Self> {
        Err(ConnectorError::Unsupported {
            message: KAFKA_FEATURE_ABSENT.into(),
        })
    }

    /// Unreachable in this build: `new` never yields a value. Present so the
    /// API is identical across both arms.
    pub fn commit_current_offset(&self) {}

    /// Unreachable in this build: `new` never yields a value.
    pub fn all_current_offsets(&self) -> Vec<KafkaOffset> {
        Vec::new()
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
            message: KAFKA_FEATURE_ABSENT.into(),
        })
    }

    fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
        None
    }

    fn encoded_checkpoint_offset(&self) -> ConnectorResult<Option<Vec<u8>>> {
        Ok(None)
    }

    fn restore_encoded_checkpoint_offset(&mut self, _encoded: &[u8]) -> ConnectorResult<()> {
        Err(ConnectorError::Unsupported {
            message: KAFKA_FEATURE_ABSENT.into(),
        })
    }
}

#[cfg(not(feature = "kafka"))]
impl CheckpointSource for KafkaSource {
    type Offset = MultiKafkaOffset;

    fn checkpoint_offset(&self) -> ConnectorResult<Self::Offset> {
        Err(ConnectorError::Unsupported {
            message: KAFKA_FEATURE_ABSENT.into(),
        })
    }

    fn restore_offset(&mut self, _offset: &Self::Offset) -> ConnectorResult<()> {
        Err(ConnectorError::Unsupported {
            message: KAFKA_FEATURE_ABSENT.into(),
        })
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

    fn encoded_checkpoint_offset(&self) -> ConnectorResult<Option<Vec<u8>>> {
        CheckpointSource::encoded_checkpoint_offset(self).map(Some)
    }

    fn restore_encoded_checkpoint_offset(&mut self, encoded: &[u8]) -> ConnectorResult<()> {
        CheckpointSource::restore_encoded_offset(self, encoded)
    }
}

#[cfg(feature = "kafka")]
impl CheckpointSource for KafkaSource {
    type Offset = MultiKafkaOffset;

    fn checkpoint_offset(&self) -> ConnectorResult<Self::Offset> {
        self.inner.checkpoint_offset()
    }

    fn restore_offset(&mut self, offset: &Self::Offset) -> ConnectorResult<()> {
        self.inner.restore_offset(offset)
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
    /// Fail closed, with the same signature as the rdkafka-backed arm — see
    /// the note on `KafkaSource::new`.
    pub fn new(_config: KafkaConfig) -> ConnectorResult<Self> {
        Err(ConnectorError::Unsupported {
            message: KAFKA_FEATURE_ABSENT.into(),
        })
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
        // Serialize every row up front, then drive all deliveries CONCURRENTLY:
        // awaiting `send()` per row serializes one broker round-trip per message
        // (~tens of ms each), which made large batches unusably slow. Enqueuing
        // all records and joining their delivery futures lets them pipeline.
        let payloads: Vec<String> = (0..batch.num_rows())
            .map(|row| Self::row_to_json(&batch, row).to_string())
            .collect();
        let deliveries = payloads.iter().map(|payload| {
            let record: FutureRecord<'_, str, str> = FutureRecord::to(&topic).payload(payload);
            self.producer.send(record, Duration::from_secs(5))
        });
        futures::future::try_join_all(deliveries)
            .await
            .map_err(|(e, _)| ConnectorError::Kafka {
                message: format!("rdkafka produce failed: {e}"),
                retriable: true,
            })?;
        Ok(())
    }

    async fn flush(&mut self) -> ConnectorResult<()> {
        use rdkafka::producer::Producer;
        use std::time::Duration;
        // `Producer::flush` is synchronous and may block for up to 10s.
        // Move it to a blocking thread so the Tokio worker thread is not stalled.
        let producer = self.producer.clone();
        tokio::task::spawn_blocking(move || {
            let _ = producer.flush(Duration::from_secs(10));
        })
        .await
        .map_err(|e| ConnectorError::Kafka {
            message: format!("rdkafka flush task panicked: {e}"),
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
        let batch = self
            .batches
            .get(self.cursor)
            .ok_or_else(|| ConnectorError::Offset {
                message: "batch cursor out of range".into(),
            })?
            .clone();
        let row_count = i64::try_from(batch.num_rows()).map_err(|_| ConnectorError::Offset {
            message: "in-memory Kafka batch row count exceeds i64".into(),
        })?;
        let next_offset =
            self.next_offset
                .checked_add(row_count)
                .ok_or_else(|| ConnectorError::Offset {
                    message: "in-memory Kafka offset overflow".into(),
                })?;
        self.cursor = self.cursor.saturating_add(1);
        self.next_offset = next_offset;
        Ok(Some(batch))
    }

    fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
        Some(Box::new(self.next_offset()))
    }

    fn encoded_checkpoint_offset(&self) -> ConnectorResult<Option<Vec<u8>>> {
        CheckpointSource::encoded_checkpoint_offset(self).map(Some)
    }

    fn restore_encoded_checkpoint_offset(&mut self, encoded: &[u8]) -> ConnectorResult<()> {
        CheckpointSource::restore_encoded_offset(self, encoded)
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
/// it.  Offset commits are triggered when [`RdkafkaKafkaSource::commit_offsets`]
/// is called — typically after the downstream sink has acknowledged the data —
/// providing at-least-once delivery semantics.
///
/// Capabilities: unbounded.
#[cfg(feature = "kafka")]
pub struct RdkafkaKafkaSource {
    consumer: std::sync::Arc<rdkafka::consumer::StreamConsumer>,
    topic: String,
    group_id: String,
    /// Timeout per poll attempt in milliseconds.
    poll_timeout_ms: u64,
    /// Maximum messages folded into ONE RecordBatch per `read_batch` call
    /// (Phase 65). The first message is awaited up to `poll_timeout_ms`;
    /// the rest are drained only if already buffered client-side, so this
    /// caps batch size without ever adding latency.
    max_decode_batch: usize,
    /// Latest offset read per partition — tracks all partitions, not just the last one.
    partition_offsets: std::collections::HashMap<i32, i64>,
    /// Typed-decode schema derived from `KafkaConfig::decode_columns`
    /// (Phase 65). `None` = the untyped all-Utf8 fold.
    decode_schema: Option<std::sync::Arc<arrow::datatypes::Schema>>,
    /// Optional schema registry deserializer for Confluent-framed Avro/Protobuf payloads.
    #[cfg(feature = "schema-registry")]
    schema_registry: Option<std::sync::Arc<dyn crate::schema_registry::KafkaDeserializer>>,
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

        // A malformed declaration is a config ERROR, not a silent fallback:
        // the caller asked for typed decode and would otherwise never learn
        // it did not happen.
        let decode_schema = match kafka_cfg.and_then(|kc| kc.decode_columns.as_deref()) {
            Some(json) => Some(decode_schema_from_columns(json)?),
            None => None,
        };

        Ok(Self {
            consumer: std::sync::Arc::new(consumer),
            topic,
            group_id: group_id.as_ref().to_string(),
            decode_schema,
            poll_timeout_ms: 100,
            max_decode_batch: std::env::var("KRISHIV_KAFKA_DECODE_BATCH")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|n| *n > 0)
                .unwrap_or(512),
            partition_offsets: std::collections::HashMap::new(),
            #[cfg(feature = "schema-registry")]
            schema_registry: None,
        })
    }

    /// Attach a Confluent-compatible schema registry deserializer.
    ///
    /// When set, `read_batch()` decodes Confluent wire-format payloads (magic byte
    /// + schema ID + Avro/Protobuf bytes) instead of raw JSON parsing.
    #[cfg(feature = "schema-registry")]
    pub fn with_schema_registry(
        mut self,
        config: crate::schema_registry::SchemaRegistryConfig,
    ) -> Result<Self, String> {
        let deser = crate::schema_registry::deserializer_for(&config)
            .map_err(|e| format!("schema registry init: {e}"))?;
        self.schema_registry = Some(deser);
        Ok(self)
    }

    /// Create a source from a [`crate::cdc::KafkaCdcConfig`] (manual commit mode).
    #[cfg(all(feature = "kafka", feature = "lakehouse"))]
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
        // CONN-6: Use Async commit to avoid blocking the Tokio reactor.
        // Sync commit waits for the broker's response, stalling the thread.
        if let Err(e) = self
            .consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Async)
        {
            tracing::warn!(
                topic = %self.topic,
                error = %e,
                "rdkafka offset commit failed"
            );
        }
    }

    /// Next-to-read offset for the partition whose last-read offset is
    /// numerically highest — NOT necessarily the most recently read partition.
    /// For multi-partition topics use [`Self::all_current_offsets`] instead.
    pub fn current_kafka_offset(&self) -> Option<KafkaOffset> {
        self.partition_offsets
            .iter()
            .max_by_key(|&(_, offset)| offset)
            .map(|(&partition, &offset)| KafkaOffset {
                topic: self.topic.clone(),
                partition,
                offset: offset.saturating_add(1), // Kafka convention: committed offset = next to read
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
                offset: offset.saturating_add(1),
            })
            .collect()
    }

    /// Build ONE multi-row batch from N drained payloads (Phase 65: the
    /// DUR-2 cert found every Kafka message arriving as its own single-row
    /// batch — the per-batch fixed costs downstream dominate at high rates).
    ///
    /// Per-row semantics are IDENTICAL to the old per-message decode: a
    /// valid JSON object's values land in per-key `Utf8` columns (sorted
    /// alphabetically for schema stability, non-string values stringified),
    /// non-object payloads in a `_raw: Utf8` column so the pipeline never
    /// silently discards messages. The schema is the sorted union of the
    /// drained objects' keys (plus `_raw` if any non-object payload is
    /// present); a row leaves keys it doesn't have as NULL. A single-payload
    /// call therefore produces byte-identical schema and rows to the old
    /// per-message path.
    /// Typed decode (Phase 65's second half): the declared schema drives an
    /// arrow-json `Decoder`, so `Int64` lands as Int64 and a window's
    /// `Timestamp` column needs no downstream cast. All payloads of one
    /// fold decode together; any failure rejects the WHOLE fold so the
    /// caller can fall back untyped without losing a message.
    fn typed_payloads_to_batch(
        schema: &std::sync::Arc<arrow::datatypes::Schema>,
        payloads: &[String],
    ) -> crate::ConnectorResult<arrow::record_batch::RecordBatch> {
        let mut decoder = arrow::json::reader::ReaderBuilder::new(std::sync::Arc::clone(schema))
            .build_decoder()
            .map_err(|e| crate::ConnectorError::Config {
                message: format!("typed kafka decoder: {e}"),
            })?;
        let ndjson = payloads.join("\n");
        let consumed =
            decoder
                .decode(ndjson.as_bytes())
                .map_err(|e| crate::ConnectorError::Config {
                    message: format!("typed kafka decode: {e}"),
                })?;
        if consumed < ndjson.len() {
            return Err(crate::ConnectorError::Config {
                message: "typed kafka decode: trailing undecodable payload".to_string(),
            });
        }
        let batch = decoder
            .flush()
            .map_err(|e| crate::ConnectorError::Config {
                message: format!("typed kafka decode flush: {e}"),
            })?
            .ok_or_else(|| crate::ConnectorError::Config {
                message: "typed kafka decode produced no rows".to_string(),
            })?;
        if batch.num_rows() != payloads.len() {
            // A dropped row here would vanish silently INSIDE a fold whose
            // offsets are already recorded — the exact shape of loss the
            // fold was built to avoid. Reject and let untyped keep it.
            return Err(crate::ConnectorError::Config {
                message: format!(
                    "typed kafka decode kept {} of {} payloads",
                    batch.num_rows(),
                    payloads.len()
                ),
            });
        }
        Ok(batch)
    }

    fn payloads_to_batch(
        payloads: &[String],
    ) -> crate::ConnectorResult<arrow::record_batch::RecordBatch> {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        // Parse once: Some(object) rows contribute keyed columns, None rows
        // contribute `_raw`. JSON arrays/scalars stay `_raw` — same as the
        // single-message path always treated them.
        let parsed: Vec<Option<serde_json::Map<String, serde_json::Value>>> = payloads
            .iter()
            .map(|p| match serde_json::from_str::<serde_json::Value>(p) {
                Ok(serde_json::Value::Object(map)) => Some(map),
                _ => None,
            })
            .collect();

        let mut keys: Vec<&str> = parsed
            .iter()
            .flatten()
            .flat_map(|map| map.keys().map(String::as_str))
            .collect::<std::collections::BTreeSet<&str>>()
            .into_iter()
            .collect();
        let any_raw = parsed.iter().any(Option::is_none);
        if any_raw {
            keys.insert(0, "_raw");
        }

        let fields: Vec<Field> = keys
            .iter()
            .map(|k| Field::new(*k, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let columns: Vec<Arc<dyn arrow::array::Array>> = keys
            .iter()
            .map(|k| {
                let arr: StringArray = parsed
                    .iter()
                    .zip(payloads)
                    .map(|(row, payload)| match row {
                        Some(map) => match map.get(*k) {
                            None | Some(serde_json::Value::Null) => None,
                            Some(serde_json::Value::String(s)) => Some(s.clone()),
                            Some(other) => Some(other.to_string()),
                        },
                        None => (*k == "_raw").then(|| payload.clone()),
                    })
                    .collect();
                Arc::new(arr) as Arc<dyn arrow::array::Array>
            })
            .collect();

        arrow::record_batch::RecordBatch::try_new(schema, columns).map_err(|e| {
            crate::ConnectorError::Schema {
                message: format!("failed to build batch from Kafka payloads: {e}"),
            }
        })
    }

    /// Merge offsets staged during a `read_batch` drain into the
    /// checkpointable per-partition map — but only when the drained payloads
    /// were successfully decoded into a batch. On an error the staged offsets
    /// are dropped, so a later `commit_offsets`/checkpoint cannot advance
    /// past messages that were never delivered downstream.
    fn commit_staged_on_success<T>(
        result: crate::ConnectorResult<T>,
        staged: std::collections::HashMap<i32, i64>,
        partition_offsets: &mut std::collections::HashMap<i32, i64>,
    ) -> crate::ConnectorResult<T> {
        if result.is_ok() {
            partition_offsets.extend(staged);
        }
        result
    }
}

#[cfg(feature = "kafka")]
impl Source for RdkafkaKafkaSource {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new()
            .with_unbounded()
            .with_checkpoint()
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
                // C6: Track offset per partition. Offsets are STAGED locally
                // during the drain and merged into `partition_offsets` only
                // once a batch has been successfully built — an error path
                // must not advance checkpoint offsets past messages that were
                // never delivered downstream. Log when a new partition is
                // first assigned — this happens after a consumer group rebalance
                // and surfaces which partitions each executor instance owns.
                let mut staged_offsets: std::collections::HashMap<i32, i64> =
                    std::collections::HashMap::new();
                let new_partition = !self.partition_offsets.contains_key(&msg.partition());
                staged_offsets.insert(msg.partition(), msg.offset());
                if new_partition {
                    tracing::info!(
                        topic = %self.topic,
                        partition = msg.partition(),
                        "Kafka partition assigned to this consumer via group rebalance"
                    );
                }

                // Schema registry path: decode Confluent wire-format payloads.
                #[cfg(feature = "schema-registry")]
                if let Some(ref deser) = self.schema_registry {
                    let raw_bytes = msg.payload().unwrap_or(&[]);
                    if raw_bytes.is_empty() {
                        self.partition_offsets.extend(staged_offsets);
                        return Ok(Some(arrow::record_batch::RecordBatch::new_empty(
                            std::sync::Arc::new(arrow::datatypes::Schema::empty()),
                        )));
                    }
                    // A decode failure returns before the staged offset is
                    // merged, so a later commit cannot skip this message.
                    let (_schema, batches) = deser.decode(raw_bytes).await.map_err(|e| {
                        crate::ConnectorError::Schema {
                            message: format!("schema registry decode: {e}"),
                        }
                    })?;
                    let Some(first) = batches.first() else {
                        self.partition_offsets.extend(staged_offsets);
                        return Ok(Some(arrow::record_batch::RecordBatch::new_empty(
                            std::sync::Arc::new(arrow::datatypes::Schema::empty()),
                        )));
                    };
                    if batches.len() == 1 {
                        self.partition_offsets.extend(staged_offsets);
                        return Ok(batches.into_iter().next());
                    }
                    let schema = first.schema();
                    let concatenated =
                        arrow::compute::concat_batches(&schema, &batches).map_err(|e| {
                            crate::ConnectorError::Schema {
                                message: format!("schema registry batch concat: {e}"),
                            }
                        })?;
                    self.partition_offsets.extend(staged_offsets);
                    return Ok(Some(concatenated));
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
                        self.partition_offsets.extend(staged_offsets);
                        return Ok(Some(arrow::record_batch::RecordBatch::new_empty(
                            std::sync::Arc::new(arrow::datatypes::Schema::empty()),
                        )));
                    }
                };

                // Phase 65: fold every message rdkafka has ALREADY buffered
                // client-side into this batch (up to max_decode_batch) instead
                // of returning one single-row batch per message — the shape
                // the DUR-2 cert flagged. `now_or_never` polls the recv
                // future exactly once, so a not-yet-buffered message never
                // adds wait time; rdkafka's recv is cancel-safe, so dropping
                // the pending future loses nothing. Offsets are recorded for
                // exactly the messages folded into the returned batch, which
                // keeps checkpoint offsets aligned with delivered rows.
                let mut payloads: Vec<String> = vec![payload.to_owned()];
                let mut partition = msg.partition();
                let mut current = msg.offset();
                while payloads.len() < self.max_decode_batch {
                    use futures::FutureExt;
                    match self.consumer.recv().now_or_never() {
                        Some(Ok(next)) => {
                            let new_partition =
                                !self.partition_offsets.contains_key(&next.partition())
                                    && !staged_offsets.contains_key(&next.partition());
                            staged_offsets.insert(next.partition(), next.offset());
                            if new_partition {
                                tracing::info!(
                                    topic = %self.topic,
                                    partition = next.partition(),
                                    "Kafka partition assigned to this consumer via group rebalance"
                                );
                            }
                            partition = next.partition();
                            current = next.offset();
                            match next.payload_view::<str>() {
                                Some(Ok(s)) => payloads.push(s.to_owned()),
                                Some(Err(_)) | None => {
                                    tracing::warn!(
                                        topic = %self.topic,
                                        partition = next.partition(),
                                        offset = next.offset(),
                                        "skipping unreadable Kafka message in drain"
                                    );
                                }
                            }
                        }
                        Some(Err(e)) => {
                            // Deliver what we have; the error resurfaces on
                            // the next read_batch call if it persists.
                            tracing::warn!(
                                topic = %self.topic,
                                error = %e,
                                "rdkafka receive error while draining buffered messages"
                            );
                            break;
                        }
                        None => break,
                    }
                }

                let decoded = match &self.decode_schema {
                    Some(schema) => match Self::typed_payloads_to_batch(schema, &payloads) {
                        Ok(batch) => Ok(batch),
                        Err(e) => {
                            // Never lose messages to a decode upgrade: the
                            // untyped fold accepts anything, and the warn
                            // names why the typed path declined.
                            tracing::warn!(
                                topic = %self.topic,
                                error = %e,
                                "typed kafka decode fell back to untyped for this batch"
                            );
                            Self::payloads_to_batch(&payloads)
                        }
                    },
                    None => Self::payloads_to_batch(&payloads),
                };
                let batch = Self::commit_staged_on_success(
                    decoded,
                    staged_offsets,
                    &mut self.partition_offsets,
                )?;
                // Record per-partition consumer lag (high_watermark - current_offset)
                // against the LAST drained message.
                let topic = self.topic.clone();
                let group_id = self.group_id.clone();
                let consumer = std::sync::Arc::clone(&self.consumer);
                tokio::task::spawn_blocking(move || {
                    use rdkafka::consumer::Consumer;
                    if let Ok((_, high)) = consumer.fetch_watermarks(
                        &topic,
                        partition,
                        std::time::Duration::from_millis(50),
                    ) {
                        let lag = high.saturating_sub(current.saturating_add(1));
                        let source_id = format!("{topic}:{partition}");
                        krishiv_metrics::global_metrics()
                            .set_source_offset_lag(&group_id, &source_id, lag);
                    }
                });
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

    fn encoded_checkpoint_offset(&self) -> ConnectorResult<Option<Vec<u8>>> {
        CheckpointSource::encoded_checkpoint_offset(self).map(Some)
    }

    fn restore_encoded_checkpoint_offset(&mut self, encoded: &[u8]) -> ConnectorResult<()> {
        CheckpointSource::restore_encoded_offset(self, encoded)
    }
}

/// Broker-backed checkpoint support via seek within the current assignment.
///
/// On restore, each stored partition is `seek()`-ed to its stored offset, so
/// the next `read_batch()` call for that partition resumes exactly there —
/// **for as long as the current partition assignment holds**. The guarantee
/// is per-assignment-epoch only: the consumer stays subscribed through the
/// group rebalance protocol, so a rebalance can reassign partitions and
/// revert their positions to the group's committed offsets, replaying or
/// skipping messages relative to the restored checkpoint. Restore refuses to
/// seek unless every stored partition is currently assigned (all-or-nothing),
/// so a partial restore can never leave some partitions repositioned and
/// others not.
#[cfg(feature = "kafka")]
impl CheckpointSource for RdkafkaKafkaSource {
    type Offset = MultiKafkaOffset;

    fn checkpoint_offset(&self) -> ConnectorResult<Self::Offset> {
        Ok(MultiKafkaOffset {
            offsets: self.all_current_offsets(),
        })
    }

    fn restore_offset(&mut self, offset: &Self::Offset) -> ConnectorResult<()> {
        use rdkafka::consumer::Consumer;
        use rdkafka::topic_partition_list::Offset as RdOffset;

        if offset.offsets.is_empty() {
            return Ok(());
        }

        // CONN-3: Use seek() instead of assign() to restore offsets.
        // assign() unsubscribes from the consumer group and switches to manual
        // assignment mode, permanently breaking rebalancing. seek() positions
        // the consumer at a specific offset while keeping the existing
        // subscription intact.
        //
        // All-or-nothing: validate that every stored partition is currently
        // assigned BEFORE seeking any. Seeking a not-yet-assigned partition
        // fails mid-loop, which would leave some partitions repositioned and
        // others not, with no state mutated to record the difference.
        let assignment = self
            .consumer
            .assignment()
            .map_err(|e| ConnectorError::Offset {
                message: format!("restore: failed to fetch current assignment: {e}"),
            })?;
        let assigned: std::collections::HashSet<(String, i32)> = assignment
            .elements()
            .iter()
            .map(|el| (el.topic().to_string(), el.partition()))
            .collect();
        let missing: Vec<String> = offset
            .offsets
            .iter()
            .filter(|ko| !assigned.contains(&(ko.topic.clone(), ko.partition)))
            .map(|ko| format!("{}/{}", ko.topic, ko.partition))
            .collect();
        if !missing.is_empty() {
            return Err(ConnectorError::Offset {
                message: format!(
                    "restore: partition(s) not currently assigned to this consumer: {}; \
                     refusing to seek any partition (all-or-nothing restore)",
                    missing.join(", ")
                ),
            });
        }

        for ko in &offset.offsets {
            self.consumer
                .seek(
                    &ko.topic,
                    ko.partition,
                    RdOffset::Offset(ko.offset),
                    std::time::Duration::from_secs(5),
                )
                .map_err(|e| ConnectorError::Offset {
                    message: format!(
                        "restore: failed to seek partition {}/{} at offset {}: {e}",
                        ko.topic, ko.partition, ko.offset
                    ),
                })?;
        }

        // Rebuild partition_offsets so that checkpoint_offset() is accurate
        // immediately after restore (before any new messages are read).
        // partition_offsets stores the raw rdkafka message offset of the last
        // received message. all_current_offsets() returns offset+1 ("next to read").
        // ko.offset is "next to read", so last_received = ko.offset - 1.
        self.partition_offsets.clear();
        for ko in &offset.offsets {
            if ko.offset > 0 {
                self.partition_offsets.insert(ko.partition, ko.offset - 1);
            }
            // offset == 0 means start-of-partition; no message yet for this
            // partition, so we leave it absent from partition_offsets.
        }

        Ok(())
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
            decode_columns: None,
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
    // MultiKafkaOffset
    // -----------------------------------------------------------------------

    #[cfg(feature = "kafka")]
    mod payload_batching {
        use super::super::RdkafkaKafkaSource;
        use arrow::array::{Array, StringArray};

        fn col<'a>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a StringArray {
            batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("column {name} missing"))
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 column")
        }

        #[test]
        fn single_payload_matches_legacy_single_message_shape() {
            let single =
                RdkafkaKafkaSource::payloads_to_batch(&[r#"{"b":"2","a":"1"}"#.to_owned()])
                    .unwrap();
            assert_eq!(
                single
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>(),
                vec!["a", "b"],
                "keys sorted for schema stability, exactly the legacy shape"
            );
            assert_eq!(single.num_rows(), 1);
            assert_eq!(col(&single, "a").value(0), "1");
            assert_eq!(col(&single, "b").value(0), "2");
        }

        #[test]
        fn drained_payloads_fold_into_one_multi_row_batch() {
            let batch = RdkafkaKafkaSource::payloads_to_batch(&[
                r#"{"user":"u1","n":1}"#.to_owned(),
                r#"{"user":"u2","n":2}"#.to_owned(),
                r#"{"user":"u3","n":3}"#.to_owned(),
            ])
            .unwrap();
            assert_eq!(batch.num_rows(), 3, "one batch, one row per message");
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(col(&batch, "user").value(2), "u3");
            assert_eq!(
                col(&batch, "n").value(0),
                "1",
                "non-string JSON stringified as before"
            );
        }

        #[test]
        fn heterogeneous_keys_union_with_nulls() {
            let batch = RdkafkaKafkaSource::payloads_to_batch(&[
                r#"{"a":"1"}"#.to_owned(),
                r#"{"b":"2"}"#.to_owned(),
            ])
            .unwrap();
            assert_eq!(batch.num_rows(), 2);
            assert!(col(&batch, "a").is_null(1), "row 2 has no key a");
            assert!(col(&batch, "b").is_null(0), "row 1 has no key b");
            assert_eq!(col(&batch, "b").value(1), "2");
        }

        #[test]
        fn mixed_json_and_raw_payloads_keep_both() {
            let batch = RdkafkaKafkaSource::payloads_to_batch(&[
                r#"{"a":"1"}"#.to_owned(),
                "not json".to_owned(),
            ])
            .unwrap();
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(col(&batch, "a").value(0), "1");
            assert!(col(&batch, "_raw").is_null(0));
            assert_eq!(col(&batch, "_raw").value(1), "not json");
            assert!(col(&batch, "a").is_null(1));
        }

        #[test]
        fn raw_only_payload_matches_legacy_raw_shape() {
            let batch = RdkafkaKafkaSource::payloads_to_batch(&["plain".to_owned()]).unwrap();
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(col(&batch, "_raw").value(0), "plain");
        }
    }

    #[test]
    fn multi_kafka_offset_empty_roundtrip() {
        let original = MultiKafkaOffset::default();
        let encoded = crate::Offset::encode(&original);
        let decoded = MultiKafkaOffset::decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn multi_kafka_offset_single_partition_roundtrip() {
        let original = MultiKafkaOffset::new(vec![KafkaOffset {
            topic: "events".into(),
            partition: 0,
            offset: 100,
        }]);
        let encoded = crate::Offset::encode(&original);
        let decoded = MultiKafkaOffset::decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn multi_kafka_offset_multi_partition_roundtrip() {
        let original = MultiKafkaOffset::new(vec![
            KafkaOffset {
                topic: "orders".into(),
                partition: 0,
                offset: 42,
            },
            KafkaOffset {
                topic: "orders".into(),
                partition: 1,
                offset: 7,
            },
            KafkaOffset {
                topic: "orders".into(),
                partition: 2,
                offset: 999,
            },
        ]);
        let encoded = crate::Offset::encode(&original);
        let decoded = MultiKafkaOffset::decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn multi_kafka_offset_decode_rejects_trailing_bytes() {
        let original = MultiKafkaOffset::new(vec![KafkaOffset {
            topic: "t".into(),
            partition: 0,
            offset: 1,
        }]);
        let mut encoded = crate::Offset::encode(&original);
        encoded.push(0xFF);
        let err = MultiKafkaOffset::decode(&encoded).expect_err("trailing bytes must be rejected");
        assert!(matches!(err, ConnectorError::Offset { .. }));
    }

    #[test]
    fn multi_kafka_offset_decode_rejects_truncated_entry() {
        let original = MultiKafkaOffset::new(vec![KafkaOffset {
            topic: "events".into(),
            partition: 0,
            offset: 5,
        }]);
        let encoded = crate::Offset::encode(&original);
        // Truncate to just the count + partial entry length
        let truncated = &encoded[..6];
        let err =
            MultiKafkaOffset::decode(truncated).expect_err("truncated entry must be rejected");
        assert!(matches!(err, ConnectorError::Offset { .. }));
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

    #[cfg(feature = "kafka")]
    #[tokio::test]
    async fn kafka_source_reports_unbounded_and_rewindable() {
        let config = test_kafka_config();
        let source = KafkaSource::new(config).expect("kafka source");
        let caps = source.capabilities();
        assert!(caps.is_unbounded());
        assert!(!caps.is_rewindable());
        assert!(!caps.is_bounded());
        assert!(!caps.is_transactional());
        assert!(!caps.is_idempotent());
        assert!(
            caps.is_checkpoint_capable(),
            "broker Kafka source must advertise checkpoint capability now that seek-based restore is implemented"
        );
    }

    /// Without the feature there is no broker client, so construction is where
    /// the build says so — not a source that silently yields nothing.
    #[cfg(not(feature = "kafka"))]
    #[test]
    fn kafka_source_construction_fails_closed_without_the_feature() {
        let err = KafkaSource::new(test_kafka_config())
            .err()
            .expect("stub KafkaSource::new must fail, not hand back a dead source");
        match err {
            ConnectorError::Unsupported { message } => assert!(
                message.contains("`kafka` feature"),
                "the error must name the feature to rebuild with, got: {message}"
            ),
            other => panic!("expected Unsupported, got: {other}"),
        }
    }

    // -----------------------------------------------------------------------
    // KafkaSink capabilities
    // -----------------------------------------------------------------------

    #[test]
    #[cfg(not(feature = "kafka"))]
    fn kafka_sink_construction_fails_closed_without_the_feature() {
        let err = KafkaSink::new(test_kafka_config())
            .err()
            .expect("stub KafkaSink::new must fail, not hand back a dead sink");
        match err {
            ConnectorError::Unsupported { message } => assert!(
                message.contains("`kafka` feature"),
                "the error must name the feature to rebuild with, got: {message}"
            ),
            other => panic!("expected Unsupported, got: {other}"),
        }
    }

    // -----------------------------------------------------------------------
    // Unsupported stubs
    // -----------------------------------------------------------------------

    /// The two arms must expose *one* API. When they drifted — this arm's
    /// `new` returning `Self` while the rdkafka arm returned
    /// `ConnectorResult<Self>` — nothing noticed, because `pub mod kafka` was
    /// itself gated on the feature and this code had never been compiled.
    /// Binding both constructors to the same signature is what makes the drift
    /// a build failure.
    #[cfg(not(feature = "kafka"))]
    #[test]
    fn stub_constructors_have_the_same_signature_as_the_rdkafka_arm() {
        let _source: fn(KafkaConfig) -> ConnectorResult<KafkaSource> = KafkaSource::new;
        let _sink: fn(KafkaConfig) -> ConnectorResult<KafkaSink> = KafkaSink::new;
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

    /// `krishiv-sql` compiles its Kafka table provider against this module
    /// unconditionally. If the stub arm ever stops satisfying `Source` /
    /// `Sink` / `CheckpointSource`, a lean build breaks in krishiv-sql with a
    /// confusing error instead of here.
    #[cfg(not(feature = "kafka"))]
    #[test]
    fn stub_arms_still_satisfy_the_connector_traits() {
        fn assert_source<T: crate::Source>() {}
        fn assert_sink<T: crate::Sink>() {}
        fn assert_checkpoint<T: crate::CheckpointSource>() {}
        assert_source::<KafkaSource>();
        assert_sink::<KafkaSink>();
        assert_checkpoint::<KafkaSource>();
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
        // depending on platform. Assert the contract on whichever branch we
        // got: a constructed source advertises unbounded + checkpoint
        // capabilities, and a failure carries a non-empty diagnostic.
        match result {
            Ok(source) => {
                let caps = crate::Source::capabilities(&source);
                assert!(caps.is_unbounded(), "broker source must be unbounded");
                assert!(
                    caps.is_checkpoint_capable(),
                    "broker source must advertise checkpoint capability"
                );
            }
            Err(message) => assert!(
                !message.is_empty(),
                "constructor failure must carry a descriptive error"
            ),
        }
    }

    /// Offsets staged during a `read_batch` drain must be dropped when the
    /// decode fails: an error return means the messages were never delivered,
    /// so a later `commit_offsets`/checkpoint must not advance past them.
    #[cfg(feature = "kafka")]
    #[test]
    fn staged_offsets_are_not_merged_when_decode_fails() {
        use std::collections::HashMap;
        let mut partition_offsets: HashMap<i32, i64> = HashMap::from([(0, 5)]);
        let staged: HashMap<i32, i64> = HashMap::from([(0, 9), (1, 3)]);

        let err: crate::ConnectorResult<()> = Err(crate::ConnectorError::Schema {
            message: "decode failed".into(),
        });
        let result = super::RdkafkaKafkaSource::commit_staged_on_success(
            err,
            staged,
            &mut partition_offsets,
        );
        assert!(result.is_err(), "the decode error must propagate");
        assert_eq!(
            partition_offsets,
            HashMap::from([(0, 5)]),
            "a failed decode must leave checkpoint offsets untouched"
        );
    }

    /// Offsets staged during a successful drain are merged, keeping
    /// checkpoint offsets aligned with delivered rows.
    #[cfg(feature = "kafka")]
    #[test]
    fn staged_offsets_are_merged_on_successful_decode() {
        use std::collections::HashMap;
        let mut partition_offsets: HashMap<i32, i64> = HashMap::from([(0, 5)]);
        let staged: HashMap<i32, i64> = HashMap::from([(0, 9), (1, 3)]);

        let result = super::RdkafkaKafkaSource::commit_staged_on_success(
            Ok(()),
            staged,
            &mut partition_offsets,
        );
        assert!(result.is_ok());
        assert_eq!(partition_offsets, HashMap::from([(0, 9), (1, 3)]));
    }

    /// `restore_offset` must refuse to seek when a stored partition is not in
    /// the current assignment — all-or-nothing, with no state mutated. A fresh
    /// consumer with no broker connection has an empty assignment, so every
    /// stored partition is "missing".
    #[cfg(feature = "kafka")]
    #[tokio::test]
    async fn restore_offset_rejects_unassigned_partitions_without_mutating_state() {
        let Ok(mut source) =
            super::RdkafkaKafkaSource::new("localhost:1", "test-group", "events", None, None)
        else {
            // Constructor behaviour is platform-dependent without a broker;
            // the validation path needs a constructed consumer.
            return;
        };
        let stored = super::MultiKafkaOffset {
            offsets: vec![super::KafkaOffset {
                topic: "events".into(),
                partition: 0,
                offset: 42,
            }],
        };
        let err = crate::CheckpointSource::restore_offset(&mut source, &stored)
            .expect_err("unassigned partition must be rejected");
        let message = err.to_string();
        assert!(
            message.contains("not currently assigned") && message.contains("events/0"),
            "error must list the missing partition(s), got: {message}"
        );
        assert!(
            source.all_current_offsets().is_empty(),
            "a refused restore must not mutate partition offsets"
        );
    }

    /// `RdkafkaKafkaSource` implements `Source`.
    #[cfg(feature = "kafka")]
    #[test]
    fn rdkafka_kafka_source_implements_source_trait() {
        fn assert_source<T: crate::Source>() {}
        assert_source::<super::RdkafkaKafkaSource>();
    }

    /// `RdkafkaKafkaSource` implements `CheckpointSource`.
    #[cfg(feature = "kafka")]
    #[test]
    fn rdkafka_kafka_source_implements_checkpoint_source_trait() {
        fn assert_checkpoint<T: crate::CheckpointSource>() {}
        assert_checkpoint::<super::RdkafkaKafkaSource>();
    }

    /// `KafkaSource` (broker wrapper) implements `CheckpointSource`.
    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_source_wrapper_implements_checkpoint_source_trait() {
        fn assert_checkpoint<T: crate::CheckpointSource>() {}
        assert_checkpoint::<super::KafkaSource>();
    }

    /// Both feature-absent arms must state the same remedy, so a lean build
    /// gives one message wherever it meets Kafka.
    #[cfg(not(feature = "kafka"))]
    #[test]
    fn both_stub_constructors_name_the_same_remedy() {
        let source_err = KafkaSource::new(test_kafka_config())
            .err()
            .expect("source must fail")
            .to_string();
        let sink_err = KafkaSink::new(test_kafka_config())
            .err()
            .expect("sink must fail")
            .to_string();
        assert_eq!(source_err, sink_err);
        assert!(
            source_err.contains("rebuild with the `kafka` feature"),
            "got: {source_err}"
        );
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

    /// Phase 65 typed decode: the declared schema drives arrow-json, so a
    /// declared Int64 IS an Int64 downstream — no everything-as-Utf8.
    #[cfg(feature = "kafka")]
    #[test]
    fn typed_decode_lands_typed_columns_and_round_trips_the_declaration() {
        use arrow::array::{Float64Array, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};

        let declared = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, true),
        ]);
        // The DDL side renders the declaration…
        let cols = decode_columns_from_schema(&declared).expect("mappable schema");
        // …and the consumer side parses it back — one vocabulary.
        let schema = decode_schema_from_columns(&cols).unwrap();

        let payloads = vec![
            r#"{"id": 1, "amount": 10.5}"#.to_string(),
            r#"{"id": 2}"#.to_string(),
        ];
        let batch = RdkafkaKafkaSource::typed_payloads_to_batch(&schema, &payloads).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id must be a REAL Int64 column")
            .clone();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        let amounts = batch
            .column(batch.schema().index_of("amount").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("amount must be a REAL Float64 column")
            .clone();
        assert_eq!(amounts.value(0), 10.5);
        use arrow::array::Array as _;
        assert!(
            amounts.is_null(1),
            "absent key -> NULL, same as the untyped fold"
        );
    }

    /// A payload the typed decoder cannot handle rejects the WHOLE fold —
    /// the read path then falls back to the untyped path, so no message is
    /// ever lost to the decode upgrade.
    #[cfg(feature = "kafka")]
    #[test]
    fn typed_decode_rejects_the_fold_rather_than_dropping_a_payload() {
        use arrow::datatypes::{DataType, Field, Schema};
        let schema =
            std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let payloads = vec![
            r#"{"id": 1}"#.to_string(),
            "not json at all".to_string(),
            r#"{"id": 3}"#.to_string(),
        ];
        RdkafkaKafkaSource::typed_payloads_to_batch(&schema, &payloads)
            .expect_err("a malformed payload must reject the fold, not vanish");
        // And the fallback accepts exactly the same payloads.
        let untyped = RdkafkaKafkaSource::payloads_to_batch(&payloads).unwrap();
        assert_eq!(
            untyped.num_rows(),
            3,
            "the untyped fold keeps every message"
        );
    }

    /// An unmappable declared field means NO typed declaration (untyped
    /// fold applies) — never a hard failure at registration.
    #[test]
    fn unmappable_declared_fields_leave_typed_decode_off() {
        use arrow::datatypes::{DataType, Field, Schema};
        let exotic = Schema::new(vec![Field::new("blob", DataType::Binary, true)]);
        assert!(decode_columns_from_schema(&exotic).is_none());
    }
}
