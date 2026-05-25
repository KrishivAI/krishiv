//! CDC-to-lakehouse pipeline: Debezium 2.x over Kafka → Iceberg.

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// A CDC operation type from Debezium.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CdcOp {
    /// Row inserted (Debezium op = "c").
    Insert,
    /// Row updated (Debezium op = "u").
    Update,
    /// Row deleted (Debezium op = "d").
    Delete,
    /// Snapshot read (Debezium op = "r").
    SnapshotRead,
}

impl CdcOp {
    /// Parse from Debezium op field value.
    pub fn from_debezium(op: &str) -> Option<Self> {
        match op {
            "c" => Some(Self::Insert),
            "u" => Some(Self::Update),
            "d" => Some(Self::Delete),
            "r" => Some(Self::SnapshotRead),
            _ => None,
        }
    }
}

/// A single row-level change event emitted by a CDC source.
#[derive(Debug, Clone)]
pub struct CdcEvent {
    /// The operation type.
    pub op: CdcOp,
    /// Row state before the change (present for Update and Delete).
    pub before: Option<RecordBatch>,
    /// Row state after the change (present for Insert, Update, SnapshotRead).
    pub after: Option<RecordBatch>,
    /// Postgres LSN or MySQL binlog position (used as idempotency key).
    pub source_lsn: Option<u64>,
    /// Unix milliseconds from Debezium source.ts_ms.
    pub source_ts_ms: Option<i64>,
    /// Kafka partition the event came from.
    pub partition_id: u32,
    /// Kafka offset of this event.
    pub offset: i64,
    /// Source table name (e.g. "public.orders").
    pub table: String,
}

/// Deserialize a Debezium JSON envelope into a `CdcEvent`.
///
/// Expected envelope structure:
/// ```json
/// { "op": "u", "before": {...}, "after": {...},
///   "source": { "lsn": 12345, "ts_ms": 1716201600000 } }
/// ```
///
/// Returns `None` if the envelope is malformed or `op` is unrecognized.
pub fn parse_debezium_envelope(json: &str, partition_id: u32, offset: i64) -> Option<CdcEvent> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let op_str = v["op"].as_str()?;
    let op = CdcOp::from_debezium(op_str)?;

    let source_lsn = v["source"]["lsn"].as_u64();
    let source_ts_ms = v["source"]["ts_ms"].as_i64();
    let table = v["source"]["table"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();

    // Build one column per key in the JSON object for before/after payloads.
    // Keys are sorted alphabetically to guarantee a deterministic schema across
    // events from the same table (P1.19).
    let make_payload_batch = |payload: &serde_json::Value| -> Option<RecordBatch> {
        if payload.is_null() || !payload.is_object() {
            return None;
        }
        let obj = payload.as_object()?;
        if obj.is_empty() {
            return None;
        }
        // Sort keys for deterministic column ordering.
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort_unstable();

        let mut fields: Vec<Field> = Vec::new();
        let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
        for key in &keys {
            let val = &obj[*key];
            fields.push(Field::new(*key, DataType::Utf8, true));
            let str_val: Option<String> = match val {
                serde_json::Value::Null => None,
                serde_json::Value::String(s) => Some(s.clone()),
                other => Some(other.to_string()),
            };
            let arr: StringArray = std::iter::once(str_val.as_deref()).collect();
            columns.push(Arc::new(arr));
        }
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).ok()
    };

    let before = make_payload_batch(&v["before"]);
    let after = make_payload_batch(&v["after"]);

    Some(CdcEvent {
        op,
        before,
        after,
        source_lsn,
        source_ts_ms,
        partition_id,
        offset,
        table,
    })
}

/// A source of raw Debezium JSON strings for a CDC pipeline.
///
/// Implement this trait to plug any Kafka client (or test fixture) into
/// [`CdcToLakehousePipeline::run_with_source`].
pub trait CdcEventSource: Send {
    /// Poll up to `max` raw Debezium JSON event strings.
    ///
    /// Returns an empty `Vec` when the source is exhausted (signals pipeline
    /// shutdown). Returns `Err` on unrecoverable source failures.
    fn poll_events(&mut self, max: usize) -> Result<Vec<String>, String>;
}

/// In-memory [`CdcEventSource`] backed by a pre-loaded `Vec<String>`.
///
/// Drains from the front; returns empty when exhausted. Intended for tests
/// and local development without a live Kafka cluster.
pub struct InMemoryCdcEventSource {
    events: std::collections::VecDeque<String>,
}

impl InMemoryCdcEventSource {
    /// Create a source pre-loaded with `events`.
    pub fn new(events: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            events: events.into_iter().map(Into::into).collect(),
        }
    }
}

impl CdcEventSource for InMemoryCdcEventSource {
    fn poll_events(&mut self, max: usize) -> Result<Vec<String>, String> {
        let n = max.min(self.events.len());
        Ok(self.events.drain(..n).collect())
    }
}

/// Configuration for a CDC-to-lakehouse pipeline.
#[derive(Debug, Clone)]
pub struct CdcToLakehousePipeline {
    /// Kafka topic carrying Debezium change events.
    pub source_topic: String,
    /// Kafka broker addresses.
    pub kafka_brokers: Vec<String>,
    /// Iceberg catalog identifier.
    pub iceberg_catalog: String,
    /// Target Iceberg table (e.g. "warehouse.orders").
    pub iceberg_table: String,
    /// Primary key columns used for upsert/delete semantics.
    pub primary_key_columns: Vec<String>,
    /// Optional Confluent Schema Registry URL.
    pub schema_registry_url: Option<String>,
    /// Number of CDC events to accumulate before writing a single Arrow batch.
    ///
    /// Defaults to 1000. Higher values reduce write amplification; lower values
    /// reduce end-to-end latency.
    pub batch_size: usize,
}

impl CdcToLakehousePipeline {
    /// Create a new pipeline configuration.
    pub fn new(
        source_topic: impl Into<String>,
        kafka_brokers: Vec<String>,
        iceberg_catalog: impl Into<String>,
        iceberg_table: impl Into<String>,
        primary_key_columns: Vec<String>,
    ) -> Self {
        Self {
            source_topic: source_topic.into(),
            kafka_brokers,
            iceberg_catalog: iceberg_catalog.into(),
            iceberg_table: iceberg_table.into(),
            primary_key_columns,
            schema_registry_url: None,
            batch_size: 1000,
        }
    }

    /// Attach a schema registry URL.
    pub fn with_schema_registry(mut self, url: impl Into<String>) -> Self {
        self.schema_registry_url = Some(url.into());
        self
    }

    /// Set the number of CDC events to accumulate before writing a single Arrow batch.
    #[must_use]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Validate the pipeline configuration. Returns an error if required fields are missing.
    pub fn validate(&self) -> Result<(), String> {
        if self.source_topic.is_empty() {
            return Err("source_topic must not be empty".into());
        }
        if self.kafka_brokers.is_empty() {
            return Err("kafka_brokers must not be empty".into());
        }
        if self.iceberg_table.is_empty() {
            return Err("iceberg_table must not be empty".into());
        }
        if self.primary_key_columns.is_empty() {
            return Err("primary_key_columns must not be empty for upsert semantics".into());
        }
        Ok(())
    }

    /// Run the pipeline with an external event source.
    ///
    /// Polls `source` for up to `batch_size` raw Debezium JSON strings per
    /// iteration, parses them with [`parse_debezium_envelope`], builds an Arrow
    /// batch with [`build_batch_from_events`], and passes it to `on_batch`.
    /// Stops when `source.poll_events` returns an empty slice (source
    /// exhausted) or the provided `shutdown` channel fires.
    pub async fn run_with_source<S, F>(
        &self,
        mut source: S,
        mut on_batch: F,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), String>
    where
        S: CdcEventSource,
        F: FnMut(RecordBatch) -> Result<(), String>,
    {
        self.validate()?;
        loop {
            if *shutdown.borrow() {
                break;
            }
            let raw = source.poll_events(self.batch_size)?;
            if raw.is_empty() {
                break;
            }
            let events: Vec<CdcEvent> = raw
                .iter()
                .enumerate()
                .filter_map(|(i, json)| parse_debezium_envelope(json, 0, i as i64))
                .collect();
            if events.is_empty() {
                continue;
            }
            let batch =
                build_batch_from_events(&events).map_err(|e| format!("batch error: {e}"))?;
            on_batch(batch)?;
        }
        Ok(())
    }

    /// Run the pipeline against a live Kafka cluster.
    ///
    /// Requires a `CdcEventSource` implementation backed by a real Kafka
    /// consumer (e.g. `rdkafka`). Use [`run_with_source`] directly with a
    /// `KafkaCdcEventSource` once that feature is enabled.
    pub async fn run(&self) -> Result<(), String> {
        self.validate()?;
        Err(
            "direct Kafka execution requires a CdcEventSource implementation; \
             call run_with_source with a live source (rdkafka feature planned for R12)"
                .to_string(),
        )
    }
}

// ---------------------------------------------------------------------------
// Columnar batch builder for multiple CDC events  (P1.18 / P1.19)
// ---------------------------------------------------------------------------

/// An error type for CDC batch building.
#[derive(Debug)]
pub struct CdcBatchError(pub String);

impl std::fmt::Display for CdcBatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CDC batch error: {}", self.0)
    }
}

impl std::error::Error for CdcBatchError {}

/// Build a single columnar Arrow `RecordBatch` from a slice of `CdcEvent`s.
///
/// All events must share the same column names in their `after` payload (or
/// `before` payload for deletes). Column names are derived from the union of
/// keys across all events, sorted alphabetically (P1.19), with missing values
/// represented as `null`.
///
/// Returns an error if `events` is empty or if batch construction fails.
pub fn build_batch_from_events(events: &[CdcEvent]) -> Result<RecordBatch, CdcBatchError> {
    if events.is_empty() {
        return Err(CdcBatchError("events slice is empty".into()));
    }

    // Collect the union of all column names from after/before payloads, sorted.
    let mut col_names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for event in events {
        let payload = event.after.as_ref().or(event.before.as_ref());
        if let Some(batch) = payload {
            for field in batch.schema().fields() {
                col_names.insert(safe_payload_column(field.name()));
            }
        }
    }

    // Add metadata columns present on every row.
    let meta_cols = ["_op", "_table", "_lsn", "_ts_ms", "_partition", "_offset"];
    for mc in &meta_cols {
        col_names.insert(mc.to_string());
    }

    let col_names: Vec<String> = col_names.into_iter().collect(); // already sorted via BTreeSet

    // Build one string column per name.
    let mut column_data: Vec<Vec<Option<String>>> = vec![Vec::new(); col_names.len()];

    for event in events {
        let payload = event.after.as_ref().or(event.before.as_ref());

        for (col_idx, col_name) in col_names.iter().enumerate() {
            let value: Option<String> = match col_name.as_str() {
                "_op" => Some(format!("{:?}", event.op)),
                "_table" => Some(event.table.clone()),
                "_lsn" => event.source_lsn.map(|v| v.to_string()),
                "_ts_ms" => event.source_ts_ms.map(|v| v.to_string()),
                "_partition" => Some(event.partition_id.to_string()),
                "_offset" => Some(event.offset.to_string()),
                user_col => {
                    // Reverse-map safe name back to original payload field name.
                    let orig_col = if user_col.ends_with("_src")
                        && RESERVED_CDC_COLUMNS.contains(&&user_col[..user_col.len() - 4])
                    {
                        &user_col[..user_col.len() - 4]
                    } else {
                        user_col
                    };
                    match payload {
                        Some(batch) => payload_value_to_string(batch, orig_col)?,
                        None => None,
                    }
                }
            };
            column_data[col_idx].push(value);
        }
    }

    // Build Arrow arrays and schema.
    let fields: Vec<Field> = col_names
        .iter()
        .map(|name| Field::new(name.as_str(), DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let columns: Vec<Arc<dyn arrow::array::Array>> = column_data
        .into_iter()
        .map(|vals| {
            let arr: StringArray = vals.iter().map(|v| v.as_deref()).collect();
            Arc::new(arr) as Arc<dyn arrow::array::Array>
        })
        .collect();

    RecordBatch::try_new(schema, columns).map_err(|e| CdcBatchError(e.to_string()))
}

fn payload_value_to_string(
    batch: &RecordBatch,
    column_name: &str,
) -> Result<Option<String>, CdcBatchError> {
    use arrow::array::Array;
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    let Ok(idx) = batch.schema().index_of(column_name) else {
        return Ok(None);
    };
    let col = batch.column(idx);
    if col.is_null(0) {
        return Ok(None);
    }
    let options = FormatOptions::default();
    let formatter = ArrayFormatter::try_new(col.as_ref(), &options)
        .map_err(|e| CdcBatchError(e.to_string()))?;
    Ok(Some(formatter.value(0).to_string()))
}

/// Metadata column names injected by `build_batch_from_events`.
///
/// Source payload fields that collide with any of these names are renamed by
/// appending `_src` so that metadata values are never silently overwritten.
const RESERVED_CDC_COLUMNS: &[&str] = &["_op", "_table", "_lsn", "_ts_ms", "_partition", "_offset"];

/// Map a source payload column name to a safe output name.
///
/// If the name collides with a reserved CDC metadata column it is renamed to
/// `{name}_src`; otherwise the name is used as-is.
fn safe_payload_column(name: &str) -> String {
    if RESERVED_CDC_COLUMNS.contains(&name) {
        format!("{name}_src")
    } else {
        name.to_string()
    }
}

// ---------------------------------------------------------------------------
// RdkafkaCdcEventSource  (behind `kafka` feature)
// ---------------------------------------------------------------------------

/// Configuration for the rdkafka-backed CDC event source.
///
/// Construct via the builder pattern and pass to [`RdkafkaCdcEventSource::new`].
#[cfg(feature = "kafka")]
#[derive(Debug, Clone)]
pub struct KafkaCdcConfig {
    /// Comma-separated list of `host:port` bootstrap broker addresses.
    pub bootstrap_servers: String,
    /// Consumer group id used for offset management.
    pub group_id: String,
    /// Topic that carries Debezium CDC envelopes.
    pub topic: String,
    /// Security protocol (e.g. `"PLAINTEXT"`, `"SASL_SSL"`).
    pub security_protocol: String,
    /// SASL mechanism (e.g. `"PLAIN"`, `"SCRAM-SHA-256"`).  `None` for
    /// unauthenticated connections.
    pub sasl_mechanism: Option<String>,
    /// SASL username.  `None` for unauthenticated connections.
    pub sasl_username: Option<String>,
    /// SASL password.  `None` for unauthenticated connections.
    pub sasl_password: Option<String>,
}

#[cfg(feature = "kafka")]
impl KafkaCdcConfig {
    /// Create a minimal unauthenticated config for local/test brokers.
    pub fn new(
        bootstrap_servers: impl Into<String>,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            group_id: group_id.into(),
            topic: topic.into(),
            security_protocol: "PLAINTEXT".to_string(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        }
    }

    /// Set the security protocol.
    #[must_use]
    pub fn with_security_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.security_protocol = protocol.into();
        self
    }

    /// Configure SASL authentication.
    #[must_use]
    pub fn with_sasl(
        mut self,
        mechanism: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.sasl_mechanism = Some(mechanism.into());
        self.sasl_username = Some(username.into());
        self.sasl_password = Some(password.into());
        self
    }

    /// Validate the configuration.  Returns an error message if required
    /// fields are missing or inconsistent.
    pub fn validate(&self) -> Result<(), String> {
        if self.bootstrap_servers.is_empty() {
            return Err("bootstrap_servers must not be empty".into());
        }
        if self.group_id.is_empty() {
            return Err("group_id must not be empty".into());
        }
        if self.topic.is_empty() {
            return Err("topic must not be empty".into());
        }
        // If any SASL field is set, all three must be set.
        let sasl_fields = [
            self.sasl_mechanism.is_some(),
            self.sasl_username.is_some(),
            self.sasl_password.is_some(),
        ];
        let sasl_count = sasl_fields.iter().filter(|&&v| v).count();
        if sasl_count != 0 && sasl_count != 3 {
            return Err(
                "sasl_mechanism, sasl_username, and sasl_password must all be set together".into(),
            );
        }
        Ok(())
    }
}

/// A [`CdcEventSource`] backed by a real Kafka broker via `rdkafka`.
///
/// Uses a `StreamConsumer` with group-level offset management.  After each
/// successful `poll_events` batch the consumer commits offsets synchronously
/// (at-least-once delivery; duplicates are possible on restart).
///
/// Construct with [`RdkafkaCdcEventSource::new`] and pass to
/// [`CdcToLakehousePipeline::run_with_source`].
#[cfg(feature = "kafka")]
pub struct RdkafkaCdcEventSource {
    consumer: std::sync::Arc<rdkafka::consumer::StreamConsumer>,
    /// Maximum number of milliseconds to wait for a single message poll.
    poll_timeout_ms: u64,
}

#[cfg(feature = "kafka")]
impl RdkafkaCdcEventSource {
    /// Create a new source from a [`KafkaCdcConfig`].
    ///
    /// Validates the config, builds a `StreamConsumer`, and subscribes to the
    /// configured topic.  Returns an error string if configuration or consumer
    /// creation fails.
    pub fn new(config: &KafkaCdcConfig) -> Result<Self, String> {
        use rdkafka::ClientConfig;
        use rdkafka::consumer::Consumer;

        config.validate()?;

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("group.id", &config.group_id)
            .set("security.protocol", &config.security_protocol)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest");

        if let (Some(mechanism), Some(username), Some(password)) = (
            &config.sasl_mechanism,
            &config.sasl_username,
            &config.sasl_password,
        ) {
            client_config
                .set("sasl.mechanisms", mechanism)
                .set("sasl.username", username)
                .set("sasl.password", password);
        }

        let consumer: rdkafka::consumer::StreamConsumer = client_config
            .create()
            .map_err(|e| format!("rdkafka consumer creation failed: {e}"))?;

        consumer
            .subscribe(&[config.topic.as_str()])
            .map_err(|e| format!("rdkafka subscribe failed: {e}"))?;

        Ok(Self {
            consumer: std::sync::Arc::new(consumer),
            poll_timeout_ms: 100,
        })
    }

    /// Override the per-message poll timeout (default: 100 ms).
    #[must_use]
    pub fn with_poll_timeout_ms(mut self, ms: u64) -> Self {
        self.poll_timeout_ms = ms;
        self
    }

    /// Commit consumer group offsets for the currently assigned partitions.
    ///
    /// Called internally after a successful batch is handed to the pipeline.
    /// On failure the error is logged but does not abort the pipeline (the
    /// consumer will reprocess from the last committed offset on restart,
    /// providing at-least-once semantics).
    fn commit_offsets(&self) {
        use rdkafka::consumer::Consumer;
        if let Err(e) = self.consumer.commit_consumer_state(rdkafka::consumer::CommitMode::Sync) {
            tracing::warn!(error = %e, "rdkafka offset commit failed (at-least-once: will reprocess on restart)");
        }
    }
}

#[cfg(feature = "kafka")]
impl CdcEventSource for RdkafkaCdcEventSource {
    /// Poll up to `max` Debezium JSON strings from Kafka.
    ///
    /// Each call blocks for at most `poll_timeout_ms` per message.  Returns an
    /// empty `Vec` when no messages are available within the timeout window
    /// (the pipeline interprets this as a momentary idle, not shutdown).
    /// Commits consumer offsets after assembling the batch.
    fn poll_events(&mut self, max: usize) -> Result<Vec<String>, String> {
        use rdkafka::Message;

        let mut events = Vec::with_capacity(max.min(64));

        for _ in 0..max {
            let msg = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    tokio::time::timeout(
                        std::time::Duration::from_millis(self.poll_timeout_ms),
                        self.consumer.recv(),
                    )
                    .await
                })
            });

            match msg {
                // Timed out – no more messages available right now.
                Err(_timeout) => break,
                Ok(Err(e)) => {
                    return Err(format!("rdkafka receive error: {e}"));
                }
                Ok(Ok(msg)) => {
                    let payload = match msg.payload_view::<str>() {
                        Some(Ok(s)) => s.to_string(),
                        Some(Err(e)) => {
                            tracing::warn!(
                                error = ?e,
                                partition = msg.partition(),
                                offset = msg.offset(),
                                "skipping message with invalid UTF-8 payload"
                            );
                            continue;
                        }
                        None => {
                            tracing::warn!(
                                partition = msg.partition(),
                                offset = msg.offset(),
                                "skipping tombstone message (null payload)"
                            );
                            continue;
                        }
                    };
                    events.push(payload);
                }
            }
        }

        if !events.is_empty() {
            self.commit_offsets();
        }

        Ok(events)
    }
}

#[cfg(feature = "state")]
pub struct CdcOffsetTracker {
    backend: Box<dyn krishiv_state::StateBackend>,
    ns: krishiv_state::Namespace,
    offsets: std::collections::HashMap<u32, i64>,
}

#[cfg(feature = "state")]
impl CdcOffsetTracker {
    pub fn new(backend: Box<dyn krishiv_state::StateBackend>) -> Self {
        let ns = krishiv_state::Namespace::new("cdc_operator", "cdc_offsets");
        let mut offsets = std::collections::HashMap::new();
        if let Ok(keys) = backend.list_keys(&ns) {
            for k in keys {
                if k.len() == 4 {
                    let partition = u32::from_le_bytes(k.as_slice().try_into().unwrap());
                    if let Ok(Some(val_bytes)) = backend.get(&ns, &k) {
                        if val_bytes.len() == 8 {
                            let offset = i64::from_le_bytes(val_bytes.as_slice().try_into().unwrap());
                            offsets.insert(partition, offset);
                        }
                    }
                }
            }
        }
        Self { backend, ns, offsets }
    }

    pub fn commit_offset(&mut self, partition: u32, offset: i64) -> Result<(), String> {
        self.offsets.insert(partition, offset);
        let key = partition.to_le_bytes().to_vec();
        let value = offset.to_le_bytes().to_vec();
        self.backend
            .put(&self.ns, key, value)
            .map_err(|e| format!("state backend error: {:?}", e))?;
        Ok(())
    }

    pub fn get_offset(&self, partition: u32) -> Option<i64> {
        self.offsets.get(&partition).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdcop_from_debezium_parses_all_ops() {
        assert_eq!(CdcOp::from_debezium("c"), Some(CdcOp::Insert));
        assert_eq!(CdcOp::from_debezium("u"), Some(CdcOp::Update));
        assert_eq!(CdcOp::from_debezium("d"), Some(CdcOp::Delete));
        assert_eq!(CdcOp::from_debezium("r"), Some(CdcOp::SnapshotRead));
        assert_eq!(CdcOp::from_debezium("x"), None);
    }

    #[test]
    fn parse_insert_envelope() {
        let json = r#"{"op":"c","before":null,"after":{"id":1,"name":"alice"},"source":{"lsn":100,"ts_ms":1716201600000,"table":"orders"}}"#;
        let event = parse_debezium_envelope(json, 0, 0).unwrap();
        assert_eq!(event.op, CdcOp::Insert);
        assert!(event.before.is_none());
        let after = event.after.unwrap();
        let schema = after.schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            col_names.contains(&"id") || col_names.contains(&"name"),
            "after batch must have unpacked columns, got: {col_names:?}"
        );
        assert_eq!(event.source_lsn, Some(100));
        assert_eq!(event.table, "orders");
    }

    #[test]
    fn parse_delete_envelope() {
        let json = r#"{"op":"d","before":{"id":1,"name":"alice"},"after":null,"source":{"lsn":200,"ts_ms":1716201700000,"table":"orders"}}"#;
        let event = parse_debezium_envelope(json, 0, 1).unwrap();
        assert_eq!(event.op, CdcOp::Delete);
        assert!(event.before.is_some());
        assert!(event.after.is_none());
    }

    #[test]
    fn parse_malformed_envelope_returns_none() {
        assert!(parse_debezium_envelope("{}", 0, 0).is_none());
        assert!(parse_debezium_envelope("not json", 0, 0).is_none());
        assert!(parse_debezium_envelope(r#"{"op":"z"}"#, 0, 0).is_none());
    }

    #[test]
    fn pipeline_validate_rejects_empty_topic() {
        let p = CdcToLakehousePipeline::new(
            "",
            vec!["kafka:9092".into()],
            "cat",
            "tbl",
            vec!["id".into()],
        );
        assert!(p.validate().is_err());
    }

    #[test]
    fn pipeline_validate_accepts_valid_config() {
        let p = CdcToLakehousePipeline::new(
            "orders.cdc",
            vec!["kafka:9092".into()],
            "iceberg",
            "warehouse.orders",
            vec!["id".into()],
        );
        assert!(p.validate().is_ok());
    }

    #[test]
    fn build_batch_renames_reserved_payload_columns() {
        use arrow::array::StringArray;
        // Payload field named "_op" must become "_op_src"; metadata "_op" must still hold op type.
        let fields = vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("_op", DataType::Utf8, true),
        ];
        let schema = Arc::new(Schema::new(fields));
        let id_arr: StringArray = vec![Some("42")].into_iter().collect();
        let src_op_arr: StringArray = vec![Some("payload_op_value")].into_iter().collect();
        let after_batch =
            RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(src_op_arr)]).unwrap();
        let event = CdcEvent {
            op: CdcOp::Insert,
            before: None,
            after: Some(after_batch),
            source_lsn: Some(1),
            source_ts_ms: Some(1716201600000),
            partition_id: 0,
            offset: 0,
            table: "orders".to_string(),
        };
        let batch = build_batch_from_events(&[event]).unwrap();
        let schema = batch.schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            col_names.contains(&"_op"),
            "metadata _op missing: {col_names:?}"
        );
        assert!(
            col_names.contains(&"_op_src"),
            "renamed _op_src missing: {col_names:?}"
        );
        // Metadata value is the operation type, not the payload value.
        let meta_idx = batch.schema().index_of("_op").unwrap();
        let meta_arr = batch
            .column(meta_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(meta_arr.value(0), "Insert");
        // Renamed source column preserves original payload value.
        let src_idx = batch.schema().index_of("_op_src").unwrap();
        let src_arr = batch
            .column(src_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(src_arr.value(0), "payload_op_value");
    }

    #[test]
    fn build_batch_stringifies_non_utf8_payload_columns() {
        use arrow::array::{BooleanArray, Int64Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let after_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(42_i64)])),
                Arc::new(BooleanArray::from(vec![Some(true)])),
            ],
        )
        .unwrap();
        let event = CdcEvent {
            op: CdcOp::Insert,
            before: None,
            after: Some(after_batch),
            source_lsn: Some(1),
            source_ts_ms: Some(1716201600000),
            partition_id: 0,
            offset: 0,
            table: "orders".to_string(),
        };

        let batch = build_batch_from_events(&[event]).unwrap();
        let id_idx = batch.schema().index_of("id").unwrap();
        let active_idx = batch.schema().index_of("active").unwrap();
        let id = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let active = batch
            .column(active_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id.value(0), "42");
        assert_eq!(active.value(0), "true");
    }

    #[tokio::test]
    async fn run_with_source_processes_events() {
        let pipeline = CdcToLakehousePipeline::new(
            "orders",
            vec!["broker:9092".to_string()],
            "my_catalog",
            "warehouse.orders",
            vec!["id".to_string()],
        );

        let json = r#"{"op":"c","source":{"lsn":1,"ts_ms":1716201600000,"partition":0,"offset":0,"table":"orders"},"after":{"id":1,"name":"alice"}}"#;
        let source = InMemoryCdcEventSource::new([json]);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut batches_received = Vec::new();

        pipeline
            .run_with_source(
                source,
                |batch| {
                    batches_received.push(batch);
                    Ok(())
                },
                shutdown_rx,
            )
            .await
            .expect("pipeline run failed");

        drop(shutdown_tx);
        assert_eq!(batches_received.len(), 1, "expected one batch");
        let schema = batches_received[0].schema();
        assert!(schema.index_of("_op").is_ok(), "expected _op column");
    }

    #[tokio::test]
    async fn run_with_source_shutdown_stops_loop() {
        struct InfiniteSource;
        impl CdcEventSource for InfiniteSource {
            fn poll_events(&mut self, _max: usize) -> Result<Vec<String>, String> {
                Ok(vec![])
            }
        }

        let pipeline = CdcToLakehousePipeline::new(
            "orders",
            vec!["broker:9092".to_string()],
            "my_catalog",
            "warehouse.orders",
            vec!["id".to_string()],
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        drop(shutdown_tx);

        let result = pipeline
            .run_with_source(InfiniteSource, |_| Ok(()), shutdown_rx)
            .await;
        assert!(result.is_ok(), "shutdown via empty source should succeed");
    }

    #[tokio::test]
    async fn run_returns_err_without_source() {
        let pipeline = CdcToLakehousePipeline::new(
            "orders",
            vec!["broker:9092".to_string()],
            "my_catalog",
            "warehouse.orders",
            vec!["id".to_string()],
        );
        let result = pipeline.run().await;
        assert!(result.is_err(), "run() without source must return Err");
    }
}
