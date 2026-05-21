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

    /// Run the pipeline.
    ///
    /// 1. Validates the configuration.
    /// 2. Sets up the event processing loop structure.
    /// 3. TODO: wire Kafka consumer here — currently returns `Ok(())` after setup.
    pub async fn run(&self) -> Result<(), String> {
        self.validate()?;
        // TODO: wire Kafka consumer here.
        // When a live Kafka client is available:
        //   - Create a consumer subscribed to `self.source_topic`.
        //   - Loop: poll for up to `self.batch_size` messages, parse each with
        //     `parse_debezium_envelope`, call `build_batch_from_events`, and write
        //     to the target table sink.
        //   - On shutdown signal, flush and commit the sink.
        Ok(())
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
                    payload.and_then(|batch| {
                        let schema = batch.schema();
                        schema.index_of(orig_col).ok().and_then(|idx| {
                            use arrow::array::{Array, StringArray};
                            let col = batch.column(idx);
                            if col.is_null(0) {
                                None
                            } else {
                                col.as_any().downcast_ref::<StringArray>().and_then(|s| {
                                    if s.is_null(0) {
                                        None
                                    } else {
                                        Some(s.value(0).to_string())
                                    }
                                })
                            }
                        })
                    })
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
}
