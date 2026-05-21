//! CDC-to-lakehouse pipeline: Debezium 2.x over Kafka → Iceberg.

use arrow::array::Array as _;
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
                col_names.insert(field.name().clone());
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
                    // Look up the column in the payload batch.
                    payload.and_then(|batch| {
                        let schema = batch.schema();
                        schema.index_of(user_col).ok().and_then(|idx| {
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

/// Build a columnar `RecordBatch` from a slice of `CdcEvent` values.
///
/// The output schema contains:
/// - Metadata columns: `_op`, `_table`, `_lsn`, `_ts_ms`, `_partition`, `_offset`
/// - Payload columns: all fields present in any event's `after` batch
///
/// Payload field names that collide with the reserved metadata names are
/// renamed with a `_src` suffix to prevent silent overwrite.
pub fn build_batch_from_events(
    events: &[CdcEvent],
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    use arrow::array::{Int64Array, Int64Builder, StringArray, UInt32Array, UInt64Builder};

    // Collect the union of all payload field names (in stable order).
    let mut payload_field_names: Vec<String> = Vec::new();
    for event in events {
        if let Some(after) = &event.after {
            for field in after.schema().fields() {
                let safe_name = safe_payload_column(field.name());
                if !payload_field_names.contains(&safe_name) {
                    payload_field_names.push(safe_name);
                }
            }
        }
    }

    let n = events.len();

    // Metadata columns.
    let op_col: StringArray = events.iter().map(|e| Some(format!("{:?}", e.op))).collect();
    let table_col: StringArray = events.iter().map(|e| Some(e.table.as_str())).collect();
    let mut lsn_builder = UInt64Builder::with_capacity(n);
    let mut ts_ms_builder = Int64Builder::with_capacity(n);
    for e in events {
        match e.source_lsn {
            Some(v) => lsn_builder.append_value(v),
            None => lsn_builder.append_null(),
        }
        match e.source_ts_ms {
            Some(v) => ts_ms_builder.append_value(v),
            None => ts_ms_builder.append_null(),
        }
    }
    let partition_col: UInt32Array = events.iter().map(|e| Some(e.partition_id)).collect();
    let offset_col: Int64Array = events.iter().map(|e| Some(e.offset)).collect();

    // Payload columns (one column per discovered field; None when event has no `after`).
    let mut payload_cols: Vec<Arc<dyn arrow::array::Array>> = payload_field_names
        .iter()
        .map(|safe_name| {
            // Reverse-map safe name back to original to look up in after batch.
            let orig_name = if safe_name.ends_with("_src")
                && RESERVED_CDC_COLUMNS.contains(&safe_name[..safe_name.len() - 4].as_ref())
            {
                &safe_name[..safe_name.len() - 4]
            } else {
                safe_name.as_str()
            };
            let arr: arrow::array::StringArray = events
                .iter()
                .map(|e| {
                    let after = e.after.as_ref()?;
                    let col_idx = after.schema().index_of(orig_name).ok()?;
                    let col = after.column(col_idx);
                    if col.is_null(0) {
                        None
                    } else {
                        col.as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                            .and_then(|s| {
                                if s.is_null(0) {
                                    None
                                } else {
                                    Some(s.value(0).to_string())
                                }
                            })
                    }
                })
                .collect();
            Arc::new(arr) as Arc<dyn arrow::array::Array>
        })
        .collect();

    // Assemble schema.
    let mut fields = vec![
        arrow::datatypes::Field::new("_op", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("_table", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("_lsn", arrow::datatypes::DataType::UInt64, true),
        arrow::datatypes::Field::new("_ts_ms", arrow::datatypes::DataType::Int64, true),
        arrow::datatypes::Field::new("_partition", arrow::datatypes::DataType::UInt32, false),
        arrow::datatypes::Field::new("_offset", arrow::datatypes::DataType::Int64, false),
    ];
    for name in &payload_field_names {
        fields.push(arrow::datatypes::Field::new(
            name,
            arrow::datatypes::DataType::Utf8,
            true,
        ));
    }

    let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(op_col),
        Arc::new(table_col),
        Arc::new(lsn_builder.finish()),
        Arc::new(ts_ms_builder.finish()),
        Arc::new(partition_col),
        Arc::new(offset_col),
    ];
    columns.append(&mut payload_cols);

    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
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
}
