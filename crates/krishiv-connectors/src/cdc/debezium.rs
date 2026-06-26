//! Debezium envelope parsing.

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

/// Raw CDC source record plus its source offset identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawCdcRecord {
    /// Raw Debezium JSON payload (UTF-8).  Always set; may be empty when
    /// `raw_bytes` carries the authoritative payload instead.
    pub payload: String,
    /// Raw binary payload bytes as received from Kafka.  Set by sources that
    /// provide Confluent wire-format (Avro/Protobuf) records for schema-registry
    /// decoding.  `None` for JSON / plain-text sources.
    pub raw_bytes: Option<Vec<u8>>,
    /// Kafka partition the record came from.
    pub partition_id: u32,
    /// Kafka offset of the record.
    pub offset: i64,
}

impl RawCdcRecord {
    /// Create a raw CDC record with source offset metadata.
    pub fn new(payload: impl Into<String>, partition_id: u32, offset: i64) -> Self {
        Self {
            payload: payload.into(),
            raw_bytes: None,
            partition_id,
            offset,
        }
    }

    /// Create a record carrying binary payload for schema-registry sources.
    pub fn with_bytes(bytes: Vec<u8>, partition_id: u32, offset: i64) -> Self {
        Self {
            payload: String::new(),
            raw_bytes: Some(bytes),
            partition_id,
            offset,
        }
    }
}

/// Parse a slice of raw CDC records as Debezium JSON envelopes.
pub(crate) fn parse_debezium_records(raw: &[RawCdcRecord]) -> Result<Vec<CdcEvent>, String> {
    raw.iter()
        .enumerate()
        .map(|(i, record)| {
            parse_debezium_envelope_result(&record.payload, record.partition_id, record.offset)
                .map_err(|e| format!("Debezium parse error at batch index {i}: {e}"))
        })
        .collect()
}

/// Error returned when a Debezium JSON envelope cannot be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DebeziumParseError {
    #[error("invalid Debezium JSON: {0}")]
    InvalidJson(String),
    #[error("Debezium envelope missing op field")]
    MissingOp,
    #[error("unknown Debezium op '{0}'")]
    UnknownOp(String),
    #[error("Debezium envelope missing required payload for {op:?}")]
    MissingPayload { op: CdcOp },
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
pub fn parse_debezium_envelope(
    json: &str,
    partition_id: u32,
    offset: i64,
) -> Result<CdcEvent, DebeziumParseError> {
    parse_debezium_envelope_result(json, partition_id, offset)
}

/// Strict Debezium JSON parser used by production CDC pipelines.
pub fn parse_debezium_envelope_result(
    json: &str,
    partition_id: u32,
    offset: i64,
) -> Result<CdcEvent, DebeziumParseError> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| DebeziumParseError::InvalidJson(e.to_string()))?;
    let op_str = v.get("op").and_then(|v| v.as_str()).ok_or(DebeziumParseError::MissingOp)?;
    let op =
        CdcOp::from_debezium(op_str).ok_or_else(|| DebeziumParseError::UnknownOp(op_str.into()))?;

    let source_lsn = v.get("source").and_then(|s| s.get("lsn")).and_then(|v| v.as_u64());
    let source_ts_ms = v.get("source").and_then(|s| s.get("ts_ms")).and_then(|v| v.as_i64());
    let table = v.get("source").and_then(|s| s.get("table")).and_then(|v| v.as_str()).unwrap_or("unknown").to_string();

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

    let before = make_payload_batch(v.get("before").unwrap_or(&serde_json::Value::Null));
    let after = make_payload_batch(v.get("after").unwrap_or(&serde_json::Value::Null));
    match &op {
        CdcOp::Insert | CdcOp::Update | CdcOp::SnapshotRead if after.is_none() => {
            return Err(DebeziumParseError::MissingPayload { op: op.clone() });
        }
        CdcOp::Delete if before.is_none() => {
            return Err(DebeziumParseError::MissingPayload { op: op.clone() });
        }
        _ => {}
    }

    Ok(CdcEvent {
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
