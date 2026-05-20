//! CDC-to-lakehouse pipeline: Debezium 2.x over Kafka → Iceberg.

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
pub fn parse_debezium_envelope(
    json: &str,
    partition_id: u32,
    offset: i64,
) -> Option<CdcEvent> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let op_str = v["op"].as_str()?;
    let op = CdcOp::from_debezium(op_str)?;

    let source_lsn = v["source"]["lsn"].as_u64();
    let source_ts_ms = v["source"]["ts_ms"].as_i64();
    let table = v["source"]["table"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();

    // Build minimal single-column record batches for before/after payloads.
    // In a real implementation this would use the full Debezium schema.
    // For R10, we represent the payload as a single JSON string column.
    let make_payload_batch = |payload: &serde_json::Value| -> Option<RecordBatch> {
        if payload.is_null() {
            return None;
        }
        let schema = Arc::new(Schema::new(vec![Field::new("_payload", DataType::Utf8, false)]));
        let arr = arrow::array::StringArray::from(vec![payload.to_string()]);
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).ok()
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
        }
    }

    /// Attach a schema registry URL.
    pub fn with_schema_registry(mut self, url: impl Into<String>) -> Self {
        self.schema_registry_url = Some(url.into());
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
        assert!(event.after.is_some());
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
            "", vec!["kafka:9092".into()], "cat", "tbl", vec!["id".into()]
        );
        assert!(p.validate().is_err());
    }

    #[test]
    fn pipeline_validate_accepts_valid_config() {
        let p = CdcToLakehousePipeline::new(
            "orders.cdc", vec!["kafka:9092".into()], "iceberg", "warehouse.orders", vec!["id".into()]
        );
        assert!(p.validate().is_ok());
    }
}
