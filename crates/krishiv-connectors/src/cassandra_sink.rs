//! E6.8 — Apache Cassandra / ScyllaDB sink.
//!
//! Converts Arrow [`RecordBatch`] values to CQL rows and inserts them into a
//! Cassandra or ScyllaDB table using the ScyllaDB Rust driver.
//!
//! # Usage
//!
//! ```no_run
//! # #[cfg(feature = "cassandra")]
//! # async fn example() -> anyhow::Result<()> {
//! use krishiv_connectors::cassandra_sink::{CassandraConfig, CassandraSink};
//!
//! let cfg = CassandraConfig::new("127.0.0.1:9042", "my_keyspace", "my_table");
//! let mut sink = CassandraSink::connect(cfg).await?;
//! // sink.write_batch(&batch).await?;
//! # Ok(())
//! # }
//! ```

use scylla::{
    client::session_builder::SessionBuilder,
    statement::batch::{Batch, BatchType},
    value::CqlValue,
};
use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    Int8Array, Int16Array, StringArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::error::{ConnectorError, ConnectorResult};

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for the Cassandra / ScyllaDB sink.
#[derive(Debug, Clone)]
pub struct CassandraConfig {
    /// Seed node address (e.g. `"127.0.0.1:9042"`).
    pub node: String,
    /// Target keyspace.
    pub keyspace: String,
    /// Target table.
    pub table: String,
}

impl CassandraConfig {
    pub fn new(
        node: impl Into<String>,
        keyspace: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            node: node.into(),
            keyspace: keyspace.into(),
            table: table.into(),
        }
    }
}

// ── Sink ──────────────────────────────────────────────────────────────────────

/// Writes Arrow [`RecordBatch`] values to Cassandra / ScyllaDB.
///
/// On each [`write_batch`][CassandraSink::write_batch] call, one CQL UNLOGGED
/// BATCH containing one `INSERT` per row is executed.
pub struct CassandraSink {
    session: scylla::client::session::Session,
    config: CassandraConfig,
}

impl CassandraSink {
    /// Connect to the cluster.
    pub async fn connect(config: CassandraConfig) -> ConnectorResult<Self> {
        let session = SessionBuilder::new()
            .known_node(&config.node)
            .build()
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

        Ok(Self { session, config })
    }

    /// Insert all rows in `batch` using an UNLOGGED BATCH statement.
    ///
    /// Columns are taken from the Arrow schema. CQL types are inferred from
    /// the Arrow data types. The target table must exist.
    pub async fn write_batch(&self, batch: &RecordBatch) -> ConnectorResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let schema = batch.schema();
        let column_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        let placeholders = column_names.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let cols = column_names.join(", ");

        let insert_cql = format!(
            "INSERT INTO {}.{} ({}) VALUES ({})",
            self.config.keyspace, self.config.table, cols, placeholders,
        );

        let mut cql_batch = Batch::new(BatchType::Unlogged);
        let mut all_values: Vec<Vec<Option<CqlValue>>> = Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            cql_batch.append_statement(insert_cql.as_str());
            let row_values: Vec<Option<CqlValue>> = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(col_idx, field)| {
                    let col = batch.column(col_idx);
                    arrow_scalar_to_cql(col.as_ref(), row, field.data_type())
                })
                .collect();
            all_values.push(row_values);
        }

        let serialized_values: Vec<Vec<Option<CqlValue>>> = all_values;

        self.session
            .batch(&cql_batch, serialized_values)
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

        Ok(())
    }
}

// ── Arrow → CQL conversion ────────────────────────────────────────────────────

/// Convert a single Arrow array cell to an `Option<CqlValue>`.
///
/// Returns `None` for null cells.
pub fn arrow_scalar_to_cql(
    col: &dyn Array,
    row: usize,
    dt: &DataType,
) -> Option<CqlValue> {
    if col.is_null(row) {
        return None;
    }
    let val = match dt {
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|arr| CqlValue::Boolean(arr.value(row)))?,
        DataType::Int8 => col
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|arr| CqlValue::TinyInt(arr.value(row)))?,
        DataType::Int16 => col
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|arr| CqlValue::SmallInt(arr.value(row)))?,
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|arr| CqlValue::Int(arr.value(row)))?,
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|arr| CqlValue::BigInt(arr.value(row)))?,
        DataType::Float32 => col
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|arr| CqlValue::Float(arr.value(row)))?,
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|arr| CqlValue::Double(arr.value(row)))?,
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|arr| CqlValue::Text(arr.value(row).to_owned()))?,
        DataType::Binary => col
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .map(|arr| CqlValue::Blob(arr.value(row).to_vec()))?,
        _ => CqlValue::Text(format!("{dt:?}")),
    };
    Some(val)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["u1", "u2"])),
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(Float64Array::from(vec![1.5, 2.5])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn arrow_to_cql_boolean() {
        let arr = Arc::new(arrow::array::BooleanArray::from(vec![true, false]));
        let cql = arrow_scalar_to_cql(arr.as_ref(), 0, &DataType::Boolean);
        assert_eq!(cql, Some(CqlValue::Boolean(true)));
    }

    #[test]
    fn arrow_to_cql_int32() {
        let arr = Arc::new(Int32Array::from(vec![42i32]));
        let cql = arrow_scalar_to_cql(arr.as_ref(), 0, &DataType::Int32);
        assert_eq!(cql, Some(CqlValue::Int(42)));
    }

    #[test]
    fn arrow_to_cql_int64() {
        let arr = Arc::new(arrow::array::Int64Array::from(vec![100i64]));
        let cql = arrow_scalar_to_cql(arr.as_ref(), 0, &DataType::Int64);
        assert_eq!(cql, Some(CqlValue::BigInt(100)));
    }

    #[test]
    fn arrow_to_cql_utf8() {
        let arr = Arc::new(StringArray::from(vec!["hello"]));
        let cql = arrow_scalar_to_cql(arr.as_ref(), 0, &DataType::Utf8);
        assert_eq!(cql, Some(CqlValue::Text("hello".to_owned())));
    }

    #[test]
    fn arrow_to_cql_float64() {
        let arr = Arc::new(Float64Array::from(vec![3.14]));
        let cql = arrow_scalar_to_cql(arr.as_ref(), 0, &DataType::Float64);
        assert!(matches!(cql, Some(CqlValue::Double(_))));
    }

    #[test]
    fn arrow_to_cql_null_returns_none() {
        let arr = Arc::new(Int32Array::from(vec![None::<i32>]));
        let cql = arrow_scalar_to_cql(arr.as_ref(), 0, &DataType::Int32);
        assert_eq!(cql, None);
    }

    #[test]
    fn config_fields() {
        let cfg = CassandraConfig::new("127.0.0.1:9042", "ks", "tbl");
        assert_eq!(cfg.node, "127.0.0.1:9042");
        assert_eq!(cfg.keyspace, "ks");
        assert_eq!(cfg.table, "tbl");
    }
}
