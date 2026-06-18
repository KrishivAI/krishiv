//! E6.7 — Apache HBase connector.
//!
//! Writes Arrow [`RecordBatch`] values to an Apache HBase table using the
//! Thrift-1 protocol.  The synchronous Thrift client is wrapped in
//! [`tokio::task::spawn_blocking`] so the async interface remains clean.
//!
//! # Arrow → HBase mapping
//!
//! Each Arrow row becomes one `Put` whose row key is the value of a designated
//! column (configurable via [`HBaseConfig::row_key_column`]).  All other columns
//! are stored under a single column family (configurable via
//! [`HBaseConfig::column_family`]) using the column name as the qualifier.
//! Values are serialised to UTF-8 bytes.
//!
//! # Usage
//!
//! ```no_run
//! # #[cfg(feature = "hbase")]
//! # async fn example() -> anyhow::Result<()> {
//! use krishiv_connectors::hbase_connector::{HBaseConfig, HBaseSink};
//!
//! let cfg = HBaseConfig::new("localhost:9090", "my_table", "cf");
//! let mut sink = HBaseSink::connect(cfg).await?;
//! // sink.write_batch(&batch).await?;
//! # Ok(())
//! # }
//! ```

use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    StringArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use hbase_thrift::{
    MutationBuilder,
    hbase::{BatchMutation, THbaseSyncClient},
};
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{
    ReadHalf, TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel, WriteHalf,
};

use crate::error::{ConnectorError, ConnectorResult};

// ── Type alias for the concrete Thrift client ─────────────────────────────────

type HBaseClient = hbase_thrift::hbase::HbaseSyncClient<
    TBinaryInputProtocol<TBufferedReadTransport<ReadHalf<TTcpChannel>>>,
    TBinaryOutputProtocol<TBufferedWriteTransport<WriteHalf<TTcpChannel>>>,
>;

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for the HBase Thrift sink.
#[derive(Debug, Clone)]
pub struct HBaseConfig {
    /// HBase Thrift server address (e.g. `"localhost:9090"`).
    pub host: String,
    /// Target HBase table name.
    pub table: String,
    /// Column family for all Arrow columns (e.g. `"cf"`).
    pub column_family: String,
    /// Arrow column whose value is used as the HBase row key.
    /// When `None`, the row index is formatted as a zero-padded decimal.
    pub row_key_column: Option<String>,
}

impl HBaseConfig {
    pub fn new(
        host: impl Into<String>,
        table: impl Into<String>,
        column_family: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            table: table.into(),
            column_family: column_family.into(),
            row_key_column: None,
        }
    }

    pub fn with_row_key_column(mut self, col: impl Into<String>) -> Self {
        self.row_key_column = Some(col.into());
        self
    }
}

// ── Sink ──────────────────────────────────────────────────────────────────────

/// Writes Arrow [`RecordBatch`] values to Apache HBase via the Thrift-1 API.
///
/// The Thrift client is synchronous; calls are dispatched with
/// [`tokio::task::spawn_blocking`].
pub struct HBaseSink {
    /// Shared, mutex-protected Thrift client (needed for `spawn_blocking`).
    client: Arc<Mutex<HBaseClient>>,
    config: HBaseConfig,
}

impl HBaseSink {
    /// Connect to the HBase Thrift server.
    pub async fn connect(config: HBaseConfig) -> ConnectorResult<Self> {
        let host = config.host.clone();
        let client = tokio::task::spawn_blocking(move || build_hbase_client(&host))
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))??;

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            config,
        })
    }

    /// Write all rows in `batch` to HBase using `mutateRows`.
    ///
    /// Each row is converted to a [`BatchMutation`] containing one
    /// [`Mutation`][hbase_thrift::hbase::Mutation] per column.
    pub async fn write_batch(&self, batch: &RecordBatch) -> ConnectorResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let row_mutations = build_row_mutations(batch, &self.config)?;
        let table_bytes = self.config.table.as_bytes().to_vec();
        let client = Arc::clone(&self.client);

        tokio::task::spawn_blocking(move || {
            let mut guard = client.lock().map_err(|_| {
                ConnectorError::Io(std::io::Error::other("hbase client mutex poisoned"))
            })?;
            guard
                .mutate_rows(table_bytes, row_mutations, Default::default())
                .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))
        })
        .await
        .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))??;

        Ok(())
    }
}

// ── Thrift client factory ─────────────────────────────────────────────────────

fn build_hbase_client(host: &str) -> ConnectorResult<HBaseClient> {
    let mut channel = TTcpChannel::new();
    channel
        .open(host)
        .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

    let (read_half, write_half) = channel
        .split()
        .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

    let read_transport = TBufferedReadTransport::new(read_half);
    let write_transport = TBufferedWriteTransport::new(write_half);

    let in_proto = TBinaryInputProtocol::new(read_transport, true);
    let out_proto = TBinaryOutputProtocol::new(write_transport, true);

    Ok(HBaseClient::new(in_proto, out_proto))
}

// ── Arrow → BatchMutation conversion ─────────────────────────────────────────

/// Build one [`BatchMutation`] per Arrow row.
///
/// This is a pure function, testable without a live HBase instance.
pub fn build_row_mutations(
    batch: &RecordBatch,
    config: &HBaseConfig,
) -> ConnectorResult<Vec<BatchMutation>> {
    let schema = batch.schema();
    let n = batch.num_rows();

    let row_key_col_idx: Option<usize> = config
        .row_key_column
        .as_deref()
        .and_then(|name| schema.index_of(name).ok());

    let mut result = Vec::with_capacity(n);

    for row in 0..n {
        let row_key: Vec<u8> = match row_key_col_idx {
            Some(col_idx) => {
                let col = batch.column(col_idx);
                arrow_cell_to_bytes(col.as_ref(), row)
                    .unwrap_or_else(|| format!("{row:012}").into_bytes())
            }
            None => format!("{row:012}").into_bytes(),
        };

        let mut mutations = Vec::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            if Some(col_idx) == row_key_col_idx {
                continue;
            }
            let col = batch.column(col_idx);
            if col.is_null(row) {
                continue;
            }
            let value = arrow_cell_to_bytes(col.as_ref(), row).unwrap_or_default();

            let mutation = MutationBuilder::default()
                .column(&config.column_family, field.name())
                .value(value)
                .build();
            mutations.push(mutation);
        }

        result.push(BatchMutation {
            row: Some(row_key),
            mutations: Some(mutations),
        });
    }

    Ok(result)
}

// ── Arrow cell → bytes ────────────────────────────────────────────────────────

/// Serialize a single Arrow array cell to raw bytes (UTF-8 representation).
///
/// Returns `None` for null cells.
pub fn arrow_cell_to_bytes(col: &dyn Array, row: usize) -> Option<Vec<u8>> {
    if col.is_null(row) {
        return None;
    }
    let s = match col.data_type() {
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|arr| arr.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int8 => col
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|arr| arr.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int16 => col
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|arr| arr.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|arr| arr.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|arr| arr.value(row).to_string())
            .unwrap_or_default(),
        DataType::Float32 => col
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|arr| arr.value(row).to_string())
            .unwrap_or_default(),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|arr| arr.value(row).to_string())
            .unwrap_or_default(),
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|arr| arr.value(row).to_owned())
            .unwrap_or_default(),
        DataType::Binary => {
            return col
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .map(|arr| arr.value(row).to_vec());
        }
        _ => {
            tracing::warn!(data_type = ?col.data_type(), "unsupported Arrow data type for HBase; skipping cell");
            return None;
        }
    };
    Some(s.into_bytes())
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
            Field::new("row_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["r001", "r002", "r003"])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn build_row_mutations_produces_one_per_row() {
        let batch = make_batch();
        let config = HBaseConfig::new("localhost:9090", "t1", "cf").with_row_key_column("row_id");
        let mutations = build_row_mutations(&batch, &config).unwrap();
        assert_eq!(mutations.len(), 3);
    }

    #[test]
    fn row_key_set_from_column() {
        let batch = make_batch();
        let config = HBaseConfig::new("localhost:9090", "t1", "cf").with_row_key_column("row_id");
        let mutations = build_row_mutations(&batch, &config).unwrap();
        assert_eq!(mutations[0].row.as_deref(), Some(b"r001".as_ref()));
        assert_eq!(mutations[1].row.as_deref(), Some(b"r002".as_ref()));
    }

    #[test]
    fn row_key_defaults_to_index_when_no_column() {
        let batch = make_batch();
        let config = HBaseConfig::new("localhost:9090", "t1", "cf");
        let mutations = build_row_mutations(&batch, &config).unwrap();
        assert_eq!(mutations[0].row.as_deref(), Some(b"000000000000".as_ref()));
        assert_eq!(mutations[2].row.as_deref(), Some(b"000000000002".as_ref()));
    }

    #[test]
    fn mutations_exclude_row_key_column() {
        let batch = make_batch();
        let config = HBaseConfig::new("localhost:9090", "t1", "cf").with_row_key_column("row_id");
        let mutations = build_row_mutations(&batch, &config).unwrap();
        let cols: Vec<String> = mutations[0]
            .mutations
            .as_ref()
            .unwrap()
            .iter()
            .map(|m| String::from_utf8(m.column.clone().unwrap()).unwrap())
            .collect();
        assert!(!cols.iter().any(|c| c.contains("row_id")));
        assert!(cols.iter().any(|c| c.contains("name")));
        assert!(cols.iter().any(|c| c.contains("score")));
    }

    #[test]
    fn mutation_column_uses_family_qualifier() {
        let batch = make_batch();
        let config = HBaseConfig::new("localhost:9090", "t1", "cf").with_row_key_column("row_id");
        let mutations = build_row_mutations(&batch, &config).unwrap();
        let cols: Vec<String> = mutations[0]
            .mutations
            .as_ref()
            .unwrap()
            .iter()
            .map(|m| String::from_utf8(m.column.clone().unwrap()).unwrap())
            .collect();
        for col in &cols {
            assert!(col.starts_with("cf:"), "Expected cf: prefix, got {col}");
        }
    }

    #[test]
    fn arrow_cell_to_bytes_int32() {
        let arr = Arc::new(Int32Array::from(vec![42i32]));
        let bytes = arrow_cell_to_bytes(arr.as_ref(), 0);
        assert_eq!(bytes, Some(b"42".to_vec()));
    }

    #[test]
    fn arrow_cell_to_bytes_null_returns_none() {
        let arr = Arc::new(Int32Array::from(vec![None::<i32>]));
        let bytes = arrow_cell_to_bytes(arr.as_ref(), 0);
        assert_eq!(bytes, None);
    }

    #[test]
    fn config_fields() {
        let cfg = HBaseConfig::new("h:9090", "tbl", "d").with_row_key_column("pk");
        assert_eq!(cfg.host, "h:9090");
        assert_eq!(cfg.table, "tbl");
        assert_eq!(cfg.column_family, "d");
        assert_eq!(cfg.row_key_column.as_deref(), Some("pk"));
    }
}
