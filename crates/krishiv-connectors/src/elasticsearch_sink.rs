//! E6.6 — Elasticsearch / OpenSearch sink.
//!
//! Converts Arrow [`RecordBatch`] values to JSON documents and bulk-indexes
//! them into an Elasticsearch / OpenSearch cluster.
//!
//! # Usage
//!
//! ```no_run
//! # #[cfg(feature = "elasticsearch")]
//! # async fn example() -> anyhow::Result<()> {
//! use krishiv_connectors::elasticsearch_sink::{ElasticsearchConfig, ElasticsearchSink};
//!
//! let cfg = ElasticsearchConfig::new("http://localhost:9200", "my-index");
//! let mut sink = ElasticsearchSink::connect(cfg).await?;
//! // batch is an Arrow RecordBatch
//! // sink.write_batch(&batch).await?;
//! # Ok(())
//! # }
//! ```

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use elasticsearch::{
    BulkOperations, BulkParts, Elasticsearch,
    auth::Credentials,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
};
use serde_json::{Map, Value as JsonValue};

use crate::error::{ConnectorError, ConnectorResult};

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for the Elasticsearch / OpenSearch sink.
#[derive(Clone)]
pub struct ElasticsearchConfig {
    /// Cluster URL (e.g. `"http://localhost:9200"`).
    pub url: String,
    /// Target index name.
    pub index: String,
    /// Optional `(username, password)` for basic auth.
    pub credentials: Option<(String, String)>,
    /// Name of an Arrow `Utf8` column to use as the `_id` field.
    /// When `None`, Elasticsearch auto-generates IDs.
    pub id_column: Option<String>,
}

impl std::fmt::Debug for ElasticsearchConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElasticsearchConfig")
            .field("url", &self.url)
            .field("index", &self.index)
            .field(
                "credentials",
                &self.credentials.as_ref().map(|(u, _)| (u, "****")),
            )
            .field("id_column", &self.id_column)
            .finish()
    }
}

impl ElasticsearchConfig {
    /// Create a minimal config.
    pub fn new(url: impl Into<String>, index: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            index: index.into(),
            credentials: None,
            id_column: None,
        }
    }

    pub fn with_credentials(mut self, user: impl Into<String>, pass: impl Into<String>) -> Self {
        self.credentials = Some((user.into(), pass.into()));
        self
    }

    pub fn with_id_column(mut self, col: impl Into<String>) -> Self {
        self.id_column = Some(col.into());
        self
    }
}

// ── Sink ──────────────────────────────────────────────────────────────────────

/// Writes Arrow [`RecordBatch`] values to Elasticsearch using the bulk API.
pub struct ElasticsearchSink {
    client: Elasticsearch,
    config: ElasticsearchConfig,
}

impl ElasticsearchSink {
    /// Create and connect a new sink.
    pub async fn connect(config: ElasticsearchConfig) -> ConnectorResult<Self> {
        let url = config
            .url
            .parse::<url::Url>()
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
        let pool = SingleNodeConnectionPool::new(url);

        let mut transport_builder = TransportBuilder::new(pool);
        if let Some((user, pass)) = &config.credentials {
            transport_builder =
                transport_builder.auth(Credentials::Basic(user.clone(), pass.clone()));
        }
        // L4.1: Set request and connect timeouts to prevent indefinite hangs
        // on stalled Elasticsearch connections.
        transport_builder = transport_builder
            .timeout(std::time::Duration::from_secs(30))
            .connect_timeout(std::time::Duration::from_secs(5));

        let transport = transport_builder
            .build()
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
        let client = Elasticsearch::new(transport);

        Ok(Self { client, config })
    }

    /// Index all rows in `batch` using the bulk API.
    ///
    /// Each row is converted to a JSON document.  The `_id` field is set from
    /// `config.id_column` when configured, otherwise Elasticsearch auto-IDs.
    ///
    /// **This method performs synchronous blocking I/O** (HTTP request to the
    /// Elasticsearch cluster). There is no separate `flush()` — each call to
    /// `write_batch` is a self-contained bulk index operation.
    pub async fn write_batch(&self, batch: &RecordBatch) -> ConnectorResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let docs = batch_to_json_docs(batch);
        let mut ops = BulkOperations::new();

        let id_col_idx = self
            .config
            .id_column
            .as_deref()
            .and_then(|name| batch.schema().index_of(name).ok());

        for (row_idx, doc) in docs.into_iter().enumerate() {
            let source = JsonValue::Object(doc);
            if let Some(col_idx) = id_col_idx {
                let id = extract_id(batch, col_idx, row_idx);
                ops.push(elasticsearch::BulkOperation::<JsonValue>::index(source).id(id))
                    .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
            } else {
                ops.push(elasticsearch::BulkOperation::<JsonValue>::index(source))
                    .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
            }
        }

        let resp = self
            .client
            .bulk(BulkParts::Index(&self.config.index))
            .body(vec![ops])
            .send()
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;

        if !resp.status_code().is_success() {
            return Err(ConnectorError::Io(std::io::Error::other(format!(
                "elasticsearch bulk error: status {}",
                resp.status_code()
            ))));
        }

        // The bulk API returns HTTP 200 even when individual document operations
        // fail. Check the `errors` field in the response body and surface any
        // per-item failures so callers don't silently lose data.
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Io(std::io::Error::other(e.to_string())))?;
        if body
            .get("errors")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            let failures: Vec<String> = body
                .get("items")
                .and_then(|v| v.as_array())
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| {
                            let op = item.get("index").or_else(|| item.get("create"))?;
                            let err = op.get("error")?;
                            Some(err.to_string())
                        })
                        .collect()
                })
                .unwrap_or_default();
            return Err(ConnectorError::Io(std::io::Error::other(format!(
                "elasticsearch bulk partial failure ({} errors): {}",
                failures.len(),
                failures.join("; ")
            ))));
        }
        Ok(())
    }
}

// ── Arrow → JSON row conversion ───────────────────────────────────────────────

/// Convert every row in `batch` to a `serde_json::Map`.
pub fn batch_to_json_docs(batch: &RecordBatch) -> Vec<Map<String, JsonValue>> {
    let schema = batch.schema();
    let n = batch.num_rows();
    let mut rows: Vec<Map<String, JsonValue>> = (0..n).map(|_| Map::new()).collect();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let col = batch.column(col_idx);
        let name = field.name().clone();
        for (row, map) in rows.iter_mut().enumerate() {
            let val = arrow_scalar_to_json(col.as_ref(), row);
            map.insert(name.clone(), val);
        }
    }
    rows
}

fn arrow_scalar_to_json(col: &dyn Array, row: usize) -> JsonValue {
    if col.is_null(row) {
        return JsonValue::Null;
    }
    match col.data_type() {
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|arr| JsonValue::Bool(arr.value(row)))
            .unwrap_or(JsonValue::Null),
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|arr| JsonValue::Number(arr.value(row).into()))
            .unwrap_or(JsonValue::Null),
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|arr| JsonValue::Number(arr.value(row).into()))
            .unwrap_or(JsonValue::Null),
        DataType::Float32 => col
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|arr| {
                let v = arr.value(row);
                if v.is_nan() || v.is_infinite() {
                    JsonValue::Null
                } else {
                    match serde_json::Number::from_f64(v as f64) {
                        Some(n) => JsonValue::Number(n),
                        None => {
                            tracing::warn!(value = %v, "unrepresentable float32 value for JSON; mapping to null");
                            JsonValue::Null
                        }
                    }
                }
            })
            .unwrap_or(JsonValue::Null),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|arr| {
                let v = arr.value(row);
                if v.is_nan() || v.is_infinite() {
                    JsonValue::Null
                } else {
                    match serde_json::Number::from_f64(v) {
                        Some(n) => JsonValue::Number(n),
                        None => {
                            tracing::warn!(value = %v, "unrepresentable float64 value for JSON; mapping to null");
                            JsonValue::Null
                        }
                    }
                }
            })
            .unwrap_or(JsonValue::Null),
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|arr| JsonValue::String(arr.value(row).to_owned()))
            .unwrap_or(JsonValue::Null),
        _ => {
            tracing::warn!(data_type = ?col.data_type(), "unsupported Arrow data type for Elasticsearch; mapping to null");
            JsonValue::Null
        }
    }
}

fn extract_id(batch: &RecordBatch, col_idx: usize, row: usize) -> String {
    let col = batch.column(col_idx);
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>()
        && !arr.is_null(row)
    {
        return arr.value(row).to_owned();
    }
    row.to_string()
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
            Field::new("id", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("score", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["u1", "u2", "u3"])),
                Arc::new(Int32Array::from(vec![25, 30, 22])),
                Arc::new(Float64Array::from(vec![9.5, 7.0, 8.3])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn batch_converts_to_json_docs() {
        let batch = make_batch();
        let docs = batch_to_json_docs(&batch);
        assert_eq!(docs.len(), 3);

        let doc = &docs[0];
        assert_eq!(doc["id"], JsonValue::String("u1".to_owned()));
        assert_eq!(doc["age"], JsonValue::Number(25.into()));
    }

    #[test]
    fn json_docs_have_all_columns() {
        let batch = make_batch();
        let docs = batch_to_json_docs(&batch);
        assert!(docs[0].contains_key("id"));
        assert!(docs[0].contains_key("age"));
        assert!(docs[0].contains_key("score"));
    }

    #[test]
    fn null_arrow_value_becomes_json_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec![Some("hi"), None]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let docs = batch_to_json_docs(&batch);
        assert_eq!(docs[0]["x"], JsonValue::String("hi".to_owned()));
        assert_eq!(docs[1]["x"], JsonValue::Null);
    }

    #[test]
    fn empty_batch_produces_no_docs() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::new_empty(schema);
        let docs = batch_to_json_docs(&batch);
        assert!(docs.is_empty());
    }

    #[test]
    fn config_defaults() {
        let cfg = ElasticsearchConfig::new("http://localhost:9200", "logs");
        assert_eq!(cfg.url, "http://localhost:9200");
        assert_eq!(cfg.index, "logs");
        assert!(cfg.credentials.is_none());
        assert!(cfg.id_column.is_none());
    }

    #[test]
    fn config_builder_chain() {
        let cfg = ElasticsearchConfig::new("http://localhost:9200", "idx")
            .with_credentials("elastic", "password")
            .with_id_column("doc_id");
        assert!(cfg.credentials.is_some());
        assert_eq!(cfg.id_column.as_deref(), Some("doc_id"));
    }

    #[test]
    fn boolean_and_int64_serialize() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("active", DataType::Boolean, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::BooleanArray::from(vec![true, false])),
                Arc::new(arrow::array::Int64Array::from(vec![100i64, 200i64])),
            ],
        )
        .unwrap();
        let docs = batch_to_json_docs(&batch);
        assert_eq!(docs[0]["active"], JsonValue::Bool(true));
        assert_eq!(docs[1]["active"], JsonValue::Bool(false));
    }
}
