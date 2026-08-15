//! Module-level sources: `read_parquet`, `read_kafka`, `read_iceberg`.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::dataframe::PyDataFrame;
use crate::errors::ConnectorError;
use crate::schema::validate_batch_against_schema_class;
use crate::session::PySession;
use crate::streaming_dataframe::PyStreamingDataFrame;

/// Build a `StreamingDataFrame` over a SQL query against an engine-registered
/// streaming source (Kafka/Kinesis/Pulsar/Iceberg). `watermark_col` empty means
/// no event-time watermark is set. This is the single unified streaming entry —
/// the old DataStream `Stream` classes were retired.
#[cfg_attr(
    not(any(
        feature = "kafka",
        feature = "iceberg",
        feature = "kinesis",
        feature = "pulsar"
    )),
    allow(dead_code)
)]
fn streaming_df_from_sql(
    session: &PySession,
    sql: String,
    watermark_col: &str,
    lag_ms: u64,
) -> PyResult<PyStreamingDataFrame> {
    let df = session
        .inner
        .sql(sql)
        .map_err(crate::errors::map_krishiv_error)?;
    let mut sdf = PyStreamingDataFrame::new(df);
    if !watermark_col.is_empty() {
        sdf = sdf.with_event_time(watermark_col.to_string());
        if lag_ms > 0 {
            sdf = sdf.with_watermark_lag(lag_ms);
        }
    }
    Ok(sdf)
}

#[pyfunction]
#[pyo3(signature = (path, schema=None))]
pub fn read_parquet(
    py: Python<'_>,
    path: String,
    schema: Option<Bound<'_, PyType>>,
) -> PyResult<PyDataFrame> {
    let schema_cls = schema.map(|s| s.unbind());
    py.detach(move || {
        let session = krishiv_api::SessionBuilder::new()
            .build()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let table_name = std::path::Path::new(&path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("table")
            .to_owned();
        session
            .register_parquet(&table_name, &path)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let escaped = table_name.replace('"', "\"\"");
        let df = session
            .sql(format!("SELECT * FROM \"{escaped}\""))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        if let Some(schema_cls) = schema_cls {
            let result = df
                .collect()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Python::attach(|py| -> PyResult<()> {
                let bound = schema_cls.bind(py);
                for batch in result.batches() {
                    validate_batch_against_schema_class(bound, batch)?;
                }
                Ok(())
            })?;
        }
        Ok(PyDataFrame { inner: df })
    })
}

#[pyfunction]
#[pyo3(signature = (session, topic, bootstrap_servers, *, schema=None, group_id=None))]
/// Read a Kafka topic as a streaming source.
///
/// **Feature gate**: Requires `pip install krishiv[kafka]` or building with `--features kafka`.
/// Without the feature, raises `ConnectorError` immediately.
///
/// With the `kafka` feature, registers the Kafka topic as a SQL streaming table and
/// returns a `Stream` descriptor. The `schema` describes the expected Arrow schema for
/// deserialized records (uses a single `value: Utf8` field when `None`). The `group_id`
/// sets the Kafka consumer group (defaults to `"krishiv-default"` when `None`).
/// The returned stream has no watermark set — call `.with_watermark(column, lag_ms)`
/// before windowing.
pub fn read_kafka(
    session: &PySession,
    topic: String,
    bootstrap_servers: String,
    schema: Option<&Bound<'_, PyType>>,
    group_id: Option<String>,
) -> PyResult<PyStreamingDataFrame> {
    #[cfg(not(feature = "kafka"))]
    {
        let _ = (session, &topic, &bootstrap_servers, schema, group_id);
        Err(ConnectorError::new_err(
            "Kafka support requires building with the `kafka` feature (pip install krishiv[kafka])",
        ))
    }
    #[cfg(feature = "kafka")]
    {
        use crate::schema::PySchema;
        use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
        use std::sync::Arc;

        let arrow_schema: SchemaRef = if let Some(cls) = schema {
            PySchema::arrow_schema_from_class(cls)?
        } else {
            Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
        };
        let gid = group_id.unwrap_or_else(|| "krishiv-default".to_string());
        // Register the topic as a SQL streaming table so `SELECT * FROM "{topic}"`
        // works through the standard stream execution path. rdkafka spawns the
        // consumer's poll thread on creation and needs a live reactor, so enter
        // the persistent global runtime first.
        let _rt_guard = crate::RUNTIME.enter();
        session
            .inner
            .register_kafka_source(&topic, arrow_schema, &bootstrap_servers, &topic, gid)
            .map_err(crate::errors::map_krishiv_error)?;
        let escaped_topic = topic.replace('"', "\"\"");
        streaming_df_from_sql(session, format!("SELECT * FROM \"{escaped_topic}\""), "", 0)
    }
}

#[pyfunction]
#[pyo3(signature = (session, catalog_uri, table_name, *, schema=None))]
/// Read an Iceberg table as a streaming source.
///
/// **Feature gate**: Requires `pip install krishiv[iceberg]` or building with `--features iceberg`.
///
/// **Alpha (with feature)**: `catalog_uri` must be a filesystem catalog — a local
/// directory (or `file://` URI) laid out by the engine's Iceberg filesystem
/// tables (what `krishiv.sinks.iceberg` writes). The table is scanned for real;
/// an empty `catalog_uri` gives an empty in-memory table (schema-only
/// pipelines/tests), and any other URI is rejected. REST catalogs are not
/// reachable from this surface yet. The returned stream has no watermark set.
pub fn read_iceberg(
    py: Python<'_>,
    session: &PySession,
    catalog_uri: String,
    table_name: String,
    schema: Option<&Bound<'_, PyType>>,
) -> PyResult<PyStreamingDataFrame> {
    let _ = schema;
    #[cfg(not(feature = "iceberg"))]
    {
        let _ = (py, session, &catalog_uri, &table_name);
        Err(ConnectorError::new_err(
            "Iceberg support requires the `iceberg` feature (pip install krishiv[iceberg])",
        ))
    }
    #[cfg(feature = "iceberg")]
    {
        read_iceberg_impl(py, session, catalog_uri, table_name, schema)
    }
}

#[cfg(feature = "iceberg")]
fn read_iceberg_impl(
    py: Python<'_>,
    session: &PySession,
    catalog_uri: String,
    table_name: String,
    schema: Option<&Bound<'_, PyType>>,
) -> PyResult<PyStreamingDataFrame> {
    use crate::schema::PySchema;

    use krishiv_connectors::lakehouse::{
        IcebergScanOptions, IcebergTableRef, LakehouseTable, MemoryLakehouseTable, SchemaField,
        SchemaVersion,
    };

    fn schema_version_from_arrow(
        schema: &std::sync::Arc<arrow::datatypes::Schema>,
    ) -> SchemaVersion {
        let fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| SchemaField {
                id: (i as i32) + 1,
                name: f.name().clone(),
                required: !f.is_nullable(),
                data_type: f.data_type().to_string(),
            })
            .collect();
        SchemaVersion {
            schema_id: 1,
            fields,
        }
    }

    let parts: Vec<&str> = table_name.split('.').collect();
    let (namespace, name) = match parts.as_slice() {
        [ns, tbl] => ((*ns).to_string(), (*tbl).to_string()),
        [tbl] => ("default".to_string(), (*tbl).to_string()),
        _ => {
            return Err(ConnectorError::new_err(
                "table_name must be 'table' or 'namespace.table'",
            ));
        }
    };
    let catalog = if catalog_uri.is_empty() {
        "default".to_string()
    } else {
        catalog_uri.clone()
    };
    let table_ref = IcebergTableRef::new(catalog, namespace, name);
    let schema_version = if let Some(cls) = schema {
        let arrow_schema = PySchema::arrow_schema_from_class(cls)?;
        schema_version_from_arrow(&arrow_schema)
    } else {
        SchemaVersion {
            schema_id: 1,
            fields: vec![],
        }
    };
    let opts = IcebergScanOptions::new();
    // A filesystem catalog (a local directory or file:// URI) is read for
    // real through `IcebergFsTable`. Anything else is rejected loudly — the
    // old behaviour validated against an always-empty in-memory table, which
    // silently returned zero rows for every existing table.
    let fs_root = catalog_uri
        .strip_prefix("file://")
        .unwrap_or(catalog_uri.as_str());
    let scanned = if !fs_root.is_empty() && std::path::Path::new(fs_root).is_dir() {
        let table = krishiv_connectors::lakehouse::IcebergFsTable::new(
            fs_root,
            table_ref.clone(),
            schema_version,
        )
        .map_err(|e| ConnectorError::new_err(format!("Iceberg open error: {e}")))?;
        py.detach(|| {
            crate::RUNTIME.block_on(async {
                table
                    .scan(&opts)
                    .await
                    .map_err(|e| ConnectorError::new_err(format!("Iceberg scan error: {e}")))
            })
        })?
    } else if catalog_uri.is_empty() {
        // No catalog: an in-memory empty table, useful for schema-only
        // pipelines and tests.
        let table = MemoryLakehouseTable::new(table_ref.clone(), schema_version);
        py.detach(|| {
            crate::RUNTIME.block_on(async {
                table
                    .scan(&opts)
                    .await
                    .map_err(|e| ConnectorError::new_err(format!("Iceberg catalog error: {e}")))
            })
        })?
    } else {
        return Err(ConnectorError::new_err(format!(
            "read_iceberg supports filesystem catalogs only (a local directory or file:// URI); \
             '{catalog_uri}' is not an existing directory"
        )));
    };
    // Register the scanned rows as a SQL table so the source is a unified
    // StreamingDataFrame (the old Stream/pipeline path was retired). The name
    // is derived from namespace+table only — `full_name()` includes the
    // catalog string, which for filesystem catalogs is a whole directory path
    // and produced unusable table names like `iceberg_/tmp/..._default_zones`.
    let table_name = format!(
        "iceberg_{}_{}",
        table_ref.namespace.replace('.', "_"),
        table_ref.name
    );
    session
        .inner
        .register_record_batches(&table_name, scanned)
        .map_err(crate::errors::map_krishiv_error)?;
    let escaped = table_name.replace('"', "\"\"");
    streaming_df_from_sql(session, format!("SELECT * FROM \"{escaped}\""), "", 0)
}

// ── G13: Kinesis source ───────────────────────────────────────────────────────

/// Read batches from an Amazon Kinesis Data Stream.
///
/// **Feature gate**: requires the `kinesis` feature (`pip install krishiv[kinesis]`).
///
/// Reads up to `max_batches` record batches from the specified shard and
/// registers them as an in-memory stream named `stream_name`.
///
/// Schema: `sequence_number Utf8`, `partition_key Utf8`, `data Binary`,
/// `arrival_timestamp_ms Int64`.
///
/// `start_position` — one of `"trim_horizon"` (default), `"latest"`,
///   `"after:<seq>"`, or `"at:<seq>"`.
#[pyfunction]
#[pyo3(signature = (session, stream_name, region, *, shard_id="shardId-000000000000", start_position="trim_horizon", max_batches=10, batch_size=100))]
#[allow(clippy::too_many_arguments)] // keyword-only pyfunction surface
pub fn read_kinesis(
    py: Python<'_>,
    session: &PySession,
    stream_name: String,
    region: String,
    shard_id: &str,
    start_position: &str,
    max_batches: usize,
    batch_size: i32,
) -> PyResult<PyStreamingDataFrame> {
    #[cfg(not(feature = "kinesis"))]
    {
        let _ = (
            py,
            session,
            stream_name,
            region,
            shard_id,
            start_position,
            max_batches,
            batch_size,
        );
        Err(ConnectorError::new_err(
            "Kinesis support requires the `kinesis` feature (pip install krishiv[kinesis])",
        ))
    }
    #[cfg(feature = "kinesis")]
    {
        use krishiv_connectors::kinesis::{KinesisConfig, KinesisSource, ShardPosition};

        let start = match start_position {
            "trim_horizon" | "TrimHorizon" => ShardPosition::TrimHorizon,
            "latest" | "Latest" => ShardPosition::Latest,
            s if s.starts_with("after:") => ShardPosition::AfterSequenceNumber(s[6..].to_string()),
            s if s.starts_with("at:") => ShardPosition::AtSequenceNumber(s[3..].to_string()),
            other => {
                return Err(ConnectorError::new_err(format!(
                    "unknown start_position '{other}'; expected trim_horizon, latest, after:<seq>, or at:<seq>"
                )));
            }
        };
        let cfg = KinesisConfig {
            stream_name: stream_name.clone(),
            region,
            shard_id: shard_id.to_string(),
            start,
            batch_size,
        };
        let inner = session.inner.clone();
        let name = stream_name.clone();
        py.detach(move || {
            crate::session::block_on_async(async move {
                let mut src = KinesisSource::new(cfg).await.map_err(|e| {
                    krishiv_api::KrishivError::Runtime {
                        message: e.to_string(),
                    }
                })?;
                let mut collected = Vec::new();
                for _ in 0..max_batches {
                    match src.next_batch().await {
                        Ok(Some(batch)) => collected.push(batch),
                        Ok(None) => break,
                        Err(e) => {
                            return Err(krishiv_api::KrishivError::Runtime {
                                message: e.to_string(),
                            });
                        }
                    }
                }
                inner.register_record_batches(&name, collected)?;
                Ok::<_, krishiv_api::KrishivError>(())
            })
        })
        .map_err(crate::errors::map_krishiv_error)?;
        let escaped = stream_name.replace('"', "\"\"");
        streaming_df_from_sql(session, format!("SELECT * FROM \"{escaped}\""), "", 0)
    }
}

// ── G14: Pulsar source ────────────────────────────────────────────────────────

/// Read batches from an Apache Pulsar topic.
///
/// **Feature gate**: requires the `pulsar` feature (`pip install krishiv[pulsar]`).
///
/// Reads up to `max_batches` record batches from the specified topic and
/// registers them as an in-memory stream named after the `topic`.
///
/// Schema: `topic Utf8`, `partition_key Utf8 (nullable)`, `publish_time_ms Int64`,
/// `data Binary`.
#[pyfunction]
#[pyo3(signature = (session, broker_url, topic, *, subscription="krishiv-default", max_batches=10, batch_size=100, start_position="earliest"))]
#[allow(clippy::too_many_arguments)] // keyword-only pyfunction surface
pub fn read_pulsar(
    py: Python<'_>,
    session: &PySession,
    broker_url: String,
    topic: String,
    subscription: &str,
    max_batches: usize,
    batch_size: usize,
    start_position: &str,
) -> PyResult<PyStreamingDataFrame> {
    #[cfg(not(feature = "pulsar"))]
    {
        let _ = (
            py,
            session,
            broker_url,
            topic,
            subscription,
            max_batches,
            batch_size,
            start_position,
        );
        Err(ConnectorError::new_err(
            "Pulsar support requires the `pulsar` feature (pip install krishiv[pulsar])",
        ))
    }
    #[cfg(feature = "pulsar")]
    {
        use krishiv_connectors::pulsar_connector::{PulsarConfig, PulsarSource};

        let earliest = match start_position {
            "earliest" | "Earliest" => true,
            "latest" | "Latest" => false,
            other => {
                return Err(ConnectorError::new_err(format!(
                    "unknown start_position '{other}'; expected earliest or latest"
                )));
            }
        };
        let cfg = PulsarConfig::new(&broker_url, &topic)
            .with_subscription(subscription)
            .with_start_at_earliest(earliest);
        let inner = session.inner.clone();
        let name = topic.clone();
        py.detach(move || {
            crate::session::block_on_async(async move {
                let mut src = PulsarSource::connect(cfg).await.map_err(|e| {
                    krishiv_api::KrishivError::Runtime {
                        message: e.to_string(),
                    }
                })?;
                let mut collected = Vec::new();
                for _ in 0..max_batches {
                    match src.next_batch(batch_size).await {
                        Ok(Some(batch)) => collected.push(batch),
                        Ok(None) => break,
                        Err(e) => {
                            return Err(krishiv_api::KrishivError::Runtime {
                                message: e.to_string(),
                            });
                        }
                    }
                }
                inner.register_record_batches(&name, collected)
            })
        })
        .map_err(crate::errors::map_krishiv_error)?;

        let escaped = topic.replace('"', "\"\"");
        streaming_df_from_sql(session, format!("SELECT * FROM \"{escaped}\""), "", 0)
    }
}

// ── Registry-generic source (mirror of sinks::ConnectorSink) ─────────────────

/// Registry-dispatched source — reads Arrow record batches through **any**
/// connector source driver registered in the engine's one connector registry.
///
/// The mirror of `krishiv.sinks.ConnectorSink`: a Python job reaches whatever
/// sources the build registers (`parquet`, `csv`, `avro`, `s3`, `jdbc`,
/// `delta`, `hudi`, `iceberg`, …) without a new pyfunction per kind. An
/// unregistered kind raises the registry's own error.
///
/// ```python
/// from krishiv import ConnectorSource
/// src = ConnectorSource("csv", {"path": "/tmp/in.csv"})
/// batches = src.read_batches()
/// ```
#[pyclass(name = "ConnectorSource")]
pub struct PyConnectorSource {
    kind: String,
    name: String,
    options: std::collections::BTreeMap<String, String>,
}

#[pymethods]
impl PyConnectorSource {
    #[new]
    #[pyo3(signature = (kind, options = None, name = None))]
    pub fn new(
        kind: String,
        options: Option<std::collections::BTreeMap<String, String>>,
        name: Option<String>,
    ) -> Self {
        Self {
            kind,
            name: name.unwrap_or_else(|| String::from("python-connector-source")),
            options: options.unwrap_or_default(),
        }
    }

    #[getter]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Read batches through the registered source driver.
    ///
    /// `max_batches` bounds the read. It is required for unbounded sources
    /// (streams have no end, so an uncapped read would never return) and
    /// optional for bounded ones, which read to exhaustion by default.
    #[pyo3(signature = (max_batches = None))]
    // Sync-over-async boundary (same contract as the sinks module): the
    // synchronous Python surface bridges to the async connector core.
    #[allow(clippy::disallowed_methods)]
    pub fn read_batches(
        &self,
        py: Python<'_>,
        max_batches: Option<usize>,
    ) -> PyResult<Vec<crate::batch::PyBatch>> {
        use krishiv_common::async_util::block_on;
        use krishiv_connectors::{ConnectorConfig, default_registry};

        let mut config = ConnectorConfig::new(&self.name, &self.kind);
        for (key, value) in &self.options {
            config = config.with_property(key, value);
        }

        let batches = py.detach(move || {
            block_on(async move {
                let registry = default_registry();
                let mut src = registry.open_source(&config).await.map_err(|e| {
                    PyRuntimeError::new_err(format!("connector source open ({}): {e}", config.kind))
                })?;
                if max_batches.is_none() && !src.capabilities().is_bounded() {
                    return Err(PyRuntimeError::new_err(format!(
                        "connector source ({}) is unbounded; pass max_batches to bound the read",
                        config.kind
                    )));
                }
                let cap = max_batches.unwrap_or(usize::MAX);
                let mut out = Vec::new();
                while out.len() < cap {
                    match src.read_batch_dyn().await.map_err(|e| {
                        PyRuntimeError::new_err(format!(
                            "connector source read ({}): {e}",
                            config.kind
                        ))
                    })? {
                        Some(batch) => out.push(batch),
                        None => break,
                    }
                }
                Ok::<_, PyErr>(out)
            })
        })?;
        Ok(batches
            .into_iter()
            .map(crate::batch::PyBatch::from_record_batch)
            .collect())
    }

    pub fn __repr__(&self) -> String {
        format!(
            "ConnectorSource(kind={:?}, options={} keys)",
            self.kind,
            self.options.len()
        )
    }
}
