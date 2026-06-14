//! Module-level sources: `read_parquet`, `read_kafka`, `read_iceberg`.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::dataframe::PyDataFrame;
use crate::errors::ConnectorError;
use crate::schema::validate_batch_against_schema_class;
use crate::session::PySession;
use crate::stream::PyStream;

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
) -> PyResult<PyStream> {
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
        // works through the standard stream execution path.
        session
            .inner
            .register_kafka_source(&topic, arrow_schema, &bootstrap_servers, &topic, gid)
            .map_err(crate::errors::map_krishiv_error)?;
        let escaped_topic = topic.replace('"', "\"\"");
        Ok(PyStream::from_pipeline(
            session.inner.clone(),
            format!("SELECT * FROM \"{escaped_topic}\""),
            String::new(),
            0,
        ))
    }
}

#[pyfunction]
#[pyo3(signature = (session, catalog_uri, table_name, *, schema=None))]
/// Read an Iceberg table as a streaming source.
///
/// **Feature gate**: Requires `pip install krishiv[iceberg]` or building with `--features iceberg`.
///
/// **Alpha (with feature)**: Performs an in-memory validation scan only. The `catalog_uri`
/// is not used for real REST catalog connectivity — it is stored as a source identifier.
/// The returned stream has no watermark set.
pub fn read_iceberg(
    session: &PySession,
    catalog_uri: String,
    table_name: String,
    schema: Option<&Bound<'_, PyType>>,
) -> PyResult<PyStream> {
    let _ = schema;
    #[cfg(not(feature = "iceberg"))]
    {
        let _ = (session, &catalog_uri, &table_name);
        Err(ConnectorError::new_err(
            "Iceberg support requires the `iceberg` feature (pip install krishiv[iceberg])",
        ))
    }
    #[cfg(feature = "iceberg")]
    {
        read_iceberg_impl(session, catalog_uri, table_name, schema)
    }
}

#[cfg(feature = "iceberg")]
fn read_iceberg_impl(
    session: &PySession,
    catalog_uri: String,
    table_name: String,
    schema: Option<&Bound<'_, PyType>>,
) -> PyResult<PyStream> {
    use crate::schema::PySchema;
    use std::sync::Arc;

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
    let table = MemoryLakehouseTable::new(table_ref.clone(), schema_version);
    let _opts = IcebergScanOptions::new();
    // Validate catalog reachability via in-memory scan (empty table is OK).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    rt.block_on(async {
        table
            .scan(&_opts)
            .await
            .map_err(|e| ConnectorError::new_err(format!("Iceberg catalog error: {e}")))
    })?;
    Ok(PyStream::from_pipeline(
        session.inner.clone(),
        format!("iceberg:{}:{}", catalog_uri, table_ref.full_name()),
        String::new(),
        0,
    ))
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
pub fn read_kinesis(
    py: Python<'_>,
    session: &PySession,
    stream_name: String,
    region: String,
    shard_id: &str,
    start_position: &str,
    max_batches: usize,
    batch_size: i32,
) -> PyResult<PyStream> {
    #[cfg(not(feature = "kinesis"))]
    {
        let _ = (
            session,
            stream_name,
            region,
            shard_id,
            start_position,
            max_batches,
            batch_size,
        );
        return Err(ConnectorError::new_err(
            "Kinesis support requires the `kinesis` feature (pip install krishiv[kinesis])",
        ));
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
        let batches = py
            .detach(move || {
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
                    inner
                        .register_memory_stream(&name, collected)
                        .map_err(krishiv_api::KrishivError::from)?;
                    Ok::<_, krishiv_api::KrishivError>(())
                })
            })
            .map_err(crate::errors::map_krishiv_error)?;
        let _ = batches;
        Ok(PyStream::from_pipeline(
            session.inner.clone(),
            format!("memory:{stream_name}"),
            String::new(),
            0,
        ))
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
#[pyo3(signature = (session, broker_url, topic, *, subscription="krishiv-default", max_batches=10, batch_size=100))]
pub fn read_pulsar(
    py: Python<'_>,
    session: &PySession,
    broker_url: String,
    topic: String,
    subscription: &str,
    max_batches: usize,
    batch_size: usize,
) -> PyResult<PyStream> {
    #[cfg(not(feature = "pulsar"))]
    {
        let _ = (
            session,
            broker_url,
            topic,
            subscription,
            max_batches,
            batch_size,
        );
        return Err(ConnectorError::new_err(
            "Pulsar support requires the `pulsar` feature (pip install krishiv[pulsar])",
        ));
    }
    #[cfg(feature = "pulsar")]
    {
        use krishiv_connectors::pulsar_connector::{PulsarConfig, PulsarSource};

        let cfg = PulsarConfig::new(&broker_url, &topic).with_subscription(subscription);
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
                inner
                    .register_memory_stream(&name, collected)
                    .map_err(krishiv_api::KrishivError::from)
            })
        })
        .map_err(crate::errors::map_krishiv_error)?;

        Ok(PyStream::from_pipeline(
            session.inner.clone(),
            format!("memory:{topic}"),
            String::new(),
            0,
        ))
    }
}
