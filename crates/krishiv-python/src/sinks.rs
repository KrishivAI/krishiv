//! Sink configuration types and `krishiv.sinks` submodule.

// Deliberate sync-over-async boundary module (Phase 51 async contract):
// block_on here bridges a synchronous public surface to the async core.
#![allow(clippy::disallowed_methods)]

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

#[pyclass(name = "ParquetSink")]
pub struct PyParquetSink {
    path: String,
}

#[pymethods]
impl PyParquetSink {
    #[new]
    pub fn new(path: String) -> Self {
        Self { path }
    }

    #[getter]
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn __repr__(&self) -> String {
        format!("ParquetSink(path={:?})", self.path)
    }
}

#[pyclass(name = "KafkaSink")]
/// Kafka sink — produces Arrow record batches to a Kafka topic.
///
/// Each record batch is serialized as Arrow IPC and sent as a single message.
/// Requires the `kafka` Cargo feature; raises `RuntimeError` when called
/// without it.
pub struct PyKafkaSink {
    topic: String,
    bootstrap_servers: String,
}

#[pymethods]
impl PyKafkaSink {
    #[new]
    pub fn new(topic: String, bootstrap_servers: String) -> Self {
        Self {
            topic,
            bootstrap_servers,
        }
    }

    #[getter]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    #[getter]
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Write a list of PyBatch objects to the configured Kafka topic as JSON rows.
    ///
    /// Requires the `kafka` Cargo feature.
    pub fn write_batches(&self, batches: Vec<crate::batch::PyBatch>) -> PyResult<usize> {
        #[cfg(feature = "kafka")]
        {
            use krishiv_common::async_util::block_on;
            use krishiv_connectors::kafka::{KafkaConfig, KafkaSink};
            use krishiv_connectors::sink::Sink as _;

            let records: Vec<arrow::record_batch::RecordBatch> =
                batches.iter().map(|b| b.record_batch().clone()).collect();
            if records.is_empty() {
                return Ok(0);
            }
            let total_rows: usize = records.iter().map(|b| b.num_rows()).sum();
            let cfg = KafkaConfig {
                bootstrap_servers: self.bootstrap_servers.clone(),
                topic: self.topic.clone(),
                group_id: String::from("krishiv-python"),
                auto_commit_interval_ms: None,
                security_protocol: None,
                ssl_ca_location: None,
                ssl_certificate_location: None,
                ssl_key_location: None,
                ssl_key_password: None,
                sasl_username: None,
                sasl_password: None,
                sasl_mechanisms: None,
                enable_idempotence: None,
                transactional_id: None,
                decode_columns: None,
            };
            let mut sink = KafkaSink::new(cfg)
                .map_err(|e| PyRuntimeError::new_err(format!("kafka sink init: {e}")))?;
            block_on(async {
                for batch in records {
                    sink.write_batch(batch).await?;
                }
                sink.flush().await
            })
            .map_err(|e| PyRuntimeError::new_err(format!("kafka write: {e}")))?;
            Ok(total_rows)
        }
        #[cfg(not(feature = "kafka"))]
        {
            let _ = batches;
            Err(PyRuntimeError::new_err(
                "KafkaSink.write_batches requires the 'kafka' feature; \
                 rebuild with: maturin develop --features kafka",
            ))
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "KafkaSink(topic={:?}, bootstrap={})",
            self.topic, self.bootstrap_servers
        )
    }
}

#[pyclass(name = "IcebergSink")]
/// Iceberg sink — appends Arrow record batches to a local Iceberg table.
///
/// `catalog` is interpreted as a local filesystem base directory;
/// `table` is the namespace-qualified table name (e.g. `"db.events"`).
/// Requires the `iceberg` Cargo feature; raises `RuntimeError` when called
/// without it.
pub struct PyIcebergSink {
    catalog: String,
    table: String,
}

#[pymethods]
impl PyIcebergSink {
    #[new]
    pub fn new(catalog: String, table: String) -> Self {
        Self { catalog, table }
    }

    #[getter]
    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    #[getter]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Append a list of PyBatch objects to the configured Iceberg table.
    ///
    /// `catalog` is the local filesystem base directory; `table` is the
    /// dot-separated table reference (e.g. `"db.events"`).
    /// Requires the `iceberg` Cargo feature.
    pub fn write_batches(&self, batches: Vec<crate::batch::PyBatch>) -> PyResult<usize> {
        #[cfg(feature = "iceberg")]
        {
            use krishiv_common::async_util::block_on;
            use krishiv_connectors::lakehouse::{
                IcebergFsTable, IcebergTableRef, LakehouseTable, schema_version_from_arrow,
            };
            use std::path::PathBuf;

            let records: Vec<arrow::record_batch::RecordBatch> =
                batches.iter().map(|b| b.record_batch().clone()).collect();
            if records.is_empty() {
                return Ok(0);
            }
            let total_rows: usize = records.iter().map(|b| b.num_rows()).sum();
            let base = PathBuf::from(&self.catalog);
            // Parse the dotted table reference (`db.events`, `cat.ns.name`, or a
            // bare `name`) into a structured `IcebergTableRef`.
            let parts: Vec<&str> = self.table.split('.').filter(|s| !s.is_empty()).collect();
            let (namespace, name) = match parts.as_slice() {
                [] => ("default".to_string(), self.table.clone()),
                [only] => ("default".to_string(), (*only).to_string()),
                [ns @ .., last] => (ns.join("."), (*last).to_string()),
            };
            let table_ref = IcebergTableRef::new("default", namespace, name);
            let Some(first) = records.first() else {
                return Ok(0);
            };
            let schema_version = schema_version_from_arrow(first.schema().as_ref(), None)
                .map_err(|e| PyRuntimeError::new_err(format!("iceberg schema: {e}")))?;
            let tbl = IcebergFsTable::new(&base, table_ref, schema_version)
                .map_err(|e| PyRuntimeError::new_err(format!("iceberg open: {e}")))?;
            block_on(tbl.append(records))
                .map_err(|e| PyRuntimeError::new_err(format!("iceberg append: {e}")))?;
            Ok(total_rows)
        }
        #[cfg(not(feature = "iceberg"))]
        {
            let _ = batches;
            Err(PyRuntimeError::new_err(
                "IcebergSink.write_batches requires the 'iceberg' feature; \
                 rebuild with: maturin develop --features iceberg",
            ))
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "IcebergSink(catalog={:?}, table={:?})",
            self.catalog, self.table
        )
    }
}

/// Cassandra / ScyllaDB sink — writes Arrow record batches to a Cassandra table.
///
/// Each batch row becomes one CQL INSERT inside an UNLOGGED BATCH.
/// Requires the `cassandra` Cargo feature.
#[pyclass(name = "CassandraSink")]
pub struct PyCassandraSink {
    node: String,
    keyspace: String,
    table: String,
    consistency: Option<String>,
}

#[pymethods]
impl PyCassandraSink {
    #[new]
    #[pyo3(signature = (node, keyspace, table, consistency=None))]
    pub fn new(node: String, keyspace: String, table: String, consistency: Option<String>) -> Self {
        Self {
            node,
            keyspace,
            table,
            consistency,
        }
    }

    pub fn write_batches(&self, batches: Vec<crate::batch::PyBatch>) -> PyResult<usize> {
        #[cfg(feature = "cassandra")]
        {
            use krishiv_common::async_util::block_on;
            use krishiv_connectors::cassandra_sink::{CassandraConfig, CassandraSink};

            let records: Vec<arrow::record_batch::RecordBatch> =
                batches.iter().map(|b| b.record_batch().clone()).collect();
            if records.is_empty() {
                return Ok(0);
            }
            let total_rows: usize = records.iter().map(|b| b.num_rows()).sum();
            let mut cfg = CassandraConfig::new(&self.node, &self.keyspace, &self.table);
            if let Some(level) = &self.consistency {
                cfg = cfg
                    .with_consistency_name(level)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            }
            let sink = block_on(CassandraSink::connect(cfg))
                .map_err(|e| PyRuntimeError::new_err(format!("cassandra sink init: {e}")))?;
            block_on(async {
                for batch in &records {
                    sink.write_batch(batch).await?;
                }
                Ok::<_, krishiv_connectors::error::ConnectorError>(())
            })
            .map_err(|e| PyRuntimeError::new_err(format!("cassandra write: {e}")))?;
            Ok(total_rows)
        }
        #[cfg(not(feature = "cassandra"))]
        {
            let _ = batches;
            Err(PyRuntimeError::new_err(
                "CassandraSink.write_batches requires the 'cassandra' feature; \
                 rebuild with: maturin develop --features cassandra",
            ))
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "CassandraSink(node={:?}, keyspace={:?}, table={:?}, consistency={:?})",
            self.node, self.keyspace, self.table, self.consistency
        )
    }
}

/// Elasticsearch / OpenSearch sink — bulk-indexes Arrow record batches as JSON documents.
///
/// Requires the `elasticsearch` Cargo feature.
#[pyclass(name = "ElasticsearchSink")]
pub struct PyElasticsearchSink {
    url: String,
    index: String,
}

#[pymethods]
impl PyElasticsearchSink {
    #[new]
    pub fn new(url: String, index: String) -> Self {
        Self { url, index }
    }

    pub fn write_batches(&self, batches: Vec<crate::batch::PyBatch>) -> PyResult<usize> {
        #[cfg(feature = "elasticsearch")]
        {
            use krishiv_common::async_util::block_on;
            use krishiv_connectors::elasticsearch_sink::{ElasticsearchConfig, ElasticsearchSink};

            let records: Vec<arrow::record_batch::RecordBatch> =
                batches.iter().map(|b| b.record_batch().clone()).collect();
            if records.is_empty() {
                return Ok(0);
            }
            let total_rows: usize = records.iter().map(|b| b.num_rows()).sum();
            let cfg = ElasticsearchConfig::new(&self.url, &self.index);
            let sink = block_on(ElasticsearchSink::connect(cfg))
                .map_err(|e| PyRuntimeError::new_err(format!("elasticsearch sink init: {e}")))?;
            block_on(async {
                for batch in &records {
                    sink.write_batch(batch).await?;
                }
                Ok::<_, krishiv_connectors::error::ConnectorError>(())
            })
            .map_err(|e| PyRuntimeError::new_err(format!("elasticsearch write: {e}")))?;
            Ok(total_rows)
        }
        #[cfg(not(feature = "elasticsearch"))]
        {
            let _ = batches;
            Err(PyRuntimeError::new_err(
                "ElasticsearchSink.write_batches requires the 'elasticsearch' feature; \
                 rebuild with: maturin develop --features elasticsearch",
            ))
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "ElasticsearchSink(url={:?}, index={:?})",
            self.url, self.index
        )
    }
}

/// HBase sink — writes Arrow record batches to an HBase table via Thrift.
///
/// `host` is the HBase Thrift server address (e.g. `"localhost:9090"`).
/// `column_family` is the HBase column family (e.g. `"cf"`).
/// Requires the `hbase` Cargo feature.
#[pyclass(name = "HBaseSink")]
pub struct PyHBaseSink {
    host: String,
    table: String,
    column_family: String,
}

#[pymethods]
impl PyHBaseSink {
    #[new]
    pub fn new(host: String, table: String, column_family: String) -> Self {
        Self {
            host,
            table,
            column_family,
        }
    }

    pub fn write_batches(&self, batches: Vec<crate::batch::PyBatch>) -> PyResult<usize> {
        #[cfg(feature = "hbase")]
        {
            use krishiv_common::async_util::block_on;
            use krishiv_connectors::hbase_connector::{HBaseConfig, HBaseSink};

            let records: Vec<arrow::record_batch::RecordBatch> =
                batches.iter().map(|b| b.record_batch().clone()).collect();
            if records.is_empty() {
                return Ok(0);
            }
            let total_rows: usize = records.iter().map(|b| b.num_rows()).sum();
            let cfg = HBaseConfig::new(&self.host, &self.table, &self.column_family);
            let sink = block_on(HBaseSink::connect(cfg))
                .map_err(|e| PyRuntimeError::new_err(format!("hbase sink init: {e}")))?;
            block_on(async {
                for batch in &records {
                    sink.write_batch(batch).await?;
                }
                Ok::<_, krishiv_connectors::error::ConnectorError>(())
            })
            .map_err(|e| PyRuntimeError::new_err(format!("hbase write: {e}")))?;
            Ok(total_rows)
        }
        #[cfg(not(feature = "hbase"))]
        {
            let _ = batches;
            Err(PyRuntimeError::new_err(
                "HBaseSink.write_batches requires the 'hbase' feature; \
                 rebuild with: maturin develop --features hbase",
            ))
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "HBaseSink(host={:?}, table={:?}, column_family={:?})",
            self.host, self.table, self.column_family
        )
    }
}

#[pyfunction]
#[pyo3(name = "parquet")]
fn sinks_parquet(path: String) -> PyParquetSink {
    PyParquetSink::new(path)
}

#[pyfunction]
#[pyo3(name = "kafka")]
fn sinks_kafka(topic: String, bootstrap_servers: String) -> PyKafkaSink {
    PyKafkaSink::new(topic, bootstrap_servers)
}

#[pyfunction]
#[pyo3(name = "iceberg")]
fn sinks_iceberg(catalog: String, table: String) -> PyIcebergSink {
    PyIcebergSink::new(catalog, table)
}

#[pyfunction]
#[pyo3(name = "cassandra")]
#[pyo3(signature = (node, keyspace, table, consistency=None))]
fn sinks_cassandra(
    node: String,
    keyspace: String,
    table: String,
    consistency: Option<String>,
) -> PyCassandraSink {
    PyCassandraSink::new(node, keyspace, table, consistency)
}

#[pyfunction]
#[pyo3(name = "elasticsearch")]
fn sinks_elasticsearch(url: String, index: String) -> PyElasticsearchSink {
    PyElasticsearchSink::new(url, index)
}

#[pyfunction]
#[pyo3(name = "hbase")]
fn sinks_hbase(host: String, table: String, column_family: String) -> PyHBaseSink {
    PyHBaseSink::new(host, table, column_family)
}

pub fn register_sinks_module(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let sinks = PyModule::new(py, "sinks")?;
    sinks.add_class::<PyParquetSink>()?;
    sinks.add_class::<PyKafkaSink>()?;
    sinks.add_class::<PyIcebergSink>()?;
    sinks.add_class::<PyCassandraSink>()?;
    sinks.add_class::<PyElasticsearchSink>()?;
    sinks.add_class::<PyHBaseSink>()?;
    sinks.add_function(wrap_pyfunction!(sinks_parquet, &sinks)?)?;
    sinks.add_function(wrap_pyfunction!(sinks_kafka, &sinks)?)?;
    sinks.add_function(wrap_pyfunction!(sinks_iceberg, &sinks)?)?;
    sinks.add_function(wrap_pyfunction!(sinks_cassandra, &sinks)?)?;
    sinks.add_function(wrap_pyfunction!(sinks_elasticsearch, &sinks)?)?;
    sinks.add_function(wrap_pyfunction!(sinks_hbase, &sinks)?)?;
    parent.add_submodule(&sinks)?;
    Ok(())
}

// ── Registry-generic sink (#197 python_sink leg) ─────────────────────────────

/// Registry-dispatched sink — writes Arrow record batches through **any**
/// connector sink driver registered in the engine's one connector registry.
///
/// The hand-written pyclasses above each bind a single connector; this one
/// binds the registry itself, so a Python job reaches whatever sinks the build
/// registers (`csv`, `avro`, `s3`, `delta`, `hudi`, `jdbc-sink`, …) without a
/// new pyclass per kind. Availability is decided by the build's connector
/// features: an unregistered kind raises the registry's own error rather than
/// a surface-local "unsupported" message.
///
/// ```python
/// from krishiv.sinks import ConnectorSink
/// sink = ConnectorSink("csv", {"path": "/tmp/out.csv"})
/// sink.write_batches(batches)
/// ```
///
/// Delivery semantics are the driver's own and are at-least-once for the
/// non-transactional drivers reached here: `write_batches` writes then flushes,
/// and a failure mid-way can leave earlier batches already written. Exactly-once
/// output requires the checkpoint-aligned two-phase-commit sinks, which are
/// driven by the streaming runtime, not by this synchronous surface.
#[pyclass(name = "ConnectorSink")]
pub struct PyConnectorSink {
    kind: String,
    name: String,
    options: std::collections::BTreeMap<String, String>,
}

#[pymethods]
impl PyConnectorSink {
    #[new]
    #[pyo3(signature = (kind, options = None, name = None))]
    pub fn new(
        kind: String,
        options: Option<std::collections::BTreeMap<String, String>>,
        name: Option<String>,
    ) -> Self {
        Self {
            kind,
            name: name.unwrap_or_else(|| String::from("python-connector-sink")),
            options: options.unwrap_or_default(),
        }
    }

    #[getter]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Write a list of `Batch` objects through the registered sink driver.
    ///
    /// Returns the number of rows written. Flush failures fail the call: the
    /// flush is what makes the output durable, so a silent success here would
    /// acknowledge unwritten rows.
    pub fn write_batches(&self, batches: Vec<crate::batch::PyBatch>) -> PyResult<usize> {
        use krishiv_common::async_util::block_on;
        use krishiv_connectors::{ConnectorConfig, default_registry};

        let records: Vec<arrow::record_batch::RecordBatch> =
            batches.iter().map(|b| b.record_batch().clone()).collect();
        let total_rows: usize = records.iter().map(|b| b.num_rows()).sum();

        let mut config = ConnectorConfig::new(&self.name, &self.kind);
        for (key, value) in &self.options {
            config = config.with_property(key, value);
        }

        block_on(async move {
            let registry = default_registry();
            let mut sink = registry.open_sink(&config).await.map_err(|e| {
                PyRuntimeError::new_err(format!("connector sink open ({}): {e}", config.kind))
            })?;
            for batch in records {
                sink.write_batch_dyn(batch).await.map_err(|e| {
                    PyRuntimeError::new_err(format!("connector sink write ({}): {e}", config.kind))
                })?;
            }
            sink.flush_dyn().await.map_err(|e| {
                PyRuntimeError::new_err(format!("connector sink flush ({}): {e}", config.kind))
            })?;
            Ok::<(), PyErr>(())
        })?;
        Ok(total_rows)
    }

    pub fn __repr__(&self) -> String {
        format!(
            "ConnectorSink(kind={:?}, options={} keys)",
            self.kind,
            self.options.len()
        )
    }
}
