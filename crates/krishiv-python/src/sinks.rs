//! Sink configuration types and `krishiv.sinks` submodule.

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
                batches.into_iter().map(|b| b.into_inner()).collect();
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
            use krishiv_lakehouse::IcebergFsTable;
            use std::path::PathBuf;

            let records: Vec<arrow::record_batch::RecordBatch> =
                batches.into_iter().map(|b| b.into_inner()).collect();
            if records.is_empty() {
                return Ok(0);
            }
            let total_rows: usize = records.iter().map(|b| b.num_rows()).sum();
            let base = PathBuf::from(&self.catalog);
            let tbl = IcebergFsTable::new(&base, self.table.clone(), records[0].schema())
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

pub fn register_sinks_module(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let sinks = PyModule::new(py, "sinks")?;
    sinks.add_class::<PyParquetSink>()?;
    sinks.add_class::<PyKafkaSink>()?;
    sinks.add_class::<PyIcebergSink>()?;
    sinks.add_function(wrap_pyfunction!(sinks_parquet, &sinks)?)?;
    sinks.add_function(wrap_pyfunction!(sinks_kafka, &sinks)?)?;
    sinks.add_function(wrap_pyfunction!(sinks_iceberg, &sinks)?)?;
    parent.add_submodule(&sinks)?;
    Ok(())
}
