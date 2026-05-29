//! Sink configuration types and `krishiv.sinks` submodule.

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
/// Kafka sink configuration.
///
/// **Note**: This is a configuration descriptor only. It does not establish a broker
/// connection or produce messages. Wire it into a pipeline via `PyRelation.sink_to()`
/// (not yet implemented) or use the `kafka` feature connector directly.
///
/// **Feature gate**: Constructing this object succeeds even when the `kafka` feature
/// is not compiled in, but actual message production requires the feature.
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

    pub fn __repr__(&self) -> String {
        format!(
            "KafkaSink(topic={:?}, bootstrap={})",
            self.topic, self.bootstrap_servers
        )
    }
}

#[pyclass(name = "IcebergSink")]
/// Iceberg sink configuration.
///
/// **Note**: This is a configuration descriptor only. It does not establish a connection
/// or write data to an Iceberg table. Wire it into a pipeline via `PyRelation.sink_to()`
/// (not yet implemented) or use the `iceberg` feature connector directly.
///
/// **Feature gate**: Constructing this object succeeds even when the `iceberg` feature
/// is not compiled in, but actual message production requires the feature.
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
