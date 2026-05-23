//! Sink configuration types.

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
