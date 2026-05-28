//! R18 lakehouse Python bindings.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::{PyDataFrame, PySession, RUNTIME};

#[pyfunction]
#[pyo3(signature = (session, path, version=None))]
pub fn read_delta(
    session: &PySession,
    path: String,
    version: Option<i64>,
) -> PyResult<PyDataFrame> {
    RUNTIME
        .block_on(session.inner.read_delta_async(path, version))
        .map(|df| PyDataFrame { inner: df })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (session, path, query_type="snapshot", begin_instant=None))]
pub fn read_hudi(
    session: &PySession,
    path: String,
    query_type: &str,
    begin_instant: Option<String>,
) -> PyResult<PyDataFrame> {
    let qt = match query_type.to_lowercase().as_str() {
        "incremental" => krishiv_lakehouse::HudiQueryType::Incremental,
        _ => krishiv_lakehouse::HudiQueryType::Snapshot,
    };
    RUNTIME
        .block_on(
            session
                .inner
                .read_hudi_async(path, qt, begin_instant.as_deref()),
        )
        .map(|df| PyDataFrame { inner: df })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

#[pyclass(name = "HudiWriteResult")]
pub struct PyHudiWriteResult {
    inner: krishiv_lakehouse::HudiWriteResult,
}

#[pymethods]
impl PyHudiWriteResult {
    #[getter]
    pub fn instant(&self) -> String {
        self.inner.instant.clone()
    }

    #[getter]
    pub fn rows_inserted(&self) -> u64 {
        self.inner.rows_inserted
    }

    #[getter]
    pub fn rows_updated(&self) -> u64 {
        self.inner.rows_updated
    }

    #[getter]
    pub fn snapshot_rows(&self) -> u64 {
        self.inner.snapshot_rows
    }
}

#[pyfunction]
pub fn write_hudi_append(
    session: &PySession,
    path: String,
    dataframe: &PyDataFrame,
) -> PyResult<PyHudiWriteResult> {
    RUNTIME
        .block_on(
            session
                .inner
                .write_hudi_append_async(path, &dataframe.inner),
        )
        .map(|inner| PyHudiWriteResult { inner })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

#[pyfunction]
pub fn write_hudi_upsert(
    session: &PySession,
    path: String,
    key_column: String,
    dataframe: &PyDataFrame,
) -> PyResult<PyHudiWriteResult> {
    RUNTIME
        .block_on(
            session
                .inner
                .write_hudi_upsert_async(path, &key_column, &dataframe.inner),
        )
        .map(|inner| PyHudiWriteResult { inner })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (url, subject, format="avro"))]
pub fn schema_registry_confluent(
    url: String,
    subject: String,
    format: &str,
) -> PyResult<PySchemaRegistryConfig> {
    let fmt = match format.to_lowercase().as_str() {
        "protobuf" => krishiv_schema_registry::RegistryFormat::Protobuf,
        "json" => krishiv_schema_registry::RegistryFormat::Json,
        _ => krishiv_schema_registry::RegistryFormat::Avro,
    };
    Ok(PySchemaRegistryConfig {
        inner: krishiv_schema_registry::SchemaRegistryConfig {
            url,
            subject,
            format: fmt,
        },
    })
}

#[pyclass(name = "SchemaRegistryConfig")]
pub struct PySchemaRegistryConfig {
    #[allow(dead_code)]
    pub(crate) inner: krishiv_schema_registry::SchemaRegistryConfig,
}

#[pyclass(name = "GlueCatalog")]
pub struct PyGlueCatalog;

#[pymethods]
impl PyGlueCatalog {
    #[new]
    pub fn new(region: String, database: String, rest_url: String) -> Self {
        let _ = (region, database, rest_url);
        Self
    }
}

#[pyclass(name = "NessieCatalog")]
pub struct PyNessieCatalog;

#[pymethods]
impl PyNessieCatalog {
    #[new]
    #[pyo3(signature = (uri, reference="main"))]
    pub fn new(uri: String, reference: &str) -> Self {
        let _ = (uri, reference);
        Self
    }
}

#[pyclass(name = "IcebergRestCatalog")]
pub struct PyIcebergRestCatalog;

#[pymethods]
impl PyIcebergRestCatalog {
    #[new]
    #[pyo3(signature = (url, warehouse=None))]
    pub fn new(url: String, warehouse: Option<String>) -> Self {
        let _ = (url, warehouse);
        Self
    }
}
