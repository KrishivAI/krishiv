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
#[doc = "**Alpha**: The config struct is accepted but not yet consumed by any source or sink in the Python API. Schema fetching is deferred."]
pub struct PySchemaRegistryConfig {
    #[allow(dead_code)]
    pub(crate) inner: krishiv_schema_registry::SchemaRegistryConfig,
}

use std::sync::Arc;
use krishiv_catalog::iceberg_rest::{
    GlueRestCatalog, IcebergCatalogClient, IcebergTableId, NessieCatalog, GenericRestCatalog,
    RestCatalogConfig,
};

/// AWS Glue Iceberg catalog (R18 S3.1).
///
/// Connects to the Glue REST-compatible endpoint and exposes table operations
/// (`list_tables`, `load_table_metadata`). Requires valid AWS credentials in
/// the environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, etc.).
#[pyclass(name = "GlueCatalog")]
pub struct PyGlueCatalog {
    inner: Arc<GlueRestCatalog>,
}

#[pymethods]
impl PyGlueCatalog {
    #[new]
    pub fn new(region: String, database: String, rest_url: String) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(GlueRestCatalog::new(region, database, rest_url)),
        })
    }

    /// List all table names in `namespace`.
    pub fn list_tables(&self, namespace: String) -> PyResult<Vec<String>> {
        RUNTIME
            .block_on(self.inner.list_tables(&namespace))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Load table metadata for `namespace.table_name` as a JSON string.
    pub fn load_table_metadata(&self, namespace: String, table_name: String) -> PyResult<String> {
        let table_id = IcebergTableId { namespace, name: table_name };
        RUNTIME
            .block_on(self.inner.load_table_metadata(&table_id))
            .map(|v| v.to_string())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

/// Nessie Iceberg catalog (R18 S3.1).
///
/// Connects to a Project Nessie server via its REST API. Wraps a
/// `GenericRestCatalog` with Nessie-specific URL construction.
#[pyclass(name = "NessieCatalog")]
pub struct PyNessieCatalog {
    inner: Arc<NessieCatalog>,
}

#[pymethods]
impl PyNessieCatalog {
    #[new]
    #[pyo3(signature = (uri, reference="main"))]
    pub fn new(uri: String, reference: &str) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(NessieCatalog::new(uri, reference)),
        })
    }

    /// List all table names in `namespace`.
    pub fn list_tables(&self, namespace: String) -> PyResult<Vec<String>> {
        RUNTIME
            .block_on(self.inner.list_tables(&namespace))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Load table metadata for `namespace.table_name` as a JSON string.
    pub fn load_table_metadata(&self, namespace: String, table_name: String) -> PyResult<String> {
        let table_id = IcebergTableId { namespace, name: table_name };
        RUNTIME
            .block_on(self.inner.load_table_metadata(&table_id))
            .map(|v| v.to_string())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

/// Generic Iceberg REST catalog (R18 S3.1).
///
/// Compatible with any Iceberg REST catalog implementation (Tabular, AWS Glue
/// via generic REST, self-hosted). See the Iceberg REST catalog specification
/// for the expected API surface.
#[pyclass(name = "IcebergRestCatalog")]
pub struct PyIcebergRestCatalog {
    inner: Arc<GenericRestCatalog>,
}

#[pymethods]
impl PyIcebergRestCatalog {
    #[new]
    #[pyo3(signature = (url, warehouse=None))]
    pub fn new(url: String, warehouse: Option<String>) -> PyResult<Self> {
        let config = RestCatalogConfig {
            base_url: url,
            warehouse,
            prefix: "v1".to_string(),
            bearer_token: None,
        };
        Ok(Self {
            inner: Arc::new(GenericRestCatalog::new(config)),
        })
    }

    /// List all table names in `namespace`.
    pub fn list_tables(&self, namespace: String) -> PyResult<Vec<String>> {
        RUNTIME
            .block_on(self.inner.list_tables(&namespace))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Load table metadata for `namespace.table_name` as a JSON string.
    pub fn load_table_metadata(&self, namespace: String, table_name: String) -> PyResult<String> {
        let table_id = IcebergTableId { namespace, name: table_name };
        RUNTIME
            .block_on(self.inner.load_table_metadata(&table_id))
            .map(|v| v.to_string())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

#[cfg(test)]
mod catalog_tests {
    use super::*;

    #[test]
    fn glue_catalog_new_stores_region_and_database() {
        let cat = PyGlueCatalog::new(
            "us-east-1".into(),
            "analytics".into(),
            "http://glue.us-east-1.amazonaws.com".into(),
        )
        .unwrap();
        assert_eq!(cat.inner.region, "us-east-1");
        assert_eq!(cat.inner.database, "analytics");
    }

    #[test]
    fn nessie_catalog_new_succeeds() {
        let cat =
            PyNessieCatalog::new("http://nessie.example.com/api".into(), "main").unwrap();
        // Constructor must not panic or error — no live server needed.
        let _ = cat;
    }

    #[test]
    fn iceberg_rest_catalog_new_succeeds() {
        let cat =
            PyIcebergRestCatalog::new("http://catalog.example.com".into(), Some("my_warehouse".into()))
                .unwrap();
        let _ = cat;
    }

    #[test]
    fn iceberg_rest_catalog_new_without_warehouse_succeeds() {
        let cat = PyIcebergRestCatalog::new("http://catalog.example.com".into(), None).unwrap();
        let _ = cat;
    }
}
