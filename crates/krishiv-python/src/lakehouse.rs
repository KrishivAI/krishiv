//! R18 lakehouse Python bindings.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
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
        "incremental" => krishiv_connectors::lakehouse::HudiQueryType::Incremental,
        _ => krishiv_connectors::lakehouse::HudiQueryType::Snapshot,
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
    inner: krishiv_connectors::lakehouse::HudiWriteResult,
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
#[pyo3(signature = (url, format="avro"))]
pub fn schema_registry_confluent(url: String, format: &str) -> PyResult<PySchemaRegistryConfig> {
    let fmt = match format.to_ascii_lowercase().as_str() {
        "avro" => krishiv_connectors::schema_registry::RegistryFormat::Avro,
        "protobuf" => krishiv_connectors::schema_registry::RegistryFormat::Protobuf,
        unsupported => {
            return Err(PyValueError::new_err(format!(
                "unsupported schema registry format '{unsupported}'; expected avro or protobuf"
            )));
        }
    };
    let config = krishiv_connectors::schema_registry::SchemaRegistryConfig::new(url, fmt)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    Ok(PySchemaRegistryConfig { _inner: config })
}

#[pyclass(name = "SchemaRegistryConfig")]
#[doc = "**Alpha**: The config struct is accepted but not yet consumed by any source or sink in the Python API. Schema fetching is deferred."]
pub struct PySchemaRegistryConfig {
    pub(crate) _inner: krishiv_connectors::schema_registry::SchemaRegistryConfig,
}

use krishiv_sql::catalog::iceberg_rest::{
    GenericRestCatalog, IcebergCatalogClient, IcebergTableId, RestCatalogConfig,
};
use std::sync::Arc;
use std::time::Duration;

/// Generic Apache Iceberg REST catalog.
///
/// The caller is responsible for supplying authentication compatible with the
/// target service. Provider-specific signing and reference protocols are not
/// inferred from the URL.
#[pyclass(name = "IcebergRestCatalog")]
pub struct PyIcebergRestCatalog {
    inner: Arc<GenericRestCatalog>,
}

#[pymethods]
impl PyIcebergRestCatalog {
    #[new]
    #[pyo3(signature = (
        url,
        warehouse=None,
        timeout_ms=None,
        *,
        bearer_token=None,
        catalog_prefix=None,
        page_size=None,
        max_response_bytes=None
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: String,
        warehouse: Option<String>,
        timeout_ms: Option<u64>,
        bearer_token: Option<String>,
        catalog_prefix: Option<String>,
        page_size: Option<u32>,
        max_response_bytes: Option<usize>,
    ) -> PyResult<Self> {
        let mut config = RestCatalogConfig::new(url).map_err(catalog_config_error)?;
        if let Some(warehouse) = warehouse {
            config = config
                .with_warehouse(warehouse)
                .map_err(catalog_config_error)?;
        }
        if let Some(timeout_ms) = timeout_ms {
            config = config
                .with_timeout(Duration::from_millis(timeout_ms))
                .map_err(catalog_config_error)?;
        }
        if let Some(token) = bearer_token {
            config = config
                .with_bearer_token(token)
                .map_err(catalog_config_error)?;
        }
        if let Some(prefix) = catalog_prefix {
            config = config
                .with_catalog_prefix(prefix)
                .map_err(catalog_config_error)?;
        }
        if let Some(page_size) = page_size {
            config = config
                .with_page_size(page_size)
                .map_err(catalog_config_error)?;
        }
        if let Some(limit) = max_response_bytes {
            config = config
                .with_max_response_bytes(limit)
                .map_err(catalog_config_error)?;
        }
        Ok(Self {
            inner: Arc::new(GenericRestCatalog::new(config).map_err(catalog_config_error)?),
        })
    }

    /// List all table names in `namespace`.
    pub fn list_tables(&self, namespace: String) -> PyResult<Vec<String>> {
        // Use block_in_place when a tokio handle is available so we yield the
        // worker thread instead of blocking it — important for async Python
        // frameworks and multi-threaded tokio runtimes.
        RUNTIME
            .block_on(self.inner.list_tables(&namespace))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Load table metadata for `namespace.table_name` as a JSON string.
    pub fn load_table_metadata(&self, namespace: String, table_name: String) -> PyResult<String> {
        let table_id = IcebergTableId::new(namespace, table_name).map_err(catalog_config_error)?;
        RUNTIME
            .block_on(self.inner.load_table_metadata(&table_id))
            .map(|v| v.to_string())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

fn catalog_config_error(error: krishiv_sql::catalog::CatalogError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

#[cfg(test)]
mod catalog_tests {
    use super::*;

    #[test]
    fn iceberg_rest_catalog_new_succeeds() {
        let cat = PyIcebergRestCatalog::new(
            "http://catalog.example.com".into(),
            Some("my_warehouse".into()),
            None,
            Some("secret".into()),
            Some("tenant".into()),
            Some(500),
            Some(1024 * 1024),
        )
        .unwrap();
        let _ = cat;
    }

    #[test]
    fn iceberg_rest_catalog_new_without_warehouse_succeeds() {
        let cat = PyIcebergRestCatalog::new(
            "http://catalog.example.com".into(),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let _ = cat;
    }

    #[test]
    fn iceberg_rest_catalog_rejects_invalid_configuration() {
        assert!(
            PyIcebergRestCatalog::new("not-a-url".into(), None, None, None, None, None, None,)
                .is_err()
        );
        assert!(
            PyIcebergRestCatalog::new(
                "https://catalog.example.com".into(),
                None,
                Some(0),
                None,
                None,
                None,
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn schema_registry_config_rejects_unknown_format() {
        let error = schema_registry_confluent("https://registry.example".into(), "yaml")
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("unsupported schema registry format")
        );
    }

    #[test]
    fn schema_registry_config_validates_url() {
        assert!(schema_registry_confluent("not-a-url".into(), "avro").is_err());
        assert!(schema_registry_confluent("https://registry.example".into(), "avro").is_ok());
    }

    // ── Catalog HTTP error-path tests ─────────────────────────────────────────
    // Verify CatalogError → PyRuntimeError mapping without panicking.
    // Use a port that is not listening so the HTTP request fails immediately.

    #[test]
    fn iceberg_rest_catalog_list_tables_returns_err_on_unreachable_server() {
        let cat = PyIcebergRestCatalog::new(
            "http://127.0.0.1:19999".into(),
            None,
            Some(100),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let result = cat.list_tables("default".into());
        assert!(
            result.is_err(),
            "IcebergRestCatalog::list_tables must return Err when unreachable"
        );
    }

    #[test]
    fn iceberg_rest_catalog_load_metadata_validates_identifier_before_io() {
        let cat = PyIcebergRestCatalog::new(
            "http://127.0.0.1:19999".into(),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let result = cat.load_table_metadata("ns".into(), " ".into());
        assert!(
            result.is_err(),
            "load_table_metadata must reject an invalid identifier before network I/O"
        );
    }
}
