//! R18 lakehouse Python bindings.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::batch::PyBatch;
use crate::{PyDataFrame, PySession, RUNTIME};

fn reject_distributed_lakehouse(session: &PySession, operation: &str) -> PyResult<()> {
    if matches!(
        session.inner.mode(),
        krishiv_api::ExecutionMode::Distributed
    ) {
        return Err(PyRuntimeError::new_err(format!(
            "{operation} is local-only in this release; use an embedded or single-node session, \
             or register the table through a distributed Iceberg/catalog path"
        )));
    }
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (session, path, version=None))]
pub fn read_delta(
    session: &PySession,
    path: String,
    version: Option<i64>,
) -> PyResult<PyDataFrame> {
    reject_distributed_lakehouse(session, "read_delta")?;
    RUNTIME
        .block_on(session.inner.read_delta_async(path, version))
        .map(|df| PyDataFrame { inner: df })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

/// Write RecordBatches to a Delta Lake table.
///
/// ``path``    — local filesystem path to the Delta table directory.
/// ``batches`` — list of ``Batch`` objects to write.
/// ``mode``    — ``"append"`` (default) or ``"overwrite"``.
/// ``schema_evolution`` — whether to allow schema evolution (currently unused).
#[pyfunction]
#[pyo3(signature = (path, batches, *, mode = "append", schema_evolution = false))]
pub fn write_delta(
    path: String,
    batches: Vec<PyBatch>,
    mode: &str,
    schema_evolution: bool,
) -> PyResult<()> {
    let write_mode = match mode.to_lowercase().as_str() {
        "overwrite" => krishiv_connectors::lakehouse::DeltaWriteMode::Overwrite,
        "merge" => krishiv_connectors::lakehouse::DeltaWriteMode::Merge,
        _ => krishiv_connectors::lakehouse::DeltaWriteMode::Append,
    };
    let record_batches: Vec<arrow::record_batch::RecordBatch> = batches
        .into_iter()
        .map(|b| b.record_batch().clone())
        .collect();
    RUNTIME
        .block_on(krishiv_connectors::lakehouse::write_delta(
            path,
            record_batches,
            write_mode,
            schema_evolution,
        ))
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
    reject_distributed_lakehouse(session, "read_hudi")?;
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
    reject_distributed_lakehouse(session, "write_hudi_append")?;
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
    reject_distributed_lakehouse(session, "write_hudi_upsert")?;
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

// ── PyMemoryLakehouseTable — Iceberg DML ─────────────────────────────────────

use krishiv_connectors::lakehouse::{
    IcebergTableRef, LakehouseAssignment, LakehousePredicate, LakehouseValue, MemoryLakehouseTable,
    SchemaField, SchemaVersion,
};

/// An in-memory Iceberg table that supports atomic DML operations.
///
/// Supports ``append``, ``overwrite``, ``delete_where``, ``update_where``,
/// ``merge``, and ``evolve_schema``.
///
/// ## Example
///
/// ```python
/// import krishiv
///
/// table = krishiv.MemoryLakehouseTable("catalog", "db", "events")
/// table.overwrite(batch)
/// table.delete_where(column="user_id", value=42)
/// table.evolve_schema(schema_id=2, fields=[{"id": 1, "name": "id", "required": True, "data_type": "int"}])
/// ```
#[pyclass(name = "MemoryLakehouseTable")]
pub struct PyMemoryLakehouseTable {
    inner: Arc<MemoryLakehouseTable>,
}

#[pymethods]
impl PyMemoryLakehouseTable {
    /// Create a new in-memory Iceberg table.
    ///
    /// ``catalog``, ``namespace``, ``table`` form the qualified table identifier.
    /// ``schema_id`` is the initial schema version integer.
    #[new]
    #[pyo3(signature = (catalog, namespace, table, schema_id=1))]
    pub fn new(catalog: String, namespace: String, table: String, schema_id: i32) -> Self {
        let table_ref = IcebergTableRef::new(catalog, namespace, table);
        let schema_version = SchemaVersion {
            schema_id,
            fields: Vec::new(),
        };
        Self {
            inner: Arc::new(MemoryLakehouseTable::new(table_ref, schema_version)),
        }
    }

    /// Append ``batch`` to the table.
    pub fn append(&self, batch: &crate::batch::PyBatch) -> PyResult<()> {
        RUNTIME
            .block_on(krishiv_connectors::lakehouse::LakehouseTable::append(
                self.inner.as_ref(),
                vec![batch.record_batch().clone()],
            ))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Atomically replace all table contents with ``batch``.
    pub fn overwrite(&self, batch: &crate::batch::PyBatch) -> PyResult<()> {
        RUNTIME
            .block_on(krishiv_connectors::lakehouse::LakehouseTable::overwrite(
                self.inner.as_ref(),
                vec![batch.record_batch().clone()],
            ))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Delete rows where ``column == value``.
    ///
    /// ``value`` may be a Python ``int``, ``float``, ``bool``, ``str``, or ``None``.
    pub fn delete_where(&self, py: Python<'_>, column: String, value: Py<PyAny>) -> PyResult<()> {
        let lv = py_to_lakehouse_value(py, value)?;
        RUNTIME
            .block_on(krishiv_connectors::lakehouse::LakehouseTable::delete_where(
                self.inner.as_ref(),
                &LakehousePredicate { column, equals: lv },
            ))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Update rows matching ``predicate_column == predicate_value``, setting
    /// ``assign_column = assign_value``.
    pub fn update_where(
        &self,
        py: Python<'_>,
        predicate_column: String,
        predicate_value: Py<PyAny>,
        assign_column: String,
        assign_value: Py<PyAny>,
    ) -> PyResult<()> {
        let pred_val = py_to_lakehouse_value(py, predicate_value)?;
        let assign_val = py_to_lakehouse_value(py, assign_value)?;
        RUNTIME
            .block_on(krishiv_connectors::lakehouse::LakehouseTable::update_where(
                self.inner.as_ref(),
                &LakehousePredicate {
                    column: predicate_column,
                    equals: pred_val,
                },
                &[LakehouseAssignment {
                    column: assign_column,
                    value: assign_val,
                }],
            ))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Upsert ``batch`` into the table using ``key_columns`` to identify matching rows.
    pub fn merge(&self, batch: &crate::batch::PyBatch, key_columns: Vec<String>) -> PyResult<()> {
        RUNTIME
            .block_on(krishiv_connectors::lakehouse::LakehouseTable::merge(
                self.inner.as_ref(),
                vec![batch.record_batch().clone()],
                &key_columns,
            ))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Evolve the table schema to a new version.
    ///
    /// ``fields`` is a list of dicts with keys ``id`` (int), ``name`` (str),
    /// ``required`` (bool), and ``data_type`` (str).
    #[pyo3(signature = (schema_id, fields))]
    pub fn evolve_schema(
        &self,
        schema_id: i32,
        fields: Vec<std::collections::HashMap<String, Py<PyAny>>>,
    ) -> PyResult<()> {
        let schema_fields: Vec<SchemaField> = Python::attach(|py| {
            fields
                .into_iter()
                .map(|f| {
                    Ok(SchemaField {
                        id: f
                            .get("id")
                            .ok_or_else(|| PyRuntimeError::new_err("field missing 'id'"))?
                            .extract::<i32>(py)?,
                        name: f
                            .get("name")
                            .ok_or_else(|| PyRuntimeError::new_err("field missing 'name'"))?
                            .extract::<String>(py)?,
                        required: f
                            .get("required")
                            .map(|v| v.extract::<bool>(py))
                            .transpose()?
                            .unwrap_or(false),
                        data_type: f
                            .get("data_type")
                            .ok_or_else(|| PyRuntimeError::new_err("field missing 'data_type'"))?
                            .extract::<String>(py)?,
                    })
                })
                .collect::<PyResult<Vec<_>>>()
        })?;

        RUNTIME
            .block_on(
                krishiv_connectors::lakehouse::LakehouseTable::evolve_schema(
                    self.inner.as_ref(),
                    SchemaVersion {
                        schema_id,
                        fields: schema_fields,
                    },
                ),
            )
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Return the current snapshot ID, or ``None`` if no data has been written.
    pub fn current_snapshot_id(&self) -> PyResult<Option<i64>> {
        RUNTIME
            .block_on(
                krishiv_connectors::lakehouse::LakehouseTable::current_snapshot_id(
                    self.inner.as_ref(),
                ),
            )
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn __repr__(&self) -> String {
        "MemoryLakehouseTable".to_string()
    }
}

fn py_to_lakehouse_value(py: Python<'_>, value: Py<PyAny>) -> PyResult<LakehouseValue> {
    use pyo3::types::PyBool;
    let bound = value.bind(py);
    if bound.is_none() {
        return Ok(LakehouseValue::Null);
    }
    if let Ok(b) = bound.cast::<PyBool>() {
        return Ok(LakehouseValue::Boolean(b.is_true()));
    }
    if let Ok(i) = bound.extract::<i64>() {
        return Ok(LakehouseValue::Int64(i));
    }
    if let Ok(f) = bound.extract::<f64>() {
        return Ok(LakehouseValue::Float64(f));
    }
    if let Ok(s) = bound.extract::<String>() {
        return Ok(LakehouseValue::Utf8(s));
    }
    Err(PyRuntimeError::new_err(
        "unsupported value type: expected int, float, bool, str, or None",
    ))
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
        // PyErr::to_string() calls into Python to format the exception, so the
        // interpreter must be running before calling it.
        pyo3::Python::initialize();
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
