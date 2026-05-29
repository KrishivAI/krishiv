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
        let df = session
            .sql(format!("SELECT * FROM \"{table_name}\""))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        if let Some(schema_cls) = schema_cls {
            let result = df
                .collect()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Python::attach(|py| -> PyResult<()> {
                let bound = schema_cls.bind(py);
                for batch in result.batches() {
                    validate_batch_against_schema_class(&bound, batch)?;
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
/// **Alpha (with feature)**: With the `kafka` feature, returns a `Stream` descriptor.
/// The `schema` and `group_id` parameters are accepted but currently ignored.
/// The returned stream has no watermark set — call `.with_watermark(column, lag_ms)`
/// before windowing.
pub fn read_kafka(
    session: &PySession,
    topic: String,
    bootstrap_servers: String,
    schema: Option<&Bound<'_, PyType>>,
    group_id: Option<String>,
) -> PyResult<PyStream> {
    let _ = (schema, group_id);
    #[cfg(not(feature = "kafka"))]
    {
        let _ = (session, &topic, &bootstrap_servers);
        return Err(ConnectorError::new_err(
            "Kafka support requires building with the `kafka` feature (pip install krishiv[kafka])",
        ));
    }
    #[cfg(feature = "kafka")]
    {
        Ok(PyStream::from_pipeline(
            session.inner.clone(),
            format!("kafka:{topic}:{bootstrap_servers}"),
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
        return Err(ConnectorError::new_err(
            "Iceberg support requires the `iceberg` feature (pip install krishiv[iceberg])",
        ));
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

    use krishiv_lakehouse::{
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
