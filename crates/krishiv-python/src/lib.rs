//! **Beta API**: may change between minor releases.
//!
//! PyO3 Python bindings for Krishiv — Session, DataFrame, Stream, WindowedStream,
//! sink factories, and Python UDF support via `spawn_blocking`.

use pyo3::prelude::*;

mod agg;
mod batch;
mod dataframe;
mod errors;
mod expression;
mod job_status;
mod lakehouse;
mod live_table;
mod memo;
mod migration;
mod pipeline;
mod prepared;
mod query_handle;
mod query_result;
mod relation;
mod schema;
mod session;
mod sinks;
mod sources;
mod stream;
mod stream_exec;
mod udf;
mod windows;

// Stub `krishiv.ai` Python submodule. The AI/RAG implementation was removed in
// the platform-layer cleanup; the empty module is kept so `import krishiv.ai`
// keeps failing gracefully at attribute level rather than at import level.
mod ai {
    use pyo3::prelude::*;

    pub fn register_ai_module(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
        let ai = PyModule::new(py, "ai")?;
        parent.add_submodule(&ai)?;
        py.import("sys")?
            .getattr("modules")?
            .set_item("krishiv.ai", &ai)?;
        Ok(())
    }
}

pub use agg::PyAggExpr;
pub use batch::PyBatch;
pub use dataframe::{PyDataFrame, PyGroupedDataFrame};
pub use errors::{
    AuthorizationError, CheckpointError, ConnectorError, KrishivError, ModeError, QueryError,
    SchemaError, UdfError,
};
pub use expression::PyColumn;
pub use job_status::PyJobStatus;
pub use live_table::{PyChangeFeedIter, PyLiveTable};
pub use prepared::PyPreparedStatement;
pub use query_handle::PyQueryHandle;
pub use query_result::PyQueryResult;
pub use relation::PyRelation;
pub use schema::PySchema;
pub use session::PySession;
pub use sinks::{PyIcebergSink, PyKafkaSink, PyParquetSink};
pub use stream::{PyKeyedStream, PyStream, PyWindowedStream};
pub use udf::call_python_udf;
pub use windows::PyWindowSpec;

// ---------------------------------------------------------------------------
// Embedded Tokio runtime — shared by session async helpers and UDF bridge
// ---------------------------------------------------------------------------

pub(crate) static RUNTIME: std::sync::LazyLock<tokio::runtime::Runtime> =
    std::sync::LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build embedded Krishiv Tokio runtime")
    });

// ---------------------------------------------------------------------------
// PyModule entry point
// ---------------------------------------------------------------------------

/// Python module `krishiv` — exposes all public types and functions.
#[pymodule]
fn krishiv(m: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::register(m)?;

    m.add_class::<session::PySession>()?;
    m.add_class::<dataframe::PyDataFrame>()?;
    m.add_class::<prepared::PyPreparedStatement>()?;
    m.add_class::<expression::PyColumn>()?;
    m.add_class::<dataframe::PyGroupedDataFrame>()?;
    m.add_class::<dataframe::PyDataFrameStream>()?;
    m.add_class::<query_handle::PyQueryHandle>()?;
    m.add_class::<stream::PyStream>()?;
    m.add_class::<stream::PyKeyedStream>()?;
    m.add_class::<stream::PyWindowedStream>()?;
    m.add_class::<batch::PyBatch>()?;
    m.add_class::<query_result::PyQueryResult>()?;
    m.add_class::<query_result::PyQueryResultIter>()?;
    m.add_class::<relation::PyRelation>()?;
    m.add_class::<job_status::PyJobStatus>()?;
    m.add_class::<schema::PySchema>()?;
    m.add_class::<windows::PyWindowSpec>()?;
    m.add_class::<agg::PyAggExpr>()?;
    m.add_class::<live_table::PyLiveTable>()?;
    m.add_class::<live_table::PyChangeFeedIter>()?;
    m.add_class::<memo::MemoCacheInfo>()?;

    m.add_class::<sinks::PyParquetSink>()?;
    m.add_class::<sinks::PyKafkaSink>()?;
    m.add_class::<sinks::PyIcebergSink>()?;

    m.add_function(wrap_pyfunction!(expression::col, m)?)?;
    m.add_function(wrap_pyfunction!(expression::lit, m)?)?;
    m.add_function(wrap_pyfunction!(expression::expr, m)?)?;
    m.add_function(wrap_pyfunction!(expression::count, m)?)?;
    m.add_function(wrap_pyfunction!(expression::count_all, m)?)?;
    m.add_function(wrap_pyfunction!(expression::sum, m)?)?;
    m.add_function(wrap_pyfunction!(expression::avg, m)?)?;
    m.add_function(wrap_pyfunction!(expression::min, m)?)?;
    m.add_function(wrap_pyfunction!(expression::max, m)?)?;
    m.add_function(wrap_pyfunction!(expression::call_function, m)?)?;

    m.add_function(wrap_pyfunction!(sources::read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(sources::read_kafka, m)?)?;
    m.add_function(wrap_pyfunction!(sources::read_iceberg, m)?)?;
    m.add_function(wrap_pyfunction!(batch::make_example_batch, m)?)?;
    m.add_function(wrap_pyfunction!(migration::register_state_migration, m)?)?;
    m.add_function(wrap_pyfunction!(migration::state_migration, m)?)?;
    m.add_function(wrap_pyfunction!(migration::apply_state_migration, m)?)?;
    m.add_function(wrap_pyfunction!(udf::udf, m)?)?;
    m.add_function(wrap_pyfunction!(memo::memo_cache_info, m)?)?;
    m.add_function(wrap_pyfunction!(memo::memo_transform_call, m)?)?;

    m.add_function(wrap_pyfunction!(lakehouse::read_delta, m)?)?;
    m.add_function(wrap_pyfunction!(lakehouse::read_hudi, m)?)?;
    m.add_function(wrap_pyfunction!(lakehouse::write_hudi_append, m)?)?;
    m.add_function(wrap_pyfunction!(lakehouse::write_hudi_upsert, m)?)?;
    m.add_function(wrap_pyfunction!(lakehouse::schema_registry_confluent, m)?)?;
    m.add_class::<lakehouse::PyHudiWriteResult>()?;
    m.add_class::<lakehouse::PySchemaRegistryConfig>()?;
    m.add_class::<lakehouse::PyIcebergRestCatalog>()?;

    sinks::register_sinks_module(m.py(), m)?;
    agg::register_agg_module(m.py(), m)?;
    windows::register_windows_module(m.py(), m)?;
    ai::register_ai_module(m.py(), m)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_plan::udf::ScalarUdf;

    use crate::RUNTIME;
    use crate::call_python_udf;

    #[test]
    fn py_session_builds_embedded() {
        let session = krishiv_api::SessionBuilder::new().build().unwrap();
        let df = session.sql("SELECT 1 AS n").unwrap();
        let result = df.collect().unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn py_session_local_mode_builds() {
        let session = krishiv_api::SessionBuilder::new()
            .with_execution_mode(krishiv_api::ExecutionMode::SingleNode)
            .build()
            .unwrap();
        assert!(matches!(
            session.mode(),
            krishiv_api::ExecutionMode::SingleNode
        ));
    }

    #[test]
    fn py_session_connect_mode_builds() {
        let session = krishiv_api::SessionBuilder::new()
            .with_coordinator("http://localhost:50051")
            .build()
            .unwrap();
        assert!(matches!(
            session.mode(),
            krishiv_api::ExecutionMode::Distributed
        ));
    }

    #[test]
    fn py_dataframe_collect_contains_column() {
        let session = krishiv_api::SessionBuilder::new().build().unwrap();
        let df = session.sql("SELECT 1 AS n").unwrap();
        let result = df.collect().unwrap();
        let pretty = result.pretty().unwrap();
        assert!(pretty.contains('n'), "expected 'n' in output: {pretty}");
    }

    #[test]
    fn call_python_udf_panic_becomes_udf_error() {
        #[derive(Debug)]
        struct PanicUdf;

        impl ScalarUdf for PanicUdf {
            fn name(&self) -> &str {
                "panic"
            }
            fn input_schema(&self) -> &Schema {
                static SCHEMA: std::sync::LazyLock<Schema> =
                    std::sync::LazyLock::new(Schema::empty);
                &SCHEMA
            }
            fn output_field(&self) -> &Field {
                static FIELD: std::sync::LazyLock<Field> = std::sync::LazyLock::new(|| {
                    Field::new("out", arrow::datatypes::DataType::Null, true)
                });
                &FIELD
            }
            fn call(&self, _batch: &RecordBatch) -> Result<ArrayRef, krishiv_plan::udf::UdfError> {
                panic!("intentional panic from test")
            }
        }

        let udf = Arc::new(PanicUdf);
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema);
        let result = RUNTIME.block_on(call_python_udf(udf, batch));
        assert!(
            matches!(result, Err(krishiv_plan::udf::UdfError::Panic(_))),
            "expected UdfError::Panic, got: {result:?}"
        );
    }

    #[test]
    fn python_scalar_udf_name() {
        let udf = krishiv_plan::udf::MultiplyScalarUdf::new("my_udf", "x", 2);
        assert_eq!(udf.name(), "my_udf");
    }

    #[test]
    fn embedded_session_allows_stream_factory() {
        let session = krishiv_api::SessionBuilder::new().build().unwrap();
        assert!(matches!(
            session.mode(),
            krishiv_api::ExecutionMode::Embedded
        ));
    }

    #[test]
    fn local_session_stream_is_allowed() {
        let session = krishiv_api::SessionBuilder::new()
            .with_execution_mode(krishiv_api::ExecutionMode::SingleNode)
            .build()
            .unwrap();
        assert!(matches!(
            session.mode(),
            krishiv_api::ExecutionMode::SingleNode
        ));
    }
}
