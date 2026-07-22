//! **Beta API**: may change between minor releases.
//!
//! PyO3 Python bindings for Krishiv — Session, DataFrame, Stream, WindowedStream,
//! sink factories, and Python UDF support via `spawn_blocking`.

use pyo3::prelude::*;

mod agg;
pub mod arrow_compat;
/// Optimized Arrow IPC paths for zero-copy-like UDF performance.
pub mod arrow_fast;
mod batch;
mod blocking_session;
mod dataframe;
mod engine_job;
mod errors;
mod expression;
mod incremental;
mod job_status;
mod lakehouse;
mod live_table;
mod memo;
mod metrics_api;
mod migration;
mod pipeline;
mod pipeline_api;
mod prepared;
mod process_api;
mod query_handle;
mod query_result;
mod relation;
mod rust_udf;
mod schema;
mod session;
mod sinks;
mod sources;
mod stream;
mod stream_exec;
mod streaming;
mod streaming_dataframe;
mod udf;
mod windows;

mod vector_sinks;

pub use agg::PyAggExpr;
pub use batch::PyBatch;
pub use dataframe::{PyDataFrame, PyGroupedDataFrame};
pub use engine_job::{PyEngineJobHandle, PyRunningJob};
pub use errors::{
    AuthorizationError, CheckpointError, ConnectorError, KrishivError, ModeError, QueryError,
    SchemaError, UdfError,
};
pub use expression::PyColumn;
pub use incremental::{PyDeltaBatch, PyIvmJob, PyStepSummary, PyViewError};
pub use job_status::PyJobStatus;
pub use live_table::{PyChangeFeedIter, PyLiveTable};
pub use pipeline_api::{PyMemorySink, PyPipeline};
pub use prepared::PyPreparedStatement;
pub use process_api::{PyListState, PyMapState, PyProcessContext, PyValueState};
pub use query_handle::PyQueryHandle;
pub use query_result::PyQueryResult;
pub use relation::PyRelation;
pub use schema::PySchema;
pub use session::{PyOperationRegistry, PySession};
pub use sinks::{
    PyCassandraSink, PyElasticsearchSink, PyHBaseSink, PyIcebergSink, PyKafkaSink, PyParquetSink,
};
pub use stream::{
    PyBroadcastStream, PyConnectedStreams, PyKeyedStream, PyMultiSourceWatermarkSpec, PyStream,
    PyWindowedStream,
};
pub use streaming::{
    PyDataStreamWriter, PyRemoteStreamingJob, PyStreamingQuery, PyStreamingQueryProgress,
};
pub use streaming_dataframe::{PyDataStreamReader, PyStreamingDataFrame, interval_join};
pub use udf::call_python_udf;
pub use vector_sinks::{
    PyInMemoryVectorSink, PyLanceDbSink, PyPgvectorSink, PyPineconeSink, PyQdrantSink,
    PyScoredChunk, PyWeaviateSink,
};
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
    m.add_class::<blocking_session::PyBlockingSession>()?;
    m.add_class::<rust_udf::PyRustScalarUdf>()?;
    m.add_class::<dataframe::PyDataFrame>()?;
    m.add_class::<prepared::PyPreparedStatement>()?;
    m.add_class::<expression::PyColumn>()?;
    m.add_class::<dataframe::PyGroupedDataFrame>()?;
    m.add_class::<dataframe::PyDataFrameStream>()?;
    m.add_class::<query_handle::PyQueryHandle>()?;
    m.add_class::<engine_job::PyEngineJobHandle>()?;
    m.add_class::<engine_job::PyRunningJob>()?;
    m.add_class::<stream::PyStream>()?;
    m.add_class::<stream::PyKeyedStream>()?;
    m.add_class::<stream::PyWindowedStream>()?;
    m.add_class::<stream::PyConnectedStreams>()?;
    m.add_class::<stream::PyMultiSourceWatermarkSpec>()?;
    m.add_class::<stream::PyBroadcastStream>()?;
    m.add_class::<stream::PyBroadcastContext>()?;
    m.add_class::<batch::PyBatch>()?;
    m.add_class::<query_result::PyQueryResult>()?;
    m.add_class::<query_result::PyQueryResultIter>()?;
    m.add_class::<relation::PyRelation>()?;
    m.add_class::<job_status::PyJobStatus>()?;
    m.add_class::<schema::PySchema>()?;
    m.add_class::<windows::PyWindowSpec>()?;
    m.add_class::<agg::PyAggExpr>()?;
    m.add_class::<incremental::PyDeltaBatch>()?;
    m.add_class::<incremental::PyIvmJob>()?;
    m.add_class::<pipeline_api::PyPipeline>()?;
    m.add_class::<pipeline_api::PyMemorySink>()?;
    m.add_class::<incremental::PyStepSummary>()?;
    m.add_class::<incremental::PyViewError>()?;
    m.add_class::<live_table::PyLiveTable>()?;
    m.add_class::<live_table::PyChangeFeedIter>()?;
    m.add_class::<memo::MemoCacheInfo>()?;

    m.add_class::<sinks::PyParquetSink>()?;
    m.add_class::<sinks::PyKafkaSink>()?;
    m.add_class::<sinks::PyIcebergSink>()?;
    m.add_class::<sinks::PyCassandraSink>()?;
    m.add_class::<sinks::PyElasticsearchSink>()?;
    m.add_class::<sinks::PyHBaseSink>()?;

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
    m.add_function(wrap_pyfunction!(expression::when, m)?)?;
    m.add_function(wrap_pyfunction!(expression::row_number, m)?)?;
    m.add_function(wrap_pyfunction!(expression::rank, m)?)?;
    m.add_function(wrap_pyfunction!(expression::dense_rank, m)?)?;
    m.add_function(wrap_pyfunction!(expression::percent_rank, m)?)?;
    m.add_function(wrap_pyfunction!(expression::cume_dist, m)?)?;
    m.add_function(wrap_pyfunction!(expression::ntile, m)?)?;
    m.add_function(wrap_pyfunction!(expression::lag, m)?)?;
    m.add_function(wrap_pyfunction!(expression::lead, m)?)?;
    m.add_function(wrap_pyfunction!(expression::first_value, m)?)?;
    m.add_function(wrap_pyfunction!(expression::last_value, m)?)?;
    m.add_function(wrap_pyfunction!(expression::nth_value, m)?)?;

    m.add_function(wrap_pyfunction!(sources::read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(sources::read_kafka, m)?)?;
    m.add_function(wrap_pyfunction!(sources::read_iceberg, m)?)?;
    m.add_function(wrap_pyfunction!(sources::read_kinesis, m)?)?;
    m.add_function(wrap_pyfunction!(sources::read_pulsar, m)?)?;
    m.add_function(wrap_pyfunction!(batch::make_example_batch, m)?)?;
    m.add_function(wrap_pyfunction!(migration::register_state_migration, m)?)?;
    m.add_function(wrap_pyfunction!(migration::state_migration, m)?)?;
    m.add_function(wrap_pyfunction!(migration::apply_state_migration, m)?)?;
    m.add_function(wrap_pyfunction!(udf::udf, m)?)?;
    m.add_function(wrap_pyfunction!(memo::memo_cache_info, m)?)?;
    m.add_function(wrap_pyfunction!(memo::memo_transform_call, m)?)?;

    m.add_function(wrap_pyfunction!(lakehouse::read_delta, m)?)?;
    m.add_function(wrap_pyfunction!(lakehouse::write_delta, m)?)?;
    m.add_function(wrap_pyfunction!(lakehouse::read_hudi, m)?)?;
    m.add_function(wrap_pyfunction!(lakehouse::write_hudi_append, m)?)?;
    m.add_function(wrap_pyfunction!(lakehouse::write_hudi_upsert, m)?)?;
    m.add_function(wrap_pyfunction!(lakehouse::schema_registry_confluent, m)?)?;
    m.add_class::<lakehouse::PyHudiWriteResult>()?;
    m.add_class::<lakehouse::PySchemaRegistryConfig>()?;
    m.add_class::<lakehouse::PyIcebergRestCatalog>()?;
    m.add_class::<lakehouse::PyMemoryLakehouseTable>()?;

    // Streaming write (Phase F parity)
    m.add_function(wrap_pyfunction!(streaming_dataframe::interval_join, m)?)?;
    m.add_function(wrap_pyfunction!(streaming_dataframe::stream_table_join, m)?)?;
    m.add_function(wrap_pyfunction!(streaming_dataframe::temporal_join, m)?)?;
    m.add_function(wrap_pyfunction!(
        streaming_dataframe::stream_stream_join,
        m
    )?)?;
    m.add_class::<streaming_dataframe::PyStreamingDataFrame>()?;
    m.add_class::<streaming_dataframe::PyDataStreamReader>()?;
    m.add_class::<streaming::PyStreamingQueryProgress>()?;
    m.add_class::<streaming::PyStreamingQuery>()?;
    m.add_class::<streaming::PyDataStreamWriter>()?;
    m.add_class::<streaming::PyRemoteStreamingJob>()?;
    m.add_function(wrap_pyfunction!(connect_streaming, m)?)?;

    // Process function / stateful operator (Phase G parity)
    m.add_class::<process_api::PyProcessContext>()?;
    m.add_class::<process_api::PyValueState>()?;
    m.add_class::<process_api::PyListState>()?;
    m.add_class::<process_api::PyMapState>()?;
    m.add_function(wrap_pyfunction!(process_api::apply_process_function, m)?)?;

    // SQL gateway (Phase H parity)
    m.add_class::<session::PyOperationRegistry>()?;

    sinks::register_sinks_module(m.py(), m)?;
    agg::register_agg_module(m.py(), m)?;
    windows::register_windows_module(m.py(), m)?;
    vector_sinks::register_ai_module(m.py(), m)?;
    metrics_api::register_metrics_module(m.py(), m)?;

    Ok(())
}

// ── Convenience module-level connection functions ─────────────────────────────
//
// IVM jobs are obtained via `Session.ivm(name)` (mode-aware), not a free
// function — a distributed session yields a remote job, an embedded session an
// in-process one. See `incremental::PyIvmJob`.

/// Connect to a remote continuous streaming job on the coordinator.
///
/// The job must already be registered; use the coordinator HTTP API to
/// create it first.
///
/// Parameters
/// ----------
/// coordinator_url : str
///     Base URL of the coordinator HTTP API.
/// job_id : str
///     The streaming job ID assigned at registration time.
///
/// Returns
/// -------
/// RemoteStreamingJob
///     Handle to the remote streaming job.
///
/// Example:
///
/// ```python
/// import krishiv
/// job = krishiv.connect_streaming("http://coordinator:8080", "etl-job")
/// job.push([batch])
/// results = job.drain()
/// ```
#[pyo3::pyfunction]
pub fn connect_streaming(
    coordinator_url: String,
    job_id: String,
) -> streaming::PyRemoteStreamingJob {
    streaming::PyRemoteStreamingJob::py_new(coordinator_url, job_id)
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
        // SingleNode now requires a coordinator URL; verify Embedded builds.
        let session = krishiv_api::SessionBuilder::new().build().unwrap();
        assert!(matches!(
            session.mode(),
            krishiv_api::ExecutionMode::Embedded
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
        // SingleNode now requires a coordinator URL; Embedded is the local stream mode.
        let session = krishiv_api::SessionBuilder::new().build().unwrap();
        assert!(matches!(
            session.mode(),
            krishiv_api::ExecutionMode::Embedded
        ));
    }
}
