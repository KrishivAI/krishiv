#![forbid(unsafe_code)]

//! PyO3 Python bindings for Krishiv.

mod agg;
mod batch;
mod dataframe;
mod errors;
mod migration;
mod pipeline;
mod schema;
mod session;
mod sinks;
mod sources;
mod stream;
mod udf;
mod windows;

pub use batch::{make_example_batch, PyBatch};
pub use errors::{
    AuthorizationError, CheckpointError, ConnectorError, KrishivError, ModeError, QueryError,
    SchemaError,
};

use pyo3::prelude::*;

use agg::PyAggExpr;
use batch::PyBatch as Batch;
use dataframe::PyDataFrame;
use migration::{apply_state_migration, register_state_migration, state_migration};
use schema::PySchema;
use session::PySession;
use sinks::{PyIcebergSink, PyKafkaSink, PyParquetSink};
use sources::{read_iceberg, read_kafka, read_parquet};
use stream::{PyKeyedStream, PyStream, PyWindowedStream};
use windows::PyWindowSpec;

/// Python module `krishiv`.
#[pymodule]
fn krishiv(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    errors::register(m)?;

    m.add_class::<PySession>()?;
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PySchema>()?;
    m.add_class::<PyStream>()?;
    m.add_class::<PyKeyedStream>()?;
    m.add_class::<PyWindowedStream>()?;
    m.add_class::<Batch>()?;
    m.add_class::<PyParquetSink>()?;
    m.add_class::<PyKafkaSink>()?;
    m.add_class::<PyIcebergSink>()?;
    m.add_class::<PyAggExpr>()?;
    m.add_class::<PyWindowSpec>()?;

    m.add_function(wrap_pyfunction!(make_example_batch, m)?)?;
    m.add_function(wrap_pyfunction!(read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(read_kafka, m)?)?;
    m.add_function(wrap_pyfunction!(read_iceberg, m)?)?;
    m.add_function(wrap_pyfunction!(register_state_migration, m)?)?;
    m.add_function(wrap_pyfunction!(state_migration, m)?)?;
    m.add_function(wrap_pyfunction!(apply_state_migration, m)?)?;

    agg::register_agg_module(py, m)?;
    windows::register_windows_module(py, m)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_udf::ScalarUdf;

    use crate::udf::call_python_udf;

    #[test]
    fn py_session_builds_embedded() {
        let session = krishiv_api::SessionBuilder::new().build().unwrap();
        let df = session.sql("SELECT 1 AS n").unwrap();
        assert_eq!(df.collect().unwrap().row_count(), 1);
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
                unimplemented!()
            }
            fn output_field(&self) -> &Field {
                unimplemented!()
            }
            fn call(
                &self,
                _batch: &RecordBatch,
            ) -> Result<arrow::array::ArrayRef, krishiv_udf::UdfError> {
                panic!("intentional panic from test")
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let udf = Arc::new(PanicUdf);
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let result = rt.block_on(call_python_udf(udf, batch));
        assert!(matches!(result, Err(krishiv_udf::UdfError::Panic(_))));
    }
}
