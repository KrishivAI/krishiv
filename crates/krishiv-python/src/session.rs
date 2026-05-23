//! `Session` factory methods and SQL entry points.

use std::sync::Arc;

use krishiv_state::SharedStateMigrationRegistry;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};

use crate::dataframe::PyDataFrame;
use crate::errors::ModeError;
use crate::stream::PyStream;

fn build_embedded_session() -> PyResult<PySession> {
    krishiv_api::SessionBuilder::new()
        .build()
        .map(|s| PySession {
            inner: Arc::new(s),
            state_migrations: SharedStateMigrationRegistry::new(),
        })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

static RUNTIME: std::sync::LazyLock<tokio::runtime::Runtime> = std::sync::LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build embedded Krishiv Tokio runtime")
});

/// A Krishiv query session.
#[pyclass(name = "Session")]
pub struct PySession {
    pub(crate) inner: Arc<krishiv_api::Session>,
    pub(crate) state_migrations: SharedStateMigrationRegistry,
}

#[pymethods]
impl PySession {
    #[new]
    pub fn new() -> PyResult<Self> {
        build_embedded_session()
    }

    #[classmethod]
    pub fn embedded(_cls: &Bound<'_, PyType>) -> PyResult<Self> {
        build_embedded_session()
    }

    #[classmethod]
    pub fn local(_cls: &Bound<'_, PyType>) -> PyResult<Self> {
        krishiv_api::SessionBuilder::new()
            .with_execution_mode(krishiv_api::ExecutionMode::SingleNode)
            .build()
            .map(|s| Self {
                inner: Arc::new(s),
                state_migrations: SharedStateMigrationRegistry::new(),
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    #[classmethod]
    pub fn connect(_cls: &Bound<'_, PyType>, url: String) -> PyResult<Self> {
        krishiv_api::SessionBuilder::new()
            .with_coordinator(url)
            .build()
            .map(|s| Self {
                inner: Arc::new(s),
                state_migrations: SharedStateMigrationRegistry::new(),
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    #[classmethod]
    pub fn from_env(_cls: &Bound<'_, PyType>) -> PyResult<Self> {
        let mode = std::env::var("KRISHIV_MODE").unwrap_or_default();
        let coordinator_url = std::env::var("KRISHIV_COORDINATOR_URL")
            .or_else(|_| std::env::var("KRISHIV_COORDINATOR"))
            .ok();

        let builder = krishiv_api::SessionBuilder::new();
        let builder = match mode.to_lowercase().as_str() {
            "local" | "single-node" => {
                builder.with_execution_mode(krishiv_api::ExecutionMode::SingleNode)
            }
            "distributed" => {
                if let Some(url) = coordinator_url {
                    builder.with_coordinator(url)
                } else {
                    builder.with_execution_mode(krishiv_api::ExecutionMode::Distributed)
                }
            }
            _ => builder,
        };
        builder
            .build()
            .map(|s| Self {
                inner: Arc::new(s),
                state_migrations: SharedStateMigrationRegistry::new(),
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    #[getter]
    pub fn mode(&self) -> &'static str {
        match self.inner.mode() {
            krishiv_api::ExecutionMode::Embedded => "embedded",
            krishiv_api::ExecutionMode::SingleNode => "local",
            krishiv_api::ExecutionMode::Distributed => "distributed",
        }
    }

    pub fn sql(&self, py: Python<'_>, query: String) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .sql(&query)
                .map(|df| PyDataFrame { inner: df })
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    pub fn sql_async(&self, py: Python<'_>, query: String) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            RUNTIME.block_on(async move {
                inner
                    .sql_async(&query)
                    .await
                    .map(|df| PyDataFrame { inner: df })
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
        })
    }

    pub fn register_parquet(&self, name: String, path: String) -> PyResult<()> {
        self.inner
            .register_parquet(&name, &path)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    #[pyo3(signature = (query, watermark_column, max_lateness_ms))]
    pub fn stream(
        &self,
        query: String,
        watermark_column: String,
        max_lateness_ms: u64,
    ) -> PyResult<PyStream> {
        if matches!(self.inner.mode(), krishiv_api::ExecutionMode::Embedded) {
            return Err(ModeError::new_err(
                "stream() requires a non-embedded session; use Session.local() or \
                 Session.connect(url) to enable streaming",
            ));
        }
        Ok(PyStream::from_pipeline(
            self.inner.clone(),
            query,
            watermark_column,
            max_lateness_ms,
        ))
    }

    #[pyo3(signature = (name_or_callable, callable=None, *, input_types=None, output_type=None, output_name=None))]
    pub fn register_udf(
        &self,
        py: Python<'_>,
        name_or_callable: Bound<'_, PyAny>,
        callable: Option<Bound<'_, PyAny>>,
        input_types: Option<Bound<'_, PyDict>>,
        output_type: Option<String>,
        output_name: Option<String>,
    ) -> PyResult<()> {
        use pyo3::types::PyDict;

        let (name, callable, input_types, output_type, output_name) =
            crate::udf::resolve_register_udf_args(
                name_or_callable,
                callable,
                input_types,
                output_type,
                output_name,
            )?;
        let input_types_bound = input_types.bind(py);
        let registered = crate::udf::build_python_scalar_udf(
            py,
            name,
            callable,
            &input_types_bound,
            &output_type,
            output_name,
        )?;
        self.inner.register_scalar_udf(registered);
        Ok(())
    }

    pub fn list_udfs(&self) -> Vec<String> {
        self.inner.scalar_udf_names()
    }
}
