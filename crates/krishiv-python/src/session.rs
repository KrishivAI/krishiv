//! `Session` factory methods and SQL entry points.

use std::sync::Arc;

use krishiv_api::StreamBatch;
use krishiv_governance::{Role, RoleBasedPolicyHook, StaticApiKeyAuthProvider};
use krishiv_state::SharedStateMigrationRegistry;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};

use crate::batch::PyBatch;
use crate::dataframe::PyDataFrame;
use crate::errors::map_krishiv_error;
use crate::job_status::PyJobStatus;
use crate::live_table::PyLiveTable;
use crate::stream::{PyStream, PyWindowedStream};
use crate::stream_exec::spec_from_pipeline;

fn build_embedded_session() -> PyResult<PySession> {
    krishiv_api::SessionBuilder::new()
        .build()
        .map(|s| PySession {
            inner: Arc::new(s),
            state_migrations: SharedStateMigrationRegistry::new(),
        })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

pub(crate) fn block_on_async<F, T>(future: F) -> Result<T, krishiv_api::KrishivError>
where
    F: std::future::Future<Output = Result<T, krishiv_api::KrishivError>>,
{
    crate::RUNTIME.block_on(future)
}

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
        let mut builder = krishiv_api::SessionBuilder::new().with_coordinator(url);
        if remote_execution_from_env() {
            builder = builder.with_remote_execution(true);
        }
        builder
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
        let builder = if remote_execution_from_env() {
            builder.with_remote_execution(true)
        } else {
            builder
        };
        builder
            .build()
            .map(|s| Self {
                inner: Arc::new(s),
                state_migrations: SharedStateMigrationRegistry::new(),
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Session with API-key auth and policy for [`Self::sql_as`].
    #[classmethod]
    #[pyo3(signature = (api_keys, *, policy = "role_based", mode = "embedded"))]
    pub fn with_policy(
        _cls: &Bound<'_, PyType>,
        api_keys: Vec<(String, String, String)>,
        policy: &str,
        mode: &str,
    ) -> PyResult<Self> {
        let auth_entries = api_keys
            .into_iter()
            .map(|(key, subject, role)| Ok((key, subject, parse_role(&role)?)))
            .collect::<PyResult<Vec<_>>>()?;
        let auth = Arc::new(StaticApiKeyAuthProvider::new(auth_entries));
        let policy_hook: Arc<dyn krishiv_governance::PolicyHook> = match policy {
            "allow_all" | "noop" => Arc::new(krishiv_governance::NoOpPolicyHook),
            "role_based" => Arc::new(RoleBasedPolicyHook),
            other => {
                return Err(PyRuntimeError::new_err(format!(
                    "unknown policy '{other}'; use allow_all or role_based"
                )));
            }
        };
        let mut builder = krishiv_api::SessionBuilder::new()
            .with_auth(auth)
            .with_policy(policy_hook);
        builder = match mode {
            "local" | "single-node" => {
                builder.with_execution_mode(krishiv_api::ExecutionMode::SingleNode)
            }
            "distributed" => builder.with_execution_mode(krishiv_api::ExecutionMode::Distributed),
            _ => builder,
        };
        builder
            .build()
            .map(|s| Self {
                inner: Arc::new(s),
                state_migrations: SharedStateMigrationRegistry::new(),
            })
            .map_err(map_krishiv_error)
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
                .map_err(map_krishiv_error)
        })
    }

    pub fn sql_async(&self, py: Python<'_>, query: String) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            block_on_async(async move {
                inner
                    .sql_async(&query)
                    .await
                    .map(|df| PyDataFrame { inner: df })
            })
            .map_err(map_krishiv_error)
        })
    }

    pub fn sql_as(&self, py: Python<'_>, api_key: String, query: String) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            block_on_async(async move {
                inner
                    .sql_as(&api_key, &query)
                    .await
                    .map(|df| PyDataFrame { inner: df })
            })
            .map_err(map_krishiv_error)
        })
    }

    pub fn read_parquet(&self, py: Python<'_>, path: String) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .read_parquet(&path)
                .map(|df| PyDataFrame { inner: df })
                .map_err(map_krishiv_error)
        })
    }

    pub fn register_parquet(&self, name: String, path: String) -> PyResult<()> {
        self.inner
            .register_parquet(&name, &path)
            .map_err(map_krishiv_error)
    }

    pub fn jobs(&self) -> Vec<PyJobStatus> {
        self.inner
            .jobs()
            .into_iter()
            .map(PyJobStatus::from_status)
            .collect()
    }

    #[pyo3(signature = (query, watermark_column, max_lateness_ms))]
    pub fn stream(
        &self,
        query: String,
        watermark_column: String,
        max_lateness_ms: u64,
    ) -> PyResult<PyStream> {
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

    pub fn live_table(&self, name: String, query: String) -> PyResult<PyLiveTable> {
        crate::live_table::create_live_table(name, query)
    }

    /// Create a [`PyStream`] backed by in-memory batches (supports windowed `collect()` / `async for`).
    #[pyo3(signature = (name, batches, watermark_column, max_lateness_ms))]
    pub fn memory_stream(
        &self,
        name: String,
        batches: Vec<PyBatch>,
        watermark_column: String,
        max_lateness_ms: u64,
    ) -> PyResult<PyStream> {
        PyStream::from_memory(
            self.inner.clone(),
            name,
            watermark_column,
            max_lateness_ms,
            batches,
        )
    }

    pub fn memory_stream_collect(
        &self,
        py: Python<'_>,
        name: String,
        batches: Vec<PyBatch>,
    ) -> PyResult<Vec<PyBatch>> {
        let inner = self.inner.clone();
        py.detach(move || {
            let stream_batches: Vec<StreamBatch> = batches
                .into_iter()
                .enumerate()
                .map(|(seq, b)| StreamBatch::new(seq as u64, b.record_batch().clone()))
                .collect();
            inner
                .memory_stream(name, stream_batches)
                .collect_bounded()
                .map(|collected| {
                    collected
                        .into_iter()
                        .map(|sb| PyBatch::from_record_batch(sb.batch().clone()))
                        .collect()
                })
                .map_err(map_krishiv_error)
        })
    }

    /// Submit a continuous streaming job. Returns the job id handle.
    pub fn submit_stream_job(&self, name: String, stream: &PyWindowedStream) -> PyResult<String> {
        let spec = spec_from_pipeline(&stream.pipeline)?;
        self.inner
            .submit_stream_job(name, spec)
            .map_err(map_krishiv_error)
    }

    /// Push input batches to a continuous streaming job.
    pub fn push_stream_job_input(&self, job_id: String, batches: Vec<PyBatch>) -> PyResult<()> {
        let record_batches: Vec<arrow::record_batch::RecordBatch> = batches
            .into_iter()
            .map(|b| b.record_batch().clone())
            .collect();
        self.inner
            .push_stream_job_input(&job_id, record_batches)
            .map_err(map_krishiv_error)
    }

    /// Drain newly emitted batches from a continuous streaming job.
    pub fn poll_stream_job(&self, py: Python<'_>, job_id: String) -> PyResult<Vec<PyBatch>> {
        let inner = self.inner.clone();
        py.detach(move || {
            block_on_async(async move { inner.poll_stream_job(&job_id).await })
                .map(|batches| {
                    batches
                        .into_iter()
                        .map(PyBatch::from_record_batch)
                        .collect()
                })
                .map_err(map_krishiv_error)
        })
    }
}

fn remote_execution_from_env() -> bool {
    std::env::var("KRISHIV_REMOTE_EXEC")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn parse_role(role: &str) -> PyResult<Role> {
    match role.to_lowercase().as_str() {
        "admin" => Ok(Role::Admin),
        "writer" => Ok(Role::Writer),
        "reader" => Ok(Role::Reader),
        other => Err(PyRuntimeError::new_err(format!(
            "unknown role '{other}'; use admin, writer, or reader"
        ))),
    }
}
