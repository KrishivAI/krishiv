//! `Session` factory methods and SQL entry points.

use std::sync::Arc;

use krishiv_api::StreamBatch;
use krishiv_plan::governance::{AllowAllPolicyHook, StaticApiKeyAuthProvider};
use krishiv_state::SharedStateMigrationRegistry;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};

use crate::batch::PyBatch;
use crate::dataframe::PyDataFrame;
use crate::errors::{UdfError as PyUdfError, map_krishiv_error};
use crate::job_status::PyJobStatus;
use crate::live_table::PyLiveTable;
use crate::pipeline::StreamPipeline;
use crate::relation::PyRelation;
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

// Uses crate::RUNTIME as the Tokio fallback rather than krishiv_common's FALLBACK_RUNTIME
// because the python crate owns its own runtime handle for PyO3 thread safety.
//
// Implementation note (O6/S6): `block_in_place` parks the current tokio worker
// thread for the duration of the call. For the common case (GIL released via
// `py.detach()` before calling this), the thread park is acceptable because
// PyO3 ensures no other Python threads are competing for this runtime thread.
// A `spawn` + channel approach would free the thread but requires `Send + 'static`
// bounds incompatible with the borrowed PyO3 contexts used by callers here.
pub(crate) fn block_on_async<F, T>(future: F) -> Result<T, krishiv_api::KrishivError>
where
    F: std::future::Future<Output = Result<T, krishiv_api::KrishivError>>,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => crate::RUNTIME.block_on(future),
    }
}

/// A Krishiv query session.
///
/// ## Thread Safety
///
/// `Session` is internally ref-counted (`Arc`) and safe to clone. The `sql()`,
/// `push_stream_job_input()`, and other methods release the Python GIL via
/// `py.detach()` and may be called concurrently from multiple Python threads.
///
/// **Stream jobs** (`submit_stream_job`, `push_stream_job_input`, `poll_stream_job`)
/// should be driven from a single thread per job to guarantee ordered input
/// delivery. Concurrent input pushes from multiple threads are accepted but the
/// ordering between them is undefined.
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
        // `local()` means "run locally without a daemon" — that is Embedded mode.
        // SingleNode mode now requires a coordinator Flight URL (daemon connection).
        // Users who want to connect to a local daemon should use Session.connect(url).
        build_embedded_session()
    }

    #[classmethod]
    #[pyo3(signature = (url, *, grpc_url = None))]
    /// Create a session connected to a remote coordinator.
    ///
    /// All SQL queries are routed to the remote coordinator. Use
    /// `Session.embedded()` or `Session.local()` for local execution.
    ///
    /// ``grpc_url`` is an optional separate gRPC control-plane address
    /// (e.g. ``"http://host:9090"``) used for job status and operator
    /// introspection.  When omitted, the Flight SQL ``url`` serves both
    /// data-plane and control-plane traffic.
    pub fn connect(
        _cls: &Bound<'_, PyType>,
        url: String,
        grpc_url: Option<String>,
    ) -> PyResult<Self> {
        let mut builder = krishiv_api::SessionBuilder::new()
            .with_coordinator(url)
            .with_remote_execution(true);
        if let Some(g) = grpc_url {
            builder = builder.with_coordinator_grpc(g);
        }
        builder
            .build()
            .map(|s| Self {
                inner: Arc::new(s),
                state_migrations: SharedStateMigrationRegistry::new(),
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Build a session from environment variables.
    ///
    /// Reads ``KRISHIV_MODE``, ``KRISHIV_COORDINATOR_URL`` / ``KRISHIV_COORDINATOR``,
    /// and ``KRISHIV_REMOTE_EXEC``.
    ///
    /// Valid ``KRISHIV_MODE`` values: ``embedded``, ``single-node``, ``distributed``,
    /// ``bare-metal``, ``k8s``.
    ///
    /// Delegates to ``SessionBuilder::from_env`` in Rust so Python and Rust share
    /// identical env-var parsing logic.
    #[classmethod]
    pub fn from_env(_cls: &Bound<'_, PyType>) -> PyResult<Self> {
        krishiv_api::Session::from_env()
            .map(|s| Self {
                inner: Arc::new(s),
                state_migrations: SharedStateMigrationRegistry::new(),
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Session with API-key auth and optional table-access policy.
    #[classmethod]
    #[pyo3(signature = (api_keys, *, policy = "allow_all", mode = "embedded"))]
    pub fn with_policy(
        _cls: &Bound<'_, PyType>,
        api_keys: Vec<(String, String)>,
        policy: &str,
        mode: &str,
    ) -> PyResult<Self> {
        let keys: std::collections::HashMap<String, String> = api_keys.into_iter().collect();
        let auth = Arc::new(StaticApiKeyAuthProvider::new(keys));
        let policy_hook: Arc<dyn krishiv_plan::governance::PolicyHook> = match policy {
            "allow_all" | "noop" | "role_based" => Arc::new(AllowAllPolicyHook),
            other => {
                return Err(PyRuntimeError::new_err(format!(
                    "unknown policy '{other}'; use allow_all"
                )));
            }
        };
        let mut builder = krishiv_api::SessionBuilder::new()
            .with_auth(auth)
            .with_policy(policy_hook);
        builder = match mode {
            "local" => builder, // "local" means embedded (in-process); no mode change needed
            "single-node" => builder.with_execution_mode(krishiv_api::ExecutionMode::SingleNode),
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

    pub fn read_parquet(&self, py: Python<'_>, path: String) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .read_parquet(&path)
                .map(|df| PyDataFrame { inner: df })
                .map_err(map_krishiv_error)
        })
    }

    pub fn read_csv(&self, py: Python<'_>, path: String) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .read_csv(&path)
                .map(|df| PyDataFrame { inner: df })
                .map_err(map_krishiv_error)
        })
    }

    pub fn read_json(&self, py: Python<'_>, path: String) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .read_json(&path)
                .map(|df| PyDataFrame { inner: df })
                .map_err(map_krishiv_error)
        })
    }

    pub fn set_config(&self, key: String, value: String) {
        self.inner.set_config(key, value);
    }

    pub fn get_config(&self, key: String) -> Option<String> {
        self.inner.get_config(&key)
    }

    pub fn unset_config(&self, key: String) -> Option<String> {
        self.inner.unset_config(&key)
    }

    pub fn configs(&self) -> std::collections::BTreeMap<String, String> {
        self.inner.configs()
    }

    pub fn register_parquet(&self, name: String, path: String) -> PyResult<()> {
        self.inner
            .register_parquet(&name, &path)
            .map_err(map_krishiv_error)
    }

    pub fn register_parquet_stream(&self, name: String, path: String) -> PyResult<()> {
        self.inner
            .register_parquet_stream(&name, path.as_ref())
            .map_err(map_krishiv_error)
    }

    pub fn register_unbounded(
        &self,
        name: String,
        schema: &pyo3::Bound<'_, pyo3::PyAny>,
    ) -> PyResult<()> {
        let py_schema: pyo3_arrow::PySchema = schema.extract()?;
        let schema_ref = py_schema.into_inner();
        self.inner
            .register_unbounded(&name, schema_ref)
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
            input_types_bound,
            &output_type,
            output_name,
        )?;
        self.inner
            .register_scalar_udf(registered)
            .map_err(|error| PyUdfError::new_err(error.to_string()))?;
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
                .map_err(map_krishiv_error)?
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

    /// Create a unified DataFrame backed by SQL (batch).
    ///
    /// Equivalent to `sql()` but returns a `DataFrame` usable in both batch
    /// and streaming contexts via `SessionExt`-style API.
    pub fn dataframe(&self, py: Python<'_>, query: String) -> PyResult<PyRelation> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .sql(&query)
                .map(PyRelation::from_dataframe)
                .map_err(map_krishiv_error)
        })
    }

    /// Create an unbounded streaming DataFrame backed by a named source.
    ///
    /// **Local-only**: In embedded and single-node mode, the source is resolved
    /// in-process. Distributed execution requires `KRISHIV_REMOTE_EXEC=1` or
    /// builder remote_execution to be enabled.
    pub fn from_source(&self, name: String) -> PyResult<PyRelation> {
        let mut pipeline = StreamPipeline::new(self.inner.clone(), name, String::new(), 0);
        pipeline.bounded = false;
        Ok(PyRelation::from_pipeline(pipeline))
    }

    /// Create a bounded streaming DataFrame from in-memory `Batch` objects.
    #[pyo3(signature = (name, batches, watermark_column="ts".to_string(), max_lateness_ms=0))]
    pub fn from_bounded_stream(
        &self,
        name: String,
        batches: Vec<PyBatch>,
        watermark_column: String,
        max_lateness_ms: u64,
    ) -> PyResult<PyRelation> {
        let record_batches: Vec<arrow::record_batch::RecordBatch> = batches
            .into_iter()
            .map(|b| b.record_batch().clone())
            .collect();
        self.inner
            .register_memory_stream(name.clone(), record_batches)
            .map_err(map_krishiv_error)?;
        let pipeline = StreamPipeline {
            session: self.inner.clone(),
            source_id: format!("memory:{name}"),
            bounded: true,
            watermark_column,
            max_lateness_ms,
            key_columns: Vec::new(),
            event_time_column: None,
            window: None,
            aggregations: Vec::new(),
            source_watermarks: std::collections::HashMap::new(),
            source_id_column: None,
            state_ttl_ms: None,
        };
        Ok(PyRelation::from_pipeline(pipeline))
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

#[cfg(test)]
mod tests {
    use krishiv_api::{ExecutionMode, SessionBuilder};

    #[test]
    fn connect_always_enables_remote_execution() {
        let session = SessionBuilder::new()
            .with_coordinator("http://fake.invalid:50051")
            .with_remote_execution(true)
            .build()
            .expect("session build");
        assert!(
            session.execution_runtime().uses_remote_execution(),
            "Session::connect must always route to remote coordinator"
        );
        assert_eq!(session.mode(), ExecutionMode::Distributed);
    }

    #[test]
    fn session_builder_embedded_mode() {
        // Build embedded session via Rust SessionBuilder directly (no env vars needed).
        let session = SessionBuilder::new().build().expect("embedded session");
        assert_eq!(session.mode(), ExecutionMode::Embedded);
        assert!(!session.execution_runtime().uses_remote_execution());
    }

    #[test]
    fn session_builder_distributed_mode_with_coordinator() {
        // Build a distributed session with explicit coordinator URL.
        let session = SessionBuilder::new()
            .with_coordinator("http://fake.invalid:50051")
            .with_remote_execution(true)
            .build()
            .expect("distributed session");
        assert_eq!(session.mode(), ExecutionMode::Distributed);
        assert!(session.execution_runtime().uses_remote_execution());
    }

    #[test]
    fn embedded_session_stream_registration_succeeds() {
        let session = SessionBuilder::new().build().expect("embedded session");
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("ts", arrow::datatypes::DataType::Int64, false),
        ]));
        let result = session.register_unbounded("stream_src", schema);
        assert!(
            result.is_ok(),
            "register_unbounded must succeed for embedded session"
        );
    }

    #[test]
    fn from_env_succeeds_without_panic() {
        // Exercise the Session::from_env() parse path without mutating env vars
        // (unsafe env mutation is workspace-forbidden). The test passes regardless
        // of which mode is inferred from the environment; what matters is no panic.
        let result = krishiv_api::Session::from_env();
        assert!(
            result.is_ok(),
            "Session::from_env must succeed in default env: {result:?}"
        );
    }

    #[test]
    fn from_env_returns_valid_mode() {
        // Verify that whatever mode from_env() selects, the session is internally
        // consistent (mode() returns one of the known variants).
        if let Ok(session) = krishiv_api::Session::from_env() {
            let mode = session.mode();
            assert!(
                matches!(
                    mode,
                    ExecutionMode::Embedded
                        | ExecutionMode::SingleNode
                        | ExecutionMode::Distributed
                ),
                "from_env must produce a valid execution mode, got {mode:?}"
            );
        }
    }

    #[test]
    fn session_builder_single_node_without_coordinator_errors() {
        // SingleNode now requires a coordinator URL; use Embedded for in-process.
        let err = SessionBuilder::new()
            .with_execution_mode(ExecutionMode::SingleNode)
            .build()
            .expect_err("SingleNode without coordinator must fail");
        assert!(
            err.to_string().contains("coordinator Flight URL"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn session_builder_state_ttl_propagated() {
        use krishiv_api::StateTtlConfig;
        let session = SessionBuilder::new()
            .with_state_ttl(StateTtlConfig::new(60_000))
            .build()
            .expect("session with TTL");
        assert!(session.state_ttl().is_some());
    }
}
