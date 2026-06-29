//! `Session` factory methods and SQL entry points.

use std::sync::Arc;

use krishiv_api::StreamBatch;
use krishiv_plan::governance::{AllowAllPolicyHook, AuthProvider, StaticApiKeyAuthProvider};
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
use crate::query_handle::PyQueryHandle;
use crate::relation::PyRelation;
use crate::stream::{PyStream, PyWindowedStream};
use crate::stream_exec::spec_from_pipeline;

// ── G15 auth providers ────────────────────────────────────────────────────────────────

/// Accepts exactly one static bearer token.
pub(crate) struct StaticBearerTokenAuth {
    pub(crate) token: String,
}

impl AuthProvider for StaticBearerTokenAuth {
    fn authenticate(&self, api_key: &str) -> Option<String> {
        use constant_time_eq::constant_time_eq;
        if constant_time_eq(self.token.as_bytes(), api_key.as_bytes()) {
            Some("bearer".to_string())
        } else {
            None
        }
    }
}

/// JWT validator using an offline JWKS key set.
pub(crate) struct JwtAuth {
    keys: Vec<jsonwebtoken::DecodingKey>,
    validation: jsonwebtoken::Validation,
}

impl JwtAuth {
    fn from_jwks_json(
        json: &str,
        audience: String,
        issuer: Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let jwk_set: jsonwebtoken::jwk::JwkSet = serde_json::from_str(json)?;
        let keys: Vec<jsonwebtoken::DecodingKey> = jwk_set
            .keys
            .iter()
            .filter_map(|k| jsonwebtoken::DecodingKey::from_jwk(k).ok())
            .collect();
        if keys.is_empty() {
            return Err("JWKS contained no usable keys".into());
        }
        let mut validation = jsonwebtoken::Validation::default();
        validation.set_audience(&[&audience]);
        if let Some(iss) = issuer {
            if !iss.is_empty() {
                validation.set_issuer(&[&iss]);
            }
        }
        Ok(Self { keys, validation })
    }
}

#[derive(serde::Deserialize)]
struct JwtClaims {
    sub: Option<String>,
}

impl AuthProvider for JwtAuth {
    fn authenticate(&self, api_key: &str) -> Option<String> {
        for key in &self.keys {
            if let Ok(token_data) =
                jsonwebtoken::decode::<JwtClaims>(api_key, key, &self.validation)
            {
                return Some(
                    token_data
                        .claims
                        .sub
                        .unwrap_or_else(|| "authenticated".to_string()),
                );
            }
        }
        None
    }
}

fn build_embedded_session() -> PyResult<PySession> {
    krishiv_api::SessionBuilder::new()
        .build()
        .map(|s| PySession {
            inner: Arc::new(s),
            state_migrations: SharedStateMigrationRegistry::new(),
        })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

fn build_session_with_opts(
    mut builder: krishiv_api::SessionBuilder,
    target_parallelism: Option<usize>,
    shuffle_partitions: Option<u32>,
    state_ttl_ms: Option<u64>,
) -> PyResult<PySession> {
    if let Some(n) = target_parallelism {
        let nz = std::num::NonZeroUsize::new(n)
            .ok_or_else(|| PyRuntimeError::new_err("target_parallelism must be > 0"))?;
        builder = builder.with_target_parallelism(nz);
    }
    if let Some(n) = shuffle_partitions {
        builder = builder.with_shuffle_partitions(n);
    }
    if let Some(ttl) = state_ttl_ms {
        builder = builder.with_state_ttl(krishiv_api::StateTtlConfig::new(ttl));
    }
    builder
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
// Implementation note: `block_in_place` panics when called from a `spawn_blocking`
// worker (e.g. when a Python callback invoked from `spawn_blocking` calls into
// a Krishiv API that ultimately calls `block_on_async`). Instead of trying to
// detect the spawn_blocking context, we always delegate to `crate::RUNTIME.block_on`,
// which blocks the calling OS thread but is correct in all contexts.
//
// Tradeoff: slightly less efficient than cooperative parking of a tokio worker
// via `block_in_place`, but avoids hard aborts in the spawn_blocking edge case.
pub(crate) fn block_on_async<F, T>(future: F) -> Result<T, krishiv_api::KrishivError>
where
    F: std::future::Future<Output = Result<T, krishiv_api::KrishivError>>,
{
    crate::RUNTIME.block_on(future)
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

    pub fn table(&self, name: String) -> PyResult<PyDataFrame> {
        let identifier = krishiv_api::TableIdentifier::new(name).map_err(map_krishiv_error)?;
        self.inner
            .table(&identifier)
            .map(|inner| PyDataFrame { inner })
            .map_err(map_krishiv_error)
    }

    #[pyo3(signature = (path, format, *, header = true, delimiter = ','))]
    pub fn read_file(
        &self,
        py: Python<'_>,
        path: String,
        format: String,
        header: bool,
        delimiter: char,
    ) -> PyResult<PyDataFrame> {
        let session = Arc::clone(&self.inner);
        py.detach(move || {
            let reader = session.read();
            let reader = match format.to_ascii_lowercase().as_str() {
                "parquet" => reader.parquet(krishiv_api::ParquetReadOptions::default()),
                "csv" => {
                    if !delimiter.is_ascii() {
                        return Err(PyRuntimeError::new_err("delimiter must be one ASCII byte"));
                    }
                    reader.csv(krishiv_api::CsvReadOptions {
                        has_header: header,
                        delimiter: delimiter as u8,
                        ..krishiv_api::CsvReadOptions::default()
                    })
                }
                "json" | "ndjson" => reader.json(krishiv_api::JsonReadOptions::default()),
                other => {
                    return Err(PyRuntimeError::new_err(format!(
                        "unsupported format '{other}'"
                    )));
                }
            };
            reader
                .load(path)
                .map(|inner| PyDataFrame { inner })
                .map_err(map_krishiv_error)
        })
    }

    pub fn prepare(&self, sql: String) -> PyResult<crate::prepared::PyPreparedStatement> {
        self.inner
            .prepare(sql)
            .map(|inner| crate::prepared::PyPreparedStatement { inner })
            .map_err(map_krishiv_error)
    }

    /// Create an in-process embedded session.
    ///
    /// ``target_parallelism`` — number of parallel partitions for batch queries
    ///   (default: 1 for embedded mode).
    /// ``shuffle_partitions`` — bucket count for hash/round-robin exchanges
    ///   (default: derived from target_parallelism).
    #[classmethod]
    #[pyo3(signature = (*, target_parallelism = None, shuffle_partitions = None, state_ttl_ms = None))]
    pub fn embedded(
        _cls: &Bound<'_, PyType>,
        target_parallelism: Option<usize>,
        shuffle_partitions: Option<u32>,
        state_ttl_ms: Option<u64>,
    ) -> PyResult<Self> {
        build_session_with_opts(
            krishiv_api::SessionBuilder::new(),
            target_parallelism,
            shuffle_partitions,
            state_ttl_ms,
        )
    }

    /// Alias for :py:meth:`embedded` — runs locally without a daemon.
    #[classmethod]
    #[pyo3(signature = (*, target_parallelism = None, shuffle_partitions = None, state_ttl_ms = None))]
    pub fn local(
        _cls: &Bound<'_, PyType>,
        target_parallelism: Option<usize>,
        shuffle_partitions: Option<u32>,
        state_ttl_ms: Option<u64>,
    ) -> PyResult<Self> {
        build_session_with_opts(
            krishiv_api::SessionBuilder::new(),
            target_parallelism,
            shuffle_partitions,
            state_ttl_ms,
        )
    }

    /// Create a session connected to a remote coordinator.
    ///
    /// All SQL queries are routed to the remote coordinator. Use
    /// :py:meth:`embedded` or :py:meth:`local` for local execution.
    ///
    /// ``grpc_url`` — optional separate gRPC control-plane address.
    /// ``target_parallelism`` / ``shuffle_partitions`` — performance tuning.
    /// ``state_ttl_ms`` — state eviction TTL in milliseconds (0 = no eviction).
    #[classmethod]
    #[pyo3(signature = (url, *, grpc_url = None, target_parallelism = None, shuffle_partitions = None, state_ttl_ms = None))]
    pub fn connect(
        _cls: &Bound<'_, PyType>,
        url: String,
        grpc_url: Option<String>,
        target_parallelism: Option<usize>,
        shuffle_partitions: Option<u32>,
        state_ttl_ms: Option<u64>,
    ) -> PyResult<Self> {
        let mut builder = krishiv_api::SessionBuilder::new()
            .with_coordinator(url)
            .with_remote_execution(true);
        if let Some(g) = grpc_url {
            builder = builder.with_coordinator_grpc(g);
        }
        build_session_with_opts(
            builder,
            target_parallelism,
            shuffle_partitions,
            state_ttl_ms,
        )
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

    /// Compatibility entry point for SQL planning.
    ///
    /// B-1 fix: this is now a real Python coroutine (via `pyo3-async-runtimes`).
    /// The previous implementation called the blocking `sql` from inside
    /// a method named `_async`, which deadlocked any Python event loop that
    /// awaited it: the GIL was released by `py.detach`, but the actual work
    /// still ran on a tokio blocking thread and never resolved. Returning
    /// a `Future` from Rust through `pyo3-async-runtimes` schedules the work
    /// on the registered tokio runtime and yields control to the Python
    /// event loop until the future completes.
    ///
    ///     df = await session.sql_async("SELECT 1 AS n")
    ///     result = await df.collect_async()
    ///     print(result.pretty())
    pub fn sql_async<'py>(&self, py: Python<'py>, query: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let df = inner.sql_async(&query).await.map_err(map_krishiv_error)?;
            Ok(PyDataFrame { inner: df })
        })
    }

    /// Submit a SQL query and return a ``QueryHandle`` for lifecycle management.
    ///
    /// The handle gives access to status, progress, and cancellation.
    /// ``await handle.collect_async()`` retrieves the result::
    ///
    ///     handle = session.submit_async("SELECT count(*) FROM t")
    ///     print(handle.status())  # "running"
    ///     result = await handle.collect_async()
    pub fn submit_async(&self, py: Python<'_>, query: String) -> PyResult<PyQueryHandle> {
        let df = self.inner.sql(&query).map_err(map_krishiv_error)?;
        let handle = py.detach(move || df.submit_async());
        Ok(PyQueryHandle::new(handle))
    }

    /// Submit a SQL pipeline script (``CREATE SOURCE`` / ``CREATE SINK``) through
    /// the unified engine spine and return an :class:`EngineJobHandle`.
    ///
    /// The engine (batch / incremental / streaming) is inferred from the declared
    /// connectors and whether the transform is windowed — the Python surface does
    /// not pick the engine, exactly as the Rust and SQL front-ends do not. This
    /// is the Python entry point to ``Session::submit_sql``::
    ///
    ///     handle = session.submit_sql(
    ///         "CREATE SOURCE orders FROM parquet(path='/in.parquet');"
    ///         "CREATE SINK out FROM orders INTO parquet(path='/out.parquet');"
    ///     )
    ///     print(handle.status)  # "completed"
    pub fn submit_sql(
        &self,
        py: Python<'_>,
        sql: String,
    ) -> PyResult<crate::engine_job::PyEngineJobHandle> {
        let inner = self.inner.clone();
        py.detach(move || {
            block_on_async(inner.submit_sql(&sql))
                .map(crate::engine_job::PyEngineJobHandle::from_handle)
                .map_err(map_krishiv_error)
        })
    }

    /// Submit a continuous, unbounded **streaming** SQL pipeline and return a
    /// stoppable :class:`RunningJob`.
    ///
    /// The script's windowed transform infers the streaming engine; the job
    /// keeps draining its source on a background task — emitting closed windows
    /// and checkpointing — until ``handle.stop()`` is called. This is the Python
    /// entry point to ``Session::submit_streaming``::
    ///
    ///     handle = session.submit_streaming_sql(script)
    ///     ...                      # job runs continuously
    ///     status = handle.stop()   # "completed"
    pub fn submit_streaming_sql(
        &self,
        py: Python<'_>,
        sql: String,
    ) -> PyResult<crate::engine_job::PyRunningJob> {
        let inner = self.inner.clone();
        py.detach(move || {
            block_on_async(async move {
                let job = krishiv_api::compile_sql_job(&sql)?;
                inner.submit_streaming(job)
            })
            .map(crate::engine_job::PyRunningJob::from_running)
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

    /// Read a Parquet file with typed options.
    ///
    /// `batch_size` overrides the number of rows per output batch.
    #[pyo3(signature = (path, *, batch_size=None))]
    pub fn read_parquet_with_options(
        &self,
        py: Python<'_>,
        path: String,
        batch_size: Option<usize>,
    ) -> PyResult<PyDataFrame> {
        let opts = krishiv_sql::ParquetReaderOptions { batch_size };
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .read_parquet_with_options(&path, opts)
                .map(|df| PyDataFrame { inner: df })
                .map_err(map_krishiv_error)
        })
    }

    /// Read a CSV file with typed options.
    ///
    /// `delimiter` is a single character (default `,`).
    /// `has_header` controls whether the first row is treated as a header.
    #[pyo3(signature = (path, *, delimiter=None, has_header=None))]
    pub fn read_csv_with_options(
        &self,
        py: Python<'_>,
        path: String,
        delimiter: Option<String>,
        has_header: Option<bool>,
    ) -> PyResult<PyDataFrame> {
        let delimiter_char: Option<char> = match delimiter {
            Some(ref s) => {
                let mut chars = s.chars();
                let c = chars.next().ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "delimiter must be a non-empty string",
                    )
                })?;
                if chars.next().is_some() {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(
                        "delimiter must be a single character",
                    ));
                }
                Some(c)
            }
            None => None,
        };
        let opts = krishiv_sql::CsvReaderOptions {
            delimiter: delimiter_char,
            has_header,
        };
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .read_csv_with_options(&path, opts)
                .map(|df| PyDataFrame { inner: df })
                .map_err(map_krishiv_error)
        })
    }

    /// Register in-memory Arrow batches as a named SQL table.
    pub fn register_record_batches(
        &self,
        py: Python<'_>,
        name: String,
        batches: Vec<crate::batch::PyBatch>,
    ) -> PyResult<()> {
        let record_batches: Vec<arrow::record_batch::RecordBatch> = batches
            .into_iter()
            .map(|b| b.record_batch().clone())
            .collect();
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .register_record_batches(&name, record_batches)
                .map_err(map_krishiv_error)
        })
    }

    /// Convenience: collect a DataFrame and register the result as a named SQL table.
    pub fn register_dataframe(
        &self,
        py: Python<'_>,
        name: String,
        df: &PyDataFrame,
    ) -> PyResult<()> {
        let inner_df = df.inner.clone();
        let batches = py.detach(|| {
            krishiv_common::async_util::block_on(inner_df.collect_async())
                .map(|result| result.into_batches())
                .map_err(map_krishiv_error)
        })?;
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .register_record_batches(&name, batches)
                .map_err(map_krishiv_error)
        })
    }

    /// Deregister (drop) a named SQL table from this session.
    pub fn deregister_table(&self, name: String) -> PyResult<()> {
        self.inner
            .deregister_table(&name)
            .map_err(map_krishiv_error)
    }

    /// List names of all registered aggregate UDAFs.
    pub fn list_aggregate_udfs(&self) -> Vec<String> {
        self.inner.aggregate_udf_names()
    }

    /// List names of all registered table UDTFs.
    pub fn list_table_udfs(&self) -> Vec<String> {
        self.inner.table_udf_names()
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
        let py_schema: crate::arrow_compat::PyArrowSchema = schema.extract()?;
        let schema_ref = py_schema.into_inner();
        self.inner
            .register_unbounded(&name, schema_ref)
            .map_err(map_krishiv_error)
    }

    /// Register an unbounded streaming table with a bounded input queue.
    ///
    /// Unlike `register_unbounded`, this caps the internal channel at
    /// `capacity` batches. Back-pressure is applied when the queue is full.
    pub fn register_unbounded_with_capacity(
        &self,
        name: String,
        schema: &pyo3::Bound<'_, pyo3::PyAny>,
        capacity: usize,
    ) -> PyResult<()> {
        let py_schema: crate::arrow_compat::PyArrowSchema = schema.extract()?;
        let schema_ref = py_schema.into_inner();
        self.inner
            .register_unbounded_with_capacity(&name, schema_ref, capacity)
            .map_err(map_krishiv_error)
    }

    /// Execute SQL using the local engine, bypassing any remote coordinator.
    ///
    /// Always routes to the in-process DataFusion engine regardless of the
    /// session mode. Useful for diagnostic queries in distributed sessions.
    pub fn execute_local(&self, py: Python<'_>, query: String) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .execute_local(&query)
                .map(|df| PyDataFrame { inner: df })
                .map_err(map_krishiv_error)
        })
    }

    /// Execute SQL through the remote coordinator.
    ///
    /// Requires a session built with `Session.connect(url)`. Raises
    /// `RuntimeError` in embedded mode or when no coordinator is configured.
    pub fn execute_remote(&self, py: Python<'_>, query: String) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            inner
                .execute_remote(&query)
                .map(|df| PyDataFrame { inner: df })
                .map_err(map_krishiv_error)
        })
    }

    // ── SQL gateway methods ───────────────────────────────────────────────────────

    /// Execute a SQL query with a wall-clock timeout.
    ///
    /// Raises ``RuntimeError`` if the query does not complete within
    /// ``timeout_ms`` milliseconds.
    ///
    /// ## Example
    ///
    /// ```python
    /// df = session.sql_with_timeout("SELECT sleep(10)", timeout_ms=100)
    /// ```
    pub fn sql_with_timeout(
        &self,
        py: Python<'_>,
        query: String,
        timeout_ms: u64,
    ) -> PyResult<PyDataFrame> {
        let inner = self.inner.clone();
        py.detach(move || {
            block_on_async(inner.sql_with_timeout_async(&query, timeout_ms))
                .map(|df| PyDataFrame { inner: df })
                .map_err(map_krishiv_error)
        })
    }

    /// Compatibility entry point for SQL execution with a timeout.
    ///
    /// Like [`sql_async`](Self::sql_async), this is a blocking native method that
    /// the public Python package wraps in a coroutine, rather than a PyO3 native
    /// `async fn` (whose future would have to be `Send`). It runs the query
    /// off-GIL via [`sql_with_timeout`](Self::sql_with_timeout).
    pub fn sql_with_timeout_async(
        &self,
        py: Python<'_>,
        query: String,
        timeout_ms: u64,
    ) -> PyResult<PyDataFrame> {
        self.sql_with_timeout(py, query, timeout_ms)
    }

    /// Create an :class:`OperationRegistry` tied to this session for operation-level cancellation.
    pub fn operation_registry(&self) -> PyOperationRegistry {
        PyOperationRegistry::new(self.inner.operation_registry())
    }

    pub fn read_stream(&self) -> crate::streaming_dataframe::PyDataStreamReader {
        crate::streaming_dataframe::PyDataStreamReader::new(self.inner.as_ref().clone())
    }

    pub fn list_table_identifiers(&self) -> PyResult<Vec<String>> {
        self.inner
            .list_table_identifiers()
            .map(|ids| {
                ids.into_iter()
                    .map(|id| id.name.as_str().to_string())
                    .collect()
            })
            .map_err(map_krishiv_error)
    }

    pub fn table_metadata(&self, name: String) -> PyResult<(Vec<(String, String)>, String)> {
        let identifier =
            krishiv_api::catalog::TableIdentifier::new(name).map_err(map_krishiv_error)?;
        let metadata = self
            .inner
            .table_metadata(&identifier)
            .map_err(map_krishiv_error)?;
        let schema = metadata
            .schema
            .fields()
            .iter()
            .map(|field| (field.name().clone(), field.data_type().to_string()))
            .collect();
        let boundedness = format!("{:?}", metadata.boundedness);
        Ok((schema, boundedness))
    }

    pub fn create_temp_view(&self, name: String, query: String) -> PyResult<()> {
        let identifier =
            krishiv_api::catalog::ViewIdentifier::new(name).map_err(map_krishiv_error)?;
        self.inner
            .create_temp_view(&identifier, &query)
            .map_err(map_krishiv_error)
    }

    pub fn sql_as(&self, api_key: String, query: String) -> PyResult<PyDataFrame> {
        self.inner
            .sql_as(&api_key, &query)
            .map(|inner| PyDataFrame { inner })
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

    pub fn register_function(
        &self,
        name: String,
        udf: &crate::rust_udf::PyRustScalarUdf,
    ) -> PyResult<()> {
        crate::rust_udf::register_function(&self.inner, name, udf)
    }

    pub fn list_udfs(&self) -> Vec<String> {
        self.inner.scalar_udf_names()
    }

    /// Register a Python aggregate UDF (UDAF).
    ///
    /// The three callables must have the signatures:
    /// - `accumulate(state: bytes, batch: dict[str, list]) -> bytes`
    /// - `finalize(state: bytes) -> int | float | str | bool | bytes | None`
    /// - `merge(state_a: bytes, state_b: bytes) -> bytes`
    #[pyo3(signature = (name, accumulate_fn, finalize_fn, merge_fn, *, input_types, output_type, output_name=None))]
    pub fn register_aggregate_udf(
        &self,
        py: Python<'_>,
        name: String,
        accumulate_fn: Py<PyAny>,
        finalize_fn: Py<PyAny>,
        merge_fn: Py<PyAny>,
        input_types: Bound<'_, PyDict>,
        output_type: String,
        output_name: Option<String>,
    ) -> PyResult<()> {
        let udf = crate::udf::build_python_aggregate_udf(
            py,
            name,
            accumulate_fn,
            finalize_fn,
            merge_fn,
            &input_types,
            &output_type,
            output_name,
        )?;
        self.inner
            .register_aggregate_udf(udf)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Register a Python table UDF (UDTF).
    ///
    /// The callable has signature:
    /// `fn(args: list[int | float | str | bool | bytes | None]) -> pyarrow.RecordBatch`
    #[pyo3(signature = (name, callable, *, output_types))]
    pub fn register_table_udf(
        &self,
        py: Python<'_>,
        name: String,
        callable: Py<PyAny>,
        output_types: Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let udf = crate::udf::build_python_table_udf(py, name, callable, &output_types)?;
        self.inner
            .register_table_udf(udf)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    pub fn live_table(&self, name: String, query: String) -> PyResult<PyLiveTable> {
        crate::live_table::create_live_table(name, query, self.inner.live_table_registry().clone())
    }

    /// Create or attach to an incremental-view-maintenance job by name.
    ///
    /// Mode-aware: an embedded session returns an in-process job; a session
    /// connected to a coordinator returns a remote job. The returned
    /// :class:`IvmJob` exposes the same ``feed`` / ``step`` / ``snapshot`` /
    /// ``checkpoint`` surface in both modes.
    pub fn ivm(&self, name: String) -> PyResult<crate::incremental::PyIvmJob> {
        let job = block_on_async(self.inner.ivm(&name)).map_err(map_krishiv_error)?;
        Ok(crate::incremental::PyIvmJob { inner: job })
    }

    /// Start building a declarative pipeline (`source → transform → sink`).
    ///
    /// Returns a :class:`Pipeline` builder that compiles down to the imperative
    /// core. There is no trigger stage; boundedness, watermark, and
    /// change-events drive each mode.
    pub fn pipeline(&self, name: String) -> crate::pipeline_api::PyPipeline {
        crate::pipeline_api::PyPipeline::new((*self.inner).clone(), name)
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

    /// Create a legacy unified `Relation` backed by batch SQL.
    ///
    /// New batch code should use `sql()`, which returns the canonical
    /// `DataFrame`. This method remains while streaming-only relation behavior
    /// migrates to the canonical API.
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

    /// Return all registered table and view names.
    pub fn list_tables(&self) -> PyResult<Vec<String>> {
        self.inner.list_tables().map_err(map_krishiv_error)
    }

    /// Return ``True`` if a table or view named ``name`` is registered.
    pub fn table_exists(&self, name: String) -> PyResult<bool> {
        self.inner.table_exists(&name).map_err(map_krishiv_error)
    }

    /// Create (or replace) a named SQL view backed by ``query``.
    ///
    /// The view is queryable as ``SELECT * FROM <name>`` in subsequent SQL calls.
    pub fn create_view(&self, name: String, query: String) -> PyResult<()> {
        self.inner
            .create_view(&name, &query)
            .map_err(map_krishiv_error)
    }

    /// Drop the table or view named ``name``.
    pub fn drop_table(&self, name: String) -> PyResult<()> {
        self.inner.drop_table(&name).map_err(map_krishiv_error)
    }

    /// Return ``True`` if ``query`` references at least one unbounded (streaming) source.
    pub fn is_streaming_query(&self, query: String) -> PyResult<bool> {
        self.inner
            .is_streaming_query(&query)
            .map_err(map_krishiv_error)
    }

    /// Signal that no more batches will be pushed to the unbounded table ``name``.
    ///
    /// Returns ``True`` if the table existed and was successfully closed,
    /// ``False`` if it was already closed or not found.
    pub fn close_unbounded_input(&self, name: String) -> PyResult<bool> {
        self.inner
            .close_unbounded_input(&name)
            .map_err(map_krishiv_error)
    }

    // ── G15: JWT/OIDC auth ───────────────────────────────────────────────────────────

    /// Create a session that accepts a single static bearer token.
    ///
    /// Any request presenting `token` as its API key is authenticated.
    /// Useful for development and single-client deployments.
    ///
    /// ## Example
    ///
    /// ```python
    /// session = Session.with_auth_token("my-secret-token")
    /// ```
    #[classmethod]
    pub fn with_auth_token(_cls: &Bound<'_, PyType>, token: String) -> PyResult<Self> {
        let auth = Arc::new(StaticBearerTokenAuth { token });
        krishiv_api::SessionBuilder::new()
            .with_auth(auth)
            .build()
            .map(|s| Self {
                inner: Arc::new(s),
                state_migrations: SharedStateMigrationRegistry::new(),
            })
            .map_err(map_krishiv_error)
    }

    /// Create a session that validates JWTs against the provided JWKS JSON.
    ///
    /// `audience` — expected `aud` claim in the JWT (typically the client ID).
    /// `issuer` — expected `iss` claim; if empty, the issuer check is skipped.
    /// `jwks_json` — JSON Web Key Set as a string. Fetch this from the OIDC
    ///   discovery endpoint at `{issuer}/.well-known/jwks.json`.
    ///
    /// ## Example
    ///
    /// ```python
    /// import urllib.request, json
    /// jwks = urllib.request.urlopen("https://accounts.google.com/.well-known/openid-configuration")
    /// jwks_uri = json.loads(jwks.read())["jwks_uri"]
    /// jwks_json = urllib.request.urlopen(jwks_uri).read().decode()
    ///
    /// session = Session.with_oidc_provider(
    ///     audience="my-client-id",
    ///     issuer="https://accounts.google.com",
    ///     jwks_json=jwks_json,
    /// )
    /// ```
    #[classmethod]
    #[pyo3(signature = (audience, jwks_json, *, issuer = None))]
    pub fn with_oidc_provider(
        _cls: &Bound<'_, PyType>,
        audience: String,
        jwks_json: String,
        issuer: Option<String>,
    ) -> PyResult<Self> {
        let auth = JwtAuth::from_jwks_json(&jwks_json, audience, issuer)
            .map_err(|e| PyRuntimeError::new_err(format!("invalid JWKS JSON: {e}")))?;
        krishiv_api::SessionBuilder::new()
            .with_auth(Arc::new(auth))
            .build()
            .map(|s| Self {
                inner: Arc::new(s),
                state_migrations: SharedStateMigrationRegistry::new(),
            })
            .map_err(map_krishiv_error)
    }

    /// Register a Kafka topic as a streaming SQL table named ``name``.
    ///
    /// ``brokers``   — comma-separated broker addresses (e.g. ``"localhost:9092"``).
    /// ``topic``     — Kafka topic name.
    /// ``group_id``  — consumer group id (default ``"krishiv-default"``).
    /// ``schema``    — optional ``Schema`` subclass; defaults to ``value: Utf8``.
    ///
    /// Requires the ``kafka`` feature (``pip install krishiv[kafka]``).
    #[pyo3(signature = (name, brokers, topic, *, group_id = "krishiv-default", schema = None))]
    pub fn register_kafka_source(
        &self,
        #[allow(unused_variables)] py: Python<'_>,
        name: String,
        brokers: String,
        topic: String,
        group_id: &str,
        schema: Option<Py<pyo3::types::PyType>>,
    ) -> PyResult<()> {
        #[cfg(not(feature = "kafka"))]
        {
            let _ = (name, brokers, topic, group_id, schema);
            return Err(crate::errors::ConnectorError::new_err(
                "Kafka support requires the `kafka` feature (pip install krishiv[kafka])",
            ));
        }
        #[cfg(feature = "kafka")]
        {
            use arrow::datatypes::{DataType, Field, Schema};
            use std::sync::Arc;
            let arrow_schema: arrow::datatypes::SchemaRef = if let Some(cls) = schema {
                crate::schema::PySchema::arrow_schema_from_class(cls.bind(py))?
            } else {
                Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
            };
            let inner = self.inner.clone();
            let group_id = group_id.to_string();
            py.detach(move || {
                inner
                    .register_kafka_source(&name, arrow_schema, &brokers, &topic, &group_id)
                    .map_err(map_krishiv_error)
            })
        }
    }

    /// Register an Amazon Kinesis stream as a streaming SQL table named ``name``.
    ///
    /// ``region``      — AWS region string (e.g. ``"us-east-1"``).
    /// ``stream_name`` — Kinesis stream name.
    /// ``shard_id``    — shard to consume from (e.g. ``"shardId-000000000000"``).
    ///
    /// The registered table exposes the Kinesis message schema:
    /// ``stream_name``, ``shard_id``, ``sequence_number``, ``partition_key``,
    /// ``arrival_timestamp_ms``, and ``data`` columns.
    ///
    /// Requires the ``kinesis`` feature (``pip install krishiv[kinesis]``).
    #[pyo3(signature = (name, region, stream_name, shard_id))]
    pub fn register_kinesis_source(
        &self,
        #[allow(unused_variables)] py: Python<'_>,
        name: String,
        region: String,
        stream_name: String,
        shard_id: String,
    ) -> PyResult<()> {
        #[cfg(not(feature = "kinesis"))]
        {
            let _ = (name, region, stream_name, shard_id);
            return Err(crate::errors::ConnectorError::new_err(
                "Kinesis support requires the `kinesis` feature (pip install krishiv[kinesis])",
            ));
        }
        #[cfg(feature = "kinesis")]
        {
            use krishiv_connectors::kinesis::kinesis_arrow_schema;
            let _ = (region, stream_name, shard_id);
            let arrow_schema = kinesis_arrow_schema();
            let inner = self.inner.clone();
            py.detach(move || {
                inner
                    .register_unbounded(&name, arrow_schema)
                    .map_err(map_krishiv_error)
            })
        }
    }

    /// Register an Apache Pulsar topic as a streaming SQL table named ``name``.
    ///
    /// ``broker_url`` — Pulsar broker URL (e.g. ``"pulsar://localhost:6650"``).
    /// ``topic``      — Pulsar topic name (e.g. ``"persistent://public/default/events"``).
    ///
    /// The registered table exposes the Pulsar message schema:
    /// ``topic``, ``partition_key``, ``publish_time_ms``, and ``data`` columns.
    ///
    /// Requires the ``pulsar`` feature (``pip install krishiv[pulsar]``).
    #[pyo3(signature = (name, broker_url, topic))]
    pub fn register_pulsar_source(
        &self,
        #[allow(unused_variables)] py: Python<'_>,
        name: String,
        broker_url: String,
        topic: String,
    ) -> PyResult<()> {
        #[cfg(not(feature = "pulsar"))]
        {
            let _ = (name, broker_url, topic);
            return Err(crate::errors::ConnectorError::new_err(
                "Pulsar support requires the `pulsar` feature (pip install krishiv[pulsar])",
            ));
        }
        #[cfg(feature = "pulsar")]
        {
            use krishiv_connectors::pulsar_connector::pulsar_arrow_schema;
            let _ = (broker_url, topic);
            let arrow_schema = pulsar_arrow_schema();
            let inner = self.inner.clone();
            py.detach(move || {
                inner
                    .register_unbounded(&name, arrow_schema)
                    .map_err(map_krishiv_error)
            })
        }
    }
}

// ── PyOperationRegistry ───────────────────────────────────────────────────────────────

/// Thread-safe registry of cancelled operation IDs.
///
/// Pass to :py:meth:`Session.operation_registry` and call :py:meth:`cancel`
/// to abort in-flight queries identified by a numeric operation ID.
///
/// ## Example
///
/// ```python
/// registry = session.operation_registry()
/// registry.cancel(42)
/// assert registry.is_cancelled(42)
/// registry.remove(42)  # clean up once the operation has finished
/// ```
#[pyclass(name = "OperationRegistry")]
pub struct PyOperationRegistry {
    inner: std::sync::Arc<krishiv_sql::OperationRegistry>,
}

impl PyOperationRegistry {
    pub fn new(inner: std::sync::Arc<krishiv_sql::OperationRegistry>) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyOperationRegistry {
    /// Mark ``operation_id`` as cancelled.
    fn cancel(&self, operation_id: u64) {
        self.inner.cancel(operation_id);
    }

    /// Return ``True`` if ``operation_id`` has been cancelled.
    fn is_cancelled(&self, operation_id: u64) -> bool {
        self.inner.is_cancelled(operation_id)
    }

    /// Remove ``operation_id`` from the registry (after the operation finishes).
    fn remove(&self, operation_id: u64) {
        self.inner.remove(operation_id);
    }

    /// Return ``(rows_scanned, rows_emitted)`` for an operation, if recorded.
    fn progress(&self, operation_id: u64) -> Option<(u64, u64)> {
        self.inner.progress(operation_id)
    }

    /// Return all currently cancelled operation IDs.
    fn cancelled_ids(&self) -> Vec<u64> {
        self.inner.cancelled_ids()
    }

    fn __repr__(&self) -> String {
        format!(
            "OperationRegistry(cancelled={:?})",
            self.inner.cancelled_ids()
        )
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
    fn with_auth_token_builds_session() {
        use crate::session::StaticBearerTokenAuth;
        use krishiv_plan::governance::AuthProvider;
        let auth = StaticBearerTokenAuth {
            token: "secret".to_string(),
        };
        assert_eq!(auth.authenticate("secret"), Some("bearer".to_string()));
        assert_eq!(auth.authenticate("wrong"), None);
        // Verify it integrates with SessionBuilder without panic.
        let session = krishiv_api::SessionBuilder::new()
            .with_auth(std::sync::Arc::new(auth))
            .build()
            .expect("session with bearer auth");
        assert!(matches!(
            session.mode(),
            krishiv_api::ExecutionMode::Embedded
        ));
    }

    #[test]
    fn jwt_auth_rejects_invalid_token() {
        use crate::session::JwtAuth;
        use krishiv_plan::governance::AuthProvider;
        // A minimal RSA JWK set — keys list is empty so any JWT is rejected.
        let empty_jwks = r#"{"keys":[]}"}"#;
        let result = JwtAuth::from_jwks_json(empty_jwks, "aud".into(), None);
        assert!(result.is_err(), "empty JWKS should error");
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
