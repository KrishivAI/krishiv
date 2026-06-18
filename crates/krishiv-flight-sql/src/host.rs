//! Shared server-side execution host for the Krishiv Flight SQL service.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_runtime::flight_protocol::{FlightDirective, apply_register_directives, parse_sql};
use krishiv_runtime::in_process_cluster::{InProcessCluster, plan_spec_to_local};
use krishiv_scheduler::{BatchSqlInlineTable, SharedCoordinator, execute_batch_sql_coordinated};
use krishiv_sql::explain_sql;
use tonic::Status;

/// Execution backend for the Flight SQL service.
///
/// Each variant provides the same operations through a different mechanism.
pub(crate) enum FlightHostBackend {
    /// Fully in-process execution on a local cluster.
    /// Used by the standalone `krishiv flight-server` process.
    InProcess(Arc<InProcessCluster>),
    /// Direct call into a co-located coordinator — same process, no network hop.
    /// Used when the coordinator spawns the Flight server as a co-located sidecar.
    Coordinator(SharedCoordinator),
}

/// Server-side catalog and cluster state shared across Flight SQL requests.
///
/// **Embedded mode** (`backend = InProcess`): all operations run on the local
/// `InProcessCluster`. Used by the standalone `krishiv flight-server` process.
///
/// **Co-located mode** (`backend = Coordinator`): all execution goes directly
/// to the co-located coordinator — no HTTP, no serialisation overhead. Used when
/// the coordinator spawns the Flight server as a sidecar within the same process.
#[derive(Clone)]
pub struct FlightExecutionHost {
    pub(crate) backend: Arc<FlightHostBackend>,
    /// Path-based Parquet table catalog shared across concurrent requests.
    /// Uses DashMap for lock-free concurrent access.
    catalog: Arc<DashMap<String, PathBuf>>,
    /// Cancelled operations and progress snapshots for remote query lifecycle control.
    operation_registry: Arc<krishiv_sql::OperationRegistry>,
    /// Optional HTTP URL of a remote coordinator, for informational / test use.
    coordinator_http_url: Option<String>,
}

impl FlightExecutionHost {
    /// Create an embedded host backed by a new in-process cluster.
    ///
    /// Used by the standalone `krishiv flight-server` subcommand.
    pub fn embedded() -> Result<Self, Status> {
        let cluster = InProcessCluster::new().map_err(|e| Status::internal(e.to_string()))?;
        Ok(Self {
            backend: Arc::new(FlightHostBackend::InProcess(Arc::new(cluster))),
            catalog: Arc::new(DashMap::new()),
            operation_registry: Arc::new(krishiv_sql::OperationRegistry::new()),
            coordinator_http_url: None,
        })
    }

    /// Create a host backed directly by a running coordinator.
    ///
    /// All execution goes directly to the coordinator — no HTTP, no serialisation
    /// overhead. Used by the coordinator when spawning Flight SQL as a co-located
    /// sidecar via `spawn_coordinator_sidecars`.
    pub fn with_coordinator(coordinator: SharedCoordinator) -> Self {
        Self {
            backend: Arc::new(FlightHostBackend::Coordinator(coordinator)),
            catalog: Arc::new(DashMap::new()),
            operation_registry: Arc::new(krishiv_sql::OperationRegistry::new()),
            coordinator_http_url: None,
        }
    }

    /// Create an embedded host that remembers a remote coordinator HTTP URL.
    ///
    /// Useful for tests and tooling that need to record the coordinator address
    /// without actually connecting to it.
    pub fn with_coordinator_http(url: Option<String>) -> Result<Self, Status> {
        let cluster = InProcessCluster::new().map_err(|e| Status::internal(e.to_string()))?;
        Ok(Self {
            backend: Arc::new(FlightHostBackend::InProcess(Arc::new(cluster))),
            catalog: Arc::new(DashMap::new()),
            operation_registry: Arc::new(krishiv_sql::OperationRegistry::new()),
            coordinator_http_url: url,
        })
    }

    /// Shared operation registry for cancellation and progress reporting.
    pub fn operation_registry(&self) -> Arc<krishiv_sql::OperationRegistry> {
        Arc::clone(&self.operation_registry)
    }

    /// Cancel an in-flight operation by ID.
    pub fn cancel_operation(&self, operation_id: u64) {
        self.operation_registry.cancel(operation_id);
    }

    /// Return the latest progress snapshot for an operation, if recorded.
    pub fn operation_progress(&self, operation_id: u64) -> Option<(u64, u64)> {
        self.operation_registry.progress(operation_id)
    }

    /// Return the coordinator HTTP URL recorded at construction time, if any.
    pub fn coordinator_http_url(&self) -> Option<&str> {
        self.coordinator_http_url.as_deref()
    }

    /// Build from environment variables (for standalone flight-server use only).
    ///
    /// Always creates an embedded host. Co-located mode is wired by the binary
    /// layer via `FlightExecutionHost::with_coordinator`.
    pub fn from_env() -> Result<Self, Status> {
        Self::embedded()
    }

    /// Legacy constructor — kept for existing call sites.
    pub fn new() -> Result<Self, Status> {
        Self::from_env()
    }

    // Catalog management (shared by both backends).

    fn apply_catalog_directives(&self, directives: &[FlightDirective]) -> Result<(), Status> {
        let mut catalog_map = std::collections::HashMap::new();
        apply_register_directives(&mut catalog_map, directives);
        for (table, path) in catalog_map {
            self.catalog.insert(table, path);
        }
        Ok(())
    }

    fn catalog_tables(&self) -> Vec<krishiv_runtime::in_process::BatchSqlTable> {
        self.catalog
            .iter()
            .map(|entry| krishiv_runtime::in_process::BatchSqlTable {
                table_name: entry.key().clone(),
                path: entry.value().clone(),
            })
            .collect()
    }

    /// Register a Parquet file path under a table name.
    ///
    /// Works identically for both backends — the catalog is client-side.
    pub fn register_parquet(&self, table: &str, path: impl Into<PathBuf>) {
        self.catalog.insert(table.to_owned(), path.into());
    }

    /// Return all registered catalog tables as (catalog, schema, table_name) tuples.
    ///
    /// Used by the Flight SQL catalog handlers (`GetDbSchemas`, `GetTables`) to
    /// list the tables registered in this host's client-side Parquet catalog.
    pub(crate) fn list_catalog_tables(&self) -> Vec<(String, String, String)> {
        let mut entries: Vec<(String, String, String)> = self
            .catalog
            .iter()
            .map(|e| {
                (
                    "krishiv".to_string(),
                    "default".to_string(),
                    e.key().clone(),
                )
            })
            .collect();
        entries.sort();
        entries
    }

    // Execution methods — dispatched by backend variant.

    /// Execute a batch SQL query and return the result as record batches.
    ///
    /// `inline_tables` carries inline Arrow IPC payloads for the Coordinator
    /// backend. For the InProcess backend the catalog tables are used instead.
    pub async fn execute_batch_sql(
        &self,
        query: &str,
        inline_tables: &[BatchSqlInlineTable],
    ) -> Result<Vec<RecordBatch>, Status> {
        match self.backend.as_ref() {
            FlightHostBackend::InProcess(cluster) => {
                let tables = self.catalog_tables();
                let sql = query.to_string();
                let cluster = Arc::clone(cluster);
                let is_streaming = match cluster.is_streaming_query(&sql) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "streaming detection failed; treating query as batch"
                        );
                        false
                    }
                };
                run_blocking(move || cluster.collect_batch_sql(&sql, &tables, is_streaming))
            }
            FlightHostBackend::Coordinator(coordinator) => {
                let outcome = execute_batch_sql_coordinated(coordinator, query, inline_tables)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                krishiv_scheduler::decode_inline_record_batches(&outcome.inline_record_batch_ipc)
                    .map_err(|e| Status::internal(e.to_string()))
            }
        }
    }

    /// Execute a batch SQL write through a sink output contract (Phase 2.3
    /// staged distributed write). Blocks until the job has succeeded and its
    /// staged output has been published; sink jobs return no result rows.
    pub async fn execute_batch_sql_sink(
        &self,
        query: &str,
        inline_tables: &[BatchSqlInlineTable],
        sink_contract: &str,
    ) -> Result<(), Status> {
        match self.backend.as_ref() {
            FlightHostBackend::InProcess(cluster) => {
                let tables = self.catalog_tables();
                let sql = query.to_string();
                let contract = sink_contract.to_string();
                let cluster = Arc::clone(cluster);
                run_blocking(move || cluster.execute_batch_sql_sink(&sql, &tables, &contract))
            }
            FlightHostBackend::Coordinator(coordinator) => {
                krishiv_scheduler::execute_batch_sql_sink_coordinated(
                    coordinator,
                    query,
                    inline_tables,
                    sink_contract,
                )
                .await
                .map(|_| ())
                .map_err(|e| Status::internal(e.to_string()))
            }
        }
    }

    /// Execute a bounded window operation.
    pub async fn execute_bounded_window(
        &self,
        topic: &str,
        spec: &krishiv_plan::window::WindowExecutionSpec,
        input_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, Status> {
        match self.backend.as_ref() {
            FlightHostBackend::InProcess(cluster) => {
                let local = plan_spec_to_local(spec);
                let topic = topic.to_string();
                let cluster = Arc::clone(cluster);
                run_blocking(move || cluster.collect_bounded_window(&topic, input_batches, &local))
            }
            FlightHostBackend::Coordinator(coordinator) => {
                let outcome = krishiv_scheduler::execute_bounded_window_coordinated(
                    coordinator,
                    topic,
                    spec,
                    &input_batches,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
                krishiv_scheduler::decode_inline_record_batches(&outcome.inline_record_batch_ipc)
                    .map_err(|e| Status::internal(e.to_string()))
            }
        }
    }

    /// Register a continuous streaming job.
    pub async fn register_continuous_stream(
        &self,
        job_id: &str,
        spec: &krishiv_plan::window::WindowExecutionSpec,
    ) -> Result<(), Status> {
        match self.backend.as_ref() {
            FlightHostBackend::InProcess(cluster) => {
                let local = plan_spec_to_local(spec);
                let job_id = job_id.to_string();
                let cluster = Arc::clone(cluster);
                run_blocking(move || cluster.register_continuous_job(&job_id, &local))
            }
            FlightHostBackend::Coordinator(coordinator) => {
                // Delegate to the public helper in krishiv-scheduler which
                // accesses coordinator internals within the same crate.
                krishiv_scheduler::register_continuous_stream_coordinated(coordinator, job_id, spec)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))
            }
        }
    }

    /// Push a batch of records as input for one continuous streaming cycle.
    pub async fn push_continuous_input(
        &self,
        job_id: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), Status> {
        match self.backend.as_ref() {
            FlightHostBackend::InProcess(cluster) => {
                let job_id = job_id.to_string();
                let cluster = Arc::clone(cluster);
                run_blocking(move || cluster.push_continuous_input(&job_id, batches))
            }
            FlightHostBackend::Coordinator(coordinator) => {
                use arrow::ipc::writer::StreamWriter;

                if batches.is_empty() {
                    return Err(Status::invalid_argument(
                        "push_continuous_input: no input batches provided",
                    ));
                }

                // Encode the record batches as an Arrow IPC stream.
                let schema = batches[0].schema();
                let mut ipc_buf = Vec::new();
                {
                    let mut writer = StreamWriter::try_new(&mut ipc_buf, &schema)
                        .map_err(|e| Status::internal(format!("ipc encode: {e}")))?;
                    for batch in &batches {
                        writer
                            .write(batch)
                            .map_err(|e| Status::internal(format!("ipc write: {e}")))?;
                    }
                    writer
                        .finish()
                        .map_err(|e| Status::internal(format!("ipc finish: {e}")))?;
                }

                // Delegate to the public helper in krishiv-scheduler which
                // accesses the coordinator internals within the same crate.
                krishiv_scheduler::push_continuous_input_coordinated(coordinator, job_id, ipc_buf)
                    .await
                    .map_err(|e| match e {
                        krishiv_scheduler::ContinuousStreamError::Unavailable(msg) => {
                            Status::unavailable(msg)
                        }
                        krishiv_scheduler::ContinuousStreamError::Aborted(msg) => {
                            Status::aborted(msg)
                        }
                        other => Status::internal(other.to_string()),
                    })
            }
        }
    }

    /// Drain completed results from a continuous streaming job.
    pub async fn drain_continuous_stream(&self, job_id: &str) -> Result<Vec<RecordBatch>, Status> {
        match self.backend.as_ref() {
            FlightHostBackend::InProcess(cluster) => {
                let job_id = job_id.to_string();
                let cluster = Arc::clone(cluster);
                run_blocking(move || cluster.drain_continuous_job(&job_id))
            }
            FlightHostBackend::Coordinator(coordinator) => {
                // Delegate to the public helper in krishiv-scheduler which
                // accesses coordinator internals within the same crate.
                let ipc_payloads =
                    krishiv_scheduler::drain_continuous_stream_coordinated(coordinator, job_id)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;

                krishiv_scheduler::decode_inline_record_batches(&ipc_payloads)
                    .map_err(|e| Status::internal(e.to_string()))
            }
        }
    }

    /// Explain a SQL query. Always local/DataFusion regardless of backend.
    pub fn explain_sql_query(&self, query: &str) -> Result<String, Status> {
        explain_sql(query).map_err(|e| Status::internal(e.to_string()))
    }

    /// Register a Kafka streaming source.
    ///
    /// In `InProcess` mode registers on the local cluster's SQL engine.
    /// In `Coordinator` mode a warning is emitted — direct coordinator kafka
    /// registration is not yet accessible via a public coordinator method.
    #[cfg(feature = "kafka")]
    pub fn register_kafka_source(
        &self,
        name: &str,
        schema_ipc_b64: &str,
        bootstrap_servers: &str,
        topic: &str,
        group_id: &str,
    ) -> Result<(), Status> {
        let schema = krishiv_runtime::decode_schema_ipc_b64(schema_ipc_b64)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        match self.backend.as_ref() {
            FlightHostBackend::InProcess(cluster) => {
                let cluster = Arc::clone(cluster);
                run_blocking(|| {
                    cluster.register_kafka_source(name, schema, bootstrap_servers, topic, group_id)
                })
            }
            FlightHostBackend::Coordinator(_) => {
                tracing::warn!(
                    name,
                    "Kafka source registration in co-located mode is not yet implemented; \
                     the source will not be visible to the coordinator's executors"
                );
                Ok(())
            }
        }
    }

    /// Execute raw SQL (legacy ADBC / simple-client path).
    ///
    /// Parses `FlightDirective` comment-encoded control messages for backward
    /// compatibility. New clients should use typed `DoAction` calls instead.
    pub async fn execute_sql(&self, raw_sql: &str) -> Result<Vec<RecordBatch>, Status> {
        let (directives, sql) = parse_sql(raw_sql);
        self.apply_catalog_directives(&directives)?;

        // Collect any inline IPC tables from directives.
        let ipc_tables: Vec<BatchSqlInlineTable> = directives
            .iter()
            .filter_map(|d| {
                if let FlightDirective::RegisterParquetIpc { table, ipc_b64 } = d {
                    Some(BatchSqlInlineTable {
                        table_name: table.clone(),
                        ipc_b64: ipc_b64.clone(),
                    })
                } else {
                    None
                }
            })
            .collect();

        // Handle explain directive.
        if directives
            .iter()
            .any(|d| matches!(d, FlightDirective::Explain))
        {
            let text = explain_sql(sql).map_err(|e| Status::internal(e.to_string()))?;
            return Ok(vec![explain_batch(&text)?]);
        }

        // Handle streaming control directives (register/push/drain).
        for directive in &directives {
            match directive {
                FlightDirective::ContinuousRegister { job_id, spec } => {
                    self.register_continuous_stream(job_id, spec).await?;
                    return Ok(vec![status_batch("ok")?]);
                }
                FlightDirective::ContinuousPush { job_id, batches } => {
                    self.push_continuous_input(job_id, batches.clone()).await?;
                    return Ok(vec![status_batch("ok")?]);
                }
                FlightDirective::ContinuousDrain { job_id } => {
                    return self.drain_continuous_stream(job_id).await;
                }
                FlightDirective::BoundedWindow {
                    topic,
                    spec,
                    input_batches,
                } => {
                    return self
                        .execute_bounded_window(topic, spec, input_batches.clone())
                        .await;
                }
                FlightDirective::Explain
                | FlightDirective::RegisterParquet { .. }
                | FlightDirective::RegisterParquetIpc { .. } => {}
            }
        }

        // Plain SQL execution.
        self.execute_batch_sql(&sql, &ipc_tables).await
    }
}

// Async bridge for blocking cluster calls.

/// Run a blocking cluster operation off the async caller without a
/// `spawn_blocking` / `block_on` triple-hop.
///
/// On a multi-threaded tokio runtime it uses `block_in_place`, which stays on
/// the current worker thread and lets the executor run other tasks while this
/// one blocks. On a current-thread/test runtime it falls back to
/// `std::thread::scope`, spawning a dedicated OS thread so the blocking call
/// never stalls the (single) worker.
///
/// The closure must be `Send` so the `std::thread::scope` fallback can move it
/// into the spawned thread. A panic inside the closure is caught and surfaced
/// as `Status::internal("run_blocking thread panicked")` rather than being
/// allowed to unwind across the thread boundary.
pub(crate) fn run_blocking<T: Send>(
    f: impl FnOnce() -> Result<T, krishiv_runtime::RuntimeError> + Send,
) -> Result<T, Status> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f).map_err(|e| Status::internal(e.to_string()))
        }
        _ => std::thread::scope(|scope| {
            scope
                .spawn(f)
                .join()
                .map_err(|_| Status::internal("run_blocking thread panicked"))
                .and_then(|r| r.map_err(|e| Status::internal(e.to_string())))
        }),
    }
}

pub(crate) fn explain_batch(text: &str) -> Result<RecordBatch, Status> {
    let schema = Arc::new(Schema::new(vec![Field::new("plan", DataType::Utf8, false)]));
    let lines: StringArray = text.lines().map(Some).collect();
    RecordBatch::try_new(schema, vec![Arc::new(lines) as ArrayRef])
        .map_err(|e| Status::internal(e.to_string()))
}

pub(crate) fn status_batch(label: &str) -> Result<RecordBatch, Status> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Utf8,
        false,
    )]));
    let col = Arc::new(StringArray::from(vec![label])) as ArrayRef;
    RecordBatch::try_new(schema, vec![col]).map_err(|e| Status::internal(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn embedded_host_executes_simple_select() {
        let host = FlightExecutionHost::embedded().unwrap();
        let batches = host.execute_sql("SELECT 42 AS n").await.unwrap();
        assert!(!batches.is_empty());
    }

    #[tokio::test]
    async fn embedded_host_explain_directive() {
        let host = FlightExecutionHost::embedded().unwrap();
        let sql = krishiv_runtime::flight_protocol::encode_explain_sql("SELECT 1");
        let batches = host.execute_sql(&sql).await.unwrap();
        assert!(!batches.is_empty());
    }

    /// Regression (Wave 2 — Panic Propagation): `run_blocking` must convert a
    /// panicking closure into a `Status::internal` error rather than letting
    /// the panic unwind across the thread boundary and take down the server.
    /// `#[tokio::test]` defaults to a current-thread runtime, so this exercises
    /// the `std::thread::scope` fallback branch rather than `block_in_place`.
    #[tokio::test]
    async fn run_blocking_converts_closure_panic_to_internal_status() {
        let result: Result<i32, Status> =
            run_blocking(|| -> Result<i32, krishiv_runtime::RuntimeError> {
                panic!("intentional panic from run_blocking test")
            });
        let status = result.expect_err("a panicking closure must surface as an error");
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(
            status.message().contains("run_blocking thread panicked"),
            "expected a 'run_blocking thread panicked' message, got: {}",
            status.message()
        );
    }

    #[tokio::test]
    async fn coordinator_backend_is_set_correctly() {
        use krishiv_proto::CoordinatorId;
        use krishiv_scheduler::{Coordinator, SharedCoordinator};

        let coord_id = CoordinatorId::try_new("test-coord-host").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(coord_id));
        let host = FlightExecutionHost::with_coordinator(shared);
        assert!(
            matches!(host.backend.as_ref(), FlightHostBackend::Coordinator(_)),
            "with_coordinator must set the Coordinator backend"
        );
    }

    #[tokio::test]
    async fn from_env_returns_embedded_backend() {
        // from_env() must not read KRISHIV_COORDINATOR_HTTP — always returns embedded.
        let host = FlightExecutionHost::from_env().unwrap();
        assert!(
            matches!(host.backend.as_ref(), FlightHostBackend::InProcess(_)),
            "from_env must produce an InProcess backend"
        );
    }
}
