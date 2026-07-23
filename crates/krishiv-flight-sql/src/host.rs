//! Shared server-side execution host for the Krishiv Flight SQL service.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_flight::error::FlightError;
use dashmap::DashMap;
use futures::{Stream, StreamExt as _, stream};
use krishiv_runtime::flight_protocol::{FlightDirective, apply_register_directives, parse_sql};
use krishiv_runtime::in_process_cluster::{InProcessCluster, plan_spec_to_local};
use krishiv_scheduler::{
    BatchSqlInlineTable, BatchSqlTable, SharedCoordinator, execute_batch_sql_coordinated_with_paths,
};
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
                ..Default::default()
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
        self.execute_batch_sql_with_paths(query, inline_tables, &[])
            .await
    }

    /// Execute a batch SQL query with both inline-IPC and path-registered
    /// tables.
    ///
    /// `path_tables` carry a shared-filesystem path instead of inline Arrow IPC
    /// bytes — the client emits them when a parquet table is too large to inline
    /// (over the inline-IPC cap) but is reachable from the coordinator and every
    /// executor. On the Coordinator backend they become `LocalParquet` inputs
    /// (and, for plain SELECTs, are eligible for partition-parallel staged
    /// execution). On the InProcess backend they are registered into the
    /// client-side catalog so the shared-context query resolves them.
    pub async fn execute_batch_sql_with_paths(
        &self,
        query: &str,
        inline_tables: &[BatchSqlInlineTable],
        path_tables: &[BatchSqlTable],
    ) -> Result<Vec<RecordBatch>, Status> {
        match self.backend.as_ref() {
            FlightHostBackend::InProcess(cluster) => {
                // The in-process backend resolves tables through the client-side
                // catalog, so register any path tables before running.
                for t in path_tables {
                    self.register_parquet(&t.table_name, t.path.clone());
                }
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
                let outcome = execute_batch_sql_coordinated_with_paths(
                    coordinator,
                    query,
                    inline_tables,
                    path_tables,
                )
                .await
                .map_err(scheduler_error_to_status)?;
                let mut batches = krishiv_scheduler::decode_inline_record_batches(
                    &outcome.inline_record_batch_ipc,
                )
                .map_err(|e| {
                    krishiv_metrics::grpc::internal_status("decode inline result batches", &e)
                })?;
                // Phase 2.10: large task results arrive as disk spools instead
                // of inline bytes; decode them straight from the spool files
                // (files delete themselves when the outcome drops).
                for spool in &outcome.result_spools {
                    batches.extend(spool.decode_record_batches().map_err(|e| {
                        krishiv_metrics::grpc::internal_status("decode result spool", &e)
                    })?);
                }
                Ok(batches)
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
                let schema = batches
                    .first()
                    .map(|b| b.schema())
                    .unwrap_or_else(|| std::sync::Arc::new(Schema::empty()));
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

    /// Best-effort output schema for a read-only statement, without
    /// executing it.
    ///
    /// Only statements whose leading keyword is side-effect-free under
    /// planning (`SELECT`/`WITH`/`VALUES`/`SHOW`/`EXPLAIN`) are planned —
    /// DDL/DML would execute eagerly at plan time. `None` means "schema
    /// unknown": the InProcess backend could not plan the statement (e.g.
    /// it references tables only the coordinator knows), or the backend is
    /// a remote coordinator (no remote plan/schema API yet). Callers must
    /// treat `None` as unknown, never as "no columns".
    pub async fn sql_query_schema(&self, query: &str) -> Option<arrow::datatypes::SchemaRef> {
        let head = query
            .trim_start()
            .get(..8)
            .unwrap_or(query.trim_start())
            .to_ascii_uppercase();
        let read_only = ["SELECT", "WITH", "VALUES", "SHOW", "EXPLAIN"]
            .iter()
            .any(|kw| head.starts_with(kw));
        if !read_only {
            return None;
        }
        match self.backend.as_ref() {
            FlightHostBackend::InProcess(cluster) => cluster
                .streaming_runtime()
                .runner_sql_engine()
                .sql(query)
                .await
                .ok()
                .map(|df| df.arrow_schema()),
            FlightHostBackend::Coordinator(_) => None,
        }
    }

    /// Register an Iceberg REST catalog on the local runner SQL engine from
    /// the environment; the platform daemon points these at its catalog so
    /// SQL can reference governed tables as `<name>.<namespace>.<table>`.
    ///
    /// Env: `KRISHIV_ICEBERG_REST_URI` (activates when set),
    /// `KRISHIV_ICEBERG_REST_WAREHOUSE`, `KRISHIV_ICEBERG_REST_TOKEN`
    /// (bearer, e.g. a platform PAT), `KRISHIV_ICEBERG_REST_NAME`
    /// (catalog name, default `main` — the platform's canonical catalog).
    ///
    /// Returns `Ok(false)` when the env var is unset or the backend is
    /// coordinator-delegated (registration must happen on the coordinator's
    /// engine — not reachable from here yet).
    #[cfg(feature = "rest-catalog")]
    pub async fn register_rest_catalog_from_env(&self) -> Result<bool, Status> {
        let Ok(uri) = std::env::var("KRISHIV_ICEBERG_REST_URI") else {
            return Ok(false);
        };
        let warehouse = std::env::var("KRISHIV_ICEBERG_REST_WAREHOUSE").unwrap_or_default();
        let token = std::env::var("KRISHIV_ICEBERG_REST_TOKEN").ok();
        let name =
            std::env::var("KRISHIV_ICEBERG_REST_NAME").unwrap_or_else(|_| String::from("main"));
        match self.backend.as_ref() {
            FlightHostBackend::InProcess(cluster) => {
                let catalog = krishiv_sql::catalog::unified::KrishivCatalog::rest(
                    &uri,
                    &warehouse,
                    token.as_deref(),
                )
                .await
                .map_err(|e| Status::internal(format!("iceberg REST catalog at {uri}: {e}")))?;
                cluster
                    .streaming_runtime()
                    .runner_sql_engine()
                    .register_iceberg_catalog(Arc::new(catalog), &name);
                tracing::info!(%uri, catalog = %name, "iceberg REST catalog registered");
                Ok(true)
            }
            FlightHostBackend::Coordinator(_) => {
                tracing::warn!(
                    "KRISHIV_ICEBERG_REST_URI is set but the Flight host delegates to a \
                     coordinator; register the catalog on the coordinator instead"
                );
                Ok(false)
            }
        }
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
                // M-12 (audit): the prior implementation returned `Ok(())`
                // and just logged a warning. Clients had no way to know
                // the registration was a no-op. Surface the limitation
                // as a proper gRPC error so the caller fails fast and
                // can fall back to the InProcess backend.
                Err(Status::unimplemented(
                    "Kafka source registration in co-located mode is not yet implemented; \
                     use the InProcess backend or call Session::register_kafka_source from \
                     the coordinator's own admin API",
                ))
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
                | FlightDirective::RegisterParquetIpc { .. }
                | FlightDirective::RegisterPythonUdf { .. } => {}
            }
        }

        // Re-attach any Python-UDF directives to the query so they travel with
        // the fragment SQL to the executors, which register them on their
        // per-task engine before planning (the executor has the python worker;
        // the coordinator only forwards the fragment).
        let udf_comments: Vec<String> = directives
            .iter()
            .filter_map(|d| match d {
                FlightDirective::RegisterPythonUdf {
                    name,
                    input_types,
                    output_type,
                    pickle_b64,
                } => Some(krishiv_runtime::flight_protocol::encode_python_udf(
                    name,
                    input_types,
                    output_type,
                    pickle_b64,
                )),
                _ => None,
            })
            .collect();
        let sql = if udf_comments.is_empty() {
            sql
        } else {
            format!("{}\n{}", udf_comments.join("\n"), sql)
        };

        // Plain SQL execution.
        self.execute_batch_sql(&sql, &ipc_tables).await
    }

    /// Like [`Self::execute_sql`], but avoids collecting the whole result
    /// into memory before the client sees a single row, when possible.
    ///
    /// #211: `execute_sql` always returned `Vec<RecordBatch>` — the Flight
    /// host materialized the entire result before `do_get` streamed a
    /// single byte to the client, which is what actually produced the
    /// reported `resource_exhausted "Flight SQL result (8149911680 bytes)
    /// exceeds maximum"` on an un-LIMITed SELECT. Directives (explain,
    /// continuous register/push/drain, bounded window) and the
    /// `InProcess` backend still execute through the existing fully
    /// buffered path — their results are inherently small (control
    /// messages, explain text) or would need touching DataFusion's own
    /// execution model to stream (residual, not attempted this pass) —
    /// and come back as [`SqlResultDelivery::Buffered`]. Plain SQL against
    /// the `Coordinator` backend (the `prod`/`distributed` preset, and the
    /// actual trigger for the reported OOM) decodes its on-disk
    /// task-result spools one at a time as the client drains `do_get`,
    /// instead of eagerly flattening every spool into one `Vec` first —
    /// peak host memory drops from O(total result size) to O(largest
    /// single spool).
    pub async fn execute_sql_stream(&self, raw_sql: &str) -> Result<SqlResultDelivery, Status> {
        let (directives, sql) = parse_sql(raw_sql);
        let has_special_directive = directives.iter().any(|d| {
            matches!(
                d,
                FlightDirective::Explain
                    | FlightDirective::ContinuousRegister { .. }
                    | FlightDirective::ContinuousPush { .. }
                    | FlightDirective::ContinuousDrain { .. }
                    | FlightDirective::BoundedWindow { .. }
            )
        });
        if has_special_directive || matches!(self.backend.as_ref(), FlightHostBackend::InProcess(_))
        {
            return self
                .execute_sql(raw_sql)
                .await
                .map(SqlResultDelivery::Buffered);
        }

        self.apply_catalog_directives(&directives)?;
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

        // Carry Python-UDF directives into the fragment SQL so executors register
        // them (the coordinator only forwards the fragment).
        let udf_comments: Vec<String> = directives
            .iter()
            .filter_map(|d| match d {
                FlightDirective::RegisterPythonUdf {
                    name,
                    input_types,
                    output_type,
                    pickle_b64,
                } => Some(krishiv_runtime::flight_protocol::encode_python_udf(
                    name,
                    input_types,
                    output_type,
                    pickle_b64,
                )),
                _ => None,
            })
            .collect();
        let sql = if udf_comments.is_empty() {
            sql
        } else {
            format!("{}\n{}", udf_comments.join("\n"), sql)
        };

        let FlightHostBackend::Coordinator(coordinator) = self.backend.as_ref() else {
            unreachable!("InProcess already returned above");
        };
        let outcome =
            execute_batch_sql_coordinated_with_paths(coordinator, &sql, &ipc_tables, &[])
                .await
                .map_err(scheduler_error_to_status)?;
        let inline = krishiv_scheduler::decode_inline_record_batches(
            &outcome.inline_record_batch_ipc,
        )
        .map_err(|e| krishiv_metrics::grpc::internal_status("decode inline result batches", &e))?;
        let mut spools = outcome.result_spools.into_iter();

        /// One spool's batches as a boxed `Result<RecordBatch, Status>`
        /// iterator — the common type both the `Ok` (genuinely lazy,
        /// `StreamReader` itself) and `Err` (one-item) cases below collapse
        /// to, so `flat_map` (and the prefix-spool case) can return either
        /// without an intermediate `Vec` forcing the whole spool to decode
        /// up front.
        type SpoolBatchIter = Box<dyn Iterator<Item = Result<RecordBatch, Status>> + Send>;
        fn spool_iter(spool: krishiv_scheduler::TaskResultSpool) -> SpoolBatchIter {
            match spool.decode_record_batches_streaming() {
                Ok(reader) => Box::new(reader.map(|r| {
                    r.map_err(|e| krishiv_metrics::grpc::internal_status("decode result spool", &e))
                })),
                Err(e) => Box::new(std::iter::once(Err(krishiv_metrics::grpc::internal_status(
                    "decode result spool",
                    &e,
                )))),
            }
        }

        // #211 residual: the schema used to come from eagerly decoding the
        // whole first spool (`decode_record_batches` collects every batch
        // into a `Vec`) just to read `.schema()` off the first `RecordBatch`
        // — for a query that ran as a single task, that spool. IS. the
        // entire result, so "peek the schema" meant "materialize the whole
        // multi-GiB result" before a single byte reached the client, the
        // exact OOM shape #211 was filed against. `StreamReader::schema()`
        // is free: the IPC stream's header carries the schema separately
        // from its batch messages, parsed once in `try_new` — no batch
        // decode needed at all.
        let (schema, prefix): (Arc<Schema>, SpoolBatchIter) = if let Some(first) = inline.first() {
            (
                first.schema(),
                Box::new(inline.into_iter().map(Ok)) as SpoolBatchIter,
            )
        } else if let Some(first_spool) = spools.next() {
            match first_spool.decode_record_batches_streaming() {
                Ok(reader) => {
                    let schema = reader.schema();
                    let iter: SpoolBatchIter = Box::new(reader.map(|r| {
                        r.map_err(|e| {
                            krishiv_metrics::grpc::internal_status("decode result spool", &e)
                        })
                    }));
                    (schema, iter)
                }
                Err(e) => {
                    let status = krishiv_metrics::grpc::internal_status("decode result spool", &e);
                    (
                        Arc::new(Schema::empty()),
                        Box::new(std::iter::once(Err(status))) as SpoolBatchIter,
                    )
                }
            }
        } else {
            (Arc::new(Schema::empty()), Box::new(std::iter::empty()))
        };

        let prefix_stream = stream::iter(prefix);
        let rest_stream = stream::iter(spools).flat_map(|spool| stream::iter(spool_iter(spool)));
        // FlightDataEncoderBuilder::build wants Result<_, FlightError>, not
        // our tonic Status — convert at this one seam (arrow_flight already
        // provides From<tonic::Status> for FlightError) rather than
        // threading FlightError through every internal error site above.
        let combined = prefix_stream
            .chain(rest_stream)
            .map(|item: Result<RecordBatch, Status>| item.map_err(FlightError::from));

        Ok(SqlResultDelivery::Streamed {
            schema,
            batches: Box::pin(combined),
        })
    }
}

/// Result of [`FlightExecutionHost::execute_sql_stream`].
pub enum SqlResultDelivery {
    /// The full result, already collected — the size cap in `do_get_statement`
    /// still applies to this variant.
    Buffered(Vec<RecordBatch>),
    /// Batches arriving incrementally; `schema` is known up front (needed by
    /// the Flight encoder) without buffering the batches themselves.
    Streamed {
        schema: Arc<Schema>,
        batches: Pin<Box<dyn Stream<Item = Result<RecordBatch, FlightError>> + Send>>,
    },
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
            tokio::task::block_in_place(f).map_err(runtime_error_to_status)
        }
        _ => std::thread::scope(|scope| {
            scope
                .spawn(f)
                .join()
                .map_err(|_| Status::internal("run_blocking thread panicked"))
                .and_then(|r| r.map_err(runtime_error_to_status))
        }),
    }
}

/// Classify a [`krishiv_runtime::RuntimeError`] into a wire [`Status`]
/// (Phase 63 / audit §11 error taxonomy).
///
/// Caller-facing query errors keep their message — they describe the submitted
/// SQL (unknown table/column, type mismatch, unsupported query shape) and are
/// safe and useful to return verbatim. Internal or infrastructure faults are
/// logged server-side under a correlation reference by
/// [`krishiv_metrics::grpc::internal_status`] and returned opaque, so no
/// internal detail (paths, addresses, invariant text) ever leaks over the wire.
pub(crate) fn runtime_error_to_status(err: krishiv_runtime::RuntimeError) -> Status {
    use krishiv_runtime::RuntimeError as RE;
    match err {
        // The submitted plan/SQL was rejected during planning or execution.
        RE::PlanRejected { reason } => Status::invalid_argument(reason),
        RE::Unsupported { feature } => {
            Status::unimplemented(format!("unsupported runtime feature: {feature}"))
        }
        RE::ServerUnimplemented { message } => Status::unimplemented(message),
        // Transport / invalid-state / partial-result / stream-lifecycle faults
        // are internal — never surface the raw detail.
        other => krishiv_metrics::grpc::internal_status("batch SQL execution", &other),
    }
}

/// Classify a [`krishiv_scheduler::SchedulerError`] into a wire [`Status`]
/// (Phase 63 / audit §11 error taxonomy).
///
/// Submission-validation and job-execution failures carry a caller-facing cause
/// (the query error); placement/transport/store faults are logged under a
/// correlation reference and returned opaque.
pub(crate) fn scheduler_error_to_status(err: krishiv_scheduler::SchedulerError) -> Status {
    use krishiv_scheduler::SchedulerError as SE;
    match err {
        SE::InvalidJob { message } | SE::InvalidPlan { message } => {
            Status::invalid_argument(message)
        }
        // A submitted job failed during execution; `reason` is the recorded
        // query-execution cause and is safe to surface. Empty reason means no
        // per-task cause was captured — treat as internal.
        SE::JobFailed { reason, .. } if !reason.is_empty() => Status::invalid_argument(reason),
        SE::NoExecutors | SE::ExecutorUnavailable { .. } => Status::unavailable(
            "no executor is currently available to run the query; retry shortly",
        ),
        other => krishiv_metrics::grpc::internal_status("coordinated batch SQL execution", &other),
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

    /// #211: the InProcess backend still returns `Buffered` from
    /// `execute_sql_stream` (true streaming is only implemented for the
    /// Coordinator backend so far) — but it must go through the SAME public
    /// method `do_get_statement` now calls, with the SAME row content
    /// `execute_sql` would have produced directly.
    #[tokio::test]
    async fn execute_sql_stream_returns_buffered_for_inprocess_backend() {
        let host = FlightExecutionHost::embedded().unwrap();
        let delivery = host.execute_sql_stream("SELECT 42 AS n").await.unwrap();
        match delivery {
            SqlResultDelivery::Buffered(batches) => {
                assert!(!batches.is_empty());
                assert_eq!(batches[0].num_rows(), 1);
            }
            SqlResultDelivery::Streamed { .. } => {
                panic!("InProcess backend must not take the Streamed path")
            }
        }
    }

    /// #211: a directive (explain) must still route through the buffered
    /// path and produce the same result `execute_sql` would, regardless of
    /// backend — directives are control messages/small results, not the
    /// large-SELECT case #211 is about.
    #[tokio::test]
    async fn execute_sql_stream_returns_buffered_for_explain_directive() {
        let host = FlightExecutionHost::embedded().unwrap();
        let sql = krishiv_runtime::flight_protocol::encode_explain_sql("SELECT 1");
        let delivery = host.execute_sql_stream(&sql).await.unwrap();
        match delivery {
            SqlResultDelivery::Buffered(batches) => assert!(!batches.is_empty()),
            SqlResultDelivery::Streamed { .. } => {
                panic!("a directive must not take the Streamed path")
            }
        }
    }

    /// Audit §11 error taxonomy: a caller's SQL error (unknown table) must
    /// surface as `InvalidArgument` with the offending name preserved — not as
    /// an opaque `Internal` (the pre-fix behaviour, which double-wrapped the
    /// already-classified `Status` into `internal(err.to_string())`).
    #[tokio::test]
    async fn unknown_table_surfaces_as_invalid_argument() {
        let host = FlightExecutionHost::embedded().unwrap();
        let err = host
            .execute_sql("SELECT * FROM definitely_missing_table")
            .await
            .expect_err("a query against a missing table must fail");
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "caller SQL errors must classify as InvalidArgument, got {:?}: {}",
            err.code(),
            err.message()
        );
        assert!(
            err.message().contains("definitely_missing_table"),
            "the error must name the offending table: {}",
            err.message()
        );
    }

    /// Internal/infrastructure runtime faults must be logged under a correlation
    /// ref and returned opaque — the raw detail (here an internal address) must
    /// never appear on the wire.
    #[test]
    fn internal_runtime_errors_are_opaque() {
        let status = runtime_error_to_status(krishiv_runtime::RuntimeError::transport(
            "connect 10.0.0.5:2001 refused",
        ));
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(
            !status.message().contains("10.0.0.5"),
            "internal transport detail leaked over the wire: {}",
            status.message()
        );
        assert!(
            status.message().contains("ref "),
            "opaque internal status should carry a correlation ref: {}",
            status.message()
        );
    }

    /// The scheduler classifier surfaces submission-validation errors to the
    /// caller but returns a retryable, detail-free status for placement
    /// starvation.
    #[test]
    fn scheduler_errors_classify_by_kind() {
        let status = scheduler_error_to_status(krishiv_scheduler::SchedulerError::InvalidJob {
            message: "empty SQL statement".to_string(),
        });
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("empty SQL statement"));

        let status = scheduler_error_to_status(krishiv_scheduler::SchedulerError::NoExecutors);
        assert_eq!(status.code(), tonic::Code::Unavailable);
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
