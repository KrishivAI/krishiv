//! Shared server-side execution host for the Krishiv Flight SQL service.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_runtime::continuous_stream::ContinuousStreamRegistry;
use krishiv_runtime::flight_protocol::{
    FlightDirective, apply_register_directives, has_control_directive, parse_sql,
};
use krishiv_runtime::in_process::BatchSqlTable;
use krishiv_runtime::in_process_cluster::{InProcessCluster, plan_spec_to_local};
use krishiv_sql::explain_sql;
use tonic::Status;

/// Extract `RegisterParquetIpc` directives into a per-request inline table list.
///
/// These tables are scoped to a single `execute_sql` call so concurrent callers
/// cannot see each other's in-flight data.
fn collect_ipc_tables(
    directives: &[FlightDirective],
) -> Vec<krishiv_scheduler::BatchSqlInlineTable> {
    directives
        .iter()
        .filter_map(|d| {
            if let FlightDirective::RegisterParquetIpc { table, ipc_b64 } = d {
                Some(krishiv_scheduler::BatchSqlInlineTable {
                    table_name: table.clone(),
                    ipc_b64: ipc_b64.clone(),
                })
            } else {
                None
            }
        })
        .collect()
}

/// Server-side catalog and cluster state shared across Flight SQL requests.
///
/// **Proxy mode** (`coordinator_http` is `Some`): batch SQL, bounded windows,
/// and continuous streams are all forwarded to the external coordinator. No
/// `InProcessCluster` or `ContinuousStreamRegistry` is created — those
/// allocations are unnecessary overhead in proxy deployments.
///
/// **Embedded mode** (`coordinator_http` is `None`): all operations run on the
/// local `InProcessCluster`.
#[derive(Clone)]
pub struct FlightExecutionHost {
    /// Present only in embedded mode. `None` in proxy mode — all execution is
    /// forwarded to the coordinator, so no local cluster is needed.
    cluster: Option<Arc<InProcessCluster>>,
    /// Present only in embedded mode for the legacy continuous-stream local path.
    continuous: Option<Arc<ContinuousStreamRegistry>>,
    /// Path-based catalog shared across requests (persisted registrations).
    catalog: Arc<DashMap<String, PathBuf>>,
    /// When set, routes all execution through the coordinator HTTP API.
    coordinator_http: Option<String>,
}

impl FlightExecutionHost {
    pub fn new() -> Result<Self, Status> {
        Self::from_env()
    }

    /// Build a host from environment variables.
    ///
    /// `KRISHIV_COORDINATOR_HTTP` — when set, enables proxy mode: all execution
    /// is forwarded to the coordinator. No `InProcessCluster` is created.
    pub fn from_env() -> Result<Self, Status> {
        let coordinator_http = std::env::var("KRISHIV_COORDINATOR_HTTP")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        Self::with_coordinator_http(coordinator_http)
    }

    /// Create a host with an optional coordinator HTTP base URL.
    ///
    /// - `Some(url)` → proxy mode: batch SQL is forwarded to the coordinator;
    ///   an `InProcessCluster` is still created for continuous streaming jobs
    ///   because window state is session-local and does not need coordinator routing.
    /// - `None` → embedded mode: all operations run on the local cluster.
    pub fn with_coordinator_http(coordinator_http: Option<String>) -> Result<Self, Status> {
        // Always create the in-process cluster and continuous registry.
        // Batch SQL is proxied to the coordinator when coordinator_http is set;
        // continuous job operations always run locally in the flight-server process.
        let c = InProcessCluster::new().map_err(|e| Status::internal(e.to_string()))?;
        let (cluster, continuous) = (
            Some(Arc::new(c)),
            Some(Arc::new(ContinuousStreamRegistry::new())),
        );
        Ok(Self {
            cluster,
            continuous,
            catalog: Arc::new(DashMap::new()),
            coordinator_http,
        })
    }

    /// Embedded cluster — `None` in proxy mode.
    pub fn cluster(&self) -> Option<Arc<InProcessCluster>> {
        self.cluster.clone()
    }

    pub fn continuous_registry(&self) -> Option<Arc<ContinuousStreamRegistry>> {
        self.continuous.clone()
    }

    pub fn coordinator_http_url(&self) -> Option<&str> {
        self.coordinator_http.as_deref()
    }

    pub async fn execute_sql(&self, raw_sql: &str) -> Result<Vec<RecordBatch>, Status> {
        let (directives, sql) = parse_sql(raw_sql);
        self.apply_catalog_directives(&directives)?;

        let ipc_tables = collect_ipc_tables(&directives);

        if has_control_directive(&directives) {
            return self.handle_control_directives(directives, &sql).await;
        }

        let tables = self.catalog_tables();
        let sql = sql.to_string();

        if let Some(http_base) = self.coordinator_http.as_deref() {
            // Proxy mode: is_streaming classification cannot use the local cluster
            // (none is allocated). The coordinator classifies the query itself based
            // on its own registered streaming sources, so passing false is safe here
            // — the coordinator's handler ignores this hint for non-streaming SQL.
            let is_streaming = false;
            return krishiv_runtime::execute_coordinator_batch_sql_inline(
                http_base,
                &sql,
                &ipc_tables,
                is_streaming,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()));
        }

        let cluster = self
            .cluster()
            .ok_or_else(|| Status::internal("embedded cluster unavailable"))?;
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
        run_blocking(|| cluster.collect_batch_sql(&sql, &tables, is_streaming))
    }

    fn apply_catalog_directives(&self, directives: &[FlightDirective]) -> Result<(), Status> {
        let mut catalog_map = std::collections::HashMap::new();
        apply_register_directives(&mut catalog_map, directives);
        for (table, path) in catalog_map {
            self.catalog.insert(table, path);
        }
        // RegisterParquetIpc directives are handled per-request in collect_ipc_tables
        // and are NOT stored in shared state.
        Ok(())
    }

    fn catalog_tables(&self) -> Vec<BatchSqlTable> {
        self.catalog
            .iter()
            .map(|entry| BatchSqlTable {
                table_name: entry.key().clone(),
                path: entry.value().clone(),
            })
            .collect()
    }

    async fn handle_control_directives(
        &self,
        directives: Vec<FlightDirective>,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, Status> {
        for directive in directives {
            match directive {
                FlightDirective::Explain => {
                    let text = explain_sql(sql).map_err(|e| Status::internal(e.to_string()))?;
                    return Ok(vec![explain_batch(&text)?]);
                }
                FlightDirective::ContinuousRegister { job_id, spec } => {
                    if let Some(http_base) = self.coordinator_http.as_deref() {
                        krishiv_runtime::execute_coordinator_continuous_register(
                            http_base, &job_id, &spec,
                        )
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                        continue;
                    }
                    let cluster = self
                        .cluster()
                        .ok_or_else(|| Status::internal("embedded cluster unavailable"))?;
                    let continuous = self
                        .continuous
                        .clone()
                        .ok_or_else(|| Status::internal("continuous registry unavailable"))?;
                    let local = plan_spec_to_local(&spec);
                    let job_id = job_id.clone();
                    let spec = spec.clone();
                    run_blocking(move || {
                        cluster.register_continuous_job(&job_id, &local)?;
                        continuous.register_job(job_id, spec)
                    })?;
                }
                FlightDirective::ContinuousPush { job_id, batches } => {
                    if let Some(http_base) = self.coordinator_http.as_deref() {
                        krishiv_runtime::execute_coordinator_continuous_push(
                            http_base, &job_id, &batches,
                        )
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                        continue;
                    }
                    let cluster = self
                        .cluster()
                        .ok_or_else(|| Status::internal("embedded cluster unavailable"))?;
                    let continuous = self
                        .continuous
                        .clone()
                        .ok_or_else(|| Status::internal("continuous registry unavailable"))?;
                    let job_id = job_id.clone();
                    run_blocking(move || {
                        cluster.push_continuous_input(&job_id, batches.clone())?;
                        continuous.push_input(&job_id, batches.to_vec())
                    })?;
                }
                FlightDirective::ContinuousDrain { job_id } => {
                    if let Some(http_base) = self.coordinator_http.as_deref() {
                        return krishiv_runtime::execute_coordinator_continuous_drain(
                            http_base, &job_id,
                        )
                        .await
                        .map_err(|e| Status::internal(e.to_string()));
                    }
                    let cluster = self
                        .cluster()
                        .ok_or_else(|| Status::internal("embedded cluster unavailable"))?;
                    let job_id = job_id.clone();
                    return run_blocking(move || cluster.drain_continuous_job(&job_id));
                }
                FlightDirective::BoundedWindow {
                    topic,
                    spec,
                    input_batches,
                } => {
                    if let Some(http_base) = self.coordinator_http.as_deref() {
                        return krishiv_runtime::execute_coordinator_bounded_window(
                            http_base,
                            &topic,
                            &spec,
                            &input_batches,
                        )
                        .await
                        .map_err(|e| Status::internal(e.to_string()));
                    }
                    let cluster = self
                        .cluster()
                        .ok_or_else(|| Status::internal("embedded cluster unavailable"))?;
                    let local = plan_spec_to_local(&spec);
                    let topic = topic.clone();
                    let input_batches = input_batches.clone();
                    return run_blocking(move || {
                        cluster.collect_bounded_window(&topic, input_batches, &local)
                    });
                }
                // Already applied in apply_catalog_directives.
                FlightDirective::RegisterParquet { .. } => {}
                FlightDirective::RegisterParquetIpc { .. } => {}
            }
        }
        Ok(vec![status_batch("ok")?])
    }
}

/// Run a blocking cluster operation on the current tokio worker thread.
///
/// Uses `block_in_place` instead of `spawn_blocking`:
/// - Stays on the same thread pool — no cross-thread overhead.
/// - Allows the tokio executor to continue other tasks while this one blocks.
/// - Eliminates the `spawn_blocking → block_on → FALLBACK_RUNTIME` triple-hop
///   that `spawn_blocking` caused (cluster methods call `block_on` internally;
///   inside `block_in_place`, `block_on` uses `block_in_place + handle.block_on`
///   rather than the fallback runtime).
/// - Drops the `'static` and `Send` bounds — the closure can borrow locals.
///
/// Requires a multi-threaded tokio runtime, which is always the case for a
/// Flight SQL gRPC server.
fn run_blocking<T>(
    f: impl FnOnce() -> Result<T, krishiv_runtime::RuntimeError>,
) -> Result<T, Status> {
    tokio::task::block_in_place(f).map_err(|e| Status::internal(e.to_string()))
}

fn explain_batch(text: &str) -> Result<RecordBatch, Status> {
    let schema = Arc::new(Schema::new(vec![Field::new("plan", DataType::Utf8, false)]));
    let lines: StringArray = text.lines().map(Some).collect();
    RecordBatch::try_new(schema, vec![Arc::new(lines) as ArrayRef])
        .map_err(|e| Status::internal(e.to_string()))
}

fn status_batch(label: &str) -> Result<RecordBatch, Status> {
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
    async fn host_executes_simple_select_in_process() {
        let host = FlightExecutionHost::with_coordinator_http(None).unwrap();
        let batches = host.execute_sql("SELECT 42 AS n").await.unwrap();
        assert!(!batches.is_empty());
    }

    #[tokio::test]
    async fn host_explain_directive() {
        let host = FlightExecutionHost::with_coordinator_http(None).unwrap();
        let sql = krishiv_runtime::flight_protocol::encode_explain_sql("SELECT 1");
        let batches = host.execute_sql(&sql).await.unwrap();
        assert!(!batches.is_empty());
    }

    #[tokio::test]
    async fn proxy_mode_has_coordinator_http_set() {
        let host =
            FlightExecutionHost::with_coordinator_http(Some("http://localhost:18080".into()))
                .unwrap();
        assert!(host.coordinator_http_url().is_some());
    }
}
