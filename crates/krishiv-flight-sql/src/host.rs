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
fn collect_ipc_tables(directives: &[FlightDirective]) -> Vec<krishiv_scheduler::BatchSqlInlineTable> {
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
/// In **proxy mode** (`coordinator_http` is `Some`), batch SQL and bounded
/// windows are forwarded to the real coordinator/executor. Continuous stream
/// operations still run on the embedded `InProcessCluster` because the
/// coordinator does not yet have a continuous-stream execution path.
///
/// In **embedded mode** (`coordinator_http` is `None`), all operations run on
/// the embedded `InProcessCluster`.
#[derive(Clone)]
pub struct FlightExecutionHost {
    /// Always present. Used for continuous streams in all modes; also handles
    /// batch SQL and bounded windows in embedded mode.
    cluster: Arc<InProcessCluster>,
    continuous: Arc<ContinuousStreamRegistry>,
    /// Path-based catalog shared across requests (persisted registrations).
    catalog: Arc<DashMap<String, PathBuf>>,
    /// When set, routes batch SQL and bounded windows through the coordinator.
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
    /// - `Some(url)` → proxy mode: batch SQL and bounded windows forwarded to coordinator.
    ///   Continuous streams still run locally on the embedded cluster.
    /// - `None` → embedded mode: all operations run on the embedded cluster.
    pub fn with_coordinator_http(coordinator_http: Option<String>) -> Result<Self, Status> {
        let cluster = InProcessCluster::new().map_err(|e| Status::internal(e.to_string()))?;
        Ok(Self {
            cluster: Arc::new(cluster),
            continuous: Arc::new(ContinuousStreamRegistry::new()),
            catalog: Arc::new(DashMap::new()),
            coordinator_http,
        })
    }

    /// Embedded cluster — always present.
    pub fn cluster(&self) -> Arc<InProcessCluster> {
        Arc::clone(&self.cluster)
    }

    pub fn continuous_registry(&self) -> Arc<ContinuousStreamRegistry> {
        Arc::clone(&self.continuous)
    }

    pub fn coordinator_http_url(&self) -> Option<&str> {
        self.coordinator_http.as_deref()
    }

    pub async fn execute_sql(&self, raw_sql: &str) -> Result<Vec<RecordBatch>, Status> {
        let (directives, sql) = parse_sql(raw_sql);
        self.apply_catalog_directives(&directives)?;

        // Build per-request inline IPC table list from this call's directives only.
        // Tables are NOT stored in shared state, so concurrent calls cannot observe
        // each other's in-flight data.
        let ipc_tables = collect_ipc_tables(&directives);

        if has_control_directive(&directives) {
            return self.handle_control_directives(directives, &sql).await;
        }

        let tables = self.catalog_tables();
        let sql = sql.to_string();

        if let Some(http_base) = self.coordinator_http.as_deref() {
            return krishiv_runtime::execute_coordinator_batch_sql_inline(
                http_base,
                &sql,
                &ipc_tables,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()));
        }

        let cluster = self.cluster();
        run_blocking(move || cluster.collect_batch_sql(&sql, &tables)).await
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
                    let cluster = self.cluster();
                    let local = plan_spec_to_local(&spec);
                    let continuous = Arc::clone(&self.continuous);
                    let job_id = job_id.clone();
                    let spec = spec.clone();
                    run_blocking(move || {
                        cluster.register_continuous_job(&job_id, &local)?;
                        continuous.register_job(job_id, spec)
                    })
                    .await?;
                }
                FlightDirective::ContinuousPush { job_id, batches } => {
                    let cluster = self.cluster();
                    let continuous = Arc::clone(&self.continuous);
                    let job_id = job_id.clone();
                    run_blocking(move || {
                        cluster.push_continuous_input(&job_id, batches.clone())?;
                        continuous.push_input(&job_id, batches.to_vec())
                    })
                    .await?;
                }
                FlightDirective::ContinuousDrain { job_id } => {
                    let cluster = self.cluster();
                    let job_id = job_id.clone();
                    return run_blocking(move || cluster.drain_continuous_job(&job_id)).await;
                }
                FlightDirective::BoundedWindow {
                    topic,
                    spec,
                    input_batches,
                } => {
                    // SQL-encoded fallback — route through coordinator in proxy mode,
                    // local cluster otherwise.
                    let local = plan_spec_to_local(&spec);
                    let cluster = self.cluster();
                    let topic = topic.clone();
                    let input_batches = input_batches.clone();
                    return run_blocking(move || {
                        cluster.collect_bounded_window(&topic, input_batches, &local)
                    })
                    .await;
                }
                // Already applied in apply_catalog_directives.
                FlightDirective::RegisterParquet { .. } => {}
                FlightDirective::RegisterParquetIpc { .. } => {}
            }
        }
        Ok(vec![status_batch("ok")?])
    }
}

async fn run_blocking<T>(
    f: impl FnOnce() -> Result<T, krishiv_runtime::RuntimeError> + Send + 'static,
) -> Result<T, Status>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Status::internal(format!("blocking task failed: {e}")))?
        .map_err(|e| Status::internal(e.to_string()))
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
