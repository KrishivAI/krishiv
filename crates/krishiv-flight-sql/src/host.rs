//! Shared server-side execution host for the Krishiv Flight SQL service.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_runtime::continuous_stream::ContinuousStreamRegistry;
use krishiv_runtime::flight_protocol::{
    FlightDirective, apply_register_directives, catalog_to_batch_tables, has_control_directive,
    parse_sql,
};
use krishiv_runtime::in_process::BatchSqlTable;
use krishiv_runtime::in_process_cluster::{plan_spec_to_local, InProcessCluster};
use krishiv_sql::explain_sql;
use tonic::Status;

/// Server-side catalog and cluster state shared across Flight SQL requests.
#[derive(Clone)]
pub struct FlightExecutionHost {
    cluster: Arc<InProcessCluster>,
    continuous: Arc<ContinuousStreamRegistry>,
    catalog: Arc<Mutex<HashMap<String, PathBuf>>>,
}

impl FlightExecutionHost {
    pub fn new() -> Result<Self, Status> {
        let cluster = InProcessCluster::new().map_err(|e| Status::internal(e.to_string()))?;
        Ok(Self {
            cluster: Arc::new(cluster),
            continuous: Arc::new(ContinuousStreamRegistry::new()),
            catalog: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn cluster(&self) -> Arc<InProcessCluster> {
        Arc::clone(&self.cluster)
    }

    pub fn continuous_registry(&self) -> Arc<ContinuousStreamRegistry> {
        Arc::clone(&self.continuous)
    }

    pub async fn execute_sql(&self, raw_sql: &str) -> Result<Vec<RecordBatch>, Status> {
        let (directives, sql) = parse_sql(raw_sql);
        self.apply_catalog_directives(&directives)?;

        if has_control_directive(&directives) {
            return self
                .handle_control_directives(directives, &sql)
                .await;
        }

        let cluster = Arc::clone(&self.cluster);
        let tables = self.catalog_tables();
        let sql = sql.to_string();
        run_blocking(move || cluster.collect_batch_sql(&sql, &tables)).await
    }

    fn apply_catalog_directives(&self, directives: &[FlightDirective]) -> Result<(), Status> {
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_| Status::internal("catalog lock poisoned"))?;
        apply_register_directives(&mut catalog, directives);
        Ok(())
    }

    fn catalog_tables(&self) -> Vec<BatchSqlTable> {
        let catalog = self
            .catalog
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        catalog_to_batch_tables(&catalog)
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
                    let local = plan_spec_to_local(&spec);
                    let cluster = Arc::clone(&self.cluster);
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
                    let cluster = Arc::clone(&self.cluster);
                    let continuous = Arc::clone(&self.continuous);
                    let job_id = job_id.clone();
                    run_blocking(move || {
                        cluster.push_continuous_input(&job_id, batches.clone())?;
                        continuous.push_input(&job_id, batches.to_vec())
                    })
                    .await?;
                }
                FlightDirective::ContinuousDrain { job_id } => {
                    let cluster = Arc::clone(&self.cluster);
                    let job_id = job_id.clone();
                    return run_blocking(move || cluster.drain_continuous_job(&job_id)).await;
                }
                FlightDirective::BoundedWindow {
                    topic,
                    spec,
                    input_batches,
                } => {
                    let local = plan_spec_to_local(&spec);
                    let cluster = Arc::clone(&self.cluster);
                    let topic = topic.clone();
                    let input_batches = input_batches.clone();
                    return run_blocking(move || {
                        cluster.collect_bounded_window(&topic, input_batches, &local)
                    })
                    .await;
                }
                FlightDirective::RegisterParquet { .. } => {}
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
    let schema = Arc::new(Schema::new(vec![Field::new("status", DataType::Utf8, false)]));
    let col = Arc::new(StringArray::from(vec![label])) as ArrayRef;
    RecordBatch::try_new(schema, vec![col]).map_err(|e| Status::internal(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn host_executes_simple_select() {
        let host = FlightExecutionHost::new().unwrap();
        let batches = host.execute_sql("SELECT 42 AS n").await.unwrap();
        assert!(!batches.is_empty());
    }

    #[tokio::test]
    async fn host_explain_directive() {
        let host = FlightExecutionHost::new().unwrap();
        let sql = krishiv_runtime::flight_protocol::encode_explain_sql("SELECT 1");
        let batches = host.execute_sql(&sql).await.unwrap();
        assert!(!batches.is_empty());
    }
}
