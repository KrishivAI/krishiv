//! Arrow Flight SQL client for [`super::DistributedBackend`] (GAP-RT-01).

use arrow_flight::Ticket;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use krishiv_plan::{ExecutionKind, PhysicalPlan};
use tonic::transport::{Channel, Endpoint};

use crate::flight_protocol::{
    encode_batch_sql, encode_bounded_window, encode_continuous_drain, encode_continuous_push,
    encode_continuous_register, encode_explain_sql,
};
use crate::in_process::BatchSqlTable;
use crate::local_streaming::LocalWindowExecutionSpec;
use crate::{RuntimeError, RuntimeResult};

/// Map a physical plan to a SQL statement understood by the Krishiv Flight SQL service.
pub fn plan_to_sql(plan: &PhysicalPlan) -> String {
    let name = plan.name();
    match plan.kind() {
        ExecutionKind::Batch => {
            let upper = name.to_ascii_uppercase();
            if upper.contains("SELECT") || upper.contains("FROM") {
                name.to_string()
            } else {
                format!("SELECT '{name}' AS plan_name")
            }
        }
        ExecutionKind::Streaming => {
            format!(
                "/* krishiv-stream:{} */ SELECT 1 AS streaming_accepted",
                name.replace('\'', "''")
            )
        }
    }
}

fn normalize_flight_endpoint(url: &str) -> RuntimeResult<String> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err(RuntimeError::transport("coordinator URL must not be empty"));
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        Ok(trimmed.to_string())
    } else {
        Ok(format!("http://{trimmed}"))
    }
}

async fn connect_flight_client(endpoint: &str) -> RuntimeResult<FlightSqlServiceClient<Channel>> {
    let channel = Endpoint::from_shared(endpoint.to_string())
        .map_err(|e| RuntimeError::transport(format!("invalid coordinator URL: {e}")))?
        .connect()
        .await
        .map_err(|e| RuntimeError::transport(format!("flight connect failed: {e}")))?;
    Ok(FlightSqlServiceClient::new(channel))
}

/// Submit `plan` to the remote Flight SQL endpoint and drain the result stream to confirm acceptance.
pub async fn execute_remote_plan(flight_url: &str, plan: &PhysicalPlan) -> RuntimeResult<()> {
    let _ = execute_remote_sql(flight_url, &plan_to_sql(plan)).await?;
    Ok(())
}

/// Execute SQL remotely via Flight and return all result batches.
pub async fn execute_remote_sql(
    flight_url: &str,
    sql: &str,
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    let endpoint = normalize_flight_endpoint(flight_url)?;
    let mut client = connect_flight_client(&endpoint).await?;

    let flight_info = client
        .execute(sql.to_string(), None)
        .await
        .map_err(|e| RuntimeError::transport(format!("flight execute failed: {e}")))?;

    let ticket = flight_info
        .endpoint
        .first()
        .and_then(|ep| ep.ticket.clone())
        .ok_or_else(|| RuntimeError::transport("flight response had no ticket endpoint"))?;

    let mut stream = client
        .do_get(Ticket {
            ticket: ticket.ticket,
        })
        .await
        .map_err(|e| RuntimeError::transport(format!("flight do_get failed: {e}")))?;

    let mut batches = Vec::new();
    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(|e| RuntimeError::transport(format!("flight decode failed: {e}")))?
    {
        batches.push(batch);
    }
    Ok(batches)
}

/// Execute batch SQL remotely with catalog sync directives.
pub async fn execute_remote_batch_sql(
    flight_url: &str,
    query: &str,
    tables: &[BatchSqlTable],
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    let sql = encode_batch_sql(query, tables);
    execute_remote_sql(flight_url, &sql).await
}

/// Explain SQL remotely via Flight.
pub async fn execute_remote_explain(flight_url: &str, query: &str) -> RuntimeResult<String> {
    let sql = encode_explain_sql(query);
    let batches = execute_remote_sql(flight_url, &sql).await?;
    Ok(flight_explain_from_batches(&batches))
}

fn flight_explain_from_batches(batches: &[arrow::record_batch::RecordBatch]) -> String {
    use arrow::array::Array;
    use arrow::array::StringArray;

    let mut lines = Vec::new();
    for batch in batches {
        for col_idx in 0..batch.num_columns() {
            if let Some(arr) = batch.column(col_idx).as_any().downcast_ref::<StringArray>() {
                for row in 0..batch.num_rows() {
                    if !arr.is_null(row) {
                        lines.push(arr.value(row).to_string());
                    }
                }
            }
        }
    }
    if lines.is_empty() {
        String::from("(no explain output)")
    } else {
        lines.join("\n")
    }
}

/// Register a continuous streaming job on the remote Flight host.
pub async fn execute_remote_continuous_register(
    flight_url: &str,
    job_id: &str,
    spec: &LocalWindowExecutionSpec,
) -> RuntimeResult<()> {
    let sql = encode_continuous_register(job_id, spec);
    let _ = execute_remote_sql(flight_url, &sql).await?;
    Ok(())
}

/// Push input batches to a remote continuous streaming job.
pub async fn execute_remote_continuous_push(
    flight_url: &str,
    job_id: &str,
    batches: Vec<arrow::record_batch::RecordBatch>,
) -> RuntimeResult<()> {
    let sql = encode_continuous_push(job_id, &batches)?;
    let _ = execute_remote_sql(flight_url, &sql).await?;
    Ok(())
}

/// Drain output from a remote continuous streaming job.
pub async fn execute_remote_continuous_drain(
    flight_url: &str,
    job_id: &str,
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    let sql = encode_continuous_drain(job_id);
    execute_remote_sql(flight_url, &sql).await
}

/// Execute a bounded window pipeline on the remote Flight host.
pub async fn execute_remote_bounded_window(
    flight_url: &str,
    topic: &str,
    input_batches: Vec<arrow::record_batch::RecordBatch>,
    spec: &LocalWindowExecutionSpec,
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    let sql = encode_bounded_window(topic, spec, &input_batches)?;
    execute_remote_sql(flight_url, &sql).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_to_sql_uses_select_verbatim() {
        let plan = PhysicalPlan::new("SELECT 2 AS x", ExecutionKind::Batch);
        assert_eq!(plan_to_sql(&plan), "SELECT 2 AS x");
    }

    #[test]
    fn plan_to_sql_wraps_opaque_batch_name() {
        let plan = PhysicalPlan::new("local-dataframe", ExecutionKind::Batch);
        assert!(plan_to_sql(&plan).contains("local-dataframe"));
    }

    #[test]
    fn plan_to_sql_streaming_marker() {
        let plan = PhysicalPlan::new("events", ExecutionKind::Streaming);
        let sql = plan_to_sql(&plan);
        assert!(sql.contains("krishiv-stream"));
        assert!(sql.contains("streaming_accepted"));
    }
}
