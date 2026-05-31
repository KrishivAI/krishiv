//! Arrow Flight SQL client for [`super::DistributedBackend`] (GAP-RT-01).

use arrow_flight::Ticket;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use krishiv_plan::{ExecutionKind, PhysicalPlan};
use std::sync::Arc;
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
            let safe_name = name.replace('\'', "''").replace("*/", "* /");
            format!(
                "/* krishiv-stream:{} */ SELECT 1 AS streaming_accepted",
                safe_name
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

async fn connect_flight_channel(endpoint: &str) -> RuntimeResult<Channel> {
    Endpoint::from_shared(endpoint.to_string())
        .map_err(|e| RuntimeError::transport(format!("invalid coordinator URL: {e}")))?
        .connect()
        .await
        .map_err(|e| RuntimeError::transport(format!("flight connect failed: {e}")))
}

/// Lazily-connected gRPC channel reused across remote Flight calls.
///
/// Supports multiple coordinator endpoints for failover — on connection
/// failure, the next endpoint in the list is tried.
#[derive(Clone)]
pub struct FlightClientPool {
    endpoints: Vec<String>,
    current: Arc<std::sync::atomic::AtomicUsize>,
    channel: Arc<tokio::sync::RwLock<Option<Channel>>>,
}

impl FlightClientPool {
    pub fn new(flight_url: impl Into<String>) -> Self {
        let url = flight_url.into();
        let endpoint = normalize_flight_endpoint(&url).unwrap_or_else(|_| String::new());
        Self {
            endpoints: vec![endpoint],
            current: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            channel: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    pub fn with_alternate(mut self, url: impl Into<String>) -> Self {
        let endpoint = normalize_flight_endpoint(&url.into()).unwrap_or_else(|_| String::new());
        if !endpoint.is_empty() {
            self.endpoints.push(endpoint);
        }
        self
    }

    pub fn flight_url(&self) -> &str {
        let idx = self.current.load(std::sync::atomic::Ordering::Relaxed);
        self.endpoints.get(idx).map(|s| s.as_str()).unwrap_or("")
    }

    pub async fn get_channel(&self) -> RuntimeResult<Channel> {
        {
            let guard = self.channel.read().await;
            if let Some(ref ch) = *guard {
                return Ok(ch.clone());
            }
        }

        let endpoint = self.flight_url();
        let result = connect_flight_channel(&endpoint).await;

        match result {
            Ok(ch) => {
                let mut guard = self.channel.write().await;
                *guard = Some(ch.clone());
                Ok(ch)
            }
            Err(e) if self.endpoints.len() > 1 => {
                self.current.store(
                    (self.current.load(std::sync::atomic::Ordering::Relaxed) + 1)
                        % self.endpoints.len(),
                    std::sync::atomic::Ordering::Relaxed,
                );
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn do_action(
        &self,
        action: &crate::flight_action::KrishivFlightAction,
    ) -> RuntimeResult<Vec<u8>> {
        use futures::StreamExt;
        let channel = self.get_channel().await?;
        let mut client = arrow_flight::flight_service_client::FlightServiceClient::new(channel);
        let body = action.to_action_body()?;
        let req = arrow_flight::Action {
            r#type: action.action_type(),
            body: body.into(),
        };
        let mut stream = client
            .do_action(tonic::Request::new(req))
            .await
            .map_err(|e| RuntimeError::transport(format!("do_action: {e}")))?
            .into_inner();

        let mut buf = Vec::new();
        while let Some(item) = stream.next().await {
            let part =
                item.map_err(|e| RuntimeError::transport(format!("do_action stream: {e}")))?;
            buf.extend_from_slice(&part.body);
        }
        Ok(buf)
    }

    pub async fn execute_sql(
        &self,
        sql: &str,
    ) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
        let channel = self.get_channel().await?;
        let mut client = FlightSqlServiceClient::new(channel);

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
}

/// Submit `plan` to the remote Flight endpoint.
///
/// Prefers the typed [`KrishivFlightAction::ExecutePlan`] payload sent over
/// `do_action`.  Falls back to the legacy SQL-comment protocol when the
/// server does not understand the action type — preserves backward compat for
/// older deployments.
pub async fn execute_remote_plan(flight_url: &str, plan: &PhysicalPlan) -> RuntimeResult<()> {
    use crate::flight_action::{ExecutePlanBody, KrishivFlightAction};
    let body = ExecutePlanBody::from_plan(plan)?;
    let action = KrishivFlightAction::ExecutePlan(body);
    match do_action(flight_url, &action).await {
        Ok(_) => Ok(()),
        Err(e) if is_unimplemented(&e) => {
            let _ = execute_remote_sql(flight_url, &plan_to_sql(plan)).await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
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

pub fn flight_explain_from_batches(batches: &[arrow::record_batch::RecordBatch]) -> String {
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
///
/// Prefers the typed [`KrishivFlightAction::ContinuousRegister`] payload sent
/// over `do_action`.  Falls back to the legacy SQL-comment protocol when the
/// server does not understand the action type — preserves backward compat for
/// older deployments.
pub async fn execute_remote_continuous_register(
    flight_url: &str,
    job_id: &str,
    spec: &LocalWindowExecutionSpec,
) -> RuntimeResult<()> {
    use crate::flight_action::{ContinuousRegisterBody, KrishivFlightAction};
    let action = KrishivFlightAction::ContinuousRegister(ContinuousRegisterBody {
        job_id: job_id.to_string(),
        spec: spec.to_plan_spec(),
    });
    match do_action(flight_url, &action).await {
        Ok(_) => Ok(()),
        Err(e) if is_unimplemented(&e) => {
            let sql = encode_continuous_register(job_id, spec)?;
            let _ = execute_remote_sql(flight_url, &sql).await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Push input batches to a remote continuous streaming job.
pub async fn execute_remote_continuous_push(
    flight_url: &str,
    job_id: &str,
    batches: Vec<arrow::record_batch::RecordBatch>,
) -> RuntimeResult<()> {
    use crate::flight_action::{ContinuousPushBody, KrishivFlightAction, encode_batches};
    let batches_b64 = encode_batches(&batches)?;
    let action = KrishivFlightAction::ContinuousPush(ContinuousPushBody {
        job_id: job_id.to_string(),
        batches_b64,
    });
    match do_action(flight_url, &action).await {
        Ok(_) => Ok(()),
        Err(e) if is_unimplemented(&e) => {
            let sql = encode_continuous_push(job_id, &batches)?;
            let _ = execute_remote_sql(flight_url, &sql).await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Drain output from a remote continuous streaming job.
pub async fn execute_remote_continuous_drain(
    flight_url: &str,
    job_id: &str,
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    use crate::flight_action::{ContinuousDrainBody, KrishivFlightAction};
    let action = KrishivFlightAction::ContinuousDrain(ContinuousDrainBody {
        job_id: job_id.to_string(),
    });
    match do_action(flight_url, &action).await {
        Ok(body) => decode_ipc_response(&body),
        Err(e) if is_unimplemented(&e) => {
            let sql = encode_continuous_drain(job_id);
            execute_remote_sql(flight_url, &sql).await
        }
        Err(e) => Err(e),
    }
}

/// Execute a bounded window pipeline on the remote Flight host.
pub async fn execute_remote_bounded_window(
    flight_url: &str,
    topic: &str,
    input_batches: Vec<arrow::record_batch::RecordBatch>,
    spec: &LocalWindowExecutionSpec,
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    use crate::flight_action::{BoundedWindowBody, KrishivFlightAction, encode_batches};
    let batches_b64 = encode_batches(&input_batches)?;
    let action = KrishivFlightAction::BoundedWindow(BoundedWindowBody {
        topic: topic.to_string(),
        spec: spec.to_plan_spec(),
        batches_b64,
    });
    match do_action(flight_url, &action).await {
        Ok(body) => decode_ipc_response(&body),
        Err(e) if is_unimplemented(&e) => {
            let sql = encode_bounded_window(topic, spec, &input_batches)?;
            execute_remote_sql(flight_url, &sql).await
        }
        Err(e) => Err(e),
    }
}

fn is_unimplemented(e: &RuntimeError) -> bool {
    matches!(e, RuntimeError::Transport { message } if message.contains("Unimplemented"))
}

pub fn decode_ipc_response(body: &[u8]) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    if body.is_empty() {
        return Ok(Vec::new());
    }
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;
    let cursor = Cursor::new(body);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| RuntimeError::transport(format!("ipc decode response: {e}")))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| RuntimeError::transport(format!("ipc read response: {e}")))
}

/// Low-level helper: send a typed Krishiv action to the Flight server.
pub(crate) async fn do_action(
    flight_url: &str,
    action: &crate::flight_action::KrishivFlightAction,
) -> RuntimeResult<Vec<u8>> {
    use futures::StreamExt;
    let endpoint = normalize_flight_endpoint(flight_url)?;
    let channel = tonic::transport::Endpoint::from_shared(endpoint.clone())
        .map_err(|e| RuntimeError::transport(format!("invalid coordinator URL: {e}")))?
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .connect()
        .await
        .map_err(|e| RuntimeError::transport(format!("flight connect failed: {e}")))?;
    let mut client = arrow_flight::flight_service_client::FlightServiceClient::new(channel);
    let body = action.to_action_body()?;
    let req = arrow_flight::Action {
        r#type: action.action_type(),
        body: body.into(),
    };
    let mut stream = client
        .do_action(tonic::Request::new(req))
        .await
        .map_err(|e| RuntimeError::transport(format!("do_action: {e}")))?
        .into_inner();

    let mut buf = Vec::new();
    while let Some(item) = stream.next().await {
        let part = item.map_err(|e| RuntimeError::transport(format!("do_action stream: {e}")))?;
        buf.extend_from_slice(&part.body);
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;

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

    #[test]
    fn plan_to_sql_uses_from_verbatim() {
        let plan = PhysicalPlan::new("FROM users SELECT *", ExecutionKind::Batch);
        assert_eq!(plan_to_sql(&plan), "FROM users SELECT *");
    }

    #[test]
    fn plan_to_sql_uppercase_select() {
        let plan = PhysicalPlan::new("SELECT 1", ExecutionKind::Batch);
        assert_eq!(plan_to_sql(&plan), "SELECT 1");
    }

    #[test]
    fn plan_to_sql_batch_with_from_keyword_case_insensitive() {
        let plan = PhysicalPlan::new("from table1 select *", ExecutionKind::Batch);
        assert_eq!(plan_to_sql(&plan), "from table1 select *");
    }

    #[test]
    fn plan_to_sql_streaming_escapes_single_quotes() {
        let plan = PhysicalPlan::new("it's a stream", ExecutionKind::Streaming);
        let sql = plan_to_sql(&plan);
        assert!(sql.contains("it''s a stream"));
    }

    #[test]
    fn plan_to_sql_streaming_escapes_comment_close() {
        let plan = PhysicalPlan::new("bad*/name", ExecutionKind::Streaming);
        let sql = plan_to_sql(&plan);
        assert!(sql.contains("* /"));
        assert!(!sql.contains("*/name"));
    }

    #[test]
    fn plan_to_sql_empty_batch_name() {
        let plan = PhysicalPlan::new("", ExecutionKind::Batch);
        let sql = plan_to_sql(&plan);
        assert!(sql.contains("SELECT '' AS plan_name"));
    }

    #[test]
    fn normalize_flight_endpoint_empty_fails() {
        let err = normalize_flight_endpoint("").unwrap_err();
        assert!(matches!(err, RuntimeError::Transport { .. }));
    }

    #[test]
    fn normalize_flight_endpoint_whitespace_only_fails() {
        let err = normalize_flight_endpoint("   ").unwrap_err();
        assert!(matches!(err, RuntimeError::Transport { .. }));
    }

    #[test]
    fn normalize_flight_endpoint_http_unchanged() {
        let result = normalize_flight_endpoint("http://localhost:50051").unwrap();
        assert_eq!(result, "http://localhost:50051");
    }

    #[test]
    fn normalize_flight_endpoint_https_unchanged() {
        let result = normalize_flight_endpoint("https://cluster.example.com").unwrap();
        assert_eq!(result, "https://cluster.example.com");
    }

    #[test]
    fn normalize_flight_endpoint_bare_adds_http() {
        let result = normalize_flight_endpoint("localhost:50051").unwrap();
        assert_eq!(result, "http://localhost:50051");
    }

    #[test]
    fn normalize_flight_endpoint_trims_whitespace() {
        let result = normalize_flight_endpoint("  http://localhost:50051  ").unwrap();
        assert_eq!(result, "http://localhost:50051");
    }

    #[test]
    fn flight_explain_from_batches_extracts_strings() {
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "plan",
            arrow::datatypes::DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::StringArray::from(vec![
                "SeqScan(users)",
                "Filter(x > 5)",
            ])) as _],
        )
        .unwrap();
        let result = flight_explain_from_batches(&[batch]);
        assert_eq!(result, "SeqScan(users)\nFilter(x > 5)");
    }

    #[test]
    fn flight_explain_from_batches_empty_returns_placeholder() {
        let result = flight_explain_from_batches(&[]);
        assert_eq!(result, "(no explain output)");
    }

    #[test]
    fn flight_explain_from_batches_all_null() {
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "plan",
            arrow::datatypes::DataType::Utf8,
            true,
        )]));
        let arr = arrow::array::StringArray::from(vec![None::<&str>]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr) as _]).unwrap();
        let result = flight_explain_from_batches(&[batch]);
        assert_eq!(result, "(no explain output)");
    }

    #[test]
    fn decode_ipc_response_empty_returns_empty() {
        let result = decode_ipc_response(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn decode_ipc_response_invalid_data() {
        let err = decode_ipc_response(b"not ipc data").unwrap_err();
        assert!(matches!(err, RuntimeError::Transport { .. }));
    }

    #[test]
    fn is_unimplemented_matches_unimplemented() {
        let err = RuntimeError::transport("Unimplemented: method not found");
        assert!(is_unimplemented(&err));
    }

    #[test]
    fn is_unimplemented_matches_invalid() {
        let err = RuntimeError::transport("invalid argument");
        assert!(!is_unimplemented(&err));
    }

    #[test]
    fn is_unimplemented_rejects_other_transport() {
        let err = RuntimeError::transport("connection refused");
        assert!(!is_unimplemented(&err));
    }

    #[test]
    fn is_unimplemented_rejects_non_transport() {
        let err = RuntimeError::unsupported("test");
        assert!(!is_unimplemented(&err));
    }
}
