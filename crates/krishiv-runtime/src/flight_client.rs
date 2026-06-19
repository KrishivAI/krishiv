//! Arrow Flight SQL client for [`super::DistributedBackend`] (GAP-RT-01).

use arrow_flight::Ticket;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use krishiv_plan::{ExecutionKind, PhysicalPlan};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

use crate::flight_protocol::{
    encode_batch_sql, encode_bounded_window, encode_continuous_drain, encode_continuous_push,
    encode_continuous_register, encode_explain_sql,
};
use crate::in_process::BatchSqlTable;
use crate::local_streaming::LocalWindowExecutionSpec;
use crate::{RuntimeError, RuntimeResult};

/// Maximum time to wait for a single batch from a Flight SQL result stream
/// before treating the server as stalled.
const FLIGHT_PER_BATCH_TIMEOUT: Duration = Duration::from_secs(60);

/// Drain a Flight SQL record-batch stream, applying `per_batch_timeout` to
/// each individual `next()` poll so a stalled server cannot hang the caller
/// indefinitely.
///
/// Factored out of [`FlightClientPool::execute_sql`] so the timeout behaviour
/// can be exercised directly with a mock stream and a short duration —
/// waiting out the real (60s) production timeout in a test would be
/// impractically slow.
async fn collect_flight_batches<S>(
    mut stream: S,
    per_batch_timeout: Duration,
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>>
where
    S: futures::Stream<Item = arrow_flight::error::Result<arrow::record_batch::RecordBatch>>
        + Unpin,
{
    let mut batches = Vec::new();
    while let Some(batch) = tokio::time::timeout(per_batch_timeout, stream.try_next())
        .await
        .map_err(|_| {
            RuntimeError::transport(format!(
                "flight streaming batch timed out after {per_batch_timeout:?}"
            ))
        })?
        .map_err(|e| RuntimeError::transport(format!("flight decode failed: {e}")))?
    {
        batches.push(batch);
    }
    Ok(batches)
}

/// Bearer token for outbound Flight SQL / Flight action requests.
fn configured_flight_api_key() -> Option<String> {
    for env_name in ["KRISHIV_FLIGHT_API_KEY", "KRISHIV_API_KEY"] {
        if let Ok(key) = std::env::var(env_name) {
            let trimmed = key.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }
    std::env::var("KRISHIV_API_KEYS").ok().and_then(|raw| {
        raw.split(',')
            .find_map(|part| part.trim().split_once('='))
            .map(|(key, _)| key.trim().to_owned())
            .filter(|key| !key.is_empty())
    })
}

fn apply_flight_auth<T>(mut req: tonic::Request<T>) -> tonic::Request<T> {
    if let Some(key) = configured_flight_api_key()
        && let Ok(value) = format!("Bearer {key}").parse()
    {
        req.metadata_mut().insert("authorization", value);
    }
    req
}

struct FlightAuthInterceptor;

impl tonic::service::Interceptor for FlightAuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(key) = configured_flight_api_key()
            && let Ok(value) = format!("Bearer {key}").parse()
        {
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}

type AuthenticatedFlightChannel =
    tonic::service::interceptor::InterceptedService<Channel, FlightAuthInterceptor>;

fn flight_sql_client(channel: Channel) -> FlightSqlServiceClient<AuthenticatedFlightChannel> {
    FlightSqlServiceClient::new(tonic::service::interceptor::InterceptedService::new(
        channel,
        FlightAuthInterceptor,
    ))
}

/// Retry delays (ms) for transient Flight connection failures.
const RETRY_DELAYS_MS: &[u64] = &[100, 500, 2_000];

/// Timeout for establishing a gRPC channel to the Flight coordinator.
const FLIGHT_CONNECT_TIMEOUT_SECS: u64 = 10;
/// Per-request timeout for in-flight gRPC calls.
const FLIGHT_REQUEST_TIMEOUT_SECS: u64 = 30;

/// Health check interval for Flight endpoints.
const FLIGHT_HEALTH_CHECK_INTERVAL_SECS: u64 = 30;
/// Maximum consecutive health check failures before marking endpoint unhealthy.
const FLIGHT_MAX_HEALTH_FAILURES: u32 = 3;

/// Returns `true` for tonic status codes that indicate a transient failure
/// worth retrying (e.g. network blip, server briefly overloaded).
fn is_transient_status(e: &RuntimeError) -> bool {
    let msg = match e {
        RuntimeError::Transport { message } => message.as_str(),
        _ => return false,
    };
    // tonic encodes the gRPC status code name into the error string.
    msg.contains("Unavailable")
        || msg.contains("DeadlineExceeded")
        || msg.contains("Cancelled")
        || msg.contains("connection refused")
        || msg.contains("connect failed")
}

/// Execute `f` up to `1 + RETRY_DELAYS_MS.len()` times, waiting between
/// attempts for transient errors.  Permanent errors are returned immediately.
async fn with_retry<F, Fut, T>(mut f: F) -> RuntimeResult<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = RuntimeResult<T>>,
{
    let delays = std::iter::once(0).chain(RETRY_DELAYS_MS.iter().copied());
    for (attempt, delay_ms) in delays.enumerate() {
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < RETRY_DELAYS_MS.len() && is_transient_status(&e) => {
                // Transient error on a non-final attempt — keep retrying.
            }
            Err(e) => return Err(e),
        }
    }
    Err(RuntimeError::transport(
        "all transient retries exhausted for flight request",
    ))
}

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
        ExecutionKind::DeltaBatch => {
            format!("/* krishiv-delta-batch:{name} */ SELECT 1 AS delta_batch_accepted")
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

async fn connect_flight_channel(endpoint: &str) -> RuntimeResult<Channel> {
    let ep = endpoint.to_string();
    with_retry(|| async {
        Endpoint::from_shared(ep.clone())
            .map_err(|e| RuntimeError::transport(format!("invalid coordinator URL: {e}")))?
            .connect_timeout(std::time::Duration::from_secs(FLIGHT_CONNECT_TIMEOUT_SECS))
            .timeout(std::time::Duration::from_secs(FLIGHT_REQUEST_TIMEOUT_SECS))
            .connect()
            .await
            .map_err(|e| RuntimeError::transport(format!("flight connect failed: {e}")))
    })
    .await
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
    /// Per-endpoint health state for failover decisions.
    health_state: Arc<tokio::sync::RwLock<Vec<EndpointHealth>>>,
    /// Background health check task handle.
    health_check_handle: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

#[derive(Debug, Clone)]
struct EndpointHealth {
    endpoint: String,
    consecutive_failures: u32,
    last_check: Option<std::time::Instant>,
    is_healthy: bool,
}

impl FlightClientPool {
    pub fn new(flight_url: impl Into<String>) -> RuntimeResult<Self> {
        let url = flight_url.into();
        let endpoint = normalize_flight_endpoint(&url)?;
        let health = EndpointHealth {
            endpoint: endpoint.clone(),
            consecutive_failures: 0,
            last_check: None,
            is_healthy: true,
        };
        Ok(Self {
            endpoints: vec![endpoint],
            current: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            channel: Arc::new(tokio::sync::RwLock::new(None)),
            health_state: Arc::new(tokio::sync::RwLock::new(vec![health])),
            health_check_handle: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    pub fn with_alternate(mut self, url: impl Into<String>) -> Self {
        let raw = url.into();
        match normalize_flight_endpoint(&raw) {
            Ok(endpoint) => {
                self.endpoints.push(endpoint.clone());
                let health = EndpointHealth {
                    endpoint,
                    consecutive_failures: 0,
                    last_check: None,
                    is_healthy: true,
                };
                if let Some(rw) = Arc::get_mut(&mut self.health_state) {
                    rw.get_mut().push(health);
                }
            }
            Err(e) => {
                tracing::warn!(
                    url = %raw,
                    error = %e,
                    "ignoring invalid alternate Flight endpoint"
                );
            }
        }
        self
    }

    pub fn flight_url(&self) -> &str {
        let idx = self.current.load(std::sync::atomic::Ordering::Relaxed);
        self.endpoints.get(idx).map(|s| s.as_str()).unwrap_or("")
    }

    /// Start background health checks for all endpoints.
    pub async fn start_health_checks(&self) {
        let mut handle_guard = self.health_check_handle.lock().await;
        if handle_guard.is_some() {
            return; // Already running
        }

        let pool = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(FLIGHT_HEALTH_CHECK_INTERVAL_SECS));
            loop {
                interval.tick().await;
                pool.run_health_checks().await;
            }
        });
        *handle_guard = Some(handle);
    }

    /// Stop background health checks.
    pub async fn stop_health_checks(&self) {
        let mut handle_guard = self.health_check_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            handle.abort();
        }
    }

    async fn run_health_checks(&self) {
        let endpoints: Vec<String> = {
            let health = self.health_state.read().await;
            health.iter().map(|h| h.endpoint.clone()).collect()
        };

        for endpoint in endpoints {
            let is_healthy = Self::check_endpoint_health(&endpoint).await;
            self.update_endpoint_health(&endpoint, is_healthy).await;
        }

        // If current endpoint is unhealthy, failover to next healthy
        self.failover_if_needed().await;
    }

    async fn check_endpoint_health(endpoint: &str) -> bool {
        // Simple health check: try to connect with short timeout
        let endpoint_str = endpoint.to_string();
        let connect_fut = async {
            let ep = Endpoint::from_shared(endpoint_str)?;
            ep.connect_timeout(Duration::from_secs(2)).connect().await
        };
        matches!(
            tokio::time::timeout(Duration::from_secs(5), connect_fut).await,
            Ok(Ok(_))
        )
    }

    async fn update_endpoint_health(&self, endpoint: &str, is_healthy: bool) {
        let mut health = self.health_state.write().await;
        if let Some(h) = health.iter_mut().find(|h| h.endpoint == endpoint) {
            h.last_check = Some(std::time::Instant::now());
            if is_healthy {
                h.consecutive_failures = 0;
                h.is_healthy = true;
            } else {
                h.consecutive_failures = h.consecutive_failures.saturating_add(1);
                if h.consecutive_failures >= FLIGHT_MAX_HEALTH_FAILURES {
                    h.is_healthy = false;
                    tracing::warn!(endpoint = %endpoint, "Flight endpoint marked unhealthy after {} consecutive failures", FLIGHT_MAX_HEALTH_FAILURES);
                }
            }
        }
    }

    async fn failover_if_needed(&self) {
        let current_idx = self.current.load(std::sync::atomic::Ordering::Acquire);
        let should_failover = {
            let health = self.health_state.read().await;
            health
                .get(current_idx)
                .map(|h| !h.is_healthy)
                .unwrap_or(false)
        };

        if should_failover {
            let health = self.health_state.read().await;
            let len = health.len();
            for i in 1..len {
                let next_idx = (current_idx + i) % len;
                if health.get(next_idx).map(|h| h.is_healthy).unwrap_or(false) {
                    self.current
                        .store(next_idx, std::sync::atomic::Ordering::Release);
                    // Clear cached channel so new connection is established
                    let mut channel = self.channel.write().await;
                    *channel = None;
                    tracing::info!(
                        from = current_idx,
                        to = next_idx,
                        "Flight client failover to healthy endpoint"
                    );
                    break;
                }
            }
        }
    }

    /// Get the current endpoint health status.
    pub async fn endpoint_health(&self) -> Vec<(String, bool, u32)> {
        let health = self.health_state.read().await;
        health
            .iter()
            .map(|h| (h.endpoint.clone(), h.is_healthy, h.consecutive_failures))
            .collect()
    }

    pub async fn get_channel(&self) -> RuntimeResult<Channel> {
        // Fast path: channel already connected.
        {
            let guard = self.channel.read().await;
            if let Some(ref ch) = *guard {
                return Ok(ch.clone());
            }
        }

        // Slow path: acquire write lock and double-check before connecting.
        // The write lock is held for the duration of the connection attempt,
        // preventing concurrent cold-start calls from each opening their own
        // TCP connection and racing to overwrite the cached channel.
        let mut guard = self.channel.write().await;
        if let Some(ref ch) = *guard {
            return Ok(ch.clone()); // another waiter already connected
        }

        // Find a healthy endpoint to connect to
        let endpoint = self.select_healthy_endpoint().await?;
        match connect_flight_channel(&endpoint).await {
            Ok(ch) => {
                *guard = Some(ch.clone());
                Ok(ch)
            }
            Err(e) => {
                // Mark this endpoint as unhealthy on connection failure
                self.mark_endpoint_unhealthy(&endpoint).await;
                // Drop the write lock before failover to prevent reentrant
                // deadlock (failover_if_needed acquires channel.write).
                drop(guard);
                // Try failover (only if there are alternate endpoints).
                if self.endpoints.len() > 1 {
                    self.failover_if_needed().await;
                }
                Err(e)
            }
        }
    }

    async fn select_healthy_endpoint(&self) -> RuntimeResult<String> {
        let health = self.health_state.read().await;
        let current_idx = self.current.load(std::sync::atomic::Ordering::Acquire);
        let len = health.len();

        // First try current endpoint if healthy
        if let Some(h) = health.get(current_idx)
            && h.is_healthy
        {
            return Ok(h.endpoint.clone());
        }

        // Search for any healthy endpoint
        for i in 0..len {
            let idx = (current_idx + i) % len;
            if let Some(h) = health.get(idx)
                && h.is_healthy
            {
                return Ok(h.endpoint.clone());
            }
        }

        // No healthy endpoints - return current anyway (will fail with clear error)
        Ok(self.flight_url().to_string())
    }

    async fn mark_endpoint_unhealthy(&self, endpoint: &str) {
        let mut health = self.health_state.write().await;
        if let Some(h) = health.iter_mut().find(|h| h.endpoint == endpoint) {
            h.consecutive_failures = h.consecutive_failures.saturating_add(1);
            if h.consecutive_failures >= FLIGHT_MAX_HEALTH_FAILURES {
                h.is_healthy = false;
                tracing::warn!(endpoint = %endpoint, "Flight endpoint marked unhealthy on connection failure");
            }
        }
    }

    pub async fn do_action(
        &self,
        action: &crate::flight_action::KrishivFlightAction,
    ) -> RuntimeResult<Vec<u8>> {
        use futures::StreamExt;
        // Serialise action body once; reused across retry attempts.
        let body = action.to_action_body()?;
        let action_type = action.action_type();
        with_retry(|| async {
            let channel = self.get_channel().await?;
            let mut client = arrow_flight::flight_service_client::FlightServiceClient::new(channel);
            let req = arrow_flight::Action {
                r#type: action_type.clone(),
                body: body.clone().into(),
            };
            let mut stream = client
                .do_action(apply_flight_auth(tonic::Request::new(req)))
                .await
                .map_err(|s| {
                    if s.code() == tonic::Code::Unimplemented {
                        RuntimeError::ServerUnimplemented {
                            message: s.message().to_string(),
                        }
                    } else {
                        RuntimeError::transport(format!("do_action: {s}"))
                    }
                })?
                .into_inner();
            let mut buf = Vec::new();
            let max_response_bytes: usize = 64 * 1024 * 1024; // 64 MiB cap
            while let Some(item) = stream.next().await {
                let part =
                    item.map_err(|e| RuntimeError::transport(format!("do_action stream: {e}")))?;
                buf.extend_from_slice(&part.body);
                if buf.len() > max_response_bytes {
                    return Err(RuntimeError::transport(format!(
                        "do_action response exceeded {} MiB limit",
                        max_response_bytes / (1024 * 1024),
                    )));
                }
            }
            Ok(buf)
        })
        .await
    }

    pub async fn execute_sql(
        &self,
        sql: &str,
    ) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
        let sql = sql.to_string();
        with_retry(|| async {
            let channel = self.get_channel().await?;
            let mut client = flight_sql_client(channel);

            let flight_info = client
                .execute(sql.clone(), None)
                .await
                .map_err(|e| RuntimeError::transport(format!("flight execute failed: {e}")))?;

            let ticket = flight_info
                .endpoint
                .first()
                .and_then(|ep| ep.ticket.clone())
                .ok_or_else(|| RuntimeError::transport("flight response had no ticket endpoint"))?;

            let stream = client
                .do_get(Ticket {
                    ticket: ticket.ticket,
                })
                .await
                .map_err(|e| RuntimeError::transport(format!("flight do_get failed: {e}")))?;

            collect_flight_batches(stream, FLIGHT_PER_BATCH_TIMEOUT).await
        })
        .await
    }

    /// Stream SQL results lazily — returns batches one at a time without
    /// buffering the full result set (R8). Useful for large result sets where
    /// `execute_sql` would exhaust coordinator memory.
    pub async fn stream_sql(
        &self,
        sql: &str,
    ) -> RuntimeResult<impl futures::Stream<Item = RuntimeResult<arrow::record_batch::RecordBatch>>>
    {
        let channel = self.get_channel().await?;
        let mut client = flight_sql_client(channel);
        let flight_info = client
            .execute(sql.to_string(), None)
            .await
            .map_err(|e| RuntimeError::transport(format!("flight execute failed: {e}")))?;
        let ticket = flight_info
            .endpoint
            .first()
            .and_then(|ep| ep.ticket.clone())
            .ok_or_else(|| RuntimeError::transport("flight response had no ticket endpoint"))?;
        let stream = client
            .do_get(Ticket {
                ticket: ticket.ticket,
            })
            .await
            .map_err(|e| RuntimeError::transport(format!("flight do_get failed: {e}")))?;
        Ok(stream.map_err(|e| RuntimeError::transport(format!("flight decode failed: {e}"))))
    }
}

impl std::fmt::Debug for FlightClientPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightClientPool")
            .field("flight_url", &self.flight_url())
            .field("endpoint_count", &self.endpoints.len())
            .finish_non_exhaustive()
    }
}

impl Drop for FlightClientPool {
    fn drop(&mut self) {
        // Try to stop health checks - best effort
        if let Ok(mut handle_guard) = self.health_check_handle.try_lock()
            && let Some(handle) = handle_guard.take()
        {
            handle.abort();
        }
    }
}

/// Submit `plan` to the remote Flight endpoint via the pool.
///
/// Prefers the typed [`KrishivFlightAction::ExecutePlan`] payload sent over
/// `do_action`.  Falls back to the legacy SQL-comment protocol when the
/// server does not understand the action type — preserves backward compat for
/// older deployments.
pub async fn execute_remote_plan(
    pool: &FlightClientPool,
    plan: &PhysicalPlan,
) -> RuntimeResult<()> {
    use crate::flight_action::{ExecutePlanBody, KrishivFlightAction};
    let body = ExecutePlanBody::from_plan(plan)?;
    let action = KrishivFlightAction::ExecutePlan(body);
    match pool.do_action(&action).await {
        Ok(_) => Ok(()),
        Err(e) if is_unimplemented(&e) => {
            if !krishiv_common::allows_remote_sql_comment_fallback() {
                return Err(e);
            }
            let _ = execute_remote_sql(pool, &plan_to_sql(plan)).await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Execute SQL remotely via Flight and return all result batches.
pub async fn execute_remote_sql(
    pool: &FlightClientPool,
    sql: &str,
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    pool.execute_sql(sql).await
}

/// Execute batch SQL remotely with catalog sync directives.
pub async fn execute_remote_batch_sql(
    pool: &FlightClientPool,
    query: &str,
    tables: &[BatchSqlTable],
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    let sql = encode_batch_sql(query, tables);
    execute_remote_sql(pool, &sql).await
}

/// Explain SQL remotely via Flight.
pub async fn execute_remote_explain(pool: &FlightClientPool, query: &str) -> RuntimeResult<String> {
    let sql = encode_explain_sql(query);
    let batches = execute_remote_sql(pool, &sql).await?;
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

/// Register a continuous streaming job on the remote Flight host via the pool.
///
/// Prefers the typed [`KrishivFlightAction::ContinuousRegister`] payload sent
/// over `do_action`.  Falls back to the legacy SQL-comment protocol when the
/// server does not understand the action type — preserves backward compat for
/// older deployments.
pub async fn execute_remote_continuous_register(
    pool: &FlightClientPool,
    job_id: &str,
    spec: &LocalWindowExecutionSpec,
) -> RuntimeResult<()> {
    use crate::flight_action::{ContinuousRegisterBody, KrishivFlightAction};
    let action = KrishivFlightAction::ContinuousRegister(ContinuousRegisterBody {
        job_id: job_id.to_string(),
        spec: spec.to_plan_spec(),
    });
    match pool.do_action(&action).await {
        Ok(_) => Ok(()),
        Err(e) if is_unimplemented(&e) => {
            if !krishiv_common::allows_remote_sql_comment_fallback() {
                return Err(e);
            }
            let sql = encode_continuous_register(job_id, spec)?;
            let _ = execute_remote_sql(pool, &sql).await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Push input batches to a remote continuous streaming job via the pool.
pub async fn execute_remote_continuous_push(
    pool: &FlightClientPool,
    job_id: &str,
    batches: Vec<arrow::record_batch::RecordBatch>,
) -> RuntimeResult<()> {
    use crate::flight_action::{ContinuousPushBody, KrishivFlightAction, encode_batches};
    let batches_b64 = encode_batches(&batches)?;
    let action = KrishivFlightAction::ContinuousPush(ContinuousPushBody {
        job_id: job_id.to_string(),
        batches_b64,
    });
    match pool.do_action(&action).await {
        Ok(_) => Ok(()),
        Err(e) if is_unimplemented(&e) => {
            if !krishiv_common::allows_remote_sql_comment_fallback() {
                return Err(e);
            }
            let sql = encode_continuous_push(job_id, &batches)?;
            let _ = execute_remote_sql(pool, &sql).await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Drain output from a remote continuous streaming job via the pool.
pub async fn execute_remote_continuous_drain(
    pool: &FlightClientPool,
    job_id: &str,
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    use crate::flight_action::{ContinuousDrainBody, KrishivFlightAction};
    let action = KrishivFlightAction::ContinuousDrain(ContinuousDrainBody {
        job_id: job_id.to_string(),
    });
    match pool.do_action(&action).await {
        Ok(body) => decode_ipc_response(&body),
        Err(e) if is_unimplemented(&e) => {
            if !krishiv_common::allows_remote_sql_comment_fallback() {
                return Err(e);
            }
            let sql = encode_continuous_drain(job_id);
            execute_remote_sql(pool, &sql).await
        }
        Err(e) => Err(e),
    }
}

/// Execute a bounded window pipeline on the remote Flight host via the pool.
pub async fn execute_remote_bounded_window(
    pool: &FlightClientPool,
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
        response_watermark_ms: None,
    });
    match pool.do_action(&action).await {
        Ok(body) => decode_ipc_response(&body),
        Err(e) if is_unimplemented(&e) => {
            if !krishiv_common::allows_remote_sql_comment_fallback() {
                return Err(e);
            }
            let sql = encode_bounded_window(topic, spec, &input_batches)?;
            execute_remote_sql(pool, &sql).await
        }
        Err(e) => Err(e),
    }
}

fn is_unimplemented(e: &RuntimeError) -> bool {
    matches!(e, RuntimeError::ServerUnimplemented { .. })
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt;

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
    fn is_unimplemented_matches_server_unimplemented_variant() {
        let err = RuntimeError::ServerUnimplemented {
            message: "action not supported".into(),
        };
        assert!(is_unimplemented(&err));
    }

    #[test]
    fn is_unimplemented_rejects_transport_error() {
        // A Transport error whose message happens to contain "Unimplemented"
        // must NOT trigger the fallback — only the dedicated variant does.
        let err = RuntimeError::transport("Unimplemented: some random message");
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

    // ── Per-batch timeout (collect_flight_batches) ───────────────────────

    /// A stream that yields `n_ready` immediate batches and then never
    /// resolves again — simulating a server that stalls mid-stream.
    fn stalling_batch_stream(
        n_ready: usize,
    ) -> impl futures::Stream<Item = arrow_flight::error::Result<RecordBatch>> + Unpin {
        let schema = Arc::new(Schema::new(Vec::<arrow::datatypes::Field>::new()));
        let ready = futures::stream::iter(
            (0..n_ready).map(move |_| Ok(RecordBatch::new_empty(Arc::clone(&schema)))),
        );
        Box::pin(ready.chain(futures::stream::pending()))
    }

    #[tokio::test]
    async fn collect_flight_batches_times_out_on_stalled_stream() {
        // The 60s production timeout would make this test impractically slow;
        // exercising the same logic with a short duration proves the timeout
        // path fires and surfaces a transport error rather than hanging.
        let stream = stalling_batch_stream(0);
        let result = collect_flight_batches(stream, Duration::from_millis(20)).await;
        match result {
            Err(RuntimeError::Transport { message }) => {
                assert!(
                    message.contains("timed out"),
                    "expected a timeout message, got: {message}"
                );
            }
            other => panic!("expected Transport timeout error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn collect_flight_batches_returns_batches_received_before_stall() {
        // Batches that arrive within the per-batch timeout must be collected
        // even if the stream subsequently stalls and the overall read fails.
        let stream = stalling_batch_stream(2);
        let result = collect_flight_batches(stream, Duration::from_millis(20)).await;
        assert!(
            matches!(result, Err(RuntimeError::Transport { .. })),
            "stalled stream must still surface a timeout error: {result:?}"
        );
    }

    // ── do_action response size cap ──────────────────────────────────────

    /// Mock Flight service whose `do_action` streams back oversized chunks —
    /// large enough in aggregate to exceed `FlightClientPool::do_action`'s
    /// 64 MiB response cap — so the cap can be exercised against a real
    /// gRPC round trip rather than just unit-testing the accumulation loop.
    mod oversized_action_service {
        use std::pin::Pin;

        use arrow_flight::flight_service_server::FlightService;
        use arrow_flight::{
            Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
            HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
        };
        use tonic::{Request, Response, Status, Streaming};

        type BoxedFlightStream<T> =
            Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

        #[derive(Clone)]
        pub(super) struct OversizedActionService {
            pub(super) chunk_size: usize,
            pub(super) chunk_count: usize,
        }

        #[tonic::async_trait]
        impl FlightService for OversizedActionService {
            type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
            type ListFlightsStream = BoxedFlightStream<FlightInfo>;
            type DoGetStream = BoxedFlightStream<FlightData>;
            type DoPutStream = BoxedFlightStream<PutResult>;
            type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
            type ListActionsStream = BoxedFlightStream<ActionType>;
            type DoExchangeStream = BoxedFlightStream<FlightData>;

            async fn handshake(
                &self,
                _r: Request<Streaming<HandshakeRequest>>,
            ) -> Result<Response<Self::HandshakeStream>, Status> {
                Err(Status::unimplemented("handshake"))
            }

            async fn list_flights(
                &self,
                _r: Request<Criteria>,
            ) -> Result<Response<Self::ListFlightsStream>, Status> {
                Err(Status::unimplemented("list_flights"))
            }

            async fn get_flight_info(
                &self,
                _r: Request<FlightDescriptor>,
            ) -> Result<Response<FlightInfo>, Status> {
                Err(Status::unimplemented("get_flight_info"))
            }

            async fn poll_flight_info(
                &self,
                _r: Request<FlightDescriptor>,
            ) -> Result<Response<PollInfo>, Status> {
                Err(Status::unimplemented("poll_flight_info"))
            }

            async fn get_schema(
                &self,
                _r: Request<FlightDescriptor>,
            ) -> Result<Response<SchemaResult>, Status> {
                Err(Status::unimplemented("get_schema"))
            }

            async fn do_get(
                &self,
                _r: Request<Ticket>,
            ) -> Result<Response<Self::DoGetStream>, Status> {
                Err(Status::unimplemented("do_get"))
            }

            async fn do_put(
                &self,
                _r: Request<Streaming<FlightData>>,
            ) -> Result<Response<Self::DoPutStream>, Status> {
                Err(Status::unimplemented("do_put"))
            }

            async fn do_action(
                &self,
                _r: Request<Action>,
            ) -> Result<Response<Self::DoActionStream>, Status> {
                let chunk = vec![0u8; self.chunk_size];
                let chunk_count = self.chunk_count;
                let stream = futures::stream::iter((0..chunk_count).map(move |_| {
                    Ok(arrow_flight::Result {
                        body: chunk.clone().into(),
                    })
                }));
                Ok(Response::new(Box::pin(stream) as Self::DoActionStream))
            }

            async fn list_actions(
                &self,
                _r: Request<Empty>,
            ) -> Result<Response<Self::ListActionsStream>, Status> {
                Err(Status::unimplemented("list_actions"))
            }

            async fn do_exchange(
                &self,
                _r: Request<Streaming<FlightData>>,
            ) -> Result<Response<Self::DoExchangeStream>, Status> {
                Err(Status::unimplemented("do_exchange"))
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn do_action_rejects_response_exceeding_size_cap() {
        use arrow_flight::flight_service_server::FlightServiceServer;
        use tonic::transport::Server;

        use oversized_action_service::OversizedActionService;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr: std::net::SocketAddr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);

        // 40 chunks * 2 MiB = 80 MiB, over the 64 MiB cap while each chunk
        // stays comfortably under tonic's default 4 MiB decode-length limit.
        let service = OversizedActionService {
            chunk_size: 2 * 1024 * 1024,
            chunk_count: 40,
        };
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .expect("serve");
        });

        let url = format!("http://{addr}");
        let pool = FlightClientPool::new(url).expect("pool");
        let action =
            crate::flight_action::KrishivFlightAction::Explain(crate::flight_action::ExplainBody {
                sql: "SELECT 1".into(),
            });

        let result = pool.do_action(&action).await;
        match result {
            Err(RuntimeError::Transport { message }) => {
                assert!(
                    message.contains("MiB limit"),
                    "expected a response-size-cap error, got: {message}"
                );
            }
            other => panic!("expected Transport size-limit error, got {other:?}"),
        }
        server.abort();
    }

    #[tokio::test]
    async fn collect_flight_batches_drains_finite_stream_without_timeout() {
        let schema = Arc::new(Schema::new(Vec::<arrow::datatypes::Field>::new()));
        let stream = Box::pin(futures::stream::iter(
            (0..3).map(move |_| Ok(RecordBatch::new_empty(Arc::clone(&schema)))),
        ))
            as std::pin::Pin<
                Box<dyn futures::Stream<Item = arrow_flight::error::Result<RecordBatch>>>,
            >;
        let batches = collect_flight_batches(stream, Duration::from_secs(5))
            .await
            .expect("finite stream must drain without timing out");
        assert_eq!(batches.len(), 3);
    }
}
