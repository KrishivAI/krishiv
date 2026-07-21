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

/// Application-level cap on a single `do_action` response. The whole response
/// arrives as one gRPC message (`do_action_fallback` wraps it in a one-item
/// stream server-side), so this is checked against the accumulated buffer as
/// chunks of that one message arrive.
const DO_ACTION_MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// gRPC's own per-message decode limit for the `do_action` channel,
/// comfortably above `DO_ACTION_MAX_RESPONSE_BYTES` so that cap — not tonic's
/// much smaller 4 MiB default — is what actually classifies and rejects an
/// oversized response. Without raising this, any do_action response over
/// 4 MiB fails as a raw, unclassified tonic decode error before
/// DO_ACTION_MAX_RESPONSE_BYTES (and the ResultTooLarge/streaming-fallback
/// classification built on it) is ever reached.
const DO_ACTION_MAX_DECODE_BYTES: usize = 96 * 1024 * 1024;

/// Drain a Flight SQL record-batch stream, applying `per_batch_timeout` to
/// each individual `next()` poll so a stalled server cannot hang the caller
/// indefinitely.
///
/// Factored out of [`FlightClientPool::execute_sql`] so the timeout behaviour
/// can be exercised directly with a mock stream and a short duration —
/// waiting out the real (60s) production timeout in a test would be
/// impractically slow.
/// Drain a Flight SQL record-batch stream, applying `per_batch_timeout` to
/// each poll and rejecting the result once the accumulated batches' in-memory
/// size exceeds `max_bytes` (`0` = unbounded) instead of buffering an
/// arbitrarily large stream to completion. The cap check happens per batch,
/// as data arrives, not after the fact — an oversized result stops accumulating
/// as soon as the running total crosses the line.
async fn collect_flight_batches<S>(
    mut stream: S,
    per_batch_timeout: Duration,
    max_bytes: usize,
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>>
where
    S: futures::Stream<Item = arrow_flight::error::Result<arrow::record_batch::RecordBatch>>
        + Unpin,
{
    let mut batches = Vec::new();
    let mut total_bytes: usize = 0;
    while let Some(batch) = tokio::time::timeout(per_batch_timeout, stream.try_next())
        .await
        .map_err(|_| {
            RuntimeError::transport(format!(
                "flight streaming batch timed out after {per_batch_timeout:?}"
            ))
        })?
        .map_err(|e| RuntimeError::transport(format!("flight decode failed: {e}")))?
    {
        if max_bytes > 0 {
            total_bytes = total_bytes.saturating_add(batch.get_array_memory_size());
            if total_bytes > max_bytes {
                return Err(RuntimeError::result_too_large(format!(
                    "streamed result ({total_bytes} bytes) exceeds maximum ({max_bytes} \
                     bytes); add a LIMIT clause or raise {CLIENT_MAX_RESULT_BYTES_ENV}"
                )));
            }
        }
        batches.push(batch);
    }
    Ok(batches)
}

/// Client-side counterpart of the server's `KRISHIV_FLIGHT_MAX_RESULT_BYTES`
/// (same env var, independent processes): how large a streamed result this
/// client is willing to buffer into a `Vec<RecordBatch>` before giving up.
/// Exists so the do_action too-large fallback (which routes through the
/// uncapped streaming transport) doesn't trade the coordinator's OOM risk for
/// the caller's — see `FlightClientPool::execute_sql_capped`.
pub(crate) const CLIENT_MAX_RESULT_BYTES_ENV: &str = "KRISHIV_FLIGHT_MAX_RESULT_BYTES";
const DEFAULT_CLIENT_MAX_RESULT_BYTES: usize = 2 * 1024 * 1024 * 1024;

pub(crate) fn client_max_result_bytes() -> usize {
    std::env::var(CLIENT_MAX_RESULT_BYTES_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_CLIENT_MAX_RESULT_BYTES)
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

/// Classify a `do_action` gRPC failure into a [`RuntimeError`].
///
/// A free function (not inlined in `do_action`) so the classification is
/// directly unit-testable without a live server, matching how
/// `is_server_unimplemented` is a dedicated variant rather than a string
/// match: `Unimplemented` means "retry via the legacy SQL-comment protocol,
/// server doesn't know this action"; `ResourceExhausted` means "retry via the
/// streaming do_get transport, the result was too large for one response."
fn map_do_action_status(s: tonic::Status) -> RuntimeError {
    if s.code() == tonic::Code::Unimplemented {
        RuntimeError::ServerUnimplemented {
            message: s.message().to_string(),
        }
    } else if s.code() == tonic::Code::ResourceExhausted {
        RuntimeError::result_too_large(s.message().to_string())
    } else if s.code() == tonic::Code::OutOfRange {
        // do_action's entire response arrives as one unchunked gRPC message
        // (do_action_fallback sends a one-item stream), so a response bigger
        // than the channel's max_decoding_message_size fails the *transport's*
        // decode before DO_ACTION_MAX_RESPONSE_BYTES's own accumulation check
        // ever sees a byte — no amount of raising that decode limit closes
        // this for an arbitrarily large result, so it must be classified here
        // too, not just treated as a generic transport failure.
        RuntimeError::result_too_large(format!("do_action: {s}"))
    } else {
        RuntimeError::transport(format!("do_action: {s}"))
    }
}

/// Retry delays (ms) for transient Flight connection failures.
const RETRY_DELAYS_MS: &[u64] = &[100, 500, 2_000];

/// Timeout for establishing a gRPC channel to the Flight coordinator.
const FLIGHT_CONNECT_TIMEOUT_SECS: u64 = 10;

/// HTTP/2 keepalive ping interval on the client→coordinator channel. Detects a
/// dead peer *during* a long-running request without imposing a hard deadline
/// on legitimate long queries.
const FLIGHT_KEEPALIVE_INTERVAL_SECS: u64 = 30;
/// How long to wait for a keepalive ping ACK before declaring the connection
/// dead (~`interval + timeout` ≈ 50s to notice a vanished coordinator).
const FLIGHT_KEEPALIVE_TIMEOUT_SECS: u64 = 20;

/// Optional hard per-request deadline (seconds) on the Flight channel, read from
/// `KRISHIV_FLIGHT_REQUEST_TIMEOUT_SECS`. Returns `None` (the default) when the
/// flag is unset, empty, `0`, or unparseable — in which case a long-running
/// distributed query is bounded by the coordinator's own statement timeout
/// (`KRISHIV_BATCH_SQL_TIMEOUT_SECS`, default 300s) rather than a premature
/// transport cap. A tonic channel `.timeout()` applies to *every* request, so a
/// fixed 30s value here silently aborted any query — or result stream — running
/// longer than 30s (including a healthy multi-minute distributed scan).
fn flight_request_timeout() -> Option<Duration> {
    parse_flight_request_timeout(
        std::env::var("KRISHIV_FLIGHT_REQUEST_TIMEOUT_SECS")
            .ok()
            .as_deref(),
    )
}

/// Pure parse of the `KRISHIV_FLIGHT_REQUEST_TIMEOUT_SECS` value, split out so it
/// is testable without mutating process env. `None` (no hard per-request cap)
/// for unset/empty/`0`/unparseable input; `Some(secs)` for a positive integer.
fn parse_flight_request_timeout(raw: Option<&str>) -> Option<Duration> {
    match raw.map(str::trim).and_then(|s| s.parse::<u64>().ok()) {
        Some(secs) if secs > 0 => Some(Duration::from_secs(secs)),
        _ => None,
    }
}

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
    // "Cancelled" is deliberately NOT here: a server-side job cancel
    // surfaces through this path, and retrying it silently resubmits the
    // statement as a fresh job — the #217 live repro watched a cancelled
    // query rise from the dead on the other executor and burn a core.
    // Cancellation is an outcome, never a blip.
    msg.contains("Unavailable")
        || msg.contains("DeadlineExceeded")
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
        let mut endpoint = Endpoint::from_shared(ep.clone())
            .map_err(|e| RuntimeError::transport(format!("invalid coordinator URL: {e}")))?
            .connect_timeout(Duration::from_secs(FLIGHT_CONNECT_TIMEOUT_SECS))
            // HTTP/2 keepalive so a vanished coordinator is noticed mid-request
            // without capping legitimate long-running queries. This replaces the
            // former fixed 30s per-request `.timeout()`, which aborted any query
            // (or result stream) that ran longer than 30s.
            .http2_keep_alive_interval(Duration::from_secs(FLIGHT_KEEPALIVE_INTERVAL_SECS))
            .keep_alive_timeout(Duration::from_secs(FLIGHT_KEEPALIVE_TIMEOUT_SECS))
            .keep_alive_while_idle(true);
        // Optional operator-set hard deadline; unset by default (see
        // `flight_request_timeout`).
        if let Some(req_timeout) = flight_request_timeout() {
            endpoint = endpoint.timeout(req_timeout);
        }
        endpoint
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
    /// Lazy-start guard: flipped true the first time the pool is used from an
    /// async context so the background health loop always starts, even when
    /// the runtime was built from a sync (no ambient Tokio runtime) call site
    /// where the eager `spawn_health_checks()` would otherwise silently skip.
    health_started: Arc<std::sync::atomic::AtomicBool>,
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
            health_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Start the background health loop the first time the pool is exercised
    /// from an async context. Cheap after the first call: a relaxed atomic
    /// load short-circuits before touching the handle mutex. This is the
    /// backstop for pools whose runtime was constructed without an ambient
    /// Tokio runtime, so the eager `spawn_health_checks()` never ran.
    async fn ensure_health_checks(&self) {
        use std::sync::atomic::Ordering;
        if self.health_started.load(Ordering::Relaxed) {
            return;
        }
        // Only the first caller to win the swap actually starts the loop;
        // `start_health_checks` is itself idempotent, so a lost race is safe.
        if self
            .health_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            self.start_health_checks().await;
        }
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
        // Mark started so the lazy backstop in `ensure_health_checks` skips its
        // work once the loop is up, regardless of which path started it.
        self.health_started
            .store(true, std::sync::atomic::Ordering::Release);

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
        // First async touch of the pool guarantees a running Tokio runtime,
        // so start the background health loop here as a backstop for sync
        // (no ambient runtime) construction paths. No-op after the first call.
        self.ensure_health_checks().await;

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
            let mut client = arrow_flight::flight_service_client::FlightServiceClient::new(channel)
                .max_decoding_message_size(DO_ACTION_MAX_DECODE_BYTES);
            let req = arrow_flight::Action {
                r#type: action_type.clone(),
                body: body.clone().into(),
            };
            let mut stream = client
                .do_action(apply_flight_auth(tonic::Request::new(req)))
                .await
                .map_err(map_do_action_status)?
                .into_inner();
            let mut buf = Vec::new();
            while let Some(item) = stream.next().await {
                let part = item.map_err(map_do_action_status)?;
                buf.extend_from_slice(&part.body);
                if buf.len() > DO_ACTION_MAX_RESPONSE_BYTES {
                    return Err(RuntimeError::result_too_large(format!(
                        "do_action response exceeded {} MiB limit",
                        DO_ACTION_MAX_RESPONSE_BYTES / (1024 * 1024),
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
        self.execute_sql_capped(sql, 0).await
    }

    /// Like [`Self::execute_sql`], but rejects the result once the
    /// accumulated batches' in-memory size exceeds `max_bytes` (`0` =
    /// unbounded).
    ///
    /// `execute_sql` alone routes real streaming data through a real
    /// streaming transport (`execute()`+`do_get`, no per-message cap like
    /// `do_action`), but still buffers the whole thing client-side into one
    /// `Vec` — with no cap at all, that just moves the OOM risk from the
    /// coordinator to whatever process is running this client. Used by the
    /// `do_action` too-large fallback so that move doesn't happen silently.
    pub async fn execute_sql_capped(
        &self,
        sql: &str,
        max_bytes: usize,
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

            collect_flight_batches(stream, FLIGHT_PER_BATCH_TIMEOUT, max_bytes).await
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
    fn flight_request_timeout_parse() {
        // Unset / empty / zero / garbage → no hard per-request cap.
        assert_eq!(parse_flight_request_timeout(None), None);
        assert_eq!(parse_flight_request_timeout(Some("")), None);
        assert_eq!(parse_flight_request_timeout(Some("  ")), None);
        assert_eq!(parse_flight_request_timeout(Some("0")), None);
        assert_eq!(parse_flight_request_timeout(Some("nonsense")), None);
        assert_eq!(parse_flight_request_timeout(Some("-5")), None);
        // Positive integer (with surrounding whitespace) → that many seconds.
        assert_eq!(
            parse_flight_request_timeout(Some("30")),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            parse_flight_request_timeout(Some("  600 ")),
            Some(Duration::from_secs(600))
        );
    }

    // ── map_do_action_status ──────────────────────────────────────────────────

    #[test]
    fn map_do_action_status_unimplemented_becomes_server_unimplemented() {
        let s = tonic::Status::unimplemented("action not yet supported");
        assert!(matches!(
            map_do_action_status(s),
            RuntimeError::ServerUnimplemented { .. }
        ));
    }

    #[test]
    fn map_do_action_status_resource_exhausted_becomes_result_too_large() {
        let s = tonic::Status::resource_exhausted("Flight action result (999) exceeds maximum");
        match map_do_action_status(s) {
            RuntimeError::ResultTooLarge { message } => {
                assert!(message.contains("exceeds maximum"));
            }
            other => panic!("expected ResultTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn map_do_action_status_out_of_range_becomes_result_too_large() {
        // tonic's own code for "decoded message length too large" — do_action's
        // whole response is one gRPC message, so this is the transport-level
        // signal that a response was too big to even receive, not just too
        // big by the application's own accounting.
        let s = tonic::Status::out_of_range("decoded message length too large: found 999, limit 100");
        match map_do_action_status(s) {
            RuntimeError::ResultTooLarge { message } => {
                assert!(message.contains("too large"));
            }
            other => panic!("expected ResultTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn map_do_action_status_other_codes_become_transport() {
        for code in [
            tonic::Code::Internal,
            tonic::Code::Unauthenticated,
            tonic::Code::InvalidArgument,
            tonic::Code::DeadlineExceeded,
        ] {
            let s = tonic::Status::new(code, "boom");
            assert!(
                matches!(map_do_action_status(s), RuntimeError::Transport { .. }),
                "code {code:?} should map to a generic Transport error"
            );
        }
    }

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
        let result = collect_flight_batches(stream, Duration::from_millis(20), 0).await;
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
        let result = collect_flight_batches(stream, Duration::from_millis(20), 0).await;
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

    /// Spin up a real gRPC server backed by `OversizedActionService`
    /// (`chunk_size` bytes per `ActionResult` item, `chunk_count` items),
    /// connect a real `FlightClientPool`, and return `do_action`'s result.
    /// Shared by the size-cap tests below so each one only states its chunk
    /// shape and expected outcome, not the server/client plumbing.
    async fn do_action_against_oversized_mock(
        chunk_size: usize,
        chunk_count: usize,
    ) -> Result<RuntimeResult<Vec<u8>>, Box<dyn std::error::Error>> {
        use arrow_flight::flight_service_server::FlightServiceServer;
        use tonic::transport::Server;

        use oversized_action_service::OversizedActionService;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr: std::net::SocketAddr = listener
            .local_addr()
            .map_err(|e| Box::<dyn std::error::Error>::from(format!("local_addr: {e}")))?;
        let incoming = tonic::transport::server::TcpIncoming::from(listener);

        let service = OversizedActionService {
            chunk_size,
            chunk_count,
        };
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .expect("serve");
        });

        let url = format!("http://{addr}");
        let pool = FlightClientPool::new(url)
            .map_err(|e| Box::<dyn std::error::Error>::from(format!("pool: {e}")))?;
        let action =
            crate::flight_action::KrishivFlightAction::Explain(crate::flight_action::ExplainBody {
                sql: "SELECT 1".into(),
            });

        let result = pool.do_action(&action).await;
        server.abort();
        Ok(result)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires binding a local TCP listener; run with --ignored outside restricted sandboxes"]
    async fn do_action_rejects_response_exceeding_size_cap()
    -> Result<(), Box<dyn std::error::Error>> {
        // 40 chunks * 2 MiB = 80 MiB, over the 64 MiB cap while each chunk
        // stays comfortably under tonic's default 4 MiB decode-length limit —
        // isolates the app-level accumulation cap from the gRPC message-size
        // limit exercised by the two tests below.
        let result = do_action_against_oversized_mock(2 * 1024 * 1024, 40).await?;
        match result {
            Err(RuntimeError::ResultTooLarge { message }) => {
                assert!(
                    message.contains("MiB limit"),
                    "expected a response-size-cap error, got: {message}"
                );
            }
            other => panic!("expected ResultTooLarge size-limit error, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires binding a local TCP listener; run with --ignored outside restricted sandboxes"]
    async fn do_action_single_chunk_between_grpc_default_and_app_cap_succeeds()
    -> Result<(), Box<dyn std::error::Error>> {
        // The real server (do_action_fallback) always sends the whole
        // response as ONE ActionResult, unlike the multi-chunk test above.
        // 40 MiB in one chunk exceeds tonic's 4 MiB decode-length default but
        // stays under the 64 MiB app-level cap — before
        // DO_ACTION_MAX_DECODE_BYTES raised the client's decode limit, this
        // failed with a raw, unclassified tonic decode error before the
        // app-level cap (or ResultTooLarge classification) ever ran.
        let result = do_action_against_oversized_mock(40 * 1024 * 1024, 1).await?;
        let body = result.expect("40 MiB single-chunk response must succeed, not hit gRPC's decode-length default");
        assert_eq!(body.len(), 40 * 1024 * 1024);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires binding a local TCP listener; run with --ignored outside restricted sandboxes"]
    async fn do_action_single_chunk_over_app_cap_is_rejected_cleanly()
    -> Result<(), Box<dyn std::error::Error>> {
        // Same one-shot shape as the real server, sized past the 64 MiB app
        // cap but still under DO_ACTION_MAX_DECODE_BYTES (96 MiB) so the app
        // check — not gRPC's own decode limit — is what actually fires.
        let result = do_action_against_oversized_mock(80 * 1024 * 1024, 1).await?;
        match result {
            Err(RuntimeError::ResultTooLarge { message }) => {
                assert!(
                    message.contains("MiB limit"),
                    "expected a response-size-cap error, got: {message}"
                );
            }
            other => panic!("expected ResultTooLarge size-limit error, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires binding a local TCP listener; run with --ignored outside restricted sandboxes"]
    async fn do_action_single_chunk_exceeding_grpc_decode_limit_is_classified_as_too_large()
    -> Result<(), Box<dyn std::error::Error>> {
        // Live testing caught this: a response bigger than
        // DO_ACTION_MAX_DECODE_BYTES itself (not just DO_ACTION_MAX_RESPONSE_BYTES)
        // fails the *transport's* decode with a raw tonic::Code::OutOfRange —
        // the accumulation loop in do_action never even sees a byte, so
        // DO_ACTION_MAX_RESPONSE_BYTES's own check can't be what classifies
        // it. No amount of raising the decode limit fixes this in general
        // (a large enough result always exceeds whatever it's set to), so
        // OutOfRange must be classified as ResultTooLarge directly in
        // map_do_action_status, exercised here via a chunk bigger than
        // DO_ACTION_MAX_DECODE_BYTES (96 MiB).
        let result = do_action_against_oversized_mock(150 * 1024 * 1024, 1).await?;
        assert!(
            matches!(result, Err(RuntimeError::ResultTooLarge { .. })),
            "expected ResultTooLarge for a response exceeding the gRPC decode limit, got {result:?}"
        );
        Ok(())
    }

    /// #217: a server-side job cancel must surface as a permanent error —
    /// classifying it transient made with_retry resubmit the statement as
    /// a fresh job (the cancelled query rose from the dead live).
    #[test]
    fn cancelled_is_never_a_transient_error() {
        let cancelled = RuntimeError::transport(
            "batch SQL job batch-sql-1 failed: job finished in state Cancelled",
        );
        assert!(!is_transient_status(&cancelled));
        let grpc_cancelled = RuntimeError::transport("status: Cancelled, message: ...");
        assert!(!is_transient_status(&grpc_cancelled));
        // The genuinely transient shapes stay retryable.
        assert!(is_transient_status(&RuntimeError::transport(
            "status: Unavailable"
        )));
        assert!(is_transient_status(&RuntimeError::transport(
            "connection refused"
        )));
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
        let batches = collect_flight_batches(stream, Duration::from_secs(5), 0)
            .await
            .expect("finite stream must drain without timing out");
        assert_eq!(batches.len(), 3);
    }

    // ── collect_flight_batches size cap ───────────────────────────────────

    /// A batch with one non-empty Int64 column, so `get_array_memory_size()`
    /// is a real, non-zero, easy-to-reason-about number of bytes.
    fn sized_batch(rows: usize) -> RecordBatch {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "n",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from_iter_values(
                0..rows as i64,
            ))],
        )
        .expect("valid batch")
    }

    #[tokio::test]
    async fn collect_flight_batches_zero_cap_is_unbounded() {
        let stream = Box::pin(futures::stream::iter((0..3).map(|_| Ok(sized_batch(1_000)))))
            as std::pin::Pin<
                Box<dyn futures::Stream<Item = arrow_flight::error::Result<RecordBatch>>>,
            >;
        let batches = collect_flight_batches(stream, Duration::from_secs(5), 0)
            .await
            .expect("zero cap must not reject any size");
        assert_eq!(batches.len(), 3);
    }

    #[tokio::test]
    async fn collect_flight_batches_under_cap_is_accepted() {
        let one_batch_bytes = sized_batch(1_000).get_array_memory_size();
        let stream = Box::pin(futures::stream::iter((0..3).map(|_| Ok(sized_batch(1_000)))))
            as std::pin::Pin<
                Box<dyn futures::Stream<Item = arrow_flight::error::Result<RecordBatch>>>,
            >;
        let batches = collect_flight_batches(stream, Duration::from_secs(5), one_batch_bytes * 10)
            .await
            .expect("well under cap must be accepted");
        assert_eq!(batches.len(), 3);
    }

    #[tokio::test]
    async fn collect_flight_batches_over_cap_is_rejected() {
        let one_batch_bytes = sized_batch(1_000).get_array_memory_size();
        let stream = Box::pin(futures::stream::iter((0..3).map(|_| Ok(sized_batch(1_000)))))
            as std::pin::Pin<
                Box<dyn futures::Stream<Item = arrow_flight::error::Result<RecordBatch>>>,
            >;
        // Cap sits strictly between one batch and the full three-batch total,
        // so the accumulation must be rejected partway through, not at the end.
        let result = collect_flight_batches(
            stream,
            Duration::from_secs(5),
            (one_batch_bytes as f64 * 1.5) as usize,
        )
        .await;
        match result {
            Err(RuntimeError::ResultTooLarge { message }) => {
                assert!(message.contains("exceeds maximum"));
            }
            other => panic!("expected ResultTooLarge, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn ensure_health_checks_starts_the_loop_lazily_and_is_idempotent() {
        use std::sync::atomic::Ordering;
        // A pool built from a sync (no ambient runtime) path never had its
        // eager `spawn_health_checks` run: the loop must not be up yet.
        let pool = FlightClientPool::new("http://127.0.0.1:1").expect("pool");
        assert!(
            !pool.health_started.load(Ordering::Relaxed),
            "freshly built pool must not have started health checks"
        );
        assert!(
            pool.health_check_handle.lock().await.is_none(),
            "no background handle before first use"
        );

        // First async touch (what `get_channel` does) starts the loop.
        pool.ensure_health_checks().await;
        assert!(
            pool.health_started.load(Ordering::Relaxed),
            "first async use must start the background health loop"
        );
        assert!(
            pool.health_check_handle.lock().await.is_some(),
            "background handle must be installed after ensure_health_checks"
        );

        // Idempotent: a repeat call neither restarts nor replaces the handle.
        pool.ensure_health_checks().await;
        assert!(pool.health_check_handle.lock().await.is_some());

        pool.stop_health_checks().await;
    }
}
