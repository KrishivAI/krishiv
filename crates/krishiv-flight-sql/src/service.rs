use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionBeginTransactionRequest, ActionBeginTransactionResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionEndTransactionRequest, CommandGetCatalogs,
    CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandPreparedStatementQuery, CommandStatementQuery, DoPutPreparedStatementResult,
    EndTransaction, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    Ticket, flight_service_server::FlightService,
};
use futures::TryStreamExt as _;
use futures::{Stream, stream};
use prost::Message as _;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use krishiv_plan::governance::{AuthProvider, PolicyHook, StaticApiKeyAuthProvider};

use crate::actions::{
    KrishivActionError, build_param_schema, count_sql_params, encode_batches_ipc,
    normalize_question_mark_params, schema_to_ipc_bytes, substitute_sql_params,
};
use crate::host::FlightExecutionHost;

/// Env var controlling how many queries may execute concurrently through the
/// Flight SQL ingress. Requests above the limit receive `Status::resource_exhausted`.
/// Default: 256.  Set to `"0"` to disable the cap entirely.
const FLIGHT_MAX_CONCURRENT_QUERIES_ENV: &str = "KRISHIV_FLIGHT_MAX_CONCURRENT_QUERIES";
/// Default cap on simultaneous Flight SQL query executions.
const DEFAULT_FLIGHT_MAX_CONCURRENT_QUERIES: usize = 256;
/// API-1: Cap on the total serialized result size per DoGet to prevent OOM.
/// Read from env `KRISHIV_FLIGHT_MAX_RESULT_BYTES`; default 2 GiB.
const FLIGHT_MAX_RESULT_BYTES_ENV: &str = "KRISHIV_FLIGHT_MAX_RESULT_BYTES";
const DEFAULT_FLIGHT_MAX_RESULT_BYTES: usize = 2 * 1024 * 1024 * 1024;

/// Server SqlInfo metadata served by `GetSqlInfo` (G1b).
///
/// Values describe the surface honestly: single-statement autocommit
/// semantics (no real transaction atomicity — see the transaction notes on
/// `do_action_end_transaction`), DataFusion-dialect SQL, `"` identifier
/// quoting. Extend as capabilities land; never claim what the engine does
/// not do.
fn server_sql_info() -> &'static SqlInfoData {
    use arrow_flight::sql::SqlSupportedTransaction;
    static INFO: std::sync::LazyLock<SqlInfoData> = std::sync::LazyLock::new(|| {
        let mut builder = SqlInfoDataBuilder::new();
        builder.append(SqlInfo::FlightSqlServerName, "Krishiv");
        builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        builder.append(SqlInfo::FlightSqlServerArrowVersion, "58.3.0");
        builder.append(SqlInfo::FlightSqlServerReadOnly, false);
        builder.append(SqlInfo::FlightSqlServerSql, true);
        builder.append(SqlInfo::FlightSqlServerSubstrait, false);
        builder.append(
            SqlInfo::FlightSqlServerTransaction,
            SqlSupportedTransaction::None as i32,
        );
        builder.append(SqlInfo::FlightSqlServerCancel, true);
        builder.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
        builder.append(SqlInfo::SqlDdlCatalog, false);
        builder.append(SqlInfo::SqlDdlSchema, false);
        builder.append(SqlInfo::SqlDdlTable, true);
        builder.append(SqlInfo::SqlMaxColumnsInTable, 0i64);
        #[allow(clippy::expect_used)]
        builder
            .build()
            .expect("static SqlInfo metadata must build: fixed keys and values")
    });
    &INFO
}

/// Maximum number of active transactions before rejecting new `BeginTransaction` requests.
const MAX_TRANSACTIONS: usize = 10_000;
/// Transaction entries older than this are evicted on each `BeginTransaction` sweep.
const TRANSACTION_TTL: Duration = Duration::from_secs(300);

/// Bookkeeping for one open Flight SQL transaction id.
///
/// `statement_count` tracks how many statements have executed under this id
/// so `do_action_end_transaction` can tell whether `Commit`/`Rollback` are
/// truly no-ops (0 or 1 statement — autocommit-per-statement already behaves
/// the same as a real transaction) or are silently failing to provide the
/// atomicity/isolation a caller of a multi-statement transaction would expect.
struct TransactionEntry {
    owner: String,
    created: Instant,
    statement_count: u32,
}

/// Per-subject LRU cache mapping handle → bound parameter record batches.
type PreparedStatementCache =
    Arc<tokio::sync::Mutex<HashMap<String, lru::LruCache<String, String>>>>;
type BoundParamCache =
    Arc<tokio::sync::Mutex<HashMap<String, lru::LruCache<String, Vec<RecordBatch>>>>>;

/// **Beta API**: may change between minor releases.
#[derive(Clone)]
pub struct KrishivFlightSqlService {
    auth: Option<Arc<dyn AuthProvider>>,
    policy: Option<Arc<dyn PolicyHook>>,
    host: FlightExecutionHost,
    /// Per-subject LRU caches of opaque handle (UUID string) → SQL text for prepared statements.
    prepared_statements: PreparedStatementCache,
    /// Per-subject LRU caches of handle → bound parameter record batches (set via DoPut).
    bound_params: BoundParamCache,
    /// Active Flight SQL transaction ids issued by `BeginTransaction`.
    /// Entries older than `TRANSACTION_TTL` are swept on each new transaction
    /// request.  Capped at `MAX_TRANSACTIONS`.
    transactions: Arc<tokio::sync::Mutex<HashMap<String, TransactionEntry>>>,
    /// Semaphore that caps the number of queries executing concurrently through
    /// the Flight ingress. `None` means no cap.
    inflight_queries: Option<Arc<tokio::sync::Semaphore>>,
    /// API-1: Maximum total bytes for a single DoGet result. Exceeding it
    /// returns `resource_exhausted` to prevent server OOM.
    max_result_bytes: usize,
}

const FLIGHT_PREPARED_STMT_CAPACITY_ENV: &str = "KRISHIV_FLIGHT_PREPARED_STMT_CAPACITY";
const DEFAULT_PREPARED_STMT_CAPACITY: usize = 128;

fn read_prepared_stmt_capacity() -> std::num::NonZeroUsize {
    let n = std::env::var(FLIGHT_PREPARED_STMT_CAPACITY_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_PREPARED_STMT_CAPACITY);
    std::num::NonZeroUsize::new(n.max(1)).unwrap_or(std::num::NonZeroUsize::MIN)
}

fn read_max_concurrent_queries() -> Option<usize> {
    let n = std::env::var(FLIGHT_MAX_CONCURRENT_QUERIES_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_FLIGHT_MAX_CONCURRENT_QUERIES);
    if n == 0 { None } else { Some(n) }
}

/// API-1: Read the result-size cap from the environment.
fn read_max_result_bytes() -> usize {
    std::env::var(FLIGHT_MAX_RESULT_BYTES_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_FLIGHT_MAX_RESULT_BYTES)
}

impl std::fmt::Debug for KrishivFlightSqlService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KrishivFlightSqlService")
            .field("auth", &self.auth.is_some())
            .field("policy", &self.policy.is_some())
            .field(
                "max_concurrent_queries",
                &self
                    .inflight_queries
                    .as_ref()
                    .map(|s| s.available_permits()),
            )
            .finish_non_exhaustive()
    }
}

impl KrishivFlightSqlService {
    /// Create a new `KrishivFlightSqlService` with a shared server-side cluster.
    ///
    /// The concurrent-query cap is read from `KRISHIV_FLIGHT_MAX_CONCURRENT_QUERIES`
    /// (default 256; set to `"0"` to disable).
    pub fn new() -> Result<Self, Status> {
        let limit = read_max_concurrent_queries();
        Ok(Self {
            auth: None,
            policy: None,
            host: FlightExecutionHost::from_env()?,
            prepared_statements: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            bound_params: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            transactions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            inflight_queries: limit.map(|n| Arc::new(tokio::sync::Semaphore::new(n))),
            max_result_bytes: read_max_result_bytes(),
        })
    }

    /// Attach a pre-built execution host (tests / custom wiring).
    pub fn with_host(host: FlightExecutionHost) -> Self {
        let limit = read_max_concurrent_queries();
        Self {
            auth: None,
            policy: None,
            host,
            prepared_statements: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            bound_params: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            transactions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            inflight_queries: limit.map(|n| Arc::new(tokio::sync::Semaphore::new(n))),
            max_result_bytes: read_max_result_bytes(),
        }
    }

    /// Override the concurrent-query cap programmatically. `0` disables the cap.
    pub fn with_max_concurrent_queries(mut self, n: usize) -> Self {
        self.inflight_queries = if n == 0 {
            None
        } else {
            Some(Arc::new(tokio::sync::Semaphore::new(n)))
        };
        self
    }

    /// Attach an [`AuthProvider`] to this service.
    ///
    /// When set, every `get_flight_info_statement` and `do_get_statement` call
    /// must supply a valid `Bearer <token>` in the `authorization` metadata header.
    pub fn with_auth(mut self, auth: Arc<dyn AuthProvider>) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Attach a [`PolicyHook`] to this service.
    ///
    /// When set, table access is checked against the policy for every query.
    pub fn with_policy(mut self, policy: Arc<dyn PolicyHook>) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Validate the `authorization: Bearer <token>` header.
    ///
    /// Returns `Ok(Some(subject))` when auth is configured and the token is
    /// valid, `Ok(None)` when no [`AuthProvider`] is attached, and
    /// `Err(Status::unauthenticated(...))` when the token is missing or invalid.
    fn authenticate_request<B>(&self, req: &Request<B>) -> Result<Option<String>, Status> {
        // SEC-2 default-deny (Phase 63): an operator who configures
        // authentication expects authorization too. If auth is set but no
        // policy engine is attached, refuse on EVERY path uniformly — not just
        // the statement paths. Folding the guard here closes the asymmetry
        // where prepared-statement updates, DoAction, and metadata handlers
        // authenticated without ever consulting a policy.
        if self.auth.is_some() && self.policy.is_none() {
            return Err(Status::permission_denied(
                "auth is configured but no policy engine is set; \
                 configure a PolicyHook or use an unauthenticated service",
            ));
        }
        let Some(auth) = &self.auth else {
            if krishiv_common::profile_requires_authenticated_flight(
                krishiv_common::resolve_durability_profile(),
            ) {
                return Err(Status::unauthenticated(
                    "Flight SQL auth is required under durable profiles; configure KRISHIV_API_KEYS",
                ));
            }
            return Ok(None);
        };
        let token = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(str::to_owned)
            .ok_or_else(|| Status::unauthenticated("missing Bearer token"))?;
        auth.authenticate(&token)
            .map(Some)
            .ok_or_else(|| Status::unauthenticated("invalid API key"))
    }

    /// Check table-level access policy if configured.
    ///
    /// Uses the AST-based `krishiv_sql::referenced_table_names` to extract
    /// ALL referenced tables (supports subqueries, CTEs, JOINs, etc. — the
    /// previous `extract_from_table` scanner was easily bypassed).
    fn check_table_access(&self, query: &str) -> Result<(), Status> {
        let Some(policy) = &self.policy else {
            return Ok(());
        };
        let table_names = krishiv_sql::referenced_table_names(query)
            .map_err(|_| Status::internal("failed to parse query for table-access policy check"))?;
        for table_name in &table_names {
            if !policy.check_table_access(table_name) {
                return Err(Status::permission_denied(format!(
                    "access denied to table: {table_name}"
                )));
            }
        }
        Ok(())
    }

    async fn validate_transaction_id(
        &self,
        transaction_id: Option<&[u8]>,
        subject: Option<&str>,
    ) -> Result<(), Status> {
        let Some(bytes) = transaction_id.filter(|id| !id.is_empty()) else {
            return Ok(());
        };
        let id = std::str::from_utf8(bytes)
            .map_err(|_| Status::invalid_argument("invalid transaction id encoding"))?;
        let transactions = self.transactions.lock().await;
        match transactions.get(id) {
            Some(entry) if subject.is_none_or(|s| s == entry.owner) => {
                if entry.created.elapsed() >= TRANSACTION_TTL {
                    Err(Status::invalid_argument(format!(
                        "transaction id {id} has expired"
                    )))
                } else {
                    Ok(())
                }
            }
            Some(_) => Err(Status::permission_denied(format!(
                "transaction id {id} does not belong to this subject"
            ))),
            None => Err(Status::invalid_argument(format!(
                "unknown transaction id: {id}"
            ))),
        }
    }

    /// Record that a statement executed under `transaction_id` (no-op if
    /// `None` or the id is no longer tracked). Used only to decide whether
    /// `Commit`/`Rollback` need to warn about missing atomicity — it has no
    /// effect on execution, which always runs autocommit.
    async fn record_transaction_statement(&self, transaction_id: Option<&[u8]>) {
        let Some(id) = transaction_id
            .filter(|id| !id.is_empty())
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
        else {
            return;
        };
        if let Some(entry) = self.transactions.lock().await.get_mut(id) {
            entry.statement_count = entry.statement_count.saturating_add(1);
        }
    }
}

/// Krishiv Flight SQL service implementation.

#[tonic::async_trait]
impl FlightSqlService for KrishivFlightSqlService {
    type FlightService = KrishivFlightSqlService;

    // Handshake requires auth when an AuthProvider is configured.
    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        if self.auth.is_some() {
            self.authenticate_request(&request)?;
        } else if krishiv_common::profile_requires_authenticated_flight(
            krishiv_common::resolve_durability_profile(),
        ) {
            return Err(Status::unauthenticated(
                "Flight SQL auth is required under durable profiles; configure KRISHIV_API_KEYS",
            ));
        }
        let resp = HandshakeResponse {
            protocol_version: 0,
            payload: bytes::Bytes::new(),
        };
        let out: Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>> =
            Box::pin(stream::once(async { Ok(resp) }));
        Ok(Response::new(out))
    }

    // Encode query into ticket, return FlightInfo
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Authenticate if an auth provider is configured (default-deny for
        // auth-without-policy is enforced inside authenticate_request, SEC-2).
        let subject = self.authenticate_request(&request)?;
        let transaction_id = query.transaction_id.unwrap_or_default();
        self.validate_transaction_id(Some(&transaction_id), subject.as_deref())
            .await?;

        // Encode the transaction id into the ticket so that do_get_statement
        // can re-validate it. Format: [4-byte txn_id_len][txn_id][query].
        let txn_len = (transaction_id.len() as u32).to_be_bytes();
        let mut handle = Vec::with_capacity(4 + transaction_id.len() + query.query.len());
        handle.extend_from_slice(&txn_len);
        handle.extend_from_slice(&transaction_id);
        handle.extend_from_slice(query.query.as_bytes());
        let ticket_query = TicketStatementQuery {
            statement_handle: handle.into(),
        };
        let ticket = Ticket {
            ticket: ticket_query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        // Leave `FlightInfo.schema` unset (empty bytes = "schema unknown",
        // per the Flight SQL spec) so clients take the authoritative schema
        // from the DoGet data stream. Explicitly encoding `Schema::empty()`
        // here declared a zero-field schema, which strict clients (ADBC,
        // and the JDBC driver's validation) reject as inconsistent with the
        // real result schema (platform gap G1a). Deriving the true schema
        // at GetFlightInfo time would require planning the query against
        // the active backend — a separate feature; unknown is honest.
        let info = FlightInfo::new().with_endpoint(endpoint);
        Ok(Response::new(info))
    }

    // Execute SQL and stream results
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // Authenticate if an auth provider is configured (default-deny for
        // auth-without-policy is enforced inside authenticate_request, SEC-2).
        let subject = self.authenticate_request(&request)?;

        // Decode the ticket. Two encodings are supported:
        //   * Prefixed: `[4-byte big-endian txn_len][txn_id][query]`, produced by
        //     `get_flight_info_statement`. `txn_len` may be `0` (no transaction).
        //   * Legacy: the whole handle is the raw SQL query, with no prefix.
        //
        // A leading `txn_len` of `0` unambiguously means a prefixed ticket with
        // no transaction id (real SQL never starts with four NUL bytes). A
        // claimed `txn_len` larger than the remaining handle is treated as a
        // legacy ticket rather than a truncated prefixed one, so raw-SQL handles
        // (whose first four bytes are ASCII SQL and parse as a huge length) are
        // decoded as the original query instead of being silently truncated.
        let handle = &ticket.statement_handle;
        let (transaction_id, query_bytes): (Option<Vec<u8>>, &[u8]) =
            if let Some(prefix) = handle.get(..4) {
                let txn_len = u32::from_be_bytes(prefix.try_into().unwrap_or([0; 4])) as usize;
                let txn_end = 4 + txn_len;
                if txn_len > 0 && handle.len() >= txn_end {
                    (
                        Some(handle.get(4..txn_end).unwrap_or(&[]).to_vec()),
                        handle.get(txn_end..).unwrap_or(&[]),
                    )
                } else if txn_len == 0 {
                    (None, handle.get(4..).unwrap_or(&[]))
                } else {
                    (None, handle)
                }
            } else {
                (None, handle)
            };

        // Re-validate the transaction id. Even though get_flight_info_statement
        // checked it, the ticket could have been reused after EndTransaction.
        self.validate_transaction_id(transaction_id.as_deref(), subject.as_deref())
            .await?;
        self.record_transaction_statement(transaction_id.as_deref())
            .await;

        let query = std::str::from_utf8(query_bytes)
            .map_err(|e| Status::invalid_argument(format!("invalid query encoding: {e}")))?;

        // Check table access if a policy is configured.
        self.check_table_access(query)?;

        // Acquire a concurrent-query slot. Returns immediately if no cap is set;
        // returns resource_exhausted when the semaphore is saturated.
        // The permit is held only for the duration of execute_sql, then dropped.
        let batches = {
            let _permit = if let Some(sem) = &self.inflight_queries {
                match sem.try_acquire() {
                    Ok(p) => Some(p),
                    Err(tokio::sync::TryAcquireError::NoPermits) => {
                        return Err(Status::resource_exhausted(
                            "too many concurrent Flight SQL queries; retry later",
                        ));
                    }
                    Err(tokio::sync::TryAcquireError::Closed) => None,
                }
            } else {
                None
            };
            self.host
                .execute_sql(query)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
            // _permit drops here
        };

        // API-1: Enforce a result-size cap to prevent server OOM from
        // unbounded `SELECT *` queries. Bypasses the 2 GiB batch-engine cap.
        if self.max_result_bytes > 0 {
            let total: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
            if total > self.max_result_bytes {
                return Err(Status::resource_exhausted(format!(
                    "Flight SQL result ({} bytes) exceeds maximum ({} bytes); \
                     add a LIMIT clause or raise {}",
                    total, self.max_result_bytes, FLIGHT_MAX_RESULT_BYTES_ENV
                )));
            }
        }

        let schema: Arc<Schema> = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(Schema::empty()));

        // Encode incrementally: FlightDataEncoder emits one message per batch
        // as the client drains do_get, instead of materializing the whole
        // encoded result up front (a second full copy for large results).
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::iter(batches.into_iter().map(Ok)))
            .map_err(Status::from);

        let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
            Box::pin(encoded);
        Ok(Response::new(stream))
    }

    // Required method — no-op for R8.1 beta (server doesn't serve SqlInfo)
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    /// Create a server-side prepared statement and return an opaque handle.
    ///
    /// The handle is a UUID string stored in the `prepared_statements` map.
    /// Clients pass it back via [`CommandPreparedStatementQuery`] to execute
    /// the statement without re-parsing the SQL.
    ///
    /// G16: The response includes a `parameter_schema` derived from `$N`
    /// positional placeholders found in the SQL text.
    ///
    /// G12: JDBC/ADBC clients bind parameters as ordinal `?` marks, not the
    /// engine's native `$N` — the SQL text is normalized to `$N` once here
    /// (before counting params, planning `dataset_schema`, or caching), so
    /// every downstream step (this handler, `do_put_prepared_statement_query`,
    /// `do_get_statement`) only ever sees `$N`.
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let subject = self.authenticate_request(&request)?;
        let subject_key = subject.as_deref().unwrap_or("__anon__").to_owned();
        let handle = Uuid::new_v4().to_string();
        let sql = normalize_question_mark_params(&query.query);
        let n_params = count_sql_params(&sql);
        let param_schema = build_param_schema(n_params);
        let parameter_schema = schema_to_ipc_bytes(&param_schema)?;
        // Best-effort result schema (G1: the JDBC driver routes
        // query-vs-update on whether `dataset_schema` has fields — leaving
        // it empty sent every SELECT down the unimplemented update path).
        // Empty bytes remain the honest "unknown" for statements the host
        // cannot plan.
        let dataset_schema = match self.host.sql_query_schema(&sql).await {
            Some(schema) => schema_to_ipc_bytes(schema.as_ref())?,
            None => Vec::new(),
        };
        {
            let mut map = self.prepared_statements.lock().await;
            let cache = map
                .entry(subject_key)
                .or_insert_with(|| lru::LruCache::new(read_prepared_stmt_capacity()));
            cache.put(handle.clone(), sql);
        }
        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into_bytes().into(),
            parameter_schema: parameter_schema.into(),
            dataset_schema: dataset_schema.into(),
        })
    }

    /// Bind parameters to a prepared statement (G16).
    ///
    /// The client sends an Arrow IPC record batch whose columns correspond to
    /// the `$1 … $N` positional parameters in the prepared statement SQL.
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        let subject = {
            let meta_req: &Request<_> = &request;
            self.authenticate_request(meta_req)?
        };
        let subject_key = subject.as_deref().unwrap_or("__anon__").to_owned();
        let handle = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| {
                Status::invalid_argument(format!("invalid prepared statement handle: {e}"))
            })?
            .to_owned();

        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
            request.into_inner().map_err(|e| e.into()),
        )
        .try_collect()
        .await?;

        if !batches.is_empty() {
            let mut map = self.bound_params.lock().await;
            let cache = map
                .entry(subject_key)
                .or_insert_with(|| lru::LruCache::new(read_prepared_stmt_capacity()));
            cache.put(handle.clone(), batches);
        }

        Ok(DoPutPreparedStatementResult {
            prepared_statement_handle: Some(handle.into_bytes().into()),
        })
    }

    /// Return [`FlightInfo`] for a prepared statement (used by clients that
    /// call `GetFlightInfo` before `DoGet`).
    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Default-deny for auth-without-policy is enforced inside
        // authenticate_request (SEC-2).
        let subject = self.authenticate_request(&request)?;
        let subject_key = subject.as_deref().unwrap_or("__anon__").to_owned();
        let handle = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| {
                Status::invalid_argument(format!("invalid prepared statement handle encoding: {e}"))
            })?
            .to_owned();

        let sql = {
            let mut map = self.prepared_statements.lock().await;
            map.get_mut(&subject_key)
                .and_then(|cache| cache.get(&handle))
                .cloned()
                .ok_or_else(|| Status::not_found(format!("unknown prepared statement: {handle}")))?
        };

        // Build the same raw-SQL ticket the statement path produces
        // (`[4-byte txn_len = 0][sql]`), but attach the result schema when
        // the host can plan the statement: the JDBC driver's prepared-query
        // flow reads `FlightInfo.getSchema()` and fails on empty schema
        // bytes (G1). Statements the host cannot plan keep the honest
        // "unknown" (no schema), which clients handle by deferring to the
        // DoGet stream.
        let schema = self.host.sql_query_schema(&sql).await;
        let mut ticket_handle = Vec::with_capacity(4 + sql.len());
        ticket_handle.extend_from_slice(&0u32.to_be_bytes());
        ticket_handle.extend_from_slice(sql.as_bytes());
        let ticket = Ticket {
            ticket: TicketStatementQuery {
                statement_handle: ticket_handle.into(),
            }
            .as_any()
            .encode_to_vec()
            .into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let flight_descriptor = request.into_inner();
        let mut info = FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        if let Some(schema) = schema {
            info = info
                .try_with_schema(schema.as_ref())
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(Response::new(info))
    }

    /// Execute a prepared statement and stream results.
    ///
    /// G16: If parameters were previously bound via `DoPut`, `$N` placeholders
    /// in the SQL are substituted with literal values before execution.
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let subject = self.authenticate_request(&request)?;
        let subject_key = subject.as_deref().unwrap_or("__anon__").to_owned();
        let handle = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| {
                Status::invalid_argument(format!("invalid prepared statement handle encoding: {e}"))
            })?
            .to_owned();

        let sql = {
            let mut map = self.prepared_statements.lock().await;
            map.get_mut(&subject_key)
                .and_then(|cache| cache.get(&handle))
                .cloned()
                .ok_or_else(|| Status::not_found(format!("unknown prepared statement: {handle}")))?
        };

        // Apply bound parameters if present.
        let effective_sql = {
            let mut params = self.bound_params.lock().await;
            match params
                .get_mut(&subject_key)
                .and_then(|cache| cache.get(&handle))
                .and_then(|b| b.first())
            {
                Some(batch) => substitute_sql_params(&sql, batch),
                None => sql,
            }
        };

        let ticket = TicketStatementQuery {
            statement_handle: effective_sql.into_bytes().into(),
        };
        self.do_get_statement(ticket, request).await
    }

    /// Execute a prepared statement as an update (no result set).
    ///
    /// The JDBC driver sends DDL/DML — and any statement whose
    /// `dataset_schema` it could not obtain — down this path (G1). Executes
    /// the stored SQL and returns the affected-row count when the engine
    /// reports one, else `-1` (unknown, per the Flight SQL convention).
    async fn do_put_prepared_statement_update(
        &self,
        query: arrow_flight::sql::CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let subject = self.authenticate_request(&request)?;
        let subject_key = subject.as_deref().unwrap_or("__anon__").to_owned();
        let handle = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| {
                Status::invalid_argument(format!("invalid prepared statement handle encoding: {e}"))
            })?
            .to_owned();

        let sql = {
            let mut map = self.prepared_statements.lock().await;
            map.get_mut(&subject_key)
                .and_then(|cache| cache.get(&handle))
                .cloned()
                .ok_or_else(|| Status::not_found(format!("unknown prepared statement: {handle}")))?
        };

        self.check_table_access(&sql)?;
        let batches = self
            .host
            .execute_sql(&sql)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        // DDL/DML through this engine returns either nothing or a summary
        // batch; a row count is not reliably reported, so `-1` (unknown) is
        // the honest answer unless the result is a single count row.
        let _ = batches;
        Ok(-1)
    }

    /// Close (drop) a previously created prepared statement.
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        let subject = self.authenticate_request(&request)?;
        let subject_key = subject.as_deref().unwrap_or("__anon__").to_owned();
        let handle = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| {
                Status::invalid_argument(format!("invalid prepared statement handle encoding: {e}"))
            })?
            .to_owned();
        {
            let mut map = self.prepared_statements.lock().await;
            if let Some(cache) = map.get_mut(&subject_key) {
                cache.pop(&handle);
            }
        }
        {
            let mut map = self.bound_params.lock().await;
            if let Some(cache) = map.get_mut(&subject_key) {
                cache.pop(&handle);
            }
        }
        Ok(())
    }

    /// Issue an opaque transaction id for client-side bookkeeping.
    ///
    /// **No atomicity or isolation**: this id is validated (exists, owned by
    /// the calling subject, not expired) on subsequent `do_get_statement`
    /// calls, but execution is unconditionally autocommit — statements run
    /// immediately with no write buffering, no snapshot reads, and no
    /// participation from the catalog or execution layer. `Commit` and
    /// `Rollback` (see [`Self::do_action_end_transaction`]) are both no-ops
    /// against already-applied statements.
    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        let subject = self.authenticate_request(&request)?;
        let subject_key = subject.unwrap_or_else(|| "__anon__".to_owned());
        let transaction_id = Uuid::new_v4().to_string();
        let now = Instant::now();
        let mut txns = self.transactions.lock().await;
        // Sweep expired entries and enforce cap before inserting.
        txns.retain(|_id, entry| now.duration_since(entry.created) < TRANSACTION_TTL);
        if txns.len() >= MAX_TRANSACTIONS {
            return Err(Status::resource_exhausted(format!(
                "transaction limit reached ({MAX_TRANSACTIONS}); retry after existing transactions expire"
            )));
        }
        txns.insert(
            transaction_id.clone(),
            TransactionEntry {
                owner: subject_key,
                created: now,
                statement_count: 0,
            },
        );
        drop(txns);
        Ok(ActionBeginTransactionResult {
            transaction_id: transaction_id.into_bytes().into(),
        })
    }

    /// Retire a transaction id issued by `BeginTransaction`.
    ///
    /// Both `Commit` and `Rollback` are no-ops with respect to the statements
    /// already executed under this transaction id: `do_get_statement` runs
    /// every statement autocommit as soon as it is submitted, so there is
    /// nothing staged to commit or discard. `Commit` "succeeding" does not
    /// mean anything was atomically applied together, and `Rollback` cannot
    /// undo statements that already ran.
    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        let subject = self.authenticate_request(&request)?;
        let subject_key = subject.as_deref().unwrap_or("__anon__");
        let transaction_id = std::str::from_utf8(&query.transaction_id)
            .map_err(|_| Status::invalid_argument("invalid transaction id encoding"))?;
        let statement_count = {
            let mut txns = self.transactions.lock().await;
            match txns.get(transaction_id) {
                None => {
                    return Err(Status::invalid_argument(format!(
                        "unknown transaction id: {transaction_id}"
                    )));
                }
                Some(entry) if entry.owner != subject_key => {
                    return Err(Status::permission_denied(format!(
                        "transaction id {transaction_id} does not belong to this subject"
                    )));
                }
                Some(entry) => {
                    let count = entry.statement_count;
                    txns.remove(transaction_id);
                    count
                }
            }
        };
        match EndTransaction::try_from(query.action)
            .map_err(|_| Status::invalid_argument("invalid EndTransaction action"))?
        {
            // A single autocommit statement (or none) behaves identically to a
            // real transaction, so only warn when there were multiple
            // statements whose atomicity as a group was not actually provided.
            EndTransaction::Commit => {
                if statement_count > 1 {
                    tracing::warn!(
                        statement_count,
                        "Flight SQL Commit is a no-op: {statement_count} statements ran \
                         autocommit as they were submitted, not atomically as a group"
                    );
                }
                Ok(())
            }
            EndTransaction::Rollback => {
                if statement_count > 0 {
                    tracing::warn!(
                        statement_count,
                        "Flight SQL Rollback is a no-op: {statement_count} already-executed \
                         statement(s) cannot be undone (no transactional storage backend)"
                    );
                }
                Ok(())
            }
            EndTransaction::Unspecified => Err(Status::invalid_argument(
                "EndTransaction action must be Commit or Rollback",
            )),
        }
    }

    // G17: Catalog introspection

    /// Return FlightInfo for a `GetSqlInfo` request.
    ///
    /// Required by the stock Arrow Flight SQL JDBC driver, which calls this
    /// during connection setup and fails the whole connection when it is
    /// unimplemented (platform gap G1b).
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authenticate_request(&request)?;
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .try_with_schema(server_sql_info().schema().as_ref())
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(info))
    }

    /// Stream the server's SqlInfo metadata, filtered to the requested ids.
    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.authenticate_request(&request)?;
        let batch = query
            .into_builder(server_sql_info())
            .build()
            .map_err(|e| Status::internal(e.to_string()))?;
        let schema = batch.schema();
        let flight_data = batches_to_flight_data(schema.as_ref(), vec![batch])
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .map(Ok::<FlightData, Status>);
        let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
            Box::pin(stream::iter(flight_data));
        Ok(Response::new(stream))
    }

    /// Return FlightInfo for a `GetCatalogs` request (what BI tools call
    /// first when browsing; platform gap G1c).
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authenticate_request(&request)?;
        let flight_descriptor = request.into_inner();
        let ticket_bytes = query.as_any().encode_to_vec();
        let schema = query.into_builder().schema();
        let ticket = Ticket {
            ticket: ticket_bytes.into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(info))
    }

    /// Stream the catalog list. The Flight SQL surface exposes the single
    /// `krishiv` catalog (matching `GetDbSchemas`/`GetTables`, which report
    /// `("krishiv", "default")` for every registered table).
    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.authenticate_request(&request)?;
        let mut builder = query.into_builder();
        builder.append("krishiv");
        let batch = builder
            .build()
            .map_err(|e| Status::internal(e.to_string()))?;
        let schema = batch.schema();
        let flight_data = batches_to_flight_data(schema.as_ref(), vec![batch])
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .map(Ok::<FlightData, Status>);
        let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
            Box::pin(stream::iter(flight_data));
        Ok(Response::new(stream))
    }

    /// Return FlightInfo for a `GetTableTypes` request. BI tools and the
    /// Flight SQL JDBC driver call this while introspecting the catalog
    /// (platform gap G1); mirrors `get_flight_info_catalogs`.
    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authenticate_request(&request)?;
        let flight_descriptor = request.into_inner();
        let ticket_bytes = query.as_any().encode_to_vec();
        let schema = query.into_builder().schema();
        let ticket = Ticket {
            ticket: ticket_bytes.into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(info))
    }

    /// Stream the list of table types. Krishiv exposes a single `TABLE`
    /// type, matching `do_get_tables`, which reports `TABLE` for every
    /// registered table.
    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.authenticate_request(&request)?;
        let mut builder = query.into_builder();
        builder.append("TABLE");
        let batch = builder
            .build()
            .map_err(|e| Status::internal(e.to_string()))?;
        let schema = batch.schema();
        let flight_data = batches_to_flight_data(schema.as_ref(), vec![batch])
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .map(Ok::<FlightData, Status>);
        let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
            Box::pin(stream::iter(flight_data));
        Ok(Response::new(stream))
    }

    /// Return FlightInfo for a `GetDbSchemas` catalog query (G17).
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authenticate_request(&request)?;
        let flight_descriptor = request.into_inner();
        let ticket_bytes = query.as_any().encode_to_vec();
        let schema = query.into_builder().schema();
        let ticket = Ticket {
            ticket: ticket_bytes.into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(info))
    }

    /// Stream the list of schemas in the Krishiv catalog (G17).
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.authenticate_request(&request)?;
        let mut builder = query.into_builder();
        // The Flight SQL catalog exposes a single (catalog, schema) namespace;
        // list_catalog_tables always returns ("krishiv", "default") tuples, so
        // emitting one schema row per table would produce duplicates. Emit the
        // schema once.
        builder.append("krishiv", "default");
        let schema = builder.schema();
        let batch = builder
            .build()
            .map_err(|e| Status::internal(e.to_string()))?;
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(futures::future::ready(Ok::<
                _,
                arrow_flight::error::FlightError,
            >(
                batch
            ))))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    /// Return FlightInfo for a `GetTables` catalog query (G17).
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authenticate_request(&request)?;
        let flight_descriptor = request.into_inner();
        let ticket_bytes = query.as_any().encode_to_vec();
        let schema = query.into_builder().schema();
        let ticket = Ticket {
            ticket: ticket_bytes.into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(info))
    }

    /// Stream the list of tables in the Krishiv catalog (G17).
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.authenticate_request(&request)?;
        let mut builder = query.into_builder();
        for (catalog, schema, table) in self.host.list_catalog_tables() {
            builder
                .append(&catalog, &schema, &table, "TABLE", &Schema::empty())
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        let schema = builder.schema();
        let batch = builder
            .build()
            .map_err(|e| Status::internal(e.to_string()))?;
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(futures::future::ready(Ok::<
                _,
                arrow_flight::error::FlightError,
            >(
                batch
            ))))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    /// Typed Krishiv `DoAction` handler (B3, D2).
    ///
    /// The legacy comment-encoded streaming control plane is still served by
    /// `do_get_statement`; new clients ship structured payloads through
    /// `do_action` using the [`krishiv_runtime::KrishivFlightAction`] envelope.
    async fn do_action_fallback(
        &self,
        request: Request<arrow_flight::Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        self.authenticate_request(&request)?;
        use krishiv_runtime::KrishivFlightAction;
        use krishiv_runtime::flight_action::strip_action_type;

        let action = request.into_inner();
        let action_type = action.r#type.clone();
        let Some(_tag) = strip_action_type(&action_type) else {
            return Err(Status::invalid_argument(format!(
                "unrecognized action type {action_type}"
            )));
        };

        let parsed = KrishivFlightAction::from_action_body(&action.body)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let response_body = self
            .handle_krishiv_action(parsed)
            .await
            .map_err(|e| match e {
                KrishivActionError::Status(status) => status,
                KrishivActionError::Other(msg) => Status::internal(msg),
            })?;
        let result = arrow_flight::Result {
            body: bytes::Bytes::from(response_body),
        };
        let stream: <Self as FlightService>::DoActionStream =
            Box::pin(stream::iter(vec![Ok(result)]));
        Ok(Response::new(stream))
    }

    async fn list_custom_actions(&self) -> Option<Vec<Result<arrow_flight::ActionType, Status>>> {
        use krishiv_runtime::flight_action::{action_type as at, tags};
        // H-24 (audit): the prior list omitted REGISTER_KAFKA_SOURCE,
        // CANCEL_OPERATION, and GET_OPERATION_PROGRESS, even though the
        // service handles them. Standards-compliant Flight-SQL clients
        // discover server actions via `list_actions`; clients that rely
        // on that discoverability could not find these three. We now
        // advertise all eleven.
        Some(
            [
                tags::REGISTER_PARQUET,
                tags::CONTINUOUS_REGISTER,
                tags::CONTINUOUS_PUSH,
                tags::CONTINUOUS_DRAIN,
                tags::BOUNDED_WINDOW,
                tags::EXPLAIN,
                tags::EXECUTE_PLAN,
                tags::BATCH_SQL,
                tags::BATCH_SQL_SINK,
                tags::REGISTER_KAFKA_SOURCE,
                tags::CANCEL_OPERATION,
                tags::GET_OPERATION_PROGRESS,
            ]
            .iter()
            .map(|tag| {
                Ok(arrow_flight::ActionType {
                    r#type: at(tag),
                    description: format!("Krishiv {tag} action"),
                })
            })
            .collect(),
        )
    }
}

/// Build a gRPC `FlightServiceServer` wrapping `KrishivFlightSqlService`.
///
/// **Beta API**: may change between minor releases.
pub fn make_flight_sql_server()
-> Result<arrow_flight::flight_service_server::FlightServiceServer<KrishivFlightSqlService>, String>
{
    let service = KrishivFlightSqlService::with_host(
        FlightExecutionHost::from_env().map_err(|e| e.to_string())?,
    );
    Ok(
        arrow_flight::flight_service_server::FlightServiceServer::new(
            configure_flight_auth_from_env(service)?,
        ),
    )
}

/// Attach auth from `KRISHIV_API_KEYS` when configured; fail in production when absent.
fn configure_flight_auth_from_env(
    service: KrishivFlightSqlService,
) -> Result<KrishivFlightSqlService, String> {
    match auth_provider_from_env() {
        Ok(Some(auth)) => Ok(service.with_auth(auth)),
        Ok(None) => Ok(service),
        Err(message) => {
            if krishiv_common::profile_requires_authenticated_flight(
                krishiv_common::resolve_durability_profile(),
            ) {
                Err(message)
            } else {
                tracing::warn!(target: "krishiv_flight_sql", "{message}; serving anonymously (dev only)");
                Ok(service)
            }
        }
    }
}

fn auth_provider_from_env() -> Result<Option<Arc<dyn AuthProvider>>, String> {
    let raw = match std::env::var("KRISHIV_API_KEYS") {
        Ok(v) if !v.trim().is_empty() => v,
        _ if krishiv_common::profile_requires_authenticated_flight(
            krishiv_common::resolve_durability_profile(),
        ) =>
        {
            return Err(String::from(
                "KRISHIV_API_KEYS is required under durable profiles (format: key1=user,...)",
            ));
        }
        _ => return Ok(None),
    };
    let mut map = std::collections::HashMap::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (key, subject) = part
            .split_once('=')
            .ok_or_else(|| format!("invalid KRISHIV_API_KEYS entry: {part}"))?;
        map.insert(key.trim().to_owned(), subject.trim().to_owned());
    }
    if map.is_empty() {
        return Err(String::from("KRISHIV_API_KEYS must list at least one key"));
    }
    Ok(Some(Arc::new(StaticApiKeyAuthProvider::new(map))))
}

/// Run the Arrow Flight SQL server with a pre-built execution host and a bound listener.
///
/// Used by the coordinator to start a co-located Flight SQL sidecar via
/// `spawn_coordinator_sidecars`. The listener is bound by the caller before the
/// tokio task starts so bind errors surface immediately rather than inside a spawned task.
pub async fn run_flight_server_with_host(
    host: FlightExecutionHost,
    listener: tokio::net::TcpListener,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tokio_stream::wrappers::TcpListenerStream;

    #[cfg(feature = "rest-catalog")]
    host.register_rest_catalog_from_env().await?;
    let service = KrishivFlightSqlService::with_host(host);
    let service = configure_flight_auth_from_env(service)?;
    let server = arrow_flight::flight_service_server::FlightServiceServer::new(service);
    tonic::transport::Server::builder()
        .add_service(server)
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;
    Ok(())
}

/// Run the Arrow Flight SQL server (env `KRISHIV_FLIGHT_ADDR`, default `127.0.0.1:2003`).
pub async fn run_flight_server_from_env() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: std::net::SocketAddr = std::env::var("KRISHIV_FLIGHT_ADDR")
        .unwrap_or_else(|_| String::from("127.0.0.1:2003"))
        .parse()?;
    run_flight_server(addr).await
}

/// Run the Arrow Flight SQL server on `addr`.
pub async fn run_flight_server(
    addr: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!(addr = %addr, "krishiv-flight-server listening");
    let host = FlightExecutionHost::from_env()?;
    #[cfg(feature = "rest-catalog")]
    host.register_rest_catalog_from_env().await?;
    let service = configure_flight_auth_from_env(KrishivFlightSqlService::with_host(host))?;
    let server = arrow_flight::flight_service_server::FlightServiceServer::new(service);
    tonic::transport::Server::builder()
        .add_service(server)
        .serve(addr)
        .await?;
    Ok(())
}

impl KrishivFlightSqlService {
    /// Dispatch a typed Krishiv DoAction into the execution host (B3, D2).
    ///
    /// The host encapsulates InProcess vs Coordinator dispatch — the action
    /// handler just calls host methods without checking the backend variant.
    async fn handle_krishiv_action(
        &self,
        action: krishiv_runtime::KrishivFlightAction,
    ) -> Result<Vec<u8>, KrishivActionError> {
        use krishiv_runtime::KrishivFlightAction as A;

        match action {
            A::RegisterParquet(body) => {
                // Update the host's client-side catalog.
                self.host.register_parquet(&body.table, body.path);
                Ok(Vec::new())
            }
            A::ContinuousRegister(body) => {
                self.host
                    .register_continuous_stream(&body.job_id, &body.spec)
                    .await
                    .map_err(KrishivActionError::Status)?;
                Ok(Vec::new())
            }
            A::ContinuousPush(body) => {
                let batches = krishiv_runtime::decode_batches(&body.batches_b64)
                    .map_err(|e| KrishivActionError::Other(e.to_string()))?;
                self.host
                    .push_continuous_input(&body.job_id, batches)
                    .await
                    .map_err(KrishivActionError::Status)?;
                Ok(Vec::new())
            }
            A::ContinuousDrain(body) => {
                let batches = self
                    .host
                    .drain_continuous_stream(&body.job_id)
                    .await
                    .map_err(KrishivActionError::Status)?;
                encode_batches_ipc(&batches)
            }
            A::BoundedWindow(body) => {
                let input_batches = krishiv_runtime::decode_batches(&body.batches_b64)
                    .map_err(|e| KrishivActionError::Other(e.to_string()))?;
                let result = self
                    .host
                    .execute_bounded_window(&body.topic, &body.spec, input_batches)
                    .await
                    .map_err(KrishivActionError::Status)?;
                encode_batches_ipc(&result)
            }
            A::Explain(body) => {
                let text = self
                    .host
                    .explain_sql_query(&body.sql)
                    .map_err(KrishivActionError::Status)?;
                Ok(text.into_bytes())
            }
            A::ExecutePlan(body) => {
                let plan = body
                    .to_plan()
                    .map_err(|e| KrishivActionError::Other(e.to_string()))?;
                // For both backends, route ExecutePlan through execute_sql (handles
                // streaming plans by registering them as continuous jobs).
                let sql = krishiv_runtime::flight_client::plan_to_sql(&plan);
                if plan.kind() == krishiv_plan::ExecutionKind::Streaming {
                    let spec = krishiv_runtime::streaming_spec_from_plan(&plan)
                        .map_err(|e| KrishivActionError::Other(e.to_string()))?;
                    let job_id = plan.name().to_string();
                    self.host
                        .register_continuous_stream(
                            &job_id,
                            &krishiv_plan::window::WindowExecutionSpec::from(&spec),
                        )
                        .await
                        .map_err(KrishivActionError::Status)?;
                    return Ok(Vec::new());
                }
                let _ = self
                    .host
                    .execute_sql(&sql)
                    .await
                    .map_err(KrishivActionError::Status)?;
                Ok(Vec::new())
            }
            A::BatchSql(body) => {
                // Convert BatchSqlTable entries to BatchSqlInlineTable.
                // For InProcess backend the ipc_b64 can be empty (path-based tables
                // are already registered in catalog). For Coordinator backend the
                // client is expected to pass IPC bytes. We use encode_batch_sql to
                // produce inline IPC via the existing protocol path for InProcess,
                // and pass the body.tables directly for Coordinator.
                use krishiv_scheduler::BatchSqlInlineTable;
                let inline_tables: Vec<BatchSqlInlineTable> = body
                    .tables
                    .iter()
                    .map(|t| BatchSqlInlineTable {
                        table_name: t.table_name.clone(),
                        // The distributed client inlines parquet data as IPC so
                        // executors need no shared filesystem; empty falls back to
                        // catalog/path resolution (single-node / InProcess backend).
                        ipc_b64: t.ipc_b64.clone(),
                    })
                    .collect();
                let batches = if body.is_streaming {
                    // Streaming queries go through execute_sql to classify properly.
                    let sql = format!(
                        "-- krishiv:streaming=true\n{}",
                        krishiv_runtime::flight_protocol::encode_batch_sql(
                            &body.query,
                            &body.tables
                        )
                    );
                    self.host
                        .execute_sql(&sql)
                        .await
                        .map_err(KrishivActionError::Status)?
                } else {
                    self.host
                        .execute_batch_sql(&body.query, &inline_tables)
                        .await
                        .map_err(KrishivActionError::Status)?
                };
                encode_batches_ipc(&batches)
            }
            A::BatchSqlSink(body) => {
                // Phase 2.3 distributed write: the result is committed through
                // the staged sink contract instead of being returned inline.
                use krishiv_scheduler::BatchSqlInlineTable;
                let inline_tables: Vec<BatchSqlInlineTable> = body
                    .tables
                    .iter()
                    .map(|t| BatchSqlInlineTable {
                        table_name: t.table_name.clone(),
                        ipc_b64: t.ipc_b64.clone(),
                    })
                    .collect();
                self.host
                    .execute_batch_sql_sink(&body.query, &inline_tables, &body.sink_contract)
                    .await
                    .map_err(KrishivActionError::Status)?;
                Ok(Vec::new())
            }
            #[cfg(feature = "kafka")]
            A::RegisterKafkaSource(body) => {
                self.host
                    .register_kafka_source(
                        &body.name,
                        &body.schema_ipc_b64,
                        &body.bootstrap_servers,
                        &body.topic,
                        &body.group_id,
                    )
                    .map_err(|e| KrishivActionError::Other(e.to_string()))?;
                Ok(Vec::new())
            }
            #[cfg(not(feature = "kafka"))]
            A::RegisterKafkaSource(_) => Err(KrishivActionError::Other(
                "Kafka support not enabled; rebuild with --features kafka".into(),
            )),
            A::CancelOperation(body) => {
                self.host.cancel_operation(body.operation_id);
                Ok(Vec::new())
            }
            A::GetOperationProgress(body) => {
                let response = self
                    .host
                    .operation_progress(body.operation_id)
                    .map(|(rows_scanned, rows_emitted)| {
                        krishiv_runtime::flight_action::OperationProgressResponse {
                            rows_scanned,
                            rows_emitted,
                        }
                    })
                    .unwrap_or(krishiv_runtime::flight_action::OperationProgressResponse {
                        rows_scanned: 0,
                        rows_emitted: 0,
                    });
                response
                    .to_json_bytes()
                    .map_err(|e| KrishivActionError::Other(e.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod transaction_tests {
    use super::*;

    fn service() -> KrishivFlightSqlService {
        KrishivFlightSqlService::with_host(FlightExecutionHost::embedded().expect("embedded host"))
    }

    fn action_request() -> Request<arrow_flight::Action> {
        Request::new(arrow_flight::Action {
            r#type: String::new(),
            body: Default::default(),
        })
    }

    /// Regression test documenting the transaction bookkeeping gap this
    /// session's sync/async audit found: `do_get_statement` executes a
    /// statement immediately, for real, as soon as it is submitted — even
    /// while a transaction is open and un-committed. There is no staging to
    /// later commit or discard, so `Commit`/`Rollback` (see
    /// `do_action_end_transaction`) cannot change whether the statement's
    /// effects happened. This test pins that observable behavior so a future
    /// change that silently "fixes" it (or regresses it further) is noticed.
    #[tokio::test]
    async fn do_get_statement_executes_immediately_under_open_transaction() {
        use futures::StreamExt as _;

        let svc = service();
        let begin = svc
            .do_action_begin_transaction(ActionBeginTransactionRequest {}, action_request())
            .await
            .expect("begin_transaction");
        let txn_id = begin.transaction_id.to_vec();

        // Build the ticket the same way `get_flight_info_statement` does:
        // [4-byte big-endian txn_len][txn_id][query].
        let query = b"SELECT 1 AS n";
        let mut handle = Vec::with_capacity(4 + txn_id.len() + query.len());
        handle.extend_from_slice(&(txn_id.len() as u32).to_be_bytes());
        handle.extend_from_slice(&txn_id);
        handle.extend_from_slice(query);
        let ticket = TicketStatementQuery {
            statement_handle: handle.into(),
        };

        // The statement runs for real right now, under an open (not yet
        // committed or rolled back) transaction.
        let mut stream = svc
            .do_get_statement(ticket, Request::new(Ticket::new(Vec::new())))
            .await
            .expect("statement executes under an open transaction")
            .into_inner();
        let mut saw_batch = false;
        while let Some(chunk) = stream.next().await {
            chunk.expect("flight data chunk");
            saw_batch = true;
        }
        assert!(
            saw_batch,
            "do_get_statement must actually execute the query"
        );

        // The bookkeeping counter now reflects the one executed statement —
        // this is the only visible trace of "the transaction," since nothing
        // was staged.
        let count = svc
            .transactions
            .lock()
            .await
            .get(std::str::from_utf8(&txn_id).unwrap())
            .expect("transaction is still open")
            .statement_count;
        assert_eq!(count, 1);

        // Rollback succeeds — but it does not, and cannot, undo the SELECT
        // that already executed and returned its result to the caller.
        svc.do_action_end_transaction(
            ActionEndTransactionRequest {
                transaction_id: txn_id.clone().into(),
                action: EndTransaction::Rollback as i32,
            },
            action_request(),
        )
        .await
        .expect("rollback succeeds even though the statement already ran");
    }

    #[tokio::test]
    async fn transaction_id_is_retired_after_end_transaction() {
        let svc = service();
        let begin = svc
            .do_action_begin_transaction(ActionBeginTransactionRequest {}, action_request())
            .await
            .expect("begin_transaction");
        let txn_id = begin.transaction_id.to_vec();

        svc.do_action_end_transaction(
            ActionEndTransactionRequest {
                transaction_id: txn_id.clone().into(),
                action: EndTransaction::Commit as i32,
            },
            action_request(),
        )
        .await
        .expect("end_transaction commit");

        let reused = svc.validate_transaction_id(Some(&txn_id), None).await;
        assert!(
            reused.is_err(),
            "a transaction id must not be reusable after EndTransaction"
        );
    }

    #[tokio::test]
    async fn end_transaction_rejects_unknown_id() {
        let svc = service();
        let result = svc
            .do_action_end_transaction(
                ActionEndTransactionRequest {
                    transaction_id: b"not-a-real-id".to_vec().into(),
                    action: EndTransaction::Commit as i32,
                },
                action_request(),
            )
            .await;
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod metadata_rpc_tests {
    use super::*;
    use futures::StreamExt as _;

    fn service() -> KrishivFlightSqlService {
        KrishivFlightSqlService::with_host(FlightExecutionHost::embedded().expect("embedded host"))
    }

    fn descriptor_request() -> Request<FlightDescriptor> {
        Request::new(FlightDescriptor::new_cmd(Vec::new()))
    }

    async fn collect_batches(
        stream: Response<<KrishivFlightSqlService as FlightService>::DoGetStream>,
    ) -> Vec<RecordBatch> {
        let mut inner = stream.into_inner();
        let mut data = Vec::new();
        while let Some(chunk) = inner.next().await {
            data.push(chunk.expect("flight data chunk"));
        }
        arrow_flight::utils::flight_data_to_batches(&data).expect("decode flight data")
    }

    /// G1a regression: `GetFlightInfo(statement)` must leave the schema
    /// UNSET (empty bytes = unknown), not declare an explicit zero-field
    /// schema. Strict clients (ADBC; the JDBC driver's validation) reject a
    /// declared-empty schema as inconsistent with the DoGet data stream —
    /// this broke every ADBC query including `SELECT 1`.
    #[tokio::test]
    async fn get_flight_info_statement_leaves_schema_unset() {
        let svc = service();
        let info = svc
            .get_flight_info_statement(
                CommandStatementQuery {
                    query: "SELECT 1 AS n".to_string(),
                    transaction_id: None,
                },
                descriptor_request(),
            )
            .await
            .expect("flight info")
            .into_inner();
        assert!(
            info.schema.is_empty(),
            "FlightInfo.schema must be empty bytes (unknown), got {} bytes",
            info.schema.len()
        );
        assert_eq!(info.endpoint.len(), 1, "one endpoint with the ticket");
    }

    /// SEC-2 (Phase 63) regression: the fail-closed default-deny —
    /// auth-configured-without-a-policy-engine — must be enforced on EVERY
    /// path, not just statements. Before the fix, `do_action_fallback` and the
    /// prepared-statement handlers authenticated without ever consulting the
    /// default-deny guard, so an operator who turned on auth but forgot the
    /// policy engine ran actions and prepared updates unauthorized. Folding
    /// the guard into `authenticate_request` closes the asymmetry; it fires
    /// before token validation, so even a credential-less request is denied.
    #[tokio::test]
    async fn sec2_auth_without_policy_default_denies_actions_and_prepared_paths() {
        use std::collections::HashMap;

        let mut keys = HashMap::new();
        keys.insert("k1".to_owned(), "alice".to_owned());
        let auth: Arc<dyn AuthProvider> = Arc::new(StaticApiKeyAuthProvider::new(keys));
        // Auth attached, policy engine deliberately absent.
        let svc = KrishivFlightSqlService::with_host(
            FlightExecutionHost::embedded().expect("embedded host"),
        )
        .with_auth(auth);

        // DoAction — the formerly-asymmetric action path. Its Ok variant is a
        // non-Debug stream Response, so match instead of expect_err.
        match svc
            .do_action_fallback(Request::new(arrow_flight::Action {
                r#type: String::new(),
                body: Default::default(),
            }))
            .await
        {
            Err(status) => assert_eq!(status.code(), tonic::Code::PermissionDenied),
            Ok(_) => panic!("DoAction must default-deny when auth has no policy"),
        }

        // Prepared-statement info — the formerly-asymmetric prepared path.
        let prepared_err = svc
            .get_flight_info_prepared_statement(
                CommandPreparedStatementQuery {
                    prepared_statement_handle: b"h".to_vec().into(),
                },
                descriptor_request(),
            )
            .await
            .expect_err("prepared-statement info must default-deny when auth has no policy");
        assert_eq!(prepared_err.code(), tonic::Code::PermissionDenied);

        // Statement path — always had the guard; behaves identically now that
        // the single enforcement point lives in authenticate_request.
        let stmt_err = svc
            .get_flight_info_statement(
                CommandStatementQuery {
                    query: "SELECT 1".to_string(),
                    transaction_id: None,
                },
                descriptor_request(),
            )
            .await
            .expect_err("statement info must default-deny when auth has no policy");
        assert_eq!(stmt_err.code(), tonic::Code::PermissionDenied);
    }

    /// G1b regression: `GetSqlInfo` is implemented (the stock JDBC driver
    /// calls it at connect time and fails the connection when it is
    /// unimplemented) and serves the server name/version rows.
    #[tokio::test]
    async fn sql_info_served_and_contains_server_name() {
        let svc = service();
        // FlightInfo advertises the SqlInfo result schema.
        let info = svc
            .get_flight_info_sql_info(CommandGetSqlInfo { info: vec![] }, descriptor_request())
            .await
            .expect("sql_info flight info")
            .into_inner();
        assert!(!info.schema.is_empty(), "SqlInfo schema is known up front");

        // Unfiltered DoGet returns the metadata rows.
        let batches = collect_batches(
            svc.do_get_sql_info(
                CommandGetSqlInfo { info: vec![] },
                Request::new(Ticket::new(Vec::new())),
            )
            .await
            .expect("do_get_sql_info"),
        )
        .await;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(rows >= 5, "expected several SqlInfo rows, got {rows}");

        // Filtered request returns exactly the requested id.
        let name_id = SqlInfo::FlightSqlServerName as u32;
        let filtered = collect_batches(
            svc.do_get_sql_info(
                CommandGetSqlInfo {
                    info: vec![name_id],
                },
                Request::new(Ticket::new(Vec::new())),
            )
            .await
            .expect("filtered do_get_sql_info"),
        )
        .await;
        let filtered_rows: usize = filtered.iter().map(|b| b.num_rows()).sum();
        assert_eq!(filtered_rows, 1, "exactly the requested SqlInfo id");
    }

    /// G1c regression: `GetCatalogs` is implemented and lists the single
    /// `krishiv` catalog, consistent with `GetDbSchemas`/`GetTables`.
    #[tokio::test]
    async fn catalogs_lists_krishiv() {
        let svc = service();
        let batches = collect_batches(
            svc.do_get_catalogs(CommandGetCatalogs {}, Request::new(Ticket::new(Vec::new())))
                .await
                .expect("do_get_catalogs"),
        )
        .await;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("catalog_name column");
        assert_eq!(names.value(0), "krishiv");
    }

    #[tokio::test]
    async fn table_types_lists_table() {
        let svc = service();
        let batches = collect_batches(
            svc.do_get_table_types(
                CommandGetTableTypes {},
                Request::new(Ticket::new(Vec::new())),
            )
            .await
            .expect("do_get_table_types"),
        )
        .await;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        let types = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("table_type column");
        assert_eq!(types.value(0), "TABLE");
    }
}

#[cfg(test)]
mod prepared_statement_schema_tests {
    use super::*;

    fn service() -> KrishivFlightSqlService {
        KrishivFlightSqlService::with_host(FlightExecutionHost::embedded().expect("embedded host"))
    }

    fn action_request() -> Request<arrow_flight::Action> {
        Request::new(arrow_flight::Action {
            r#type: String::new(),
            body: Default::default(),
        })
    }

    /// G1 regression: a prepared SELECT carries a non-empty
    /// `dataset_schema`. The JDBC driver routes query-vs-update on exactly
    /// this — an empty schema sent every SELECT down the (previously
    /// unimplemented) update path.
    #[tokio::test]
    async fn prepared_select_has_dataset_schema() {
        let svc = service();
        let result = svc
            .do_action_create_prepared_statement(
                ActionCreatePreparedStatementRequest {
                    query: "SELECT 1 AS n, 'x' AS s".to_string(),
                    transaction_id: None,
                },
                action_request(),
            )
            .await
            .expect("create prepared statement");
        assert!(
            !result.dataset_schema.is_empty(),
            "SELECT must carry a planned dataset_schema"
        );
    }

    /// Statements the host cannot plan without side effects (DDL) keep the
    /// honest empty ("unknown") dataset_schema — never a fabricated one.
    #[tokio::test]
    async fn prepared_ddl_keeps_unknown_dataset_schema() {
        let svc = service();
        let result = svc
            .do_action_create_prepared_statement(
                ActionCreatePreparedStatementRequest {
                    query: "CREATE TABLE t AS SELECT 1 AS v".to_string(),
                    transaction_id: None,
                },
                action_request(),
            )
            .await
            .expect("create prepared statement");
        assert!(
            result.dataset_schema.is_empty(),
            "DDL must not fabricate a dataset schema (and must not execute at prepare time)"
        );
    }
}
