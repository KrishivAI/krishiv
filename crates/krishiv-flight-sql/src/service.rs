use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionBeginTransactionRequest, ActionBeginTransactionResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionEndTransactionRequest, CommandGetDbSchemas,
    CommandGetTables, CommandPreparedStatementQuery, CommandStatementQuery,
    DoPutPreparedStatementResult, EndTransaction, ProstMessageExt, SqlInfo, TicketStatementQuery,
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
    schema_to_ipc_bytes, substitute_sql_params,
};
use crate::host::FlightExecutionHost;

/// Env var controlling how many queries may execute concurrently through the
/// Flight SQL ingress. Requests above the limit receive `Status::resource_exhausted`.
/// Default: 256.  Set to `"0"` to disable the cap entirely.
const FLIGHT_MAX_CONCURRENT_QUERIES_ENV: &str = "KRISHIV_FLIGHT_MAX_CONCURRENT_QUERIES";
/// Default cap on simultaneous Flight SQL query executions.
const DEFAULT_FLIGHT_MAX_CONCURRENT_QUERIES: usize = 256;

/// Maximum number of active transactions before rejecting new `BeginTransaction` requests.
const MAX_TRANSACTIONS: usize = 10_000;
/// Transaction entries older than this are evicted on each `BeginTransaction` sweep.
const TRANSACTION_TTL: Duration = Duration::from_secs(300);

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
    /// Maps txn_id -> (subject, created_at).  Entries older than `TRANSACTION_TTL`
    /// are swept on each new transaction request.  Capped at `MAX_TRANSACTIONS`.
    transactions: Arc<tokio::sync::Mutex<HashMap<String, (String, Instant)>>>,
    /// Semaphore that caps the number of queries executing concurrently through
    /// the Flight ingress. `None` means no cap.
    inflight_queries: Option<Arc<tokio::sync::Semaphore>>,
}

const FLIGHT_PREPARED_STMT_CAPACITY_ENV: &str = "KRISHIV_FLIGHT_PREPARED_STMT_CAPACITY";
const DEFAULT_PREPARED_STMT_CAPACITY: usize = 128;

fn read_prepared_stmt_capacity() -> std::num::NonZeroUsize {
    let n = std::env::var(FLIGHT_PREPARED_STMT_CAPACITY_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_PREPARED_STMT_CAPACITY);
    std::num::NonZeroUsize::new(n.max(1)).unwrap()
}

fn read_max_concurrent_queries() -> Option<usize> {
    let n = std::env::var(FLIGHT_MAX_CONCURRENT_QUERIES_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_FLIGHT_MAX_CONCURRENT_QUERIES);
    if n == 0 { None } else { Some(n) }
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
            Some((owner, created)) if subject.is_none_or(|s| s == owner) => {
                if created.elapsed() >= TRANSACTION_TTL {
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
        // Default deny: if auth is configured but no policy engine is set,
        // operators who configure authentication expect policy enforcement too.
        if self.auth.is_some() && self.policy.is_none() {
            return Err(Status::permission_denied(
                "auth is configured but no policy engine is set; \
                 configure a PolicyHook or use an unauthenticated service",
            ));
        }

        // Authenticate if an auth provider is configured.
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
        let info = FlightInfo::new()
            .try_with_schema(&Schema::empty())
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(endpoint);
        Ok(Response::new(info))
    }

    // Execute SQL and stream results
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // Default deny: if auth is configured but no policy engine is set,
        // operators who configure authentication expect policy enforcement too.
        if self.auth.is_some() && self.policy.is_none() {
            return Err(Status::permission_denied(
                "auth is configured but no policy engine is set; \
                 configure a PolicyHook or use an unauthenticated service",
            ));
        }

        // Authenticate if an auth provider is configured.
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
        let (transaction_id, query_bytes): (Option<Vec<u8>>, &[u8]) = if handle.len() >= 4 {
            let txn_len = u32::from_be_bytes([handle[0], handle[1], handle[2], handle[3]]) as usize;
            let txn_end = 4 + txn_len;
            if txn_len > 0 && handle.len() >= txn_end {
                (Some(handle[4..txn_end].to_vec()), &handle[txn_end..])
            } else if txn_len == 0 {
                (None, &handle[4..])
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

        let schema: Arc<Schema> = if batches.is_empty() {
            Arc::new(Schema::empty())
        } else {
            batches[0].schema()
        };

        let flight_data = batches_to_flight_data(&schema, batches)
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .map(Ok::<FlightData, Status>);

        let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
            Box::pin(stream::iter(flight_data));
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
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let subject = self.authenticate_request(&request)?;
        let subject_key = subject.as_deref().unwrap_or("__anon__").to_owned();
        let handle = Uuid::new_v4().to_string();
        let n_params = count_sql_params(&query.query);
        let param_schema = build_param_schema(n_params);
        let parameter_schema = schema_to_ipc_bytes(&param_schema)?;
        {
            let mut map = self.prepared_statements.lock().await;
            let cache = map
                .entry(subject_key)
                .or_insert_with(|| lru::LruCache::new(read_prepared_stmt_capacity()));
            cache.put(handle.clone(), query.query);
        }
        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into_bytes().into(),
            parameter_schema: parameter_schema.into(),
            ..Default::default()
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

        // Delegate to the existing statement query path.
        let cmd = CommandStatementQuery {
            query: sql,
            transaction_id: None,
        };
        self.get_flight_info_statement(cmd, request).await
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
        txns.retain(|_id, (_owner, created)| now.duration_since(*created) < TRANSACTION_TTL);
        if txns.len() >= MAX_TRANSACTIONS {
            return Err(Status::resource_exhausted(format!(
                "transaction limit reached ({MAX_TRANSACTIONS}); retry after existing transactions expire"
            )));
        }
        txns.insert(transaction_id.clone(), (subject_key, now));
        drop(txns);
        Ok(ActionBeginTransactionResult {
            transaction_id: transaction_id.into_bytes().into(),
        })
    }

    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        let subject = self.authenticate_request(&request)?;
        let subject_key = subject.as_deref().unwrap_or("__anon__");
        let transaction_id = std::str::from_utf8(&query.transaction_id)
            .map_err(|_| Status::invalid_argument("invalid transaction id encoding"))?;
        {
            let mut txns = self.transactions.lock().await;
            match txns.get(transaction_id) {
                None => {
                    return Err(Status::invalid_argument(format!(
                        "unknown transaction id: {transaction_id}"
                    )));
                }
                Some((owner, _created)) if owner != subject_key => {
                    return Err(Status::permission_denied(format!(
                        "transaction id {transaction_id} does not belong to this subject"
                    )));
                }
                Some(_) => {
                    txns.remove(transaction_id);
                }
            }
        }
        match EndTransaction::try_from(query.action)
            .map_err(|_| Status::invalid_argument("invalid EndTransaction action"))?
        {
            EndTransaction::Commit | EndTransaction::Rollback => Ok(()),
            EndTransaction::Unspecified => Err(Status::invalid_argument(
                "EndTransaction action must be Commit or Rollback",
            )),
        }
    }

    // G17: Catalog introspection

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
    let server = make_flight_sql_server()?;
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
                        ipc_b64: String::new(), // path-based: coordinator will resolve via catalog
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
                        ipc_b64: String::new(), // path-based: resolved via catalog
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
