#![forbid(unsafe_code)]
//! Flight SQL service — thin adapter over the Krishiv Session API.
//! **Beta API**: may change between minor releases.

mod host;

use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandPreparedStatementQuery, CommandStatementQuery,
    ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    Ticket, flight_service_server::FlightService,
};
use futures::{Stream, stream};
use prost::Message as _; // brings encode_to_vec() into scope
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use krishiv_plan::governance::{AuthProvider, PolicyHook, StaticApiKeyAuthProvider};

pub use host::FlightExecutionHost;

/// **Beta API**: may change between minor releases.
#[derive(Clone)]
pub struct KrishivFlightSqlService {
    auth: Option<Arc<dyn AuthProvider>>,
    policy: Option<Arc<dyn PolicyHook>>,
    host: FlightExecutionHost,
    /// LRU cache of opaque handle (UUID string) → SQL text for prepared statements.
    prepared_statements: Arc<tokio::sync::Mutex<lru::LruCache<String, String>>>,
}

const PREPARED_STMT_CAPACITY: std::num::NonZeroUsize = match std::num::NonZeroUsize::new(128) {
    Some(n) => n,
    None => panic!("capacity must be non-zero"),
};

impl std::fmt::Debug for KrishivFlightSqlService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KrishivFlightSqlService")
            .field("auth", &self.auth.is_some())
            .field("policy", &self.policy.is_some())
            .finish_non_exhaustive()
    }
}

impl KrishivFlightSqlService {
    /// Create a new `KrishivFlightSqlService` with a shared server-side cluster.
    pub fn new() -> Result<Self, Status> {
        Ok(Self {
            auth: None,
            policy: None,
            host: FlightExecutionHost::from_env()?,
            prepared_statements: Arc::new(tokio::sync::Mutex::new(lru::LruCache::new(
                PREPARED_STMT_CAPACITY,
            ))),
        })
    }

    /// Attach a pre-built execution host (tests / custom wiring).
    pub fn with_host(host: FlightExecutionHost) -> Self {
        Self {
            auth: None,
            policy: None,
            host,
            prepared_statements: Arc::new(tokio::sync::Mutex::new(lru::LruCache::new(
                PREPARED_STMT_CAPACITY,
            ))),
        }
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

    #[allow(clippy::result_large_err)]
    fn bearer_token<B>(&self, req: &Request<B>) -> Result<Option<String>, Status> {
        let Some(_auth) = &self.auth else {
            return Ok(None);
        };
        req.metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(str::to_owned)
            .map(Some)
            .ok_or_else(|| Status::unauthenticated("missing Bearer token"))
    }

    /// Validate the `authorization: Bearer <token>` header.
    ///
    /// Returns `Ok(Some(subject))` when auth is configured and the token is
    /// valid, `Ok(None)` when no [`AuthProvider`] is attached, and
    /// `Err(Status::unauthenticated(...))` when the token is missing or invalid.
    #[allow(clippy::result_large_err)]
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
    /// Extracts the table name from a simple `SELECT ... FROM <table>` pattern.
    /// When no policy is configured, all access is allowed.
    #[allow(clippy::result_large_err)]
    fn check_table_access(&self, query: &str) -> Result<(), Status> {
        let Some(policy) = &self.policy else {
            return Ok(());
        };
        // Simple heuristic: extract table name after FROM keyword.
        if let Some(table_name) = extract_from_table(query) {
            if !policy.check_table_access(&table_name) {
                return Err(Status::permission_denied(format!(
                    "access denied to table: {table_name}"
                )));
            }
        }
        Ok(())
    }
}

/// Simple heuristic to extract the table name from `FROM <table>` in a query.
fn extract_from_table(query: &str) -> Option<String> {
    let upper = query.to_uppercase();
    let from_pos = upper.find(" FROM ")?;
    let rest = query[from_pos + 6..].trim_start();
    let end = rest
        .find(|c: char| c.is_whitespace() || c == ';' || c == ')')
        .unwrap_or(rest.len());
    let table = rest[..end].trim().to_string();
    if table.is_empty() { None } else { Some(table) }
}

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
        self.authenticate_request(&request)?;

        let ticket_query = TicketStatementQuery {
            statement_handle: query.query.into_bytes().into(),
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
        self.authenticate_request(&request)?;

        let query = std::str::from_utf8(&ticket.statement_handle)
            .map_err(|e| Status::invalid_argument(format!("invalid query encoding: {e}")))?;

        // Check table access if a policy is configured.
        self.check_table_access(query)?;

        let batches = self
            .host
            .execute_sql(query)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

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
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        self.authenticate_request(&request)?;
        let handle = Uuid::new_v4().to_string();
        self.prepared_statements
            .lock()
            .await
            .put(handle.clone(), query.query);
        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into_bytes().into(),
            ..Default::default()
        })
    }

    /// Return [`FlightInfo`] for a prepared statement (used by clients that
    /// call `GetFlightInfo` before `DoGet`).
    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let handle = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| {
                Status::invalid_argument(format!("invalid prepared statement handle encoding: {e}"))
            })?
            .to_owned();

        let sql = {
            let mut map = self.prepared_statements.lock().await;
            map.get(&handle)
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
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let handle = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| {
                Status::invalid_argument(format!("invalid prepared statement handle encoding: {e}"))
            })?
            .to_owned();

        let sql = {
            let mut map = self.prepared_statements.lock().await;
            map.get(&handle)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("unknown prepared statement: {handle}")))?
        };

        // Delegate to the existing statement execution path.
        let ticket = TicketStatementQuery {
            statement_handle: sql.into_bytes().into(),
        };
        self.do_get_statement(ticket, request).await
    }

    /// Close (drop) a previously created prepared statement.
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        self.authenticate_request(&request)?;
        let handle = std::str::from_utf8(&query.prepared_statement_handle)
            .map_err(|e| {
                Status::invalid_argument(format!("invalid prepared statement handle encoding: {e}"))
            })?
            .to_owned();
        self.prepared_statements.lock().await.pop(&handle);
        Ok(())
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

/// Error type for Krishiv DoAction handlers.
enum KrishivActionError {
    Status(Status),
    Other(String),
}

impl From<Status> for KrishivActionError {
    fn from(s: Status) -> Self {
        Self::Status(s)
    }
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
                    let mut sql = krishiv_runtime::flight_protocol::encode_batch_sql(
                        &body.query,
                        &body.tables,
                    );
                    sql = format!("-- krishiv:streaming=true\n{sql}");
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
        }
    }
}

fn encode_batches_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, KrishivActionError> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| KrishivActionError::Other(format!("ipc encode: {e}")))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| KrishivActionError::Other(format!("ipc write: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| KrishivActionError::Other(format!("ipc finish: {e}")))?;
    }
    Ok(buf)
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

/// Run the Arrow Flight SQL server (env `KRISHIV_FLIGHT_ADDR`, default `127.0.0.1:50051`).
pub async fn run_flight_server_from_env() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: std::net::SocketAddr = std::env::var("KRISHIV_FLIGHT_ADDR")
        .unwrap_or_else(|_| String::from("127.0.0.1:50051"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use krishiv_plan::governance::{AllowAllPolicyHook, PolicyHook, StaticApiKeyAuthProvider};
    use tonic::metadata::MetadataValue;

    fn make_auth_service() -> KrishivFlightSqlService {
        let mut keys = std::collections::HashMap::new();
        keys.insert("secret-key".to_string(), "alice".to_string());
        let auth = Arc::new(StaticApiKeyAuthProvider::new(keys));
        KrishivFlightSqlService::new()
            .expect("flight host")
            .with_auth(auth)
    }

    struct DenySecretPolicy;

    impl PolicyHook for DenySecretPolicy {
        fn check_table_access(&self, table_name: &str) -> bool {
            table_name != "secret"
        }
    }

    fn make_auth_policy_service() -> KrishivFlightSqlService {
        make_auth_service().with_policy(Arc::new(DenySecretPolicy))
    }

    #[test]
    fn service_is_default_constructible() {
        let _ = KrishivFlightSqlService::new().expect("flight host");
    }

    #[test]
    fn make_session_returns_ok() {
        let _ = KrishivFlightSqlService::new().expect("flight host");
    }

    #[test]
    fn make_flight_sql_server_compiles() {
        let _ = make_flight_sql_server().expect("make flight sql server");
    }

    #[tokio::test]
    async fn get_flight_info_encodes_query_into_ticket() {
        let svc = KrishivFlightSqlService::new().expect("flight host");
        let cmd = CommandStatementQuery {
            query: "SELECT 42".to_string(),
            transaction_id: None,
        };
        let descriptor = FlightDescriptor::new_cmd(vec![]);
        let resp = svc
            .get_flight_info_statement(cmd, Request::new(descriptor))
            .await
            .unwrap();
        let info = resp.into_inner();
        assert_eq!(info.endpoint.len(), 1);
        assert!(!info.endpoint[0].ticket.as_ref().unwrap().ticket.is_empty());
    }

    #[tokio::test]
    async fn do_get_statement_executes_select_1() {
        let svc = KrishivFlightSqlService::new().expect("flight host");
        let ticket = TicketStatementQuery {
            statement_handle: b"SELECT 1 AS n".to_vec().into(),
        };
        let resp = svc
            .do_get_statement(ticket, Request::new(Ticket::new(vec![])))
            .await
            .unwrap();
        let items: Vec<_> = resp.into_inner().collect().await;
        // At minimum a schema FlightData item is returned
        assert!(!items.is_empty());
        assert!(items[0].is_ok());
    }

    #[tokio::test]
    async fn do_action_explain_round_trip() {
        // B3/D2: the typed DoAction path returns the explain text as raw
        // bytes inside arrow_flight::Result.body — no SQL involved on the
        // wire, no comment-injection surface.
        use krishiv_runtime::{ExplainBody, KrishivFlightAction};

        let svc = KrishivFlightSqlService::new().expect("flight host");
        let action = KrishivFlightAction::Explain(ExplainBody {
            sql: "SELECT 1 AS n".into(),
        });
        let req = arrow_flight::Action {
            r#type: action.action_type(),
            body: action.to_action_body().unwrap().into(),
        };
        let resp = svc
            .do_action_fallback(Request::new(req))
            .await
            .expect("do_action_fallback");
        let parts: Vec<_> = resp.into_inner().collect().await;
        assert!(!parts.is_empty());
        let first = parts.into_iter().next().unwrap().unwrap();
        assert!(!first.body.is_empty());
        let text = std::str::from_utf8(&first.body).unwrap();
        // explain text comes from DataFusion; should at least include 'Projection' or similar.
        assert!(!text.is_empty());
    }

    #[tokio::test]
    async fn do_action_rejects_unknown_type() {
        let svc = KrishivFlightSqlService::new().expect("flight host");
        let req = arrow_flight::Action {
            r#type: "unknown.action".to_string(),
            body: bytes::Bytes::new(),
        };
        let result = svc.do_action_fallback(Request::new(req)).await;
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn list_custom_actions_lists_krishiv_types() {
        let svc = KrishivFlightSqlService::new().expect("flight host");
        let listed = svc.list_custom_actions().await.expect("listed");
        assert!(listed.iter().any(|r| {
            r.as_ref()
                .map(|a| a.r#type == "krishiv.v1.explain")
                .unwrap_or(false)
        }));
    }

    #[tokio::test]
    async fn do_get_statement_invalid_utf8_returns_invalid_argument() {
        let svc = KrishivFlightSqlService::new().expect("flight host");
        let ticket = TicketStatementQuery {
            statement_handle: vec![0xFF, 0xFE].into(),
        };
        let result = svc
            .do_get_statement(ticket, Request::new(Ticket::new(vec![])))
            .await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // ── Auth tests ────────────────────────────────────────────────────────────

    // GAP-GV-03: when auth is configured without a policy engine the service
    // must return PermissionDenied before any token inspection.
    #[tokio::test]
    async fn auth_without_policy_is_denied() {
        // Service with auth but no policy — default deny must fire.
        let svc = make_auth_service();

        // do_get_statement: no token
        let ticket = TicketStatementQuery {
            statement_handle: b"SELECT 1".to_vec().into(),
        };
        let result = svc
            .do_get_statement(ticket, Request::new(Ticket::new(vec![])))
            .await;
        assert!(result.is_err(), "auth-without-policy must be denied");
        assert_eq!(
            result.err().unwrap().code(),
            tonic::Code::PermissionDenied,
            "auth-without-policy must return PermissionDenied"
        );

        // do_get_statement: valid token — still denied because no policy
        let ticket2 = TicketStatementQuery {
            statement_handle: b"SELECT 42".to_vec().into(),
        };
        let mut req2 = Request::new(Ticket::new(vec![]));
        req2.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer secret-key"),
        );
        let result2 = svc.do_get_statement(ticket2, req2).await;
        assert!(result2.is_err());
        assert_eq!(result2.err().unwrap().code(), tonic::Code::PermissionDenied);

        // get_flight_info_statement: valid token — still denied because no policy
        let cmd = CommandStatementQuery {
            query: "SELECT 1".to_string(),
            transaction_id: None,
        };
        let descriptor = FlightDescriptor::new_cmd(vec![]);
        let mut req3 = Request::new(descriptor);
        req3.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer secret-key"),
        );
        let result3 = svc.get_flight_info_statement(cmd, req3).await;
        assert!(result3.is_err());
        assert_eq!(result3.err().unwrap().code(), tonic::Code::PermissionDenied);
    }

    // Auth enforcement tests use auth+policy (the complete, non-deny-default config).
    #[tokio::test]
    async fn auth_required_rejects_missing_token_on_get_flight_info() {
        let svc = make_auth_policy_service();
        let cmd = CommandStatementQuery {
            query: "SELECT 1".to_string(),
            transaction_id: None,
        };
        let descriptor = FlightDescriptor::new_cmd(vec![]);
        // No authorization header — should be rejected.
        let result = svc
            .get_flight_info_statement(cmd, Request::new(descriptor))
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn auth_required_rejects_invalid_token_on_get_flight_info() {
        let svc = make_auth_policy_service();
        let cmd = CommandStatementQuery {
            query: "SELECT 1".to_string(),
            transaction_id: None,
        };
        let descriptor = FlightDescriptor::new_cmd(vec![]);
        let mut req = Request::new(descriptor);
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer wrong-key"),
        );
        let result = svc.get_flight_info_statement(cmd, req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn auth_required_accepts_valid_token_on_get_flight_info() {
        let svc = make_auth_policy_service();
        let cmd = CommandStatementQuery {
            query: "SELECT 1".to_string(),
            transaction_id: None,
        };
        let descriptor = FlightDescriptor::new_cmd(vec![]);
        let mut req = Request::new(descriptor);
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer secret-key"),
        );
        let result = svc.get_flight_info_statement(cmd, req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn auth_required_rejects_missing_token_on_do_get() {
        let svc = make_auth_policy_service();
        let ticket = TicketStatementQuery {
            statement_handle: b"SELECT 1".to_vec().into(),
        };
        // No authorization header.
        let result = svc
            .do_get_statement(ticket, Request::new(Ticket::new(vec![])))
            .await;
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn auth_required_rejects_invalid_token_on_do_get() {
        let svc = make_auth_policy_service();
        let ticket = TicketStatementQuery {
            statement_handle: b"SELECT 1".to_vec().into(),
        };
        let mut req = Request::new(Ticket::new(vec![]));
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer bad-key"),
        );
        let result = svc.do_get_statement(ticket, req).await;
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn auth_required_accepts_valid_token_on_do_get() {
        let svc = make_auth_policy_service();
        let ticket = TicketStatementQuery {
            statement_handle: b"SELECT 42 AS val".to_vec().into(),
        };
        let mut req = Request::new(Ticket::new(vec![]));
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer secret-key"),
        );
        let result = svc.do_get_statement(ticket, req).await;
        assert!(result.is_ok());
        let items: Vec<_> = result.unwrap().into_inner().collect().await;
        assert!(!items.is_empty());
        assert!(items[0].is_ok());
    }

    #[tokio::test]
    async fn auth_policy_rejects_denied_table_on_do_get() {
        let svc = make_auth_policy_service();
        let ticket = TicketStatementQuery {
            statement_handle: b"SELECT * FROM secret".to_vec().into(),
        };
        let mut req = Request::new(Ticket::new(vec![]));
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer secret-key"),
        );
        let result = svc.do_get_statement(ticket, req).await;
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn no_auth_configured_allows_any_request() {
        // Service with no auth provider — should pass through without auth check.
        let svc = KrishivFlightSqlService::new().expect("flight host");
        let ticket = TicketStatementQuery {
            statement_handle: b"SELECT 1".to_vec().into(),
        };
        let result = svc
            .do_get_statement(ticket, Request::new(Ticket::new(vec![])))
            .await;
        assert!(result.is_ok());
    }

    // ── Prepared statement tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn create_prepared_statement_returns_handle() {
        use arrow_flight::sql::ActionCreatePreparedStatementRequest;

        let svc = KrishivFlightSqlService::new().expect("flight host");
        let req = ActionCreatePreparedStatementRequest {
            query: "SELECT 42 AS answer".to_string(),
            ..Default::default()
        };
        let result = svc
            .do_action_create_prepared_statement(
                req,
                Request::new(arrow_flight::Action {
                    r#type: String::new(),
                    body: bytes::Bytes::new(),
                }),
            )
            .await;
        assert!(result.is_ok(), "create_prepared_statement must succeed");
        let res = result.unwrap();
        assert!(
            !res.prepared_statement_handle.is_empty(),
            "handle must be non-empty"
        );
    }

    #[tokio::test]
    async fn do_get_prepared_statement_executes_stored_sql() {
        use arrow_flight::sql::ActionCreatePreparedStatementRequest;

        let svc = KrishivFlightSqlService::new().expect("flight host");

        // Create a prepared statement.
        let create_req = ActionCreatePreparedStatementRequest {
            query: "SELECT 99 AS val".to_string(),
            ..Default::default()
        };
        let create_result = svc
            .do_action_create_prepared_statement(
                create_req,
                Request::new(arrow_flight::Action {
                    r#type: String::new(),
                    body: bytes::Bytes::new(),
                }),
            )
            .await
            .unwrap();

        let handle = create_result.prepared_statement_handle;

        // Execute via do_get_prepared_statement.
        let exec_req = arrow_flight::sql::CommandPreparedStatementQuery {
            prepared_statement_handle: handle,
        };
        let result = svc
            .do_get_prepared_statement(exec_req, Request::new(Ticket::new(vec![])))
            .await;
        assert!(result.is_ok(), "do_get_prepared_statement must succeed");
        let items: Vec<_> = result.unwrap().into_inner().collect().await;
        assert!(
            !items.is_empty(),
            "must return at least a schema FlightData"
        );
        assert!(items[0].is_ok());
    }

    #[tokio::test]
    async fn do_get_prepared_statement_unknown_handle_returns_not_found() {
        let svc = KrishivFlightSqlService::new().expect("flight host");
        let req = arrow_flight::sql::CommandPreparedStatementQuery {
            prepared_statement_handle: b"no-such-handle".to_vec().into(),
        };
        let result = svc
            .do_get_prepared_statement(req, Request::new(Ticket::new(vec![])))
            .await;
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn close_prepared_statement_removes_handle() {
        use arrow_flight::sql::{
            ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
            CommandPreparedStatementQuery,
        };

        let svc = KrishivFlightSqlService::new().expect("flight host");

        // Create a prepared statement.
        let create_req = ActionCreatePreparedStatementRequest {
            query: "SELECT 1 AS x".to_string(),
            ..Default::default()
        };
        let handle = svc
            .do_action_create_prepared_statement(
                create_req,
                Request::new(arrow_flight::Action {
                    r#type: String::new(),
                    body: bytes::Bytes::new(),
                }),
            )
            .await
            .unwrap()
            .prepared_statement_handle;

        // Close the prepared statement.
        let close_req = ActionClosePreparedStatementRequest {
            prepared_statement_handle: handle.clone(),
        };
        let close_result = svc
            .do_action_close_prepared_statement(
                close_req,
                Request::new(arrow_flight::Action {
                    r#type: String::new(),
                    body: bytes::Bytes::new(),
                }),
            )
            .await;
        assert!(close_result.is_ok(), "close must succeed");

        // Attempting to execute after close must return NotFound.
        let exec_req = CommandPreparedStatementQuery {
            prepared_statement_handle: handle,
        };
        let result = svc
            .do_get_prepared_statement(exec_req, Request::new(Ticket::new(vec![])))
            .await;
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().code(),
            tonic::Code::NotFound,
            "after close, handle must be gone"
        );
    }

    #[tokio::test]
    async fn get_flight_info_prepared_statement_returns_endpoint() {
        use arrow_flight::sql::ActionCreatePreparedStatementRequest;

        let svc = KrishivFlightSqlService::new().expect("flight host");

        // Create a prepared statement.
        let create_req = ActionCreatePreparedStatementRequest {
            query: "SELECT 7 AS n".to_string(),
            ..Default::default()
        };
        let handle = svc
            .do_action_create_prepared_statement(
                create_req,
                Request::new(arrow_flight::Action {
                    r#type: String::new(),
                    body: bytes::Bytes::new(),
                }),
            )
            .await
            .unwrap()
            .prepared_statement_handle;

        let info_req = arrow_flight::sql::CommandPreparedStatementQuery {
            prepared_statement_handle: handle,
        };
        let descriptor = FlightDescriptor::new_cmd(vec![]);
        let result = svc
            .get_flight_info_prepared_statement(info_req, Request::new(descriptor))
            .await;
        assert!(
            result.is_ok(),
            "get_flight_info_prepared_statement must succeed"
        );
        let info = result.unwrap().into_inner();
        assert_eq!(info.endpoint.len(), 1, "must return one endpoint");
        assert!(
            !info.endpoint[0].ticket.as_ref().unwrap().ticket.is_empty(),
            "endpoint must carry a ticket"
        );
    }

    // ── P0.13 — check_table_access enforcement ────────────────────────────────

    #[tokio::test]
    async fn p0_13_check_table_access_allow_path() {
        // When the policy allows the table, the query should succeed.
        let svc = make_auth_policy_service();
        // SELECT 42 has no FROM clause so it always succeeds regardless of policy.
        let ticket = TicketStatementQuery {
            statement_handle: b"SELECT 42 AS v".to_vec().into(),
        };
        let mut req = Request::new(Ticket::new(vec![]));
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer secret-key"),
        );
        let result = svc.do_get_statement(ticket, req).await;
        assert!(result.is_ok(), "allowed query must succeed");
    }

    #[tokio::test]
    async fn p0_13_check_table_access_deny_path() {
        // When the policy denies a table, the query must return PermissionDenied.
        let svc = make_auth_policy_service();
        let ticket = TicketStatementQuery {
            statement_handle: b"SELECT * FROM secret".to_vec().into(),
        };
        let mut req = Request::new(Ticket::new(vec![]));
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::from_static("Bearer secret-key"),
        );
        let result = svc.do_get_statement(ticket, req).await;
        assert!(result.is_err(), "denied table must return an error");
        assert_eq!(
            result.err().unwrap().code(),
            tonic::Code::PermissionDenied,
            "denied table must return PermissionDenied"
        );
    }

    // ── Service Debug format ────────────────────────────────────────────────

    #[test]
    fn service_debug_format() {
        let svc = KrishivFlightSqlService::new().expect("flight host");
        let debug = format!("{:?}", svc);
        assert!(debug.contains("KrishivFlightSqlService"));
        assert!(debug.contains("auth: false"));
        assert!(debug.contains("policy: false"));
    }

    #[test]
    fn service_with_auth_debug_shows_true() {
        let mut keys = std::collections::HashMap::new();
        keys.insert("key".to_string(), "user".to_string());
        let auth = Arc::new(StaticApiKeyAuthProvider::new(keys));
        let svc = KrishivFlightSqlService::new()
            .expect("flight host")
            .with_auth(auth);
        let debug = format!("{:?}", svc);
        assert!(debug.contains("auth: true"));
    }

    // ── Host tests ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn host_execute_empty_sql() {
        let host = FlightExecutionHost::with_coordinator_http(None).unwrap();
        // Empty SQL is handled by DataFusion; behavior depends on implementation.
        // Just verify it doesn't panic.
        let _result = host.execute_sql("").await;
    }

    #[test]
    fn host_coordinator_http_none() {
        let host = FlightExecutionHost::with_coordinator_http(None).unwrap();
        assert!(host.coordinator_http_url().is_none());
    }

    #[test]
    fn host_coordinator_http_some() {
        let host =
            FlightExecutionHost::with_coordinator_http(Some("http://coord:8080".into())).unwrap();
        assert_eq!(host.coordinator_http_url(), Some("http://coord:8080"));
    }

    // ── AllowAllPolicyHook test ─────────────────────────────────────────────

    #[test]
    fn allow_all_policy_hook_allows_all_tables() {
        let hook = AllowAllPolicyHook;
        assert!(hook.check_table_access("any_table"));
        assert!(hook.check_table_access("secret_table"));
        assert!(hook.check_table_access("internal_data"));
    }
}
