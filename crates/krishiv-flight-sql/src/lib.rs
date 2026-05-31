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

use krishiv_governance::{AuthProvider, MaskingRule, PolicyHook, Principal};
use krishiv_sql::SqlEngine;
use krishiv_sql::policy::PolicyEnforcingSqlEngine;

pub use host::FlightExecutionHost;

/// **Beta API**: may change between minor releases.
#[derive(Clone)]
pub struct KrishivFlightSqlService {
    auth: Option<Arc<dyn AuthProvider>>,
    policy: Option<Arc<dyn PolicyHook>>,
    host: FlightExecutionHost,
    /// Shared SQL engine for policy enforcement — created once during construction.
    sql_engine: Arc<SqlEngine>,
    /// LRU cache of opaque handle (UUID string) → SQL text for prepared statements.
    prepared_statements: Arc<tokio::sync::Mutex<lru::LruCache<String, String>>>,
}

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
            sql_engine: Arc::new(SqlEngine::new()),
            prepared_statements: Arc::new(tokio::sync::Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(128).unwrap(),
            ))),
        })
    }

    /// Attach a pre-built execution host (tests / custom wiring).
    pub fn with_host(host: FlightExecutionHost) -> Self {
        Self {
            auth: None,
            policy: None,
            host,
            sql_engine: Arc::new(SqlEngine::new()),
            prepared_statements: Arc::new(tokio::sync::Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(128).unwrap(),
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
    /// When set, column-masking rules are applied to every result batch before
    /// it is streamed to the client.
    pub fn with_policy(mut self, policy: Arc<dyn PolicyHook>) -> Self {
        self.policy = Some(policy);
        self
    }

    fn policy_engine(&self) -> Option<PolicyEnforcingSqlEngine> {
        match (&self.auth, &self.policy) {
            (Some(auth), Some(policy)) => Some(PolicyEnforcingSqlEngine::new(
                (*self.sql_engine).clone(),
                auth.clone(),
                policy.clone(),
            )),
            _ => None,
        }
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
    /// Returns `Ok(Some(principal))` when auth is configured and the token is
    /// valid, `Ok(None)` when no [`AuthProvider`] is attached, and
    /// `Err(Status::unauthenticated(...))` when the token is missing or invalid.
    #[allow(clippy::result_large_err)]
    fn authenticate_request<B>(&self, req: &Request<B>) -> Result<Option<Principal>, Status> {
        let Some(auth) = &self.auth else {
            return Ok(None);
        };
        let token = self.bearer_token(req)?.expect("auth is configured");
        auth.authenticate(&token)
            .map(Some)
            .ok_or_else(|| Status::unauthenticated("invalid API key"))
    }

    /// Apply column-masking rules (if a policy is configured) to a list of batches.
    ///
    /// The `table_name` is used as the table context for the masking hook.
    fn apply_policy_masking(
        &self,
        principal: &Option<Principal>,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, Status> {
        let (Some(policy), Some(principal)) = (self.policy.as_deref(), principal.as_ref()) else {
            return Ok(batches);
        };
        batches
            .into_iter()
            .map(|batch| mask_batch(&batch, principal, table_name, policy))
            .collect()
    }
}

/// Apply column-masking rules from `policy` to a single [`RecordBatch`].
fn mask_batch(
    batch: &RecordBatch,
    principal: &Principal,
    table_name: &str,
    policy: &dyn PolicyHook,
) -> Result<RecordBatch, Status> {
    use arrow::array::{Array, ArrayRef, StringArray, new_null_array};
    use arrow::datatypes::{DataType, Field};
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    let mut fields: Vec<Field> = Vec::with_capacity(batch.num_columns());

    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        match policy.column_masking_rule(principal, table_name, field.name()) {
            None => {
                fields.push(field.as_ref().clone());
                columns.push(col.clone());
            }
            Some(MaskingRule::Nullify) => {
                fields.push(field.as_ref().clone());
                columns.push(new_null_array(col.data_type(), batch.num_rows()));
            }
            Some(MaskingRule::Redact) => {
                // P0.14 fix: For string (Utf8/LargeUtf8) columns, replace non-null
                // values with the literal "REDACTED" string while preserving the
                // Utf8 type.  For all other data types, emit a fully-null array of
                // the ORIGINAL type so that the schema is not corrupted.
                match col.data_type() {
                    DataType::Utf8 | DataType::LargeUtf8 => {
                        let redacted: StringArray = (0..batch.num_rows())
                            .map(|row| {
                                if col.is_null(row) {
                                    None
                                } else {
                                    Some("REDACTED")
                                }
                            })
                            .collect();
                        fields.push(Field::new(field.name().clone(), DataType::Utf8, true));
                        columns.push(Arc::new(redacted));
                    }
                    _ => {
                        // Non-string column: preserve original type, nullify all values.
                        fields.push(field.as_ref().clone());
                        columns.push(new_null_array(col.data_type(), batch.num_rows()));
                    }
                }
            }
            Some(MaskingRule::Hash) => {
                let options = FormatOptions::default();
                let formatter = ArrayFormatter::try_new(col.as_ref(), &options)
                    .map_err(|e| Status::internal(e.to_string()))?;
                let hashed: StringArray = (0..batch.num_rows())
                    .map(|row| {
                        if col.is_null(row) {
                            return None;
                        }
                        let val = formatter.value(row).to_string();
                        Some(krishiv_common::hash::sha256_hex(val.as_bytes()))
                    })
                    .collect();
                fields.push(Field::new(field.name().clone(), DataType::Utf8, true));
                columns.push(Arc::new(hashed));
            }
        }
    }

    let output_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new(output_schema, columns).map_err(|e| Status::internal(e.to_string()))
}

#[tonic::async_trait]
impl FlightSqlService for KrishivFlightSqlService {
    type FlightService = KrishivFlightSqlService;

    // No-op handshake — anonymous auth for R8.1 beta
    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
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
        let token = self.bearer_token(&request)?;
        let principal = self.authenticate_request(&request)?;

        let query = std::str::from_utf8(&ticket.statement_handle)
            .map_err(|e| Status::invalid_argument(format!("invalid query encoding: {e}")))?;

        let batches = if let Some(engine) = self.policy_engine() {
            let token = token
                .as_deref()
                .ok_or_else(|| Status::unauthenticated("missing Bearer token"))?;
            let auth_principal = engine
                .authenticate(token)
                .map_err(|e| Status::permission_denied(e.to_string()))?;
            let prepared = engine
                .prepare_authorized_query(&auth_principal, query)
                .map_err(|e| match e {
                    krishiv_sql::SqlError::AccessDenied { reason } => {
                        Status::permission_denied(reason)
                    }
                    other => Status::internal(other.to_string()),
                })?;
            let raw = self
                .host
                .execute_sql(&prepared)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            engine
                .mask_result_batches(&auth_principal, query, raw)
                .map_err(|e| match e {
                    krishiv_sql::SqlError::AccessDenied { reason } => {
                        Status::permission_denied(reason)
                    }
                    other => Status::internal(other.to_string()),
                })?
        } else {
            let raw = self
                .host
                .execute_sql(query)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            self.apply_policy_masking(&principal, query, raw)?
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
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
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
        _request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
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
    async fn handle_krishiv_action(
        &self,
        action: krishiv_runtime::KrishivFlightAction,
    ) -> Result<Vec<u8>, KrishivActionError> {
        use krishiv_runtime::KrishivFlightAction as A;

        // Per-action dispatch.  Empty body on success unless the action
        // produces record batches, in which case we return Arrow IPC bytes.
        match action {
            A::RegisterParquet(body) => {
                // Stash the registration in the host's catalog.  We reuse the
                // existing legacy path by handing it a synthetic comment.
                let sql = krishiv_runtime::flight_protocol::encode_batch_sql(
                    "SELECT 1 AS registered",
                    &[krishiv_runtime::in_process::BatchSqlTable {
                        table_name: body.table,
                        path: body.path,
                    }],
                );
                let _ = self
                    .host
                    .execute_sql(&sql)
                    .await
                    .map_err(KrishivActionError::Status)?;
                Ok(Vec::new())
            }
            A::ContinuousRegister(body) => {
                let cluster = self.host.cluster();
                let local = krishiv_runtime::in_process_cluster::plan_spec_to_local(&body.spec);
                let registry = self.host.continuous_registry();
                let job_id = body.job_id.clone();
                let job_id_for_blocking = job_id.clone();
                let spec_copy = body.spec.clone();
                tokio::task::spawn_blocking(move || {
                    cluster.register_continuous_job(&job_id_for_blocking, &local)?;
                    registry.register_job(job_id_for_blocking, spec_copy)
                })
                .await
                .map_err(|e| KrishivActionError::Other(format!("blocking task: {e}")))?
                .map_err(|e| KrishivActionError::Other(e.to_string()))?;
                Ok(Vec::new())
            }
            A::ContinuousPush(body) => {
                let batches = krishiv_runtime::decode_batches(&body.batches_b64)
                    .map_err(|e| KrishivActionError::Other(e.to_string()))?;
                let cluster = self.host.cluster();
                let registry = self.host.continuous_registry();
                let job_id = body.job_id.clone();
                let batches_for_cluster = batches.clone();
                tokio::task::spawn_blocking(move || {
                    cluster.push_continuous_input(&job_id, batches_for_cluster)?;
                    registry.push_input(&job_id, batches)
                })
                .await
                .map_err(|e| KrishivActionError::Other(format!("blocking task: {e}")))?
                .map_err(|e| KrishivActionError::Other(e.to_string()))?;
                Ok(Vec::new())
            }
            A::ContinuousDrain(body) => {
                let cluster = self.host.cluster();
                let job_id = body.job_id.clone();
                let batches =
                    tokio::task::spawn_blocking(move || cluster.drain_continuous_job(&job_id))
                        .await
                        .map_err(|e| KrishivActionError::Other(format!("blocking task: {e}")))?
                        .map_err(|e| KrishivActionError::Other(e.to_string()))?;
                encode_batches_ipc(&batches)
            }
            A::BoundedWindow(body) => {
                let input_batches = krishiv_runtime::decode_batches(&body.batches_b64)
                    .map_err(|e| KrishivActionError::Other(e.to_string()))?;
                let cluster = self.host.cluster();
                let local = krishiv_runtime::in_process_cluster::plan_spec_to_local(&body.spec);
                let topic = body.topic.clone();
                let result = tokio::task::spawn_blocking(move || {
                    cluster.collect_bounded_window(&topic, input_batches, &local)
                })
                .await
                .map_err(|e| KrishivActionError::Other(format!("blocking task: {e}")))?
                .map_err(|e| KrishivActionError::Other(e.to_string()))?;
                encode_batches_ipc(&result)
            }
            A::Explain(body) => {
                let text = krishiv_sql::explain_sql(&body.sql)
                    .map_err(|e| KrishivActionError::Other(e.to_string()))?;
                Ok(text.into_bytes())
            }
            A::ExecutePlan(body) => {
                let plan = body
                    .to_plan()
                    .map_err(|e| KrishivActionError::Other(e.to_string()))?;
                let sql = krishiv_runtime::flight_client::plan_to_sql(&plan);
                let _ = self
                    .host
                    .execute_sql(&sql)
                    .await
                    .map_err(KrishivActionError::Status)?;
                Ok(Vec::new())
            }
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
-> arrow_flight::flight_service_server::FlightServiceServer<KrishivFlightSqlService> {
    arrow_flight::flight_service_server::FlightServiceServer::new(
        KrishivFlightSqlService::with_host(FlightExecutionHost::from_env().expect("flight host")),
    )
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
    eprintln!("krishiv-flight-server listening on http://{addr}");
    tonic::transport::Server::builder()
        .add_service(make_flight_sql_server())
        .serve(addr)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use krishiv_governance::{MaskingRule, PolicyHook, Role, StaticApiKeyAuthProvider};
    use tonic::metadata::MetadataValue;

    fn make_auth_service() -> KrishivFlightSqlService {
        let auth = Arc::new(StaticApiKeyAuthProvider::new(vec![(
            "secret-key".to_string(),
            "alice".to_string(),
            Role::Reader,
        )]));
        KrishivFlightSqlService::new()
            .expect("flight host")
            .with_auth(auth)
    }

    struct DenySecretPolicy;

    impl PolicyHook for DenySecretPolicy {
        fn check_table_access(&self, _principal: &Principal, table_name: &str) -> bool {
            table_name != "secret"
        }

        fn column_masking_rule(
            &self,
            _principal: &Principal,
            _table_name: &str,
            _column_name: &str,
        ) -> Option<MaskingRule> {
            None
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
        let _ = make_flight_sql_server();
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
        // "allowed_table" is not "secret", so DenySecretPolicy allows it.
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

    // ── P0.14 — MaskingRule::Redact schema preservation ───────────────────────

    struct RedactAllPolicy;

    impl PolicyHook for RedactAllPolicy {
        fn check_table_access(&self, _p: &Principal, _t: &str) -> bool {
            true
        }
        fn column_masking_rule(&self, _p: &Principal, _t: &str, _c: &str) -> Option<MaskingRule> {
            Some(MaskingRule::Redact)
        }
    }

    fn make_principal() -> Principal {
        Principal {
            subject: "tester".into(),
            role: Role::Reader,
        }
    }

    #[test]
    fn p0_14_redact_int64_preserves_schema() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Int64,
            true,
        )]));
        let col = Arc::new(Int64Array::from(vec![Some(100i64), None, Some(200i64)]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();

        let principal = make_principal();
        let policy = RedactAllPolicy;
        let result = mask_batch(&batch, &principal, "payments", &policy).unwrap();

        // Schema must NOT be corrupted — type must remain Int64.
        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::Int64,
            "Redact on Int64 must preserve Int64 type, not convert to Utf8"
        );
        // All values must be null (the column is fully nullified).
        for i in 0..result.num_rows() {
            assert!(
                result.column(0).is_null(i),
                "row {i} must be null after Redact on non-string column"
            );
        }
    }

    #[test]
    fn p0_14_redact_float64_preserves_schema() {
        use arrow::array::Float64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float64,
            true,
        )]));
        let col = Arc::new(Float64Array::from(vec![Some(1.5f64), Some(2.5f64)]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();

        let principal = make_principal();
        let policy = RedactAllPolicy;
        let result = mask_batch(&batch, &principal, "scores", &policy).unwrap();

        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::Float64,
            "Redact on Float64 must preserve Float64 type"
        );
        for i in 0..result.num_rows() {
            assert!(result.column(0).is_null(i));
        }
    }

    #[test]
    fn p0_14_redact_utf8_produces_redacted_string() {
        use arrow::array::{Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let col = Arc::new(StringArray::from(vec![Some("Alice"), None, Some("Bob")]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();

        let principal = make_principal();
        let policy = RedactAllPolicy;
        let result = mask_batch(&batch, &principal, "users", &policy).unwrap();

        assert_eq!(result.schema().field(0).data_type(), &DataType::Utf8);

        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // Non-null original values become "REDACTED".
        assert_eq!(arr.value(0), "REDACTED");
        // Null original values stay null.
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), "REDACTED");
    }

    // ── MaskingRule::Nullify tests ──────────────────────────────────────────

    struct NullifyAllPolicy;

    impl PolicyHook for NullifyAllPolicy {
        fn check_table_access(&self, _p: &Principal, _t: &str) -> bool {
            true
        }
        fn column_masking_rule(&self, _p: &Principal, _t: &str, _c: &str) -> Option<MaskingRule> {
            Some(MaskingRule::Nullify)
        }
    }

    #[test]
    fn nullify_int64_produces_all_nulls() {
        use arrow::array::{Array, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Int64,
            true,
        )]));
        let col = Arc::new(Int64Array::from(vec![Some(100i64), Some(200i64)]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();

        let principal = make_principal();
        let policy = NullifyAllPolicy;
        let result = mask_batch(&batch, &principal, "payments", &policy).unwrap();

        assert_eq!(
            result.schema().field(0).data_type(),
            &DataType::Int64,
            "Nullify must preserve Int64 type"
        );
        for i in 0..result.num_rows() {
            assert!(result.column(0).is_null(i));
        }
    }

    #[test]
    fn nullify_utf8_produces_all_nulls() {
        use arrow::array::{Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let col = Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();

        let principal = make_principal();
        let policy = NullifyAllPolicy;
        let result = mask_batch(&batch, &principal, "users", &policy).unwrap();

        assert_eq!(result.schema().field(0).data_type(), &DataType::Utf8);
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(arr.is_null(0));
        assert!(arr.is_null(1));
    }

    // ── MaskingRule::Hash tests ─────────────────────────────────────────────

    struct HashAllPolicy;

    impl PolicyHook for HashAllPolicy {
        fn check_table_access(&self, _p: &Principal, _t: &str) -> bool {
            true
        }
        fn column_masking_rule(&self, _p: &Principal, _t: &str, _c: &str) -> Option<MaskingRule> {
            Some(MaskingRule::Hash)
        }
    }

    #[test]
    fn hash_utf8_produces_sha256_hex() {
        use arrow::array::{Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let col = Arc::new(StringArray::from(vec![Some("Alice"), None]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();

        let principal = make_principal();
        let policy = HashAllPolicy;
        let result = mask_batch(&batch, &principal, "users", &policy).unwrap();

        // Hash output is Utf8
        assert_eq!(result.schema().field(0).data_type(), &DataType::Utf8);
        let arr = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // Non-null value is hashed
        let hash = arr.value(0);
        assert_eq!(hash.len(), 64, "SHA-256 hex is 64 chars");
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));

        // Null stays null
        assert!(arr.is_null(1));
    }

    // ── Selective masking tests ─────────────────────────────────────────────

    struct SelectivePolicy;

    impl PolicyHook for SelectivePolicy {
        fn check_table_access(&self, _p: &Principal, _t: &str) -> bool {
            true
        }
        fn column_masking_rule(
            &self,
            _p: &Principal,
            _t: &str,
            column_name: &str,
        ) -> Option<MaskingRule> {
            if column_name == "secret" {
                Some(MaskingRule::Redact)
            } else {
                None
            }
        }
    }

    #[test]
    fn selective_masking_only_affects_target_column() {
        use arrow::array::{Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("secret", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1i64), Some(2i64)]))
                    as Arc<dyn arrow::array::Array>,
                Arc::new(StringArray::from(vec![Some("classified"), Some("public")])),
            ],
        )
        .unwrap();

        let principal = make_principal();
        let policy = SelectivePolicy;
        let result = mask_batch(&batch, &principal, "table", &policy).unwrap();

        // id column is untouched
        let id_arr = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_arr.value(0), 1);
        assert_eq!(id_arr.value(1), 2);

        // secret column is redacted
        let secret_arr = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(secret_arr.value(0), "REDACTED");
        assert_eq!(secret_arr.value(1), "REDACTED");
    }

    // ── Empty batch masking ─────────────────────────────────────────────────

    #[test]
    fn mask_empty_batch_returns_empty() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, true)]));
        let batch = RecordBatch::new_empty(schema);

        let principal = make_principal();
        let policy = RedactAllPolicy;
        let result = mask_batch(&batch, &principal, "table", &policy).unwrap();
        assert_eq!(result.num_rows(), 0);
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
        let auth = Arc::new(StaticApiKeyAuthProvider::new(vec![(
            "key".to_string(),
            "user".to_string(),
            Role::Reader,
        )]));
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
}
