#![forbid(unsafe_code)]
//! Flight SQL service — thin adapter over the Krishiv Session API.
//! **Beta API**: may change between minor releases.

use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    Ticket, flight_service_server::FlightService,
};
use futures::{Stream, stream};
use prost::Message as _; // brings encode_to_vec() into scope
use tonic::{Request, Response, Status, Streaming};

use krishiv_api::SessionBuilder;
use krishiv_governance::{AuthProvider, MaskingRule, PolicyHook, Principal};

/// **Beta API**: may change between minor releases.
#[derive(Clone, Default)]
pub struct KrishivFlightSqlService {
    auth: Option<Arc<dyn AuthProvider>>,
    policy: Option<Arc<dyn PolicyHook>>,
}

impl std::fmt::Debug for KrishivFlightSqlService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KrishivFlightSqlService")
            .field("auth", &self.auth.is_some())
            .field("policy", &self.policy.is_some())
            .finish()
    }
}

impl KrishivFlightSqlService {
    /// Create a new `KrishivFlightSqlService` with no auth or policy.
    pub fn new() -> Self {
        Self::default()
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

    #[allow(clippy::result_large_err)]
    fn make_session(&self) -> Result<krishiv_api::Session, Status> {
        let mut builder = SessionBuilder::new();
        if let Some(auth) = &self.auth {
            builder = builder.with_auth(auth.clone());
        }
        if let Some(policy) = &self.policy {
            builder = builder.with_policy(policy.clone());
        }
        builder.build().map_err(|e| Status::internal(e.to_string()))
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
    use sha2::{Digest, Sha256};

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
                        let digest = Sha256::digest(val.as_bytes());
                        Some(format!("{digest:x}"))
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
        // Authenticate if an auth provider is configured.
        let token = self.bearer_token(&request)?;
        let principal = self.authenticate_request(&request)?;

        let query = std::str::from_utf8(&ticket.statement_handle)
            .map_err(|e| Status::invalid_argument(format!("invalid query encoding: {e}")))?;

        let session = self.make_session()?;
        let result = if self.auth.is_some() && self.policy.is_some() {
            let token = token
                .as_deref()
                .ok_or_else(|| Status::unauthenticated("missing Bearer token"))?;
            session
                .sql_as(token, query)
                .await
                .map_err(|e| match e {
                    krishiv_api::KrishivError::AccessDenied { reason } => {
                        Status::permission_denied(reason)
                    }
                    other => Status::internal(other.to_string()),
                })?
                .collect_async()
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        } else {
            // Use async — do_get_statement is async, sync Session::sql() would panic inside a runtime.
            let df = session
                .sql_async(query)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            df.collect_async()
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };

        // `sql_as` already applies policy checks and masking. The local helper is
        // retained for auth-only/no-auth beta flows.
        let raw_batches = result.batches().to_vec();
        let batches = if self.auth.is_some() && self.policy.is_some() {
            raw_batches
        } else {
            self.apply_policy_masking(&principal, "", raw_batches)?
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
}

/// Build a gRPC `FlightServiceServer` wrapping `KrishivFlightSqlService`.
///
/// **Beta API**: may change between minor releases.
pub fn make_flight_sql_server()
-> arrow_flight::flight_service_server::FlightServiceServer<KrishivFlightSqlService> {
    arrow_flight::flight_service_server::FlightServiceServer::new(KrishivFlightSqlService::new())
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
        KrishivFlightSqlService::new().with_auth(auth)
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
        let _ = KrishivFlightSqlService::default();
    }

    #[test]
    fn make_session_returns_ok() {
        let svc = KrishivFlightSqlService::new();
        assert!(svc.make_session().is_ok());
    }

    #[test]
    fn make_flight_sql_server_compiles() {
        let _ = make_flight_sql_server();
    }

    #[tokio::test]
    async fn get_flight_info_encodes_query_into_ticket() {
        let svc = KrishivFlightSqlService::new();
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
        let svc = KrishivFlightSqlService::new();
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
    async fn do_get_statement_invalid_utf8_returns_invalid_argument() {
        let svc = KrishivFlightSqlService::new();
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

    #[tokio::test]
    async fn auth_required_rejects_missing_token_on_get_flight_info() {
        let svc = make_auth_service();
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
        let svc = make_auth_service();
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
        let svc = make_auth_service();
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
        let svc = make_auth_service();
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
        let svc = make_auth_service();
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
        let svc = make_auth_service();
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
        let svc = KrishivFlightSqlService::new();
        let ticket = TicketStatementQuery {
            statement_handle: b"SELECT 1".to_vec().into(),
        };
        let result = svc
            .do_get_statement(ticket, Request::new(Ticket::new(vec![])))
            .await;
        assert!(result.is_ok());
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
        fn column_masking_rule(
            &self,
            _p: &Principal,
            _t: &str,
            _c: &str,
        ) -> Option<MaskingRule> {
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

        let schema = Arc::new(Schema::new(vec![Field::new("amount", DataType::Int64, true)]));
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

        let schema = Arc::new(Schema::new(vec![Field::new("score", DataType::Float64, true)]));
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
}
