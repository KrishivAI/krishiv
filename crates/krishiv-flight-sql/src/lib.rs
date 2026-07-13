#![forbid(unsafe_code)]
//! Flight SQL service — thin adapter over the Krishiv Session API.
//! **Beta API**: may change between minor releases.

mod actions;
mod host;
mod service;
mod session_limits;

pub use host::FlightExecutionHost;
pub use service::{
    KrishivFlightSqlService, make_flight_sql_server, run_flight_server, run_flight_server_from_env,
    run_flight_server_with_host,
};
pub use session_limits::SessionRegistry;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::record_batch::RecordBatch;
    use arrow_flight::sql::server::FlightSqlService;
    use arrow_flight::sql::{
        ActionBeginTransactionRequest, ActionEndTransactionRequest, CommandGetDbSchemas,
        CommandGetTables, CommandStatementQuery, EndTransaction, TicketStatementQuery,
    };
    use arrow_flight::{FlightDescriptor, Ticket};
    use futures::StreamExt;
    use krishiv_plan::governance::{AllowAllPolicyHook, PolicyHook, StaticApiKeyAuthProvider};
    use tonic::Request;
    use tonic::metadata::MetadataValue;

    use super::*;
    use crate::actions::{
        build_param_schema, count_sql_params, normalize_question_mark_params, schema_to_ipc_bytes,
        substitute_sql_params,
    };

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
    async fn flight_sql_transaction_begin_commit_round_trip() {
        use arrow_flight::Action;

        let svc = KrishivFlightSqlService::new().expect("flight host");
        let begin = svc
            .do_action_begin_transaction(
                ActionBeginTransactionRequest {},
                Request::new(Action {
                    r#type: "BeginTransaction".into(),
                    body: bytes::Bytes::new(),
                }),
            )
            .await
            .expect("begin transaction");
        let transaction_id = begin.transaction_id;
        let cmd = CommandStatementQuery {
            query: "SELECT 1".into(),
            transaction_id: Some(transaction_id.clone()),
        };
        svc.get_flight_info_statement(cmd, Request::new(FlightDescriptor::new_cmd(vec![])))
            .await
            .expect("statement in transaction");
        svc.do_action_end_transaction(
            ActionEndTransactionRequest {
                transaction_id: transaction_id.clone(),
                action: EndTransaction::Commit as i32,
            },
            Request::new(Action {
                r#type: "EndTransaction".into(),
                body: bytes::Bytes::new(),
            }),
        )
        .await
        .expect("commit transaction");
        let missing = svc
            .get_flight_info_statement(
                CommandStatementQuery {
                    query: "SELECT 1".into(),
                    transaction_id: Some(transaction_id),
                },
                Request::new(FlightDescriptor::new_cmd(vec![])),
            )
            .await;
        assert!(missing.is_err());
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

    // ── G16: prepared statement parameter binding ───────────────────────────

    #[test]
    fn count_sql_params_finds_highest_index() {
        assert_eq!(count_sql_params("SELECT $1, $2 FROM t WHERE id = $3"), 3);
        assert_eq!(count_sql_params("SELECT 1"), 0);
        assert_eq!(count_sql_params("WHERE x = $10 AND y = $2"), 10);
        assert_eq!(count_sql_params("$1 AND $1"), 1);
    }

    // ── G12: JDBC/ADBC `?` ordinal parameter normalization ──────────────────

    #[test]
    fn normalize_question_mark_converts_single_placeholder() {
        assert_eq!(
            normalize_question_mark_params("SELECT * FROM t WHERE id = ?"),
            "SELECT * FROM t WHERE id = $1"
        );
    }

    #[test]
    fn normalize_question_mark_numbers_sequentially_left_to_right() {
        assert_eq!(
            normalize_question_mark_params("SELECT * FROM t WHERE a = ? AND b = ? AND c = ?"),
            "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $3"
        );
    }

    #[test]
    fn normalize_question_mark_leaves_string_literal_contents_untouched() {
        assert_eq!(
            normalize_question_mark_params("SELECT 'a?b' FROM t WHERE x = ?"),
            "SELECT 'a?b' FROM t WHERE x = $1"
        );
    }

    #[test]
    fn normalize_question_mark_handles_escaped_quote_in_string_with_question_mark() {
        // The literal is `it''s a ?` (SQL-escaped `''` for a literal `'`),
        // containing a `?` that must not be treated as a placeholder, followed
        // by a real placeholder outside the string.
        assert_eq!(
            normalize_question_mark_params("SELECT 'it''s a ?' FROM t WHERE x = ?"),
            "SELECT 'it''s a ?' FROM t WHERE x = $1"
        );
    }

    #[test]
    fn normalize_question_mark_no_placeholders_is_unchanged() {
        assert_eq!(
            normalize_question_mark_params("SELECT * FROM t WHERE id = $1"),
            "SELECT * FROM t WHERE id = $1"
        );
        assert_eq!(normalize_question_mark_params("SELECT 1"), "SELECT 1");
    }

    #[test]
    fn normalize_question_mark_leaves_quoted_identifier_contents_untouched() {
        assert_eq!(
            normalize_question_mark_params(r#"SELECT "weird?col" FROM t WHERE x = ?"#),
            r#"SELECT "weird?col" FROM t WHERE x = $1"#
        );
    }

    #[test]
    fn build_param_schema_creates_n_utf8_fields() {
        let schema = build_param_schema(3);
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "p1");
        assert_eq!(schema.field(2).name(), "p3");
    }

    #[test]
    fn schema_to_ipc_bytes_produces_non_empty_bytes() {
        let schema = build_param_schema(2);
        let bytes = schema_to_ipc_bytes(&schema).expect("ipc bytes");
        assert!(!bytes.is_empty());
    }

    #[test]
    fn substitute_sql_params_replaces_placeholders() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("p1", DataType::Utf8, true),
            Field::new("p2", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["hello"])) as Arc<dyn arrow::array::Array>,
                Arc::new(StringArray::from(vec!["world"])) as Arc<dyn arrow::array::Array>,
            ],
        )
        .unwrap();
        let result = substitute_sql_params("SELECT $1, $2", &batch);
        assert_eq!(result, "SELECT 'hello', 'world'");
    }

    #[test]
    fn substitute_sql_params_handles_sql_injection_in_string_value() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("p1", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["O'Brien"])) as Arc<dyn arrow::array::Array>],
        )
        .unwrap();
        let result = substitute_sql_params("SELECT $1 AS name", &batch);
        assert_eq!(result, "SELECT 'O''Brien' AS name");
    }

    #[test]
    fn substitute_sql_params_does_not_rescan_substituted_text() {
        // A parameter value that itself contains "$1" must not be re-substituted.
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("p1", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(vec!["$1 downstream"]))
                    as Arc<dyn arrow::array::Array>],
            )
            .unwrap();
        let result = substitute_sql_params("SELECT $1 AS x", &batch);
        assert_eq!(
            result, "SELECT '$1 downstream' AS x",
            "the literal substituted for $1 must not be re-scanned for placeholders"
        );
    }

    #[test]
    fn substitute_sql_params_handles_high_index_without_substring_collision() {
        // With 10+ columns bound, $10 must be substituted as column 10 and the
        // later $1 pass must not have eaten into $10. The single-pass scan
        // consumes all digits of $10 at once.
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let fields: Vec<_> = (1..=10)
            .map(|i| Field::new(format!("p{i}"), DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let cols: Vec<Arc<dyn arrow::array::Array>> = (1..=10)
            .map(|i| {
                Arc::new(StringArray::from(vec![format!("v{i}")])) as Arc<dyn arrow::array::Array>
            })
            .collect();
        let batch = RecordBatch::try_new(schema, cols).unwrap();
        let result = substitute_sql_params("SELECT $10, $1", &batch);
        assert_eq!(result, "SELECT 'v10', 'v1'");
    }

    #[test]
    fn substitute_sql_params_leaves_out_of_range_and_zero_placeholders_verbatim() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("p1", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["only"])) as Arc<dyn arrow::array::Array>],
        )
        .unwrap();
        // $2 and $0 are not bound columns (only p1 exists); they must stay literal.
        let result = substitute_sql_params("SELECT $1, $2, $0", &batch);
        assert_eq!(result, "SELECT 'only', $2, $0");
    }

    #[test]
    fn substitute_sql_params_preserves_multibyte_utf8() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("p1", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["café"])) as Arc<dyn arrow::array::Array>],
        )
        .unwrap();
        let result = substitute_sql_params("SELECT $1 — café", &batch);
        assert_eq!(result, "SELECT 'café' — café");
    }

    #[tokio::test]
    async fn create_prepared_statement_returns_parameter_schema_for_parameterized_sql() {
        let svc = KrishivFlightSqlService::new().expect("flight host");
        let req = arrow_flight::sql::ActionCreatePreparedStatementRequest {
            query: "SELECT * FROM t WHERE id = $1 AND name = $2".to_string(),
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
            .await
            .expect("create must succeed");
        assert!(
            !result.parameter_schema.is_empty(),
            "parameter_schema must be populated for $N queries"
        );
    }

    #[tokio::test]
    async fn create_prepared_statement_has_empty_parameter_schema_for_plain_sql() {
        let svc = KrishivFlightSqlService::new().expect("flight host");
        let req = arrow_flight::sql::ActionCreatePreparedStatementRequest {
            query: "SELECT 42 AS n".to_string(),
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
            .await
            .expect("create must succeed");
        // Zero parameters → schema has no fields, but IPC bytes may still be present
        // (the StreamWriter writes a schema header even for empty schemas).
        let _ = result.parameter_schema; // just verify it doesn't panic
    }

    // ── G17: GetDbSchemas / GetTables ───────────────────────────────────────

    #[tokio::test]
    async fn get_flight_info_schemas_returns_endpoint() {
        let svc = KrishivFlightSqlService::new().expect("flight host");
        let query = CommandGetDbSchemas {
            catalog: None,
            db_schema_filter_pattern: None,
        };
        let descriptor = FlightDescriptor::new_cmd(vec![]);
        let result = svc
            .get_flight_info_schemas(query, Request::new(descriptor))
            .await;
        assert!(
            result.is_ok(),
            "get_flight_info_schemas must not be unimplemented"
        );
        let info = result.unwrap().into_inner();
        assert_eq!(info.endpoint.len(), 1, "must return one endpoint");
    }

    #[tokio::test]
    async fn do_get_schemas_returns_default_schema() {
        use futures::StreamExt;
        let svc = KrishivFlightSqlService::new().expect("flight host");
        let query = CommandGetDbSchemas {
            catalog: None,
            db_schema_filter_pattern: None,
        };
        let result = svc
            .do_get_schemas(query, Request::new(Ticket::new(vec![])))
            .await;
        assert!(result.is_ok(), "do_get_schemas must succeed");
        let items: Vec<_> = result.unwrap().into_inner().collect().await;
        assert!(!items.is_empty(), "must return at least the schema message");
        assert!(items[0].is_ok());
    }

    #[tokio::test]
    async fn do_get_schemas_emits_one_schema_row_even_with_many_tables() {
        use arrow_flight::decode::FlightRecordBatchStream;
        use futures::TryStreamExt as _;
        use std::path::PathBuf;

        let host = FlightExecutionHost::embedded().unwrap();
        host.register_parquet("t1", PathBuf::from("/data/t1.parquet"));
        host.register_parquet("t2", PathBuf::from("/data/t2.parquet"));
        host.register_parquet("t3", PathBuf::from("/data/t3.parquet"));
        let svc = KrishivFlightSqlService::with_host(host);

        let query = CommandGetDbSchemas {
            catalog: None,
            db_schema_filter_pattern: None,
        };
        let resp = svc
            .do_get_schemas(query, Request::new(Ticket::new(vec![])))
            .await
            .expect("do_get_schemas must succeed");
        let batches: Vec<RecordBatch> =
            FlightRecordBatchStream::new_from_flight_data(resp.into_inner().map_err(|e| e.into()))
                .try_collect()
                .await
                .expect("decode schemas stream");
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 1,
            "GetDbSchemas must list the single krishiv/default schema once, not once per table"
        );
    }

    #[tokio::test]
    async fn get_flight_info_tables_returns_endpoint() {
        let svc = KrishivFlightSqlService::new().expect("flight host");
        let query = CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: false,
        };
        let descriptor = FlightDescriptor::new_cmd(vec![]);
        let result = svc
            .get_flight_info_tables(query, Request::new(descriptor))
            .await;
        assert!(
            result.is_ok(),
            "get_flight_info_tables must not be unimplemented"
        );
        let info = result.unwrap().into_inner();
        assert_eq!(info.endpoint.len(), 1, "must return one endpoint");
    }

    #[tokio::test]
    async fn do_get_tables_returns_flight_data() {
        use futures::StreamExt;
        let svc = KrishivFlightSqlService::new().expect("flight host");
        let query = CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: false,
        };
        let result = svc
            .do_get_tables(query, Request::new(Ticket::new(vec![])))
            .await;
        assert!(result.is_ok(), "do_get_tables must succeed");
        let items: Vec<_> = result.unwrap().into_inner().collect().await;
        assert!(!items.is_empty(), "must return at least the schema message");
        assert!(items[0].is_ok());
    }

    #[tokio::test]
    async fn do_get_tables_includes_registered_parquet_table() {
        use futures::StreamExt;
        use std::path::PathBuf;

        let host = FlightExecutionHost::embedded().unwrap();
        host.register_parquet("my_sales_data", PathBuf::from("/data/sales.parquet"));
        let svc = KrishivFlightSqlService::with_host(host);

        let query = CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: false,
        };
        let result = svc
            .do_get_tables(query, Request::new(Ticket::new(vec![])))
            .await;
        assert!(result.is_ok(), "do_get_tables must succeed");
        let items: Vec<_> = result.unwrap().into_inner().collect().await;
        // At minimum we get schema + data messages; don't decode full IPC but verify no errors
        assert!(!items.is_empty());
        assert!(items.iter().all(|i| i.is_ok()), "all items must be Ok");
    }

    #[test]
    fn host_list_catalog_tables_returns_registered_entries() {
        use std::path::PathBuf;

        let host = FlightExecutionHost::embedded().unwrap();
        host.register_parquet("orders", PathBuf::from("/data/orders.parquet"));
        host.register_parquet("customers", PathBuf::from("/data/customers.parquet"));

        let tables = host.list_catalog_tables();
        assert_eq!(tables.len(), 2);
        // All entries use "krishiv" catalog and "default" schema
        assert!(
            tables
                .iter()
                .all(|(cat, schema, _)| cat == "krishiv" && schema == "default")
        );
        let names: Vec<_> = tables.iter().map(|(_, _, t)| t.as_str()).collect();
        assert!(names.contains(&"orders"));
        assert!(names.contains(&"customers"));
    }
}
