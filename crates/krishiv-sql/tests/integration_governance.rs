#![forbid(unsafe_code)]

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use krishiv_governance::{
    AuditOutcome, AuditSink, MaskingRule, Principal, Role, RoleBasedPolicyHook,
    StaticApiKeyAuthProvider,
};
use krishiv_sql::SqlEngine;
use krishiv_sql::policy::PolicyEnforcingSqlEngine;

// ── Capture audit sink ────────────────────────────────────────────────────────

/// A global capture sink. Tests filter events by unique principal name to
/// avoid cross-test interference from parallel execution.
static CAPTURED_EVENTS: std::sync::LazyLock<std::sync::Mutex<Vec<krishiv_governance::AuditEvent>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(Vec::new()));

struct GlobalCaptureAuditSink;

impl AuditSink for GlobalCaptureAuditSink {
    fn record(&self, event: &krishiv_governance::AuditEvent) {
        CAPTURED_EVENTS.lock().unwrap().push(event.clone());
    }
}

fn install_capture_audit_sink() {
    let _ = krishiv_governance::set_audit_sink(Box::new(GlobalCaptureAuditSink));
}

/// Return all captured events filtered by the given principal name.
fn events_for_principal(principal_name: &str) -> Vec<krishiv_governance::AuditEvent> {
    CAPTURED_EVENTS
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.principal == principal_name)
        .cloned()
        .collect()
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Schema: ssn is nullable to accommodate NULL masking.
fn make_users_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("ssn", DataType::Utf8, true),
    ]));
    let ids = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let names = Arc::new(StringArray::from(vec!["alice", "bob", "carol"]));
    let ssns = Arc::new(StringArray::from(vec![
        Some("111-11-1111"),
        Some("222-22-2222"),
        Some("333-33-3333"),
    ]));
    RecordBatch::try_new(schema, vec![ids, names, ssns]).unwrap()
}

fn make_internal_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "secret_key",
        DataType::Utf8,
        false,
    )]));
    let keys = Arc::new(StringArray::from(vec!["key-1", "key-2"]));
    RecordBatch::try_new(schema, vec![keys]).unwrap()
}

fn make_orders_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("owner", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let order_ids = Arc::new(Int64Array::from(vec![101, 102, 103, 104]));
    let owners = Arc::new(StringArray::from(vec!["alice", "bob", "alice", "carol"]));
    let amounts = Arc::new(Int64Array::from(vec![50, 120, 30, 200]));
    RecordBatch::try_new(schema, vec![order_ids, owners, amounts]).unwrap()
}

/// Auth setup with unique principal names per test that needs audit verification.
fn make_auth_provider_with(principals: Vec<(&str, &str, Role)>) -> Arc<StaticApiKeyAuthProvider> {
    let entries: Vec<(String, String, Role)> = principals
        .into_iter()
        .map(|(key, name, role)| (key.to_string(), name.to_string(), role))
        .collect();
    Arc::new(StaticApiKeyAuthProvider::new(entries))
}

/// Standard auth: alice (Reader), bob (Writer), carol (Admin).
fn make_standard_auth_provider() -> Arc<StaticApiKeyAuthProvider> {
    make_auth_provider_with(vec![
        ("key-alice", "alice", Role::Reader),
        ("key-bob", "bob", Role::Writer),
        ("key-carol", "carol", Role::Admin),
    ])
}

async fn make_engine(
    auth: Arc<StaticApiKeyAuthProvider>,
    policy: Arc<dyn krishiv_governance::PolicyHook>,
    tables: Vec<(&str, Vec<RecordBatch>)>,
) -> PolicyEnforcingSqlEngine {
    let inner = SqlEngine::new();
    for (name, batches) in tables {
        inner.register_record_batches(name, batches).await.unwrap();
    }
    PolicyEnforcingSqlEngine::new(inner, auth, policy)
}

async fn make_standard_engine(tables: Vec<(&str, Vec<RecordBatch>)>) -> PolicyEnforcingSqlEngine {
    make_engine(
        make_standard_auth_provider(),
        Arc::new(RoleBasedPolicyHook),
        tables,
    )
    .await
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// 1. Full flow: authenticate → policy check → SQL execution → audit event.
#[tokio::test(flavor = "multi_thread")]
async fn auth_policy_sql_audit_full_flow() {
    install_capture_audit_sink();
    let unique = "flow_audit_user";
    let auth = make_auth_provider_with(vec![("key-flow", unique, Role::Reader)]);
    let engine = make_engine(
        auth,
        Arc::new(RoleBasedPolicyHook),
        vec![("users", vec![make_users_batch()])],
    )
    .await;

    let principal = engine.authenticate("key-flow").unwrap();
    assert_eq!(principal.subject, unique);
    assert_eq!(principal.role, Role::Reader);

    let batches = engine
        .execute_as(&principal, "SELECT id, name FROM users")
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    let events = events_for_principal(unique);
    let allowed = events.iter().filter(|e| e.outcome == AuditOutcome::Allowed);
    assert!(
        allowed.count() >= 1,
        "at least one allowed audit event must be emitted"
    );
}

/// 2. RBAC: Admin can query all tables including internal_ tables.
#[tokio::test(flavor = "multi_thread")]
async fn rbac_admin_queries_internal_table() {
    install_capture_audit_sink();
    let engine = make_standard_engine(vec![("internal_keys", vec![make_internal_batch()])]).await;

    let carol = engine.authenticate("key-carol").unwrap();
    assert_eq!(carol.role, Role::Admin);

    let batches = engine
        .execute_as(&carol, "SELECT secret_key FROM internal_keys")
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

/// 3. RBAC: Writer can query internal_ tables.
#[tokio::test(flavor = "multi_thread")]
async fn rbac_writer_queries_internal_table() {
    install_capture_audit_sink();
    let engine = make_standard_engine(vec![("internal_keys", vec![make_internal_batch()])]).await;

    let bob = engine.authenticate("key-bob").unwrap();
    assert_eq!(bob.role, Role::Writer);

    let batches = engine
        .execute_as(&bob, "SELECT secret_key FROM internal_keys")
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

/// 4. RBAC: Reader is denied access to internal_ tables.
#[tokio::test(flavor = "multi_thread")]
async fn rbac_reader_denied_internal_table() {
    install_capture_audit_sink();
    let engine = make_standard_engine(vec![("internal_keys", vec![make_internal_batch()])]).await;

    let alice = engine.authenticate("key-alice").unwrap();
    let result = engine
        .execute_as(&alice, "SELECT secret_key FROM internal_keys")
        .await;
    assert!(
        result.is_err(),
        "reader must be denied access to internal_ table"
    );
}

/// 5. RBAC: Reader can query non-internal tables.
#[tokio::test(flavor = "multi_thread")]
async fn rbac_reader_queries_public_table() {
    install_capture_audit_sink();
    let engine = make_standard_engine(vec![("users", vec![make_users_batch()])]).await;

    let alice = engine.authenticate("key-alice").unwrap();
    let batches = engine
        .execute_as(&alice, "SELECT id, name FROM users")
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

/// 6. Column masking: Reader sees NULL for sensitive columns.
#[tokio::test(flavor = "multi_thread")]
async fn column_masking_reader_sees_null_for_sensitive() {
    install_capture_audit_sink();
    let engine = make_standard_engine(vec![("users", vec![make_users_batch()])]).await;

    let alice = engine.authenticate("key-alice").unwrap();
    let batches = engine
        .execute_as(&alice, "SELECT id, name, ssn FROM users")
        .await
        .unwrap();

    assert!(!batches.is_empty());
    let ssn_col = batches[0].column_by_name("ssn").unwrap();
    for i in 0..ssn_col.len() {
        assert!(
            ssn_col.is_null(i),
            "reader must see NULL for sensitive column 'ssn' at row {i}"
        );
    }
}

/// 7. Column masking: Writer sees actual values for sensitive columns.
#[tokio::test(flavor = "multi_thread")]
async fn column_masking_writer_sees_actual_values() {
    install_capture_audit_sink();
    let engine = make_standard_engine(vec![("users", vec![make_users_batch()])]).await;

    let bob = engine.authenticate("key-bob").unwrap();
    let batches = engine
        .execute_as(&bob, "SELECT id, name, ssn FROM users")
        .await
        .unwrap();

    assert!(!batches.is_empty());
    let ssn_col = batches[0]
        .column_by_name("ssn")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(ssn_col.value(0), "111-11-1111");
    assert_eq!(ssn_col.value(1), "222-22-2222");
    assert_eq!(ssn_col.value(2), "333-33-3333");
}

/// 8. Row-level security: Reader only sees their own rows.
#[tokio::test(flavor = "multi_thread")]
async fn row_level_security_filters_rows_for_reader() {
    install_capture_audit_sink();

    struct RlsPolicy;
    impl krishiv_governance::PolicyHook for RlsPolicy {
        fn check_table_access(&self, _p: &Principal, _table: &str) -> bool {
            true
        }
        fn column_masking_rule(
            &self,
            _p: &Principal,
            _table: &str,
            _column: &str,
        ) -> Option<MaskingRule> {
            None
        }
        fn row_predicate(&self, principal: &Principal, table: &str) -> Option<String> {
            if table == "orders" && principal.role == Role::Reader {
                Some(format!("owner = '{}'", principal.subject))
            } else {
                None
            }
        }
    }

    let engine = make_engine(
        make_standard_auth_provider(),
        Arc::new(RlsPolicy),
        vec![("orders", vec![make_orders_batch()])],
    )
    .await;

    // Alice (Reader) should only see her orders.
    let alice = engine.authenticate("key-alice").unwrap();
    let batches = engine
        .execute_as(&alice, "SELECT order_id, owner, amount FROM orders")
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "alice should only see 2 of her orders");

    let owners = batches[0]
        .column_by_name("owner")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..owners.len() {
        assert_eq!(owners.value(i), "alice");
    }

    // Bob (Writer) should see all rows.
    let bob = engine.authenticate("key-bob").unwrap();
    let batches = engine
        .execute_as(&bob, "SELECT order_id, owner, amount FROM orders")
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4, "writer should see all 4 orders");
}

/// 9. Audit trail: Verify audit events for allowed and denied queries.
#[tokio::test(flavor = "multi_thread")]
async fn audit_trail_emits_events_for_each_query() {
    install_capture_audit_sink();
    let unique = "audit_trail_user";
    let auth = make_auth_provider_with(vec![("key-audit", unique, Role::Reader)]);
    let engine = make_engine(
        auth,
        Arc::new(RoleBasedPolicyHook),
        vec![
            ("users", vec![make_users_batch()]),
            ("internal_keys", vec![make_internal_batch()]),
        ],
    )
    .await;

    let principal = engine.authenticate("key-audit").unwrap();

    // Allowed query.
    let _ = engine
        .execute_as(&principal, "SELECT id FROM users")
        .await
        .unwrap();

    // Denied query.
    let _ = engine
        .execute_as(&principal, "SELECT secret_key FROM internal_keys")
        .await;

    let events = events_for_principal(unique);
    assert!(
        !events.is_empty(),
        "audit events must be recorded for queries"
    );

    let has_allowed = events.iter().any(|e| e.outcome == AuditOutcome::Allowed);
    let has_denied = events.iter().any(|e| e.outcome == AuditOutcome::Denied);
    assert!(has_allowed, "must have at least one Allowed audit event");
    assert!(has_denied, "must have at least one Denied audit event");
}

/// 10. Authentication: Invalid API key returns AccessDenied.
#[tokio::test(flavor = "multi_thread")]
async fn invalid_api_key_returns_access_denied() {
    install_capture_audit_sink();
    let engine = make_standard_engine(vec![]).await;

    let err = engine.authenticate("invalid-key").unwrap_err();
    assert!(
        matches!(err, krishiv_sql::SqlError::AccessDenied { .. }),
        "expected AccessDenied for invalid key"
    );
}

/// 11. Full pipeline: Authenticate, query with column masking and audit.
#[tokio::test(flavor = "multi_thread")]
async fn full_pipeline_auth_mask_audit() {
    install_capture_audit_sink();
    let unique = "pipeline_user";
    let auth = make_auth_provider_with(vec![("key-pipe", unique, Role::Reader)]);
    let engine = make_engine(
        auth,
        Arc::new(RoleBasedPolicyHook),
        vec![("users", vec![make_users_batch()])],
    )
    .await;

    let principal = engine.authenticate("key-pipe").unwrap();
    let batches = engine
        .execute_as(&principal, "SELECT id, name, ssn FROM users")
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    // Verify masking on ssn.
    let ssn_col = batches[0].column_by_name("ssn").unwrap();
    for i in 0..ssn_col.len() {
        assert!(ssn_col.is_null(i));
    }

    // Verify non-sensitive columns are untouched.
    let name_col = batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name_col.value(0), "alice");
    assert_eq!(name_col.value(1), "bob");
    assert_eq!(name_col.value(2), "carol");

    // Verify audit events.
    let events = events_for_principal(unique);
    assert!(
        events.iter().any(|e| e.outcome == AuditOutcome::Allowed),
        "allowed audit event must be present"
    );
}

/// 12. Denied query emits Denied audit event with correct principal.
#[tokio::test(flavor = "multi_thread")]
async fn denied_query_emits_denied_audit_event() {
    install_capture_audit_sink();
    let unique = "denied_event_user";
    let auth = make_auth_provider_with(vec![("key-denied", unique, Role::Reader)]);
    let engine = make_engine(
        auth,
        Arc::new(RoleBasedPolicyHook),
        vec![("internal_keys", vec![make_internal_batch()])],
    )
    .await;

    let principal = engine.authenticate("key-denied").unwrap();
    let _ = engine
        .execute_as(&principal, "SELECT secret_key FROM internal_keys")
        .await;

    let events = events_for_principal(unique);
    let denied_events: Vec<_> = events
        .iter()
        .filter(|e| e.outcome == AuditOutcome::Denied)
        .collect();
    assert!(
        !denied_events.is_empty(),
        "denied audit event must be recorded"
    );
    assert_eq!(denied_events[0].principal, unique);
}

/// 13. Column masking with Redact rule for Reader.
#[tokio::test(flavor = "multi_thread")]
async fn column_masking_redact_rule_for_reader() {
    install_capture_audit_sink();

    struct RedactNamePolicy;
    impl krishiv_governance::PolicyHook for RedactNamePolicy {
        fn check_table_access(&self, _p: &Principal, _table: &str) -> bool {
            true
        }
        fn column_masking_rule(
            &self,
            principal: &Principal,
            _table: &str,
            column: &str,
        ) -> Option<MaskingRule> {
            if column == "name" && principal.role == Role::Reader {
                Some(MaskingRule::Redact)
            } else {
                None
            }
        }
    }

    let engine = make_engine(
        make_standard_auth_provider(),
        Arc::new(RedactNamePolicy),
        vec![("users", vec![make_users_batch()])],
    )
    .await;

    let alice = engine.authenticate("key-alice").unwrap();
    let batches = engine
        .execute_as(&alice, "SELECT id, name FROM users")
        .await
        .unwrap();

    let name_col = batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    for i in 0..name_col.len() {
        assert_eq!(name_col.value(i), "REDACTED");
    }
}

/// 14. Multi-table join: Reader denied if ANY table in the join is forbidden.
#[tokio::test(flavor = "multi_thread")]
async fn multi_table_join_denied_if_any_forbidden() {
    install_capture_audit_sink();
    let engine = make_standard_engine(vec![
        ("users", vec![make_users_batch()]),
        ("internal_keys", vec![make_internal_batch()]),
    ])
    .await;

    let alice = engine.authenticate("key-alice").unwrap();
    let result = engine
        .execute_as(
            &alice,
            "SELECT u.name, k.secret_key FROM users u JOIN internal_keys k ON u.id = 1",
        )
        .await;

    assert!(
        result.is_err(),
        "reader must be denied a join that includes an internal_ table"
    );
}

/// 15. Admin can query non-internal tables without masking.
#[tokio::test(flavor = "multi_thread")]
async fn admin_queries_non_internal_with_no_masking() {
    install_capture_audit_sink();
    let engine = make_standard_engine(vec![("users", vec![make_users_batch()])]).await;

    let carol = engine.authenticate("key-carol").unwrap();
    let batches = engine
        .execute_as(&carol, "SELECT id, name, ssn FROM users")
        .await
        .unwrap();

    let ssn_col = batches[0]
        .column_by_name("ssn")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(ssn_col.value(0), "111-11-1111");
    assert_eq!(ssn_col.value(1), "222-22-2222");
    assert_eq!(ssn_col.value(2), "333-33-3333");
}
