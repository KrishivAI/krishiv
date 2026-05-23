#![forbid(unsafe_code)]
//! **Beta API**: Audit logging, OpenLineage, RBAC, and policy hooks for Krishiv.

// ─── RBAC ────────────────────────────────────────────────────────────────────

/// **Beta API**: Roles assignable to authenticated principals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    /// Full administrative access.
    Admin,
    /// Read and write access.
    Writer,
    /// Read-only access.
    Reader,
}

/// **Beta API**: An authenticated identity with an assigned role.
#[derive(Debug, Clone)]
pub struct Principal {
    /// Unique subject identifier (e.g. username or service account name).
    pub subject: String,
    /// The role granted to this principal.
    pub role: Role,
}

/// **Beta API**: Authenticate an API key and return the associated [`Principal`], if known.
pub trait AuthProvider: Send + Sync {
    /// Return `Some(principal)` if the key is valid, `None` otherwise.
    fn authenticate(&self, api_key: &str) -> Option<Principal>;
}

/// **Beta API**: API-key → [`Principal`] mapping loaded from configuration.
pub struct StaticApiKeyAuthProvider {
    keys: std::collections::HashMap<String, Principal>,
}

impl StaticApiKeyAuthProvider {
    /// **Beta API**: Build from a list of `(api_key, subject, role)` tuples.
    pub fn new(entries: impl IntoIterator<Item = (String, String, Role)>) -> Self {
        let keys = entries
            .into_iter()
            .map(|(k, s, r)| {
                (
                    k,
                    Principal {
                        subject: s,
                        role: r,
                    },
                )
            })
            .collect();
        Self { keys }
    }
}

impl AuthProvider for StaticApiKeyAuthProvider {
    fn authenticate(&self, api_key: &str) -> Option<Principal> {
        self.keys.get(api_key).cloned()
    }
}

// ─── Policy Hooks ─────────────────────────────────────────────────────────────

/// **Beta API**: Column masking rule applied by the policy layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaskingRule {
    /// Replace the column value with a static redaction marker (`"REDACTED"`).
    Redact,
    /// Replace the column value with SQL NULL.
    Nullify,
    /// Replace the column value with its SHA-256 hex digest.
    Hash,
}

/// **Beta API**: Pluggable policy hook evaluated per query/row/column.
pub trait PolicyHook: Send + Sync {
    /// Return `false` to deny access to the named table for this principal.
    fn check_table_access(&self, principal: &Principal, table: &str) -> bool;

    /// Return `Some(rule)` to mask the named column for this principal, `None` to pass through.
    fn column_masking_rule(
        &self,
        principal: &Principal,
        table: &str,
        column: &str,
    ) -> Option<MaskingRule>;

    /// Optional SQL predicate injected before execution (row-level security).
    fn row_predicate(&self, _principal: &Principal, _table: &str) -> Option<String> {
        None
    }
}

/// **Beta API**: No-op hook that allows everything and masks nothing.
pub struct NoOpPolicyHook;

impl PolicyHook for NoOpPolicyHook {
    fn check_table_access(&self, _principal: &Principal, _table: &str) -> bool {
        true
    }

    fn column_masking_rule(
        &self,
        _principal: &Principal,
        _table: &str,
        _column: &str,
    ) -> Option<MaskingRule> {
        None
    }
}

/// **Beta API**: Role-based policy hook: Readers cannot access tables prefixed with `"internal_"`.
pub struct RoleBasedPolicyHook;

impl PolicyHook for RoleBasedPolicyHook {
    fn check_table_access(&self, principal: &Principal, table: &str) -> bool {
        if table.starts_with("internal_") {
            return matches!(principal.role, Role::Admin | Role::Writer);
        }
        true
    }

    fn column_masking_rule(
        &self,
        principal: &Principal,
        _table: &str,
        column: &str,
    ) -> Option<MaskingRule> {
        const SENSITIVE: &[&str] = &["ssn", "credit_card", "password_hash"];
        if matches!(principal.role, Role::Reader) && SENSITIVE.contains(&column) {
            Some(MaskingRule::Nullify)
        } else {
            None
        }
    }
}

// ─── Audit Log ────────────────────────────────────────────────────────────────

/// **Beta API**: Actions that must be recorded in the audit log.
#[derive(Debug)]
pub enum AuditAction<'a> {
    /// A SQL query was executed; identified by its hash.
    QueryExecuted { query_hash: &'a str },
    /// A job was submitted to the scheduler.
    JobSubmitted { job_id: &'a str },
    /// A running or queued job was cancelled.
    JobCancelled { job_id: &'a str },
    /// A savepoint was created for a job.
    SavepointCreated { job_id: &'a str },
    /// A job was restored from a savepoint at the given epoch.
    SavepointRestored { job_id: &'a str, epoch: u64 },
    /// A privileged administrative action was performed.
    AdminAction { description: &'a str },
}

/// A structured audit event for external SIEM/audit-log forwarding.
#[derive(Debug, Clone)]
pub struct AuditEvent {
    pub principal: String,
    pub action: String,
    pub resource: Option<String>,
    pub timestamp_ms: i64,
    pub outcome: AuditOutcome,
}

/// Outcome of an audited action.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditOutcome {
    /// Action was permitted and completed.
    Allowed,
    /// Action was denied by a policy check.
    Denied,
}

/// Pluggable audit log sink.
pub trait AuditSink: Send + Sync {
    fn record(&self, event: &AuditEvent);
}

/// No-op audit sink that routes to tracing.
pub struct TracingAuditSink;
impl AuditSink for TracingAuditSink {
    fn record(&self, event: &AuditEvent) {
        tracing::info!(
            principal = %event.principal,
            action = %event.action,
            resource = ?event.resource,
            outcome = ?event.outcome,
            "audit"
        );
    }
}

// P1.15 — Global pluggable AuditSink so callers don't need to pass a sink reference.
//
// Defaults to `TracingAuditSink` when no custom sink has been installed.
// Call `set_audit_sink` once at process startup (e.g. in `krishiv-metrics::init`)
// to route audit events to a production SIEM or log aggregator.
static GLOBAL_AUDIT_SINK: std::sync::OnceLock<Box<dyn AuditSink + Send + Sync>> =
    std::sync::OnceLock::new();

// P0.21 — Dedup key for the last-emitted audit event.
//
// Stores a hash of `(principal, action_name, detail)` for the most recently
// emitted event.  If two consecutive calls produce the same key the second
// emission is suppressed and a warning is logged instead.
thread_local! {
    static LAST_AUDIT_KEY: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Compute a stable 64-bit dedup key for an audit event.
fn audit_dedup_key(principal: &str, action_name: &str, detail: &str) -> u64 {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(principal.as_bytes());
    h.update(b"\x00");
    h.update(action_name.as_bytes());
    h.update(b"\x00");
    h.update(detail.as_bytes());
    let digest = h.finalize();
    // Use the first 8 bytes as a u64 key – collisions are astronomically rare
    // for the sequential-emission dedup use case.
    u64::from_le_bytes(digest[..8].try_into().expect("sha256 is at least 8 bytes"))
}

/// Install a custom [`AuditSink`] for the lifetime of the process.
///
/// Must be called before the first `audit_log` invocation.  Subsequent calls are
/// silently ignored (the first installation wins).
pub fn set_audit_sink(sink: Box<dyn AuditSink + Send + Sync>) {
    GLOBAL_AUDIT_SINK.set(sink).ok();
}

fn get_audit_sink() -> &'static dyn AuditSink {
    GLOBAL_AUDIT_SINK
        .get_or_init(|| Box::new(TracingAuditSink))
        .as_ref()
}

/// **Beta API**: Emit a structured audit log event via `tracing` (target `"krishiv::audit"`).
///
/// In production, the `tracing` subscriber routes these to the audit log
/// destination configured by `krishiv-metrics::init()`.
///
/// Duplicate events are suppressed: if two consecutive calls produce the same
/// `(principal, action, detail)` triple the second emission is skipped and a
/// warning is logged instead (P0.21 deduplication).
///
/// # Parameters
/// - `principal`: identity of the actor performing the action.
/// - `action`: the audited action.
/// - `outcome`: whether the action was permitted or denied.
pub fn audit_log(principal: &str, action: &AuditAction<'_>, outcome: AuditOutcome) {
    let (action_name, detail): (&str, String) = match action {
        AuditAction::QueryExecuted { query_hash } => {
            ("query_executed", format!("hash={query_hash}"))
        }
        AuditAction::JobSubmitted { job_id } => ("job_submitted", format!("job_id={job_id}")),
        AuditAction::JobCancelled { job_id } => ("job_cancelled", format!("job_id={job_id}")),
        AuditAction::SavepointCreated { job_id } => {
            ("savepoint_created", format!("job_id={job_id}"))
        }
        AuditAction::SavepointRestored { job_id, epoch } => (
            "savepoint_restored",
            format!("job_id={job_id} epoch={epoch}"),
        ),
        AuditAction::AdminAction { description } => ("admin_action", (*description).to_owned()),
    };

    // P0.21 — Deduplication: skip if this event is identical to the last one.
    let key = audit_dedup_key(principal, action_name, &detail);
    let suppressed = LAST_AUDIT_KEY.with(|last| {
        if last.get() == Some(key) {
            tracing::warn!(
                target: "krishiv::audit",
                principal = principal,
                action = action_name,
                "duplicate audit event suppressed",
            );
            true
        } else {
            last.set(Some(key));
            false
        }
    });
    if suppressed {
        return;
    }

    let event = AuditEvent {
        principal: principal.to_string(),
        action: action_name.to_string(),
        resource: Some(detail.clone()),
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64,
        outcome,
    };
    get_audit_sink().record(&event);
    tracing::info!(
        target: "krishiv::audit",
        principal = principal,
        action = action_name,
        detail = detail.as_str(),
        "audit event",
    );
}

// ─── OpenLineage ──────────────────────────────────────────────────────────────

use serde::{Deserialize, Serialize};

/// **Beta API**: OpenLineage `RunEvent` type (<https://openlineage.io/spec>).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEvent {
    /// Whether this event marks the start, successful completion, or failure of a run.
    pub event_type: RunEventType,
    /// ISO 8601 UTC timestamp (or Unix epoch seconds for R9 beta).
    pub event_time: String,
    /// Reference to the run that produced this event.
    pub run: RunRef,
    /// Reference to the job definition associated with the run.
    pub job: JobRef,
    /// Datasets read by this run.
    pub inputs: Vec<LineageDataset>,
    /// Datasets written by this run.
    pub outputs: Vec<LineageDataset>,
}

/// **Beta API**: Lifecycle phase of an OpenLineage run event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunEventType {
    /// The run has started.
    Start,
    /// The run completed successfully.
    Complete,
    /// The run failed.
    Fail,
}

/// **Beta API**: Identifies a specific run by its UUID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRef {
    /// UUID v4 string uniquely identifying the run.
    pub run_id: String,
}

/// **Beta API**: Identifies a job within a namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRef {
    /// Logical name of the job.
    pub name: String,
    /// Namespace that scopes the job name.
    pub namespace: String,
}

/// **Beta API**: A dataset that is an input or output of a lineage run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageDataset {
    /// Logical name of the dataset.
    pub name: String,
    /// Namespace that scopes the dataset name.
    pub namespace: String,
}

/// **Beta API**: Error emitting an OpenLineage event.
#[derive(Debug)]
pub enum EmitError {
    /// The HTTP transport failed (connection error, timeout, etc.).
    Transport(String),
    /// The server rejected the event with a 4xx or 5xx status.
    SinkRejected { status: u16, message: String },
}

impl std::fmt::Display for EmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmitError::Transport(msg) => write!(f, "emit transport error: {msg}"),
            EmitError::SinkRejected { status, message } => {
                write!(f, "emit rejected by sink: HTTP {status} — {message}")
            }
        }
    }
}

impl std::error::Error for EmitError {}

/// **Beta API**: Emit OpenLineage run events.
#[async_trait::async_trait]
pub trait OpenLineageEmitter: Send + Sync {
    /// Emit a single [`RunEvent`]. Returns [`Err`] if the event could not be delivered.
    async fn emit(&self, event: RunEvent) -> Result<(), EmitError>;
}

/// **Beta API**: No-op emitter — discards all events.
pub struct NoOpEmitter;

#[async_trait::async_trait]
impl OpenLineageEmitter for NoOpEmitter {
    async fn emit(&self, _event: RunEvent) -> Result<(), EmitError> {
        Ok(())
    }
}

/// **Beta API**: Emitter that writes events as structured `tracing` events (target `"krishiv::lineage"`).
pub struct LoggingEmitter;

#[async_trait::async_trait]
impl OpenLineageEmitter for LoggingEmitter {
    async fn emit(&self, event: RunEvent) -> Result<(), EmitError> {
        let json =
            serde_json::to_string(&event).map_err(|e| EmitError::Transport(e.to_string()))?;
        tracing::info!(target: "krishiv::lineage", event = json.as_str(), "openlineage event");
        Ok(())
    }
}

/// **Beta API**: HTTP emitter — POSTs events to an OpenLineage API endpoint.
pub struct HttpEmitter {
    endpoint: String,
    client: reqwest::Client,
}

impl HttpEmitter {
    /// **Beta API**: Create an [`HttpEmitter`] that sends events to the given endpoint URL.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl OpenLineageEmitter for HttpEmitter {
    async fn emit(&self, event: RunEvent) -> Result<(), EmitError> {
        let response = self
            .client
            .post(&self.endpoint)
            .json(&event)
            .send()
            .await
            .map_err(|e| EmitError::Transport(e.to_string()))?;

        // P0.20: propagate 4xx/5xx instead of silently ignoring them.
        if let Err(e) = response.error_for_status_ref() {
            let status = e.status().map_or(0, |s| s.as_u16());
            let message = e.to_string();
            return Err(EmitError::SinkRejected { status, message });
        }

        Ok(())
    }
}

/// Return the current UTC time as an RFC 3339 / ISO 8601 string.
///
/// Example output: `"2024-05-21T12:34:56.789000000Z"`
fn event_time_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let nanos = dur.subsec_nanos();
    // Manual RFC 3339 formatting without external deps.
    // secs since epoch → broken-down UTC date/time.
    let (year, month, day, hour, min, sec) = epoch_secs_to_datetime(secs);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{nanos:09}Z")
}

/// Decompose Unix epoch seconds into (year, month, day, hour, min, sec) in UTC.
fn epoch_secs_to_datetime(secs: u64) -> (u64, u8, u8, u8, u8, u8) {
    let time_of_day = secs % 86_400;
    let days_since_epoch = secs / 86_400;
    let hour = (time_of_day / 3_600) as u8;
    let min = ((time_of_day % 3_600) / 60) as u8;
    let sec = (time_of_day % 60) as u8;

    // Gregorian calendar computation from days since 1970-01-01.
    // Algorithm: http://howardhinnant.github.io/date_algorithms.html#civil_from_days
    let z = days_since_epoch as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u8;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u8;
    let year = if month <= 2 { y + 1 } else { y } as u64;
    (year, month, day, hour, min, sec)
}

/// **Beta API**: Build a new [`RunEvent`] with the current UTC timestamp and a fresh UUID run_id.
pub fn new_run_event(
    event_type: RunEventType,
    job_name: impl Into<String>,
    job_namespace: impl Into<String>,
    inputs: Vec<LineageDataset>,
    outputs: Vec<LineageDataset>,
) -> RunEvent {
    RunEvent {
        event_type,
        event_time: event_time_now(),
        run: RunRef {
            run_id: uuid::Uuid::new_v4().to_string(),
        },
        job: JobRef {
            name: job_name.into(),
            namespace: job_namespace.into(),
        },
        inputs,
        outputs,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_reader() -> Principal {
        Principal {
            subject: "alice".to_string(),
            role: Role::Reader,
        }
    }

    fn make_admin() -> Principal {
        Principal {
            subject: "bob".to_string(),
            role: Role::Admin,
        }
    }

    fn sample_run_event() -> RunEvent {
        new_run_event(RunEventType::Start, "test_job", "default", vec![], vec![])
    }

    #[test]
    fn static_auth_provider_known_key() {
        let provider = StaticApiKeyAuthProvider::new([(
            "key1".to_string(),
            "alice".to_string(),
            Role::Reader,
        )]);
        let p = provider.authenticate("key1");
        assert!(p.is_some());
        assert_eq!(p.unwrap().subject, "alice");
    }

    #[test]
    fn static_auth_provider_unknown_key() {
        let provider = StaticApiKeyAuthProvider::new([(
            "key1".to_string(),
            "alice".to_string(),
            Role::Reader,
        )]);
        assert!(provider.authenticate("unknown").is_none());
    }

    #[test]
    fn no_op_policy_hook_allows_all() {
        let hook = NoOpPolicyHook;
        let principal = make_reader();
        assert!(hook.check_table_access(&principal, "any_table"));
    }

    #[test]
    fn role_based_hook_reader_denied_internal() {
        let hook = RoleBasedPolicyHook;
        let reader = make_reader();
        assert!(!hook.check_table_access(&reader, "internal_accounts"));
    }

    #[test]
    fn role_based_hook_admin_allowed_internal() {
        let hook = RoleBasedPolicyHook;
        let admin = make_admin();
        assert!(hook.check_table_access(&admin, "internal_accounts"));
    }

    #[test]
    fn role_based_hook_reader_ssn_nullify() {
        let hook = RoleBasedPolicyHook;
        let reader = make_reader();
        let rule = hook.column_masking_rule(&reader, "users", "ssn");
        assert_eq!(rule, Some(MaskingRule::Nullify));
    }

    #[test]
    fn audit_log_does_not_panic() {
        audit_log(
            "alice",
            &AuditAction::JobSubmitted { job_id: "j1" },
            AuditOutcome::Allowed,
        );
    }

    #[test]
    fn audit_log_denied_does_not_panic() {
        audit_log(
            "eve",
            &AuditAction::AdminAction {
                description: "unauthorized escalation",
            },
            AuditOutcome::Denied,
        );
    }

    #[test]
    fn event_time_now_is_iso8601() {
        let ts = super::event_time_now();
        // Must match YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ
        assert!(ts.ends_with('Z'), "timestamp must end with Z: {ts}");
        assert!(ts.contains('T'), "timestamp must contain T separator: {ts}");
        assert_eq!(ts.len(), 30, "ISO 8601 with 9 nanos: {ts}");
    }

    #[test]
    fn audit_event_constructs_correctly() {
        let event = AuditEvent {
            principal: "alice".into(),
            action: "query_execute".into(),
            resource: Some("orders".into()),
            timestamp_ms: 1716201600000,
            outcome: AuditOutcome::Allowed,
        };
        assert_eq!(event.principal, "alice");
        assert_eq!(event.outcome, AuditOutcome::Allowed);
    }

    #[test]
    fn tracing_audit_sink_does_not_panic() {
        let sink = TracingAuditSink;
        let event = AuditEvent {
            principal: "bob".into(),
            action: "job_submit".into(),
            resource: None,
            timestamp_ms: 0,
            outcome: AuditOutcome::Allowed,
        };
        sink.record(&event);
    }

    #[test]
    fn new_run_event_has_unique_run_ids() {
        let e1 = new_run_event(RunEventType::Start, "job", "ns", vec![], vec![]);
        let e2 = new_run_event(RunEventType::Start, "job", "ns", vec![], vec![]);
        assert_ne!(e1.run.run_id, e2.run.run_id);
    }

    #[tokio::test]
    async fn logging_emitter_does_not_fail() {
        let emitter = LoggingEmitter;
        let event = sample_run_event();
        assert!(emitter.emit(event).await.is_ok());
    }

    #[tokio::test]
    async fn no_op_emitter_does_not_fail() {
        let emitter = NoOpEmitter;
        let event = sample_run_event();
        assert!(emitter.emit(event).await.is_ok());
    }

    // ── P0.15 — Deterministic hash masking ───────────────────────────────────

    #[test]
    fn audit_dedup_key_is_deterministic() {
        // P0.15: the same input must always produce the same hash.
        let key1 = super::audit_dedup_key("alice", "job_submitted", "job_id=j1");
        let key2 = super::audit_dedup_key("alice", "job_submitted", "job_id=j1");
        assert_eq!(key1, key2, "audit_dedup_key must be deterministic");
    }

    #[test]
    fn audit_dedup_key_differs_for_different_inputs() {
        let key1 = super::audit_dedup_key("alice", "job_submitted", "job_id=j1");
        let key2 = super::audit_dedup_key("bob", "job_submitted", "job_id=j1");
        assert_ne!(key1, key2);
    }

    // ── P0.21 — Audit deduplication ──────────────────────────────────────────

    static AUDIT_DEDUP_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn audit_log_dedup_suppresses_consecutive_identical_events() {
        let _guard = AUDIT_DEDUP_TEST_LOCK.lock().unwrap();
        super::LAST_AUDIT_KEY.with(|last| last.set(None));

        audit_log(
            "dedup_user",
            &AuditAction::JobSubmitted { job_id: "dup-job" },
            AuditOutcome::Allowed,
        );
        let key_after_first = LAST_AUDIT_KEY.with(|last| last.get());
        assert!(key_after_first.is_some(), "key must be set after first emission");

        // Second identical call – should be suppressed (key unchanged).
        audit_log(
            "dedup_user",
            &AuditAction::JobSubmitted { job_id: "dup-job" },
            AuditOutcome::Allowed,
        );
        let key_after_second = LAST_AUDIT_KEY.with(|last| last.get());
        assert_eq!(
            key_after_first, key_after_second,
            "duplicate event must not change the stored key"
        );

        // A different event should be emitted and update the key.
        audit_log(
            "dedup_user",
            &AuditAction::JobSubmitted { job_id: "different-job" },
            AuditOutcome::Allowed,
        );
        let key_after_different = LAST_AUDIT_KEY.with(|last| last.get());
        assert_ne!(
            key_after_first, key_after_different,
            "a different event must update the stored key"
        );
    }

    // ── P0.20: HttpEmitter error-for-status tests ─────────────────────────────

    #[tokio::test]
    async fn http_emitter_400_returns_sink_rejected() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/lineage")
            .with_status(400)
            .with_body("bad request")
            .create_async()
            .await;

        let emitter = HttpEmitter::new(format!("{}/lineage", server.url()));
        let event = sample_run_event();
        let result = emitter.emit(event).await;
        assert!(result.is_err(), "400 must be an error");
        if let Err(EmitError::SinkRejected { status, .. }) = result {
            assert_eq!(status, 400);
        } else {
            panic!("expected SinkRejected, got: {result:?}");
        }
    }

    #[tokio::test]
    async fn http_emitter_429_returns_sink_rejected() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/lineage")
            .with_status(429)
            .with_body("rate limited")
            .create_async()
            .await;

        let emitter = HttpEmitter::new(format!("{}/lineage", server.url()));
        let event = sample_run_event();
        let result = emitter.emit(event).await;
        assert!(result.is_err(), "429 must be an error");
        if let Err(EmitError::SinkRejected { status, .. }) = result {
            assert_eq!(status, 429);
        } else {
            panic!("expected SinkRejected, got: {result:?}");
        }
    }

    #[tokio::test]
    async fn http_emitter_500_returns_sink_rejected() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/lineage")
            .with_status(500)
            .with_body("internal server error")
            .create_async()
            .await;

        let emitter = HttpEmitter::new(format!("{}/lineage", server.url()));
        let event = sample_run_event();
        let result = emitter.emit(event).await;
        assert!(result.is_err(), "500 must be an error");
        if let Err(EmitError::SinkRejected { status, .. }) = result {
            assert_eq!(status, 500);
        } else {
            panic!("expected SinkRejected, got: {result:?}");
        }
    }

    #[tokio::test]
    async fn http_emitter_200_succeeds() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/lineage")
            .with_status(200)
            .create_async()
            .await;

        let emitter = HttpEmitter::new(format!("{}/lineage", server.url()));
        let event = sample_run_event();
        assert!(emitter.emit(event).await.is_ok(), "200 must succeed");
    }
}
