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

/// **Beta API**: Emit a structured audit log event via `tracing` (target `"krishiv::audit"`).
///
/// In production, the `tracing` subscriber routes these to the audit log
/// destination configured by `krishiv-metrics::init()`.
pub fn audit_log(principal: &str, action: &AuditAction<'_>) {
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
pub struct EmitError(pub String);

impl std::fmt::Display for EmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "emit error: {}", self.0)
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
        let json = serde_json::to_string(&event).map_err(|e| EmitError(e.to_string()))?;
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
        self.client
            .post(&self.endpoint)
            .json(&event)
            .send()
            .await
            .map_err(|e| EmitError(e.to_string()))?;
        Ok(())
    }
}

/// Return the current time as a Unix epoch seconds string.
///
/// Full ISO 8601 formatting is deferred to R10; this is sufficient for the R9 beta.
fn event_time_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{secs}")
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
        audit_log("alice", &AuditAction::JobSubmitted { job_id: "j1" });
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
}
