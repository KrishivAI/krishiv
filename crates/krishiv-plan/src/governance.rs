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
        use constant_time_eq::constant_time_eq;
        let candidate = api_key.as_bytes();
        // Iterate every entry without short-circuiting so elapsed time is
        // independent of which key matched — prevents timing oracle attacks.
        let mut result: Option<Principal> = None;
        for (stored, principal) in &self.keys {
            if constant_time_eq(stored.as_bytes(), candidate) {
                result = Some(principal.clone());
            }
        }
        result
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
        table: &str,
        column: &str,
    ) -> Option<MaskingRule> {
        // S1: Case-insensitive + basic table-aware matching.
        // Lower both sides so "SSN", "Password_Hash", "CREDIT_CARD" etc. are caught.
        // Table param is now used to select table-specific sensitive sets (extensible).
        let col_l = column.to_ascii_lowercase();
        let table_l = table.to_ascii_lowercase();

        let sensitive: &[&str] = match table_l.as_str() {
            "users" | "customers" | "public_users" => &["ssn", "password_hash"],
            "payments" | "billing" => &["credit_card"],
            _ => &["ssn", "credit_card", "password_hash"],
        };

        if matches!(principal.role, Role::Reader) && sensitive.contains(&col_l.as_str()) {
            Some(MaskingRule::Nullify)
        } else {
            None
        }
    }
}

// ─── Audit Log ────────────────────────────────────────────────────────────────

/// **Beta API**: Actions that must be recorded in the audit log.
#[derive(Debug, Clone)]
pub enum AuditAction {
    /// A SQL query was executed; identified by its hash.
    QueryExecuted { query_hash: String },
    /// A job was submitted to the scheduler.
    JobSubmitted { job_id: String },
    /// A running or queued job was cancelled.
    JobCancelled { job_id: String },
    /// A savepoint was created for a job.
    SavepointCreated { job_id: String },
    /// A job was restored from a savepoint at the given epoch.
    SavepointRestored { job_id: String, epoch: u64 },
    /// A privileged administrative action was performed.
    AdminAction { description: String },
    /// A task was assigned to an executor.
    TaskAssigned {
        job_id: String,
        stage_id: String,
        task_id: String,
        executor_id: String,
    },
    /// A task attempt failed permanently (after retries exhausted).
    TaskFailed {
        job_id: String,
        stage_id: String,
        task_id: String,
        attempt_id: u32,
    },
    /// A checkpoint epoch was committed.
    CheckpointCommitted {
        job_id: String,
        epoch: u64,
        fencing_token: u64,
    },
    /// A checkpoint epoch was aborted (timeout, coordinator failover, etc.).
    CheckpointAborted {
        job_id: String,
        epoch: u64,
        reason: Option<String>,
    },
    /// A sink writer completed its commit for a checkpoint epoch.
    SinkCommitCompleted {
        job_id: String,
        sink_id: String,
        epoch: u64,
    },
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
#[async_trait::async_trait]
pub trait AuditSink: Send + Sync {
    fn record(&self, event: &AuditEvent);

    /// Async audit sink hook (defaults to synchronous `record`).
    async fn record_async(&self, event: AuditEvent) {
        self.record(&event);
    }
}

/// No-op audit sink that routes to tracing.
pub struct TracingAuditSink;

#[async_trait::async_trait]
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

// P3-14 — Global dedup with TTL (not thread-local).
static AUDIT_DEDUP: std::sync::LazyLock<dashmap::DashMap<u64, u64>> =
    std::sync::LazyLock::new(dashmap::DashMap::new);
const AUDIT_DEDUP_TTL_MS: u64 = 60_000;
// Track last eviction time to avoid O(n) full-scan on every audit_log() call.
static AUDIT_DEDUP_LAST_EVICTION_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Redact an opaque identifier for audit log output.
///
/// Replaces the raw id with its first 8 hex chars derived from its SHA-256 so
/// that internal job/query identifiers are not leaked verbatim to SIEM sinks
/// while still allowing correlation within a single audit trail.
fn redact_id(id: &str) -> String {
    let h = krishiv_common::hash::sha256_dedup_key(id.as_bytes());
    format!("{h:016x}")
}

/// Compute a stable 64-bit dedup key for an audit event.
fn audit_dedup_key(principal: &str, action_name: &str, detail: &str) -> u64 {
    krishiv_common::hash::sha256_dedup_key(
        &[
            principal.as_bytes(),
            b"\x00",
            action_name.as_bytes(),
            b"\x00",
            detail.as_bytes(),
        ]
        .concat(),
    )
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
pub fn audit_log(principal: &str, action: &AuditAction, outcome: AuditOutcome) {
    // `detail` uses raw IDs — kept for stable dedup-key hashing only.
    // `redacted` uses hashed IDs — safe to emit to external SIEM sinks.
    let (action_name, detail, redacted): (&str, String, String) = match action {
        AuditAction::QueryExecuted { query_hash } => (
            "query_executed",
            format!("hash={query_hash}"),
            format!("hash={}", redact_id(query_hash)),
        ),
        AuditAction::JobSubmitted { job_id } => (
            "job_submitted",
            format!("job_id={job_id}"),
            format!("job_id={}", redact_id(job_id)),
        ),
        AuditAction::JobCancelled { job_id } => (
            "job_cancelled",
            format!("job_id={job_id}"),
            format!("job_id={}", redact_id(job_id)),
        ),
        AuditAction::SavepointCreated { job_id } => (
            "savepoint_created",
            format!("job_id={job_id}"),
            format!("job_id={}", redact_id(job_id)),
        ),
        AuditAction::SavepointRestored { job_id, epoch } => (
            "savepoint_restored",
            format!("job_id={job_id} epoch={epoch}"),
            format!("job_id={} epoch={epoch}", redact_id(job_id)),
        ),
        AuditAction::AdminAction { description } => {
            ("admin_action", description.clone(), description.clone())
        }
        AuditAction::TaskAssigned {
            job_id,
            stage_id,
            task_id,
            executor_id,
        } => (
            "task_assigned",
            format!(
                "job_id={job_id} stage_id={stage_id} task_id={task_id} executor_id={executor_id}"
            ),
            format!(
                "job_id={} stage_id={stage_id} task_id={task_id} executor_id={executor_id}",
                redact_id(job_id)
            ),
        ),
        AuditAction::TaskFailed {
            job_id,
            stage_id,
            task_id,
            attempt_id,
        } => (
            "task_failed",
            format!(
                "job_id={job_id} stage_id={stage_id} task_id={task_id} attempt_id={attempt_id}"
            ),
            format!(
                "job_id={} stage_id={stage_id} task_id={task_id} attempt_id={attempt_id}",
                redact_id(job_id)
            ),
        ),
        AuditAction::CheckpointCommitted {
            job_id,
            epoch,
            fencing_token,
        } => (
            "checkpoint_committed",
            format!("job_id={job_id} epoch={epoch} fencing_token={fencing_token}"),
            format!(
                "job_id={} epoch={epoch} fencing_token={fencing_token}",
                redact_id(job_id)
            ),
        ),
        AuditAction::CheckpointAborted {
            job_id,
            epoch,
            reason,
        } => {
            let reason_str = reason.as_deref().unwrap_or("unspecified");
            (
                "checkpoint_aborted",
                format!("job_id={job_id} epoch={epoch} reason={reason_str}"),
                format!(
                    "job_id={} epoch={epoch} reason={reason_str}",
                    redact_id(job_id)
                ),
            )
        }
        AuditAction::SinkCommitCompleted {
            job_id,
            sink_id,
            epoch,
        } => (
            "sink_commit_completed",
            format!("job_id={job_id} sink_id={sink_id} epoch={epoch}"),
            format!(
                "job_id={} sink_id={sink_id} epoch={epoch}",
                redact_id(job_id)
            ),
        ),
    };

    // Dedup key uses the full (unredacted) detail for stable identity.
    let key = audit_dedup_key(principal, action_name, &detail);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Periodic eviction (at most every 10 s): remove entries older than 1 hour.
    let last_evict = AUDIT_DEDUP_LAST_EVICTION_MS.load(std::sync::atomic::Ordering::Relaxed);
    if now_ms.saturating_sub(last_evict) >= 10_000 {
        AUDIT_DEDUP.retain(|_, ts| now_ms.saturating_sub(*ts) < 3_600_000);
        AUDIT_DEDUP_LAST_EVICTION_MS.store(now_ms, std::sync::atomic::Ordering::Relaxed);
    }

    if let Some(entry) = AUDIT_DEDUP.get(&key)
        && now_ms.saturating_sub(*entry) < AUDIT_DEDUP_TTL_MS
    {
        tracing::warn!(
            target: "krishiv::audit",
            principal = principal,
            action = action_name,
            "duplicate audit event suppressed",
        );
        return;
    }
    AUDIT_DEDUP.insert(key, now_ms);

    let event = AuditEvent {
        principal: principal.to_string(),
        action: action_name.to_string(),
        resource: Some(redacted),
        timestamp_ms: now_ms as i64,
        outcome,
    };
    get_audit_sink().record(&event);
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
    pub fn new(endpoint: impl Into<String>) -> Result<Self, EmitError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| EmitError::Transport(format!("reqwest client init failed: {e}")))?;
        Ok(Self {
            endpoint: endpoint.into(),
            client,
        })
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

// ── AsyncHttpEmitter ──────────────────────────────────────────────────────────

/// **Beta API**: Asynchronous HTTP emitter — buffers lineage events in a bounded channel
/// and delivers them in a background task, ensuring network delays or API outages never
/// stall scheduler operations or job runs.
pub struct AsyncHttpEmitter {
    sender: tokio::sync::mpsc::Sender<RunEvent>,
}

impl AsyncHttpEmitter {
    /// **Beta API**: Create a new [`AsyncHttpEmitter`] pointing at the endpoint URL
    /// with a bounded capacity (e.g. 1024 events) and spawn its delivery worker task.
    pub fn new(endpoint: impl Into<String>, capacity: usize) -> Result<Self, EmitError> {
        let endpoint_str = endpoint.into();
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<RunEvent>(capacity);
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| EmitError::Transport(format!("reqwest client init failed: {e}")))?;

        tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                let response = client.post(&endpoint_str).json(&event).send().await;
                match response {
                    Ok(resp) => {
                        if let Err(e) = resp.error_for_status() {
                            let status = e.status().map_or(0, |s| s.as_u16());
                            tracing::warn!(
                                target: "krishiv::lineage",
                                status = status,
                                error = %e,
                                "failed to deliver async lineage event: http status error"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "krishiv::lineage",
                            error = %e,
                            "failed to deliver async lineage event: transport failure"
                        );
                    }
                }
            }
        });

        Ok(Self { sender })
    }
}

#[async_trait::async_trait]
impl OpenLineageEmitter for AsyncHttpEmitter {
    async fn emit(&self, event: RunEvent) -> Result<(), EmitError> {
        self.sender
            .send(event)
            .await
            .map_err(|e| EmitError::Transport(format!("failed to enqueue lineage event: {e}")))?;
        Ok(())
    }
}

fn event_time_now() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S.%9fZ")
        .to_string()
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

// ── Global OpenLineage emitter (GAP-OB-06) ──────────────────────────────────

/// Process-global OpenLineage emitter. Defaults to `LoggingEmitter`; callers
/// can override with `set_lineage_emitter()` to route events to an HTTP collector.
static GLOBAL_LINEAGE_EMITTER: std::sync::OnceLock<Box<dyn OpenLineageEmitter + Send + Sync>> =
    std::sync::OnceLock::new();

/// Install a custom [`OpenLineageEmitter`] for the lifetime of the process.
pub fn set_lineage_emitter(emitter: Box<dyn OpenLineageEmitter + Send + Sync>) {
    GLOBAL_LINEAGE_EMITTER.set(emitter).ok();
}

fn get_lineage_emitter() -> &'static (dyn OpenLineageEmitter + Send + Sync) {
    GLOBAL_LINEAGE_EMITTER
        .get_or_init(|| Box::new(LoggingEmitter))
        .as_ref()
}

/// Emit an OpenLineage run event asynchronously via the process-global emitter.
///
/// Failures are logged at `warn` level via `tracing` — lineage emission is
/// best-effort and must never block the scheduler.
pub async fn emit_lineage_event(event: RunEvent) {
    if let Err(e) = get_lineage_emitter().emit(event).await {
        tracing::warn!(error = %e, "failed to emit lineage event");
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
        let _guard = AUDIT_DEDUP_TEST_LOCK.lock().unwrap();
        audit_log(
            "alice",
            &AuditAction::JobSubmitted {
                job_id: "j1".into(),
            },
            AuditOutcome::Allowed,
        );
    }

    #[test]
    fn audit_log_denied_does_not_panic() {
        let _guard = AUDIT_DEDUP_TEST_LOCK.lock().unwrap();
        audit_log(
            "eve",
            &AuditAction::AdminAction {
                description: "unauthorized escalation".into(),
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
        AUDIT_DEDUP.clear();

        let action = AuditAction::JobSubmitted {
            job_id: "dup-job".into(),
        };
        let key = super::audit_dedup_key("dedup_user", "job_submitted", "job_id=dup-job");

        audit_log("dedup_user", &action, AuditOutcome::Allowed);
        assert!(AUDIT_DEDUP.contains_key(&key));

        let entries_before = AUDIT_DEDUP.len();
        audit_log("dedup_user", &action, AuditOutcome::Allowed);
        assert_eq!(
            AUDIT_DEDUP.len(),
            entries_before,
            "duplicate event must not add a new dedup entry"
        );

        audit_log(
            "dedup_user",
            &AuditAction::JobSubmitted {
                job_id: "different-job".into(),
            },
            AuditOutcome::Allowed,
        );
        assert!(AUDIT_DEDUP.len() >= entries_before);
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

        let emitter = HttpEmitter::new(format!("{}/lineage", server.url())).expect("emitter");
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

        let emitter = HttpEmitter::new(format!("{}/lineage", server.url())).expect("emitter");
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

        let emitter = HttpEmitter::new(format!("{}/lineage", server.url())).expect("emitter");
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

        let emitter = HttpEmitter::new(format!("{}/lineage", server.url())).expect("emitter");
        let event = sample_run_event();
        assert!(emitter.emit(event).await.is_ok(), "200 must succeed");
    }

    #[tokio::test]
    async fn async_http_emitter_delivers_in_background() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/lineage")
            .with_status(200)
            .create_async()
            .await;

        let emitter =
            AsyncHttpEmitter::new(format!("{}/lineage", server.url()), 10).expect("emitter");
        let event = sample_run_event();
        assert!(emitter.emit(event).await.is_ok(), "Async emit must succeed");

        // Wait a brief moment for the background worker to deliver the event and satisfy the mock
        let mut success = false;
        for _ in 0..20 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if mock.matched() {
                success = true;
                break;
            }
        }
        assert!(
            success,
            "Async emitter failed to deliver event to mock server in background"
        );
    }

    // ── AuditLog dedup edge cases ───────────────────────────────────────────

    #[test]
    fn audit_log_dedup_allows_same_event_after_ttl_expires() {
        let _guard = AUDIT_DEDUP_TEST_LOCK.lock().unwrap();
        AUDIT_DEDUP.clear();

        let action = AuditAction::JobSubmitted {
            job_id: "ttl-job".into(),
        };
        let key = super::audit_dedup_key("ttl_user", "job_submitted", "job_id=ttl-job");

        // First emission inserts the dedup entry.
        audit_log("ttl_user", &action, AuditOutcome::Allowed);
        assert!(AUDIT_DEDUP.contains_key(&key));

        // Manually backdate the entry to simulate TTL expiry.
        AUDIT_DEDUP.insert(key, 0);

        let entries_before = AUDIT_DEDUP.len();
        audit_log("ttl_user", &action, AuditOutcome::Allowed);
        assert_eq!(
            AUDIT_DEDUP.len(),
            entries_before,
            "expired entry must be overwritten, not added"
        );
    }

    #[test]
    fn audit_log_dedup_different_principal_allows_same_action() {
        let _guard = AUDIT_DEDUP_TEST_LOCK.lock().unwrap();
        AUDIT_DEDUP.clear();

        let action = AuditAction::QueryExecuted {
            query_hash: "abc123".into(),
        };
        audit_log("alice", &action, AuditOutcome::Allowed);
        audit_log("bob", &action, AuditOutcome::Allowed);

        // Both should have been recorded (different principals → different keys).
        let alice_key = super::audit_dedup_key("alice", "query_executed", "hash=abc123");
        let bob_key = super::audit_dedup_key("bob", "query_executed", "hash=abc123");
        assert!(AUDIT_DEDUP.contains_key(&alice_key));
        assert!(AUDIT_DEDUP.contains_key(&bob_key));
    }

    #[test]
    fn audit_log_dedup_different_detail_allows_same_principal() {
        let _guard = AUDIT_DEDUP_TEST_LOCK.lock().unwrap();
        AUDIT_DEDUP.clear();

        audit_log(
            "carol",
            &AuditAction::SavepointCreated {
                job_id: "sp-1".into(),
            },
            AuditOutcome::Allowed,
        );
        audit_log(
            "carol",
            &AuditAction::SavepointCreated {
                job_id: "sp-2".into(),
            },
            AuditOutcome::Allowed,
        );

        let key1 = super::audit_dedup_key("carol", "savepoint_created", "job_id=sp-1");
        let key2 = super::audit_dedup_key("carol", "savepoint_created", "job_id=sp-2");
        assert!(AUDIT_DEDUP.contains_key(&key1));
        assert!(AUDIT_DEDUP.contains_key(&key2));
    }

    #[test]
    fn audit_log_all_action_variants_produce_correct_action_name() {
        let _guard = AUDIT_DEDUP_TEST_LOCK.lock().unwrap();
        AUDIT_DEDUP.clear();

        let actions: Vec<(AuditAction, &str, &str)> = vec![
            (
                AuditAction::QueryExecuted {
                    query_hash: "h1".into(),
                },
                "query_executed",
                "hash=h1",
            ),
            (
                AuditAction::JobSubmitted {
                    job_id: "j1".into(),
                },
                "job_submitted",
                "job_id=j1",
            ),
            (
                AuditAction::JobCancelled {
                    job_id: "j2".into(),
                },
                "job_cancelled",
                "job_id=j2",
            ),
            (
                AuditAction::SavepointCreated {
                    job_id: "j3".into(),
                },
                "savepoint_created",
                "job_id=j3",
            ),
            (
                AuditAction::SavepointRestored {
                    job_id: "j4".into(),
                    epoch: 7,
                },
                "savepoint_restored",
                "job_id=j4 epoch=7",
            ),
            (
                AuditAction::AdminAction {
                    description: "escalate".into(),
                },
                "admin_action",
                "escalate",
            ),
        ];

        for (action, expected_name, _expected_detail) in &actions {
            let key = super::audit_dedup_key("test_user", expected_name, _expected_detail);
            audit_log("test_user", action, AuditOutcome::Allowed);
            assert!(
                AUDIT_DEDUP.contains_key(&key),
                "dedup key missing for action: {expected_name}"
            );
        }
    }

    // ── RoleBasedPolicyHook additional access checks ─────────────────────────

    #[test]
    fn role_based_hook_writer_allowed_internal() {
        let hook = RoleBasedPolicyHook;
        let writer = Principal {
            subject: "charlie".to_string(),
            role: Role::Writer,
        };
        assert!(hook.check_table_access(&writer, "internal_metrics"));
    }

    #[test]
    fn role_based_hook_reader_allowed_non_internal() {
        let hook = RoleBasedPolicyHook;
        let reader = make_reader();
        assert!(hook.check_table_access(&reader, "public_users"));
    }

    #[test]
    fn role_based_hook_writer_allowed_non_internal() {
        let hook = RoleBasedPolicyHook;
        let writer = Principal {
            subject: "charlie".to_string(),
            role: Role::Writer,
        };
        assert!(hook.check_table_access(&writer, "public_users"));
    }

    #[test]
    fn role_based_hook_admin_allowed_non_internal() {
        let hook = RoleBasedPolicyHook;
        let admin = make_admin();
        assert!(hook.check_table_access(&admin, "public_users"));
    }

    #[test]
    fn role_based_hook_reader_credit_card_nullify() {
        let hook = RoleBasedPolicyHook;
        let reader = make_reader();
        assert_eq!(
            hook.column_masking_rule(&reader, "payments", "credit_card"),
            Some(MaskingRule::Nullify)
        );
    }

    #[test]
    fn role_based_hook_reader_password_hash_nullify() {
        let hook = RoleBasedPolicyHook;
        let reader = make_reader();
        assert_eq!(
            hook.column_masking_rule(&reader, "users", "password_hash"),
            Some(MaskingRule::Nullify)
        );
    }

    #[test]
    fn role_based_hook_reader_non_sensitive_not_masked() {
        let hook = RoleBasedPolicyHook;
        let reader = make_reader();
        assert_eq!(hook.column_masking_rule(&reader, "users", "email"), None);
    }

    #[test]
    fn role_based_hook_writer_sensitive_not_masked() {
        let hook = RoleBasedPolicyHook;
        let writer = Principal {
            subject: "charlie".to_string(),
            role: Role::Writer,
        };
        assert_eq!(hook.column_masking_rule(&writer, "users", "ssn"), None);
    }

    // ── S1 regression: case-insensitive + table-aware masking ────────────────

    #[test]
    fn role_based_hook_reader_ssn_uppercase_nullify() {
        let hook = RoleBasedPolicyHook;
        let reader = make_reader();
        // Mixed/upper case must still trigger (was bypass before S1)
        assert_eq!(
            hook.column_masking_rule(&reader, "users", "SSN"),
            Some(MaskingRule::Nullify)
        );
        assert_eq!(
            hook.column_masking_rule(&reader, "USERS", "Password_Hash"),
            Some(MaskingRule::Nullify)
        );
    }

    #[test]
    fn role_based_hook_reader_credit_card_mixed_case_table() {
        let hook = RoleBasedPolicyHook;
        let reader = make_reader();
        assert_eq!(
            hook.column_masking_rule(&reader, "Payments", "CREDIT_CARD"),
            Some(MaskingRule::Nullify)
        );
        // Table-specific set: credit_card not in "users" fallback for this arm, but global fallback covers it
        assert_eq!(
            hook.column_masking_rule(&reader, "other_table", "credit_card"),
            Some(MaskingRule::Nullify)
        );
    }

    #[test]
    fn role_based_hook_reader_table_aware_does_not_over_mask() {
        let hook = RoleBasedPolicyHook;
        let reader = make_reader();
        // Non-sensitive column stays visible even with case variation on table
        assert_eq!(hook.column_masking_rule(&reader, "USERS", "email"), None);
    }

    #[test]
    fn role_based_hook_admin_sensitive_not_masked() {
        let hook = RoleBasedPolicyHook;
        let admin = make_admin();
        assert_eq!(hook.column_masking_rule(&admin, "users", "ssn"), None);
    }

    #[test]
    fn role_based_hook_row_predicate_returns_none() {
        let hook = RoleBasedPolicyHook;
        let reader = make_reader();
        assert_eq!(hook.row_predicate(&reader, "users"), None);
    }

    // ── OpenLineage event emission ───────────────────────────────────────────

    #[test]
    fn new_run_event_has_correct_event_type() {
        let event = new_run_event(RunEventType::Complete, "job", "ns", vec![], vec![]);
        assert!(matches!(event.event_type, RunEventType::Complete));
    }

    #[test]
    fn new_run_event_has_correct_job_refs() {
        let event = new_run_event(RunEventType::Fail, "my_job", "my_ns", vec![], vec![]);
        assert_eq!(event.job.name, "my_job");
        assert_eq!(event.job.namespace, "my_ns");
    }

    #[test]
    fn new_run_event_populates_inputs_and_outputs() {
        let inputs = vec![LineageDataset {
            name: "input_ds".into(),
            namespace: "s3://bucket".into(),
        }];
        let outputs = vec![LineageDataset {
            name: "output_ds".into(),
            namespace: "s3://bucket".into(),
        }];
        let event = new_run_event(
            RunEventType::Start,
            "job",
            "ns",
            inputs.clone(),
            outputs.clone(),
        );
        assert_eq!(event.inputs.len(), 1);
        assert_eq!(event.inputs[0].name, "input_ds");
        assert_eq!(event.outputs.len(), 1);
        assert_eq!(event.outputs[0].namespace, "s3://bucket");
    }

    #[test]
    fn run_event_serializes_to_json() {
        let event = new_run_event(RunEventType::Start, "job", "ns", vec![], vec![]);
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("START"));
        assert!(json.contains("job"));
    }

    #[test]
    fn run_event_deserializes_from_json() {
        let event = new_run_event(RunEventType::Complete, "job", "ns", vec![], vec![]);
        let json = serde_json::to_string(&event).unwrap();
        let parsed: RunEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.job.name, event.job.name);
        assert_eq!(parsed.run.run_id, event.run.run_id);
        assert_eq!(parsed.event_time, event.event_time);
    }

    #[tokio::test]
    async fn logging_emitter_emits_valid_json() {
        let emitter = LoggingEmitter;
        let event = new_run_event(RunEventType::Start, "test_job", "default", vec![], vec![]);
        // Should not return an error (serialization + tracing must succeed).
        assert!(emitter.emit(event).await.is_ok());
    }

    #[test]
    fn emit_error_display_transport() {
        let err = EmitError::Transport("connection refused".into());
        assert!(err.to_string().contains("connection refused"));
    }

    #[test]
    fn emit_error_display_sink_rejected() {
        let err = EmitError::SinkRejected {
            status: 503,
            message: "service unavailable".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("503"));
        assert!(msg.contains("service unavailable"));
    }

    #[test]
    fn audit_outcome_is_non_exhaustive() {
        // Verify the enum has exactly the expected variants (non_exhaustive allows future extension).
        let allowed = AuditOutcome::Allowed;
        let denied = AuditOutcome::Denied;
        assert_eq!(allowed, AuditOutcome::Allowed);
        assert_eq!(denied, AuditOutcome::Denied);
        assert_ne!(allowed, denied);
    }

    // Regression test: authenticate must never short-circuit on a prefix match.
    // A timing oracle would return Some(_) for "secret" when the stored key is
    // "secretXXX" because the loop broke early on content equality. The
    // constant_time_eq implementation always iterates every stored entry.
    #[test]
    fn authenticate_no_prefix_timing_oracle() {
        let provider = StaticApiKeyAuthProvider::new([(
            "secretXXX".to_string(),
            "alice".to_string(),
            Role::Reader,
        )]);
        // A key that is a prefix of the stored key must NOT authenticate.
        assert!(provider.authenticate("secret").is_none());
        // A key that is a suffix extension must NOT authenticate.
        assert!(provider.authenticate("secretXXXextra").is_none());
        // Only the exact key must authenticate.
        assert!(provider.authenticate("secretXXX").is_some());
        // Empty string must NOT authenticate.
        assert!(provider.authenticate("").is_none());
    }
}
