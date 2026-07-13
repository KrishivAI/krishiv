//! Per-session hardening for the Flight SQL front door (Phase 59).
//!
//! Flight SQL is stateless per RPC, so a "session" here is the authenticated
//! subject (or a single shared `<anonymous>` session when auth is off). The
//! service already caps *global* inflight queries and result size; this module
//! adds the *per-session* knobs a multi-tenant platform needs from the engine:
//!
//!   * a **per-session concurrent-statement cap**, so one noisy tenant cannot
//!     monopolise the global inflight budget
//!     (`KRISHIV_SESSION_MAX_CONCURRENT_STATEMENTS`, `0` = disabled);
//!   * **idle-session eviction**, so abandoned subjects' bookkeeping does not
//!     accumulate (`KRISHIV_SESSION_IDLE_TIMEOUT_SECS`, `0` = disabled); and
//!   * **structured session metrics** — active sessions, admitted statements,
//!     and per-cap rejections — surfaced on the central `/metrics` endpoint.
//!
//! Per-session *time* and *memory* limits are enforced deeper in the stack
//! (statement wall-clock by the coordinator's `KRISHIV_BATCH_SQL_TIMEOUT_SECS`,
//! query memory by `KRISHIV_QUERY_MEMORY_LIMIT_BYTES` / the fair spill pool);
//! this module owns the session-scoped concurrency and liveness dimension.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tonic::Status;

/// Per-session cap on concurrently executing statements (`0` = disabled).
const MAX_CONCURRENT_STATEMENTS_ENV: &str = "KRISHIV_SESSION_MAX_CONCURRENT_STATEMENTS";
/// Idle-session eviction threshold in seconds (`0` = disabled).
const IDLE_TIMEOUT_ENV: &str = "KRISHIV_SESSION_IDLE_TIMEOUT_SECS";

/// Session key used when the request carries no authenticated subject (auth
/// disabled): all anonymous traffic shares one session.
const ANONYMOUS_SUBJECT: &str = "<anonymous>";

/// Defensive upper bound on tracked sessions so a client minting fresh subjects
/// cannot grow the registry without limit between idle sweeps.
const MAX_TRACKED_SESSIONS: usize = 100_000;

/// Liveness + concurrency bookkeeping for a single session (subject).
struct SessionState {
    /// Statements currently executing under this session.
    active_statements: usize,
    /// Statements admitted under this session over its lifetime.
    statements_total: u64,
    /// Last time a statement started or finished (drives idle eviction).
    last_activity: Instant,
}

/// Process-wide per-session limit registry for the Flight SQL front door.
pub struct SessionRegistry {
    inner: Mutex<HashMap<String, SessionState>>,
    /// Per-session concurrent-statement cap; `0` disables the cap.
    max_concurrent_statements: usize,
    /// Idle-session eviction threshold; `None` disables eviction.
    idle_timeout: Option<Duration>,
}

impl std::fmt::Debug for SessionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionRegistry")
            .field("max_concurrent_statements", &self.max_concurrent_statements)
            .field("idle_timeout", &self.idle_timeout)
            .finish_non_exhaustive()
    }
}

fn read_usize_env(name: &str) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

impl SessionRegistry {
    /// Build a registry from the environment. Both limits default to disabled.
    pub fn from_env() -> Arc<Self> {
        let max_concurrent_statements = read_usize_env(MAX_CONCURRENT_STATEMENTS_ENV);
        let idle_secs = read_usize_env(IDLE_TIMEOUT_ENV);
        let idle_timeout = (idle_secs > 0).then(|| Duration::from_secs(idle_secs as u64));
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
            max_concurrent_statements,
            idle_timeout,
        })
    }

    /// Build a registry with explicit limits (tests / programmatic wiring).
    /// `max_concurrent_statements == 0` disables the cap; `idle_timeout == None`
    /// disables idle eviction.
    pub fn new(max_concurrent_statements: usize, idle_timeout: Option<Duration>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
            max_concurrent_statements,
            idle_timeout,
        })
    }

    /// Admit one statement for `subject`, returning an RAII guard that releases
    /// the session slot on drop. Rejects with `resource_exhausted` when the
    /// session is already at its concurrent-statement cap.
    ///
    /// Idle sessions are swept on entry (bounded, O(sessions)); this is the same
    /// sweep-on-access pattern the transaction registry uses.
    pub fn begin_statement(
        self: &Arc<Self>,
        subject: Option<&str>,
    ) -> Result<StatementGuard, Status> {
        let key = subject.unwrap_or(ANONYMOUS_SUBJECT).to_owned();
        let metrics = krishiv_metrics::global_metrics();
        let mut map = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Sweep idle sessions (active_statements == 0 and past the idle
        // threshold). Never evicts a session with statements in flight.
        if let Some(idle) = self.idle_timeout {
            map.retain(|_, s| s.active_statements > 0 || s.last_activity.elapsed() < idle);
        }

        // Defensive: a brand-new subject cannot be admitted once the registry is
        // saturated with distinct sessions (existing sessions still proceed).
        if !map.contains_key(&key) && map.len() >= MAX_TRACKED_SESSIONS {
            metrics.inc_session_statements_rejected();
            return Err(Status::resource_exhausted(
                "too many active sessions; retry later",
            ));
        }

        let now = Instant::now();
        let state = map.entry(key.clone()).or_insert_with(|| SessionState {
            active_statements: 0,
            statements_total: 0,
            last_activity: now,
        });

        if self.max_concurrent_statements > 0
            && state.active_statements >= self.max_concurrent_statements
        {
            metrics.inc_session_statements_rejected();
            return Err(Status::resource_exhausted(format!(
                "session '{key}' exceeded its concurrent-statement limit ({}); retry later or raise {}",
                self.max_concurrent_statements, MAX_CONCURRENT_STATEMENTS_ENV
            )));
        }

        state.active_statements += 1;
        state.statements_total = state.statements_total.saturating_add(1);
        state.last_activity = now;
        let active_sessions = map.len() as u64;
        drop(map);

        metrics.inc_session_statements();
        metrics.set_active_sessions(active_sessions);

        Ok(StatementGuard {
            registry: Arc::clone(self),
            key,
        })
    }

    /// Snapshot `(active_sessions, active_statements)` for tests / diagnostics.
    pub fn snapshot(&self) -> (usize, usize) {
        let map = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let active_statements = map.values().map(|s| s.active_statements).sum();
        (map.len(), active_statements)
    }

    /// Release one statement slot for `key` (called from the guard's `Drop`).
    fn end_statement(&self, key: &str) {
        let mut map = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(state) = map.get_mut(key) {
            state.active_statements = state.active_statements.saturating_sub(1);
            state.last_activity = Instant::now();
        }
        let active_sessions = map.len() as u64;
        drop(map);
        krishiv_metrics::global_metrics().set_active_sessions(active_sessions);
    }
}

/// RAII guard: holds one session statement slot for its lifetime and releases
/// it (decrementing the session's active-statement count) on drop, whether the
/// statement succeeded, failed, or the future was cancelled.
#[derive(Debug)]
pub struct StatementGuard {
    registry: Arc<SessionRegistry>,
    key: String,
}

impl Drop for StatementGuard {
    fn drop(&mut self) {
        self.registry.end_statement(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_session_cap_rejects_over_limit_but_allows_release_and_retry() {
        let reg = SessionRegistry::new(2, None);
        let g1 = reg.begin_statement(Some("alice")).expect("1st admits");
        let _g2 = reg.begin_statement(Some("alice")).expect("2nd admits");
        // Third concurrent statement for alice is over the cap.
        let err = reg.begin_statement(Some("alice")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        // A different subject is unaffected by alice's cap.
        let _b1 = reg.begin_statement(Some("bob")).expect("bob independent");
        // Releasing one of alice's slots frees capacity for a retry.
        drop(g1);
        let _g3 = reg
            .begin_statement(Some("alice"))
            .expect("slot freed on drop");
    }

    #[test]
    fn zero_cap_disables_the_per_session_limit() {
        let reg = SessionRegistry::new(0, None);
        let mut guards = Vec::new();
        for _ in 0..1000 {
            guards.push(reg.begin_statement(Some("alice")).expect("no cap"));
        }
        let (sessions, active) = reg.snapshot();
        assert_eq!(sessions, 1);
        assert_eq!(active, 1000);
    }

    #[test]
    fn idle_sessions_are_evicted_but_active_ones_are_not() {
        let reg = SessionRegistry::new(0, Some(Duration::from_millis(1)));
        // An idle session (guard already dropped).
        drop(reg.begin_statement(Some("idle")).expect("admit"));
        // A live session with a statement in flight.
        let _live = reg.begin_statement(Some("live")).expect("admit");
        std::thread::sleep(Duration::from_millis(5));
        // Any new admission sweeps: "idle" is gone, "live" survives, "new" added.
        let _new = reg.begin_statement(Some("new")).expect("admit");
        let (sessions, _) = reg.snapshot();
        // "live" + "new" remain; "idle" evicted.
        assert_eq!(sessions, 2);
    }

    #[test]
    fn anonymous_subjects_share_one_session() {
        let reg = SessionRegistry::new(1, None);
        let _g = reg.begin_statement(None).expect("anon admits");
        // A second anonymous statement hits the shared-session cap of 1.
        let err = reg.begin_statement(None).unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }
}
