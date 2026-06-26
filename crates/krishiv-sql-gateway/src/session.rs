use std::sync::{Arc, Mutex};

use krishiv_api::{BlockingSession, DataFrame, QueryResult, Session, SessionBuilder};

use crate::error::{GatewayError, GatewayResult};
use krishiv_sql::sqlstate::INTERNAL_ERROR;

/// Blocking SQL gateway session backed by the public Krishiv APIs.
///
/// JDBC/ODBC drivers should target this facade rather than embedding protocol
/// behavior inside [`krishiv_api`].
pub struct GatewaySession {
    blocking: BlockingSession,
}

impl GatewaySession {
    pub fn embedded() -> GatewayResult<Self> {
        BlockingSession::embedded()
            .map(|blocking| Self { blocking })
            .map_err(GatewayError::from_krishiv_error)
    }

    pub fn connect(coordinator_url: impl Into<String>) -> GatewayResult<Self> {
        BlockingSession::connect(coordinator_url)
            .map(|blocking| Self { blocking })
            .map_err(GatewayError::from_krishiv_error)
    }

    pub fn execute_sql(&self, query: &str) -> GatewayResult<GatewayQueryResult> {
        self.blocking
            .sql(query)
            .map(|result| GatewayQueryResult { result })
            .map_err(GatewayError::from_krishiv_error)
    }

    pub fn collect(&self, dataframe: DataFrame) -> GatewayResult<GatewayQueryResult> {
        self.blocking
            .collect(dataframe)
            .map(|result| GatewayQueryResult { result })
            .map_err(GatewayError::from_krishiv_error)
    }

    pub fn session(&self) -> &Session {
        self.blocking.session()
    }
}

impl GatewaySession {
    pub fn builder() -> SessionBuilder {
        SessionBuilder::new()
    }
}

/// Result of a gateway SQL execution, wrapping a public [`QueryResult`].
///
/// This is a thin newtype so gateway callers depend only on the gateway crate's
/// API surface while still being able to extract the underlying [`QueryResult`].
pub struct GatewayQueryResult {
    /// The underlying Krishiv query result.
    pub result: QueryResult,
}

impl std::fmt::Debug for GatewayQueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayQueryResult")
            .field("row_count", &self.result.row_count())
            .finish()
    }
}

impl GatewayQueryResult {
    /// The number of rows in the result.
    pub fn row_count(&self) -> usize {
        self.result.row_count()
    }

    /// Consume the wrapper and return the underlying [`QueryResult`].
    pub fn into_inner(self) -> QueryResult {
        self.result
    }
}

/// A bounded pool of reusable embedded [`GatewaySession`] instances.
///
/// JDBC/ODBC drivers that need to serve many short-lived queries can create a
/// pool at startup and borrow sessions rather than paying the embedded-runtime
/// initialization cost on every connection.
///
/// Sessions are returned to the pool after each borrow; if the pool is empty
/// a new session is created on demand.
pub struct SessionPool {
    pool: Arc<Mutex<Vec<GatewaySession>>>,
    capacity: usize,
}

impl SessionPool {
    /// Create a pool pre-warmed with `capacity` embedded sessions.
    pub fn new_embedded(capacity: usize) -> GatewayResult<Self> {
        let mut sessions = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            sessions.push(GatewaySession::embedded()?);
        }
        Ok(Self {
            pool: Arc::new(Mutex::new(sessions)),
            capacity,
        })
    }

    /// Borrow a session from the pool (creates a new one if the pool is empty).
    pub fn acquire(&self) -> GatewayResult<PooledSession<'_>> {
        let session = {
            let mut guard = self.lock_pool();
            guard.pop()
        };
        let session = match session {
            Some(s) => s,
            None => GatewaySession::embedded()?,
        };
        Ok(PooledSession {
            session: Some(session),
            pool: &self.pool,
            capacity: self.capacity,
        })
    }

    /// Current number of idle sessions in the pool.
    pub fn idle_count(&self) -> usize {
        self.lock_pool().len()
    }

    /// Lock the pool, recovering the guard even if a previous holder panicked
    /// (poisoned lock) so a single panicking borrower cannot deadlock the pool.
    fn lock_pool(&self) -> std::sync::MutexGuard<'_, Vec<GatewaySession>> {
        self.pool
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// A session borrowed from a [`SessionPool`].
///
/// On drop, the session is returned to the pool (up to `capacity`).
pub struct PooledSession<'pool> {
    session: Option<GatewaySession>,
    pool: &'pool Arc<Mutex<Vec<GatewaySession>>>,
    capacity: usize,
}

impl PooledSession<'_> {
    pub fn execute_sql(&self, query: &str) -> GatewayResult<GatewayQueryResult> {
        self.session
            .as_ref()
            .ok_or_else(|| GatewayError {
                sqlstate: INTERNAL_ERROR.to_string(),
                message: "internal: pool session not present".into(),
            })?
            .execute_sql(query)
    }

    pub fn collect(&self, dataframe: DataFrame) -> GatewayResult<GatewayQueryResult> {
        self.session
            .as_ref()
            .ok_or_else(|| GatewayError {
                sqlstate: INTERNAL_ERROR.to_string(),
                message: "internal: pool session not present".into(),
            })?
            .collect(dataframe)
    }

    pub fn session(&self) -> GatewayResult<&Session> {
        Ok(self
            .session
            .as_ref()
            .ok_or_else(|| GatewayError {
                sqlstate: INTERNAL_ERROR.to_string(),
                message: "internal: pool session not present".into(),
            })?
            .session())
    }
}

impl Drop for PooledSession<'_> {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let mut guard = self
                .pool
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if guard.len() < self.capacity {
                guard.push(session);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::GatewayError;
    use krishiv_api::KrishivError;
    use krishiv_sql::SqlError;

    #[test]
    fn gateway_embedded_select_round_trip() {
        let gateway = GatewaySession::embedded().expect("embedded gateway");
        let result = gateway
            .execute_sql("SELECT 42 AS answer")
            .expect("gateway query");
        assert_eq!(result.result.row_count(), 1);
    }

    #[test]
    fn gateway_maps_syntax_errors_to_sqlstate() {
        let gateway = GatewaySession::embedded().expect("embedded gateway");
        let error = gateway
            .execute_sql("SELECT FROM")
            .expect_err("invalid SQL should fail");
        assert_eq!(error.sqlstate().len(), 5);
        assert!(!error.message().is_empty());
    }

    #[test]
    fn session_pool_pre_warms_sessions() {
        let pool = SessionPool::new_embedded(2).expect("pool creation");
        assert_eq!(pool.idle_count(), 2);
    }

    #[test]
    fn session_pool_acquire_and_return() {
        let pool = SessionPool::new_embedded(2).expect("pool creation");
        {
            let borrowed = pool.acquire().expect("acquire");
            let result = borrowed.execute_sql("SELECT 1 AS n").expect("query");
            assert_eq!(result.result.row_count(), 1);
            // `borrowed` drops here — session is returned to pool
        }
        assert_eq!(pool.idle_count(), 2);
    }

    #[test]
    fn session_pool_creates_on_demand_when_empty() {
        let pool = SessionPool::new_embedded(1).expect("pool creation");
        // Borrow the single pre-warmed session without dropping it
        let _held = pool.acquire().expect("acquire first");
        // Pool is empty now; acquire should create a new session on demand
        let second = pool.acquire().expect("acquire second on-demand");
        let result = second.execute_sql("SELECT 99").expect("on-demand query");
        assert_eq!(result.result.row_count(), 1);
    }

    #[test]
    fn session_pool_respects_capacity_on_return() {
        let pool = SessionPool::new_embedded(1).expect("pool");
        // Acquire, execute, return
        let borrowed = pool.acquire().expect("acquire");
        drop(borrowed);
        // Pool had capacity 1; only 1 session returned
        assert_eq!(pool.idle_count(), 1);
    }

    #[test]
    fn session_pool_capacity_zero_starts_empty() {
        let pool = SessionPool::new_embedded(0).expect("pool");
        assert_eq!(pool.idle_count(), 0);
        // Acquire must create on demand since nothing is pre-warmed.
        let borrowed = pool.acquire().expect("acquire on-demand");
        let result = borrowed.execute_sql("SELECT 7 AS n").expect("query");
        assert_eq!(result.result.row_count(), 1);
        // On drop, the session is NOT returned because capacity is 0.
        drop(borrowed);
        assert_eq!(pool.idle_count(), 0);
    }

    #[test]
    fn session_pool_drops_excess_sessions_beyond_capacity() {
        let pool = SessionPool::new_embedded(1).expect("pool");
        // Acquire the single pre-warmed session, then create a second on-demand
        // while the first is still held.
        let first = pool.acquire().expect("acquire first");
        let second = pool.acquire().expect("acquire second on-demand");
        // Return the first; pool now has 1 idle (at capacity).
        drop(first);
        assert_eq!(pool.idle_count(), 1);
        // Returning the second exceeds capacity and must be dropped, not stored.
        drop(second);
        assert_eq!(pool.idle_count(), 1);
    }

    #[test]
    fn gateway_query_result_row_count_and_into_inner() {
        let gateway = GatewaySession::embedded().expect("embedded gateway");
        let result = gateway
            .execute_sql("SELECT 1 AS a UNION ALL SELECT 2 AS a")
            .expect("query");
        assert_eq!(result.row_count(), 2);
        let inner = result.into_inner();
        assert_eq!(inner.row_count(), 2);
    }

    #[test]
    fn gateway_query_result_debug_shows_row_count() {
        let gateway = GatewaySession::embedded().expect("embedded gateway");
        let result = gateway.execute_sql("SELECT 1").expect("query");
        let debug = format!("{:?}", result);
        assert!(debug.contains("row_count"));
        assert!(debug.contains("1"));
    }

    #[test]
    fn gateway_session_builder_is_available() {
        let _builder = GatewaySession::builder();
    }

    #[test]
    fn gateway_session_exposes_inner_session() {
        let gateway = GatewaySession::embedded().expect("embedded gateway");
        let _session: &Session = gateway.session();
    }

    #[test]
    fn from_krishiv_error_maps_runtime_to_internal_error_sqlstate() {
        let error = GatewayError::from(KrishivError::Runtime {
            message: "boom".into(),
        });
        assert_eq!(error.sqlstate(), "XX000");
        assert!(error.message().contains("boom"));
    }

    #[test]
    fn from_krishiv_error_maps_access_denied_to_insufficient_privilege() {
        let error = GatewayError::from(KrishivError::AccessDenied {
            reason: "no token".into(),
        });
        assert_eq!(error.sqlstate(), "42501");
        assert!(error.message().contains("no token"));
    }

    #[test]
    fn from_krishiv_error_maps_unsupported_to_feature_not_supported() {
        let error = GatewayError::from(KrishivError::Unsupported {
            feature: "time travel".into(),
        });
        assert_eq!(error.sqlstate(), "0A000");
    }

    #[test]
    fn from_krishiv_error_maps_invalid_config_to_feature_not_supported() {
        let error = GatewayError::from(KrishivError::InvalidConfig {
            message: "bad".into(),
        });
        assert_eq!(error.sqlstate(), "0A000");
    }

    #[test]
    fn from_sql_error_uses_canonical_sqlstate() {
        let error = GatewayError::from(SqlError::EmptyQuery);
        assert_eq!(error.sqlstate(), "42000");
        assert!(!error.message().is_empty());
    }

    #[test]
    fn gateway_error_display_formats_sqlstate_and_message() {
        let error = GatewayError {
            sqlstate: "42000".into(),
            message: "bad query".into(),
        };
        let rendered = error.to_string();
        assert!(rendered.contains("[42000]"));
        assert!(rendered.contains("bad query"));
    }

    #[test]
    fn pooled_session_exposes_inner_session() {
        let pool = SessionPool::new_embedded(1).expect("pool");
        let borrowed = pool.acquire().expect("acquire");
        let _: &Session = borrowed.session().expect("session present");
    }
}
