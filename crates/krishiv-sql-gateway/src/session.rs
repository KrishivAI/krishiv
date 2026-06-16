use std::sync::{Arc, Mutex};

use krishiv_api::{BlockingSession, DataFrame, Session, SessionBuilder};

use crate::error::{GatewayError, GatewayQueryResult, GatewayResult};

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

// ── SessionPool ───────────────────────────────────────────────────────────────

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
            let mut guard = self.pool.lock().expect("session pool lock");
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
        self.pool.lock().expect("session pool lock").len()
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
        self.session.as_ref().expect("session present").execute_sql(query)
    }

    pub fn collect(&self, dataframe: DataFrame) -> GatewayResult<GatewayQueryResult> {
        self.session.as_ref().expect("session present").collect(dataframe)
    }

    pub fn session(&self) -> &Session {
        self.session.as_ref().expect("session present").session()
    }
}

impl Drop for PooledSession<'_> {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let mut guard = self.pool.lock().expect("session pool lock");
            if guard.len() < self.capacity {
                guard.push(session);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
