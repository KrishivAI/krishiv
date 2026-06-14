//! Explicit blocking facade — Phase E.
//!
//! [`BlockingSession`] wraps a [`Session`] and an owned `tokio::Runtime`
//! so all methods are purely synchronous with no hidden global runtime.
//!
//! Use this in CLI tools, scripts, or any context that doesn't run under
//! an async executor and wants a straightforward synchronous API.

use std::sync::Arc;

use tokio::runtime::Runtime;

use crate::DataFrame;
use crate::error::{KrishivError, Result};
use crate::session::{Session, SessionBuilder};
use crate::types::QueryResult;

/// Synchronous session backed by an owned Tokio runtime.
///
/// All blocking operations go through this runtime — no hidden global
/// runtime is consulted. Construct with [`BlockingSession::embedded`]
/// or [`BlockingSession::from_env`].
pub struct BlockingSession {
    inner: Arc<Session>,
    rt: Arc<Runtime>,
}

impl BlockingSession {
    fn build(session: Session) -> Result<Self> {
        let rt = Runtime::new().map_err(|e| KrishivError::Runtime {
            message: format!("failed to create blocking runtime: {e}"),
        })?;
        Ok(Self {
            inner: Arc::new(session),
            rt: Arc::new(rt),
        })
    }

    /// Create an embedded (in-process) blocking session.
    pub fn embedded() -> Result<Self> {
        Self::build(SessionBuilder::new().build()?)
    }

    /// Create a session from environment variables.
    ///
    /// Reads `KRISHIV_MODE`, `KRISHIV_COORDINATOR_URL`, and `KRISHIV_REMOTE_EXEC`.
    pub fn from_env() -> Result<Self> {
        Self::build(Session::from_env()?)
    }

    /// Create a session connected to a remote coordinator.
    pub fn connect(coordinator_url: impl Into<String>) -> Result<Self> {
        Self::build(
            SessionBuilder::new()
                .with_coordinator(coordinator_url)
                .with_remote_execution(true)
                .build()?,
        )
    }

    /// Execute a SQL query and collect results synchronously.
    pub fn sql(&self, query: &str) -> Result<QueryResult> {
        let df = self.inner.sql(query)?;
        self.collect(df)
    }

    /// Collect a [`DataFrame`] synchronously using the owned runtime.
    pub fn collect(&self, df: DataFrame) -> Result<QueryResult> {
        self.rt.block_on(df.collect_async())
    }

    /// Borrow the underlying async [`Session`].
    pub fn session(&self) -> &Session {
        &self.inner
    }

    /// Borrow the underlying Tokio [`Runtime`].
    pub fn runtime(&self) -> &Runtime {
        &self.rt
    }
}

impl std::fmt::Debug for BlockingSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockingSession")
            .field("session", &self.inner)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocking_session_embedded_sql() {
        let session = BlockingSession::embedded().unwrap();
        let result = session.sql("SELECT 42 AS n").unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn blocking_session_collect_dataframe() {
        let session = BlockingSession::embedded().unwrap();
        let df = session.session().sql("SELECT 1 AS x, 2 AS y").unwrap();
        let result = session.collect(df).unwrap();
        assert_eq!(result.row_count(), 1);
    }
}
