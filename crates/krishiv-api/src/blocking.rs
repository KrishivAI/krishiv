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

    /// Drive an async operation to completion on this session's **owned**
    /// runtime — the single blocking primitive every method below goes through,
    /// so there is no hidden global runtime.
    ///
    /// Rejects unsafe runtime nesting instead of panicking: `Runtime::block_on`
    /// from a thread already driving a Tokio runtime panics with "Cannot start a
    /// runtime from within a runtime". Callers already inside async code should
    /// use the async [`Session`] API directly instead of `BlockingSession`.
    fn block<T>(&self, fut: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(KrishivError::Runtime {
                message: "BlockingSession cannot be used from within an active Tokio runtime; \
                          call the async Session API (e.g. DataFrame::collect_async().await) \
                          instead of BlockingSession when already in async code"
                    .to_string(),
            });
        }
        self.rt.block_on(fut)
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
        let df = self.block(self.inner.sql_async(query))?;
        self.collect(df)
    }

    /// Policy-enforced `sql_as` (requires `SessionBuilder::with_auth`), collected
    /// synchronously.
    pub fn sql_as(&self, api_key: &str, query: &str) -> Result<QueryResult> {
        let df = self.block(self.inner.sql_as_async(api_key, query))?;
        self.collect(df)
    }

    /// Collect a [`DataFrame`] synchronously using the owned runtime.
    pub fn collect(&self, df: DataFrame) -> Result<QueryResult> {
        self.block(df.collect_async())
    }

    /// Create a live table synchronously — the keystone surface through the
    /// sync facade (delegates to [`Session::create_live_table`]).
    pub fn create_live_table(
        &self,
        name: &str,
        query: &str,
        refresh: crate::Refresh,
    ) -> Result<()> {
        self.inner.create_live_table(name, query, refresh)
    }

    /// Register record batches as a named table synchronously.
    pub fn register_record_batches(
        &self,
        name: &str,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Result<()> {
        self.block(self.inner.register_record_batches_async(name, batches))
    }

    /// Register a Parquet file as a named table synchronously.
    pub fn register_parquet(
        &self,
        table_name: impl AsRef<str>,
        path: impl AsRef<std::path::Path>,
    ) -> Result<()> {
        self.block(self.inner.register_parquet_async(table_name, path))
    }

    /// Deregister (drop) a named table synchronously.
    pub fn deregister_table(&self, name: &str) -> Result<()> {
        self.inner.deregister_table(name)
    }

    /// Read a Parquet file into a [`DataFrame`] synchronously.
    pub fn read_parquet(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame> {
        self.block(self.inner.read_parquet_async(path))
    }

    /// Read a Parquet file with explicit reader options, synchronously.
    pub fn read_parquet_with_options(
        &self,
        path: impl AsRef<std::path::Path>,
        opts: krishiv_sql::ParquetReaderOptions,
    ) -> Result<DataFrame> {
        self.block(self.inner.read_parquet_with_options_async(path, opts))
    }

    /// Read a CSV file into a [`DataFrame`] synchronously.
    pub fn read_csv(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame> {
        self.block(self.inner.read_csv_async(path))
    }

    /// Read a CSV file with explicit header/delimiter options, synchronously.
    pub fn read_csv_with_options(
        &self,
        path: impl AsRef<std::path::Path>,
        has_header: bool,
        delimiter: u8,
    ) -> Result<DataFrame> {
        self.block(
            self.inner
                .read_csv_with_options_async(path, has_header, delimiter),
        )
    }

    /// Read a JSON file into a [`DataFrame`] synchronously.
    pub fn read_json(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame> {
        self.block(self.inner.read_json_async(path))
    }

    /// Read a Delta Lake table (optionally at a version) synchronously.
    pub fn read_delta(&self, path: impl AsRef<str>, version: Option<i64>) -> Result<DataFrame> {
        self.block(self.inner.read_delta_async(path, version))
    }

    /// Read a Hudi table synchronously.
    pub fn read_hudi(
        &self,
        path: impl AsRef<str>,
        query_type: krishiv_connectors::lakehouse::HudiQueryType,
        begin_instant: Option<&str>,
    ) -> Result<DataFrame> {
        self.block(self.inner.read_hudi_async(path, query_type, begin_instant))
    }

    /// Append a [`DataFrame`] to a Hudi table synchronously.
    pub fn write_hudi_append(
        &self,
        path: impl AsRef<std::path::Path>,
        dataframe: &DataFrame,
    ) -> Result<krishiv_connectors::lakehouse::HudiWriteResult> {
        self.block(self.inner.write_hudi_append_async(path, dataframe))
    }

    /// Upsert a [`DataFrame`] into a Hudi table synchronously.
    pub fn write_hudi_upsert(
        &self,
        path: impl AsRef<std::path::Path>,
        key_column: &str,
        dataframe: &DataFrame,
    ) -> Result<krishiv_connectors::lakehouse::HudiWriteResult> {
        self.block(
            self.inner
                .write_hudi_upsert_async(path, key_column, dataframe),
        )
    }

    /// Borrow the underlying async [`Session`].
    pub fn session(&self) -> &Session {
        &self.inner
    }

    /// API-11: Close the underlying session, clearing all registries and
    /// aborting background tasks. Because `BlockingSession` holds the session
    /// in an `Arc`, this attempts to unwrap it. If other references exist,
    /// the registries are cleared but the session object survives.
    pub fn close(self) {
        if let Ok(session) = Arc::try_unwrap(self.inner) {
            session.close();
        }
        // Runtime is dropped when `self` goes out of scope.
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

    #[test]
    fn blocking_session_covers_the_core_sync_facade() {
        // The single complete sync facade (Phase 61): BlockingSession must expose
        // a synchronous entry for every core data-plane operation. This list is
        // the *enforced* contract — a new core capability must be mirrored here so
        // the sync facade never silently lags the async Session. (The async-first
        // core flip + `_async`-twin deprecation + Python mirror are the remaining
        // structural residual of the one-sync/async-contract work.)
        let source = include_str!("blocking.rs");
        const CORE_FACADE: &[&str] = &[
            "sql",
            "sql_as",
            "collect",
            "create_live_table",
            "register_record_batches",
            "register_parquet",
            "deregister_table",
            "read_parquet",
            "read_parquet_with_options",
            "read_csv",
            "read_csv_with_options",
            "read_json",
            "read_delta",
            "read_hudi",
            "write_hudi_append",
            "write_hudi_upsert",
            "session",
            "runtime",
            "close",
        ];
        for method in CORE_FACADE {
            assert!(
                source.contains(&format!("pub fn {method}")),
                "BlockingSession is missing a core sync-facade method: `{method}`"
            );
        }
    }

    #[test]
    fn blocking_session_covers_live_table_facade() {
        // The sync facade reaches the Phase 61 keystone without an async
        // runtime: create a batch live table, query it, drop it — all blocking.
        let bs = BlockingSession::embedded().unwrap();
        bs.create_live_table(
            "bt",
            "SELECT 1 AS a UNION ALL SELECT 2 AS a",
            crate::Refresh::Batch,
        )
        .unwrap();
        assert_eq!(bs.sql("SELECT a FROM bt").unwrap().row_count(), 2);
        bs.deregister_table("bt").unwrap();
    }

    #[test]
    fn blocking_session_collect_rejects_nested_runtime() {
        // Calling BlockingSession::collect from a thread already driving a
        // Tokio runtime must return an error, not panic with "Cannot start a
        // runtime from within a runtime". Build and tear down `session` and
        // the probe runtime outside of any active async context (dropping a
        // `tokio::Runtime` from within an async context is itself a separate
        // panic) — only the `collect` call happens while nested.
        let session = BlockingSession::embedded().unwrap();
        let df = session.session().sql("SELECT 1 AS x").unwrap();
        let probe_rt = tokio::runtime::Runtime::new().unwrap();
        let err = probe_rt
            .block_on(async { session.collect(df) })
            .unwrap_err();
        assert!(err.to_string().contains("active Tokio runtime"));
    }
}
