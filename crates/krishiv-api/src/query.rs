//! Typed query lifecycle — Phase E.
//!
//! [`QueryHandle`] is the single handle returned by [`DataFrame::submit_async`].
//! It carries the query ID, live status, progress, and cancellation.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::watch;

use crate::error::{KrishivError, Result};
use crate::types::QueryResult;

static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);

/// Opaque identifier for a submitted query.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryId(u64);

impl QueryId {
    /// Allocate the next unique query ID.
    pub fn next() -> Self {
        QueryId(NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed))
    }

    /// ID as a u64.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for QueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "q{}", self.0)
    }
}

/// Completion payload carried inside [`QueryStatus::Completed`].
#[derive(Debug, Clone)]
pub struct QueryCompletion {
    pub result: QueryResult,
    pub rows: u64,
}

/// Live status of a submitted query.
#[derive(Debug, Clone)]
pub enum QueryStatus {
    /// Accepted but execution has not started.
    Pending,
    /// Execution is in progress.
    Running { rows_so_far: u64 },
    /// Query completed successfully.
    Completed(QueryCompletion),
    /// Query was cancelled by the caller.
    Cancelled,
    /// Query failed with an error message.
    Failed(String),
}

impl QueryStatus {
    /// `true` if the query has reached a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed(_) | Self::Cancelled | Self::Failed(_))
    }

    /// `true` if the query completed successfully.
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed(_))
    }
}

/// Progress snapshot for a running query.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryProgress {
    /// Rows scanned from input sources.
    pub rows_scanned: u64,
    /// Rows emitted to output.
    pub rows_emitted: u64,
    /// Fractional progress in [0.0, 1.0], or `None` when unknown.
    pub fraction_complete: Option<f64>,
}

/// Shared state between [`QueryHandle`] and the executor task.
#[derive(Debug, Clone)]
pub(crate) struct QueryState {
    pub(crate) status: QueryStatus,
    pub(crate) progress: QueryProgress,
}

/// Handle returned by [`DataFrame::submit_async`].
///
/// The handle allows callers to:
/// - poll the current [`QueryStatus`] (non-blocking),
/// - read live [`QueryProgress`] (non-blocking),
/// - cancel the query,
/// - `await` completion and collect the result.
pub struct QueryHandle {
    pub(crate) id: QueryId,
    pub(crate) state_rx: watch::Receiver<QueryState>,
    pub(crate) cancel_tx: Arc<watch::Sender<bool>>,
    /// Background task handle; set after construction when the driver task is
    /// spawned by `DataFrame::submit_async`. `None` only in tests.
    pub(crate) _task: Option<tokio::task::JoinHandle<()>>,
}

/// Driver held by the executor task — not part of the public API.
pub(crate) struct QueryDriver {
    pub(crate) state_tx: watch::Sender<QueryState>,
    pub(crate) cancel_rx: watch::Receiver<bool>,
}

impl QueryHandle {
    /// Create a linked `(handle, driver)` pair.  The caller (typically
    /// `DataFrame::submit_async`) spawns the background task and sets
    /// `handle._task` afterwards.
    pub(crate) fn new(id: QueryId) -> (Self, QueryDriver) {
        let (state_tx, state_rx) = watch::channel(QueryState {
            status: QueryStatus::Pending,
            progress: QueryProgress::default(),
        });
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let handle = QueryHandle {
            id,
            state_rx,
            cancel_tx: Arc::new(cancel_tx),
            _task: None,
        };
        let driver = QueryDriver {
            state_tx,
            cancel_rx,
        };
        (handle, driver)
    }

    /// The query's unique identifier.
    pub fn id(&self) -> &QueryId {
        &self.id
    }

    /// Latest known status snapshot (non-blocking).
    pub fn status(&self) -> QueryStatus {
        self.state_rx.borrow().status.clone()
    }

    /// Latest progress snapshot (non-blocking).
    pub fn progress(&self) -> QueryProgress {
        self.state_rx.borrow().progress
    }

    /// Request cancellation. Returns immediately; the task may not stop instantly.
    pub fn cancel(&self) {
        let _ = self.cancel_tx.send(true);
    }

    /// Await completion and return the [`QueryResult`].
    ///
    /// Returns `Err` if the query failed or was cancelled.
    pub async fn wait(mut self) -> Result<QueryResult> {
        loop {
            {
                let state = self.state_rx.borrow();
                match &state.status {
                    QueryStatus::Completed(c) => return Ok(c.result.clone()),
                    QueryStatus::Cancelled => {
                        return Err(KrishivError::Runtime {
                            message: format!("query {} was cancelled", self.id),
                        });
                    }
                    QueryStatus::Failed(msg) => {
                        return Err(KrishivError::Runtime {
                            message: msg.clone(),
                        });
                    }
                    _ => {}
                }
            }
            if self.state_rx.changed().await.is_err() {
                // The watch::Sender was dropped — either the task panicked or
                // was aborted without setting a terminal status.  Check the
                // task handle to distinguish a panic from a clean abort.
                if let Some(task) = self._task.take()
                    && task.is_finished()
                {
                    match task.await {
                        Ok(()) => {
                            // Task completed cleanly but never set status —
                            // treat as an internal bug rather than silence.
                            return Err(KrishivError::Runtime {
                                message: format!(
                                    "query {} driver exited without setting a terminal status",
                                    self.id
                                ),
                            });
                        }
                        Err(join_err) if join_err.is_panic() => {
                            return Err(KrishivError::Runtime {
                                message: format!("query {} task panicked: {:?}", self.id, join_err),
                            });
                        }
                        Err(_) => {
                            return Err(KrishivError::Runtime {
                                message: format!("query {} task was cancelled", self.id),
                            });
                        }
                    }
                }
                return Err(KrishivError::Runtime {
                    message: format!("query {} driver dropped without completing", self.id),
                });
            }
        }
    }

    /// Await completion with a timeout.
    pub async fn wait_timeout(self, timeout: std::time::Duration) -> Result<QueryResult> {
        tokio::time::timeout(timeout, self.wait())
            .await
            .map_err(|_| KrishivError::Runtime {
                message: "query timed out".to_string(),
            })?
    }
}

impl std::fmt::Debug for QueryHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryHandle")
            .field("id", &self.id)
            .field("status", &self.state_rx.borrow().status)
            .finish_non_exhaustive()
    }
}

impl QueryDriver {
    /// Signal that execution has started.
    pub(crate) fn set_running(&self) {
        self.state_tx.send_modify(|s| {
            s.status = QueryStatus::Running { rows_so_far: 0 };
        });
    }

    /// Update row-level progress mid-execution.
    pub(crate) fn update_progress(&self, rows_scanned: u64, rows_emitted: u64) {
        self.state_tx.send_modify(|s| {
            s.progress.rows_scanned = rows_scanned;
            s.progress.rows_emitted = rows_emitted;
            if let QueryStatus::Running { rows_so_far } = &mut s.status {
                *rows_so_far = rows_emitted;
            }
        });
    }

    /// Signal successful completion.
    pub(crate) fn set_completed(&self, result: QueryResult) {
        let rows = result.row_count() as u64;
        self.state_tx.send_modify(|s| {
            s.status = QueryStatus::Completed(QueryCompletion { result, rows });
            s.progress.rows_emitted = rows;
        });
    }

    /// Signal failure.
    pub(crate) fn set_failed(&self, message: impl Into<String>) {
        self.state_tx.send_modify(|s| {
            s.status = QueryStatus::Failed(message.into());
        });
    }

    /// Returns `true` if cancellation was requested.
    pub(crate) fn is_cancelled(&self) -> bool {
        *self.cancel_rx.borrow()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn query_handle_pending_to_completed() {
        let id = QueryId::next();
        let (handle, driver) = QueryHandle::new(id.clone());
        assert!(matches!(handle.status(), QueryStatus::Pending));
        driver.set_running();
        assert!(matches!(handle.status(), QueryStatus::Running { .. }));

        let batches = vec![];
        let result = QueryResult::new(batches);
        driver.set_completed(result);

        let r = handle.wait().await.unwrap();
        assert_eq!(r.row_count(), 0);
    }

    #[tokio::test]
    async fn query_handle_cancel_propagates() {
        let id = QueryId::next();
        let (handle, driver) = QueryHandle::new(id);
        assert!(!driver.is_cancelled());
        handle.cancel();
        assert!(driver.is_cancelled());
    }

    #[tokio::test]
    async fn query_handle_failure_returns_err() {
        let id = QueryId::next();
        let (handle, driver) = QueryHandle::new(id);
        driver.set_failed("something went wrong");
        let err = handle.wait().await.unwrap_err();
        assert!(err.to_string().contains("something went wrong"));
    }

    #[tokio::test]
    async fn query_id_is_unique() {
        let a = QueryId::next();
        let b = QueryId::next();
        assert_ne!(a, b);
        assert!(b.as_u64() > a.as_u64());
    }
}
