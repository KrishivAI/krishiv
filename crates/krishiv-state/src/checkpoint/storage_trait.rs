use crate::checkpoint::metadata::{CheckpointError, CheckpointResult};

// ── CheckpointStorage trait ───────────────────────────────────────────────────

/// Storage backend for checkpoint data.
///
/// The async methods are the primary API for scheduler/executor paths that
/// already run inside Tokio. Synchronous methods remain as compatibility
/// wrappers for tests and blocking-friendly call sites.
#[async_trait::async_trait]
pub trait CheckpointStorage: Send + Sync {
    /// Async write `data` to `path`.  Overwrites if it already exists.
    async fn write_bytes_async(&self, path: &str, data: &[u8]) -> CheckpointResult<()>;

    /// Async read the bytes stored at `path`. Returns `None` if absent.
    async fn read_bytes_async(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>>;

    /// Async list immediate children of `prefix` one level deep.
    async fn list_dir_async(&self, prefix: &str) -> CheckpointResult<Vec<String>>;

    /// Async recursively delete everything under `prefix`.
    async fn delete_prefix_async(&self, prefix: &str) -> CheckpointResult<()>;

    /// Write `data` to `path`.  Overwrites if it already exists.
    ///
    /// Implementations should write atomically (temp-file + rename) to prevent
    /// partial reads of in-progress writes.
    fn write_bytes(&self, path: &str, data: &[u8]) -> CheckpointResult<()> {
        run_blocking_on_tokio("checkpoint write_bytes", self.write_bytes_async(path, data))
    }

    /// Read the bytes stored at `path`.  Returns `None` if the path does not exist.
    fn read_bytes(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>> {
        run_blocking_on_tokio("checkpoint read_bytes", self.read_bytes_async(path))
    }

    /// List immediate children of `prefix` (directory listing one level deep).
    ///
    /// Returns relative names (not full paths).  Returns an empty `Vec` if the
    /// prefix does not exist.
    fn list_dir(&self, prefix: &str) -> CheckpointResult<Vec<String>> {
        run_blocking_on_tokio("checkpoint list_dir", self.list_dir_async(prefix))
    }

    /// Recursively delete everything under `prefix`.  No-op if `prefix` does
    /// not exist.
    fn delete_prefix(&self, prefix: &str) -> CheckpointResult<()> {
        run_blocking_on_tokio("checkpoint delete_prefix", self.delete_prefix_async(prefix))
    }
}

/// Run an async block from a synchronous `CheckpointStorage` impl without
/// deadlocking the Tokio runtime.
///
/// The previous object-store backend used `futures::executor::block_on`, which
/// parks the current thread without yielding to Tokio.  If the inner future
/// awaits a Tokio resource (timer / TCP socket — both used by `reqwest`),
/// the worker thread deadlocks (D4).
///
/// This helper uses `block_in_place` when called from inside a multi-thread
/// Tokio runtime, falls back to a short-lived runtime when no runtime is
/// active, and returns a clear `Storage` error when called from a
/// `current_thread` runtime (where neither approach is safe).
pub fn run_blocking_on_tokio<F, T>(label: &'static str, fut: F) -> CheckpointResult<T>
where
    F: std::future::Future<Output = CheckpointResult<T>> + Send,
    T: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            // We are inside a Tokio runtime.  block_in_place is only legal on
            // multi-thread runtimes; current_thread will panic.  Detect that
            // and return a clear error instead.
            match handle.runtime_flavor() {
                tokio::runtime::RuntimeFlavor::MultiThread => {
                    tokio::task::block_in_place(|| handle.block_on(fut))
                }
                _ => Err(CheckpointError::Storage {
                    message: format!(
                        "{label}: cannot block on a current_thread Tokio runtime; \
                         call from a multi-thread runtime (#[tokio::main(flavor = \"multi_thread\")]) \
                         or use the async API directly"
                    ),
                }),
            }
        }
        Err(_) => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(1)
                .build()
                .map_err(|e| CheckpointError::Storage {
                    message: format!("{label}: failed to build temporary Tokio runtime: {e}"),
                })?;
            rt.block_on(fut)
        }
    }
}

pub(crate) fn uuid_simple() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
