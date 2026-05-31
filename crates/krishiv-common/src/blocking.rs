//! Async-safe utilities for running blocking or synchronous work in Tokio runtimes.

/// Safely execute synchronous / blocking code inside a Tokio async context.
///
/// If a multi-threaded Tokio runtime is active, it yields the current worker
/// thread using `tokio::task::block_in_place` so that other async tasks are
/// not starved. Otherwise, it executes the closure directly.
pub fn run_blocking_safely<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return tokio::task::block_in_place(f);
        }
    }
    f()
}
