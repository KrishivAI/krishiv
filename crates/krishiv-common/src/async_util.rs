//! Tokio/blocking bridge and wall-clock helpers for sync call sites.

use std::future::Future;
use std::sync::OnceLock;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

/// Env var that overrides the fallback runtime's worker-thread count.
///
/// Default: number of logical CPUs capped at 4 so the fallback runtime does
/// not monopolise cores in embedded/test workloads.  Set to `"0"` to let
/// Tokio default to the CPU count without a cap.
const FALLBACK_RUNTIME_THREADS_ENV: &str = "KRISHIV_FALLBACK_RUNTIME_THREADS";

static FALLBACK_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn fallback_runtime() -> &'static tokio::runtime::Runtime {
    FALLBACK_RUNTIME.get_or_init(|| {
        let threads = std::env::var(FALLBACK_RUNTIME_THREADS_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or_else(|| {
                // Default: min(cpu_count, 4) — avoids monopolising the host
                // under embedded workloads while providing enough parallelism
                // for concurrent block_on callers (bottleneck B3).
                std::thread::available_parallelism()
                    .map(|n| n.get().min(4))
                    .unwrap_or(4)
            });
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all();
        if threads > 0 {
            builder.worker_threads(threads);
        }
        builder.build().unwrap_or_else(|e| {
            tracing::error!(error = %e, "failed to create fallback Tokio runtime; aborting process");
            std::process::abort()
        })
    })
}

/// Drive `fut` to completion on a Tokio runtime.
///
/// Resolution order:
/// 1. If called inside a multi-threaded Tokio runtime, use `block_in_place`
///    so the current worker thread can be borrowed without starving the
///    runtime.
/// 2. If called inside a current-thread (single-threaded) Tokio runtime, hop
///    to a fresh OS thread with no Tokio context and drive `fut` there on the
///    lazily-initialised fallback multi-thread runtime. `block_in_place`
///    cannot be used here (it requires a multi-threaded runtime), and calling
///    `.block_on` on *any* runtime — even a separate fallback one — from this
///    thread would still panic with "Cannot start a runtime from within a
///    runtime": Tokio's nesting guard is a per-OS-thread flag, not a
///    per-runtime-instance one, so only a thread with no entered runtime at
///    all can safely call `block_on`.
/// 3. If no Tokio runtime is active in the calling thread, drive `fut` on a
///    lazily-initialised multi-thread fallback runtime. The fallback is
///    deliberately lazy because constructing a runtime while another runtime
///    is active on the same thread is undefined behaviour in Tokio.
pub fn block_on<T: Send, F: Future<Output = T> + Send>(fut: F) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(fut))
            }
            _ => std::thread::scope(|scope| {
                scope
                    .spawn(|| fallback_runtime().block_on(fut))
                    .join()
                    .unwrap_or_else(|e| std::panic::resume_unwind(e))
            }),
        },
        Err(_) => fallback_runtime().block_on(fut),
    }
}

pub fn unix_now_ms_checked() -> Result<i64, SystemTimeError> {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| {
        i64::try_from(d.as_millis()).unwrap_or_else(|_| {
            tracing::warn!(
                "unix_now_ms_checked: timestamp exceeds i64::MAX milliseconds; clamping"
            );
            i64::MAX
        })
    })
}

pub fn unix_now_ms() -> i64 {
    unix_now_ms_checked().unwrap_or_else(|e| {
        tracing::warn!(error = %e, "system clock before UNIX epoch; returning 0");
        0
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_now_ms_is_positive() {
        let now = unix_now_ms();
        assert!(now > 0, "unix_now_ms must be positive, got {now}");
    }

    #[test]
    fn unix_now_ms_checked_is_ok() {
        assert!(unix_now_ms_checked().is_ok());
    }

    #[test]
    fn block_on_works_outside_tokio_runtime() {
        let result = block_on(async { 42u32 });
        assert_eq!(result, 42);
    }

    #[test]
    fn block_on_works_inside_multi_thread_tokio_runtime_via_spawn() {
        // Simulate the production call site: a sync method on a tokio worker
        // thread that needs to drive a future. The block_in_place path is
        // exercised by spawning the call onto the runtime's worker threads
        // and joining the result.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(async {
            tokio::task::spawn_blocking(|| block_on(async { 42u32 }))
                .await
                .unwrap()
        });
        assert_eq!(result, 42);
    }

    #[test]
    fn block_on_works_inside_current_thread_tokio_runtime() {
        // Regression test: calling `block_on` synchronously from within an
        // *async* fn running on a current-thread (single-threaded) runtime —
        // e.g. the default `#[tokio::test]` flavor, or any
        // `#[tokio::main(flavor = "current_thread")]` app — used to panic
        // ("Cannot start a runtime from within a runtime") because the old
        // fallback path called `.block_on` on a *different* runtime object
        // without first releasing this thread from its already-entered
        // runtime context. Tokio's nesting guard is per-OS-thread, not
        // per-runtime-instance, so that didn't actually avoid the panic.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(async { block_on(async { 42u32 }) });
        assert_eq!(result, 42);
    }
}
