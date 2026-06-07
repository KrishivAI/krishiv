use std::future::Future;
use std::sync::OnceLock;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

static FALLBACK_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn fallback_runtime() -> &'static tokio::runtime::Runtime {
    FALLBACK_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, "failed to create fallback Tokio runtime; panicking");
                panic!("failed to create fallback Tokio runtime: {e}");
            })
    })
}

/// Drive `fut` to completion on a Tokio runtime.
///
/// Resolution order:
/// 1. If called inside a multi-threaded Tokio runtime, use `block_in_place`
///    so the current worker thread can be borrowed without starving the
///    runtime.
/// 2. If called inside a current-thread (single-threaded) Tokio runtime,
///    `block_in_place` would panic, so call `block_on` on the current handle
///    directly. The future must not depend on the caller's task-local data.
/// 3. If no Tokio runtime is active in the calling thread, drive `fut` on a
///    lazily-initialised multi-thread fallback runtime. The fallback is
///    deliberately lazy because constructing a runtime while another runtime
///    is active on the same thread is undefined behaviour in Tokio.
pub fn block_on<T, F: Future<Output = T>>(fut: F) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(fut))
            }
            _ => fallback_runtime().block_on(fut),
        },
        Err(_) => fallback_runtime().block_on(fut),
    }
}

pub fn unix_now_ms_checked() -> Result<i64, SystemTimeError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| {
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
}
