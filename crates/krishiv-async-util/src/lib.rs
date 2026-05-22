#![forbid(unsafe_code)]

//! Shared async utilities: panic-safe runtime bridging and system-time helpers.
//!
//! All crates that need a sync-to-async bridge or a Unix timestamp should import
//! from here rather than reinventing these patterns.

use std::future::Future;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

/// Block on `fut`, safely bridging a synchronous caller into an async context.
///
/// When called from inside an existing Tokio runtime (e.g. inside a Tokio task
/// or inside a `#[tokio::test]`), this uses `block_in_place` so the current
/// thread parks without spawning a second runtime — which would panic.
///
/// When called with no active runtime (e.g. from `main()` or a `#[test]`),
/// this creates a short-lived multi-thread runtime for the duration of the
/// call.
pub fn block_on<T, F: Future<Output = T>>(fut: F) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
        Err(_) => tokio::runtime::Runtime::new()
            .expect("failed to create Tokio runtime for block_on")
            .block_on(fut),
    }
}

/// Return the current Unix timestamp in milliseconds, or an error if the
/// system clock is before the Unix epoch.
pub fn unix_now_ms_checked() -> Result<i64, SystemTimeError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Return the current Unix timestamp in milliseconds.
///
/// Returns `0` if the clock reports a time before the Unix epoch (should not
/// happen in production; avoids panics in environments with mocked clocks).
pub fn unix_now_ms() -> i64 {
    unix_now_ms_checked().unwrap_or(0)
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn block_on_works_inside_tokio_runtime() {
        let result = block_on(async { 42u32 });
        assert_eq!(result, 42);
    }
}
