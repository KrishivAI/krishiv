#![forbid(unsafe_code)]

use std::future::Future;
use std::sync::LazyLock;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

static FALLBACK_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Runtime::new().expect("failed to create fallback Tokio runtime")
});

pub fn block_on<T, F: Future<Output = T>>(fut: F) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
        Err(_) => FALLBACK_RUNTIME.block_on(fut),
    }
}

pub fn unix_now_ms_checked() -> Result<i64, SystemTimeError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

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
