//! Graceful-shutdown RPC drain barrier.
//!
//! Coordinator demotion must wait for in-flight gRPC handlers (heartbeats,
//! task updates, management calls) to finish before releasing leadership —
//! otherwise the new leader can observe stale state mid-write (R11). Rather
//! than a fixed sleep, [`InFlightTracker`] counts active calls via a
//! [`tower::Layer`] applied to the whole gRPC server, and [`InFlightTracker::drain`]
//! waits for that count to reach zero (bounded by a timeout fallback in case a
//! handler is wedged).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;
use tower::{Layer, Service};

/// Shared counter of in-flight gRPC calls, plus a notification used to wake
/// drain waiters as soon as the count reaches zero.
#[derive(Clone, Debug)]
pub struct InFlightTracker {
    count: Arc<AtomicUsize>,
    idle: Arc<Notify>,
}

impl InFlightTracker {
    /// Create a tracker with no in-flight calls.
    pub fn new() -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
            idle: Arc::new(Notify::new()),
        }
    }

    /// Number of calls currently being handled.
    pub fn active_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    fn enter(&self) -> InFlightGuard {
        self.count.fetch_add(1, Ordering::SeqCst);
        InFlightGuard {
            count: Arc::clone(&self.count),
            idle: Arc::clone(&self.idle),
        }
    }

    /// Wait until no gRPC calls are in flight, or `timeout` elapses.
    ///
    /// Returns `true` if the tracker drained cleanly, `false` if the timeout
    /// fired with calls still active (a handler is likely wedged; the caller
    /// should log and proceed rather than block shutdown indefinitely).
    pub async fn drain(&self, timeout: Duration) -> bool {
        if self.active_count() == 0 {
            return true;
        }
        let deadline = Instant::now() + timeout;
        loop {
            if self.active_count() == 0 {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            // Re-check after either a wakeup or the remaining timeout, since a
            // notification can race with a fresh call starting just after the
            // count reached zero.
            let _ = tokio::time::timeout(remaining, self.idle.notified()).await;
        }
    }
}

impl Default for InFlightTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard decrementing the in-flight count on drop — covers normal
/// completion, error returns, and task cancellation alike.
struct InFlightGuard {
    count: Arc<AtomicUsize>,
    idle: Arc<Notify>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if self.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.idle.notify_waiters();
        }
    }
}

/// [`tower::Layer`] that wraps a service so every call increments
/// [`InFlightTracker`] for its duration.
#[derive(Clone, Debug)]
pub struct InFlightLayer {
    tracker: InFlightTracker,
}

impl InFlightLayer {
    /// Create a layer reporting into `tracker`.
    pub fn new(tracker: InFlightTracker) -> Self {
        Self { tracker }
    }
}

impl<S> Layer<S> for InFlightLayer {
    type Service = InFlightService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        InFlightService {
            inner,
            tracker: self.tracker.clone(),
        }
    }
}

/// Service wrapper produced by [`InFlightLayer`].
#[derive(Clone, Debug)]
pub struct InFlightService<S> {
    inner: S,
    tracker: InFlightTracker,
}

impl<S, Req> Service<Req> for InFlightService<S>
where
    S: Service<Req> + Send + 'static,
    S::Future: Send + 'static,
    Req: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let guard = self.tracker.enter();
        let fut = self.inner.call(req);
        Box::pin(async move {
            let result = fut.await;
            drop(guard);
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    #[derive(Clone)]
    struct Echo;

    impl Service<u32> for Echo {
        type Response = u32;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<u32, Infallible>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: u32) -> Self::Future {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(req)
            })
        }
    }

    #[tokio::test]
    async fn drain_returns_immediately_when_idle() {
        let tracker = InFlightTracker::new();
        assert_eq!(tracker.active_count(), 0);
        assert!(tracker.drain(Duration::from_millis(10)).await);
    }

    #[tokio::test]
    async fn drain_waits_for_in_flight_calls_to_complete() {
        let tracker = InFlightTracker::new();
        let layer = InFlightLayer::new(tracker.clone());
        let mut svc = layer.layer(Echo);

        let mut calls = Vec::new();
        for i in 0..4u32 {
            calls.push(svc.call(i));
        }
        assert_eq!(tracker.active_count(), 4);

        let drain = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.drain(Duration::from_secs(5)).await }
        });

        for call in calls {
            call.await.unwrap();
        }

        assert!(drain.await.unwrap(), "drain must report clean completion");
        assert_eq!(tracker.active_count(), 0);
    }

    #[tokio::test]
    async fn drain_times_out_when_a_call_is_wedged() {
        let tracker = InFlightTracker::new();
        let _guard_count = tracker.enter();
        let drained = tracker.drain(Duration::from_millis(50)).await;
        assert!(!drained, "drain must report timeout when calls remain active");
    }
}
