#![forbid(unsafe_code)]

//! E1.3 — Credit-based backpressure between streaming operators.
//!
//! Design: each operator-to-operator handoff has a `CreditGate` that tracks
//! how many bytes downstream is ready to accept.  The producer calls
//! `try_send(bytes)` before producing; the consumer calls `ack(bytes)` after
//! draining output.  When credits drop to zero the producer signals backpressure
//! upward via `BackpressureSignal`.
//!
//! Producers that need to block until credits are available should use
//! `wait_for_credit` which parks on a `Notify` that the consumer wakes on
//! every `ack` call.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;

/// Backpressure signal attached to a task output or operator boundary.
///
/// Carried in executor task output metadata so the coordinator knows whether to
/// schedule more work (or wait) at the upstream edge of this operator.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum BackpressureSignal {
    /// Downstream is keeping up — no flow control needed.
    #[default]
    None,
    /// Downstream is partially full; slow down to `fraction` × current rate.
    Throttle {
        /// Fraction of the current throughput the producer should target
        /// (0.0 = stop completely, 1.0 = full speed).
        fraction: f32,
    },
    /// Downstream is at capacity — producer must stop until credits return.
    Pause,
}

impl BackpressureSignal {
    /// Derive a signal from available credits as a ratio of total capacity.
    pub fn from_credit_ratio(available: u64, capacity: u64) -> Self {
        if capacity == 0 || available == capacity {
            return Self::None;
        }
        if available == 0 {
            return Self::Pause;
        }
        let ratio = available as f64 / capacity as f64;
        if ratio < 0.1 {
            Self::Throttle { fraction: 0.1 }
        } else if ratio < 0.5 {
            Self::Throttle {
                fraction: ratio as f32,
            }
        } else {
            Self::None
        }
    }
}

/// Shared credit gate between a producer and consumer operator.
///
/// The consumer initialises the gate with `capacity` bytes of credit; the
/// producer deducts before writing, the consumer restores after draining.
///
/// Ordering: `try_send` and `ack` use `AcqRel` / `Acquire` so that all writes
/// the consumer completed before `ack` are visible to the producer after a
/// successful `try_send`.  Advisory reads (`available`, `signal`) use `Relaxed`.
///
/// Producers that must block until credits are available should call
/// `wait_for_credit` which parks on the embedded `Notify` and is woken by
/// every successful `ack`.
#[derive(Debug)]
pub struct CreditGate {
    available: AtomicU64,
    capacity: u64,
    /// Woken by `ack` so producers blocked in `wait_for_credit` are unparked.
    notify: Notify,
}

impl CreditGate {
    /// Create a gate with `capacity` bytes of initial credit.
    pub fn new(capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            available: AtomicU64::new(capacity),
            capacity,
            notify: Notify::new(),
        })
    }

    /// Total capacity the gate was initialised with.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Current available credits (advisory; may be stale).
    pub fn available(&self) -> u64 {
        self.available.load(Ordering::Relaxed)
    }

    /// Derive the current [`BackpressureSignal`] from credit availability.
    pub fn signal(&self) -> BackpressureSignal {
        BackpressureSignal::from_credit_ratio(self.available(), self.capacity)
    }

    /// Producer: try to deduct `bytes` credits.
    ///
    /// Returns `true` if the deduction was accepted (credits were available).
    /// Returns `false` if insufficient credits remain; the producer should
    /// park in `wait_for_credit` and retry.
    pub fn try_send(&self, bytes: u64) -> bool {
        if bytes == 0 {
            return true;
        }
        self.available
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                if cur >= bytes {
                    Some(cur - bytes)
                } else {
                    None
                }
            })
            .is_ok()
    }

    /// Consumer: return `bytes` credits after draining output.
    ///
    /// Saturates at `capacity` and wakes any producer parked in
    /// `wait_for_credit`.
    pub fn ack(&self, bytes: u64) {
        let _ = self
            .available
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                Some((cur + bytes).min(self.capacity))
            });
        self.notify.notify_waiters();
    }

    /// Reset to full capacity (e.g. on pipeline restart) and wake waiting producers.
    pub fn reset(&self) {
        self.available.store(self.capacity, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Park until credits are available, then return.
    ///
    /// Does NOT deduct credits — the caller must call `try_send` after waking.
    /// This avoids the thundering-herd problem: multiple producers all wake on
    /// a single `ack` and compete fairly via `try_send`.
    pub async fn wait_for_credit(&self) {
        loop {
            // Fast path: credits already available.
            if self.available.load(Ordering::Acquire) > 0 {
                return;
            }
            // Register the notification listener BEFORE re-checking credits so
            // we cannot miss a concurrent `ack`.
            let notified = self.notify.notified();
            if self.available.load(Ordering::Acquire) > 0 {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_capacity_returns_no_signal() {
        let g = CreditGate::new(1024);
        assert_eq!(g.signal(), BackpressureSignal::None);
    }

    #[test]
    fn exhausted_credits_returns_pause() {
        let g = CreditGate::new(100);
        assert!(g.try_send(100));
        assert_eq!(g.signal(), BackpressureSignal::Pause);
    }

    #[test]
    fn send_fails_when_insufficient() {
        let g = CreditGate::new(50);
        assert!(!g.try_send(100));
        assert_eq!(g.available(), 50); // unchanged
    }

    #[test]
    fn ack_restores_credits() {
        let g = CreditGate::new(100);
        g.try_send(80);
        assert_eq!(g.available(), 20);
        g.ack(80);
        assert_eq!(g.available(), 100);
    }

    #[test]
    fn ack_saturates_at_capacity() {
        let g = CreditGate::new(100);
        g.ack(999);
        assert_eq!(g.available(), 100);
    }

    #[test]
    fn throttle_signal_at_low_credits() {
        let g = CreditGate::new(100);
        g.try_send(95); // leaves 5 / 100 = 5%
        assert!(matches!(g.signal(), BackpressureSignal::Throttle { .. }));
    }

    #[test]
    fn backpressure_signal_is_default_none() {
        assert_eq!(BackpressureSignal::default(), BackpressureSignal::None);
    }

    #[test]
    fn reset_restores_full_capacity() {
        let g = CreditGate::new(200);
        g.try_send(200);
        assert_eq!(g.available(), 0);
        g.reset();
        assert_eq!(g.available(), 200);
    }

    #[tokio::test]
    async fn wait_for_credit_wakes_on_ack() {
        let gate = CreditGate::new(100);
        gate.try_send(100); // exhaust credits

        let gate2 = Arc::clone(&gate);
        let waker = tokio::spawn(async move {
            gate2.wait_for_credit().await;
            gate2.try_send(50)
        });

        // Give the task time to park.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        gate.ack(50);

        assert!(
            waker.await.unwrap(),
            "producer should acquire credits after ack"
        );
    }
}
