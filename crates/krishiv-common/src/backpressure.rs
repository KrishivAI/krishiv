#![forbid(unsafe_code)]

//! E1.3 — Credit-based backpressure between streaming operators.
//!
//! Design: each operator-to-operator handoff has a `CreditGate` that tracks
//! how many bytes downstream is ready to accept.  The producer calls
//! `try_send(bytes)` before producing; the consumer calls `ack(bytes)` after
//! draining output.  When credits drop to zero the producer signals backpressure
//! upward via `BackpressureSignal`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Backpressure signal attached to a task output or operator boundary.
///
/// Carried in [`ExecutorTaskOutput`] so the coordinator knows whether to
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
        let ratio = available as f32 / capacity as f32;
        if ratio < 0.1 {
            Self::Throttle { fraction: 0.1 }
        } else if ratio < 0.5 {
            Self::Throttle { fraction: ratio }
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
/// All operations use `Relaxed` ordering — the gate is advisory.
#[derive(Debug)]
pub struct CreditGate {
    available: AtomicU64,
    capacity: u64,
}

impl CreditGate {
    /// Create a gate with `capacity` bytes of initial credit.
    pub fn new(capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            available: AtomicU64::new(capacity),
            capacity,
        })
    }

    /// Total capacity the gate was initialised with.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Current available credits.
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
    /// pause and retry after the consumer calls [`Self::ack`].
    pub fn try_send(&self, bytes: u64) -> bool {
        if bytes == 0 {
            return true;
        }
        let prev = self
            .available
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                if cur >= bytes {
                    Some(cur - bytes)
                } else {
                    None
                }
            });
        prev.is_ok()
    }

    /// Consumer: return `bytes` credits after draining output.
    ///
    /// Saturates at `capacity`.
    pub fn ack(&self, bytes: u64) {
        let _ = self
            .available
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some((cur + bytes).min(self.capacity))
            });
    }

    /// Reset to full capacity (e.g. on pipeline restart).
    pub fn reset(&self) {
        self.available.store(self.capacity, Ordering::Relaxed);
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
}
