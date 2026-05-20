#![forbid(unsafe_code)]
//! Chaos test utilities for Krishiv R10.
//!
//! Provides deterministic fault injection helpers used by chaos integration tests.

/// Fault injection mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultMode {
    /// Introduce a delay of `duration_ms` milliseconds.
    Delay { duration_ms: u64 },
    /// Return an error instead of completing the operation.
    Error { message: String },
    /// Drop the operation silently (no response).
    Drop,
    /// Complete normally (no fault).
    None,
}

/// Deterministic fault injector that cycles through a list of faults by call index.
pub struct FaultInjector {
    faults: Vec<FaultMode>,
    call_count: std::sync::atomic::AtomicUsize,
}

impl FaultInjector {
    pub fn new(faults: Vec<FaultMode>) -> Self {
        Self {
            faults,
            call_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Return the fault for the current call, then advance the counter.
    /// Wraps around when all faults have been exhausted.
    pub fn next_fault(&self) -> &FaultMode {
        if self.faults.is_empty() {
            return &FaultMode::None;
        }
        let idx = self
            .call_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        &self.faults[idx % self.faults.len()]
    }
}
