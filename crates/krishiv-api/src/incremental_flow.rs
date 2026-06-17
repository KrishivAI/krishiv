#![forbid(unsafe_code)]

//! `IncrementalFlow` — Rust API for incremental view maintenance.
//!
//! Re-exports from `krishiv-ivm`, adding `From<IvmError> for KrishivError`
//! so callers using the `krishiv-api` error type continue to work unchanged.

pub use krishiv_ivm::{IncrementalFlow, IvmError, StepSummary};

use crate::error::KrishivError;

impl From<IvmError> for KrishivError {
    fn from(e: IvmError) -> Self {
        KrishivError::Runtime { message: e.to_string() }
    }
}
