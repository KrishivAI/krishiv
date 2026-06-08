#![forbid(unsafe_code)]
//! Facade crate re-exporting lakehouse implementations from `krishiv-connectors`.
//!
//! New code may depend on `krishiv_connectors::lakehouse` directly; this crate
//! preserves the historical `krishiv_lakehouse` import path.

pub use krishiv_connectors::lakehouse::*;
