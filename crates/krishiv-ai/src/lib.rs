#![forbid(unsafe_code)]

//! Re-exports for vector sinks (from krishiv-connectors).

#[cfg(feature = "vector-sinks")]
pub use krishiv_connectors::vector::*;
