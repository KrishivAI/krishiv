//! Lakehouse connector integration notes.
//!
//! Iceberg, Delta, and Hudi table I/O remain in this crate because the
//! lakehouse stack is shared by SQL, exec, and CDC paths. Use
//! [`krishiv_connectors::ConnectorKind`] for discovery and keep format-specific
//! writes in `crate::lakehouse` APIs.

/// Canonical configuration kind strings for lakehouse integrations.
pub const ICEBERG_KIND: &str = "iceberg";
pub const DELTA_KIND: &str = "delta";
pub const HUDI_KIND: &str = "hudi";
