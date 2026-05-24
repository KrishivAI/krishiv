#![forbid(unsafe_code)]

//! # Krishiv
//!
//! Hybrid batch and streaming compute engine built on Apache Arrow and DataFusion.
//!
//! This crate is the **single user-facing entry point** for Rust applications.
//! All internal crates (`krishiv-api`, `krishiv-sql`, `krishiv-scheduler`, …)
//! are implementation details that you never need to add to your `Cargo.toml`.
//!
//! ## Quick start — batch SQL
//!
//! ```rust,no_run
//! use krishiv::{Session, Result};
//!
//! fn main() -> Result<()> {
//!     let session = Session::builder().build()?;
//!     session.register_parquet("orders", "./orders.parquet")?;
//!     let df = session.sql("SELECT region, sum(amount) FROM orders GROUP BY region")?;
//!     println!("{}", df.collect()?.pretty()?);
//!     Ok(())
//! }
//! ```
//!
//! ## Quick start — local streaming
//!
//! ```rust,no_run
//! use krishiv::{Session, StreamBatch, WatermarkSpec, Result};
//! use arrow::record_batch::RecordBatch;
//!
//! fn main() -> Result<()> {
//!     let session = Session::builder().build()?;
//!     let batch: RecordBatch = todo!("build your Arrow batch");
//!     let stream = session
//!         .memory_stream("events", vec![StreamBatch::new(0, batch)])
//!         .key_by("user_id")
//!         .with_event_time("event_ts")
//!         .watermark(WatermarkSpec::fixed_lag_ms(5_000))
//!         .tumbling_window(60_000);
//!     let _ = stream;   // wire into an execution backend in R12+
//!     Ok(())
//! }
//! ```
//!
//! ## Policy-enforced SQL
//!
//! ```rust,ignore
//! use krishiv::{Session, SessionBuilder, Result};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let session = SessionBuilder::new()
//!         .with_auth(Arc::new(my_auth_provider))
//!         .with_policy(Arc::new(my_policy_hook))
//!         .build()?;
//!     let df = session.sql_as("api-key-xyz", "SELECT * FROM customers").await?;
//!     println!("{}", df.collect()?.pretty()?);
//!     Ok(())
//! }
//! ```

// ── Session API ───────────────────────────────────────────────────────────────

pub use krishiv_api::{
    DataFrame, ExecutionMode, KeyedStream, KrishivError, MultiSourceWatermarkSpec, QueryResult,
    RecordBatch, Session, SessionBuilder, SessionWindowedStream, SlidingWindowedStream,
    StateTtlConfig, Stream, StreamBatch, StreamMode, WatermarkSpec, WindowedStream,
};

// Arrow schema/type primitives re-exported so users never import `arrow` directly.
pub use krishiv_api::{DataType, Field, Schema, SchemaRef};

// ── UDF API ───────────────────────────────────────────────────────────────────

pub use krishiv_udf::{AggregateUdf, ScalarUdf, TableUdf, UdfError, UdfRegistry};

// ── Connector API ─────────────────────────────────────────────────────────────

pub use krishiv_connectors::{
    CommitHandle, ConnectorCapabilities, ConnectorConfig, ConnectorError, Offset, Sink, Source,
};

// ── Lakehouse API ─────────────────────────────────────────────────────────────

pub use krishiv_lakehouse::{
    IcebergScanOptions, IcebergTableRef, LakehouseError, LakehouseTable, SchemaField, SchemaVersion,
};

// ── Top-level aliases ─────────────────────────────────────────────────────────

/// Krishiv result type — errors are always [`KrishivError`].
pub type Result<T> = std::result::Result<T, KrishivError>;

// ── Prelude ───────────────────────────────────────────────────────────────────

#[doc(hidden)]
pub mod cli;
pub mod compat;
#[doc(hidden)]
pub mod remote_client;

#[doc(hidden)]
pub mod local_cluster;

/// Convenient glob import for the most common Krishiv types.
///
/// ```rust
/// use krishiv::prelude::*;
/// ```
pub mod prelude {
    pub use crate::{
        AggregateUdf, CommitHandle, ConnectorCapabilities, ConnectorConfig, ConnectorError,
        DataFrame, DataType, ExecutionMode, Field, IcebergScanOptions, IcebergTableRef,
        KeyedStream, KrishivError, LakehouseTable, Offset, QueryResult, RecordBatch, Result,
        ScalarUdf, Schema, SchemaRef, Session, SessionBuilder, SessionWindowedStream, Sink,
        SlidingWindowedStream, Source, StateTtlConfig, Stream, StreamBatch, StreamMode, TableUdf,
        UdfError, UdfRegistry, WatermarkSpec, WindowedStream,
    };
}
