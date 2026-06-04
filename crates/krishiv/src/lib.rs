#![deny(unsafe_code)]

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
//!     // Build the Arrow batch your workflow expects — for example read
//!     // Parquet from object_store, fan in from a stream, or run a SQL
//!     // query that returns RecordBatch.
//!     let batch: RecordBatch = todo!();
//!     let stream = session
//!         .memory_stream("events", vec![StreamBatch::new(0, batch)])?
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
    AggExpr, AggFunction, DataFrame, DeploymentTarget, ExecutionMode, KeyedStream, KrishivError,
    MultiSourceWatermarkSpec, QueryResult, RecordBatch, Session, SessionBuilder,
    SessionWindowedStream, SlidingWindowedStream, StateTtlConfig, Stream, StreamBatch, StreamMode,
    StreamingDataFrame, WatermarkSpec, WindowedStream,
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
    IcebergScanOptions, IcebergTableRef, LakehouseError, LakehouseTable, MemoryLakehouseTable,
    MultiWriterGuard, PartitionField, PartitionSpecResolver, PartitionSpecVersion, SchemaField,
    SchemaVersion, check_write_precondition,
};

// ── Top-level aliases ─────────────────────────────────────────────────────────

/// Krishiv result type — errors are always [`KrishivError`].
pub type Result<T> = std::result::Result<T, KrishivError>;

// ── Unified batch+streaming Relation API ──────────────────────────────────────

pub mod execute;
pub mod relation;
pub mod session_ext;
pub mod stream_handle;

pub use execute::Execute;
pub use relation::{EmitMode, Relation, WindowSpec};
pub use session_ext::SessionExt;
pub use stream_handle::StreamHandle;

// ── Prelude ───────────────────────────────────────────────────────────────────

#[doc(hidden)]
pub mod cli;
pub mod compat;
#[doc(hidden)]
pub mod daemon_cmd;
#[doc(hidden)]
pub mod process_util;
#[doc(hidden)]
pub mod remote_client;

#[doc(hidden)]
pub mod cluster_cmd;
pub mod local_cluster;
#[doc(hidden)]
pub mod query_cli;
#[doc(hidden)]
pub mod stream_cmd;
#[doc(hidden)]
pub mod table_cmd;

/// Distributed control-plane and data-plane building blocks for advanced embedding.
///
/// Most applications use [`Session`] only; operators and custom tooling can use these
/// types to run coordinators and executors in-process or compose custom deployments.
pub mod distributed {
    pub use krishiv_executor::{
        ExecutorConfig, ExecutorRuntime, ExecutorTaskRunner, GrpcCoordinatorService,
    };
    #[cfg(feature = "k8s")]
    pub use krishiv_operator::{
        BootstrapExecutor, K8sLeaseElection, KrishivJobReconciler, KubernetesControllerConfig,
        KubernetesControllerRuntime, KubernetesReconcileReport, run_kubernetes_controller,
        run_kubernetes_controller_runtime_with_client, run_kubernetes_controller_with_client,
    };
    pub use krishiv_proto::{
        CoordinatorId, ExecutorDescriptor, ExecutorId, JobId, JobKind, JobSpec,
    };
    pub use krishiv_scheduler::{
        ClusterControlPlane, Coordinator, CoordinatorDaemonConfig, JobCoordinator,
        JobCoordinatorDaemonConfig, SharedCoordinator, build_shared_coordinator,
        coordinator_daemon_help, coordinator_http_router, job_coordinator_daemon_help,
        parse_coordinator_daemon_config, parse_job_coordinator_daemon_config,
        run_cluster_control_plane, run_clusterd_daemon, run_job_coordinator_daemon,
        run_standalone_coordinator, spawn_coordinator_sidecars,
    };
}

/// Convenient glob import for the most common Krishiv types.
///
/// ```rust
/// use krishiv::prelude::*;
/// ```
pub mod prelude {
    pub use crate::{
        AggExpr, AggFunction, AggregateUdf, CommitHandle, ConnectorCapabilities, ConnectorConfig,
        ConnectorError, DataFrame, DataType, EmitMode, Execute, ExecutionMode, Field,
        IcebergScanOptions, IcebergTableRef, KeyedStream, KrishivError, LakehouseTable, Offset,
        QueryResult, RecordBatch, Relation, Result, ScalarUdf, Schema, SchemaRef, Session,
        SessionBuilder, SessionExt, SessionWindowedStream, Sink, SlidingWindowedStream, Source,
        StateTtlConfig, Stream, StreamBatch, StreamHandle, StreamMode, TableUdf, UdfError,
        UdfRegistry, WatermarkSpec, WindowSpec, WindowedStream,
    };
}
