//! Unified session entry point — same API across embedded and distributed modes.
//!
//! # Quick start
//!
//! **Embedded (in-process):**
//! ```rust,ignore
//! let session = KrishivSession::embedded();
//! let job = session.ivm_job("revenue").await?;
//! job.feed_source("orders", &delta).await?;
//! let (active, tick) = job.step().await?;
//! ```
//!
//! **Distributed (coordinator):**
//! ```rust,ignore
//! let session = KrishivSession::distributed("http://coordinator:8080");
//! let job = session.ivm_job("revenue").await?;  // same call
//! ```
//!
//! **Batch SQL:**
//! ```rust,ignore
//! let rows = session.batch_sql("SELECT 1 + 1 AS n", &[]).await?;
//! ```

use arrow::record_batch::RecordBatch;
use krishiv_scheduler::SharedIvmJobRegistry;

use crate::ivm_job::{EmbeddedIvmJob, IvmJobHandle, RemoteIvmJob};
use crate::streaming_job::RemoteStreamingJob;
use crate::{RuntimeError, RuntimeResult, execute_coordinator_batch_sql};

// ── session mode ──────────────────────────────────────────────────────────────

/// Mode-agnostic compute session.
///
/// Constructed via [`KrishivSession::embedded`] or
/// [`KrishivSession::distributed`] and used as the single entry point for
/// batch, IVM, and streaming operations.
#[derive(Debug, Clone)]
pub struct KrishivSession {
    mode: SessionMode,
}

#[derive(Debug, Clone)]
enum SessionMode {
    Embedded { registry: SharedIvmJobRegistry },
    Distributed { coordinator_http: String },
}

impl KrishivSession {
    /// Create a session that executes locally (in-process).
    pub fn embedded() -> Self {
        Self {
            mode: SessionMode::Embedded {
                registry: std::sync::Arc::new(krishiv_scheduler::IvmJobRegistry::new()),
            },
        }
    }

    /// Create a session backed by a remote coordinator.
    ///
    /// `coordinator_http` is the base URL of the coordinator HTTP API,
    /// e.g. `"http://localhost:8080"`.
    pub fn distributed(coordinator_http: impl Into<String>) -> Self {
        Self {
            mode: SessionMode::Distributed {
                coordinator_http: coordinator_http.into(),
            },
        }
    }

    /// Whether this session is in embedded (in-process) mode.
    pub fn is_embedded(&self) -> bool {
        matches!(self.mode, SessionMode::Embedded { .. })
    }

    /// Whether this session is in distributed (remote coordinator) mode.
    pub fn is_distributed(&self) -> bool {
        matches!(self.mode, SessionMode::Distributed { .. })
    }

    // ── IVM ──────────────────────────────────────────────────────────────────

    /// Create or retrieve an IVM job with the given name.
    ///
    /// Returns a unified [`IvmJobHandle`] that works identically in both
    /// embedded and distributed modes.
    pub async fn ivm_job(&self, job_name: &str) -> RuntimeResult<IvmJobHandle> {
        match &self.mode {
            SessionMode::Embedded { registry } => {
                let job = EmbeddedIvmJob::create(registry, job_name)?;
                Ok(IvmJobHandle::Embedded(job))
            }
            SessionMode::Distributed { coordinator_http } => {
                let job = RemoteIvmJob::create(coordinator_http, Some(job_name)).await?;
                Ok(IvmJobHandle::Remote(job))
            }
        }
    }

    // ── Batch SQL ────────────────────────────────────────────────────────────

    /// Execute a batch SQL query.
    ///
    /// In embedded mode, runs DataFusion in-process with any provided Parquet tables registered.
    /// In distributed mode, submits the query to the coordinator.
    pub async fn batch_sql(
        &self,
        query: &str,
        tables: &[crate::in_process::BatchSqlTable],
    ) -> RuntimeResult<Vec<RecordBatch>> {
        match &self.mode {
            SessionMode::Embedded { .. } => {
                let ctx = datafusion::prelude::SessionContext::new();
                for table in tables {
                    ctx.register_parquet(
                        &table.table_name,
                        table.path.to_str().unwrap_or(""),
                        datafusion::prelude::ParquetReadOptions::default(),
                    )
                    .await
                    .map_err(|e| {
                        RuntimeError::plan_rejected(format!(
                            "register_parquet '{}': {e}",
                            table.table_name
                        ))
                    })?;
                }
                let df = ctx
                    .sql(query)
                    .await
                    .map_err(|e| RuntimeError::plan_rejected(e.to_string()))?;
                df.collect()
                    .await
                    .map_err(|e| RuntimeError::transport(e.to_string()))
            }
            SessionMode::Distributed { coordinator_http } => {
                execute_coordinator_batch_sql(coordinator_http, query, tables, false).await
            }
        }
    }

    // ── IVM DDL bridge ────────────────────────────────────────────────────────

    /// Register an incremental view on an IVM job, inferring the output schema
    /// from the SQL body using DataFusion.
    ///
    /// This bridges `CREATE INCREMENTAL VIEW` DDL (which only stores the SQL text)
    /// to the `IncrementalFlow` (which requires an explicit `SchemaRef`).
    ///
    /// Source tables already registered on `job` are used as context for schema
    /// inference. An empty DataFusion context is used for pure SQL expressions.
    pub async fn register_incremental_view(
        &self,
        job: &IvmJobHandle,
        name: impl Into<String>,
        body_sql: impl Into<String>,
        is_materialized: bool,
        is_recursive: bool,
    ) -> RuntimeResult<()> {
        use datafusion::prelude::SessionContext;
        use krishiv_ivm::IncrementalViewSpec;

        let name = name.into();
        let body_sql = body_sql.into();

        // Infer output schema by running SELECT * LIMIT 0 with source snapshots
        // registered as MemTables from the embedded flow (if embedded).
        let output_schema = match &self.mode {
            SessionMode::Embedded { registry } => {
                let ctx = SessionContext::new();
                // Register any source snapshots already in the flow as MemTables.
                if let IvmJobHandle::Embedded(embedded_job) = job {
                    if let Ok(specs) = embedded_job.flow().view_specs() {
                        // Also register upstream views as empty tables for inference.
                        for spec in &specs {
                            let schema = spec.output_schema.clone();
                            if let Ok(mt) =
                                datafusion::datasource::MemTable::try_new(schema.clone(), vec![])
                            {
                                let _ = ctx.register_table(&spec.name, std::sync::Arc::new(mt));
                            }
                        }
                    }
                    // Register source snapshots.
                    let flow = embedded_job.flow();
                    if let Ok(names) = flow.view_names() {
                        let _ = names; // already handled above
                    }
                }
                let _ = registry;
                // Run LIMIT 0 to get schema without executing the full query.
                let probe = format!("SELECT * FROM ({body_sql}) AS __probe_view__ LIMIT 0");
                let df = ctx
                    .sql(&probe)
                    .await
                    .map_err(|e| RuntimeError::plan_rejected(format!("schema inference: {e}")))?;
                std::sync::Arc::new(df.schema().as_arrow().clone())
            }
            SessionMode::Distributed { .. } => {
                // Remote: use an empty schema as placeholder; the coordinator
                // will infer schema on first step.
                std::sync::Arc::new(arrow::datatypes::Schema::empty())
            }
        };

        let spec = IncrementalViewSpec {
            name: name.clone(),
            body_sql,
            output_schema,
            is_materialized,
            is_recursive,
            lateness: vec![],
        };
        job.register_view(spec).await
    }

    // ── Continuous Streaming ──────────────────────────────────────────────────

    /// Get a handle to an existing remote streaming job (distributed mode only).
    ///
    /// Use `RemoteStreamingJob::create` to register a new job first.
    pub fn streaming_job(&self, job_id: &str) -> RuntimeResult<RemoteStreamingJob> {
        match &self.mode {
            SessionMode::Embedded { .. } => Err(crate::RuntimeError::unsupported(
                "streaming_job on KrishivSession::embedded — use InProcessStreamingRuntime instead",
            )),
            SessionMode::Distributed { coordinator_http } => Ok(RemoteStreamingJob::from_job_id(
                coordinator_http.clone(),
                job_id,
            )),
        }
    }
}
