//! Declarative pipeline layer (Tier 2) — `source → transform → sink`.
//!
//! A [`Pipeline`] is a thin **compiler to the imperative core** (Tier 1:
//! [`IvmJob`](crate::IvmJob) / [`StreamJob`](crate::StreamJob) / batch SQL). It
//! owns connectors and a driver loop; it contains **no parallel execution
//! logic** — the moment it reimplements `feed`/`step`, the unification we built
//! is lost. The driver only calls existing Tier-1 methods.
//!
//! There is **no trigger stage**: boundedness ends a batch pipeline, the
//! watermark drives streaming emit, and change-events drive IVM. The optional
//! [`RunPolicy`] only controls *coalescing* (how many input records per `step`),
//! never *whether* to compute.
//!
//! ```ignore
//! session
//!     .pipeline("revenue")
//!     .source_cdc("orders", changes)
//!     .view("revenue", "SELECT region, SUM(amount) AS total FROM orders GROUP BY region", true)
//!     .sink_memory("revenue", sink_handle.clone())
//!     .run(RunPolicy::Once)
//!     .await?;
//! ```

mod connector_factory;
mod driver;
mod sink;
mod source;

pub(crate) use connector_factory::{build_sink, build_source};

pub use driver::RunPolicy;
pub use sink::Egress;
pub use source::{CdcChange, Ingest};

/// Configuration for backpressure in streaming mode.
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum bytes in flight before applying backpressure.
    pub max_bytes_in_flight: usize,
    /// Maximum rows in flight before applying backpressure.
    pub max_rows_in_flight: usize,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_bytes_in_flight: 1024 * 1024 * 10, // 10MB
            max_rows_in_flight: 10_000,
        }
    }
}

/// Configuration for streaming execution.
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// How the driver advances the logical clock.
    pub run_policy: RunPolicy,
    /// Backpressure configuration.
    pub backpressure: BackpressureConfig,
    /// Checkpoint interval in milliseconds. `None` disables checkpointing.
    pub checkpoint_interval_ms: Option<u64>,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            run_policy: RunPolicy::EveryMs(100),
            backpressure: BackpressureConfig::default(),
            checkpoint_interval_ms: None,
        }
    }
}

use std::sync::Arc;

use crate::{Result, Session};

/// Which execution model a pipeline runs under.
///
/// Inferred from the source kind unless set explicitly: CDC ⇒ `Ivm`,
/// unbounded records ⇒ `Stream`, bounded records ⇒ `Batch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineMode {
    /// Bounded input, run to completion once.
    Batch,
    /// Unbounded input, watermark-driven.
    Stream,
    /// Change-driven incremental view maintenance.
    Ivm,
}

/// A view (transformation) declared on a pipeline.
#[derive(Clone)]
pub struct ViewDef {
    pub name: String,
    pub sql: String,
    pub materialized: bool,
}

/// What to do when a row violates an [`Expectation`] (Spark SDP / DLT parity).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnViolation {
    /// Drop violating rows from the view's output before it reaches the sink.
    Drop,
    /// Fail the pipeline run if any row violates the predicate.
    Fail,
}

/// A data-quality constraint on a view's output: rows for which `predicate`
/// is not true are violations.
#[derive(Clone)]
pub struct Expectation {
    pub view: String,
    pub name: String,
    /// A SQL boolean expression over the view's output columns.
    pub predicate: String,
    pub on_violation: OnViolation,
}

/// A fully-built declarative pipeline, ready to [`run`](Pipeline::run).
pub struct Pipeline {
    session: Session,
    name: String,
    mode: PipelineMode,
    sources: Vec<(String, Ingest)>,
    views: Vec<ViewDef>,
    sinks: Vec<(String, Egress)>,
    expectations: Vec<Expectation>,
    /// Streaming configuration (only used in Stream mode).
    streaming_config: Option<StreamingConfig>,
}

/// Builder returned by [`Session::pipeline`](crate::Session::pipeline).
pub struct PipelineBuilder {
    session: Session,
    name: String,
    mode: Option<PipelineMode>,
    sources: Vec<(String, Ingest)>,
    views: Vec<ViewDef>,
    sinks: Vec<(String, Egress)>,
    expectations: Vec<Expectation>,
    /// Append flows `(target, select_sql)` — multiple with the same target are
    /// `UNION ALL`-ed into a single view at [`build`](PipelineBuilder::build).
    flows: Vec<(String, String)>,
    /// Streaming configuration (only used in Stream mode).
    streaming_config: Option<StreamingConfig>,
}

impl PipelineBuilder {
    pub(crate) fn new(session: Session, name: impl Into<String>) -> Self {
        Self {
            session,
            name: name.into(),
            mode: None,
            sources: Vec::new(),
            views: Vec::new(),
            sinks: Vec::new(),
            expectations: Vec::new(),
            flows: Vec::new(),
        }
    }

    /// Force the execution mode instead of inferring it from the source kind.
    pub fn mode(mut self, mode: PipelineMode) -> Self {
        self.mode = Some(mode);
        self
    }

    /// Add a source that yields plain record batches (fed as insertions).
    pub fn source(mut self, name: impl Into<String>, ingest: Ingest) -> Self {
        self.sources.push((name.into(), ingest));
        self
    }

    /// Add an in-memory CDC source (change events → `DeltaBatch::from_cdc`).
    pub fn source_cdc(self, name: impl Into<String>, changes: Vec<CdcChange>) -> Self {
        self.source(name, Ingest::Cdc(changes))
    }

    /// Add an in-memory record source (testing / embedding).
    pub fn source_memory(
        self,
        name: impl Into<String>,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Self {
        self.source(name, Ingest::Memory(batches))
    }

    /// Declare a transformation view by SQL.
    pub fn view(
        mut self,
        name: impl Into<String>,
        sql: impl Into<String>,
        materialized: bool,
    ) -> Self {
        self.views.push(ViewDef {
            name: name.into(),
            sql: sql.into(),
            materialized,
        });
        self
    }

    /// Declare a pipeline-scoped temporary view (Spark SDP `CREATE TEMPORARY VIEW`).
    ///
    /// A temporary view is a non-materialized intermediate that other views can
    /// reference; it is not maintained as a snapshot and exists only for the
    /// duration of the run. Sugar over a non-materialized [`view`](Self::view).
    pub fn temp_view(self, name: impl Into<String>, sql: impl Into<String>) -> Self {
        self.view(name, sql, false)
    }

    /// Add an append *flow* into `target` (Spark SDP `CREATE FLOW … INSERT INTO`).
    ///
    /// `select_sql` is a full `SELECT` whose rows are appended to `target`.
    /// Multiple flows with the same `target` are `UNION ALL`-ed into a single
    /// materialized view at build time — the fan-in pattern (e.g. several
    /// sources feeding one table).
    pub fn flow(mut self, target: impl Into<String>, select_sql: impl Into<String>) -> Self {
        self.flows.push((target.into(), select_sql.into()));
        self
    }

    /// Attach a sink that consumes a view's output.
    pub fn sink(mut self, view: impl Into<String>, egress: Egress) -> Self {
        self.sinks.push((view.into(), egress));
        self
    }

    /// Attach an in-memory sink that collects a view's output batches.
    pub fn sink_memory(
        self,
        view: impl Into<String>,
        handle: Arc<std::sync::Mutex<Vec<arrow::record_batch::RecordBatch>>>,
    ) -> Self {
        self.sink(view, Egress::Memory(handle))
    }

    /// Add a data-quality expectation on a view's output (Spark SDP / DLT parity).
    ///
    /// `predicate` is a SQL boolean expression over the view's columns. Rows for
    /// which it is not true are violations: with [`OnViolation::Drop`] they are
    /// filtered out before the sink; with [`OnViolation::Fail`] the run errors.
    pub fn expect(
        mut self,
        view: impl Into<String>,
        name: impl Into<String>,
        predicate: impl Into<String>,
        on_violation: OnViolation,
    ) -> Self {
        self.expectations.push(Expectation {
            view: view.into(),
            name: name.into(),
            predicate: predicate.into(),
            on_violation,
        });
        self
    }

    /// Finalize the builder into a [`Pipeline`], inferring the mode if unset.
    pub fn build(self) -> Pipeline {
        let mode = self.mode.unwrap_or_else(|| infer_mode(&self.sources));
        let mut views = self.views;
        // Coalesce append flows by target into one materialized view each
        // (UNION ALL of the flow SELECTs), in first-seen target order.
        let mut order: Vec<String> = Vec::new();
        let mut by_target: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for (target, sql) in self.flows {
            if !by_target.contains_key(&target) {
                order.push(target.clone());
            }
            by_target.entry(target).or_default().push(sql);
        }
        for target in order {
            let sqls = by_target.remove(&target).unwrap_or_default();
            let union_sql = sqls.join(" UNION ALL ");
            views.push(ViewDef {
                name: target,
                sql: union_sql,
                materialized: true,
            });
        }
        Pipeline {
            session: self.session,
            name: self.name,
            mode,
            sources: self.sources,
            views,
            sinks: self.sinks,
            expectations: self.expectations,
        }
    }

    /// Build and run the pipeline under `policy`.
    pub async fn run(self, policy: RunPolicy) -> Result<()> {
        self.build().run(policy).await
    }

    /// Build, full-refresh (reset persisted state), and run the pipeline.
    pub async fn refresh(self, policy: RunPolicy) -> Result<()> {
        self.build().refresh(policy).await
    }
}

impl Pipeline {
    /// The pipeline name (also the underlying job name).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The resolved execution mode.
    pub fn mode(&self) -> PipelineMode {
        self.mode
    }

    /// Validate the pipeline without executing it (a "dry run", à la Spark SDP).
    ///
    /// Checks that every sink references a declared view, that each view's SQL
    /// is analyzable against the known source/upstream-view schemas, that
    /// connector kinds are supported, and that the view dependency graph has no
    /// cycles. Returns a descriptive error on the first problem found.
    ///
    /// Connector source schemas are not probed (that would read data), so views
    /// over connector sources are checked structurally only.
    pub async fn validate(&self) -> Result<()> {
        driver::validate(&self.sources, &self.views, &self.sinks, &self.expectations).await
    }

    /// Run the pipeline to its mode's natural completion / advance policy.
    ///
    /// A named pipeline's IVM job persists across runs, so repeated runs feed new
    /// input **incrementally**. Use [`refresh`](Self::refresh) to reset first.
    pub async fn run(self, policy: RunPolicy) -> Result<()> {
        match self.mode {
            PipelineMode::Ivm => driver::run_incremental(self, policy).await,
            PipelineMode::Batch => driver::run_batch(self).await,
            PipelineMode::Stream => driver::run_stream(self, policy).await,
        }
    }

    /// Full-refresh: reset the pipeline's persisted IVM job, then run from a
    /// fresh, empty state (Spark SDP `--full-refresh`).
    pub async fn refresh(self, policy: RunPolicy) -> Result<()> {
        self.session.reset_ivm_job(&self.name);
        self.run(policy).await
    }
}

fn infer_mode(sources: &[(String, Ingest)]) -> PipelineMode {
    // CDC source ⇒ IVM; bounded records ⇒ Batch; otherwise Stream.
    if sources.iter().any(|(_, s)| matches!(s, Ingest::Cdc(_))) {
        PipelineMode::Ivm
    } else if sources.iter().all(|(_, s)| matches!(s, Ingest::Memory(_))) {
        PipelineMode::Batch
    } else {
        PipelineMode::Stream
    }
}
