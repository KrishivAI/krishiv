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

mod driver;
mod sink;
mod source;

pub use driver::RunPolicy;
pub use sink::Egress;
pub use source::{CdcChange, Ingest};

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

/// A fully-built declarative pipeline, ready to [`run`](Pipeline::run).
pub struct Pipeline {
    session: Session,
    name: String,
    mode: PipelineMode,
    sources: Vec<(String, Ingest)>,
    views: Vec<ViewDef>,
    sinks: Vec<(String, Egress)>,
}

/// Builder returned by [`Session::pipeline`](crate::Session::pipeline).
pub struct PipelineBuilder {
    session: Session,
    name: String,
    mode: Option<PipelineMode>,
    sources: Vec<(String, Ingest)>,
    views: Vec<ViewDef>,
    sinks: Vec<(String, Egress)>,
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

    /// Finalize the builder into a [`Pipeline`], inferring the mode if unset.
    pub fn build(self) -> Pipeline {
        let mode = self.mode.unwrap_or_else(|| infer_mode(&self.sources));
        Pipeline {
            session: self.session,
            name: self.name,
            mode,
            sources: self.sources,
            views: self.views,
            sinks: self.sinks,
        }
    }

    /// Build and run the pipeline under `policy`.
    pub async fn run(self, policy: RunPolicy) -> Result<()> {
        self.build().run(policy).await
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

    /// Run the pipeline to its mode's natural completion / advance policy.
    pub async fn run(self, policy: RunPolicy) -> Result<()> {
        match self.mode {
            PipelineMode::Ivm => driver::run_ivm(self, policy).await,
            PipelineMode::Batch => driver::run_batch(self).await,
            PipelineMode::Stream => driver::run_stream(self, policy).await,
        }
    }
}

fn infer_mode(sources: &[(String, Ingest)]) -> PipelineMode {
    // CDC source ⇒ IVM; bounded records ⇒ Batch; otherwise Stream.
    if sources.iter().any(|(_, s)| matches!(s, Ingest::Cdc(_))) {
        PipelineMode::Ivm
    } else if sources
        .iter()
        .all(|(_, s)| matches!(s, Ingest::Memory(_)))
    {
        PipelineMode::Batch
    } else {
        PipelineMode::Stream
    }
}
