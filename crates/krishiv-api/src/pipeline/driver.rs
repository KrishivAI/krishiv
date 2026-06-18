//! Pipeline driver — the loop that compiles `source → transform → sink` down to
//! the Tier-1 imperative core (`feed` / `step` / `snapshot`).
//!
//! **Invariant:** this module only orchestrates existing Tier-1 methods. It must
//! never reimplement incremental/streaming execution.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use krishiv_delta::DeltaBatch;
use krishiv_ivm::IncrementalViewSpec;

use super::{Egress, Ingest, Pipeline};
use crate::compute::FeedableJob;
use crate::{IvmJob, KrishivError, Result};

/// How the driver advances the logical clock — the *coalescing* knob, never a
/// compute trigger. Boundedness (batch), watermark (stream), and change-events
/// (IVM) decide *whether* to compute; this only decides *how often* to `step`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunPolicy {
    /// Feed all input, then `step` once (the natural choice for bounded input).
    Once,
    /// `step` after every individual input change (lowest latency).
    OnChange,
    /// `step` after every `n` input rows have been fed (coalesced).
    EveryRows(usize),
    /// `step` at most every `ms` milliseconds (time-coalesced).
    EveryMs(u64),
}

fn rt(msg: impl std::fmt::Display) -> KrishivError {
    KrishivError::Runtime {
        message: msg.to_string(),
    }
}

// ── IVM path ────────────────────────────────────────────────────────────────

pub(super) async fn run_ivm(pipeline: Pipeline, policy: RunPolicy) -> Result<()> {
    let Pipeline {
        session,
        name,
        sources,
        views,
        sinks,
        ..
    } = pipeline;

    // 1. Source schemas (for view-schema inference).
    let mut schemas: HashMap<String, SchemaRef> = HashMap::new();
    for (sname, ingest) in &sources {
        if let Some(s) = ingest_schema(ingest) {
            schemas.insert(sname.clone(), s);
        }
    }

    // 2. Mode-aware IVM job + view registration (declaration order, so a later
    //    view may reference an earlier one).
    let job = session.ivm(&name).await?;
    for v in &views {
        let out_schema = infer_view_schema(&schemas, &v.sql).await?;
        schemas.insert(v.name.clone(), out_schema.clone());
        job.register_view(IncrementalViewSpec {
            name: v.name.clone(),
            body_sql: v.sql.clone(),
            output_schema: out_schema,
            is_materialized: v.materialized,
            is_recursive: false,
            lateness: vec![],
        })
        .await?;
    }

    // 3. Feed sources, advancing per policy.
    let mut rows_since_step = 0usize;
    for (sname, ingest) in sources {
        match ingest {
            Ingest::Memory(batches) => {
                for b in batches {
                    if b.num_rows() == 0 {
                        continue;
                    }
                    let n = b.num_rows();
                    let delta = DeltaBatch::from_inserts(b).map_err(rt)?;
                    job.feed(&sname, &delta).await?;
                    rows_since_step += n;
                    maybe_step(&job, policy, &mut rows_since_step).await?;
                }
            }
            Ingest::Cdc(changes) => {
                for c in changes {
                    if let Some(delta) = DeltaBatch::from_cdc(c.before, c.after).map_err(rt)? {
                        let n = delta.num_rows();
                        job.feed(&sname, &delta).await?;
                        rows_since_step += n;
                        maybe_step(&job, policy, &mut rows_since_step).await?;
                    }
                }
            }
            Ingest::Connector(_) => {
                return Err(rt(
                    "connector sources in pipeline IVM mode are not yet wired; use Memory/Cdc",
                ));
            }
        }
    }

    // 4. Final flush step.
    job.step().await?;

    // 5. Write each view's current snapshot to its sinks.
    write_snapshots(&job, sinks).await
}

async fn maybe_step(
    job: &IvmJob,
    policy: RunPolicy,
    rows_since_step: &mut usize,
) -> Result<()> {
    let should = match policy {
        RunPolicy::Once => false,
        RunPolicy::OnChange => true,
        RunPolicy::EveryRows(n) => *rows_since_step >= n.max(1),
        RunPolicy::EveryMs(_) => true,
    };
    if should {
        job.step().await?;
        *rows_since_step = 0;
    }
    Ok(())
}

async fn write_snapshots(job: &IvmJob, sinks: Vec<(String, Egress)>) -> Result<()> {
    for (view, mut egress) in sinks {
        if let Some(snapshot) = job.snapshot(&view).await? {
            if snapshot.num_rows() > 0 {
                egress.write(snapshot).await?;
            }
            egress.flush().await?;
        }
    }
    Ok(())
}

// ── Batch path (self-contained DataFusion execution) ──────────────────────────

pub(super) async fn run_batch(pipeline: Pipeline) -> Result<()> {
    let Pipeline {
        sources,
        views,
        sinks,
        ..
    } = pipeline;
    let ctx = SessionContext::new();

    // Register sources as in-memory tables.
    for (sname, ingest) in sources {
        let batches = match ingest {
            Ingest::Memory(b) => b,
            Ingest::Cdc(_) => {
                return Err(rt("CDC source is not valid in batch mode; use Memory/Connector"));
            }
            Ingest::Connector(_) => {
                return Err(rt("connector sources in batch mode are not yet wired; use Memory"));
            }
        };
        if let Some(first) = batches.first() {
            let schema = first.schema();
            let mt = MemTable::try_new(schema, vec![batches]).map_err(rt)?;
            ctx.register_table(sname.as_str(), Arc::new(mt)).map_err(rt)?;
        }
    }

    // Run each view; register its result so later views can reference it.
    let mut outputs: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    for v in &views {
        let df = ctx.sql(&v.sql).await.map_err(rt)?;
        let out = df.collect().await.map_err(rt)?;
        if let Some(first) = out.first() {
            let mt = MemTable::try_new(first.schema(), vec![out.clone()]).map_err(rt)?;
            ctx.register_table(v.name.as_str(), Arc::new(mt)).map_err(rt)?;
        }
        outputs.insert(v.name.clone(), out);
    }

    // Write each view's result to its sinks.
    for (view, mut egress) in sinks {
        if let Some(batches) = outputs.get(&view) {
            for b in batches {
                egress.write(b.clone()).await?;
            }
            egress.flush().await?;
        }
    }
    Ok(())
}

// ── Stream path ───────────────────────────────────────────────────────────────

pub(super) async fn run_stream(_pipeline: Pipeline, _policy: RunPolicy) -> Result<()> {
    Err(rt(
        "streaming pipelines require a window spec; use session.stream() directly for now \
         (declarative streaming wiring is the next driver increment)",
    ))
}

// ── Schema inference ──────────────────────────────────────────────────────────

fn ingest_schema(ingest: &Ingest) -> Option<SchemaRef> {
    match ingest {
        Ingest::Memory(b) => b.first().map(|rb| rb.schema()),
        Ingest::Cdc(changes) => changes
            .iter()
            .find_map(|c| c.after.as_ref().or(c.before.as_ref()))
            .map(|rb| rb.schema()),
        Ingest::Connector(_) => None,
    }
}

/// Infer a view's output schema by registering the known source/upstream-view
/// schemas as empty tables and probing the SQL with `LIMIT 0`.
async fn infer_view_schema(
    available: &HashMap<String, SchemaRef>,
    sql: &str,
) -> Result<SchemaRef> {
    let ctx = SessionContext::new();
    for (name, schema) in available {
        let mt = MemTable::try_new(schema.clone(), vec![]).map_err(rt)?;
        ctx.register_table(name.as_str(), Arc::new(mt)).map_err(rt)?;
    }
    let probe = format!("SELECT * FROM ({sql}) AS __pipeline_probe__ LIMIT 0");
    let df = ctx.sql(&probe).await.map_err(rt)?;
    Ok(Arc::new(df.schema().as_arrow().clone()))
}
