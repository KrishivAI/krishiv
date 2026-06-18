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

use super::{Egress, Expectation, Ingest, OnViolation, Pipeline, ViewDef};
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

/// Incremental path shared by `Ivm` and `Stream` modes: register the views,
/// feed every source (CDC → `from_cdc`; records → inserts; connector → drained),
/// advance per policy, then write each view's snapshot to its sinks.
///
/// `Stream` differs from `Ivm` only in intent — an unbounded source feeds an
/// append-only stream. Bounded connector sources are drained up front; true
/// unbounded continuous looping is a future increment.
pub(super) async fn run_incremental(pipeline: Pipeline, policy: RunPolicy) -> Result<()> {
    let Pipeline {
        session,
        name,
        sources,
        views,
        sinks,
        expectations,
        ..
    } = pipeline;

    // 0. Normalize connector sources by draining them to in-memory batches.
    let sources = normalize_sources(sources).await?;

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
            // Connectors were drained to Memory in step 0.
            Ingest::Connector(_) => unreachable!("connector sources are normalized to Memory"),
        }
    }

    // 4. Final flush step.
    job.step().await?;

    // 5. Write each view's current snapshot to its sinks (applying expectations).
    write_snapshots(&job, sinks, &expectations).await
}

/// Drain a bounded connector source to in-memory batches, replacing any
/// `Ingest::Connector` with `Ingest::Memory`. Memory/Cdc sources pass through.
async fn normalize_sources(sources: Vec<(String, Ingest)>) -> Result<Vec<(String, Ingest)>> {
    let mut out = Vec::with_capacity(sources.len());
    for (name, ingest) in sources {
        let ingest = match ingest {
            Ingest::Connector(mut src) => {
                let mut batches = Vec::new();
                while let Some(b) = src.read_batch_dyn().await.map_err(rt)? {
                    batches.push(b);
                }
                Ingest::Memory(batches)
            }
            other => other,
        };
        out.push((name, ingest));
    }
    Ok(out)
}

async fn maybe_step(job: &IvmJob, policy: RunPolicy, rows_since_step: &mut usize) -> Result<()> {
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

async fn write_snapshots(
    job: &IvmJob,
    sinks: Vec<(String, Egress)>,
    expectations: &[Expectation],
) -> Result<()> {
    for (view, mut egress) in sinks {
        if let Some(snapshot) = job.snapshot(&view).await? {
            let snapshot = apply_expectations(&view, snapshot, expectations).await?;
            if snapshot.num_rows() > 0 {
                egress.write(snapshot).await?;
            }
            egress.flush().await?;
        }
    }
    Ok(())
}

/// Apply every expectation declared on `view` to its output `batch`.
///
/// `Drop` filters out rows where the predicate is not true; `Fail` errors if any
/// row's predicate is explicitly false. Predicates are evaluated with DataFusion.
async fn apply_expectations(
    view: &str,
    batch: RecordBatch,
    expectations: &[Expectation],
) -> Result<RecordBatch> {
    let mut current = batch;
    for exp in expectations.iter().filter(|e| e.view == view) {
        if current.num_rows() == 0 {
            continue;
        }
        let ctx = SessionContext::new();
        let schema = current.schema();
        let mt = MemTable::try_new(schema.clone(), vec![vec![current.clone()]]).map_err(rt)?;
        ctx.register_table("__expect", Arc::new(mt)).map_err(rt)?;

        match exp.on_violation {
            OnViolation::Fail => {
                let bad = ctx
                    .sql(&format!(
                        "SELECT COUNT(*) AS n FROM __expect WHERE NOT ({})",
                        exp.predicate
                    ))
                    .await
                    .map_err(rt)?
                    .collect()
                    .await
                    .map_err(rt)?;
                let n = bad
                    .first()
                    .and_then(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                    })
                    .map(|a| a.value(0))
                    .unwrap_or(0);
                if n > 0 {
                    return Err(rt(format!(
                        "expectation '{}' on view '{view}' failed: {n} row(s) violate `{}`",
                        exp.name, exp.predicate
                    )));
                }
            }
            OnViolation::Drop => {
                let kept = ctx
                    .sql(&format!("SELECT * FROM __expect WHERE ({})", exp.predicate))
                    .await
                    .map_err(rt)?
                    .collect()
                    .await
                    .map_err(rt)?;
                current = if kept.is_empty() {
                    RecordBatch::new_empty(schema)
                } else {
                    arrow::compute::concat_batches(&kept[0].schema(), &kept).map_err(rt)?
                };
            }
        }
    }
    Ok(current)
}

// ── Batch path (self-contained DataFusion execution) ──────────────────────────

pub(super) async fn run_batch(pipeline: Pipeline) -> Result<()> {
    let Pipeline {
        sources,
        views,
        sinks,
        expectations,
        ..
    } = pipeline;
    let ctx = SessionContext::new();

    // Drain connector sources first, then register all sources as tables.
    let sources = normalize_sources(sources).await?;
    for (sname, ingest) in sources {
        let batches = match ingest {
            Ingest::Memory(b) => b,
            Ingest::Cdc(_) => {
                return Err(rt(
                    "CDC source is not valid in batch mode; use Memory/Connector",
                ));
            }
            Ingest::Connector(_) => unreachable!("connector sources are normalized to Memory"),
        };
        if let Some(first) = batches.first() {
            let schema = first.schema();
            let mt = MemTable::try_new(schema, vec![batches]).map_err(rt)?;
            ctx.register_table(sname.as_str(), Arc::new(mt))
                .map_err(rt)?;
        }
    }

    // Run each view; register its result so later views can reference it.
    let mut outputs: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    for v in &views {
        let df = ctx.sql(&v.sql).await.map_err(rt)?;
        let out = df.collect().await.map_err(rt)?;
        if let Some(first) = out.first() {
            let mt = MemTable::try_new(first.schema(), vec![out.clone()]).map_err(rt)?;
            ctx.register_table(v.name.as_str(), Arc::new(mt))
                .map_err(rt)?;
        }
        outputs.insert(v.name.clone(), out);
    }

    // Write each view's result to its sinks (applying expectations).
    for (view, mut egress) in sinks {
        if let Some(batches) = outputs.get(&view) {
            let schema = batches
                .first()
                .map(|b| b.schema())
                .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
            let combined = if batches.is_empty() {
                RecordBatch::new_empty(schema)
            } else {
                arrow::compute::concat_batches(&schema, batches).map_err(rt)?
            };
            let checked = apply_expectations(&view, combined, &expectations).await?;
            if checked.num_rows() > 0 {
                egress.write(checked).await?;
            }
            egress.flush().await?;
        }
    }
    Ok(())
}

// ── Stream path ───────────────────────────────────────────────────────────────

/// Streaming pipeline: an unbounded/append-only record source feeding an
/// incremental view. Shares the incremental engine with `Ivm` — the difference
/// is intent (records are append-only inserts rather than CDC changes). Bounded
/// connector sources are drained up front; for true unbounded sources the
/// driver processes all currently-available batches per the advance policy.
pub(super) async fn run_stream(pipeline: Pipeline, policy: RunPolicy) -> Result<()> {
    run_incremental(pipeline, policy).await
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
async fn infer_view_schema(available: &HashMap<String, SchemaRef>, sql: &str) -> Result<SchemaRef> {
    let ctx = SessionContext::new();
    for (name, schema) in available {
        // One empty partition (not zero partitions) — DataFusion rejects an
        // empty partition list. An empty table is enough for a LIMIT 0 probe.
        let mt = MemTable::try_new(schema.clone(), vec![vec![]]).map_err(rt)?;
        ctx.register_table(name.as_str(), Arc::new(mt))
            .map_err(rt)?;
    }
    let probe = format!("SELECT * FROM ({sql}) AS __pipeline_probe__ LIMIT 0");
    let df = ctx.sql(&probe).await.map_err(rt)?;
    Ok(Arc::new(df.schema().as_arrow().clone()))
}

// ── Validation (dry-run) ───────────────────────────────────────────────────────

/// Validate a pipeline plan without executing it (see [`Pipeline::validate`]).
pub(super) async fn validate(
    sources: &[(String, Ingest)],
    views: &[ViewDef],
    sinks: &[(String, Egress)],
    expectations: &[Expectation],
) -> Result<()> {
    use std::collections::HashSet;

    let view_names: HashSet<&str> = views.iter().map(|v| v.name.as_str()).collect();

    // 1. Sinks and expectations must reference declared views.
    for (view, _) in sinks {
        if !view_names.contains(view.as_str()) {
            return Err(rt(format!("sink references undefined view '{view}'")));
        }
    }
    for e in expectations {
        if !view_names.contains(e.view.as_str()) {
            return Err(rt(format!(
                "expectation '{}' references undefined view '{}'",
                e.name, e.view
            )));
        }
    }

    // 2. No cycles in the view dependency graph.
    detect_view_cycles(views, &view_names)?;

    // 3. Schema inference for views over fully-known (Memory/Cdc) sources.
    let mut schemas: HashMap<String, SchemaRef> = HashMap::new();
    let mut connector_sources: HashSet<&str> = HashSet::new();
    for (name, ingest) in sources {
        match ingest_schema(ingest) {
            Some(s) => {
                schemas.insert(name.clone(), s);
            }
            None => {
                connector_sources.insert(name.as_str());
            }
        }
    }
    for v in views {
        // Skip views that reference a connector source — its schema is unknown
        // until run time, so we can only check such views structurally.
        let refs = referenced_names(&v.sql);
        if refs.iter().any(|r| connector_sources.contains(r.as_str())) {
            continue;
        }
        let out = infer_view_schema(&schemas, &v.sql)
            .await
            .map_err(|e| rt(format!("view '{}' failed validation: {e}", v.name)))?;
        schemas.insert(v.name.clone(), out);
    }
    Ok(())
}

/// Tokenize a SQL string into lowercased identifier-like names.
fn referenced_names(sql: &str) -> Vec<String> {
    sql.split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

/// Detect a cycle in the view→view dependency graph (DFS with an active stack).
fn detect_view_cycles(
    views: &[ViewDef],
    view_names: &std::collections::HashSet<&str>,
) -> Result<()> {
    use std::collections::{HashMap as Map, HashSet};

    let deps: Map<String, Vec<String>> = views
        .iter()
        .map(|v| {
            let d = referenced_names(&v.sql)
                .into_iter()
                .filter(|r| r != &v.name.to_lowercase() && view_names.contains(r.as_str()))
                .collect();
            (v.name.to_lowercase(), d)
        })
        .collect();

    fn visit(
        node: &str,
        deps: &Map<String, Vec<String>>,
        visited: &mut HashSet<String>,
        stack: &mut HashSet<String>,
    ) -> Result<()> {
        if stack.contains(node) {
            return Err(rt(format!(
                "view dependency cycle detected involving '{node}'"
            )));
        }
        if visited.contains(node) {
            return Ok(());
        }
        stack.insert(node.to_string());
        if let Some(children) = deps.get(node) {
            for c in children {
                visit(c, deps, visited, stack)?;
            }
        }
        stack.remove(node);
        visited.insert(node.to_string());
        Ok(())
    }

    let mut visited = HashSet::new();
    let mut stack = HashSet::new();
    for v in views {
        visit(&v.name.to_lowercase(), &deps, &mut visited, &mut stack)?;
    }
    Ok(())
}
