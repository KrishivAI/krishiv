//! Pipeline driver — the loop that compiles `source → transform → sink` down to
//! the Tier-1 imperative core (`feed` / `step` / `snapshot`).
//!
//! **Invariant:** this module only orchestrates existing Tier-1 methods. It must
//! never reimplement incremental/streaming execution.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::config::SessionConfig;
use datafusion::prelude::SessionContext;
use futures::StreamExt as _;
use krishiv_common::async_util::unix_now_ms;
use krishiv_delta::DeltaBatch;
use krishiv_ivm::IncrementalViewSpec;

use super::{
    BackpressureConfig, Egress, Expectation, Ingest, OnViolation, Pipeline, StreamingConfig,
    ViewDef,
};
use crate::compute::FeedableJob;
use crate::{IvmJob, KrishivError, Result};

use krishiv_connectors::DynSource;

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

// ── Streaming primitives ──────────────────────────────────────────────────────

/// Controls backpressure for streaming sources.
///
/// Monitors bytes and rows in flight to prevent overwhelming the compute engine.
pub struct BackpressureController {
    /// Maximum bytes in flight before applying backpressure.
    max_bytes_in_flight: usize,
    /// Current bytes in flight.
    current_bytes: usize,
    /// Maximum rows in flight before applying backpressure.
    max_rows_in_flight: usize,
    /// Current rows in flight.
    current_rows: usize,
    /// Rows since last step (for EveryRows policy).
    pub rows_since_step: usize,
    /// Whether source is exhausted (bounded sources only).
    exhausted: bool,
}

impl BackpressureController {
    /// Create a new backpressure controller with the given config.
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            max_bytes_in_flight: config.max_bytes_in_flight,
            current_bytes: 0,
            max_rows_in_flight: config.max_rows_in_flight,
            current_rows: 0,
            rows_since_step: 0,
            exhausted: false,
        }
    }

    /// Check if source can produce more data.
    pub fn is_available(&self) -> bool {
        !self.exhausted
            && self.current_bytes < self.max_bytes_in_flight
            && self.current_rows < self.max_rows_in_flight
    }

    /// Record that a batch was produced.
    pub fn record_batch(&mut self, batch: &RecordBatch) {
        let num_rows = batch.num_rows();
        self.current_rows += num_rows;
        self.rows_since_step += num_rows;
        self.current_bytes += batch.get_array_memory_size();
    }

    /// Mark source as exhausted.
    pub fn mark_exhausted(&mut self) {
        self.exhausted = true;
    }

    /// Check if source is exhausted.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Reset counters after step.
    pub fn reset_after_step(&mut self) {
        self.current_bytes = 0;
        self.current_rows = 0;
        self.rows_since_step = 0;
    }
}

/// A streaming source that wraps a connector source.
pub struct StreamingSource {
    /// Source name (for feeding to job).
    pub name: String,
    /// The underlying connector source.
    pub source: Box<dyn DynSource>,
    /// Batches read during schema/bootstrap probing but not yet fed.
    pub pending_batches: VecDeque<RecordBatch>,
    /// Backpressure controller.
    pub backpressure: BackpressureController,
}

/// Checkpoint state for streaming sources.
#[derive(Debug, Clone)]
pub struct StreamingCheckpoint {
    /// Unique checkpoint identifier.
    pub checkpoint_id: String,
    /// Source name → serialized offset.
    pub source_offsets: std::collections::HashMap<String, Vec<u8>>,
    /// Checkpoint timestamp in milliseconds since epoch.
    pub timestamp_ms: i64,
}

/// Save checkpoint state for streaming sources.
pub async fn save_streaming_checkpoint(
    sources: &[StreamingSource],
    checkpoint_id: &str,
) -> Result<StreamingCheckpoint> {
    let mut source_offsets = std::collections::HashMap::new();

    for src in sources {
        if let Some(offset) = src.source.encoded_checkpoint_offset_dyn().map_err(rt)? {
            source_offsets.insert(src.name.clone(), offset);
        }
    }

    Ok(StreamingCheckpoint {
        checkpoint_id: checkpoint_id.to_string(),
        source_offsets,
        timestamp_ms: unix_now_ms(),
    })
}

/// Restore streaming sources from checkpoint.
///
/// The connector validates that the encoded bytes belong to the source and name
/// a valid read boundary.
#[cfg(test)]
pub async fn restore_streaming_checkpoint(
    sources: &mut [StreamingSource],
    checkpoint: &StreamingCheckpoint,
) -> Result<()> {
    for src in sources.iter_mut() {
        if let Some(offset) = checkpoint.source_offsets.get(&src.name) {
            src.source
                .restore_encoded_checkpoint_offset_dyn(offset)
                .map_err(rt)?;
        }
    }
    Ok(())
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
    let mut sinks = sinks;
    write_snapshots(&job, &mut sinks, &expectations).await
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
    sinks: &mut [(String, Egress)],
    expectations: &[Expectation],
) -> Result<()> {
    for (view, egress) in sinks.iter_mut() {
        if let Some(snapshot) = job.snapshot(view).await? {
            let snapshot = apply_expectations(view, snapshot, expectations).await?;
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
                    arrow::compute::concat_batches(
                        &kept.first().map(|b| b.schema()).unwrap_or_else(|| {
                            std::sync::Arc::new(arrow::datatypes::Schema::empty())
                        }),
                        &kept,
                    )
                    .map_err(rt)?
                };
            }
        }
    }
    Ok(current)
}

// ── Batch path (self-contained DataFusion execution) ──────────────────────────

/// True if `name` appears as a standalone SQL identifier in `sql`.
///
/// A plain substring check would match "orders" inside "global_orders" or
/// "orders_by_customer".  Instead we require that the match position is not
/// bordered by an alphanumeric or underscore character on either side, which
/// covers unquoted identifiers, dot-qualified names, and quoted forms like
/// `"orders"`.
fn sql_references_name(sql: &str, name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let bytes = sql.as_bytes();
    let name_bytes = name.as_bytes();
    let nlen = name_bytes.len();
    let mut start = 0;
    while start + nlen <= bytes.len() {
        if let Some(rel) = sql[start..].find(name) {
            let abs = start + rel;
            let before_ok = abs == 0
                || !matches!(
                    bytes.get(abs - 1).copied(),
                    Some(b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
                );
            let after_ok = abs + nlen >= bytes.len()
                || !matches!(
                    bytes.get(abs + nlen).copied(),
                    Some(b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
                );
            if before_ok && after_ok {
                return true;
            }
            start = abs + 1;
        } else {
            break;
        }
    }
    false
}

fn batch_session_context() -> SessionContext {
    let parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let config = SessionConfig::new()
        .with_target_partitions(parallelism)
        .with_batch_size(65_536)
        .with_repartition_joins(true)
        .with_repartition_aggregations(true);
    SessionContext::new_with_config(config)
}

pub(super) async fn run_batch(pipeline: Pipeline) -> Result<()> {
    let Pipeline {
        sources,
        views,
        mut sinks,
        expectations,
        ..
    } = pipeline;
    let ctx = batch_session_context();

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
        if batches.is_empty() {
            return Err(rt(format!(
                "source '{sname}' produced no batches; \
                 empty connector sources are not supported in batch mode"
            )));
        }
        let schema = batches
            .first()
            .ok_or_else(|| {
                rt(format!(
                    "source '{sname}' produced no batches; \
                     empty connector sources are not supported in batch mode"
                ))
            })?
            .schema();
        let mt = MemTable::try_new(schema, vec![batches]).map_err(rt)?;
        ctx.register_table(sname.as_str(), Arc::new(mt))
            .map_err(rt)?;
    }

    // Determine which views are referenced by downstream views (non-leaf).
    // Leaf views only feed sinks; non-leaf views must be collected into a
    // MemTable so downstream views can scan them.
    //
    // Uses word-boundary matching: a view named "orders" must not match inside
    // "global_orders" or "orders_by_customer".  sql_references_name() checks
    // that the match is not bordered by [A-Za-z0-9_] on either side.
    let referenced_by_views: std::collections::HashSet<&str> = views
        .iter()
        .filter(|v| {
            views
                .iter()
                .any(|other| other.name != v.name && sql_references_name(&other.sql, &v.name))
        })
        .map(|v| v.name.as_str())
        .collect();

    // Pre-compute which views have expectations so we know at view-time
    // whether we can stream or must collect.
    let views_with_expectations: std::collections::HashSet<&str> =
        expectations.iter().map(|e| e.view.as_str()).collect();

    // Run each view in dependency order.
    //
    // - Non-leaf views: collect → register MemTable → store in `outputs` for
    //   the sink-write loop.
    // - Leaf views with expectations: collect → store in `outputs` (the
    //   expectations check needs a combined single batch).
    // - Leaf views without expectations and with a direct sink: stream
    //   output directly to the sink during this loop — no collect, no copy,
    //   no `outputs` entry needed.
    let mut outputs: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    for v in &views {
        let df = ctx.sql(&v.sql).await.map_err(rt)?;
        let is_non_leaf = referenced_by_views.contains(v.name.as_str());
        let sink_idx = sinks.iter().position(|(n, _)| n == &v.name);
        let has_exp = views_with_expectations.contains(v.name.as_str());

        if is_non_leaf {
            // Must collect: later views need to scan this as a MemTable.
            // Derive schema from the DataFrame plan so we can register even
            // when the view returns zero rows (a first-batch guard would silently
            // skip the registration and cause a "table not found" error in any
            // downstream view).
            let schema = Arc::new(df.schema().as_arrow().clone());
            let out = df.collect().await.map_err(rt)?;
            let mt = MemTable::try_new(schema, vec![out.clone()]).map_err(rt)?;
            ctx.register_table(v.name.as_str(), Arc::new(mt))
                .map_err(rt)?;
            if sink_idx.is_some() {
                outputs.insert(v.name.clone(), out);
            }
        } else if let Some(idx) = sink_idx {
            if !has_exp {
                // Leaf view, no expectations, direct sink: stream to avoid
                // buffering the entire output in memory.
                let (_, egress) = sinks
                    .get_mut(idx)
                    .ok_or_else(|| rt(format!("sink index {idx} not found")))?;
                let mut stream = df.execute_stream().await.map_err(rt)?;
                while let Some(batch) = stream.next().await {
                    let batch = batch.map_err(rt)?;
                    if batch.num_rows() > 0 {
                        egress.write(batch).await?;
                    }
                }
                egress.flush().await?;
            } else {
                let out = df.collect().await.map_err(rt)?;
                outputs.insert(v.name.clone(), out);
            }
        } else {
            // Leaf view with no sink: referenced only by output mapping.
            let out = df.collect().await.map_err(rt)?;
            outputs.insert(v.name.clone(), out);
        }
    }

    // Write any remaining sink outputs (non-leaf or expectation-checked views).
    for (view, egress) in sinks.iter_mut() {
        let batches = match outputs.get(view.as_str()) {
            Some(b) => b,
            None => continue, // already streamed directly above
        };
        let has_exp = views_with_expectations.contains(view.as_str());
        if has_exp {
            // Expectations need a single combined batch for predicate evaluation.
            let schema = batches
                .first()
                .map(|b| b.schema())
                .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
            let combined = if batches.is_empty() {
                RecordBatch::new_empty(schema)
            } else {
                arrow::compute::concat_batches(&schema, batches).map_err(rt)?
            };
            let checked = apply_expectations(view, combined, &expectations).await?;
            if checked.num_rows() > 0 {
                egress.write(checked).await?;
            }
        } else {
            for batch in batches {
                if batch.num_rows() > 0 {
                    egress.write(batch.clone()).await?;
                }
            }
        }
        egress.flush().await?;
    }
    Ok(())
}

// ── Stream path ───────────────────────────────────────────────────────────────

/// Streaming pipeline: an unbounded/append-only record source feeding an
/// incremental view. Shares the incremental engine with `Ivm` — the difference
/// is intent (records are append-only inserts rather than CDC changes). Bounded
/// connector sources are read incrementally; for true unbounded sources the
/// driver processes batches as they become available per the advance policy.
pub(super) async fn run_stream(pipeline: Pipeline, policy: RunPolicy) -> Result<()> {
    run_streaming(
        pipeline,
        StreamingConfig {
            run_policy: policy,
            ..StreamingConfig::default()
        },
    )
    .await
}

/// True continuous streaming pipeline driver.
///
/// Reads connector sources incrementally with backpressure control, rather than
/// draining them to memory first. Supports cancellation and checkpoint-controlled
/// source offsets.
pub(super) async fn run_streaming(pipeline: Pipeline, config: StreamingConfig) -> Result<()> {
    let Pipeline {
        session,
        name,
        sources,
        views,
        mut sinks,
        expectations,
        ..
    } = pipeline;

    // 1. Prepare source schemas. Connectors that expose schema metadata can
    // start even when no data is currently available. Data-dependent connectors
    // are probed for the first batch only; that batch is kept pending and fed
    // after view registration.
    let mut schemas: HashMap<String, SchemaRef> = HashMap::new();
    let mut streaming_sources: Vec<StreamingSource> = Vec::new();
    let mut memory_sources: Vec<(String, Ingest)> = Vec::new();
    for (sname, ingest) in sources {
        match ingest {
            Ingest::Connector(mut src) => {
                let capabilities = src.capabilities();
                capabilities.validate().map_err(rt)?;
                let mut pending_batches = VecDeque::new();

                if let Some(schema) = src.source_schema_dyn() {
                    schemas.insert(sname.clone(), schema);
                } else {
                    match src.read_batch_dyn().await.map_err(rt)? {
                        Some(batch) => {
                            schemas.insert(sname.clone(), batch.schema());
                            pending_batches.push_back(batch);
                        }
                        None if capabilities.is_bounded() => {
                            return Err(rt(format!(
                                "connector source '{sname}' produced no batches; schema inference \
                                 for empty connector sources requires explicit connector schema \
                                 metadata"
                            )));
                        }
                        None => {
                            return Err(rt(format!(
                                "connector source '{sname}' did not produce an initial batch; \
                                 schema inference for idle unbounded connector sources requires \
                                 explicit connector schema metadata"
                            )));
                        }
                    }
                }

                let streaming_src = StreamingSource {
                    name: sname,
                    source: src,
                    pending_batches,
                    backpressure: BackpressureController::new(config.backpressure.clone()),
                };
                streaming_sources.push(streaming_src);
            }
            other => {
                if let Some(schema) = ingest_schema(&other) {
                    schemas.insert(sname.clone(), schema);
                }
                memory_sources.push((sname, other));
            }
        }
    }

    // 2. Register views once source schemas are known.
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

    // 3. Feed in-memory sources first. They share the same IVM job and are
    // stepped with connector input below, so mixed memory+connector pipelines
    // still produce one coherent snapshot.
    let mut rows_since_step = 0usize;
    for (sname, ingest) in memory_sources {
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
                }
            }
            Ingest::Cdc(changes) => {
                for c in changes {
                    if let Some(delta) = DeltaBatch::from_cdc(c.before, c.after).map_err(rt)? {
                        let n = delta.num_rows();
                        job.feed(&sname, &delta).await?;
                        rows_since_step += n;
                    }
                }
            }
            Ingest::Connector(_) => unreachable!("connector sources handled separately"),
        }
    }

    // 4. Run continuous loop with backpressure
    let mut last_step = std::time::Instant::now();
    let mut last_checkpoint = std::time::Instant::now();
    let mut running = true;
    let mut checkpoint_counter = 0u64;

    while running {
        let mut made_progress = false;
        let mut saw_idle_unbounded = false;

        // Read from streaming sources with backpressure
        for src in &mut streaming_sources {
            let can_read =
                matches!(config.run_policy, RunPolicy::Once) || src.backpressure.is_available();
            if can_read {
                let next_batch = if let Some(batch) = src.pending_batches.pop_front() {
                    Some(batch)
                } else {
                    src.source.read_batch_dyn().await.map_err(rt)?
                };

                match next_batch {
                    Some(batch) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        // Feed batch to job
                        let delta = DeltaBatch::from_inserts(batch).map_err(rt)?;
                        let batch = delta.data_batch().clone();
                        job.feed(&src.name, &delta).await?;

                        // Update offset tracking for checkpoint
                        // The actual offset is managed by the connector internally
                        // We just track that data was read successfully

                        // Apply backpressure
                        src.backpressure.record_batch(&batch);
                        made_progress = true;
                    }
                    None if src.source.capabilities().is_bounded() => {
                        // Bounded source exhausted
                        src.backpressure.mark_exhausted();
                    }
                    None => {
                        // Unbounded source returned None (temporary)
                        // This is normal for sources waiting for data
                        saw_idle_unbounded = true;
                    }
                }
            }
        }

        // Step job based on run policy
        let elapsed = last_step.elapsed();
        let rows_ready = rows_since_step
            + streaming_sources
                .iter()
                .map(|s| s.backpressure.rows_since_step)
                .sum::<usize>();
        let backpressure_full = rows_ready > 0
            && streaming_sources
                .iter()
                .any(|s| !s.backpressure.is_available() && !s.backpressure.is_exhausted());
        let should_step = match config.run_policy {
            RunPolicy::Once => false,
            RunPolicy::OnChange => made_progress,
            RunPolicy::EveryRows(n) => rows_ready >= n.max(1) || backpressure_full,
            RunPolicy::EveryMs(ms) => {
                rows_ready > 0 && (elapsed.as_millis() >= ms as u128 || backpressure_full)
            }
        };

        if should_step {
            job.step().await?;
            last_step = std::time::Instant::now();

            // Write snapshots to sinks
            write_snapshots(&job, &mut sinks, &expectations).await?;

            // Reset row counters
            rows_since_step = 0;
            for src in &mut streaming_sources {
                src.backpressure.reset_after_step();
            }

            // Checkpoint if configured
            if let Some(interval_ms) = config.checkpoint_interval_ms
                && last_checkpoint.elapsed().as_millis() >= interval_ms as u128
            {
                checkpoint_counter = checkpoint_counter.saturating_add(1);
                let checkpoint = save_streaming_checkpoint(
                    &streaming_sources,
                    &format!("cp-{}", checkpoint_counter),
                )
                .await?;
                tracing::debug!(
                    timestamp_ms = checkpoint.timestamp_ms,
                    "Saved checkpoint {} with {} source offsets",
                    checkpoint.checkpoint_id,
                    checkpoint.source_offsets.len()
                );
                last_checkpoint = std::time::Instant::now();
            }
        }

        // Check if all bounded sources are exhausted. In `Once` mode, unbounded
        // sources are treated as drained once every currently-available batch
        // has been read and a poll returns idle.
        running =
            if matches!(config.run_policy, RunPolicy::Once) && saw_idle_unbounded && !made_progress
            {
                false
            } else {
                streaming_sources.iter().any(|s| {
                    !s.source.capabilities().is_bounded()
                        || !s.backpressure.is_exhausted()
                        || !s.pending_batches.is_empty()
                })
            };

        if running && !made_progress && !should_step {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    // Final flush for rows not covered by a policy-triggered step.
    let rows_ready = rows_since_step
        + streaming_sources
            .iter()
            .map(|s| s.backpressure.rows_since_step)
            .sum::<usize>();
    if rows_ready > 0 {
        job.step().await?;
        write_snapshots(&job, &mut sinks, &expectations).await?;
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::VecDeque;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use krishiv_connectors::{
        CheckpointSource, ConnectorCapabilities, ConnectorError, ConnectorResult, Offset, Source,
    };

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CursorOffset(usize);

    impl Offset for CursorOffset {
        fn encode(&self) -> Vec<u8> {
            (self.0 as u64).to_le_bytes().to_vec()
        }

        fn decode(bytes: &[u8]) -> ConnectorResult<Self> {
            if bytes.len() != 8 {
                return Err(ConnectorError::Offset {
                    message: format!("CursorOffset decode: expected 8 bytes, got {}", bytes.len()),
                });
            }
            let raw = bytes.get(..8).ok_or_else(|| ConnectorError::Offset {
                message: "CursorOffset decode: missing offset bytes".into(),
            })?;
            let arr: [u8; 8] = raw.try_into().map_err(|_| ConnectorError::Offset {
                message: "CursorOffset decode: slice length mismatch".into(),
            })?;
            Ok(Self(usize::try_from(u64::from_le_bytes(arr)).map_err(
                |_| ConnectorError::Offset {
                    message: "CursorOffset decode: value exceeds usize".into(),
                },
            )?))
        }
    }

    struct OffsetSource {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        cursor: usize,
    }

    impl Source for OffsetSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new()
                .with_bounded()
                .with_rewindable()
                .with_checkpoint()
        }

        fn source_schema(&self) -> Option<SchemaRef> {
            Some(self.schema.clone())
        }

        async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
            let batch = self.batches.get(self.cursor).cloned();
            if batch.is_some() {
                self.cursor = self.cursor.saturating_add(1);
            }
            Ok(batch)
        }

        fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
            Some(Box::new(CursorOffset(self.cursor)))
        }

        fn encoded_checkpoint_offset(&self) -> ConnectorResult<Option<Vec<u8>>> {
            CheckpointSource::encoded_checkpoint_offset(self).map(Some)
        }

        fn restore_encoded_checkpoint_offset(&mut self, encoded: &[u8]) -> ConnectorResult<()> {
            CheckpointSource::restore_encoded_offset(self, encoded)
        }

        fn reset(&mut self) {
            self.cursor = 0;
        }
    }

    impl CheckpointSource for OffsetSource {
        type Offset = CursorOffset;

        fn checkpoint_offset(&self) -> ConnectorResult<Self::Offset> {
            Ok(CursorOffset(self.cursor))
        }

        fn restore_offset(&mut self, offset: &Self::Offset) -> ConnectorResult<()> {
            if offset.0 > self.batches.len() {
                return Err(ConnectorError::Offset {
                    message: format!("cursor {} out of range", offset.0),
                });
            }
            self.cursor = offset.0;
            Ok(())
        }
    }

    fn batch(value: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap()
    }

    #[tokio::test]
    async fn streaming_checkpoint_uses_dyn_encoded_source_offsets() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let source = OffsetSource {
            schema,
            batches: vec![batch(10), batch(20)],
            cursor: 0,
        };
        let mut sources = vec![StreamingSource {
            name: "numbers".to_string(),
            source: Box::new(source),
            pending_batches: VecDeque::new(),
            backpressure: BackpressureController::new(BackpressureConfig::default()),
        }];

        let first = sources[0]
            .source
            .read_batch_dyn()
            .await
            .expect("first read")
            .expect("first batch");
        assert_eq!(first.num_rows(), 1);

        let checkpoint = save_streaming_checkpoint(&sources, "cp-1")
            .await
            .expect("checkpoint");
        assert_eq!(
            checkpoint.source_offsets.get("numbers"),
            Some(&CursorOffset(1).encode())
        );

        let second = sources[0]
            .source
            .read_batch_dyn()
            .await
            .expect("second read")
            .expect("second batch");
        assert_eq!(
            second
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            20
        );

        restore_streaming_checkpoint(&mut sources, &checkpoint)
            .await
            .expect("restore");

        let replayed = sources[0]
            .source
            .read_batch_dyn()
            .await
            .expect("replay read")
            .expect("replayed batch");
        assert_eq!(
            replayed
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            20
        );
    }
}
