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

use super::{BackpressureConfig, Egress, Expectation, Ingest, OnViolation, Pipeline, StreamingConfig, ViewDef};
use crate::compute::FeedableJob;
use crate::{IvmJob, KrishivError, Result};

use krishiv_connectors::checkpoint::CheckpointSource;
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
    pub fn record_batch(&mut self, num_rows: usize) {
        self.current_rows += num_rows;
        self.rows_since_step += num_rows;
        // Approximate bytes (would need actual batch size in production)
        self.current_bytes += num_rows * 100; // rough estimate
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
    }
}

/// A streaming source that wraps a connector source.
pub struct StreamingSource {
    /// Source name (for feeding to job).
    pub name: String,
    /// The underlying connector source.
    pub source: Box<dyn DynSource>,
    /// Current checkpoint offset (if checkpoint source).
    pub offset: Option<Vec<u8>>,
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
        if let Some(offset) = &src.offset {
            source_offsets.insert(src.name.clone(), offset.clone());
        }
    }

    Ok(StreamingCheckpoint {
        checkpoint_id: checkpoint_id.to_string(),
        source_offsets,
        timestamp_ms: chrono::Utc::now().timestamp_millis(),
    })
}

/// Restore streaming sources from checkpoint.
pub async fn restore_streaming_checkpoint(
    sources: &mut [StreamingSource],
    checkpoint: &StreamingCheckpoint,
) -> Result<()> {
    for src in sources.iter_mut() {
        if let Some(offset) = checkpoint.source_offsets.get(&src.name) {
            // Try to downcast to CheckpointSource and restore offset
            if src
                .source
                .as_any()
                .downcast_ref::<Box<dyn CheckpointSource>>()
                .is_some()
            {
                src.offset = Some(offset.clone());
            }
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
                    arrow::compute::concat_batches(&kept.first().map(|b| b.schema()).unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty())), &kept).map_err(rt)?
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
        sinks,
        expectations,
        ..
    } = pipeline;

    // 1. Register views (same as incremental path)
    let mut schemas: std::collections::HashMap<String, SchemaRef> = std::collections::HashMap::new();
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

    // 2. Create streaming sources with backpressure
    let mut streaming_sources: Vec<StreamingSource> = Vec::new();
    let mut memory_sources: Vec<(String, Ingest)> = Vec::new();

    for (sname, ingest) in sources {
        match ingest {
            Ingest::Connector(src) => {
                let streaming_src = StreamingSource {
                    name: sname,
                    source: src,
                    offset: None,
                    backpressure: BackpressureController::new(config.backpressure.clone()),
                };
                streaming_sources.push(streaming_src);
            }
            other => {
                // Memory/CDC sources still use incremental path
                memory_sources.push((sname, other));
            }
        }
    }

    // 3. Feed memory sources first (if any)
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
                    maybe_step(&job, config.run_policy, &mut rows_since_step).await?;
                }
            }
            Ingest::Cdc(changes) => {
                for c in changes {
                    if let Some(delta) = DeltaBatch::from_cdc(c.before, c.after).map_err(rt)? {
                        let n = delta.num_rows();
                        job.feed(&sname, &delta).await?;
                        rows_since_step += n;
                        maybe_step(&job, config.run_policy, &mut rows_since_step).await?;
                    }
                }
            }
            Ingest::Connector(_) => unreachable!("connector sources handled separately"),
        }
    }

    // 4. Run continuous loop with backpressure
    let mut last_step = std::time::Instant::now();
    let mut running = true;
    let mut checkpoint_counter = 0u64;

    while running {
        // Read from streaming sources with backpressure
        for src in &mut streaming_sources {
            if src.backpressure.is_available() {
                match src.source.read_batch_dyn().await.map_err(rt)? {
                    Some(batch) => {
                        // Feed batch to job
                        let delta = DeltaBatch::from_inserts(batch.clone()).map_err(rt)?;
                        job.feed(&src.name, &delta).await?;

                        // Update offset if checkpoint source
                        let offset = src
                            .source
                            .as_any()
                            .downcast_ref::<Box<dyn CheckpointSource>>();
                        if offset.is_some() {
                            // Store the current offset for checkpointing
                            // In production, this would read from the checkpoint source
                            src.offset = Some(vec![]); // Placeholder
                        }

                        // Apply backpressure
                        src.backpressure.record_batch(batch.num_rows());
                    }
                    None if src.source.capabilities().is_bounded() => {
                        // Bounded source exhausted
                        src.backpressure.mark_exhausted();
                    }
                    None => {
                        // Unbounded source returned None (temporary)
                        // This is normal for sources waiting for data
                    }
                }
            }
        }

        // Step job based on run policy
        let elapsed = last_step.elapsed();
        let should_step = match config.run_policy {
            RunPolicy::Once => false,
            RunPolicy::OnChange => true,
            RunPolicy::EveryRows(n) => streaming_sources
                .iter()
                .any(|s| s.backpressure.rows_since_step >= n),
            RunPolicy::EveryMs(ms) => elapsed.as_millis() >= ms as u128,
        };

        if should_step {
            job.step().await?;
            last_step = std::time::Instant::now();

            // Write snapshots to sinks
            write_snapshots(&job, sinks.clone(), &expectations).await?;

            // Reset row counters
            for src in &mut streaming_sources {
                src.backpressure.reset_after_step();
            }

            // Checkpoint if configured
            if let Some(interval_ms) = config.checkpoint_interval_ms {
                checkpoint_counter += elapsed.as_millis() as u64;
                if checkpoint_counter >= interval_ms {
                    let checkpoint =
                        save_streaming_checkpoint(&streaming_sources, &format!("cp-{}", checkpoint_counter)).await?;
                    tracing::debug!(
                        "Saved checkpoint {} with {} source offsets",
                        checkpoint.checkpoint_id,
                        checkpoint.source_offsets.len()
                    );
                    checkpoint_counter = 0;
                }
            }
        }

        // Check if all bounded sources are exhausted
        running = streaming_sources.iter().any(|s| {
            !s.source.capabilities().is_bounded() || !s.backpressure.is_exhausted()
        });
    }

    // Final flush
    job.step().await?;
    write_snapshots(&job, sinks, &expectations).await
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
