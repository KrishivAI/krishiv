#![forbid(unsafe_code)]

//! `IncrementalFlow` — driver for incremental view maintenance (IVM).
//!
//! # Execution model
//!
//! `step_datafusion` implements **diff-based IVM**:
//!
//! 1. Each source accumulates a running snapshot via `apply_delta`.
//! 2. Views execute in **topological order** (Kahn's algorithm on SQL tokens).
//! 3. Each view's full SQL result is **differenced** against the previous
//!    output (`diff_and_update`) to produce a true incremental `DeltaBatch`.
//! 4. Only non-empty deltas are published to watch subscribers.
//!
//! # Optimisations
//!
//! * **Dirty-bit scheduling**: views whose SQL references no dirty source or
//!   upstream view are skipped entirely; their previous snapshot is reused.
//! * **Content-addressed dedup**: opt-in per-source row-hash filter drops
//!   re-delivered insertion rows (at-least-once delivery resilience).
//! * **Delta checkpoints**: accumulate per-source `DeltaBatch`es and serialise
//!   only the incremental slice since the last checkpoint.
//! * **Streaming bridge**: `feed_snapshot` converts micro-batch output
//!   (all-positive `RecordBatch`es) into source `DeltaBatch`es.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use ahash::{AHashMap, AHashSet};
use arrow::array::{Array, RecordBatch};
use arrow::compute::cast;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use tokio::sync::watch;

use krishiv_delta::{
    DeltaBatch, DeltaError, IncrementalView, IncrementalViewRegistry, IncrementalViewSpec,
    LatenessSpec, WatermarkTracker, apply_delta, consolidate_batch, deserialize_delta_batch,
    differentiate, serialize_delta_batch,
};

use crate::error::{IvmError, IvmResult};
use crate::plan::{ViewPlan, ViewPlanKind};

/// Maximum number of row hashes retained per source for content-addressed dedup.
const DEDUP_SEEN_CAPACITY: usize = 10_000_000;

/// Number of oldest entries to evict when the dedup set is full.
/// Evicts 1% of the cap at a time so bursts only briefly allow re-delivery,
/// rather than the previous behaviour of clearing the entire set (which
/// silently re-admitted every previously-seen row).
const DEDUP_EVICT_BATCH: usize = 100_000;

/// Maximum iterations for recursive view fixpoint computation.
const MAX_FIXPOINT_ITERS: usize = 100;

// ── StepSummary ───────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct StepSummary {
    pub total_output_rows: usize,
    pub active_views: usize,
    /// View names that emitted a non-Apply output (degraded to DiffBased) during
    /// this step. Useful for surfacing join-type degradations to operators.
    pub degraded_views: Vec<String>,
    /// View names whose incremental operator or SQL execution returned an
    /// error and were silently skipped. The error message is the same string
    /// the operator logged. Step did not panic; subsequent ticks re-evaluate.
    pub errored_views: Vec<ViewError>,
}

/// One incremental view's failure during a step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewError {
    pub view: String,
    pub kind: ViewErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewErrorKind {
    /// The incremental operator (`apply`) returned an error (trace capacity,
    /// schema mismatch, type coercion, etc.).
    OperatorApply,
    /// The view's SQL body failed to execute (column not found, type mismatch).
    ViewSql,
    /// The view's published output failed (downstream backpressure, etc.).
    Publish,
}

// ── IncrementalFlowInner ──────────────────────────────────────────────────────

struct IncrementalFlowInner {
    view_registry: IncrementalViewRegistry,
    pending: HashMap<String, Vec<DeltaBatch>>,
    tick: u64,
    source_snapshots: HashMap<String, RecordBatch>,

    // Content-addressed dedup: opt-in per-source insertion row dedup.
    // Each entry is (insertion-order FIFO queue, fast-lookup set).
    // When the set reaches DEDUP_SEEN_CAPACITY the oldest DEDUP_EVICT_BATCH
    // entries are popped from the queue and removed from the set, so only a
    // small window of rows is re-admitted rather than the whole history.
    input_dedup_enabled: bool,
    seen_input_hashes: AHashMap<String, (VecDeque<u64>, AHashSet<u64>)>,

    // Delta checkpoints: accumulate deltas since last checkpoint call.
    delta_checkpoint_enabled: bool,
    checkpoint_deltas: HashMap<String, Vec<DeltaBatch>>,

    // Streaming → IVM bridge: previous materialized snapshot per source.
    // Used by feed_snapshot to differentiate consecutive snapshots.
    streaming_prev_snapshots: HashMap<String, RecordBatch>,

    // Opt-in provenance tracking: input row hash → output row hashes.
    provenance: Option<crate::provenance::ProvenanceIndex>,

    // Gap 1: cached incremental execution plans per view.
    view_plans: AHashMap<String, ViewPlan>,
    // SQL text that was used to build each cached plan (for Gap 7 invalidation).
    view_plan_sqls: AHashMap<String, String>,

    // Gap 5: last-processed offset per source (skip-if-unchanged).
    source_ordinals: AHashMap<String, Vec<u8>>,

    // Gap 6: LATENESS / watermark trackers per source.
    watermark_trackers: AHashMap<String, WatermarkTracker>,

    // Coordinator-authoritative distributed IVM: when true, step_datafusion
    // never uses cached incremental plans (whose accumulator state is not
    // transferable) and always recomputes views via full SQL + diff. Set on
    // the transient executor flow so a remote tick matches central compute.
    force_diff_based: bool,

    // Precise SQL dependency sets per view (populated at register_view time).
    // Maps view_name → set of lowercased table/view names referenced in FROM/JOIN.
    // Views absent from this map fall back to the conservative sql_identifiers
    // tokenizer for dirty-bit detection (see extract_sql_table_refs).
    view_deps: AHashMap<String, HashSet<String>>,

    // Per-step output deltas, keyed by view name, captured during the most
    // recent `step_datafusion`. Cleared at the start of each step. Lets a caller
    // consume the O(Δ) changelog the flow already computed (`take_step_output`)
    // instead of re-materializing the full view and diffing snapshots.
    last_step_outputs: AHashMap<String, DeltaBatch>,
}

// ── IncrementalFlow ───────────────────────────────────────────────────────────

/// Driver for an incremental computation pipeline.
///
/// Thread-safe and `Clone`-able: all clones share the same underlying state.
#[derive(Clone)]
pub struct IncrementalFlow {
    inner: Arc<Mutex<IncrementalFlowInner>>,
}

impl IncrementalFlow {
    pub fn new() -> Self {
        Self::with_registry(IncrementalViewRegistry::new())
    }

    /// Create a flow that shares an existing view registry with other components
    /// (e.g. the SQL engine). Views registered via SQL DDL (`CREATE INCREMENTAL
    /// VIEW`) are visible to this flow, and vice versa.
    pub fn with_registry(view_registry: IncrementalViewRegistry) -> Self {
        Self {
            inner: Arc::new(Mutex::new(IncrementalFlowInner {
                view_registry,
                pending: HashMap::new(),
                tick: 0,
                source_snapshots: HashMap::new(),
                input_dedup_enabled: false,
                seen_input_hashes: AHashMap::new(),
                delta_checkpoint_enabled: false,
                checkpoint_deltas: HashMap::new(),
                streaming_prev_snapshots: HashMap::new(),
                provenance: None,
                view_plans: AHashMap::new(),
                view_plan_sqls: AHashMap::new(),
                source_ordinals: AHashMap::new(),
                watermark_trackers: AHashMap::new(),
                force_diff_based: false,
                view_deps: AHashMap::new(),
                last_step_outputs: AHashMap::new(),
            })),
        }
    }

    /// Enable opt-in provenance tracking (input row hash → output row hashes).
    ///
    /// Once enabled, each `step_datafusion` call records the mapping from every
    /// input insertion hash to the output hashes produced that tick. Use
    /// `query_provenance` to look up which output rows a given input row produced,
    /// enabling automatic retraction without Z-set algebra.
    pub fn enable_provenance_tracking(&self) -> IvmResult<()> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.provenance = Some(crate::provenance::ProvenanceIndex::new());
        Ok(())
    }

    /// Query the provenance index for output hashes derived from `input_hash`.
    pub fn query_provenance(&self, input_hash: u64) -> IvmResult<Option<AHashSet<u64>>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner
            .provenance
            .as_ref()
            .and_then(|p| p.outputs_for(input_hash))
            .cloned())
    }

    /// Remove the provenance record for `input_hash` (call after retracting its outputs).
    pub fn forget_provenance(&self, input_hash: u64) -> IvmResult<()> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        if let Some(ref mut p) = inner.provenance {
            p.forget(input_hash);
        }
        Ok(())
    }

    /// Take all pending input deltas (for external dispatch, e.g. executor fragments).
    ///
    /// Extracts and clears the current pending queue without running a tick.
    /// Use this to encode a `delta:step:` fragment for executor-side execution.
    pub fn take_pending(&self) -> IvmResult<HashMap<String, Vec<DeltaBatch>>> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        Ok(std::mem::take(&mut inner.pending))
    }

    /// Enable content-addressed dedup for all sources.
    ///
    /// Once enabled, re-delivered insertion rows (same hash as a previously
    /// accepted row) are silently dropped. Retractions always pass through.
    pub fn enable_input_dedup(&self) -> IvmResult<()> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.input_dedup_enabled = true;
        Ok(())
    }

    /// Enable accumulation of per-source deltas for `checkpoint_delta`.
    pub fn enable_delta_checkpoints(&self) -> IvmResult<()> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.delta_checkpoint_enabled = true;
        Ok(())
    }

    /// Force every `step_datafusion` to use full SQL recompute + diff
    /// (`DiffBased`), bypassing cached incremental plans.
    ///
    /// Incremental plans carry accumulator state that is **not** captured by
    /// `checkpoint_full`, so a transient executor flow restored from a
    /// coordinator snapshot must not use them — it would emit deltas computed
    /// against an empty accumulator rather than the restored baseline. Setting
    /// this flag makes a remote tick bit-identical to a central tick.
    pub fn force_diff_based(&self) -> IvmResult<()> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.force_diff_based = true;
        Ok(())
    }

    /// Register or re-register an incremental view.
    ///
    /// **Idempotent**: re-registering a view with an identical spec (same SQL,
    /// materialized, recursive flags) is a no-op that **preserves the view's
    /// accumulated state** — this is what lets a named pipeline run incrementally
    /// across repeated `run()` calls instead of recomputing from scratch.
    ///
    /// When the spec *changes*, the view is re-registered: its `full_output`
    /// baseline is reset (behavior-version invalidation — the next tick treats
    /// the full SQL result as insertions) and the cached incremental plan is
    /// cleared so a fresh plan is built.
    pub fn register_view(&self, spec: IncrementalViewSpec) -> IvmResult<()> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        if let Ok(existing) = inner.view_registry.get(&spec.name) {
            let unchanged = existing.spec.body_sql == spec.body_sql
                && existing.spec.is_materialized == spec.is_materialized
                && existing.spec.is_recursive == spec.is_recursive;
            if unchanged {
                // Identical re-registration — keep the view and its state.
                return Ok(());
            }
            // Spec changed: reset the baseline and invalidate the cached plan.
            let _ = existing.reset_full_output();
            inner.view_plans.remove(&spec.name);
            inner.view_plan_sqls.remove(&spec.name);
            inner.view_deps.remove(&spec.name);
        }
        // Populate precise SQL dep set for fast dirty-bit detection.
        // Falls back to sql_identifiers at tick time for views where parsing fails
        // or the SQL contains subqueries (see extract_sql_table_refs).
        if let Some(deps) = extract_sql_table_refs(&spec.body_sql) {
            inner.view_deps.insert(spec.name.clone(), deps);
        }
        inner.view_registry.register(spec).map_err(delta_err)
    }

    pub fn drop_view(&self, name: &str) -> IvmResult<bool> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.view_deps.remove(name);
        inner.view_plans.remove(name);
        inner.view_plan_sqls.remove(name);
        inner.view_registry.drop_view(name).map_err(delta_err)
    }

    // ── Gap 6: LATENESS registration ──────────────────────────────────────────

    /// Register a LATENESS annotation on a source column.
    ///
    /// Once registered, the watermark for this source advances as records are
    /// ingested. Join operator traces can be GC'd via `gc_watermark` on the
    /// corresponding `ViewPlan::Join`.
    pub fn register_lateness(&self, source_name: &str, spec: LatenessSpec) -> IvmResult<()> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner
            .watermark_trackers
            .insert(source_name.to_string(), WatermarkTracker::new(spec));
        Ok(())
    }

    /// Return the current watermark (milliseconds) for a source, or `i64::MIN`
    /// if no lateness spec has been registered for it.
    pub fn watermark_for(&self, source_name: &str) -> IvmResult<i64> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner
            .watermark_trackers
            .get(source_name)
            .map(|t| t.watermark())
            .unwrap_or(i64::MIN))
    }

    // ── Source-ordinal skip-if-unchanged ──────────────────────────────────────

    /// Feed a delta only if the source's offset (ordinal) has advanced.
    ///
    /// If `ordinal == last_processed_ordinal`, the delta is silently dropped.
    /// This prevents re-processing when a source snapshot is re-delivered.
    /// Stateful: owns the per-source `source_ordinals` map, so it cannot be a
    /// `DeltaBatch` constructor — it stays a method on the flow.
    pub fn feed_if_advanced(
        &self,
        source_name: impl Into<String>,
        batch: DeltaBatch,
        ordinal: Vec<u8>,
    ) -> IvmResult<()> {
        let source_name = source_name.into();
        {
            let mut inner = self.inner.lock().map_err(lock_err)?;
            if let Some(last) = inner.source_ordinals.get(&source_name)
                && *last == ordinal
            {
                return Ok(()); // Same offset — nothing new.
            }
            inner.source_ordinals.insert(source_name.clone(), ordinal);
        } // Release lock before calling feed.
        self.feed(source_name, batch)
    }

    /// Push a `DeltaBatch` as input for a named source on the next step.
    ///
    /// This is the single canonical feed primitive. Build the `DeltaBatch` with
    /// the appropriate constructor first:
    /// - `DeltaBatch::from_inserts(batch)` — plain rows / batch / shuffle output
    /// - `DeltaBatch::from_deletes(batch)` — retractions
    /// - `DeltaBatch::from_cdc(before, after)` — CDC INSERT/DELETE/UPDATE
    ///
    /// If content-addressed dedup is enabled, insertion rows already seen in a
    /// prior tick are silently dropped.
    pub fn feed(&self, source_name: impl Into<String>, batch: DeltaBatch) -> IvmResult<()> {
        let source_name = source_name.into();
        let mut inner = self.inner.lock().map_err(lock_err)?;

        // Content-addressed dedup: filter out re-delivered insertion rows.
        let batch = if inner.input_dedup_enabled {
            let (order, set) = inner
                .seen_input_hashes
                .entry(source_name.clone())
                .or_default();
            // Evict oldest entries when capacity is reached. Evicting a small
            // batch (1% of capacity) means only those rows can be re-delivered
            // on the next burst, versus the old full-clear which re-admitted
            // the entire history.
            if set.len() >= DEDUP_SEEN_CAPACITY {
                tracing::warn!(
                    source = %source_name,
                    capacity = DEDUP_SEEN_CAPACITY,
                    evicting = DEDUP_EVICT_BATCH,
                    "dedup set capacity reached; evicting oldest entries"
                );
                for _ in 0..DEDUP_EVICT_BATCH {
                    if let Some(h) = order.pop_front() {
                        set.remove(&h);
                    } else {
                        break;
                    }
                }
            }
            let data = batch.data_batch();
            let weights = batch.weights();
            let mask: arrow::array::BooleanArray = (0..data.num_rows())
                .map(|row| {
                    if weights.value(row) > 0 {
                        let h = hash_row(&data, row);
                        if set.contains(&h) {
                            Some(false) // already seen
                        } else {
                            set.insert(h);
                            order.push_back(h);
                            Some(true)
                        }
                    } else {
                        Some(true) // retractions always pass
                    }
                })
                .collect();
            batch.filter_mask(&mask).map_err(delta_err)?
        } else {
            batch
        };

        if batch.is_empty() {
            return Ok(());
        }

        // Accumulate for delta checkpoints.
        if inner.delta_checkpoint_enabled {
            inner
                .checkpoint_deltas
                .entry(source_name.clone())
                .or_default()
                .push(batch.clone());
        }

        inner.pending.entry(source_name).or_default().push(batch);
        Ok(())
    }

    /// Feed a full streaming snapshot into IVM by differentiating against the
    /// previously-fed snapshot for this source.
    ///
    /// Streaming jobs typically output a **full materialized snapshot** each tick.
    /// This method calls `differentiate(prev_snapshot, new_snapshot)` to extract the
    /// true delta (insertions and retractions) before pushing it to `feed`.
    ///
    /// On the first call for a source, all rows are treated as insertions (no previous
    /// snapshot). Identical consecutive snapshots produce an empty delta and no tick work.
    ///
    /// Stateful: owns the per-source `streaming_prev_snapshots` map (which
    /// participates in checkpoint/restore), so it cannot be a `DeltaBatch`
    /// constructor. Use `feed` directly if your producer already emits `DeltaBatch`es.
    pub fn feed_snapshot(
        &self,
        source_name: impl Into<String>,
        batches: &[RecordBatch],
    ) -> IvmResult<()> {
        let name: String = source_name.into();

        // Combine all incoming batches into one new snapshot.
        let non_empty: Vec<&RecordBatch> = batches.iter().filter(|b| b.num_rows() > 0).collect();
        if non_empty.is_empty() {
            return Ok(());
        }
        let first = *non_empty
            .first()
            .ok_or_else(|| IvmError::execution("empty batch list".to_string()))?;
        let schema = first.schema();
        let new_snapshot = if non_empty.len() == 1 {
            first.clone()
        } else {
            arrow::compute::concat_batches(&schema, non_empty.iter().copied())
                .map_err(|e| IvmError::execution(e.to_string()))?
        };

        // Differentiate: true delta vs previous snapshot.
        let mut inner = self.inner.lock().map_err(lock_err)?;
        let prev = inner.streaming_prev_snapshots.get(&name);
        let delta = differentiate(&schema, prev, &new_snapshot).map_err(delta_err)?;
        inner
            .streaming_prev_snapshots
            .insert(name.clone(), new_snapshot);

        if delta.is_empty() {
            return Ok(());
        }

        // Accumulate for delta checkpoints.
        if inner.delta_checkpoint_enabled {
            inner
                .checkpoint_deltas
                .entry(name.clone())
                .or_default()
                .push(delta.clone());
        }

        inner.pending.entry(name).or_default().push(delta);
        Ok(())
    }

    /// Structural step: drain pending, bump tick, no SQL.
    pub fn step(&self) -> IvmResult<StepSummary> {
        self.step_with(|_inputs| Ok(HashMap::new()))
    }

    /// Step with a user-supplied compute callback.
    pub fn step_with<F>(&self, mut compute: F) -> IvmResult<StepSummary>
    where
        F: FnMut(HashMap<String, DeltaBatch>) -> IvmResult<HashMap<String, DeltaBatch>>,
    {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        let raw = std::mem::take(&mut inner.pending);
        inner.tick += 1;
        let inputs = coalesce_pending(raw)?;
        let output_deltas = compute(inputs)?;
        let mut total_output_rows = 0usize;
        let mut active_views = 0usize;
        for (view_name, delta) in output_deltas {
            if let Ok(view) = inner.view_registry.get(&view_name) {
                if !delta.is_empty() {
                    total_output_rows += delta.num_rows();
                    active_views += 1;
                }
                let _ = view.publish_output(delta);
            }
        }
        Ok(StepSummary {
            total_output_rows,
            active_views,
            degraded_views: Vec::new(),
            errored_views: Vec::new(),
        })
    }

    /// Advance one tick using DataFusion to execute view SQL.
    pub async fn step_datafusion(&self) -> IvmResult<StepSummary> {
        self.step_datafusion_with_ctx(&SessionContext::new()).await
    }

    /// Advance one tick using the supplied `SessionContext`.
    ///
    /// Views whose SQL references no dirty source or upstream view are skipped
    /// (dirty-bit scheduling); their previous snapshot is reused unchanged.
    ///
    /// Views with a cached incremental plan (`ViewPlan::Aggregate`, `Join`,
    /// `Distinct`) are executed O(Δ) without running DataFusion SQL. Views
    /// with `ViewPlan::DiffBased` (or no cached plan yet) fall back to full
    /// SQL re-execution + diff.
    pub async fn step_datafusion_with_ctx(&self, ctx: &SessionContext) -> IvmResult<StepSummary> {
        // ── Phase 1 (lock): drain pending + snapshot state ────────────────────
        let (
            raw_pending,
            current_snapshots,
            view_specs,
            view_prev_snapshots,
            view_plan_kinds,
            views_needing_plans,
            force_diff_based,
            view_deps,
        ) = {
            let mut inner = self.inner.lock().map_err(lock_err)?;
            let raw = std::mem::take(&mut inner.pending);
            let snapshots = inner.source_snapshots.clone();
            let force_diff_based = inner.force_diff_based;
            let names = inner.view_registry.view_names().map_err(delta_err)?;
            let specs: Vec<IncrementalViewSpec> = names
                .iter()
                .filter_map(|n| inner.view_registry.get(n).ok().map(|v| v.spec.clone()))
                .collect();
            let prev_outputs: HashMap<String, RecordBatch> = names
                .iter()
                .filter_map(|n| {
                    inner
                        .view_registry
                        .get(n)
                        .ok()
                        .and_then(|v| v.snapshot().ok().flatten())
                        .map(|snap| (n.clone(), snap))
                })
                .collect();
            // Gap 1: extract plan kinds so Phase 4 can skip SQL for incremental views.
            let plan_kinds: AHashMap<String, ViewPlanKind> = inner
                .view_plans
                .iter()
                .map(|(k, v)| (k.clone(), v.kind()))
                .collect();
            let needs_plans: HashSet<String> = names
                .iter()
                .filter(|n| !inner.view_plans.contains_key(n.as_str()))
                .cloned()
                .collect();
            // Snapshot of precise SQL deps for dirty-bit detection.
            let deps = inner.view_deps.clone();
            (
                raw,
                snapshots,
                specs,
                prev_outputs,
                plan_kinds,
                needs_plans,
                force_diff_based,
                deps,
            )
        };

        // ── Phase 2 (no lock): coalesce deltas ───────────────────────────────
        let inputs = coalesce_pending(raw_pending)?;

        if inputs.is_empty() {
            let mut inner = self.inner.lock().map_err(lock_err)?;
            // A step with no input changes nothing, so no per-step delta exists.
            inner.last_step_outputs.clear();
            inner.tick += 1;
            return Ok(StepSummary::default());
        }

        let dirty_sources: HashSet<String> = inputs.keys().map(|k| k.to_lowercase()).collect();

        let mut new_snapshots = current_snapshots;
        for (name, delta) in &inputs {
            let current = new_snapshots.remove(name);
            let updated = apply_delta(current, delta).map_err(delta_err)?;
            new_snapshots.insert(name.clone(), updated);
        }

        // ── Phase 3 (no lock): register source MemTables ─────────────────────
        for (name, snapshot) in &new_snapshots {
            if snapshot.num_rows() == 0 {
                continue;
            }
            let schema = snapshot.schema();
            let table = MemTable::try_new(schema, vec![vec![snapshot.clone()]])
                .map_err(|e| IvmError::execution(e.to_string()))?;
            ctx.register_table(name.as_str(), Arc::new(table))
                .map_err(|e| IvmError::execution(e.to_string()))?;
        }

        // ── Phase 4 (no lock): build plans + execute DiffBased SQL ───────────
        let topo = toposort_views(&view_specs, &view_deps);
        let spec_map: HashMap<&str, &IncrementalViewSpec> =
            view_specs.iter().map(|s| (s.name.as_str(), s)).collect();

        // Schema map for plan construction: sources + upstream view schemas.
        let mut available_schemas: AHashMap<String, SchemaRef> = AHashMap::new();
        for (name, snap) in &new_snapshots {
            available_schemas.insert(name.clone(), snap.schema());
        }
        for spec in &view_specs {
            available_schemas.insert(spec.name.clone(), spec.output_schema.clone());
        }

        // view_full_outputs: pre-populated with prev snapshots for clean views.
        // DiffBased dirty views add their SQL result here during this phase.
        let mut view_full_outputs: HashMap<String, RecordBatch> = view_prev_snapshots;
        // Capture view-SQL execution errors from the lock-free Phase 3
        // execution path so they can be surfaced in the StepSummary
        // returned by Phase 5+6 (which holds the lock).
        let mut pre_lock_view_errors: Vec<ViewError> = Vec::new();
        let mut dirty_views: HashSet<String> = HashSet::new();
        // Newly built plans to insert in Phase 5: (name, plan, body_sql)
        let mut new_plans: Vec<(String, ViewPlan, String)> = Vec::new();

        for view_name in &topo {
            let spec = match spec_map.get(view_name.as_str()) {
                Some(s) => s,
                None => continue,
            };

            let view_name_lower = view_name.to_lowercase();
            let is_dirty = view_deps
                .get(view_name)
                .map(|deps| {
                    deps.iter()
                        .any(|dep| dirty_sources.contains(dep) || dirty_views.contains(dep))
                })
                .unwrap_or_else(|| {
                    sql_identifiers(&spec.body_sql).iter().any(|token| {
                        dirty_sources.contains(token.as_str())
                            || dirty_views.contains(token.as_str())
                    })
                });
            if !is_dirty {
                continue;
            }
            dirty_views.insert(view_name_lower);

            // Determine if this view gets an incremental plan (skip SQL) or DiffBased (run SQL).
            // `force_diff_based` (transient executor flows) never uses incremental
            // plans: their accumulator state is not transferable via checkpoint.
            let plan_is_incremental = if force_diff_based {
                false
            } else if views_needing_plans.contains(view_name) {
                let plan = crate::plan::build_view_plan(
                    ctx,
                    &spec.body_sql,
                    &spec.output_schema,
                    &available_schemas,
                )
                .await;
                let is_incr = matches!(plan.kind(), ViewPlanKind::Incremental);
                new_plans.push((view_name.clone(), plan, spec.body_sql.clone()));
                is_incr
            } else {
                view_plan_kinds
                    .get(view_name)
                    .copied()
                    .map(|k| k == ViewPlanKind::Incremental)
                    .unwrap_or(false)
            };

            if plan_is_incremental {
                // Register the previous snapshot for downstream DiffBased views.
                if let Some(prev) = view_full_outputs.get(view_name)
                    && prev.num_rows() > 0
                {
                    let schema = prev.schema();
                    if let Ok(table) = MemTable::try_new(schema, vec![vec![prev.clone()]]) {
                        let _ = ctx.register_table(view_name.as_str(), Arc::new(table));
                    }
                }
            } else {
                // DiffBased: register all upstream outputs, then execute SQL.
                for (up_name, up_batch) in &view_full_outputs {
                    if up_batch.num_rows() == 0 {
                        continue;
                    }
                    let schema = up_batch.schema();
                    if let Ok(table) = MemTable::try_new(schema, vec![vec![up_batch.clone()]]) {
                        let _ = ctx.register_table(up_name.as_str(), Arc::new(table));
                    }
                }

                macro_rules! make_empty_batch {
                    ($spec:expr) => {{
                        let empty_cols: Vec<_> = $spec
                            .output_schema
                            .fields()
                            .iter()
                            .map(|f| arrow::array::new_empty_array(f.data_type()))
                            .collect();
                        RecordBatch::try_new($spec.output_schema.clone(), empty_cols)
                            .map_err(|e| IvmError::execution(e.to_string()))?
                    }};
                }

                if spec.is_recursive {
                    // Fixpoint iteration: re-run SQL until output stabilises.
                    let mut iter = 0usize;
                    loop {
                        let new_full = match execute_view_sql(ctx, spec).await {
                            Ok(rb) => rb,
                            Err(e) => {
                                tracing::warn!(
                                    view = %view_name,
                                    error = %e,
                                    "view SQL execution failed; using empty batch"
                                );
                                make_empty_batch!(spec)
                            }
                        };
                        let prev = view_full_outputs.get(view_name).map(|b| b as &RecordBatch);
                        let converged = differentiate(&spec.output_schema, prev, &new_full)
                            .map(|d| d.is_empty())
                            .unwrap_or(true);
                        let hit_limit = iter >= MAX_FIXPOINT_ITERS;
                        if hit_limit {
                            tracing::warn!(
                                view = %view_name,
                                "recursive view reached MAX_FIXPOINT_ITERS without convergence"
                            );
                        }
                        view_full_outputs.insert(view_name.clone(), new_full.clone());
                        if converged || hit_limit {
                            break;
                        }
                        // Register updated self-view for the next iteration.
                        if new_full.num_rows() > 0 {
                            let _ = ctx.deregister_table(view_name.as_str());
                            match MemTable::try_new(new_full.schema(), vec![vec![new_full]]) {
                                Ok(tbl) => {
                                    if let Err(e) =
                                        ctx.register_table(view_name.as_str(), Arc::new(tbl))
                                    {
                                        tracing::warn!(
                                            view = %view_name,
                                            error = %e,
                                            "fixpoint: failed to register updated view; \
                                             next iteration will use stale data"
                                        );
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        view = %view_name,
                                        error = %e,
                                        "fixpoint: failed to build MemTable for updated view"
                                    );
                                }
                            }
                        }
                        iter += 1;
                    }
                } else {
                    let new_full = match execute_view_sql(ctx, spec).await {
                        Ok(rb) => rb,
                        Err(e) => {
                            tracing::warn!(
                                view = %view_name,
                                error = %e,
                                "view SQL execution failed; using empty batch"
                            );
                            pre_lock_view_errors.push(ViewError {
                                view: view_name.clone(),
                                kind: ViewErrorKind::ViewSql,
                                message: e.to_string(),
                            });
                            make_empty_batch!(spec)
                        }
                    };
                    view_full_outputs.insert(view_name.clone(), new_full);
                }
            }
        }

        // ── Phase 5+6 (lock): apply plans / diff, publish, update state ───────
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.source_snapshots = new_snapshots;
        inner.last_step_outputs.clear();
        inner.tick += 1;
        let mut total_output_rows = 0usize;
        let mut active_views = 0usize;
        let mut errored_views: Vec<ViewError> = pre_lock_view_errors;
        let mut degraded_views: Vec<String> = Vec::new();

        // Provenance: pre-compute weight-aware input hashes when enabled.
        // Each row is hashed with its weight encoded so rows that differ only
        // in multiplicity produce distinct provenance entries (G5 fix).
        let input_hashes: Option<Vec<u64>> = if inner.provenance.is_some() {
            let hashes = inputs
                .values()
                .flat_map(|delta| {
                    let data = delta.data_batch();
                    let weights = delta.weights();
                    (0..data.num_rows()).map(move |row| {
                        let base = hash_row(&data, row);
                        let w = weights.value(row);
                        // Mix weight into the hash so weight=+1 ≠ weight=+2.
                        base.wrapping_add(w.unsigned_abs().wrapping_mul(0x9e37_79b9_7f4a_7c15))
                    })
                })
                .collect();
            Some(hashes)
        } else {
            None
        };

        // Insert newly built plans.
        for (name, plan, sql) in new_plans {
            inner.view_plan_sqls.insert(name.clone(), sql);
            inner.view_plans.insert(name, plan);
        }

        // Accumulate deltas: start with source deltas; views append as processed.
        let mut available_deltas: AHashMap<String, DeltaBatch> =
            inputs.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        // Collect view Arcs (clone from registry) before any mutable borrows.
        let dirty_view_arcs: Vec<(String, Arc<IncrementalView>)> = topo
            .iter()
            .filter(|n| dirty_views.contains(&n.to_lowercase()))
            .filter_map(|n| inner.view_registry.get(n).ok().map(|v| (n.clone(), v)))
            .collect();

        for (view_name, view) in &dirty_view_arcs {
            // Read plan kind (releases borrow immediately via .map). Forced
            // DiffBased (transient executor flows) ignores cached plans.
            let plan_kind = if inner.force_diff_based {
                ViewPlanKind::DiffBased
            } else {
                inner
                    .view_plans
                    .get(view_name)
                    .map(|p| p.kind())
                    .unwrap_or(ViewPlanKind::DiffBased)
            };
            // Record views that ended up on the O(state) DiffBased path
            // (forced or because the only cached plan was DiffBased). This
            // surfaces the join-type degradation noted in the IVM plan code.
            if matches!(plan_kind, ViewPlanKind::DiffBased) {
                degraded_views.push(view_name.clone());
            }

            let output_delta = if plan_kind == ViewPlanKind::Incremental {
                // O(Δ) path: apply stateful operator.
                match inner.view_plans.get_mut(view_name) {
                    Some(ViewPlan::Aggregate { source, op }) => {
                        let src = source.clone();
                        let delta = match available_deltas.get(&src).cloned() {
                            Some(d) => d,
                            None => continue,
                        };
                        match op.apply(delta) {
                            Ok(d) => d,
                            Err(e) => {
                                tracing::warn!(
                                    view = %view_name,
                                    error = %e,
                                    "incremental view aggregate apply failed; skipping view"
                                );
                                errored_views.push(ViewError {
                                    view: view_name.clone(),
                                    kind: ViewErrorKind::OperatorApply,
                                    message: e.to_string(),
                                });
                                continue;
                            }
                        }
                    }
                    Some(ViewPlan::Join {
                        left_source,
                        right_source,
                        op,
                    }) => {
                        let left = available_deltas.get(left_source.as_str()).cloned();
                        let right = available_deltas.get(right_source.as_str()).cloned();
                        if left.is_none() && right.is_none() {
                            continue;
                        }
                        match op.apply(left, right) {
                            Ok(d) => d,
                            Err(e) => {
                                tracing::warn!(
                                    view = %view_name,
                                    error = %e,
                                    "incremental view join apply failed; skipping view"
                                );
                                errored_views.push(ViewError {
                                    view: view_name.clone(),
                                    kind: ViewErrorKind::OperatorApply,
                                    message: e.to_string(),
                                });
                                continue;
                            }
                        }
                    }
                    Some(ViewPlan::Distinct { source, op }) => {
                        let src = source.clone();
                        let delta = match available_deltas.get(&src).cloned() {
                            Some(d) => d,
                            None => continue,
                        };
                        match op.apply(delta) {
                            Ok(d) => d,
                            Err(e) => {
                                tracing::warn!(
                                    view = %view_name,
                                    error = %e,
                                    "incremental view distinct apply failed; skipping view"
                                );
                                errored_views.push(ViewError {
                                    view: view_name.clone(),
                                    kind: ViewErrorKind::OperatorApply,
                                    message: e.to_string(),
                                });
                                continue;
                            }
                        }
                    }
                    _ => continue,
                }
            } else {
                // DiffBased path: diff SQL result against previous snapshot.
                let new_full = match view_full_outputs.get(view_name).cloned() {
                    Some(b) => b,
                    None => continue,
                };
                match view.diff_and_update(new_full) {
                    Ok(d) => d,
                    Err(e) => {
                        tracing::warn!(
                            view = %view_name,
                            error = %e,
                            "incremental view diff_and_update failed; skipping view"
                        );
                        errored_views.push(ViewError {
                            view: view_name.clone(),
                            kind: ViewErrorKind::ViewSql,
                            message: e.to_string(),
                        });
                        continue;
                    }
                }
            };

            if output_delta.is_empty() {
                continue;
            }
            total_output_rows += output_delta.num_rows();
            active_views += 1;

            // Provenance (DiffBased only).
            if plan_kind == ViewPlanKind::DiffBased
                && let (Some(input_hs), Some(prov)) = (&input_hashes, &mut inner.provenance)
            {
                let output_hs = crate::provenance::hash_all_rows(&output_delta.data_batch());
                for &ih in input_hs {
                    prov.record_many(ih, output_hs.iter().copied());
                }
            }

            // Propagate this view's output delta to downstream views.
            available_deltas.insert(view_name.clone(), output_delta.clone());
            // Retain it so a caller can consume the O(Δ) changelog directly.
            inner
                .last_step_outputs
                .insert(view_name.clone(), output_delta.clone());
            if let Err(e) = view.publish_output(output_delta) {
                tracing::warn!(
                    view = %view_name,
                    error = %e,
                    is_materialized = view.spec.is_materialized,
                    "publish_output failed"
                );
                errored_views.push(ViewError {
                    view: view_name.clone(),
                    kind: ViewErrorKind::Publish,
                    message: e.to_string(),
                });
            }
        }

        // Gap 6: GC join traces for sources with watermark trackers.
        let watermarks: AHashMap<String, i64> = inner
            .watermark_trackers
            .iter()
            .map(|(k, v)| (k.clone(), v.watermark()))
            .collect();
        if !watermarks.is_empty() {
            for plan in inner.view_plans.values_mut() {
                let _ = plan.gc_watermark(&watermarks);
            }
        }

        Ok(StepSummary {
            total_output_rows,
            active_views,
            degraded_views,
            errored_views,
        })
    }

    // ── Subscriptions / snapshots ─────────────────────────────────────────────

    pub fn view_output_stream(&self, name: &str) -> IvmResult<watch::Receiver<Option<DeltaBatch>>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        let view = inner.view_registry.get(name).map_err(delta_err)?;
        Ok(view.subscribe())
    }

    /// Peek the view's latest emitted output delta without subscribing.
    ///
    /// Returns a clone of the current watch value (`None` until the first
    /// non-empty output). Used by the `/output` HTTP endpoint and by partitioned
    /// flows to merge per-shard outputs.
    pub fn view_output_peek(&self, name: &str) -> IvmResult<Option<DeltaBatch>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        let view = inner.view_registry.get(name).map_err(delta_err)?;
        let rx = view.subscribe();
        let value = rx.borrow().clone();
        Ok(value)
    }

    pub fn snapshot(&self, name: &str) -> IvmResult<Option<RecordBatch>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        let view = inner.view_registry.get(name).map_err(delta_err)?;
        view.snapshot().map_err(delta_err)
    }

    /// Take this view's output delta from the most recent `step` — the
    /// insertions and retractions the flow computed for that step — removing it
    /// so a later call returns `None` until another step runs.
    ///
    /// This is the O(Δ) changelog the flow already produces internally; a caller
    /// maintaining an external sink should prefer it over `snapshot` plus an
    /// external `differentiate`, which is O(view size) per step. Returns `None`
    /// when the last step produced no change for the view.
    pub fn take_step_output(&self, name: &str) -> IvmResult<Option<DeltaBatch>> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner.last_step_outputs.remove(name))
    }

    pub fn view_spec(&self, name: &str) -> IvmResult<Option<IncrementalViewSpec>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner.view_registry.get(name).ok().map(|v| v.spec.clone()))
    }

    pub fn source_snapshot(&self, name: &str) -> IvmResult<Option<RecordBatch>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner.source_snapshots.get(name).cloned())
    }

    pub fn view_names(&self) -> IvmResult<Vec<String>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        inner.view_registry.view_names().map_err(delta_err)
    }

    pub fn view_specs(&self) -> IvmResult<Vec<IncrementalViewSpec>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        let names = inner.view_registry.view_names().map_err(delta_err)?;
        names
            .into_iter()
            .map(|n| {
                inner
                    .view_registry
                    .get(&n)
                    .map(|v| v.spec.clone())
                    .map_err(delta_err)
            })
            .collect()
    }

    pub fn tick(&self) -> IvmResult<u64> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner.tick)
    }

    // ── Checkpoint / restore ──────────────────────────────────────────────────

    /// Serialize all source snapshots to Arrow IPC bytes (full checkpoint).
    ///
    /// Format: `u32 count || (u32 name_len || name_bytes || u32 data_len || ipc_bytes)*`
    pub fn checkpoint(&self) -> IvmResult<Vec<u8>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        let mut out: Vec<u8> = Vec::new();
        let entries: Vec<(&String, &RecordBatch)> = inner.source_snapshots.iter().collect();
        out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for (name, snapshot) in entries {
            let delta = DeltaBatch::from_inserts(snapshot.clone()).map_err(delta_err)?;
            let ipc = serialize_delta_batch(&delta).map_err(delta_err)?;
            let name_bytes = name.as_bytes();
            out.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(name_bytes);
            out.extend_from_slice(&(ipc.len() as u32).to_le_bytes());
            out.extend_from_slice(&ipc);
        }
        Ok(out)
    }

    /// Restore source snapshots from bytes produced by [`checkpoint`].
    pub fn restore(&self, bytes: &[u8]) -> IvmResult<()> {
        let mut pos = 0usize;
        let n = read_u32(bytes, &mut pos)? as usize;
        let mut source_snapshots: HashMap<String, RecordBatch> = HashMap::with_capacity(n);
        for _ in 0..n {
            let name_len = read_u32(bytes, &mut pos)? as usize;
            let name = std::str::from_utf8(bytes.get(pos..pos + name_len).ok_or_else(slice_err)?)
                .map_err(|e| IvmError::execution(e.to_string()))?
                .to_string();
            pos += name_len;
            let data_len = read_u32(bytes, &mut pos)? as usize;
            let data = bytes.get(pos..pos + data_len).ok_or_else(slice_err)?;
            pos += data_len;
            let delta = deserialize_delta_batch(data).map_err(delta_err)?;
            let snapshot = delta.filter_positive().map_err(delta_err)?;
            source_snapshots.insert(name, snapshot);
        }
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.source_snapshots = source_snapshots;
        let names = inner.view_registry.view_names().map_err(delta_err)?;
        for name in &names {
            if let Ok(view) = inner.view_registry.get(name) {
                let _ = view.reset_full_output();
            }
        }
        Ok(())
    }

    /// Re-insert previously drained pending deltas back into the queue.
    ///
    /// Used by coordinator-authoritative distributed dispatch to restore the
    /// pending queue when a remote executor tick fails and the coordinator
    /// must fall back to local compute. No tick is advanced.
    pub fn re_feed(&self, pending: HashMap<String, Vec<DeltaBatch>>) -> IvmResult<()> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        for (source, batches) in pending {
            inner.pending.entry(source).or_default().extend(batches);
        }
        Ok(())
    }

    /// Apply a tick that was computed remotely.
    ///
    /// `local_pending` is the pending queue the coordinator drained before
    /// dispatch (it is *not* re-read from `self`). `view_full_outputs` is the
    /// full materialized output per view, as computed by the executor. This
    /// method coalesces the pending deltas, advances `source_snapshots`
    /// deterministically (matching what `step_datafusion` does), replaces each
    /// view's full state wholesale (so the diff baseline cannot drift), and
    /// advances the tick.
    ///
    /// The coordinator's flow ends this call in exactly the same state the
    /// executor's transient flow was in after its `step_datafusion`.
    pub fn apply_computed_tick(
        &self,
        local_pending: HashMap<String, Vec<DeltaBatch>>,
        view_full_outputs: HashMap<String, RecordBatch>,
    ) -> IvmResult<StepSummary> {
        let inputs = coalesce_pending(local_pending)?;
        let mut inner = self.inner.lock().map_err(lock_err)?;

        // Advance source snapshots deterministically (mirrors step_datafusion).
        for (name, delta) in &inputs {
            let current = inner.source_snapshots.remove(name);
            let updated = apply_delta(current, delta).map_err(delta_err)?;
            inner.source_snapshots.insert(name.clone(), updated);
        }

        inner.tick += 1;
        let mut total_output_rows = 0usize;
        let mut active_views = 0usize;
        for (name, full) in view_full_outputs {
            if let Ok(view) = inner.view_registry.get(&name) {
                let delta = view.replace_full(full).map_err(delta_err)?;
                if !delta.is_empty() {
                    total_output_rows += delta.num_rows();
                    active_views += 1;
                }
            }
        }
        Ok(StepSummary {
            total_output_rows,
            active_views,
            degraded_views: Vec::new(),
            errored_views: Vec::new(),
        })
    }

    /// Serialize source snapshots **and** view state (snapshot + full-output
    /// baseline) to a self-contained byte blob.
    ///
    /// This is the state-transfer payload for coordinator-authoritative
    /// executor offload: a remote executor restores it into a transient flow,
    /// feeds the tick's deltas, runs one `step_datafusion`, and returns the
    /// resulting full view outputs. Capturing view baselines is what makes the
    /// remote diff correct (the source-only [`checkpoint`] does not).
    ///
    /// Format: `u32 num_sources || (source entries) || u32 num_views || (view entries)`
    /// where each entry is `u32 name_len || name || u32 ipc_len || arrow_ipc`.
    pub fn checkpoint_full(&self) -> IvmResult<Vec<u8>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        let mut out: Vec<u8> = Vec::new();
        let sources: Vec<(&String, &RecordBatch)> = inner.source_snapshots.iter().collect();
        out.extend_from_slice(&(sources.len() as u32).to_le_bytes());
        for (name, snap) in sources {
            encode_named_batch(&mut out, name, snap)?;
        }
        let names = inner.view_registry.view_names().map_err(delta_err)?;
        out.extend_from_slice(&(names.len() as u32).to_le_bytes());
        for name in &names {
            let view = inner.view_registry.get(name).map_err(delta_err)?;
            let snap = view.snapshot().map_err(delta_err)?;
            let full = view.full_output_baseline().map_err(delta_err)?;
            // Encode snapshot (or empty) then full-output (or empty) so restore
            // can reconstruct both fields. Empty rows are signalled by a zero
            // IPC length followed by the schema-only batch.
            encode_named_batch_optional(&mut out, name, snap.as_ref(), &view)?;
            encode_named_batch_optional(&mut out, name, full.as_ref(), &view)?;
        }
        Ok(out)
    }

    /// Restore source snapshots and view state from [`checkpoint_full`] bytes.
    pub fn restore_full(&self, bytes: &[u8]) -> IvmResult<()> {
        let mut pos = 0usize;
        let n_sources = read_u32(bytes, &mut pos)? as usize;
        let mut source_snapshots: HashMap<String, RecordBatch> = HashMap::with_capacity(n_sources);
        for _ in 0..n_sources {
            let (name, batch) = decode_named_batch(bytes, &mut pos)?;
            source_snapshots.insert(name, batch);
        }
        let n_views = read_u32(bytes, &mut pos)? as usize;
        // Pairs of (snapshot, full_output) per view name.
        let mut view_state: HashMap<String, (Option<RecordBatch>, Option<RecordBatch>)> =
            HashMap::with_capacity(n_views);
        for _ in 0..n_views {
            let (name, snap) = decode_named_batch_opt(bytes, &mut pos)?;
            let (_name2, full) = decode_named_batch_opt(bytes, &mut pos)?;
            view_state.insert(name, (snap, full));
        }
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.source_snapshots = source_snapshots;
        let names = inner.view_registry.view_names().map_err(delta_err)?;
        for name in &names {
            if let Ok(view) = inner.view_registry.get(name) {
                let (snap, full) = view_state.get(name).cloned().unwrap_or((None, None));
                view.restore_state(snap, full).map_err(delta_err)?;
            }
        }
        Ok(())
    }

    /// `checkpoint_delta` (or since `enable_delta_checkpoints` was called).
    ///
    /// The returned bytes can be applied on top of a full [`checkpoint`] via
    /// [`restore_delta`].  Accumulated deltas are cleared after serialisation.
    ///
    /// Returns empty bytes (`count = 0`) if no new input has arrived.
    pub fn checkpoint_delta(&self) -> IvmResult<Vec<u8>> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        let deltas = std::mem::take(&mut inner.checkpoint_deltas);
        let mut out: Vec<u8> = Vec::new();
        let entries: Vec<(String, Vec<DeltaBatch>)> = deltas.into_iter().collect();
        out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for (name, delta_list) in entries {
            let combined = if delta_list.len() == 1 {
                delta_list
                    .into_iter()
                    .next()
                    .ok_or_else(|| IvmError::execution("empty delta list in checkpoint"))?
            } else {
                DeltaBatch::concat(&delta_list).map_err(delta_err)?
            };
            let ipc = serialize_delta_batch(&combined).map_err(delta_err)?;
            let name_bytes = name.as_bytes();
            out.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(name_bytes);
            out.extend_from_slice(&(ipc.len() as u32).to_le_bytes());
            out.extend_from_slice(&ipc);
        }
        Ok(out)
    }

    /// Apply a delta checkpoint (produced by [`checkpoint_delta`]) to the
    /// current source snapshots without re-executing view SQL.
    ///
    /// Intended for use after a full [`restore`]: apply accumulated delta
    /// slices in order to reach a mid-session consistent state.
    ///
    /// Consolidates each snapshot after applying so stacked restores do not
    /// accumulate paired ±1 rows that never cancel (G2 fix).
    pub fn restore_delta(&self, bytes: &[u8]) -> IvmResult<()> {
        let mut pos = 0usize;
        let n = read_u32(bytes, &mut pos)? as usize;
        let mut inner = self.inner.lock().map_err(lock_err)?;
        for _ in 0..n {
            let name_len = read_u32(bytes, &mut pos)? as usize;
            let name = std::str::from_utf8(bytes.get(pos..pos + name_len).ok_or_else(slice_err)?)
                .map_err(|e| IvmError::execution(e.to_string()))?
                .to_string();
            pos += name_len;
            let data_len = read_u32(bytes, &mut pos)? as usize;
            let data = bytes.get(pos..pos + data_len).ok_or_else(slice_err)?;
            pos += data_len;
            let delta = deserialize_delta_batch(data).map_err(delta_err)?;
            let current = inner.source_snapshots.remove(&name);
            let updated = apply_delta(current, &delta).map_err(delta_err)?;
            // Consolidate: turns the snapshot (all-positive) into a DeltaBatch,
            // consolidates to cancel any residual paired rows, then strips weights.
            let schema = updated.schema();
            let as_delta = DeltaBatch::from_inserts(updated).map_err(delta_err)?;
            let consolidated = consolidate_batch(as_delta, &[], &schema).map_err(delta_err)?;
            let snapshot = consolidated.filter_positive().map_err(delta_err)?;
            inner.source_snapshots.insert(name, snapshot);
        }
        // Reset view baselines so the next step re-diffs from current state.
        let names = inner.view_registry.view_names().map_err(delta_err)?;
        for name in &names {
            if let Ok(view) = inner.view_registry.get(name) {
                let _ = view.reset_full_output();
            }
        }
        Ok(())
    }
}

impl Default for IncrementalFlow {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for IncrementalFlow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tick = self.inner.lock().map(|g| g.tick).unwrap_or(0);
        f.debug_struct("IncrementalFlow")
            .field("tick", &tick)
            .finish_non_exhaustive()
    }
}

// ── Row hashing (content-addressed dedup) ─────────────────────────────────────

/// Hash all data column values for a single row using XxHash64.
///
/// Uses string representations with null-byte separators so different column
/// counts cannot collide.  Retractions (weight < 0) are never hashed —
/// callers must gate on weight before calling.
pub(crate) fn hash_row(batch: &RecordBatch, row: usize) -> u64 {
    let mut combined: Vec<u8> = Vec::with_capacity(64);
    for col in batch.columns() {
        let s = scalar_to_string(col.as_ref(), row);
        combined.extend_from_slice(s.as_bytes());
        combined.push(0u8);
    }
    twox_hash::XxHash64::oneshot(0xcafe_babe_dead_beef_u64, &combined)
}

fn scalar_to_string(arr: &dyn arrow::array::Array, row: usize) -> String {
    use arrow::array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
        LargeStringArray, StringArray, UInt32Array, UInt64Array,
    };
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            a.value(row).to_string()
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            a.value(row).to_string()
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            a.value(row).to_string()
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt32Array>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            a.value(row).to_string()
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            a.value(row).to_string()
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            a.value(row).to_string()
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            a.value(row).to_string()
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            a.value(row).to_string()
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            a.value(row).to_string()
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<BinaryArray>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            let bytes = a.value(row);
            let mut s = String::with_capacity(2 + bytes.len() * 2);
            s.push_str("0x");
            for b in bytes {
                s.push_str(&format!("{b:02x}"));
            }
            s
        };
    }
    if arr.is_null(row) {
        "NULL".into()
    } else {
        format!("<unsupported type: {}>", arr.data_type())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

pub fn coalesce_pending(
    raw: HashMap<String, Vec<DeltaBatch>>,
) -> IvmResult<HashMap<String, DeltaBatch>> {
    raw.into_iter()
        .map(|(name, deltas)| {
            let batch = if deltas.len() == 1 {
                deltas
                    .into_iter()
                    .next()
                    .ok_or_else(|| IvmError::execution("empty delta list"))?
            } else {
                DeltaBatch::concat(&deltas).map_err(delta_err)?
            };
            // Gap 8: consolidate (sum weights for identical rows, drop zeros).
            let schema = batch.data_schema().clone();
            let consolidated = consolidate_batch(batch, &[], &schema).map_err(delta_err)?;
            Ok((name, consolidated))
        })
        .collect()
}

async fn execute_view_sql(
    ctx: &SessionContext,
    spec: &IncrementalViewSpec,
) -> IvmResult<RecordBatch> {
    let df = ctx
        .sql(&spec.body_sql)
        .await
        .map_err(|e| IvmError::execution(e.to_string()))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| IvmError::execution(e.to_string()))?;
    let non_empty: Vec<RecordBatch> = batches.into_iter().filter(|b| b.num_rows() > 0).collect();
    if non_empty.is_empty() {
        let empty_cols: Vec<_> = spec
            .output_schema
            .fields()
            .iter()
            .map(|f| arrow::array::new_empty_array(f.data_type()))
            .collect();
        return RecordBatch::try_new(spec.output_schema.clone(), empty_cols)
            .map_err(|e| IvmError::execution(e.to_string()));
    }
    let combined = arrow::compute::concat_batches(
        &non_empty
            .first()
            .ok_or_else(|| IvmError::execution("empty batch list".to_string()))?
            .schema(),
        &non_empty,
    )
    .map_err(|e| IvmError::execution(e.to_string()))?;
    coerce_to_schema(combined, &spec.output_schema)
}

fn coerce_to_schema(
    batch: RecordBatch,
    target: &arrow::datatypes::SchemaRef,
) -> IvmResult<RecordBatch> {
    if batch.schema().as_ref() == target.as_ref() {
        return Ok(batch);
    }
    let cols: Vec<Arc<dyn arrow::array::Array>> = target
        .fields()
        .iter()
        .map(|field| {
            let col_idx = batch.schema().index_of(field.name()).map_err(|_| {
                IvmError::execution(format!(
                    "view output missing column '{}' declared in output_schema",
                    field.name()
                ))
            })?;
            let col = batch.column(col_idx);
            if col.data_type() == field.data_type() {
                Ok(Arc::clone(col))
            } else {
                cast(col.as_ref(), field.data_type())
                    .map_err(|e| IvmError::execution(e.to_string()))
            }
        })
        .collect::<IvmResult<_>>()?;
    RecordBatch::try_new(Arc::clone(target), cols).map_err(|e| IvmError::execution(e.to_string()))
}

/// Compute a topological execution order for `specs`.
///
/// Uses `view_deps` (AST-derived precise deps) when a view is present in it,
/// falling back to the `sql_identifiers` tokenizer only for views whose deps
/// were not yet computed (e.g. complex SQL that `extract_sql_table_refs` could
/// not analyse). Using the tokenizer for all views risks phantom edges when a
/// SQL keyword or string literal matches a view name, which can create false
/// cycles and corrupt the execution order.
fn toposort_views(
    specs: &[IncrementalViewSpec],
    view_deps: &AHashMap<String, HashSet<String>>,
) -> Vec<String> {
    let all_names: HashSet<&str> = specs.iter().map(|s| s.name.as_str()).collect();
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    for spec in specs {
        in_degree.entry(spec.name.clone()).or_insert(0);
        // Use precise AST-derived deps when available; tokenizer as fallback.
        let deps: Box<dyn Iterator<Item = String>> =
            if let Some(dep_set) = view_deps.get(&spec.name) {
                Box::new(dep_set.iter().cloned())
            } else {
                Box::new(sql_identifiers(&spec.body_sql).into_iter())
            };
        for token in deps {
            if all_names.contains(token.as_str()) && token != spec.name {
                dependents
                    .entry(token.clone())
                    .or_default()
                    .push(spec.name.clone());
                *in_degree.entry(spec.name.clone()).or_insert(0) += 1;
            }
        }
    }
    let mut queue: VecDeque<String> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(name, _)| name.clone())
        .collect();
    let mut order: Vec<String> = Vec::new();
    while let Some(name) = queue.pop_front() {
        if let Some(deps) = dependents.get(&name) {
            for dep in deps.clone() {
                let deg = in_degree.entry(dep.clone()).or_insert(1);
                *deg = deg.saturating_sub(1);
                if *deg == 0 {
                    queue.push_back(dep);
                }
            }
        }
        order.push(name);
    }
    let in_order: HashSet<&str> = order.iter().map(|s| s.as_str()).collect();
    let remaining: Vec<String> = specs
        .iter()
        .filter(|s| !in_order.contains(s.name.as_str()))
        .map(|s| s.name.clone())
        .collect();
    order.extend(remaining);
    order
}

/// Extract the set of table/view names referenced in FROM and JOIN clauses.
///
/// Returns `None` when the SQL can't be parsed or contains patterns we can't
/// safely analyse (subqueries, derived tables). `None` tells the caller to fall
/// back to the conservative `sql_identifiers` tokenizer so those views are
/// never silently skipped.
///
/// Returns `Some(refs)` for simple `FROM t1 JOIN t2 ON ...` shapes, which
/// covers the vast majority of IVM view SQL. Using the AST avoids the
/// false-positive dirty marks produced by `sql_identifiers` when source names
/// coincide with SQL keywords or aggregate function names (COUNT, SUM, …).
fn extract_sql_table_refs(sql: &str) -> Option<HashSet<String>> {
    use sqlparser::ast::{SetExpr, Statement, TableFactor};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    let stmt = stmts.into_iter().next()?;
    let Statement::Query(q) = stmt else {
        return None;
    };
    let SetExpr::Select(select) = q.body.as_ref() else {
        // UNION/INTERSECT/EXCEPT or other set operations — fall back to tokenizer.
        return None;
    };

    let mut refs = HashSet::new();
    for twj in &select.from {
        match &twj.relation {
            TableFactor::Table { name, .. } => {
                if let Some(ident) = name.0.last().and_then(|part| part.as_ident()) {
                    refs.insert(ident.value.to_lowercase());
                }
            }
            // Subquery or table function in FROM — can't safely enumerate deps.
            _ => return None,
        }
        for join in &twj.joins {
            match &join.relation {
                TableFactor::Table { name, .. } => {
                    if let Some(ident) = name.0.last().and_then(|part| part.as_ident()) {
                        refs.insert(ident.value.to_lowercase());
                    }
                }
                _ => return None,
            }
        }
    }
    Some(refs)
}

fn sql_identifiers(sql: &str) -> Vec<String> {
    sql.split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> IvmResult<u32> {
    let slice = bytes.get(*pos..*pos + 4).ok_or_else(slice_err)?;
    *pos += 4;
    let mut arr = [0u8; 4];
    arr.copy_from_slice(slice);
    Ok(u32::from_le_bytes(arr))
}

fn slice_err() -> IvmError {
    IvmError::execution("checkpoint bytes truncated")
}

fn delta_err(e: DeltaError) -> IvmError {
    IvmError::execution(e.to_string())
}

fn lock_err<T>(_: T) -> IvmError {
    IvmError::execution("incremental flow lock poisoned")
}

// ── RecordBatch framing helpers (for checkpoint_full / restore_full) ──────────

/// Encode `name` + a required `RecordBatch` as Arrow IPC into `out`.
fn encode_named_batch(out: &mut Vec<u8>, name: &str, batch: &RecordBatch) -> IvmResult<()> {
    out.extend_from_slice(&(name.len() as u32).to_le_bytes());
    out.extend_from_slice(name.as_bytes());
    let ipc = encode_record_batch_ipc(batch)?;
    out.extend_from_slice(&(ipc.len() as u32).to_le_bytes());
    out.extend_from_slice(&ipc);
    Ok(())
}

/// Encode `name` + an optional `RecordBatch`. `None` or a zero-row batch over
/// the view's output schema still round-trips; absence is encoded as a schema-
/// only IPC stream (zero data rows) so the schema is never lost.
fn encode_named_batch_optional(
    out: &mut Vec<u8>,
    name: &str,
    batch: Option<&RecordBatch>,
    view: &krishiv_delta::IncrementalView,
) -> IvmResult<()> {
    let to_encode = match batch {
        Some(b) if b.num_rows() > 0 => b.clone(),
        _ => empty_batch_for_view(view)?,
    };
    encode_named_batch(out, name, &to_encode)
}

fn decode_named_batch(bytes: &[u8], pos: &mut usize) -> IvmResult<(String, RecordBatch)> {
    let name = decode_name(bytes, pos)?;
    let batch = decode_one_ipc(bytes, pos)?;
    Ok((name, batch))
}

fn decode_named_batch_opt(
    bytes: &[u8],
    pos: &mut usize,
) -> IvmResult<(String, Option<RecordBatch>)> {
    let name = decode_name(bytes, pos)?;
    let batch = decode_one_ipc(bytes, pos)?;
    // A schema-only / zero-row batch encodes "no prior state".
    let opt = if batch.num_rows() == 0 {
        None
    } else {
        Some(batch)
    };
    Ok((name, opt))
}

fn decode_name(bytes: &[u8], pos: &mut usize) -> IvmResult<String> {
    let name_len = read_u32(bytes, pos)? as usize;
    let name = std::str::from_utf8(bytes.get(*pos..*pos + name_len).ok_or_else(slice_err)?)
        .map_err(|e| IvmError::execution(e.to_string()))?
        .to_string();
    *pos += name_len;
    Ok(name)
}

fn decode_one_ipc(bytes: &[u8], pos: &mut usize) -> IvmResult<RecordBatch> {
    let ipc_len = read_u32(bytes, pos)? as usize;
    let ipc = bytes.get(*pos..*pos + ipc_len).ok_or_else(slice_err)?;
    *pos += ipc_len;
    decode_record_batch_ipc(ipc)
}

fn encode_record_batch_ipc(batch: &RecordBatch) -> IvmResult<Vec<u8>> {
    use arrow::ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| IvmError::execution(e.to_string()))?;
        writer
            .write(batch)
            .map_err(|e| IvmError::execution(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| IvmError::execution(e.to_string()))?;
    }
    Ok(buf)
}

fn decode_record_batch_ipc(bytes: &[u8]) -> IvmResult<RecordBatch> {
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| IvmError::execution(e.to_string()))?;
    reader
        .next()
        .ok_or_else(|| IvmError::execution("empty IPC stream in checkpoint_full"))?
        .map_err(|e| IvmError::execution(e.to_string()))
}

fn empty_batch_for_view(view: &krishiv_delta::IncrementalView) -> IvmResult<RecordBatch> {
    let schema = view.spec.output_schema.clone();
    let cols: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| arrow::array::new_empty_array(f.data_type()))
        .collect();
    RecordBatch::try_new(schema, cols).map_err(|e| IvmError::execution(e.to_string()))
}

// ── Batch-map framing (executor → coordinator result return) ──────────────────

/// Encode a `name → RecordBatch` map as a length-framed binary blob.
///
/// Used to return per-view full outputs from a stateless executor tick back to
/// the authoritative coordinator. Format:
/// `u32 count || (u32 name_len || name || u32 ipc_len || arrow_ipc)*`
pub fn encode_batch_map(map: &HashMap<String, RecordBatch>) -> IvmResult<Vec<u8>> {
    let mut out: Vec<u8> = Vec::new();
    out.extend_from_slice(&(map.len() as u32).to_le_bytes());
    for (name, batch) in map {
        encode_named_batch(&mut out, name, batch)?;
    }
    Ok(out)
}

/// Decode a blob produced by [`encode_batch_map`] back into a map.
pub fn decode_batch_map(bytes: &[u8]) -> IvmResult<HashMap<String, RecordBatch>> {
    let mut pos = 0usize;
    let n = read_u32(bytes, &mut pos)? as usize;
    let mut map = HashMap::with_capacity(n);
    for _ in 0..n {
        let (name, batch) = decode_named_batch(bytes, &mut pos)?;
        map.insert(name, batch);
    }
    Ok(map)
}

// ── Fragment encoding helpers (coordinator-authoritative executor dispatch) ───

/// Encode a coordinator-authoritative IVM dispatch fragment.
///
/// Format: `delta:step:{job_id}|{deltas_b64}|{specs_b64}|{state_b64}`
///
/// Each `|`-separated payload part is **base64-encoded**, so a `|` inside a
/// SQL string literal in `body_sql` cannot corrupt the framing. `state_b64`
/// is the base64 of [`IncrementalFlow::checkpoint_full`]; the executor restores
/// it into a transient flow so the remote tick sees correct source snapshots
/// and view baselines.
pub fn encode_ivm_step_fragment(
    job_id: &str,
    pending: &HashMap<String, DeltaBatch>,
    specs: &[IncrementalViewSpec],
    state_bytes: &[u8],
) -> IvmResult<String> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;

    let delta_entries: Vec<serde_json::Value> = pending
        .iter()
        .map(|(source, delta)| {
            let ipc = serialize_delta_batch(delta).map_err(delta_err)?;
            let enc = b64.encode(&ipc);
            Ok(serde_json::json!({ "source": source, "delta_b64": enc }))
        })
        .collect::<IvmResult<_>>()?;
    let deltas_json =
        serde_json::to_string(&delta_entries).map_err(|e| IvmError::execution(e.to_string()))?;
    let deltas_b64 = b64.encode(deltas_json);

    let spec_entries: Vec<serde_json::Value> = specs
        .iter()
        .map(|s| {
            let fields: Vec<serde_json::Value> = s
                .output_schema
                .fields()
                .iter()
                .map(|f| {
                    serde_json::json!({
                        "name": f.name(),
                        "data_type": format!("{:?}", f.data_type()),
                        "nullable": f.is_nullable()
                    })
                })
                .collect();
            serde_json::json!({
                "name": s.name,
                "body_sql": s.body_sql,
                "output_schema_fields": fields,
                "is_materialized": s.is_materialized,
                "is_recursive": s.is_recursive
            })
        })
        .collect();
    let specs_json =
        serde_json::to_string(&spec_entries).map_err(|e| IvmError::execution(e.to_string()))?;
    let specs_b64 = b64.encode(specs_json);

    let state_b64 = b64.encode(state_bytes);

    Ok(format!(
        "delta:step:{job_id}|{deltas_b64}|{specs_b64}|{state_b64}"
    ))
}

// ── Integration tests (3d) ────────────────────────────────────────────────────

#[cfg(test)]
mod integration_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_delta::{DeltaBatch, deserialize_delta_batch, serialize_delta_batch};

    use super::IncrementalFlow;

    fn make_batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    // ── G2: restore_delta idempotency ─────────────────────────────────────────

    #[test]
    fn restore_delta_twice_does_not_bloat_snapshot() {
        let flow = IncrementalFlow::new();
        flow.enable_delta_checkpoints().unwrap();

        // Feed 3 rows.
        let batch = DeltaBatch::from_inserts(make_batch(&[1, 2, 3])).unwrap();
        flow.feed("src", batch).unwrap();
        flow.step().unwrap();

        // Checkpoint: full baseline.
        let full_ck = flow.checkpoint().unwrap();
        let delta_ck = flow.checkpoint_delta().unwrap();

        // Restore full, then apply delta TWICE (simulates re-delivery).
        flow.restore(&full_ck).unwrap();
        flow.restore_delta(&delta_ck).unwrap();
        flow.restore_delta(&delta_ck).unwrap(); // second application

        // Snapshot should still have exactly 3 rows (duplicates cancelled).
        let snap = flow.source_snapshot("src").unwrap().unwrap();
        assert_eq!(
            snap.num_rows(),
            3,
            "stacked restore must not duplicate rows"
        );
    }

    // ── feed() with DeltaBatch::from_inserts (was feed_source_from_record_batch) ──

    #[tokio::test]
    async fn feed_from_inserts_creates_insertions() {
        let flow = IncrementalFlow::new();
        let delta = DeltaBatch::from_inserts(make_batch(&[10, 20])).unwrap();
        flow.feed("s", delta).unwrap();
        // step_datafusion updates source_snapshots; step() alone does not.
        flow.step_datafusion().await.unwrap();
        let snap = flow.source_snapshot("s").unwrap().unwrap();
        assert_eq!(snap.num_rows(), 2);
    }

    // ── feed() with a pre-computed delta (was feed_stream_delta) ──────────────

    #[tokio::test]
    async fn feed_precomputed_delta_applies_directly() {
        let flow = IncrementalFlow::new();
        let insert_delta = DeltaBatch::from_inserts(make_batch(&[1, 2])).unwrap();
        flow.feed("src", insert_delta).unwrap();
        flow.step_datafusion().await.unwrap();
        let snap = flow.source_snapshot("src").unwrap().unwrap();
        assert_eq!(snap.num_rows(), 2);

        // Feed a retraction.
        let retract_delta = DeltaBatch::from_deletes(make_batch(&[1])).unwrap();
        flow.feed("src", retract_delta).unwrap();
        flow.step_datafusion().await.unwrap();
        let snap2 = flow.source_snapshot("src").unwrap().unwrap();
        assert_eq!(snap2.num_rows(), 1, "retraction must remove row 1");
    }

    // ── feed() with DeltaBatch::from_cdc (was feed_cdc_source) ────────────────

    #[tokio::test]
    async fn feed_from_cdc_update_retracts_and_inserts() {
        let flow = IncrementalFlow::new();
        // Seed a row, then CDC-update it.
        flow.feed("src", DeltaBatch::from_inserts(make_batch(&[1])).unwrap())
            .unwrap();
        flow.step_datafusion().await.unwrap();

        let update = DeltaBatch::from_cdc(Some(make_batch(&[1])), Some(make_batch(&[2])))
            .unwrap()
            .expect("update produces a delta");
        flow.feed("src", update).unwrap();
        flow.step_datafusion().await.unwrap();

        let snap = flow.source_snapshot("src").unwrap().unwrap();
        assert_eq!(snap.num_rows(), 1, "update replaces row 1 with row 2");
    }

    // ── materialized view snapshot ────────────────────────────────────────────

    #[tokio::test]
    async fn materialized_view_snapshot_sum_no_group_by() {
        use arrow::array::Float64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use krishiv_delta::DeltaBatch;

        let flow = IncrementalFlow::new();

        // Register materialized view: SUM with no GROUP BY.
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "total",
            DataType::Float64,
            true,
        )]));
        flow.register_view(krishiv_delta::IncrementalViewSpec {
            name: "total_sales".into(),
            body_sql: "SELECT SUM(amount) AS total FROM sales".into(),
            output_schema,
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        })
        .unwrap();

        // Feed three rows: amount=[100, 200, 50].
        let sales_schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Float64,
            false,
        )]));
        let sales_batch = RecordBatch::try_new(
            sales_schema,
            vec![Arc::new(Float64Array::from(vec![100.0_f64, 200.0, 50.0]))],
        )
        .unwrap();
        flow.feed("sales", DeltaBatch::from_inserts(sales_batch).unwrap())
            .unwrap();

        let summary = flow.step_datafusion().await.unwrap();
        assert_eq!(summary.active_views, 1, "view should be active");
        assert_eq!(summary.total_output_rows, 1, "one aggregate row expected");

        // Snapshot should be Some after step with is_materialized=true.
        let snap = flow
            .snapshot("total_sales")
            .expect("snapshot call failed")
            .expect("snapshot is None — materialized view must have a snapshot");
        assert_eq!(snap.num_rows(), 1, "snapshot should have 1 row");
        let totals = snap
            .column_by_name("total")
            .expect("missing 'total' column")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("total is not Float64");
        assert!(
            (totals.value(0) - 350.0).abs() < 1e-9,
            "expected total=350.0, got {}",
            totals.value(0)
        );
    }

    // ── 3c: serialization versioning ──────────────────────────────────────────

    #[test]
    fn serialization_version_magic_prefix_roundtrip() {
        let delta = DeltaBatch::from_inserts(make_batch(&[42])).unwrap();
        let bytes = serialize_delta_batch(&delta).unwrap();
        assert!(bytes.starts_with(b"DLT1"), "must have DLT1 magic prefix");
        let restored = deserialize_delta_batch(&bytes).unwrap();
        assert_eq!(restored.num_rows(), 1);
        assert_eq!(restored.weights().value(0), 1);
    }

    // ── coordinator-authoritative distributed IVM ──────────────────────────────

    use arrow::array::Float64Array;

    fn sales_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Float64,
            false,
        )]))
    }

    fn sales_batch(amounts: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            sales_schema(),
            vec![Arc::new(Float64Array::from(amounts.to_vec()))],
        )
        .unwrap()
    }

    fn sum_view_spec() -> krishiv_delta::IncrementalViewSpec {
        krishiv_delta::IncrementalViewSpec {
            name: "total_sales".into(),
            body_sql: "SELECT SUM(amount) AS total FROM sales".into(),
            output_schema: Arc::new(Schema::new(vec![Field::new(
                "total",
                DataType::Float64,
                true,
            )])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        }
    }

    fn sum_total(flow: &IncrementalFlow) -> f64 {
        let snap = flow
            .snapshot("total_sales")
            .unwrap()
            .expect("materialized snapshot must exist after step");
        snap.column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    }

    fn empty_view_batch(schema: &arrow::datatypes::SchemaRef) -> RecordBatch {
        let cols: Vec<_> = schema
            .fields()
            .iter()
            .map(|f| arrow::array::new_empty_array(f.data_type()))
            .collect();
        RecordBatch::try_new(schema.clone(), cols).unwrap()
    }

    #[tokio::test]
    async fn take_step_output_returns_per_step_delta_then_none() {
        let flow = IncrementalFlow::new();
        flow.register_view(sum_view_spec()).unwrap();

        // Step 1: an insert produces the view's output delta exactly once.
        flow.feed(
            "sales",
            DeltaBatch::from_inserts(sales_batch(&[100.0, 200.0])).unwrap(),
        )
        .unwrap();
        flow.step_datafusion().await.unwrap();
        let d1 = flow.take_step_output("total_sales").unwrap();
        assert!(d1.is_some_and(|d| !d.is_empty()), "step 1 emitted a delta");
        // Drained: a second take without a new step yields None.
        assert!(flow.take_step_output("total_sales").unwrap().is_none());

        // Step 2: no input → no change → None.
        flow.step_datafusion().await.unwrap();
        assert!(flow.take_step_output("total_sales").unwrap().is_none());

        // Step 3: another insert → a fresh delta (SUM update = retract + insert).
        flow.feed(
            "sales",
            DeltaBatch::from_inserts(sales_batch(&[50.0])).unwrap(),
        )
        .unwrap();
        flow.step_datafusion().await.unwrap();
        assert!(flow.take_step_output("total_sales").unwrap().is_some());
    }

    /// `checkpoint_full` → `restore_full` must preserve view baselines so that a
    /// transient (executor) flow computes the same next tick as the source flow.
    #[tokio::test]
    async fn checkpoint_full_restore_full_preserves_view_baseline() {
        let flow = IncrementalFlow::new();
        flow.register_view(sum_view_spec()).unwrap();
        flow.feed(
            "sales",
            DeltaBatch::from_inserts(sales_batch(&[100.0, 200.0, 50.0])).unwrap(),
        )
        .unwrap();
        flow.step_datafusion().await.unwrap();
        assert!((sum_total(&flow) - 350.0).abs() < 1e-9);

        // Capture full state and seed a fresh flow.
        let state = flow.checkpoint_full().unwrap();
        let remote = IncrementalFlow::new();
        remote.register_view(sum_view_spec()).unwrap();
        remote.restore_full(&state).unwrap();
        // Mirror the executor: DiffBased only (no transferable plan accumulators).
        remote.force_diff_based().unwrap();

        // Both see the same next-tick result for the same delta.
        let delta = DeltaBatch::from_inserts(sales_batch(&[25.0, 10.0])).unwrap();
        flow.feed("sales", delta.clone()).unwrap();
        remote.feed("sales", delta).unwrap();
        flow.step_datafusion().await.unwrap();
        remote.step_datafusion().await.unwrap();

        assert!(
            (sum_total(&flow) - 385.0).abs() < 1e-9,
            "central total wrong"
        );
        assert!(
            (sum_total(&remote) - 385.0).abs() < 1e-9,
            "restored-flow total must match central after one tick"
        );
    }

    /// The coordinator-authoritative offload protocol (drain → checkpoint_full →
    /// remote compute → apply_computed_tick) must leave the authoritative flow
    /// identical to a plain central `step_datafusion`. This is the core
    /// correctness guarantee for distributed delta batch: no divergence, no
    /// baseline drift, real `StepSummary`, correct snapshot.
    #[tokio::test]
    async fn apply_computed_tick_matches_central_step() {
        let setup = |flow: &IncrementalFlow| {
            flow.register_view(sum_view_spec()).unwrap();
            flow.feed(
                "sales",
                DeltaBatch::from_inserts(sales_batch(&[100.0, 200.0, 50.0])).unwrap(),
            )
            .unwrap();
        };

        // Baseline tick 1 (identical on both flows).
        let central = IncrementalFlow::new();
        let auth = IncrementalFlow::new();
        setup(&central);
        setup(&auth);
        central.step_datafusion().await.unwrap();
        auth.step_datafusion().await.unwrap();
        assert!((sum_total(&central) - 350.0).abs() < 1e-9);
        assert!((sum_total(&auth) - 350.0).abs() < 1e-9);
        let baseline_tick = auth.tick().unwrap();

        // Tick 2: feed the same delta on both.
        let delta = DeltaBatch::from_inserts(sales_batch(&[25.0, 10.0])).unwrap();
        central.feed("sales", delta.clone()).unwrap();
        auth.feed("sales", delta).unwrap();

        // Central computes tick 2 directly.
        let central_summary = central.step_datafusion().await.unwrap();

        // Authoritative offload: drain pending, snapshot state, run a transient
        // remote tick, then apply the returned outputs.
        let local_pending = auth.take_pending().unwrap();
        let state = auth.checkpoint_full().unwrap();
        let specs = auth.view_specs().unwrap();

        // Simulate the stateless executor: fresh flow, restore, feed, step.
        let remote = IncrementalFlow::new();
        for spec in &specs {
            remote.register_view(spec.clone()).unwrap();
        }
        remote.restore_full(&state).unwrap();
        // Mirror the executor: force DiffBased (no incremental-plan accumulators).
        remote.force_diff_based().unwrap();
        for (src, batches) in &local_pending {
            for b in batches {
                remote.feed(src, b.clone()).unwrap();
            }
        }
        let remote_summary = remote.step_datafusion().await.unwrap();
        let mut view_outputs: HashMap<String, RecordBatch> = HashMap::new();
        for spec in &specs {
            let snap = remote
                .snapshot(&spec.name)
                .unwrap()
                .unwrap_or_else(|| empty_view_batch(&spec.output_schema));
            view_outputs.insert(spec.name.clone(), snap);
        }

        // Apply the remote result to the authoritative flow.
        let applied_summary = auth
            .apply_computed_tick(local_pending, view_outputs)
            .unwrap();

        // The authoritative flow now matches the central flow exactly.
        assert!(
            (sum_total(&auth) - 385.0).abs() < 1e-9,
            "authoritative total {} != 385",
            sum_total(&auth)
        );
        assert!(
            (sum_total(&auth) - sum_total(&central)).abs() < 1e-9,
            "authoritative total must equal central total"
        );
        assert_eq!(
            auth.tick().unwrap(),
            baseline_tick + 1,
            "apply_computed_tick must advance the tick exactly once"
        );
        assert_eq!(
            auth.tick().unwrap(),
            central.tick().unwrap(),
            "tick counts must match"
        );
        // Real summaries (not fabricated zeros): the remote tick produced output.
        assert_eq!(
            remote_summary.total_output_rows, applied_summary.total_output_rows,
            "applied summary must reflect the real remote output row count"
        );
    }

    /// A failed offload that re-feeds pending must leave the flow able to compute
    /// centrally with the same input (no data loss).
    #[tokio::test]
    async fn re_feed_restores_pending_for_central_fallback() {
        let flow = IncrementalFlow::new();
        flow.register_view(sum_view_spec()).unwrap();
        flow.feed(
            "sales",
            DeltaBatch::from_inserts(sales_batch(&[10.0])).unwrap(),
        )
        .unwrap();
        flow.step_datafusion().await.unwrap();

        // Drain, then simulate a failed dispatch by re-feeding.
        let pending = flow.take_pending().unwrap();
        assert!(pending.is_empty(), "nothing pending right after a step");
        flow.feed(
            "sales",
            DeltaBatch::from_inserts(sales_batch(&[5.0])).unwrap(),
        )
        .unwrap();
        let pending = flow.take_pending().unwrap();
        assert_eq!(pending.len(), 1, "one source pending after feed");
        flow.re_feed(pending).unwrap();
        // Central fallback now sees the re-fed pending and computes correctly.
        flow.step_datafusion().await.unwrap();
        assert!(
            (sum_total(&flow) - 15.0).abs() < 1e-9,
            "central fallback after re_feed must total 15"
        );
    }
}
