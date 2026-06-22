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
/// When the cap is reached the oldest entries are evicted to bound memory.
const DEDUP_SEEN_CAPACITY: usize = 10_000_000;

// ── StepSummary ───────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct StepSummary {
    pub total_output_rows: usize,
    pub active_views: usize,
}

// ── IncrementalFlowInner ──────────────────────────────────────────────────────

struct IncrementalFlowInner {
    view_registry: IncrementalViewRegistry,
    pending: HashMap<String, Vec<DeltaBatch>>,
    tick: u64,
    source_snapshots: HashMap<String, RecordBatch>,

    // Content-addressed dedup: opt-in per-source insertion row dedup.
    // Capped at DEDUP_SEEN_CAPACITY entries to prevent unbounded memory growth.
    input_dedup_enabled: bool,
    seen_input_hashes: AHashMap<String, AHashSet<u64>>,

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
        }
        inner.view_registry.register(spec).map_err(delta_err)
    }

    pub fn drop_view(&self, name: &str) -> IvmResult<bool> {
        let inner = self.inner.lock().map_err(lock_err)?;
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
            let seen = inner
                .seen_input_hashes
                .entry(source_name.clone())
                .or_default();
            // Evict when capacity is exceeded to prevent unbounded growth.
            // This may allow a small number of duplicate rows through on the
            // next tick, which is acceptable for at-least-once semantics.
            if seen.len() >= DEDUP_SEEN_CAPACITY {
                seen.clear();
            }
            let data = batch.data_batch();
            let weights = batch.weights();
            let mask: arrow::array::BooleanArray = (0..data.num_rows())
                .map(|row| {
                    if weights.value(row) > 0 {
                        let h = hash_row(&data, row);
                        if seen.contains(&h) {
                            Some(false) // already seen
                        } else {
                            seen.insert(h);
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
        let schema = non_empty[0].schema();
        let new_snapshot = if non_empty.len() == 1 {
            (*non_empty[0]).clone()
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
        ) = {
            let mut inner = self.inner.lock().map_err(lock_err)?;
            let raw = std::mem::take(&mut inner.pending);
            let snapshots = inner.source_snapshots.clone();
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
            (raw, snapshots, specs, prev_outputs, plan_kinds, needs_plans)
        };

        // ── Phase 2 (no lock): coalesce deltas ───────────────────────────────
        let inputs = coalesce_pending(raw_pending)?;

        if inputs.is_empty() {
            let mut inner = self.inner.lock().map_err(lock_err)?;
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
        let topo = toposort_views(&view_specs);
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
        let mut dirty_views: HashSet<String> = HashSet::new();
        // Newly built plans to insert in Phase 5: (name, plan, body_sql)
        let mut new_plans: Vec<(String, ViewPlan, String)> = Vec::new();

        for view_name in &topo {
            let spec = match spec_map.get(view_name.as_str()) {
                Some(s) => s,
                None => continue,
            };

            let view_name_lower = view_name.to_lowercase();
            let is_dirty = sql_identifiers(&spec.body_sql).iter().any(|token| {
                dirty_sources.contains(token.as_str()) || dirty_views.contains(token.as_str())
            });
            if !is_dirty {
                continue;
            }
            dirty_views.insert(view_name_lower);

            // Determine if this view gets an incremental plan (skip SQL) or DiffBased (run SQL).
            let plan_is_incremental = if views_needing_plans.contains(view_name) {
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
                let new_full = match execute_view_sql(ctx, spec).await {
                    Ok(rb) => rb,
                    Err(_) => {
                        let empty_cols: Vec<_> = spec
                            .output_schema
                            .fields()
                            .iter()
                            .map(|f| arrow::array::new_empty_array(f.data_type()))
                            .collect();
                        RecordBatch::try_new(spec.output_schema.clone(), empty_cols)
                            .map_err(|e| IvmError::execution(e.to_string()))?
                    }
                };
                view_full_outputs.insert(view_name.clone(), new_full);
            }
        }

        // ── Phase 5+6 (lock): apply plans / diff, publish, update state ───────
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.source_snapshots = new_snapshots;
        inner.tick += 1;
        let mut total_output_rows = 0usize;
        let mut active_views = 0usize;

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
            // Read plan kind (releases borrow immediately via .map).
            let plan_kind = inner
                .view_plans
                .get(view_name)
                .map(|p| p.kind())
                .unwrap_or(ViewPlanKind::DiffBased);

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
                            Err(_) => continue,
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
                            Err(_) => continue,
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
                            Err(_) => continue,
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
                    Err(_) => continue,
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
            let _ = view.publish_output(output_delta);
        }

        // Gap 6: GC join traces for sources with watermark trackers.
        let watermarks: AHashMap<String, i64> = inner
            .watermark_trackers
            .iter()
            .map(|(k, v)| (k.clone(), v.watermark()))
            .collect();
        if !watermarks.is_empty() {
            for plan in inner.view_plans.values_mut() {
                let _ = plan.gc_watermark(watermarks.values().copied().fold(i64::MAX, i64::min));
            }
        }

        Ok(StepSummary {
            total_output_rows,
            active_views,
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

    /// Serialize only the `DeltaBatch`es accumulated since the last call to
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
                delta_list.into_iter().next().unwrap()
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
    use arrow::array::{Float32Array, Float64Array, Int32Array, Int64Array, StringArray};
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
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            a.value(row).to_string()
        };
    }
    "NULL".to_string()
}

// ── Helpers ───────────────────────────────────────────────────────────────────

pub fn coalesce_pending(
    raw: HashMap<String, Vec<DeltaBatch>>,
) -> IvmResult<HashMap<String, DeltaBatch>> {
    raw.into_iter()
        .map(|(name, deltas)| {
            let batch = if deltas.len() == 1 {
                deltas.into_iter().next().unwrap()
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
    let combined = arrow::compute::concat_batches(&non_empty[0].schema(), &non_empty)
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

fn toposort_views(specs: &[IncrementalViewSpec]) -> Vec<String> {
    let all_names: HashSet<&str> = specs.iter().map(|s| s.name.as_str()).collect();
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    for spec in specs {
        in_degree.entry(spec.name.clone()).or_insert(0);
        for token in sql_identifiers(&spec.body_sql) {
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

fn sql_identifiers(sql: &str) -> Vec<String> {
    sql.split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> IvmResult<u32> {
    let slice = bytes.get(*pos..*pos + 4).ok_or_else(slice_err)?;
    *pos += 4;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
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

// ── Fragment encoding helpers (for Gap-2 executor dispatch) ───────────────────

/// Encode pending deltas and view specs into a `delta:step:` fragment body.
///
/// Format: `delta:step:{job_id}|{pending_deltas_json}|{view_specs_json}`
///
/// The resulting string can be dispatched to an executor that runs
/// `execute_ivm_fragment` to perform the SQL step remotely.
pub fn encode_ivm_step_fragment(
    job_id: &str,
    pending: &HashMap<String, DeltaBatch>,
    specs: &[IncrementalViewSpec],
) -> IvmResult<String> {
    use base64::Engine;

    // Encode deltas as JSON array of {source, delta_b64}.
    let delta_entries: Vec<serde_json::Value> = pending
        .iter()
        .map(|(source, delta)| {
            let ipc = serialize_delta_batch(delta).map_err(delta_err)?;
            let b64 = base64::engine::general_purpose::STANDARD.encode(&ipc);
            Ok(serde_json::json!({ "source": source, "delta_b64": b64 }))
        })
        .collect::<IvmResult<_>>()?;
    let deltas_json =
        serde_json::to_string(&delta_entries).map_err(|e| IvmError::execution(e.to_string()))?;

    // Encode view specs as JSON array.
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

    Ok(format!("delta:step:{job_id}|{deltas_json}|{specs_json}"))
}

// ── Integration tests (3d) ────────────────────────────────────────────────────

#[cfg(test)]
mod integration_tests {
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
}
