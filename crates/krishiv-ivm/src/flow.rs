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

use krishiv_delta::operators::key_util::scalar_to_string;
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
    /// Logical rows inserted this tick across all views (sum of positive
    /// delta weights). `total_output_rows` counts physical delta rows;
    /// these two count the multiset changes (#94 freshness rates).
    pub total_inserted_rows: u64,
    /// Logical rows retracted this tick across all views (sum of negative
    /// delta weight magnitudes).
    pub total_retracted_rows: u64,
    pub active_views: usize,
    /// View names that emitted a non-Apply output (degraded to DiffBased) during
    /// this step. Useful for surfacing join-type degradations to operators.
    pub degraded_views: Vec<String>,
    /// View names whose incremental operator or SQL execution returned an
    /// error and were silently skipped. The error message is the same string
    /// the operator logged. Step did not panic; subsequent ticks re-evaluate.
    pub errored_views: Vec<ViewError>,
}

/// Cumulative insert/retract counters for one view (#94).
///
/// Counts are logical multiset changes: a delta row with weight `+3` counts
/// as 3 inserts, `-2` as 2 retracts. Monotonic for the life of the flow
/// (reset only when the process restarts), so a poller can diff two reads
/// to derive a rate.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ViewDeltaStats {
    /// Total logical rows inserted since registration.
    pub rows_inserted_total: u64,
    /// Total logical rows retracted since registration.
    pub rows_retracted_total: u64,
    /// Inserts in the most recent tick that produced output for this view.
    pub last_tick_inserts: u64,
    /// Retracts in the most recent tick that produced output for this view.
    pub last_tick_retracts: u64,
}

/// Count logical inserts/retracts in a delta (sum of positive weights,
/// sum of negative weight magnitudes).
fn delta_insert_retract_counts(delta: &DeltaBatch) -> (u64, u64) {
    let mut inserts = 0u64;
    let mut retracts = 0u64;
    for weight in delta.weights().iter().flatten() {
        if weight > 0 {
            inserts += weight as u64;
        } else {
            retracts += weight.unsigned_abs();
        }
    }
    (inserts, retracts)
}

/// AUD-8 (retention): the maximum epoch-millisecond value in a timestamp column,
/// or `None` if the column is empty, all-null, or not a supported timestamp type.
///
/// The LATENESS contract is an `Int64` epoch-ms column or a millisecond
/// `Timestamp` (the engine's canonical `event_time`; see the kafka-bridge
/// protocol). Other timestamp units are not observed here — advancing a
/// millisecond watermark from a nanosecond column would misplace it by 10^6 —
/// so they are ignored rather than mis-scaled.
fn max_epoch_ms(arr: &dyn arrow::array::Array) -> Option<i64> {
    use arrow::array::{Int64Array, TimestampMillisecondArray};
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return arrow::compute::max(a);
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return arrow::compute::max(a);
    }
    None
}

/// AUD-8 (retention): advance a source's LATENESS watermark from a fed batch.
///
/// No-op unless the source has a registered [`WatermarkTracker`]. Retraction
/// rows carry event times that were necessarily observed on their earlier
/// insertion, so taking the column max over all rows never moves the watermark
/// backward — the tracker itself is monotonic (`observe` only raises it).
fn observe_source_watermark(
    inner: &mut IncrementalFlowInner,
    source_name: &str,
    batch: &DeltaBatch,
) {
    let Some(column) = inner
        .watermark_trackers
        .get(source_name)
        .map(|t| t.lateness_column().to_string())
    else {
        return;
    };
    let data = batch.data_batch();
    let Ok(idx) = data.schema().index_of(&column) else {
        return;
    };
    if let Some(max_ts) = max_epoch_ms(data.column(idx).as_ref())
        && let Some(tracker) = inner.watermark_trackers.get_mut(source_name)
    {
        tracker.observe(max_ts);
    }
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
    /// Per-view cumulative insert/retract counters (#94); keyed by view name.
    view_delta_stats: AHashMap<String, ViewDeltaStats>,

    // Operator accumulator state captured by `checkpoint_full`, awaiting the
    // (lazy) rebuild of each view's plan. `restore_full` stashes it here; the
    // plan-build step in `step_datafusion` drains the matching entry and applies
    // it to the fresh operator, restoring the incremental view losslessly across
    // a coordinator restart (G6/F4). Views absent here fall back to seeding.
    pending_plan_state: HashMap<String, Vec<u8>>,
}

// ── IncrementalFlow ───────────────────────────────────────────────────────────

/// Driver for an incremental computation pipeline.
///
/// Thread-safe and `Clone`-able: all clones share the same underlying state.
#[derive(Clone)]
pub struct IncrementalFlow {
    inner: Arc<Mutex<IncrementalFlowInner>>,
    /// Spill-capable `SessionContext` reused across `step_datafusion` ticks
    /// (G14): building a fresh context per tick dominated tick latency in the
    /// IVM-vs-recompute benchmark. Guarded by an async mutex so cached-path
    /// ticks serialize; `step_datafusion_with_ctx` callers are unaffected.
    tick_ctx: Arc<tokio::sync::Mutex<CachedTickContext>>,
}

/// Cached tick execution context plus the table names currently registered in
/// it. The set lets each tick reconcile the catalog to exactly what a fresh
/// context would contain (dropped sources/views deregistered, empty snapshots
/// absent), so reuse is observationally identical to per-tick construction.
#[derive(Default)]
struct CachedTickContext {
    ctx: Option<SessionContext>,
    registered: AHashSet<String>,
}

/// Per-tick view of the tick `SessionContext`'s table catalog. Registration
/// and removal go through this so the cached-context path can track what is
/// registered; with `tracked: None` (external caller's context) it degrades
/// to the plain register/deregister calls the per-tick path always made.
struct TickTables<'a> {
    ctx: &'a SessionContext,
    tracked: Option<&'a mut AHashSet<String>>,
}

impl TickTables<'_> {
    /// Replace-register: `SessionContext::register_table` errors on a
    /// duplicate name (it does not overwrite), so deregister first. Besides
    /// enabling cross-tick context reuse, this fixes a latent same-tick bug:
    /// a downstream DiffBased view re-registering an upstream view's fresh
    /// output hit the duplicate error (swallowed by `let _ =`) and kept
    /// reading the upstream's previous-tick snapshot.
    fn register(&mut self, name: &str, batch: &RecordBatch) -> datafusion::error::Result<()> {
        let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])?;
        let _ = self.ctx.deregister_table(name);
        self.ctx.register_table(name, Arc::new(table))?;
        if let Some(reg) = self.tracked.as_deref_mut() {
            reg.insert(name.to_owned());
        }
        Ok(())
    }

    fn remove(&mut self, name: &str) {
        let _ = self.ctx.deregister_table(name);
        if let Some(reg) = self.tracked.as_deref_mut() {
            reg.remove(name);
        }
    }

    /// Deregister every tracked table that a fresh context would not contain
    /// this tick (dropped sources, dropped views). No-op for untracked
    /// (fresh) contexts.
    fn reconcile(&mut self, expected: &AHashSet<String>) {
        let Some(reg) = self.tracked.as_deref_mut() else {
            return;
        };
        let stale: Vec<String> = reg
            .iter()
            .filter(|n| !expected.contains(*n))
            .cloned()
            .collect();
        for name in stale {
            let _ = self.ctx.deregister_table(name.as_str());
            reg.remove(&name);
        }
    }
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
                view_delta_stats: AHashMap::new(),
                pending_plan_state: HashMap::new(),
            })),
            tick_ctx: Arc::new(tokio::sync::Mutex::new(CachedTickContext::default())),
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

    /// Returns `true` when [`force_diff_based`] has been set; otherwise `false`.
    /// Useful for tests and operator tooling to assert the flag took effect.
    pub fn is_force_diff_based(&self) -> IvmResult<bool> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner.force_diff_based)
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

        // AUD-8 (retention): activate LATENESS. Each declared spec creates a
        // watermark tracker so the tick loop's per-plan `gc_watermark` actually
        // advances and prunes stale join/aggregate trace entries instead of the
        // mechanism sitting inert with zero callers. A bare `LatenessSpec` names
        // only the timestamp column, so it is associated with the view's source
        // — unambiguous for a single-source view. Multi-source association needs
        // an explicit source qualifier (view-spec / SQL surface, Phase 60) and
        // is skipped with a warning rather than guessed.
        if !spec.lateness.is_empty() {
            match inner.view_deps.get(&spec.name) {
                Some(deps) if deps.len() == 1 => {
                    let source = deps.iter().next().cloned().unwrap_or_default();
                    for l in &spec.lateness {
                        inner
                            .watermark_trackers
                            .entry(source.clone())
                            .or_insert_with(|| WatermarkTracker::new(l.clone()));
                    }
                }
                other => {
                    tracing::warn!(
                        view = %spec.name,
                        sources = other.map(|d| d.len()).unwrap_or(0),
                        "LATENESS declared but the source is ambiguous (need exactly \
                         one source dependency); watermark tracker not created"
                    );
                }
            }
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

    /// AUD-9 (loud degradation): classify how a registered view currently
    /// executes — `(incremental, human_reason)` — so a view silently running
    /// full recompute is visible instead of hidden behind a tracing log.
    ///
    /// Returns `None` if the view isn't registered. The O(Δ) plan is built
    /// lazily on the first tick, so before any step the view is reported as
    /// not-yet-planned (`incremental = false`, with an explanatory reason).
    pub fn view_plan_classification(&self, view: &str) -> IvmResult<Option<(bool, String)>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        if inner.view_registry.get(view).is_err() {
            return Ok(None);
        }
        Ok(Some(match inner.view_plans.get(view) {
            None => (
                false,
                "not yet planned — no tick has executed; the O(Δ) plan is built lazily on \
                 the first step, after which this view will report its true strategy"
                    .to_string(),
            ),
            Some(plan) => (
                matches!(plan.kind(), ViewPlanKind::Incremental),
                plan.describe().to_string(),
            ),
        }))
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

        // AUD-8 (retention): advance this source's LATENESS watermark.
        observe_source_watermark(&mut inner, &source_name, &batch);

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

    /// Coalesced feed: replaces any pending delta for `source_name` instead
    /// of accumulating it. Same as CocoIndex's `update()` which collapses
    /// same-subpath ops. Only the latest snapshot matters for file-based or
    /// snapshot sources.
    pub fn feed_coalesced(
        &self,
        source_name: impl Into<String>,
        batch: DeltaBatch,
    ) -> IvmResult<()> {
        let source_name = source_name.into();
        let mut inner = self.inner.lock().map_err(lock_err)?;
        if batch.is_empty() {
            return Ok(());
        }
        // AUD-8 (retention): advance this source's LATENESS watermark.
        observe_source_watermark(&mut inner, &source_name, &batch);
        inner.pending.insert(source_name, vec![batch]);
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
        let mut total_inserted_rows = 0u64;
        let mut total_retracted_rows = 0u64;
        let mut active_views = 0usize;
        for (view_name, delta) in output_deltas {
            if let Ok(view) = inner.view_registry.get(&view_name) {
                if !delta.is_empty() {
                    total_output_rows += delta.num_rows();
                    active_views += 1;
                    let (inserts, retracts) = delta_insert_retract_counts(&delta);
                    total_inserted_rows += inserts;
                    total_retracted_rows += retracts;
                    let stats = inner.view_delta_stats.entry(view_name.clone()).or_default();
                    stats.rows_inserted_total += inserts;
                    stats.rows_retracted_total += retracts;
                    stats.last_tick_inserts = inserts;
                    stats.last_tick_retracts = retracts;
                }
                let _ = view.publish_output(delta);
            }
        }
        Ok(StepSummary {
            total_output_rows,
            total_inserted_rows,
            total_retracted_rows,
            active_views,
            degraded_views: Vec::new(),
            errored_views: Vec::new(),
        })
    }

    /// Advance one tick using DataFusion to execute view SQL.
    ///
    /// Runs on a spill-capable context (memory pool sized from
    /// `KRISHIV_QUERY_MEMORY_LIMIT_BYTES` or the container cgroup limit) so
    /// large recomputes spill to disk instead of exhausting process memory.
    ///
    /// The context is built once and reused across ticks (G14): per-tick
    /// `SessionContext` construction dominated tick latency in the
    /// IVM-vs-recompute benchmark. The catalog is reconciled every tick so
    /// reuse is observationally identical to a fresh context; on a tick
    /// error the cached context is discarded so partial registrations can
    /// never leak into the next tick.
    pub async fn step_datafusion(&self) -> IvmResult<StepSummary> {
        let mut cache = self.tick_ctx.lock().await;
        let ctx = cache
            .ctx
            .get_or_insert_with(crate::spill::spill_session_context)
            .clone();
        let mut registered = std::mem::take(&mut cache.registered);
        let result = self
            .step_datafusion_inner(&ctx, Some(&mut registered))
            .await;
        if result.is_ok() {
            cache.registered = registered;
        } else {
            cache.ctx = None;
            cache.registered = AHashSet::new();
        }
        result
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
    ///
    /// The context is treated as tick-scoped: pass a fresh (or otherwise
    /// tick-exclusive) context. For the cached, reused-context path use
    /// [`Self::step_datafusion`].
    pub async fn step_datafusion_with_ctx(&self, ctx: &SessionContext) -> IvmResult<StepSummary> {
        self.step_datafusion_inner(ctx, None).await
    }

    async fn step_datafusion_inner(
        &self,
        ctx: &SessionContext,
        tracked: Option<&mut AHashSet<String>>,
    ) -> IvmResult<StepSummary> {
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

        // Pre-delta source snapshots, kept only when we may build new
        // incremental operators this tick (first step after a view is
        // registered or after a checkpoint restore, where `view_plans` is
        // empty). A freshly built operator is seeded from these so it holds the
        // restored state before this tick's delta is applied (G6/F4). In steady
        // state `views_needing_plans` is empty, so this clone never happens.
        let pre_delta_snapshots: HashMap<String, RecordBatch> = if views_needing_plans.is_empty() {
            HashMap::new()
        } else {
            current_snapshots.clone()
        };

        let mut new_snapshots = current_snapshots;
        for (name, delta) in &inputs {
            let current = new_snapshots.remove(name);
            let updated = apply_delta(current, delta).map_err(delta_err)?;
            new_snapshots.insert(name.clone(), updated);
        }

        // ── Phase 3 (no lock): register source MemTables ─────────────────────
        let mut tables = TickTables { ctx, tracked };
        {
            // Reconcile a reused catalog to this tick's expected contents
            // first: drop tables for sources/views a fresh context would not
            // contain (dropped sources, dropped views).
            let expected: AHashSet<String> = new_snapshots
                .keys()
                .cloned()
                .chain(view_specs.iter().map(|s| s.name.clone()))
                .collect();
            tables.reconcile(&expected);
        }
        for (name, snapshot) in &new_snapshots {
            if snapshot.num_rows() == 0 {
                // A fresh context would have no table for an empty source.
                tables.remove(name.as_str());
                continue;
            }
            tables
                .register(name.as_str(), snapshot)
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

        // Pre-tick view outputs, frozen for operator seeding: a newly built
        // incremental operator must start from the upstream state *before*
        // this tick's delta, or applying the delta double-counts it
        // (view-on-view regression caught by pipeline_temp_view_intermediate).
        let view_seed_snapshots: HashMap<String, RecordBatch> = view_prev_snapshots.clone();
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
                match view_full_outputs.get(view_name) {
                    Some(prev) if prev.num_rows() > 0 => {
                        let _ = tables.register(view_name.as_str(), prev);
                    }
                    // Empty/missing: a fresh context would have no such table.
                    _ => tables.remove(view_name.as_str()),
                }
            } else {
                // DiffBased: register all upstream outputs, then execute SQL.
                for (up_name, up_batch) in &view_full_outputs {
                    if up_batch.num_rows() == 0 {
                        // Keep parity with a fresh context: no table at all.
                        tables.remove(up_name.as_str());
                        continue;
                    }
                    let _ = tables.register(up_name.as_str(), up_batch);
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
                        if new_full.num_rows() > 0
                            && let Err(e) = tables.register(view_name.as_str(), &new_full)
                        {
                            tracing::warn!(
                                view = %view_name,
                                error = %e,
                                "fixpoint: failed to register updated view; \
                                 next iteration will use stale data"
                            );
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
        let mut total_inserted_rows = 0u64;
        let mut total_retracted_rows = 0u64;
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

        // Insert newly built plans, seeding each operator from the restored
        // pre-tick state of its source(s). Precedence:
        //   1. A checkpoint-restored operator accumulator (lossless, incl.
        //      duplicate-valued sources) stashed in `pending_plan_state` by
        //      `restore_full` — Aggregate/Distinct accumulators and (#160)
        //      join traces.
        //   2. Otherwise seed from the restored source/view snapshots — the
        //      fallback for pre-#160 checkpoints, failed state decodes, and
        //      the no-op normal first-build case (empty source).
        // Without either, the first post-restore delta emits a non-retracting
        // insertion and corrupts the materialized view on the next restore
        // cycle (G6/F4).
        for (name, mut plan, sql) in new_plans {
            let restored = match inner.pending_plan_state.remove(&name) {
                Some(state_bytes) => plan.restore_state_bytes(&state_bytes).unwrap_or_else(|e| {
                    tracing::warn!(
                        view = %name,
                        error = %e,
                        "failed to restore incremental operator state from checkpoint"
                    );
                    false
                }),
                None => false,
            };
            if !restored
                && let Err(e) = plan.seed_from_snapshots(|src| {
                    pre_delta_snapshots
                        .get(src)
                        .cloned()
                        .or_else(|| view_seed_snapshots.get(src).cloned())
                })
            {
                tracing::warn!(
                    view = %name,
                    error = %e,
                    "failed to seed incremental operator from restored state; \
                     view may diverge until re-registered"
                );
            }
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
                    Some(ViewPlan::Aggregate { source, op, filter }) => {
                        let src = source.clone();
                        let delta = match available_deltas.get(&src).cloned() {
                            Some(d) => d,
                            None => continue,
                        };
                        // AUD-1: apply the view's WHERE predicate to the source
                        // delta before aggregation.
                        let delta = if let Some(f) = filter {
                            match f.apply(delta) {
                                Ok(d) => d,
                                Err(e) => {
                                    tracing::warn!(
                                        view = %view_name,
                                        error = %e,
                                        "incremental view filter apply failed; skipping view"
                                    );
                                    errored_views.push(ViewError {
                                        view: view_name.clone(),
                                        kind: ViewErrorKind::OperatorApply,
                                        message: e.to_string(),
                                    });
                                    continue;
                                }
                            }
                        } else {
                            delta
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
                        left_filter,
                        right_filter,
                    }) => {
                        let left = available_deltas.get(left_source.as_str()).cloned();
                        let right = available_deltas.get(right_source.as_str()).cloned();
                        if left.is_none() && right.is_none() {
                            continue;
                        }
                        // AUD-1: apply per-side WHERE predicates before probing.
                        let (left, right) = match (
                            crate::plan::apply_side_filter(left_filter, left),
                            crate::plan::apply_side_filter(right_filter, right),
                        ) {
                            (Ok(l), Ok(r)) => (l, r),
                            (Err(e), _) | (_, Err(e)) => {
                                tracing::warn!(
                                    view = %view_name,
                                    error = %e,
                                    "incremental view join filter apply failed; skipping view"
                                );
                                errored_views.push(ViewError {
                                    view: view_name.clone(),
                                    kind: ViewErrorKind::OperatorApply,
                                    message: e.to_string(),
                                });
                                continue;
                            }
                        };
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
                    Some(ViewPlan::Distinct { source, op, filter }) => {
                        let src = source.clone();
                        let delta = match available_deltas.get(&src).cloned() {
                            Some(d) => d,
                            None => continue,
                        };
                        // AUD-1: filter is None today (filtered DISTINCT falls
                        // back to DiffBased) but apply it for forward-compat.
                        let delta = match crate::plan::apply_side_filter(filter, Some(delta)) {
                            Ok(Some(d)) => d,
                            Ok(None) => continue,
                            Err(e) => {
                                tracing::warn!(
                                    view = %view_name,
                                    error = %e,
                                    "incremental view distinct filter apply failed; skipping view"
                                );
                                errored_views.push(ViewError {
                                    view: view_name.clone(),
                                    kind: ViewErrorKind::OperatorApply,
                                    message: e.to_string(),
                                });
                                continue;
                            }
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
            let (inserts, retracts) = delta_insert_retract_counts(&output_delta);
            total_inserted_rows += inserts;
            total_retracted_rows += retracts;
            {
                let stats = inner.view_delta_stats.entry(view_name.clone()).or_default();
                stats.rows_inserted_total += inserts;
                stats.rows_retracted_total += retracts;
                stats.last_tick_inserts = inserts;
                stats.last_tick_retracts = retracts;
            }

            // Provenance (DiffBased only).
            //
            // IVM-7: This recording maps each input hash to ALL output hashes
            // of the tick (a complete bipartite graph). This means
            // `query_provenance(input)` returns the entire output set for any
            // input, so targeted per-row retraction via provenance is not
            // possible on the DiffBased path. True per-row provenance requires
            // the incremental operators to emit input→output lineage.
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
            for (view_name, plan) in inner.view_plans.iter_mut() {
                // AUD-2: GC failures were silently swallowed. A failing GC
                // means join/aggregate traces keep growing without bound, so
                // surface it (non-fatal for the tick) instead of hiding it.
                if let Err(e) = plan.gc_watermark(&watermarks) {
                    tracing::warn!(
                        view = %view_name,
                        error = %e,
                        "watermark GC failed for view plan"
                    );
                }
            }
        }

        Ok(StepSummary {
            total_output_rows,
            total_inserted_rows,
            total_retracted_rows,
            active_views,
            degraded_views,
            errored_views,
        })
    }

    /// Cumulative insert/retract counters for one view (#94), if it has
    /// produced any output.
    pub fn view_delta_stats(&self, view: &str) -> IvmResult<Option<ViewDeltaStats>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner.view_delta_stats.get(view).copied())
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
        let mut source_snapshots: HashMap<String, RecordBatch> =
            HashMap::with_capacity(bounded_capacity(n, bytes.len()));
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
            // Multiset materialization (#160): keep duplicate-row copies.
            let snapshot = delta.filter_positive_expanded().map_err(delta_err)?;
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
        let mut total_inserted_rows = 0u64;
        let mut total_retracted_rows = 0u64;
        let mut active_views = 0usize;
        for (name, full) in view_full_outputs {
            if let Ok(view) = inner.view_registry.get(&name) {
                let delta = view.replace_full(full).map_err(delta_err)?;
                if !delta.is_empty() {
                    total_output_rows += delta.num_rows();
                    active_views += 1;
                    let (inserts, retracts) = delta_insert_retract_counts(&delta);
                    total_inserted_rows += inserts;
                    total_retracted_rows += retracts;
                    let stats = inner.view_delta_stats.entry(name.clone()).or_default();
                    stats.rows_inserted_total += inserts;
                    stats.rows_retracted_total += retracts;
                    stats.last_tick_inserts = inserts;
                    stats.last_tick_retracts = retracts;
                }
            }
        }
        Ok(StepSummary {
            total_output_rows,
            total_inserted_rows,
            total_retracted_rows,
            active_views,
            degraded_views: Vec::new(),
            errored_views: Vec::new(),
        })
    }

    /// Apply a tick computed on a **resident** executor (AUD-6).
    ///
    /// Unlike [`apply_computed_tick`], the executor returns per-view **output
    /// deltas** (O(Δ)), not full outputs. The coordinator mirrors the tick:
    /// source snapshots advance by the input deltas, each view's snapshot and
    /// diff baseline advance by its output delta, and the tick counter bumps.
    /// After this call the coordinator's materialized state matches the
    /// resident flow's exactly, which is what makes central fallback and
    /// re-attach (from `checkpoint_full` of this mirror) correct.
    pub fn apply_remote_tick(
        &self,
        local_pending: HashMap<String, Vec<DeltaBatch>>,
        view_output_deltas: HashMap<String, DeltaBatch>,
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
        inner.last_step_outputs.clear();
        let mut total_output_rows = 0usize;
        let mut total_inserted_rows = 0u64;
        let mut total_retracted_rows = 0u64;
        let mut active_views = 0usize;
        for (name, delta) in view_output_deltas {
            if delta.is_empty() {
                continue;
            }
            if let Ok(view) = inner.view_registry.get(&name) {
                view.apply_output_delta(&delta).map_err(delta_err)?;
                total_output_rows += delta.num_rows();
                active_views += 1;
                let (inserts, retracts) = delta_insert_retract_counts(&delta);
                total_inserted_rows += inserts;
                total_retracted_rows += retracts;
                let stats = inner.view_delta_stats.entry(name.clone()).or_default();
                stats.rows_inserted_total += inserts;
                stats.rows_retracted_total += retracts;
                stats.last_tick_inserts = inserts;
                stats.last_tick_retracts = retracts;
                inner.last_step_outputs.insert(name, delta);
            }
        }
        Ok(StepSummary {
            total_output_rows,
            total_inserted_rows,
            total_retracted_rows,
            active_views,
            degraded_views: Vec::new(),
            errored_views: Vec::new(),
        })
    }

    /// Drop all cached incremental view plans (and their accumulator state).
    ///
    /// AUD-6: when a job is promoted to a resident executor, the executor's
    /// flow owns the live accumulators. The coordinator's cached plans go
    /// stale from that point; invalidating them forces any later central tick
    /// (fallback) to rebuild plans and seed from the mirrored snapshots
    /// instead of applying deltas to a stale accumulator.
    pub fn invalidate_view_plans(&self) -> IvmResult<()> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.view_plans.clear();
        inner.view_plan_sqls.clear();
        inner.pending_plan_state.clear();
        Ok(())
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
    /// Format: `u32 num_sources || (source entries) || u32 num_views ||
    /// (view entries) || u32 num_plan_states || (plan-state entries)` where each
    /// source/view entry is `u32 name_len || name || u32 ipc_len || arrow_ipc`
    /// and each plan-state entry is `u32 name_len || name || u32 len || bytes`.
    ///
    /// The trailing plan-state section carries incremental operators' accumulator
    /// state (per-group SUM/COUNT/AVG/MIN-MAX, DISTINCT multiplicities). Unlike
    /// the view snapshot/baseline, this state cannot be reconstructed from the
    /// materialized snapshots (the source snapshot is a set, not a multiset), so
    /// persisting it is what lets an incremental view be restored losslessly
    /// after a coordinator restart — including duplicate-valued sources (G6/F4).
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
        // Plan-state section: incremental operator accumulators (Aggregate,
        // Distinct). Views on the DiffBased/Join path contribute nothing.
        let plan_states: Vec<(&String, Vec<u8>)> = inner
            .view_plans
            .iter()
            .filter_map(|(name, plan)| plan.checkpoint_state().map(|b| (name, b)))
            .collect();
        out.extend_from_slice(&(plan_states.len() as u32).to_le_bytes());
        for (name, bytes) in plan_states {
            out.extend_from_slice(&(name.len() as u32).to_le_bytes());
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&bytes);
        }
        Ok(out)
    }

    /// Restore source snapshots and view state from [`checkpoint_full`] bytes.
    pub fn restore_full(&self, bytes: &[u8]) -> IvmResult<()> {
        let mut pos = 0usize;
        let n_sources = read_u32(bytes, &mut pos)? as usize;
        let mut source_snapshots: HashMap<String, RecordBatch> =
            HashMap::with_capacity(bounded_capacity(n_sources, bytes.len()));
        for _ in 0..n_sources {
            let (name, batch) = decode_named_batch(bytes, &mut pos)?;
            source_snapshots.insert(name, batch);
        }
        let n_views = read_u32(bytes, &mut pos)? as usize;
        // Pairs of (snapshot, full_output) per view name.
        let mut view_state: HashMap<String, (Option<RecordBatch>, Option<RecordBatch>)> =
            HashMap::with_capacity(bounded_capacity(n_views, bytes.len()));
        for _ in 0..n_views {
            let (name, snap) = decode_named_batch_opt(bytes, &mut pos)?;
            let (_name2, full) = decode_named_batch_opt(bytes, &mut pos)?;
            view_state.insert(name, (snap, full));
        }
        // Plan-state section (optional for forward-compat with older blobs that
        // predate it): stash operator accumulators for the lazy plan rebuild.
        let mut pending_plan_state: HashMap<String, Vec<u8>> = HashMap::new();
        if pos < bytes.len() {
            let n_states = read_u32(bytes, &mut pos)? as usize;
            for _ in 0..n_states {
                let name_len = read_u32(bytes, &mut pos)? as usize;
                let name =
                    std::str::from_utf8(bytes.get(pos..pos + name_len).ok_or_else(slice_err)?)
                        .map_err(|e| IvmError::execution(e.to_string()))?
                        .to_string();
                pos += name_len;
                let len = read_u32(bytes, &mut pos)? as usize;
                let data = bytes.get(pos..pos + len).ok_or_else(slice_err)?.to_vec();
                pos += len;
                pending_plan_state.insert(name, data);
            }
        }
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.source_snapshots = source_snapshots;
        // Drop any stale cached plans so the next step rebuilds them fresh and
        // applies the restored operator state (below) at build time.
        inner.view_plans.clear();
        inner.view_plan_sqls.clear();
        inner.pending_plan_state = pending_plan_state;
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
            // Deliberately SET-materialized (no #160 multiset expansion):
            // stacked restores are made idempotent by this collapse (G2) —
            // re-applying the same slice dedupes instead of doubling. The
            // trade: duplicate-row sources restored through *delta*
            // checkpoints collapse to one copy (the modern `checkpoint_full`
            // path restores multiplicity losslessly via operator state).
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

/// Capacity hint for a collection whose element count `n` was just read from an
/// untrusted checkpoint blob. Every element consumes at least a 4-byte length
/// prefix, so a blob of `len` bytes can encode at most `len / 4` elements —
/// preallocating beyond that is impossible-to-fill and, on a corrupt/garbage
/// blob, a huge `n` (up to `u32::MAX`) turns `with_capacity(n)` into a
/// multi-gigabyte allocation that aborts the process. Clamp the hint; the
/// per-element reads below still fail cleanly with `slice_err` once the bytes
/// run out.
fn bounded_capacity(n: usize, total_bytes: usize) -> usize {
    n.min(total_bytes / 4)
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
    let mut map = HashMap::with_capacity(bounded_capacity(n, bytes.len()));
    for _ in 0..n {
        let (name, batch) = decode_named_batch(bytes, &mut pos)?;
        map.insert(name, batch);
    }
    Ok(map)
}

// ── Delta-map framing (resident executor → coordinator, AUD-6) ────────────────

/// Magic prefix distinguishing a per-view **output-delta** map from the legacy
/// full-output batch map returned by the stateless `delta:step:` path.
const DELTA_MAP_MAGIC: &[u8; 5] = b"IVMD1";

/// Encode a `view → output DeltaBatch` map as a length-framed binary blob.
///
/// AUD-6: a resident executor tick returns **deltas, not snapshots** — this is
/// the O(Δ) wire format for the `delta:tick:` result. Format:
/// `b"IVMD1" || u32 count || (u32 name_len || name || u32 ipc_len || delta_ipc)*`
pub fn encode_delta_map(map: &HashMap<String, DeltaBatch>) -> IvmResult<Vec<u8>> {
    let mut out: Vec<u8> = Vec::new();
    out.extend_from_slice(DELTA_MAP_MAGIC);
    out.extend_from_slice(&(map.len() as u32).to_le_bytes());
    for (name, delta) in map {
        let ipc = serialize_delta_batch(delta).map_err(delta_err)?;
        out.extend_from_slice(&(name.len() as u32).to_le_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&(ipc.len() as u32).to_le_bytes());
        out.extend_from_slice(&ipc);
    }
    Ok(out)
}

/// Decode a blob produced by [`encode_delta_map`].
pub fn decode_delta_map(bytes: &[u8]) -> IvmResult<HashMap<String, DeltaBatch>> {
    let rest = bytes
        .strip_prefix(DELTA_MAP_MAGIC.as_slice())
        .ok_or_else(|| IvmError::execution("blob is not an IVM delta map (missing magic)"))?;
    let mut pos = 0usize;
    let n = read_u32(rest, &mut pos)? as usize;
    let mut map = HashMap::with_capacity(bounded_capacity(n, rest.len()));
    for _ in 0..n {
        let name = decode_name(rest, &mut pos)?;
        let len = read_u32(rest, &mut pos)? as usize;
        let data = rest.get(pos..pos + len).ok_or_else(slice_err)?;
        pos += len;
        map.insert(name, deserialize_delta_batch(data).map_err(delta_err)?);
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
                "is_recursive": s.is_recursive,
                // AUD-4: carry lateness so an offloaded tick applies the same
                // retention/GC semantics as a central tick of the same job.
                "lateness": s.lateness,
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

// ── Resident-executor fragment encoding (AUD-6) ───────────────────────────────
//
// The resident protocol replaces the per-tick full-state round trip with four
// ops. State ships ONCE at attach; every tick afterwards carries only deltas
// plus a fence:
//
// ```text
// delta:attach:{job}|{specs_b64}|{state_b64}|{fence}   create/replace resident flow
// delta:tick:{job}|{deltas_b64}|{fence}                feed Δ, step, return Δ-map
// delta:ckpt:{job}                                     checkpoint_full of resident flow
// delta:detach:{job}                                   drop resident flow
// ```
//
// The fence is a per-job monotonically increasing tick number. A resident
// executor accepts a tick only when `fence == last_fence + 1`; anything else
// (replay after a retry, a gap after a missed tick, a tick landing on an
// executor that never attached) errors, and the coordinator re-attaches from
// its state mirror. This makes placement drift self-healing without hard
// executor pinning.

fn encode_specs_b64(specs: &[IncrementalViewSpec]) -> IvmResult<String> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
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
                "is_recursive": s.is_recursive,
                "lateness": s.lateness,
            })
        })
        .collect();
    let specs_json =
        serde_json::to_string(&spec_entries).map_err(|e| IvmError::execution(e.to_string()))?;
    Ok(b64.encode(specs_json))
}

fn encode_deltas_b64(pending: &HashMap<String, DeltaBatch>) -> IvmResult<String> {
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
    Ok(b64.encode(deltas_json))
}

/// Encode a `delta:attach:` fragment (ships full state ONCE at promotion).
pub fn encode_ivm_attach_fragment(
    job_id: &str,
    specs: &[IncrementalViewSpec],
    state_bytes: &[u8],
    fence: u64,
) -> IvmResult<String> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let specs_b64 = encode_specs_b64(specs)?;
    let state_b64 = b64.encode(state_bytes);
    Ok(format!(
        "delta:attach:{job_id}|{specs_b64}|{state_b64}|{fence}"
    ))
}

/// Encode a `delta:tick:` fragment (deltas + fence only — no state).
pub fn encode_ivm_tick_fragment(
    job_id: &str,
    pending: &HashMap<String, DeltaBatch>,
    fence: u64,
) -> IvmResult<String> {
    let deltas_b64 = encode_deltas_b64(pending)?;
    Ok(format!("delta:tick:{job_id}|{deltas_b64}|{fence}"))
}

/// Encode a `delta:ckpt:` fragment (resident flow → `checkpoint_full` bytes).
pub fn encode_ivm_ckpt_fragment(job_id: &str) -> String {
    format!("delta:ckpt:{job_id}")
}

/// Encode a `delta:detach:` fragment (drop the resident flow).
pub fn encode_ivm_detach_fragment(job_id: &str) -> String {
    format!("delta:detach:{job_id}")
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

    // ── Robustness: corrupt/garbage checkpoint bytes must error, not OOM ───────

    /// A garbage blob whose leading u32 count is enormous must NOT be trusted as
    /// a `with_capacity` hint — before the `bounded_capacity` clamp, four bytes
    /// of `0xFF` made `restore_full`/`restore`/`restore_delta` try to allocate
    /// ~69 GB and abort the process (SIGABRT). Now every corrupt blob returns a
    /// clean `Err` (truncated bytes) once the per-element reads run past the end.
    #[test]
    fn corrupt_checkpoint_bytes_error_instead_of_aborting() {
        let flow = IncrementalFlow::new();
        // u32::MAX count, then nothing — the classic length-prefix attack.
        let huge_count = [0xFFu8, 0xFF, 0xFF, 0xFF];
        assert!(flow.restore_full(&huge_count).is_err());
        assert!(flow.restore(&huge_count).is_err());
        assert!(flow.restore_delta(&huge_count).is_err());
        // Fully random short blob.
        assert!(flow.restore_full(b"not a checkpoint").is_err());
        // Empty blob (can't even read the first u32).
        assert!(flow.restore_full(&[]).is_err());
    }

    #[test]
    fn corrupt_delta_map_bytes_error_instead_of_aborting() {
        // decode_delta_map is the resident-executor → coordinator wire decoder;
        // a corrupt tick result must not OOM the coordinator.
        let mut blob = b"IVMD1".to_vec();
        blob.extend_from_slice(&[0xFFu8, 0xFF, 0xFF, 0xFF]); // u32::MAX views
        assert!(super::decode_delta_map(&blob).is_err());
        // decode_batch_map (attach state) shares the bug class.
        assert!(super::decode_batch_map(&[0xFFu8, 0xFF, 0xFF, 0xFF]).is_err());
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

    /// AUD-8 (retention): a LATENESS annotation on a single-source view creates
    /// a watermark tracker at registration, and every `feed` advances it from
    /// the batch's timestamp column. Previously the whole mechanism sat inert
    /// (zero callers), so join/aggregate traces grew without bound.
    #[tokio::test]
    async fn lateness_watermark_activates_and_advances() {
        use arrow::array::{Array, Float64Array, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use krishiv_delta::{DeltaBatch, LatenessSpec};

        let flow = IncrementalFlow::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_time", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        // Single-source view → LATENESS binds unambiguously to `events`.
        flow.register_view(krishiv_delta::IncrementalViewSpec {
            name: "recent".into(),
            body_sql: "SELECT event_time, amount FROM events".into(),
            output_schema: schema.clone(),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![LatenessSpec::new("event_time", 1_000)],
        })
        .unwrap();

        // No data yet → watermark unset.
        assert_eq!(flow.watermark_for("events").unwrap(), i64::MIN);

        let batch = |ts: &[i64], amt: &[f64]| {
            DeltaBatch::from_inserts(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(ts.to_vec())) as Arc<dyn Array>,
                        Arc::new(Float64Array::from(amt.to_vec())) as Arc<dyn Array>,
                    ],
                )
                .unwrap(),
            )
            .unwrap()
        };

        // watermark = max_ts(12_000) − lateness(1_000).
        flow.feed("events", batch(&[10_000, 12_000], &[1.0, 2.0]))
            .unwrap();
        assert_eq!(flow.watermark_for("events").unwrap(), 11_000);

        // A later batch advances it.
        flow.feed("events", batch(&[20_000], &[3.0])).unwrap();
        assert_eq!(flow.watermark_for("events").unwrap(), 19_000);

        // An older batch never moves the watermark backward (monotonic).
        flow.feed("events", batch(&[5_000], &[4.0])).unwrap();
        assert_eq!(
            flow.watermark_for("events").unwrap(),
            19_000,
            "watermark must be monotonic"
        );
    }

    /// AUD-9 (loud degradation): `view_plan_classification` reports a view as
    /// unplanned before its first tick, incremental once an O(Δ) plan is cached,
    /// and `None` for an unregistered view — so a silent full-recompute fallback
    /// is visible on the debug surface.
    #[tokio::test]
    async fn view_plan_classification_reports_incremental_after_tick() {
        use arrow::array::Float64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use krishiv_delta::DeltaBatch;

        let flow = IncrementalFlow::new();
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

        // Unregistered view → None.
        assert!(flow.view_plan_classification("nope").unwrap().is_none());

        // Before any tick the plan is lazy → not-yet-planned, not incremental.
        let (incr, reason) = flow
            .view_plan_classification("total_sales")
            .unwrap()
            .unwrap();
        assert!(!incr, "pre-tick view must not claim incremental");
        assert!(
            reason.contains("not yet planned"),
            "pre-tick reason should say so, got: {reason}"
        );

        // Feed + step so the O(Δ) aggregate plan is built and cached.
        let sales_schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Float64,
            false,
        )]));
        let sales_batch = RecordBatch::try_new(
            sales_schema,
            vec![Arc::new(Float64Array::from(vec![100.0_f64, 200.0]))],
        )
        .unwrap();
        flow.feed("sales", DeltaBatch::from_inserts(sales_batch).unwrap())
            .unwrap();
        flow.step_datafusion().await.unwrap();

        let (incr, reason) = flow
            .view_plan_classification("total_sales")
            .unwrap()
            .unwrap();
        assert!(incr, "aggregate view must report incremental after tick");
        assert!(
            reason.contains("incremental aggregate"),
            "reason should describe the incremental strategy, got: {reason}"
        );
    }

    /// Regression (Phase 51): a downstream view with a fresh incremental
    /// operator (COUNT over an upstream view) must seed from the upstream's
    /// **pre-tick** output, not the output already computed this tick —
    /// otherwise the same tick's delta is applied on top of a snapshot that
    /// already contains it and the aggregate double-counts.
    #[tokio::test]
    async fn view_on_view_incremental_agg_does_not_double_count() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use krishiv_delta::DeltaBatch;

        let flow = IncrementalFlow::new();

        let big_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        flow.register_view(krishiv_delta::IncrementalViewSpec {
            name: "big".into(),
            body_sql: "SELECT id, amount FROM raw WHERE amount > 60".into(),
            output_schema: big_schema,
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        })
        .unwrap();
        let count_schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        flow.register_view(krishiv_delta::IncrementalViewSpec {
            name: "count_big".into(),
            body_sql: "SELECT COUNT(*) AS n FROM big".into(),
            output_schema: count_schema,
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        })
        .unwrap();

        let raw_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let raw_batch = RecordBatch::try_new(
            raw_schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])),
                Arc::new(Int64Array::from(vec![100_i64, 50])),
            ],
        )
        .unwrap();
        flow.feed("raw", DeltaBatch::from_inserts(raw_batch).unwrap())
            .unwrap();
        flow.step_datafusion().await.unwrap();

        let snap = flow
            .snapshot("count_big")
            .expect("snapshot call failed")
            .expect("count_big must have a snapshot");
        let n = snap
            .column_by_name("n")
            .expect("missing n")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("n is not Int64")
            .value(0);
        assert_eq!(n, 1, "only the amount=100 row passes the upstream filter");
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

    /// #160: `checkpoint_full` → `restore_full` round-trips **join trace
    /// state** losslessly. The probe: a right-side row with multiplicity 2
    /// (duplicate customer). After restore, retracting ONE copy must leave the
    /// joined row alive (net weight 1). Snapshot seeding — the pre-#160
    /// fallback — replays the materialized snapshot, a set, so the trace would
    /// hold weight 1 and the same retraction would wrongly kill the row.
    #[tokio::test]
    async fn checkpoint_full_restore_full_preserves_join_traces() {
        use arrow::array::{Int32Array, StringArray};

        let orders_schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("customer_id", DataType::Int32, false),
        ]));
        let customers_schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let orders = RecordBatch::try_new(
            orders_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![100])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        )
        .unwrap();
        // Customer 1 twice: weight 2 in the right trace.
        let customers = RecordBatch::try_new(
            customers_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["Alice", "Alice"])),
            ],
        )
        .unwrap();
        let one_customer = RecordBatch::try_new(
            customers_schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["Alice"])),
            ],
        )
        .unwrap();
        let join_spec = || krishiv_delta::IncrementalViewSpec {
            name: "order_names".into(),
            body_sql: "SELECT orders.order_id, orders.customer_id, customers.name \
                       FROM orders JOIN customers \
                       ON orders.customer_id = customers.customer_id"
                .into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("order_id", DataType::Int32, false),
                Field::new("customer_id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        };
        let view_rows = |flow: &IncrementalFlow| -> usize {
            flow.snapshot("order_names")
                .unwrap()
                .map(|rb| rb.num_rows())
                .unwrap_or(0)
        };

        // Original flow: seed both sides, tick (builds the incremental plan).
        let flow = IncrementalFlow::new();
        flow.register_view(join_spec()).unwrap();
        flow.feed("orders", DeltaBatch::from_inserts(orders).unwrap())
            .unwrap();
        flow.feed("customers", DeltaBatch::from_inserts(customers).unwrap())
            .unwrap();
        flow.step_datafusion().await.unwrap();
        // SQL multiset semantics: one order x duplicate customer = 2 rows.
        assert_eq!(view_rows(&flow), 2, "both joined copies materialize");

        // Checkpoint (now carries the join traces) → restore into a new flow.
        let state = flow.checkpoint_full().unwrap();
        let restored = IncrementalFlow::new();
        restored.register_view(join_spec()).unwrap();
        restored.restore_full(&state).unwrap();

        // Retract ONE duplicate on both flows.
        let del = DeltaBatch::from_deletes(one_customer).unwrap();
        flow.feed("customers", del.clone()).unwrap();
        restored.feed("customers", del).unwrap();
        let summary_orig = flow.step_datafusion().await.unwrap();
        let summary_rest = restored.step_datafusion().await.unwrap();
        // Both ran the O(Δ) plan, not DiffBased (the restored flow restored
        // trace state rather than degrading).
        assert!(
            !summary_orig.degraded_views.contains(&"order_names".into()),
            "original must run incrementally"
        );
        assert!(
            !summary_rest.degraded_views.contains(&"order_names".into()),
            "restored flow must run incrementally from restored traces"
        );

        assert_eq!(
            view_rows(&flow),
            1,
            "one customer copy remains; one joined row survives (central)"
        );
        assert_eq!(
            view_rows(&restored),
            1,
            "restored traces must remember multiplicity 2 — retracting one \
             copy may not kill the row"
        );

        // Retract the second copy: now the row must disappear on both.
        let del2 = DeltaBatch::from_deletes(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("customer_id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1])),
                    Arc::new(StringArray::from(vec!["Alice"])),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        flow.feed("customers", del2.clone()).unwrap();
        restored.feed("customers", del2).unwrap();
        flow.step_datafusion().await.unwrap();
        restored.step_datafusion().await.unwrap();
        assert_eq!(view_rows(&flow), 0);
        assert_eq!(view_rows(&restored), 0);
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
        assert_eq!(
            central_summary.total_output_rows, applied_summary.total_output_rows,
            "offloaded tick summary must match the central tick summary"
        );
    }

    /// Phase 57 (AUD-6): `apply_remote_tick` — mirroring a RESIDENT executor
    /// tick from output DELTAS — converges the coordinator to exactly the
    /// central result, including view snapshot, source snapshots, and a later
    /// central fallback tick (which must rebuild plans from the mirror, not a
    /// stale accumulator).
    #[tokio::test]
    async fn apply_remote_tick_mirrors_central_and_supports_fallback() {
        let setup = |flow: &IncrementalFlow| {
            flow.register_view(sum_view_spec()).unwrap();
            flow.feed(
                "sales",
                DeltaBatch::from_inserts(sales_batch(&[100.0, 200.0, 50.0])).unwrap(),
            )
            .unwrap();
        };
        let central = IncrementalFlow::new();
        let auth = IncrementalFlow::new();
        setup(&central);
        setup(&auth);
        central.step_datafusion().await.unwrap();
        auth.step_datafusion().await.unwrap();

        // Promote: the resident flow starts from the coordinator's mirror.
        let resident = IncrementalFlow::new();
        for spec in auth.view_specs().unwrap() {
            resident.register_view(spec).unwrap();
        }
        resident
            .restore_full(&auth.checkpoint_full().unwrap())
            .unwrap();
        auth.invalidate_view_plans().unwrap();

        // Tick 2 via the resident protocol: deltas out, output deltas back.
        let delta = DeltaBatch::from_inserts(sales_batch(&[25.0, 10.0])).unwrap();
        central.feed("sales", delta.clone()).unwrap();
        auth.feed("sales", delta).unwrap();
        central.step_datafusion().await.unwrap();

        let local_pending = auth.take_pending().unwrap();
        for (src, batches) in &local_pending {
            for b in batches {
                resident.feed(src, b.clone()).unwrap();
            }
        }
        resident.step_datafusion().await.unwrap();
        let mut view_deltas: HashMap<String, DeltaBatch> = HashMap::new();
        for name in resident.view_names().unwrap() {
            if let Some(d) = resident.take_step_output(&name).unwrap() {
                view_deltas.insert(name, d);
            }
        }
        assert!(!view_deltas.is_empty(), "resident tick produced deltas");

        // Delta-map framing round-trips.
        let blob = super::encode_delta_map(&view_deltas).unwrap();
        let view_deltas = super::decode_delta_map(&blob).unwrap();

        let summary = auth.apply_remote_tick(local_pending, view_deltas).unwrap();
        assert!(summary.total_output_rows > 0);
        assert!(
            (sum_total(&auth) - 385.0).abs() < 1e-9,
            "mirrored total {} != 385",
            sum_total(&auth)
        );
        assert_eq!(auth.tick().unwrap(), central.tick().unwrap());

        // Central FALLBACK after residency: the mirror must be a valid basis —
        // one more delta computed centrally lands on the same total as central.
        let d3 = DeltaBatch::from_inserts(sales_batch(&[15.0])).unwrap();
        central.feed("sales", d3.clone()).unwrap();
        auth.feed("sales", d3).unwrap();
        central.step_datafusion().await.unwrap();
        auth.step_datafusion().await.unwrap();
        assert!(
            (sum_total(&auth) - 400.0).abs() < 1e-9,
            "fallback tick total {} != 400",
            sum_total(&auth)
        );
        assert!((sum_total(&auth) - sum_total(&central)).abs() < 1e-9);
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

    /// G6/F4 recreate path: repeatedly destroying the flow and rebuilding it
    /// from `checkpoint_full`/`restore_full` must converge across *multiple*
    /// cycles on the O(Δ) incremental path (no `force_diff_based`).
    ///
    /// Regression: a restored flow rebuilds its incremental operator empty, so
    /// before the seed-from-restored-state fix the first post-restore delta
    /// emitted a non-retracting insertion. Cycle 1 happened to total correctly
    /// (the inserted group row was new), but cycle 2 re-emitted the identical
    /// row, `apply_delta` deduplicated it, and the increment was lost — the view
    /// froze at the cycle-1 value. This drives the exact `spike_b --recreate`
    /// scenario in-process.
    #[tokio::test]
    async fn checkpoint_full_recreate_converges_across_cycles() {
        use arrow::array::{Float64Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        fn orders(regions: &[&str], amounts: &[i64]) -> RecordBatch {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("region", DataType::Utf8, false),
                    Field::new("amount", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(regions.to_vec())),
                    Arc::new(Int64Array::from(amounts.to_vec())),
                ],
            )
            .unwrap()
        }
        fn revenue_spec() -> krishiv_delta::IncrementalViewSpec {
            krishiv_delta::IncrementalViewSpec {
                name: "revenue".into(),
                body_sql: "SELECT region, SUM(amount) AS total FROM orders GROUP BY region".into(),
                output_schema: Arc::new(Schema::new(vec![
                    Field::new("region", DataType::Utf8, true),
                    Field::new("total", DataType::Float64, true),
                ])),
                is_materialized: true,
                is_recursive: false,
                lateness: vec![],
            }
        }
        fn total(flow: &IncrementalFlow) -> f64 {
            let snap = flow.snapshot("revenue").unwrap().unwrap();
            snap.column_by_name("total")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap_or(0.0))
                .sum()
        }

        // Original flow, mirrors spike_b's pre-restore state (185).
        let mut flow = IncrementalFlow::new();
        flow.register_view(revenue_spec()).unwrap();
        flow.feed(
            "orders",
            DeltaBatch::from_inserts(orders(&["US", "EU", "US", "APAC"], &[100, 50, 25, 10]))
                .unwrap(),
        )
        .unwrap();
        flow.step_datafusion().await.unwrap();
        let mut running = total(&flow);
        assert!(
            (running - 185.0).abs() < 1e-9,
            "pre-restore total: {running}"
        );

        // Five destroy → recreate → restore → feed +2 → step cycles.
        for cycle in 1..=5 {
            let cp = flow.checkpoint_full().unwrap();
            // Destroy the flow entirely and rebuild from the checkpoint (the
            // real coordinator-restart recovery, not restore-into-live-flow).
            let fresh = IncrementalFlow::new();
            fresh.register_view(revenue_spec()).unwrap();
            fresh.restore_full(&cp).unwrap();
            flow = fresh;

            flow.feed(
                "orders",
                DeltaBatch::from_inserts(orders(&["US", "EU"], &[1, 1])).unwrap(),
            )
            .unwrap();
            flow.step_datafusion().await.unwrap();
            running += 2.0;
            let got = total(&flow);
            assert!(
                (got - running).abs() < 1e-9,
                "cycle {cycle}: total={got} expected={running} (baseline lost across restore)"
            );
        }
        assert!((running - 195.0).abs() < 1e-9); // 185 + 2*5
    }
}
