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
use tokio::sync::{broadcast, watch};

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

/// Per-map entry counts for one flow (IVM-AUD-CORE-25).
///
/// Every field counts *entries*, not bytes, except `dedup_hashes_retained`
/// which counts individual retained row hashes across all sources — the only
/// one of these that can reach eight figures on its own.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetainedState {
    pub sources_with_snapshots: usize,
    pub sources_with_pending: usize,
    pub sources_with_dedup_hashes: usize,
    pub dedup_hashes_retained: usize,
    pub sources_with_checkpoint_deltas: usize,
    pub sources_with_streaming_snapshots: usize,
    pub sources_with_ordinals: usize,
    pub sources_with_watermarks: usize,
    pub views_registered: usize,
    pub views_with_plans: usize,
    pub views_with_stats: usize,
    pub views_with_pending_plan_state: usize,
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

/// One row's event time as epoch milliseconds, for the same column types
/// `max_epoch_ms` accepts.
fn epoch_ms_at(arr: &dyn arrow::array::Array, row: usize) -> Option<i64> {
    use arrow::array::{Int64Array, TimestampMillisecondArray};
    if arr.is_null(row) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Some(a.value(row));
    }
    None
}

/// Enforce this source's LATENESS bound on an incoming delta, and advance its
/// watermark.
///
/// Returns the batch to actually ingest and how many rows the bound dropped.
///
/// IVM-AUD-CORE-7: `lateness.rs` states the contract plainly — "records
/// arriving with `ts < watermark` are dropped at ingestion" — but
/// `WatermarkTracker::is_late` had ZERO production callers, so a record three
/// days late mutated the aggregate exactly like an on-time one while the
/// module claimed otherwise. It is enforced here now, with two deliberate
/// rules:
///
///   * **Only insertions are dropped.** Dropping a retraction would strand its
///     insertion in the Z-set forever — the view would never converge. A late
///     retraction is always applied.
///   * **The bound is evaluated against the watermark from PRIOR batches**, so
///     a batch can never make its own rows late.
///
/// IVM-AUD-CORE-9: the tracker for a source is created here, on first sight of
/// a batch that actually carries a declared lateness column. That is what lets
/// a join view (two sources, previously skipped as "ambiguous") get a
/// watermark per side.
fn apply_lateness(
    inner: &mut IncrementalFlowInner,
    source_name: &str,
    batch: DeltaBatch,
) -> IvmResult<(DeltaBatch, usize)> {
    let data = batch.data_batch();
    let schema = data.schema();

    // Resolve (and lazily create) this source's tracker from the declarations.
    if !inner.watermark_trackers.contains_key(source_name)
        && let Some(spec) = inner
            .declared_lateness
            .iter()
            .find(|l| schema.index_of(&l.column).is_ok())
            .cloned()
    {
        inner
            .watermark_trackers
            .insert(source_name.to_string(), WatermarkTracker::new(spec));
    }
    let Some(column) = inner
        .watermark_trackers
        .get(source_name)
        .map(|t| t.lateness_column().to_string())
    else {
        return Ok((batch, 0));
    };
    let Ok(idx) = schema.index_of(&column) else {
        return Ok((batch, 0));
    };

    // Evaluate lateness against the watermark established by earlier batches.
    let prior_watermark = inner
        .watermark_trackers
        .get(source_name)
        .map(|t| t.watermark());
    let mut dropped = 0usize;
    let kept = if let Some(watermark) = prior_watermark {
        let weights = batch.weights();
        let ts = data.column(idx);
        let mask: arrow::array::BooleanArray = (0..data.num_rows())
            .map(|row| {
                // Retractions are never dropped: stranding an insertion would
                // stop the view from ever converging.
                if weights.value(row) < 0 {
                    return Some(true);
                }
                match epoch_ms_at(ts.as_ref(), row) {
                    Some(v) if v < watermark => {
                        dropped += 1;
                        Some(false)
                    }
                    _ => Some(true),
                }
            })
            .collect();
        if dropped == 0 {
            batch
        } else {
            batch.filter_mask(&mask).map_err(delta_err)?
        }
    } else {
        batch
    };

    // Advance the watermark from the rows actually ingested.
    let kept_data = kept.data_batch();
    if let Ok(kept_idx) = kept_data.schema().index_of(&column)
        && let Some(max_ts) = max_epoch_ms(kept_data.column(kept_idx).as_ref())
        && let Some(tracker) = inner.watermark_trackers.get_mut(source_name)
    {
        tracker.observe(max_ts);
    }
    Ok((kept, dropped))
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
    /// A recursive view's body did not reach a fixed point within
    /// `MAX_FIXPOINT_ITERS` iterations, so this tick has no value for it
    /// (IVM-AUD-CORE-12). The view keeps its previous value.
    FixpointNotConverged,
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
    /// Rows dropped per source by a LATENESS bound, so the enforcement is
    /// observable rather than silent (IVM-AUD-CORE-7).
    late_dropped_rows: AHashMap<String, u64>,
    /// Duplicate row copies per source discarded by `restore_delta`'s
    /// set-materialization (IVM-AUD-CORE-29). Non-zero means the restored
    /// source is not multiset-equal to the one that was checkpointed.
    delta_restore_collapsed_rows: AHashMap<String, u64>,
    /// Union of every registered view's LATENESS declarations.
    ///
    /// IVM-AUD-CORE-9: association used to happen at register time and only
    /// for a view with EXACTLY ONE source dependency, so a join view — by
    /// construction two deps, and the only shape whose state GC actually
    /// matters — never got a tracker at all. Trackers are now resolved at feed
    /// time by schema membership: the source whose batch actually carries the
    /// declared timestamp column is the source that watermark applies to,
    /// which is unambiguous for single-source, multi-source and join views
    /// alike.
    declared_lateness: Vec<krishiv_delta::LatenessSpec>,

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
    /// Set by `restore`/`restore_delta`: the sources were replaced wholesale,
    /// so every view's derived state was cleared and must be recomputed from
    /// the restored inputs on the next tick — even a tick with no new input.
    /// Without it a restored flow never rebuilds: a view is recomputed only
    /// when a dependency is dirty, and a restore makes nothing dirty
    /// (IVM-AUD-CORE-16).
    rebuild_all_views: bool,
    /// Bumped by every operation that replaces state a tick in flight has
    /// already read: `restore`, `restore_full`, `restore_delta`,
    /// `apply_computed_tick`, `apply_remote_tick`, `invalidate_view_plans`, and
    /// each committed `step_datafusion`.
    ///
    /// IVM-AUD-CORE-19: a tick clones `source_snapshots` under the lock, runs
    /// view SQL for as long as it takes with the lock RELEASED, then reassigns
    /// `inner.source_snapshots = new_snapshots` wholesale. Anything that landed
    /// in that window — a restore, a mirrored remote tick, a second concurrent
    /// `step_datafusion_with_ctx` (which does not take the `tick_ctx` mutex at
    /// all) — was overwritten with no error anywhere. A tick now records this
    /// counter when it reads the state and re-checks it before committing; a
    /// mismatch aborts the tick, which returns its drained deltas to `pending`
    /// through the custody guard so the caller can simply step again.
    state_epoch: u64,

    // Per-step output deltas, keyed by view name, captured during the most
    // recent `step_datafusion`. Cleared at the start of each step. Lets a caller
    // consume the O(Δ) changelog the flow already computed (`take_step_output`)
    // instead of re-materializing the full view and diffing snapshots.
    last_step_outputs: AHashMap<String, DeltaBatch>,
    /// Flow tick at which each view last successfully published an output
    /// delta — i.e. which tick the value behind `view_output_peek` came from.
    ///
    /// IVM-AUD-PART-11: a partitioned flow concatenates its shards' peeked
    /// deltas, and without this there was no way to tell a shard that emitted
    /// this tick from one that last emitted five ticks ago; the two were
    /// merged and served as "the latest delta".
    view_output_ticks: AHashMap<String, u64>,
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
/// How a view executes, as far as the flow can tell at this moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewExecution {
    /// Registered, but no tick has built a plan for it yet — a shard that owns
    /// none of a view's keys stays here indefinitely.
    NotYetPlanned,
    /// An O(Δ) incremental plan is cached for it.
    Incremental,
    /// It is recomputed in full each tick it is dirty.
    DiffBased,
}

/// Thread-safe and `Clone`-able: all clones share the same underlying state.
#[derive(Clone)]
pub struct IncrementalFlow {
    inner: Arc<Mutex<IncrementalFlowInner>>,
    /// Spill-capable `SessionContext` reused across `step_datafusion` ticks
    /// (G14): building a fresh context per tick dominated tick latency in the
    /// IVM-vs-recompute benchmark. Guarded by an async mutex so cached-path
    /// ticks serialize; `step_datafusion_with_ctx` callers are unaffected.
    tick_ctx: Arc<tokio::sync::Mutex<CachedTickContext>>,
    /// Byte budget for this flow's tick `SessionContext` memory pool, resolved
    /// once at construction; `None` means DataFusion's unbounded pool.
    ///
    /// IVM-AUD-PART-13: this used to be re-derived inside every flow from
    /// `ivm_memory_limit_bytes()` — 25% of the *whole container* — at the
    /// moment the flow first built its context. A partitioned job is N flows,
    /// so an 8-shard job (`default_ivm_shards()` = `min(cores, 8)`) licensed
    /// itself to 8 × 25% = 200% of the container: precisely the OOM the spill
    /// module exists to prevent. `PartitionedIncrementalFlow::new` now divides
    /// one budget across its shards and hands each shard its share.
    tick_memory_limit: Option<usize>,
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
        Self::with_registry_and_memory_limit(view_registry, crate::spill::ivm_memory_limit_bytes())
    }

    /// A flow whose tick `SessionContext` is capped at `tick_memory_limit`
    /// bytes instead of the process-wide default.
    ///
    /// This exists for `PartitionedIncrementalFlow`, which must divide one
    /// container budget across its shards rather than give each shard the
    /// whole thing (IVM-AUD-PART-13). `None` means unbounded.
    pub fn with_memory_limit(tick_memory_limit: Option<usize>) -> Self {
        Self::with_registry_and_memory_limit(IncrementalViewRegistry::new(), tick_memory_limit)
    }

    fn with_registry_and_memory_limit(
        view_registry: IncrementalViewRegistry,
        tick_memory_limit: Option<usize>,
    ) -> Self {
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
                late_dropped_rows: AHashMap::new(),
                delta_restore_collapsed_rows: AHashMap::new(),
                declared_lateness: Vec::new(),
                force_diff_based: false,
                view_deps: AHashMap::new(),
                rebuild_all_views: false,
                state_epoch: 0,
                last_step_outputs: AHashMap::new(),
                view_delta_stats: AHashMap::new(),
                pending_plan_state: HashMap::new(),
                view_output_ticks: AHashMap::new(),
            })),
            tick_ctx: Arc::new(tokio::sync::Mutex::new(CachedTickContext::default())),
            tick_memory_limit,
        }
    }

    /// Enable opt-in **tick-granular** provenance tracking.
    ///
    /// Once enabled, each `step_datafusion` call records which input row hashes
    /// arrived in the tick and which output row hashes the tick produced, for
    /// views that run on the **DiffBased** path only. `query_provenance(h)` then
    /// returns the output hashes of the tick that carried `h` — not the outputs
    /// derived from `h`. Those coincide only when a tick carries one input row.
    ///
    /// This is deliberately narrower than the docs used to claim ("look up which
    /// output rows a given input row produced, enabling automatic retraction
    /// without Z-set algebra"): the recorded relation has always been
    /// tick-granular, and no code path here can produce a finer one. See
    /// [`crate::provenance`] and IVM-AUD-PART-23.
    ///
    /// Memory is bounded by
    /// [`DEFAULT_RETENTION_TICKS`](crate::provenance::DEFAULT_RETENTION_TICKS);
    /// use [`enable_provenance_tracking_with_retention`](Self::enable_provenance_tracking_with_retention)
    /// to choose the window.
    pub fn enable_provenance_tracking(&self) -> IvmResult<()> {
        self.enable_provenance_tracking_with_retention(crate::provenance::DEFAULT_RETENTION_TICKS)
    }

    /// [`enable_provenance_tracking`](Self::enable_provenance_tracking) with an
    /// explicit retention window, in ticks (minimum 1).
    pub fn enable_provenance_tracking_with_retention(&self, retention_ticks: u64) -> IvmResult<()> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.provenance = Some(crate::provenance::ProvenanceIndex::with_retention(
            retention_ticks,
        ));
        Ok(())
    }

    /// The LATENESS bounds declared on this flow's registered views.
    ///
    /// Exposed so a caller can verify a bound actually reached the engine —
    /// the distributed register-view path used to drop it at the wire
    /// (IVM-AUD-DDL-B1).
    pub fn declared_lateness(&self) -> IvmResult<Vec<krishiv_delta::LatenessSpec>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner.declared_lateness.clone())
    }

    /// Output row hashes produced by the tick that carried `input_hash`.
    ///
    /// `None` when provenance is disabled, the hash was never recorded, or its
    /// tick has aged out of the retention window. Tick-granular — see
    /// [`enable_provenance_tracking`](Self::enable_provenance_tracking).
    pub fn query_provenance(&self, input_hash: u64) -> IvmResult<Option<AHashSet<u64>>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner
            .provenance
            .as_ref()
            .and_then(|p| p.outputs_for(input_hash))
            .cloned())
    }

    /// Drop the provenance mapping for `input_hash`.
    ///
    /// Only that input's own entry goes; the tick's output set is shared with
    /// every other input row of the tick and ages out with the tick.
    pub fn forget_provenance(&self, input_hash: u64) -> IvmResult<()> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        if let Some(ref mut p) = inner.provenance {
            p.forget(input_hash);
        }
        Ok(())
    }

    /// Approximate in-memory size of the input deltas queued for the next tick.
    ///
    /// IVM-AUD-INT-F11: `pending` is an unbounded `Vec` per source and nothing
    /// anywhere applied backpressure, so a producer feeding faster than the
    /// stepper drains simply grew the coordinator's heap until it died, with
    /// every `/feed` answering `success: true` on the way. This is the number
    /// an ingress gate needs to refuse the next feed instead
    /// (`ivm_http::ensure_pending_headroom`).
    ///
    /// "Approximate" is Arrow's own `get_array_memory_size`: allocated buffer
    /// bytes, which over-counts a sliced array sharing a parent buffer and
    /// ignores per-`DeltaBatch` overhead. It is the right order of magnitude
    /// for a backlog limit, not an accounting figure.
    pub fn pending_bytes(&self) -> IvmResult<usize> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner
            .pending
            .values()
            .flatten()
            .map(|delta| delta.inner().get_array_memory_size())
            .sum())
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
            // IVM-AUD-CORE-E4: `lateness` belongs in this comparison. Leaving
            // it out meant re-registering a view with only its LATENESS
            // changed hit the early return and silently kept the old
            // watermark — the declaration was accepted and had no effect.
            let unchanged = existing.spec.body_sql == spec.body_sql
                && existing.spec.is_materialized == spec.is_materialized
                && existing.spec.is_recursive == spec.is_recursive
                && existing.spec.lateness.len() == spec.lateness.len()
                && existing
                    .spec
                    .lateness
                    .iter()
                    .zip(spec.lateness.iter())
                    .all(|(a, b)| a.column == b.column && a.lateness_ms == b.lateness_ms);
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

        // AUD-8 / IVM-AUD-CORE-9: record the declaration. Which SOURCE each
        // declaration governs is resolved at feed time by schema membership
        // (see `apply_lateness`), so a join view's two sources each get their
        // own watermark instead of the whole view being skipped as
        // "ambiguous".
        for l in &spec.lateness {
            if !inner
                .declared_lateness
                .iter()
                .any(|d| d.column == l.column && d.lateness_ms == l.lateness_ms)
            {
                // A changed bound for the same column replaces the old one,
                // and any tracker built from the old bound is discarded so the
                // new bound actually takes effect (IVM-AUD-CORE-E4).
                inner.declared_lateness.retain(|d| d.column != l.column);
                inner
                    .watermark_trackers
                    .retain(|_, t| t.lateness_column() != l.column);
                inner.declared_lateness.push(l.clone());
            }
        }

        inner.view_registry.register(spec).map_err(delta_err)
    }

    /// Drop a view and every piece of per-view state the flow holds for it.
    ///
    /// IVM-AUD-CORE-25: this used to prune four of the seven per-view maps.
    /// `view_delta_stats`, `pending_plan_state` and `last_step_outputs` kept
    /// their entries for the life of the process, so a job that registered and
    /// dropped views (a session re-running a pipeline under generated names, a
    /// coordinator rehydrating jobs) grew monotonically — and a view later
    /// re-registered under the same name inherited the dead view's counters and
    /// its stale checkpointed operator state.
    pub fn drop_view(&self, name: &str) -> IvmResult<bool> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.view_deps.remove(name);
        inner.view_plans.remove(name);
        inner.view_plan_sqls.remove(name);
        inner.view_output_ticks.remove(name);
        inner.view_delta_stats.remove(name);
        inner.pending_plan_state.remove(name);
        inner.last_step_outputs.remove(name);
        inner.view_registry.drop_view(name).map_err(delta_err)
    }

    /// Drop a source and every piece of per-source state the flow holds for it.
    ///
    /// Returns `true` if anything was actually held for `name`.
    ///
    /// IVM-AUD-CORE-25: there was no such method at all. Nine maps are keyed by
    /// source name — the materialized snapshot, the un-stepped `pending`
    /// deltas, the dedup hash set (up to 10 M hashes), the delta-checkpoint
    /// accumulator, the streaming-bridge previous snapshot, the ordinal, the
    /// watermark tracker and two counters — and every one of them was
    /// write-only for the life of the flow. A long-lived coordinator whose
    /// sources come and go (per-tenant tables, a connector reconfigured under a
    /// new name) had no way to give any of it back.
    ///
    /// Matching is case-insensitive, because `feed` accepts `"Sales"` and
    /// `"sales"` as the same target.
    pub fn drop_source(&self, name: &str) -> IvmResult<bool> {
        let wanted = name.to_lowercase();
        let mut inner = self.inner.lock().map_err(lock_err)?;
        let mut found = false;
        macro_rules! prune {
            ($map:expr) => {{
                let keys: Vec<String> = $map
                    .keys()
                    .filter(|k| k.to_lowercase() == wanted)
                    .cloned()
                    .collect();
                for k in keys {
                    $map.remove(&k);
                    found = true;
                }
            }};
        }
        prune!(inner.pending);
        prune!(inner.source_snapshots);
        prune!(inner.seen_input_hashes);
        prune!(inner.checkpoint_deltas);
        prune!(inner.streaming_prev_snapshots);
        prune!(inner.source_ordinals);
        prune!(inner.watermark_trackers);
        prune!(inner.late_dropped_rows);
        prune!(inner.delta_restore_collapsed_rows);
        Ok(found)
    }

    /// How many per-source / per-view entries the flow is currently holding.
    ///
    /// IVM-AUD-CORE-25: the maps below have no size bound of their own — they
    /// grow with the number of distinct names ever fed or registered — so the
    /// first question about a flow that will not stop growing is which map is
    /// growing. Before this there was no way to ask from outside the crate.
    pub fn retained_state(&self) -> IvmResult<RetainedState> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(RetainedState {
            sources_with_snapshots: inner.source_snapshots.len(),
            sources_with_pending: inner.pending.len(),
            sources_with_dedup_hashes: inner.seen_input_hashes.len(),
            dedup_hashes_retained: inner.seen_input_hashes.values().map(|(_, s)| s.len()).sum(),
            sources_with_checkpoint_deltas: inner.checkpoint_deltas.len(),
            sources_with_streaming_snapshots: inner.streaming_prev_snapshots.len(),
            sources_with_ordinals: inner.source_ordinals.len(),
            sources_with_watermarks: inner.watermark_trackers.len(),
            views_registered: inner.view_registry.view_names().map_err(delta_err)?.len(),
            views_with_plans: inner.view_plans.len(),
            views_with_stats: inner.view_delta_stats.len(),
            views_with_pending_plan_state: inner.pending_plan_state.len(),
        })
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
        Ok(self
            .view_execution(view)?
            .map(|(execution, reason)| (matches!(execution, ViewExecution::Incremental), reason)))
    }

    /// How `view` executes right now, distinguishing "no plan has been built
    /// yet" from "a plan was built and it is a full recompute".
    ///
    /// [`view_plan_classification`](Self::view_plan_classification) folds both
    /// into `false`, which is the honest answer for one flow — neither is
    /// incremental *yet*. A partitioned job cannot fold them: a shard that owns
    /// none of the keys never builds a plan, so with three shards and two keys
    /// one shard is permanently unplanned, and treating that as "not
    /// incremental" would report every partitioned view as degraded
    /// (IVM-AUD-PART-12).
    pub fn view_execution(&self, view: &str) -> IvmResult<Option<(ViewExecution, String)>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        if inner.view_registry.get(view).is_err() {
            return Ok(None);
        }
        // A recursive view never gets an O(Δ) plan (see the `is_recursive`
        // guard in the tick), so "not yet planned" would be a permanent lie
        // about it: it is recomputed to a fixed point every tick it is dirty.
        if inner
            .view_registry
            .get(view)
            .is_ok_and(|v| v.spec.is_recursive)
        {
            return Ok(Some((
                ViewExecution::DiffBased,
                "recursive view — re-evaluated to a fixed point from SQL each tick it is \
                 dirty; recursive bodies have no O(Δ) plan"
                    .to_string(),
            )));
        }
        Ok(Some(match inner.view_plans.get(view) {
            None => (
                ViewExecution::NotYetPlanned,
                "not yet planned — no tick has executed; the O(Δ) plan is built lazily on \
                 the first step, after which this view will report its true strategy"
                    .to_string(),
            ),
            Some(plan) => (
                match plan.kind() {
                    ViewPlanKind::Incremental => ViewExecution::Incremental,
                    _ => ViewExecution::DiffBased,
                },
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
            let inner = self.inner.lock().map_err(lock_err)?;
            if let Some(last) = inner.source_ordinals.get(&source_name)
                && *last == ordinal
            {
                return Ok(()); // Same offset — nothing new.
            }
        } // Release lock before calling feed.
        // IVM-AUD-CORE-21: commit the ordinal only AFTER the feed succeeds.
        // Committing first meant a failing `feed` (a dedup filter error, or
        // now an unknown-source rejection) advanced the offset anyway: the
        // batch was dropped permanently and a retry at the same offset was a
        // silent no-op.
        self.feed(source_name.clone(), batch)?;
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.source_ordinals.insert(source_name, ordinal);
        Ok(())
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
        validate_feed_target(&inner, &source_name)?;

        // Content-addressed dedup: filter out re-delivered insertion rows.
        let batch = if inner.input_dedup_enabled {
            let (order, set) = inner
                .seen_input_hashes
                .entry(source_name.clone())
                .or_default();
            let (filtered, evicted) =
                dedup_filter(order, set, batch, DEDUP_SEEN_CAPACITY, DEDUP_EVICT_BATCH)?;
            if evicted > 0 {
                tracing::warn!(
                    source = %source_name,
                    capacity = DEDUP_SEEN_CAPACITY,
                    evicted,
                    "dedup set capacity reached; evicted oldest entries"
                );
            }
            filtered
        } else {
            batch
        };

        if batch.is_empty() {
            return Ok(());
        }

        // AUD-8 / IVM-AUD-CORE-7: enforce the LATENESS bound and advance the
        // watermark. Drops are counted and logged — a declared bound silently
        // discarding rows would be its own kind of dishonesty.
        let (batch, dropped_late) = apply_lateness(&mut inner, &source_name, batch)?;
        if dropped_late > 0 {
            tracing::warn!(
                source = %source_name,
                dropped = dropped_late,
                "LATENESS bound dropped late insertion rows at ingestion"
            );
            *inner
                .late_dropped_rows
                .entry(source_name.clone())
                .or_insert(0) += dropped_late as u64;
        }
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
        validate_feed_target(&inner, &source_name)?;
        if batch.is_empty() {
            return Ok(());
        }
        // AUD-8 / IVM-AUD-CORE-7: same bound on the coalesced path.
        let (batch, dropped_late) = apply_lateness(&mut inner, &source_name, batch)?;
        if dropped_late > 0 {
            tracing::warn!(
                source = %source_name,
                dropped = dropped_late,
                "LATENESS bound dropped late insertion rows at ingestion"
            );
            *inner
                .late_dropped_rows
                .entry(source_name.clone())
                .or_insert(0) += dropped_late as u64;
        }
        if batch.is_empty() {
            return Ok(());
        }
        // IVM-AUD-CORE-20: keep the delta-checkpoint accumulator consistent
        // with `pending`. This path REPLACES the pending delta (that is the
        // point — snapshot sources coalesce), so the accumulator must replace
        // too. It previously skipped accumulation entirely, so a job using
        // coalesced feeds produced delta checkpoints that silently omitted
        // every coalesced input and restored to a wrong state.
        if inner.delta_checkpoint_enabled {
            inner
                .checkpoint_deltas
                .insert(source_name.clone(), vec![batch.clone()]);
        }
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
    /// Stateful: owns the per-source `streaming_prev_snapshots` map, which
    /// `checkpoint_full`/`restore_full` carry (IVM-AUD-CORE-27), so it cannot be
    /// a `DeltaBatch` constructor. Use `feed` directly if your producer already
    /// emits `DeltaBatch`es.
    ///
    /// IVM-AUD-INT-F8: this is a **seam you call**, not a wired integration.
    /// Nothing in the engine pipes a `StreamingJob`'s output into it — every
    /// caller repo-wide is a user-facing surface (the Python handle, MCP, the
    /// coordinator's `/stream-bridge` route) handing over batches the caller
    /// already has. Running a streaming job into an incremental view is
    /// application code: drain the job, call this.
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
                // The caller supplied this delta; nothing advanced the diff
                // baseline for it, so it must advance here (IVM-AUD-CORE-18).
                let _ = view.apply_output_delta(&delta);
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
            .get_or_insert_with(|| {
                crate::spill::spill_session_context_with_limit(self.tick_memory_limit)
            })
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
            epoch_at_read,
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
            // IVM-AUD-CORE-19: everything above was read under this epoch. If
            // it moves before Phase 5 commits, the results computed from it are
            // stale and must not be written back.
            let epoch = inner.state_epoch;
            (
                epoch,
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
        // IVM-AUD-PART-1: take custody of the drained deltas for the rest of
        // the tick. If we leave by any path other than `commit()` — a `?`, a
        // panic, or the future being dropped by a timeout or a sibling shard's
        // error — Drop returns them to `pending` so the next tick reprocesses
        // them instead of silently losing them. `DeltaBatch` clones share
        // Arc'd Arrow buffers, so custody costs a refcount bump per batch.
        let custody = DrainedPending::new(Arc::clone(&self.inner), raw_pending.clone());
        let inputs = coalesce_pending(raw_pending)?;

        // A restore replaced the sources and cleared every view's derived
        // state, so this tick must rebuild the views even though no input
        // arrived (IVM-AUD-CORE-16).
        let rebuild_all_views = {
            let mut inner = self.inner.lock().map_err(lock_err)?;
            std::mem::take(&mut inner.rebuild_all_views)
        };

        if inputs.is_empty() && !rebuild_all_views {
            // Nothing to reprocess: release custody before returning.
            custody.commit();
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

        // Pass A — resolve, in topo order, which views this tick touches and
        // which of them get an O(Δ) incremental plan. This is separated from
        // the execution pass below because a view's execution needs to know
        // whether any *downstream* view will run SQL against its output, and
        // that is only knowable once every plan kind is resolved
        // (IVM-AUD-CORE-17).
        let mut dirty_order: Vec<String> = Vec::new();
        let mut plan_is_incremental_by_view: HashMap<String, bool> = HashMap::new();
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
            if !is_dirty && !rebuild_all_views {
                continue;
            }
            dirty_views.insert(view_name_lower);

            // Determine if this view gets an incremental plan (skip SQL) or DiffBased (run SQL).
            // `force_diff_based` (transient executor flows) never uses incremental
            // plans: their accumulator state is not transferable via checkpoint.
            // On a rebuild tick every view recomputes its full output from
            // SQL: the incremental operators were cleared by the restore, and
            // an operator seeded from the restored state emits no delta, so an
            // incremental path would leave the view empty. Diffing the full
            // result against the cleared (None) baseline republishes the whole
            // view, which is exactly what a restore needs.
            // A recursive view is never O(Δ): its value is the fixed point of
            // its own body, which `run_recursive_fixpoint` reaches by re-running
            // the SQL. Nothing checked this before, so a recursive declaration
            // whose body happened to lower to an Aggregate/Distinct/Join plan
            // (`DECLARE RECURSIVE VIEW v AS SELECT k, SUM(x) … GROUP BY k` is
            // legal, if pointless) skipped the fixpoint loop altogether and was
            // maintained as if the RECURSIVE keyword were absent.
            let plan_is_incremental = if force_diff_based || rebuild_all_views || spec.is_recursive
            {
                false
            } else if views_needing_plans.contains(view_name) {
                let plan = crate::plan::build_view_plan(
                    &spec.body_sql,
                    &spec.output_schema,
                    &available_schemas,
                    &spec.lateness,
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
            dirty_order.push(view_name.clone());
            plan_is_incremental_by_view.insert(view_name.clone(), plan_is_incremental);
        }

        // A dirty view that will run SQL this tick needs every view it reads to
        // exist as a table holding *this* tick's output. An incremental view
        // does not otherwise produce one during this phase — its operator runs
        // later, under the lock — so name the incremental views that owe a
        // fresh full output to a SQL-running dependent.
        let owes_full_output_to_a_sql_dependent: HashSet<String> = dirty_order
            .iter()
            .filter(|v| {
                plan_is_incremental_by_view
                    .get(v.as_str())
                    .copied()
                    .unwrap_or(false)
            })
            .filter(|v| {
                let v_lower = v.to_lowercase();
                dirty_order.iter().any(|w| {
                    if plan_is_incremental_by_view
                        .get(w.as_str())
                        .copied()
                        .unwrap_or(false)
                    {
                        return false;
                    }
                    match view_deps.get(w.as_str()) {
                        Some(deps) => deps.contains(&v_lower),
                        None => spec_map
                            .get(w.as_str())
                            .is_some_and(|s| sql_identifiers(&s.body_sql).contains(&v_lower)),
                    }
                })
            })
            .cloned()
            .collect();

        // Pass B — execute.
        for view_name in &dirty_order {
            let spec = match spec_map.get(view_name.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let plan_is_incremental = plan_is_incremental_by_view
                .get(view_name.as_str())
                .copied()
                .unwrap_or(false);

            if plan_is_incremental && !owes_full_output_to_a_sql_dependent.contains(view_name) {
                // Register the previous snapshot for downstream DiffBased views.
                match view_full_outputs.get(view_name) {
                    Some(prev) if prev.num_rows() > 0 => {
                        let _ = tables.register(view_name.as_str(), prev);
                    }
                    // Empty/missing: a fresh context would have no such table.
                    _ => tables.remove(view_name.as_str()),
                }
            } else {
                // DiffBased — or an incremental view that owes a fresh full
                // output to a SQL-running dependent. In the latter case the
                // view pays one full recompute this tick purely to give its
                // dependent a table to read; its own output delta still comes
                // from its O(Δ) operator under the lock, so the incremental
                // plan is not abandoned. The cost is real and is the price of
                // attaching a non-incrementalizable view to an incremental
                // one; the alternative (processing the DAG level by level,
                // alternating locked operator application with unlocked SQL)
                // avoids the recompute and is the better long-term shape.
                //
                // Register all upstream outputs, then execute SQL.
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
                    let seed = view_full_outputs.get(view_name).cloned();
                    match run_recursive_fixpoint(spec, view_name, seed, &mut tables).await {
                        Ok(fixed_point) => {
                            view_full_outputs.insert(view_name.clone(), fixed_point);
                        }
                        Err(e) => {
                            tracing::warn!(
                                view = %view_name,
                                kind = ?e.kind,
                                error = %e.message,
                                "recursive view did not produce a fixed point this tick; \
                                 its previous value is left in place"
                            );
                            // Leave `view_full_outputs[view_name]` at the
                            // previous value: Phase 5 diffs it against the same
                            // baseline and publishes nothing, so the view holds
                            // its last fixed point instead of a fabricated one.
                            pre_lock_view_errors.push(e);
                        }
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
        // IVM-AUD-CORE-19: refuse to commit onto state that moved underneath
        // this tick. `new_snapshots` was derived from the Phase 1 snapshot and
        // the SQL results were computed against it, so assigning it now would
        // erase whatever landed in between — a restore, a mirrored remote tick,
        // or a concurrent tick — silently and completely. Drop the guard before
        // returning so the custody guard can reclaim the lock and put this
        // tick's deltas back in `pending`.
        if inner.state_epoch != epoch_at_read {
            drop(inner);
            return Err(IvmError::execution(
                "IVM tick aborted: the flow's state was replaced while this tick was running \
                 (a restore, a remote tick, or a concurrent step). The drained input deltas \
                 have been returned to the pending queue; step again.",
            ));
        }
        inner.state_epoch = inner.state_epoch.wrapping_add(1);
        inner.source_snapshots = new_snapshots;
        inner.last_step_outputs.clear();
        inner.tick += 1;
        let mut total_output_rows = 0usize;
        let mut total_inserted_rows = 0u64;
        let mut total_retracted_rows = 0u64;
        let mut active_views = 0usize;
        let mut errored_views: Vec<ViewError> = pre_lock_view_errors;
        let mut degraded_views: Vec<String> = Vec::new();

        // The tick this pass is publishing (already incremented above). Used to
        // stamp provenance so it can age out (IVM-AUD-PART-22).
        let current_tick = inner.tick;

        // Provenance: pre-compute weight-aware input hashes when enabled.
        // Each row is hashed with its weight encoded so rows that differ only
        // in multiplicity produce distinct provenance entries (G5 fix).
        let input_hashes: Option<Vec<u64>> = if inner.provenance.is_some() {
            let mut hashes: Vec<u64> = Vec::new();
            for delta in inputs.values() {
                let data = delta.data_batch();
                let weights = delta.weights();
                for row in 0..data.num_rows() {
                    let base = hash_row(&data, row)?;
                    let w = weights.value(row);
                    // Mix weight into the hash so weight=+1 ≠ weight=+2.
                    hashes.push(
                        base.wrapping_add(w.unsigned_abs().wrapping_mul(0x9e37_79b9_7f4a_7c15)),
                    );
                }
            }
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
            // What is recorded is tick-granular, not row-granular: "these input
            // rows arrived in tick N, and tick N produced these output rows".
            // The DiffBased path runs the view SQL and differentiates the
            // result, so there is no operator anywhere in it that could report
            // which output row came from which input row — see the module docs
            // on `provenance` and IVM-AUD-PART-23.
            //
            // It used to store that same tick-granular answer as a complete
            // bipartite graph: every input hash → every output hash, i.e.
            // O(inputs × outputs) hash inserts and the same again in memory
            // (10 k in / 10 k out = 10^8 inserts in one tick). `record_tick`
            // stores the relation once, in O(inputs + outputs), and ages it out.
            if plan_kind == ViewPlanKind::DiffBased
                && let (Some(input_hs), Some(prov)) = (&input_hashes, &mut inner.provenance)
            {
                let output_hs = crate::provenance::hash_all_rows(&output_delta.data_batch())?;
                prov.record_tick(current_tick, input_hs.iter().copied(), output_hs);
            }

            // Propagate this view's output delta to downstream views.
            available_deltas.insert(view_name.clone(), output_delta.clone());
            // Retain it so a caller can consume the O(Δ) changelog directly.
            inner
                .last_step_outputs
                .insert(view_name.clone(), output_delta.clone());
            // IVM-AUD-CORE-18: which publish is correct depends on whether the
            // diff baseline has already been advanced this tick.
            //
            //   * DiffBased ran `diff_and_update` a few lines up, which set the
            //     baseline to the full SQL result; `publish_output` must not
            //     advance it a second time.
            //   * The O(Δ) path never touched the baseline, so the publish has
            //     to advance it — and `publish_output` did that only for
            //     MATERIALIZED views. A non-materialized view maintained
            //     incrementally therefore kept `full_output = None` for its
            //     whole life, and the first pass that recomputed it in full (a
            //     `force_diff_based` executor tick, a plan invalidation, a
            //     restore) diffed against `None` and re-emitted the entire view
            //     as insertions. `apply_output_delta` advances both halves.
            let published = if plan_kind == ViewPlanKind::Incremental {
                view.apply_output_delta(&output_delta)
            } else {
                view.publish_output(output_delta)
            };
            match published {
                Ok(()) => {
                    // Stamp the tick the watch value now holds (PART-11).
                    let published_at = inner.tick;
                    inner
                        .view_output_ticks
                        .insert(view_name.clone(), published_at);
                }
                Err(e) => {
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

        // The tick applied its inputs: custody is released so Drop does not
        // re-queue them. Any earlier exit leaves this un-run and the deltas
        // return to `pending` (IVM-AUD-PART-1).
        custody.commit();

        Ok(StepSummary {
            total_output_rows,
            total_inserted_rows,
            total_retracted_rows,
            active_views,
            degraded_views,
            errored_views,
        })
    }

    /// Rows this source has had dropped by its LATENESS bound (IVM-AUD-CORE-7).
    ///
    /// Enforcement of a declared bound is real data loss — intentional and
    /// requested, but never something to hide. This counter (plus the WARN on
    /// every drop) is how an operator sees it happening.
    pub fn late_dropped_rows(&self, source: &str) -> IvmResult<u64> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner.late_dropped_rows.get(source).copied().unwrap_or(0))
    }

    /// Duplicate row copies this source lost to `restore_delta`'s
    /// set-materialization (IVM-AUD-CORE-29).
    ///
    /// `restore_delta` collapses each row to one copy so that re-applying the
    /// same delta slice is idempotent (G2). That trade is deliberate, but for a
    /// multiset source it is data loss, and a comment at the call site is not
    /// something an operator can query. Non-zero here means the restored source
    /// is not multiset-equal to the checkpointed one; `checkpoint_full` /
    /// `restore_full` preserve multiplicity and this counter stays 0 for them.
    pub fn delta_restore_collapsed_rows(&self, source: &str) -> IvmResult<u64> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner
            .delta_restore_collapsed_rows
            .get(source)
            .copied()
            .unwrap_or(0))
    }

    /// Cumulative insert/retract counters for one view (#94), if it has
    /// produced any output.
    pub fn view_delta_stats(&self, view: &str) -> IvmResult<Option<ViewDeltaStats>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner.view_delta_stats.get(view).copied())
    }

    // ── Subscriptions / snapshots ─────────────────────────────────────────────

    /// Subscribe to every output delta of `name`, in order.
    ///
    /// Audit: this used to hand back the view's `watch` receiver, which retains
    /// only the latest value — a subscriber slower than the step engine skipped
    /// deltas outright, so a vector sink could permanently miss an upsert or a
    /// delete with nothing logged. The broadcast stream reports a lagging
    /// subscriber explicitly instead of dropping silently.
    pub fn view_output_stream(&self, name: &str) -> IvmResult<broadcast::Receiver<DeltaBatch>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        let view = inner.view_registry.get(name).map_err(delta_err)?;
        Ok(view.subscribe_deltas())
    }

    /// Subscribe to the *latest* output delta of `name` (coalescing).
    ///
    /// For readers that only want current state; see [`Self::view_output_stream`]
    /// when every delta matters.
    pub fn view_output_latest(&self, name: &str) -> IvmResult<watch::Receiver<Option<DeltaBatch>>> {
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

    /// The byte ceiling on this flow's tick `SessionContext` memory pool, or
    /// `None` when it runs unbounded.
    ///
    /// Exposed so a caller that built several flows out of one budget can check
    /// that the division actually reached them (IVM-AUD-PART-13).
    pub fn tick_memory_limit(&self) -> Option<usize> {
        self.tick_memory_limit
    }

    /// The flow tick the value behind [`view_output_peek`](Self::view_output_peek)
    /// was published at, or `None` if this view has never emitted an output.
    ///
    /// The watch is coalescing and retains its value across quiet ticks, so
    /// "there is a delta" and "there is a delta *from this tick*" are different
    /// questions. A partitioned flow has to ask the second one before merging
    /// its shards' peeks (IVM-AUD-PART-11).
    pub fn view_output_tick(&self, name: &str) -> IvmResult<Option<u64>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        inner.view_registry.get(name).map_err(delta_err)?;
        Ok(inner.view_output_ticks.get(name).copied())
    }

    /// Whether `name` was registered as a materialized view.
    ///
    /// Callers need this to tell "not materialized" from "materialized but
    /// empty": both produce `snapshot() == None`, and conflating them reads a
    /// correct engine as a broken one.
    pub fn view_is_materialized(&self, name: &str) -> bool {
        let Ok(inner) = self.inner.lock() else {
            return false;
        };
        inner
            .view_registry
            .get(name)
            .map(|view| view.spec.is_materialized)
            .unwrap_or(false)
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
        write_u32_len(&mut out, entries.len(), "source count")?;
        for (name, snapshot) in entries {
            let delta = DeltaBatch::from_inserts(snapshot.clone()).map_err(delta_err)?;
            let ipc = serialize_delta_batch(&delta).map_err(delta_err)?;
            let name_bytes = name.as_bytes();
            write_u32_len(&mut out, name_bytes.len(), "source name")?;
            out.extend_from_slice(name_bytes);
            write_u32_len(&mut out, ipc.len(), "source snapshot")?;
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
        // IVM-AUD-CORE-16: the sources were just replaced, so any cached
        // incremental operator holds accumulators built from the PREVIOUS
        // inputs. `restore_full` clears them for exactly this reason; this
        // path did not, so the next tick applied fresh deltas to a stale
        // accumulator. Clear them here too, and reset each view's baseline AND
        // snapshot together (see `reset_state`) so the recompute cannot land
        // on top of stale materialized rows.
        inner.view_plans.clear();
        inner.view_plan_sqls.clear();
        inner.pending_plan_state.clear();
        let names = inner.view_registry.view_names().map_err(delta_err)?;
        for name in &names {
            if let Ok(view) = inner.view_registry.get(name) {
                view.reset_state().map_err(delta_err)?;
            }
        }
        inner.rebuild_all_views = true;
        inner.state_epoch = inner.state_epoch.wrapping_add(1);
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
        inner.state_epoch = inner.state_epoch.wrapping_add(1);
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
    ///
    /// # All-or-nothing (IVM-AUD-INT-F10)
    ///
    /// An error from this call leaves the mirror **as it was**, so the caller
    /// can re-feed the input deltas it drained and recompute the tick centrally
    /// without applying anything twice. Before, the source snapshots were
    /// committed one at a time and the tick counter bumped before any view was
    /// touched, so a failure part-way left a mirror that had eaten some of its
    /// input — a re-feed would have double-counted it, and not re-feeding lost
    /// it. That is why `submit_resident_ivm_step` used to skip its `refeed`
    /// guard on this path and let the deltas go.
    ///
    /// Two things a rollback here cannot take back, because they have already
    /// left the process: a delta emitted to a view's `subscribe` /
    /// `subscribe_deltas` channels, and the `last_output` value behind
    /// `view_output_peek`. A subscriber that saw a rolled-back delta will see
    /// the equivalent delta again when the central fallback recomputes the same
    /// tick — at-least-once, which is what that change feed already is.
    pub fn apply_remote_tick(
        &self,
        local_pending: HashMap<String, Vec<DeltaBatch>>,
        view_output_deltas: HashMap<String, DeltaBatch>,
    ) -> IvmResult<StepSummary> {
        let inputs = coalesce_pending(local_pending)?;
        let mut inner = self.inner.lock().map_err(lock_err)?;

        // Phase 1 — compute the advanced source snapshots but do NOT commit
        // them (mirrors step_datafusion's arithmetic, not its commit point).
        let mut advanced_sources: Vec<(String, RecordBatch)> = Vec::with_capacity(inputs.len());
        for (name, delta) in &inputs {
            let current = inner.source_snapshots.get(name).cloned();
            let updated = apply_delta(current, delta).map_err(delta_err)?;
            advanced_sources.push((name.clone(), updated));
        }

        // Phase 2 — mirror the executor's per-view output deltas, remembering
        // each view's prior (snapshot, baseline) so a failure part-way can put
        // every already-applied view back.
        let mut undo: Vec<(String, Option<RecordBatch>, Option<RecordBatch>)> = Vec::new();
        let mut applied: Vec<(String, DeltaBatch)> = Vec::new();
        let mut total_output_rows = 0usize;
        let mut total_inserted_rows = 0u64;
        let mut total_retracted_rows = 0u64;
        let mut active_views = 0usize;
        for (name, delta) in view_output_deltas {
            if delta.is_empty() {
                continue;
            }
            let Ok(view) = inner.view_registry.get(&name) else {
                continue;
            };
            let before_snapshot = view.snapshot().map_err(delta_err)?;
            let before_baseline = view.full_output_baseline().map_err(delta_err)?;
            if let Err(e) = view.apply_output_delta(&delta) {
                for (undo_name, snapshot, baseline) in undo {
                    if let Ok(undo_view) = inner.view_registry.get(&undo_name) {
                        // Best effort: the only way this fails is a poisoned
                        // view lock, which the apply above would have hit first.
                        let _ = undo_view.restore_state(snapshot, baseline);
                    }
                }
                return Err(delta_err(e));
            }
            undo.push((name.clone(), before_snapshot, before_baseline));
            total_output_rows += delta.num_rows();
            active_views += 1;
            let (inserts, retracts) = delta_insert_retract_counts(&delta);
            total_inserted_rows += inserts;
            total_retracted_rows += retracts;
            applied.push((name, delta));
        }

        // Phase 3 — commit. Nothing from here on can fail.
        for (name, batch) in advanced_sources {
            inner.source_snapshots.insert(name, batch);
        }
        inner.tick += 1;
        inner.state_epoch = inner.state_epoch.wrapping_add(1);
        inner.last_step_outputs.clear();
        let published_at = inner.tick;
        for (name, delta) in applied {
            let (inserts, retracts) = delta_insert_retract_counts(&delta);
            let stats = inner.view_delta_stats.entry(name.clone()).or_default();
            stats.rows_inserted_total += inserts;
            stats.rows_retracted_total += retracts;
            stats.last_tick_inserts = inserts;
            stats.last_tick_retracts = retracts;
            inner.view_output_ticks.insert(name.clone(), published_at);
            inner.last_step_outputs.insert(name, delta);
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
        inner.state_epoch = inner.state_epoch.wrapping_add(1);
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
        write_u32_len(&mut out, sources.len(), "source count")?;
        for (name, snap) in sources {
            encode_named_batch(&mut out, name, snap)?;
        }
        let names = inner.view_registry.view_names().map_err(delta_err)?;
        write_u32_len(&mut out, names.len(), "view count")?;
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
        write_u32_len(&mut out, plan_states.len(), "plan-state count")?;
        for (name, bytes) in plan_states {
            write_u32_len(&mut out, name.len(), "view name")?;
            out.extend_from_slice(name.as_bytes());
            write_u32_len(&mut out, bytes.len(), "operator state")?;
            out.extend_from_slice(&bytes);
        }
        encode_exact_state(&mut out, &inner)?;
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
            let (paired_name, full) = decode_named_batch_opt(bytes, &mut pos)?;
            // IVM-AUD-CORE-28: `checkpoint_full` writes the view name twice —
            // once with the snapshot, once with the full-output baseline — and
            // this used to drop the second copy unread. A blob whose two halves
            // had drifted out of step (a truncated write, a framing bug, two
            // writers) then restored view A's snapshot together with view B's
            // baseline, and every later diff was against the wrong view. The
            // second name is a checksum on the pairing; read it.
            if paired_name != name {
                return Err(IvmError::execution(format!(
                    "checkpoint view entry is inconsistent: snapshot is labelled '{name}' but \
                     the full-output baseline that follows it is labelled '{paired_name}'"
                )));
            }
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
        // Exact-state section (IVM-AUD-CORE-27), absent from blobs written
        // before it existed. Decoded before the lock is taken so a malformed
        // blob cannot leave the flow half-restored.
        let exact = decode_exact_state(bytes, &mut pos)?;
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
        if let Some(exact) = exact {
            apply_exact_state(&mut inner, exact);
        }
        inner.state_epoch = inner.state_epoch.wrapping_add(1);
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
        write_u32_len(&mut out, entries.len(), "source count")?;
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
            write_u32_len(&mut out, name_bytes.len(), "source name")?;
            out.extend_from_slice(name_bytes);
            write_u32_len(&mut out, ipc.len(), "accumulated delta")?;
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
            // IVM-AUD-CORE-29: the collapse above was guarded only by the
            // comment. It is a real, silent loss — a source holding the same
            // row twice comes back holding it once — so count it and say so.
            // `filter_positive` keeps one copy per positive row; the weights
            // it is about to discard are exactly the lost copies.
            let collapsed: u64 = consolidated
                .weights()
                .iter()
                .flatten()
                .filter(|w| *w > 1)
                .map(|w| (w - 1) as u64)
                .sum();
            if collapsed > 0 {
                tracing::warn!(
                    source = %name,
                    collapsed_copies = collapsed,
                    "restore_delta is set-materializing: duplicate row copies in this source \
                     were collapsed to one. Restore from checkpoint_full to keep multiplicity."
                );
                *inner
                    .delta_restore_collapsed_rows
                    .entry(name.clone())
                    .or_insert(0) += collapsed;
            }
            let snapshot = consolidated.filter_positive().map_err(delta_err)?;
            inner.source_snapshots.insert(name, snapshot);
        }
        // IVM-AUD-CORE-16: same treatment as `restore` — stale cached plans
        // must go, and the baseline and snapshot must be cleared together or
        // the next tick republishes the whole view on top of stale rows.
        inner.view_plans.clear();
        inner.view_plan_sqls.clear();
        inner.pending_plan_state.clear();
        let names = inner.view_registry.view_names().map_err(delta_err)?;
        for name in &names {
            if let Ok(view) = inner.view_registry.get(name) {
                view.reset_state().map_err(delta_err)?;
            }
        }
        inner.rebuild_all_views = true;
        inner.state_epoch = inner.state_epoch.wrapping_add(1);
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
pub(crate) fn hash_row(batch: &RecordBatch, row: usize) -> IvmResult<u64> {
    let mut combined: Vec<u8> = Vec::with_capacity(64);
    for col in batch.columns() {
        // Null-unambiguous encoding (crate-13 audit): the plain
        // `scalar_to_string` renders SQL null as the sentinel "NULL", which a
        // Utf8 value "NULL" collides with — dedup would silently drop the
        // legitimate row. `scalar_to_group_key` prefixes real values.
        // IVM-AUD-1: an unencodable type is an error, never a constant. A
        // constant would make every such row hash alike, and dedup drops
        // rows that hash alike — silent data loss.
        let s = krishiv_delta::operators::key_util::scalar_to_group_key(col.as_ref(), row)
            .map_err(|e| IvmError::execution(e.to_string()))?;
        combined.extend_from_slice(s.as_bytes());
        combined.push(0u8);
    }
    Ok(twox_hash::XxHash64::oneshot(
        0xcafe_babe_dead_beef_u64,
        &combined,
    ))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Custody of the deltas a tick drained out of `pending`.
///
/// IVM-AUD-PART-1: `step_datafusion_inner` drains `pending` in Phase 1 and
/// only applies it in the final phase, with an unbounded `await` on DataFusion
/// in between. Nothing put the deltas back if the tick did not reach the end,
/// and there are two live ways it does not:
///
///   * `PartitionedIncrementalFlow::step_datafusion` uses `try_join_all`, so
///     the first shard error DROPS the sibling futures mid-flight;
///   * the coordinator wraps the central step in `tokio::time::timeout`, so a
///     slow tick's future is dropped at the deadline.
///
/// In both cases the drained rows were simply gone: the caller saw a failed
/// step, retried, and the retry found nothing pending — a permanent undercount
/// for exactly the keys that shard owned, with no error naming the loss.
///
/// The guard restores custody on `Drop` unless `commit()` was called, which
/// covers the `?` early-return paths and future-cancellation alike (Drop runs
/// for a dropped future). Restoration puts the reclaimed deltas BEFORE
/// anything fed during the failed attempt, so replay order matches the order
/// the rows were originally accepted in (IVM-AUD-DIST-B2).
struct DrainedPending {
    inner: Arc<Mutex<IncrementalFlowInner>>,
    batches: Option<HashMap<String, Vec<DeltaBatch>>>,
}

impl DrainedPending {
    /// Take custody of `batches`, which have already been removed from
    /// `pending` by the caller.
    fn new(
        inner: Arc<Mutex<IncrementalFlowInner>>,
        batches: HashMap<String, Vec<DeltaBatch>>,
    ) -> Self {
        Self {
            inner,
            batches: Some(batches),
        }
    }

    /// The tick reached its commit point; release custody so `Drop` is a no-op.
    fn commit(mut self) {
        self.batches = None;
    }
}

impl Drop for DrainedPending {
    fn drop(&mut self) {
        let Some(batches) = self.batches.take() else {
            return;
        };
        if batches.is_empty() {
            return;
        }
        let restored: usize = batches.values().map(Vec::len).sum();
        match self.inner.lock() {
            Ok(mut inner) => {
                for (source, mut reclaimed) in batches {
                    let entry = inner.pending.entry(source).or_default();
                    // reclaimed (older) first, then whatever arrived since.
                    reclaimed.append(entry);
                    *entry = reclaimed;
                }
                tracing::warn!(
                    restored_batches = restored,
                    "IVM tick did not commit; drained input deltas returned to pending"
                );
            }
            Err(_) => {
                // The only unrecoverable case: without the lock the deltas
                // cannot be returned. Say so loudly rather than losing them
                // in silence.
                tracing::error!(
                    lost_batches = restored,
                    "IVM tick did not commit and the flow lock is poisoned; \
                     drained input deltas could not be restored"
                );
            }
        }
    }
}

/// Reject a feed aimed at a source no registered view reads (IVM-AUD-API-F1).
///
/// `feed` used to accept ANY name into `pending`, and the tick drains
/// `pending` wholesale — so a key no view references was drained and dropped.
/// `iv.insert(batch, source="order")` (a typo for `"orders"`) returned
/// success, advanced the tick, and lost the data forever. The check lives
/// here, at the single choke point, so it covers every surface: the Rust and
/// Python handles, the HTTP `/feed` and `/stream-delta` routes, the CLI and
/// MCP.
///
/// Deliberately permissive in one direction: with no views registered yet
/// there is nothing to validate against, so pre-registration feeds are still
/// allowed. Matching is case-insensitive because DataFusion lowercases
/// unquoted identifiers while callers name sources however they like
/// (IVM-AUD-CORE-24).
/// Drop insertion rows this source has already delivered, recording the ones
/// admitted, and keep the retained-hash set within `capacity`.
///
/// Returns the surviving delta and how many old hashes were evicted.
/// Retractions always pass: a retraction is not a re-delivery, and dropping one
/// would strand its insertion.
///
/// IVM-AUD-CORE-26: the capacity check used to run once per `feed`, *before* any
/// row was inserted, so one batch of N rows pushed the set N entries past the
/// cap — `DEDUP_SEEN_CAPACITY` bounded the set only for a caller feeding a row
/// at a time, which no caller does. The check is per admitted row here, so the
/// set never exceeds `capacity` regardless of batch size.
///
/// Eviction is FIFO and takes `evict_batch` entries at a time (1% of the cap in
/// production) so a burst can re-admit only that small window of rows, rather
/// than the whole history a full clear would re-admit.
fn dedup_filter(
    order: &mut VecDeque<u64>,
    set: &mut AHashSet<u64>,
    batch: DeltaBatch,
    capacity: usize,
    evict_batch: usize,
) -> IvmResult<(DeltaBatch, usize)> {
    let data = batch.data_batch();
    let weights = batch.weights();
    let mut evicted = 0usize;
    let mask: arrow::array::BooleanArray = (0..data.num_rows())
        .map(|row| -> IvmResult<Option<bool>> {
            if weights.value(row) <= 0 {
                return Ok(Some(true)); // retractions always pass
            }
            let h = hash_row(&data, row)?;
            if set.contains(&h) {
                return Ok(Some(false)); // already seen
            }
            if set.len() >= capacity {
                for _ in 0..evict_batch.max(1) {
                    match order.pop_front() {
                        Some(old) => {
                            set.remove(&old);
                            evicted += 1;
                        }
                        None => break,
                    }
                }
            }
            set.insert(h);
            order.push_back(h);
            Ok(Some(true))
        })
        .collect::<IvmResult<arrow::array::BooleanArray>>()?;
    let filtered = batch.filter_mask(&mask).map_err(delta_err)?;
    Ok((filtered, evicted))
}

fn validate_feed_target(inner: &IncrementalFlowInner, source_name: &str) -> IvmResult<()> {
    if inner.view_deps.is_empty() {
        return Ok(());
    }
    let wanted = source_name.to_lowercase();
    // A view name is itself a legal feed target on the view-DAG path (a
    // derived view reads its parent's output).
    let known: Vec<String> = inner
        .view_deps
        .iter()
        .flat_map(|(view, deps)| {
            std::iter::once(view.to_lowercase()).chain(deps.iter().map(|d| d.to_lowercase()))
        })
        .collect();
    if known.contains(&wanted) {
        return Ok(());
    }
    let mut sorted = known;
    sorted.sort();
    sorted.dedup();
    Err(IvmError::execution(format!(
        "no registered view reads source '{source_name}'; feeding it would silently          discard the delta at the next tick. Known sources: {sorted:?}"
    )))
}

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

/// A schema-only batch shaped like `spec`'s declared output.
fn empty_output_batch(spec: &IncrementalViewSpec) -> IvmResult<RecordBatch> {
    let cols: Vec<_> = spec
        .output_schema
        .fields()
        .iter()
        .map(|f| arrow::array::new_empty_array(f.data_type()))
        .collect();
    RecordBatch::try_new(spec.output_schema.clone(), cols)
        .map_err(|e| IvmError::execution(e.to_string()))
}

/// Rows that appear more than once in `batch` (counting multiplicity above the
/// first copy). Zero means the batch is a set.
///
/// Used only to explain a divergence: it is the difference between "your body
/// is not set-semantic" and "your recursion genuinely enumerates unboundedly
/// many distinct rows", and those need different fixes.
fn duplicate_row_count(batch: &RecordBatch) -> IvmResult<i64> {
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let schema = batch.schema();
    let as_delta = DeltaBatch::from_inserts(batch.clone()).map_err(delta_err)?;
    let consolidated = consolidate_batch(as_delta, &[], &schema).map_err(delta_err)?;
    Ok(consolidated
        .weights()
        .iter()
        .flatten()
        .filter(|w| *w > 1)
        .map(|w| w - 1)
        .sum())
}

/// Iterate a recursive view's body to a fixed point, inside one tick.
///
/// Naive evaluation: the view's own current value is registered as a table, the
/// body SQL is re-run against it, and the result becomes the next iterate. The
/// loop ends when an iterate equals its predecessor — an actual fixed point —
/// and in no other way. Every other outcome is an `Err`, never a value:
///
/// * the body SQL failed (IVM-AUD-CORE-11: this used to substitute an empty
///   batch and record nothing, so a recursive view whose SQL broke retracted
///   itself entirely and the tick reported success);
/// * the convergence diff failed (IVM-AUD-CORE-10: `differentiate(..)
///   .map(|d| d.is_empty()).unwrap_or(true)` read a *failed* comparison as
///   "converged" and stopped the loop at a non-fixed point);
/// * `MAX_FIXPOINT_ITERS` was reached (IVM-AUD-CORE-12: the cap used to publish
///   the Nth iterate as the view's answer, so a diverging query returned a
///   truncated result with no error and nothing in the step summary).
///
/// On `Err` the caller leaves the view at its previous value and the error
/// travels out in [`StepSummary::errored_views`].
///
/// The self-reference is seeded here. A recursive body reads its own name, and
/// the tick registers a table for a view only when it has a non-empty previous
/// output — so on the first tick `ctx.sql()` failed with "table not found",
/// that failure was swallowed as above, and the view stayed empty forever.
/// A `DECLARE RECURSIVE VIEW` produced no rows at all, on any input, and
/// reported a clean tick while doing it.
async fn run_recursive_fixpoint(
    spec: &IncrementalViewSpec,
    view_name: &str,
    seed: Option<RecordBatch>,
    tables: &mut TickTables<'_>,
) -> Result<RecordBatch, ViewError> {
    let err = |kind: ViewErrorKind, message: String| ViewError {
        view: view_name.to_string(),
        kind,
        message,
    };
    let sql_err = |e: &dyn std::fmt::Display| err(ViewErrorKind::ViewSql, e.to_string());

    let mut current = match seed {
        Some(b) => b,
        None => empty_output_batch(spec).map_err(|e| sql_err(&e))?,
    };
    tables
        .register(view_name, &current)
        .map_err(|e| sql_err(&e))?;

    for _ in 0..MAX_FIXPOINT_ITERS {
        let next = execute_view_sql(tables.ctx, spec)
            .await
            .map_err(|e| sql_err(&e))?;
        let delta =
            differentiate(&spec.output_schema, Some(&current), &next).map_err(|e| sql_err(&e))?;
        if delta.is_empty() {
            return Ok(next);
        }
        tables.register(view_name, &next).map_err(|e| sql_err(&e))?;
        current = next;
    }

    // IVM-AUD-CORE-13: nothing checks that a recursive body is set-semantic
    // before running it, and nothing can — the body is opaque SQL. What is
    // knowable is why THIS body failed to converge, and the two causes need
    // opposite fixes, so say which one it is.
    let dups = duplicate_row_count(&current).unwrap_or(0);
    let rows = current.num_rows();
    let message = if dups > 0 {
        format!(
            "recursive view did not reach a fixed point in {MAX_FIXPOINT_ITERS} iterations; \
             its {rows}-row iterate carries {dups} duplicate rows, so the body is not \
             set-semantic — a UNION ALL recursion over a cyclic input grows without bound. \
             Write the body with UNION (or SELECT DISTINCT) so each derived row is produced \
             once. Nothing de-duplicates it for you."
        )
    } else {
        format!(
            "recursive view did not reach a fixed point in {MAX_FIXPOINT_ITERS} iterations; \
             the iterate reached {rows} distinct rows and was still growing, so the recursion \
             enumerates unboundedly many rows (an unbounded counter, or a join that widens \
             every round). Bound it in the body."
        )
    };
    Err(err(ViewErrorKind::FixpointNotConverged, message))
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

fn read_u64(bytes: &[u8], pos: &mut usize) -> IvmResult<u64> {
    let slice = bytes.get(*pos..*pos + 8).ok_or_else(slice_err)?;
    *pos += 8;
    let mut arr = [0u8; 8];
    arr.copy_from_slice(slice);
    Ok(u64::from_le_bytes(arr))
}

fn read_i64(bytes: &[u8], pos: &mut usize) -> IvmResult<i64> {
    Ok(read_u64(bytes, pos)? as i64)
}

fn write_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn write_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// `u32 len || name bytes`.
fn write_name(out: &mut Vec<u8>, name: &str) -> IvmResult<()> {
    write_u32_len(out, name.len(), "name")?;
    out.extend_from_slice(name.as_bytes());
    Ok(())
}

/// `u32 count || (u32 ipc_len || delta ipc)*`
fn write_delta_list(out: &mut Vec<u8>, deltas: &[DeltaBatch]) -> IvmResult<()> {
    write_u32_len(out, deltas.len(), "delta count")?;
    for d in deltas {
        let ipc = serialize_delta_batch(d).map_err(delta_err)?;
        write_u32_len(out, ipc.len(), "delta payload")?;
        out.extend_from_slice(&ipc);
    }
    Ok(())
}

fn read_delta_list(bytes: &[u8], pos: &mut usize) -> IvmResult<Vec<DeltaBatch>> {
    let n = read_u32(bytes, pos)? as usize;
    let mut out = Vec::with_capacity(bounded_capacity(n, bytes.len()));
    for _ in 0..n {
        let len = read_u32(bytes, pos)? as usize;
        let data = bytes.get(*pos..*pos + len).ok_or_else(slice_err)?;
        *pos += len;
        out.push(deserialize_delta_batch(data).map_err(delta_err)?);
    }
    Ok(out)
}

/// Append a `u32` little-endian length prefix, refusing anything that does not
/// fit.
///
/// IVM-AUD-CORE-30: every length in these frames used to be written as
/// `(len as u32)`, which does not fail above 4 GiB — it truncates. A 4 GiB + 1
/// byte payload wrote the length `1`, the reader took one byte and then read
/// the rest of the payload as the next frame's fields: a corrupt blob that
/// decodes without error into wrong state. `as` casts cannot be trusted at a
/// format boundary, so the boundary refuses instead.
fn write_u32_len(out: &mut Vec<u8>, len: usize, what: &str) -> IvmResult<()> {
    let n = u32::try_from(len).map_err(|_| {
        IvmError::execution(format!(
            "{what} is {len} bytes, past the {} the checkpoint frame can address; \
             the frame uses u32 length prefixes",
            u32::MAX
        ))
    })?;
    out.extend_from_slice(&n.to_le_bytes());
    Ok(())
}

fn delta_err(e: DeltaError) -> IvmError {
    IvmError::execution(e.to_string())
}

fn lock_err<T>(_: T) -> IvmError {
    IvmError::execution("incremental flow lock poisoned")
}

// ── Exact-state section of `checkpoint_full` (IVM-AUD-CORE-27) ───────────────
//
// `checkpoint_full` used to capture source snapshots, view snapshot/baseline
// pairs and operator accumulators — and nothing else. Everything else the flow
// holds was silently reset by a restore:
//
//   * `tick` went back to 0, so `view_output_tick` and every fence built on the
//     tick counter went backwards after a coordinator restart;
//   * `pending` — deltas already accepted (`/feed` answered 200) but not yet
//     stepped — was **dropped**;
//   * `streaming_prev_snapshots` was lost, so the next `feed_snapshot` had no
//     previous snapshot to differentiate against and re-inserted the ENTIRE
//     snapshot as insertions, double-counting every row still present;
//   * `source_ordinals` was lost, so `feed_if_advanced` no longer recognised an
//     offset it had already processed and re-applied it;
//   * watermarks reset to `i64::MIN`, so a LATENESS bound stopped dropping late
//     rows until it re-observed a high-water mark;
//   * the delta-checkpoint accumulator, the per-view counters and the drop/
//     collapse counters all reset to zero.
//
// The section below carries all of it. It is appended AFTER the plan-state
// section behind a magic tag, so: a blob written before this exists has no tag
// and restores exactly as it used to (the fields above keep whatever the live
// flow had); and a binary predating this reads a new blob up to the plan-state
// section and ignores the rest. Neither direction needs a version bump.
const EXACT_STATE_MAGIC: &[u8; 5] = b"IVMF2";

/// Flow state that is neither a source snapshot, a view baseline nor an
/// operator accumulator, captured so a restore reproduces the flow exactly.
struct ExactState {
    tick: u64,
    pending: HashMap<String, Vec<DeltaBatch>>,
    streaming_prev_snapshots: HashMap<String, RecordBatch>,
    source_ordinals: Vec<(String, Vec<u8>)>,
    watermarks: Vec<(String, LatenessSpec, i64)>,
    checkpoint_deltas: HashMap<String, Vec<DeltaBatch>>,
    view_delta_stats: Vec<(String, ViewDeltaStats)>,
    late_dropped_rows: Vec<(String, u64)>,
    delta_restore_collapsed_rows: Vec<(String, u64)>,
    input_dedup_enabled: bool,
    delta_checkpoint_enabled: bool,
    force_diff_based: bool,
    rebuild_all_views: bool,
    /// Per source, the retained row hashes in FIFO (eviction) order.
    ///
    /// Written only when dedup is enabled. It is the one part of this section
    /// that can be large — `DEDUP_SEEN_CAPACITY` is 10 M hashes, i.e. up to
    /// 80 MB per source — and it is here because dropping it silently breaks
    /// the at-most-once guarantee the caller opted into: after a restore every
    /// row already applied would be admitted again.
    dedup_hashes: Vec<(String, Vec<u64>)>,
}

fn encode_exact_state(out: &mut Vec<u8>, inner: &IncrementalFlowInner) -> IvmResult<()> {
    out.extend_from_slice(EXACT_STATE_MAGIC);
    write_u64(out, inner.tick);

    write_u32_len(out, inner.pending.len(), "pending source count")?;
    for (name, deltas) in &inner.pending {
        write_name(out, name)?;
        write_delta_list(out, deltas)?;
    }

    write_u32_len(
        out,
        inner.streaming_prev_snapshots.len(),
        "streaming snapshot count",
    )?;
    for (name, batch) in &inner.streaming_prev_snapshots {
        encode_named_batch(out, name, batch)?;
    }

    write_u32_len(out, inner.source_ordinals.len(), "ordinal count")?;
    for (name, ordinal) in &inner.source_ordinals {
        write_name(out, name)?;
        write_u32_len(out, ordinal.len(), "ordinal")?;
        out.extend_from_slice(ordinal);
    }

    write_u32_len(out, inner.watermark_trackers.len(), "watermark count")?;
    for (name, tracker) in &inner.watermark_trackers {
        write_name(out, name)?;
        write_name(out, tracker.lateness_column())?;
        write_i64(out, tracker.lateness_ms());
        write_i64(out, tracker.max_observed_ts());
    }

    write_u32_len(
        out,
        inner.checkpoint_deltas.len(),
        "delta accumulator count",
    )?;
    for (name, deltas) in &inner.checkpoint_deltas {
        write_name(out, name)?;
        write_delta_list(out, deltas)?;
    }

    write_u32_len(out, inner.view_delta_stats.len(), "view stats count")?;
    for (name, stats) in &inner.view_delta_stats {
        write_name(out, name)?;
        write_u64(out, stats.rows_inserted_total);
        write_u64(out, stats.rows_retracted_total);
        write_u64(out, stats.last_tick_inserts);
        write_u64(out, stats.last_tick_retracts);
    }

    write_u32_len(out, inner.late_dropped_rows.len(), "late-drop count")?;
    for (name, n) in &inner.late_dropped_rows {
        write_name(out, name)?;
        write_u64(out, *n);
    }

    write_u32_len(
        out,
        inner.delta_restore_collapsed_rows.len(),
        "collapse count",
    )?;
    for (name, n) in &inner.delta_restore_collapsed_rows {
        write_name(out, name)?;
        write_u64(out, *n);
    }

    let flags = (inner.input_dedup_enabled as u8)
        | ((inner.delta_checkpoint_enabled as u8) << 1)
        | ((inner.force_diff_based as u8) << 2)
        | ((inner.rebuild_all_views as u8) << 3);
    out.push(flags);

    // Dedup hashes, in eviction order, only when the feature is on.
    let dedup: Vec<(&String, &VecDeque<u64>)> = if inner.input_dedup_enabled {
        inner
            .seen_input_hashes
            .iter()
            .map(|(name, (order, _))| (name, order))
            .collect()
    } else {
        Vec::new()
    };
    write_u32_len(out, dedup.len(), "dedup source count")?;
    for (name, order) in dedup {
        write_name(out, name)?;
        write_u32_len(out, order.len(), "dedup hash count")?;
        for h in order {
            write_u64(out, *h);
        }
    }
    Ok(())
}

/// Read the exact-state section if this blob has one.
///
/// Returns `Ok(None)` for a blob written before the section existed, which is
/// how a restore of an old checkpoint keeps its old (lossy) behaviour instead
/// of failing.
fn decode_exact_state(bytes: &[u8], pos: &mut usize) -> IvmResult<Option<ExactState>> {
    match bytes.get(*pos..*pos + EXACT_STATE_MAGIC.len()) {
        Some(tag) if tag == EXACT_STATE_MAGIC.as_slice() => {}
        _ => return Ok(None),
    }
    *pos += EXACT_STATE_MAGIC.len();

    let tick = read_u64(bytes, pos)?;

    let n = read_u32(bytes, pos)? as usize;
    let mut pending = HashMap::with_capacity(bounded_capacity(n, bytes.len()));
    for _ in 0..n {
        let name = decode_name(bytes, pos)?;
        pending.insert(name, read_delta_list(bytes, pos)?);
    }

    let n = read_u32(bytes, pos)? as usize;
    let mut streaming_prev_snapshots = HashMap::with_capacity(bounded_capacity(n, bytes.len()));
    for _ in 0..n {
        let (name, batch) = decode_named_batch(bytes, pos)?;
        streaming_prev_snapshots.insert(name, batch);
    }

    let n = read_u32(bytes, pos)? as usize;
    let mut source_ordinals = Vec::with_capacity(bounded_capacity(n, bytes.len()));
    for _ in 0..n {
        let name = decode_name(bytes, pos)?;
        let len = read_u32(bytes, pos)? as usize;
        let ordinal = bytes.get(*pos..*pos + len).ok_or_else(slice_err)?.to_vec();
        *pos += len;
        source_ordinals.push((name, ordinal));
    }

    let n = read_u32(bytes, pos)? as usize;
    let mut watermarks = Vec::with_capacity(bounded_capacity(n, bytes.len()));
    for _ in 0..n {
        let name = decode_name(bytes, pos)?;
        let column = decode_name(bytes, pos)?;
        let lateness_ms = read_i64(bytes, pos)?;
        let max_observed_ts = read_i64(bytes, pos)?;
        watermarks.push((
            name,
            LatenessSpec::new(column, lateness_ms),
            max_observed_ts,
        ));
    }

    let n = read_u32(bytes, pos)? as usize;
    let mut checkpoint_deltas = HashMap::with_capacity(bounded_capacity(n, bytes.len()));
    for _ in 0..n {
        let name = decode_name(bytes, pos)?;
        checkpoint_deltas.insert(name, read_delta_list(bytes, pos)?);
    }

    let n = read_u32(bytes, pos)? as usize;
    let mut view_delta_stats = Vec::with_capacity(bounded_capacity(n, bytes.len()));
    for _ in 0..n {
        let name = decode_name(bytes, pos)?;
        view_delta_stats.push((
            name,
            ViewDeltaStats {
                rows_inserted_total: read_u64(bytes, pos)?,
                rows_retracted_total: read_u64(bytes, pos)?,
                last_tick_inserts: read_u64(bytes, pos)?,
                last_tick_retracts: read_u64(bytes, pos)?,
            },
        ));
    }

    let n = read_u32(bytes, pos)? as usize;
    let mut late_dropped_rows = Vec::with_capacity(bounded_capacity(n, bytes.len()));
    for _ in 0..n {
        let name = decode_name(bytes, pos)?;
        late_dropped_rows.push((name, read_u64(bytes, pos)?));
    }

    let n = read_u32(bytes, pos)? as usize;
    let mut delta_restore_collapsed_rows = Vec::with_capacity(bounded_capacity(n, bytes.len()));
    for _ in 0..n {
        let name = decode_name(bytes, pos)?;
        delta_restore_collapsed_rows.push((name, read_u64(bytes, pos)?));
    }

    let flags = *bytes.get(*pos).ok_or_else(slice_err)?;
    *pos += 1;

    let n = read_u32(bytes, pos)? as usize;
    let mut dedup_hashes = Vec::with_capacity(bounded_capacity(n, bytes.len()));
    for _ in 0..n {
        let name = decode_name(bytes, pos)?;
        let count = read_u32(bytes, pos)? as usize;
        // Each hash is 8 bytes, so a blob of `len` bytes cannot hold more than
        // `len / 8` of them — a corrupt count must not become a huge alloc.
        let mut hashes = Vec::with_capacity(count.min(bytes.len() / 8));
        for _ in 0..count {
            hashes.push(read_u64(bytes, pos)?);
        }
        dedup_hashes.push((name, hashes));
    }

    Ok(Some(ExactState {
        tick,
        pending,
        streaming_prev_snapshots,
        source_ordinals,
        watermarks,
        checkpoint_deltas,
        view_delta_stats,
        late_dropped_rows,
        delta_restore_collapsed_rows,
        input_dedup_enabled: flags & 1 != 0,
        delta_checkpoint_enabled: flags & 2 != 0,
        force_diff_based: flags & 4 != 0,
        rebuild_all_views: flags & 8 != 0,
        dedup_hashes,
    }))
}

fn apply_exact_state(inner: &mut IncrementalFlowInner, exact: ExactState) {
    inner.tick = exact.tick;
    inner.pending = exact.pending;
    inner.streaming_prev_snapshots = exact.streaming_prev_snapshots;
    inner.source_ordinals = exact.source_ordinals.into_iter().collect();
    inner.watermark_trackers = exact
        .watermarks
        .into_iter()
        .map(|(name, spec, max_observed_ts)| {
            let mut tracker = WatermarkTracker::new(spec);
            tracker.observe(max_observed_ts);
            (name, tracker)
        })
        .collect();
    inner.checkpoint_deltas = exact.checkpoint_deltas;
    inner.view_delta_stats = exact.view_delta_stats.into_iter().collect();
    inner.late_dropped_rows = exact.late_dropped_rows.into_iter().collect();
    inner.delta_restore_collapsed_rows = exact.delta_restore_collapsed_rows.into_iter().collect();
    inner.input_dedup_enabled = exact.input_dedup_enabled;
    inner.delta_checkpoint_enabled = exact.delta_checkpoint_enabled;
    inner.force_diff_based = exact.force_diff_based;
    inner.rebuild_all_views = exact.rebuild_all_views;
    inner.seen_input_hashes = exact
        .dedup_hashes
        .into_iter()
        .map(|(name, hashes)| {
            let set: AHashSet<u64> = hashes.iter().copied().collect();
            (name, (VecDeque::from(hashes), set))
        })
        .collect();
}

// ── RecordBatch framing helpers (for checkpoint_full / restore_full) ──────────

/// Encode `name` + a required `RecordBatch` as Arrow IPC into `out`.
fn encode_named_batch(out: &mut Vec<u8>, name: &str, batch: &RecordBatch) -> IvmResult<()> {
    write_u32_len(out, name.len(), "entry name")?;
    out.extend_from_slice(name.as_bytes());
    let ipc = encode_record_batch_ipc(batch)?;
    write_u32_len(out, ipc.len(), "entry payload")?;
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
    write_u32_len(&mut out, map.len(), "view count")?;
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
    write_u32_len(&mut out, map.len(), "view count")?;
    for (name, delta) in map {
        let ipc = serialize_delta_batch(delta).map_err(delta_err)?;
        write_u32_len(&mut out, name.len(), "view name")?;
        out.extend_from_slice(name.as_bytes());
        write_u32_len(&mut out, ipc.len(), "output delta")?;
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
/// **No production caller** (IVM-AUD-INT-F20). The coordinator's resident
/// dispatch encodes [`encode_ivm_attach_fragment`] + [`encode_ivm_tick_fragment`]
/// instead; the only callers of this function, of
/// [`encode_ivm_ckpt_fragment`], and of the executor's `execute_ivm_fragment`
/// that decodes them are tests. The receiving half is still wired into the
/// executor's task runner, so a `delta:step:` fragment WOULD execute if one were
/// ever sent — nothing sends one. Kept, unremoved, only because deleting it
/// means deleting the executor half too; do not read its existence as evidence
/// that the stateless dispatch path is in use.
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

/// Base64 of JSON of base64 of Arrow IPC.
///
/// IVM-AUD-INT-F19: this is the `delta:tick:` payload — the wire whose stated
/// purpose is "O(Δ)". Each delta is base64'd (×4/3), embedded in a JSON array
/// (a per-entry string quote/escape pass plus the field names), and the whole
/// array is base64'd again (×4/3): ≈1.78× the IPC bytes, plus one full buffer
/// copy per layer. [`encode_delta_map`] already frames the same map in binary
/// at ×1.0, and [`decode_delta_map`] already reads it — the `delta:tick:`
/// fragment could carry `b64(encode_delta_map(..))` for ×1.33 and one copy.
/// Not changed here because the decoder lives in `krishiv-executor` and both
/// ends must move together, which also means a mixed-version cluster needs a
/// negotiated cutover.
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
///
/// **No production caller** (IVM-AUD-INT-F20) — see
/// [`encode_ivm_step_fragment`].
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

    use super::DrainedPending;

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

    // ── tick custody of drained deltas (IVM-AUD-PART-1) ───────────────────────

    /// A tick whose future is dropped mid-flight must return its drained
    /// inputs to `pending`, or those rows are lost forever with only a failed
    /// step to show for it. This is how a 300 s coordinator timeout, and a
    /// sibling shard's error under `try_join_all`, used to permanently
    /// undercount exactly the keys the cancelled shard owned.
    ///
    /// Revert-proof: delete the `custody.commit()`/`DrainedPending::new`
    /// pairing (or make Drop a no-op) and the post-cancellation step observes
    /// zero rows instead of the fed rows.
    /// A tick that fails must return its drained inputs to `pending`, or
    /// those rows are lost forever with only a failed step to show for it.
    /// The same `Drop` path also covers cancellation — a dropped future (a
    /// coordinator timeout, or `try_join_all` dropping sibling shards on the
    /// first error) never reaches `commit()` either.
    ///
    /// Revert-proof: delete the `DrainedPending::new(...)` custody line and
    /// the surviving-rows assertion fails — the deltas are gone after the
    /// failed step, which is the silent undercount this defends.
    #[tokio::test]
    async fn a_failed_tick_returns_its_drained_deltas_to_pending() {
        use arrow::array::StringArray;

        let flow = IncrementalFlow::new();
        flow.feed(
            "s",
            DeltaBatch::from_inserts(make_batch(&[1, 2, 3])).unwrap(),
        )
        .unwrap();
        // A second batch for the same source with an incompatible schema makes
        // the tick's `coalesce_pending` fail — a real error path, not a stub.
        let odd_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let odd =
            RecordBatch::try_new(odd_schema, vec![Arc::new(StringArray::from(vec!["x"]))]).unwrap();
        flow.feed("s", DeltaBatch::from_inserts(odd).unwrap())
            .unwrap();

        assert!(
            flow.step_datafusion().await.is_err(),
            "mixed-schema deltas for one source must fail the tick"
        );

        let inner = flow.inner.lock().unwrap();
        let queue = inner
            .pending
            .get("s")
            .expect("a failed tick must return its drained deltas to pending");
        assert_eq!(
            queue.len(),
            2,
            "both drained deltas must be reclaimed, not eaten by the failed tick"
        );
    }

    /// Restoration must preserve arrival order: reclaimed (older) deltas go
    /// ahead of anything fed while the failed attempt was in flight
    /// (IVM-AUD-DIST-B2).
    #[tokio::test]
    async fn restored_deltas_precede_deltas_fed_during_the_failed_attempt() {
        let flow = IncrementalFlow::new();
        let drained: HashMap<String, Vec<DeltaBatch>> = HashMap::from([(
            "s".to_string(),
            vec![DeltaBatch::from_inserts(make_batch(&[1])).unwrap()],
        )]);
        // Simulate the in-flight window: something else feeds while the tick
        // holds custody.
        flow.feed("s", DeltaBatch::from_inserts(make_batch(&[2])).unwrap())
            .unwrap();
        drop(DrainedPending::new(Arc::clone(&flow.inner), drained));

        let inner = flow.inner.lock().unwrap();
        let queue = inner.pending.get("s").expect("pending queue for s");
        assert_eq!(queue.len(), 2);
        assert_eq!(
            queue[0].data_batch().num_rows(),
            1,
            "the reclaimed delta must be replayed first"
        );
    }

    // ── feed target validation (IVM-AUD-API-F1) ───────────────────────────────

    /// A typo'd source name used to be accepted, tick the clock, and lose the
    /// data forever: `feed` put it in `pending` under an unreferenced key and
    /// the tick drained `pending` wholesale.
    ///
    /// Revert-proof: delete the `validate_feed_target(&inner, &source_name)?`
    /// line from `feed` and the first assertion fails (the typo is accepted),
    /// which is exactly the silent-loss behaviour.
    #[tokio::test]
    async fn feeding_a_source_no_view_reads_is_rejected_not_silently_dropped() {
        use arrow::datatypes::{DataType, Field, Schema};

        let flow = IncrementalFlow::new();
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "total",
            DataType::Int64,
            true,
        )]));
        flow.register_view(krishiv_delta::IncrementalViewSpec {
            name: "totals".into(),
            body_sql: "SELECT SUM(value) AS total FROM orders".into(),
            output_schema,
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        })
        .unwrap();

        let delta = DeltaBatch::from_inserts(make_batch(&[1, 2])).unwrap();
        let err = flow
            .feed("order", delta)
            .expect_err("a source no view reads must be rejected, not dropped");
        let msg = err.to_string();
        assert!(
            msg.contains("no registered view reads source 'order'"),
            "the error must name the offending source: {msg}"
        );
        assert!(
            msg.contains("orders"),
            "the error must list the real sources so the typo is obvious: {msg}"
        );

        // The real name is accepted, and case does not matter (DataFusion
        // lowercases unquoted identifiers — IVM-AUD-CORE-24).
        flow.feed(
            "orders",
            DeltaBatch::from_inserts(make_batch(&[3])).unwrap(),
        )
        .unwrap();
        flow.feed(
            "ORDERS",
            DeltaBatch::from_inserts(make_batch(&[4])).unwrap(),
        )
        .unwrap();
    }

    /// Pre-registration feeds stay legal: with no views there is nothing to
    /// validate against, so the guard must not break the feed-then-register
    /// ordering.
    #[tokio::test]
    async fn feeding_before_any_view_is_registered_is_still_allowed() {
        let flow = IncrementalFlow::new();
        flow.feed(
            "anything",
            DeltaBatch::from_inserts(make_batch(&[7])).unwrap(),
        )
        .unwrap();
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

        // IVM-AUD-CORE-7/CORE-8: the old version of this test stopped here —
        // it asserted only `watermark_for`, a getter over the setter it had
        // just called, so it could not observe that NOTHING enforced the
        // bound. `WatermarkTracker::is_late` had zero production callers and a
        // record three days late mutated the view exactly like an on-time one.
        // These assertions are what that test was missing.
        // Materialize what has been fed so far, so `before_rows` is the real
        // snapshot size rather than `None` (source snapshots only advance on a
        // step).
        flow.step_datafusion().await.unwrap();
        let before_rows = flow
            .source_snapshot("events")
            .unwrap()
            .map(|b| b.num_rows())
            .unwrap_or(0);
        assert_eq!(before_rows, 3, "three on-time rows were fed");

        // ts 5_000 is far below the 19_000 watermark: dropped at ingestion.
        flow.feed("events", batch(&[5_000], &[99.0])).unwrap();
        assert_eq!(
            flow.late_dropped_rows("events").unwrap(),
            1,
            "a late insertion must be dropped by the declared LATENESS bound"
        );
        flow.step_datafusion().await.unwrap();
        assert_eq!(
            flow.source_snapshot("events")
                .unwrap()
                .map(|b| b.num_rows())
                .unwrap_or(0),
            before_rows,
            "the dropped row must not reach the source snapshot"
        );

        // A late RETRACTION is never dropped: stranding its insertion would
        // stop the view from ever converging.
        let retraction = DeltaBatch::from_deletes(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![10_000_i64])) as Arc<dyn Array>,
                    Arc::new(Float64Array::from(vec![1.0_f64])) as Arc<dyn Array>,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        flow.feed("events", retraction).unwrap();
        assert_eq!(
            flow.late_dropped_rows("events").unwrap(),
            1,
            "the retraction must NOT be counted as dropped"
        );
        flow.step_datafusion().await.unwrap();
        assert_eq!(
            flow.source_snapshot("events")
                .unwrap()
                .map(|b| b.num_rows())
                .unwrap_or(0),
            before_rows - 1,
            "the late retraction must have been applied"
        );

        // The watermark stays monotonic: an older batch neither moves it back
        // nor survives the bound (it is dropped, raising the drop counter).
        flow.feed("events", batch(&[5_000], &[4.0])).unwrap();
        assert_eq!(
            flow.watermark_for("events").unwrap(),
            19_000,
            "watermark must be monotonic"
        );
        assert_eq!(flow.late_dropped_rows("events").unwrap(), 2);
    }

    /// IVM-AUD-CORE-9: a join view has TWO source dependencies, and the old
    /// register-time association skipped any view without exactly one — so the
    /// only shape whose state GC actually matters never got a watermark at
    /// all. Association now happens at feed time by schema membership, so each
    /// side gets its own.
    ///
    /// Revert-proof: restrict tracker creation to single-dep views again and
    /// both `watermark_for` assertions return `i64::MIN`.
    #[tokio::test]
    async fn a_join_view_gets_a_watermark_per_side() {
        use arrow::array::{Array, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use krishiv_delta::{DeltaBatch, LatenessSpec};

        let flow = IncrementalFlow::new();
        let left = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let right = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        flow.register_view(krishiv_delta::IncrementalViewSpec {
            name: "joined".into(),
            body_sql: "SELECT o.id, o.ts FROM orders o JOIN shipments s ON o.id = s.id".into(),
            output_schema: left.clone(),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![LatenessSpec::new("ts", 1_000)],
        })
        .unwrap();

        let row = |schema: &Arc<Schema>, id: i64, ts: i64| {
            DeltaBatch::from_inserts(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![id])) as Arc<dyn Array>,
                        Arc::new(Int64Array::from(vec![ts])) as Arc<dyn Array>,
                    ],
                )
                .unwrap(),
            )
            .unwrap()
        };
        flow.feed("orders", row(&left, 1, 50_000)).unwrap();
        flow.feed("shipments", row(&right, 1, 40_000)).unwrap();

        assert_eq!(
            flow.watermark_for("orders").unwrap(),
            49_000,
            "the left side of a join must get its own watermark"
        );
        assert_eq!(
            flow.watermark_for("shipments").unwrap(),
            39_000,
            "the right side of a join must get its own watermark"
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

    /// IVM-AUD-CORE-23. The aggregate planner zipped `aggr_expr` (SELECT
    /// order) against the declared schema's non-group columns (schema order),
    /// so a view whose declared schema lists its aggregates in the other order
    /// computed SUM into the COUNT column and COUNT into the SUM column — with
    /// the arity check still passing, so nothing complained.
    #[tokio::test]
    async fn aggregates_follow_their_names_not_their_positions() {
        let flow = IncrementalFlow::new();
        // SELECT order: (SUM, COUNT). Declared order: (cnt, total).
        flow.register_view(krishiv_delta::IncrementalViewSpec {
            name: "by_region".into(),
            body_sql: "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt \
                       FROM sales GROUP BY region"
                .into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("cnt", DataType::Int64, true),
                Field::new("total", DataType::Float64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        })
        .unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("amount", DataType::Float64, true),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["us", "us", "us"])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
            ],
        )
        .unwrap();
        flow.feed("sales", DeltaBatch::from_inserts(batch).unwrap())
            .unwrap();
        flow.step_datafusion().await.unwrap();

        let snap = flow
            .snapshot("by_region")
            .unwrap()
            .expect("materialized snapshot must exist");
        let cnt = snap
            .column_by_name("cnt")
            .expect("cnt column")
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("cnt is Int64")
            .value(0);
        let total = snap
            .column_by_name("total")
            .expect("total column")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("total is Float64")
            .value(0);
        assert_eq!(cnt, 3, "COUNT(*) must land in `cnt`, not in `total`");
        assert!(
            (total - 60.0).abs() < 1e-9,
            "SUM(amount) must land in `total`, got {total}"
        );
        // Correct *and* still incremental. Degrading this shape to DiffBased
        // would also produce the right numbers, so without this the test
        // cannot tell the name-pairing fix from a full-recompute fallback.
        let (incremental, how) = flow
            .view_plan_classification("by_region")
            .unwrap()
            .expect("the view is registered");
        assert!(
            incremental,
            "the view must keep its O(delta) plan, not fall back to DiffBased: {how}"
        );
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

    // ── IVM-AUD-CORE-10/11/12/13: recursive-view fixpoint ────────────────────
    //
    // Before these, `DECLARE RECURSIVE VIEW` was a silent no-op end to end: the
    // tick registers a table for a view only when it already has a non-empty
    // output, so the body's self-reference resolved to nothing, the SQL failed
    // with "table not found", the failure was swallowed into an empty batch,
    // and the empty batch diffed to nothing against an empty baseline. The
    // step summary said `errored_views: []`.

    mod recursive_views {
        use super::*;
        use krishiv_delta::IncrementalViewSpec;

        use crate::flow::{ViewErrorKind, ViewExecution};

        fn edge_batch(srcs: &[i32], dsts: &[i32]) -> RecordBatch {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("src", DataType::Int32, false),
                    Field::new("dst", DataType::Int32, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(srcs.to_vec())) as arrow::array::ArrayRef,
                    Arc::new(Int32Array::from(dsts.to_vec())),
                ],
            )
            .unwrap()
        }

        fn reach_spec(body: &str) -> IncrementalViewSpec {
            IncrementalViewSpec {
                name: "reach".into(),
                body_sql: body.into(),
                output_schema: Arc::new(Schema::new(vec![
                    Field::new("src", DataType::Int32, true),
                    Field::new("dst", DataType::Int32, true),
                ])),
                is_materialized: true,
                is_recursive: true,
                lateness: vec![],
            }
        }

        /// `reach` as a set of (src, dst) pairs.
        fn pairs(flow: &IncrementalFlow) -> Vec<(i32, i32)> {
            let Some(snap) = flow.snapshot("reach").unwrap() else {
                return Vec::new();
            };
            let src = snap
                .column_by_name("src")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let dst = snap
                .column_by_name("dst")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let mut out: Vec<(i32, i32)> = (0..snap.num_rows())
                .map(|i| (src.value(i), dst.value(i)))
                .collect();
            out.sort_unstable();
            out.dedup();
            out
        }

        const SET_BODY: &str = "SELECT src, dst FROM edges \
             UNION SELECT e.src, r.dst FROM edges e JOIN reach r ON e.dst = r.src";
        const MULTISET_BODY: &str = "SELECT src, dst FROM edges \
             UNION ALL SELECT e.src, r.dst FROM edges e JOIN reach r ON e.dst = r.src";

        /// The whole feature, end to end: a set-semantic body over an acyclic
        /// graph reaches its transitive closure. This produced ZERO rows before
        /// the self-reference was seeded — `snapshot()` was `None`.
        #[tokio::test]
        async fn a_recursive_view_reaches_its_transitive_closure() {
            let flow = IncrementalFlow::new();
            flow.register_view(reach_spec(SET_BODY)).unwrap();
            flow.feed(
                "edges",
                DeltaBatch::from_inserts(edge_batch(&[1, 2], &[2, 3])).unwrap(),
            )
            .unwrap();
            let summary = flow.step_datafusion().await.unwrap();
            assert_eq!(
                summary.errored_views,
                Vec::new(),
                "a converging recursive view must not report an error"
            );
            assert_eq!(
                pairs(&flow),
                vec![(1, 2), (1, 3), (2, 3)],
                "1→2→3 closes over 1→3"
            );
        }

        /// IVM-AUD-CORE-12: a body that cannot converge must NOT have its last
        /// iterate published as the view's value. The cap is a guard, not a
        /// silent truncation — and the view keeps the fixed point it last had.
        #[tokio::test]
        async fn a_diverging_recursive_view_keeps_its_last_fixed_point_and_reports() {
            let flow = IncrementalFlow::new();
            flow.register_view(reach_spec(MULTISET_BODY)).unwrap();

            // Tick 1: acyclic, so even the UNION ALL body converges.
            flow.feed(
                "edges",
                DeltaBatch::from_inserts(edge_batch(&[1], &[2])).unwrap(),
            )
            .unwrap();
            let first = flow.step_datafusion().await.unwrap();
            assert_eq!(first.errored_views, Vec::new());
            assert_eq!(pairs(&flow), vec![(1, 2)]);

            // Tick 2: close the cycle. `UNION ALL` re-derives every round, so
            // the iterate grows forever.
            flow.feed(
                "edges",
                DeltaBatch::from_inserts(edge_batch(&[2], &[1])).unwrap(),
            )
            .unwrap();
            let second = flow.step_datafusion().await.unwrap();

            let e = second
                .errored_views
                .iter()
                .find(|e| e.view == "reach")
                .expect("divergence must be reported in the step summary");
            assert_eq!(e.kind, ViewErrorKind::FixpointNotConverged);
            assert!(
                e.message.contains("not set-semantic"),
                "the message must name the cause: {}",
                e.message
            );
            // The view is unchanged — NOT the 100th iterate, which is a wrong
            // answer that looks like a right one.
            assert_eq!(
                pairs(&flow),
                vec![(1, 2)],
                "a non-converged iterate must never be published as the view's value"
            );
        }

        /// IVM-AUD-CORE-11: the recursive branch recorded no `ViewError` where
        /// the non-recursive branch does, so a recursive view whose SQL cannot
        /// run reported a clean tick.
        #[tokio::test]
        async fn a_recursive_view_whose_sql_fails_is_reported() {
            let flow = IncrementalFlow::new();
            flow.register_view(reach_spec(
                "SELECT src, dst FROM edges \
                 UNION SELECT src, dst FROM no_such_table",
            ))
            .unwrap();
            flow.feed(
                "edges",
                DeltaBatch::from_inserts(edge_batch(&[1], &[2])).unwrap(),
            )
            .unwrap();
            let summary = flow.step_datafusion().await.unwrap();
            let e = summary
                .errored_views
                .iter()
                .find(|e| e.view == "reach")
                .expect("a failing recursive body must surface as a ViewError");
            assert_eq!(e.kind, ViewErrorKind::ViewSql);
            assert!(
                e.message.contains("no_such_table"),
                "the error must name what failed: {}",
                e.message
            );
        }

        /// A recursive view has no O(Δ) plan, so reporting it as "not yet
        /// planned — no tick has executed" after many ticks was false.
        #[tokio::test]
        async fn a_recursive_view_reports_how_it_actually_executes() {
            let flow = IncrementalFlow::new();
            flow.register_view(reach_spec(SET_BODY)).unwrap();
            flow.feed(
                "edges",
                DeltaBatch::from_inserts(edge_batch(&[1], &[2])).unwrap(),
            )
            .unwrap();
            flow.step_datafusion().await.unwrap();
            let (execution, reason) = flow.view_execution("reach").unwrap().unwrap();
            assert_eq!(execution, ViewExecution::DiffBased);
            assert!(reason.contains("fixed point"), "{reason}");
        }
    }

    // ── IVM-AUD-CORE-27: checkpoint_full / restore_full reproduce exact state ─

    mod checkpoint_fidelity {
        use super::*;
        use arrow::array::Int64Array;
        use krishiv_delta::{IncrementalViewSpec, LatenessSpec};

        fn ids_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
        }

        fn ids_view() -> IncrementalViewSpec {
            IncrementalViewSpec {
                name: "v".into(),
                body_sql: "SELECT id FROM src".into(),
                output_schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
                is_materialized: true,
                is_recursive: false,
                lateness: vec![],
            }
        }

        fn fresh_with_view() -> IncrementalFlow {
            let flow = IncrementalFlow::new();
            flow.register_view(ids_view()).unwrap();
            flow
        }

        /// The streaming→IVM bridge differentiates each snapshot against the
        /// previous one. `checkpoint_full` did not carry that previous
        /// snapshot, so the first `feed_snapshot` after a restore had nothing to
        /// diff against and re-inserted the whole snapshot: every row still
        /// present was counted twice.
        #[tokio::test]
        async fn a_restored_flow_does_not_re_insert_the_streaming_snapshot() {
            let flow = fresh_with_view();
            let snap = RecordBatch::try_new(
                ids_schema(),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            flow.feed_snapshot("src", std::slice::from_ref(&snap))
                .unwrap();
            flow.step_datafusion().await.unwrap();
            assert_eq!(flow.source_snapshot("src").unwrap().unwrap().num_rows(), 3);

            let blob = flow.checkpoint_full().unwrap();
            let restored = fresh_with_view();
            restored.restore_full(&blob).unwrap();

            // The same snapshot again is NO change.
            restored
                .feed_snapshot("src", std::slice::from_ref(&snap))
                .unwrap();
            restored.step_datafusion().await.unwrap();
            assert_eq!(
                restored.source_snapshot("src").unwrap().unwrap().num_rows(),
                3,
                "an unchanged streaming snapshot must produce no delta after a restore"
            );
        }

        /// Deltas already accepted but not yet stepped are part of the flow's
        /// state. Dropping them on restore loses input the caller was told had
        /// been accepted; resetting `tick` to 0 makes every tick-stamped read
        /// go backwards.
        #[tokio::test]
        async fn a_restored_flow_keeps_its_tick_and_its_un_stepped_deltas() {
            let flow = fresh_with_view();
            flow.feed(
                "src",
                DeltaBatch::from_inserts(
                    RecordBatch::try_new(ids_schema(), vec![Arc::new(Int64Array::from(vec![1]))])
                        .unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
            flow.step_datafusion().await.unwrap();
            let tick_before = flow.tick().unwrap();
            assert_eq!(tick_before, 1);

            // Accepted, not yet stepped.
            flow.feed(
                "src",
                DeltaBatch::from_inserts(
                    RecordBatch::try_new(ids_schema(), vec![Arc::new(Int64Array::from(vec![2]))])
                        .unwrap(),
                )
                .unwrap(),
            )
            .unwrap();

            let blob = flow.checkpoint_full().unwrap();
            let restored = fresh_with_view();
            restored.restore_full(&blob).unwrap();

            assert_eq!(
                restored.tick().unwrap(),
                tick_before,
                "the tick counter must not go backwards across a restore"
            );
            restored.step_datafusion().await.unwrap();
            assert_eq!(
                restored.source_snapshot("src").unwrap().unwrap().num_rows(),
                2,
                "the un-stepped delta must survive the checkpoint"
            );
        }

        /// Dedup, ordinals, watermarks and the per-view counters are all state
        /// a caller can observe and depend on; all of them reset to zero.
        #[tokio::test]
        async fn a_restored_flow_keeps_dedup_ordinals_watermarks_and_counters() {
            let flow = fresh_with_view();
            flow.enable_input_dedup().unwrap();
            // `id` is Int64, which the engine reads as epoch milliseconds —
            // the only shape a watermark can advance from.
            flow.register_lateness("src", LatenessSpec::new("id", 1))
                .unwrap();

            let row1 = || {
                DeltaBatch::from_inserts(
                    RecordBatch::try_new(ids_schema(), vec![Arc::new(Int64Array::from(vec![100]))])
                        .unwrap(),
                )
                .unwrap()
            };
            flow.feed_if_advanced("src", row1(), b"offset-1".to_vec())
                .unwrap();
            flow.step_datafusion().await.unwrap();
            let stats_before = flow.view_delta_stats("v").unwrap().unwrap();
            let watermark_before = flow.watermark_for("src").unwrap();
            assert!(stats_before.rows_inserted_total > 0);
            assert_ne!(watermark_before, i64::MIN);

            let blob = flow.checkpoint_full().unwrap();
            let restored = fresh_with_view();
            restored.restore_full(&blob).unwrap();

            assert_eq!(
                restored.watermark_for("src").unwrap(),
                watermark_before,
                "the LATENESS watermark must survive the restore"
            );
            assert_eq!(
                restored.view_delta_stats("v").unwrap().unwrap(),
                stats_before,
                "per-view counters must survive the restore"
            );

            // The same offset is still a no-op...
            restored
                .feed_if_advanced("src", row1(), b"offset-1".to_vec())
                .unwrap();
            // ...and the same row is still a duplicate even at a new offset.
            restored
                .feed_if_advanced("src", row1(), b"offset-2".to_vec())
                .unwrap();
            restored.step_datafusion().await.unwrap();
            assert_eq!(
                restored.source_snapshot("src").unwrap().unwrap().num_rows(),
                1,
                "a re-delivered row must still be deduped after a restore"
            );
        }

        /// A checkpoint written before the exact-state section existed has no
        /// magic tag; it must still restore, with the pre-existing (lossy)
        /// behaviour rather than an error.
        #[tokio::test]
        async fn a_checkpoint_without_the_exact_state_section_still_restores() {
            let flow = fresh_with_view();
            flow.feed(
                "src",
                DeltaBatch::from_inserts(
                    RecordBatch::try_new(ids_schema(), vec![Arc::new(Int64Array::from(vec![7]))])
                        .unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
            flow.step_datafusion().await.unwrap();
            let blob = flow.checkpoint_full().unwrap();

            // Cut the blob back to what the previous format produced.
            let cut = blob
                .windows(super::super::EXACT_STATE_MAGIC.len())
                .position(|w| w == super::super::EXACT_STATE_MAGIC.as_slice())
                .expect("a fresh checkpoint carries the exact-state section");
            let legacy = &blob[..cut];

            let restored = fresh_with_view();
            restored.restore_full(legacy).unwrap();
            assert_eq!(
                restored.source_snapshot("src").unwrap().unwrap().num_rows(),
                1
            );
            assert_eq!(restored.tick().unwrap(), 0, "an old blob carries no tick");
        }

        /// IVM-AUD-CORE-28: the view name is written twice per view entry and
        /// the second copy was read and thrown away. Corrupt the second copy
        /// and the restore must refuse rather than pair view A's snapshot with
        /// view B's baseline.
        #[tokio::test]
        async fn a_view_entry_whose_two_halves_disagree_is_refused() {
            let flow = fresh_with_view();
            flow.feed(
                "src",
                DeltaBatch::from_inserts(
                    RecordBatch::try_new(ids_schema(), vec![Arc::new(Int64Array::from(vec![1]))])
                        .unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
            flow.step_datafusion().await.unwrap();
            let mut blob = flow.checkpoint_full().unwrap();

            // The view is named "v"; both copies are the single byte b'v'.
            // Flip the SECOND one — the one the decoder used to discard.
            let occurrences: Vec<usize> = blob
                .iter()
                .enumerate()
                .filter(|(_, b)| **b == b'v')
                .map(|(i, _)| i)
                .collect();
            assert!(
                occurrences.len() >= 2,
                "expected the view name twice in the blob"
            );
            blob[occurrences[1]] = b'w';

            let restored = fresh_with_view();
            let err = restored
                .restore_full(&blob)
                .expect_err("a mismatched view-entry pair must be refused");
            assert!(
                err.to_string().contains("inconsistent"),
                "the error must say what is wrong: {err}"
            );
        }
    }

    // ── IVM-AUD-CORE-18 / 19 / 25 / 26 ───────────────────────────────────────

    mod state_discipline {
        use super::*;
        use arrow::array::{Int64Array, StringArray};
        use krishiv_delta::{IncrementalViewSpec, LatenessSpec};

        use crate::flow::ViewExecution;

        fn orders(ks: &[&str], vs: &[i64]) -> RecordBatch {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Utf8, false),
                    Field::new("v", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(ks.to_vec())) as arrow::array::ArrayRef,
                    Arc::new(Int64Array::from(vs.to_vec())),
                ],
            )
            .unwrap()
        }

        fn totals_spec(is_materialized: bool) -> IncrementalViewSpec {
            IncrementalViewSpec {
                name: "totals".into(),
                body_sql: "SELECT k, SUM(v) AS total FROM orders GROUP BY k".into(),
                output_schema: Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Utf8, true),
                    Field::new("total", DataType::Int64, true),
                ])),
                is_materialized,
                is_recursive: false,
                lateness: vec![],
            }
        }

        /// IVM-AUD-CORE-18: `publish_output` advanced the diff baseline only for
        /// materialized views, so a NON-materialized view maintained by an O(Δ)
        /// operator kept `full_output = None`. The first full recompute then
        /// diffed against nothing and re-emitted the whole view as insertions —
        /// every group counted twice downstream.
        #[tokio::test]
        async fn a_non_materialized_view_keeps_its_diff_baseline_across_a_recompute() {
            let flow = IncrementalFlow::new();
            flow.register_view(totals_spec(false)).unwrap();
            flow.feed(
                "orders",
                DeltaBatch::from_inserts(orders(&["a", "b"], &[10, 5])).unwrap(),
            )
            .unwrap();
            flow.step_datafusion().await.unwrap();
            // Without an O(Δ) plan this test proves nothing: the DiffBased path
            // advances the baseline through `diff_and_update` either way.
            assert_eq!(
                flow.view_execution("totals").unwrap().unwrap().0,
                ViewExecution::Incremental,
                "the first tick must have built an O(Δ) plan"
            );

            // Now make the next tick recompute in full.
            flow.force_diff_based().unwrap();
            flow.feed(
                "orders",
                DeltaBatch::from_inserts(orders(&["a"], &[1])).unwrap(),
            )
            .unwrap();
            let summary = flow.step_datafusion().await.unwrap();
            assert_eq!(
                (summary.total_inserted_rows, summary.total_retracted_rows),
                (1, 1),
                "the recompute must emit only the changed group (retract a=10, insert a=11); \
                 re-emitting the whole view means the baseline was lost"
            );
        }

        /// IVM-AUD-CORE-19: a tick reads the source snapshots under the lock,
        /// runs view SQL with the lock RELEASED, then assigns the snapshots back
        /// wholesale. A restore landing in that window used to be erased without
        /// a trace. The gate UDF below holds the tick inside its SQL phase so
        /// the interleaving is exact, not raced.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_restore_during_a_tick_aborts_the_tick_instead_of_being_erased() {
            use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
            use datafusion::prelude::SessionContext;

            // The body calls `gate`, so executing it parks the tick inside
            // Phase 4 — the unlocked window this test is about.
            let mut gated = totals_spec(true);
            gated.body_sql = "SELECT k, SUM(gate(v)) AS total FROM orders GROUP BY k".into();
            let flow = IncrementalFlow::new();
            flow.register_view(gated).unwrap();
            // Force SQL execution so the tick actually reaches the gate.
            flow.force_diff_based().unwrap();
            flow.feed(
                "orders",
                DeltaBatch::from_inserts(orders(&["a"], &[10])).unwrap(),
            )
            .unwrap();

            // A checkpoint of a DIFFERENT state, to restore mid-tick.
            let other = IncrementalFlow::new();
            other.register_view(totals_spec(true)).unwrap();
            other
                .feed(
                    "orders",
                    DeltaBatch::from_inserts(orders(&["z"], &[999])).unwrap(),
                )
                .unwrap();
            other.step_datafusion().await.unwrap();
            let blob = other.checkpoint_full().unwrap();

            let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let once = Arc::new(std::sync::Mutex::new(Some((entered_tx, release_rx))));
            let gate = create_udf(
                "gate",
                vec![DataType::Int64],
                DataType::Int64,
                Volatility::Volatile,
                Arc::new(move |args: &[ColumnarValue]| {
                    if let Some((entered, release)) = once.lock().expect("gate lock").take() {
                        let _ = entered.send(());
                        let _ = release.recv();
                    }
                    Ok(args[0].clone())
                }),
            );
            let ctx = SessionContext::new();
            ctx.register_udf(gate);

            let stepping = flow.clone();
            let tick = tokio::spawn(async move {
                let ctx = ctx;
                stepping.step_datafusion_with_ctx(&ctx).await
            });

            // The tick is now inside its unlocked SQL phase.
            entered_rx.recv().expect("the view SQL must reach the gate");
            flow.restore_full(&blob).unwrap();
            let _ = release_tx.send(());

            let result = tick.await.expect("the tick task must not panic");
            let err = result.expect_err(
                "a tick whose state was replaced underneath it must not commit its \
                 pre-restore snapshots",
            );
            assert!(err.to_string().contains("aborted"), "{err}");

            // The restore stands...
            let snap = flow.source_snapshot("orders").unwrap().unwrap();
            let ks = snap
                .column_by_name("k")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                ks.value(0),
                "z",
                "the restored source must survive the tick"
            );
            // ...and the aborted tick's input is back in the queue, not lost.
            assert_eq!(
                flow.take_pending().unwrap().get("orders").map(Vec::len),
                Some(1),
                "the aborted tick must return its drained deltas to pending"
            );
        }

        /// IVM-AUD-CORE-25: nine per-source and seven per-view maps, and no way
        /// to give any of them back. `drop_view` pruned four of the seven;
        /// there was no `drop_source` at all.
        #[tokio::test]
        async fn dropping_a_view_and_a_source_reclaims_every_map() {
            let flow = IncrementalFlow::new();
            flow.register_view(totals_spec(true)).unwrap();
            flow.enable_input_dedup().unwrap();
            flow.enable_delta_checkpoints().unwrap();
            flow.register_lateness("orders", LatenessSpec::new("v", 1))
                .unwrap();
            flow.feed_if_advanced(
                "orders",
                DeltaBatch::from_inserts(orders(&["a"], &[10])).unwrap(),
                b"off-1".to_vec(),
            )
            .unwrap();
            flow.step_datafusion().await.unwrap();
            flow.feed_snapshot("orders", &[orders(&["a"], &[10])])
                .unwrap();

            let before = flow.retained_state().unwrap();
            assert!(before.sources_with_snapshots > 0);
            assert!(before.sources_with_pending > 0);
            assert!(before.sources_with_dedup_hashes > 0);
            assert!(before.dedup_hashes_retained > 0);
            assert!(before.sources_with_checkpoint_deltas > 0);
            assert!(before.sources_with_streaming_snapshots > 0);
            assert!(before.sources_with_ordinals > 0);
            assert!(before.sources_with_watermarks > 0);
            assert!(before.views_with_plans > 0);
            assert!(before.views_with_stats > 0);

            assert!(flow.drop_view("totals").unwrap());
            assert!(flow.drop_source("orders").unwrap());

            assert_eq!(
                flow.retained_state().unwrap(),
                super::super::RetainedState::default(),
                "dropping the only view and the only source must leave nothing behind"
            );
        }

        /// IVM-AUD-CORE-26: the dedup set's capacity check ran once per `feed`,
        /// before any row was inserted, so one batch of N rows overshot the cap
        /// by N. Ten rows into a cap of four must leave four.
        #[test]
        fn the_dedup_set_is_bounded_per_row_not_per_feed() {
            let mut order: std::collections::VecDeque<u64> = Default::default();
            let mut set: ahash::AHashSet<u64> = Default::default();
            let batch =
                DeltaBatch::from_inserts(make_batch(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])).unwrap();
            let (kept, evicted) =
                super::super::dedup_filter(&mut order, &mut set, batch, 4, 2).unwrap();
            assert_eq!(kept.num_rows(), 10, "no row is a duplicate, so all pass");
            assert!(
                set.len() <= 4,
                "the retained-hash set must respect its cap; got {}",
                set.len()
            );
            assert_eq!(set.len(), order.len(), "queue and set must stay in step");
            assert!(
                evicted > 0,
                "reaching the cap must be reported to the caller"
            );
        }
    }
}
