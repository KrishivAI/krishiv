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
//! * **Streaming bridge**: `feed_stream_output` converts micro-batch output
//!   (all-positive `RecordBatch`es) into source `DeltaBatch`es.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use ahash::{AHashMap, AHashSet};
use arrow::array::{Array, RecordBatch};
use arrow::compute::cast;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use tokio::sync::watch;

use krishiv_delta::{
    DeltaBatch, DeltaError, IncrementalViewRegistry, IncrementalViewSpec, apply_delta,
    deserialize_delta_batch, differentiate, serialize_delta_batch,
};

use crate::error::{IvmError, IvmResult};

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
    input_dedup_enabled: bool,
    seen_input_hashes: AHashMap<String, AHashSet<u64>>,

    // Delta checkpoints: accumulate deltas since last checkpoint call.
    delta_checkpoint_enabled: bool,
    checkpoint_deltas: HashMap<String, Vec<DeltaBatch>>,

    // Streaming → IVM bridge: previous materialized snapshot per source.
    // Used by feed_stream_output to differentiate consecutive snapshots.
    streaming_prev_snapshots: HashMap<String, RecordBatch>,

    // Opt-in provenance tracking: input row hash → output row hashes.
    provenance: Option<crate::provenance::ProvenanceIndex>,
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
        Self {
            inner: Arc::new(Mutex::new(IncrementalFlowInner {
                view_registry: IncrementalViewRegistry::new(),
                pending: HashMap::new(),
                tick: 0,
                source_snapshots: HashMap::new(),
                input_dedup_enabled: false,
                seen_input_hashes: AHashMap::new(),
                delta_checkpoint_enabled: false,
                checkpoint_deltas: HashMap::new(),
                streaming_prev_snapshots: HashMap::new(),
                provenance: None,
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
    pub fn query_provenance(
        &self,
        input_hash: u64,
    ) -> IvmResult<Option<AHashSet<u64>>> {
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
    /// Re-registering resets the `full_output` baseline so the next tick
    /// treats the full SQL result as insertions (behavior-version invalidation).
    pub fn register_view(&self, spec: IncrementalViewSpec) -> IvmResult<()> {
        let inner = self.inner.lock().map_err(lock_err)?;
        if let Ok(old_view) = inner.view_registry.get(&spec.name) {
            let _ = old_view.reset_full_output();
        }
        inner.view_registry.register(spec).map_err(delta_err)
    }

    pub fn drop_view(&self, name: &str) -> IvmResult<bool> {
        let inner = self.inner.lock().map_err(lock_err)?;
        inner.view_registry.drop_view(name).map_err(delta_err)
    }

    /// Push a `DeltaBatch` as input for a named source on the next step.
    ///
    /// If content-addressed dedup is enabled, insertion rows already seen in a
    /// prior tick are silently dropped.
    pub fn feed_source(&self, source_name: impl Into<String>, batch: DeltaBatch) -> IvmResult<()> {
        let source_name = source_name.into();
        let mut inner = self.inner.lock().map_err(lock_err)?;

        // Content-addressed dedup: filter out re-delivered insertion rows.
        let batch = if inner.input_dedup_enabled {
            let seen = inner
                .seen_input_hashes
                .entry(source_name.clone())
                .or_default();
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

        inner
            .pending
            .entry(source_name)
            .or_default()
            .push(batch);
        Ok(())
    }

    /// Feed streaming micro-batch output into IVM by differentiating consecutive snapshots.
    ///
    /// Streaming jobs typically output a **full materialized snapshot** each tick.
    /// This method calls `differentiate(prev_snapshot, new_snapshot)` to extract the
    /// true delta (insertions and retractions) before pushing it to `feed_source`.
    ///
    /// On the first call for a source, all rows are treated as insertions (no previous
    /// snapshot). Identical consecutive snapshots produce an empty delta and no tick work.
    ///
    /// Use `feed_source` directly if your streaming job already produces `DeltaBatch`es.
    pub fn feed_stream_output(
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
            arrow::compute::concat_batches(
                &schema,
                non_empty.iter().copied(),
            )
            .map_err(|e| IvmError::execution(e.to_string()))?
        };

        // Differentiate: true delta vs previous snapshot.
        let mut inner = self.inner.lock().map_err(lock_err)?;
        let prev = inner.streaming_prev_snapshots.get(&name);
        let delta = differentiate(&schema, prev, &new_snapshot).map_err(delta_err)?;
        inner.streaming_prev_snapshots.insert(name.clone(), new_snapshot);

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
    pub async fn step_datafusion_with_ctx(&self, ctx: &SessionContext) -> IvmResult<StepSummary> {
        // Phase 1: drain pending, snapshot state, extract prev view outputs (brief lock).
        let (raw_pending, current_snapshots, view_specs, view_prev_snapshots) = {
            let mut inner = self.inner.lock().map_err(lock_err)?;
            let raw = std::mem::take(&mut inner.pending);
            let snapshots = inner.source_snapshots.clone();
            let names = inner.view_registry.view_names().map_err(delta_err)?;
            let specs: Vec<IncrementalViewSpec> = names
                .iter()
                .filter_map(|n| inner.view_registry.get(n).ok().map(|v| v.spec.clone()))
                .collect();
            // Extract previous view outputs for dirty-bit reuse.
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
            (raw, snapshots, specs, prev_outputs)
        };

        // Phase 2: apply deltas to source snapshots (no lock).
        let inputs = coalesce_pending(raw_pending)?;

        // Early exit: if no sources have pending input, just bump the tick.
        if inputs.is_empty() {
            let mut inner = self.inner.lock().map_err(lock_err)?;
            inner.tick += 1;
            return Ok(StepSummary::default());
        }

        // Dirty-bit set: lowercase source names that had non-empty input.
        let dirty_sources: HashSet<String> =
            inputs.keys().map(|k| k.to_lowercase()).collect();

        let mut new_snapshots = current_snapshots;
        for (name, delta) in &inputs {
            let current = new_snapshots.remove(name);
            let updated = apply_delta(current, delta).map_err(delta_err)?;
            new_snapshots.insert(name.clone(), updated);
        }

        // Phase 3: register source snapshots as DataFusion MemTables.
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

        // Phase 4: execute views in topological order with dirty-bit skip.
        let topo = toposort_views(&view_specs);
        let spec_map: HashMap<&str, &IncrementalViewSpec> =
            view_specs.iter().map(|s| (s.name.as_str(), s)).collect();

        // Pre-populate with previous outputs so clean views are available as
        // MemTables for downstream dirty views.
        let mut view_full_outputs: HashMap<String, RecordBatch> = view_prev_snapshots;
        let mut dirty_views: HashSet<String> = HashSet::new();

        for view_name in &topo {
            let spec = match spec_map.get(view_name.as_str()) {
                Some(s) => s,
                None => continue,
            };

            // A view is dirty when any SQL token matches a dirty source or view.
            let view_name_lower = view_name.to_lowercase();
            let is_dirty = sql_identifiers(&spec.body_sql).iter().any(|token| {
                dirty_sources.contains(token.as_str())
                    || dirty_views.contains(token.as_str())
            });

            if !is_dirty {
                // Keep the previous snapshot in view_full_outputs (already set above).
                continue;
            }

            dirty_views.insert(view_name_lower);

            // Register all upstream view outputs produced so far.
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

        // Phase 5 + 6: diff, publish, bump tick (only dirty views).
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.source_snapshots = new_snapshots;
        inner.tick += 1;
        let mut total_output_rows = 0usize;
        let mut active_views = 0usize;

        // Provenance tracking: pre-compute input hashes when enabled.
        let input_hashes: Option<Vec<u64>> = if inner.provenance.is_some() {
            let hashes = inputs
                .values()
                .flat_map(|delta| crate::provenance::hash_all_rows(&delta.data_batch()))
                .collect();
            Some(hashes)
        } else {
            None
        };

        for view_name in &dirty_views {
            // Map lowercase back to original name for registry lookup.
            let original = topo.iter().find(|n| n.to_lowercase() == *view_name);
            let view_name_orig = match original {
                Some(n) => n,
                None => continue,
            };
            let new_full = match view_full_outputs.get(view_name_orig) {
                Some(b) => b.clone(),
                None => continue,
            };
            let view = match inner.view_registry.get(view_name_orig) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let delta = view.diff_and_update(new_full).map_err(delta_err)?;
            if !delta.is_empty() {
                total_output_rows += delta.num_rows();
                active_views += 1;

                // Record provenance: each input hash → all output insertion hashes.
                if let (Some(input_hs), Some(prov)) =
                    (&input_hashes, &mut inner.provenance)
                {
                    let output_hs = crate::provenance::hash_all_rows(&delta.data_batch());
                    for &ih in input_hs {
                        prov.record_many(ih, output_hs.iter().copied());
                    }
                }

                let _ = view.publish_output(delta);
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
            inner.source_snapshots.insert(name, updated);
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

fn coalesce_pending(
    raw: HashMap<String, Vec<DeltaBatch>>,
) -> IvmResult<HashMap<String, DeltaBatch>> {
    raw.into_iter()
        .map(|(name, deltas)| {
            let batch = if deltas.len() == 1 {
                deltas.into_iter().next().unwrap()
            } else {
                DeltaBatch::concat(&deltas).map_err(delta_err)?
            };
            Ok((name, batch))
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
    let deltas_json = serde_json::to_string(&delta_entries)
        .map_err(|e| IvmError::execution(e.to_string()))?;

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
    let specs_json = serde_json::to_string(&spec_entries)
        .map_err(|e| IvmError::execution(e.to_string()))?;

    Ok(format!("delta:step:{job_id}|{deltas_json}|{specs_json}"))
}
