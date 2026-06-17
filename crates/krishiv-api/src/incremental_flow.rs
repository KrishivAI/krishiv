#![forbid(unsafe_code)]

//! `IncrementalFlow` — Rust API for incremental view maintenance.
//!
//! # Execution model
//!
//! `step_datafusion` implements **diff-based IVM**:
//!
//! 1. Each source accumulates a running snapshot via `apply_delta` (insertions
//!    add rows, retractions remove them). The snapshot represents the full live
//!    state of that source across all ticks.
//! 2. Views are executed in **topological order** so that views referencing
//!    other views see the correct upstream output.
//! 3. For each view, the full SQL is run against the current source snapshots.
//!    The result is **differenced** against the previous full output (`diff_and_update`)
//!    to produce a true incremental delta (+1 new rows, −1 removed rows).
//! 4. Only non-empty deltas are published to subscribers.
//!
//! This gives correct incremental semantics for all SQL (aggregates, joins,
//! etc.) at O(result-set) cost per tick. Operator-level O(delta) execution
//! can be layered on top per-view without changing the public API.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::compute::cast;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use tokio::sync::watch;

use krishiv_delta::{
    DeltaBatch, DeltaError, IncrementalViewRegistry, IncrementalViewSpec,
    apply_delta, deserialize_delta_batch, serialize_delta_batch,
};

use crate::error::{KrishivError, Result};

// ── StepSummary ───────────────────────────────────────────────────────────────

/// Summary returned by [`IncrementalFlow::step`].
#[derive(Debug, Default)]
pub struct StepSummary {
    /// Total number of output rows emitted across all views this tick.
    pub total_output_rows: usize,
    /// Number of views that had non-empty deltas this tick.
    pub active_views: usize,
}

// ── IncrementalFlowInner ──────────────────────────────────────────────────────

struct IncrementalFlowInner {
    view_registry: IncrementalViewRegistry,
    /// Pending input deltas keyed by source name, accumulated between steps.
    /// Multiple `feed_source` calls for the same name are stored as a list
    /// and concatenated on the next step.
    pending: HashMap<String, Vec<DeltaBatch>>,
    /// Monotonically increasing tick counter.
    tick: u64,
    /// Cumulative live state per source: the net result of all deltas applied
    /// so far (positive-weight rows only). Registered as MemTables when
    /// executing view SQL via DataFusion.
    source_snapshots: HashMap<String, RecordBatch>,
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
            })),
        }
    }

    /// Register or re-register an incremental view.
    ///
    /// Re-registering an existing view name resets its `full_output` baseline
    /// (behavior-version invalidation): the next `step_datafusion` tick will
    /// treat the entire fresh SQL result as insertions.
    pub fn register_view(&self, spec: IncrementalViewSpec) -> Result<()> {
        let inner = self.inner.lock().map_err(lock_err)?;
        // If the view already exists, reset its full_output before overwriting
        // so diff-based IVM starts fresh (behavior_version invalidation).
        if let Ok(old_view) = inner.view_registry.get(&spec.name) {
            let _ = old_view.reset_full_output();
        }
        inner.view_registry.register(spec).map_err(delta_err)
    }

    /// Remove a registered view. Returns `true` if the view existed.
    pub fn drop_view(&self, name: &str) -> Result<bool> {
        let inner = self.inner.lock().map_err(lock_err)?;
        inner.view_registry.drop_view(name).map_err(delta_err)
    }

    /// Push a `DeltaBatch` as input for a named source on the next step.
    ///
    /// Multiple calls with the same `source_name` before a step are coalesced:
    /// all deltas are concatenated and applied together on the next tick.
    pub fn feed_source(&self, source_name: impl Into<String>, batch: DeltaBatch) -> Result<()> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner
            .pending
            .entry(source_name.into())
            .or_default()
            .push(batch);
        Ok(())
    }

    /// Advance one clock tick (structural: drains pending, bumps tick, no SQL).
    pub fn step(&self) -> Result<StepSummary> {
        self.step_with(|_inputs| Ok(HashMap::new()))
    }

    /// Advance one clock tick with a user-supplied computation callback.
    ///
    /// `compute` receives the pending input deltas (`source_name → DeltaBatch`,
    /// with multiple same-name deltas concatenated) and returns output deltas
    /// (`view_name → DeltaBatch`) that are published to each view's channel.
    pub fn step_with<F>(&self, mut compute: F) -> Result<StepSummary>
    where
        F: FnMut(HashMap<String, DeltaBatch>) -> Result<HashMap<String, DeltaBatch>>,
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

        Ok(StepSummary { total_output_rows, active_views })
    }

    // ── DataFusion-driven incremental execution ────────────────────────────────

    /// Advance one clock tick using DataFusion to execute view SQL bodies.
    ///
    /// Creates a fresh `SessionContext`. See [`step_datafusion_with_ctx`] for
    /// details on the execution model.
    pub async fn step_datafusion(&self) -> Result<StepSummary> {
        self.step_datafusion_with_ctx(&SessionContext::new()).await
    }

    /// Advance one clock tick using the supplied DataFusion `SessionContext`.
    ///
    /// ## Execution model
    ///
    /// 1. **Drain** all pending deltas (coalescing by source name).
    /// 2. **Apply** each source delta to the cumulative source snapshot via
    ///    `apply_delta` — the snapshot grows/shrinks as rows arrive/depart.
    /// 3. **Register** all source snapshots as DataFusion `MemTable`s.
    /// 4. **Execute** each view's `body_sql` in topological order (views
    ///    that reference other views are processed after their dependencies).
    ///    Each upstream view's fresh output is also registered as a `MemTable`
    ///    so downstream views can reference it.
    /// 5. **Diff** each view's new full output against its previous full output
    ///    via `diff_and_update` — produces a true incremental `DeltaBatch`.
    /// 6. **Publish** non-empty deltas to each view's `watch` channel.
    /// 7. **Bump** the tick counter.
    pub async fn step_datafusion_with_ctx(&self, ctx: &SessionContext) -> Result<StepSummary> {
        // ── Phase 1: drain pending, snapshot source states, collect view specs.
        // Hold the mutex only for this extraction.
        let (raw_pending, current_snapshots, view_specs) = {
            let mut inner = self.inner.lock().map_err(lock_err)?;
            let raw = std::mem::take(&mut inner.pending);
            let snapshots = inner.source_snapshots.clone();
            let names = inner.view_registry.view_names().map_err(delta_err)?;
            let specs: Vec<IncrementalViewSpec> = names
                .into_iter()
                .filter_map(|n| inner.view_registry.get(&n).ok().map(|v| v.spec.clone()))
                .collect();
            (raw, snapshots, specs)
        }; // Mutex released — safe to await below.

        // ── Phase 2: apply new deltas to source snapshots (no lock held).
        let inputs = coalesce_pending(raw_pending)?;
        let mut new_snapshots = current_snapshots;
        for (name, delta) in &inputs {
            let current = new_snapshots.remove(name);
            let updated = apply_delta(current, delta).map_err(delta_err)?;
            new_snapshots.insert(name.clone(), updated);
        }

        // ── Phase 3: register all source snapshots as DataFusion MemTables.
        for (name, snapshot) in &new_snapshots {
            if snapshot.num_rows() == 0 {
                continue;
            }
            let schema = snapshot.schema();
            let table = MemTable::try_new(schema, vec![vec![snapshot.clone()]])
                .map_err(|e| KrishivError::Runtime { message: e.to_string() })?;
            ctx.register_table(name.as_str(), Arc::new(table))
                .map_err(|e| KrishivError::Runtime { message: e.to_string() })?;
        }

        // ── Phase 4: execute views in topological order.
        let topo = toposort_views(&view_specs);
        let spec_map: HashMap<&str, &IncrementalViewSpec> =
            view_specs.iter().map(|s| (s.name.as_str(), s)).collect();

        let mut view_full_outputs: HashMap<String, RecordBatch> = HashMap::new();

        for view_name in &topo {
            let spec = match spec_map.get(view_name.as_str()) {
                Some(s) => s,
                None => continue,
            };

            // Register upstream view outputs so this view's SQL can reference them.
            for (up_name, up_batch) in &view_full_outputs {
                if up_batch.num_rows() == 0 {
                    continue;
                }
                let schema = up_batch.schema();
                if let Ok(table) = MemTable::try_new(schema, vec![vec![up_batch.clone()]]) {
                    let _ = ctx.register_table(up_name.as_str(), Arc::new(table));
                }
            }

            // Run the view SQL. On DataFusion error (e.g., missing table),
            // skip this view and emit an empty delta rather than failing the tick.
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
                        .map_err(|e| KrishivError::Runtime { message: e.to_string() })?
                }
            };

            view_full_outputs.insert(view_name.clone(), new_full);
        }

        // ── Phase 5 & 6: diff, publish, bump tick — re-acquire lock.
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.source_snapshots = new_snapshots;
        inner.tick += 1;

        let mut total_output_rows = 0usize;
        let mut active_views = 0usize;

        for (view_name, new_full) in view_full_outputs {
            let view = match inner.view_registry.get(&view_name) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let delta = view.diff_and_update(new_full).map_err(delta_err)?;
            if !delta.is_empty() {
                total_output_rows += delta.num_rows();
                active_views += 1;
                let _ = view.publish_output(delta);
            }
        }

        Ok(StepSummary { total_output_rows, active_views })
    }

    // ── Subscriptions and snapshots ───────────────────────────────────────────

    /// Subscribe to the output stream of a named view.
    pub fn view_output_stream(&self, name: &str) -> Result<watch::Receiver<Option<DeltaBatch>>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        let view = inner.view_registry.get(name).map_err(delta_err)?;
        Ok(view.subscribe())
    }

    /// Return the latest materialized snapshot (only for `is_materialized = true` views).
    pub fn snapshot(&self, name: &str) -> Result<Option<RecordBatch>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        let view = inner.view_registry.get(name).map_err(delta_err)?;
        view.snapshot().map_err(delta_err)
    }

    /// Return the current cumulative source snapshot for a named source.
    pub fn source_snapshot(&self, name: &str) -> Result<Option<RecordBatch>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner.source_snapshots.get(name).cloned())
    }

    pub fn view_names(&self) -> Result<Vec<String>> {
        let inner = self.inner.lock().map_err(lock_err)?;
        inner.view_registry.view_names().map_err(delta_err)
    }

    /// Return a snapshot of all registered view specs.
    pub fn view_specs(&self) -> Result<Vec<IncrementalViewSpec>> {
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

    /// Current tick count (incremented on each `step*` call).
    pub fn tick(&self) -> Result<u64> {
        let inner = self.inner.lock().map_err(lock_err)?;
        Ok(inner.tick)
    }

    // ── Checkpoint / restore ──────────────────────────────────────────────────

    /// Serialize all source snapshots to Arrow IPC bytes.
    ///
    /// Format: `u32 count || (u32 name_len || name_bytes || u32 data_len || ipc_bytes)*`
    ///
    /// Restoring from these bytes with [`restore`] puts the flow back into the
    /// same cumulative source state, so the next `step_datafusion` tick sees
    /// the correct history without reprocessing past events.
    pub fn checkpoint(&self) -> Result<Vec<u8>> {
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
    ///
    /// All view `full_output` baselines are reset so the next
    /// `step_datafusion` tick diffs the freshly-recomputed result against
    /// nothing (emitting all rows as insertions, correctly reflecting the
    /// restored state to downstream subscribers).
    pub fn restore(&self, bytes: &[u8]) -> Result<()> {
        let mut pos = 0usize;

        let n = read_u32(bytes, &mut pos)? as usize;
        let mut source_snapshots: HashMap<String, RecordBatch> = HashMap::with_capacity(n);

        for _ in 0..n {
            let name_len = read_u32(bytes, &mut pos)? as usize;
            let name = std::str::from_utf8(bytes.get(pos..pos + name_len).ok_or_else(slice_err)?)
                .map_err(|e| KrishivError::Runtime { message: e.to_string() })?
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

        // Reset view full_output baselines so next tick diffs from None.
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

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Concatenate multiple `DeltaBatch`es per source name into a single delta.
fn coalesce_pending(raw: HashMap<String, Vec<DeltaBatch>>) -> Result<HashMap<String, DeltaBatch>> {
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

/// Execute a view's `body_sql` and return a coerced `RecordBatch` matching
/// the view's `output_schema`.
async fn execute_view_sql(
    ctx: &SessionContext,
    spec: &IncrementalViewSpec,
) -> Result<RecordBatch> {
    let df = ctx
        .sql(&spec.body_sql)
        .await
        .map_err(|e| KrishivError::Runtime { message: e.to_string() })?;
    let batches = df
        .collect()
        .await
        .map_err(|e| KrishivError::Runtime { message: e.to_string() })?;

    let non_empty: Vec<RecordBatch> = batches.into_iter().filter(|b| b.num_rows() > 0).collect();
    if non_empty.is_empty() {
        // Return an empty batch with the declared output schema.
        let empty_cols: Vec<_> = spec
            .output_schema
            .fields()
            .iter()
            .map(|f| arrow::array::new_empty_array(f.data_type()))
            .collect();
        return RecordBatch::try_new(spec.output_schema.clone(), empty_cols)
            .map_err(|e| KrishivError::Runtime { message: e.to_string() });
    }

    let combined = arrow::compute::concat_batches(&non_empty[0].schema(), &non_empty)
        .map_err(|e| KrishivError::Runtime { message: e.to_string() })?;

    coerce_to_schema(combined, &spec.output_schema)
}

/// Project/cast a `RecordBatch` to match `target_schema` (by column name).
/// Columns in `target_schema` not present in `batch` return an error.
fn coerce_to_schema(batch: RecordBatch, target: &arrow::datatypes::SchemaRef) -> Result<RecordBatch> {
    if batch.schema().as_ref() == target.as_ref() {
        return Ok(batch);
    }
    let cols: Vec<Arc<dyn arrow::array::Array>> = target
        .fields()
        .iter()
        .map(|field| {
            let col_idx = batch
                .schema()
                .index_of(field.name())
                .map_err(|_| KrishivError::Runtime {
                    message: format!(
                        "view output missing column '{}' declared in output_schema",
                        field.name()
                    ),
                })?;
            let col = batch.column(col_idx);
            if col.data_type() == field.data_type() {
                Ok(Arc::clone(col))
            } else {
                cast(col.as_ref(), field.data_type())
                    .map_err(|e| KrishivError::Runtime { message: e.to_string() })
            }
        })
        .collect::<Result<_>>()?;
    RecordBatch::try_new(Arc::clone(target), cols)
        .map_err(|e| KrishivError::Runtime { message: e.to_string() })
}

/// Topologically sort views so that views referenced by other views are
/// processed before their dependents. Detects dependencies by tokenizing
/// each view's `body_sql` and checking for other view names.
///
/// Cycles (recursive views) are appended at the end after all acyclic views.
fn toposort_views(specs: &[IncrementalViewSpec]) -> Vec<String> {
    let all_names: HashSet<&str> = specs.iter().map(|s| s.name.as_str()).collect();

    // Build adjacency using owned Strings to avoid lifetime issues.
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

    // Kahn's algorithm.
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

    // Append any remaining nodes (cycle participants) after acyclic ones.
    let in_order: HashSet<&str> = order.iter().map(|s| s.as_str()).collect();
    let remaining: Vec<String> = specs
        .iter()
        .filter(|s| !in_order.contains(s.name.as_str()))
        .map(|s| s.name.clone())
        .collect();
    order.extend(remaining);

    order
}

/// Tokenize SQL into lowercase identifiers (splits on non-word chars).
fn sql_identifiers(sql: &str) -> Vec<String> {
    sql.split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

// ── Checkpoint helpers ────────────────────────────────────────────────────────

fn read_u32(bytes: &[u8], pos: &mut usize) -> Result<u32> {
    let slice = bytes.get(*pos..*pos + 4).ok_or_else(slice_err)?;
    *pos += 4;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

fn slice_err() -> KrishivError {
    KrishivError::Runtime { message: "checkpoint bytes truncated".into() }
}

// ── Error conversions ─────────────────────────────────────────────────────────

fn delta_err(e: DeltaError) -> KrishivError {
    KrishivError::Runtime { message: e.to_string() }
}

fn lock_err<T>(_: T) -> KrishivError {
    KrishivError::Runtime { message: "incremental flow lock poisoned".into() }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn empty_spec(name: &str) -> IncrementalViewSpec {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        IncrementalViewSpec {
            name: name.to_string(),
            body_sql: "SELECT 1 AS x".into(),
            output_schema: schema,
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        }
    }

    fn int_schema(col: &str) -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(col, DataType::Int32, false)]))
    }

    fn int_batch(schema: Arc<Schema>, vals: &[i32]) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vals.to_vec()))]).unwrap()
    }

    // ── Basic API ─────────────────────────────────────────────────────────────

    #[test]
    fn register_and_list_views() {
        let flow = IncrementalFlow::new();
        flow.register_view(empty_spec("revenue")).unwrap();
        flow.register_view(empty_spec("counts")).unwrap();
        let mut names = flow.view_names().unwrap();
        names.sort();
        assert_eq!(names, vec!["counts", "revenue"]);
    }

    #[test]
    fn view_specs_returns_all_registered() {
        let flow = IncrementalFlow::new();
        flow.register_view(empty_spec("a")).unwrap();
        flow.register_view(empty_spec("b")).unwrap();
        let mut names: Vec<String> =
            flow.view_specs().unwrap().into_iter().map(|s| s.name).collect();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn step_increments_tick() {
        let flow = IncrementalFlow::new();
        assert_eq!(flow.tick().unwrap(), 0);
        flow.step().unwrap();
        assert_eq!(flow.tick().unwrap(), 1);
        flow.step().unwrap();
        assert_eq!(flow.tick().unwrap(), 2);
    }

    #[test]
    fn feed_source_coalesces_multiple_calls() {
        let flow = IncrementalFlow::new();
        let schema = int_schema("id");
        let b1 = int_batch(schema.clone(), &[1, 2]);
        let b2 = int_batch(schema.clone(), &[3]);
        let cb1 = DeltaBatch::from_inserts(b1).unwrap();
        let cb2 = DeltaBatch::from_inserts(b2).unwrap();
        // Both calls for the same source should be coalesced.
        flow.feed_source("orders", cb1).unwrap();
        flow.feed_source("orders", cb2).unwrap();

        let received = Arc::new(Mutex::new(0usize));
        let recv_clone = Arc::clone(&received);
        let summary = flow
            .step_with(|inputs| {
                let orders = inputs.get("orders").unwrap();
                *recv_clone.lock().unwrap() = orders.num_rows();
                Ok(HashMap::new())
            })
            .unwrap();
        assert_eq!(*received.lock().unwrap(), 3, "deltas should be concatenated");
    }

    #[test]
    fn feed_source_drains_on_step() {
        let flow = IncrementalFlow::new();
        let schema = int_schema("id");
        let batch = int_batch(schema.clone(), &[1, 2, 3]);
        let cb = DeltaBatch::from_inserts(batch).unwrap();
        flow.feed_source("orders", cb).unwrap();
        flow.step().unwrap();
        assert_eq!(flow.tick().unwrap(), 1);
    }

    #[test]
    fn view_output_stream_returns_receiver() {
        let flow = IncrementalFlow::new();
        flow.register_view(empty_spec("v1")).unwrap();
        let rx = flow.view_output_stream("v1").unwrap();
        assert!(rx.borrow().is_none());
    }

    #[test]
    fn snapshot_returns_none_for_non_materialized_view() {
        let flow = IncrementalFlow::new();
        flow.register_view(empty_spec("v1")).unwrap();
        assert!(flow.snapshot("v1").unwrap().is_none());
    }

    #[test]
    fn step_with_publishes_output_to_view() {
        let flow = IncrementalFlow::new();
        flow.register_view(empty_spec("v1")).unwrap();
        let rx = flow.view_output_stream("v1").unwrap();

        let schema = int_schema("x");
        let batch = int_batch(schema.clone(), &[10, 20]);
        let input_delta = DeltaBatch::from_inserts(batch).unwrap();
        flow.feed_source("src", input_delta).unwrap();

        let summary = flow
            .step_with(|mut inputs| {
                let delta = inputs.remove("src").unwrap();
                let mut out = HashMap::new();
                out.insert("v1".to_string(), delta);
                Ok(out)
            })
            .unwrap();

        assert_eq!(summary.active_views, 1);
        assert_eq!(summary.total_output_rows, 2);
        assert!(rx.borrow().is_some());
    }

    #[test]
    fn drop_view_removes_view() {
        let flow = IncrementalFlow::new();
        flow.register_view(empty_spec("v1")).unwrap();
        assert!(flow.drop_view("v1").unwrap());
        assert!(!flow.drop_view("v1").unwrap(), "already gone");
        assert!(flow.view_names().unwrap().is_empty());
    }

    // ── DataFusion execution ──────────────────────────────────────────────────

    #[tokio::test]
    async fn step_datafusion_runs_sql_view_body() {
        let flow = IncrementalFlow::new();
        let out_schema = int_schema("val");
        flow.register_view(IncrementalViewSpec {
            name: "passthrough".to_string(),
            body_sql: "SELECT val FROM src".to_string(),
            output_schema: out_schema,
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        })
        .unwrap();

        let schema = int_schema("val");
        let batch = int_batch(schema.clone(), &[1, 2, 3]);
        let cb = DeltaBatch::from_inserts(batch).unwrap();
        flow.feed_source("src", cb).unwrap();

        let rx = flow.view_output_stream("passthrough").unwrap();
        let summary = flow.step_datafusion().await.unwrap();

        assert_eq!(summary.active_views, 1);
        assert_eq!(summary.total_output_rows, 3);
        assert_eq!(flow.tick().unwrap(), 1);
        assert!(rx.borrow().is_some());
    }

    #[tokio::test]
    async fn step_datafusion_cumulative_source_state() {
        // Feed rows across multiple ticks; the view sees ALL rows, not just the
        // current tick's delta.
        let flow = IncrementalFlow::new();
        let s = int_schema("id");
        flow.register_view(IncrementalViewSpec {
            name: "all_ids".to_string(),
            body_sql: "SELECT id FROM ids".to_string(),
            output_schema: s.clone(),
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        })
        .unwrap();

        // Tick 1: feed rows 1, 2
        flow.feed_source("ids", DeltaBatch::from_inserts(int_batch(s.clone(), &[1, 2])).unwrap())
            .unwrap();
        let s1 = flow.step_datafusion().await.unwrap();
        assert_eq!(s1.total_output_rows, 2, "tick 1: 2 insertions");

        // Tick 2: feed row 3
        flow.feed_source("ids", DeltaBatch::from_inserts(int_batch(s.clone(), &[3])).unwrap())
            .unwrap();
        let s2 = flow.step_datafusion().await.unwrap();
        assert_eq!(s2.total_output_rows, 1, "tick 2: only 1 new row (delta, not full result)");

        // Tick 3: retract row 1
        flow.feed_source("ids", DeltaBatch::from_deletes(int_batch(s.clone(), &[1])).unwrap())
            .unwrap();
        let s3 = flow.step_datafusion().await.unwrap();
        assert_eq!(s3.total_output_rows, 1, "tick 3: 1 retraction");

        // Source snapshot should now have rows 2, 3
        let snap = flow.source_snapshot("ids").unwrap().unwrap();
        assert_eq!(snap.num_rows(), 2);
    }

    #[tokio::test]
    async fn step_datafusion_view_references_other_view() {
        // base_view: SELECT val FROM raw
        // doubled_view: SELECT val FROM base_view (inter-view dependency)
        let flow = IncrementalFlow::new();
        let s = int_schema("val");

        flow.register_view(IncrementalViewSpec {
            name: "base_view".to_string(),
            body_sql: "SELECT val FROM raw".to_string(),
            output_schema: s.clone(),
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        })
        .unwrap();
        flow.register_view(IncrementalViewSpec {
            name: "filtered".to_string(),
            body_sql: "SELECT val FROM base_view WHERE val > 1".to_string(),
            output_schema: s.clone(),
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        })
        .unwrap();

        flow.feed_source("raw", DeltaBatch::from_inserts(int_batch(s.clone(), &[1, 2, 3])).unwrap())
            .unwrap();
        let summary = flow.step_datafusion().await.unwrap();

        // base_view emits 3 rows; filtered emits 2 (val > 1)
        assert_eq!(summary.total_output_rows, 5);
    }

    #[tokio::test]
    async fn step_datafusion_constant_view_no_sources() {
        let flow = IncrementalFlow::new();
        flow.register_view(empty_spec("v")).unwrap();
        let summary = flow.step_datafusion().await.unwrap();
        assert_eq!(summary.active_views, 1);
        assert_eq!(summary.total_output_rows, 1);
    }

    #[tokio::test]
    async fn behavior_version_invalidation_resets_baseline() {
        let flow = IncrementalFlow::new();
        let s = int_schema("val");

        flow.register_view(IncrementalViewSpec {
            name: "v".to_string(),
            body_sql: "SELECT val FROM t".to_string(),
            output_schema: s.clone(),
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        })
        .unwrap();

        flow.feed_source("t", DeltaBatch::from_inserts(int_batch(s.clone(), &[1, 2])).unwrap())
            .unwrap();
        let s1 = flow.step_datafusion().await.unwrap();
        assert_eq!(s1.total_output_rows, 2, "initial 2 rows");

        // Re-register with new SQL (behavior_version change).
        flow.register_view(IncrementalViewSpec {
            name: "v".to_string(),
            body_sql: "SELECT val FROM t WHERE val > 1".to_string(),
            output_schema: s.clone(),
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        })
        .unwrap();

        // Next tick: no new source data, but view baseline was reset.
        // The view should re-emit all matching rows as insertions.
        let s2 = flow.step_datafusion().await.unwrap();
        assert_eq!(s2.total_output_rows, 1, "re-registration: only val=2 matches new SQL");
    }

    // ── Checkpoint / restore ──────────────────────────────────────────────────

    #[tokio::test]
    async fn checkpoint_restore_preserves_source_state() {
        let flow = IncrementalFlow::new();
        let s = int_schema("id");
        flow.register_view(IncrementalViewSpec {
            name: "ids".to_string(),
            body_sql: "SELECT id FROM src".to_string(),
            output_schema: s.clone(),
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        })
        .unwrap();

        // Feed some rows.
        flow.feed_source("src", DeltaBatch::from_inserts(int_batch(s.clone(), &[10, 20])).unwrap())
            .unwrap();
        flow.step_datafusion().await.unwrap();

        // Checkpoint.
        let bytes = flow.checkpoint().unwrap();
        assert!(!bytes.is_empty());

        // Create a fresh flow and restore.
        let flow2 = IncrementalFlow::new();
        flow2.register_view(IncrementalViewSpec {
            name: "ids".to_string(),
            body_sql: "SELECT id FROM src".to_string(),
            output_schema: s.clone(),
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        })
        .unwrap();
        flow2.restore(&bytes).unwrap();

        // The restored flow should have the same source snapshot.
        let snap = flow2.source_snapshot("src").unwrap().unwrap();
        assert_eq!(snap.num_rows(), 2);

        // On next tick with no new data, the diff emits all rows as insertions
        // (since the baseline was reset on restore).
        let summary = flow2.step_datafusion().await.unwrap();
        assert_eq!(summary.total_output_rows, 2);
    }

    // ── toposort ──────────────────────────────────────────────────────────────

    #[test]
    fn toposort_independent_views_any_order() {
        let specs: Vec<IncrementalViewSpec> = ["a", "b", "c"].iter().map(|n| empty_spec(n)).collect();
        let order = toposort_views(&specs);
        assert_eq!(order.len(), 3);
    }

    #[test]
    fn toposort_chained_dependency() {
        let s = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let specs = vec![
            IncrementalViewSpec {
                name: "c".to_string(),
                body_sql: "SELECT x FROM b".to_string(),
                output_schema: s.clone(),
                is_materialized: false,
                is_recursive: false,
                lateness: vec![],
            },
            IncrementalViewSpec {
                name: "b".to_string(),
                body_sql: "SELECT x FROM a".to_string(),
                output_schema: s.clone(),
                is_materialized: false,
                is_recursive: false,
                lateness: vec![],
            },
            IncrementalViewSpec {
                name: "a".to_string(),
                body_sql: "SELECT x FROM raw".to_string(),
                output_schema: s.clone(),
                is_materialized: false,
                is_recursive: false,
                lateness: vec![],
            },
        ];
        let order = toposort_views(&specs);
        let pos: HashMap<&str, usize> =
            order.iter().enumerate().map(|(i, n)| (n.as_str(), i)).collect();
        assert!(pos["a"] < pos["b"], "a must come before b");
        assert!(pos["b"] < pos["c"], "b must come before c");
    }
}
