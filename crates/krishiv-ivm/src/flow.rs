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
    pub fn feed_source(&self, source_name: impl Into<String>, batch: DeltaBatch) -> IvmResult<()> {
        let mut inner = self.inner.lock().map_err(lock_err)?;
        inner.pending.entry(source_name.into()).or_default().push(batch);
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
        Ok(StepSummary { total_output_rows, active_views })
    }

    /// Advance one tick using DataFusion to execute view SQL.
    pub async fn step_datafusion(&self) -> IvmResult<StepSummary> {
        self.step_datafusion_with_ctx(&SessionContext::new()).await
    }

    /// Advance one tick using the supplied `SessionContext`.
    pub async fn step_datafusion_with_ctx(&self, ctx: &SessionContext) -> IvmResult<StepSummary> {
        // Phase 1: drain pending, snapshot state (brief lock).
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
        };

        // Phase 2: apply deltas to snapshots (no lock).
        let inputs = coalesce_pending(raw_pending)?;
        let mut new_snapshots = current_snapshots;
        for (name, delta) in &inputs {
            let current = new_snapshots.remove(name);
            let updated = apply_delta(current, delta).map_err(delta_err)?;
            new_snapshots.insert(name.clone(), updated);
        }

        // Phase 3: register source snapshots as MemTables.
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

        // Phase 4: execute views in topological order.
        let topo = toposort_views(&view_specs);
        let spec_map: HashMap<&str, &IncrementalViewSpec> =
            view_specs.iter().map(|s| (s.name.as_str(), s)).collect();
        let mut view_full_outputs: HashMap<String, RecordBatch> = HashMap::new();

        for view_name in &topo {
            let spec = match spec_map.get(view_name.as_str()) {
                Some(s) => s,
                None => continue,
            };
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

        // Phase 5 + 6: diff, publish, bump tick.
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

    /// Serialize all source snapshots to Arrow IPC bytes.
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
            let name =
                std::str::from_utf8(bytes.get(pos..pos + name_len).ok_or_else(slice_err)?)
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

async fn execute_view_sql(ctx: &SessionContext, spec: &IncrementalViewSpec) -> IvmResult<RecordBatch> {
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
            let col_idx = batch
                .schema()
                .index_of(field.name())
                .map_err(|_| IvmError::execution(format!(
                    "view output missing column '{}' declared in output_schema",
                    field.name()
                )))?;
            let col = batch.column(col_idx);
            if col.data_type() == field.data_type() {
                Ok(Arc::clone(col))
            } else {
                cast(col.as_ref(), field.data_type())
                    .map_err(|e| IvmError::execution(e.to_string()))
            }
        })
        .collect::<IvmResult<_>>()?;
    RecordBatch::try_new(Arc::clone(target), cols)
        .map_err(|e| IvmError::execution(e.to_string()))
}

fn toposort_views(specs: &[IncrementalViewSpec]) -> Vec<String> {
    let all_names: HashSet<&str> = specs.iter().map(|s| s.name.as_str()).collect();
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    for spec in specs {
        in_degree.entry(spec.name.clone()).or_insert(0);
        for token in sql_identifiers(&spec.body_sql) {
            if all_names.contains(token.as_str()) && token != spec.name {
                dependents.entry(token.clone()).or_default().push(spec.name.clone());
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
