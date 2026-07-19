#![forbid(unsafe_code)]

//! Stateless IVM (DeltaBatch) fragment execution — coordinator-authoritative.
//!
//! The coordinator is the single source of truth for an IVM job. Each tick it
//! drains pending deltas locally, snapshots the full flow state (source
//! snapshots + view baselines) via `IncrementalFlow::checkpoint_full`, and
//! ships a self-contained `delta:step:` fragment to a registered executor.
//!
//! The executor performs **one stateless tick** on a *transient* flow:
//!
//! 1. register the shipped view specs,
//! 2. restore the shipped state (so diffs use the correct baselines),
//! 3. feed the tick's deltas,
//! 4. run `step_datafusion`,
//! 5. return each view's full materialized output as a framed
//!    `name → RecordBatch` blob.
//!
//! No per-job state is retained on the executor: it is a replaceable worker.
//! If the executor fails or is reassigned mid-tick, the coordinator re-feeds
//! the pending deltas and computes centrally — so state can never diverge or
//! be lost. See `submit_resident_ivm_step` in `krishiv-scheduler`.
//!
//! # Fragment format
//!
//! ```text
//! delta:step:{job_id}|{deltas_b64}|{specs_b64}|{state_b64}
//! ```
//!
//! Every payload part is base64-encoded, so a `|` inside a SQL string literal
//! in `body_sql` cannot corrupt the framing. `state_b64` is the base64 of
//! `IncrementalFlow::checkpoint_full`.

use std::collections::HashMap;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use krishiv_ivm::{
    IncrementalFlow, IncrementalViewSpec, deserialize_delta_batch, encode_batch_map,
};
use serde::Deserialize;

// ── fragment wire types ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PendingDeltaJson {
    pub source: String,
    pub delta_b64: String,
}

#[derive(Debug, Deserialize)]
pub struct ViewSpecJson {
    pub name: String,
    pub body_sql: String,
    pub output_schema_fields: Vec<SchemaFieldJson>,
    #[serde(default)]
    pub is_materialized: bool,
    #[serde(default)]
    pub is_recursive: bool,
    /// AUD-4: lateness retention specs, carried so an offloaded tick applies
    /// the same watermark GC as a central tick. Defaults to empty for
    /// backward-compatible fragments produced before this field existed.
    #[serde(default)]
    pub lateness: Vec<krishiv_ivm::LatenessSpec>,
}

#[derive(Debug, Deserialize)]
pub struct SchemaFieldJson {
    pub name: String,
    pub data_type: String,
    #[serde(default)]
    pub nullable: bool,
}

fn parse_schema_fields(fields: &[SchemaFieldJson]) -> Option<SchemaRef> {
    let arrow_fields: Option<Vec<Field>> = fields
        .iter()
        .map(|f| {
            let dt = match f.data_type.as_str() {
                "Int8" => Some(DataType::Int8),
                "Int16" => Some(DataType::Int16),
                "Int32" => Some(DataType::Int32),
                "Int64" => Some(DataType::Int64),
                "UInt8" => Some(DataType::UInt8),
                "UInt16" => Some(DataType::UInt16),
                "UInt32" => Some(DataType::UInt32),
                "UInt64" => Some(DataType::UInt64),
                "Float32" => Some(DataType::Float32),
                "Float64" => Some(DataType::Float64),
                "Utf8" => Some(DataType::Utf8),
                "LargeUtf8" => Some(DataType::LargeUtf8),
                "Boolean" => Some(DataType::Boolean),
                "Binary" => Some(DataType::Binary),
                "TimestampMs" => Some(DataType::Timestamp(TimeUnit::Millisecond, None)),
                "TimestampUs" => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
                "Date32" => Some(DataType::Date32),
                "Date64" => Some(DataType::Date64),
                _ => None,
            }?;
            Some(Field::new(f.name.clone(), dt, f.nullable))
        })
        .collect();
    Some(std::sync::Arc::new(Schema::new(arrow_fields?)))
}

fn empty_batch(schema: SchemaRef) -> RecordBatch {
    let cols: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| arrow::array::new_empty_array(f.data_type()))
        .collect();
    RecordBatch::try_new(schema, cols)
        .unwrap_or_else(|_| RecordBatch::new_empty(std::sync::Arc::new(Schema::empty())))
}

// ── fragment prefixes ─────────────────────────────────────────────────────────

/// Prefix for legacy stateless IVM step fragments (full state per tick).
pub const IVM_FRAGMENT_PREFIX: &str = "delta:step:";
/// Resident protocol (AUD-6): create/replace a resident flow (state ships once).
pub const IVM_ATTACH_PREFIX: &str = "delta:attach:";
/// Resident protocol: feed deltas + step the resident flow (fence-guarded).
pub const IVM_TICK_PREFIX: &str = "delta:tick:";
/// Resident protocol: return `checkpoint_full` bytes of the resident flow.
pub const IVM_CKPT_PREFIX: &str = "delta:ckpt:";
/// Resident protocol: drop the resident flow.
pub const IVM_DETACH_PREFIX: &str = "delta:detach:";

/// True when `body` is any resident-protocol IVM fragment.
pub fn is_resident_ivm_fragment(body: &str) -> bool {
    body.starts_with(IVM_ATTACH_PREFIX)
        || body.starts_with(IVM_TICK_PREFIX)
        || body.starts_with(IVM_CKPT_PREFIX)
        || body.starts_with(IVM_DETACH_PREFIX)
}

// ── resident flows (AUD-6) ────────────────────────────────────────────────────

/// One executor-resident IVM flow plus its dispatch fence.
///
/// The flow persists across ticks — cached `SessionContext`, compiled view
/// plans, and operator accumulators all stay warm (the exact state the old
/// stateless path rebuilt from a shipped snapshot every tick). The fence is
/// the coordinator's tick number: a tick is accepted only when
/// `fence == last_fence + 1`, so replays and gaps error instead of silently
/// double-applying or skipping deltas.
pub struct ResidentIvmFlow {
    pub flow: IncrementalFlow,
    pub fence: u64,
}

/// Executor-wide map of resident IVM flows, keyed by IVM job id.
///
/// The per-entry async mutex serializes ticks for one job (matching the
/// coordinator's per-job step lock) while independent jobs run in parallel.
pub type ResidentIvmFlows =
    std::sync::Arc<dashmap::DashMap<String, std::sync::Arc<tokio::sync::Mutex<ResidentIvmFlow>>>>;

fn register_specs_on_flow(flow: &IncrementalFlow, view_specs: &[ViewSpecJson]) -> Result<(), String> {
    for vs in view_specs {
        if let Some(schema) = parse_schema_fields(&vs.output_schema_fields) {
            let spec = IncrementalViewSpec {
                name: vs.name.clone(),
                body_sql: vs.body_sql.clone(),
                output_schema: schema,
                is_materialized: vs.is_materialized,
                is_recursive: vs.is_recursive,
                lateness: vs.lateness.clone(),
            };
            flow.register_view(spec).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

fn decode_specs_b64(specs_b64: &str) -> Result<Vec<ViewSpecJson>, String> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let specs_json = b64
        .decode(specs_b64)
        .map_err(|e| format!("specs b64: {e}"))?;
    let specs_str = std::str::from_utf8(&specs_json).map_err(|e| format!("specs utf8: {e}"))?;
    serde_json::from_str(specs_str).map_err(|e| format!("specs json: {e}"))
}

fn decode_deltas_b64(deltas_b64: &str) -> Result<Vec<PendingDeltaJson>, String> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let deltas_json = b64
        .decode(deltas_b64)
        .map_err(|e| format!("deltas b64: {e}"))?;
    let deltas_str = std::str::from_utf8(&deltas_json).map_err(|e| format!("deltas utf8: {e}"))?;
    serde_json::from_str(deltas_str).map_err(|e| format!("deltas json: {e}"))
}

/// Execute a resident-protocol IVM fragment against the executor's flow map.
///
/// Returns `(StepSummary, Option<blob>)` like [`execute_ivm_fragment`]. The
/// blob is:
/// - `delta:tick:`   → per-view **output-delta** map (`encode_delta_map`)
/// - `delta:ckpt:`   → `checkpoint_full` bytes of the resident flow
/// - `delta:attach:` / `delta:detach:` → `None`
pub async fn execute_resident_ivm_fragment(
    flows: &ResidentIvmFlows,
    fragment_body: &str,
) -> Result<(krishiv_ivm::StepSummary, Option<Vec<u8>>), String> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;

    if let Some(rest) = fragment_body.strip_prefix(IVM_ATTACH_PREFIX) {
        // delta:attach:{job}|{specs_b64}|{state_b64}|{fence}
        let parts: Vec<&str> = rest.splitn(4, '|').collect();
        let [job, specs_b64, state_b64, fence_s] = parts.as_slice() else {
            return Err("invalid delta:attach fragment: expected 4 parts".into());
        };
        let fence: u64 = fence_s
            .parse()
            .map_err(|e| format!("attach fence parse: {e}"))?;
        let view_specs = decode_specs_b64(specs_b64)?;
        let state_bytes = b64
            .decode(state_b64)
            .map_err(|e| format!("state b64: {e}"))?;

        // A resident flow uses cached incremental plans across ticks — this is
        // the point of residency, so `force_diff_based` is deliberately NOT set
        // (the accumulators live here and never need to transfer per tick).
        let flow = IncrementalFlow::new();
        register_specs_on_flow(&flow, &view_specs)?;
        if !state_bytes.is_empty() {
            flow.restore_full(&state_bytes)
                .map_err(|e| format!("attach restore_full: {e}"))?;
        }
        flows.insert(
            (*job).to_owned(),
            std::sync::Arc::new(tokio::sync::Mutex::new(ResidentIvmFlow { flow, fence })),
        );
        tracing::info!(job = %job, fence, state_bytes = state_bytes.len(),
            "resident IVM flow attached");
        return Ok((krishiv_ivm::StepSummary::default(), None));
    }

    if let Some(rest) = fragment_body.strip_prefix(IVM_TICK_PREFIX) {
        // delta:tick:{job}|{deltas_b64}|{fence}
        let parts: Vec<&str> = rest.splitn(3, '|').collect();
        let [job, deltas_b64, fence_s] = parts.as_slice() else {
            return Err("invalid delta:tick fragment: expected 3 parts".into());
        };
        let fence: u64 = fence_s
            .parse()
            .map_err(|e| format!("tick fence parse: {e}"))?;
        let entry = flows
            .get(*job)
            .map(|e| e.value().clone())
            .ok_or_else(|| format!("no resident IVM flow for job '{job}' (needs attach)"))?;
        let mut resident = entry.lock().await;
        if fence != resident.fence + 1 {
            return Err(format!(
                "fence mismatch for job '{job}': expected {}, got {fence} \
                 (replay or gap — coordinator must re-attach)",
                resident.fence + 1
            ));
        }
        let pending = decode_deltas_b64(deltas_b64)?;
        for pd in &pending {
            let ipc_bytes = b64
                .decode(&pd.delta_b64)
                .map_err(|e| format!("base64 decode delta for '{}': {e}", pd.source))?;
            let delta = deserialize_delta_batch(&ipc_bytes)
                .map_err(|e| e.to_string())?
                .drop_zeros()
                .map_err(|e| e.to_string())?;
            resident
                .flow
                .feed(pd.source.clone(), delta)
                .map_err(|e| e.to_string())?;
        }
        let summary = crate::erased(resident.flow.step_datafusion())
            .await
            .map_err(|e| e.to_string())?;
        resident.fence = fence;

        // AUD-6 exit contract: return per-view OUTPUT DELTAS, never snapshots.
        let mut outputs: HashMap<String, krishiv_ivm::DeltaBatch> = HashMap::new();
        for name in resident.flow.view_names().map_err(|e| e.to_string())? {
            if let Some(delta) = resident
                .flow
                .take_step_output(&name)
                .map_err(|e| e.to_string())?
            {
                outputs.insert(name, delta);
            }
        }
        let blob = krishiv_ivm::encode_delta_map(&outputs).map_err(|e| e.to_string())?;
        return Ok((summary, Some(blob)));
    }

    if let Some(job) = fragment_body.strip_prefix(IVM_CKPT_PREFIX) {
        let entry = flows
            .get(job)
            .map(|e| e.value().clone())
            .ok_or_else(|| format!("no resident IVM flow for job '{job}' (needs attach)"))?;
        let resident = entry.lock().await;
        let bytes = resident
            .flow
            .checkpoint_full()
            .map_err(|e| e.to_string())?;
        return Ok((krishiv_ivm::StepSummary::default(), Some(bytes)));
    }

    if let Some(job) = fragment_body.strip_prefix(IVM_DETACH_PREFIX) {
        flows.remove(job);
        tracing::info!(job = %job, "resident IVM flow detached");
        return Ok((krishiv_ivm::StepSummary::default(), None));
    }

    Err(format!(
        "not a resident IVM fragment: {}",
        fragment_body.chars().take(40).collect::<String>()
    ))
}

// ── execution ─────────────────────────────────────────────────────────────────

/// Execute one stateless IVM tick for a `delta:step:` fragment.
///
/// Returns `(StepSummary, Option<Vec<u8>>)` where the blob is the framed
/// `name → RecordBatch` map of view full outputs (via `encode_batch_map`),
/// which the coordinator applies to its authoritative flow.
pub async fn execute_ivm_fragment(
    fragment_body: &str,
) -> Result<(krishiv_ivm::StepSummary, Option<Vec<u8>>), String> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;

    // Format: "delta:step:{job_id}|{deltas_b64}|{specs_b64}|{state_b64}"
    let rest = fragment_body
        .strip_prefix(IVM_FRAGMENT_PREFIX)
        .ok_or_else(|| "invalid IVM fragment: missing prefix".to_string())?;
    let parts: Vec<&str> = rest.splitn(4, '|').collect();
    if parts.len() < 4 {
        return Err(format!(
            "invalid IVM fragment: expected 4 '|'-separated parts, got {}",
            parts.len()
        ));
    }
    let _job_id = parts.first().copied().unwrap_or("");
    let deltas_b64 = parts.get(1).copied().unwrap_or("");
    let specs_b64 = parts.get(2).copied().unwrap_or("");
    let state_b64 = parts.get(3).copied().unwrap_or("");

    // Decode payloads.
    let deltas_json = b64
        .decode(deltas_b64)
        .map_err(|e| format!("deltas b64: {e}"))?;
    let deltas_str = std::str::from_utf8(&deltas_json).map_err(|e| format!("deltas utf8: {e}"))?;
    let pending_deltas: Vec<PendingDeltaJson> =
        serde_json::from_str(deltas_str).map_err(|e| format!("deltas json: {e}"))?;

    let specs_json = b64
        .decode(specs_b64)
        .map_err(|e| format!("specs b64: {e}"))?;
    let specs_str = std::str::from_utf8(&specs_json).map_err(|e| format!("specs utf8: {e}"))?;
    let view_specs: Vec<ViewSpecJson> =
        serde_json::from_str(specs_str).map_err(|e| format!("specs json: {e}"))?;

    let state_bytes = b64
        .decode(state_b64)
        .map_err(|e| format!("state b64: {e}"))?;

    // Build a transient flow. Register views BEFORE restoring state so the
    // restored baselines land on real views (restore_full walks registered
    // view names and seeds each view's snapshot + full_output).
    let flow = IncrementalFlow::new();
    for vs in &view_specs {
        if let Some(schema) = parse_schema_fields(&vs.output_schema_fields) {
            let spec = IncrementalViewSpec {
                name: vs.name.clone(),
                body_sql: vs.body_sql.clone(),
                output_schema: schema,
                is_materialized: vs.is_materialized,
                is_recursive: vs.is_recursive,
                // AUD-4: preserve lateness so an offloaded tick applies the same
                // retention/GC semantics as a central tick (was hardcoded empty).
                lateness: vs.lateness.clone(),
            };
            flow.register_view(spec).map_err(|e| e.to_string())?;
        }
    }
    flow.restore_full(&state_bytes)
        .map_err(|e| format!("restore_full: {e}"))?;
    // Operator accumulator state is included in checkpoint_full (plan-state
    // section) and restored via pending_plan_state. The restored flow can use
    // incremental plans without force_diff_based. The flow's accumulators are
    // seeded from the checkpointed bytes in Phase 5 of step_datafusion.

    // Feed pending deltas (input dedup / zero-drop mirror the coordinator path).
    for pd in &pending_deltas {
        let ipc_bytes = b64
            .decode(&pd.delta_b64)
            .map_err(|e| format!("base64 decode delta for '{}': {e}", pd.source))?;
        let delta = deserialize_delta_batch(&ipc_bytes)
            .map_err(|e| e.to_string())?
            .drop_zeros()
            .map_err(|e| e.to_string())?;
        flow.feed(pd.source.clone(), delta)
            .map_err(|e| e.to_string())?;
    }

    // Run one tick.
    let summary = crate::erased(flow.step_datafusion())
        .await
        .map_err(|e| e.to_string())?;

    // Collect each view's full materialized output. Non-materialized or
    // never-stepped views yield an empty batch over their schema so the
    // coordinator's replace_full is a deterministic no-op.
    let mut outputs: HashMap<String, RecordBatch> = HashMap::new();
    let names = flow.view_names().map_err(|e| e.to_string())?;
    for name in &names {
        let snap = flow.snapshot(name).map_err(|e| e.to_string())?;
        match snap {
            Some(rb) => {
                outputs.insert(name.clone(), rb);
            }
            None => {
                if let Ok(Some(spec)) = flow.view_spec(name) {
                    outputs.insert(name.clone(), empty_batch(spec.output_schema));
                }
            }
        }
    }

    let blob = encode_batch_map(&outputs).map_err(|e| e.to_string())?;
    Ok((summary, Some(blob)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Float64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_ivm::{
        DeltaBatch, IncrementalFlow, IncrementalViewSpec, decode_batch_map,
        encode_ivm_step_fragment,
    };

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

    fn sum_view_spec() -> IncrementalViewSpec {
        IncrementalViewSpec {
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

    fn total_of(map: &std::collections::HashMap<String, RecordBatch>) -> f64 {
        map.get("total_sales")
            .unwrap()
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    }

    /// Phase 57 (AUD-6) resident protocol: state attaches ONCE, ticks carry
    /// deltas + fences only, results are per-view OUTPUT DELTAS, and state
    /// accumulates across ticks on the executor (the whole point of residency).
    /// A replayed fence is rejected instead of double-applying.
    #[tokio::test]
    async fn resident_flow_accumulates_across_ticks_and_returns_deltas() {
        use krishiv_ivm::{
            decode_delta_map, encode_ivm_attach_fragment, encode_ivm_ckpt_fragment,
            encode_ivm_detach_fragment, encode_ivm_tick_fragment,
        };

        let flows: super::ResidentIvmFlows = Arc::new(dashmap::DashMap::new());
        let specs = vec![sum_view_spec()];

        // Attach with EMPTY state (fresh job promotion) at fence 0.
        let attach = encode_ivm_attach_fragment("job-r", &specs, &[], 0).unwrap();
        super::execute_resident_ivm_fragment(&flows, &attach)
            .await
            .unwrap();

        let tick = |amounts: Vec<f64>, fence: u64| {
            let mut pending = std::collections::HashMap::new();
            pending.insert(
                "sales".to_string(),
                DeltaBatch::from_inserts(sales_batch(&amounts)).unwrap(),
            );
            encode_ivm_tick_fragment("job-r", &pending, fence).unwrap()
        };

        // Tick 1: 100+200 → total 300 (all-insert first output).
        let (_s1, blob1) = super::execute_resident_ivm_fragment(&flows, &tick(vec![100.0, 200.0], 1))
            .await
            .unwrap();
        let d1 = decode_delta_map(blob1.as_ref().unwrap()).unwrap();
        let out1 = d1.get("total_sales").expect("view emitted a delta");
        assert!(
            out1.weights().iter().flatten().any(|w| w > 0),
            "first tick output must contain an insertion"
        );

        // Tick 2: +50 → the RESIDENT state accumulates: retract 300, insert 350.
        let (_s2, blob2) = super::execute_resident_ivm_fragment(&flows, &tick(vec![50.0], 2))
            .await
            .unwrap();
        let d2 = decode_delta_map(blob2.as_ref().unwrap()).unwrap();
        let out2 = d2.get("total_sales").expect("second tick delta");
        let data = out2.data_batch();
        let weights = out2.weights();
        let totals = data
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut retract_300 = false;
        let mut insert_350 = false;
        for i in 0..data.num_rows() {
            let (w, t) = (weights.value(i), totals.value(i));
            if w < 0 && (t - 300.0).abs() < 1e-9 {
                retract_300 = true;
            }
            if w > 0 && (t - 350.0).abs() < 1e-9 {
                insert_350 = true;
            }
        }
        assert!(
            retract_300 && insert_350,
            "resident state must accumulate across ticks (retract 300, insert 350); got {out2:?}"
        );

        // Fence replay (2 again) and gap (5) are both rejected.
        let replay = super::execute_resident_ivm_fragment(&flows, &tick(vec![1.0], 2)).await;
        assert!(replay.is_err(), "fence replay must be rejected");
        let gap = super::execute_resident_ivm_fragment(&flows, &tick(vec![1.0], 5)).await;
        assert!(gap.is_err(), "fence gap must be rejected");

        // Checkpoint returns restorable full-state bytes.
        let (_sc, ckpt) = super::execute_resident_ivm_fragment(
            &flows,
            &encode_ivm_ckpt_fragment("job-r"),
        )
        .await
        .unwrap();
        let restored = IncrementalFlow::new();
        restored.register_view(sum_view_spec()).unwrap();
        restored.restore_full(ckpt.as_ref().unwrap()).unwrap();
        let snap = restored.snapshot("total_sales").unwrap().unwrap();
        let total = snap
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((total - 350.0).abs() < 1e-9, "ckpt restores 350, got {total}");

        // Detach drops the flow; the next tick errors (needs re-attach).
        super::execute_resident_ivm_fragment(&flows, &encode_ivm_detach_fragment("job-r"))
            .await
            .unwrap();
        let after = super::execute_resident_ivm_fragment(&flows, &tick(vec![1.0], 3)).await;
        assert!(after.is_err(), "tick after detach must error");
    }

    /// The stateless fragment round-trip: encode a tick on the coordinator side,
    /// execute it on the executor, decode the result — and confirm it matches a
    /// plain central tick. Also confirms statelessness: running the same fragment
    /// twice does not accumulate (no per-job state on the executor).
    #[tokio::test]
    async fn fragment_round_trip_matches_central_and_is_stateless() {
        // Baseline tick 1 on a coordinator flow.
        let coord = IncrementalFlow::new();
        coord.register_view(sum_view_spec()).unwrap();
        coord
            .feed(
                "sales",
                DeltaBatch::from_inserts(sales_batch(&[100.0, 200.0, 50.0])).unwrap(),
            )
            .unwrap();
        coord.step_datafusion().await.unwrap();

        // Tick 2 delta.
        let delta = DeltaBatch::from_inserts(sales_batch(&[25.0, 10.0])).unwrap();
        coord.feed("sales", delta.clone()).unwrap();

        // Central reference tick 2.
        let central = IncrementalFlow::new();
        central.register_view(sum_view_spec()).unwrap();
        central
            .feed(
                "sales",
                DeltaBatch::from_inserts(sales_batch(&[100.0, 200.0, 50.0])).unwrap(),
            )
            .unwrap();
        central.step_datafusion().await.unwrap();
        central.feed("sales", delta.clone()).unwrap();
        central.step_datafusion().await.unwrap();

        // Build the dispatch fragment (what the coordinator sends).
        let local_pending = coord.take_pending().unwrap();
        let dispatch_deltas = krishiv_ivm::coalesce_pending(local_pending.clone()).unwrap();
        let state = coord.checkpoint_full().unwrap();
        let specs = coord.view_specs().unwrap();
        let fragment = encode_ivm_step_fragment("job-1", &dispatch_deltas, &specs, &state).unwrap();

        // Execute on the (stateless) executor — twice, to prove no accumulation.
        let (summary1, blob1) = execute_ivm_fragment(&fragment).await.unwrap();
        let (_summary2, blob2) = execute_ivm_fragment(&fragment).await.unwrap();

        let out1 = decode_batch_map(blob1.as_ref().unwrap()).unwrap();
        let out2 = decode_batch_map(blob2.as_ref().unwrap()).unwrap();

        // Both runs produce the same full output (stateless: no accumulation).
        assert!(
            (total_of(&out1) - 385.0).abs() < 1e-9,
            "first run total wrong"
        );
        assert!(
            (total_of(&out2) - 385.0).abs() < 1e-9,
            "second run total must match (stateless)"
        );
        // The tick produced real output (not fabricated zeros).
        assert!(
            summary1.total_output_rows > 0,
            "summary must report real output rows"
        );
    }
}
