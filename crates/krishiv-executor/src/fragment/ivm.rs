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
//! be lost. See `submit_distributed_ivm_step` in `krishiv-scheduler`.
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

// ── fragment prefix ───────────────────────────────────────────────────────────

/// Prefix for IVM step fragments.
pub const IVM_FRAGMENT_PREFIX: &str = "delta:step:";

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
    let _job_id = parts[0];
    let deltas_b64 = parts[1];
    let specs_b64 = parts[2];
    let state_b64 = parts[3];

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
                lateness: vec![],
            };
            flow.register_view(spec).map_err(|e| e.to_string())?;
        }
    }
    flow.restore_full(&state_bytes)
        .map_err(|e| format!("restore_full: {e}"))?;
    // Force DiffBased: the transient flow's incremental-plan accumulators are
    // empty (not transferable), so only full SQL recompute + diff is correct.
    flow.force_diff_based()
        .map_err(|e| format!("force_diff_based: {e}"))?;

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
    let summary = flow.step_datafusion().await.map_err(|e| e.to_string())?;

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

    use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_ivm::{DeltaBatch, IncrementalFlow, IncrementalViewSpec, decode_batch_map};

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
