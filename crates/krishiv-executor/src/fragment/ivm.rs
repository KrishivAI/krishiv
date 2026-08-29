#![forbid(unsafe_code)]

//! Resident IVM (DeltaBatch) fragment execution — coordinator-authoritative.
//!
//! The coordinator is the single source of truth for an IVM job. Full state
//! ships to an executor ONCE, at `delta:attach:`; every tick afterwards carries
//! only that tick's input deltas plus a fence, and the executor answers with
//! per-view **output deltas** — never snapshots. The flow lives in this
//! process across ticks (`ResidentIvmFlows`), which is what keeps compiled view
//! plans and operator accumulators warm.
//!
//! ```text
//! delta:attach:{job}|{specs_b64}|{state_b64}|{fence}  → capability echo blob
//! delta:tick:{job}|{deltas_b64}|{fence}               → tick result blob
//! delta:detach:{job}                                  → none
//! ```
//!
//! Every payload part is base64-encoded, so a `|` inside a SQL string literal
//! in `body_sql` cannot corrupt the framing. `state_b64` is the base64 of
//! `IncrementalFlow::checkpoint_full`.
//!
//! The executor holds no authority: a failed or reassigned tick makes the
//! coordinator re-feed the pending deltas and compute centrally, and the fence
//! turns a replay or a gap into an error instead of a double-apply. See
//! `submit_resident_ivm_step` in `krishiv-scheduler`.
//!
//! # Wire versions (IVM-AUD-INT-F19, IVM-AUD-A5-RESIDENT)
//!
//! The tick payload comes in two dialects and this module answers in whichever
//! one it was asked in:
//!
//! | input `deltas_b64`             | result blob                        |
//! |--------------------------------|------------------------------------|
//! | base64(JSON(base64(IPC)))      | `IVMD1` delta map, no health       |
//! | base64(`IVMD1` delta map)      | `IVMD2` tick result, with health   |
//!
//! The input dialect is an unforgeable statement of what the coordinator can
//! read, so a rolled-forward executor answering a not-yet-upgraded coordinator
//! cannot hand it a blob it would reject. The `delta:attach:` reply carries the
//! capability echo the coordinator uses to pick the dialect in the first place.
//!
//! The **stateless** `delta:step:` path — a full `checkpoint_full` shipped per
//! tick, answered with each view's full materialized output — was deleted under
//! IVM-AUD-INT-F20: no coordinator in any released version ever sent one. A
//! `delta:step:` fragment now fails loudly here rather than executing.

use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use krishiv_ivm::{
    DeltaBatch, IncrementalFlow, IncrementalViewSpec, TickHealth, WireCapabilities,
    decode_delta_map, deserialize_delta_batch, encode_attach_echo, encode_delta_map,
    encode_tick_result,
};
use serde::Deserialize;

// ── fragment wire types ───────────────────────────────────────────────────────

/// One entry of the **legacy** JSON tick payload (base64(JSON(base64(IPC)))).
///
/// Still read so a new executor can serve a coordinator that has not been
/// upgraded yet; the binary payload does not go through serde at all.
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
    /// IVM-AUD-DIST-4: run this flow on the O(state) DiffBased path instead of
    /// an incremental plan. Flow-level despite living per spec — see
    /// `encode_specs_b64`. Defaults false, so a fragment produced before this
    /// field existed keeps the incremental behaviour it had.
    #[serde(default)]
    pub force_diff_based: bool,
}

#[derive(Debug, Deserialize)]
pub struct SchemaFieldJson {
    pub name: String,
    pub data_type: String,
    #[serde(default)]
    pub nullable: bool,
}

/// Parse one Arrow `DataType` from the wire string. The coordinator encodes
/// each field with `format!("{:?}", data_type)` (see `encode_specs_b64`), so
/// this must round-trip the Arrow **Debug** representation — including the
/// `Utf8View`/`BinaryView` types DataFusion 54 emits by default for string and
/// binary columns, and the `Timestamp(<unit>, <tz>)` Debug form. A gap here
/// silently drops the view (its output schema won't parse), which surfaces as
/// an empty coordinator snapshot; keep it faithful to the encoder.
fn parse_data_type(s: &str) -> Option<DataType> {
    Some(match s {
        "Int8" => DataType::Int8,
        "Int16" => DataType::Int16,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "UInt8" => DataType::UInt8,
        "UInt16" => DataType::UInt16,
        "UInt32" => DataType::UInt32,
        "UInt64" => DataType::UInt64,
        "Float16" => DataType::Float16,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "Utf8" => DataType::Utf8,
        "LargeUtf8" => DataType::LargeUtf8,
        // DataFusion 54 default string/binary representations.
        "Utf8View" => DataType::Utf8View,
        "BinaryView" => DataType::BinaryView,
        "Boolean" => DataType::Boolean,
        "Binary" => DataType::Binary,
        "LargeBinary" => DataType::LargeBinary,
        // Legacy short aliases (kept for backward-compatible fragments).
        "TimestampMs" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "TimestampUs" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "Date32" => DataType::Date32,
        "Date64" => DataType::Date64,
        // Arrow Debug form: `Timestamp(<Unit>, None)` / `Timestamp(<Unit>, Some("<tz>"))`.
        other if other.starts_with("Timestamp(") => return parse_timestamp_debug(other),
        _ => return None,
    })
}

/// Parse the Arrow Debug form of a `Timestamp` data type, e.g.
/// `Timestamp(Millisecond, None)` or `Timestamp(Microsecond, Some("UTC"))`.
fn parse_timestamp_debug(s: &str) -> Option<DataType> {
    let inner = s.strip_prefix("Timestamp(")?.strip_suffix(')')?;
    let (unit_s, tz_s) = inner.split_once(',')?;
    let unit = match unit_s.trim() {
        "Second" => TimeUnit::Second,
        "Millisecond" => TimeUnit::Millisecond,
        "Microsecond" => TimeUnit::Microsecond,
        "Nanosecond" => TimeUnit::Nanosecond,
        _ => return None,
    };
    let tz_s = tz_s.trim();
    let tz = if tz_s == "None" {
        None
    } else {
        // `Some("UTC")` → UTC
        let q = tz_s.strip_prefix("Some(")?.strip_suffix(')')?;
        Some(q.trim_matches('"').to_string().into())
    };
    Some(DataType::Timestamp(unit, tz))
}

fn parse_schema_fields(fields: &[SchemaFieldJson]) -> Option<SchemaRef> {
    let arrow_fields: Option<Vec<Field>> = fields
        .iter()
        .map(|f| {
            let dt = parse_data_type(&f.data_type)?;
            Some(Field::new(f.name.clone(), dt, f.nullable))
        })
        .collect();
    Some(std::sync::Arc::new(Schema::new(arrow_fields?)))
}

// ── fragment prefixes ─────────────────────────────────────────────────────────

/// Resident protocol (AUD-6): create/replace a resident flow (state ships once).
const IVM_ATTACH_PREFIX: &str = "delta:attach:";
/// Resident protocol: feed deltas + step the resident flow (fence-guarded).
const IVM_TICK_PREFIX: &str = "delta:tick:";
/// Resident protocol: drop the resident flow.
const IVM_DETACH_PREFIX: &str = "delta:detach:";

/// What this build can do on the resident tick wire. Echoed back to the
/// coordinator at attach so it can pick a dialect both ends understand.
const WIRE_CAPABILITIES: WireCapabilities = WireCapabilities {
    binary_input_deltas: true,
    tick_health: true,
};

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

fn register_specs_on_flow(
    flow: &IncrementalFlow,
    view_specs: &[ViewSpecJson],
) -> Result<(), String> {
    for vs in view_specs {
        // Fail loud on an unparseable output schema: a silently skipped view
        // produces no output delta, which the coordinator mirror reads as an
        // empty snapshot — a ghost failure. Surface it as a tick error instead.
        let schema = parse_schema_fields(&vs.output_schema_fields).ok_or_else(|| {
            format!(
                "view '{}' has an unparseable output schema in the wire fragment: {:?} \
                 (executor parse_data_type is missing an arm for the encoder's Debug form)",
                vs.name, vs.output_schema_fields
            )
        })?;
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

/// Which dialect a `delta:tick:` payload was written in.
///
/// The executor answers in the same one, which is what makes a rolled-forward
/// executor safe against a not-yet-upgraded coordinator (IVM-AUD-INT-F19).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickWireDialect {
    /// base64(JSON(base64(IPC))) — pre-IVMD2 coordinators.
    LegacyJson,
    /// base64(`IVMD1` delta map) — 25% smaller, one fewer buffer copy.
    Binary,
}

/// Decode a `delta:tick:` payload in either dialect.
///
/// The sniff is on the `IVMD1` magic, which the legacy payload can never carry
/// because it is a bare JSON array — `legacy_json_payload_never_looks_like_binary`
/// in `krishiv-ivm` asserts that rather than trusting this comment.
///
/// Both branches apply `drop_zeros()`. `decode_delta_map` does not, the JSON
/// path always did, and `IncrementalFlow::feed` does not either — so without it
/// here the two dialects would put different rows into `pending` for the same
/// tick. A weight-0 row is ABSENT in a Z-set, and while it contributes nothing
/// to this tick's output (source materialization clamps at 0), it is retained:
/// it counts against the INT-F11 backlog cap and lands in the delta-checkpoint
/// accumulator. `binary_payload_drops_zero_weight_rows_like_the_json_one`
/// asserts it at this boundary, where the difference actually exists, rather
/// than downstream where it does not.
fn decode_tick_deltas(
    deltas_b64: &str,
) -> Result<(TickWireDialect, Vec<(String, DeltaBatch)>), String> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let raw = b64
        .decode(deltas_b64)
        .map_err(|e| format!("deltas b64: {e}"))?;

    if raw.starts_with(b"IVMD1") {
        let map = decode_delta_map(&raw).map_err(|e| format!("deltas binary: {e}"))?;
        let mut out = Vec::with_capacity(map.len());
        for (source, delta) in map {
            let delta = delta.drop_zeros().map_err(|e| e.to_string())?;
            out.push((source, delta));
        }
        return Ok((TickWireDialect::Binary, out));
    }

    let deltas_str = std::str::from_utf8(&raw).map_err(|e| format!("deltas utf8: {e}"))?;
    let entries: Vec<PendingDeltaJson> =
        serde_json::from_str(deltas_str).map_err(|e| format!("deltas json: {e}"))?;
    let mut out = Vec::with_capacity(entries.len());
    for pd in entries {
        let ipc_bytes = b64
            .decode(&pd.delta_b64)
            .map_err(|e| format!("base64 decode delta for '{}': {e}", pd.source))?;
        let delta = deserialize_delta_batch(&ipc_bytes)
            .map_err(|e| e.to_string())?
            .drop_zeros()
            .map_err(|e| e.to_string())?;
        out.push((pd.source, delta));
    }
    Ok((TickWireDialect::LegacyJson, out))
}

/// Execute a resident-protocol IVM fragment against the executor's flow map.
///
/// Returns `(StepSummary, Option<blob>)`. The blob is:
/// - `delta:attach:` → the capability echo (`encode_attach_echo`)
/// - `delta:tick:`   → the tick result: `encode_tick_result` (`IVMD2`, deltas +
///   health) for a binary payload, `encode_delta_map` (`IVMD1`, deltas only)
///   for a legacy JSON one
/// - `delta:detach:` → `None`
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
        // the point of residency (the accumulators live here and never need to
        // transfer per tick), so incremental is the DEFAULT and stays the
        // default when the wire says nothing. IVM-AUD-DIST-4 added the one
        // exception below: an explicit request for the O(state) recompute arm,
        // which exists so a distributed A/B can vary the mode without also
        // varying the route.
        let flow = IncrementalFlow::new();
        // IVM-AUD-DIST-4: the recompute arm of a distributed A/B. Without this
        // the wire could only express the incremental path, so "delta vs batch
        // on the cluster" had no batch side to measure and any comparison had
        // to change the ROUTE as well as the mode — which is exactly how the
        // retracted 28.5x sharding claim happened (register §68).
        if view_specs.iter().any(|vs| vs.force_diff_based) {
            flow.force_diff_based().map_err(|e| e.to_string())?;
        }
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
        // The coordinator reads this to decide which tick dialect to send. An
        // executor that predates it answers `None`, which decodes fail-closed
        // to the legacy wire (`krishiv_ivm::decode_attach_echo`).
        return Ok((
            krishiv_ivm::StepSummary::default(),
            Some(encode_attach_echo(WIRE_CAPABILITIES)),
        ));
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
        let (dialect, pending) = decode_tick_deltas(deltas_b64)?;
        for (source, delta) in pending {
            resident
                .flow
                .feed(source, delta)
                .map_err(|e| e.to_string())?;
        }
        let summary = crate::erased(resident.flow.step_datafusion())
            .await
            .map_err(|e| e.to_string())?;
        resident.fence = fence;

        // AUD-6 exit contract: return per-view OUTPUT DELTAS, never snapshots.
        let mut outputs: HashMap<String, DeltaBatch> = HashMap::new();
        for name in resident.flow.view_names().map_err(|e| e.to_string())? {
            if let Some(delta) = resident
                .flow
                .take_step_output(&name)
                .map_err(|e| e.to_string())?
            {
                outputs.insert(name, delta);
            }
        }

        // IVM-AUD-A5-RESIDENT: `summary` has held real per-view health all
        // along — `step_datafusion` fills `degraded_views` / `errored_views` —
        // and the v1 wire simply had nowhere to put it, so the coordinator's
        // mirror invented empty vectors and every resident tick had to be
        // reported as "health not available". v2 carries the real thing.
        //
        // Answer in the dialect the tick was written in. A coordinator that
        // sent JSON cannot read `IVMD2`, and handing it one would fail the
        // decode, mark the job detached, and cost a full `checkpoint_full`
        // re-attach on the next tick — per tick, for the length of a rollout.
        let blob = match dialect {
            TickWireDialect::Binary => {
                encode_tick_result(&outputs, &TickHealth::from_summary(&summary))
                    .map_err(|e| e.to_string())?
            }
            TickWireDialect::LegacyJson => encode_delta_map(&outputs).map_err(|e| e.to_string())?,
        };
        return Ok((summary, Some(blob)));
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_ivm::{
        DeltaBatch, IncrementalFlow, IncrementalViewSpec, decode_tick_result,
        encode_ivm_attach_fragment, encode_ivm_tick_fragment, serialize_delta_batch,
    };

    fn sales_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Float64,
            false,
        )]))
    }

    /// The wire encoder ships each field type as `format!("{:?}", data_type)`;
    /// `parse_data_type` must round-trip that Debug form for every type an IVM
    /// view output can carry — notably the DataFusion 54 `Utf8View` default and
    /// the `Timestamp(<unit>, <tz>)` form (windowed views).
    #[test]
    fn parse_data_type_round_trips_debug_form() {
        use arrow::datatypes::{DataType, TimeUnit};
        let cases = [
            DataType::Int64,
            DataType::Float64,
            DataType::Utf8,
            DataType::Utf8View,
            DataType::BinaryView,
            DataType::Boolean,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        ];
        for dt in cases {
            let encoded = format!("{dt:?}");
            assert_eq!(
                super::parse_data_type(&encoded),
                Some(dt.clone()),
                "parse_data_type must round-trip the encoder's Debug form {encoded:?}"
            );
        }
        assert_eq!(super::parse_data_type("NoSuchType"), None);
    }

    /// Repro of the live distributed regression: a resident flow attached with
    /// EMPTY state, then ticked with a **GROUP BY** aggregate delta, must return
    /// a non-empty per-view output delta (the mirror skips empty deltas, so an
    /// empty return leaves the coordinator snapshot empty → `snapshot()` None).
    #[tokio::test]
    async fn resident_group_by_aggregate_first_tick_emits_delta() {
        use arrow::array::StringArray;

        fn orders_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Float64, false),
            ]))
        }
        fn orders_batch(regions: &[&str], amounts: &[f64]) -> RecordBatch {
            RecordBatch::try_new(
                orders_schema(),
                vec![
                    Arc::new(StringArray::from(regions.to_vec())),
                    Arc::new(Float64Array::from(amounts.to_vec())),
                ],
            )
            .unwrap()
        }
        // The output schema uses Utf8View for `region` — exactly what DataFusion
        // 54 emits and the coordinator ships (`format!("{:?}")` → "Utf8View").
        // Before the parse_data_type fix this view was silently dropped on the
        // executor (unparseable schema), yielding an empty coordinator snapshot.
        let group_spec = IncrementalViewSpec {
            name: "rev".into(),
            body_sql: "SELECT region, SUM(amount) AS total FROM orders GROUP BY region".into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8View, true),
                Field::new("total", DataType::Float64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        };

        let flows: super::ResidentIvmFlows = Arc::new(dashmap::DashMap::new());
        let specs = vec![group_spec];
        let attach = encode_ivm_attach_fragment("job-g", &specs, &[], 0, false).unwrap();
        super::execute_resident_ivm_fragment(&flows, &attach)
            .await
            .unwrap();

        let mut pending = std::collections::HashMap::new();
        pending.insert(
            "orders".to_string(),
            DeltaBatch::from_inserts(orders_batch(
                &["us", "eu", "us", "ap"],
                &[100.0, 50.0, 25.0, 75.0],
            ))
            .unwrap(),
        );
        let tick = encode_ivm_tick_fragment("job-g", &pending, 1, true).unwrap();
        let (summary, blob) = super::execute_resident_ivm_fragment(&flows, &tick)
            .await
            .unwrap();
        let d = decode_tick_result(blob.as_ref().unwrap())
            .unwrap()
            .view_deltas;
        assert!(
            summary.total_output_rows > 0,
            "GROUP BY first tick must report output rows, got summary {summary:?}"
        );
        let out = d
            .get("rev")
            .expect("GROUP BY view must emit an output delta on first tick");
        assert!(
            out.weights().iter().flatten().any(|w| w > 0),
            "first tick GROUP BY output must contain insertions; got {out:?}"
        );
    }

    /// IVM-AUD-DIST-4. The wire had no way to ask for the O(state) recompute
    /// path, so a distributed "delta vs batch" comparison had no batch arm and
    /// could only be faked by changing the ROUTE (central vs resident) — which
    /// is how the retracted 28.5x sharding claim happened (register §68).
    ///
    /// Two things must hold, and the second is the one that makes the A/B mean
    /// anything: the flag must ARRIVE, and the recompute arm must produce the
    /// SAME ANSWER as the incremental arm. A batch arm that is merely faster
    /// because it computes something else is not a baseline.
    #[tokio::test]
    async fn the_wire_can_ask_for_the_recompute_arm_and_it_agrees_with_incremental() {
        async fn run(force_diff_based: bool) -> (bool, f64) {
            let flows: super::ResidentIvmFlows = Arc::new(dashmap::DashMap::new());
            let specs = vec![sum_view_spec()];
            let attach =
                encode_ivm_attach_fragment("job-fdb", &specs, &[], 0, force_diff_based).unwrap();
            super::execute_resident_ivm_fragment(&flows, &attach)
                .await
                .unwrap();

            let entry = flows
                .get("job-fdb")
                .expect("attach must register the flow")
                .value()
                .clone();
            let observed = entry.lock().await.flow.is_force_diff_based().unwrap();

            // Two ticks, so the incremental arm is genuinely maintaining state
            // across a tick rather than computing a single batch once.
            let mut total = f64::NAN;
            for (fence, amounts) in [(1u64, vec![100.0, 50.0]), (2, vec![25.0])] {
                let mut pending = std::collections::HashMap::new();
                pending.insert(
                    "sales".to_string(),
                    DeltaBatch::from_inserts(sales_batch(&amounts)).unwrap(),
                );
                let tick = encode_ivm_tick_fragment("job-fdb", &pending, fence, true).unwrap();
                let (_summary, blob) = super::execute_resident_ivm_fragment(&flows, &tick)
                    .await
                    .unwrap();
                if let Some(d) = decode_tick_result(blob.as_ref().unwrap())
                    .unwrap()
                    .view_deltas
                    .get("total_sales")
                {
                    total = total_from_delta(d);
                }
            }
            (observed, total)
        }

        let (incremental_flag, incremental_total) = run(false).await;
        let (recompute_flag, recompute_total) = run(true).await;

        assert!(
            !incremental_flag,
            "absent/false on the wire must leave the resident flow incremental"
        );
        assert!(
            recompute_flag,
            "force_diff_based on the wire must reach the resident flow; without \
             it the cluster has no batch arm to measure against"
        );
        assert_eq!(
            incremental_total, 175.0,
            "incremental arm must total 100+50+25"
        );
        assert_eq!(
            recompute_total, incremental_total,
            "the recompute arm must agree with the incremental arm — a batch \
             baseline that computes a different answer measures nothing"
        );
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

    /// Phase 57 (AUD-6) resident protocol: state attaches ONCE, ticks carry
    /// deltas + fences only, results are per-view OUTPUT DELTAS, and state
    /// accumulates across ticks on the executor (the whole point of residency).
    /// A replayed fence is rejected instead of double-applying.
    #[tokio::test]
    async fn resident_flow_accumulates_across_ticks_and_returns_deltas() {
        use krishiv_ivm::encode_ivm_detach_fragment;

        let flows: super::ResidentIvmFlows = Arc::new(dashmap::DashMap::new());
        let specs = vec![sum_view_spec()];

        // Attach with EMPTY state (fresh job promotion) at fence 0.
        let attach = encode_ivm_attach_fragment("job-r", &specs, &[], 0, false).unwrap();
        super::execute_resident_ivm_fragment(&flows, &attach)
            .await
            .unwrap();

        let tick = |amounts: Vec<f64>, fence: u64| {
            let mut pending = std::collections::HashMap::new();
            pending.insert(
                "sales".to_string(),
                DeltaBatch::from_inserts(sales_batch(&amounts)).unwrap(),
            );
            encode_ivm_tick_fragment("job-r", &pending, fence, true).unwrap()
        };

        // Tick 1: 100+200 → total 300 (all-insert first output).
        let (_s1, blob1) =
            super::execute_resident_ivm_fragment(&flows, &tick(vec![100.0, 200.0], 1))
                .await
                .unwrap();
        let d1 = decode_tick_result(blob1.as_ref().unwrap())
            .unwrap()
            .view_deltas;
        let out1 = d1.get("total_sales").expect("view emitted a delta");
        assert!(
            out1.weights().iter().flatten().any(|w| w > 0),
            "first tick output must contain an insertion"
        );

        // Tick 2: +50 → the RESIDENT state accumulates: retract 300, insert 350.
        let (_s2, blob2) = super::execute_resident_ivm_fragment(&flows, &tick(vec![50.0], 2))
            .await
            .unwrap();
        let d2 = decode_tick_result(blob2.as_ref().unwrap())
            .unwrap()
            .view_deltas;
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

        // Detach drops the flow; the next tick errors (needs re-attach).
        super::execute_resident_ivm_fragment(&flows, &encode_ivm_detach_fragment("job-r"))
            .await
            .unwrap();
        let after = super::execute_resident_ivm_fragment(&flows, &tick(vec![1.0], 3)).await;
        assert!(after.is_err(), "tick after detach must error");
    }

    fn total_from_delta(d: &DeltaBatch) -> f64 {
        let data = d.data_batch();
        let weights = d.weights();
        let totals = data
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        (0..data.num_rows())
            .filter(|i| weights.value(*i) > 0)
            .map(|i| totals.value(i))
            .next_back()
            .unwrap_or(f64::NAN)
    }

    async fn attach(flows: &super::ResidentIvmFlows, job: &str, state: &[u8]) -> Option<Vec<u8>> {
        let specs = vec![sum_view_spec()];
        let frag = encode_ivm_attach_fragment(job, &specs, state, 0, false).unwrap();
        super::execute_resident_ivm_fragment(flows, &frag)
            .await
            .unwrap()
            .1
    }

    fn sales_pending(amounts: &[f64]) -> std::collections::HashMap<String, DeltaBatch> {
        let mut pending = std::collections::HashMap::new();
        pending.insert(
            "sales".to_string(),
            DeltaBatch::from_inserts(sales_batch(amounts)).unwrap(),
        );
        pending
    }

    /// Hand-build the legacy payload rather than going through the encoder, so
    /// this cannot quietly become a tautology once the encoder switches
    /// dialects.
    fn legacy_json_tick(job: &str, amounts: &[f64], fence: u64) -> String {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;
        let delta = DeltaBatch::from_inserts(sales_batch(amounts)).unwrap();
        let entries = serde_json::json!([{
            "source": "sales",
            "delta_b64": b64.encode(serialize_delta_batch(&delta).unwrap()),
        }]);
        let payload = b64.encode(serde_json::to_string(&entries).unwrap());
        format!("delta:tick:{job}|{payload}|{fence}")
    }

    /// T10 (IVM-AUD-INT-F19). Attach answers with the capability echo the
    /// coordinator negotiates on. Before this it answered `None` and there was
    /// nothing to negotiate with.
    #[tokio::test]
    async fn attach_echoes_wire_capabilities() {
        let flows: super::ResidentIvmFlows = Arc::new(dashmap::DashMap::new());
        let echo = attach(&flows, "job-e", &[]).await.expect("capability echo");
        assert!(echo.starts_with(b"IVMW"), "echo magic: {echo:?}");
        let caps = krishiv_ivm::decode_attach_echo(Some(&echo));
        assert!(caps.binary_input_deltas);
        assert!(caps.tick_health);
    }

    /// T6 (IVM-AUD-INT-F19). The binary tick payload computes the same tick as
    /// the JSON one. Before the sniffing decoder this fragment died in
    /// `std::str::from_utf8` on the IPC bytes.
    #[tokio::test]
    async fn resident_tick_accepts_binary_delta_payload() {
        let bin_flows: super::ResidentIvmFlows = Arc::new(dashmap::DashMap::new());
        attach(&bin_flows, "job-b", &[]).await;
        let frag =
            encode_ivm_tick_fragment("job-b", &sales_pending(&[100.0, 200.0]), 1, true).unwrap();
        let (_s, blob) = super::execute_resident_ivm_fragment(&bin_flows, &frag)
            .await
            .unwrap();
        let bin = decode_tick_result(blob.as_ref().unwrap()).unwrap();

        let json_flows: super::ResidentIvmFlows = Arc::new(dashmap::DashMap::new());
        attach(&json_flows, "job-j", &[]).await;
        let (_s, blob) = super::execute_resident_ivm_fragment(
            &json_flows,
            &legacy_json_tick("job-j", &[100.0, 200.0], 1),
        )
        .await
        .unwrap();
        let json = decode_tick_result(blob.as_ref().unwrap()).unwrap();

        assert_eq!(
            total_from_delta(bin.view_deltas.get("total_sales").unwrap()),
            total_from_delta(json.view_deltas.get("total_sales").unwrap()),
        );
        assert_eq!(
            total_from_delta(bin.view_deltas.get("total_sales").unwrap()),
            300.0
        );
    }

    /// The binary branch must keep the `drop_zeros()` the JSON branch always
    /// did: a weight-0 row is ABSENT in a Z-set, and admitting it on one
    /// dialect and not the other makes the same tick two different ticks.
    ///
    /// Asserted at the decoder, not through a tick: a zero-weight row changes
    /// nothing about a tick's OUTPUT (source materialization clamps negative
    /// and zero weights away), so a tick-level assertion passes with or without
    /// the `drop_zeros` call — the first version of this test did exactly that
    /// and had to be moved down a layer before it could go red.
    #[test]
    fn binary_payload_drops_zero_weight_rows_like_the_json_one() {
        use arrow::array::Int64Array;
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;

        let weighted = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("amount", DataType::Float64, false),
                Field::new("_weight", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
                Arc::new(Int64Array::from(vec![0i64, 1, 0])),
            ],
        )
        .unwrap();
        let zeroed = DeltaBatch::from_weighted(weighted).unwrap();
        assert_eq!(zeroed.num_rows(), 3, "fixture carries two zero-weight rows");

        let mut map = std::collections::HashMap::new();
        map.insert("sales".to_string(), zeroed.clone());
        let binary = b64.encode(krishiv_ivm::encode_delta_map(&map).unwrap());
        let json = b64.encode(
            serde_json::to_string(&serde_json::json!([{
                "source": "sales",
                "delta_b64": b64.encode(serialize_delta_batch(&zeroed).unwrap()),
            }]))
            .unwrap(),
        );

        for (label, payload) in [("binary", binary), ("json", json)] {
            let (_dialect, decoded) = super::decode_tick_deltas(&payload).unwrap();
            assert_eq!(
                decoded[0].1.num_rows(),
                1,
                "{label} payload must drop both zero-weight rows before feed()"
            );
        }
    }

    /// T7 (IVM-AUD-INT-F19). The rollout-window guard: a coordinator that has
    /// not been upgraded still gets its ticks executed.
    #[tokio::test]
    async fn resident_tick_still_accepts_legacy_json_payload() {
        let flows: super::ResidentIvmFlows = Arc::new(dashmap::DashMap::new());
        attach(&flows, "job-l", &[]).await;
        let (_s, blob) =
            super::execute_resident_ivm_fragment(&flows, &legacy_json_tick("job-l", &[7.0], 1))
                .await
                .expect("a legacy JSON tick must still execute");
        let out = decode_tick_result(blob.as_ref().unwrap()).unwrap();
        assert_eq!(
            total_from_delta(out.view_deltas.get("total_sales").unwrap()),
            7.0
        );
    }

    /// T9 (IVM-AUD-INT-F19). The new-executor/old-coordinator guard: answer in
    /// the dialect the tick was written in. An `IVMD2` blob would fail the old
    /// coordinator's `decode_delta_map` magic check, which marks the job
    /// detached and costs a full `checkpoint_full` re-attach — every tick, for
    /// the length of the rollout.
    #[tokio::test]
    async fn resident_tick_answers_v1_when_input_was_json() {
        let flows: super::ResidentIvmFlows = Arc::new(dashmap::DashMap::new());
        attach(&flows, "job-v", &[]).await;
        let (_s, blob) =
            super::execute_resident_ivm_fragment(&flows, &legacy_json_tick("job-v", &[1.0], 1))
                .await
                .unwrap();
        let blob = blob.unwrap();
        assert!(
            blob.starts_with(b"IVMD1"),
            "a JSON tick must be answered in v1; got magic {:?}",
            String::from_utf8_lossy(&blob[..5])
        );
        assert!(
            decode_tick_result(&blob).unwrap().health.is_none(),
            "and therefore with no health section"
        );
    }

    /// T8 — **the** IVM-AUD-A5-RESIDENT test. A per-view failure crosses the
    /// resident wire for the first time. Before this the tick branch called
    /// `encode_delta_map`, so the real `errored_views` the tick produced was
    /// computed and then dropped on the floor.
    #[tokio::test]
    async fn resident_tick_reports_failed_view_health() {
        let broken = IncrementalViewSpec {
            name: "broken".into(),
            body_sql: "SELECT amount, no_such_column FROM sales".into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("amount", DataType::Float64, true),
                Field::new("no_such_column", DataType::Float64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        };
        let flows: super::ResidentIvmFlows = Arc::new(dashmap::DashMap::new());
        let specs = vec![sum_view_spec(), broken];
        let frag = encode_ivm_attach_fragment("job-h", &specs, &[], 0, false).unwrap();
        super::execute_resident_ivm_fragment(&flows, &frag)
            .await
            .unwrap();

        let tick = encode_ivm_tick_fragment("job-h", &sales_pending(&[5.0]), 1, true).unwrap();
        let (_s, blob) = super::execute_resident_ivm_fragment(&flows, &tick)
            .await
            .expect("a failing view must not fail the tick");
        let health = decode_tick_result(blob.as_ref().unwrap())
            .unwrap()
            .health
            .expect("executor reported health");
        let e = health
            .errored_views
            .iter()
            .find(|e| e.view == "broken")
            .unwrap_or_else(|| panic!("the failed view must be named: {health:?}"));
        assert_eq!(e.kind, "view_sql");
        assert!(!e.message.is_empty(), "the failure must carry its message");
        // The healthy view still produced its output on the same tick.
        let out = decode_tick_result(blob.as_ref().unwrap()).unwrap();
        assert_eq!(
            total_from_delta(out.view_deltas.get("total_sales").unwrap()),
            5.0
        );
    }

    /// The healthy tick must not fabricate failures: a v2 result whose health
    /// is present and EMPTY is a real "nothing failed", and it has to be
    /// distinguishable from "no health available" (which is `None`).
    #[tokio::test]
    async fn a_healthy_resident_tick_reports_empty_health_not_absent_health() {
        let flows: super::ResidentIvmFlows = Arc::new(dashmap::DashMap::new());
        attach(&flows, "job-ok", &[]).await;
        let tick = encode_ivm_tick_fragment("job-ok", &sales_pending(&[3.0]), 1, true).unwrap();
        let (_s, blob) = super::execute_resident_ivm_fragment(&flows, &tick)
            .await
            .unwrap();
        let health = decode_tick_result(blob.as_ref().unwrap())
            .unwrap()
            .health
            .expect("a v2 answer always carries a health section");
        assert!(health.errored_views.is_empty());
        assert_eq!(health.errored_omitted, 0);
    }

    /// Replaces the statelessness assertion that died with `execute_ivm_fragment`
    /// (IVM-AUD-INT-F20): that test was the only cover for `restore_full` into a
    /// fresh flow. Attaching the same shipped state twice must produce the same
    /// tick result, and the restored baselines must actually be in effect —
    /// a restored total of 300 makes the next tick RETRACT 300 and insert 350,
    /// which an unrestored flow cannot do.
    #[tokio::test]
    async fn attach_restores_shipped_state_and_replaces_it_deterministically() {
        let coord = IncrementalFlow::new();
        coord.register_view(sum_view_spec()).unwrap();
        coord
            .feed(
                "sales",
                DeltaBatch::from_inserts(sales_batch(&[100.0, 200.0])).unwrap(),
            )
            .unwrap();
        coord.step_datafusion().await.unwrap();
        let state = coord.checkpoint_full().unwrap();

        let run = async |flows: &super::ResidentIvmFlows| {
            attach(flows, "job-s", &state).await;
            let tick = encode_ivm_tick_fragment("job-s", &sales_pending(&[50.0]), 1, true).unwrap();
            let (_s, blob) = super::execute_resident_ivm_fragment(flows, &tick)
                .await
                .unwrap();
            decode_tick_result(blob.as_ref().unwrap())
                .unwrap()
                .view_deltas
                .remove("total_sales")
                .expect("view emitted a delta")
        };

        let flows: super::ResidentIvmFlows = Arc::new(dashmap::DashMap::new());
        let first = run(&flows).await;
        // Re-attaching REPLACES the resident flow, so the identical tick must
        // give the identical answer — no accumulation across attaches.
        let second = run(&flows).await;

        for d in [&first, &second] {
            let data = d.data_batch();
            let weights = d.weights();
            let totals = data
                .column_by_name("total")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let mut retract_300 = false;
            let mut insert_350 = false;
            for i in 0..data.num_rows() {
                if weights.value(i) < 0 && (totals.value(i) - 300.0).abs() < 1e-9 {
                    retract_300 = true;
                }
                if weights.value(i) > 0 && (totals.value(i) - 350.0).abs() < 1e-9 {
                    insert_350 = true;
                }
            }
            assert!(
                retract_300 && insert_350,
                "the shipped state must be restored into the resident flow \
                 (retract 300, insert 350); got {d:?}"
            );
        }
    }

    /// IVM-AUD-INT-F20: the stateless `delta:step:` path is gone, and a
    /// fragment that asks for it must fail loudly rather than find a half of it
    /// still wired up.
    #[tokio::test]
    async fn a_stateless_delta_step_fragment_is_refused() {
        let flows: super::ResidentIvmFlows = Arc::new(dashmap::DashMap::new());
        for gone in ["delta:step:j|d|s|st", "delta:ckpt:j"] {
            let err = super::execute_resident_ivm_fragment(&flows, gone)
                .await
                .expect_err("neither verb executes anywhere any more");
            assert!(err.contains("not a resident IVM fragment"), "{err}");
        }
    }
}
