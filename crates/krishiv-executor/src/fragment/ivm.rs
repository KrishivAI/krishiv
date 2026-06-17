#![forbid(unsafe_code)]

//! IVM (DeltaBatch) fragment execution for distributed mode.
//!
//! The coordinator dispatches `delta:step:{job_id}|{encoded_deltas_b64}|{view_specs_json}`
//! task fragments to executors. Each executor maintains a long-lived
//! `IncrementalFlow` per job (keyed by job_id), applying deltas and running SQL
//! across ticks without re-creating the DataFusion context each time.
//!
//! # Fragment format
//!
//! ```text
//! delta:step:{job_id}|{pending_deltas_json}|{view_specs_json}
//! ```
//!
//! * `job_id` — matches the IVM job on the coordinator.
//! * `pending_deltas_json` — JSON array of `{"source": "...", "delta_b64": "..."}` objects.
//! * `view_specs_json` — JSON array of view spec objects (authoritative on each step so
//!   the executor can update its local registry if the coordinator registered new views).

use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use krishiv_ivm::{DeltaBatch, IncrementalFlow, IncrementalViewSpec, IvmError, deserialize_delta_batch};
use serde::Deserialize;

// ── per-job executor state ────────────────────────────────────────────────────

/// Long-lived IVM state held on the executor between ticks.
pub struct IvmJobState {
    pub flow: IncrementalFlow,
}

impl IvmJobState {
    pub fn new() -> Self {
        Self { flow: IncrementalFlow::new() }
    }
}

impl Default for IvmJobState {
    fn default() -> Self {
        Self::new()
    }
}

pub type IvmJobMap = Arc<DashMap<String, Arc<Mutex<IvmJobState>>>>;

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

fn parse_schema_fields(fields: &[SchemaFieldJson]) -> Option<arrow::datatypes::SchemaRef> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
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

// ── fragment prefix ───────────────────────────────────────────────────────────

/// Prefix for IVM step fragments.
pub const IVM_FRAGMENT_PREFIX: &str = "delta:step:";

/// Returns true if the fragment body is an IVM step fragment.
pub fn is_ivm_fragment(fragment: &str) -> bool {
    fragment.starts_with(IVM_FRAGMENT_PREFIX)
}

// ── execution ─────────────────────────────────────────────────────────────────

/// Execute one IVM tick for a `delta:step:` fragment.
///
/// Decodes pending deltas and (optional) view spec updates from the fragment
/// body, updates the executor's per-job `IncrementalFlow`, runs one tick,
/// and returns `Ok(active_views)`.
pub async fn execute_ivm_fragment(
    ivm_jobs: &IvmJobMap,
    fragment_body: &str,
) -> Result<krishiv_ivm::StepSummary, String> {
    // Fragment format: "delta:step:{job_id}|{deltas_json}|{specs_json}"
    let rest = fragment_body
        .strip_prefix(IVM_FRAGMENT_PREFIX)
        .ok_or_else(|| format!("invalid IVM fragment: missing prefix"))?;

    let parts: Vec<&str> = rest.splitn(3, '|').collect();
    if parts.len() < 2 {
        return Err(format!("invalid IVM fragment: expected at least 2 '|'-separated parts, got {}", parts.len()));
    }

    let job_id = parts[0];
    let deltas_json = parts[1];
    let specs_json = if parts.len() >= 3 { parts[2] } else { "[]" };

    // Retrieve or create per-job state.
    let state_arc = ivm_jobs
        .entry(job_id.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(IvmJobState::new())))
        .clone();

    let pending_deltas: Vec<PendingDeltaJson> = serde_json::from_str(deltas_json)
        .map_err(|e| format!("delta decode: {e}"))?;
    let view_specs: Vec<ViewSpecJson> = serde_json::from_str(specs_json)
        .map_err(|e| format!("spec decode: {e}"))?;

    let flow = {
        let state = state_arc.lock().map_err(|_| "ivm state lock poisoned".to_string())?;
        state.flow.clone()
    };

    // Apply view spec updates (idempotent via behavior-version reset).
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

    // Feed pending deltas.
    for pd in &pending_deltas {
        let ipc_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &pd.delta_b64,
        )
        .map_err(|e| format!("base64 decode delta for '{}': {e}", pd.source))?;
        let delta = deserialize_delta_batch(&ipc_bytes).map_err(|e| e.to_string())?;
        flow.feed_source(pd.source.clone(), delta).map_err(|e| e.to_string())?;
    }

    // Run one tick.
    flow.step_datafusion().await.map_err(|e| e.to_string())
}
