//! Typed Arrow Flight `DoAction` payloads for the Krishiv streaming control
//! plane (B3, D2).
//!
//! The legacy `flight_protocol::FlightDirective` path packed batches, window
//! specs, and identifiers into SQL comments.  That has several problems:
//! - identifier validation depends on every consumer correctly rejecting
//!   `*/` and other escape sequences;
//! - SQL parsers strip or rewrite comments in subtle ways;
//! - the entire body has to be base64-encoded which inflates payloads ~33%.
//!
//! The typed `DoAction` API replaces the comment encoding for new clients.
//! Each variant of [`KrishivFlightAction`] becomes a JSON body shipped via
//! Flight's `do_action`; record batches travel as raw Arrow IPC bytes inside
//! the JSON (still Arrow-native, no SQL involvement).  Comments are accepted
//! as a deprecated fallback for old clients.

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use krishiv_plan::window::WindowExecutionSpec;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::path::PathBuf;

use crate::{RuntimeError, RuntimeResult};

/// Stable Flight action type prefix.  Action types are formed as
/// `"krishiv.v1." + tag` (e.g. `"krishiv.v1.continuous.push"`).
pub const ACTION_TYPE_PREFIX: &str = "krishiv.v1.";

/// Concrete action tags.
pub mod tags {
    pub const REGISTER_PARQUET: &str = "register_parquet";
    pub const CONTINUOUS_REGISTER: &str = "continuous.register";
    pub const CONTINUOUS_PUSH: &str = "continuous.push";
    pub const CONTINUOUS_DRAIN: &str = "continuous.drain";
    pub const BOUNDED_WINDOW: &str = "bounded_window";
    pub const EXPLAIN: &str = "explain";
    pub const EXECUTE_PLAN: &str = "execute_plan";
    pub const BATCH_SQL: &str = "batch_sql";
}

/// Build a stable action type for a tag — `format!("krishiv.v1.{tag}")`.
pub fn action_type(tag: &str) -> String {
    format!("{ACTION_TYPE_PREFIX}{tag}")
}

/// Strip the [`ACTION_TYPE_PREFIX`] from an action type; returns `None` for
/// foreign action types.
pub fn strip_action_type(action_type: &str) -> Option<&str> {
    action_type.strip_prefix(ACTION_TYPE_PREFIX)
}

/// Encoded action body for transit on `Action::body`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterParquetBody {
    pub table: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContinuousRegisterBody {
    pub job_id: String,
    pub spec: WindowExecutionSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContinuousPushBody {
    pub job_id: String,
    /// Arrow IPC stream bytes, base64-encoded for transport — preferred over
    /// raw bytes inside the JSON because the underlying transport (`Action.body`)
    /// is bytes and we keep the rest of the body human-readable.
    pub batches_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContinuousDrainBody {
    pub job_id: String,
}

/// Request-only bounded-window action body (no response fields).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BoundedWindowRequest {
    pub topic: String,
    pub spec: WindowExecutionSpec,
    pub batches_b64: String,
}

/// Full bounded-window body including optional server-populated response fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BoundedWindowBody {
    pub topic: String,
    pub spec: WindowExecutionSpec,
    pub batches_b64: String,
    /// Maximum watermark observed across all output batches, populated by the
    /// server on the response path. `None` if no window has closed yet or if
    /// the executor has not yet advanced its watermark (C8).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_watermark_ms: Option<i64>,
}

impl BoundedWindowBody {
    /// Create a request body (response_watermark_ms is None for requests).
    pub fn request(topic: String, spec: WindowExecutionSpec, batches_b64: String) -> Self {
        Self {
            topic,
            spec,
            batches_b64,
            response_watermark_ms: None,
        }
    }
}

impl From<BoundedWindowRequest> for BoundedWindowBody {
    fn from(req: BoundedWindowRequest) -> Self {
        Self {
            topic: req.topic,
            spec: req.spec,
            batches_b64: req.batches_b64,
            response_watermark_ms: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExplainBody {
    pub sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutePlanBody {
    pub name: String,
    pub execution_kind: String,
    pub plan_json: String,
}

impl ExecutePlanBody {
    pub fn from_plan(plan: &krishiv_plan::PhysicalPlan) -> RuntimeResult<Self> {
        plan.validate()
            .map_err(|error| RuntimeError::plan_rejected(error.to_string()))?;
        let plan_json = serde_json::to_string(plan)
            .map_err(|e| RuntimeError::transport(format!("plan serialize: {e}")))?;
        Ok(Self {
            name: plan.name().to_string(),
            execution_kind: plan.kind().to_string(),
            plan_json,
        })
    }

    pub fn to_plan(&self) -> RuntimeResult<krishiv_plan::PhysicalPlan> {
        let plan: krishiv_plan::PhysicalPlan = serde_json::from_str(&self.plan_json)
            .map_err(|e| RuntimeError::transport(format!("plan deserialize: {e}")))?;
        plan.validate()
            .map_err(|error| RuntimeError::plan_rejected(error.to_string()))?;
        if self.name != plan.name() {
            return Err(RuntimeError::plan_rejected(format!(
                "execute-plan envelope name '{}' does not match physical plan name '{}'",
                self.name,
                plan.name()
            )));
        }
        let actual_execution_kind = plan.kind().to_string();
        if self.execution_kind != actual_execution_kind {
            return Err(RuntimeError::plan_rejected(format!(
                "execute-plan envelope kind '{}' does not match physical plan kind '{actual_execution_kind}'",
                self.execution_kind
            )));
        }
        Ok(plan)
    }
}

/// Typed body for a batch-SQL `DoAction` request.
///
/// Carries the query, optional table registrations, and the streaming intent
/// flag. This replaces the fragile `-- krishiv:streaming=true` SQL-comment
/// protocol (H2).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchSqlBody {
    pub query: String,
    pub tables: Vec<crate::in_process::BatchSqlTable>,
    pub is_streaming: bool,
}

/// Typed Flight `DoAction` payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind")]
pub enum KrishivFlightAction {
    RegisterParquet(RegisterParquetBody),
    ContinuousRegister(ContinuousRegisterBody),
    ContinuousPush(ContinuousPushBody),
    ContinuousDrain(ContinuousDrainBody),
    BoundedWindow(BoundedWindowBody),
    Explain(ExplainBody),
    ExecutePlan(ExecutePlanBody),
    BatchSql(BatchSqlBody),
}

impl KrishivFlightAction {
    /// Stable action type string for this variant.
    pub fn action_type(&self) -> String {
        let tag = match self {
            Self::RegisterParquet(_) => tags::REGISTER_PARQUET,
            Self::ContinuousRegister(_) => tags::CONTINUOUS_REGISTER,
            Self::ContinuousPush(_) => tags::CONTINUOUS_PUSH,
            Self::ContinuousDrain(_) => tags::CONTINUOUS_DRAIN,
            Self::BoundedWindow(_) => tags::BOUNDED_WINDOW,
            Self::Explain(_) => tags::EXPLAIN,
            Self::ExecutePlan(_) => tags::EXECUTE_PLAN,
            Self::BatchSql(_) => tags::BATCH_SQL,
        };
        action_type(tag)
    }

    /// Encode the action body as JSON bytes for `Action::body`.
    pub fn to_action_body(&self) -> RuntimeResult<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|e| RuntimeError::transport(format!("flight action encode: {e}")))
    }

    /// Decode an action body from JSON bytes received in `Action::body`.
    pub fn from_action_body(bytes: &[u8]) -> RuntimeResult<Self> {
        serde_json::from_slice(bytes)
            .map_err(|e| RuntimeError::transport(format!("flight action decode: {e}")))
    }
}

/// Encode a slice of record batches into base64-wrapped Arrow IPC stream bytes.
pub fn encode_batches(batches: &[RecordBatch]) -> RuntimeResult<String> {
    if batches.is_empty() {
        return Ok(String::new());
    }
    let schema = batches[0].schema();
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)
            .map_err(|e| RuntimeError::transport(format!("ipc encode failed: {e}")))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| RuntimeError::transport(format!("ipc write failed: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| RuntimeError::transport(format!("ipc finish failed: {e}")))?;
    }
    Ok(BASE64.encode(buffer))
}

/// Decode batches from a string previously produced by [`encode_batches`].
pub fn decode_batches(encoded: &str) -> RuntimeResult<Vec<RecordBatch>> {
    if encoded.is_empty() {
        return Ok(Vec::new());
    }
    let bytes = BASE64
        .decode(encoded)
        .map_err(|e| RuntimeError::transport(format!("invalid batches b64: {e}")))?;
    let cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| RuntimeError::transport(format!("ipc decode failed: {e}")))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| RuntimeError::transport(format!("ipc read failed: {e}")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_plan::window::WindowExecutionSpec;
    use krishiv_plan::{ExecutionKind, PhysicalPlan, PlanNode};

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as _,
                Arc::new(Int64Array::from(vec![1_000, 5_000])) as _,
            ],
        )
        .unwrap()
    }

    #[test]
    fn round_trip_register_parquet() {
        let action = KrishivFlightAction::RegisterParquet(RegisterParquetBody {
            table: "events".into(),
            path: PathBuf::from("/data/events.parquet"),
        });
        let bytes = action.to_action_body().unwrap();
        let decoded = KrishivFlightAction::from_action_body(&bytes).unwrap();
        assert_eq!(decoded, action);
        assert_eq!(decoded.action_type(), "krishiv.v1.register_parquet");
    }

    #[test]
    fn round_trip_continuous_register() {
        let action = KrishivFlightAction::ContinuousRegister(ContinuousRegisterBody {
            job_id: "j1".into(),
            spec: WindowExecutionSpec::tumbling("k", "ts", 10_000),
        });
        let bytes = action.to_action_body().unwrap();
        let decoded = KrishivFlightAction::from_action_body(&bytes).unwrap();
        assert_eq!(decoded, action);
        assert_eq!(decoded.action_type(), "krishiv.v1.continuous.register");
    }

    #[test]
    fn round_trip_continuous_push_with_batches() {
        let batch = test_batch();
        let batches_b64 = encode_batches(&[batch]).unwrap();
        let action = KrishivFlightAction::ContinuousPush(ContinuousPushBody {
            job_id: "push-job".into(),
            batches_b64: batches_b64.clone(),
        });
        let bytes = action.to_action_body().unwrap();
        let decoded = KrishivFlightAction::from_action_body(&bytes).unwrap();
        match decoded {
            KrishivFlightAction::ContinuousPush(body) => {
                assert_eq!(body.job_id, "push-job");
                assert_eq!(body.batches_b64, batches_b64);
                let decoded_batches = decode_batches(&body.batches_b64).unwrap();
                assert_eq!(decoded_batches.len(), 1);
                assert_eq!(decoded_batches[0].num_rows(), 2);
            }
            other => panic!("expected ContinuousPush, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_continuous_drain() {
        let action = KrishivFlightAction::ContinuousDrain(ContinuousDrainBody {
            job_id: "drain-job".into(),
        });
        let bytes = action.to_action_body().unwrap();
        let decoded = KrishivFlightAction::from_action_body(&bytes).unwrap();
        assert_eq!(decoded, action);
        assert_eq!(decoded.action_type(), "krishiv.v1.continuous.drain");
    }

    #[test]
    fn round_trip_bounded_window() {
        let batch = test_batch();
        let batches_b64 = encode_batches(&[batch]).unwrap();
        let action = KrishivFlightAction::BoundedWindow(BoundedWindowBody {
            topic: "events".into(),
            spec: WindowExecutionSpec::tumbling("user_id", "ts", 5_000),
            batches_b64: batches_b64.clone(),
            response_watermark_ms: None,
        });
        let bytes = action.to_action_body().unwrap();
        let decoded = KrishivFlightAction::from_action_body(&bytes).unwrap();
        match decoded {
            KrishivFlightAction::BoundedWindow(body) => {
                assert_eq!(body.topic, "events");
                assert_eq!(body.spec.window_size_ms, 5_000);
                assert_eq!(body.batches_b64, batches_b64);
                let decoded_batches = decode_batches(&body.batches_b64).unwrap();
                assert_eq!(decoded_batches[0].num_rows(), 2);
            }
            other => panic!("expected BoundedWindow, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_execute_plan() {
        let plan = PhysicalPlan::new("my-batch-job", ExecutionKind::Batch);
        let action = KrishivFlightAction::ExecutePlan(ExecutePlanBody::from_plan(&plan).unwrap());
        let bytes = action.to_action_body().unwrap();
        let decoded = KrishivFlightAction::from_action_body(&bytes).unwrap();
        match decoded {
            KrishivFlightAction::ExecutePlan(body) => {
                assert_eq!(body.name, "my-batch-job");
                assert_eq!(body.execution_kind, "batch");
                let recovered = body.to_plan().unwrap();
                assert_eq!(recovered.name(), "my-batch-job");
                assert_eq!(recovered.kind(), ExecutionKind::Batch);
            }
            other => panic!("expected ExecutePlan, got {other:?}"),
        }
    }

    #[test]
    fn execute_plan_body_rejects_invalid_graph() {
        let plan = PhysicalPlan::new("invalid", ExecutionKind::Batch).with_node(
            PlanNode::new("sink", "sink", ExecutionKind::Batch).with_inputs(["missing"]),
        );
        let body = ExecutePlanBody {
            name: plan.name().to_string(),
            execution_kind: plan.kind().to_string(),
            plan_json: serde_json::to_string(&plan).expect("serialize"),
        };

        let error = body.to_plan().expect_err("invalid graph");

        assert!(matches!(error, RuntimeError::PlanRejected { .. }));
        assert!(error.to_string().contains("missing input 'missing'"));
    }

    #[test]
    fn execute_plan_body_rejects_tampered_envelope_metadata() {
        let plan = PhysicalPlan::new("actual", ExecutionKind::Batch);
        let plan_json = serde_json::to_string(&plan).expect("serialize");

        let wrong_name = ExecutePlanBody {
            name: "other".to_string(),
            execution_kind: "batch".to_string(),
            plan_json: plan_json.clone(),
        };
        assert!(
            wrong_name
                .to_plan()
                .expect_err("name mismatch")
                .to_string()
                .contains("envelope name")
        );

        let wrong_kind = ExecutePlanBody {
            name: "actual".to_string(),
            execution_kind: "streaming".to_string(),
            plan_json,
        };
        assert!(
            wrong_kind
                .to_plan()
                .expect_err("kind mismatch")
                .to_string()
                .contains("envelope kind")
        );
    }

    #[test]
    fn round_trip_explain() {
        let action = KrishivFlightAction::Explain(ExplainBody {
            sql: "SELECT 1".into(),
        });
        let bytes = action.to_action_body().unwrap();
        let decoded = KrishivFlightAction::from_action_body(&bytes).unwrap();
        assert_eq!(decoded, action);
        assert_eq!(decoded.action_type(), "krishiv.v1.explain");
    }

    #[test]
    fn action_type_prefix_is_stable() {
        let a = KrishivFlightAction::Explain(ExplainBody {
            sql: "SELECT 1".into(),
        });
        assert_eq!(a.action_type(), "krishiv.v1.explain");
        assert_eq!(strip_action_type(&a.action_type()), Some("explain"));
        assert_eq!(strip_action_type("other.action"), None);
    }

    #[test]
    fn action_types_for_all_variants() {
        let exec_plan = KrishivFlightAction::ExecutePlan(ExecutePlanBody {
            name: "job".into(),
            execution_kind: "batch".into(),
            plan_json: "{}".into(),
        });
        assert_eq!(exec_plan.action_type(), "krishiv.v1.execute_plan");

        let register = KrishivFlightAction::RegisterParquet(RegisterParquetBody {
            table: "t".into(),
            path: PathBuf::from("/t.parquet"),
        });
        assert_eq!(register.action_type(), "krishiv.v1.register_parquet");

        let continuous_reg = KrishivFlightAction::ContinuousRegister(ContinuousRegisterBody {
            job_id: "j".into(),
            spec: WindowExecutionSpec::tumbling("k", "ts", 1_000),
        });
        assert_eq!(
            continuous_reg.action_type(),
            "krishiv.v1.continuous.register"
        );

        let push = KrishivFlightAction::ContinuousPush(ContinuousPushBody {
            job_id: "j".into(),
            batches_b64: String::new(),
        });
        assert_eq!(push.action_type(), "krishiv.v1.continuous.push");

        let drain =
            KrishivFlightAction::ContinuousDrain(ContinuousDrainBody { job_id: "j".into() });
        assert_eq!(drain.action_type(), "krishiv.v1.continuous.drain");

        let bounded = KrishivFlightAction::BoundedWindow(BoundedWindowBody {
            topic: "t".into(),
            spec: WindowExecutionSpec::tumbling("k", "ts", 1_000),
            batches_b64: String::new(),
            response_watermark_ms: None,
        });
        assert_eq!(bounded.action_type(), "krishiv.v1.bounded_window");

        let explain = KrishivFlightAction::Explain(ExplainBody {
            sql: "SELECT 1".into(),
        });
        assert_eq!(explain.action_type(), "krishiv.v1.explain");
    }

    #[test]
    fn encode_batches_empty() {
        let encoded = encode_batches(&[]).unwrap();
        assert!(encoded.is_empty());
    }

    #[test]
    fn encode_decode_batches_roundtrip() {
        let batch = test_batch();
        let encoded = encode_batches(&[batch]).unwrap();
        let decoded = decode_batches(&encoded).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 2);
        assert_eq!(decoded[0].num_columns(), 2);
    }

    #[test]
    fn encode_decode_multiple_batches() {
        let batch1 = test_batch();
        let schema = batch1.schema();
        let batch2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["c"])) as _,
                Arc::new(Int64Array::from(vec![9_000])) as _,
            ],
        )
        .unwrap();
        let encoded = encode_batches(&[batch1, batch2]).unwrap();
        let decoded = decode_batches(&encoded).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].num_rows(), 2);
        assert_eq!(decoded[1].num_rows(), 1);
    }

    #[test]
    fn decode_empty_string_returns_empty() {
        let decoded = decode_batches("").unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn from_action_body_invalid_json_fails() {
        let err = KrishivFlightAction::from_action_body(b"not json").unwrap_err();
        assert!(matches!(err, RuntimeError::Transport { .. }));
    }

    #[test]
    fn round_trip_continuous_register_sliding_window() {
        let spec = WindowExecutionSpec {
            key_column: "k".into(),
            key_column_type: String::from("utf8"),
            event_time_column: "ts".into(),
            watermark_lag_ms: 500,
            window_kind: krishiv_plan::window::WindowKind::Sliding,
            window_size_ms: 30_000,
            slide_ms: Some(10_000),
            session_gap_ms: None,
            agg_exprs: WindowExecutionSpec::default_count_agg(),
            state_ttl_ms: Some(60_000),
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let action = KrishivFlightAction::ContinuousRegister(ContinuousRegisterBody {
            job_id: "sliding-job".into(),
            spec,
        });
        let bytes = action.to_action_body().unwrap();
        let decoded = KrishivFlightAction::from_action_body(&bytes).unwrap();
        match decoded {
            KrishivFlightAction::ContinuousRegister(body) => {
                assert_eq!(
                    body.spec.window_kind,
                    krishiv_plan::window::WindowKind::Sliding
                );
                assert_eq!(body.spec.slide_ms, Some(10_000));
                assert_eq!(body.spec.state_ttl_ms, Some(60_000));
            }
            other => panic!("expected ContinuousRegister, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_bounded_window_with_session_spec() {
        let spec = WindowExecutionSpec {
            key_column: "user_id".into(),
            key_column_type: String::from("utf8"),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: krishiv_plan::window::WindowKind::Session,
            window_size_ms: 15_000,
            slide_ms: None,
            session_gap_ms: Some(5_000),
            agg_exprs: WindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let batch = test_batch();
        let action = KrishivFlightAction::BoundedWindow(BoundedWindowBody {
            topic: "user-events".into(),
            spec,
            batches_b64: encode_batches(&[batch]).unwrap(),
            response_watermark_ms: None,
        });
        let bytes = action.to_action_body().unwrap();
        let decoded = KrishivFlightAction::from_action_body(&bytes).unwrap();
        match decoded {
            KrishivFlightAction::BoundedWindow(body) => {
                assert_eq!(body.topic, "user-events");
                assert_eq!(
                    body.spec.window_kind,
                    krishiv_plan::window::WindowKind::Session
                );
                assert_eq!(body.spec.session_gap_ms, Some(5_000));
            }
            other => panic!("expected BoundedWindow, got {other:?}"),
        }
    }

    #[test]
    fn decode_batches_invalid_base64() {
        let err = decode_batches("!!!invalid-base64!!!").unwrap_err();
        assert!(matches!(err, RuntimeError::Transport { .. }));
    }

    #[test]
    fn decode_batches_invalid_ipc_bytes() {
        let valid_b64 = BASE64.encode(b"this is not ipc data");
        let err = decode_batches(&valid_b64).unwrap_err();
        assert!(matches!(err, RuntimeError::Transport { .. }));
    }

    #[test]
    fn from_action_body_empty_bytes() {
        let err = KrishivFlightAction::from_action_body(b"").unwrap_err();
        assert!(matches!(err, RuntimeError::Transport { .. }));
    }

    #[test]
    fn from_action_body_wrong_json_structure() {
        let json = serde_json::to_vec(&serde_json::json!({"not": "a valid action"})).unwrap();
        let err = KrishivFlightAction::from_action_body(&json).unwrap_err();
        assert!(matches!(err, RuntimeError::Transport { .. }));
    }

    #[test]
    fn encode_batches_returns_empty_string_for_empty_input() {
        let result = encode_batches(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn encode_decode_multiple_batches_preserves_count() {
        let b1 = test_batch();
        let schema = b1.schema();
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["x"])) as _,
                Arc::new(Int64Array::from(vec![7_777])) as _,
            ],
        )
        .unwrap();
        let b3 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["y", "z"])) as _,
                Arc::new(Int64Array::from(vec![100, 200])) as _,
            ],
        )
        .unwrap();
        let encoded = encode_batches(&[b1, b2, b3]).unwrap();
        let decoded = decode_batches(&encoded).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].num_rows(), 2);
        assert_eq!(decoded[1].num_rows(), 1);
        assert_eq!(decoded[2].num_rows(), 2);
    }

    #[test]
    fn action_type_prefix_constant() {
        assert_eq!(ACTION_TYPE_PREFIX, "krishiv.v1.");
    }

    #[test]
    fn strip_action_type_returns_none_for_unknown() {
        assert_eq!(strip_action_type("completely.different"), None);
        assert_eq!(strip_action_type(""), None);
        assert_eq!(strip_action_type("krishiv.v1."), Some(""));
        assert_eq!(strip_action_type("krishiv.v1.foo"), Some("foo"));
    }

    #[test]
    fn action_type_returns_correct_prefix_for_all_variants() {
        let variants = vec![
            KrishivFlightAction::RegisterParquet(RegisterParquetBody {
                table: "t".into(),
                path: "/t.parquet".into(),
            }),
            KrishivFlightAction::ContinuousRegister(ContinuousRegisterBody {
                job_id: "j".into(),
                spec: WindowExecutionSpec::tumbling("k", "ts", 1_000),
            }),
            KrishivFlightAction::ContinuousPush(ContinuousPushBody {
                job_id: "j".into(),
                batches_b64: String::new(),
            }),
            KrishivFlightAction::ContinuousDrain(ContinuousDrainBody { job_id: "j".into() }),
            KrishivFlightAction::BoundedWindow(BoundedWindowBody {
                topic: "t".into(),
                spec: WindowExecutionSpec::tumbling("k", "ts", 1_000),
                batches_b64: String::new(),
                response_watermark_ms: None,
            }),
            KrishivFlightAction::Explain(ExplainBody {
                sql: "SELECT 1".into(),
            }),
            KrishivFlightAction::ExecutePlan(ExecutePlanBody {
                name: "j".into(),
                execution_kind: "batch".into(),
                plan_json: "{}".into(),
            }),
        ];
        for v in variants {
            let at = v.action_type();
            assert!(at.starts_with(ACTION_TYPE_PREFIX));
        }
    }

    #[test]
    fn round_trip_register_parquet_with_long_path() {
        let action = KrishivFlightAction::RegisterParquet(RegisterParquetBody {
            table: "events".into(),
            path: PathBuf::from("/very/long/path/to/data/events/partition-001.parquet"),
        });
        let bytes = action.to_action_body().unwrap();
        let decoded = KrishivFlightAction::from_action_body(&bytes).unwrap();
        assert_eq!(decoded, action);
    }

    #[test]
    fn round_trip_continuous_drain_special_job_id() {
        let action = KrishivFlightAction::ContinuousDrain(ContinuousDrainBody {
            job_id: "my-job-123_v2.test".into(),
        });
        let bytes = action.to_action_body().unwrap();
        let decoded = KrishivFlightAction::from_action_body(&bytes).unwrap();
        assert_eq!(decoded, action);
    }

    #[test]
    fn round_trip_explain_complex_sql() {
        let sql = "SELECT a, b, COUNT(*) as cnt FROM t1 JOIN t2 ON t1.id = t2.id GROUP BY a, b HAVING COUNT(*) > 10 ORDER BY cnt DESC LIMIT 100";
        let action = KrishivFlightAction::Explain(ExplainBody { sql: sql.into() });
        let bytes = action.to_action_body().unwrap();
        let decoded = KrishivFlightAction::from_action_body(&bytes).unwrap();
        match decoded {
            KrishivFlightAction::Explain(body) => assert_eq!(body.sql, sql),
            other => panic!("expected Explain, got {other:?}"),
        }
    }

    #[test]
    fn encode_decode_batches_preserves_schema() {
        let batch = test_batch();
        let encoded = encode_batches(&[batch]).unwrap();
        let decoded = decode_batches(&encoded).unwrap();
        assert_eq!(decoded[0].schema().field(0).name(), "user_id");
        assert_eq!(decoded[0].schema().field(1).name(), "ts");
    }

    #[test]
    fn register_parquet_body_serde_round_trip() {
        let body = RegisterParquetBody {
            table: "my_table".into(),
            path: PathBuf::from("/data/my_table.parquet"),
        };
        let json = serde_json::to_string(&body).unwrap();
        let decoded: RegisterParquetBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn continuous_push_body_serde_round_trip() {
        let body = ContinuousPushBody {
            job_id: "push-1".into(),
            batches_b64: "abc123".into(),
        };
        let json = serde_json::to_string(&body).unwrap();
        let decoded: ContinuousPushBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn continuous_drain_body_serde_round_trip() {
        let body = ContinuousDrainBody {
            job_id: "drain-1".into(),
        };
        let json = serde_json::to_string(&body).unwrap();
        let decoded: ContinuousDrainBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn explain_body_serde_round_trip() {
        let body = ExplainBody {
            sql: "SELECT 1".into(),
        };
        let json = serde_json::to_string(&body).unwrap();
        let decoded: ExplainBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, body);
    }

    #[test]
    fn bounded_window_body_serde_round_trip() {
        let body = BoundedWindowBody {
            topic: "events".into(),
            spec: WindowExecutionSpec::tumbling("k", "ts", 5_000),
            batches_b64: String::new(),
            response_watermark_ms: None,
        };
        let json = serde_json::to_string(&body).unwrap();
        let decoded: BoundedWindowBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.topic, body.topic);
        assert_eq!(decoded.spec.window_size_ms, 5_000);
    }
}
