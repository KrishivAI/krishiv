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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BoundedWindowBody {
    pub topic: String,
    pub spec: WindowExecutionSpec,
    pub batches_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExplainBody {
    pub sql: String,
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
    use super::*;
    use krishiv_plan::window::WindowExecutionSpec;

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
    fn action_type_prefix_is_stable() {
        let a = KrishivFlightAction::Explain(ExplainBody {
            sql: "SELECT 1".into(),
        });
        assert_eq!(a.action_type(), "krishiv.v1.explain");
        assert_eq!(strip_action_type(&a.action_type()), Some("explain"));
        assert_eq!(strip_action_type("other.action"), None);
    }
}
