//! The distributed streaming task classes (task #147).
//!
//! One wire type every layer agrees on: the scheduler's registration
//! request, the runtime client, and the executor's fragment payloads all
//! carry a [`StreamingTaskSpec`], so a query class cannot be silently
//! reinterpreted between surfaces.

use serde::{Deserialize, Serialize};

use crate::PlanError;
use crate::stream_join::{StreamingJoinSpec, StreamingPipelineSpec};
use crate::window::WindowExecutionSpec;

/// Every streaming job class the distributed path can carry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "class", rename_all = "kebab-case")]
pub enum StreamingTaskSpec {
    Window(Box<WindowExecutionSpec>),
    Join(Box<StreamingJoinSpec>),
    Pipeline(Box<StreamingPipelineSpec>),
    Stateless(Box<StatelessQuerySpec>),
}

impl StreamingTaskSpec {
    /// Class name, echoed in registration acks so a coordinator that
    /// silently dropped the class field is caught (the verify_ack rule).
    #[must_use]
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Window(_) => "window",
            Self::Join(_) => "join",
            Self::Pipeline(_) => "pipeline",
            Self::Stateless(_) => "stateless",
        }
    }

    /// # Errors
    /// When the inner spec fails its own validation.
    pub fn validate(&self) -> Result<(), PlanError> {
        match self {
            Self::Window(w) => crate::window::validate_window_execution_spec(w),
            Self::Join(j) => j.validate(),
            Self::Pipeline(p) => p.validate(),
            Self::Stateless(s) => s.validate(),
        }
    }
}

/// A stateless per-batch SQL task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatelessQuerySpec {
    pub sql: String,
    /// Table name each input batch is registered under.
    pub source: String,
    /// Bounded reference tables (NEXMark Q13), Arrow IPC, base64.
    #[serde(default)]
    pub side_tables: Vec<SideTableSpec>,
}

/// One bounded side table shipped with a stateless task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SideTableSpec {
    pub name: String,
    pub ipc_base64: String,
}

/// Side tables ride the registration request; a cap keeps a fat reference
/// table from wedging the control plane. 8 MiB of base64 per table.
pub const MAX_SIDE_TABLE_BASE64_BYTES: usize = 8 * 1024 * 1024;

impl StatelessQuerySpec {
    /// # Errors
    /// Empty sql/source, an unnamed side table, or an oversized one.
    pub fn validate(&self) -> Result<(), PlanError> {
        if self.sql.trim().is_empty() {
            return Err(PlanError::Validation("stateless task sql is empty".into()));
        }
        if self.source.trim().is_empty() {
            return Err(PlanError::Validation(
                "stateless task source table name is empty".into(),
            ));
        }
        for t in &self.side_tables {
            if t.name.trim().is_empty() {
                return Err(PlanError::Validation("side table has no name".into()));
            }
            if t.ipc_base64.len() > MAX_SIDE_TABLE_BASE64_BYTES {
                return Err(PlanError::Validation(format!(
                    "side table '{}' is {} base64 bytes; the registration cap is {} — ship \
                     large reference data as a source, not inline",
                    t.name,
                    t.ipc_base64.len(),
                    MAX_SIDE_TABLE_BASE64_BYTES
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    /// The class tag is the wire contract: a round-trip must preserve the
    /// class and the spec, and the tag names must stay stable.
    #[test]
    fn class_tag_round_trips_and_is_stable() {
        let spec = StreamingTaskSpec::Stateless(Box::new(StatelessQuerySpec {
            sql: String::from("SELECT v FROM t"),
            source: String::from("t"),
            side_tables: vec![],
        }));
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"class\":\"stateless\""), "{json}");
        let back: StreamingTaskSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, spec);
        assert_eq!(back.class_name(), "stateless");
    }

    #[test]
    fn oversized_side_table_is_refused_by_name() {
        let spec = StatelessQuerySpec {
            sql: String::from("SELECT 1"),
            source: String::from("t"),
            side_tables: vec![SideTableSpec {
                name: String::from("fat"),
                ipc_base64: "x".repeat(MAX_SIDE_TABLE_BASE64_BYTES + 1),
            }],
        };
        let err = spec.validate().unwrap_err().to_string();
        assert!(err.contains("fat") && err.contains("cap"), "{err}");
    }
}
