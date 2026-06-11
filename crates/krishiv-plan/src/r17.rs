//! R17 plan types: vector sink configuration.

use serde::{Deserialize, Serialize};

use crate::PlanError;

/// Vector sink configuration (delegates to krishiv-connectors::vector JSON).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorSinkPlanConfig {
    pub sink_type: String,
    pub options: serde_json::Value,
}

impl VectorSinkPlanConfig {
    pub fn new(
        sink_type: impl Into<String>,
        options: serde_json::Value,
    ) -> Result<Self, PlanError> {
        let cfg = Self {
            sink_type: sink_type.into(),
            options,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), PlanError> {
        if self.sink_type.trim().is_empty() {
            return Err(PlanError::Validation(String::from(
                "VectorSinkPlanConfig sink_type must not be empty",
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_sink_rejects_empty_sink_type() {
        assert!(VectorSinkPlanConfig::new("", serde_json::json!({})).is_err());
    }

    #[test]
    fn vector_sink_accepts_valid_config() {
        let cfg =
            VectorSinkPlanConfig::new("pinecone", serde_json::json!({"index": "my-index"})).unwrap();
        assert_eq!(cfg.sink_type, "pinecone");
    }
}
