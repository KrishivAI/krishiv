//! Plan-level description of a stream-to-stream interval join.
//!
//! Mirrors the split that [`crate::window`] already makes: the SQL compiler
//! produces this, and `krishiv-dataflow` turns it into the operator. It lives
//! in this crate because `krishiv-sql` cannot depend on `krishiv-dataflow`, and
//! a spec is the only thing the two need to agree on.

use serde::{Deserialize, Serialize};

/// A windowed equi-join between two streams.
///
/// The window is symmetric — a left event matches a right event whose
/// event-time falls within `± window_ms` — because that is what the executing
/// operator implements. Asymmetric bounds are refused by the compiler rather
/// than rounded to something the operator would silently do differently.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamingJoinSpec {
    pub left_source: String,
    pub right_source: String,
    /// Event-time column, present in both streams under the same name.
    pub time_column: String,
    pub left_key_column: String,
    pub right_key_column: String,
    /// Half-width of the join window in milliseconds.
    pub window_ms: u64,
}

impl StreamingJoinSpec {
    /// Validate what the operator cannot check for itself.
    ///
    /// # Errors
    /// Returns a validation error for an empty column or source name, or a
    /// zero window — a zero-width join matches only exactly-simultaneous
    /// events, which is almost never what was meant and is more likely a unit
    /// mix-up (seconds written where milliseconds were expected).
    pub fn validate(&self) -> Result<(), crate::PlanError> {
        let named = [
            ("left_source", &self.left_source),
            ("right_source", &self.right_source),
            ("time_column", &self.time_column),
            ("left_key_column", &self.left_key_column),
            ("right_key_column", &self.right_key_column),
        ];
        for (field, value) in named {
            if value.trim().is_empty() {
                return Err(crate::PlanError::Validation(format!(
                    "streaming join {field} must not be empty"
                )));
            }
        }
        if self.window_ms == 0 {
            return Err(crate::PlanError::Validation(String::from(
                "streaming join window must be greater than zero: a zero-width window matches \
                 only exactly-simultaneous events, which is more often a unit mistake than an \
                 intent",
            )));
        }
        if self.left_source == self.right_source {
            return Err(crate::PlanError::Validation(format!(
                "streaming join needs two distinct sources; both sides name '{}'. A self-join \
                 needs the same stream registered twice under different names",
                self.left_source
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> StreamingJoinSpec {
        StreamingJoinSpec {
            left_source: "bid".into(),
            right_source: "auction".into(),
            time_column: "ts".into(),
            left_key_column: "auction".into(),
            right_key_column: "id".into(),
            window_ms: 5_000,
        }
    }

    #[test]
    fn a_valid_spec_passes() {
        spec().validate().expect("valid");
    }

    #[test]
    fn a_zero_window_is_refused_with_the_reason() {
        let mut s = spec();
        s.window_ms = 0;
        let err = s.validate().expect_err("zero window must be refused");
        assert!(err.to_string().contains("unit mistake"), "got: {err}");
    }

    #[test]
    fn identical_sources_are_refused() {
        let mut s = spec();
        s.right_source = s.left_source.clone();
        let err = s.validate().expect_err("self-join must be refused");
        assert!(
            err.to_string().contains("two distinct sources"),
            "got: {err}"
        );
    }

    #[test]
    fn an_empty_column_is_refused_and_named() {
        let mut s = spec();
        s.right_key_column = String::new();
        let err = s.validate().expect_err("empty key must be refused");
        assert!(err.to_string().contains("right_key_column"), "got: {err}");
    }
}

/// A streaming pipeline: a banded two-source join feeding a chain of
/// windowed stages (task #146, NEXMark Q4/Q9).
///
/// Stage 0 consumes the join's output; stage N consumes stage N-1's output.
/// Both halves existed before this type (the interval join and the windowed
/// aggregate); this is the pipe between them. The SQL surface is a WITH
/// chain: the first CTE is the join, each later CTE (and the final SELECT)
/// is a windowed query over the previous name.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StreamingPipelineSpec {
    pub join: StreamingJoinSpec,
    /// Windowed stages in execution order. Never empty — a pipeline with no
    /// stage is just a join and must be planned as one.
    pub stages: Vec<crate::window::WindowExecutionSpec>,
}

impl StreamingPipelineSpec {
    /// # Errors
    /// When the join is invalid, the stage list is empty, or a stage fails
    /// window validation.
    pub fn validate(&self) -> Result<(), crate::PlanError> {
        self.join.validate()?;
        if self.stages.is_empty() {
            return Err(crate::PlanError::Validation(String::from(
                "a streaming pipeline needs at least one windowed stage; a bare join must be \
                 planned as a join",
            )));
        }
        for stage in &self.stages {
            crate::window::validate_window_execution_spec(stage)?;
        }
        // Enforced here so hand-built specs fail closed too, not only the
        // SQL compiler's: join matches arrive up to `window_ms` out of
        // event-time order, and a first stage with less lateness tolerance
        // silently drops them.
        if let Some(first) = self.stages.first()
            && first.watermark_lag_ms < self.join.window_ms
        {
            return Err(crate::PlanError::Validation(format!(
                "pipeline stage 0 has watermark_lag_ms {} but the join band is {} ms: \
                 matches can arrive that far out of event-time order and would be \
                 silently dropped as late",
                first.watermark_lag_ms, self.join.window_ms
            )));
        }
        Ok(())
    }
}
