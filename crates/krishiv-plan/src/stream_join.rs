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
