//! Streaming plan specs (R16).

use serde::{Deserialize, Serialize};

use crate::PlanError;

/// Temporal (as-of) stream-table join specification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporalJoinSpec {
    pub stream_time_col: String,
    pub table_version_col: String,
    pub join_keys: Vec<String>,
    pub inner_join: bool,
}

impl TemporalJoinSpec {
    pub fn new(
        stream_time_col: impl Into<String>,
        table_version_col: impl Into<String>,
        join_keys: Vec<String>,
        inner_join: bool,
    ) -> Result<Self, PlanError> {
        let spec = Self {
            stream_time_col: stream_time_col.into(),
            table_version_col: table_version_col.into(),
            join_keys,
            inner_join,
        };
        spec.validate()?;
        Ok(spec)
    }

    pub fn validate(&self) -> Result<(), PlanError> {
        if self.stream_time_col.trim().is_empty() {
            return Err(PlanError::Validation(String::from(
                "TemporalJoinSpec stream_time_col must not be empty",
            )));
        }
        if self.table_version_col.trim().is_empty() {
            return Err(PlanError::Validation(String::from(
                "TemporalJoinSpec table_version_col must not be empty",
            )));
        }
        if self.join_keys.is_empty() {
            return Err(PlanError::Validation(String::from(
                "TemporalJoinSpec requires at least one join key",
            )));
        }
        for key in &self.join_keys {
            if key.trim().is_empty() {
                return Err(PlanError::Validation(String::from(
                    "TemporalJoinSpec join key must not be empty",
                )));
            }
        }
        Ok(())
    }
}

/// Stream-stream interval join specification.
///
/// The bounds define the allowed time difference: `lower_bound_ms <= left_ts - right_ts <= upper_bound_ms`.
/// Negative lower bounds allow right events that arrive before left events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntervalJoinSpec {
    pub left_time_col: String,
    pub right_time_col: String,
    /// Lower bound on `left_ts - right_ts` in milliseconds (may be negative).
    pub lower_bound_ms: i64,
    /// Upper bound on `left_ts - right_ts` in milliseconds (may be negative).
    pub upper_bound_ms: i64,
    pub join_keys: Vec<String>,
}

impl IntervalJoinSpec {
    pub fn new(
        left_time_col: impl Into<String>,
        right_time_col: impl Into<String>,
        lower_bound_ms: i64,
        upper_bound_ms: i64,
        join_keys: Vec<String>,
    ) -> Result<Self, PlanError> {
        let spec = Self {
            left_time_col: left_time_col.into(),
            right_time_col: right_time_col.into(),
            lower_bound_ms,
            upper_bound_ms,
            join_keys,
        };
        spec.validate()?;
        Ok(spec)
    }

    pub fn validate(&self) -> Result<(), PlanError> {
        if self.left_time_col.trim().is_empty() {
            return Err(PlanError::Validation(String::from(
                "IntervalJoinSpec left_time_col must not be empty",
            )));
        }
        if self.right_time_col.trim().is_empty() {
            return Err(PlanError::Validation(String::from(
                "IntervalJoinSpec right_time_col must not be empty",
            )));
        }
        if self.lower_bound_ms > self.upper_bound_ms {
            return Err(PlanError::Validation(format!(
                "IntervalJoinSpec lower_bound_ms ({}) must not exceed upper_bound_ms ({})",
                self.lower_bound_ms, self.upper_bound_ms
            )));
        }
        if self.join_keys.is_empty() {
            return Err(PlanError::Validation(String::from(
                "IntervalJoinSpec requires at least one join key",
            )));
        }
        for key in &self.join_keys {
            if key.trim().is_empty() {
                return Err(PlanError::Validation(String::from(
                    "IntervalJoinSpec join key must not be empty",
                )));
            }
        }
        Ok(())
    }
}

/// Late-data side output routing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SideOutput {
    pub name: String,
    pub lateness_threshold_ms: u64,
}

impl SideOutput {
    pub fn new(name: impl Into<String>, lateness_threshold_ms: u64) -> Result<Self, PlanError> {
        let spec = Self {
            name: name.into(),
            lateness_threshold_ms,
        };
        spec.validate()?;
        Ok(spec)
    }

    pub fn validate(&self) -> Result<(), PlanError> {
        if self.name.trim().is_empty() {
            return Err(PlanError::Validation(String::from(
                "SideOutput name must not be empty",
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporal_join_spec_valid() {
        let spec = TemporalJoinSpec::new("event_ts", "version_ts", vec!["user_id".into()], true);
        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert!(spec.inner_join);
    }

    #[test]
    fn temporal_join_spec_rejects_empty_time_cols() {
        assert!(TemporalJoinSpec::new("", "version_ts", vec!["k".into()], true).is_err());
        assert!(TemporalJoinSpec::new("event_ts", "", vec!["k".into()], true).is_err());
    }

    #[test]
    fn temporal_join_spec_rejects_empty_join_keys() {
        assert!(TemporalJoinSpec::new("event_ts", "version_ts", vec![], true).is_err());
        assert!(TemporalJoinSpec::new("event_ts", "version_ts", vec!["".into()], true).is_err());
    }

    #[test]
    fn interval_join_spec_valid() {
        let spec = IntervalJoinSpec::new("left_ts", "right_ts", -1000, 5000, vec!["k".into()]);
        assert!(spec.is_ok());
    }

    #[test]
    fn interval_join_spec_rejects_inverted_bounds() {
        let err = IntervalJoinSpec::new("left_ts", "right_ts", 5000, -1000, vec!["k".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("lower_bound_ms"));
    }

    #[test]
    fn interval_join_spec_equal_bounds_valid() {
        let spec = IntervalJoinSpec::new("l", "r", 0, 0, vec!["k".into()]);
        assert!(spec.is_ok());
    }

    #[test]
    fn interval_join_spec_rejects_empty_cols() {
        assert!(IntervalJoinSpec::new("", "r", 0, 1000, vec!["k".into()]).is_err());
        assert!(IntervalJoinSpec::new("l", "", 0, 1000, vec!["k".into()]).is_err());
    }

    #[test]
    fn interval_join_spec_rejects_empty_keys() {
        assert!(IntervalJoinSpec::new("l", "r", 0, 1000, vec![]).is_err());
    }

    #[test]
    fn side_output_valid() {
        let so = SideOutput::new("late_events", 5000).unwrap();
        assert_eq!(so.lateness_threshold_ms, 5000);
    }

    #[test]
    fn side_output_rejects_empty_name() {
        assert!(SideOutput::new("", 1000).is_err());
        assert!(SideOutput::new("  ", 1000).is_err());
    }

    #[test]
    fn streaming_types_serde_roundtrip() {
        let temporal =
            TemporalJoinSpec::new("event_ts", "version_ts", vec!["user_id".into()], false).unwrap();
        let json = serde_json::to_string(&temporal).unwrap();
        let decoded: TemporalJoinSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(temporal, decoded);

        let interval = IntervalJoinSpec::new("l", "r", -500, 2000, vec!["k".into()]).unwrap();
        let json = serde_json::to_string(&interval).unwrap();
        let decoded: IntervalJoinSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(interval, decoded);

        let side = SideOutput::new("late", 3000).unwrap();
        let json = serde_json::to_string(&side).unwrap();
        let decoded: SideOutput = serde_json::from_str(&json).unwrap();
        assert_eq!(side, decoded);
    }
}
