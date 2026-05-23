//! Streaming plan specs (R16).

/// Temporal (as-of) stream-table join specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalJoinSpec {
    pub stream_time_col: String,
    pub table_version_col: String,
    pub join_keys: Vec<String>,
    pub inner_join: bool,
}

/// Stream-stream interval join specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntervalJoinSpec {
    pub left_time_col: String,
    pub right_time_col: String,
    pub lower_bound_ms: i64,
    pub upper_bound_ms: i64,
    pub join_keys: Vec<String>,
}

/// Late-data side output routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SideOutput {
    pub name: String,
    pub lateness_threshold_ms: u64,
}
