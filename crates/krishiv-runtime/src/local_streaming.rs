//! Bounded window execution (delegates to unified `krishiv-exec` operator runtime).

use arrow::record_batch::RecordBatch;
use krishiv_exec::{AggExpr, AggFunction, execute_bounded_window};
use krishiv_plan::window::WindowExecutionSpec;

use crate::RuntimeError;
use crate::in_process_cluster::local_spec_to_plan_spec;

/// Window operator kind for local execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalWindowKind {
    Tumbling,
    Sliding { slide_ms: u64 },
    Session { gap_ms: u64 },
}

/// Specification for executing a keyed, windowed stream in-process.
#[derive(Debug, Clone)]
pub struct LocalWindowExecutionSpec {
    pub key_column: String,
    pub event_time_column: String,
    pub watermark_lag_ms: u64,
    pub window_kind: LocalWindowKind,
    pub window_size_ms: u64,
    pub agg_exprs: Vec<AggExpr>,
    pub state_ttl_ms: Option<u64>,
    /// Per-source watermark lags (R5.2). Effective watermark is the minimum across sources.
    pub source_watermark_lags: std::collections::HashMap<String, u64>,
    /// Source id column required when `source_watermark_lags` is non-empty.
    pub source_id_column: Option<String>,
}

impl LocalWindowExecutionSpec {
    pub fn default_count_agg() -> Vec<AggExpr> {
        vec![AggExpr {
            function: AggFunction::Count,
            input_column: String::new(),
            output_column: String::from("count"),
        }]
    }

    pub fn to_plan_spec(&self) -> WindowExecutionSpec {
        local_spec_to_plan_spec(self)
    }
}

/// Run windowed aggregation over bounded input batches (canonical operator path).
pub fn execute_windowed_stream(
    input_batches: Vec<RecordBatch>,
    spec: &LocalWindowExecutionSpec,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    let plan_spec = spec.to_plan_spec();
    execute_bounded_window(input_batches, &plan_spec)
        .map_err(|e| RuntimeError::transport(e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn events_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b"])) as _,
                Arc::new(Int64Array::from(vec![1_000, 5_000, 2_000])) as _,
            ],
        )
        .unwrap()
    }

    #[test]
    fn tumbling_window_produces_closed_buckets() {
        let spec = LocalWindowExecutionSpec {
            key_column: String::from("user_id"),
            event_time_column: String::from("ts"),
            watermark_lag_ms: 0,
            window_kind: LocalWindowKind::Tumbling,
            window_size_ms: 10_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let out =
            execute_windowed_stream(vec![events_batch()], &spec).expect("execute_windowed_stream");
        assert!(!out.is_empty());
    }

    #[test]
    fn session_window_produces_output() {
        let spec = LocalWindowExecutionSpec {
            key_column: String::from("user_id"),
            event_time_column: String::from("ts"),
            watermark_lag_ms: 0,
            window_kind: LocalWindowKind::Session { gap_ms: 5_000 },
            window_size_ms: 5_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let out = execute_windowed_stream(vec![events_batch()], &spec).expect("session");
        assert!(!out.is_empty());
    }
}
