//! Session-scoped in-process coordinator + executor cluster (ADR-12.4 / ADR-13.3).

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use krishiv_plan::window::{encode_stream_fragment, WindowExecutionSpec, WindowKind};

use crate::in_process::InProcessStreamingRuntime;
use crate::local_streaming::LocalWindowExecutionSpec;
use crate::{RuntimeError, RuntimeResult};

/// Shared local cluster: one coordinator + executor per session.
#[derive(Clone)]
pub struct InProcessCluster {
    inner: Arc<InProcessStreamingRuntime>,
}

impl InProcessCluster {
    /// Create and register the in-process executor with a new coordinator.
    pub fn new() -> RuntimeResult<Self> {
        Ok(Self {
            inner: Arc::new(InProcessStreamingRuntime::new()?),
        })
    }

    /// Execute a bounded windowed stream through coordinator → executor.
    pub fn collect_bounded_window(
        &self,
        topic: &str,
        input_batches: Vec<RecordBatch>,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        let plan_spec = local_spec_to_plan_spec(spec);
        self.inner
            .execute_windowed(topic, input_batches, &plan_spec)
    }

    /// Borrow the underlying streaming runtime (tests, advanced use).
    pub fn streaming_runtime(&self) -> &InProcessStreamingRuntime {
        &self.inner
    }
}

pub(crate) fn local_spec_to_plan_spec(spec: &LocalWindowExecutionSpec) -> WindowExecutionSpec {
    use krishiv_exec::AggFunction;
    use krishiv_plan::window::{WindowAgg, WindowAggKind};

    let (window_kind, slide_ms, session_gap_ms) = match &spec.window_kind {
        crate::local_streaming::LocalWindowKind::Tumbling => (WindowKind::Tumbling, None, None),
        crate::local_streaming::LocalWindowKind::Sliding { slide_ms } => {
            (WindowKind::Sliding, Some(*slide_ms), None)
        }
        crate::local_streaming::LocalWindowKind::Session { gap_ms } => {
            (WindowKind::Session, None, Some(*gap_ms))
        }
    };

    WindowExecutionSpec {
        key_column: spec.key_column.clone(),
        event_time_column: spec.event_time_column.clone(),
        watermark_lag_ms: spec.watermark_lag_ms,
        window_kind,
        window_size_ms: spec.window_size_ms,
        slide_ms,
        session_gap_ms,
        agg_exprs: spec
            .agg_exprs
            .iter()
            .map(|a| {
                let kind = match a.function {
                    AggFunction::Count => WindowAggKind::Count,
                    AggFunction::Sum => WindowAggKind::Sum,
                    AggFunction::Min => WindowAggKind::Min,
                    AggFunction::Max => WindowAggKind::Max,
                    AggFunction::Avg => WindowAggKind::Avg,
                };
                WindowAgg {
                    kind,
                    input_column: a.input_column.clone(),
                    output_column: a.output_column.clone(),
                }
            })
            .collect(),
        state_ttl_ms: spec.state_ttl_ms,
    }
}

pub(crate) fn plan_spec_to_local(spec: &WindowExecutionSpec) -> LocalWindowExecutionSpec {
    use krishiv_exec::{AggExpr, AggFunction};
    use krishiv_plan::window::WindowAggKind;

    let window_kind = match spec.window_kind {
        WindowKind::Tumbling => crate::local_streaming::LocalWindowKind::Tumbling,
        WindowKind::Sliding => crate::local_streaming::LocalWindowKind::Sliding {
            slide_ms: spec.slide_ms.unwrap_or(spec.window_size_ms),
        },
        WindowKind::Session => crate::local_streaming::LocalWindowKind::Session {
            gap_ms: spec.session_gap_ms.unwrap_or(spec.window_size_ms),
        },
    };

    LocalWindowExecutionSpec {
        key_column: spec.key_column.clone(),
        event_time_column: spec.event_time_column.clone(),
        watermark_lag_ms: spec.watermark_lag_ms,
        window_kind,
        window_size_ms: spec.window_size_ms,
        agg_exprs: spec
            .agg_exprs
            .iter()
            .map(|a| {
                let function = match a.kind {
                    WindowAggKind::Count => AggFunction::Count,
                    WindowAggKind::Sum => AggFunction::Sum,
                    WindowAggKind::Min => AggFunction::Min,
                    WindowAggKind::Max => AggFunction::Max,
                    WindowAggKind::Avg => AggFunction::Avg,
                };
                AggExpr {
                    function,
                    input_column: a.input_column.clone(),
                    output_column: a.output_column.clone(),
                }
            })
            .collect(),
        state_ttl_ms: spec.state_ttl_ms,
    }
}

/// Encode fragment for coordinator submission from a local window spec.
pub fn fragment_from_local_spec(spec: &LocalWindowExecutionSpec) -> String {
    encode_stream_fragment(&local_spec_to_plan_spec(spec))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::local_streaming::LocalWindowExecutionSpec;

    #[test]
    fn cluster_reused_across_collects() {
        let cluster = InProcessCluster::new().expect("cluster");
        let ptr1 = Arc::as_ptr(&cluster.inner);
        let cluster2 = cluster.clone();
        let ptr2 = Arc::as_ptr(&cluster2.inner);
        assert_eq!(ptr1, ptr2);
    }

    #[test]
    fn session_scoped_sliding_window_collect() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as _,
                Arc::new(Int64Array::from(vec![1_000, 5_000])) as _,
            ],
        )
        .unwrap();
        let spec = LocalWindowExecutionSpec {
            key_column: "user_id".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::local_streaming::LocalWindowKind::Sliding { slide_ms: 5_000 },
            window_size_ms: 10_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
        };
        let cluster = InProcessCluster::new().expect("cluster");
        let out = cluster
            .collect_bounded_window("events", vec![batch], &spec)
            .expect("collect");
        assert!(!out.is_empty());
    }
}
