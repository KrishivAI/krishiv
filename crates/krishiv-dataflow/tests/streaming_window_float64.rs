//! Integration regression for C2: streaming windowed `Float64` aggregates must
//! not be silently truncated to `Int64`.
//!
//! This lives as an integration test (separate compile unit linking the
//! normally-built lib) on purpose: it exercises only the public API and is
//! independent of the crate's in-tree `#[cfg(test)]` modules.

use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use futures::stream::{self, Stream, StreamExt};
use krishiv_dataflow::{ExecResult, execute_streaming_window};
use krishiv_plan::window::{WindowAgg, WindowAggKind, WindowExecutionSpec, WindowKind};

#[test]
fn streaming_window_preserves_float64_sum() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    // Two events in the same 10s tumbling window. Per-value Int truncation would
    // give 1 + 1 = 2; the Float64 sum is 1.5 + 1.5 = 3.0.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "a"])) as _,
            Arc::new(Int64Array::from(vec![1_000, 2_000])) as _,
            Arc::new(Float64Array::from(vec![1.5, 1.5])) as _,
        ],
    )
    .unwrap();

    let spec = WindowExecutionSpec {
        key_column: "user_id".into(),
        key_column_type: "utf8".into(),
        event_time_column: "ts".into(),
        watermark_lag_ms: 0,
        window_kind: WindowKind::Tumbling,
        window_size_ms: 10_000,
        slide_ms: None,
        session_gap_ms: None,
        agg_exprs: vec![WindowAgg {
            kind: WindowAggKind::Sum,
            input_column: "amount".into(),
            output_column: "amount_sum".into(),
        }],
        state_ttl_ms: None,
        allowed_lateness_ms: None,
        source_watermark_lags: std::collections::HashMap::new(),
        source_id_column: None,
        window_timezone: None,
    };

    let input: Pin<Box<dyn Stream<Item = ExecResult<RecordBatch>> + Send>> =
        Box::pin(stream::iter(vec![Ok(batch)]));
    let outputs = futures::executor::block_on(async move {
        let mut out_stream = execute_streaming_window(input, spec, None).expect("stream");
        let mut collected = Vec::new();
        while let Some(item) = out_stream.next().await {
            collected.push(item.expect("batch"));
        }
        collected
    });

    assert!(!outputs.is_empty(), "tumbling flush must emit the window");
    let col = outputs[0]
        .column_by_name("amount_sum")
        .expect("amount_sum column");
    let arr = col
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("amount_sum must be Float64 (regression: streaming truncated to Int64)");
    assert_eq!(
        arr.value(0),
        3.0,
        "float sum must be 3.0, not truncated to 2"
    );
}
