//! Processing-time windows, SQL text to emitted rows (task #143, NEXMark Q12).
//!
//! `PROCTIME()` in the descriptor slot means: stamp each row with its arrival
//! wall-clock time and window on the stamps. The distinguishing assertion is
//! the window BOUNDS: events carrying ancient event times must land in the
//! CURRENT wall-clock window, because arrival time — not the payload's
//! timestamp — is the time axis.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_sql::streaming_window_plan::compile_streaming_window_sql;

const SQL: &str = "SELECT bidder, COUNT(*) AS c FROM TUMBLE(TABLE bid, PROCTIME(), 60000) \
                   GROUP BY bidder, window_start, window_end";

/// Bids whose EVENT times are from 1970. Under event-time windowing these
/// close a 1970 window; under processing time they land in today's window.
fn ancient_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("bidder", DataType::Utf8, false),
        Field::new("dateTime", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "a", "b"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_000_i64, 2_000, 3_000])) as ArrayRef,
        ],
    )
    .expect("batch")
}

#[test]
fn proctime_windows_key_on_arrival_not_event_time() {
    let plan = compile_streaming_window_sql(SQL).expect("PROCTIME query must compile");
    assert!(
        plan.spec.processing_time,
        "premise: the compiler must set the processing_time flag"
    );
    let before_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();

    let mut exec = ContinuousWindowExecutor::new(plan.spec).expect("operator");
    exec.drain(vec![ancient_batch()]).expect("drain");
    let out = exec.flush_all().expect("flush");

    let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 2, "two bidders, one window each");
    for batch in out.iter().filter(|b| b.num_rows() > 0) {
        let ws_idx = batch.schema().index_of("window_start_ms").expect("ws");
        let ws = batch
            .column(ws_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64")
            .value(0);
        assert!(
            ws >= before_ms - 60_000 && ws <= before_ms + 60_000,
            "window_start {ws} must be the CURRENT wall-clock window, not the \
             1970 window the event times would produce ({} vs now {before_ms})",
            ws
        );
    }
}

/// A source column colliding with the engine-owned stamp name is refused —
/// silently shadowing user data is the alternative, and it is worse.
#[test]
fn source_column_colliding_with_stamp_name_is_refused() {
    let plan = compile_streaming_window_sql(SQL).expect("compiles");
    let schema = Arc::new(Schema::new(vec![
        Field::new("bidder", DataType::Utf8, false),
        Field::new("__krishiv_proc_time", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut exec = ContinuousWindowExecutor::new(plan.spec).expect("operator");
    let err = exec.drain(vec![batch]).expect_err("must refuse");
    assert!(
        err.to_string().contains("__krishiv_proc_time"),
        "the refusal must name the colliding column: {err}"
    );
}
