//! The column-type axis the conformance corpus was blind on (§41's second
//! blind axis, closed in task #146): the SAME windowed query over the SAME
//! logical data must give the SAME answer for every supported key/value
//! column type. UInt64 blindness produced five sibling defects; this sweep
//! executes — not merely compiles — each type through the full driver path.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_dataflow::stream_driver::{StreamDriver, StreamingLoop};
use krishiv_sql::streaming_window_plan::compile_streaming_window_sql;

const SQL: &str = "SELECT k, SUM(v) AS total FROM TUMBLE(TABLE t, DESCRIPTOR(ts), 10000) \
                   GROUP BY k, window_start, window_end";

fn key_array(dt: &DataType, keys: &[u64]) -> ArrayRef {
    match dt {
        DataType::Int64 => Arc::new(Int64Array::from(
            keys.iter().map(|k| *k as i64).collect::<Vec<_>>(),
        )),
        DataType::UInt64 => Arc::new(UInt64Array::from(keys.to_vec())),
        DataType::Int32 => Arc::new(Int32Array::from(
            keys.iter().map(|k| *k as i32).collect::<Vec<_>>(),
        )),
        DataType::UInt32 => Arc::new(UInt32Array::from(
            keys.iter().map(|k| *k as u32).collect::<Vec<_>>(),
        )),
        DataType::Utf8 => Arc::new(StringArray::from(
            keys.iter().map(|k| format!("k{k}")).collect::<Vec<_>>(),
        )),
        other => panic!("unexpected key type {other}"),
    }
}

fn value_array(dt: &DataType, vals: &[u64]) -> ArrayRef {
    match dt {
        DataType::Int64 => Arc::new(Int64Array::from(
            vals.iter().map(|v| *v as i64).collect::<Vec<_>>(),
        )),
        DataType::UInt64 => Arc::new(UInt64Array::from(vals.to_vec())),
        DataType::Int32 => Arc::new(Int32Array::from(
            vals.iter().map(|v| *v as i32).collect::<Vec<_>>(),
        )),
        DataType::UInt32 => Arc::new(UInt32Array::from(
            vals.iter().map(|v| *v as u32).collect::<Vec<_>>(),
        )),
        other => panic!("unexpected value type {other}"),
    }
}

/// Group sums, keyed by the key's canonical text so the answers compare
/// across key types.
fn run(key_dt: &DataType, val_dt: &DataType) -> Vec<(String, i64)> {
    let plan = compile_streaming_window_sql(SQL)
        .unwrap_or_else(|e| panic!("{key_dt}/{val_dt}: compile: {e}"));
    let mut exec = ContinuousWindowExecutor::new(plan.spec)
        .unwrap_or_else(|e| panic!("{key_dt}/{val_dt}: operator: {e}"));
    let mut driver = StreamDriver::new(StreamingLoop::EmbeddedContinuous);

    let schema = Arc::new(Schema::new(vec![
        Field::new("k", key_dt.clone(), false),
        Field::new("ts", DataType::Int64, false),
        Field::new("v", val_dt.clone(), false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            key_array(key_dt, &[1, 1, 2, 1]),
            Arc::new(Int64Array::from(vec![1_000_i64, 1_001, 1_002, 1_003])) as ArrayRef,
            value_array(val_dt, &[10, 20, 5, 30]),
        ],
    )
    .unwrap();

    let mut out = driver
        .on_input(&mut exec, vec![batch])
        .unwrap_or_else(|e| panic!("{key_dt}/{val_dt}: on_input: {e}"));
    out.extend(
        exec.flush_all()
            .unwrap_or_else(|e| panic!("{key_dt}/{val_dt}: flush: {e}")),
    );

    let mut rows = Vec::new();
    for b in out.iter().filter(|b| b.num_rows() > 0) {
        let k_idx = b.schema().index_of("k").expect("k");
        let t_idx = b.schema().index_of("total").expect("total");
        let totals = b
            .column(t_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("{key_dt}/{val_dt}: total not Int64"));
        for row in 0..b.num_rows() {
            let key_text = arrow::util::display::array_value_to_string(b.column(k_idx), row)
                .expect("key text");
            rows.push((
                key_text.trim_start_matches('k').to_owned(),
                totals.value(row),
            ));
        }
    }
    rows.sort();
    rows
}

/// Every key type x value type combination gives the identical answer:
/// group 1 sums to 60, group 2 to 5.
#[test]
fn every_column_type_gives_the_same_answer() {
    let expected = vec![(String::from("1"), 60_i64), (String::from("2"), 5)];
    for key_dt in [
        DataType::Int64,
        DataType::UInt64,
        DataType::Int32,
        DataType::UInt32,
        DataType::Utf8,
    ] {
        for val_dt in [
            DataType::Int64,
            DataType::UInt64,
            DataType::Int32,
            DataType::UInt32,
        ] {
            assert_eq!(
                run(&key_dt, &val_dt),
                expected,
                "key {key_dt} / value {val_dt} must give the same sums as every other type"
            );
        }
    }
}
