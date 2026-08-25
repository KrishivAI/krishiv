//! IVM-AUD-DEC-1: exact fixed-point aggregation.
//!
//! Every test here is written to fail against the pre-fix operator, which
//! refused `Decimal128` outright (falling the view back to full recompute) and
//! keyed MIN/MAX through `f64` for every input type.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use std::sync::Arc;

use arrow::array::{Array, Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krishiv_delta::{Aggregation, DeltaBatch, IncrementalAggOp};

const P: u8 = 38;
const S: i8 = 2;

fn money_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("amount", DataType::Decimal128(P, S), true),
    ]))
}

/// `amounts` are **unscaled** i128 values at scale `S`.
fn money_batch(rows: &[(&str, i128)]) -> RecordBatch {
    let dec = Decimal128Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())
        .with_precision_and_scale(P, S)
        .unwrap();
    RecordBatch::try_new(
        money_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(dec),
        ],
    )
    .unwrap()
}

fn int_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, true),
    ]))
}

fn int_batch(rows: &[(&str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        int_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// The unscaled value of `col` on the single `+1` row of an aggregate output.
fn positive_dec(out: &DeltaBatch, col: &str) -> i128 {
    let data = out.data_batch();
    let idx = data.schema().index_of(col).unwrap();
    let arr = data.column(idx);
    let arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
    let w = out.weights();
    for row in 0..data.num_rows() {
        if w.value(row) == 1 {
            return arr.value(row);
        }
    }
    panic!("no +1 row in output");
}

fn positive_i64(out: &DeltaBatch, col: &str) -> i64 {
    let data = out.data_batch();
    let idx = data.schema().index_of(col).unwrap();
    let arr = data.column(idx);
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    let w = out.weights();
    for row in 0..data.num_rows() {
        if w.value(row) == 1 {
            return arr.value(row);
        }
    }
    panic!("no +1 row in output");
}

fn sum_op() -> IncrementalAggOp {
    IncrementalAggOp::new(
        &money_schema(),
        vec!["k".into()],
        vec![Aggregation::Sum {
            input_col: "amount".into(),
            output_col: "total".into(),
        }],
    )
    .unwrap()
}

#[test]
fn sum_over_decimal_is_exact_where_f64_would_round() {
    // 1e19 + 1 (unscaled) is not representable in f64; three of them sum to a
    // value whose last digit only survives exact i128 accumulation.
    let v: i128 = 10_000_000_000_000_000_001;
    let mut op = sum_op();
    let out = op
        .apply(DeltaBatch::from_inserts(money_batch(&[("a", v), ("a", v), ("a", v)])).unwrap())
        .unwrap();

    assert_eq!(
        positive_dec(&out, "total"),
        3 * v,
        "decimal SUM must be exact; the f64 path answers {}",
        (v as f64 * 3.0) as i128
    );
    // ...and the f64 path really would have been wrong, so the assertion above
    // is not a tautology on this fixture.
    assert_ne!((v as f64 * 3.0) as i128, 3 * v);
}

#[test]
fn decimal_sum_emits_the_sql_result_type() {
    // SUM(Decimal128(38,2)) is Decimal128(38,2): precision widens by 10 but
    // saturates at the Decimal128 maximum, and the scale never moves.
    let op = sum_op();
    let f = op.output_schema().field_with_name("total").unwrap();
    assert_eq!(f.data_type(), &DataType::Decimal128(38, 2));
}

#[test]
fn decimal_avg_emits_the_sql_result_type_and_truncates() {
    let mut op = IncrementalAggOp::new(
        &money_schema(),
        vec!["k".into()],
        vec![Aggregation::Avg {
            input_col: "amount".into(),
            output_col: "mean".into(),
        }],
    )
    .unwrap();
    let f = op.output_schema().field_with_name("mean").unwrap();
    // AVG(Decimal128(p,s)) → Decimal128(min(38,p+4), min(38,s+4)).
    assert_eq!(f.data_type(), &DataType::Decimal128(38, 6));

    // 0.10 / 3 at scale 6 = 0.033333 (truncated toward zero, as DataFusion does).
    let out = op
        .apply(DeltaBatch::from_inserts(money_batch(&[("a", 10), ("a", 0), ("a", 0)])).unwrap())
        .unwrap();
    assert_eq!(positive_dec(&out, "mean"), 33_333);
}

#[test]
fn decimal_sum_retracts_back_to_exactly_zero() {
    let v: i128 = 987_654_321_098_765_432;
    let mut op = sum_op();
    op.apply(DeltaBatch::from_inserts(money_batch(&[("a", v)])).unwrap())
        .unwrap();
    let out = op
        .apply(DeltaBatch::from_deletes(money_batch(&[("a", v)])).unwrap())
        .unwrap();
    // The group is gone: a retraction of the only row leaves no `+1` row.
    let w = out.weights();
    let positives = (0..out.data_batch().num_rows())
        .filter(|&r| w.value(r) == 1)
        .count();
    assert_eq!(positives, 0, "retracting the only row must empty the group");
}

#[test]
fn min_over_int64_past_2_pow_53_does_not_collide() {
    // 2^53 and 2^53+1 are DISTINCT i64 values that share one f64. The old
    // multiset keyed on f64, so these two merged into a single entry: retract
    // the smaller one and the map still reports it as present, making MIN
    // return a value no longer in the relation.
    let lo: i64 = 9_007_199_254_740_992; // 2^53
    let hi: i64 = 9_007_199_254_740_993; // 2^53 + 1
    assert_eq!(lo as f64, hi as f64, "fixture premise: these share one f64");

    let mut op = IncrementalAggOp::new(
        &int_schema(),
        vec!["k".into()],
        vec![Aggregation::Min {
            input_col: "v".into(),
            output_col: "smallest".into(),
        }],
    )
    .unwrap();
    op.apply(DeltaBatch::from_inserts(int_batch(&[("a", lo), ("a", hi)])).unwrap())
        .unwrap();
    let out = op
        .apply(DeltaBatch::from_deletes(int_batch(&[("a", lo)])).unwrap())
        .unwrap();

    assert_eq!(
        positive_i64(&out, "smallest"),
        hi,
        "after retracting 2^53 the minimum is 2^53+1, not the retracted value"
    );
}

#[test]
fn max_over_decimal_returns_the_column_type_not_a_float() {
    let mut op = IncrementalAggOp::new(
        &money_schema(),
        vec!["k".into()],
        vec![Aggregation::Max {
            input_col: "amount".into(),
            output_col: "largest".into(),
        }],
    )
    .unwrap();
    // MIN/MAX draw a value from the column, so the type is the column's own.
    assert_eq!(
        op.output_schema()
            .field_with_name("largest")
            .unwrap()
            .data_type(),
        &DataType::Decimal128(P, S)
    );
    let a: i128 = 79_228_162_514_264_337_593_543_950_336; // > 2^96, unreachable via f64
    let b: i128 = a + 1;
    let out = op
        .apply(DeltaBatch::from_inserts(money_batch(&[("k", a), ("k", b)])).unwrap())
        .unwrap();
    assert_eq!(positive_dec(&out, "largest"), b);
}

#[test]
fn integer_sum_overflow_fails_closed_instead_of_saturating() {
    // The old accumulator was `saturating_add(saturating_mul(..))`: it clamped
    // at i64::MAX and published the clamp as the total, with no error anywhere.
    let mut op = IncrementalAggOp::new(
        &int_schema(),
        vec!["k".into()],
        vec![Aggregation::Sum {
            input_col: "v".into(),
            output_col: "total".into(),
        }],
    )
    .unwrap();
    op.apply(DeltaBatch::from_inserts(int_batch(&[("a", i64::MAX)])).unwrap())
        .unwrap();
    let err = op
        .apply(DeltaBatch::from_inserts(int_batch(&[("a", 1)])).unwrap())
        .expect_err("an unrepresentable total must fail the tick, not saturate");
    assert!(
        err.to_string().contains("overflow"),
        "error should name the overflow, got: {err}"
    );
}

#[test]
fn a_poisoned_accumulator_stays_poisoned() {
    let mut op = IncrementalAggOp::new(
        &int_schema(),
        vec!["k".into()],
        vec![Aggregation::Sum {
            input_col: "v".into(),
            output_col: "total".into(),
        }],
    )
    .unwrap();
    op.apply(DeltaBatch::from_inserts(int_batch(&[("a", i64::MAX)])).unwrap())
        .unwrap();
    let _ = op.apply(DeltaBatch::from_inserts(int_batch(&[("a", 1)])).unwrap());
    // A later, individually-harmless delta must not quietly resume from a
    // total that was never correct.
    let err = op
        .apply(DeltaBatch::from_inserts(int_batch(&[("a", 1)])).unwrap())
        .expect_err("the group's accumulator is poisoned for good");
    assert!(err.to_string().contains("overflow"), "got: {err}");
}

#[test]
fn decimal_state_survives_a_checkpoint_round_trip_exactly() {
    let v: i128 = 10_000_000_000_000_000_001;
    let mut op = sum_op();
    op.apply(DeltaBatch::from_inserts(money_batch(&[("a", v), ("a", v)])).unwrap())
        .unwrap();
    let blob = op.state_bytes();

    let mut restored = sum_op();
    restored.restore_state_bytes(&blob).unwrap();
    let out = restored
        .apply(DeltaBatch::from_inserts(money_batch(&[("a", v)])).unwrap())
        .unwrap();
    assert_eq!(
        positive_dec(&out, "total"),
        3 * v,
        "restored state must carry the exact i128 total, not an f64 shadow of it"
    );
}

#[test]
fn a_v2_state_blob_is_rejected_rather_than_misread() {
    let mut op = sum_op();
    op.apply(DeltaBatch::from_inserts(money_batch(&[("a", 100)])).unwrap())
        .unwrap();
    let mut blob = op.state_bytes();
    blob[..5].copy_from_slice(b"AGGS2");
    let err = sum_op()
        .restore_state_bytes(&blob)
        .expect_err("an older state format must not be parsed as v3");
    assert!(err.to_string().contains("v3"), "got: {err}");
}
