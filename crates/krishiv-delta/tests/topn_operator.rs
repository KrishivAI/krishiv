//! IVM-TOPN-1: incremental `ORDER BY … LIMIT k`.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krishiv_delta::DeltaBatch;
use krishiv_delta::operators::topn::{IncrementalTopNOp, TopNSortKey};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
    ]))
}
fn batch(rows: &[(&str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
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
fn desc_by_score(k: usize) -> IncrementalTopNOp {
    IncrementalTopNOp::new(
        schema(),
        vec![TopNSortKey {
            column: 1,
            descending: true,
            nulls_first: false,
        }],
        k,
    )
    .unwrap()
}
/// Accumulate a delta into a name->weight map, so a test asserts on the
/// relation rather than on emission order.
fn accumulate(acc: &mut std::collections::BTreeMap<String, i64>, d: &DeltaBatch) {
    let data = d.data_batch();
    let names = data
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let w = d.weights();
    for i in 0..data.num_rows() {
        *acc.entry(names.value(i).to_string()).or_insert(0) += w.value(i);
    }
    acc.retain(|_, v| *v != 0);
}
fn names(acc: &std::collections::BTreeMap<String, i64>) -> Vec<String> {
    acc.keys().cloned().collect()
}

#[test]
fn the_first_delta_emits_only_the_top_k() {
    let mut op = desc_by_score(2);
    let mut acc = Default::default();
    let out = op
        .apply(DeltaBatch::from_inserts(batch(&[("a", 10), ("b", 30), ("c", 20)])).unwrap())
        .unwrap();
    accumulate(&mut acc, &out);
    assert_eq!(
        names(&acc),
        vec!["b".to_string(), "c".to_string()],
        "top 2 by score DESC"
    );
}

/// A row arriving above the cut must evict the one it displaces — the emitted
/// delta has to carry the retraction, not just the insertion, or the downstream
/// relation grows past `k`.
#[test]
fn an_insert_above_the_cut_evicts_the_row_it_displaces() {
    let mut op = desc_by_score(2);
    let mut acc = Default::default();
    accumulate(
        &mut acc,
        &op.apply(DeltaBatch::from_inserts(batch(&[("a", 10), ("b", 30)])).unwrap())
            .unwrap(),
    );
    assert_eq!(names(&acc), vec!["a".to_string(), "b".to_string()]);

    accumulate(
        &mut acc,
        &op.apply(DeltaBatch::from_inserts(batch(&[("c", 20)])).unwrap())
            .unwrap(),
    );
    assert_eq!(
        names(&acc),
        vec!["b".to_string(), "c".to_string()],
        "a(10) must be evicted by c(20)"
    );
}

/// **The reason this operator holds the whole relation.** Retracting a row
/// inside the top-k promotes one from outside it. An operator keeping only k
/// rows cannot name the promoted row and would have to re-read upstream — the
/// O(state) recompute this exists to avoid.
#[test]
fn a_retraction_inside_the_cut_promotes_a_row_from_below_it() {
    let mut op = desc_by_score(2);
    let mut acc = Default::default();
    accumulate(
        &mut acc,
        &op.apply(DeltaBatch::from_inserts(batch(&[("a", 10), ("b", 30), ("c", 20)])).unwrap())
            .unwrap(),
    );
    assert_eq!(names(&acc), vec!["b".to_string(), "c".to_string()]);

    // Retract b, the current leader. c moves up and a must be promoted in.
    accumulate(
        &mut acc,
        &op.apply(DeltaBatch::from_deletes(batch(&[("b", 30)])).unwrap())
            .unwrap(),
    );
    assert_eq!(
        names(&acc),
        vec!["a".to_string(), "c".to_string()],
        "a(10) was outside the top 2 and must be promoted when b is retracted"
    );
}

/// A delta that does not change the top-k must emit nothing — otherwise every
/// tick republishes the whole window and the view churns.
#[test]
fn a_delta_below_the_cut_emits_nothing() {
    let mut op = desc_by_score(2);
    let _ = op
        .apply(DeltaBatch::from_inserts(batch(&[("a", 10), ("b", 30)])).unwrap())
        .unwrap();
    let out = op
        .apply(DeltaBatch::from_inserts(batch(&[("z", 1)])).unwrap())
        .unwrap();
    assert!(
        out.is_empty(),
        "z(1) is below the cut; the top-2 did not change"
    );
}

/// Insert/delete churn must not grow the index: a row whose net weight returns
/// to zero is gone, not a tombstone.
#[test]
fn churn_does_not_grow_the_retained_index() {
    let mut op = desc_by_score(2);
    let _ = op
        .apply(DeltaBatch::from_inserts(batch(&[("a", 10), ("b", 30)])).unwrap())
        .unwrap();
    let before = op.retained_rows();
    for _ in 0..100 {
        let _ = op
            .apply(DeltaBatch::from_inserts(batch(&[("t", 5)])).unwrap())
            .unwrap();
        let _ = op
            .apply(DeltaBatch::from_deletes(batch(&[("t", 5)])).unwrap())
            .unwrap();
    }
    assert_eq!(
        op.retained_rows(),
        before,
        "100 insert/delete cycles left {} rows against {before} before — zero-weight \
         entries are being retained as tombstones",
        op.retained_rows()
    );
}

/// `LIMIT` counts rows, so a row present twice occupies two of the k slots.
#[test]
fn multiplicity_counts_against_the_limit() {
    let mut op = desc_by_score(2);
    let mut acc = Default::default();
    let dup = DeltaBatch::from_weighted({
        let b = batch(&[("a", 100), ("b", 50)]);
        let mut cols = b.columns().to_vec();
        cols.push(Arc::new(Int64Array::from(vec![2i64, 1])));
        let mut fields: Vec<Arc<Field>> = b.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new("_weight", DataType::Int64, false)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    })
    .unwrap();
    accumulate(&mut acc, &op.apply(dup).unwrap());
    assert_eq!(
        acc.get("a").copied(),
        Some(2),
        "a is present twice and fills both slots"
    );
    assert!(
        !acc.contains_key("b"),
        "b(50) is pushed out by a's multiplicity"
    );
}
