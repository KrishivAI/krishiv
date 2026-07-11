//! Property tests for the Z-set algebra (audit §14 TEST-3).
//!
//! The model is the mathematical Z-set: a map `key → net weight` with zero
//! weights removed.  Every property checks a `DeltaBatch`/`Trace` operation
//! against the model computed independently in plain Rust, over randomized
//! row sequences with colliding keys and arbitrary (including zero and
//! multi-unit) weights.

// Test harness: panicking on invariant violation is the assertion.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krishiv_delta::{
    DeltaBatch, Trace, WEIGHT_COLUMN, consolidate_batch, deserialize_delta_batch,
    serialize_delta_batch,
};
use proptest::prelude::*;

// ── Model helpers ─────────────────────────────────────────────────────────────

/// A raw Z-set sample: (key, weight) rows, unconsolidated.
type Rows = Vec<(i64, i64)>;

fn data_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

fn zbatch(rows: &Rows) -> DeltaBatch {
    let full_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]));
    let keys: Vec<i64> = rows.iter().map(|(k, _)| *k).collect();
    let weights: Vec<i64> = rows.iter().map(|(_, w)| *w).collect();
    let inner = RecordBatch::try_new(
        full_schema,
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(weights)),
        ],
    )
    .expect("build weighted batch");
    DeltaBatch::from_weighted(inner).expect("wrap weighted batch")
}

/// Model Z-set: sum weights per key, drop zeros.
fn model(rows: &Rows) -> BTreeMap<i64, i64> {
    let mut m = BTreeMap::new();
    for (k, w) in rows {
        *m.entry(*k).or_insert(0) += *w;
    }
    m.retain(|_, w| *w != 0);
    m
}

/// Read a `DeltaBatch` back into the model representation.
fn batch_model(batch: &DeltaBatch) -> BTreeMap<i64, i64> {
    let data = batch.data_batch();
    let keys = data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("k column is Int64");
    let weights = batch.weights();
    let mut m = BTreeMap::new();
    for i in 0..batch.num_rows() {
        *m.entry(keys.value(i)).or_insert(0) += weights.value(i);
    }
    m.retain(|_, w| *w != 0);
    m
}

/// Multiset of a plain `RecordBatch` (single Int64 column): key → row count.
fn multiset(batch: &RecordBatch) -> BTreeMap<i64, i64> {
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("k column is Int64");
    let mut m = BTreeMap::new();
    for i in 0..batch.num_rows() {
        *m.entry(keys.value(i)).or_insert(0) += 1;
    }
    m
}

fn model_add(a: &BTreeMap<i64, i64>, b: &BTreeMap<i64, i64>) -> BTreeMap<i64, i64> {
    let mut m = a.clone();
    for (k, w) in b {
        *m.entry(*k).or_insert(0) += *w;
    }
    m.retain(|_, w| *w != 0);
    m
}

fn consolidated(rows: &Rows) -> DeltaBatch {
    consolidate_batch(zbatch(rows), &[], &data_schema()).expect("consolidate")
}

// ── Strategies ────────────────────────────────────────────────────────────────

/// Small key domain forces collisions; weights include 0 and multi-unit ±.
fn rows_strategy() -> impl Strategy<Value = Rows> {
    proptest::collection::vec((-2_i64..6, -3_i64..=3), 0..24)
}

// ── Properties ────────────────────────────────────────────────────────────────

proptest! {
    /// consolidate(a ++ b) computes the model sum a + b.
    #[test]
    fn consolidation_matches_model_addition(a in rows_strategy(), b in rows_strategy()) {
        let cat = DeltaBatch::concat(&[zbatch(&a), zbatch(&b)]).expect("concat");
        let out = consolidate_batch(cat, &[], &data_schema()).expect("consolidate");
        prop_assert_eq!(batch_model(&out), model_add(&model(&a), &model(&b)));
    }

    /// Z-set addition is commutative under consolidation.
    #[test]
    fn addition_commutes(a in rows_strategy(), b in rows_strategy()) {
        let ab = DeltaBatch::concat(&[zbatch(&a), zbatch(&b)]).expect("concat ab");
        let ba = DeltaBatch::concat(&[zbatch(&b), zbatch(&a)]).expect("concat ba");
        let schema = data_schema();
        let cab = consolidate_batch(ab, &[], &schema).expect("consolidate ab");
        let cba = consolidate_batch(ba, &[], &schema).expect("consolidate ba");
        prop_assert_eq!(batch_model(&cab), batch_model(&cba));
    }

    /// negate() is the additive inverse: a + (−a) = ∅.
    #[test]
    fn negation_cancels(a in rows_strategy()) {
        let batch = zbatch(&a);
        let neg = batch.negate().expect("negate");
        let cat = DeltaBatch::concat(&[batch, neg]).expect("concat");
        let out = consolidate_batch(cat, &[], &data_schema()).expect("consolidate");
        prop_assert!(out.is_empty(), "a + (−a) must consolidate to empty, got {} rows", out.num_rows());
    }

    /// Consolidation is idempotent.
    #[test]
    fn consolidation_is_idempotent(a in rows_strategy()) {
        let once = consolidated(&a);
        let twice = consolidate_batch(once.clone(), &[], &data_schema()).expect("re-consolidate");
        prop_assert_eq!(batch_model(&once), batch_model(&twice));
        prop_assert_eq!(once.num_rows(), twice.num_rows());
    }

    /// serialize → deserialize preserves rows and weights exactly (raw form,
    /// not just up to consolidation).
    #[test]
    fn serialization_roundtrips_exactly(a in rows_strategy()) {
        let batch = zbatch(&a);
        let bytes = serialize_delta_batch(&batch).expect("serialize");
        let restored = deserialize_delta_batch(&bytes).expect("deserialize");
        prop_assert_eq!(restored.num_rows(), batch.num_rows());
        let orig_w: Vec<i64> = batch.weights().iter().map(|w| w.unwrap_or(0)).collect();
        let rest_w: Vec<i64> = restored.weights().iter().map(|w| w.unwrap_or(0)).collect();
        prop_assert_eq!(orig_w, rest_w);
        prop_assert_eq!(multiset(&batch.data_batch()), multiset(&restored.data_batch()));
    }

    /// The positive part with multiset semantics: a consolidated row with net
    /// weight w appears max(w, 0) times.
    #[test]
    fn positive_expansion_matches_model(a in rows_strategy()) {
        let out = consolidated(&a).filter_positive_expanded().expect("expand");
        let expected: BTreeMap<i64, i64> = model(&a)
            .into_iter()
            .filter(|(_, w)| *w > 0)
            .collect();
        prop_assert_eq!(multiset(&out), expected);
    }

    /// normalize_weights clamps to sign; drop_zeros then removes exactly the
    /// zero-weight rows.
    #[test]
    fn normalize_and_drop_zeros_match_signs(a in rows_strategy()) {
        let out = zbatch(&a)
            .normalize_weights()
            .expect("normalize")
            .drop_zeros()
            .expect("drop zeros");
        let expected: Vec<i64> = a.iter().map(|(_, w)| w.signum()).filter(|s| *s != 0).collect();
        let got: Vec<i64> = out.weights().iter().map(|w| w.unwrap_or(0)).collect();
        prop_assert_eq!(got, expected);
    }

    /// A `Trace` fed the same rows in arbitrary chunkings always snapshots to
    /// the model's positive part — regardless of how the LSM levels merged.
    #[test]
    fn trace_snapshot_matches_model(
        a in rows_strategy(),
        chunk_size in 1_usize..5,
        force_consolidate in proptest::bool::ANY,
    ) {
        let mut trace = Trace::new(data_schema(), &["k"]).expect("trace");
        for chunk in a.chunks(chunk_size) {
            trace.insert(zbatch(&chunk.to_vec()));
        }
        if force_consolidate {
            trace.consolidate().expect("consolidate");
        }
        let snap = trace.snapshot().expect("snapshot");
        let expected: BTreeMap<i64, i64> = model(&a)
            .into_iter()
            .filter(|(_, w)| *w > 0)
            .collect();
        prop_assert_eq!(multiset(&snap), expected);
    }
}
