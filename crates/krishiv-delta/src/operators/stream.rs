#![forbid(unsafe_code)]

//! Stream ↔ Z-set primitives: `differentiate` and `apply_delta`.
//!
//! These implement the two fundamental DBSP conversion operators:
//!
//! * `differentiate(prev, next)` — computes the delta between two materialized
//!   snapshots: rows gained = insertions (+1), rows lost = retractions (−1).
//!
//! * `apply_delta(current, delta)` — applies an incremental `DeltaBatch` to
//!   a materialized snapshot and returns the new snapshot.
//!
//! Together they form the integration/differentiation duality at the core of
//! the DBSP stream calculus:
//!
//! ```text
//! integrate: stream of Δs  →  running snapshot   (apply_delta, repeated)
//! differentiate: snapshot₁, snapshot₂  →  Δ      (what changed tick-to-tick)
//! ```
//!
//! `IntegrateOp` is a stateful wrapper around repeated `apply_delta` calls.

use std::collections::VecDeque;

use ahash::AHashMap;
use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use arrow::row::{RowConverter, SortField};

use crate::delta_batch::DeltaBatch;
use crate::error::{DeltaError, DeltaResult};
use crate::operators::consolidate::consolidate_batch;

// ── differentiate ─────────────────────────────────────────────────────────────

/// Compute the delta between two materialized snapshots.
///
/// * Rows in `next` not in `prev` → weight `+1` (insertions).
/// * Rows in `prev` not in `next` → weight `−1` (retractions).
/// * Rows present in both with equal multiplicity → no output.
///
/// If `prev` is `None`, all rows of `next` are treated as insertions.
pub fn differentiate(
    schema: &SchemaRef,
    prev: Option<&RecordBatch>,
    next: &RecordBatch,
) -> DeltaResult<DeltaBatch> {
    let prev = match prev {
        None => return DeltaBatch::from_inserts(next.clone()),
        Some(p) => p,
    };

    if prev.num_rows() == 0 && next.num_rows() == 0 {
        return DeltaBatch::empty(schema.clone());
    }
    if prev.num_rows() == 0 {
        return DeltaBatch::from_inserts(next.clone());
    }
    if next.num_rows() == 0 {
        return DeltaBatch::from_deletes(prev.clone());
    }

    // RowConverter gives a stable, comparable byte encoding for each row.
    // This avoids hash collisions by using Arrow's own canonical row format.
    let sort_fields: Vec<SortField> = schema
        .fields()
        .iter()
        .map(|f| SortField::new(f.data_type().clone()))
        .collect();
    let converter = RowConverter::new(sort_fields).map_err(DeltaError::Arrow)?;

    let prev_rows = converter
        .convert_columns(prev.columns())
        .map_err(DeltaError::Arrow)?;
    let next_rows = converter
        .convert_columns(next.columns())
        .map_err(DeltaError::Arrow)?;

    // Pool of prev-row indices available to cancel matching next rows.
    let mut available: AHashMap<Vec<u8>, VecDeque<u32>> = AHashMap::new();
    for i in 0..prev.num_rows() {
        available
            .entry(prev_rows.row(i).as_ref().to_vec())
            .or_default()
            .push_back(i as u32);
    }

    // Match each next row against a prev row of the same content.
    // Unmatched next rows → insertions.
    let mut insert_indices: Vec<u32> = Vec::new();
    for i in 0..next.num_rows() {
        let key = next_rows.row(i).as_ref().to_vec();
        if available
            .get_mut(key.as_slice())
            .and_then(|q| q.pop_front())
            .is_none()
        {
            insert_indices.push(i as u32);
        }
    }

    // Any remaining prev slots were not cancelled → retractions.
    let mut retract_indices: Vec<u32> = Vec::new();
    for q in available.values_mut() {
        retract_indices.extend(q.drain(..));
    }

    if insert_indices.is_empty() && retract_indices.is_empty() {
        return DeltaBatch::empty(schema.clone());
    }

    let mut parts: Vec<DeltaBatch> = Vec::new();
    if !insert_indices.is_empty() {
        parts.push(DeltaBatch::from_inserts(take_rb(next, &insert_indices)?)?);
    }
    if !retract_indices.is_empty() {
        parts.push(DeltaBatch::from_deletes(take_rb(prev, &retract_indices)?)?);
    }
    DeltaBatch::concat(&parts)
}

// ── apply_delta ───────────────────────────────────────────────────────────────

/// Apply `delta` on top of `current` snapshot, returning the updated snapshot.
///
/// Positive-weight rows in `delta` are insertions; negative-weight rows are
/// retractions. The result is the **multiset** with net positive weights — a
/// row with net weight k appears k times, matching what the equivalent SQL
/// over the relation returns (#160).
///
/// For unit-weight insert-only deltas, uses a fast path that concatenates
/// batches via `arrow::compute::concat_batches` instead of the full consolidate-
/// based merge, making source snapshot maintenance truly O(delta) for
/// append-only workloads.
pub fn apply_delta(current: Option<RecordBatch>, delta: &DeltaBatch) -> DeltaResult<RecordBatch> {
    match current {
        None => delta.filter_positive_expanded(),
        Some(prev) => {
            if prev.num_rows() == 0 {
                return delta.filter_positive_expanded();
            }
            // Fast path: every weight exactly +1. Simply append the new rows
            // to the accumulated snapshot without the full O(n) stringify-
            // consolidate roundtrip — the common case for append-only sources.
            // (#160: the previous `is_insert_only` condition also admitted
            // weight-0 rows — wrongly kept — and weight>1 rows, whose extra
            // copies were silently dropped.)
            if delta.weights().iter().all(|w| w == Some(1)) {
                let new_rows = delta.data_batch();
                let combined = arrow::compute::concat_batches(&prev.schema(), &[prev, new_rows])
                    .map_err(|e| {
                        DeltaError::Operator(format!("apply_delta insert-only concat failed: {e}"))
                    })?;
                return Ok(combined);
            }
            // Full case (retractions and/or non-unit weights). Multiset
            // materialization: previously this path collapsed duplicate rows
            // to one (while the fast path above kept them — inconsistent), so
            // a single retraction deleted every copy.
            let prev_db = DeltaBatch::from_inserts(prev)?;
            let merged = DeltaBatch::concat(&[prev_db, delta.clone()])?;
            let consolidated = consolidate_batch(merged, &[], delta.data_schema())?;
            consolidated.filter_positive_expanded()
        }
    }
}

// ── IntegrateOp ───────────────────────────────────────────────────────────────

/// Stateful accumulator of `DeltaBatch`es into a running materialized snapshot.
///
/// Equivalent to repeatedly calling `apply_delta` with the same `current`:
///
/// ```text
/// integrate(Δ₁) = snapshot₁
/// integrate(Δ₂) = snapshot₂  (snapshot₁ + Δ₂)
/// …
/// ```
#[derive(Debug, Default)]
pub struct IntegrateOp {
    snapshot: Option<RecordBatch>,
}

impl IntegrateOp {
    pub fn new() -> Self {
        Self { snapshot: None }
    }

    /// Incorporate `delta` and return the new snapshot.
    pub fn apply(&mut self, delta: &DeltaBatch) -> DeltaResult<RecordBatch> {
        let new_snap = apply_delta(self.snapshot.take(), delta)?;
        self.snapshot = Some(new_snap.clone());
        Ok(new_snap)
    }

    /// Return the current snapshot without ingesting a new delta.
    /// Returns an empty `RecordBatch` using the delta's data schema if no
    /// deltas have been applied yet.
    pub fn snapshot_or_empty(&self, data_schema: &SchemaRef) -> DeltaResult<RecordBatch> {
        match &self.snapshot {
            Some(s) => Ok(s.clone()),
            None => {
                let cols: Vec<_> = data_schema
                    .fields()
                    .iter()
                    .map(|f| arrow::array::new_empty_array(f.data_type()))
                    .collect();
                RecordBatch::try_new(data_schema.clone(), cols).map_err(DeltaError::Arrow)
            }
        }
    }

    pub fn current(&self) -> Option<&RecordBatch> {
        self.snapshot.as_ref()
    }

    pub fn is_empty(&self) -> bool {
        self.snapshot.as_ref().is_none_or(|s| s.num_rows() == 0)
    }

    /// Reset to empty state (e.g., on behavior_version change).
    pub fn reset(&mut self) {
        self.snapshot = None;
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn take_rb(batch: &RecordBatch, indices: &[u32]) -> DeltaResult<RecordBatch> {
    let idx_arr = UInt32Array::from(indices.to_vec());
    let cols: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &idx_arr, None).map_err(DeltaError::Arrow))
        .collect::<DeltaResult<_>>()?;
    RecordBatch::try_new(batch.schema(), cols).map_err(DeltaError::Arrow)
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn mk_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn mk_batch(ids: &[i32]) -> RecordBatch {
        let schema = mk_schema();
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    // ── differentiate ─────────────────────────────────────────────────────────

    #[test]
    fn differentiate_no_prev_all_insertions() {
        let s = mk_schema();
        let next = mk_batch(&[1, 2, 3]);
        let delta = differentiate(&s, None, &next).unwrap();
        assert_eq!(delta.num_rows(), 3);
        assert!(delta.weights().iter().all(|w| w == Some(1)));
    }

    #[test]
    fn differentiate_equal_snapshots_empty_delta() {
        let s = mk_schema();
        let b = mk_batch(&[1, 2]);
        let delta = differentiate(&s, Some(&b), &b).unwrap();
        assert!(delta.is_empty(), "no change → empty delta");
    }

    #[test]
    fn differentiate_row_added() {
        let s = mk_schema();
        let prev = mk_batch(&[1]);
        let next = mk_batch(&[1, 2]);
        let delta = differentiate(&s, Some(&prev), &next).unwrap();
        let positives = delta.filter_positive().unwrap();
        assert_eq!(positives.num_rows(), 1, "only row 2 is new");
        let ids = positives
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
    }

    #[test]
    fn differentiate_row_removed() {
        let s = mk_schema();
        let prev = mk_batch(&[1, 2]);
        let next = mk_batch(&[1]);
        let delta = differentiate(&s, Some(&prev), &next).unwrap();
        let negatives = delta.filter_negative().unwrap();
        assert_eq!(negatives.num_rows(), 1, "only row 2 was removed");
    }

    #[test]
    fn differentiate_empty_prev_empty_next() {
        let s = mk_schema();
        let empty = mk_batch(&[]);
        let delta = differentiate(&s, Some(&empty), &empty).unwrap();
        assert!(delta.is_empty());
    }

    #[test]
    fn differentiate_full_replacement() {
        let s = mk_schema();
        let prev = mk_batch(&[1, 2, 3]);
        let next = mk_batch(&[4, 5]);
        let delta = differentiate(&s, Some(&prev), &next).unwrap();
        let ins = delta.filter_positive().unwrap();
        let ret = delta.filter_negative().unwrap();
        assert_eq!(ins.num_rows(), 2, "4 and 5 added");
        assert_eq!(ret.num_rows(), 3, "1, 2, 3 removed");
    }

    // ── apply_delta ───────────────────────────────────────────────────────────

    #[test]
    fn apply_delta_insertion_on_empty() {
        let rb = mk_batch(&[10]);
        let delta = DeltaBatch::from_inserts(rb).unwrap();
        let snap = apply_delta(None, &delta).unwrap();
        assert_eq!(snap.num_rows(), 1);
    }

    #[test]
    fn apply_delta_retraction_removes_row() {
        let initial = mk_batch(&[1, 2]);
        let del = DeltaBatch::from_deletes(mk_batch(&[1])).unwrap();
        let snap = apply_delta(Some(initial), &del).unwrap();
        assert_eq!(snap.num_rows(), 1);
        let ids = snap
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
    }

    #[test]
    fn apply_delta_net_zero_removes_row() {
        // Insert then retract the same row → net zero → not in snapshot.
        let initial = mk_batch(&[1]);
        let del = DeltaBatch::from_deletes(mk_batch(&[1])).unwrap();
        let snap = apply_delta(Some(initial), &del).unwrap();
        assert_eq!(snap.num_rows(), 0);
    }

    // ── IntegrateOp ───────────────────────────────────────────────────────────

    #[test]
    fn integrate_op_accumulates_across_ticks() {
        let mut op = IntegrateOp::new();
        let s = mk_schema();

        let snap1 = op
            .apply(&DeltaBatch::from_inserts(mk_batch(&[1])).unwrap())
            .unwrap();
        assert_eq!(snap1.num_rows(), 1);

        let snap2 = op
            .apply(&DeltaBatch::from_inserts(mk_batch(&[2])).unwrap())
            .unwrap();
        assert_eq!(snap2.num_rows(), 2);

        let snap3 = op
            .apply(&DeltaBatch::from_deletes(mk_batch(&[1])).unwrap())
            .unwrap();
        assert_eq!(snap3.num_rows(), 1, "row 1 retracted");

        op.reset();
        assert!(op.is_empty());
        let empty = op.snapshot_or_empty(&s).unwrap();
        assert_eq!(empty.num_rows(), 0);
    }
}
