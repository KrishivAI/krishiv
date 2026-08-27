#![forbid(unsafe_code)]

//! Incremental top-N (`ORDER BY … LIMIT k`).
//!
//! # Why this one must hold state, when ORDER BY does not
//!
//! `ORDER BY` alone is a read-time property of a Z-set (IVM-ORDER-1): the
//! relation is unordered and the clause only says how to present it. `LIMIT k`
//! is categorically different — it changes *which rows are in the relation*,
//! and that depends on the order. So it is a real operator.
//!
//! The shape of the state follows from one observation: **a retraction inside
//! the top-k promotes a row from outside it.** An operator holding only k rows
//! cannot name the promoted row, so it would have to re-read the upstream
//! relation — which is the O(state) full recompute this exists to avoid. It
//! therefore holds the whole relation in sort order, the same trade a join
//! trace already makes.
//!
//! Cost per tick is `O(Δ log n + k)`: `log n` to place each delta row in the
//! ordered index, and `k` to diff the new top-k against the published one.
//! Memory is `O(n)`, stated plainly rather than hidden.
//!
//! # Ordering
//!
//! Keys are Arrow row-format encodings ([`RowConverter`]), which are
//! *byte-comparable* and already honour `ASC`/`DESC` and null placement per
//! column. That is why a `BTreeMap` keyed on the encoded bytes sorts correctly
//! for any column type without this module knowing anything about types.
//!
//! The map key is `(sort_key, row_identity)`, not `sort_key` alone: two
//! distinct rows may tie on the sort columns, and collapsing them would lose
//! one. The row identity is the full row's encoding, so ties break
//! deterministically and the same input always yields the same top-k.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::row::{RowConverter, SortField};

use crate::delta_batch::DeltaBatch;
use crate::error::{DeltaError, DeltaResult};

/// A row's position in the index: its byte-comparable sort encoding, then its
/// full-row encoding to break ties between rows that compare equal on the sort
/// columns. Ordering `RowKey` lexicographically orders the relation.
type RowKey = (Vec<u8>, Vec<u8>);

/// A row of the top-k and how many of the `k` slots it occupies.
type TopSlot = (RowKey, i64);

/// One `ORDER BY` term, as the operator needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopNSortKey {
    /// Index of the column in the operator's input schema.
    pub column: usize,
    pub descending: bool,
    pub nulls_first: bool,
}

/// Incremental `ORDER BY … LIMIT k`.
pub struct IncrementalTopNOp {
    schema: SchemaRef,
    keys: Vec<TopNSortKey>,
    k: usize,
    sort_converter: RowConverter,
    row_converter: RowConverter,
    /// The whole relation, ordered. `(sort_key, row_identity) -> net weight`.
    /// Entries reaching weight 0 are removed, so the map never accumulates
    /// tombstones for rows that came and went.
    rows: BTreeMap<RowKey, i64>,
    /// The top-k as last emitted, so a tick can diff against it instead of
    /// recomputing what the downstream already knows.
    published: Vec<TopSlot>,
}

impl IncrementalTopNOp {
    pub fn new(schema: SchemaRef, keys: Vec<TopNSortKey>, k: usize) -> DeltaResult<Self> {
        if keys.is_empty() {
            return Err(DeltaError::Operator(
                "top-N needs at least one sort key".into(),
            ));
        }
        let mut sort_fields = Vec::with_capacity(keys.len());
        for key in &keys {
            let field = schema.fields().get(key.column).ok_or_else(|| {
                DeltaError::Operator(format!("top-N sort column {} out of range", key.column))
            })?;
            sort_fields.push(SortField::new_with_options(
                field.data_type().clone(),
                arrow::compute::SortOptions {
                    descending: key.descending,
                    nulls_first: key.nulls_first,
                },
            ));
        }
        let sort_converter = RowConverter::new(sort_fields)
            .map_err(|e| DeltaError::Operator(format!("top-N sort converter: {e}")))?;
        let row_fields = schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect::<Vec<_>>();
        let row_converter = RowConverter::new(row_fields)
            .map_err(|e| DeltaError::Operator(format!("top-N row converter: {e}")))?;
        Ok(Self {
            schema,
            keys,
            k,
            sort_converter,
            row_converter,
            rows: BTreeMap::new(),
            published: Vec::new(),
        })
    }

    /// Incorporate a delta and return the change to the top-k.
    pub fn apply(&mut self, delta: DeltaBatch) -> DeltaResult<DeltaBatch> {
        let data = delta.data_batch();
        if data.num_rows() > 0 {
            let sort_cols = self
                .keys
                .iter()
                .map(|key| {
                    data.columns().get(key.column).cloned().ok_or_else(|| {
                        DeltaError::Operator("top-N: delta missing sort column".into())
                    })
                })
                .collect::<DeltaResult<Vec<_>>>()?;
            let sort_rows = self
                .sort_converter
                .convert_columns(&sort_cols)
                .map_err(|e| DeltaError::Operator(format!("top-N sort encode: {e}")))?;
            let row_rows = self
                .row_converter
                .convert_columns(data.columns())
                .map_err(|e| DeltaError::Operator(format!("top-N row encode: {e}")))?;
            let weights = delta.weights();
            for i in 0..data.num_rows() {
                let w = weights.value(i);
                if w == 0 {
                    continue;
                }
                let key = (
                    sort_rows.row(i).as_ref().to_vec(),
                    row_rows.row(i).as_ref().to_vec(),
                );
                let updated = {
                    let slot = self.rows.entry(key.clone()).or_insert(0);
                    *slot += w;
                    *slot
                };
                // A row whose net weight reaches zero is not in the relation;
                // keeping a 0-weight entry would make the index grow without
                // bound under insert/delete churn. Removed by key, in
                // O(log n) — an earlier draft scanned the map for *a* zero
                // entry, which is O(n) per delta row and would have made the
                // tick O(delta * n), the exact shape IVM-AUD-PERF-1 removed.
                if updated == 0 {
                    self.rows.remove(&key);
                }
            }
        }
        let next = self.current_top();
        let out = self.diff_against_published(&next)?;
        self.published = next;
        Ok(out)
    }

    /// The top-k of the current relation, in order.
    ///
    /// A row with weight `w > 1` occupies `w` of the `k` slots — `LIMIT` counts
    /// rows, and a multiset row present three times is three rows.
    fn current_top(&self) -> Vec<TopSlot> {
        let mut out: Vec<TopSlot> = Vec::new();
        let mut taken: i64 = 0;
        let cap = i64::try_from(self.k).unwrap_or(i64::MAX);
        for (key, &w) in &self.rows {
            if taken >= cap {
                break;
            }
            if w <= 0 {
                continue;
            }
            let take = w.min(cap - taken);
            out.push((key.clone(), take));
            taken += take;
        }
        out
    }

    /// Emit the change from the published top-k to `next`.
    fn diff_against_published(&self, next: &[TopSlot]) -> DeltaResult<DeltaBatch> {
        let mut delta: BTreeMap<&RowKey, i64> = BTreeMap::new();
        for (key, w) in &self.published {
            *delta.entry(key).or_insert(0) -= w;
        }
        for (key, w) in next {
            *delta.entry(key).or_insert(0) += w;
        }
        delta.retain(|_, w| *w != 0);
        if delta.is_empty() {
            return DeltaBatch::empty(self.schema.clone());
        }
        let parser = self.row_converter.parser();
        let rows: Vec<_> = delta.keys().map(|(_, row)| parser.parse(row)).collect();
        let columns = self
            .row_converter
            .convert_rows(rows)
            .map_err(|e| DeltaError::Operator(format!("top-N row decode: {e}")))?;
        let batch = RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| DeltaError::Operator(format!("top-N rebuild: {e}")))?;
        let weights: Vec<i64> = delta.values().copied().collect();
        DeltaBatch::from_weighted(append_weights(&batch, &weights)?)
    }

    /// Rows retained in the ordered index — the operator's memory footprint,
    /// exposed because IVM-AUD-CORE-25 requires unbounded state to be
    /// observable rather than discovered in a heap profile.
    pub fn retained_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn limit(&self) -> usize {
        self.k
    }
}

pub(crate) fn append_weights(batch: &RecordBatch, weights: &[i64]) -> DeltaResult<RecordBatch> {
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(weights.to_vec())));
    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(
        crate::delta_batch::WEIGHT_COLUMN,
        DataType::Int64,
        false,
    )));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| DeltaError::Operator(format!("top-N weight append: {e}")))
}
