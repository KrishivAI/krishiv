#![forbid(unsafe_code)]

//! Incremental keyed top-N (TOPNK-1): `ORDER BY … LIMIT k` **per partition**.
//!
//! The streaming SQL surface spells per-key-per-window ranking as `GROUP BY
//! <keys> ORDER BY <col> LIMIT n` (NEXMark q19's per-auction top-10, q18's
//! keep-last dedup as top-1 by event time). The relation is the union of every
//! partition's top-k, and a delta touching one partition can only change that
//! partition's slice — which is what makes the maintenance O(Δ) instead of
//! O(partitions).
//!
//! Everything [`super::topn`] establishes carries over per partition: the
//! whole partition is held in sort order because a retraction inside its top-k
//! promotes a row from outside it; keys are byte-comparable Arrow row
//! encodings; the map key is `(sort_key, row_identity)` so ties never
//! collapse. The state is a map of partitions rather than one flat index, and
//! `apply` diffs ONLY the partitions the delta touched against their published
//! slices.
//!
//! Cost per tick is `O(Δ log n_p + t·k)` where `t` is the number of touched
//! partitions and `n_p` the touched partition's size. Memory is `O(n)` over
//! all partitions, observable via [`retained_rows`](IncrementalKeyedTopNOp::retained_rows);
//! a partition whose rows and published slice both empty is removed outright,
//! so insert/delete churn does not accumulate tombstone partitions.

use std::collections::BTreeMap;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::row::{RowConverter, SortField};

use super::topn::{TopNSortKey, append_weights};
use crate::delta_batch::DeltaBatch;
use crate::error::{DeltaError, DeltaResult};

/// A row's position inside its partition: sort encoding, then full-row
/// encoding to break ties. Identical to the global operator's key — the
/// partition lives in the outer map, not in here.
type RowKey = (Vec<u8>, Vec<u8>);

/// A row of a partition's top-k and how many of the `k` slots it occupies.
type TopSlot = (RowKey, i64);

/// Incremental `ORDER BY … LIMIT k` per partition.
pub struct IncrementalKeyedTopNOp {
    schema: SchemaRef,
    partition_cols: Vec<usize>,
    keys: Vec<TopNSortKey>,
    k: usize,
    part_converter: RowConverter,
    sort_converter: RowConverter,
    row_converter: RowConverter,
    /// Partition encoding -> that partition's whole relation, ordered.
    rows: BTreeMap<Vec<u8>, BTreeMap<RowKey, i64>>,
    /// Each partition's top-k as last emitted.
    published: BTreeMap<Vec<u8>, Vec<TopSlot>>,
}

impl IncrementalKeyedTopNOp {
    pub fn new(
        schema: SchemaRef,
        partition_cols: Vec<usize>,
        keys: Vec<TopNSortKey>,
        k: usize,
    ) -> DeltaResult<Self> {
        if partition_cols.is_empty() {
            return Err(DeltaError::Operator(
                "keyed top-N needs at least one partition column (use the \
                 global top-N operator otherwise)"
                    .into(),
            ));
        }
        if keys.is_empty() {
            return Err(DeltaError::Operator(
                "keyed top-N needs at least one sort key".into(),
            ));
        }
        let mut part_fields = Vec::with_capacity(partition_cols.len());
        for &col in &partition_cols {
            let field = schema.fields().get(col).ok_or_else(|| {
                DeltaError::Operator(format!("keyed top-N partition column {col} out of range"))
            })?;
            part_fields.push(SortField::new(field.data_type().clone()));
        }
        let part_converter = RowConverter::new(part_fields)
            .map_err(|e| DeltaError::Operator(format!("keyed top-N partition converter: {e}")))?;
        let mut sort_fields = Vec::with_capacity(keys.len());
        for key in &keys {
            let field = schema.fields().get(key.column).ok_or_else(|| {
                DeltaError::Operator(format!(
                    "keyed top-N sort column {} out of range",
                    key.column
                ))
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
            .map_err(|e| DeltaError::Operator(format!("keyed top-N sort converter: {e}")))?;
        let row_fields = schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect::<Vec<_>>();
        let row_converter = RowConverter::new(row_fields)
            .map_err(|e| DeltaError::Operator(format!("keyed top-N row converter: {e}")))?;
        Ok(Self {
            schema,
            partition_cols,
            keys,
            k,
            part_converter,
            sort_converter,
            row_converter,
            rows: BTreeMap::new(),
            published: BTreeMap::new(),
        })
    }

    /// Incorporate a delta and return the change to the union of per-partition
    /// top-k slices.
    pub fn apply(&mut self, delta: DeltaBatch) -> DeltaResult<DeltaBatch> {
        let data = delta.data_batch();
        // Partitions this delta touched — the only ones whose slice can change.
        let mut touched: BTreeMap<Vec<u8>, ()> = BTreeMap::new();
        if data.num_rows() > 0 {
            let part_cols = self
                .partition_cols
                .iter()
                .map(|&c| {
                    data.columns().get(c).cloned().ok_or_else(|| {
                        DeltaError::Operator("keyed top-N: delta missing partition column".into())
                    })
                })
                .collect::<DeltaResult<Vec<_>>>()?;
            let part_rows = self
                .part_converter
                .convert_columns(&part_cols)
                .map_err(|e| DeltaError::Operator(format!("keyed top-N partition encode: {e}")))?;
            let sort_cols = self
                .keys
                .iter()
                .map(|key| {
                    data.columns().get(key.column).cloned().ok_or_else(|| {
                        DeltaError::Operator("keyed top-N: delta missing sort column".into())
                    })
                })
                .collect::<DeltaResult<Vec<_>>>()?;
            let sort_rows = self
                .sort_converter
                .convert_columns(&sort_cols)
                .map_err(|e| DeltaError::Operator(format!("keyed top-N sort encode: {e}")))?;
            let row_rows = self
                .row_converter
                .convert_columns(data.columns())
                .map_err(|e| DeltaError::Operator(format!("keyed top-N row encode: {e}")))?;
            let weights = delta.weights();
            for i in 0..data.num_rows() {
                let w = weights.value(i);
                if w == 0 {
                    continue;
                }
                let part = part_rows.row(i).as_ref().to_vec();
                let key = (
                    sort_rows.row(i).as_ref().to_vec(),
                    row_rows.row(i).as_ref().to_vec(),
                );
                let part_map = self.rows.entry(part.clone()).or_default();
                let updated = {
                    let slot = part_map.entry(key.clone()).or_insert(0);
                    *slot += w;
                    *slot
                };
                // Net-zero rows leave the index (the topn.rs churn rule),
                // and a partition drained of rows leaves the outer map —
                // unless its published slice still owes retractions, which
                // the diff below emits before the entry is dropped.
                if updated == 0 {
                    part_map.remove(&key);
                }
                touched.insert(part, ());
            }
        }
        // Diff every touched partition's new top-k against its published one.
        let mut delta_rows: BTreeMap<(Vec<u8>, RowKey), i64> = BTreeMap::new();
        for (part, ()) in touched {
            let next = self.current_top(&part);
            let published = self.published.get(&part).cloned().unwrap_or_default();
            for (key, w) in &published {
                *delta_rows.entry((part.clone(), key.clone())).or_insert(0) -= w;
            }
            for (key, w) in &next {
                *delta_rows.entry((part.clone(), key.clone())).or_insert(0) += w;
            }
            let empty = self.rows.get(&part).is_none_or(|m| m.is_empty());
            if empty {
                self.rows.remove(&part);
            }
            if next.is_empty() {
                self.published.remove(&part);
            } else {
                self.published.insert(part, next);
            }
        }
        delta_rows.retain(|_, w| *w != 0);
        if delta_rows.is_empty() {
            return DeltaBatch::empty(self.schema.clone());
        }
        let parser = self.row_converter.parser();
        let rows: Vec<_> = delta_rows
            .keys()
            .map(|(_, (_, row))| parser.parse(row))
            .collect();
        let columns = self
            .row_converter
            .convert_rows(rows)
            .map_err(|e| DeltaError::Operator(format!("keyed top-N row decode: {e}")))?;
        let batch = RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| DeltaError::Operator(format!("keyed top-N rebuild: {e}")))?;
        let weights: Vec<i64> = delta_rows.values().copied().collect();
        DeltaBatch::from_weighted(append_weights(&batch, &weights)?)
    }

    /// One partition's current top-k, in order. Multiset rows occupy as many
    /// of the `k` slots as their weight (`LIMIT` counts rows).
    fn current_top(&self, part: &[u8]) -> Vec<TopSlot> {
        let Some(part_map) = self.rows.get(part) else {
            return Vec::new();
        };
        let mut out: Vec<TopSlot> = Vec::new();
        let mut taken: i64 = 0;
        let cap = i64::try_from(self.k).unwrap_or(i64::MAX);
        for (key, &w) in part_map {
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

    /// Rows retained across all partitions — the operator's memory footprint
    /// (IVM-AUD-CORE-25: unbounded state must be observable).
    pub fn retained_rows(&self) -> usize {
        self.rows.values().map(BTreeMap::len).sum()
    }

    pub fn limit(&self) -> usize {
        self.k
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int64, false),
            Field::new("score", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
        ]))
    }

    fn batch(rows: &[(i64, i64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn canonical(d: &DeltaBatch) -> Vec<(i64, i64, i64, i64)> {
        let b = d.data_batch();
        let w = d.weights();
        let c0 = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let c1 = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let c2 = b
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let mut out: Vec<_> = (0..b.num_rows())
            .map(|i| (c0.value(i), c1.value(i), c2.value(i), w.value(i)))
            .collect();
        out.sort();
        out
    }

    fn op(k: usize) -> IncrementalKeyedTopNOp {
        IncrementalKeyedTopNOp::new(
            schema(),
            vec![0],
            vec![TopNSortKey {
                column: 1,
                descending: true,
                nulls_first: false,
            }],
            k,
        )
        .unwrap()
    }

    /// A retraction INSIDE a partition's top-k promotes that partition's next
    /// row — and only that partition emits. The untouched partition proves the
    /// per-partition diff never re-emits what the downstream already has.
    #[test]
    fn retraction_promotes_within_one_partition_only() {
        let mut op = op(2);
        let d = op
            .apply(
                DeltaBatch::from_inserts(batch(&[
                    (1, 30, 100),
                    (1, 20, 101),
                    (1, 10, 102),
                    (2, 5, 200),
                ]))
                .unwrap(),
            )
            .unwrap();
        assert_eq!(
            canonical(&d),
            vec![(1, 20, 101, 1), (1, 30, 100, 1), (2, 5, 200, 1)],
            "top-2 of partition 1 plus all of partition 2"
        );
        let d = op
            .apply(DeltaBatch::from_deletes(batch(&[(1, 30, 100)])).unwrap())
            .unwrap();
        assert_eq!(
            canonical(&d),
            vec![(1, 10, 102, 1), (1, 30, 100, -1)],
            "the retracted leader leaves, the outside row is promoted; \
             partition 2 stays silent"
        );
    }

    /// Draining a partition retracts its published slice and removes the
    /// partition entirely (no tombstones under churn).
    #[test]
    fn a_drained_partition_retracts_and_disappears() {
        let mut op = op(1);
        op.apply(DeltaBatch::from_inserts(batch(&[(1, 30, 100), (2, 7, 200)])).unwrap())
            .unwrap();
        let d = op
            .apply(DeltaBatch::from_deletes(batch(&[(1, 30, 100)])).unwrap())
            .unwrap();
        assert_eq!(canonical(&d), vec![(1, 30, 100, -1)]);
        assert_eq!(op.retained_rows(), 1, "only partition 2's row remains");
    }
}
