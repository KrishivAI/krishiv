#![forbid(unsafe_code)]

//! Incremental sessionization (SESSION-1): assign every row of a partition to
//! a GAP SESSION — a maximal run of events where each is less than `gap`
//! after its predecessor — and emit the row extended with the session's
//! bounds (`window_start` = first event, `window_end` = last event + gap,
//! the streaming engine's own boundary convention: a distance of exactly
//! `gap` STARTS a new session).
//!
//! Sessions are the one window kind a delta can restructure wholesale: an
//! insert can MERGE two sessions (a bridging event lands within `gap` of
//! both) and a retraction can SPLIT one. No per-row locality survives that,
//! so the operator holds each partition's whole event multiset ordered by
//! time, recomputes the TOUCHED partitions' session assignment per tick —
//! linear in that partition's events — and diffs against what it last
//! emitted, so downstream sees only the rows whose session membership
//! actually changed. Untouched partitions cost nothing.
//!
//! The emitted relation also carries the rewrite's marker columns
//! (`__ivm_snew`, `__ivm_sid`): they are exact per-row functions of the
//! partition's multiset, and emitting them keeps the operator's relation
//! byte-identical to the logical plan it replaces — no special-cased hop
//! schema anywhere (the SCHEMA-1 rule holds by construction). Ties on the
//! timestamp share a session by definition (distance 0), so which tied row
//! wears the boundary marker never changes a session's bounds.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array as _, ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow::row::{RowConverter, SortField};

use crate::delta_batch::DeltaBatch;
use crate::error::{DeltaError, DeltaResult};

/// A row's identity inside its partition: event time, then the full-row
/// encoding to keep tied distinct rows apart.
type RowKey = (i64, Vec<u8>);

/// What one row was last published as: `(snew, sid, window_start,
/// window_end, weight)`.
type Published = (i64, i64, i64, i64, i64);

/// Incremental gap-session assignment per partition.
pub struct IncrementalSessionizeOp {
    /// The SOURCE relation the operator consumes.
    schema: SchemaRef,
    /// The relation it emits: source columns ++ `__ivm_snew`, `__ivm_sid`,
    /// `window_start`, `window_end` (the cascade's own order and types).
    output_schema: SchemaRef,
    partition_cols: Vec<usize>,
    ts_col: usize,
    gap: i64,
    part_converter: RowConverter,
    row_converter: RowConverter,
    /// Partition -> its whole event multiset, time-ordered.
    rows: BTreeMap<Vec<u8>, BTreeMap<RowKey, i64>>,
    /// Partition -> each row's last-emitted session assignment.
    published: BTreeMap<Vec<u8>, BTreeMap<RowKey, Published>>,
}

impl IncrementalSessionizeOp {
    pub fn new(
        schema: SchemaRef,
        output_schema: SchemaRef,
        partition_cols: Vec<usize>,
        ts_col: usize,
        gap: i64,
    ) -> DeltaResult<Self> {
        if partition_cols.is_empty() {
            return Err(DeltaError::Operator(
                "sessionize needs at least one partition column".into(),
            ));
        }
        if gap <= 0 {
            return Err(DeltaError::Operator(
                "sessionize needs a positive gap".into(),
            ));
        }
        if output_schema.fields().len() != schema.fields().len() + 4 {
            return Err(DeltaError::Operator(
                "sessionize output relation must be the source plus \
                 __ivm_snew, __ivm_sid, window_start, window_end"
                    .into(),
            ));
        }
        let mut part_fields = Vec::with_capacity(partition_cols.len());
        for &col in &partition_cols {
            let field = schema.fields().get(col).ok_or_else(|| {
                DeltaError::Operator(format!("sessionize partition column {col} out of range"))
            })?;
            part_fields.push(SortField::new(field.data_type().clone()));
        }
        let part_converter = RowConverter::new(part_fields)
            .map_err(|e| DeltaError::Operator(format!("sessionize partition converter: {e}")))?;
        let row_fields = schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect::<Vec<_>>();
        let row_converter = RowConverter::new(row_fields)
            .map_err(|e| DeltaError::Operator(format!("sessionize row converter: {e}")))?;
        Ok(Self {
            schema,
            output_schema,
            partition_cols,
            ts_col,
            gap,
            part_converter,
            row_converter,
            rows: BTreeMap::new(),
            published: BTreeMap::new(),
        })
    }

    /// Incorporate a delta and return the change to the sessionized relation.
    pub fn apply(&mut self, delta: DeltaBatch) -> DeltaResult<DeltaBatch> {
        let data = delta.data_batch();
        let mut touched: BTreeMap<Vec<u8>, ()> = BTreeMap::new();
        if data.num_rows() > 0 {
            let part_cols = self
                .partition_cols
                .iter()
                .map(|&c| {
                    data.columns().get(c).cloned().ok_or_else(|| {
                        DeltaError::Operator("sessionize: delta missing partition column".into())
                    })
                })
                .collect::<DeltaResult<Vec<_>>>()?;
            let part_rows = self
                .part_converter
                .convert_columns(&part_cols)
                .map_err(|e| DeltaError::Operator(format!("sessionize partition encode: {e}")))?;
            let row_rows = self
                .row_converter
                .convert_columns(data.columns())
                .map_err(|e| DeltaError::Operator(format!("sessionize row encode: {e}")))?;
            let ts_raw = data.columns().get(self.ts_col).ok_or_else(|| {
                DeltaError::Operator("sessionize: delta missing timestamp column".into())
            })?;
            let ts_arr = arrow::compute::cast(ts_raw, &arrow::datatypes::DataType::Int64)
                .map_err(|e| DeltaError::Operator(format!("sessionize timestamp cast: {e}")))?;
            let ts_arr = ts_arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DeltaError::Operator("sessionize timestamp not integral".into()))?;
            let weights = delta.weights();
            for i in 0..data.num_rows() {
                let w = weights.value(i);
                if w == 0 {
                    continue;
                }
                if ts_arr.is_null(i) {
                    return Err(DeltaError::Operator(
                        "sessionize: NULL event time has no session".into(),
                    ));
                }
                let part = part_rows.row(i).as_ref().to_vec();
                let key = (ts_arr.value(i), row_rows.row(i).as_ref().to_vec());
                let part_map = self.rows.entry(part.clone()).or_default();
                let updated = {
                    let slot = part_map.entry(key.clone()).or_insert(0);
                    *slot += w;
                    *slot
                };
                if updated == 0 {
                    part_map.remove(&key);
                }
                touched.insert(part, ());
            }
        }
        // Recompute each touched partition's whole session assignment and
        // diff row by row against what was last emitted.
        let mut out_rows: Vec<(Vec<u8>, Published)> = Vec::new();
        for (part, ()) in touched {
            let next = self.sessionize(&part);
            let published = self.published.remove(&part).unwrap_or_default();
            for (key, old) in &published {
                match next.get(key) {
                    Some(new) if new == old => {}
                    _ => out_rows.push((key.1.clone(), (old.0, old.1, old.2, old.3, -old.4))),
                }
            }
            for (key, new) in &next {
                match published.get(key) {
                    Some(old) if old == new => {}
                    _ => out_rows.push((key.1.clone(), *new)),
                }
            }
            let empty = self.rows.get(&part).is_none_or(|m| m.is_empty());
            if empty {
                self.rows.remove(&part);
            }
            if !next.is_empty() {
                self.published.insert(part, next);
            }
        }
        if out_rows.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }
        self.build_output(&out_rows)
    }

    /// One partition's full session assignment: walk its time-ordered
    /// multiset, a distance `>= gap` starts a new session, and every row of
    /// a session wears the session's `[first, last + gap)` bounds.
    fn sessionize(&self, part: &[u8]) -> BTreeMap<RowKey, Published> {
        let Some(part_map) = self.rows.get(part) else {
            return BTreeMap::new();
        };
        // Split into session runs first, so bounds are known before emitting.
        let mut runs: Vec<Vec<(&RowKey, i64)>> = Vec::new();
        let mut prev_ts: Option<i64> = None;
        for (key, &w) in part_map {
            let new_session = match prev_ts {
                Some(p) => key.0.saturating_sub(p) >= self.gap,
                None => true,
            };
            if new_session {
                runs.push(Vec::new());
            }
            if let Some(run) = runs.last_mut() {
                run.push((key, w));
            }
            prev_ts = Some(key.0);
        }
        let mut out = BTreeMap::new();
        for (sid, run) in runs.iter().enumerate() {
            let (Some(first), Some(last)) = (run.first(), run.last()) else {
                continue;
            };
            let ws = first.0.0;
            let we = last.0.0.saturating_add(self.gap);
            for (idx, (key, w)) in run.iter().enumerate() {
                // The SQL's LAG flag: 1 only on the row that OPENED a later
                // session; the partition's very first row compares against
                // NULL and stays 0.
                let snew = i64::from(idx == 0 && sid > 0);
                out.insert((*key).clone(), (snew, sid as i64, ws, we, *w));
            }
        }
        out
    }

    fn build_output(&self, rows: &[(Vec<u8>, Published)]) -> DeltaResult<DeltaBatch> {
        let parser = self.row_converter.parser();
        let parsed: Vec<_> = rows.iter().map(|(bytes, _)| parser.parse(bytes)).collect();
        let mut columns: Vec<ArrayRef> = self
            .row_converter
            .convert_rows(parsed)
            .map_err(|e| DeltaError::Operator(format!("sessionize row decode: {e}")))?;
        let snew: Vec<i64> = rows.iter().map(|(_, p)| p.0).collect();
        let sid: Vec<i64> = rows.iter().map(|(_, p)| p.1).collect();
        let ws: Vec<i64> = rows.iter().map(|(_, p)| p.2).collect();
        let we: Vec<i64> = rows.iter().map(|(_, p)| p.3).collect();
        for extra in [snew, sid, ws, we] {
            columns.push(Arc::new(Int64Array::from(extra)));
        }
        // Conform every column to the PLANNED output type (MAP-TYPE-1): the
        // cascade's planner may have typed the markers or bounds wider or
        // narrower than the operator's native Int64.
        let conformed: Vec<ArrayRef> = columns
            .iter()
            .zip(self.output_schema.fields())
            .map(|(c, f)| {
                if c.data_type() == f.data_type() {
                    Ok(c.clone())
                } else {
                    arrow::compute::cast(c, f.data_type()).map_err(|e| {
                        DeltaError::Operator(format!(
                            "sessionize column '{}' cast to planned type failed: {e}",
                            f.name()
                        ))
                    })
                }
            })
            .collect::<DeltaResult<Vec<_>>>()?;
        let batch = RecordBatch::try_new(self.output_schema.clone(), conformed)
            .map_err(|e| DeltaError::Operator(format!("sessionize rebuild: {e}")))?;
        let weights: Vec<i64> = rows.iter().map(|(_, p)| p.4).collect();
        DeltaBatch::from_weighted(super::topn::append_weights(&batch, &weights)?)
    }

    /// Rows retained across all partitions (IVM-AUD-CORE-25: unbounded state
    /// must be observable).
    pub fn retained_rows(&self) -> usize {
        self.rows.values().map(BTreeMap::len).sum()
    }

    pub fn source_schema(&self) -> &SchemaRef {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn source_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("bidder", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
        ]))
    }
    fn out_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("bidder", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("__ivm_snew", DataType::Int64, false),
            Field::new("__ivm_sid", DataType::Int64, false),
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
        ]))
    }
    fn batch(rows: &[(i64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            source_schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }
    fn canonical(d: &DeltaBatch) -> Vec<(i64, i64, i64, i64, i64)> {
        let b = d.data_batch();
        let w = d.weights();
        let get = |i: usize| {
            b.column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone()
        };
        let (bidder, ts, ws, we) = (get(0), get(1), get(4), get(5));
        let mut out: Vec<_> = (0..b.num_rows())
            .map(|i| {
                (
                    bidder.value(i),
                    ts.value(i),
                    ws.value(i),
                    we.value(i),
                    w.value(i),
                )
            })
            .collect();
        out.sort();
        out
    }
    fn op() -> IncrementalSessionizeOp {
        IncrementalSessionizeOp::new(source_schema(), out_schema(), vec![0], 1, 5).unwrap()
    }

    /// A bridging event MERGES two sessions: every row of both old sessions
    /// is retracted and re-emitted with the merged bounds. Retracting the
    /// bridge SPLITS them back. The untouched partition never re-emits.
    #[test]
    fn a_bridge_merges_and_its_retraction_splits() {
        let mut op = op();
        let d = op
            .apply(DeltaBatch::from_inserts(batch(&[(7, 1), (7, 2), (7, 8), (8, 50)])).unwrap())
            .unwrap();
        // Sessions: [1,2] → [1,7); [8] → [8,13); bidder 8: [50,55).
        assert_eq!(
            canonical(&d),
            vec![
                (7, 1, 1, 7, 1),
                (7, 2, 1, 7, 1),
                (7, 8, 8, 13, 1),
                (8, 50, 50, 55, 1)
            ]
        );
        // ts=4 bridges: 2→4 and 4→8 are both < 5 → ONE session [1,13).
        let d = op
            .apply(DeltaBatch::from_inserts(batch(&[(7, 4)])).unwrap())
            .unwrap();
        assert_eq!(
            canonical(&d),
            vec![
                (7, 1, 1, 7, -1),
                (7, 1, 1, 13, 1),
                (7, 2, 1, 7, -1),
                (7, 2, 1, 13, 1),
                (7, 4, 1, 13, 1),
                (7, 8, 1, 13, 1),
                (7, 8, 8, 13, -1),
            ],
            "merge retracts both old sessions' rows; bidder 8 silent"
        );
        let d = op
            .apply(DeltaBatch::from_deletes(batch(&[(7, 4)])).unwrap())
            .unwrap();
        assert_eq!(
            canonical(&d),
            vec![
                (7, 1, 1, 7, 1),
                (7, 1, 1, 13, -1),
                (7, 2, 1, 7, 1),
                (7, 2, 1, 13, -1),
                (7, 4, 1, 13, -1),
                (7, 8, 1, 13, -1),
                (7, 8, 8, 13, 1),
            ],
            "the split is the exact inverse"
        );
    }

    /// A distance of EXACTLY `gap` starts a new session — the streaming
    /// engine's own boundary convention (`event_time >= last + gap`).
    #[test]
    fn a_distance_of_exactly_gap_splits() {
        let mut op = op();
        let d = op
            .apply(DeltaBatch::from_inserts(batch(&[(7, 10), (7, 15)])).unwrap())
            .unwrap();
        assert_eq!(
            canonical(&d),
            vec![(7, 10, 10, 15, 1), (7, 15, 15, 20, 1)],
            "15 - 10 == gap → two sessions"
        );
    }
}
