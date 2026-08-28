#![forbid(unsafe_code)]

//! A materialized view's snapshot, maintained in O(Δ) (IVM-AUD-PERF-3).
//!
//! # The defect this replaces
//!
//! `operators::stream::apply_delta` rebuilds the whole snapshot every tick: any
//! delta carrying a retraction concatenates the ENTIRE prior snapshot with the
//! delta and re-consolidates the lot, building a `Vec<String>` group key per
//! row. Measured with the delta pinned at ~1000 rows: 10k snapshot → 20.3 ms,
//! 50k → 122.4 ms, 100k → 253.8 ms. Linear in accumulated state, per tick, for
//! every materialized view whose output carries retractions — keep-last,
//! top-N, sessions, and any aggregate that updates a group. The all-`+1` fast
//! path is a plain concat, which is why append-only views never showed it.
//!
//! # The shape
//!
//! Rows are held as a multiset keyed by Arrow's byte-comparable row encoding —
//! the same `RowConverter` `keyed_topn` and `differentiate` already use.
//! Applying a delta is a hash update per delta row; the accumulated rows are
//! never touched. Materialization is deferred to whoever actually reads the
//! snapshot (a checkpoint, the console, a DiffBased baseline) and cached until
//! the next apply, so the per-TICK cost no longer carries it.
//!
//! # Semantics deliberately preserved, not improved
//!
//! `apply_delta` CLAMPS: a retraction with nothing left to cancel is dropped
//! and forgotten, not banked as a debt against a future insert (the deficit
//! that `SourceState` keeps for sources is a different contract — see
//! IVM-AUD-CORE-2). The multiset materializes a weight of `k` as `k` copies.
//! Both are reproduced here exactly, and pinned by
//! `view_snapshot_shape::the_snapshot_contents_are_unchanged_by_the_representation`,
//! which passes against the OLD implementation. Changing either under cover of
//! a performance fix is the silent behaviour change this register keeps
//! catching.
//!
//! A schema the row format cannot encode falls back to the old whole-snapshot
//! path, so an exotic column type loses the speed and keeps the answer.

use ahash::AHashMap;
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::row::{RowConverter, SortField};

use crate::{DeltaBatch, DeltaError, DeltaResult};

/// One view's accumulated output rows.
pub struct SnapshotIndex {
    schema: SchemaRef,
    converter: RowConverter,
    /// Encoded row → (multiplicity, first-seen sequence). Multiplicity is
    /// always `> 0`: an entry that reaches zero is removed, which is what makes
    /// the clamp above fall out of the data structure rather than a branch.
    counts: AHashMap<Vec<u8>, (i64, u64)>,
    next_seq: u64,
    rows: usize,
    /// Materialization, valid until the next apply.
    cache: Option<RecordBatch>,
}

impl SnapshotIndex {
    /// `None` when the schema has a column Arrow's row format cannot encode —
    /// the caller then keeps the whole-snapshot path.
    pub fn new(schema: SchemaRef) -> Option<Self> {
        let fields: Vec<SortField> = schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect();
        let converter = RowConverter::new(fields).ok()?;
        Some(Self {
            schema,
            converter,
            counts: AHashMap::new(),
            next_seq: 0,
            rows: 0,
            cache: None,
        })
    }

    /// Seed from an already-materialized snapshot (restore, or the first
    /// publication of a view that had one).
    pub fn from_batch(schema: SchemaRef, batch: &RecordBatch) -> Option<Self> {
        let mut idx = Self::new(schema)?;
        let delta = DeltaBatch::from_inserts(batch.clone()).ok()?;
        idx.apply(&delta).ok()?;
        Some(idx)
    }

    pub fn num_rows(&self) -> usize {
        self.rows
    }

    /// Incorporate `delta`. Cost is O(|delta|); accumulated rows are untouched.
    ///
    /// Returns the number of retraction rows that had nothing to cancel and
    /// were clamped away, which the view reports as `clamped_retraction_rows`
    /// (IVM-AUD-CORE-2b) — the count must survive this rewrite, because it is
    /// the only signal that a view is being fed retractions it cannot honour.
    pub fn apply(&mut self, delta: &DeltaBatch) -> DeltaResult<u64> {
        let data = delta.data_batch();
        if data.num_rows() == 0 {
            return Ok(0);
        }
        // A delta whose schema differs from the index's is NOT an error here.
        // The pre-PERF-3 path accepted it (IVM-AUD-SCHEMA-1 polices that
        // contract elsewhere, at planning time), and a performance fix must not
        // introduce a new failure. The caller falls back to the whole-snapshot
        // path for these, so the answer is whatever it was before.
        if data.schema() != self.schema {
            return Err(DeltaError::SchemaMismatch(
                "delta schema differs from the accumulated snapshot's".into(),
            ));
        }
        // Net the delta against ITSELF first. `apply_delta` consolidated
        // prev ++ delta together, so a delta carrying `-1` then `+1` for one
        // row nets to zero there; applying row-by-row without this would clamp
        // at the intermediate `-1` and then re-add the row — a different
        // answer for churn deltas, which aggregates emit routinely.
        let settled =
            crate::operators::consolidate::consolidate_batch(delta.clone(), &[], &self.schema)?;
        let data = settled.data_batch();
        let weights = settled.weights();
        let encoded = self
            .converter
            .convert_columns(data.columns())
            .map_err(|e| DeltaError::Operator(format!("snapshot row encode: {e}")))?;

        let mut clamped: u64 = 0;
        for i in 0..data.num_rows() {
            let w = weights.value(i);
            if w == 0 {
                continue;
            }
            let key = encoded.row(i).as_ref().to_vec();
            match self.counts.get_mut(&key) {
                Some(entry) => {
                    let before = entry.0;
                    let after = before + w;
                    if after <= 0 {
                        // Clamp: the unsatisfiable part of the retraction is
                        // dropped and forgotten, exactly as `apply_delta` did.
                        clamped += u64::try_from(-after).unwrap_or(0);
                        self.rows -= usize::try_from(before).unwrap_or(0);
                        self.counts.remove(&key);
                    } else {
                        self.rows = self.rows + usize::try_from(after).unwrap_or(0)
                            - usize::try_from(before).unwrap_or(0);
                        entry.0 = after;
                    }
                }
                None => {
                    if w < 0 {
                        clamped += u64::try_from(-w).unwrap_or(0);
                        continue;
                    }
                    let seq = self.next_seq;
                    self.next_seq += 1;
                    self.counts.insert(key, (w, seq));
                    self.rows += usize::try_from(w).unwrap_or(0);
                }
            }
        }
        self.cache = None;
        Ok(clamped)
    }

    /// The snapshot as one batch, in first-appearance order, each row repeated
    /// by its multiplicity. O(state), paid by the READER and cached.
    pub fn batch(&mut self) -> DeltaResult<RecordBatch> {
        if let Some(cached) = &self.cache {
            return Ok(cached.clone());
        }
        let mut entries: Vec<(&Vec<u8>, i64, u64)> = self
            .counts
            .iter()
            .map(|(k, (count, seq))| (k, *count, *seq))
            .collect();
        entries.sort_by_key(|(_, _, seq)| *seq);
        let parser = self.converter.parser();
        let mut rows = Vec::with_capacity(self.rows);
        for (key, count, _) in entries {
            for _ in 0..count {
                rows.push(parser.parse(key));
            }
        }
        let batch = if rows.is_empty() {
            RecordBatch::new_empty(self.schema.clone())
        } else {
            let columns = self
                .converter
                .convert_rows(rows)
                .map_err(|e| DeltaError::Operator(format!("snapshot row decode: {e}")))?;
            RecordBatch::try_new(self.schema.clone(), columns)
                .map_err(|e| DeltaError::Operator(format!("snapshot rebuild: {e}")))?
        };
        self.cache = Some(batch.clone());
        Ok(batch)
    }

    /// Concatenate `batches` under this index's schema — used when a caller
    /// must hand the snapshot to something that wants one contiguous batch.
    pub fn concat(schema: &SchemaRef, batches: &[RecordBatch]) -> DeltaResult<RecordBatch> {
        concat_batches(schema, batches).map_err(DeltaError::Arrow)
    }
}

/// A view's accumulated state, indexed when the schema allows it.
///
/// The `Raw` arm keeps the pre-IVM-AUD-PERF-3 whole-snapshot path for schemas
/// Arrow's row format cannot encode: such a view loses the speed and keeps the
/// answer, which is the right way round.
#[derive(Default)]
pub enum ViewState {
    #[default]
    Empty,
    Indexed(SnapshotIndex),
    Raw(RecordBatch),
}

impl ViewState {
    /// Incorporate `delta`, returning the retraction rows that were clamped
    /// away (IVM-AUD-CORE-2b).
    pub fn apply(&mut self, schema: &SchemaRef, delta: &DeltaBatch) -> DeltaResult<u64> {
        match self {
            // Built from the DELTA's schema, not the view's declared one: the
            // two can legitimately differ on existing flows, and adopting the
            // delta's keeps this a pure performance change.
            Self::Empty => {
                let delta_schema = delta.data_batch().schema();
                let use_schema = if delta_schema.fields().is_empty() {
                    schema.clone()
                } else {
                    delta_schema
                };
                match SnapshotIndex::new(use_schema) {
                    Some(mut idx) => {
                        let clamped = idx.apply(delta)?;
                        *self = Self::Indexed(idx);
                        Ok(clamped)
                    }
                    None => {
                        let updated = crate::operators::stream::apply_delta(None, delta)?;
                        let clamped = clamped_rows(None, delta, &updated);
                        *self = Self::Raw(updated);
                        Ok(clamped)
                    }
                }
            }
            Self::Indexed(idx) => match idx.apply(delta) {
                Err(DeltaError::SchemaMismatch(_)) => {
                    // Fall back, preserving the pre-PERF-3 answer for a view
                    // whose published schema drifts from its accumulated one.
                    let prev = idx.batch()?;
                    let updated = crate::operators::stream::apply_delta(Some(prev.clone()), delta)?;
                    let clamped = clamped_rows(Some(&prev), delta, &updated);
                    *self = Self::Raw(updated);
                    Ok(clamped)
                }
                other => other,
            },
            Self::Raw(prev) => {
                // Promote to the indexed form ONCE, then every later apply is
                // O(|delta|). The promotion costs one pass over the accumulated
                // rows — which is what this arm paid on EVERY apply before. A
                // schema the row format cannot encode stays Raw and keeps the
                // old behaviour.
                if let Some(mut idx) = SnapshotIndex::from_batch(prev.schema(), prev) {
                    let clamped = idx.apply(delta)?;
                    *self = Self::Indexed(idx);
                    return Ok(clamped);
                }
                let updated = crate::operators::stream::apply_delta(Some(prev.clone()), delta)?;
                let clamped = clamped_rows(Some(prev), delta, &updated);
                *self = Self::Raw(updated);
                Ok(clamped)
            }
        }
    }

    /// The materialized state, or `None` if the view has never published.
    pub fn batch(&mut self) -> DeltaResult<Option<RecordBatch>> {
        match self {
            Self::Empty => Ok(None),
            Self::Indexed(idx) => idx.batch().map(Some),
            Self::Raw(b) => Ok(Some(b.clone())),
        }
    }

    /// Replace the state wholesale (restore, executor swap-in, reset).
    pub fn set(&mut self, schema: &SchemaRef, batch: Option<RecordBatch>) {
        *self = match batch {
            None => Self::Empty,
            Some(b) => match SnapshotIndex::from_batch(schema.clone(), &b) {
                Some(idx) => Self::Indexed(idx),
                None => Self::Raw(b),
            },
        };
    }

    /// Store `batch` WITHOUT indexing it.
    ///
    /// For the DiffBased path, which sets the baseline from an
    /// already-materialized snapshot every tick: indexing there costs a full
    /// re-encode of every row and buys nothing, because the next apply promotes
    /// it anyway. Measured: re-indexing here made the DiffBased arm ~1.26x
    /// slower than before PERF-3, which would have inflated the incremental
    /// speedup this change reports.
    pub fn set_raw(&mut self, batch: Option<RecordBatch>) {
        *self = match batch {
            None => Self::Empty,
            Some(b) => Self::Raw(b),
        };
    }

    pub fn is_set(&self) -> bool {
        !matches!(self, Self::Empty)
    }
}

/// The pre-fix clamp accounting, kept for the `Raw` arm so both paths report
/// `clamped_retraction_rows` the same way.
fn clamped_rows(prev: Option<&RecordBatch>, delta: &DeltaBatch, updated: &RecordBatch) -> u64 {
    let mut expected: i64 = prev.map_or(0, |p| p.num_rows() as i64);
    for w in delta.weights().iter().flatten() {
        expected = expected.saturating_add(w);
    }
    let clamped = (updated.num_rows() as i64).saturating_sub(expected).max(0);
    u64::try_from(clamped).unwrap_or(0)
}
