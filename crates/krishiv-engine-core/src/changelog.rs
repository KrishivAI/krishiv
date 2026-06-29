//! Changelog output: the shared sink contract for all three engines.
//!
//! Batch output is append-only ([`RowKind::Insert`]). Incremental and
//! streaming engines emit the full changelog so upsert/CDC sinks (Iceberg MOR,
//! upsert-Kafka, JDBC) can apply updates and deletes correctly. Unifying the
//! sink side on one type is what lets a sink work identically across the
//! stateful engines.

use arrow::record_batch::RecordBatch;

use crate::error::{EngineError, EngineResult};

/// The kind of change a row represents in a changelog stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKind {
    /// A new row.
    Insert,
    /// The prior image of an updated row (retraction half of an update).
    UpdateBefore,
    /// The new image of an updated row (addition half of an update).
    UpdateAfter,
    /// A removed row.
    Delete,
}

impl RowKind {
    /// DBSP/Z-set weight: `+1` for additions, `-1` for retractions. This is the
    /// bridge to the incremental engine's delta weights.
    pub fn weight(self) -> i64 {
        match self {
            Self::Insert | Self::UpdateAfter => 1,
            Self::Delete | Self::UpdateBefore => -1,
        }
    }

    /// Whether this row retracts a previously emitted row.
    pub fn is_retraction(self) -> bool {
        matches!(self, Self::Delete | Self::UpdateBefore)
    }
}

/// A batch of rows tagged with per-row change kinds.
///
/// Invariant: `row_kinds.len() == batch.num_rows()`, enforced by [`new`](Self::new).
#[derive(Debug, Clone)]
pub struct ChangelogBatch {
    batch: RecordBatch,
    row_kinds: Vec<RowKind>,
}

impl ChangelogBatch {
    /// Build a changelog batch, validating one row kind per row.
    pub fn new(batch: RecordBatch, row_kinds: Vec<RowKind>) -> EngineResult<Self> {
        if row_kinds.len() != batch.num_rows() {
            return Err(EngineError::InvalidJob(format!(
                "changelog row_kinds length {} != batch rows {}",
                row_kinds.len(),
                batch.num_rows()
            )));
        }
        Ok(Self { batch, row_kinds })
    }

    /// An append-only batch where every row is an insertion — batch-engine
    /// output and the common streaming-append case.
    pub fn inserts(batch: RecordBatch) -> Self {
        let row_kinds = vec![RowKind::Insert; batch.num_rows()];
        Self { batch, row_kinds }
    }

    /// The underlying record batch.
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Number of rows (== number of row kinds).
    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    /// Per-row change kinds, aligned with the batch's rows.
    pub fn row_kinds(&self) -> &[RowKind] {
        &self.row_kinds
    }

    /// The change kind of row `row`, or `None` if out of range.
    pub fn row_kind(&self, row: usize) -> Option<RowKind> {
        self.row_kinds.get(row).copied()
    }

    /// Whether every row is an [`RowKind::Insert`] (no retractions present).
    pub fn is_append_only(&self) -> bool {
        self.row_kinds.iter().all(|k| *k == RowKind::Insert)
    }

    /// Consume into the underlying batch and its row kinds.
    pub fn into_parts(self) -> (RecordBatch, Vec<RowKind>) {
        (self.batch, self.row_kinds)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(n: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let arr = Int32Array::from((0..n).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    #[test]
    fn inserts_is_append_only() {
        let cl = ChangelogBatch::inserts(batch(3));
        assert_eq!(cl.num_rows(), 3);
        assert!(cl.is_append_only());
        assert_eq!(cl.row_kind(0), Some(RowKind::Insert));
        assert_eq!(cl.row_kind(3), None);
    }

    #[test]
    fn new_rejects_length_mismatch() {
        let err = ChangelogBatch::new(batch(2), vec![RowKind::Insert]).unwrap_err();
        assert!(matches!(err, EngineError::InvalidJob(_)));
    }

    #[test]
    fn delete_makes_batch_not_append_only() {
        let cl = ChangelogBatch::new(batch(2), vec![RowKind::Insert, RowKind::Delete]).unwrap();
        assert!(!cl.is_append_only());
    }

    #[test]
    fn weights_match_dbsp_convention() {
        assert_eq!(RowKind::Insert.weight(), 1);
        assert_eq!(RowKind::UpdateAfter.weight(), 1);
        assert_eq!(RowKind::Delete.weight(), -1);
        assert_eq!(RowKind::UpdateBefore.weight(), -1);
        assert!(RowKind::Delete.is_retraction());
        assert!(!RowKind::Insert.is_retraction());
    }
}
