#![forbid(unsafe_code)]

//! Opt-in provenance tracking for automatic retraction without Z-set algebra.
//!
//! `ProvenanceIndex` records a mapping from input-row hashes to the output-row
//! hashes that were derived from them.  When an input row is retracted, callers
//! can look up which output rows to retract without needing to re-run the view
//! SQL in reverse or maintain Z-set weights per row.
//!
//! # When to use
//!
//! Use this for sources with **at-most-once** semantics where rows are deleted
//! by logical key (e.g. CDC DELETE events) and the downstream view cannot
//! express a retraction via SQL alone (e.g. ML feature extraction, custom
//! projection).
//!
//! For pure-SQL IVM views, `step_datafusion` handles retractions automatically
//! via diff-and-update.  `ProvenanceIndex` is for the cases outside that path.
//!
//! # Row hashing
//!
//! Use [`hash_batch_row`] to compute reproducible XxHash64 hashes for rows in
//! a `RecordBatch`.  The hash covers all data columns with null-byte separators.

use ahash::{AHashMap, AHashSet};
use arrow::array::RecordBatch;

use crate::flow::hash_row;

// ── ProvenanceIndex ───────────────────────────────────────────────────────────

/// Maps input row hashes → sets of output row hashes derived from each input.
///
/// Thread-safety: wrap in `Arc<Mutex<ProvenanceIndex>>` for shared use.
#[derive(Debug, Default)]
pub struct ProvenanceIndex {
    input_to_outputs: AHashMap<u64, AHashSet<u64>>,
}

impl ProvenanceIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `input_hash` produced `output_hash`.
    pub fn record(&mut self, input_hash: u64, output_hash: u64) {
        self.input_to_outputs
            .entry(input_hash)
            .or_default()
            .insert(output_hash);
    }

    /// Record that `input_hash` produced all hashes in `output_hashes`.
    pub fn record_many(
        &mut self,
        input_hash: u64,
        output_hashes: impl IntoIterator<Item = u64>,
    ) {
        let set = self.input_to_outputs.entry(input_hash).or_default();
        set.extend(output_hashes);
    }

    /// Return the set of output hashes produced by `input_hash`, if any.
    pub fn outputs_for(&self, input_hash: u64) -> Option<&AHashSet<u64>> {
        self.input_to_outputs.get(&input_hash)
    }

    /// Forget all provenance for `input_hash`.
    ///
    /// Call this after retracting all outputs so the index does not grow
    /// without bound.
    pub fn forget(&mut self, input_hash: u64) {
        self.input_to_outputs.remove(&input_hash);
    }

    /// Forget provenance for a batch of input hashes.
    pub fn forget_many(&mut self, input_hashes: &[u64]) {
        for h in input_hashes {
            self.input_to_outputs.remove(h);
        }
    }

    pub fn len(&self) -> usize {
        self.input_to_outputs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.input_to_outputs.is_empty()
    }
}

// ── Row hashing helpers ───────────────────────────────────────────────────────

/// Compute an XxHash64 hash for row `row` in `batch` (data columns only).
///
/// Uses the same algorithm as the IVM content-addressed dedup so hashes are
/// consistent between dedup and provenance tracking.
pub fn hash_batch_row(batch: &RecordBatch, row: usize) -> u64 {
    hash_row(batch, row)
}

/// Hash all rows in `batch` and return a `Vec<u64>` of row hashes.
pub fn hash_all_rows(batch: &RecordBatch) -> Vec<u64> {
    (0..batch.num_rows()).map(|row| hash_row(batch, row)).collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_lookup() {
        let mut idx = ProvenanceIndex::new();
        idx.record(1, 100);
        idx.record(1, 101);
        idx.record(2, 200);

        let outs = idx.outputs_for(1).unwrap();
        assert!(outs.contains(&100));
        assert!(outs.contains(&101));
        assert_eq!(idx.outputs_for(2).unwrap().len(), 1);
    }

    #[test]
    fn forget_removes_entry() {
        let mut idx = ProvenanceIndex::new();
        idx.record(1, 42);
        assert!(idx.outputs_for(1).is_some());
        idx.forget(1);
        assert!(idx.outputs_for(1).is_none());
    }

    #[test]
    fn hash_batch_row_deterministic() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["alice", "bob"]))],
        )
        .unwrap();

        let h0a = hash_batch_row(&batch, 0);
        let h0b = hash_batch_row(&batch, 0);
        let h1 = hash_batch_row(&batch, 1);
        assert_eq!(h0a, h0b);
        assert_ne!(h0a, h1);
    }
}
