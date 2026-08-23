#![forbid(unsafe_code)]

//! Opt-in **tick-granular** provenance for the diff-based IVM path.
//!
//! # What this actually records (IVM-AUD-PART-23)
//!
//! For every tick in which provenance is enabled, the flow records:
//!
//! * the hash of each input row fed that tick → the tick number, and
//! * the tick number → the set of output row hashes the tick produced.
//!
//! So [`outputs_for`](ProvenanceIndex::outputs_for) answers *"which output rows
//! did the tick that carried this input row produce?"* — **not** *"which output
//! rows were derived from this input row"*. Those are the same answer only when
//! a tick carries a single input row.
//!
//! This is a deliberate narrowing of what the module used to claim. It
//! advertised per-row lineage for "automatic retraction without Z-set algebra",
//! and the only production writer recorded a complete bipartite
//! `inputs × outputs` graph per tick: every input hash mapped to *every* output
//! hash, so a targeted retraction would have retracted the entire tick's output.
//! The stated purpose was unreachable, and the cost of not reaching it was
//! `O(inputs × outputs)` hash inserts — 10 k rows in and 10 k rows out was 10⁸
//! inserts in one tick.
//!
//! The relation stored is now exactly the relation that was answerable all
//! along, stored once instead of `inputs × outputs` times: cost per tick is
//! `O(inputs + outputs)`.
//!
//! Real per-row lineage would have to come from the incremental operators
//! themselves (each emitting an input-row → output-row mapping), and the path
//! that records provenance is the **DiffBased** one, which has no operators at
//! all — it runs the view SQL and differentiates the result. See the register
//! entry for PART-23 for the full trade-off.
//!
//! # Memory
//!
//! Bounded by a retention window of ticks
//! ([`DEFAULT_RETENTION_TICKS`]); everything older is evicted on the next
//! `record_tick`. A lookup for an evicted input returns `None`, which is
//! indistinguishable from "never recorded" — check
//! [`oldest_retained_tick`](ProvenanceIndex::oldest_retained_tick) if that
//! distinction matters.
//!
//! # When to use
//!
//! For sources with **at-most-once** semantics where rows are deleted by
//! logical key (e.g. CDC DELETE events) and the caller wants to know which
//! output rows an ingested batch was responsible for.
//!
//! For pure-SQL IVM views, `step_datafusion` handles retractions automatically
//! via diff-and-update. `ProvenanceIndex` is for the cases outside that path.
//!
//! # Row hashing
//!
//! Use [`hash_batch_row`] to compute reproducible XxHash64 hashes for rows in
//! a `RecordBatch`. The hash covers all data columns with null-byte separators.

use std::collections::BTreeMap;

use ahash::{AHashMap, AHashSet};
use arrow::array::RecordBatch;

use crate::flow::hash_row;

/// Default number of ticks of provenance retained.
///
/// Provenance is a lookup aid for a caller that has just fed a batch and wants
/// to know what it produced; it is not a durable lineage log. Anything older
/// than this many ticks is dropped so an always-on flow cannot grow without
/// bound (IVM-AUD-PART-22: nothing ever evicted before, because the only
/// production writer never recorded an epoch to evict by).
pub const DEFAULT_RETENTION_TICKS: u64 = 256;

// ── ProvenanceIndex ───────────────────────────────────────────────────────────

/// One retained tick.
#[derive(Debug, Default)]
struct TickEntry {
    /// Input hashes first seen in this tick. Kept so eviction can drop exactly
    /// those keys from `input_tick` instead of scanning it (IVM-AUD-PART-24).
    inputs: Vec<u64>,
    /// Output hashes this tick produced, unioned across the tick's views.
    outputs: AHashSet<u64>,
}

/// Tick-granular provenance: input row hash → the outputs of its tick.
///
/// Thread-safety: wrap in `Arc<Mutex<ProvenanceIndex>>` for shared use.
#[derive(Debug)]
pub struct ProvenanceIndex {
    /// Input row hash → the tick it was first seen in.
    input_tick: AHashMap<u64, u64>,
    /// Tick → that tick's inputs and outputs. `BTreeMap` so eviction below a
    /// watermark splits the retained half off in one operation.
    ticks: BTreeMap<u64, TickEntry>,
    /// How many ticks back to retain.
    retention_ticks: u64,
}

impl Default for ProvenanceIndex {
    fn default() -> Self {
        Self::with_retention(DEFAULT_RETENTION_TICKS)
    }
}

impl ProvenanceIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Retain `retention_ticks` ticks of provenance (minimum 1).
    pub fn with_retention(retention_ticks: u64) -> Self {
        Self {
            input_tick: AHashMap::new(),
            ticks: BTreeMap::new(),
            retention_ticks: retention_ticks.max(1),
        }
    }

    /// Record one tick: every hash in `input_hashes` arrived in `tick`, and
    /// `tick` produced every hash in `output_hashes`.
    ///
    /// Cost is `O(inputs + outputs)`. Called once per view that emitted output
    /// in the tick, so a tick's output set is the union across its views.
    /// Evicts everything outside the retention window before returning.
    pub fn record_tick(
        &mut self,
        tick: u64,
        input_hashes: impl IntoIterator<Item = u64>,
        output_hashes: impl IntoIterator<Item = u64>,
    ) {
        // First tick wins. It also keeps the index self-consistent: a hash
        // appears in exactly one tick's `inputs`, and `input_tick` points at
        // that same tick, so evicting a tick can never orphan a live mapping.
        let mut fresh: Vec<u64> = Vec::new();
        for h in input_hashes {
            if let std::collections::hash_map::Entry::Vacant(slot) = self.input_tick.entry(h) {
                slot.insert(tick);
                fresh.push(h);
            }
        }
        let entry = self.ticks.entry(tick).or_default();
        entry.inputs.extend(fresh);
        entry.outputs.extend(output_hashes);
        // Retention: keep [tick - retention + 1, tick].
        let floor = tick.saturating_sub(self.retention_ticks - 1);
        if floor > 0 {
            self.gc_before_epoch(floor);
        }
    }

    /// Evict every tick strictly below `committed_epoch`, and the input hashes
    /// first seen in those ticks.
    ///
    /// `O(evicted ticks + evicted inputs)` — not `O(index)`. The old
    /// implementation walked every epoch-tagged key on every call to build a
    /// list of victims, which made a per-commit GC cost proportional to the
    /// whole index (IVM-AUD-PART-24).
    pub fn gc_before_epoch(&mut self, committed_epoch: u64) {
        let retained = self.ticks.split_off(&committed_epoch);
        let evicted = std::mem::replace(&mut self.ticks, retained);
        for (_, entry) in evicted {
            for h in entry.inputs {
                self.input_tick.remove(&h);
            }
        }
    }

    /// Output hashes produced by the tick that carried `input_hash`.
    ///
    /// `None` if the input was never recorded **or** its tick has aged out of
    /// the retention window. This is tick-granular, not row-granular: see the
    /// module docs.
    pub fn outputs_for(&self, input_hash: u64) -> Option<&AHashSet<u64>> {
        let tick = self.input_tick.get(&input_hash)?;
        self.ticks.get(tick).map(|e| &e.outputs)
    }

    /// The tick `input_hash` was first seen in, if still retained.
    pub fn tick_of(&self, input_hash: u64) -> Option<u64> {
        self.input_tick.get(&input_hash).copied()
    }

    /// Oldest tick still retained, if any. Lets a caller tell "never recorded"
    /// apart from "evicted".
    pub fn oldest_retained_tick(&self) -> Option<u64> {
        self.ticks.keys().next().copied()
    }

    /// Forget the mapping for `input_hash`.
    ///
    /// Only the input's own entry is dropped; the tick's output set is shared
    /// with every other input row of that tick and stays until the tick ages
    /// out.
    pub fn forget(&mut self, input_hash: u64) {
        self.input_tick.remove(&input_hash);
    }

    /// Number of input rows currently tracked.
    pub fn len(&self) -> usize {
        self.input_tick.len()
    }

    pub fn is_empty(&self) -> bool {
        self.input_tick.is_empty()
    }

    /// Number of ticks currently retained.
    pub fn retained_ticks(&self) -> usize {
        self.ticks.len()
    }

    /// Total output hashes held across all retained ticks.
    ///
    /// The memory this index costs is `len() + output_hashes_retained()`. It is
    /// exposed because that total is the whole point of IVM-AUD-PART-23: the
    /// old shape stored `inputs × outputs` output hashes per tick, so a tick
    /// with 200 rows in and 200 rows out held 40 000 of them instead of 200.
    pub fn output_hashes_retained(&self) -> usize {
        self.ticks.values().map(|e| e.outputs.len()).sum()
    }
}

// ── Row hashing helpers ───────────────────────────────────────────────────────

/// Compute an XxHash64 hash for row `row` in `batch` (data columns only).
///
/// Uses the same algorithm as the IVM content-addressed dedup so hashes are
/// consistent between dedup and provenance tracking.
pub fn hash_batch_row(batch: &RecordBatch, row: usize) -> crate::IvmResult<u64> {
    hash_row(batch, row)
}

/// Hash all rows in `batch` and return a `Vec<u64>` of row hashes.
pub fn hash_all_rows(batch: &RecordBatch) -> crate::IvmResult<Vec<u64>> {
    (0..batch.num_rows())
        .map(|row| hash_row(batch, row))
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_lookup() {
        let mut idx = ProvenanceIndex::new();
        idx.record_tick(1, [10, 11], [100, 101]);
        idx.record_tick(2, [20], [200]);

        let outs = idx.outputs_for(10).unwrap();
        assert!(outs.contains(&100));
        assert!(outs.contains(&101));
        // Both inputs of tick 1 see the same tick output set — this index is
        // tick-granular and says so.
        assert_eq!(idx.outputs_for(11), idx.outputs_for(10));
        assert_eq!(idx.outputs_for(20).unwrap().len(), 1);
        assert!(idx.outputs_for(999).is_none());
    }

    #[test]
    fn a_second_view_in_the_same_tick_unions_its_outputs() {
        let mut idx = ProvenanceIndex::new();
        idx.record_tick(7, [1], [100]);
        idx.record_tick(7, [1], [101]);
        let outs = idx.outputs_for(1).unwrap();
        assert!(outs.contains(&100) && outs.contains(&101));
        assert_eq!(idx.retained_ticks(), 1);
    }

    #[test]
    fn forget_removes_the_input_mapping() {
        let mut idx = ProvenanceIndex::new();
        idx.record_tick(1, [1], [42]);
        assert!(idx.outputs_for(1).is_some());
        idx.forget(1);
        assert!(idx.outputs_for(1).is_none());
    }

    /// IVM-AUD-PART-22: nothing ever evicted, because the only production
    /// writer (`record_many`) recorded no epoch and `gc_before_epoch` could
    /// only evict entries that had one. Recording *is* the epoch now, and the
    /// retention window is applied by the writer itself.
    #[test]
    fn recording_evicts_ticks_outside_the_retention_window() {
        let mut idx = ProvenanceIndex::with_retention(3);
        for tick in 1..=10u64 {
            idx.record_tick(tick, [tick], [tick * 100]);
        }
        assert_eq!(
            idx.retained_ticks(),
            3,
            "only the retention window may survive"
        );
        assert_eq!(idx.len(), 3, "input hashes must be evicted with their tick");
        assert_eq!(idx.oldest_retained_tick(), Some(8));
        assert!(idx.outputs_for(1).is_none(), "tick 1 must be gone");
        assert!(idx.outputs_for(10).is_some(), "the latest tick must remain");
    }

    /// IVM-AUD-PART-23: the cost of one tick must be `O(inputs + outputs)`,
    /// not `O(inputs × outputs)`.
    ///
    /// Honest note: this is a property assertion, not a revert-proof. The
    /// defect was the *shape* of the index (one output set per input hash), so
    /// there is no single production line to revert — the 200×200 tick below
    /// held 40 000 output-hash entries under the old shape, which follows from
    /// `record_many` having been called once per input with the whole output
    /// set, not from an experiment run here.
    #[test]
    fn one_tick_costs_inputs_plus_outputs_not_inputs_times_outputs() {
        let mut idx = ProvenanceIndex::new();
        let inputs: Vec<u64> = (0..200).collect();
        let outputs: Vec<u64> = (1_000..1_200).collect();
        idx.record_tick(1, inputs.iter().copied(), outputs.iter().copied());

        assert_eq!(idx.len(), 200, "one entry per input row");
        assert_eq!(
            idx.output_hashes_retained(),
            200,
            "the tick's output set is stored once and shared, not once per input"
        );
        // The answer is unchanged: every input of the tick still maps to the
        // tick's whole output set. That was always the only answer available.
        assert_eq!(idx.outputs_for(0).unwrap().len(), 200);
        assert_eq!(idx.outputs_for(199), idx.outputs_for(0));
    }

    #[test]
    fn gc_before_epoch_keeps_the_watermark_tick() {
        let mut idx = ProvenanceIndex::with_retention(1_000);
        for tick in 1..=5u64 {
            idx.record_tick(tick, [tick], [tick]);
        }
        idx.gc_before_epoch(4);
        assert_eq!(idx.oldest_retained_tick(), Some(4));
        assert!(idx.outputs_for(3).is_none());
        assert!(idx.outputs_for(4).is_some());
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
