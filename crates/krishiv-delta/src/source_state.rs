#![forbid(unsafe_code)]

//! `SourceState` — an IVM source's accumulated state held as a faithful Z-set.
//!
//! # Why this exists (IVM-AUD-CORE-2)
//!
//! Source state used to be a plain `RecordBatch` advanced by
//! [`apply_delta`](crate::apply_delta), which ends in
//! [`DeltaBatch::filter_positive_expanded`] — and that function clamps a net
//! weight at zero, because a relation cannot contain a row `-1` times. That is
//! the right answer for *materialization* and the wrong one for *state*: a
//! retraction with nothing to cancel was forgotten, so an out-of-order CDC
//! stream that delivered `DELETE 42` before `INSERT 42` ended with row 42
//! present. The Z-set answer is weight 0 — absent.
//!
//! `SourceState` keeps the same relation as before **plus** the part of the
//! Z-set that cannot be materialized:
//!
//! ```text
//! zset  =  positive (a multiset: a row with weight k appears k times)
//!        + deficit  (rows whose net weight is strictly negative)
//! ```
//!
//! # Invariants
//!
//! 1. Every weight in `deficit` is strictly negative (never `0`, never `> 0`).
//! 2. `positive` and `deficit` share no row value: a row is either present
//!    some number of times or owed some number of times, never both.
//!
//! Both are `debug_assert`ed by [`SourceState::assert_invariants`], which
//! [`SourceState::from_zset`], [`SourceState::apply`] and
//! [`SourceState::set_deficit`] call. ([`SourceState::from_positive`] does not
//! need it: it produces no deficit, so both hold trivially.) The check returns
//! immediately when there is no deficit, so the append-only path never pays
//! for it even in a debug build.
//!
//! # Cost
//!
//! [`SourceState::apply`] has three branches, and which one runs is observable
//! through [`SourceState::consolidations`] (a test cannot tell them apart from
//! the answer alone — every branch computes the same Z-set):
//!
//! | Delta | Deficit | Work | Counted |
//! |---|---|---|---|
//! | all `+1` | empty | `concat_batches(positive, delta)` — O(Δ) | no |
//! | all `+1` | non-empty | consolidate `deficit ++ delta` — O(deficit + Δ) | no |
//! | any `≤ 0` or `> 1` | either | consolidate the whole Z-set — O(state) | yes |
//!
//! The first row is the same `concat_batches` the append-only fast path in
//! `apply_delta` already did, which is why an append-only source pays what it
//! paid before. The third runs the same consolidate `apply_delta` already ran
//! for a delta carrying a retraction, plus one boolean mask over the
//! consolidated result to split the negatives out — so it is that path's cost
//! plus O(consolidated rows), not a new order of growth.

use arrow::array::{BooleanArray, RecordBatch};

use crate::delta_batch::DeltaBatch;
use crate::error::{DeltaError, DeltaResult};
use crate::operators::consolidate::consolidate_batch;

/// One IVM source's accumulated state, as a Z-set split into the part that can
/// be materialized and the part that cannot.
///
/// Cloning is a refcount bump per Arrow buffer (the tick clones the whole
/// source map under the lock before releasing it for the SQL phase, so this
/// has to stay cheap — see `IncrementalFlow::step_datafusion` Phase 1).
#[derive(Debug, Clone)]
pub struct SourceState {
    /// The relation, materialized. A row with net weight `k > 0` appears `k`
    /// times. This is exactly what the source map held before CORE-2, and it
    /// is what gets registered as a `MemTable` for view SQL.
    positive: RecordBatch,
    /// Rows whose net weight is strictly negative — retractions that had
    /// nothing to cancel. `None` when nothing is owed, which is the normal
    /// state of every append-only and every in-order-CDC source.
    deficit: Option<DeltaBatch>,
    /// How many times [`Self::apply`] has taken the O(state) branch since this
    /// state was created. Reset when the source is dropped and re-fed, because
    /// that creates a new `SourceState`.
    consolidations: u64,
}

impl SourceState {
    /// State holding `positive` as the materialized relation and owing nothing.
    pub fn from_positive(positive: RecordBatch) -> Self {
        Self {
            positive,
            deficit: None,
            consolidations: 0,
        }
    }

    /// Build state from an arbitrary Z-set, consolidating and splitting it.
    ///
    /// The all-`+1` case (what `checkpoint` wrote before CORE-2, and every
    /// append-only source) skips the consolidate entirely.
    pub fn from_zset(zset: &DeltaBatch) -> DeltaResult<Self> {
        if all_unit_inserts(zset) {
            return Ok(Self::from_positive(zset.data_batch()));
        }
        let consolidated = consolidate_batch(zset.clone(), &[], zset.data_schema())?;
        // Not counted in `consolidations`: that counter exists to distinguish
        // which branch of `apply` a *tick* took, and building a state from a
        // checkpoint is not a tick.
        let state = Self {
            positive: consolidated.filter_positive_expanded()?,
            deficit: negative_part(&consolidated)?,
            consolidations: 0,
        };
        state.assert_invariants();
        Ok(state)
    }

    /// Advance an optional prior state by `delta`.
    ///
    /// Mirrors the shape of [`apply_delta`](crate::apply_delta), whose
    /// `current` argument is `Option<RecordBatch>` for the same reason: a
    /// source has no entry in the flow's map until its first delta arrives.
    pub fn apply_to(current: Option<Self>, delta: &DeltaBatch) -> DeltaResult<Self> {
        match current {
            None => Self::from_zset(delta),
            Some(mut state) => {
                state.apply(delta)?;
                Ok(state)
            }
        }
    }

    /// Incorporate `delta` into this state.
    pub fn apply(&mut self, delta: &DeltaBatch) -> DeltaResult<()> {
        let unit_inserts = all_unit_inserts(delta);
        match (&self.deficit, unit_inserts) {
            // Branch (a): append-only against a source that owes nothing.
            // The O(Δ) concat `apply_delta` already had.
            (None, true) => {
                self.positive = append_positive(&self.positive, delta.data_batch())?;
            }
            // Branch (b): append-only against a source that owes rows. Only
            // the owed rows can cancel, so consolidate `deficit ++ delta` and
            // leave the (much larger) materialized relation untouched.
            (Some(_), true) => {
                // The scrutinee already matched `Some`, so this cannot be
                // `None`. An earlier version guarded it with an `Err` arm that
                // no input could reach — a dead error path inside the module
                // whose whole subject is state being faithful.
                let Some(deficit) = self.deficit.take() else {
                    unreachable!("branch (b) matched Some(_) on the same scrutinee")
                };
                let merged = DeltaBatch::concat(&[deficit, delta.clone()])?;
                let settled = consolidate_batch(merged, &[], delta.data_schema())?;
                self.deficit = negative_part(&settled)?;
                self.positive =
                    append_positive(&self.positive, settled.filter_positive_expanded()?)?;
            }
            // Branch (c): the delta carries a retraction or a non-unit weight,
            // so any row of the relation may be affected. This is exactly the
            // work `apply_delta` already did for such a delta; the only
            // difference is that the negative remainder is kept instead of
            // being clamped away.
            (_, false) => {
                self.consolidations = self.consolidations.saturating_add(1);
                let merged = self.zset_with(Some(delta))?;
                let settled = consolidate_batch(merged, &[], delta.data_schema())?;
                self.positive = settled.filter_positive_expanded()?;
                self.deficit = negative_part(&settled)?;
            }
        }
        self.assert_invariants();
        Ok(())
    }

    /// The materialized relation — what view SQL reads and what
    /// `IncrementalFlow::source_snapshot` returns.
    pub fn positive(&self) -> &RecordBatch {
        &self.positive
    }

    /// The strictly-negative part of the Z-set, if anything is owed.
    pub fn deficit(&self) -> Option<&DeltaBatch> {
        self.deficit.as_ref()
    }

    /// Replace the deficit wholesale. Used by `restore_full` to merge the
    /// deficit section of a checkpoint into a state decoded from the
    /// positives-only source section.
    ///
    /// Rows of `deficit` that are also present in `positive` are cancelled
    /// against it, so invariant 2 holds however the two halves were produced.
    pub fn set_deficit(&mut self, deficit: Option<DeltaBatch>) -> DeltaResult<()> {
        let Some(deficit) = deficit else {
            self.deficit = None;
            return Ok(());
        };
        if deficit.is_empty() {
            self.deficit = None;
            return Ok(());
        }
        self.deficit = Some(deficit);
        // Re-consolidate so a deficit row that also appears in `positive`
        // cancels instead of being double-counted.
        let merged = self.zset_with(None)?;
        let schema = merged.data_schema().clone();
        let settled = consolidate_batch(merged, &[], &schema)?;
        self.positive = settled.filter_positive_expanded()?;
        self.deficit = negative_part(&settled)?;
        self.assert_invariants();
        Ok(())
    }

    /// Number of distinct rows the source owes (physical rows in the deficit),
    /// not the total owed multiplicity (a row owed twice counts once here).
    pub fn deficit_rows(&self) -> usize {
        self.deficit.as_ref().map_or(0, |d| d.num_rows())
    }

    /// How many O(state) consolidations [`Self::apply`] has performed.
    ///
    /// This is the only way to tell the fast path from the slow one: all three
    /// branches compute the same Z-set, so a test asserting the answer cannot
    /// distinguish them (the "fallback mask" trap).
    pub fn consolidations(&self) -> u64 {
        self.consolidations
    }

    /// The whole Z-set as one `DeltaBatch`, positives and deficit together.
    ///
    /// O(state); used by `checkpoint` and by `restore_delta`, not on the tick
    /// path.
    pub fn zset(&self) -> DeltaResult<DeltaBatch> {
        self.zset_with(None)
    }

    /// `positive ++ deficit ++ extra`, unconsolidated.
    fn zset_with(&self, extra: Option<&DeltaBatch>) -> DeltaResult<DeltaBatch> {
        let mut parts: Vec<DeltaBatch> = Vec::with_capacity(3);
        // A zero-row `positive` contributes nothing but can carry a different
        // (e.g. placeholder) schema, which `DeltaBatch::concat` would reject.
        if self.positive.num_rows() > 0 {
            parts.push(DeltaBatch::from_inserts(self.positive.clone())?);
        }
        if let Some(d) = &self.deficit {
            parts.push(d.clone());
        }
        if let Some(extra) = extra {
            parts.push(extra.clone());
        }
        if parts.is_empty() {
            return DeltaBatch::from_inserts(self.positive.clone());
        }
        DeltaBatch::concat(&parts)
    }

    /// Invariants 1 and 2 from the module doc. Debug builds only, and only
    /// when something is owed: invariant 2 costs a key scan of the whole
    /// relation, so it must not run on the append-only path.
    fn assert_invariants(&self) {
        #[cfg(debug_assertions)]
        {
            let Some(deficit) = &self.deficit else { return };
            for w in deficit.weights().iter().flatten() {
                debug_assert!(
                    w < 0,
                    "SourceState invariant 1: deficit weight {w} is not negative"
                );
            }
            debug_assert!(
                deficit.num_rows() > 0,
                "SourceState invariant 1: an empty deficit must be None, not Some"
            );
            // Invariant 2 — no row is both present and owed. Compared through
            // the same key encoding `consolidate_batch` uses, so "the same
            // row" means the same thing here as it does there.
            use crate::operators::key_util::scalar_to_group_key;
            let key_of = |batch: &RecordBatch, row: usize| -> Option<Vec<String>> {
                (0..batch.num_columns())
                    .map(|c| scalar_to_group_key(batch.column(c), row).ok())
                    .collect()
            };
            let deficit_data = deficit.data_batch();
            if deficit_data.schema() != self.positive.schema() {
                return;
            }
            let owed: std::collections::HashSet<Vec<String>> = (0..deficit_data.num_rows())
                .filter_map(|r| key_of(&deficit_data, r))
                .collect();
            for r in 0..self.positive.num_rows() {
                if let Some(k) = key_of(&self.positive, r) {
                    debug_assert!(
                        !owed.contains(&k),
                        "SourceState invariant 2: row {r} is both present and owed"
                    );
                }
            }
        }
    }
}

/// True when every weight is exactly `+1` — the append-only shape that gets
/// the concat fast path. An empty delta qualifies vacuously, exactly as it did
/// in `apply_delta`.
fn all_unit_inserts(delta: &DeltaBatch) -> bool {
    delta.weights().iter().all(|w| w == Some(1))
}

/// Append `new_rows` to `prev`, or replace `prev` when it holds no rows.
///
/// The zero-row replacement mirrors `apply_delta`'s `prev.num_rows() == 0`
/// early return: an empty accumulated batch may carry a placeholder schema
/// that `concat_batches` would refuse against the incoming one.
fn append_positive(prev: &RecordBatch, new_rows: RecordBatch) -> DeltaResult<RecordBatch> {
    if prev.num_rows() == 0 {
        return Ok(new_rows);
    }
    if new_rows.num_rows() == 0 {
        return Ok(prev.clone());
    }
    arrow::compute::concat_batches(&prev.schema(), &[prev.clone(), new_rows])
        .map_err(|e| DeltaError::Operator(format!("source state append failed: {e}")))
}

/// The strictly-negative rows of a **consolidated** Z-set, or `None` if there
/// are none.
fn negative_part(consolidated: &DeltaBatch) -> DeltaResult<Option<DeltaBatch>> {
    let mask: BooleanArray = consolidated
        .weights()
        .iter()
        .map(|w| Some(w.unwrap_or(0) < 0))
        .collect();
    let negatives = consolidated.filter_mask(&mask)?;
    if negatives.num_rows() == 0 {
        Ok(None)
    } else {
        Ok(Some(negatives))
    }
}
