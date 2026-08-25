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
//! | all `+1` | empty | push a chunk — O(Δ) amortized | no |
//! | all `+1` | non-empty | consolidate `deficit ++ delta` — O(deficit + Δ) | no |
//! | any `≤ 0` or `> 1` | either | consolidate the whole Z-set — O(state) | yes |
//!
//! # Why `positive` is chunked (IVM-AUD-PERF-1)
//!
//! The first row used to read `concat_batches(positive, delta)` and claim
//! O(Δ). It was not: [`arrow::compute::concat_batches`] builds a new
//! contiguous batch, so it copies **the whole accumulated relation** on every
//! append — O(n + Δ) per tick, with `n` the rows accumulated so far. The
//! comment named the delta; the cost was the relation. Measured on the
//! `ivm_vs_full_recompute` bench, the tick was flat to ~1M rows (where the
//! copy hid under fixed overhead) and then grew linearly: 7.6x the time for
//! 7.5x the rows, for an identical 5,000-row delta. An incrementally
//! maintained view whose tick scales with total accumulated rows is not
//! incrementally maintained.
//!
//! So `positive` is a **list of chunks** whose concatenation is the relation
//! the single batch used to hold. Appending pushes; nothing already accumulated
//! is touched. `MemTable` takes a batch list natively, so the registration path
//! is unchanged.
//!
//! Pushing alone would trade one growth problem for another: a source fed one
//! row at a time would reach one chunk per row, and a scan over millions of
//! single-row batches is its own O(n) penalty. So trailing chunks are sealed
//! into one compacted run once they reach [`COMPACT_TARGET_ROWS`]. Each row is
//! copied at most once, when its run is sealed — O(1) amortized per row,
//! against the old O(n) per tick — and chunks stay at a size DataFusion scans
//! efficiently. [`SourceState::rows_copied`] counts exactly this, so a test can
//! assert the amortized bound instead of an answer both versions produce.
//!
//! The third branch runs the same consolidate `apply_delta` already ran
//! for a delta carrying a retraction, plus one boolean mask over the
//! consolidated result to split the negatives out — so it is that path's cost
//! plus O(consolidated rows), not a new order of growth.

use arrow::array::{BooleanArray, RecordBatch};
use arrow::datatypes::SchemaRef;

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
    /// Schema of the materialized relation. Held separately because `positive`
    /// may legitimately be empty, and an empty source is still a declared
    /// source with a schema — a view reading it must plan and return no rows
    /// rather than fail with "table not found" (IVM-AUD-CORE-2c).
    schema: SchemaRef,
    /// The relation, materialized, as chunks whose concatenation is the batch
    /// this field used to hold. A row with net weight `k > 0` appears `k`
    /// times across them. This is what gets registered as a `MemTable` for
    /// view SQL, which takes the chunk list directly.
    ///
    /// Every chunk holds at least one row: a zero-row batch would carry schema
    /// and no data, and the schema already lives in `schema`.
    positive: Vec<RecordBatch>,
    /// Total rows across `positive`, maintained incrementally so callers need
    /// not sum the chunk list.
    positive_rows: usize,
    /// `positive[..sealed]` are compacted runs; `positive[sealed..]` is the
    /// open tail still accumulating toward [`COMPACT_TARGET_ROWS`].
    sealed: usize,
    /// Rows in the open tail — `positive[sealed..]`.
    unsealed_rows: usize,
    /// Rows copied by sealing since this state was created. See
    /// [`Self::rows_copied`].
    rows_copied: u64,
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
        let schema = positive.schema();
        let rows = positive.num_rows();
        // A zero-row batch contributes no chunk; it was only ever carrying the
        // schema, and the schema now has its own field.
        let chunks = if rows == 0 {
            Vec::new()
        } else {
            vec![positive]
        };
        // The incoming batch is already one contiguous run, so it starts
        // SEALED. Leaving it in the open tail would mean the very next append
        // seals `[seed, delta]` together and copies the whole seeded relation
        // — reintroducing exactly the O(n)-per-tick defect this type exists to
        // remove, on the most common shape there is: a source restored from a
        // checkpoint, or seeded by one bulk load, and then fed deltas.
        let sealed = chunks.len();
        Self {
            schema,
            positive: chunks,
            positive_rows: rows,
            sealed,
            unsealed_rows: 0,
            rows_copied: 0,
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
        let mut state = Self::from_positive(consolidated.filter_positive_expanded()?);
        state.deficit = negative_part(&consolidated)?;
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
            // Pushes a chunk; nothing already accumulated is copied.
            (None, true) => {
                self.push_positive(delta.data_batch())?;
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
                self.push_positive(settled.filter_positive_expanded()?)?;
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
                self.replace_positive(settled.filter_positive_expanded()?);
                self.deficit = negative_part(&settled)?;
            }
        }
        self.assert_invariants();
        Ok(())
    }

    /// The materialized relation as chunks — what view SQL reads. Registering
    /// these is the tick-path use; `MemTable` takes the list as one partition,
    /// so no concatenation happens here or downstream.
    ///
    /// May be empty (a declared source with no rows). Use [`Self::schema`] for
    /// the schema, which exists whether or not there are chunks.
    pub fn positive_chunks(&self) -> &[RecordBatch] {
        &self.positive
    }

    /// Schema of the materialized relation, valid even with no chunks.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Rows in the materialized relation.
    pub fn num_rows(&self) -> usize {
        self.positive_rows
    }

    /// The materialized relation as one contiguous batch.
    ///
    /// **O(state) — never call this on the tick path.** It is the thing
    /// IVM-AUD-PERF-1 removed from it. For checkpointing, for the public
    /// `source_snapshot` accessor, and for seeding a newly built operator, one
    /// batch is genuinely what the caller needs and the cost is paid once, not
    /// per tick.
    pub fn positive_batch(&self) -> DeltaResult<RecordBatch> {
        match self.positive.as_slice() {
            [] => Ok(RecordBatch::new_empty(self.schema.clone())),
            [only] => Ok(only.clone()),
            chunks => arrow::compute::concat_batches(&self.schema, chunks)
                .map_err(|e| DeltaError::Operator(format!("source state concat failed: {e}"))),
        }
    }

    /// How many chunks the relation is held in.
    ///
    /// Exposed for the same reason as [`Self::consolidations`]: every
    /// representation computes the same answer, so a test that asserts on rows
    /// cannot tell a chunked append from a copy-the-world one.
    pub fn positive_chunk_count(&self) -> usize {
        self.positive.len()
    }

    /// Rows copied by sealing compacted runs since this state was created.
    ///
    /// This is the direct observable for IVM-AUD-PERF-1. Appending `n` rows
    /// leaves this at most `n` (each row is copied at most once, when its run
    /// is sealed); the `concat_batches`-per-append version it replaced grows
    /// this quadratically in the number of appends. Asserting the amortized
    /// bound here is what distinguishes fixed from broken — the relation
    /// itself is identical either way.
    pub fn rows_copied(&self) -> u64 {
        self.rows_copied
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
        self.replace_positive(settled.filter_positive_expanded()?);
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
        let mut parts: Vec<DeltaBatch> = Vec::with_capacity(self.positive.len() + 2);
        // Each chunk goes in as its own part: `DeltaBatch::concat` already
        // concatenates, so pre-concatenating the chunks would copy them twice.
        // An empty `positive` contributes nothing — its schema, which
        // `DeltaBatch::concat` would reject against the others, lives in
        // `self.schema` and is only needed for the empty result below.
        for chunk in &self.positive {
            parts.push(DeltaBatch::from_inserts(chunk.clone())?);
        }
        if let Some(d) = &self.deficit {
            parts.push(d.clone());
        }
        if let Some(extra) = extra {
            parts.push(extra.clone());
        }
        if parts.is_empty() {
            return DeltaBatch::from_inserts(RecordBatch::new_empty(self.schema.clone()));
        }
        DeltaBatch::concat(&parts)
    }

    /// Append `new_rows` as a chunk, sealing the open tail if it has grown to
    /// [`COMPACT_TARGET_ROWS`].
    ///
    /// This is the line IVM-AUD-PERF-1 is about. It must not touch chunks
    /// already accumulated: doing so is what made the tick O(n).
    fn push_positive(&mut self, new_rows: RecordBatch) -> DeltaResult<()> {
        let rows = new_rows.num_rows();
        if rows == 0 {
            // Nothing to hold. Not even a schema: a zero-row batch here would
            // violate the "every chunk holds a row" invariant, and an empty
            // relation's schema is whatever `self.schema` already says.
            return Ok(());
        }
        if self.positive.is_empty() {
            // Adopt the incoming schema, mirroring what the old
            // `append_positive` did by returning `new_rows` wholesale when the
            // accumulated batch had no rows: an empty state may be carrying a
            // placeholder schema that the real data does not match.
            self.schema = new_rows.schema();
        }
        self.positive.push(new_rows);
        self.positive_rows += rows;
        self.unsealed_rows += rows;
        if self.unsealed_rows >= COMPACT_TARGET_ROWS {
            self.seal_tail()?;
        }
        Ok(())
    }

    /// Fold the open tail `positive[sealed..]` into one compacted run.
    ///
    /// Copies exactly the tail — never the sealed runs before it — which is
    /// what bounds the amortized cost to one copy per row.
    fn seal_tail(&mut self) -> DeltaResult<()> {
        // `get` rather than a slice index: `clippy::indexing_slicing` is denied
        // workspace-wide, and `sealed` is an index this function itself moves.
        let Some(tail) = self.positive.get(self.sealed..) else {
            return Ok(());
        };
        if tail.len() > 1 {
            let merged = arrow::compute::concat_batches(&self.schema, tail).map_err(|e| {
                DeltaError::Operator(format!("source state chunk seal failed: {e}"))
            })?;
            self.rows_copied = self
                .rows_copied
                .saturating_add(self.unsealed_rows.try_into().unwrap_or(u64::MAX));
            self.positive.truncate(self.sealed);
            self.positive.push(merged);
        }
        // A single-chunk tail is already one contiguous run; sealing it would
        // copy every row to produce a batch equal to the one it started from.
        self.sealed = self.positive.len();
        self.unsealed_rows = 0;
        Ok(())
    }

    /// Replace the whole relation with `batch` — the consolidating branches
    /// recompute it from scratch, so there is nothing to append to.
    ///
    /// Resets the chunking, which is correct: the result is one contiguous
    /// batch, and the copy that produced it was `consolidate_batch`'s, already
    /// counted as O(state) by `consolidations`.
    fn replace_positive(&mut self, batch: RecordBatch) {
        let rows_copied = self.rows_copied;
        let consolidations = self.consolidations;
        let deficit = self.deficit.take();
        *self = Self::from_positive(batch);
        self.rows_copied = rows_copied;
        self.consolidations = consolidations;
        self.deficit = deficit;
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
            if deficit_data.schema() != self.schema {
                return;
            }
            let owed: std::collections::HashSet<Vec<String>> = (0..deficit_data.num_rows())
                .filter_map(|r| key_of(&deficit_data, r))
                .collect();
            for chunk in &self.positive {
                for r in 0..chunk.num_rows() {
                    if let Some(k) = key_of(chunk, r) {
                        debug_assert!(
                            !owed.contains(&k),
                            "SourceState invariant 2: row {r} is both present and owed"
                        );
                    }
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

/// Rows the open tail must reach before it is sealed into one compacted run.
///
/// DataFusion's default batch size, so a sealed run is exactly the chunk size
/// its scan is tuned for. The value trades two things that are both bounded:
/// sealing copies at most this many rows at once (so it caps the latency spike
/// a single tick can absorb), and the relation holds about `rows / this` many
/// chunks (so it caps per-batch scan overhead). It does not affect the
/// amortized cost, which is one copy per row at any target.
const COMPACT_TARGET_ROWS: usize = 8192;

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn rows(start: i64, n: i64) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int64Array::from_iter_values(start..start + n))],
        )
        .expect("build test batch")
    }

    fn inserts(start: i64, n: i64) -> DeltaBatch {
        DeltaBatch::from_inserts(rows(start, n)).expect("build test delta")
    }

    /// IVM-AUD-PERF-1. The defect was a cost, not an answer: `concat_batches`
    /// per append rebuilt the whole relation every tick, and every version of
    /// this code returns the identical relation either way. So this asserts
    /// the amortized copy bound — the only thing that separates fixed from
    /// broken (the "fallback mask" trap).
    ///
    /// Appending `n` rows may copy at most `n`: a row is copied once, when the
    /// run holding it is sealed, and sealed runs are never re-copied. Rows the
    /// state was *seeded* with are never copied at all. The
    /// `concat_batches`-per-append version copies the accumulated relation on
    /// every append, so after `k` appends of `m` rows it has copied
    /// `m * k * (k - 1) / 2` — quadratic in the number of appends.
    #[test]
    fn append_only_copies_each_row_at_most_once() {
        const APPENDS: i64 = 200;
        const PER_APPEND: i64 = 100;

        let mut state = SourceState::from_positive(RecordBatch::new_empty(test_schema()));
        for i in 0..APPENDS {
            state
                .apply(&inserts(i * PER_APPEND, PER_APPEND))
                .expect("apply");
        }

        let total = u64::try_from(state.num_rows()).expect("row count fits");
        assert_eq!(total, u64::try_from(APPENDS * PER_APPEND).expect("fits"));
        assert!(
            state.rows_copied() <= total,
            "appending {total} rows copied {} — a chunked append copies each row \
             at most once, so anything above {total} means the relation is being \
             rebuilt on append",
            state.rows_copied(),
        );
        // The relation is genuinely held in pieces. Pinned separately from the
        // copy bound because a single contiguous batch would satisfy the bound
        // vacuously if it were never appended to at all.
        assert!(
            state.positive_chunk_count() > 1,
            "expected the relation to be chunked, found one contiguous batch",
        );
        // Never taken the O(state) consolidate branch — this is the fast path.
        assert_eq!(state.consolidations(), 0);
    }

    /// Guards the obvious wrong fix rather than the original defect: pushing a
    /// chunk per append with no compaction also satisfies the copy bound above
    /// (it copies nothing), while leaving a source fed one row at a time
    /// holding one batch per row — an O(n) scan penalty traded for the O(n)
    /// copy that was removed. Sealing bounds chunks at
    /// `rows / COMPACT_TARGET_ROWS` sealed runs plus an open tail that cannot
    /// exceed `COMPACT_TARGET_ROWS` batches before it seals.
    #[test]
    fn single_row_appends_do_not_grow_one_chunk_per_row() {
        const APPENDS: i64 = 50_000;

        let mut state = SourceState::from_positive(RecordBatch::new_empty(test_schema()));
        for i in 0..APPENDS {
            state.apply(&inserts(i, 1)).expect("apply");
        }

        let bound = state.num_rows() / COMPACT_TARGET_ROWS + COMPACT_TARGET_ROWS;
        assert!(
            state.positive_chunk_count() <= bound,
            "{APPENDS} single-row appends left {} chunks, above the {bound}-chunk \
             bound — the open tail is not being sealed",
            state.positive_chunk_count(),
        );
        // The amortized bound must hold under this pattern too.
        assert!(state.rows_copied() <= u64::try_from(state.num_rows()).expect("fits"));
    }

    /// A state seeded with a bulk batch must not copy that batch when deltas
    /// arrive — the shape every restored-from-checkpoint and bulk-loaded source
    /// has, and the one an empty-start test cannot reach.
    ///
    /// This caught a real defect in the first cut of the fix: `from_positive`
    /// left the seed batch in the *open tail*, so the first append sealed
    /// `[seed, delta]` together and copied the entire seeded relation. The
    /// benchmark's `seeded_flow` takes exactly this path, so the fix would have
    /// measured as no fix at all.
    #[test]
    fn a_seeded_state_never_copies_its_seed() {
        const SEED: i64 = 100_000;
        const APPENDS: i64 = 100;
        const PER_APPEND: i64 = 100;

        let mut state = SourceState::from_positive(rows(0, SEED));
        assert_eq!(state.rows_copied(), 0, "construction must copy nothing");
        for i in 0..APPENDS {
            state
                .apply(&inserts(SEED + i * PER_APPEND, PER_APPEND))
                .expect("apply");
        }

        let appended = u64::try_from(APPENDS * PER_APPEND).expect("fits");
        assert!(
            state.rows_copied() <= appended,
            "seeded with {SEED} rows and appended {appended}, but copied {} — the \
             seed is being re-copied, so the tick is O(accumulated rows) again",
            state.rows_copied(),
        );
    }

    /// The chunks concatenate to exactly the relation the single batch held,
    /// in order — the property every other caller of `positive_batch` relies
    /// on (checkpointing, `source_snapshot`, operator seeding).
    #[test]
    fn chunked_positive_concatenates_to_the_relation() {
        let mut state = SourceState::from_positive(RecordBatch::new_empty(test_schema()));
        for i in 0..2_000 {
            state.apply(&inserts(i * 10, 10)).expect("apply");
        }

        let flat = state.positive_batch().expect("concatenate");
        assert_eq!(flat.num_rows(), state.num_rows());
        assert_eq!(flat.num_rows(), 20_000);
        let ids = flat
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        assert_eq!(ids.values(), &(0..20_000).collect::<Vec<i64>>()[..]);
    }

    /// A declared source with no rows keeps its schema, so a view reading it
    /// plans and returns nothing instead of failing "table not found"
    /// (IVM-AUD-CORE-2c). The schema used to ride on a zero-row batch; it now
    /// has its own field, and this pins that the move preserved the behaviour.
    #[test]
    fn empty_state_keeps_its_schema() {
        let state = SourceState::from_positive(RecordBatch::new_empty(test_schema()));
        assert_eq!(state.num_rows(), 0);
        assert_eq!(state.positive_chunk_count(), 0);
        assert_eq!(state.schema(), &test_schema());
        assert_eq!(
            state.positive_batch().expect("empty batch").schema(),
            test_schema()
        );
    }
}
