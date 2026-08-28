#![forbid(unsafe_code)]

//! `IncrementalView` and `IncrementalViewRegistry`.
//!
//! An `IncrementalView` holds the operator pipeline for one SQL incremental
//! view, its current pending output `DeltaBatch`, and its registered sinks.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use tokio::sync::{broadcast, watch};

/// Buffer depth of the per-view lossless delta stream.
///
/// Audit: the watch channel below retains only the *latest* value, so a
/// subscriber that is slower than the step engine silently skips every delta
/// but the last one — for a vector sink that means an upsert or a delete that
/// never reaches the index and never reports an error. The broadcast channel
/// carries every delta; if a subscriber falls this far behind it receives an
/// explicit `Lagged(n)` instead of a silent gap.
pub const VIEW_DELTA_STREAM_CAPACITY: usize = 1024;

use crate::delta_batch::DeltaBatch;
use crate::error::{DeltaError, DeltaResult};
use crate::lateness::LatenessSpec;
use crate::operators::stream::differentiate;
use crate::snapshot_index::ViewState;

/// Specification of one incremental view as registered from SQL DDL.
#[derive(Debug, Clone)]
pub struct IncrementalViewSpec {
    pub name: String,
    pub body_sql: String,
    pub output_schema: SchemaRef,
    pub is_materialized: bool,
    pub is_recursive: bool,
    pub lateness: Vec<LatenessSpec>,
}

/// Runtime state for one incremental view.
pub struct IncrementalView {
    pub spec: IncrementalViewSpec,
    /// Latest output DeltaBatch from the last `step()`. None if never stepped.
    last_output: Arc<Mutex<Option<DeltaBatch>>>,
    /// Watch channel carrying the *latest* output, for peek-style readers.
    sender: watch::Sender<Option<DeltaBatch>>,
    /// Lossless delta stream: every published delta, in order.
    ///
    /// Audit: subscribers that must see *every* delta (a vector index sink
    /// applying upserts and deletes, say) cannot use `sender` — a watch channel
    /// coalesces, so a subscriber slower than the step engine skips deltas with
    /// no error anywhere. See [`VIEW_DELTA_STREAM_CAPACITY`].
    delta_tx: broadcast::Sender<DeltaBatch>,
    /// Snapshot accumulation for materialized views.
    ///
    /// IVM-AUD-PERF-3: held as a row-indexed multiset, not a `RecordBatch`, so
    /// applying a delta costs O(|delta|) instead of re-consolidating the whole
    /// accumulated snapshot every tick. Materialization is deferred to
    /// `snapshot()` and cached.
    snapshot: Arc<Mutex<ViewState>>,
    /// Previous full materialized output used for diff-based IVM.
    /// `differentiate(full_output_prev, new_full)` produces the true delta.
    full_output: Arc<Mutex<ViewState>>,
    /// Retraction rows this view's integration had to clamp away
    /// (IVM-AUD-CORE-2b). See [`Self::clamped_retraction_rows`].
    clamped_retractions: Arc<AtomicU64>,
}

impl IncrementalView {
    pub fn new(spec: IncrementalViewSpec) -> (Self, watch::Receiver<Option<DeltaBatch>>) {
        let (sender, receiver) = watch::channel(None);
        let (delta_tx, _) = broadcast::channel(VIEW_DELTA_STREAM_CAPACITY);
        let view = Self {
            spec,
            last_output: Arc::new(Mutex::new(None)),
            sender,
            delta_tx,
            snapshot: Arc::new(Mutex::new(ViewState::default())),
            full_output: Arc::new(Mutex::new(ViewState::default())),
            clamped_retractions: Arc::new(AtomicU64::new(0)),
        };
        (view, receiver)
    }

    /// Retraction rows this view's integration clamped away, cumulative
    /// (IVM-AUD-CORE-2b).
    ///
    /// **This is a measurement, not a fix.** A view's materialized state is a
    /// `RecordBatch`, so a delta that retracts a row the view does not hold
    /// has nowhere to record the debt — `apply_delta` clamps it at zero copies
    /// and the remainder is dropped. CORE-2 fixed that for *source* state (see
    /// [`SourceState`](crate::SourceState)); the view level still clamps, and
    /// this counter is what makes the loss visible instead of silent, exactly
    /// as `delta_restore_collapsed_rows` did for `restore_delta`'s collapse.
    ///
    /// Non-zero means this view's snapshot and diff baseline are **not** the
    /// Z-set the deltas describe.
    ///
    /// It stays zero on the DiffBased path in normal operation, whose delta
    /// comes from `differentiate` against the view's own baseline and so
    /// cannot retract more than that baseline holds. It can go non-zero
    /// through a caller-supplied delta
    /// (`IncrementalFlow::step_with`) or a remote tick mirrored onto a drifted
    /// baseline (`IncrementalFlow::apply_remote_tick`).
    pub fn clamped_retraction_rows(&self) -> u64 {
        self.clamped_retractions.load(Ordering::Relaxed)
    }

    /// Count what a materialization of `delta` on top of `prev` had to clamp.
    ///
    /// The Z-set answer has `prev.num_rows() + Σweights` rows; the materialized
    /// answer has `Σ max(0, net)` rows. The difference is exactly the owed
    /// multiplicity that was clamped away, and computing it costs one pass over
    /// the delta's weights — no second consolidate.
    /// Report retraction rows that had nothing to cancel (IVM-AUD-CORE-2b).
    ///
    /// The count now comes from the state itself — the indexed path knows
    /// exactly how much of a retraction it could not honour — instead of being
    /// inferred from row-count arithmetic over the whole snapshot.
    fn note_clamped(&self, clamped: u64) {
        if clamped > 0 {
            tracing::warn!(
                view = %self.spec.name,
                clamped_rows = clamped,
                "view integration clamped a retraction with nothing to cancel; the view's \
                 state is not the Z-set the delta describes (IVM-AUD-CORE-2b)"
            );
            self.clamped_retractions
                .fetch_add(clamped, Ordering::Relaxed);
        }
    }

    /// Publish an output delta whose diff baseline has **already** been
    /// advanced — i.e. the DiffBased path, where [`Self::diff_and_update`] set
    /// `full_output` to the fresh full result immediately before this call.
    ///
    /// It advances the materialized `snapshot` and re-syncs `full_output` to it,
    /// which is a no-op against what `diff_and_update` just wrote.
    ///
    /// IVM-AUD-CORE-18: any path that produces a delta WITHOUT a preceding
    /// `diff_and_update` — an O(Δ) operator, a caller-supplied delta, a
    /// mirrored remote tick — must use [`Self::apply_output_delta`] instead.
    /// This one leaves `full_output` at `None` for a non-materialized view, and
    /// a `None` baseline makes the next full recompute re-emit the whole view.
    pub fn publish_output(&self, output: DeltaBatch) -> DeltaResult<()> {
        {
            let mut guard = self
                .last_output
                .lock()
                .map_err(|_| DeltaError::Operator("view output lock poisoned".into()))?;
            *guard = Some(output.clone());
        }
        // Update materialized snapshot for materialized views.
        // Apply the delta to the prior full snapshot — don't replace it with
        // just the delta's positive rows (that would lose prior state).
        if self.spec.is_materialized {
            // IVM-AUD-CORE-14 still holds: nothing is destroyed before the
            // fallible step succeeds. `ViewState::apply` mutates in place and
            // returns Err without having changed the state on the index path;
            // the Raw arm computes the replacement before assigning.
            let clamped = {
                let mut snap = self
                    .snapshot
                    .lock()
                    .map_err(|_| DeltaError::Operator("snapshot lock poisoned".into()))?;
                snap.apply(&self.spec.output_schema, &output)?
            };
            self.note_clamped(clamped);
            // Keep the diff baseline in lockstep with the materialized
            // snapshot. This is the DiffBased path — `diff_and_update` has
            // already advanced `full_output` to the fresh full result — so the
            // baseline is SET from the snapshot rather than re-applying the
            // delta, which would double-count it. Materializing here is O(state)
            // and deliberately so: this path just re-ran the whole view SQL,
            // which is O(state) already. The O(delta) path is
            // `apply_output_delta`.
            let updated = {
                let mut snap = self
                    .snapshot
                    .lock()
                    .map_err(|_| DeltaError::Operator("snapshot lock poisoned".into()))?;
                snap.batch()?
            };
            tracing::debug!(
                view = %self.spec.name,
                rows = updated.as_ref().map_or(0, RecordBatch::num_rows),
                "snapshot updated"
            );
            let mut fo = self
                .full_output
                .lock()
                .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))?;
            fo.set_raw(updated);
        } else {
            tracing::debug!(
                view = %self.spec.name,
                "publish_output: not materialized, snapshot skipped"
            );
        }
        self.emit(output);
        Ok(())
    }

    /// Publish `delta` to both output channels.
    ///
    /// The watch channel keeps the latest-value semantics `view_output_peek`
    /// depends on; the broadcast channel is the lossless stream. Both sends are
    /// best-effort in the same sense: no subscribers is not an error.
    fn emit(&self, delta: DeltaBatch) {
        let _ = self.delta_tx.send(delta.clone());
        let _ = self.sender.send(Some(delta));
    }

    /// Return the last output, or an empty batch.
    pub fn last_output(&self) -> DeltaResult<Option<DeltaBatch>> {
        self.last_output
            .lock()
            .map(|g| g.clone())
            .map_err(|_| DeltaError::Operator("view lock poisoned".into()))
    }

    /// Return the current materialized snapshot (only for materialized views).
    pub fn snapshot(&self) -> DeltaResult<Option<arrow::array::RecordBatch>> {
        let mut guard = self
            .snapshot
            .lock()
            .map_err(|_| DeltaError::Operator("snapshot lock poisoned".into()))?;
        guard.batch()
    }

    /// Return the previous full output used as the diff-based IVM baseline.
    ///
    /// Exposed so a coordinator-authoritative checkpoint can capture view
    /// baselines and ship them to a stateless executor.
    pub fn full_output_baseline(&self) -> DeltaResult<Option<arrow::array::RecordBatch>> {
        let mut guard = self
            .full_output
            .lock()
            .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))?;
        guard.batch()
    }

    /// Subscribe to the *latest* output delta (coalescing).
    ///
    /// Correct for peek-style readers that only want current state. A consumer
    /// that must act on every delta wants [`Self::subscribe_deltas`] instead —
    /// this channel silently drops intermediate values.
    pub fn subscribe(&self) -> watch::Receiver<Option<DeltaBatch>> {
        self.sender.subscribe()
    }

    /// Subscribe to the lossless stream of every published delta.
    ///
    /// Deltas published *before* this call are not replayed; from this point on
    /// the subscriber sees each one in order, or an explicit
    /// `RecvError::Lagged(n)` if it falls more than
    /// [`VIEW_DELTA_STREAM_CAPACITY`] behind.
    pub fn subscribe_deltas(&self) -> broadcast::Receiver<DeltaBatch> {
        self.delta_tx.subscribe()
    }

    /// Compute the delta between the previous full output and `new_full`, store
    /// `new_full` as the new baseline, and return the delta.
    ///
    /// Used by `step_datafusion`: the caller runs the view SQL to get a fresh
    /// full result, then calls this to obtain the true incremental delta.
    pub fn diff_and_update(&self, new_full: RecordBatch) -> DeltaResult<DeltaBatch> {
        let mut guard = self
            .full_output
            .lock()
            .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))?;
        let prev = guard.batch()?;
        let delta = differentiate(&self.spec.output_schema, prev.as_ref(), &new_full)?;
        guard.set(&self.spec.output_schema, Some(new_full));
        Ok(delta)
    }

    /// Clear the stored full output so the next `diff_and_update` call treats
    /// all rows as new insertions. Call this when `body_sql` changes
    /// (behavior_version invalidation).
    pub fn reset_full_output(&self) -> DeltaResult<()> {
        let mut guard = self
            .full_output
            .lock()
            .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))?;
        guard.set(&self.spec.output_schema, None);
        Ok(())
    }

    /// Clear the diff baseline **and** the materialized snapshot together.
    ///
    /// IVM-AUD-CORE-16: a checkpoint restore replaces the source snapshots
    /// wholesale, so the view's derived state no longer corresponds to its
    /// inputs and must be recomputed. Clearing only `full_output` (what
    /// `reset_full_output` does) left `snapshot = Some(old)`: the next
    /// DiffBased tick diffed against `None`, emitted the ENTIRE result as
    /// insertions, and `publish_output` applied those insertions on top of the
    /// stale snapshot — every row doubled. Both halves must move together.
    pub fn reset_state(&self) -> DeltaResult<()> {
        {
            let mut guard = self
                .full_output
                .lock()
                .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))?;
            guard.set(&self.spec.output_schema, None);
        }
        let mut snap = self
            .snapshot
            .lock()
            .map_err(|_| DeltaError::Operator("snapshot lock poisoned".into()))?;
        snap.set(&self.spec.output_schema, None);
        Ok(())
    }

    /// Replace the view's full materialized state with `new_full`.
    ///
    /// Used by coordinator-authoritative IVM to apply a tick computed on a
    /// remote executor: the executor returns the full output, and the
    /// coordinator swaps it in wholesale. This recomputes the output delta
    /// from the prior `full_output` baseline, then updates both `full_output`
    /// and (for materialized views) the `snapshot`, and emits the delta to
    /// subscribers. Replacing — rather than applying a delta — keeps the
    /// baseline and the snapshot in lockstep, so a later central
    /// `diff_and_update` cannot drift.
    pub fn replace_full(&self, new_full: RecordBatch) -> DeltaResult<DeltaBatch> {
        // Diff against the prior baseline and advance it under one lock.
        let delta = {
            let mut guard = self
                .full_output
                .lock()
                .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))?;
            let prev = guard.batch()?;
            let delta = differentiate(&self.spec.output_schema, prev.as_ref(), &new_full)?;
            guard.set(&self.spec.output_schema, Some(new_full.clone()));
            delta
        };
        if self.spec.is_materialized {
            let mut snap = self
                .snapshot
                .lock()
                .map_err(|_| DeltaError::Operator("snapshot lock poisoned".into()))?;
            snap.set(&self.spec.output_schema, Some(new_full));
        }
        {
            let mut lo = self
                .last_output
                .lock()
                .map_err(|_| DeltaError::Operator("view output lock poisoned".into()))?;
            *lo = Some(delta.clone());
        }
        self.emit(delta.clone());
        Ok(delta)
    }

    /// Apply an **output delta** to the view's materialized state (AUD-6).
    ///
    /// The publish for every path that computes a delta directly rather than by
    /// diffing a full result: an O(Δ) operator, a caller-supplied delta in
    /// `step_with`, and a tick mirrored from a resident executor. Both the diff
    /// baseline (`full_output`) and, for materialized views, the `snapshot`
    /// advance by the delta — unconditionally, because the baseline is what a
    /// later full recompute diffs against and is needed whether or not the view
    /// is materialized (IVM-AUD-CORE-18). The delta is also published to
    /// subscribers and stored as `last_output`.
    ///
    /// The counterpart is [`Self::publish_output`], for the DiffBased path
    /// where `diff_and_update` has already advanced the baseline.
    pub fn apply_output_delta(&self, delta: &DeltaBatch) -> DeltaResult<()> {
        // IVM-AUD-CORE-15: same take-then-`?` hazard as publish_output, twice.
        // A failed mirror of a resident-executor tick used to leave both the
        // diff baseline and the snapshot permanently `None` — after the tick
        // counter had already advanced. Compute both updates from clones
        // first, and only commit them once BOTH have succeeded, so a partial
        // failure cannot leave the two halves inconsistent either.
        // IVM-AUD-PERF-3: this is the O(delta) path — the one every
        // incremental operator publishes through — so neither state is
        // re-materialized here. `ViewState::apply` updates a row-indexed
        // multiset in place at O(|delta|).
        //
        // IVM-AUD-CORE-15's guarantee still holds: `apply`'s fallible steps
        // (consolidating the delta, encoding its rows) run BEFORE any mutation
        // and are pure functions of the delta, which is the same for both
        // states — so they cannot succeed for one and fail for the other and
        // leave the two halves inconsistent. No new failure mode is introduced
        // here: a delta whose schema differs from the accumulated state falls
        // back to the old whole-snapshot path rather than erroring.
        {
            let mut fo = self
                .full_output
                .lock()
                .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))?;
            // Counted on the baseline only, not again on the snapshot: for a
            // materialized view the two integrate the same delta, so counting
            // both would double every loss.
            let clamped = fo.apply(&self.spec.output_schema, delta)?;
            self.note_clamped(clamped);
        }
        if self.spec.is_materialized {
            let mut snap = self
                .snapshot
                .lock()
                .map_err(|_| DeltaError::Operator("snapshot lock poisoned".into()))?;
            snap.apply(&self.spec.output_schema, delta)?;
        }
        {
            let mut lo = self
                .last_output
                .lock()
                .map_err(|_| DeltaError::Operator("view output lock poisoned".into()))?;
            *lo = Some(delta.clone());
        }
        self.emit(delta.clone());
        Ok(())
    }

    /// Restore previously checkpointed view state (`snapshot` + `full_output`).
    ///
    /// Used to seed a transient flow on a remote executor so its single tick
    /// computes correct output deltas. Both fields are optional; `None` resets
    /// that field to its never-stepped state.
    pub fn restore_state(
        &self,
        snapshot: Option<RecordBatch>,
        full_output: Option<RecordBatch>,
    ) -> DeltaResult<()> {
        {
            let mut snap = self
                .snapshot
                .lock()
                .map_err(|_| DeltaError::Operator("snapshot lock poisoned".into()))?;
            snap.set(&self.spec.output_schema, snapshot);
        }
        let mut fo = self
            .full_output
            .lock()
            .map_err(|_| DeltaError::Operator("full_output lock poisoned".into()))?;
        fo.set(&self.spec.output_schema, full_output);
        Ok(())
    }
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Registry of all incremental views for a session/flow.
pub struct IncrementalViewRegistry {
    views: Mutex<HashMap<String, Arc<IncrementalView>>>,
    receivers: Mutex<HashMap<String, watch::Receiver<Option<DeltaBatch>>>>,
}

impl IncrementalViewRegistry {
    pub fn new() -> Self {
        Self {
            views: Mutex::new(HashMap::new()),
            receivers: Mutex::new(HashMap::new()),
        }
    }

    pub fn register(&self, spec: IncrementalViewSpec) -> DeltaResult<()> {
        let name = spec.name.clone();
        let (view, receiver) = IncrementalView::new(spec);
        {
            let mut views = self
                .views
                .lock()
                .map_err(|_| DeltaError::Operator("registry lock poisoned".into()))?;
            views.insert(name.clone(), Arc::new(view));
        }
        {
            let mut receivers = self
                .receivers
                .lock()
                .map_err(|_| DeltaError::Operator("registry lock poisoned".into()))?;
            receivers.insert(name, receiver);
        }
        Ok(())
    }

    pub fn get(&self, name: &str) -> DeltaResult<Arc<IncrementalView>> {
        let views = self
            .views
            .lock()
            .map_err(|_| DeltaError::Operator("registry lock poisoned".into()))?;
        views
            .get(name)
            .cloned()
            .ok_or_else(|| DeltaError::ViewNotFound(name.to_string()))
    }

    pub fn view_names(&self) -> DeltaResult<Vec<String>> {
        let views = self
            .views
            .lock()
            .map_err(|_| DeltaError::Operator("registry lock poisoned".into()))?;
        Ok(views.keys().cloned().collect())
    }

    pub fn contains(&self, name: &str) -> bool {
        self.views
            .lock()
            .map(|v| v.contains_key(name))
            .unwrap_or(false)
    }

    pub fn drop_view(&self, name: &str) -> DeltaResult<bool> {
        let removed = {
            let mut views = self
                .views
                .lock()
                .map_err(|_| DeltaError::Operator("registry lock poisoned".into()))?;
            views.remove(name).is_some()
        };
        // Crate-13 audit: the paired receiver entry leaked for every dropped
        // view (one watch receiver retained forever per drop).
        if let Ok(mut receivers) = self.receivers.lock() {
            receivers.remove(name);
        }
        Ok(removed)
    }
}

impl Default for IncrementalViewRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_spec(name: &str) -> IncrementalViewSpec {
        IncrementalViewSpec {
            name: name.to_string(),
            body_sql: format!("SELECT 1 AS x -- {name}"),
            output_schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        }
    }

    fn materialized_spec(name: &str) -> IncrementalViewSpec {
        IncrementalViewSpec {
            is_materialized: true,
            ..test_spec(name)
        }
    }

    /// A batch whose column type differs from `int_batch`, so applying it as
    /// a delta onto an Int64 snapshot is a genuine Arrow concat failure.
    /// (A same-typed, differently-named column is NOT a failure: Arrow's
    /// `concat_batches` concatenates positionally.)
    fn str_batch(col: &str, vals: &[&str]) -> arrow::record_batch::RecordBatch {
        use arrow::array::StringArray;
        let schema = Arc::new(Schema::new(vec![Field::new(col, DataType::Utf8, false)]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vals.to_vec()))],
        )
        .unwrap()
    }

    fn int_batch(col: &str, vals: &[i64]) -> arrow::record_batch::RecordBatch {
        use arrow::array::Int64Array;
        let schema = Arc::new(Schema::new(vec![Field::new(col, DataType::Int64, false)]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vals.to_vec()))],
        )
        .unwrap()
    }

    /// IVM-AUD-CORE-14. Revert-proof: change the `snap.clone()` back to
    /// `snap.take()` and the second assertion fails — the snapshot is `None`
    /// after the failed publish, so the view has silently lost all history
    /// while the log claims it was merely "not updated".
    #[test]
    fn failed_publish_leaves_the_materialized_snapshot_intact() {
        let (view, _rx) = IncrementalView::new(materialized_spec("v_publish"));
        view.publish_output(DeltaBatch::from_inserts(int_batch("x", &[1, 2])).unwrap())
            .unwrap();
        let before = view
            .snapshot()
            .unwrap()
            .expect("snapshot after first publish");
        assert_eq!(before.num_rows(), 2);

        // A delta whose column TYPE cannot be concatenated onto the snapshot.
        let mismatched = DeltaBatch::from_inserts(str_batch("x", &["nine"])).unwrap();
        assert!(
            view.publish_output(mismatched).is_err(),
            "a schema-mismatched delta must fail the publish"
        );

        let after = view
            .snapshot()
            .unwrap()
            .expect("snapshot must survive a failed publish");
        assert_eq!(
            after.num_rows(),
            2,
            "the failed publish must not destroy prior state"
        );
    }

    /// IVM-AUD-CORE-15. Revert-proof: restore the `fo.take()` / `snap.take()`
    /// form and both baselines are `None` after the failed mirror.
    #[test]
    fn failed_output_delta_mirror_leaves_both_baselines_intact() {
        let (view, _rx) = IncrementalView::new(materialized_spec("v_mirror"));
        view.apply_output_delta(&DeltaBatch::from_inserts(int_batch("x", &[1, 2, 3])).unwrap())
            .unwrap();
        assert_eq!(view.snapshot().unwrap().unwrap().num_rows(), 3);

        let mismatched = DeltaBatch::from_inserts(str_batch("x", &["nine"])).unwrap();
        assert!(view.apply_output_delta(&mismatched).is_err());

        assert_eq!(
            view.snapshot()
                .unwrap()
                .expect("snapshot must survive a failed mirror")
                .num_rows(),
            3
        );
        assert!(
            view.full_output_baseline()
                .unwrap()
                .is_some_and(|b| b.num_rows() == 3),
            "the diff baseline must survive a failed mirror too"
        );
    }

    #[test]
    fn registry_register_and_get() {
        let reg = IncrementalViewRegistry::new();
        reg.register(test_spec("v1")).unwrap();
        let v = reg.get("v1").unwrap();
        assert_eq!(v.spec.name, "v1");
    }

    #[test]
    fn registry_get_missing_returns_error() {
        let reg = IncrementalViewRegistry::new();
        assert!(matches!(
            reg.get("missing"),
            Err(DeltaError::ViewNotFound(_))
        ));
    }

    #[test]
    fn registry_drop_view() {
        let reg = IncrementalViewRegistry::new();
        reg.register(test_spec("v1")).unwrap();
        assert!(reg.drop_view("v1").unwrap());
        assert!(!reg.contains("v1"));
    }

    /// Regression (crate-13 audit, F-class): dropping a view previously left
    /// its watch receiver in the registry forever.
    #[test]
    fn drop_view_releases_receiver_entry() {
        let reg = IncrementalViewRegistry::new();
        reg.register(test_spec("v1")).unwrap();
        assert_eq!(reg.receivers.lock().unwrap().len(), 1);
        reg.drop_view("v1").unwrap();
        assert_eq!(
            reg.receivers.lock().unwrap().len(),
            0,
            "dropped view's receiver must be released"
        );
    }
}
