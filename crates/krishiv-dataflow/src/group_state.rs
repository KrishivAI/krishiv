//! K: `mapGroupsWithState` — arbitrary keyed stateful processing.
//!
//! [`GroupStateFn<S>`] is Krishiv's equivalent to Spark's `mapGroupsWithState`
//! and `flatMapGroupsWithState`.  Unlike [`ProcessFunction`] (which fires once
//! **per row**), a `GroupStateFn` is invoked **once per group per micro-batch**,
//! receiving all rows for that key at once — matching Spark's semantics exactly.
//!
//! # Timeout support
//! Call [`GroupState::set_timeout_ms`] during any invocation.  When the
//! event-time watermark subsequently exceeds that deadline, [`GroupStateExecutor::fire_timeouts`]
//! calls `on_group` once more with an **empty** row slice, allowing the user
//! function to emit final output and remove the state.
//!
//! # Example (conceptual)
//! ```ignore
//! struct SessionAggFn;
//! impl GroupStateFn<Vec<i64>> for SessionAggFn {
//!     fn on_group(&mut self, key: &str, rows: &[(&RecordBatch, usize)], state: &mut GroupState<Vec<i64>>) -> ExecResult<Vec<RecordBatch>> {
//!         let vals = state.value.get_or_insert_with(Vec::new);
//!         for (batch, row) in rows {
//!             vals.push(extract_value(batch, *row));
//!         }
//!         state.set_timeout_ms(current_watermark + 30_000);
//!         Ok(vec![])  // Emit on timeout.
//!     }
//! }
//! ```
//!
//! [`ProcessFunction`]: crate::process_fn::ProcessFunction

use std::collections::{BTreeMap, HashMap, HashSet};

use arrow::record_batch::RecordBatch;
use indexmap::IndexMap;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::ExecError;
use crate::ExecResult;
use crate::join::extract_agg_key;

const DEFAULT_GROUP_STATE_MAX_KEYS: usize = 100_000;

// ── GroupState ────────────────────────────────────────────────────────────────

/// Per-group mutable state handle passed to [`GroupStateFn::on_group`].
///
/// `S` is the user-defined state type.
pub struct GroupState<S> {
    /// The current state value; `None` on the first call for a new key.
    pub value: Option<S>,
    /// Optional event-time timeout (milliseconds).  When the watermark
    /// exceeds this value, [`GroupStateExecutor::fire_timeouts`] calls
    /// `on_group` with an empty row slice.
    expires_at_ms: Option<i64>,
    /// Set to `true` to remove state and cancel any timeout after the
    /// current invocation.
    pub remove: bool,
}

impl<S> GroupState<S> {
    fn new(value: Option<S>) -> Self {
        Self {
            value,
            expires_at_ms: None,
            remove: false,
        }
    }

    /// `true` if the group has a live state value.
    pub fn exists(&self) -> bool {
        self.value.is_some()
    }

    /// Replace the state value, marking it as live.
    pub fn update(&mut self, v: S) {
        self.value = Some(v);
        self.remove = false;
    }

    /// Schedule removal of this group's state after the current invocation.
    pub fn remove_state(&mut self) {
        self.remove = true;
        self.value = None;
    }

    /// Set an event-time timeout.  Replaces any previously set timeout.
    pub fn set_timeout_ms(&mut self, deadline_ms: i64) {
        self.expires_at_ms = Some(deadline_ms);
    }

    /// Cancel the current timeout.
    pub fn clear_timeout(&mut self) {
        self.expires_at_ms = None;
    }

    /// Read the current timeout deadline.
    pub fn timeout_ms(&self) -> Option<i64> {
        self.expires_at_ms
    }
}

// ── GroupStateFn trait ────────────────────────────────────────────────────────

/// User-defined function called **once per group per micro-batch**.
///
/// Implement this trait to process all rows for a single key together —
/// equivalent to Spark's `mapGroupsWithState` / `flatMapGroupsWithState`.
///
/// `S` is the per-group state type.
pub trait GroupStateFn<S>: Send {
    /// Process all rows for `key` in the current micro-batch.
    ///
    /// - `key` — the group's key string (derived from the key column).
    /// - `rows` — slice of `(batch, row_index)` pairs; **empty** when called
    ///   for a timeout expiry.
    /// - `state` — mutable per-group state.  Call `state.update(…)` to persist
    ///   state for the next batch, or `state.remove_state()` to drop it.
    ///
    /// Returns output [`RecordBatch`]es emitted by this invocation (may be empty).
    fn on_group(
        &mut self,
        key: &str,
        rows: &[(&RecordBatch, usize)],
        state: &mut GroupState<S>,
    ) -> ExecResult<Vec<RecordBatch>>;
}

// ── GroupStateExecutor ────────────────────────────────────────────────────────

/// Executes a [`GroupStateFn`] against a stream of micro-batches.
///
/// # Lifecycle
/// 1. Call [`process_batch`][Self::process_batch] for each incoming batch.
/// 2. Call [`fire_timeouts`][Self::fire_timeouts] whenever the watermark advances.
pub struct GroupStateExecutor<S> {
    func: Box<dyn GroupStateFn<S>>,
    key_column: String,
    states: HashMap<String, S>,
    /// Event-time timeouts: `deadline_ms → Set<key>`.
    timeouts: BTreeMap<i64, HashSet<String>>,
    current_watermark_ms: i64,
    /// LRU eviction cap: oldest key is dropped when `states.len() > max_keys`.
    max_keys: usize,
    access_order: IndexMap<String, ()>,
}

impl<S: Send + 'static> GroupStateExecutor<S> {
    /// Create a new executor.
    ///
    /// `key_column` is the name of the column that holds the group key
    /// (String or Int64 — interpreted as a string).
    pub fn new(key_column: impl Into<String>, func: Box<dyn GroupStateFn<S>>) -> Self {
        Self {
            func,
            key_column: key_column.into(),
            states: HashMap::new(),
            timeouts: BTreeMap::new(),
            current_watermark_ms: i64::MIN,
            max_keys: DEFAULT_GROUP_STATE_MAX_KEYS,
            access_order: IndexMap::new(),
        }
    }

    /// Override the default LRU cap (default: 100 000 keys).
    pub fn with_max_keys(mut self, max_keys: usize) -> Self {
        self.max_keys = max_keys;
        self
    }

    /// Process one micro-batch.
    ///
    /// Rows are grouped by key, then `on_group` is called once per unique key.
    /// Returns all output batches emitted by the user function.
    pub fn process_batch(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        self.current_watermark_ms = self.current_watermark_ms.max(watermark_ms);

        let key_idx = batch
            .schema()
            .index_of(&self.key_column)
            .map_err(|_| ExecError::ColumnNotFound(self.key_column.clone()))?;

        // Group row indices by key (preserving insertion order for determinism).
        let mut groups: Vec<(String, Vec<usize>)> = Vec::new();
        let mut key_to_group: HashMap<String, usize> = HashMap::new();
        for row in 0..batch.num_rows() {
            let key_val = extract_agg_key(batch, key_idx, row)?;
            let key_str = key_val.to_string();
            let idx = key_to_group.len();
            let group_idx = *key_to_group.entry(key_str.clone()).or_insert_with(|| {
                groups.push((key_str, Vec::new()));
                idx
            });
            if let Some(g) = groups.get_mut(group_idx) {
                g.1.push(row);
            }
        }

        let mut output = Vec::new();
        for (key, rows) in &groups {
            self.touch_key(key);
            let row_refs: Vec<(&RecordBatch, usize)> = rows.iter().map(|&r| (batch, r)).collect();
            let mut gs = GroupState::new(self.states.remove(key));
            let mut emitted = self.func.on_group(key, &row_refs, &mut gs)?;
            output.append(&mut emitted);
            self.apply_group_state(key, gs);
            // Evict only after applying state so the cap accounts for the new entry.
            self.maybe_evict_lru();
        }

        Ok(output)
    }

    /// Fire expired event-time timeouts (deadlines ≤ `watermark_ms`).
    ///
    /// For each expired key, `on_group` is called with an empty row slice.
    /// Returns all output batches emitted by the user function.
    pub fn fire_timeouts(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        self.current_watermark_ms = self.current_watermark_ms.max(watermark_ms);

        let fired: Vec<i64> = self
            .timeouts
            .range(..=watermark_ms)
            .map(|(&t, _)| t)
            .collect();

        let mut output = Vec::new();
        for deadline in fired {
            let Some(keys) = self.timeouts.remove(&deadline) else {
                continue;
            };
            for key in keys {
                self.touch_key(&key);
                let mut gs = GroupState::new(self.states.remove(&key));
                let mut emitted = self.func.on_group(&key, &[], &mut gs)?;
                output.append(&mut emitted);
                self.apply_group_state(&key, gs);
            }
        }
        Ok(output)
    }

    /// Current event-time watermark.
    pub fn watermark_ms(&self) -> i64 {
        self.current_watermark_ms
    }

    /// Number of groups with live state.
    pub fn active_group_count(&self) -> usize {
        self.states.len()
    }

    /// Number of pending timeout registrations.
    pub fn pending_timeout_count(&self) -> usize {
        self.timeouts.values().map(|s| s.len()).sum()
    }

    // ── Private ────────────────────────────────────────────────────────────────

    fn touch_key(&mut self, key: &str) {
        self.access_order.shift_remove(key);
        self.access_order.insert(key.to_owned(), ());
    }

    fn maybe_evict_lru(&mut self) {
        while self.states.len() > self.max_keys {
            if let Some((oldest, _)) = self.access_order.shift_remove_index(0) {
                self.states.remove(&oldest);
                // Cancel timeouts for evicted key.
                self.timeouts.values_mut().for_each(|s| {
                    s.remove(&oldest);
                });
                self.timeouts.retain(|_, s| !s.is_empty());
            } else {
                break;
            }
        }
    }

    fn apply_group_state(&mut self, key: &str, gs: GroupState<S>) {
        if gs.remove {
            // Explicit removal: discard state and cancel all timeouts for this key.
            self.states.remove(key);
            self.access_order.shift_remove(key);
            self.cancel_timeout(key);
            return;
        }
        // Persist state if the function produced a value.
        if let Some(s) = gs.value {
            self.states.insert(key.to_owned(), s);
        }
        // Register or update the timeout if one was set, independent of whether
        // state was updated. This allows set_timeout_ms() without update() to work.
        if let Some(deadline) = gs.expires_at_ms {
            self.cancel_timeout(key);
            self.timeouts
                .entry(deadline)
                .or_default()
                .insert(key.to_owned());
        }
    }

    fn cancel_timeout(&mut self, key: &str) {
        self.timeouts.values_mut().for_each(|s| {
            s.remove(key);
        });
        self.timeouts.retain(|_, s| !s.is_empty());
    }
}

// ── Snapshot / restore (requires S: Serialize + DeserializeOwned) ─────────────

/// Serialized snapshot of `GroupStateExecutor` state (excludes the user fn).
#[derive(serde::Serialize, serde::Deserialize)]
pub struct GroupStateSnapshot {
    pub states_json: String,
    pub timeouts: BTreeMap<i64, HashSet<String>>,
    pub current_watermark_ms: i64,
}

impl<S: Send + 'static + Serialize + DeserializeOwned> GroupStateExecutor<S> {
    /// Serialize current per-group states + timeout index into a JSON snapshot.
    pub fn snapshot(&self) -> Result<GroupStateSnapshot, serde_json::Error> {
        let states_json = serde_json::to_string(&self.states)?;
        Ok(GroupStateSnapshot {
            states_json,
            timeouts: self.timeouts.clone(),
            current_watermark_ms: self.current_watermark_ms,
        })
    }

    /// Restore states + timeout index from a previously generated snapshot.
    ///
    /// The user function is not touched — callers must construct the executor
    /// with the same function before calling `restore`.
    pub fn restore(&mut self, snap: GroupStateSnapshot) -> Result<(), serde_json::Error> {
        let states: HashMap<String, S> = serde_json::from_str(&snap.states_json)?;
        self.states = states;
        self.access_order = self
            .states
            .keys()
            .map(|k| (k.clone(), ()))
            .collect::<IndexMap<_, _>>();
        self.timeouts = snap.timeouts;
        self.current_watermark_ms = snap.current_watermark_ms;
        Ok(())
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    // ── Helpers ────────────────────────────────────────────────────────────────

    fn batch_with_key_and_val(keys: &[&str], vals: &[i64]) -> RecordBatch {
        assert_eq!(keys.len(), vals.len());
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())) as _,
                Arc::new(Int64Array::from(vals.to_vec())) as _,
            ],
        )
        .unwrap()
    }

    fn single_i64_batch(v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("out", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v])) as _]).unwrap()
    }

    fn extract_i64_col0(batch: &RecordBatch, row: usize) -> i64 {
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)
    }

    // ── Accumulator: sums all values seen for each key ─────────────────────────

    struct SumFn;
    impl GroupStateFn<i64> for SumFn {
        fn on_group(
            &mut self,
            _key: &str,
            rows: &[(&RecordBatch, usize)],
            state: &mut GroupState<i64>,
        ) -> ExecResult<Vec<RecordBatch>> {
            let current = state.value.unwrap_or(0);
            let delta: i64 = rows
                .iter()
                .map(|(batch, row)| extract_i64_col0(batch, *row))
                .sum();
            state.update(current + delta);
            Ok(vec![single_i64_batch(current + delta)])
        }
    }

    // ── Tests ──────────────────────────────────────────────────────────────────

    #[test]
    fn groups_are_called_once_per_key_per_batch() {
        let mut exec = GroupStateExecutor::new("key", Box::new(SumFn));
        let batch = batch_with_key_and_val(&["a", "a", "b"], &[1, 2, 10]);
        let out = exec.process_batch(&batch, 0).unwrap();
        // Two distinct keys → two on_group calls → two output batches.
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn state_accumulates_across_batches() {
        let mut exec = GroupStateExecutor::new("key", Box::new(SumFn));
        let b1 = batch_with_key_and_val(&["k"], &[5]);
        exec.process_batch(&b1, 0).unwrap();
        let b2 = batch_with_key_and_val(&["k"], &[3]);
        let out = exec.process_batch(&b2, 0).unwrap();
        assert_eq!(out.len(), 1);
        let sum = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(sum, 8, "accumulated state should be 5+3=8");
    }

    #[test]
    fn different_keys_have_independent_state() {
        let mut exec = GroupStateExecutor::new("key", Box::new(SumFn));
        let b = batch_with_key_and_val(&["x", "y"], &[100, 200]);
        exec.process_batch(&b, 0).unwrap();
        assert_eq!(exec.active_group_count(), 2);
    }

    // ── State removal ──────────────────────────────────────────────────────────

    struct RemoveAfterFirstFn;
    impl GroupStateFn<i64> for RemoveAfterFirstFn {
        fn on_group(
            &mut self,
            _key: &str,
            rows: &[(&RecordBatch, usize)],
            state: &mut GroupState<i64>,
        ) -> ExecResult<Vec<RecordBatch>> {
            if rows.is_empty() {
                // Timeout expiry — emit final value.
                let v = state.value.unwrap_or(0);
                state.remove_state();
                return Ok(vec![single_i64_batch(v)]);
            }
            state.update(1);
            state.remove_state(); // remove immediately
            Ok(vec![])
        }
    }

    #[test]
    fn remove_state_cleans_up_after_invocation() {
        let mut exec = GroupStateExecutor::new("key", Box::new(RemoveAfterFirstFn));
        let b = batch_with_key_and_val(&["k"], &[1]);
        exec.process_batch(&b, 0).unwrap();
        assert_eq!(exec.active_group_count(), 0, "state should be removed");
    }

    // ── Timeout ────────────────────────────────────────────────────────────────

    struct TimeoutFn {
        timeout_ms: i64,
    }
    impl GroupStateFn<i64> for TimeoutFn {
        fn on_group(
            &mut self,
            _key: &str,
            rows: &[(&RecordBatch, usize)],
            state: &mut GroupState<i64>,
        ) -> ExecResult<Vec<RecordBatch>> {
            if rows.is_empty() {
                // Timeout fired — emit accumulated total.
                let v = state.value.unwrap_or(0);
                state.remove_state();
                return Ok(vec![single_i64_batch(v)]);
            }
            let current = state.value.unwrap_or(0);
            let delta: i64 = rows
                .iter()
                .map(|(batch, row)| extract_i64_col0(batch, *row))
                .sum();
            state.update(current + delta);
            state.set_timeout_ms(self.timeout_ms);
            Ok(vec![])
        }
    }

    #[test]
    fn timeout_fires_when_watermark_advances() {
        let mut exec = GroupStateExecutor::new("key", Box::new(TimeoutFn { timeout_ms: 1000 }));
        let b = batch_with_key_and_val(&["k"], &[42]);
        exec.process_batch(&b, 0).unwrap();
        assert_eq!(exec.pending_timeout_count(), 1);

        // Watermark hasn't crossed deadline yet.
        let out = exec.fire_timeouts(500).unwrap();
        assert!(out.is_empty(), "timeout must not fire before deadline");

        // Watermark crosses deadline.
        let out = exec.fire_timeouts(1001).unwrap();
        assert_eq!(out.len(), 1, "timeout must fire once after deadline");
        let v = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(v, 42);
        assert_eq!(
            exec.active_group_count(),
            0,
            "state should be removed after timeout"
        );
        assert_eq!(exec.pending_timeout_count(), 0);
    }

    #[test]
    fn timeout_is_not_fired_again_after_state_removed() {
        let mut exec = GroupStateExecutor::new("key", Box::new(TimeoutFn { timeout_ms: 500 }));
        let b = batch_with_key_and_val(&["k"], &[7]);
        exec.process_batch(&b, 0).unwrap();
        exec.fire_timeouts(600).unwrap();
        // Fire again — must not re-fire.
        let out = exec.fire_timeouts(1000).unwrap();
        assert!(out.is_empty(), "timeout must not fire a second time");
    }

    #[test]
    fn timeout_can_be_rescheduled_within_invocation() {
        struct RescheduleFn;
        impl GroupStateFn<i64> for RescheduleFn {
            fn on_group(
                &mut self,
                _key: &str,
                rows: &[(&RecordBatch, usize)],
                state: &mut GroupState<i64>,
            ) -> ExecResult<Vec<RecordBatch>> {
                if rows.is_empty() {
                    state.remove_state();
                    return Ok(vec![single_i64_batch(99)]);
                }
                state.update(1);
                // Set an initial timeout, then immediately reschedule.
                state.set_timeout_ms(500);
                state.set_timeout_ms(2000);
                Ok(vec![])
            }
        }
        let mut exec = GroupStateExecutor::new("key", Box::new(RescheduleFn));
        let b = batch_with_key_and_val(&["k"], &[0]);
        exec.process_batch(&b, 0).unwrap();
        // Watermark at 600 — old deadline 500 should not fire; new deadline is 2000.
        let out = exec.fire_timeouts(600).unwrap();
        assert!(
            out.is_empty(),
            "rescheduled timeout must not fire at old deadline"
        );
        // Watermark at 2001 — new deadline fires.
        let out = exec.fire_timeouts(2001).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn empty_batch_produces_no_output() {
        let mut exec = GroupStateExecutor::new("key", Box::new(SumFn));
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let empty = RecordBatch::new_empty(schema);
        let out = exec.process_batch(&empty, 0).unwrap();
        assert!(out.is_empty());
        assert_eq!(exec.active_group_count(), 0);
    }

    #[test]
    fn missing_key_column_returns_error() {
        let mut exec = GroupStateExecutor::new("nonexistent", Box::new(SumFn));
        let b = batch_with_key_and_val(&["k"], &[1]);
        assert!(exec.process_batch(&b, 0).is_err());
    }

    // ── LRU eviction ──────────────────────────────────────────────────────────

    #[test]
    fn lru_eviction_caps_state_size() {
        let mut exec = GroupStateExecutor::new("key", Box::new(SumFn)).with_max_keys(3);
        // Insert 4 unique keys — the oldest should be evicted.
        for (k, v) in [("a", 1i64), ("b", 2), ("c", 3), ("d", 4)] {
            exec.process_batch(&batch_with_key_and_val(&[k], &[v]), 0)
                .unwrap();
        }
        assert!(
            exec.active_group_count() <= 3,
            "LRU cap must keep at most 3 keys, got {}",
            exec.active_group_count()
        );
    }

    // ── Snapshot / restore (Fix #6) ───────────────────────────────────────────

    #[test]
    fn snapshot_and_restore_preserves_state() {
        let mut exec = GroupStateExecutor::new("key", Box::new(SumFn));
        // Accumulate state for two keys.
        exec.process_batch(&batch_with_key_and_val(&["x", "y"], &[10, 20]), 0)
            .unwrap();
        let snap = exec.snapshot().expect("snapshot must succeed");

        // Create a fresh executor and restore.
        let mut exec2 = GroupStateExecutor::new("key", Box::new(SumFn));
        exec2.restore(snap).expect("restore must succeed");
        assert_eq!(
            exec2.active_group_count(),
            2,
            "restored executor must have 2 keys"
        );

        // Further accumulation on the restored executor should see prior state.
        exec2
            .process_batch(&batch_with_key_and_val(&["x"], &[5]), 0)
            .unwrap();
        assert_eq!(exec2.active_group_count(), 2);
    }

    #[test]
    fn snapshot_watermark_is_preserved() {
        let mut exec = GroupStateExecutor::new("key", Box::new(SumFn));
        exec.process_batch(&batch_with_key_and_val(&["k"], &[1]), 5_000)
            .unwrap();
        let snap = exec.snapshot().expect("snapshot");
        assert_eq!(snap.current_watermark_ms, 5_000);
    }

    // ── Fix #7: set_timeout_ms without update() works ─────────────────────────

    #[test]
    fn timeout_without_state_value_is_registered() {
        // A function that calls set_timeout_ms but does NOT call state.update().
        struct TimeoutOnlyFn;
        impl GroupStateFn<i64> for TimeoutOnlyFn {
            fn on_group(
                &mut self,
                _key: &str,
                rows: &[(&RecordBatch, usize)],
                state: &mut GroupState<i64>,
            ) -> ExecResult<Vec<RecordBatch>> {
                if rows.is_empty() {
                    return Ok(vec![single_i64_batch(99)]);
                }
                // Only set a timeout — no state.update().
                state.set_timeout_ms(1000);
                Ok(vec![])
            }
        }
        let mut exec = GroupStateExecutor::new("key", Box::new(TimeoutOnlyFn));
        exec.process_batch(&batch_with_key_and_val(&["k"], &[0]), 0)
            .unwrap();
        // Timeout must be registered even though no state value was set.
        assert_eq!(
            exec.pending_timeout_count(),
            1,
            "timeout must be registered without update()"
        );
        let out = exec.fire_timeouts(1001).unwrap();
        assert_eq!(
            out.len(),
            1,
            "timeout must fire when watermark crosses deadline"
        );
        let v = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(v, 99);
    }
}
