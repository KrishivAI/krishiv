#![forbid(unsafe_code)]

//! E3.5 — ProcessFunction: per-key stateful stream processing with timers.
//!
//! Inspired by Apache Flink's `KeyedProcessFunction`. Users implement the
//! [`ProcessFunction`] trait and register timers via [`ProcessContext`].
//! The runtime calls [`ProcessFunctionExecutor::fire_timers`] whenever the
//! watermark advances, triggering any expired timers in key order.
//!
//! # Design
//! - State is `Vec<u8>` (serialised by the user). The runtime doesn't inspect it.
//! - Event-time timers fire at `fire_time_ms ≤ watermark_ms`.
//! - Processing-time timers fire at `fire_time_ms ≤ processing_time_ms`.
//! - Output is collected as a `Vec<RecordBatch>` (one per `emit_record` call).
//! - The executor is single-threaded (no `Arc<Mutex<…>>`); async variants can
//!   wrap it in a task.

use std::collections::{BTreeMap, HashMap, HashSet};

use arrow::record_batch::RecordBatch;
use indexmap::IndexMap;

use crate::ExecError;
use serde::{Deserialize, Serialize};

use crate::ExecResult;

// ── TimerKind ─────────────────────────────────────────────────────────────────

/// Discriminates between event-time and processing-time timers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimerKind {
    /// Fires when the event-time watermark reaches `fire_time_ms`.
    EventTime,
    /// Fires when the wall-clock processing time reaches `fire_time_ms`.
    ProcessingTime,
}

/// A timer registration entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimerEntry {
    pub key: String,
    pub fire_time_ms: i64,
    pub kind: TimerKind,
}

// ── ProcessContext ────────────────────────────────────────────────────────────

/// Context passed to [`ProcessFunction`] callbacks.
///
/// Gives the function access to per-key state, timer registration, and output.
pub struct ProcessContext<'a> {
    /// Current event-time watermark in milliseconds.
    pub watermark_ms: i64,
    /// Current wall-clock processing time in milliseconds.
    pub processing_time_ms: i64,
    /// Serialisable per-key state blob (empty on first access for a key).
    pub state: &'a mut Vec<u8>,
    /// Collected output batches; the function appends here via [`emit`][Self::emit].
    pub output: &'a mut Vec<RecordBatch>,
    /// Timers to register; collected and merged into the global timer map.
    pub(crate) timers_to_register: &'a mut Vec<TimerEntry>,
}

impl<'a> ProcessContext<'a> {
    /// Append an output record batch.
    pub fn emit(&mut self, batch: RecordBatch) {
        self.output.push(batch);
    }

    /// Register an event-time timer to fire when the watermark reaches `fire_time_ms`.
    ///
    /// Registering the same `(key, fire_time_ms)` pair twice is idempotent.
    pub fn register_event_time_timer(&mut self, key: impl Into<String>, fire_time_ms: i64) {
        self.timers_to_register.push(TimerEntry {
            key: key.into(),
            fire_time_ms,
            kind: TimerKind::EventTime,
        });
    }

    /// Register a processing-time timer to fire when processing time reaches `fire_time_ms`.
    ///
    /// Registering the same `(key, fire_time_ms)` pair twice is idempotent.
    pub fn register_processing_time_timer(&mut self, key: impl Into<String>, fire_time_ms: i64) {
        self.timers_to_register.push(TimerEntry {
            key: key.into(),
            fire_time_ms,
            kind: TimerKind::ProcessingTime,
        });
    }

    /// Register a timer to fire when the watermark reaches `fire_time_ms`.
    ///
    /// Deprecated: use [`register_event_time_timer`][Self::register_event_time_timer] instead.
    /// Kept for backward compatibility.
    pub fn register_timer(&mut self, key: impl Into<String>, fire_time_ms: i64) {
        self.register_event_time_timer(key, fire_time_ms);
    }
}

// ── ProcessFunction trait ─────────────────────────────────────────────────────

/// User-defined per-key processing function.
///
/// Implement this trait to build stateful stream operators that are not
/// covered by the built-in window operators.
pub trait ProcessFunction: Send {
    /// Process one row from an input batch.
    ///
    /// `key` is the serialised join key. `batch` is the full input batch;
    /// `row` is the row index within it.
    ///
    /// The function may read and write `ctx.state` and call `ctx.emit` and
    /// `ctx.register_event_time_timer` / `ctx.register_processing_time_timer`.
    fn on_event(
        &mut self,
        key: &str,
        batch: &RecordBatch,
        row: usize,
        ctx: &mut ProcessContext<'_>,
    ) -> ExecResult<()>;

    /// Called when a timer fires (watermark ≥ `fire_time_ms` for event-time
    /// timers, or processing_time ≥ `fire_time_ms` for processing-time timers).
    ///
    /// The function may emit output and register new timers.
    fn on_timer(
        &mut self,
        key: &str,
        fire_time_ms: i64,
        ctx: &mut ProcessContext<'_>,
    ) -> ExecResult<()>;
}

// ── Snapshot helpers ──────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct ExecutorSnapshot {
    /// Per-key state map: key → base64-encoded raw bytes.
    state: HashMap<String, Vec<u8>>,
    /// Pending timers.
    timers: Vec<TimerEntry>,
    current_watermark_ms: i64,
    access_order: Vec<String>,
}

// ── ProcessFunctionExecutor ───────────────────────────────────────────────────

/// Default cap on the number of distinct per-key states retained in memory.
const DEFAULT_PROCESS_FN_MAX_KEYS: usize = 100_000;

/// Runtime wrapper for a [`ProcessFunction`].
///
/// Manages per-key state, timer registration, and timer firing.
pub struct ProcessFunctionExecutor {
    func: Box<dyn ProcessFunction>,
    key_column: String,
    /// Per-key state: `key_str → Vec<u8>`.
    state: HashMap<String, Vec<u8>>,
    /// Event-time timer map: `fire_time_ms → Set<key_str>`.
    event_timers: BTreeMap<i64, HashSet<String>>,
    /// Processing-time timer map: `fire_time_ms → Set<key_str>`.
    processing_timers: BTreeMap<i64, HashSet<String>>,
    current_watermark_ms: i64,
    max_keys: usize,
    access_order: IndexMap<String, ()>,
}

impl ProcessFunctionExecutor {
    pub fn new(key_column: impl Into<String>, func: Box<dyn ProcessFunction>) -> Self {
        Self {
            func,
            key_column: key_column.into(),
            state: HashMap::new(),
            event_timers: BTreeMap::new(),
            processing_timers: BTreeMap::new(),
            current_watermark_ms: i64::MIN,
            max_keys: DEFAULT_PROCESS_FN_MAX_KEYS,
            access_order: IndexMap::new(),
        }
    }

    /// Process one input batch, collecting output batches and any timers fired.
    pub fn process_batch(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        self.process_batch_with_processing_time(batch, watermark_ms, watermark_ms)
    }

    /// Process one input batch with explicit processing-time support.
    pub fn process_batch_with_processing_time(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
        processing_time_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        self.current_watermark_ms = self.current_watermark_ms.max(watermark_ms);

        let key_idx = batch
            .schema()
            .index_of(&self.key_column)
            .map_err(|_| crate::ExecError::ColumnNotFound(self.key_column.clone()))?;

        let mut output = Vec::new();
        let mut timers_to_register: Vec<TimerEntry> = Vec::new();

        for row in 0..batch.num_rows() {
            let key = crate::join::extract_agg_key(batch, key_idx, row)?;
            let key_str = key.to_string();
            self.touch_key(&key_str);
            self.maybe_evict();
            let key_state = self.state.entry(key_str.clone()).or_default();

            let mut ctx = ProcessContext {
                watermark_ms,
                processing_time_ms,
                state: key_state,
                output: &mut output,
                timers_to_register: &mut timers_to_register,
            };

            self.func.on_event(&key_str, batch, row, &mut ctx)?;
        }

        self.merge_timer_registrations(timers_to_register);

        Ok(output)
    }

    /// Fire all timers with `fire_time_ms ≤ watermark_ms` (event-time) and
    /// `fire_time_ms ≤ processing_time_ms` (processing-time).
    ///
    /// Returns all output batches emitted by timer callbacks.
    pub fn fire_timers(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        self.fire_timers_with_processing_time(watermark_ms, watermark_ms)
    }

    /// Fire timers with explicit processing-time threshold.
    pub fn fire_timers_with_processing_time(
        &mut self,
        watermark_ms: i64,
        processing_time_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        self.current_watermark_ms = self.current_watermark_ms.max(watermark_ms);

        let mut output = Vec::new();
        let mut timers_to_register: Vec<TimerEntry> = Vec::new();

        // Fire event-time timers (fire_time ≤ watermark_ms).
        let fired_event_times: Vec<i64> = self
            .event_timers
            .range(..=watermark_ms)
            .map(|(&t, _)| t)
            .collect();

        for fire_time in fired_event_times {
            let Some(keys) = self.event_timers.remove(&fire_time) else {
                continue;
            };
            for key in keys {
                self.touch_key(&key);
                self.maybe_evict();
                let key_state = self.state.entry(key.clone()).or_default();
                let mut ctx = ProcessContext {
                    watermark_ms,
                    processing_time_ms,
                    state: key_state,
                    output: &mut output,
                    timers_to_register: &mut timers_to_register,
                };
                if let Err(e) = self.func.on_timer(&key, fire_time, &mut ctx) {
                    tracing::error!(key = %key, fire_time = fire_time, error = %e,
                        "event timer callback failed — continuing with remaining timers");
                }
            }
        }

        // Fire processing-time timers (fire_time ≤ processing_time_ms).
        let fired_proc_times: Vec<i64> = self
            .processing_timers
            .range(..=processing_time_ms)
            .map(|(&t, _)| t)
            .collect();

        for fire_time in fired_proc_times {
            let Some(keys) = self.processing_timers.remove(&fire_time) else {
                continue;
            };
            for key in keys {
                self.touch_key(&key);
                self.maybe_evict();
                let key_state = self.state.entry(key.clone()).or_default();
                let mut ctx = ProcessContext {
                    watermark_ms,
                    processing_time_ms,
                    state: key_state,
                    output: &mut output,
                    timers_to_register: &mut timers_to_register,
                };
                if let Err(e) = self.func.on_timer(&key, fire_time, &mut ctx) {
                    tracing::error!(key = %key, fire_time = fire_time, error = %e,
                        "processing-time timer callback failed — continuing with remaining timers");
                }
            }
        }

        self.merge_timer_registrations(timers_to_register);

        Ok(output)
    }

    /// Merge newly registered timers into the executor's timer maps.
    fn merge_timer_registrations(&mut self, timers: Vec<TimerEntry>) {
        for entry in timers {
            match entry.kind {
                TimerKind::EventTime => {
                    self.event_timers
                        .entry(entry.fire_time_ms)
                        .or_default()
                        .insert(entry.key);
                }
                TimerKind::ProcessingTime => {
                    self.processing_timers
                        .entry(entry.fire_time_ms)
                        .or_default()
                        .insert(entry.key);
                }
            }
        }
    }

    /// Return the current watermark.
    pub fn watermark_ms(&self) -> i64 {
        self.current_watermark_ms
    }

    fn touch_key(&mut self, key: &str) {
        self.access_order.shift_remove(key);
        self.access_order.insert(key.to_owned(), ());
    }

    fn maybe_evict(&mut self) {
        if self.access_order.len() > self.max_keys
            && let Some((oldest, _)) = self.access_order.shift_remove_index(0)
        {
            self.state.remove(&oldest);
        }
    }

    /// Return a reference to the per-key state map.
    pub fn state_map(&self) -> &HashMap<String, Vec<u8>> {
        &self.state
    }

    /// Return total pending timer count (event-time + processing-time).
    pub fn pending_timer_count(&self) -> usize {
        let et: usize = self.event_timers.values().map(|v| v.len()).sum();
        let pt: usize = self.processing_timers.values().map(|v| v.len()).sum();
        et + pt
    }

    /// Serialize operator state and pending timers to a snapshot blob.
    ///
    /// The snapshot can later be passed to [`restore`][Self::restore] to
    /// reconstruct the executor's state (minus the `ProcessFunction` itself).
    pub fn snapshot(&self) -> ExecResult<Vec<u8>> {
        let mut timers: Vec<TimerEntry> = Vec::new();
        for (&fire_time, keys) in &self.event_timers {
            for key in keys {
                timers.push(TimerEntry {
                    key: key.clone(),
                    fire_time_ms: fire_time,
                    kind: TimerKind::EventTime,
                });
            }
        }
        for (&fire_time, keys) in &self.processing_timers {
            for key in keys {
                timers.push(TimerEntry {
                    key: key.clone(),
                    fire_time_ms: fire_time,
                    kind: TimerKind::ProcessingTime,
                });
            }
        }

        let access_order: Vec<String> = self
            .access_order
            .iter()
            .map(|(k, _): (&String, &())| k.clone())
            .collect();

        let snap = ExecutorSnapshot {
            state: self.state.clone(),
            timers,
            current_watermark_ms: self.current_watermark_ms,
            access_order,
        };
        serde_json::to_vec(&snap)
            .map_err(|e| ExecError::InvalidInput(format!("snapshot serialization failed: {e}")))
    }

    /// Restore operator state and pending timers from a snapshot blob.
    ///
    /// The `ProcessFunction` itself is not serialised; only state and timers
    /// are restored. Multiple task snapshots can be merged by calling
    /// `restore` repeatedly — state keys from later calls overwrite earlier
    /// ones, and timers are merged idempotently.
    pub fn restore(&mut self, bytes: &[u8]) -> ExecResult<()> {
        let snap: ExecutorSnapshot = serde_json::from_slice(bytes)
            .map_err(|e| crate::ExecError::InvalidInput(e.to_string()))?;

        // Merge state (later calls override earlier ones for the same key).
        for (k, v) in snap.state {
            self.state.insert(k, v);
        }

        // Merge access order idempotently.
        for key in &snap.access_order {
            self.access_order.shift_remove(key);
            self.access_order.insert(key.clone(), ());
        }

        // Advance watermark monotonically.
        self.current_watermark_ms = self.current_watermark_ms.max(snap.current_watermark_ms);

        // Merge timers idempotently.
        self.merge_timer_registrations(snap.timers);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn int_batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    /// A simple ProcessFunction that counts events per key and registers a
    /// "flush" timer 10ms after the first event.
    struct CountAndTimerFn {
        counts: HashMap<String, u64>,
    }

    impl CountAndTimerFn {
        fn new() -> Self {
            Self {
                counts: HashMap::new(),
            }
        }
    }

    impl ProcessFunction for CountAndTimerFn {
        fn on_event(
            &mut self,
            key: &str,
            _batch: &RecordBatch,
            _row: usize,
            ctx: &mut ProcessContext<'_>,
        ) -> ExecResult<()> {
            let count = self.counts.entry(key.to_owned()).or_default();
            *count += 1;
            if *count == 1 {
                // Register timer 10ms after first event.
                ctx.register_timer(key, ctx.watermark_ms + 10);
            }
            Ok(())
        }

        fn on_timer(
            &mut self,
            key: &str,
            fire_time_ms: i64,
            ctx: &mut ProcessContext<'_>,
        ) -> ExecResult<()> {
            let count = self.counts.get(key).copied().unwrap_or(0);
            // Emit a summary batch (key count as Int64).
            use arrow::array::Int64Array;
            use arrow::datatypes::DataType;
            let schema = Arc::new(Schema::new(vec![
                Field::new("key_str", DataType::Utf8, false),
                Field::new("count", DataType::Int64, false),
                Field::new("fired_at_ms", DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(arrow::array::StringArray::from(vec![key])),
                    Arc::new(Int64Array::from(vec![count as i64])),
                    Arc::new(Int64Array::from(vec![fire_time_ms])),
                ],
            )
            .unwrap();
            ctx.emit(batch);
            Ok(())
        }
    }

    #[test]
    fn process_function_fires_timer_on_watermark_advance() {
        let func = CountAndTimerFn::new();
        let mut exec = ProcessFunctionExecutor::new("id", Box::new(func));

        let batch = int_batch(&[1, 1, 2]);
        let out = exec.process_batch(&batch, 100).unwrap();
        assert!(out.is_empty(), "no timers fired yet");
        assert_eq!(exec.pending_timer_count(), 2, "one timer per key");

        // Advance watermark past timer fire time.
        let fired = exec.fire_timers(200).unwrap();
        assert_eq!(fired.len(), 2, "one output batch per key timer");
    }

    #[test]
    fn process_function_timer_not_fired_before_watermark() {
        let func = CountAndTimerFn::new();
        let mut exec = ProcessFunctionExecutor::new("id", Box::new(func));

        let batch = int_batch(&[1]);
        exec.process_batch(&batch, 100).unwrap();
        assert_eq!(exec.pending_timer_count(), 1);

        // Watermark at 105, timer at 110 — should not fire.
        let fired = exec.fire_timers(105).unwrap();
        assert!(fired.is_empty(), "timer at 110 should not fire at wm=105");
        assert_eq!(exec.pending_timer_count(), 1, "timer remains pending");

        // Now advance past 110.
        let fired = exec.fire_timers(115).unwrap();
        assert_eq!(fired.len(), 1, "timer fires at wm=115");
        assert_eq!(exec.pending_timer_count(), 0);
    }

    #[test]
    fn process_function_state_persists_across_batches() {
        let func = CountAndTimerFn::new();
        let mut exec = ProcessFunctionExecutor::new("id", Box::new(func));

        exec.process_batch(&int_batch(&[1, 1]), 100).unwrap();
        exec.process_batch(&int_batch(&[1]), 200).unwrap();
        // Total 3 events for key "1".
        let fired = exec.fire_timers(500).unwrap();
        assert_eq!(fired.len(), 1);
        // Check the count column.
        use arrow::array::Int64Array;
        let count_col = fired[0].column_by_name("count").unwrap();
        let count = count_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(count.value(0), 3, "count should be 3 across batches");
    }

    // ── G: processing-time timer test ────────────────────────────────────────

    struct ProcessingTimerFn {
        fired: bool,
    }

    impl ProcessFunction for ProcessingTimerFn {
        fn on_event(
            &mut self,
            key: &str,
            _batch: &RecordBatch,
            _row: usize,
            ctx: &mut ProcessContext<'_>,
        ) -> ExecResult<()> {
            // Register a processing-time timer 50ms from now.
            ctx.register_processing_time_timer(key, ctx.processing_time_ms + 50);
            Ok(())
        }

        fn on_timer(
            &mut self,
            _key: &str,
            _fire_time_ms: i64,
            _ctx: &mut ProcessContext<'_>,
        ) -> ExecResult<()> {
            self.fired = true;
            Ok(())
        }
    }

    #[test]
    fn processing_time_timer_fires_at_threshold() {
        let func = ProcessingTimerFn { fired: false };
        let mut exec = ProcessFunctionExecutor::new("id", Box::new(func));

        // Register timer at processing_time = 100 + 50 = 150.
        exec.process_batch_with_processing_time(&int_batch(&[1]), 0, 100)
            .unwrap();

        // Fire at processing_time = 140 — should NOT fire.
        let out = exec.fire_timers_with_processing_time(0, 140).unwrap();
        assert!(
            out.is_empty(),
            "timer at 150 should not fire at proc_time=140"
        );
        assert_eq!(exec.pending_timer_count(), 1);

        // Fire at processing_time = 150 — should fire.
        let out = exec.fire_timers_with_processing_time(0, 150).unwrap();
        // Timer fires (no output batch in this impl, but pending count drops).
        assert_eq!(
            exec.pending_timer_count(),
            0,
            "timer should have been consumed"
        );
        let _ = out; // no output expected from ProcessingTimerFn
    }

    // ── G: snapshot/restore test ──────────────────────────────────────────────

    struct StatefulFn;

    impl ProcessFunction for StatefulFn {
        fn on_event(
            &mut self,
            _key: &str,
            _batch: &RecordBatch,
            _row: usize,
            _ctx: &mut ProcessContext<'_>,
        ) -> ExecResult<()> {
            Ok(())
        }

        fn on_timer(
            &mut self,
            _key: &str,
            _fire_time_ms: i64,
            _ctx: &mut ProcessContext<'_>,
        ) -> ExecResult<()> {
            Ok(())
        }
    }

    #[test]
    fn snapshot_restore_preserves_state_and_timers() {
        let func = CountAndTimerFn::new();
        let mut exec = ProcessFunctionExecutor::new("id", Box::new(func));

        exec.process_batch(&int_batch(&[1, 2]), 100).unwrap();
        // Two keys → two event-time timers at 110.
        assert_eq!(exec.pending_timer_count(), 2);

        let snap = exec.snapshot().unwrap();

        // Create a new executor and restore into it.
        let func2 = CountAndTimerFn::new();
        let mut exec2 = ProcessFunctionExecutor::new("id", Box::new(func2));
        exec2.restore(&snap).unwrap();

        assert_eq!(
            exec2.pending_timer_count(),
            2,
            "restored timer count must match"
        );
        assert_eq!(
            exec2.watermark_ms(),
            exec.watermark_ms(),
            "restored watermark must match"
        );
    }

    #[test]
    fn rescaling_restore_merges_snapshots() {
        // Simulate two parallel task snapshots being merged into one executor.
        let func_a = CountAndTimerFn::new();
        let mut exec_a = ProcessFunctionExecutor::new("id", Box::new(func_a));
        // Task A handles key 1.
        exec_a.process_batch(&int_batch(&[1]), 100).unwrap();
        let snap_a = exec_a.snapshot().unwrap();

        let func_b = CountAndTimerFn::new();
        let mut exec_b = ProcessFunctionExecutor::new("id", Box::new(func_b));
        // Task B handles key 2.
        exec_b.process_batch(&int_batch(&[2]), 200).unwrap();
        let snap_b = exec_b.snapshot().unwrap();

        // Merged executor.
        let func_merged = CountAndTimerFn::new();
        let mut exec_merged = ProcessFunctionExecutor::new("id", Box::new(func_merged));
        exec_merged.restore(&snap_a).unwrap();
        exec_merged.restore(&snap_b).unwrap();

        // Should have 2 pending timers (one per key) and watermark = max(100, 200) = 200.
        assert_eq!(
            exec_merged.pending_timer_count(),
            2,
            "merged timers from both tasks"
        );
        assert_eq!(
            exec_merged.watermark_ms(),
            200,
            "watermark must be max of both tasks"
        );
        assert_eq!(
            exec_merged.state_map().len(),
            2,
            "state from both tasks must be present"
        );
    }
}
