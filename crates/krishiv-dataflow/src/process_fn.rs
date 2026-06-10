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
//! - Timers fire at `fire_time_ms ≤ watermark_ms`.
//! - Output is collected as a `Vec<RecordBatch>` (one per `emit_record` call).
//! - The executor is single-threaded (no `Arc<Mutex<…>>`); async variants can
//!   wrap it in a task.

use std::collections::{BTreeMap, HashMap};

use arrow::record_batch::RecordBatch;

use crate::ExecResult;

// ── ProcessContext ────────────────────────────────────────────────────────────

/// Context passed to [`ProcessFunction`] callbacks.
///
/// Gives the function access to per-key state, timer registration, and output.
pub struct ProcessContext<'a> {
    /// Current event-time watermark in milliseconds.
    pub watermark_ms: i64,
    /// Serialisable per-key state blob (empty on first access for a key).
    pub state: &'a mut Vec<u8>,
    /// Collected output batches; the function appends here via [`emit`][Self::emit].
    pub output: &'a mut Vec<RecordBatch>,
    /// Timers to register; collected and merged into the global timer map.
    pub(crate) timers_to_register: &'a mut Vec<(String, i64)>,
}

impl<'a> ProcessContext<'a> {
    /// Append an output record batch.
    pub fn emit(&mut self, batch: RecordBatch) {
        self.output.push(batch);
    }

    /// Register a timer to fire when the watermark reaches `fire_time_ms`.
    ///
    /// Registering the same `(key, fire_time_ms)` pair twice is idempotent.
    pub fn register_timer(&mut self, key: impl Into<String>, fire_time_ms: i64) {
        self.timers_to_register.push((key.into(), fire_time_ms));
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
    /// `ctx.register_timer`.
    fn on_event(
        &mut self,
        key: &str,
        batch: &RecordBatch,
        row: usize,
        ctx: &mut ProcessContext<'_>,
    ) -> ExecResult<()>;

    /// Called when a timer fires (watermark ≥ `fire_time_ms`).
    ///
    /// The function may emit output and register new timers.
    fn on_timer(
        &mut self,
        key: &str,
        fire_time_ms: i64,
        ctx: &mut ProcessContext<'_>,
    ) -> ExecResult<()>;
}

// ── ProcessFunctionExecutor ───────────────────────────────────────────────────

/// Runtime wrapper for a [`ProcessFunction`].
///
/// Manages per-key state, timer registration, and timer firing.
pub struct ProcessFunctionExecutor {
    func: Box<dyn ProcessFunction>,
    key_column: String,
    /// Per-key state: `key_str → Vec<u8>`.
    state: HashMap<String, Vec<u8>>,
    /// Timer map: `fire_time_ms → Set<key_str>`.
    timers: BTreeMap<i64, Vec<String>>,
    current_watermark_ms: i64,
}

impl ProcessFunctionExecutor {
    pub fn new(key_column: impl Into<String>, func: Box<dyn ProcessFunction>) -> Self {
        Self {
            func,
            key_column: key_column.into(),
            state: HashMap::new(),
            timers: BTreeMap::new(),
            current_watermark_ms: i64::MIN,
        }
    }

    /// Process one input batch, collecting output batches and any timers fired.
    pub fn process_batch(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        self.current_watermark_ms = self.current_watermark_ms.max(watermark_ms);

        let key_idx = batch
            .schema()
            .index_of(&self.key_column)
            .map_err(|_| crate::ExecError::ColumnNotFound(self.key_column.clone()))?;

        let mut output = Vec::new();
        let mut timers_to_register: Vec<(String, i64)> = Vec::new();

        for row in 0..batch.num_rows() {
            let key = crate::join::extract_agg_key(batch, key_idx, row)?;
            let key_str = key.to_string();
            let key_state = self.state.entry(key_str.clone()).or_default();

            let mut ctx = ProcessContext {
                watermark_ms,
                state: key_state,
                output: &mut output,
                timers_to_register: &mut timers_to_register,
            };

            self.func.on_event(&key_str, batch, row, &mut ctx)?;
        }

        for (key, fire_time) in timers_to_register {
            let bucket = self.timers.entry(fire_time).or_default();
            if !bucket.contains(&key) {
                bucket.push(key);
            }
        }

        Ok(output)
    }

    /// Fire all timers with `fire_time_ms ≤ watermark_ms`.
    ///
    /// Returns all output batches emitted by timer callbacks.
    pub fn fire_timers(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        self.current_watermark_ms = self.current_watermark_ms.max(watermark_ms);

        let mut output = Vec::new();
        let mut timers_to_register: Vec<(String, i64)> = Vec::new();

        // Collect fired timer keys (all timers with fire_time ≤ watermark).
        let fired_times: Vec<i64> = self
            .timers
            .range(..=watermark_ms)
            .map(|(&t, _)| t)
            .collect();

        for fire_time in fired_times {
            let Some(keys) = self.timers.remove(&fire_time) else {
                continue;
            };
            for key in keys {
                let key_state = self.state.entry(key.clone()).or_default();
                let mut ctx = ProcessContext {
                    watermark_ms,
                    state: key_state,
                    output: &mut output,
                    timers_to_register: &mut timers_to_register,
                };
                self.func.on_timer(&key, fire_time, &mut ctx)?;
            }
        }

        for (key, fire_time) in timers_to_register {
            let bucket = self.timers.entry(fire_time).or_default();
            if !bucket.contains(&key) {
                bucket.push(key);
            }
        }

        Ok(output)
    }

    /// Return the current watermark.
    pub fn watermark_ms(&self) -> i64 {
        self.current_watermark_ms
    }

    /// Return a reference to the per-key state map.
    pub fn state_map(&self) -> &HashMap<String, Vec<u8>> {
        &self.state
    }

    /// Return pending timer count.
    pub fn pending_timer_count(&self) -> usize {
        self.timers.values().map(|v| v.len()).sum()
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
        fn new() -> Self { Self { counts: HashMap::new() } }
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
}
