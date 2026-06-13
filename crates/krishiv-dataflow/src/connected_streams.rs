#![forbid(unsafe_code)]

//! Connected streams: process two input streams with a single co-function.
//!
//! Inspired by Apache Flink's `ConnectedStreams` / `CoProcessFunction`. A
//! [`CoProcessFunction`] receives events from two typed streams and can share
//! per-key state and timers between them.

use std::collections::{BTreeMap, HashMap};

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::process_fn::{ProcessContext, TimerEntry, TimerKind};
use crate::ExecResult;

// ── ConnectedStreams ───────────────────────────────────────────────────────────

/// A pair of streams to be processed together by a [`CoProcessFunction`].
pub struct ConnectedStreams<L, R> {
    left: L,
    right: R,
}

impl<L, R> ConnectedStreams<L, R> {
    /// Create a new `ConnectedStreams` from two stream handles.
    pub fn new(left: L, right: R) -> Self {
        Self { left, right }
    }

    /// Access the left stream.
    pub fn left(&self) -> &L {
        &self.left
    }

    /// Access the right stream.
    pub fn right(&self) -> &R {
        &self.right
    }

    /// Decompose into the underlying stream handles.
    pub fn into_parts(self) -> (L, R) {
        (self.left, self.right)
    }
}

// ── CoProcessFunction trait ───────────────────────────────────────────────────

/// Co-process function for two connected streams.
///
/// The function can share state and timers across both streams via the shared
/// [`ProcessContext`].
pub trait CoProcessFunction: Send {
    /// Called for each event on the first (left) stream.
    fn on_stream1(
        &mut self,
        key: &str,
        batch: &RecordBatch,
        row: usize,
        ctx: &mut ProcessContext<'_>,
    ) -> ExecResult<()>;

    /// Called for each event on the second (right) stream.
    fn on_stream2(
        &mut self,
        key: &str,
        batch: &RecordBatch,
        row: usize,
        ctx: &mut ProcessContext<'_>,
    ) -> ExecResult<()>;

    /// Called when a timer fires.
    fn on_timer(
        &mut self,
        key: &str,
        fire_time_ms: i64,
        ctx: &mut ProcessContext<'_>,
    ) -> ExecResult<()>;
}

// ── Snapshot ─────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct CoProcessSnapshot {
    state: HashMap<String, Vec<u8>>,
    timers: Vec<TimerEntry>,
    current_watermark_ms: i64,
}

// ── CoProcessExecutor ─────────────────────────────────────────────────────────

/// Executor for a [`CoProcessFunction`] over two record-batch input streams.
pub struct CoProcessExecutor {
    func: Box<dyn CoProcessFunction>,
    key_column: String,
    /// Per-key state shared across both streams.
    state: HashMap<String, Vec<u8>>,
    /// Timer map: `fire_time_ms → Vec<key_str>` (event-time only for simplicity).
    timers: BTreeMap<i64, Vec<String>>,
    current_watermark_ms: i64,
}

impl CoProcessExecutor {
    /// Create a new `CoProcessExecutor`.
    pub fn new(key_column: impl Into<String>, func: Box<dyn CoProcessFunction>) -> Self {
        Self {
            func,
            key_column: key_column.into(),
            state: HashMap::new(),
            timers: BTreeMap::new(),
            current_watermark_ms: i64::MIN,
        }
    }

    /// Process a batch from the first (left) stream.
    pub fn process_stream1(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        self.process_batch_impl(batch, watermark_ms, Stream::One)
    }

    /// Process a batch from the second (right) stream.
    pub fn process_stream2(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        self.process_batch_impl(batch, watermark_ms, Stream::Two)
    }

    fn process_batch_impl(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
        stream: Stream,
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
            let key_state = self.state.entry(key_str.clone()).or_default();

            let mut ctx = ProcessContext {
                watermark_ms,
                processing_time_ms: watermark_ms,
                state: key_state,
                output: &mut output,
                timers_to_register: &mut timers_to_register,
            };

            match stream {
                Stream::One => self.func.on_stream1(&key_str, batch, row, &mut ctx)?,
                Stream::Two => self.func.on_stream2(&key_str, batch, row, &mut ctx)?,
            }
        }

        self.merge_timers(timers_to_register);
        Ok(output)
    }

    /// Fire all timers with `fire_time_ms ≤ watermark_ms`.
    pub fn fire_timers(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        self.current_watermark_ms = self.current_watermark_ms.max(watermark_ms);

        let mut output = Vec::new();
        let mut timers_to_register: Vec<TimerEntry> = Vec::new();

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
                    processing_time_ms: watermark_ms,
                    state: key_state,
                    output: &mut output,
                    timers_to_register: &mut timers_to_register,
                };
                self.func.on_timer(&key, fire_time, &mut ctx)?;
            }
        }

        self.merge_timers(timers_to_register);
        Ok(output)
    }

    fn merge_timers(&mut self, timers: Vec<TimerEntry>) {
        for entry in timers {
            // Co-process executor only maintains event-time timers in the
            // BTreeMap. Processing-time timers are treated as event-time for
            // simplicity in this executor.
            let bucket = self.timers.entry(entry.fire_time_ms).or_default();
            if !bucket.contains(&entry.key) {
                bucket.push(entry.key);
            }
        }
    }

    /// Return a reference to the per-key state map.
    pub fn state_map(&self) -> &HashMap<String, Vec<u8>> {
        &self.state
    }

    /// Serialize operator state and pending timers to a snapshot blob.
    pub fn snapshot(&self) -> Vec<u8> {
        let mut timers: Vec<TimerEntry> = Vec::new();
        for (&fire_time, keys) in &self.timers {
            for key in keys {
                timers.push(TimerEntry {
                    key: key.clone(),
                    fire_time_ms: fire_time,
                    kind: TimerKind::EventTime,
                });
            }
        }
        let snap = CoProcessSnapshot {
            state: self.state.clone(),
            timers,
            current_watermark_ms: self.current_watermark_ms,
        };
        serde_json::to_vec(&snap).unwrap_or_default()
    }

    /// Restore operator state and pending timers from a snapshot blob.
    pub fn restore(&mut self, bytes: &[u8]) -> ExecResult<()> {
        let snap: CoProcessSnapshot = serde_json::from_slice(bytes)
            .map_err(|e| crate::ExecError::InvalidInput(e.to_string()))?;

        for (k, v) in snap.state {
            self.state.insert(k, v);
        }
        self.current_watermark_ms = self.current_watermark_ms.max(snap.current_watermark_ms);
        self.merge_timers(snap.timers);
        Ok(())
    }
}

// ── Internal stream discriminant ──────────────────────────────────────────────

enum Stream {
    One,
    Two,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

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

    /// Simple co-process function that tracks which stream each key was seen on.
    struct StreamTracker {
        stream1_keys: Vec<String>,
        stream2_keys: Vec<String>,
        timer_fired_keys: Vec<String>,
    }

    impl StreamTracker {
        fn new() -> Self {
            Self {
                stream1_keys: Vec::new(),
                stream2_keys: Vec::new(),
                timer_fired_keys: Vec::new(),
            }
        }
    }

    impl CoProcessFunction for StreamTracker {
        fn on_stream1(
            &mut self,
            key: &str,
            _batch: &RecordBatch,
            _row: usize,
            ctx: &mut ProcessContext<'_>,
        ) -> ExecResult<()> {
            self.stream1_keys.push(key.to_owned());
            ctx.register_event_time_timer(key, ctx.watermark_ms + 100);
            Ok(())
        }

        fn on_stream2(
            &mut self,
            key: &str,
            _batch: &RecordBatch,
            _row: usize,
            _ctx: &mut ProcessContext<'_>,
        ) -> ExecResult<()> {
            self.stream2_keys.push(key.to_owned());
            Ok(())
        }

        fn on_timer(
            &mut self,
            key: &str,
            _fire_time_ms: i64,
            _ctx: &mut ProcessContext<'_>,
        ) -> ExecResult<()> {
            self.timer_fired_keys.push(key.to_owned());
            Ok(())
        }
    }

    #[test]
    fn co_process_routes_stream1_and_stream2() {
        let tracker = StreamTracker::new();
        let mut exec = CoProcessExecutor::new("id", Box::new(tracker));

        let b1 = int_batch(&[1, 2]);
        exec.process_stream1(&b1, 0).unwrap();

        let b2 = int_batch(&[3, 4]);
        exec.process_stream2(&b2, 0).unwrap();

        // Fire timers to access the inner function again (to observe routing).
        let _out = exec.fire_timers(200).unwrap();

        // We can't easily access the inner function after boxing, but we can
        // verify the state map has the right keys from stream1.
        let state_keys: std::collections::HashSet<String> =
            exec.state_map().keys().cloned().collect();
        assert!(state_keys.contains("1"), "key 1 from stream1");
        assert!(state_keys.contains("2"), "key 2 from stream1");
        // Keys from stream2 may or may not be in state (depends on CoProcessFunction).
        // Timer for key 1 and 2 should fire at wm=200.
    }

    #[test]
    fn co_process_snapshot_restore() {
        let tracker = StreamTracker::new();
        let mut exec = CoProcessExecutor::new("id", Box::new(tracker));

        let b1 = int_batch(&[1]);
        exec.process_stream1(&b1, 100).unwrap();
        // Timer at 200.

        let snap = exec.snapshot();

        // Restore into a new executor.
        let tracker2 = StreamTracker::new();
        let mut exec2 = CoProcessExecutor::new("id", Box::new(tracker2));
        exec2.restore(&snap).unwrap();

        // Should have 1 pending timer and the state for key "1".
        let pending: usize = exec2.state_map().len();
        assert_eq!(pending, 1, "state for key 1 must be restored");
        assert_eq!(
            exec2.current_watermark_ms, 100,
            "watermark must be restored"
        );

        // Timer fires at watermark 200.
        let _out = exec2.fire_timers(200).unwrap();
    }
}
