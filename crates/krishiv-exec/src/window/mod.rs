pub mod session;
pub mod sliding;
pub mod state_persistence;
pub mod state_tumbling;
pub mod tumbling;

pub use session::{SessionWindowOperator, SessionWindowSpec};
pub use sliding::{SlidingWindowOperator, SlidingWindowSpec};
pub use state_tumbling::{
    StateBackedSessionWindowOperator, StateBackedSlidingWindowOperator,
    StateBackedTumblingWindowOperator,
};
pub use tumbling::{TumblingWindowOperator, TumblingWindowSpec};

use std::collections::HashMap;

/// Handler for late events that arrive after the watermark has passed.
///
/// Window operators call this for each event whose event-time is below the
/// current watermark, instead of silently dropping them. Implementations can
/// route late events to a side-output, dead-letter queue, or metrics system.
pub trait LateEventHandler: Send + Sync {
    /// Called for each late event. `key` is the serialised join/group key,
    /// `event_time_ms` is the event timestamp, and `batch` is the full
    /// batch containing the late row at index `row_idx`.
    fn on_late_event(&self, key: &str, event_time_ms: i64, row_idx: usize);
}

/// Default no-op handler that only counts late events.
#[derive(Debug, Default)]
pub struct CountingLateEventHandler {
    pub dropped: std::sync::atomic::AtomicU64,
}

impl Clone for CountingLateEventHandler {
    fn clone(&self) -> Self {
        Self {
            dropped: std::sync::atomic::AtomicU64::new(
                self.dropped.load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }
}

impl CountingLateEventHandler {
    pub fn new() -> Self {
        Self {
            dropped: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn count(&self) -> u64 {
        self.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl LateEventHandler for CountingLateEventHandler {
    fn on_late_event(&self, _key: &str, _event_time_ms: i64, _row_idx: usize) {
        self.dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

// ── WatermarkState ────────────────────────────────────────────────────────────

/// Per-operator monotonic watermark tracker for event-time streaming.
///
/// Watermark = max(event_time_seen) − lag_ms.  The watermark never decreases.
/// Events with `event_time_ms < current_watermark_ms()` are late and must be
/// dropped by the operator before calling `advance`.
#[derive(Debug, Clone)]
pub struct WatermarkState {
    max_event_time_ms: i64,
    lag_ms: u64,
    /// Cumulative count of events dropped because their event time was strictly
    /// below the current watermark. Exposed for observability; callers should
    /// surface this in metrics so operators can detect misconfigured lag windows.
    pub late_events_dropped: u64,
}

impl WatermarkState {
    /// Create a watermark tracker with the given allowed lateness in milliseconds.
    pub fn new(lag_ms: u64) -> Self {
        Self {
            max_event_time_ms: i64::MIN,
            lag_ms,
            late_events_dropped: 0,
        }
    }

    /// Record a single late-event drop. Callers MUST call this whenever they
    /// skip a row due to `is_late()` returning true.
    pub fn record_late_drop(&mut self) {
        self.late_events_dropped = self.late_events_dropped.saturating_add(1);
    }

    /// Advance the high-water mark to `event_time_ms` if it is greater than
    /// the current maximum.  The watermark is recalculated after each advance.
    pub fn advance(&mut self, event_time_ms: i64) {
        if event_time_ms > self.max_event_time_ms {
            self.max_event_time_ms = event_time_ms;
        }
    }

    /// Current watermark in milliseconds.  Returns `i64::MIN` until the first
    /// event has been observed.
    pub fn current_watermark_ms(&self) -> i64 {
        if self.max_event_time_ms == i64::MIN {
            i64::MIN
        } else {
            self.max_event_time_ms.saturating_sub(self.lag_ms as i64)
        }
    }

    /// Whether `event_time_ms` is strictly less than the current watermark
    /// (i.e. the event arrived late and must be dropped).
    pub fn is_late(&self, event_time_ms: i64) -> bool {
        event_time_ms < self.current_watermark_ms()
    }
}

// ── MultiSourceWatermarkState ─────────────────────────────────────────────────

/// Tracks watermarks for multiple input sources (R5.2).
///
/// The effective watermark is `min(watermark_source_0, watermark_source_1, …)`.
/// A window is only closed when the effective watermark passes the window end,
/// so a stalled source holds back all windows.
#[derive(Debug, Clone)]
pub struct MultiSourceWatermarkState {
    source_watermarks: HashMap<String, i64>,
    last_update_ms: HashMap<String, u64>,
    idle_timeout_ms: Option<u64>,
    idle_watermark_ms: i64,
    /// Wall-clock instant of the last effective-watermark advance.
    /// `None` until at least one source emits its first event.
    last_advance_instant: Option<std::time::Instant>,
    /// Effective watermark at the time of the last advance, used to detect
    /// whether the watermark actually moved on the next `update()` call.
    prev_effective_watermark_ms: i64,
}

impl Default for MultiSourceWatermarkState {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiSourceWatermarkState {
    /// Create an empty multi-source watermark tracker.
    pub fn new() -> Self {
        Self {
            source_watermarks: HashMap::new(),
            last_update_ms: HashMap::new(),
            idle_timeout_ms: None,
            idle_watermark_ms: i64::MAX,
            last_advance_instant: None,
            prev_effective_watermark_ms: i64::MIN,
        }
    }

    /// Advance idle sources to `idle_watermark_ms` after `timeout_ms` without updates (ADR-DIST-17).
    pub fn with_idle_source_policy(mut self, timeout_ms: u64, idle_watermark_ms: i64) -> Self {
        self.idle_timeout_ms = Some(timeout_ms);
        self.idle_watermark_ms = idle_watermark_ms;
        self
    }

    /// Register an expected source without marking it as having produced data.
    ///
    /// Configured sources participate in the effective watermark immediately
    /// with `i64::MIN`, so a source that has never emitted holds back window
    /// closure until it produces data. This differs from [`update`], which also
    /// records a last-update timestamp and is reserved for real source events.
    pub fn register_source(&mut self, source_id: impl Into<String>) {
        self.source_watermarks
            .entry(source_id.into())
            .or_insert(i64::MIN);
    }

    /// Update the watermark for `source_id` (monotonic — decreasing values are ignored).
    pub fn update(&mut self, source_id: &str, watermark_ms: i64) {
        let entry = self
            .source_watermarks
            .entry(source_id.to_owned())
            .or_insert(i64::MIN);
        if watermark_ms > *entry {
            *entry = watermark_ms;
        }
        self.last_update_ms.insert(source_id.to_owned(), wall_ms());
        // Track whether the *effective* watermark (minimum across all sources) advanced.
        let effective = self.effective_watermark_ms();
        if effective > self.prev_effective_watermark_ms {
            self.prev_effective_watermark_ms = effective;
            self.last_advance_instant = Some(std::time::Instant::now());
        }
    }

    /// Wall-clock duration since the effective watermark last advanced.
    ///
    /// Returns `None` if no source has emitted any event yet (the watermark
    /// has never moved from `i64::MIN`). A large duration indicates a stalled
    /// source that is holding back all downstream windows.
    pub fn stall_duration(&self) -> Option<std::time::Duration> {
        self.last_advance_instant.map(|t| t.elapsed())
    }

    /// Returns `true` when the effective watermark has not advanced for longer
    /// than `threshold`. Use this to detect stalled sources and emit
    /// observability signals.
    ///
    /// Returns `false` if no events have been observed yet — a source that has
    /// never emitted is idle, not stalled.
    pub fn is_stalled(&self, threshold: std::time::Duration) -> bool {
        match self.last_advance_instant {
            None => false,
            Some(t) => t.elapsed() > threshold,
        }
    }

    /// Apply idle-source policy using current wall clock.
    ///
    /// GAP-14: Only advance the watermark for sources that have already seen at
    /// least one real event (i.e. whose current watermark is not `i64::MIN`).
    /// Advancing the watermark to `idle_watermark_ms` for a source that has
    /// never emitted any events would allow windows to close before data arrives,
    /// silently producing empty window output.
    pub fn apply_idle_source_policy(&mut self) {
        let Some(timeout_ms) = self.idle_timeout_ms else {
            return;
        };
        let now = wall_ms();
        for source_id in self.last_update_ms.keys().cloned().collect::<Vec<_>>() {
            let Some(&last) = self.last_update_ms.get(&source_id) else {
                continue;
            };
            if now.saturating_sub(last) >= timeout_ms {
                let entry = self.source_watermarks.entry(source_id).or_insert(i64::MIN);
                // Guard: only advance watermark if the source has seen events.
                // A watermark of i64::MIN means no real event has ever been
                // observed from this source; advancing it here would cause
                // downstream windows to close without data.
                if *entry != i64::MIN && self.idle_watermark_ms > *entry {
                    *entry = self.idle_watermark_ms;
                }
            }
        }
    }

    /// Effective watermark across all registered sources.  Returns `i64::MIN`
    /// if no source has reported a watermark yet.
    pub fn effective_watermark_ms(&self) -> i64 {
        self.source_watermarks
            .values()
            .copied()
            .min()
            .unwrap_or(i64::MIN)
    }

    /// Number of sources registered.
    pub fn source_count(&self) -> usize {
        self.source_watermarks.len()
    }
}

fn wall_ms() -> u64 {
    static BASE: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    BASE.get_or_init(std::time::Instant::now)
        .elapsed()
        .as_millis() as u64
}

#[cfg(test)]
mod watermark_tests {
    use super::*;

    #[test]
    fn watermark_state_returns_min_before_any_event() {
        let w = WatermarkState::new(1_000);
        assert_eq!(
            w.current_watermark_ms(),
            i64::MIN,
            "no events → watermark must remain i64::MIN"
        );
    }

    #[test]
    fn watermark_state_advances_after_event() {
        let mut w = WatermarkState::new(1_000);
        w.advance(5_000);
        assert_eq!(w.current_watermark_ms(), 4_000);
    }

    #[test]
    fn multi_source_register_source_participates_without_marking_event_seen() {
        let mut state = MultiSourceWatermarkState::new().with_idle_source_policy(0, i64::MAX);
        state.register_source("source-a");
        state.update("source-b", 10_000);

        assert_eq!(state.source_count(), 2);
        assert_eq!(
            state.effective_watermark_ms(),
            i64::MIN,
            "registered source-a has not emitted and must hold back the effective watermark"
        );

        state.apply_idle_source_policy();
        assert_eq!(
            state.effective_watermark_ms(),
            i64::MIN,
            "idle policy must not advance a registered source that has never emitted"
        );
    }

    /// GAP-14: idle-source policy must NOT advance a watermark that is still
    /// i64::MIN (source never emitted events).  Advancing it would close windows
    /// before any data arrives, producing silent data loss.
    #[test]
    fn idle_source_policy_does_not_advance_watermark_for_never_seen_source() {
        let mut state = MultiSourceWatermarkState::new().with_idle_source_policy(0, 99_999_999);

        // Register source-a with one real event so it appears in last_update_ms.
        // Intentionally set last_update_ms to 0 so the idle timeout always fires.
        state.update("source-a", i64::MIN);

        // Call apply_idle_source_policy — the source has seen no real events
        // (watermark == i64::MIN) so the policy must leave it untouched.
        state.apply_idle_source_policy();

        assert_eq!(
            state.effective_watermark_ms(),
            i64::MIN,
            "idle policy must not advance a never-seen source's watermark"
        );
    }

    // ── Watermark stall signal ────────────────────────────────────────────────

    #[test]
    fn stall_duration_returns_none_before_any_events() {
        let state = MultiSourceWatermarkState::new();
        assert!(
            state.stall_duration().is_none(),
            "no events yet — stall_duration must be None"
        );
        assert!(
            !state.is_stalled(std::time::Duration::from_secs(1)),
            "is_stalled must be false before any events"
        );
    }

    #[test]
    fn stall_duration_is_some_after_first_event() {
        let mut state = MultiSourceWatermarkState::new();
        state.update("s0", 1_000);
        assert!(
            state.stall_duration().is_some(),
            "stall_duration must be Some after first event"
        );
    }

    #[test]
    fn is_stalled_false_right_after_advance() {
        let mut state = MultiSourceWatermarkState::new();
        state.update("s0", 5_000);
        assert!(
            !state.is_stalled(std::time::Duration::from_secs(60)),
            "watermark just advanced — is_stalled must be false"
        );
    }

    #[test]
    fn is_stalled_true_after_zero_threshold() {
        let mut state = MultiSourceWatermarkState::new();
        state.update("s0", 1_000);
        // A threshold of zero is always exceeded once any event has been seen.
        assert!(
            state.is_stalled(std::time::Duration::ZERO),
            "zero threshold must be exceeded immediately after first event"
        );
    }

    #[test]
    fn stall_duration_resets_on_watermark_advance() {
        let mut state = MultiSourceWatermarkState::new();
        state.update("s0", 1_000);
        let d1 = state.stall_duration().unwrap();
        state.update("s0", 2_000);
        let d2 = state.stall_duration().unwrap();
        assert!(
            d2 <= d1 + std::time::Duration::from_millis(50),
            "stall duration should reset on watermark advance"
        );
    }

    // ── Late-event drop counter ───────────────────────────────────────────────

    #[test]
    fn late_event_drop_counter_increments_on_late_events() {
        use super::tumbling::{TumblingWindowOperator, TumblingWindowSpec};
        use crate::AggExpr;
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let spec = TumblingWindowSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            window_size_ms: 10_000,
            agg_exprs: vec![AggExpr {
                function: crate::AggFunction::Count,
                input_column: String::new(),
                output_column: "count".into(),
            }],
        };
        let mut op = TumblingWindowOperator::new(spec);

        // First batch advances watermark to 5000.
        let b1 = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![5_000_i64])) as _,
            ],
        )
        .unwrap();
        let _ = op.process_batch(&b1, 5_000);
        assert_eq!(op.late_events_dropped, 0, "no late events yet");

        // Second batch has an event at t=1000 which is below watermark=5000.
        let b2 = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![1_000_i64])) as _,
            ],
        )
        .unwrap();
        let _ = op.process_batch(&b2, 5_000);
        assert_eq!(op.late_events_dropped, 1, "one late event must be counted");
    }

    #[test]
    fn on_time_events_do_not_increment_drop_counter() {
        use super::tumbling::{TumblingWindowOperator, TumblingWindowSpec};
        use crate::AggExpr;
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let spec = TumblingWindowSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            window_size_ms: 10_000,
            agg_exprs: vec![AggExpr {
                function: crate::AggFunction::Count,
                input_column: String::new(),
                output_column: "count".into(),
            }],
        };
        let mut op = TumblingWindowOperator::new(spec);
        let b = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])) as _,
                Arc::new(Int64Array::from(vec![1_000_i64, 2_000, 3_000])) as _,
            ],
        )
        .unwrap();
        let _ = op.process_batch(&b, 3_000);
        assert_eq!(op.late_events_dropped, 0, "no events should be late");
    }

    /// GAP-14 (positive case): a source that HAS seen events should be advanced
    /// by the idle policy when the timeout expires.
    #[test]
    fn idle_source_policy_advances_watermark_for_idle_source_with_events() {
        let idle_wm = 50_000i64;
        let mut state = MultiSourceWatermarkState::new().with_idle_source_policy(0, idle_wm);

        // Register source-a with a real event at t=1000; watermark = 1000.
        state.update("source-a", 1_000);

        // The idle timeout is 0 ms, so the policy fires immediately.
        state.apply_idle_source_policy();

        assert_eq!(
            state.effective_watermark_ms(),
            idle_wm,
            "idle policy must advance watermark for a source that has seen events"
        );
    }
}
