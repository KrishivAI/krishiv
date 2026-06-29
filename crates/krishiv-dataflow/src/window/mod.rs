pub mod count;
pub mod session;
pub mod sliding;
pub mod state_persistence;
pub mod state_tumbling;
pub mod tumbling;

pub use count::{CountWindowOperator, CountWindowSpec};
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
        self.dropped
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

// ── WatermarkState ────────────────────────────────────────────────────────────

/// Per-operator monotonic watermark tracker for event-time streaming.
///
/// Watermark = max(event_time_seen) − lag_ms.  The watermark never decreases.
/// Events with `event_time_ms < current_watermark_ms()` are late and must be
/// dropped by the operator before calling `advance`.
///
/// The watermark is recomputed once per `advance` and cached, so
/// `current_watermark_ms()` is a field read rather than an `i128` subtract +
/// clamp per call. Per-batch window operators may call it thousands of times,
/// and the prior implementation did the arithmetic per call.
#[derive(Debug, Clone)]
pub struct WatermarkState {
    max_event_time_ms: i64,
    /// Cached result of `current_watermark_ms()`. `i64::MIN` means "no events
    /// observed yet". Updated on every `advance` and on the first non-`MIN`
    /// max so the public API stays O(1).
    cached_watermark_ms: i64,
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
            cached_watermark_ms: i64::MIN,
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
    /// the current maximum.  The cached watermark is recomputed only when the
    /// max moves, so a no-op advance costs a single `i64` comparison.
    pub fn advance(&mut self, event_time_ms: i64) {
        if event_time_ms > self.max_event_time_ms {
            self.max_event_time_ms = event_time_ms;
            self.recompute_cached_watermark();
        }
    }

    /// Current watermark in milliseconds. Returns `i64::MIN` until the first
    /// event has been observed. This is now an O(1) field read — the prior
    /// implementation did `i128` arithmetic per call, which added up over
    /// per-row `is_late()` checks inside `process_batch`.
    pub fn current_watermark_ms(&self) -> i64 {
        self.cached_watermark_ms
    }

    /// Recompute the cached watermark from the current `max_event_time_ms`.
    /// Factored out so `set_lag_ms`-style consumers (none today, but the
    /// invariant is "cache reflects max and lag at all times") can keep the
    /// invariant when the lag changes at runtime.
    fn recompute_cached_watermark(&mut self) {
        if self.max_event_time_ms == i64::MIN {
            self.cached_watermark_ms = i64::MIN;
        } else {
            let watermark = i128::from(self.max_event_time_ms) - i128::from(self.lag_ms);
            self.cached_watermark_ms =
                watermark.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64;
        }
    }

    /// Whether `event_time_ms` is strictly less than the current watermark
    /// (i.e. the event arrived late and must be dropped).
    pub fn is_late(&self, event_time_ms: i64) -> bool {
        event_time_ms < self.cached_watermark_ms
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
        self.last_update_ms
            .insert(source_id.to_owned(), elapsed_ms());
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
        let now = elapsed_ms();
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

    /// E3.6 — Per-source watermark lag relative to the most-advanced source.
    ///
    /// Returns a map of `source_id → lag_ms` where `lag_ms` is:
    /// `max_source_watermark_ms - source_watermark_ms`.
    ///
    /// A positive value means the source is behind the fastest source.
    /// A value of 0 means the source is at (or ahead of) all others.
    /// Sources with watermark `i64::MIN` (never emitted) are reported with
    /// `lag_ms = i64::MAX` to indicate they've not contributed any data yet.
    pub fn per_source_lag_ms(&self) -> HashMap<String, i64> {
        let max_wm = self
            .source_watermarks
            .values()
            .copied()
            .filter(|&w| w != i64::MIN)
            .max()
            .unwrap_or(i64::MIN);
        self.source_watermarks
            .iter()
            .map(|(id, &wm)| {
                let lag = if wm == i64::MIN {
                    i64::MAX
                } else {
                    max_wm.saturating_sub(wm).max(0)
                };
                (id.clone(), lag)
            })
            .collect()
    }

    /// E3.6 — Returns a snapshot of all registered source watermarks.
    pub fn source_watermarks(&self) -> HashMap<String, i64> {
        self.source_watermarks.clone()
    }
}

fn elapsed_ms() -> u64 {
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
    fn watermark_state_saturates_for_lag_larger_than_i64() {
        let mut w = WatermarkState::new(u64::MAX);
        w.advance(i64::MAX);

        assert_eq!(w.current_watermark_ms(), i64::MIN);
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
            key_column_type: "utf8".into(),
            event_time_column: "ts".into(),
            window_size_ms: 10_000,
            agg_exprs: vec![AggExpr {
                function: crate::AggFunction::Count,
                input_column: String::new(),
                output_column: "count".into(),
            }],
            agg_is_float: vec![false],
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
            key_column_type: "utf8".into(),
            event_time_column: "ts".into(),
            window_size_ms: 10_000,
            agg_exprs: vec![AggExpr {
                function: crate::AggFunction::Count,
                input_column: String::new(),
                output_column: "count".into(),
            }],
            agg_is_float: vec![false],
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

    // ── E3.6: per-source lag metric tests ─────────────────────────────────────

    #[test]
    fn per_source_lag_returns_zero_when_all_sources_equal() {
        let mut state = MultiSourceWatermarkState::new();
        state.update("a", 1_000);
        state.update("b", 1_000);
        let lag = state.per_source_lag_ms();
        assert_eq!(
            lag["a"], 0,
            "source at effective watermark should have lag 0"
        );
        assert_eq!(lag["b"], 0);
    }

    #[test]
    fn per_source_lag_reports_lagging_source() {
        let mut state = MultiSourceWatermarkState::new();
        state.update("fast", 5_000);
        state.update("slow", 2_000);
        // effective = min = 2000; lag = max - source (how far behind fastest)
        let lag = state.per_source_lag_ms();
        // Fast source is at the maximum watermark → 0 lag.
        assert_eq!(
            lag["fast"], 0,
            "fast source at max watermark should have 0 lag"
        );
        // Slow source lags behind the fastest by 3000 ms.
        assert_eq!(
            lag["slow"], 3_000,
            "slow source lags behind fastest by 3000 ms"
        );
    }

    #[test]
    fn per_source_lag_max_for_never_seen_source() {
        let mut state = MultiSourceWatermarkState::new();
        state.register_source("never_seen");
        state.update("active", 1_000);
        let lag = state.per_source_lag_ms();
        assert_eq!(
            lag["never_seen"],
            i64::MAX,
            "never-seen source should have i64::MAX lag"
        );
    }

    #[test]
    fn source_watermarks_snapshot_matches_internal_state() {
        let mut state = MultiSourceWatermarkState::new();
        state.update("a", 3_000);
        state.update("b", 7_000);
        let snap = state.source_watermarks();
        assert_eq!(snap.get("a"), Some(&3_000));
        assert_eq!(snap.get("b"), Some(&7_000));
    }
}

/// Property-based tests for `WatermarkState`, `MultiSourceWatermarkState`, and
/// `SlidingWindowOperator` correctness invariants.
#[cfg(test)]
mod window_props {
    use super::*;
    use crate::aggregate::{AggExpr, AggFunction};
    use crate::window::sliding::{SlidingWindowOperator, SlidingWindowSpec};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use proptest::prelude::*;
    use std::sync::Arc;

    // ── WatermarkState properties ────────────────────────────────────────────

    proptest! {
        /// After advancing `WatermarkState` with an arbitrary sequence of
        /// event times, `current_watermark_ms` must never decrease between
        /// consecutive calls (monotonicity invariant).
        #[test]
        fn watermark_state_is_monotonic(
            lag in 0u64..100_000u64,
            events in prop::collection::vec(0i64..10_000_000i64, 1..64usize),
        ) {
            let mut w = WatermarkState::new(lag);
            let mut prev = w.current_watermark_ms();
            for t in events {
                w.advance(t);
                let wm = w.current_watermark_ms();
                prop_assert!(
                    wm >= prev,
                    "watermark decreased: {} < {} after advance({})", wm, prev, t
                );
                prev = wm;
            }
        }

        /// After advancing with a known event time, the watermark should equal
        /// `event_time_ms - lag_ms` (clamped to i64 range).
        #[test]
        fn watermark_lag_invariant(
            t in 1_000_000i64..100_000_000i64,
            lag in 0u64..1_000_000u64,
        ) {
            let mut w = WatermarkState::new(lag);
            w.advance(t);
            let expected = (t as i128 - lag as i128)
                .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64;
            prop_assert_eq!(
                w.current_watermark_ms(), expected,
                "watermark should equal event_time - lag"
            );
        }

        /// `MultiSourceWatermarkState::effective_watermark_ms` must equal the
        /// minimum watermark across all registered sources.
        #[test]
        fn multi_source_effective_is_min_of_sources(
            wms in prop::collection::vec((1..16usize, 0i64..10_000_000i64), 1..8usize),
        ) {
            let mut state = MultiSourceWatermarkState::new();
            for (src_idx, wm) in &wms {
                let src = format!("src-{src_idx}");
                state.update(&src, *wm);
            }
            // Build expected min: for each source take the max wm they were updated with.
            let mut per_src_max: std::collections::HashMap<usize, i64> = Default::default();
            for (src_idx, wm) in &wms {
                let e = per_src_max.entry(*src_idx).or_insert(i64::MIN);
                if *wm > *e { *e = *wm; }
            }
            let expected_min = per_src_max.values().copied().min().unwrap_or(i64::MIN);
            prop_assert_eq!(
                state.effective_watermark_ms(),
                expected_min,
                "effective watermark must be the minimum across all sources"
            );
        }

        /// `MultiSourceWatermarkState` must not allow the watermark for a
        /// source to decrease — updates with lower values must be ignored.
        #[test]
        fn multi_source_watermark_is_monotonic_per_source(
            updates in prop::collection::vec(0i64..5_000_000i64, 2..32usize),
        ) {
            let mut state = MultiSourceWatermarkState::new();
            let mut peak = i64::MIN;
            for wm in updates {
                state.update("s", wm);
                if wm > peak { peak = wm; }
                let snap = state.source_watermarks();
                let recorded = *snap.get("s").unwrap_or(&i64::MIN);
                prop_assert_eq!(
                    recorded, peak,
                    "source watermark must not decrease: recorded {} != peak {}", recorded, peak
                );
            }
        }

        // ── SlidingWindowOperator properties ─────────────────────────────────

        /// Processing an arbitrary sequence of events into a sliding window
        /// must never panic.
        #[test]
        fn sliding_window_process_batch_never_panics(
            events in prop::collection::vec(0i64..10_000i64, 0..32usize),
        ) {
            let spec = SlidingWindowSpec {
                key_column: "k".into(),
                key_column_type: "utf8".into(),
                event_time_column: "ts".into(),
                window_size_ms: 1000,
                slide_ms: 250,
                agg_exprs: vec![AggExpr {
                    function: AggFunction::Count,
                    input_column: String::new(),
                    output_column: "cnt".into(),
                }],
                agg_is_float: vec![false],
            };
            let mut op = SlidingWindowOperator::new(spec).expect("valid spec");
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Utf8, false),
                Field::new("ts", DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(schema, vec![
                Arc::new(StringArray::from(vec!["k"; events.len()])) as _,
                Arc::new(Int64Array::from(events.clone())) as _,
            ]).unwrap();
            let wm = events.iter().max().copied().unwrap_or(0);
            let _ = op.process_batch(&batch, wm);
        }

        /// A single on-time event fed into a sliding window with size `S` and
        /// slide `D` must open exactly `ceil(S / D)` window buckets (fan-out).
        #[test]
        fn sliding_window_fan_out_factor(
            ts in 1_000_000i64..10_000_000i64,
            slide_shift in 0u64..4u64,
        ) {
            // size = 1000ms, slide = 125 * 2^shift (so slide divides size evenly)
            let slide_ms: u64 = 125 << slide_shift; // 125, 250, 500, 1000
            let size_ms: u64 = 1000;
            let expected_fan_out = (size_ms + slide_ms - 1) / slide_ms; // ceil(size/slide)

            let spec = SlidingWindowSpec {
                key_column: "k".into(),
                key_column_type: "utf8".into(),
                event_time_column: "ts".into(),
                window_size_ms: size_ms,
                slide_ms,
                agg_exprs: vec![AggExpr {
                    function: AggFunction::Count,
                    input_column: String::new(),
                    output_column: "cnt".into(),
                }],
                agg_is_float: vec![false],
            };
            let mut op = SlidingWindowOperator::new(spec).expect("valid spec");
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Utf8, false),
                Field::new("ts", DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(schema, vec![
                Arc::new(StringArray::from(vec!["k"])) as _,
                Arc::new(Int64Array::from(vec![ts])) as _,
            ]).unwrap();
            // Watermark = ts: doesn't close any window (all window ends > ts).
            let _ = op.process_batch(&batch, ts).expect("process");
            prop_assert_eq!(
                op.open_window_count(), expected_fan_out as usize,
                "sliding fan-out: expected {} open buckets for size={} slide={} ts={}",
                expected_fan_out, size_ms, slide_ms, ts
            );
        }
    }
}
