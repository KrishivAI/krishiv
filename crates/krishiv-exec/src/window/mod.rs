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
}

impl WatermarkState {
    /// Create a watermark tracker with the given allowed lateness in milliseconds.
    pub fn new(lag_ms: u64) -> Self {
        Self {
            max_event_time_ms: i64::MIN,
            lag_ms,
        }
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
        }
    }

    /// Advance idle sources to `idle_watermark_ms` after `timeout_ms` without updates (ADR-DIST-17).
    pub fn with_idle_source_policy(mut self, timeout_ms: u64, idle_watermark_ms: i64) -> Self {
        self.idle_timeout_ms = Some(timeout_ms);
        self.idle_watermark_ms = idle_watermark_ms;
        self
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
