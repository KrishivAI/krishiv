//! Late-data side output routing (R16 S3.3).

use crate::window::WatermarkState;

/// Named side output for late records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SideOutput {
    pub name: String,
    pub lateness_threshold_ms: u64,
}

impl SideOutput {
    pub fn new(name: impl Into<String>, lateness_threshold_ms: u64) -> Self {
        Self {
            name: name.into(),
            lateness_threshold_ms,
        }
    }
}

/// Routes batches to main or side output based on event-time vs watermark.
#[derive(Debug, Clone)]
pub struct SideOutputRouter {
    pub spec: SideOutput,
    pub event_time_column: String,
}

impl SideOutputRouter {
    pub fn new(spec: SideOutput, event_time_column: impl Into<String>) -> Self {
        Self {
            spec,
            event_time_column: event_time_column.into(),
        }
    }

    /// Classify `event_time_ms` relative to current watermark.
    pub fn is_late(&self, watermark: &WatermarkState, event_time_ms: i64) -> bool {
        let threshold = self.spec.lateness_threshold_ms as i64;
        event_time_ms < watermark.current_watermark_ms().saturating_sub(threshold)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_record_detected_beyond_threshold() {
        let wm = WatermarkState::new(1000);
        let mut state = wm;
        state.advance(10_000);
        let router = SideOutputRouter::new(SideOutput::new("late", 500), "ts");
        assert!(router.is_late(&state, 8_000));
        assert!(!router.is_late(&state, 9_500));
    }

    #[test]
    fn zero_threshold_exact_watermark_boundary() {
        let mut state = WatermarkState::new(0);
        state.advance(1000);
        let router = SideOutputRouter::new(SideOutput::new("late", 0), "ts");
        // event_time < watermark → late
        assert!(router.is_late(&state, 999));
        // event_time == watermark → not late (strict less-than)
        assert!(!router.is_late(&state, 1000));
        // event_time > watermark → not late
        assert!(!router.is_late(&state, 1001));
    }

    #[test]
    fn not_late_before_watermark_advanced() {
        let mut state = WatermarkState::new(0);
        state.advance(500);
        let router = SideOutputRouter::new(SideOutput::new("late", 200), "ts");
        // watermark=500, threshold=200 → effective threshold=300
        // event at 400: 400 < 300? No → not late
        assert!(!router.is_late(&state, 400));
        // event at 200: 200 < 300? Yes → late
        assert!(router.is_late(&state, 200));
    }

    #[test]
    fn high_threshold_prevents_lateness() {
        let mut state = WatermarkState::new(0);
        state.advance(10_000);
        // Very high threshold: 20000
        let router = SideOutputRouter::new(SideOutput::new("late", 20_000), "ts");
        // watermark=10000, threshold=20000 → effective = 10000 - 20000 underflow = 0 (saturating_sub)
        assert!(!router.is_late(&state, 0));
    }

    #[test]
    fn side_output_fields_correctly_set() {
        let so = SideOutput::new("dlq", 5000);
        assert_eq!(so.name, "dlq");
        assert_eq!(so.lateness_threshold_ms, 5000);
    }

    #[test]
    fn router_stores_event_time_column() {
        let router = SideOutputRouter::new(SideOutput::new("late", 100), "event_ts");
        assert_eq!(router.event_time_column, "event_ts");
    }

    #[test]
    fn router_debug_trait() {
        let router = SideOutputRouter::new(SideOutput::new("late", 100), "ts");
        let dbg = format!("{:?}", router);
        assert!(dbg.contains("SideOutputRouter"));
        assert!(dbg.contains("late"));
        assert!(dbg.contains("ts"));
    }
}
