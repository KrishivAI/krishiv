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
        event_time_ms < watermark.current_watermark_ms() - threshold
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
}
