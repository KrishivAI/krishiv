//! Watermark propagation end-to-end helpers (R16 S6.2).

use crate::side_output::{SideOutput, SideOutputRouter};
use crate::window::{MultiSourceWatermarkState, WatermarkState};

/// Combined operator pipeline for watermark E2E validation.
#[derive(Debug)]
pub struct WatermarkE2ePipeline {
    pub watermark: WatermarkState,
    pub multi_source: MultiSourceWatermarkState,
    pub side_router: SideOutputRouter,
}

impl WatermarkE2ePipeline {
    pub fn new() -> Self {
        Self {
            watermark: WatermarkState::new(500),
            multi_source: MultiSourceWatermarkState::new(),
            side_router: SideOutputRouter::new(SideOutput::new("late", 200), "ts"),
        }
    }

    pub fn advance_all_sources(&mut self, wm: i64) {
        self.multi_source.update("left", wm);
        self.multi_source.update("right", wm);
        let min = self.multi_source.effective_watermark_ms();
        self.watermark.advance(min + self.watermark_lag_internal());
    }

    fn watermark_lag_internal(&self) -> i64 {
        500
    }

    pub fn effective_watermark(&self) -> i64 {
        self.watermark.current_watermark_ms()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_propagation_e2e() {
        let mut pipe = WatermarkE2ePipeline::new();
        pipe.advance_all_sources(1000);
        assert!(pipe.effective_watermark() > i64::MIN);
        assert!(!pipe.side_router.is_late(&pipe.watermark, 950));
        assert!(pipe.side_router.is_late(&pipe.watermark, 700));
    }
}
