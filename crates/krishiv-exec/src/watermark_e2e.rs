//! Watermark propagation end-to-end helpers (R16 S6.2).

use crate::cep::CepOperator;
use crate::interval_join::{IntervalJoinSpec, PerKeyIntervalJoin};
use crate::side_output::{SideOutput, SideOutputRouter};
use crate::window::{MultiSourceWatermarkState, WatermarkState};
use krishiv_cep::{CompiledPattern, PatternStage};

/// Combined operator pipeline for watermark E2E validation.
#[derive(Debug)]
pub struct WatermarkE2ePipeline {
    pub watermark: WatermarkState,
    pub multi_source: MultiSourceWatermarkState,
    pub side_router: SideOutputRouter,
    pub interval: PerKeyIntervalJoin,
    pub cep: CepOperator,
}

impl WatermarkE2ePipeline {
    pub fn new() -> Self {
        let pattern = CompiledPattern {
            stages: vec![
                PatternStage {
                    name: "a".into(),
                    max_gap_ms: None,
                },
                PatternStage {
                    name: "b".into(),
                    max_gap_ms: None,
                },
            ],
            window_ms: 60_000,
        };
        Self {
            watermark: WatermarkState::new(500),
            multi_source: MultiSourceWatermarkState::new(),
            side_router: SideOutputRouter::new(SideOutput::new("late", 200), "ts"),
            interval: PerKeyIntervalJoin::new(IntervalJoinSpec {
                lower_bound_ms: -100,
                upper_bound_ms: 100,
                key_column: "k".into(),
                max_buffer_per_side: 1000,
            }),
            cep: CepOperator::new(pattern, "k"),
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
    use crate::barrier_align::BarrierAligner;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::time::Duration;

    fn batch(v: i32) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![v]))],
        )
        .unwrap()
    }

    #[test]
    fn watermark_propagation_e2e() {
        let mut pipe = WatermarkE2ePipeline::new();
        pipe.advance_all_sources(1000);
        assert!(pipe.effective_watermark() > i64::MIN);
        assert!(!pipe.side_router.is_late(&pipe.watermark, 950));
        assert!(pipe.side_router.is_late(&pipe.watermark, 700));
        let mut aligner = BarrierAligner::new(2, Duration::from_secs(1)).unwrap();
        aligner.buffer_data(0, batch(1));
        assert!(!aligner.on_barrier(0, 1).unwrap());
        assert!(aligner.on_barrier(1, 1).unwrap());
    }
}
