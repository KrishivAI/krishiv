//! Join → windowed-stage pipeline runner (task #146, NEXMark Q4/Q9).
//!
//! The interval join and the windowed aggregate both existed; this is the
//! pipe. Matches stream out of the join as they arrive and flow through the
//! stage chain immediately; at end of stream the stages flush in CASCADE —
//! stage i's flushed windows are fed through stage i+1 before stage i+1
//! flushes, so a two-stage Q4 (winning bid per auction, then average per
//! category) sees every winning bid before its average closes.
//!
//! Column naming, stated rather than hidden: the join emits ALL left+right
//! columns concatenated (it does not apply the CTE's projection), and a name
//! present on BOTH sides is prefixed `left_`/`right_` — so a stage windowing
//! on the bid stream's event time reads `left_dateTime`, never a bare
//! `dateTime` that would be silently ambiguous.

use arrow::record_batch::RecordBatch;
use krishiv_plan::stream_join::StreamingPipelineSpec;

use crate::continuous::ContinuousWindowExecutor;
use crate::watermark_join::{WatermarkWindowJoinOperator, WatermarkWindowJoinSpec};
use crate::{ExecError, ExecResult};

/// A banded join feeding a chain of windowed executors.
pub struct JoinAggPipeline {
    join: WatermarkWindowJoinOperator,
    stages: Vec<ContinuousWindowExecutor>,
}

impl JoinAggPipeline {
    /// # Errors
    /// When the spec is invalid or a stage executor cannot be built.
    pub fn new(spec: &StreamingPipelineSpec) -> ExecResult<Self> {
        spec.validate()
            .map_err(|e| ExecError::InvalidWindowConfig(e.to_string()))?;
        let join = WatermarkWindowJoinOperator::new(WatermarkWindowJoinSpec::from(&spec.join));
        let stages = spec
            .stages
            .iter()
            .map(|s| ContinuousWindowExecutor::new(s.clone()))
            .collect::<ExecResult<Vec<_>>>()?;
        Ok(Self { join, stages })
    }

    fn run_stages(&mut self, batches: Vec<RecordBatch>) -> ExecResult<Vec<RecordBatch>> {
        let mut current = batches;
        for stage in &mut self.stages {
            if current.is_empty() {
                return Ok(Vec::new());
            }
            current = Self::coerce_for(stage, current)?;
            current = stage.drain(current)?;
        }
        Ok(current)
    }

    /// The typing step a `StreamDriver` would perform (`InputTyping::
    /// CoerceToSpec`). The pipeline drives stage executors directly, and
    /// skipping this is exactly the defect the NEXMark harness once had at
    /// the top level: joined batches carry realistic source types (UInt64
    /// prices) that the aggregate pre-downcast refuses uncoerced.
    fn coerce_for(
        stage: &ContinuousWindowExecutor,
        batches: Vec<RecordBatch>,
    ) -> ExecResult<Vec<RecordBatch>> {
        batches
            .into_iter()
            .map(|b| crate::stream_driver::coerce_batch_for_window(&b, stage.spec()))
            .collect()
    }

    /// Feed one LEFT batch; returns whatever the FINAL stage emitted.
    ///
    /// # Errors
    /// Propagates join and stage errors.
    pub fn on_left(&mut self, batch: &RecordBatch) -> ExecResult<Vec<RecordBatch>> {
        let joined = self.join.process_left(batch)?;
        self.run_stages(joined)
    }

    /// Feed one RIGHT batch; returns whatever the FINAL stage emitted.
    ///
    /// # Errors
    /// Propagates join and stage errors.
    pub fn on_right(&mut self, batch: &RecordBatch) -> ExecResult<Vec<RecordBatch>> {
        let joined = self.join.process_right(batch)?;
        self.run_stages(joined)
    }

    /// Advance the JOIN's watermark (evicting unreachable band state). Stage
    /// watermarks advance from their own input event times.
    pub fn advance_watermark(&mut self, watermark_ms: i64) {
        self.join.advance_watermark(watermark_ms);
    }

    /// End of stream: flush every stage in cascade order.
    ///
    /// # Errors
    /// Propagates stage errors.
    pub fn flush_all(&mut self) -> ExecResult<Vec<RecordBatch>> {
        let mut carried: Vec<RecordBatch> = Vec::new();
        for i in 0..self.stages.len() {
            let fed = if carried.is_empty() {
                Vec::new()
            } else {
                let input = std::mem::take(&mut carried);
                match self.stages.get_mut(i) {
                    Some(s) => {
                        let input = Self::coerce_for(s, input)?;
                        s.drain(input)?
                    }
                    None => Vec::new(),
                }
            };
            let flushed = self
                .stages
                .get_mut(i)
                .map(ContinuousWindowExecutor::flush_all)
                .transpose()?
                .unwrap_or_default();
            carried = fed;
            carried.extend(flushed);
        }
        Ok(carried)
    }
}
