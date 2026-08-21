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

    /// Serialize the whole pipeline's state — the join operator's buffered
    /// events plus every stage's window state — as one snapshot (task #149
    /// fix 4: pipelines registered with checkpointing previously wrote
    /// nothing recoverable; a restore came back with empty join and stage
    /// state).
    ///
    /// # Errors
    /// Propagates join and stage serialization errors.
    pub fn snapshot_bytes(&mut self) -> ExecResult<Vec<u8>> {
        #[derive(serde::Serialize)]
        struct PipelineSnapshot {
            join_b64: String,
            stage_b64: Vec<String>,
        }
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::STANDARD;
        let join_b64 = b64.encode(self.join.snapshot_bytes().map_err(|e| {
            crate::ExecError::InvalidWindowConfig(format!("pipeline join snapshot: {e}"))
        })?);
        let mut stage_b64 = Vec::with_capacity(self.stages.len());
        for stage in &mut self.stages {
            stage_b64.push(b64.encode(stage.snapshot()?));
        }
        serde_json::to_vec(&PipelineSnapshot {
            join_b64,
            stage_b64,
        })
        .map_err(|e| {
            crate::ExecError::InvalidWindowConfig(format!("pipeline snapshot encode: {e}"))
        })
    }

    /// Restore state from a snapshot produced by [`Self::snapshot_bytes`].
    ///
    /// The pipeline must have been built from the SAME spec: the snapshot
    /// carries one entry per stage and restoring against a different stage
    /// count is refused rather than silently mis-assigned.
    ///
    /// # Errors
    /// Propagates decode and per-component restore errors.
    pub fn restore_from_snapshot(&mut self, bytes: &[u8]) -> ExecResult<()> {
        #[derive(serde::Deserialize)]
        struct PipelineSnapshot {
            join_b64: String,
            stage_b64: Vec<String>,
        }
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::STANDARD;
        let snap: PipelineSnapshot = serde_json::from_slice(bytes).map_err(|e| {
            crate::ExecError::InvalidWindowConfig(format!("pipeline snapshot decode: {e}"))
        })?;
        if snap.stage_b64.len() != self.stages.len() {
            return Err(crate::ExecError::InvalidWindowConfig(format!(
                "pipeline snapshot has {} stage entries but this pipeline has {} stages; \
                 restore refused rather than mis-assigning state",
                snap.stage_b64.len(),
                self.stages.len()
            )));
        }
        let join_bytes = b64.decode(&snap.join_b64).map_err(|e| {
            crate::ExecError::InvalidWindowConfig(format!("pipeline join snapshot b64: {e}"))
        })?;
        self.join = WatermarkWindowJoinOperator::restore_from_bytes(&join_bytes).map_err(|e| {
            crate::ExecError::InvalidWindowConfig(format!("pipeline join restore: {e}"))
        })?;
        for (stage, encoded) in self.stages.iter_mut().zip(&snap.stage_b64) {
            let bytes = b64.decode(encoded).map_err(|e| {
                crate::ExecError::InvalidWindowConfig(format!("pipeline stage snapshot b64: {e}"))
            })?;
            stage.restore_from_snapshot(&bytes)?;
        }
        Ok(())
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

#[cfg(test)]
mod snapshot_roundtrip {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn spec() -> krishiv_plan::stream_join::StreamingPipelineSpec {
        let mut stage =
            krishiv_plan::window::WindowExecutionSpec::tumbling("left_k", "left_ts", 60_000);
        stage.watermark_lag_ms = 120_000; // 2x the join band, per the validator
        krishiv_plan::stream_join::StreamingPipelineSpec {
            join: krishiv_plan::stream_join::StreamingJoinSpec {
                left_source: "l".into(),
                right_source: "r".into(),
                time_column: "ts".into(),
                left_key_column: "k".into(),
                right_key_column: "k".into(),
                window_ms: 60_000,
            },
            stages: vec![stage],
        }
    }

    fn side(keys: &[&str], ts: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(Int64Array::from(ts.to_vec())),
            ],
        )
        .expect("batch")
    }

    /// A checkpointed pipeline must come back with its join buffers AND stage
    /// window state (task #149 fix 4 — before snapshot/restore existed, a
    /// restored pipeline was empty and every accumulated window was lost).
    /// Key "a" exists only pre-snapshot; key "b" only post-restore. Flush
    /// emitting both proves the restore; one row means it was lost.
    #[test]
    fn snapshot_restores_join_and_stage_state() {
        let mut original = JoinAggPipeline::new(&spec()).expect("build");
        original.on_left(&side(&["a"], &[1_000])).expect("left");
        original.on_right(&side(&["a"], &[2_000])).expect("right");
        let snapshot = original.snapshot_bytes().expect("snapshot");

        let mut restored = JoinAggPipeline::new(&spec()).expect("rebuild");
        restored.restore_from_snapshot(&snapshot).expect("restore");
        // The stage operator initializes lazily from its first batch, so the
        // restored window state is applied when post-restore traffic arrives
        // — exactly the production shape.
        restored.on_left(&side(&["b"], &[3_000])).expect("left b");
        restored.on_right(&side(&["b"], &[3_000])).expect("right b");
        let flushed = restored.flush_all().expect("flush");
        let rows: usize = flushed.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 2,
            "flush must emit BOTH the pre-snapshot key (restored state) and \
             the post-restore key — one row means the restore was lost"
        );

        // Stage-count drift is refused, not mis-assigned.
        let mut two_stage_spec = spec();
        two_stage_spec.stages.push(two_stage_spec.stages[0].clone());
        let mut mismatched = JoinAggPipeline::new(&two_stage_spec).expect("build 2-stage");
        let err = mismatched
            .restore_from_snapshot(&snapshot)
            .expect_err("stage-count drift must be refused");
        assert!(err.to_string().contains("restore refused"));
    }
}
