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
        let stages = self.stages.len();
        self.run_stage_range(batches, 0, stages)
    }

    fn run_stage_range(
        &mut self,
        batches: Vec<RecordBatch>,
        from: usize,
        to: usize,
    ) -> ExecResult<Vec<RecordBatch>> {
        let mut current = batches;
        for stage in self.stages.iter_mut().take(to).skip(from) {
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

    /// Split-phase LEFT input (parallel pipelines with a re-key point): run
    /// the join and the join-co-located stages `[..split]`, and return the
    /// rows destined for the exchange — NOT fed to the later stages, whose
    /// key the caller must first re-route by (`StreamingPipelineSpec::
    /// parallel_plan`).
    ///
    /// # Errors
    /// Propagates join and stage errors.
    pub fn on_left_pre_split(
        &mut self,
        batch: &RecordBatch,
        split: usize,
    ) -> ExecResult<Vec<RecordBatch>> {
        let joined = self.join.process_left(batch)?;
        self.run_stage_range(joined, 0, split)
    }

    /// Split-phase RIGHT input; see [`Self::on_left_pre_split`].
    ///
    /// # Errors
    /// Propagates join and stage errors.
    pub fn on_right_pre_split(
        &mut self,
        batch: &RecordBatch,
        split: usize,
    ) -> ExecResult<Vec<RecordBatch>> {
        let joined = self.join.process_right(batch)?;
        self.run_stage_range(joined, 0, split)
    }

    /// Split-phase stage input: rows the exchange routed to THIS subtask,
    /// fed through the stages from the re-key point on.
    ///
    /// # Errors
    /// Propagates stage errors.
    pub fn on_stage_input(
        &mut self,
        batches: Vec<RecordBatch>,
        split: usize,
    ) -> ExecResult<Vec<RecordBatch>> {
        let stages = self.stages.len();
        self.run_stage_range(batches, split, stages)
    }

    /// End of stream, phase one of the split flush: flush the co-located
    /// stages `[..split]` in cascade and return their output for the
    /// EXCHANGE — these rows belong to other subtasks' post-split stages,
    /// and flushing them straight into the local ones is exactly the
    /// scattered-key wrongness the re-key point exists to prevent. The
    /// final `flush_all` (after the exchanged rows were applied) then only
    /// finds post-split state.
    ///
    /// # Errors
    /// Propagates stage errors.
    pub fn flush_pre_split(&mut self, split: usize) -> ExecResult<Vec<RecordBatch>> {
        let mut carried: Vec<RecordBatch> = Vec::new();
        for i in 0..split {
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
mod split_phase {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Two-stage q4 shape over a shared key column "k": stage0 keys by the
    /// join key (co-located), stage1 keys by "left_k" — the re-key point is
    /// stage 1 in a parallel deployment. Here both halves run in ONE
    /// operator, which is exactly what the equivalence claim needs: the
    /// split-phase API fed end to end must produce what the fused API does.
    fn spec() -> krishiv_plan::stream_join::StreamingPipelineSpec {
        let mut stage0 =
            krishiv_plan::window::WindowExecutionSpec::tumbling("left_k", "left_ts", 60_000);
        stage0.watermark_lag_ms = 120_000;
        let mut stage1 = krishiv_plan::window::WindowExecutionSpec::tumbling(
            "left_k",
            "window_start_ms",
            60_000,
        );
        stage1.watermark_lag_ms = 120_000;
        krishiv_plan::stream_join::StreamingPipelineSpec {
            join: krishiv_plan::stream_join::StreamingJoinSpec {
                left_source: "l".into(),
                right_source: "r".into(),
                time_column: "ts".into(),
                left_key_column: "k".into(),
                right_key_column: "k".into(),
                window_ms: 60_000,
            },
            stages: vec![stage0, stage1],
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

    /// The split-phase API (pre-split → exchange seam → post-split) must be
    /// row-for-row equivalent to the fused path, INCLUDING the two-phase EOS
    /// flush: flush_pre_split's output fed back through on_stage_input, then
    /// flush_all, equals the fused flush_all. Pre-fix behavior does not
    /// exist (new API); this is the equivalence contract the distributed
    /// exchange relies on.
    #[test]
    fn split_phase_end_to_end_equals_the_fused_path() {
        let split = 1;
        let traffic: Vec<(Vec<&str>, Vec<i64>, bool)> = vec![
            (vec!["a", "b"], vec![1_000, 2_000], true),
            (vec!["a", "b"], vec![1_500, 2_500], false),
            (vec!["a"], vec![70_000], true),
            (vec!["a"], vec![70_500], false),
        ];

        let mut fused = JoinAggPipeline::new(&spec()).expect("fused build");
        let mut fused_out: Vec<RecordBatch> = Vec::new();
        for (keys, ts, left) in &traffic {
            let b = side(keys, ts);
            let out = if *left {
                fused.on_left(&b).expect("fused left")
            } else {
                fused.on_right(&b).expect("fused right")
            };
            fused_out.extend(out);
        }
        fused_out.extend(fused.flush_all().expect("fused flush"));

        let mut splitp = JoinAggPipeline::new(&spec()).expect("split build");
        let mut split_out: Vec<RecordBatch> = Vec::new();
        for (keys, ts, left) in &traffic {
            let b = side(keys, ts);
            let pre = if *left {
                splitp.on_left_pre_split(&b, split).expect("pre left")
            } else {
                splitp.on_right_pre_split(&b, split).expect("pre right")
            };
            // The exchange seam: at parallelism 1 every row is owned.
            let out = splitp.on_stage_input(pre, split).expect("stage input");
            split_out.extend(out);
        }
        let pre_flushed = splitp.flush_pre_split(split).expect("pre-split flush");
        split_out.extend(
            splitp
                .on_stage_input(pre_flushed, split)
                .expect("flushed stage input"),
        );
        split_out.extend(splitp.flush_all().expect("final flush"));

        let rows =
            |batches: &[RecordBatch]| -> usize { batches.iter().map(RecordBatch::num_rows).sum() };
        assert!(rows(&fused_out) > 0, "the fixture must produce output");
        assert_eq!(
            rows(&split_out),
            rows(&fused_out),
            "split-phase execution must emit exactly what the fused path emits"
        );
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
