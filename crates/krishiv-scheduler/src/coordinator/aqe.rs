//! Phase 54: AQE stage-boundary re-optimization on real distributed stages.
//!
//! When a ShuffleMap stage succeeds, the coordinator inspects the measured
//! per-partition shuffle output sizes and rewrites the downstream Result
//! stage's still-unlaunched dfplan tasks:
//!
//! - **Partition coalescing**: partitions whose combined upstream bytes fit
//!   under `aqe_target_partition_bytes` are merged into one task carrying a
//!   `dfplan:v1:p1,p2,…` body (each root partition is an independent hash
//!   group, so stream concatenation is result-identical).
//! - **Skew split**: a partition ≥ `aqe_skew_factor × median` (and ≥
//!   `aqe_skew_min_bytes`) is split into several tasks, each reading a
//!   disjoint map-task range of the dominant upstream stage
//!   (`dfplan:v1:p/s<stage>m<a>-<b>`). Only plans
//!   [`krishiv_sql::distributed_plan::dfplan_body_is_split_safe`] approves
//!   are split.
//!
//! Rewrites only touch Result-kind stages whose tasks are Pending or
//! Assigned-but-never-launched: launch is gated on upstream success, so at
//! the moment the last upstream succeeds no downstream task can have been
//! delivered to an executor. ShuffleMap stages are never rewritten — their
//! task index is part of the `s{i}.m{t}` sub-stage-key wire contract.

use std::collections::HashMap;

use krishiv_proto::{JobId, JobKind, StageId, StageKind, StageState, TaskId, TaskSpec, TaskState};
use krishiv_sql::distributed_plan::{
    DfplanMapRange, DfplanTaskSpec, dfplan_body_is_split_safe, dfplan_body_partition_spec,
    dfplan_body_with_spec, is_dfplan_body,
};

use super::Coordinator;
use crate::adaptive::{AdaptiveDecisionKind, AdaptiveDecisionLog};
use crate::job::JobRecord;

/// Thresholds driving one AQE planning pass (lifted off `CoordinatorConfig`
/// so the planner is a pure function of the job record).
#[derive(Debug, Clone, Copy)]
pub(crate) struct AqeThresholds {
    pub coalesce: bool,
    pub skew_split: bool,
    pub target_partition_bytes: u64,
    pub skew_factor: f64,
    pub skew_min_bytes: u64,
}

/// One planned stage rewrite: the replacement task bodies plus bookkeeping.
#[derive(Debug)]
pub(crate) struct AqeStageRewrite {
    pub stage_id: StageId,
    /// Replacement dfplan bodies, one per new task.
    pub bodies: Vec<String>,
    pub original_task_count: usize,
    /// Partitions that were skew-split (each into ≥2 map-range tasks).
    pub skew_split_partitions: Vec<usize>,
    /// Human-readable decision summary for the adaptive log.
    pub details: String,
}

impl Coordinator {
    /// Apply stage-boundary AQE after `succeeded_stage_id` completed.
    ///
    /// Returns the number of downstream stages rewritten. Failures inside
    /// the rewrite are logged and skipped — AQE must never fail a job.
    pub(crate) fn apply_stage_boundary_aqe(
        &mut self,
        job_id: &JobId,
        succeeded_stage_id: &StageId,
    ) -> usize {
        let thresholds = AqeThresholds {
            coalesce: self.config.aqe_coalesce_enabled(),
            skew_split: self.config.aqe_skew_split_enabled(),
            target_partition_bytes: self.config.aqe_target_partition_bytes(),
            skew_factor: self.config.aqe_skew_factor(),
            skew_min_bytes: self.config.aqe_skew_min_bytes(),
        };
        if !thresholds.coalesce && !thresholds.skew_split {
            return 0;
        }

        let mut decision_logs: Vec<AdaptiveDecisionLog> = Vec::new();
        let mut applied = 0usize;
        let mut coalesced_tasks_removed = 0u64;
        let mut skew_splits = 0u64;

        if let Some(mut job) = self
            .job_coordinators
            .get(job_id)
            .map(|jc| jc.write_record())
        {
            if job.spec.kind() == JobKind::Streaming {
                return 0;
            }
            let rewrites = plan_aqe_rewrites(&job, succeeded_stage_id, &thresholds);
            let now_ms = u64::try_from(krishiv_common::async_util::unix_now_ms()).unwrap_or(0);
            for rewrite in rewrites {
                match apply_stage_rewrite(&mut job, &rewrite) {
                    Ok(()) => {
                        applied += 1;
                        coalesced_tasks_removed += (rewrite.original_task_count as u64)
                            .saturating_sub(rewrite.bodies.len() as u64)
                            .saturating_add(rewrite.skew_split_partitions.len() as u64);
                        skew_splits += rewrite.skew_split_partitions.len() as u64;
                        tracing::info!(
                            job_id = %job_id,
                            stage_id = %rewrite.stage_id,
                            original_tasks = rewrite.original_task_count,
                            new_tasks = rewrite.bodies.len(),
                            skew_split_partitions = ?rewrite.skew_split_partitions,
                            "AQE stage-boundary rewrite applied"
                        );
                        decision_logs.push(AdaptiveDecisionLog {
                            timestamp_ms: now_ms,
                            kind: if rewrite.skew_split_partitions.is_empty() {
                                AdaptiveDecisionKind::PartitionCoalesce
                            } else {
                                AdaptiveDecisionKind::SkewSplit
                            },
                            affected_job_id: job_id.clone(),
                            details: rewrite.details.clone(),
                            applied: true,
                        });
                    }
                    Err(reason) => {
                        tracing::warn!(
                            job_id = %job_id,
                            stage_id = %rewrite.stage_id,
                            reason,
                            "AQE stage rewrite skipped"
                        );
                    }
                }
            }
        }

        if applied > 0 {
            use std::sync::atomic::Ordering as AtomicOrdering;
            crate::metrics::AQE_STAGES_COALESCED_TOTAL
                .fetch_add(applied as u64, AtomicOrdering::Relaxed);
            crate::metrics::AQE_TASKS_COALESCED_TOTAL
                .fetch_add(coalesced_tasks_removed, AtomicOrdering::Relaxed);
            crate::metrics::AQE_SKEW_SPLITS_TOTAL.fetch_add(skew_splits, AtomicOrdering::Relaxed);
            for log in decision_logs {
                let bucket = self
                    .adaptive_decision_log
                    .entry(job_id.clone())
                    .or_default();
                const MAX_LOG_PER_JOB: usize = 100;
                if bucket.len() >= MAX_LOG_PER_JOB {
                    bucket.pop_front();
                }
                bucket.push_back(log);
            }
            self.launch_dirty_jobs.insert(job_id.clone());
            if let Err(error) = self.persist_job_record(
                job_id,
                krishiv_common::profile_requires_fail_closed_metadata(self.durability_profile),
            ) {
                tracing::warn!(job_id = %job_id, %error, "AQE rewrite persistence failed");
            }
        }
        applied
    }
}

/// Plan rewrites for every downstream stage of `succeeded_stage_id` that is
/// eligible (see module docs). Pure function of the job record.
pub(crate) fn plan_aqe_rewrites(
    job: &JobRecord,
    succeeded_stage_id: &StageId,
    thresholds: &AqeThresholds,
) -> Vec<AqeStageRewrite> {
    let mut rewrites = Vec::new();
    for stage in job.stages() {
        if !stage
            .spec
            .upstream_stage_ids()
            .iter()
            .any(|up| up == succeeded_stage_id)
        {
            continue;
        }
        if let Some(rewrite) = plan_stage_rewrite(job, stage.stage_id(), thresholds) {
            rewrites.push(rewrite);
        }
    }
    rewrites
}

fn plan_stage_rewrite(
    job: &JobRecord,
    stage_id: &StageId,
    thresholds: &AqeThresholds,
) -> Option<AqeStageRewrite> {
    let stage = job.stages().iter().find(|s| s.stage_id() == stage_id)?;
    // Only terminal Result stages: rewriting a ShuffleMap stage would break
    // the s{i}.m{t} sub-stage-key wire contract with downstream readers.
    if stage.spec.kind() != StageKind::Result || stage.tasks().is_empty() {
        return None;
    }
    // Every task must still be rewritable: never launched, no side contracts.
    let mut bodies_by_partition: HashMap<usize, String> = HashMap::new();
    for task in stage.tasks() {
        let rewritable = matches!(task.state(), TaskState::Pending | TaskState::Assigned)
            && !task.launch_in_flight();
        if !rewritable
            || task.spec.shuffle_write().is_some()
            || task.spec.shuffle_read().is_some()
            || task.spec.sink_contract().is_some()
        {
            return None;
        }
        let body = krishiv_plan::TypedTaskFragment::decode_or_legacy(task.spec.description()).body;
        if !is_dfplan_body(&body) {
            return None;
        }
        let spec = dfplan_body_partition_spec(&body).ok()?;
        // Already rewritten (multi-partition or map-range) → idempotence skip.
        let (&[partition], None) = (spec.partitions.as_slice(), &spec.map_range) else {
            return None;
        };
        if bodies_by_partition.insert(partition, body).is_some() {
            return None;
        }
    }
    let partition_count = stage.tasks().len();
    if !(0..partition_count).all(|p| bodies_by_partition.contains_key(&p)) {
        return None;
    }

    // Upstream stages must all have succeeded with measured shuffle output.
    let upstream_ids = stage.spec.upstream_stage_ids();
    if upstream_ids.is_empty() {
        return None;
    }
    struct UpstreamInfo {
        builder_index: Option<usize>,
        map_task_count: usize,
        partition_bytes: HashMap<usize, u64>,
    }
    let mut upstreams: Vec<UpstreamInfo> = Vec::new();
    for up_id in upstream_ids {
        let up = job.stages().iter().find(|s| s.stage_id() == up_id)?;
        if up.state() != StageState::Succeeded {
            return None;
        }
        let mut partition_bytes: HashMap<usize, u64> = HashMap::new();
        for task in up.tasks() {
            if task.state() != TaskState::Succeeded {
                continue;
            }
            if let Some(meta) = task.output_metadata() {
                for p in meta.shuffle_partitions() {
                    *partition_bytes.entry(p.partition_id as usize).or_insert(0) +=
                        p.size_bytes;
                }
            }
        }
        upstreams.push(UpstreamInfo {
            builder_index: builder_stage_index(up_id.as_str()),
            map_task_count: up.tasks().len(),
            partition_bytes,
        });
    }

    // Combined size per reduce partition across all upstream stages.
    let sizes: Vec<u64> = (0..partition_count)
        .map(|p| {
            upstreams
                .iter()
                .map(|u| u.partition_bytes.get(&p).copied().unwrap_or(0))
                .sum()
        })
        .collect();
    if sizes.iter().all(|&s| s == 0) {
        // No measurements → nothing to base a decision on.
        return None;
    }
    let median = median_of(&sizes);

    // ── Skew detection ────────────────────────────────────────────────────
    let mut skew_plan: Vec<(usize, usize, DfplanMapRange)> = Vec::new(); // (partition, k, first-range template)
    let mut skew_ranges: HashMap<usize, Vec<DfplanMapRange>> = HashMap::new();
    if thresholds.skew_split && median > 0.0 {
        // Split-safety decoding is per-plan, and every task shares the same
        // encoded plan — evaluate once, lazily.
        let mut split_safe: Option<bool> = None;
        for (p, &size) in sizes.iter().enumerate() {
            if size < thresholds.skew_min_bytes
                || (size as f64) < thresholds.skew_factor * median
            {
                continue;
            }
            // Attribute the skew to the upstream contributing the most bytes.
            let Some((dominant, up)) = upstreams
                .iter()
                .enumerate()
                .max_by_key(|(_, u)| u.partition_bytes.get(&p).copied().unwrap_or(0))
            else {
                continue;
            };
            let _ = dominant;
            let Some(builder_index) = up.builder_index else {
                continue;
            };
            if up.map_task_count < 2 {
                continue;
            }
            let safe = *split_safe.get_or_insert_with(|| {
                bodies_by_partition
                    .get(&p)
                    .is_some_and(|b| dfplan_body_is_split_safe(b))
            });
            if !safe {
                continue;
            }
            let k = usize::try_from(size.div_ceil(thresholds.target_partition_bytes.max(1)))
                .unwrap_or(usize::MAX)
                .clamp(2, up.map_task_count);
            let ranges = even_map_ranges(builder_index, up.map_task_count, k);
            skew_plan.push((p, k, ranges.first().cloned()?));
            skew_ranges.insert(p, ranges);
        }
    }

    // ── Coalescing of the remaining partitions ────────────────────────────
    let mut groups: Vec<Vec<usize>> = Vec::new();
    if thresholds.coalesce {
        let mut current: Vec<usize> = Vec::new();
        let mut current_bytes = 0u64;
        for (p, &size) in sizes.iter().enumerate() {
            if skew_ranges.contains_key(&p) {
                continue;
            }
            if !current.is_empty()
                && current_bytes.saturating_add(size) > thresholds.target_partition_bytes
            {
                groups.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            current.push(p);
            current_bytes = current_bytes.saturating_add(size);
        }
        if !current.is_empty() {
            groups.push(current);
        }
    } else {
        groups.extend(
            (0..partition_count)
                .filter(|p| !skew_ranges.contains_key(p))
                .map(|p| vec![p]),
        );
    }

    let new_task_count: usize =
        groups.len() + skew_ranges.values().map(Vec::len).sum::<usize>();
    if new_task_count == partition_count {
        return None;
    }

    // ── Materialize replacement bodies ────────────────────────────────────
    let mut bodies = Vec::with_capacity(new_task_count);
    for group in &groups {
        let first = group.first()?;
        let template = bodies_by_partition.get(first)?;
        let spec = DfplanTaskSpec {
            partitions: group.clone(),
            map_range: None,
        };
        bodies.push(dfplan_body_with_spec(template, &spec).ok()?);
    }
    let mut skew_split_partitions: Vec<usize> = skew_ranges.keys().copied().collect();
    skew_split_partitions.sort_unstable();
    for &p in &skew_split_partitions {
        let template = bodies_by_partition.get(&p)?;
        for range in skew_ranges.get(&p)? {
            let spec = DfplanTaskSpec {
                partitions: vec![p],
                map_range: Some(range.clone()),
            };
            bodies.push(dfplan_body_with_spec(template, &spec).ok()?);
        }
    }

    let details = format!(
        "stage {stage_id}: {partition_count} reduce partitions → {new} tasks \
         (coalesced groups: {groups_n}, skew-split partitions: {skews:?}, \
         median bytes: {median:.0}, target bytes: {target})",
        new = bodies.len(),
        groups_n = groups.len(),
        skews = skew_split_partitions,
        target = thresholds.target_partition_bytes,
    );
    Some(AqeStageRewrite {
        stage_id: stage_id.clone(),
        bodies,
        original_task_count: partition_count,
        skew_split_partitions,
        details,
    })
}

/// Replace the stage's task records with the rewrite's bodies.
fn apply_stage_rewrite(job: &mut JobRecord, rewrite: &AqeStageRewrite) -> Result<(), String> {
    let stage = job
        .stages_mut()
        .iter_mut()
        .find(|s| s.stage_id() == &rewrite.stage_id)
        .ok_or_else(|| String::from("stage disappeared"))?;
    let mut tasks = Vec::with_capacity(rewrite.bodies.len());
    for (index, body) in rewrite.bodies.iter().enumerate() {
        let task_id = TaskId::try_new(format!("{}-aqe-{index}", rewrite.stage_id))
            .map_err(|e| format!("aqe task id: {e}"))?;
        let fragment =
            krishiv_plan::TypedTaskFragment::new(krishiv_plan::ExecutionKind::Batch, body)
                .encode()
                .map_err(|e| format!("aqe fragment encode: {e}"))?;
        tasks.push(crate::job::TaskRecord::from_spec(TaskSpec::new(
            task_id, fragment,
        )));
    }
    stage.replace_tasks(tasks);
    Ok(())
}

/// Builder index of a distributed stage id (`dist-s{i}` → `i`).
fn builder_stage_index(stage_id: &str) -> Option<usize> {
    stage_id.strip_prefix("dist-s")?.parse().ok()
}

/// Split `0..map_tasks` into `k` contiguous, non-empty, disjoint ranges.
fn even_map_ranges(builder_index: usize, map_tasks: usize, k: usize) -> Vec<DfplanMapRange> {
    let k = k.clamp(1, map_tasks.max(1));
    let base = map_tasks / k;
    let rem = map_tasks % k;
    let mut ranges = Vec::with_capacity(k);
    let mut start = 0usize;
    for i in 0..k {
        let len = base + usize::from(i < rem);
        ranges.push(DfplanMapRange {
            upstream_stage_index: builder_index,
            start,
            end: start + len,
        });
        start += len;
    }
    ranges
}

fn median_of(sizes: &[u64]) -> f64 {
    if sizes.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<u64> = sizes.to_vec();
    sorted.sort_unstable();
    let n = sorted.len();
    let mid = n / 2;
    if n.is_multiple_of(2) {
        let a = sorted.get(mid.saturating_sub(1)).copied().unwrap_or(0);
        let b = sorted.get(mid).copied().unwrap_or(0);
        (a as f64 + b as f64) / 2.0
    } else {
        sorted.get(mid).copied().unwrap_or(0) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn even_map_ranges_cover_all_map_tasks_disjointly() {
        for (map_tasks, k) in [(4usize, 2usize), (5, 2), (7, 3), (3, 3), (10, 4)] {
            let ranges = even_map_ranges(0, map_tasks, k);
            assert_eq!(ranges.len(), k);
            let mut covered = Vec::new();
            for r in &ranges {
                assert!(r.start < r.end, "empty range in {ranges:?}");
                covered.extend(r.start..r.end);
            }
            let expected: Vec<usize> = (0..map_tasks).collect();
            assert_eq!(covered, expected, "map_tasks={map_tasks} k={k}");
        }
    }

    #[test]
    fn even_map_ranges_clamp_k_to_map_tasks() {
        let ranges = even_map_ranges(1, 2, 5);
        assert_eq!(ranges.len(), 2);
    }

    #[test]
    fn builder_stage_index_parses_dist_ids_only() {
        assert_eq!(builder_stage_index("dist-s0"), Some(0));
        assert_eq!(builder_stage_index("dist-s12"), Some(12));
        assert_eq!(builder_stage_index("stage-1"), None);
    }

    #[test]
    fn median_of_even_and_odd() {
        assert_eq!(median_of(&[1, 3, 5]), 3.0);
        assert_eq!(median_of(&[1, 3]), 2.0);
        assert_eq!(median_of(&[]), 0.0);
    }
}
