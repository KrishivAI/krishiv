//! Build structured observability reports for production diagnosis (GAP-OB-07).

use krishiv_metrics::observability_report::{
    ObservabilityReport, ReportCheckpoint, ReportExecutor, ReportJob, ReportStage, ReportTask,
};
use krishiv_proto::JobId;
use std::collections::BTreeMap;

use super::Coordinator;
use crate::SchedulerResult;

/// Build a full observability report for one job from live coordinator state.
pub fn build_observability_report(
    coordinator: &Coordinator,
    job_id: &JobId,
) -> SchedulerResult<ObservabilityReport> {
    let detail = coordinator.job_detail_snapshot(job_id)?;
    let job = detail.job();
    let record = coordinator
        .job_coordinators
        .get(job_id)
        .map(|jc| jc.read_record());
    let job_name = record
        .as_ref()
        .map(|r| r.spec.name().to_owned())
        .unwrap_or_else(|| job.job_id().as_str().to_owned());

    let mut stages = Vec::new();
    for stage in detail.stages() {
        let mut task_counts: BTreeMap<String, usize> = BTreeMap::new();
        let mut task_details = Vec::new();
        for task in stage.tasks() {
            let state = task.state().to_string();
            *task_counts.entry(state.clone()).or_default() += 1;
            task_details.push(ReportTask {
                task_id: task.task_id().as_str().to_owned(),
                state,
                executor_id: task.assigned_executor().map(|e| e.as_str().to_owned()),
                attempt_id: task.attempt(),
                failure_count: task.failure_count(),
                last_failure_reason: task.last_failure_reason().map(str::to_owned),
                watermark_ms: task.last_watermark_ms(),
                source_offset: task
                    .last_source_offset()
                    .map(|b| String::from_utf8_lossy(b).into_owned()),
                runtime_stats: None,
            });
        }
        stages.push(ReportStage {
            stage_id: stage.stage_id().as_str().to_owned(),
            stage_name: stage.stage_id().as_str().to_owned(),
            state: stage.state().to_string(),
            task_counts,
            upstream_stage_ids: Vec::new(),
            output_partition_count: None,
            task_details,
        });
    }

    let checkpoint = coordinator
        .checkpoint_coordinators
        .get(job_id)
        .map(|coord| ReportCheckpoint {
            latest_epoch: coord.current_epoch(),
            fencing_token: coord.fencing_token().as_u64(),
            committed_at_ms: 0,
            source_offsets: BTreeMap::new(),
            operator_snapshots: BTreeMap::new(),
            is_savepoint: false,
            savepoint_label: None,
            iceberg_snapshot_id: None,
            kafka_offsets: None,
        });

    let executors = coordinator
        .executor_snapshots()
        .into_iter()
        .map(|record| ReportExecutor {
            executor_id: record.executor_id().as_str().to_owned(),
            host: record.descriptor().host().to_owned(),
            state: record.state().to_string(),
            slots: record.descriptor().slots(),
            active_tasks: record.running_tasks().len(),
            heartbeat_age_ticks: coordinator
                .ticks_since_restart
                .saturating_sub(record.last_heartbeat_tick()),
            task_endpoint: record.descriptor().task_endpoint().map(str::to_owned),
            memory_used_bytes: record.health_snapshot().and_then(|h| h.memory_used_bytes),
            memory_limit_bytes: record.health_snapshot().and_then(|h| h.memory_limit_bytes),
        })
        .collect();

    Ok(ObservabilityReport {
        generated_at: format!("{}", krishiv_common::async_util::unix_now_ms()),
        coordinator_id: coordinator.coordinator_id().as_str().to_owned(),
        job: ReportJob {
            job_id: job.job_id().as_str().to_owned(),
            job_name,
            job_kind: job.kind().to_string(),
            state: job.state().to_string(),
            submitted_at_ms: 0,
            priority: job.priority(),
            namespace_id: job.namespace_id().map(str::to_owned),
            elapsed_ms: 0,
        },
        stages,
        executors,
        checkpoint,
        shuffle_partitions: None,
        streaming_state: None,
        recent_events: Vec::new(),
        connector_metrics: None,
    })
}
