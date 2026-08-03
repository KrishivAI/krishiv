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

    // Upstream edges are not carried on `StageSnapshot`, but this report is
    // read to answer "why is this stage not starting" — a DAG report with no
    // edges cannot answer it. Take them from the record we already hold.
    let upstreams_by_stage: BTreeMap<String, Vec<String>> = record
        .as_ref()
        .map(|r| {
            r.stages()
                .iter()
                .map(|s| {
                    (
                        s.stage_id().as_str().to_owned(),
                        s.spec
                            .upstream_stage_ids()
                            .iter()
                            .map(|id| id.as_str().to_owned())
                            .collect(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();

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
            upstream_stage_ids: upstreams_by_stage
                .get(stage.stage_id().as_str())
                .cloned()
                .unwrap_or_default(),
            output_partition_count: None,
            task_details,
        });
    }

    let checkpoint = coordinator
        .ckpt
        .coordinators
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
            // The registry's own clock — the one `last_heartbeat_tick` is
            // stamped from. This read `exec.ticks_since_restart`, which is a
            // different counter entirely: it starts at `u64::MAX` (so that a
            // coordinator that never recovered is never inside the streaming
            // re-attach grace window) and only resets to 0 in
            // `recover_from_store`. Every executor in this report therefore
            // showed a heartbeat age of ~1.8e19 ticks on any coordinator that
            // had not recovered from a store — in the one report an operator
            // reads to decide whether an executor is alive.
            heartbeat_age_ticks: coordinator
                .exec
                .executors
                .current_tick()
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

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_proto::{
        CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorState, JobKind, JobSpec,
        StageId, StageSpec, TaskId, TaskSpec,
    };

    #[test]
    fn build_observability_report_errs_for_an_unknown_job() {
        let coordinator = Coordinator::active(CoordinatorId::try_new("obs-unknown").unwrap());
        let err = build_observability_report(&coordinator, &JobId::try_new("no-such-job").unwrap())
            .unwrap_err();
        assert!(matches!(err, crate::SchedulerError::UnknownJob { .. }));
    }

    #[test]
    fn build_observability_report_reflects_live_coordinator_state() {
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("obs-live").unwrap());
        let exec_id = krishiv_proto::ExecutorId::try_new("exec-obs").unwrap();
        coordinator
            .register_executor(
                ExecutorDescriptor::new(exec_id.clone(), "10.0.0.5", 4)
                    .with_task_endpoint("http://10.0.0.5:2001"),
            )
            .unwrap();
        coordinator
            .executor_heartbeat(ExecutorHeartbeat::new(
                exec_id.clone(),
                ExecutorState::Healthy,
            ))
            .unwrap();

        let job_id = JobId::try_new("obs-job").unwrap();
        let spec = JobSpec::new(job_id.clone(), "obs-job-name", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-0").unwrap(), "stage").with_task(TaskSpec::new(
                TaskId::try_new("t0").unwrap(),
                "sql: select 1",
            )),
        );
        coordinator.submit_job(spec).unwrap();

        let report = build_observability_report(&coordinator, &job_id).unwrap();

        assert_eq!(report.job.job_id, "obs-job");
        assert_eq!(report.job.job_name, "obs-job-name");
        assert_eq!(report.coordinator_id, "obs-live");
        assert_eq!(report.stages.len(), 1);
        assert_eq!(report.stages[0].task_details.len(), 1);
        assert_eq!(report.stages[0].task_details[0].task_id, "t0");
        assert_eq!(
            report.stages[0].task_counts.values().sum::<usize>(),
            1,
            "every task must be bucketed into exactly one state count"
        );
        assert_eq!(report.executors.len(), 1);
        assert_eq!(report.executors[0].executor_id, "exec-obs");
        assert_eq!(report.executors[0].host, "10.0.0.5");
        assert_eq!(report.executors[0].slots, 4);
        assert!(
            report.checkpoint.is_none(),
            "a job without checkpoint config must report no checkpoint block"
        );
    }

    /// The report's executor liveness column must be a real heartbeat age.
    /// It was computed against `exec.ticks_since_restart`, which starts at
    /// `u64::MAX` and is only reset by `recover_from_store` — so on any
    /// coordinator that had not recovered from a store (every fresh process)
    /// it reported ~1.8e19 ticks for a healthy executor that had just
    /// heartbeated.
    #[test]
    fn heartbeat_age_is_measured_against_the_registry_clock() {
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("obs-age").unwrap());
        let exec_id = krishiv_proto::ExecutorId::try_new("exec-age").unwrap();
        coordinator
            .register_executor(ExecutorDescriptor::new(exec_id.clone(), "10.0.0.9", 2))
            .unwrap();
        coordinator
            .executor_heartbeat(ExecutorHeartbeat::new(
                exec_id.clone(),
                ExecutorState::Healthy,
            ))
            .unwrap();

        let job_id = JobId::try_new("obs-age-job").unwrap();
        coordinator
            .submit_job(
                JobSpec::new(job_id.clone(), "obs-age-job", JobKind::Batch).with_stage(
                    StageSpec::new(StageId::try_new("stage-0").unwrap(), "stage").with_task(
                        TaskSpec::new(TaskId::try_new("t0").unwrap(), "sql: select 1"),
                    ),
                ),
            )
            .unwrap();

        let report = build_observability_report(&coordinator, &job_id).unwrap();
        assert_eq!(
            report.executors[0].heartbeat_age_ticks, 0,
            "an executor that just heartbeated is zero ticks stale"
        );

        coordinator.advance_heartbeat_clock(2).unwrap();
        let report = build_observability_report(&coordinator, &job_id).unwrap();
        assert_eq!(
            report.executors[0].heartbeat_age_ticks, 2,
            "the age must track the registry clock the heartbeat is stamped from"
        );
    }

    /// A DAG report with no edges cannot answer the question it exists for
    /// ("why has this stage not started"). The upstream ids are on the job
    /// record the builder already holds; they were being emitted as empty.
    #[test]
    fn stage_upstream_edges_are_reported() {
        let mut coordinator = Coordinator::active(CoordinatorId::try_new("obs-dag").unwrap());
        let job_id = JobId::try_new("obs-dag-job").unwrap();
        let up = StageId::try_new("dist-s0").unwrap();
        let down = StageId::try_new("dist-s1").unwrap();
        coordinator
            .submit_job(
                JobSpec::new(job_id.clone(), "dag", JobKind::Batch)
                    .with_stage(StageSpec::new(up.clone(), "map").with_task(TaskSpec::new(
                        TaskId::try_new("m0").unwrap(),
                        "sql: select 1",
                    )))
                    .with_stage(
                        StageSpec::new(down, "reduce")
                            .with_upstream_stage(up)
                            .with_task(TaskSpec::new(
                                TaskId::try_new("r0").unwrap(),
                                "sql: select 1",
                            )),
                    ),
            )
            .unwrap();

        let report = build_observability_report(&coordinator, &job_id).unwrap();
        let reduce = report
            .stages
            .iter()
            .find(|s| s.stage_id == "dist-s1")
            .expect("reduce stage in report");
        assert_eq!(
            reduce.upstream_stage_ids,
            vec!["dist-s0".to_string()],
            "the reduce stage's dependency must appear in the report"
        );
    }
}
