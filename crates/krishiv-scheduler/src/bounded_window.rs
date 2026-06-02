//! Coordinated bounded window execution through the cluster control plane.

use std::time::Duration;

use base64::Engine as _;
use krishiv_plan::window::WindowExecutionSpec;
use krishiv_proto::{
    InputPartition, InputPartitionDescriptor, JobId, JobKind, JobSpec, JobState, StageId,
    StageSpec, TaskId, TaskSpec,
};

use crate::{SchedulerError, SchedulerResult, SharedCoordinator};

const WINDOW_JOB_PREFIX: &str = "bounded-window-";

/// Outcome of a coordinated bounded window job.
#[derive(Debug, Clone)]
pub struct BoundedWindowOutcome {
    pub job_id: JobId,
    pub inline_record_batch_ipc: Vec<Vec<u8>>,
}

/// Execute a bounded window via the active coordinator and a registered executor.
///
/// Input batches are delivered as `InlineIpc` task input partitions so the
/// fragment description never grows beyond a few hundred bytes regardless of
/// dataset size.
pub async fn execute_bounded_window_coordinated(
    coordinator: &SharedCoordinator,
    topic: &str,
    spec: &WindowExecutionSpec,
    input_batches: &[arrow::record_batch::RecordBatch],
) -> SchedulerResult<BoundedWindowOutcome> {
    let job_id = JobId::try_new(format!(
        "{WINDOW_JOB_PREFIX}{}",
        krishiv_common::async_util::unix_now_ms()
    ))
    .map_err(|e| SchedulerError::InvalidJob {
        message: e.to_string(),
    })?;

    let stage_id = StageId::try_new("stage-window").map_err(|e| SchedulerError::InvalidJob {
        message: e.to_string(),
    })?;
    let task_id = TaskId::try_new("task-window").map_err(|e| SchedulerError::InvalidJob {
        message: e.to_string(),
    })?;

    let spec_json = serde_json::to_string(spec).map_err(|e| SchedulerError::InvalidJob {
        message: format!("window spec json: {e}"),
    })?;
    let spec_b64 = base64::engine::general_purpose::STANDARD.encode(spec_json.as_bytes());

    // Fragment carries only the topic and spec — no inline data.
    let fragment = format!("window:{topic}:{spec_b64}");

    // Encode input batches as a single InlineIpc partition.
    let ipc_bytes = encode_batches_ipc(input_batches).map_err(|e| SchedulerError::InvalidJob {
        message: format!("window input ipc encode: {e}"),
    })?;
    let input_partition = InputPartition::typed(
        "window-input",
        InputPartitionDescriptor::InlineIpc {
            table_name: topic.to_string(),
            ipc_bytes,
        },
    );

    let stage =
        StageSpec::new(stage_id, "bounded-window").with_task(TaskSpec::new(task_id, fragment));
    let job_spec = JobSpec::new(job_id.clone(), "bounded-window", JobKind::Batch).with_stage(stage);

    {
        let mut coord = coordinator.write().await;
        coord.ensure_active()?;
        coord.submit_job(job_spec)?;
        // Register inline partitions separately — same pattern as batch_sql_job_tables.
        coord.register_job_input_partitions(job_id.clone(), vec![input_partition]);
    }

    let notify = {
        let coord = coordinator.read().await;
        coord.notify.clone()
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        if tokio::time::Instant::now() >= deadline {
            let _ = coordinator.write().await.cancel_job(&job_id);
            return Err(SchedulerError::Transport {
                message: format!("bounded window job {job_id} timed out after 120s"),
            });
        }

        let state = {
            let coord = coordinator.read().await;
            coord.job_snapshot(&job_id).map(|s| s.state())?
        };

        match state {
            JobState::Succeeded => {
                let batches = coordinator
                    .write()
                    .await
                    .take_job_inline_results(&job_id)
                    .unwrap_or_default();
                return Ok(BoundedWindowOutcome {
                    job_id,
                    inline_record_batch_ipc: batches,
                });
            }
            JobState::Failed | JobState::Cancelled => {
                return Err(SchedulerError::Transport {
                    message: format!("bounded window job {job_id} finished in state {state:?}"),
                });
            }
            JobState::Accepted | JobState::Planning | JobState::Running => {
                let state_changed = notify.notified();
                let recheck = {
                    let coord = coordinator.read().await;
                    coord.job_snapshot(&job_id).map(|s| s.state())?
                };
                if matches!(
                    recheck,
                    JobState::Accepted | JobState::Planning | JobState::Running
                ) {
                    tokio::select! {
                        _ = state_changed => {}
                        _ = tokio::time::sleep_until(deadline) => {}
                    }
                }
            }
        }
    }
}

fn encode_batches_ipc(batches: &[arrow::record_batch::RecordBatch]) -> Result<Vec<u8>, String> {
    use arrow::ipc::writer::StreamWriter;

    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    let mut writer =
        StreamWriter::try_new(&mut buf, &schema).map_err(|e| format!("ipc writer: {e}"))?;
    for batch in batches {
        writer.write(batch).map_err(|e| format!("ipc write: {e}"))?;
    }
    writer.finish().map_err(|e| format!("ipc finish: {e}"))?;
    Ok(buf)
}
