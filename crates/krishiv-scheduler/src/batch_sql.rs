//! Coordinated batch SQL execution through the cluster control plane.

use std::path::PathBuf;
use std::time::Duration;

use krishiv_proto::{JobId, JobKind, JobSpec, JobState, StageId, StageSpec, TaskId, TaskSpec};

use crate::{SchedulerError, SchedulerResult, SharedCoordinator};

const BATCH_SQL_JOB_PREFIX: &str = "batch-sql-";

/// Parquet table registration for coordinated batch SQL.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchSqlTable {
    pub table_name: String,
    pub path: PathBuf,
}

/// Outcome of a coordinated batch SQL job.
#[derive(Debug, Clone)]
pub struct BatchSqlOutcome {
    pub job_id: JobId,
    pub inline_record_batch_ipc: Vec<Vec<u8>>,
}

/// Execute SQL on registered executors via the active coordinator.
pub async fn execute_batch_sql_coordinated(
    coordinator: &SharedCoordinator,
    query: &str,
    tables: &[BatchSqlTable],
) -> SchedulerResult<BatchSqlOutcome> {
    let job_id = JobId::try_new(format!(
        "{BATCH_SQL_JOB_PREFIX}{}",
        krishiv_async_util::unix_now_ms()
    ))
    .map_err(|error| SchedulerError::InvalidJob {
        message: error.to_string(),
    })?;

    let stage_id = StageId::try_new("stage-sql").map_err(|error| SchedulerError::InvalidJob {
        message: error.to_string(),
    })?;
    let task_id = TaskId::try_new("task-sql").map_err(|error| SchedulerError::InvalidJob {
        message: error.to_string(),
    })?;
    let fragment = format!("sql: {query}");
    let stage = StageSpec::new(stage_id, "batch-sql").with_task(TaskSpec::new(task_id, fragment));
    let spec = JobSpec::new(job_id.clone(), "batch-sql", JobKind::Batch).with_stage(stage);

    {
        let mut coord = coordinator.write().await;
        coord.ensure_active()?;
        coord.submit_job(spec)?;
        coord.register_batch_sql_tables(job_id.clone(), tables.to_vec());
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    loop {
        if tokio::time::Instant::now() >= deadline {
            let _ = coordinator.write().await.cancel_job(&job_id);
            return Err(SchedulerError::Transport {
                message: format!("batch SQL job {job_id} timed out after 300s"),
            });
        }

        if let Err(e) = coordinator.drive_pending_task_launches().await {
            eprintln!("drive_pending_task_launches failed: {:?}", e);
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
                return Ok(BatchSqlOutcome {
                    job_id,
                    inline_record_batch_ipc: batches,
                });
            }
            JobState::Failed | JobState::Cancelled => {
                return Err(SchedulerError::Transport {
                    message: format!("batch SQL job {job_id} finished in state {state:?}"),
                });
            }
            JobState::Accepted | JobState::Planning | JobState::Running => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Decode inline Arrow IPC payloads into record batches.
pub fn decode_inline_record_batches(
    payloads: &[Vec<u8>],
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    let mut out = Vec::new();
    for payload in payloads {
        if payload.is_empty() {
            continue;
        }
        let cursor = Cursor::new(payload.clone());
        let reader = StreamReader::try_new(cursor, None).map_err(|e| format!("ipc decode: {e}"))?;
        for batch in reader {
            out.push(batch.map_err(|e| format!("ipc read: {e}"))?);
        }
    }
    Ok(out)
}
