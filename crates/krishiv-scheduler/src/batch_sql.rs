//! Coordinated batch SQL execution through the cluster control plane.

use std::path::PathBuf;
use std::time::Duration;

use krishiv_proto::{
    InputPartition, InputPartitionDescriptor, JobId, JobKind, JobSpec, JobState, StageId,
    StageSpec, TaskId, TaskSpec,
};

/// BATCH-5: Configurable batch SQL timeout (env `KRISHIV_BATCH_SQL_TIMEOUT_SECS`,
/// default 300s).
fn batch_sql_timeout() -> Duration {
    std::env::var("KRISHIV_BATCH_SQL_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(300))
}

use crate::{SchedulerError, SchedulerResult, SharedCoordinator};

const BATCH_SQL_JOB_PREFIX: &str = "batch-sql-";

/// Legacy file-path table registration (single-node local cluster only).
///
/// For multi-node / distributed deployments use `BatchSqlInlineTable` instead,
/// which delivers Arrow IPC bytes in-band so executors need no shared filesystem.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchSqlTable {
    pub table_name: String,
    pub path: PathBuf,
}

/// Inline Arrow IPC table for distributed batch SQL.
///
/// The data is delivered as base64-encoded Arrow IPC stream bytes so the
/// executor receives everything it needs in the task assignment without
/// accessing any shared filesystem or object store.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchSqlInlineTable {
    pub table_name: String,
    /// Arrow IPC stream bytes, base64-encoded.
    pub ipc_b64: String,
}

/// Outcome of a coordinated batch SQL job.
#[derive(Debug, Clone)]
pub struct BatchSqlOutcome {
    pub job_id: JobId,
    pub inline_record_batch_ipc: Vec<Vec<u8>>,
}

/// Execute SQL on registered executors via the active coordinator.
///
/// Input tables are provided as inline Arrow IPC (base64-encoded) so this
/// function works on multi-node clusters where executors have no access to
/// the client's local filesystem.
/// Execute SQL synchronously — submits a job then polls until completion.
///
/// For non-blocking submission use [`submit_batch_sql_job`] and poll the
/// `GET /api/v1/batch-sql/{job_id}` HTTP endpoint instead.
pub async fn execute_batch_sql_coordinated(
    coordinator: &SharedCoordinator,
    query: &str,
    tables: &[BatchSqlInlineTable],
) -> SchedulerResult<BatchSqlOutcome> {
    let job_id = submit_batch_sql_job(coordinator, query, tables, false).await?;

    let notify = {
        let coord = coordinator.read().await;
        coord.notify().clone()
    };

    let deadline = tokio::time::Instant::now() + batch_sql_timeout();
    loop {
        if tokio::time::Instant::now() >= deadline {
            let _ = coordinator.write().await.cancel_job(&job_id);
            return Err(SchedulerError::Transport {
                message: format!("batch SQL job {job_id} timed out after 300s"),
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
            JobState::Queued | JobState::Accepted | JobState::Planning | JobState::Running => {
                let state_changed = notify.notified();
                let recheck_state = {
                    let coord = coordinator.read().await;
                    coord.job_snapshot(&job_id).map(|s| s.state())?
                };
                if matches!(
                    recheck_state,
                    JobState::Queued | JobState::Accepted | JobState::Planning | JobState::Running
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

/// Execute a batch SQL write job whose terminal task writes through a sink
/// contract (Phase 2.3 distributed writes) instead of returning rows inline.
///
/// `sink_contract` is the full output contract description, e.g.
/// `object-parquet-sink:<base_dir>:<dest>:mode=overwrite:partition_by=c`.
/// The executor stages output under `<dest>/_staging/<job_id>/`; on job
/// success the coordinator publishes the staged files into the destination,
/// and on failure it removes them. Returns the job id once the job has
/// reached `Succeeded`.
pub async fn execute_batch_sql_sink_coordinated(
    coordinator: &SharedCoordinator,
    query: &str,
    tables: &[BatchSqlInlineTable],
    sink_contract: &str,
) -> SchedulerResult<JobId> {
    let job_id =
        submit_batch_sql_job_inner(coordinator, query, tables, false, Some(sink_contract)).await?;

    let notify = {
        let coord = coordinator.read().await;
        coord.notify().clone()
    };

    let deadline = tokio::time::Instant::now() + batch_sql_timeout();
    loop {
        if tokio::time::Instant::now() >= deadline {
            let _ = coordinator.write().await.cancel_job(&job_id);
            return Err(SchedulerError::Transport {
                message: format!("batch SQL sink job {job_id} timed out after 300s"),
            });
        }

        let state = {
            let coord = coordinator.read().await;
            coord.job_snapshot(&job_id).map(|s| s.state())?
        };

        match state {
            JobState::Succeeded => {
                // Sink jobs do not return inline results; drop any that the
                // executor reported alongside the staged write.
                let _ = coordinator.write().await.take_job_inline_results(&job_id);
                return Ok(job_id);
            }
            JobState::Failed | JobState::Cancelled => {
                return Err(SchedulerError::Transport {
                    message: format!("batch SQL sink job {job_id} finished in state {state:?}"),
                });
            }
            JobState::Queued | JobState::Accepted | JobState::Planning | JobState::Running => {
                let state_changed = notify.notified();
                let recheck_state = {
                    let coord = coordinator.read().await;
                    coord.job_snapshot(&job_id).map(|s| s.state())?
                };
                if matches!(
                    recheck_state,
                    JobState::Queued | JobState::Accepted | JobState::Planning | JobState::Running
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

/// Submit a batch SQL job and return immediately with the `JobId`.
///
/// The background orchestration loop drives task dispatch.  The caller
/// should poll `coordinator.job_snapshot(&job_id).state()` or use the
/// `GET /api/v1/batch-sql/{job_id}` HTTP endpoint for results.
pub async fn submit_batch_sql_job(
    coordinator: &SharedCoordinator,
    query: &str,
    tables: &[BatchSqlInlineTable],
    is_streaming: bool,
) -> SchedulerResult<JobId> {
    submit_batch_sql_job_inner(coordinator, query, tables, is_streaming, None).await
}

async fn submit_batch_sql_job_inner(
    coordinator: &SharedCoordinator,
    query: &str,
    tables: &[BatchSqlInlineTable],
    is_streaming: bool,
    sink_contract: Option<&str>,
) -> SchedulerResult<JobId> {
    use base64::Engine as _;

    // BATCH-4: Append a process-unique counter to avoid job-ID collisions
    // when two batch SQL jobs are submitted in the same millisecond.
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    static BATCH_SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = BATCH_SEQ.fetch_add(1, AtomicOrdering::SeqCst);
    let job_id = JobId::try_new(format!(
        "{BATCH_SQL_JOB_PREFIX}{}-{seq}",
        krishiv_common::async_util::unix_now_ms()
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
    let mut task = TaskSpec::new(task_id, fragment);
    if let Some(contract) = sink_contract {
        if contract.trim().is_empty() {
            return Err(SchedulerError::InvalidJob {
                message: String::from("batch SQL sink contract cannot be empty"),
            });
        }
        task = task.with_sink_contract(contract.trim());
    }
    let stage = StageSpec::new(stage_id, "batch-sql").with_task(task);
    let job_kind = if is_streaming {
        JobKind::Streaming
    } else {
        JobKind::Batch
    };
    let spec = JobSpec::new(job_id.clone(), "batch-sql", job_kind).with_stage(stage);

    // OPTIMIZATION OPPORTUNITY: In the embedded in-process path, the caller
    // already has RecordBatch values in memory. Routing them through Base64 +
    // Arrow IPC encode → coordinator store → decode → re-register is
    // unnecessary serialisation. A future `InputPartitionDescriptor::InMemory`
    // variant could pass `Arc<RecordBatch>` directly, eliminating two encode
    // and two decode round-trips per partition. This is safe in the in-process
    // path because coordinator and executor share the same address space.
    // Track: replace InlineIpc with InMemory for embedded sessions (no cross-
    // process boundary, zero-copy).
    let mut input_partitions: Vec<InputPartition> = Vec::with_capacity(tables.len());
    for (idx, t) in tables.iter().enumerate() {
        let ipc_bytes = base64::engine::general_purpose::STANDARD
            .decode(t.ipc_b64.as_bytes())
            .map_err(|e| SchedulerError::InvalidJob {
                message: format!(
                    "inline partition {idx} ({}) base64 decode failed: {e}",
                    t.table_name
                ),
            })?;
        let limit = {
            let coord = coordinator.read().await;
            coord.config().inline_partition_limit_bytes()
        };
        if ipc_bytes.len() > limit {
            return Err(SchedulerError::InvalidJob {
                message: format!(
                    "inline partition {idx} ('{}') is {} bytes, which exceeds the \
                     {limit}-byte per-partition limit (coordinator config \
                     inline_partition_limit_bytes); split the table into smaller \
                     chunks or raise the limit via CoordinatorConfig",
                    t.table_name,
                    ipc_bytes.len(),
                ),
            });
        }
        input_partitions.push(InputPartition::typed(
            format!("inline-{idx}"),
            InputPartitionDescriptor::InlineIpc {
                table_name: t.table_name.clone(),
                ipc_bytes,
            },
        ));
    }

    let mut coord = coordinator.write().await;
    coord.ensure_active()?;
    coord.submit_job(spec)?;
    coord.register_job_input_partitions(job_id.clone(), input_partitions);
    Ok(job_id)
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
