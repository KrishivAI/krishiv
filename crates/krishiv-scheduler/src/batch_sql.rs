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
#[derive(Debug)]
pub struct BatchSqlOutcome {
    pub job_id: JobId,
    pub inline_record_batch_ipc: Vec<Vec<u8>>,
    /// Disk-backed result spools for tasks whose output exceeded the inline
    /// threshold (delete themselves on drop). Decode via
    /// [`crate::result_spool::TaskResultSpool::decode_record_batches`].
    pub result_spools: Vec<crate::result_spool::TaskResultSpool>,
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
                let (batches, result_spools) = {
                    let mut coord = coordinator.write().await;
                    (
                        coord.take_job_inline_results(&job_id).unwrap_or_default(),
                        coord.take_job_result_spools(&job_id),
                    )
                };
                return Ok(BatchSqlOutcome {
                    job_id,
                    inline_record_batch_ipc: batches,
                    result_spools,
                });
            }
            JobState::Failed | JobState::Cancelled => {
                return Err(SchedulerError::Transport {
                    message: format!("batch SQL job {job_id} finished in state {state:?}"),
                });
            }
            JobState::Queued
            | JobState::Accepted
            | JobState::Planning
            | JobState::Running
            | JobState::Committing => {
                let state_changed = notify.notified();
                let recheck_state = {
                    let coord = coordinator.read().await;
                    coord.job_snapshot(&job_id).map(|s| s.state())?
                };
                if matches!(
                    recheck_state,
                    JobState::Queued
                        | JobState::Accepted
                        | JobState::Planning
                        | JobState::Running
                        | JobState::Committing
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
        submit_batch_sql_job_inner(coordinator, query, tables, &[], false, Some(sink_contract))
            .await?;

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
                // executor reported alongside the staged write (spools
                // delete their files on drop).
                {
                    let mut coord = coordinator.write().await;
                    let _ = coord.take_job_inline_results(&job_id);
                    let _ = coord.take_job_result_spools(&job_id);
                }
                return Ok(job_id);
            }
            JobState::Failed | JobState::Cancelled => {
                return Err(SchedulerError::Transport {
                    message: format!("batch SQL sink job {job_id} finished in state {state:?}"),
                });
            }
            JobState::Queued
            | JobState::Accepted
            | JobState::Planning
            | JobState::Running
            | JobState::Committing => {
                let state_changed = notify.notified();
                let recheck_state = {
                    let coord = coordinator.read().await;
                    coord.job_snapshot(&job_id).map(|s| s.state())?
                };
                if matches!(
                    recheck_state,
                    JobState::Queued
                        | JobState::Accepted
                        | JobState::Planning
                        | JobState::Running
                        | JobState::Committing
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
    submit_batch_sql_job_inner(coordinator, query, tables, &[], is_streaming, None).await
}

/// [`submit_batch_sql_job`] with additional path-registered tables.
///
/// Path tables require every executor (and the coordinator, which plans the
/// stages) to read `path` directly — a shared filesystem or single-node
/// daemon. In exchange, plain SELECTs over them are eligible for
/// partition-parallel staged execution (Phase 52): the coordinator cuts the
/// physical plan at shuffle boundaries and pins each map task to a subset of
/// the parquet files. Queries the stage builder declines run single-task
/// with the path tables attached as `LocalParquet` inputs, exactly like the
/// inline path.
pub async fn submit_batch_sql_job_with_paths(
    coordinator: &SharedCoordinator,
    query: &str,
    tables: &[BatchSqlInlineTable],
    path_tables: &[BatchSqlTable],
    is_streaming: bool,
) -> SchedulerResult<JobId> {
    submit_batch_sql_job_inner(coordinator, query, tables, path_tables, is_streaming, None).await
}

async fn submit_batch_sql_job_inner(
    coordinator: &SharedCoordinator,
    query: &str,
    tables: &[BatchSqlInlineTable],
    path_tables: &[BatchSqlTable],
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

    // Phase 52: attempt partition-parallel staged execution for plain batch
    // SELECTs over path-registered parquet tables. The stage builder pins
    // the parquet scans into each map task's plan, so staged jobs need no
    // input partitions. Inline IPC tables cannot be stage-split; any query
    // or plan shape the builder declines falls back to the single-task
    // `sql:` path exactly as before (capability honesty).
    let staged_stages =
        if !is_streaming && sink_contract.is_none() && tables.is_empty() && !path_tables.is_empty()
        {
            let table_refs: Vec<(String, std::path::PathBuf)> = path_tables
                .iter()
                .map(|t| (t.table_name.clone(), t.path.clone()))
                .collect();
            crate::distributed_batch::plan_staged_batch_stages(query, &table_refs).await
        } else {
            None
        };

    let job_kind = if is_streaming {
        JobKind::Streaming
    } else {
        JobKind::Batch
    };
    let is_staged = staged_stages.is_some();
    let spec = if let Some(stages) = staged_stages {
        stages.into_iter().fold(
            JobSpec::new(job_id.clone(), "batch-sql", job_kind),
            |spec, stage| spec.with_stage(stage),
        )
    } else {
        let stage_id =
            StageId::try_new("stage-sql").map_err(|error| SchedulerError::InvalidJob {
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
        JobSpec::new(job_id.clone(), "batch-sql", job_kind).with_stage(stage)
    };

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
    // Path tables ride along as LocalParquet inputs on the single-task path;
    // staged plans pin their parquet scans and need no input partitions.
    if !is_staged {
        for (idx, t) in path_tables.iter().enumerate() {
            input_partitions.push(InputPartition::typed(
                format!("local-parquet-{idx}"),
                InputPartitionDescriptor::LocalParquet {
                    table_name: t.table_name.clone(),
                    path: t.path.to_string_lossy().into_owned(),
                },
            ));
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::{Coordinator, SharedCoordinator};
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn write_test_parquet(dir: &std::path::Path) -> PathBuf {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Utf8, false),
        ]));
        let table_dir = dir.join("t");
        std::fs::create_dir_all(&table_dir).unwrap();
        for file_index in 0..2i64 {
            let ids: Vec<i64> = (0..100).map(|i| file_index * 100 + i).collect();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids.clone())),
                    Arc::new(StringArray::from(
                        ids.iter()
                            .map(|i| if i % 2 == 0 { "even" } else { "odd" })
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let file = std::fs::File::create(table_dir.join(format!("part-{file_index}.parquet")))
                .unwrap();
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(file, schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        table_dir
    }

    /// Phase 52 Leg 7: the daemon submission path stage-splits plain
    /// SELECTs over path-registered parquet tables (previously only the
    /// in-process runtime staged; every daemon job was single-task).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn path_table_group_by_submits_staged_job() {
        let tmp = tempfile::tempdir().unwrap();
        let table_dir = write_test_parquet(tmp.path());
        let coordinator = SharedCoordinator::new(Coordinator::new_active(None).unwrap());
        let path_tables = vec![BatchSqlTable {
            table_name: "t".into(),
            path: table_dir,
        }];

        let job_id = submit_batch_sql_job_with_paths(
            &coordinator,
            "SELECT category, COUNT(*) AS n FROM t GROUP BY category",
            &[],
            &path_tables,
            false,
        )
        .await
        .unwrap();

        let coord = coordinator.read().await;
        let snapshot = coord.job_snapshot(&job_id).unwrap();
        assert!(
            snapshot.stage_count() > 1,
            "GROUP BY over a path table must be stage-split, got {} stage(s)",
            snapshot.stage_count()
        );
    }

    /// A query shape the stage builder declines still submits — as the
    /// single-task fallback with the path table attached as a LocalParquet
    /// input (capability honesty, no hard failure).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn path_table_scan_only_falls_back_to_single_task() {
        let tmp = tempfile::tempdir().unwrap();
        let table_dir = write_test_parquet(tmp.path());
        let coordinator = SharedCoordinator::new(Coordinator::new_active(None).unwrap());
        let path_tables = vec![BatchSqlTable {
            table_name: "t".into(),
            path: table_dir,
        }];

        let job_id = submit_batch_sql_job_with_paths(
            &coordinator,
            "SELECT id FROM t WHERE id < 5",
            &[],
            &path_tables,
            false,
        )
        .await
        .unwrap();

        let coord = coordinator.read().await;
        let snapshot = coord.job_snapshot(&job_id).unwrap();
        assert_eq!(
            snapshot.stage_count(),
            1,
            "scan-only query must use the single-task path"
        );
    }

    /// Chaos (Phase 52 Leg 7): losing the executor that produced a staged
    /// job's map output must invalidate its shuffle partitions and reset the
    /// producing map tasks for re-execution — the reduce stage must never
    /// consume vanished data, and the job must recover rather than fail.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn executor_loss_after_map_invalidates_shuffle_and_reruns_maps() {
        use krishiv_proto::{
            ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, JobState,
            LeaseGeneration, ShufflePartitionOutput, TaskOutputMetadata, TaskState,
            TaskStatusUpdate,
        };

        let tmp = tempfile::tempdir().unwrap();
        let table_dir = write_test_parquet(tmp.path());
        let mut coord = Coordinator::new_active(None).unwrap();
        let exec_id = ExecutorId::try_new("chaos-exec").unwrap();
        coord
            .register_executor(ExecutorDescriptor::new(exec_id.clone(), "localhost", 8))
            .unwrap();
        coord
            .executor_heartbeat(
                ExecutorHeartbeat::new(exec_id.clone(), ExecutorState::Healthy)
                    .with_lease_generation(LeaseGeneration::initial()),
            )
            .unwrap();
        let coordinator = SharedCoordinator::new(coord);

        let path_tables = vec![BatchSqlTable {
            table_name: "t".into(),
            path: table_dir,
        }];
        let job_id = submit_batch_sql_job_with_paths(
            &coordinator,
            "SELECT category, COUNT(*) AS n FROM t GROUP BY category",
            &[],
            &path_tables,
            false,
        )
        .await
        .unwrap();

        // Assign + launch the runnable (map) tasks, then report each
        // succeeded with shuffle outputs attributed to the executor.
        let assignments = {
            let mut coord = coordinator.write().await;
            coord.assign_pending_tasks(&job_id).unwrap();
            coord.launch_assigned_task_assignments(&job_id).unwrap()
        };
        assert!(!assignments.is_empty(), "map tasks must be assignable");
        let map_tasks = assignments.len();
        for assignment in &assignments {
            let succeeded = TaskStatusUpdate::new(
                job_id.clone(),
                assignment.stage_id().clone(),
                assignment.task_id().clone(),
                assignment.executor_id().clone(),
                TaskState::Succeeded,
                assignment.attempt_id().as_u32(),
            )
            .with_lease_generation(assignment.lease_generation())
            .with_output_metadata(
                // A non-empty flight endpoint marks the partition as served
                // from the executor's process (the daemon path); an empty
                // endpoint means co-located in-process data, which cannot
                // outlive the coordinator and is exempt from invalidation.
                TaskOutputMetadata::new("shuffle_write", 1, 0, 1).with_shuffle_partitions(vec![
                    ShufflePartitionOutput::new(0, 64, "127.0.0.1:19999".to_string()),
                ]),
            );
            let mut coord = coordinator.write().await;
            coord.apply_task_update(succeeded).unwrap();
            let _ = coord.take_pending_sink_finalize();
        }
        {
            let coord = coordinator.read().await;
            let snapshot = coord.job_snapshot(&job_id).unwrap();
            assert_eq!(snapshot.succeeded_task_count(), map_tasks);
        }

        // Kill the executor: its shuffle outputs vanish with it.
        coordinator
            .write()
            .await
            .mark_executor_lost(&exec_id)
            .unwrap();

        let coord = coordinator.read().await;
        let snapshot = coord.job_snapshot(&job_id).unwrap();
        assert_eq!(
            snapshot.succeeded_task_count(),
            0,
            "map results on the lost executor must be invalidated, not trusted"
        );
        assert!(
            !matches!(snapshot.state(), JobState::Failed | JobState::Cancelled),
            "the job must recover by re-running maps, not fail (state: {:?})",
            snapshot.state()
        );
    }

    /// Chaos (Phase 52 Leg 7), reduce side: a consumer task that fails
    /// because upstream shuffle partitions vanished must re-queue the
    /// producing map tasks (invalidate-specific path), not fail the job.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reduce_missing_partition_report_reruns_producing_maps() {
        use krishiv_proto::{
            ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, JobState,
            LeaseGeneration, MissingShufflePartition, ShufflePartitionOutput, TaskOutputMetadata,
            TaskState, TaskStatusUpdate,
        };

        let tmp = tempfile::tempdir().unwrap();
        let table_dir = write_test_parquet(tmp.path());
        let mut coord = Coordinator::new_active(None).unwrap();
        let exec_id = ExecutorId::try_new("chaos-exec-2").unwrap();
        coord
            .register_executor(ExecutorDescriptor::new(exec_id.clone(), "localhost", 8))
            .unwrap();
        coord
            .executor_heartbeat(
                ExecutorHeartbeat::new(exec_id.clone(), ExecutorState::Healthy)
                    .with_lease_generation(LeaseGeneration::initial()),
            )
            .unwrap();
        let coordinator = SharedCoordinator::new(coord);

        let path_tables = vec![BatchSqlTable {
            table_name: "t".into(),
            path: table_dir,
        }];
        let job_id = submit_batch_sql_job_with_paths(
            &coordinator,
            "SELECT category, COUNT(*) AS n FROM t GROUP BY category",
            &[],
            &path_tables,
            false,
        )
        .await
        .unwrap();

        // Round 1: maps run and succeed with shuffle outputs.
        let map_assignments = {
            let mut coord = coordinator.write().await;
            coord.assign_pending_tasks(&job_id).unwrap();
            coord.launch_assigned_task_assignments(&job_id).unwrap()
        };
        assert!(!map_assignments.is_empty());
        let map_stage_id = map_assignments[0].stage_id().clone();
        for assignment in &map_assignments {
            let succeeded = TaskStatusUpdate::new(
                job_id.clone(),
                assignment.stage_id().clone(),
                assignment.task_id().clone(),
                assignment.executor_id().clone(),
                TaskState::Succeeded,
                assignment.attempt_id().as_u32(),
            )
            .with_lease_generation(assignment.lease_generation())
            .with_output_metadata(
                TaskOutputMetadata::new("shuffle_write", 1, 0, 1).with_shuffle_partitions(vec![
                    ShufflePartitionOutput::new(0, 64, "127.0.0.1:19999".to_string()),
                ]),
            );
            let mut coord = coordinator.write().await;
            coord.apply_task_update(succeeded).unwrap();
            let _ = coord.take_pending_sink_finalize();
        }

        // Round 2: the reduce task launches, then fails reporting the
        // upstream partitions as missing (producer data vanished).
        let reduce_assignments = {
            let mut coord = coordinator.write().await;
            coord.assign_pending_tasks(&job_id).unwrap();
            coord.launch_assigned_task_assignments(&job_id).unwrap()
        };
        assert!(
            !reduce_assignments.is_empty(),
            "reduce stage must become runnable once maps succeeded"
        );
        let reduce = &reduce_assignments[0];
        let failed = TaskStatusUpdate::new(
            job_id.clone(),
            reduce.stage_id().clone(),
            reduce.task_id().clone(),
            reduce.executor_id().clone(),
            TaskState::Failed,
            reduce.attempt_id().as_u32(),
        )
        .with_lease_generation(reduce.lease_generation())
        .with_missing_shuffle_partitions(vec![MissingShufflePartition::new(
            map_stage_id.clone(),
            0,
        )]);
        {
            let mut coord = coordinator.write().await;
            coord.apply_task_update(failed).unwrap();
            let _ = coord.take_pending_sink_finalize();
        }

        let coord = coordinator.read().await;
        let snapshot = coord.job_snapshot(&job_id).unwrap();
        assert!(
            snapshot.succeeded_task_count() < map_assignments.len(),
            "producers of the missing partitions must be re-queued"
        );
        assert!(
            !matches!(snapshot.state(), JobState::Failed | JobState::Cancelled),
            "the job must recover by re-running producers, not fail (state: {:?})",
            snapshot.state()
        );
    }
}
