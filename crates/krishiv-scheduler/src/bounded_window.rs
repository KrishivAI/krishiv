//! Coordinated bounded window execution through the cluster control plane.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use base64::Engine as _;
use krishiv_plan::window::{WindowExecutionSpec, validate_window_execution_spec};
use krishiv_proto::{
    InputPartition, InputPartitionDescriptor, JobId, JobKind, JobSpec, JobState, StageId,
    StageSpec, TaskId, TaskSpec,
};

use crate::{SchedulerError, SchedulerResult, SharedCoordinator};

const WINDOW_JOB_PREFIX: &str = "bounded-window-";
static NEXT_WINDOW_JOB_SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct PreparedBoundedWindowJob {
    job_spec: JobSpec,
    task_inputs: HashMap<TaskId, Vec<InputPartition>>,
}

/// Outcome of a coordinated bounded window job.
#[derive(Debug, Clone)]
pub struct BoundedWindowOutcome {
    pub job_id: JobId,
    pub inline_record_batch_ipc: Vec<Vec<u8>>,
}

/// Default target bytes per bounded-window shard: 128 MiB.
///
/// Each shard processes roughly this much data.  The AQE `AutoPartitionRule`
/// uses the same default so that batch jobs and bounded-window operations agree
/// on partition sizing.  Shared canonical constant from `krishiv-common`.
const TARGET_BYTES_PER_SHARD: u64 = krishiv_common::partition::TARGET_BYTES_PER_PARTITION;

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
    validate_bounded_window_request(topic, spec)?;

    let input_row_count = input_batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .try_fold(0usize, |total, rows| total.checked_add(rows))
        .ok_or_else(|| SchedulerError::InvalidJob {
            message: "bounded window input row count overflowed usize".into(),
        })?;

    // Total data volume across all input batches.
    let total_data_bytes: u64 = input_batches
        .iter()
        .map(|b| b.get_array_memory_size() as u64)
        .sum();

    let (shard_limit, coordinator_id) = {
        let coord = coordinator.read().await;
        coord.ensure_active()?;
        let executor_count = coord.executors().schedulable_executors().len();
        if executor_count == 0 {
            return Err(SchedulerError::NoExecutors);
        }
        // Shared cross-mode sizing brain: data-size driven, capped by available
        // executors and by input row count (never more shards than rows).
        let max_shards =
            u32::try_from(executor_count.min(input_row_count.max(1))).unwrap_or(u32::MAX);
        let shard_limit = krishiv_common::partition::recommend_buckets(
            total_data_bytes,
            1,
            max_shards,
            TARGET_BYTES_PER_SHARD,
        ) as usize;
        (shard_limit, coord.coordinator_id().to_string())
    };

    let mut shards = if input_row_count == 0 {
        vec![Vec::new()]
    } else {
        krishiv_common::partition::partition_record_batches_by_key(
            input_batches,
            &spec.key_column,
            shard_limit,
        )
        .map_err(|error| SchedulerError::InvalidJob {
            message: error.to_string(),
        })?
        .into_iter()
        .filter(|batches| !batches.is_empty())
        .collect::<Vec<_>>()
    };
    if shards.is_empty() {
        shards.push(Vec::new());
    }

    let sequence = NEXT_WINDOW_JOB_SEQUENCE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| SchedulerError::InvalidJob {
            message: "bounded window job-id sequence exhausted".into(),
        })?;
    let job_id = JobId::try_new(format!(
        "{WINDOW_JOB_PREFIX}{coordinator_id}-{}-{sequence}",
        krishiv_common::async_util::unix_now_ms(),
    ))
    .map_err(|e| SchedulerError::InvalidJob {
        message: e.to_string(),
    })?;

    let prepared = prepare_bounded_window_job(job_id.clone(), topic, spec, shards)?;

    {
        let mut coord = coordinator.write().await;
        coord.ensure_active()?;
        coord.submit_job_with_task_input_partitions(prepared.job_spec, prepared.task_inputs)?;
    }

    let notify = {
        let coord = coordinator.read().await;
        coord.notify().clone()
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
            JobState::Queued
            | JobState::Accepted
            | JobState::Planning
            | JobState::Running
            | JobState::Committing => {
                let state_changed = notify.notified();
                let recheck = {
                    let coord = coordinator.read().await;
                    coord.job_snapshot(&job_id).map(|s| s.state())?
                };
                if matches!(
                    recheck,
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

fn prepare_bounded_window_job(
    job_id: JobId,
    topic: &str,
    spec: &WindowExecutionSpec,
    shards: Vec<Vec<arrow::record_batch::RecordBatch>>,
) -> SchedulerResult<PreparedBoundedWindowJob> {
    validate_bounded_window_request(topic, spec)?;
    if shards.is_empty() {
        return Err(SchedulerError::InvalidJob {
            message: "bounded window job requires at least one shard".into(),
        });
    }

    let stage_id =
        StageId::try_new("stage-window").map_err(|error| SchedulerError::InvalidJob {
            message: error.to_string(),
        })?;
    let spec_json = serde_json::to_string(spec).map_err(|error| SchedulerError::InvalidJob {
        message: format!("window spec json: {error}"),
    })?;
    let spec_b64 = base64::engine::general_purpose::STANDARD.encode(spec_json.as_bytes());
    let fragment = format!("window:{topic}:{spec_b64}");

    let mut stage = StageSpec::new(stage_id, "bounded-window");
    let mut task_inputs = HashMap::with_capacity(shards.len());
    for (shard_idx, batches) in shards.into_iter().enumerate() {
        let task_id = TaskId::try_new(format!("task-window-{shard_idx}")).map_err(|error| {
            SchedulerError::InvalidJob {
                message: error.to_string(),
            }
        })?;
        let ipc_bytes =
            encode_batches_ipc(&batches).map_err(|error| SchedulerError::InvalidJob {
                message: format!("window shard {shard_idx} ipc encode: {error}"),
            })?;
        let input_partition = InputPartition::typed(
            format!("window-input-{shard_idx}"),
            InputPartitionDescriptor::InlineIpc {
                table_name: topic.to_string(),
                ipc_bytes,
            },
        );
        stage = stage.with_task(TaskSpec::new(task_id.clone(), fragment.clone()));
        task_inputs.insert(task_id, vec![input_partition]);
    }

    Ok(PreparedBoundedWindowJob {
        job_spec: JobSpec::new(job_id, "bounded-window", JobKind::Batch).with_stage(stage),
        task_inputs,
    })
}

fn validate_bounded_window_request(topic: &str, spec: &WindowExecutionSpec) -> SchedulerResult<()> {
    if !krishiv_common::validate::is_safe_identifier(topic) {
        return Err(SchedulerError::InvalidJob {
            message: format!("bounded window topic '{topic}' must match [A-Za-z0-9_.-]+"),
        });
    }
    validate_window_execution_spec(spec).map_err(|error| SchedulerError::InvalidJob {
        message: error.to_string(),
    })
}

fn encode_batches_ipc(batches: &[arrow::record_batch::RecordBatch]) -> Result<Vec<u8>, String> {
    use arrow::ipc::writer::StreamWriter;

    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches
        .first()
        .ok_or_else(|| "empty batches".to_string())?
        .schema();
    let mut buf = Vec::new();
    let mut writer =
        StreamWriter::try_new(&mut buf, &schema).map_err(|e| format!("ipc writer: {e}"))?;
    for batch in batches {
        writer.write(batch).map_err(|e| format!("ipc write: {e}"))?;
    }
    writer.finish().map_err(|e| format!("ipc finish: {e}"))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::StreamReader;
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn input_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "c"])),
                Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn prepared_job_binds_each_shard_to_exactly_one_task() {
        let batches = vec![input_batch()];
        let shards = krishiv_common::partition::partition_record_batches_by_key(&batches, "key", 3)
            .unwrap()
            .into_iter()
            .filter(|shard| !shard.is_empty())
            .collect::<Vec<_>>();
        let expected_task_count = shards.len();
        let job_id = JobId::try_new("bounded-window-test").unwrap();
        let spec = WindowExecutionSpec::tumbling("key", "ts", 1_000);

        let prepared = prepare_bounded_window_job(job_id, "events", &spec, shards).unwrap();
        assert_eq!(prepared.job_spec.task_count(), expected_task_count);
        assert_eq!(prepared.task_inputs.len(), expected_task_count);

        let task_ids = prepared
            .job_spec
            .stages()
            .iter()
            .flat_map(StageSpec::tasks)
            .map(|task| task.task_id().clone())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(task_ids, prepared.task_inputs.keys().cloned().collect());

        let mut total_rows = 0;
        let mut key_owners: HashMap<String, TaskId> = HashMap::new();
        for (task_id, inputs) in &prepared.task_inputs {
            assert_eq!(inputs.len(), 1);
            let descriptor = inputs[0].descriptor().unwrap();
            let InputPartitionDescriptor::InlineIpc {
                table_name,
                ipc_bytes,
            } = descriptor
            else {
                panic!("bounded window input must use InlineIpc");
            };
            assert_eq!(table_name, "events");
            let reader = StreamReader::try_new(Cursor::new(ipc_bytes), None).unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                total_rows += batch.num_rows();
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row in 0..keys.len() {
                    let key = keys.value(row).to_owned();
                    if let Some(owner) = key_owners.insert(key.clone(), task_id.clone()) {
                        assert_eq!(&owner, task_id, "key {key} crossed task boundaries");
                    }
                }
            }
        }
        assert_eq!(total_rows, 4);
    }

    #[test]
    fn prepared_job_rejects_zero_shards() {
        let error = prepare_bounded_window_job(
            JobId::try_new("bounded-window-empty").unwrap(),
            "events",
            &WindowExecutionSpec::tumbling("key", "ts", 1_000),
            Vec::new(),
        )
        .err()
        .expect("zero shards must fail");
        assert!(matches!(error, SchedulerError::InvalidJob { .. }));
    }

    #[test]
    fn prepared_job_rejects_unsafe_topic() {
        let error = prepare_bounded_window_job(
            JobId::try_new("bounded-window-invalid-topic").unwrap(),
            "events:raw",
            &WindowExecutionSpec::tumbling("key", "ts", 1_000),
            vec![vec![input_batch()]],
        )
        .err()
        .expect("fragment-delimiter topic must fail");
        assert!(matches!(error, SchedulerError::InvalidJob { .. }));
    }
}
