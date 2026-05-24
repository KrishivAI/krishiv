//! In-process coordinator + executor over shared mpsc/inbox transport (ADR-12.4).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use krishiv_async_util::block_on;
use krishiv_executor::{ExecutorAssignmentInbox, ExecutorTaskRunner};
use krishiv_proto::{
    CoordinatorId, ExecutorDescriptor, ExecutorId, InputPartition, JobId,
    JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec,
};
use krishiv_scheduler::{
    Coordinator, InProcessCoordinatorBridge, IN_PROCESS_TASK_ENDPOINT, SubmitOutcome,
};

use crate::local_streaming::LocalWindowExecutionSpec;
use crate::stream_kafka::encode_stream_kafka_partition;
use crate::{RuntimeError, RuntimeResult};

static JOB_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_job_id() -> RuntimeResult<JobId> {
    let n = JOB_COUNTER.fetch_add(1, Ordering::Relaxed);
    JobId::try_new(format!("in-process-job-{n}")).map_err(|e| RuntimeError::transport(e.to_string()))
}

fn fragment_from_spec(spec: &LocalWindowExecutionSpec) -> String {
    let agg = spec
        .agg_exprs
        .first()
        .map(|a| {
            use krishiv_exec::AggFunction;
            match a.function {
                AggFunction::Count => "agg=count".to_string(),
                AggFunction::Sum => "agg=sum:col=val".to_string(),
                _ => "agg=count".to_string(),
            }
        })
        .unwrap_or_else(|| "agg=count".to_string());
    // `stream-kafka:` input partitions normalize to columns `key`, `ts`, `val`.
    let _ = (&spec.key_column, &spec.event_time_column);
    format!(
        "stream:tw:key=key:time=ts:win={}:lag={}:{}",
        spec.window_size_ms,
        spec.watermark_lag_ms,
        agg
    )
}

/// Shared in-process streaming runtime (coordinator + executor inbox).
#[derive(Clone)]
pub struct InProcessStreamingRuntime {
    coordinator: Arc<Mutex<Coordinator>>,
    bridge: InProcessCoordinatorBridge,
    inbox: ExecutorAssignmentInbox,
    runner: Arc<ExecutorTaskRunner>,
    _executor_id: ExecutorId,
}

impl InProcessStreamingRuntime {
    /// Create and register a local in-process executor with the coordinator.
    pub fn new() -> RuntimeResult<Self> {
        let coordinator_id = CoordinatorId::try_new("in-process-coord")
            .map_err(|e| RuntimeError::transport(e.to_string()))?;
        let coordinator = Arc::new(Mutex::new(Coordinator::active(coordinator_id)));
        let executor_id = ExecutorId::try_new("in-process-exec")
            .map_err(|e| RuntimeError::transport(e.to_string()))?;
        let descriptor = ExecutorDescriptor::new(executor_id.clone(), "localhost", 8)
            .with_task_endpoint(IN_PROCESS_TASK_ENDPOINT);
        {
            let mut coord = coordinator
                .lock()
                .map_err(|_| RuntimeError::transport("coordinator lock poisoned"))?;
            coord
                .register_executor(descriptor)
                .map_err(|e| RuntimeError::transport(e.to_string()))?;
        }
        let inbox = ExecutorAssignmentInbox::new();
        let runner = Arc::new(ExecutorTaskRunner::new(inbox.clone()));
        let bridge = InProcessCoordinatorBridge::new(Arc::clone(&coordinator));
        Ok(Self {
            coordinator,
            bridge,
            inbox,
            runner,
            _executor_id: executor_id,
        })
    }

    /// Execute a windowed stream through the full coordinator → inbox → executor path.
    pub fn execute_windowed(
        &self,
        topic: &str,
        input_batches: Vec<RecordBatch>,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        if input_batches.is_empty() {
            return Ok(Vec::new());
        }
        let fragment = fragment_from_spec(spec);
        let job_id = next_job_id()?;
        let task_id =
            TaskId::try_new("task-0").map_err(|e| RuntimeError::transport(e.to_string()))?;
        let stage_id =
            StageId::try_new("stage-0").map_err(|e| RuntimeError::transport(e.to_string()))?;
        let job_spec = JobSpec::new(job_id.clone(), fragment.clone(), JobKind::Streaming).with_stage(
            StageSpec::new(stage_id, "stream-stage").with_task(TaskSpec::new(
                task_id.clone(),
                fragment,
            )),
        );

        let mut assignments = {
            let mut coord = self
                .coordinator
                .lock()
                .map_err(|_| RuntimeError::transport("coordinator lock poisoned"))?;
            match coord.submit_job(job_spec) {
                Ok(SubmitOutcome::Accepted) | Ok(SubmitOutcome::Queued { .. }) => {}
                Err(e) => return Err(RuntimeError::transport(e.to_string())),
            }
            coord
                .launch_assigned_task_assignments(&job_id)
                .map_err(|e| RuntimeError::transport(e.to_string()))?
        };

        if assignments.is_empty() {
            return Err(RuntimeError::transport(
                "in-process coordinator produced no task assignments",
            ));
        }

        let partitions: Vec<InputPartition> = input_batches
            .iter()
            .enumerate()
            .map(|(idx, batch)| {
                let desc = encode_stream_kafka_partition(
                    topic,
                    idx as u32,
                    0,
                    batch,
                    &spec.key_column,
                    &spec.event_time_column,
                )?;
                Ok(InputPartition::new(format!("stream-kafka-{idx}"), desc))
            })
            .collect::<RuntimeResult<Vec<_>>>()?;
        let assignment = assignments.remove(0).with_input_partitions(partitions);
        self.inbox
            .push(assignment)
            .map_err(|e| RuntimeError::transport(e.to_string()))?;

        let bridge = self.bridge.clone();
        let runner = Arc::clone(&self.runner);
        let mut output_batches = Vec::new();
        block_on(async {
            while let Some(report) = runner.run_next_with(&bridge).await.map_err(|e| {
                RuntimeError::transport(e.message())
            })? {
                output_batches.extend(report.output().record_batches().to_vec());
            }
            Ok::<(), RuntimeError>(())
        })?;

        Ok(output_batches)
    }
}

/// Run windowed aggregation via the in-process coordinator when mode is local.
pub fn execute_windowed_in_process(
    topic: &str,
    input_batches: Vec<RecordBatch>,
    spec: &LocalWindowExecutionSpec,
) -> RuntimeResult<Vec<RecordBatch>> {
    let runtime = InProcessStreamingRuntime::new()?;
    runtime.execute_windowed(topic, input_batches, spec)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::local_streaming::{LocalWindowExecutionSpec, LocalWindowKind};

    #[test]
    fn in_process_windowed_stream_returns_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as _,
                Arc::new(Int64Array::from(vec![1_000, 5_000])) as _,
            ],
        )
        .unwrap();
        let spec = LocalWindowExecutionSpec {
            key_column: "user_id".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: LocalWindowKind::Tumbling,
            window_size_ms: 10_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
        };
        let out = InProcessStreamingRuntime::new()
            .unwrap()
            .execute_windowed("events", vec![batch], &spec)
            .unwrap();
        assert!(!out.is_empty());
    }
}
