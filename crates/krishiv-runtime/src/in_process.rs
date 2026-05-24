//! In-process coordinator + executor over shared mpsc/inbox transport (ADR-12.4).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use krishiv_async_util::block_on;
use krishiv_executor::{ExecutorAssignmentInbox, ExecutorTaskRunner};
use krishiv_plan::window::{encode_stream_fragment, WindowExecutionSpec};
use krishiv_proto::{
    CoordinatorId, ExecutorDescriptor, ExecutorId, InputPartition, JobId, JobKind, JobSpec,
    StageId, StageSpec, TaskId, TaskSpec,
};
use krishiv_scheduler::{
    Coordinator, InProcessCoordinatorBridge, IN_PROCESS_TASK_ENDPOINT, SubmitOutcome,
};

use crate::in_process_cluster::plan_spec_to_local;
use crate::local_streaming::LocalWindowExecutionSpec;
use crate::stream_kafka::encode_stream_kafka_partition;
use crate::{RuntimeError, RuntimeResult};

static JOB_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_job_id() -> RuntimeResult<JobId> {
    let n = JOB_COUNTER.fetch_add(1, Ordering::Relaxed);
    JobId::try_new(format!("in-process-job-{n}"))
        .map_err(|e| RuntimeError::transport(e.to_string()))
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

    pub fn execute_windowed(
        &self,
        topic: &str,
        input_batches: Vec<RecordBatch>,
        spec: &WindowExecutionSpec,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        if input_batches.is_empty() {
            return Ok(Vec::new());
        }
        let fragment = encode_stream_fragment(spec);
        let local = plan_spec_to_local(spec);
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

        let value_column = sum_value_column_from_aggs(&local);
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
                    value_column.as_deref(),
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

    /// Execute using the API-local window spec type.
    /// Stable identity for session-scoped coordinator reuse tests.
    pub fn coordinator_instance_id(&self) -> usize {
        Arc::as_ptr(&self.coordinator) as usize
    }

    pub fn execute_windowed_local(
        &self,
        topic: &str,
        input_batches: Vec<RecordBatch>,
        spec: &LocalWindowExecutionSpec,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        use crate::in_process_cluster::local_spec_to_plan_spec;
        self.execute_windowed(topic, input_batches, &local_spec_to_plan_spec(spec))
    }
}

fn sum_value_column_from_aggs(spec: &LocalWindowExecutionSpec) -> Option<String> {
    use krishiv_exec::AggFunction;
    spec.agg_exprs.iter().find_map(|a| {
        if a.function == AggFunction::Sum && !a.input_column.is_empty() {
            Some(a.input_column.clone())
        } else {
            None
        }
    })
}

/// Run windowed aggregation via a session-scoped cluster (preferred).
pub fn execute_windowed_in_process(
    cluster: &crate::InProcessCluster,
    topic: &str,
    input_batches: Vec<RecordBatch>,
    spec: &LocalWindowExecutionSpec,
) -> RuntimeResult<Vec<RecordBatch>> {
    cluster.collect_bounded_window(topic, input_batches, spec)
}

/// Legacy entry: creates an ephemeral in-process cluster (tests only).
pub fn execute_windowed_in_process_ephemeral(
    topic: &str,
    input_batches: Vec<RecordBatch>,
    spec: &LocalWindowExecutionSpec,
) -> RuntimeResult<Vec<RecordBatch>> {
    let cluster = crate::InProcessCluster::new()?;
    cluster.collect_bounded_window(topic, input_batches, spec)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::in_process_cluster::InProcessCluster;
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
        let cluster = InProcessCluster::new().unwrap();
        let out = cluster
            .collect_bounded_window("events", vec![batch], &spec)
            .unwrap();
        assert!(!out.is_empty());
    }
}
