//! In-process coordinator + executor over shared mpsc/inbox transport (ADR-12.4).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use krishiv_async_util::block_on;
use krishiv_executor::{ContinuousJobDrainer, ExecutorAssignmentInbox, ExecutorTaskRunner};
use krishiv_plan::window::{WindowExecutionSpec, encode_stream_fragment};
use krishiv_proto::{
    CoordinatorId, ExecutorDescriptor, ExecutorId, InputPartition, InputPartitionDescriptor, JobId,
    JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec,
};
use krishiv_scheduler::{
    Coordinator, IN_PROCESS_TASK_ENDPOINT, InProcessCoordinatorBridge, SubmitOutcome,
};

use crate::continuous_stream::{ContinuousStreamRegistry, SharedContinuousStreamRegistry};
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

/// Parquet table registration forwarded to executor SQL tasks.
#[derive(Debug, Clone)]
pub struct BatchSqlTable {
    pub table_name: String,
    pub path: PathBuf,
}

struct RegistryDrainer(Arc<ContinuousStreamRegistry>);

impl ContinuousJobDrainer for RegistryDrainer {
    fn drain_job(&self, job_id: &str) -> Result<Vec<RecordBatch>, String> {
        self.0.drain_job(job_id).map_err(|e| e.to_string())
    }
}

/// Shared in-process streaming runtime (coordinator + executor inbox).
#[derive(Clone)]
pub struct InProcessStreamingRuntime {
    coordinator: Arc<Mutex<Coordinator>>,
    bridge: InProcessCoordinatorBridge,
    inbox: ExecutorAssignmentInbox,
    runner: Arc<ExecutorTaskRunner>,
    continuous_registry: SharedContinuousStreamRegistry,
    _executor_id: ExecutorId,
}

impl InProcessStreamingRuntime {
    pub fn new() -> RuntimeResult<Self> {
        Self::with_continuous_registry(Arc::new(ContinuousStreamRegistry::new()))
    }

    pub fn with_continuous_registry(
        registry: SharedContinuousStreamRegistry,
    ) -> RuntimeResult<Self> {
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
        let drainer = Arc::new(RegistryDrainer(Arc::clone(&registry)));
        let runner =
            Arc::new(ExecutorTaskRunner::new(inbox.clone()).with_continuous_drainer(drainer));
        let bridge = InProcessCoordinatorBridge::new(Arc::clone(&coordinator));
        Ok(Self {
            coordinator,
            bridge,
            inbox,
            runner,
            continuous_registry: registry,
            _executor_id: executor_id,
        })
    }

    pub fn continuous_registry(&self) -> &ContinuousStreamRegistry {
        &self.continuous_registry
    }

    /// Register a continuous streaming job (window operator state retained in registry).
    pub fn register_continuous_job(
        &self,
        job_id: &str,
        spec: WindowExecutionSpec,
    ) -> RuntimeResult<()> {
        self.continuous_registry.register_job(job_id, spec)
    }

    /// Push input batches for a continuous job before draining via coordinator.
    pub fn push_continuous_input(
        &self,
        job_id: &str,
        batches: Vec<RecordBatch>,
    ) -> RuntimeResult<()> {
        self.continuous_registry.push_input(job_id, batches)
    }

    /// Execute batch SQL through coordinator → executor (`sql:` fragment).
    pub fn execute_batch_sql(
        &self,
        query: &str,
        tables: &[BatchSqlTable],
    ) -> RuntimeResult<Vec<RecordBatch>> {
        let fragment = format!("sql: {query}");
        self.run_terminal_task(&fragment, JobKind::Batch, tables, Vec::new())
    }

    /// Drain a continuous streaming job through coordinator → executor.
    pub fn drain_continuous_job(&self, job_id: &str) -> RuntimeResult<Vec<RecordBatch>> {
        let fragment = format!("stream:continuous:{job_id}");
        self.run_terminal_task(&fragment, JobKind::Streaming, &[], Vec::new())
    }

    fn run_terminal_task(
        &self,
        fragment: &str,
        kind: JobKind,
        tables: &[BatchSqlTable],
        stream_partitions: Vec<InputPartition>,
    ) -> RuntimeResult<Vec<RecordBatch>> {
        let job_id = next_job_id()?;
        let task_id =
            TaskId::try_new("task-0").map_err(|e| RuntimeError::transport(e.to_string()))?;
        let stage_id =
            StageId::try_new("stage-0").map_err(|e| RuntimeError::transport(e.to_string()))?;
        let job_spec = JobSpec::new(job_id.clone(), fragment.to_string(), kind).with_stage(
            StageSpec::new(stage_id, "stage-0")
                .with_task(TaskSpec::new(task_id.clone(), fragment.to_string())),
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

        let mut partitions: Vec<InputPartition> = tables
            .iter()
            .enumerate()
            .map(|(idx, table)| {
                InputPartition::new(format!("local-parquet-{idx}"), String::new()).with_descriptor(
                    InputPartitionDescriptor::LocalParquet {
                        table_name: table.table_name.clone(),
                        path: table.path.to_string_lossy().into_owned(),
                    },
                )
            })
            .collect();
        partitions.extend(stream_partitions);

        let assignment = assignments.remove(0).with_input_partitions(partitions);
        self.inbox
            .push(assignment)
            .map_err(|e| RuntimeError::transport(e.to_string()))?;

        let bridge = self.bridge.clone();
        let runner = Arc::clone(&self.runner);
        let mut output_batches = Vec::new();
        block_on(async {
            while let Some(report) = runner
                .run_next_with(&bridge)
                .await
                .map_err(|e| RuntimeError::transport(e.message()))?
            {
                output_batches.extend(report.output().record_batches().to_vec());
            }
            Ok::<(), RuntimeError>(())
        })?;

        Ok(output_batches)
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
        self.run_terminal_task(&fragment, JobKind::Streaming, &[], partitions)
    }

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
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        };
        let cluster = InProcessCluster::new().unwrap();
        let out = cluster
            .collect_bounded_window("events", vec![batch], &spec)
            .unwrap();
        assert!(!out.is_empty());
    }

    #[test]
    fn batch_sql_routes_through_coordinator() {
        let runtime = InProcessStreamingRuntime::new().expect("runtime");
        let batches = runtime
            .execute_batch_sql("SELECT 1 AS value", &[])
            .expect("batch sql");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn continuous_job_drains_via_coordinator() {
        let runtime = InProcessStreamingRuntime::new().expect("runtime");
        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        runtime
            .register_continuous_job("events", spec)
            .expect("register");
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int64Array::from(vec![1_000])) as _,
            ],
        )
        .unwrap();
        runtime
            .push_continuous_input("events", vec![batch])
            .expect("push");
        let _ = runtime.drain_continuous_job("events").expect("drain");
    }
}
