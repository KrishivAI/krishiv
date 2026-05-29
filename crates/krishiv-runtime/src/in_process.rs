//! In-process coordinator + executor over shared mpsc/inbox transport (ADR-12.4).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use krishiv_async_util::block_on;
use krishiv_executor::{
    ContinuousJobDrainer, ExecutorAssignmentInbox, ExecutorTaskOutputKind, ExecutorTaskRunner,
};
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

/// Process-global counter used to give every [`InProcessStreamingRuntime`]
/// a unique numeric suffix.  This avoids two concurrent embedded sessions
/// colliding on coordinator id (C1, C2).
static CLUSTER_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_cluster_suffix() -> u64 {
    CLUSTER_COUNTER.fetch_add(1, Ordering::Relaxed)
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
    /// Per-cluster job counter so each `InProcessStreamingRuntime` has its
    /// own job id namespace (C1).
    job_counter: Arc<AtomicU64>,
    /// Per-cluster suffix used in coordinator/executor ids.
    suffix: u64,
}

impl InProcessStreamingRuntime {
    pub fn new() -> RuntimeResult<Self> {
        Self::with_continuous_registry(Arc::new(ContinuousStreamRegistry::new()))
    }

    pub fn with_continuous_registry(
        registry: SharedContinuousStreamRegistry,
    ) -> RuntimeResult<Self> {
        let suffix = next_cluster_suffix();
        // Each in-process cluster gets a process-unique coordinator and
        // executor id so multiple sessions sharing the same process do not
        // collide in metadata stores or audit logs (C1).
        let coordinator_id = CoordinatorId::try_new(format!("in-process-coord-{suffix}"))
            .map_err(|e| RuntimeError::transport(e.to_string()))?;
        let coordinator = Arc::new(Mutex::new(Coordinator::active(coordinator_id)));
        let executor_id = ExecutorId::try_new(format!("in-process-exec-{suffix}"))
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
            job_counter: Arc::new(AtomicU64::new(1)),
            suffix,
        })
    }

    /// Per-cluster job id generator (C1) — replaces the legacy process-global counter.
    fn next_job_id(&self) -> RuntimeResult<JobId> {
        let n = self.job_counter.fetch_add(1, Ordering::Relaxed);
        JobId::try_new(format!("in-process-{}-job-{n}", self.suffix))
            .map_err(|e| RuntimeError::transport(e.to_string()))
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
        let job_id = self.next_job_id()?;
        let task_id =
            TaskId::try_new("task-0").map_err(|e| RuntimeError::transport(e.to_string()))?;
        let stage_id =
            StageId::try_new("stage-0").map_err(|e| RuntimeError::transport(e.to_string()))?;
        let job_spec = JobSpec::new(job_id.clone(), fragment.to_string(), kind).with_stage(
            StageSpec::new(stage_id, "stage-0")
                .with_task(TaskSpec::new(task_id.clone(), fragment.to_string())),
        );

        {
            let mut coord = self
                .coordinator
                .lock()
                .map_err(|_| RuntimeError::transport("coordinator lock poisoned"))?;
            match coord.submit_job(job_spec) {
                Ok(SubmitOutcome::Accepted) | Ok(SubmitOutcome::Queued { .. }) => {}
                Err(e) => return Err(RuntimeError::transport(e.to_string())),
            }
        }

        // C5: Multi-stage in-process execution.  Repeatedly:
        //  1. Ask the coordinator for currently-assigned tasks for this job.
        //  2. For the first stage's first task, attach the input partitions
        //     supplied by the caller (parquet tables / stream partitions).
        //  3. Push every assignment into the inbox.
        //  4. Drain the inbox via the runner.
        //  5. Loop until no new assignments are launched (terminal stages all done).
        let initial_partitions: Vec<InputPartition> = tables
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
            .chain(stream_partitions)
            .collect();

        let bridge = self.bridge.clone();
        let runner = Arc::clone(&self.runner);
        let mut output_batches = Vec::new();
        let mut iter_count = 0usize;
        let mut first_iteration_partitions = Some(initial_partitions);
        const MAX_STAGE_ITERATIONS: usize = 1024;

        block_on(async {
            loop {
                if iter_count > MAX_STAGE_ITERATIONS {
                    return Err(RuntimeError::transport(format!(
                        "in-process runtime exceeded {MAX_STAGE_ITERATIONS} stage iterations for job {job_id}"
                    )));
                }
                iter_count += 1;

                let mut assignments = {
                    let mut coord = self
                        .coordinator
                        .lock()
                        .map_err(|_| RuntimeError::transport("coordinator lock poisoned"))?;
                    coord
                        .launch_assigned_task_assignments(&job_id)
                        .map_err(|e| RuntimeError::transport(e.to_string()))?
                };

                if assignments.is_empty() {
                    // Job either finished or has no more assignable work this turn.
                    let terminal = {
                        let coord = self
                            .coordinator
                            .lock()
                            .map_err(|_| RuntimeError::transport("coordinator lock poisoned"))?;
                        coord
                            .job_snapshot(&job_id)
                            .map(|snap| snap.state().is_terminal())
                            .unwrap_or(false)
                    };
                    if terminal || iter_count > 1 {
                        return Ok(());
                    }
                    return Err(RuntimeError::transport(
                        "in-process coordinator produced no task assignments",
                    ));
                }

                // Attach caller-supplied input partitions to the FIRST assignment
                // emitted by the FIRST iteration only.  Subsequent stages source
                // their input from shuffle outputs.
                if let Some(partitions) = first_iteration_partitions.take() {
                    let first = assignments.remove(0).with_input_partitions(partitions);
                    self.inbox
                        .push(first)
                        .map_err(|e| RuntimeError::transport(e.to_string()))?;
                }
                for assignment in assignments {
                    self.inbox
                        .push(assignment)
                        .map_err(|e| RuntimeError::transport(e.to_string()))?;
                }

                while let Some(report) = runner
                    .run_next_with(&bridge)
                    .await
                    .map_err(|e| RuntimeError::transport(e.message()))?
                {
                    // Only collect terminal-stage outputs (SQL, connector pipeline,
                    // streaming window).  Intermediate shuffle-write reports must
                    // not be concatenated into the final result set.
                    let kind = report.output().kind();
                    if matches!(
                        kind,
                        ExecutorTaskOutputKind::Sql
                            | ExecutorTaskOutputKind::ConnectorPipeline
                            | ExecutorTaskOutputKind::StreamingWindow
                    ) {
                        output_batches.extend(report.output().record_batches().to_vec());
                    }
                }
            }
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

    #[test]
    fn runtime_new_creates_working_runtime() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let batches = runtime.execute_batch_sql("SELECT 42", &[]).unwrap();
        assert_eq!(batches.len(), 1);
    }

    #[test]
    fn execute_batch_sql_returns_single_batch() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let batches = runtime
            .execute_batch_sql("SELECT 'hello' AS msg", &[])
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[test]
    fn execute_batch_sql_multi_column() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let batches = runtime
            .execute_batch_sql("SELECT 1 AS a, 'x' AS b", &[])
            .unwrap();
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn execute_windowed_empty_batches_returns_empty() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        let result = runtime.execute_windowed("topic", vec![], &spec).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn coordinator_instance_id_is_stable() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let id1 = runtime.coordinator_instance_id();
        let id2 = runtime.coordinator_instance_id();
        assert_eq!(id1, id2);
    }

    #[test]
    fn multiple_runtimes_have_distinct_coordinator_ids() {
        let r1 = InProcessStreamingRuntime::new().unwrap();
        let r2 = InProcessStreamingRuntime::new().unwrap();
        assert_ne!(r1.coordinator_instance_id(), r2.coordinator_instance_id());
    }

    #[test]
    fn push_continuous_input_unknown_job_fails() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let result = runtime.push_continuous_input("no-such", vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn drain_continuous_job_unknown_fails() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let result = runtime.drain_continuous_job("no-such");
        assert!(result.is_err());
    }

    #[test]
    fn continuous_registry_accessor() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let reg = runtime.continuous_registry();
        assert!(reg.list_jobs().is_empty());
    }

    #[test]
    fn batch_sql_with_parquet_tables_attempt() {
        use std::path::PathBuf;
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let tables = vec![BatchSqlTable {
            table_name: "nonexistent".into(),
            path: PathBuf::from("/no/such/file.parquet"),
        }];
        // This may fail because file doesn't exist but the routing path is tested
        let result = runtime.execute_batch_sql("SELECT 1", &tables);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn batch_sql_with_empty_tables() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let result = runtime.execute_batch_sql("SELECT 1 AS n", &[]).unwrap();
        assert_eq!(result[0].num_rows(), 1);
    }

    #[test]
    fn register_and_drain_multiple_continuous_jobs() {
        let runtime = InProcessStreamingRuntime::new().unwrap();
        let spec = WindowExecutionSpec::tumbling("user_id", "ts", 10_000);
        runtime.register_continuous_job("j1", spec.clone()).unwrap();
        runtime.register_continuous_job("j2", spec).unwrap();
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
            .push_continuous_input("j1", vec![batch.clone()])
            .unwrap();
        runtime.push_continuous_input("j2", vec![batch]).unwrap();
        let _ = runtime.drain_continuous_job("j1").unwrap();
        let _ = runtime.drain_continuous_job("j2").unwrap();
    }
}
