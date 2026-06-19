//! Job records, snapshots, and scheduling helpers.

mod record;
mod scheduler;
mod snapshot;

pub use record::{JobRecord, StageRecord, TaskRecord};
pub(crate) use scheduler::validate_job;
pub use scheduler::{
    NamespaceQuotaSnapshot, ResourceUsage, SlotAwareScheduler, StaticScheduler, SubmitOutcome,
    job_spec_from_logical_plan, job_spec_from_physical_plan,
};
pub use snapshot::{JobDetailSnapshot, JobSnapshot, StabilityMetrics, StageSnapshot, TaskSnapshot};

#[cfg(test)]
mod exchange_stage_tests {
    use super::record::JobRecord;
    use super::scheduler::{job_spec_from_physical_plan, topo_sort_plan_nodes};
    use crate::job::record::key_group_range_for_task;
    use krishiv_plan::{
        ExecutionKind, NodeOp, Partitioning, PhysicalPlan, PlanNode, TypedTaskFragment,
    };
    use krishiv_proto::{
        ExecutorId, InputPartition, JobId, JobKind, JobSpec, LeaseGeneration, StageId, StageSpec,
        TaskAssignment, TaskId, TaskSpec,
    };

    use crate::SchedulerError;

    #[test]
    fn physical_plan_with_exchange_produces_multi_stage_job() {
        let scan = PlanNode::new("scan", "scan", ExecutionKind::Batch).with_op(NodeOp::Scan {
            table: String::from("t"),
            filters: vec![],
        });
        let exchange = PlanNode::new("ex", "exchange", ExecutionKind::Batch)
            .with_inputs(["scan"])
            .with_op(NodeOp::Exchange {
                partitioning: Partitioning::Hash {
                    keys: vec![String::from("k")],
                    buckets: 2,
                },
            });
        let agg = PlanNode::new("agg", "aggregate", ExecutionKind::Batch)
            .with_inputs(["ex"])
            .with_op(NodeOp::Aggregate {
                group_keys: vec![String::from("k")],
            });
        let plan = PhysicalPlan::new("exchange-plan", ExecutionKind::Batch)
            .with_node(scan)
            .with_node(exchange)
            .with_node(agg);
        let job_id = JobId::try_new("job-exchange-test").unwrap();
        let spec = job_spec_from_physical_plan(job_id, &plan).unwrap();
        assert_eq!(spec.stages().len(), 2);
        assert_eq!(
            spec.stages()[1].upstream_stage_ids().len(),
            1,
            "downstream stage must declare upstream dependency"
        );
    }

    #[test]
    fn physical_plan_conversion_rejects_invalid_graph() {
        let plan = PhysicalPlan::new("invalid", ExecutionKind::Batch).with_node(
            PlanNode::new("sink", "sink", ExecutionKind::Batch).with_inputs(["missing"]),
        );
        let job_id = JobId::try_new("job-invalid-plan").unwrap();

        let error = job_spec_from_physical_plan(job_id, &plan).expect_err("invalid graph");

        assert!(matches!(error, SchedulerError::InvalidPlan { .. }));
        assert!(error.to_string().contains("missing input 'missing'"));
    }

    #[test]
    fn topological_sort_handles_duplicate_edges() {
        let nodes = vec![
            PlanNode::new("scan", "scan", ExecutionKind::Batch),
            PlanNode::new("self-join", "self join", ExecutionKind::Batch)
                .with_inputs(["scan", "scan"])
                .with_op(NodeOp::Join {
                    join_type: krishiv_plan::JoinType::Inner,
                }),
        ];

        let ordered = topo_sort_plan_nodes(&nodes).expect("topological order");

        assert_eq!(
            ordered.iter().map(|node| node.id()).collect::<Vec<_>>(),
            vec!["scan", "self-join"]
        );
    }

    #[test]
    fn key_group_ranges_split_stage_parallelism() {
        let first = key_group_range_for_task(0, 4);
        let second = key_group_range_for_task(1, 4);
        let last = key_group_range_for_task(3, 4);

        assert_eq!((first.start(), first.end()), (0, 8191));
        assert_eq!((second.start(), second.end()), (8192, 16383));
        assert_eq!((last.start(), last.end()), (24576, 32767));
    }

    #[test]
    fn typed_continuous_loop_assignment_requires_reattach() {
        let job_id = JobId::try_new("continuous-assignment-job").unwrap();
        let stage_id = StageId::try_new("continuous-stage").unwrap();
        let task_id = TaskId::try_new("continuous-task").unwrap();
        let executor_id = ExecutorId::try_new("continuous-executor").unwrap();
        let fragment = TypedTaskFragment::new(
            ExecutionKind::Streaming,
            "stream:loop:continuous-assignment-job|\
             stream:tw:key=key:time=ts:win=10000:lag=0:agg=count",
        )
        .encode()
        .unwrap();
        let spec = JobSpec::new(job_id, "continuous", JobKind::Streaming).with_stage(
            StageSpec::new(stage_id, "continuous-stage")
                .with_task(TaskSpec::new(task_id.clone(), fragment)),
        );
        let mut job = JobRecord::from_spec(spec, 1);
        job.apply_assignments(vec![TaskAssignment::new(task_id, executor_id.clone())]);
        let input = vec![InputPartition::new("cycle-input", "inline")];

        let assignments = job
            .launch_assigned_task_assignments(
                &[(executor_id, LeaseGeneration::initial())],
                None,
                Some(&input),
                None,
                None,
            )
            .unwrap();

        assert_eq!(assignments.len(), 1);
        assert!(assignments[0].requires_reattach());
    }

    #[test]
    fn task_scoped_inputs_are_bound_to_the_matching_task() {
        let job_id = JobId::try_new("task-scoped-input-job").unwrap();
        let stage_id = StageId::try_new("task-scoped-stage").unwrap();
        let task_a = TaskId::try_new("task-a").unwrap();
        let task_b = TaskId::try_new("task-b").unwrap();
        let executor_a = ExecutorId::try_new("executor-a").unwrap();
        let executor_b = ExecutorId::try_new("executor-b").unwrap();
        let spec = JobSpec::new(job_id, "task-scoped", JobKind::Batch).with_stage(
            StageSpec::new(stage_id, "stage")
                .with_task(TaskSpec::new(task_a.clone(), "window:a"))
                .with_task(TaskSpec::new(task_b.clone(), "window:b")),
        );
        let mut job = JobRecord::from_spec(spec, 1);
        job.apply_assignments(vec![
            TaskAssignment::new(task_a.clone(), executor_a.clone()),
            TaskAssignment::new(task_b.clone(), executor_b.clone()),
        ]);
        let task_inputs = std::collections::HashMap::from([
            (
                task_a.clone(),
                vec![InputPartition::new("input-a", "partition-a")],
            ),
            (
                task_b.clone(),
                vec![InputPartition::new("input-b", "partition-b")],
            ),
        ]);

        let assignments = job
            .launch_assigned_task_assignments(
                &[
                    (executor_a, LeaseGeneration::initial()),
                    (executor_b, LeaseGeneration::initial()),
                ],
                None,
                None,
                Some(&task_inputs),
                None,
            )
            .unwrap();
        assert_eq!(assignments.len(), 2);
        for assignment in assignments {
            let expected_partition = if assignment.task_id() == &task_a {
                "input-a"
            } else {
                assert_eq!(assignment.task_id(), &task_b);
                "input-b"
            };
            assert_eq!(assignment.input_partitions().len(), 1);
            assert_eq!(
                assignment.input_partitions()[0].partition_id(),
                expected_partition
            );
        }
    }
}
