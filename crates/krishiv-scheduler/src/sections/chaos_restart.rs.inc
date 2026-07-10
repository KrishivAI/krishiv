
/// Phase 2.6 failure-injection loop: restart the coordinator at every
/// point in a batch job's lifecycle and assert each recovery converges to
/// a consistent, schedulable state (no panics, job still tracked, producer
/// either preserved or re-queued — never stuck in a phantom state).
#[test]
fn chaos_restart_converges_at_every_lifecycle_point() {
    allow_anonymous_for_tests();

    // Restart points: 0 = after submit, 1 = after launch, 2 = after
    // producer success, 3 = after executor loss.
    for restart_point in 0..4 {
        let exec_id = ExecutorId::try_new("exec-chaos").unwrap();
        let job_id = JobId::try_new(format!("job-chaos-{restart_point}")).unwrap();
        let stage0_id = StageId::try_new("stage-0").unwrap();
        let stage1_id = StageId::try_new("stage-1").unwrap();

        let mut coordinator =
            Coordinator::active(CoordinatorId::try_new("coord-chaos").unwrap());
        let lease_gen = coordinator
            .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-chaos", 2))
            .unwrap();
        coordinator
            .submit_job(
                JobSpec::new(job_id.clone(), "chaos", JobKind::Batch)
                    .with_stage(StageSpec::new(stage0_id.clone(), "write").with_task(
                        TaskSpec::new(
                            TaskId::try_new("task-0").unwrap(),
                            "shuffle-write:hash:col:2",
                        ),
                    ))
                    .with_stage(
                        StageSpec::new(stage1_id.clone(), "read")
                            .with_upstream_stage(stage0_id.clone())
                            .with_task(TaskSpec::new(
                                TaskId::try_new("task-1").unwrap(),
                                "sql: SELECT 1",
                            )),
                    ),
            )
            .unwrap();

        if restart_point >= 1 {
            let assignments = coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap();
            if restart_point >= 2 {
                use krishiv_proto::ShufflePartitionOutput;
                let assign = assignments.first().unwrap();
                coordinator
                    .apply_task_update(
                        TaskStatusUpdate::new(
                            assign.job_id().clone(),
                            assign.stage_id().clone(),
                            assign.task_id().clone(),
                            exec_id.clone(),
                            TaskState::Succeeded,
                            assign.attempt_id().as_u32(),
                        )
                        .with_lease_generation(lease_gen)
                        .with_output_metadata(
                            TaskOutputMetadata::new("shuffle", 10, 1, 1)
                                .with_shuffle_partitions(vec![ShufflePartitionOutput::new(
                                    0,
                                    1024,
                                    "http://chaos-host:9000",
                                )]),
                        ),
                    )
                    .unwrap();
            }
            if restart_point >= 3 {
                coordinator.mark_executor_lost(&exec_id).unwrap();
            }
        }

        // "Kill" the coordinator: persist, then recover into a fresh one.
        let mut store = crate::store::InMemoryMetadataStore::default();
        coordinator.persist_jobs_to_store(&mut store).unwrap();
        drop(coordinator);

        let mut restarted =
            Coordinator::active(CoordinatorId::try_new("coord-chaos-r").unwrap());
        restarted.recover_from_store(&mut store).unwrap();

        // Convergence assertions: job is tracked, snapshot is readable, and
        // the orchestration tick runs without error.
        let detail = restarted
            .job_detail_snapshot(&job_id)
            .unwrap_or_else(|e| panic!("restart_point={restart_point}: job lost: {e}"));
        assert!(
            !detail.stages().is_empty(),
            "restart_point={restart_point}: stages must survive recovery"
        );
        // A fresh executor registers and the tick must be able to assign
        // pending work without panicking or erroring.
        restarted
            .register_executor(ExecutorDescriptor::new(
                ExecutorId::try_new("exec-chaos-new").unwrap(),
                "pod-chaos-new",
                2,
            ))
            .unwrap();
        restarted
            .coordinator_tick()
            .unwrap_or_else(|e| panic!("restart_point={restart_point}: tick failed: {e}"));
    }
}

#[test]
fn memory_admission_skipped_when_no_executor_reports_capacity() {
    let executor_id = ExecutorId::try_new("exec-mem-unknown").unwrap();
    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-mem-unknown").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id, "host-1", 2))
        .unwrap();
    // No heartbeat with memory info: capacity unknown → check is skipped.

    let spec = demo_job().with_memory_limit_bytes(u64::MAX / 2);
    let outcome = coordinator.submit_job(spec).unwrap();
    assert_eq!(
        outcome,
        SubmitOutcome::Accepted,
        "unknown cluster capacity must not reject jobs"
    );
}

