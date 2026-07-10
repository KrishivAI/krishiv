// ── P0.19: O(1) duplicate task-id detection tests ─────────────────────────

#[test]
fn validate_job_rejects_duplicate_task_ids() {
    let job_id = JobId::try_new("job-dup").unwrap();
    // Two stages both containing task-1 — duplicate across stages.
    let spec = JobSpec::new(job_id, "duplicate task ids", JobKind::Batch)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "s1")
                .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t1")),
        )
        .with_stage(
            StageSpec::new(StageId::try_new("stage-2").unwrap(), "s2")
                .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t1-dup")),
        );
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-dup").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-1").unwrap(),
            "pod-a",
            2,
        ))
        .unwrap();
    let result = coordinator.submit_job(spec);
    assert!(
        matches!(result, Err(SchedulerError::InvalidJob { .. })),
        "expected InvalidJob for duplicate task id, got {result:?}"
    );
}

#[test]
fn validate_job_accepts_large_unique_task_set() {
    // P0.19: Verify correct behaviour with 1000+ tasks using the HashSet path.
    let job_id = JobId::try_new("job-large").unwrap();
    const TASK_COUNT: usize = 1024;
    let mut stage = StageSpec::new(StageId::try_new("stage-big").unwrap(), "big stage");
    for i in 0..TASK_COUNT {
        stage = stage.with_task(TaskSpec::new(
            TaskId::try_new(format!("task-{i}")).unwrap(),
            format!("task {i}"),
        ));
    }
    let spec = JobSpec::new(job_id, "large unique task set", JobKind::Batch).with_stage(stage);
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-large").unwrap());
    // Register enough slots for all tasks.
    coordinator
        .register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-1").unwrap(),
            "pod-a",
            TASK_COUNT,
        ))
        .unwrap();
    assert!(
        coordinator.submit_job(spec).is_ok(),
        "1024 unique task ids must be accepted"
    );
}

#[test]
fn validate_job_rejects_cyclic_stage_deps() {
    // stage-1 → stage-2 → stage-1 creates a cycle; validate_job must reject it.
    let job_id = JobId::try_new("job-cycle").unwrap();
    let spec = JobSpec::new(job_id, "cyclic", JobKind::Batch)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "s1")
                .with_upstream_stage(StageId::try_new("stage-2").unwrap())
                .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t1")),
        )
        .with_stage(
            StageSpec::new(StageId::try_new("stage-2").unwrap(), "s2")
                .with_upstream_stage(StageId::try_new("stage-1").unwrap())
                .with_task(TaskSpec::new(TaskId::try_new("task-2").unwrap(), "t2")),
        );
    let result = crate::job::validate_job(&spec);
    assert!(
        matches!(result, Err(SchedulerError::InvalidJob { ref message }) if message.contains("cyclic")),
        "expected InvalidJob with 'cyclic' message, got {result:?}"
    );
}

#[test]
fn apply_status_update_rejects_attempt_zero() {
    let task_id = TaskId::try_new("task-attempt-zero").unwrap();
    let executor_id = ExecutorId::try_new("exec-zero").unwrap();
    let mut task = crate::job::TaskRecord::from_spec(krishiv_proto::TaskSpec::new(
        task_id.clone(),
        "some description",
    ));
    // task.attempt is 0 — simulate a status update arriving before launch.
    let update = TaskStatusUpdate::new(
        JobId::try_new("job-zero").unwrap(),
        StageId::try_new("stage-zero").unwrap(),
        task_id.clone(),
        executor_id,
        krishiv_proto::TaskState::Running,
        0,
    );
    let result = task.apply_status_update(&update);
    assert!(
        matches!(result, Err(SchedulerError::InvalidJob { .. })),
        "attempt 0 must return InvalidJob (never launched), got {result:?}"
    );
}

#[test]
fn in_memory_metadata_store_round_trips() {
    let coord_id = CoordinatorId::try_new("coord-1").unwrap();
    let job_id = JobId::try_new("job-1").unwrap();
    let mut store = crate::store::InMemoryMetadataStore::default();

    let event = EventLogEvent::JobSubmitted {
        job_id: job_id.clone(),
    };
    store.append_event(event.clone()).unwrap();
    assert_eq!(store.events().len(), 1);
    assert_eq!(store.events()[0], event);

    let mut coordinator = Coordinator::active(coord_id);
    coordinator
        .register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-1").unwrap(),
            "pod-a",
            2,
        ))
        .unwrap();
    coordinator.submit_job(demo_job()).unwrap();
    let record = coordinator
        .job_coordinators
        .values()
        .map(|jc| jc.read_record())
        .next()
        .unwrap();
    store.save_job(&record).unwrap();
    assert_eq!(store.jobs().len(), 1);
    assert_eq!(store.jobs()[0].job_id(), &job_id);

    // Overwrite with the same record is idempotent.
    store
        .save_job(
            &coordinator
                .job_coordinators
                .values()
                .map(|jc| jc.read_record())
                .next()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(store.jobs().len(), 1);
}

#[test]
fn single_node_election_is_always_leader() {
    let election = SingleNodeElection;
    assert!(election.is_leader());
}

#[test]
fn coordinator_recovers_jobs_from_store() {
    let coord_id = CoordinatorId::try_new("coord-1").unwrap();
    let job_id = JobId::try_new("job-1").unwrap();
    let mut store = crate::store::InMemoryMetadataStore::default();

    let mut prev = Coordinator::active(coord_id.clone());
    prev.register_executor(ExecutorDescriptor::new(
        ExecutorId::try_new("exec-1").unwrap(),
        "pod-a",
        2,
    ))
    .unwrap();
    prev.submit_job(demo_job()).unwrap();
    store
        .save_job(&prev.job_coordinators.values().next().unwrap().read_record())
        .unwrap();

    let mut coordinator = Coordinator::active(coord_id);
    coordinator.recover_from_store(&mut store).unwrap();
    let snapshot = coordinator.job_snapshot(&job_id).unwrap();
    assert_eq!(snapshot.state(), JobState::Running);
}

