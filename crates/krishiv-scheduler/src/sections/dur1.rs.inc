// DUR-1 regression: distributed-sink false success.
//
// A batch job with a staged object-sink must not be observed (or persisted)
// `Succeeded` until the staged output is durably published. The coordinator
// demotes such a job to the non-terminal `Committing` state before persisting,
// promotes it to `Succeeded` only after the publish completes, and re-drives a
// `Committing` job on restart (idempotently) rather than trusting a false
// success.

use krishiv_common::write_commit::staged_file_name;

/// Build a coordinator with an in-memory store holding one batch job whose
/// single task carries a staged object-parquet-sink contract, and stage one
/// output file for it. Returns (coordinator, job_id, base_dir, dest_rel).
fn dur1_fixture(base: &std::path::Path) -> (Coordinator, JobId, String) {
    let base_dir = base.to_str().unwrap().to_owned();
    let dest_rel = "out".to_owned();
    let job_id = JobId::try_new("dur1-sink-job").unwrap();
    let task_id = TaskId::try_new("task-0").unwrap();
    let contract = format!("object-parquet-sink:{base_dir}:{dest_rel}:mode=overwrite");

    let spec = JobSpec::new(job_id.clone(), "dur1", JobKind::Batch).with_stage(
        StageSpec::new(StageId::try_new("stage-0").unwrap(), "s")
            .with_task(TaskSpec::new(task_id.clone(), "sql: select 1").with_sink_contract(contract)),
    );

    // Stage one output file under <base>/out/_staging/<job_id>/<task>-0.parquet.
    let staging_dir = base.join(&dest_rel).join("_staging").join(job_id.as_str());
    std::fs::create_dir_all(&staging_dir).unwrap();
    std::fs::write(
        staging_dir.join(staged_file_name(task_id.as_str(), 0)),
        b"PAR1-staged-bytes",
    )
    .unwrap();

    let mut record = crate::job::JobRecord::from_spec(spec, 0);
    // The job's tasks have all succeeded; only the publish remains.
    record.state = JobState::Succeeded;

    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-dur1").unwrap())
            .with_store(crate::store::InMemoryMetadataStore::default());
    coordinator.job_coordinators.insert(
        job_id.clone(),
        Arc::new(crate::job_coordinator::JobCoordinator::new(
            job_id.clone(),
            record,
        )),
    );
    (coordinator, job_id, base_dir)
}

fn dur1_job_state(coordinator: &Coordinator, job_id: &JobId) -> JobState {
    coordinator
        .job_coordinators
        .get(job_id)
        .unwrap()
        .read_record()
        .state()
}

/// finalize_staged_sink_outputs demotes a Succeeded sink job to Committing (so
/// the persisted state is never a false Succeeded), and mark_sink_publish_committed
/// promotes it to Succeeded only after the publish — persisting the terminal
/// state durably.
#[test]
fn dur1_finalize_demotes_to_committing_then_commit_succeeds() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut coordinator, job_id, base_dir) = dur1_fixture(tmp.path());

    // The job is Succeeded in memory but its output is still staged.
    coordinator.finalize_staged_sink_outputs(&job_id);

    // DUR-1 invariant: the record is demoted to the non-terminal Committing
    // state before any persist, so a crash here recovers as Committing.
    assert_eq!(
        dur1_job_state(&coordinator, &job_id),
        JobState::Committing,
        "a Succeeded job with staged output must be demoted to Committing"
    );

    let mut work = coordinator.take_pending_sink_finalize();
    assert_eq!(work.len(), 1, "one publish work item queued");
    let single = work.pop().unwrap();
    assert_eq!(
        single.state,
        JobState::Succeeded,
        "the work carries the publish intent (Succeeded), not the record's live state"
    );

    // Run the publish, then resolve — mirrors the gRPC/in-process caller.
    assert!(single.execute());
    coordinator.mark_sink_publish_committed(&job_id);

    assert_eq!(
        dur1_job_state(&coordinator, &job_id),
        JobState::Succeeded,
        "commit promotes Committing -> Succeeded"
    );
    // Succeeded is persisted only now, after the publish.
    let persisted = {
        let handle = coordinator.store.as_ref().unwrap();
        let guard = handle.inner();
        guard
            .jobs()
            .iter()
            .find(|j| j.job_id() == &job_id)
            .map(|j| j.state())
    };
    assert_eq!(persisted, Some(JobState::Succeeded));

    // The staged file was published to the destination and staging removed.
    let dest_dir = std::path::Path::new(&base_dir).join("out");
    let published: Vec<_> = std::fs::read_dir(&dest_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
        .collect();
    assert_eq!(published.len(), 1, "one part file published to the destination");
    assert!(
        !dest_dir.join("_staging").join(job_id.as_str()).exists(),
        "staging directory removed after publish"
    );
}

/// A publish failure demotes the Committing job to a durable Failed — never a
/// silent success.
#[test]
fn dur1_publish_failure_marks_failed_durably() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut coordinator, job_id, _base) = dur1_fixture(tmp.path());
    coordinator.finalize_staged_sink_outputs(&job_id);
    assert_eq!(dur1_job_state(&coordinator, &job_id), JobState::Committing);

    // Simulate the publish having failed (drop the work without executing).
    let _ = coordinator.take_pending_sink_finalize();
    coordinator.mark_sink_publish_failed(&job_id);

    assert_eq!(dur1_job_state(&coordinator, &job_id), JobState::Failed);
    let persisted = {
        let handle = coordinator.store.as_ref().unwrap();
        let guard = handle.inner();
        guard
            .jobs()
            .iter()
            .find(|j| j.job_id() == &job_id)
            .map(|j| j.state())
    };
    assert_eq!(
        persisted,
        Some(JobState::Failed),
        "a publish failure must persist Failed, not leave a false Succeeded"
    );
}

/// The crash-window regression: a job persisted `Committing` (crash between the
/// durable state write and publish completion) is re-driven on restart — the
/// idempotent re-publish completes and the job resolves to a durable Succeeded.
#[test]
fn dur1_recovery_redrives_committing_job() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();
    let base_dir = base.to_str().unwrap().to_owned();
    let dest_rel = "out";
    let job_id = JobId::try_new("dur1-recover-job").unwrap();
    let task_id = TaskId::try_new("task-0").unwrap();
    let contract = format!("object-parquet-sink:{base_dir}:{dest_rel}:mode=overwrite");

    let spec = JobSpec::new(job_id.clone(), "dur1", JobKind::Batch).with_stage(
        StageSpec::new(StageId::try_new("stage-0").unwrap(), "s")
            .with_task(TaskSpec::new(task_id.clone(), "sql: select 1").with_sink_contract(contract)),
    );
    // Stage the output the crashed coordinator never got to publish.
    let staging_dir = base.join(dest_rel).join("_staging").join(job_id.as_str());
    std::fs::create_dir_all(&staging_dir).unwrap();
    std::fs::write(
        staging_dir.join(staged_file_name(task_id.as_str(), 0)),
        b"PAR1-staged-bytes",
    )
    .unwrap();

    // Persist a record in the mid-publish Committing state.
    let mut record = crate::job::JobRecord::from_spec(spec, 0);
    record.state = JobState::Committing;
    let mut store = crate::store::InMemoryMetadataStore::default();
    store.save_job(&record).unwrap();

    // Restart: a fresh coordinator recovers from the store and must re-drive.
    let mut recovered = Coordinator::active(CoordinatorId::try_new("coord-dur1-2").unwrap());
    recovered.recover_from_store(&mut store).unwrap();

    assert_eq!(
        dur1_job_state(&recovered, &job_id),
        JobState::Succeeded,
        "a Committing job must be re-driven to Succeeded on recovery, never left false-Succeeded"
    );
    // The resolved terminal state is persisted so a second restart does not
    // re-drive a job whose staging has already been consumed.
    let persisted = store
        .jobs()
        .iter()
        .find(|j| j.job_id() == &job_id)
        .map(|j| j.state());
    assert_eq!(persisted, Some(JobState::Succeeded));

    let dest_dir = base.join("out");
    let published = std::fs::read_dir(&dest_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
        .count();
    assert_eq!(published, 1, "recovery re-published the staged output");
}
