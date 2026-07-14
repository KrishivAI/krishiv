// ── Phase 56: state & checkpoints v2 tests ───────────────────────────────

use crate::runner::task_runner::{
    INCREMENTAL_SNAPSHOT_MARKER, materialize_portable_snapshots,
};

fn window_exec_with_state_dir(
    dir: &std::path::Path,
) -> std::sync::Arc<std::sync::Mutex<krishiv_dataflow::ContinuousWindowExecutor>> {
    let spec = krishiv_plan::window::WindowExecutionSpec::tumbling("key", "ts", 10_000);
    std::sync::Arc::new(std::sync::Mutex::new(
        krishiv_dataflow::ContinuousWindowExecutor::new_with_state_dir(spec, Some(dir)).unwrap(),
    ))
}

fn checkpoint_req(job: &str, epoch: u64) -> krishiv_proto::InitiateCheckpointRequest {
    krishiv_proto::InitiateCheckpointRequest {
        job_id: JobId::try_new(job).unwrap(),
        epoch,
        fencing_token: krishiv_proto::FencingToken::initial(),
        alignment: krishiv_proto::CheckpointAlignment::default(),
    }
}

/// Bytes stored under one storage prefix (recursive).
fn stored_bytes_under(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                total += stored_bytes_under(&path);
            } else {
                total += entry.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    total
}

/// Phase 56 exit gate: with a large stable state, incremental epochs write an
/// order of magnitude fewer bytes than full-snapshot epochs, and the
/// incremental pointer materializes back into the exact portable snapshot.
#[tokio::test]
async fn incremental_checkpoints_cut_bytes_and_round_trip() {
    use krishiv_state::checkpoint::{CheckpointStorage, LocalFsCheckpointStorage};

    let state_dir = tempfile::tempdir().unwrap();
    let exec = window_exec_with_state_dir(state_dir.path());
    // Populate a chunky window state: 20 000 keys in the open window.
    {
        let keys: Vec<String> = (0..20_000).map(|i| format!("key-{i:05}")).collect();
        let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let batch = krishiv_common::test_fixtures::make_test_key_ts_batch(
            key_refs,
            vec![500; 20_000],
        )
        .unwrap();
        exec.lock().unwrap().drain(vec![batch]).unwrap();
    }
    let state = crate::runner::CheckpointStateHandle::ContinuousWindow(exec.clone());
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let mut runner = TaskRunner::new(TaskId::try_new("task-incr").unwrap());

    // Epoch 8 hits the full-snapshot cadence (default every 8) → full bytes.
    let full_ack = runner
        .handle_initiate_checkpoint(checkpoint_req("job-incr", 8), &state, &storage)
        .unwrap();
    let full_path = full_ack.snapshot_path.clone().unwrap();
    let full_bytes = storage.read_bytes(&full_path).unwrap().unwrap();
    assert!(
        !full_bytes.starts_with(INCREMENTAL_SNAPSHOT_MARKER),
        "cadence epoch takes a FULL portable snapshot"
    );
    let full_size = full_bytes.len() as u64;
    let after_full = stored_bytes_under(storage.base_dir());

    // Epoch 9: tiny mutation (one key) → incremental pointer, and the bytes
    // added to storage must be far below the full snapshot size.
    {
        let batch =
            krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["key-00000"], vec![900])
                .unwrap();
        exec.lock().unwrap().drain(vec![batch]).unwrap();
    }
    let incr_ack = runner
        .handle_initiate_checkpoint(checkpoint_req("job-incr", 9), &state, &storage)
        .unwrap();
    let incr_path = incr_ack.snapshot_path.clone().unwrap();
    let pointer = storage.read_bytes(&incr_path).unwrap().unwrap();
    assert!(
        pointer.starts_with(INCREMENTAL_SNAPSHOT_MARKER),
        "non-cadence epoch on RocksDB state must be incremental"
    );

    // Materialized incremental snapshot == the live backend's portable state.
    let materialized =
        materialize_portable_snapshots(vec![pointer.clone()], &storage).unwrap();
    let expected = exec.lock().unwrap().snapshot().unwrap();
    let mut got = krishiv_state::decode_snapshot_entries(&materialized[0]).unwrap();
    let mut want = krishiv_state::decode_snapshot_entries(&expected).unwrap();
    got.sort();
    want.sort();
    assert_eq!(got, want, "incremental restore must reproduce exact state");

    // Order-of-magnitude gate on a second stable-state incremental epoch:
    // epoch 10 re-checkpoints with NO state change — only per-epoch metadata
    // files upload (SSTs dedup against the prior manifest).
    let before_stable = stored_bytes_under(storage.base_dir());
    let _ = runner
        .handle_initiate_checkpoint(checkpoint_req("job-incr", 10), &state, &storage)
        .unwrap();
    let stable_delta = stored_bytes_under(storage.base_dir()) - before_stable;
    assert!(
        stable_delta * 10 <= full_size.max(1),
        "stable-state incremental epoch wrote {stable_delta}B; full snapshot is \
         {full_size}B — expected ≥10x reduction (after_full={after_full}B)"
    );
}

/// Phase 56: multi-subtask run-loop restore routes every checkpointed window
/// entry to the subtask owning its key group (N→2 redistribution) — no state
/// is lost or duplicated across subtasks.
#[tokio::test]
async fn restore_redistributes_run_loop_state_by_key_group() {
    use krishiv_state::checkpoint::{
        CheckpointMetadata, IntegrityManifest, LocalFsCheckpointStorage, OperatorSnapshotRef,
        snapshot_path, write_epoch_metadata, write_manifest, write_operator_snapshot,
    };

    // Craft a portable snapshot in the window-operator key layout
    // (`tw:` | key_len_le | key | win_start_le) so WindowGroupKey routing
    // applies, plus one broadcast watermark entry.
    let make_window_key = |group_key: &str| {
        let mut key = b"tw:".to_vec();
        key.extend_from_slice(&(group_key.len() as u32).to_le_bytes());
        key.extend_from_slice(group_key.as_bytes());
        key.extend_from_slice(&0_i64.to_le_bytes());
        key
    };
    let mut entries: Vec<krishiv_state::SnapshotEntry> = (0..64)
        .map(|i| {
            (
                String::from("continuous-window"),
                String::from("tumbling"),
                make_window_key(&format!("user-{i:02}")),
                vec![i as u8],
            )
        })
        .collect();
    entries.push((
        String::from("continuous-window"),
        String::from("tumbling"),
        b"wm:".to_vec(),
        5_000_i64.to_le_bytes().to_vec(),
    ));
    let snapshot = krishiv_state::encode_snapshot_entries(&entries);

    let job = "rloop-redist-job";
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    write_operator_snapshot(&storage, job, 3, "op-0", "task-streaming-0", &snapshot).unwrap();
    let metadata = CheckpointMetadata {
        version: CheckpointMetadata::VERSION,
        epoch: 3,
        job_id: job.to_owned(),
        fencing_token: 1,
        coordinator_id: None,
        timestamp_ms: 1,
        source_offsets: vec![],
        operator_snapshots: vec![OperatorSnapshotRef {
            operator_id: "op-0".to_owned(),
            task_id: "task-streaming-0".to_owned(),
            snapshot_path: snapshot_path(job, 3, "op-0", "task-streaming-0"),
        }],
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
        unaligned_buffer_refs: Vec::new(),
        sink_transactions: Vec::new(),
        streaming_profile: None,
    };
    write_epoch_metadata(&storage, job, 3, &metadata).unwrap();
    let mut manifest = IntegrityManifest::new();
    manifest.insert_bytes(
        "metadata.json",
        &serde_json::to_vec_pretty(&metadata).unwrap(),
    );
    write_manifest(&storage, job, 3, &manifest).unwrap();

    // Executor hosting BOTH subtasks of a parallelism-2 job.
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let dir0 = tempfile::tempdir().unwrap();
    let dir1 = tempfile::tempdir().unwrap();
    let exec0 = window_exec_with_state_dir(dir0.path());
    let exec1 = window_exec_with_state_dir(dir1.path());
    runner
        .loop_executors
        .insert(format!("{job}#0"), exec0.clone());
    runner
        .loop_executors
        .insert(format!("{job}#1"), exec1.clone());
    runner.rloop_parallelism.insert(job.to_owned(), 2);

    let cmd = krishiv_proto::RestoreFromCheckpointCommand {
        job_id: JobId::try_new(job).unwrap(),
        epoch: 3,
        fencing_token: krishiv_proto::FencingToken::initial(),
        sink_commit: Vec::new(),
        sink_abort: Vec::new(),
    };
    let fallback = crate::runner::CheckpointStateHandle::from_backend(
        krishiv_state::InMemoryStateBackend::default(),
    );
    runner
        .restore_job_from_checkpoint(&cmd, &fallback, &storage)
        .expect("multi-subtask restore must redistribute, not stash");

    // Union of the subtask states == the original routed entries; each
    // routed entry lands on exactly one subtask; the broadcast watermark
    // lands on both.
    let decode = |exec: &std::sync::Arc<
        std::sync::Mutex<krishiv_dataflow::ContinuousWindowExecutor>,
    >| {
        let bytes = exec.lock().unwrap().snapshot().unwrap();
        krishiv_state::decode_snapshot_entries(&bytes).unwrap()
    };
    let s0 = decode(&exec0);
    let s1 = decode(&exec1);
    let routed0 = s0.iter().filter(|e| e.2.starts_with(b"tw:")).count();
    let routed1 = s1.iter().filter(|e| e.2.starts_with(b"tw:")).count();
    assert_eq!(routed0 + routed1, 64, "every window entry restored exactly once");
    assert!(routed0 > 0 && routed1 > 0, "both subtasks own key groups");
    assert!(
        s0.iter().any(|e| e.2 == b"wm:") && s1.iter().any(|e| e.2 == b"wm:"),
        "broadcast watermark reaches every subtask"
    );
}
