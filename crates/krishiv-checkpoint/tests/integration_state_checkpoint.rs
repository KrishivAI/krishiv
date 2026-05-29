//! Integration tests for state management and checkpoint storage interaction.

use std::sync::Arc;

use krishiv_checkpoint::{
    CheckpointMetadata, CheckpointStorage, IntegrityManifest, LocalFsCheckpointStorage,
    OperatorSnapshotRef, SourceOffsetRecord, delete_epoch, latest_valid_epoch, read_epoch_metadata,
    validate_epoch, validate_fencing_token, write_epoch_hint, write_epoch_metadata, write_manifest,
    write_operator_snapshot,
};
use krishiv_state::{
    InMemoryStateBackend, Namespace, RedbStateBackend, StateBackend, TtlConfig, TtlStateBackend,
    migration::{SharedStateMigrationRegistry, StateMigrationRegistry},
};

fn ns(op: &str, name: &str) -> Namespace {
    Namespace::new(op, name)
}

fn sample_metadata(job_id: &str, epoch: u64) -> CheckpointMetadata {
    CheckpointMetadata {
        version: CheckpointMetadata::VERSION,
        epoch,
        job_id: job_id.to_owned(),
        fencing_token: 1,
        timestamp_ms: 1_716_000_000_000,
        source_offsets: vec![SourceOffsetRecord {
            partition_id: "partition-0".to_owned(),
            offset: 42,
        }],
        operator_snapshots: vec![OperatorSnapshotRef {
            operator_id: "op-0".to_owned(),
            task_id: "task-0".to_owned(),
            snapshot_path: krishiv_checkpoint::snapshot_path(
                "job-integration",
                epoch,
                "op-0",
                "task-0",
            ),
        }],
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
    }
}

fn commit_epoch(storage: &dyn CheckpointStorage, job_id: &str, epoch: u64, state: &[u8]) {
    let meta = sample_metadata(job_id, epoch);
    write_operator_snapshot(storage, job_id, epoch, "op-0", "task-0", state).unwrap();
    let meta_json = serde_json::to_vec_pretty(&meta).unwrap();
    write_epoch_metadata(storage, job_id, epoch, &meta).unwrap();
    let mut manifest = IntegrityManifest::new();
    manifest.insert_bytes("metadata.json", &meta_json);
    manifest.insert_bytes("op-0/task-0/state.bin", state);
    write_manifest(storage, job_id, epoch, &manifest).unwrap();
    write_epoch_hint(storage, job_id, epoch).unwrap();
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// ── 1. InMemory state: put → get → delete → snapshot → load → verify ──────

#[test]
fn integration_in_memory_put_get_delete_snapshot_load() {
    let mut backend = InMemoryStateBackend::new();
    let n = ns("window-op", "counts");

    // put + get
    backend.put(&n, b"user-a".to_vec(), b"42".to_vec()).unwrap();
    backend.put(&n, b"user-b".to_vec(), b"17".to_vec()).unwrap();
    assert_eq!(backend.get(&n, b"user-a").unwrap(), Some(b"42".to_vec()));
    assert_eq!(backend.get(&n, b"user-b").unwrap(), Some(b"17".to_vec()));

    // delete
    backend.delete(&n, b"user-a").unwrap();
    assert!(backend.get(&n, b"user-a").unwrap().is_none());
    assert_eq!(backend.get(&n, b"user-b").unwrap(), Some(b"17".to_vec()));

    // snapshot
    let snap = backend.snapshot().unwrap();
    assert!(!snap.is_empty());

    // load into fresh backend
    let mut loaded = InMemoryStateBackend::new();
    loaded.load_snapshot(&snap).unwrap();

    // verify
    assert!(loaded.get(&n, b"user-a").unwrap().is_none());
    assert_eq!(loaded.get(&n, b"user-b").unwrap(), Some(b"17".to_vec()));
    assert_eq!(loaded.key_count(), 1);
}

// ── 2. Redb state: put → get → delete → snapshot → load → verify ──────────

#[test]
fn integration_redb_put_get_delete_snapshot_load() {
    let mut backend = RedbStateBackend::in_memory().expect("in-memory redb");
    let n = ns("agg-op", "state");

    // put + get
    backend.put(&n, b"key1".to_vec(), b"val1".to_vec()).unwrap();
    backend.put(&n, b"key2".to_vec(), b"val2".to_vec()).unwrap();
    assert_eq!(backend.get(&n, b"key1").unwrap(), Some(b"val1".to_vec()));
    assert_eq!(backend.get(&n, b"key2").unwrap(), Some(b"val2".to_vec()));

    // delete
    backend.delete(&n, b"key1").unwrap();
    assert!(backend.get(&n, b"key1").unwrap().is_none());
    assert_eq!(backend.get(&n, b"key2").unwrap(), Some(b"val2".to_vec()));

    // snapshot
    let snap = backend.snapshot().unwrap();
    assert!(!snap.is_empty());

    // load into fresh backend
    let mut loaded = RedbStateBackend::in_memory().expect("in-memory redb");
    loaded.load_snapshot(&snap).unwrap();

    // verify
    assert!(loaded.get(&n, b"key1").unwrap().is_none());
    assert_eq!(loaded.get(&n, b"key2").unwrap(), Some(b"val2".to_vec()));
}

// ── 3. TTL state: put → wait/check expiry → verify entry gone ──────────────

#[test]
fn integration_ttl_put_expiry_verify() {
    let inner = InMemoryStateBackend::new();
    let mut ttl = TtlStateBackend::new(inner, TtlConfig::new(60_000));
    let n = ns("session-op", "session");

    // put + immediate read (should be live)
    ttl.put(&n, b"sid".to_vec(), b"active".to_vec()).unwrap();
    assert_eq!(ttl.get(&n, b"sid").unwrap(), Some(b"active".to_vec()));

    // Simulate expiry: set watermark far in the future
    let future_ms = now_ms() + 100_000;
    ttl.set_watermark(future_ms);
    // Key should now be expired
    assert!(ttl.get(&n, b"sid").unwrap().is_none());

    // Purge removes the dead entry from the inner store
    let evicted = ttl.purge_expired().unwrap();
    assert_eq!(evicted, 1);
}

// ── 4. State migration: register migrations → migrate → verify ─────────────

#[test]
fn integration_state_migration_chained_apply() {
    let mut reg = StateMigrationRegistry::new();

    // v1 → v2: append "-v2"
    reg.register(
        1,
        2,
        Arc::new(|b| {
            let mut v = b.to_vec();
            v.extend_from_slice(b"-v2");
            Ok(v)
        }),
    );
    // v2 → v3: append "-v3"
    reg.register(
        2,
        3,
        Arc::new(|b| {
            let mut v = b.to_vec();
            v.extend_from_slice(b"-v3");
            Ok(v)
        }),
    );
    // v3 → v4: append "-v4"
    reg.register(
        3,
        4,
        Arc::new(|b| {
            let mut v = b.to_vec();
            v.extend_from_slice(b"-v4");
            Ok(v)
        }),
    );

    // Migrate v1 → v4
    let result = reg.migrate(1, 4, b"state-v1").unwrap();
    assert_eq!(result, b"state-v1-v2-v3-v4");

    // Same version is a no-op
    let same = reg.migrate(2, 2, b"unchanged").unwrap();
    assert_eq!(same, b"unchanged");

    // Downgrade is rejected
    assert!(reg.migrate(3, 1, b"down").is_err());

    // Missing migration returns error
    let mut partial = StateMigrationRegistry::new();
    partial.register(1, 2, Arc::new(|b| Ok(b.to_vec())));
    assert!(partial.migrate(1, 3, b"x").is_err());
}

#[test]
fn integration_shared_migration_registry_thread_safe() {
    let registry = SharedStateMigrationRegistry::new();
    registry
        .register(
            1,
            2,
            Arc::new(|b| {
                let mut v = b.to_vec();
                v.extend_from_slice(b" upgraded");
                Ok(v)
            }),
        )
        .unwrap();

    let registry2 = registry.clone();
    let handle = std::thread::spawn(move || registry2.migrate(1, 2, b"original"));
    let result = handle.join().unwrap().unwrap();
    assert_eq!(result, b"original upgraded");
}

// ── 5. Checkpoint write → validate manifest → verify integrity ─────────────

#[test]
fn integration_checkpoint_write_validate_integrity() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let job_id = "job-integ-5";
    let epoch = 1u64;
    let state = b"checkpoint state payload";

    commit_epoch(&storage, job_id, epoch, state);

    // Validate epoch integrity
    assert!(validate_epoch(&storage, job_id, epoch).unwrap());

    // Read back metadata
    let meta = read_epoch_metadata(&storage, job_id, epoch)
        .unwrap()
        .unwrap();
    assert_eq!(meta.epoch, epoch);
    assert_eq!(meta.version, CheckpointMetadata::VERSION);
    assert_eq!(meta.job_id, job_id);

    // Read back snapshot
    let snap =
        krishiv_checkpoint::read_operator_snapshot(&storage, job_id, epoch, "op-0", "task-0")
            .unwrap()
            .unwrap();
    assert_eq!(snap, state.to_vec());
}

#[test]
fn integration_tampered_file_fails_integrity() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let job_id = "job-tamper";
    let epoch = 1u64;
    let state = b"good state";

    commit_epoch(&storage, job_id, epoch, state);

    // Tamper with the state file
    storage
        .write_bytes(
            &krishiv_checkpoint::snapshot_path(job_id, epoch, "op-0", "task-0"),
            b"tampered",
        )
        .unwrap();

    // Integrity check must fail
    assert!(!validate_epoch(&storage, job_id, epoch).unwrap());
}

// ── 6. Checkpoint metadata roundtrip with all fields ───────────────────────

#[test]
fn integration_metadata_roundtrip_all_fields() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let job_id = "job-full-meta";
    let epoch = 42u64;

    let meta = CheckpointMetadata {
        version: 1,
        epoch,
        job_id: job_id.to_owned(),
        fencing_token: 7,
        timestamp_ms: 1_716_100_000_000,
        source_offsets: vec![
            SourceOffsetRecord {
                partition_id: "p-0".to_owned(),
                offset: 100,
            },
            SourceOffsetRecord {
                partition_id: "p-1".to_owned(),
                offset: 200,
            },
        ],
        operator_snapshots: vec![
            OperatorSnapshotRef {
                operator_id: "op-a".to_owned(),
                task_id: "task-0".to_owned(),
                snapshot_path: krishiv_checkpoint::snapshot_path(job_id, epoch, "op-a", "task-0"),
            },
            OperatorSnapshotRef {
                operator_id: "op-b".to_owned(),
                task_id: "task-1".to_owned(),
                snapshot_path: krishiv_checkpoint::snapshot_path(job_id, epoch, "op-b", "task-1"),
            },
        ],
        is_savepoint: true,
        savepoint_label: Some("manual-snap".to_owned()),
        iceberg_snapshot_id: Some(999),
        kafka_offsets: Some({
            let mut m = std::collections::BTreeMap::new();
            m.insert("topic-a".to_owned(), 500i64);
            m.insert("topic-b".to_owned(), 600i64);
            m
        }),
    };

    write_epoch_metadata(&storage, job_id, epoch, &meta).unwrap();
    let read_back = read_epoch_metadata(&storage, job_id, epoch)
        .unwrap()
        .unwrap();
    assert_eq!(meta, read_back);

    // Verify optional fields survive roundtrip
    assert_eq!(read_back.iceberg_snapshot_id, Some(999));
    let offsets = read_back.kafka_offsets.unwrap();
    assert_eq!(offsets.get("topic-a"), Some(&500i64));
    assert_eq!(offsets.get("topic-b"), Some(&600i64));
    assert!(read_back.is_savepoint);
    assert_eq!(read_back.savepoint_label.as_deref(), Some("manual-snap"));
}

// ── 7. Fencing token: write with token → verify → reject stale token ───────

#[test]
fn integration_fencing_token_write_verify_reject_stale() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let job_id = "job-fence";

    // Epoch 1 with token=1
    commit_epoch(&storage, job_id, 1, b"state-1");

    // Epoch 2 with token=2 — should be accepted
    let mut meta2 = sample_metadata(job_id, 2);
    meta2.fencing_token = 2;
    write_epoch_metadata(&storage, job_id, 2, &meta2).unwrap();
    let mut m2 = IntegrityManifest::new();
    m2.insert_bytes("metadata.json", &serde_json::to_vec_pretty(&meta2).unwrap());
    write_manifest(&storage, job_id, 2, &m2).unwrap();

    // Verify token=2 is valid against current_token=2
    assert!(validate_fencing_token(&meta2, 2).is_ok());

    // Stale token (token=1) against current_token=2 must be rejected
    let stale_meta = sample_metadata(job_id, 1);
    let result = validate_fencing_token(&stale_meta, 2);
    assert!(result.is_err());
    match result.unwrap_err() {
        krishiv_checkpoint::CheckpointError::StaleFencingToken { stored, current } => {
            assert_eq!(stored, 1);
            assert_eq!(current, 2);
        }
        other => panic!("expected StaleFencingToken, got: {other}"),
    }

    // Future token (token=5) against current_token=2 must be accepted
    let mut future_meta = sample_metadata(job_id, 3);
    future_meta.fencing_token = 5;
    assert!(validate_fencing_token(&future_meta, 2).is_ok());
}

// ── 8. Ephemeral checkpoint: write → verify cleanup on drop ────────────────

#[test]
fn integration_ephemeral_checkpoint_cleanup_on_drop() {
    let base_path;
    {
        let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
        base_path = storage.base_dir().to_path_buf();
        commit_epoch(&*storage, "job-eph", 1, b"ephemeral-data");
        assert!(base_path.exists());
        assert!(
            base_path
                .join("job-eph/checkpoints/00000000000000000001/metadata.json")
                .exists()
        );
    }
    // After drop, directory must be cleaned up
    assert!(
        !base_path.exists(),
        "ephemeral checkpoint dir should be removed on drop"
    );
}

#[test]
fn integration_ephemeral_two_instances_independent() {
    let (path_a, path_b);
    {
        let a = LocalFsCheckpointStorage::ephemeral().unwrap();
        let b = LocalFsCheckpointStorage::ephemeral().unwrap();
        path_a = a.base_dir().to_path_buf();
        path_b = b.base_dir().to_path_buf();

        commit_epoch(&*a, "job-a", 1, b"state-a");
        commit_epoch(&*b, "job-b", 1, b"state-b");

        // Each sees its own data
        let snap_a = krishiv_checkpoint::read_operator_snapshot(&*a, "job-a", 1, "op-0", "task-0")
            .unwrap()
            .unwrap();
        let snap_b = krishiv_checkpoint::read_operator_snapshot(&*b, "job-b", 1, "op-0", "task-0")
            .unwrap()
            .unwrap();
        assert_eq!(snap_a, b"state-a");
        assert_eq!(snap_b, b"state-b");
    }
    assert!(!path_a.exists());
    assert!(!path_b.exists());
}

// ── 9. State + checkpoint integration: put state → snapshot → checkpoint → restore → verify state ──

#[test]
fn integration_state_checkpoint_restore_roundtrip() {
    let job_id = "job-integ-9";
    let epoch = 1u64;
    let n = ns("window-op", "user-counts");

    // 1. Write state
    let mut backend = InMemoryStateBackend::new();
    backend.put(&n, b"alice".to_vec(), b"10".to_vec()).unwrap();
    backend.put(&n, b"bob".to_vec(), b"20".to_vec()).unwrap();

    // 2. Take snapshot
    let state_snap = backend.snapshot().unwrap();

    // 3. Write to checkpoint storage
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    commit_epoch(&storage, job_id, epoch, &state_snap);

    // 4. Verify epoch is valid
    assert!(validate_epoch(&storage, job_id, epoch).unwrap());
    let latest = latest_valid_epoch(&storage, job_id).unwrap();
    assert_eq!(latest, epoch);

    // 5. Read snapshot back from checkpoint
    let restored_snap =
        krishiv_checkpoint::read_operator_snapshot(&storage, job_id, epoch, "op-0", "task-0")
            .unwrap()
            .unwrap();

    // 6. Load into fresh state backend
    let mut restored = InMemoryStateBackend::new();
    restored.load_snapshot(&restored_snap).unwrap();

    // 7. Verify restored state matches original
    assert_eq!(restored.get(&n, b"alice").unwrap(), Some(b"10".to_vec()));
    assert_eq!(restored.get(&n, b"bob").unwrap(), Some(b"20".to_vec()));
}

#[test]
fn integration_redb_state_checkpoint_restore_roundtrip() {
    let job_id = "job-integ-9b";
    let epoch = 1u64;
    let n = ns("agg-op", "metrics");

    // Write state in Redb backend
    let mut backend = RedbStateBackend::in_memory().expect("in-memory redb");
    backend
        .put(&n, b"metric-1".to_vec(), b"100".to_vec())
        .unwrap();
    backend
        .put(&n, b"metric-2".to_vec(), b"200".to_vec())
        .unwrap();

    // Snapshot → checkpoint → restore
    let state_snap = backend.snapshot().unwrap();
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    commit_epoch(&storage, job_id, epoch, &state_snap);

    let restored_snap =
        krishiv_checkpoint::read_operator_snapshot(&storage, job_id, epoch, "op-0", "task-0")
            .unwrap()
            .unwrap();

    let mut restored = RedbStateBackend::in_memory().expect("in-memory redb");
    restored.load_snapshot(&restored_snap).unwrap();

    assert_eq!(
        restored.get(&n, b"metric-1").unwrap(),
        Some(b"100".to_vec())
    );
    assert_eq!(
        restored.get(&n, b"metric-2").unwrap(),
        Some(b"200".to_vec())
    );
}

// ── 10. Cross-backend: InMemory → Redb migration via snapshot ──────────────

#[test]
fn integration_cross_backend_inmemory_to_redb_via_snapshot() {
    let n = ns("cross-op", "migrate-state");

    // Populate InMemory state
    let mut src = InMemoryStateBackend::new();
    src.put(&n, b"k1".to_vec(), b"v1".to_vec()).unwrap();
    src.put(&n, b"k2".to_vec(), b"v2".to_vec()).unwrap();
    src.put(&n, b"k3".to_vec(), b"v3".to_vec()).unwrap();

    // Snapshot from InMemory
    let snap = src.snapshot().unwrap();
    assert!(!snap.is_empty());

    // Load into Redb (cross-backend migration)
    let mut dst = RedbStateBackend::in_memory().expect("in-memory redb");
    dst.load_snapshot(&snap).unwrap();

    // Verify all entries survived the cross-backend transfer
    assert_eq!(dst.get(&n, b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(dst.get(&n, b"k2").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(dst.get(&n, b"k3").unwrap(), Some(b"v3".to_vec()));

    // Reverse: Redb → InMemory
    let snap2 = dst.snapshot().unwrap();
    let mut dst2 = InMemoryStateBackend::new();
    dst2.load_snapshot(&snap2).unwrap();
    assert_eq!(dst2.get(&n, b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(dst2.get(&n, b"k2").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(dst2.get(&n, b"k3").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn integration_cross_backend_redb_file_persist_snapshot() {
    let n = ns("persist-op", "durable");

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("state.redb");

    // Write to file-backed Redb, snapshot, store in checkpoint, restore to InMemory
    {
        let mut backend = RedbStateBackend::open(&path).expect("open redb");
        backend.put(&n, b"pk1".to_vec(), b"pv1".to_vec()).unwrap();
        backend.put(&n, b"pk2".to_vec(), b"pv2".to_vec()).unwrap();
        let snap = backend.snapshot().unwrap();

        let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
        commit_epoch(&*storage, "job-cross", 1, &snap);

        let restored_snap =
            krishiv_checkpoint::read_operator_snapshot(&*storage, "job-cross", 1, "op-0", "task-0")
                .unwrap()
                .unwrap();
        let mut mem = InMemoryStateBackend::new();
        mem.load_snapshot(&restored_snap).unwrap();
        assert_eq!(mem.get(&n, b"pk1").unwrap(), Some(b"pv1".to_vec()));
        assert_eq!(mem.get(&n, b"pk2").unwrap(), Some(b"pv2".to_vec()));
    }
}

// ── Additional edge-case integration tests ─────────────────────────────────

#[test]
fn integration_checkpoint_multi_epoch_progression() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let job_id = "job-prog";

    // Epoch 1
    let mut backend = InMemoryStateBackend::new();
    let n = ns("op", "counts");
    backend.put(&n, b"a".to_vec(), b"1".to_vec()).unwrap();
    let snap1 = backend.snapshot().unwrap();
    commit_epoch(&storage, job_id, 1, &snap1);

    // Epoch 2
    backend.put(&n, b"b".to_vec(), b"2".to_vec()).unwrap();
    let snap2 = backend.snapshot().unwrap();
    commit_epoch(&storage, job_id, 2, &snap2);

    // Epoch 3
    backend.delete(&n, b"a").unwrap();
    let snap3 = backend.snapshot().unwrap();
    commit_epoch(&storage, job_id, 3, &snap3);

    // Latest valid epoch should be 3
    assert_eq!(latest_valid_epoch(&storage, job_id).unwrap(), 3);

    // Restore epoch 1 → should have only key "a"
    let snap_r1 = krishiv_checkpoint::read_operator_snapshot(&storage, job_id, 1, "op-0", "task-0")
        .unwrap()
        .unwrap();
    let mut r1 = InMemoryStateBackend::new();
    r1.load_snapshot(&snap_r1).unwrap();
    assert_eq!(r1.get(&n, b"a").unwrap(), Some(b"1".to_vec()));
    assert!(r1.get(&n, b"b").unwrap().is_none());

    // Restore epoch 3 → should have only key "b"
    let snap_r3 = krishiv_checkpoint::read_operator_snapshot(&storage, job_id, 3, "op-0", "task-0")
        .unwrap()
        .unwrap();
    let mut r3 = InMemoryStateBackend::new();
    r3.load_snapshot(&snap_r3).unwrap();
    assert!(r3.get(&n, b"a").unwrap().is_none());
    assert_eq!(r3.get(&n, b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn integration_checkpoint_delete_epoch_preserves_others() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let job_id = "job-del";

    let snap = b"some state";
    commit_epoch(&storage, job_id, 1, snap);
    commit_epoch(&storage, job_id, 2, snap);
    commit_epoch(&storage, job_id, 3, snap);

    // Delete epoch 2
    delete_epoch(&storage, job_id, 2).unwrap();

    assert!(validate_epoch(&storage, job_id, 1).unwrap());
    assert!(!validate_epoch(&storage, job_id, 2).unwrap());
    assert!(validate_epoch(&storage, job_id, 3).unwrap());
    assert_eq!(latest_valid_epoch(&storage, job_id).unwrap(), 3);
}

#[test]
fn integration_ttl_snapshot_preserves_across_redb() {
    let inner1 = InMemoryStateBackend::new();
    let mut ttl1 = TtlStateBackend::new(inner1, TtlConfig::new(60_000));
    let n = ns("ttl-op", "session");

    ttl1.put(&n, b"sid1".to_vec(), b"active".to_vec()).unwrap();
    ttl1.put(&n, b"sid2".to_vec(), b"also-active".to_vec())
        .unwrap();

    // Snapshot strips TTL prefixes (only live entries)
    let snap = ttl1.snapshot().unwrap();

    // Load into TTL-wrapped Redb
    let inner2 = RedbStateBackend::in_memory().expect("in-memory redb");
    let mut ttl2 = TtlStateBackend::new(inner2, TtlConfig::new(60_000));
    ttl2.load_snapshot(&snap).unwrap();

    assert_eq!(ttl2.get(&n, b"sid1").unwrap(), Some(b"active".to_vec()));
    assert_eq!(
        ttl2.get(&n, b"sid2").unwrap(),
        Some(b"also-active".to_vec())
    );
}

#[test]
fn integration_state_inspector_after_checkpoint_restore() {
    let job_id = "job-inspect";
    let epoch = 1u64;

    let mut backend = InMemoryStateBackend::new();
    let n1 = ns("op1", "window");
    let n2 = ns("op2", "counts");
    backend.put(&n1, b"k1".to_vec(), b"v1".to_vec()).unwrap();
    backend.put(&n1, b"k2".to_vec(), b"v2".to_vec()).unwrap();
    backend.put(&n2, b"k3".to_vec(), b"v3".to_vec()).unwrap();

    let snap = backend.snapshot().unwrap();
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    commit_epoch(&storage, job_id, epoch, &snap);

    // Restore
    let restored_snap =
        krishiv_checkpoint::read_operator_snapshot(&storage, job_id, epoch, "op-0", "task-0")
            .unwrap()
            .unwrap();
    let mut restored = InMemoryStateBackend::new();
    restored.load_snapshot(&restored_snap).unwrap();

    // Inspect restored state
    let inspector = krishiv_state::StateInspector::new(&restored);
    let mut namespaces = inspector.list_namespaces().unwrap();
    namespaces.sort();
    assert_eq!(namespaces, vec![n1.clone(), n2.clone()]);
    assert_eq!(inspector.key_count(&n1).unwrap(), 2);
    assert_eq!(inspector.key_count(&n2).unwrap(), 1);
}
