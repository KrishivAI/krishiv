// ── LocalShuffleStore ─────────────────────────────────────────────────

#[tokio::test]
async fn local_store_write_and_read_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalShuffleStore::new(dir.path());
    let path = ShufflePath {
        job_id: "job-rw".into(),
        stage_id: "s1".into(),
        partition_id: 0,
    };
    let data = b"hello shuffle".as_slice();
    store.write_partition(&path, data).await.unwrap();
    let read = store.read_partition(&path).await.unwrap();
    assert_eq!(read, data);
}

#[tokio::test]
async fn local_store_restart_reads_with_persisted_hash_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let path = ShufflePath::new("job-local-restart", "s0", 0);
    let data = b"restart-safe local shuffle bytes";

    let writer = LocalShuffleStore::new(dir.path());
    writer.write_partition(&path, data).await.unwrap();

    let restarted_reader = LocalShuffleStore::new(dir.path());
    let read = restarted_reader.read_partition(&path).await.unwrap();
    assert_eq!(read, data);
}

#[tokio::test]
async fn local_store_tampered_data_fails_before_decompression() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalShuffleStore::new(dir.path()).with_compression(CompressionCodec::Lz4);
    let path = ShufflePath::new("job-local-tamper", "s0", 0);

    store.write_partition(&path, b"tamper me").await.unwrap();
    std::fs::write(dir.path().join("job-local-tamper/s0/0.ipc"), b"KSH\x01bad").unwrap();

    let err = LocalShuffleStore::new(dir.path())
        .read_partition(&path)
        .await
        .unwrap_err();
    assert!(
        matches!(err, ShuffleError::ContentHashMismatch { .. }),
        "expected ContentHashMismatch for tampered local shuffle bytes, got {err}"
    );
}

#[tokio::test]
async fn local_store_data_without_hash_sidecar_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalShuffleStore::new(dir.path());
    let path = ShufflePath::new("job-local-missing-hash", "s0", 0);

    store
        .write_partition(&path, b"missing sidecar")
        .await
        .unwrap();
    std::fs::remove_file(dir.path().join("job-local-missing-hash/s0/0.ipc.blake3")).unwrap();

    let err = LocalShuffleStore::new(dir.path())
        .read_partition(&path)
        .await
        .unwrap_err();
    assert!(
        matches!(err, ShuffleError::ContentHashMismatch { .. }),
        "expected ContentHashMismatch for missing local shuffle hash sidecar, got {err}"
    );
}

#[tokio::test]
async fn local_store_malformed_hash_sidecar_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalShuffleStore::new(dir.path());
    let path = ShufflePath::new("job-local-bad-hash", "s0", 0);

    store.write_partition(&path, b"bad sidecar").await.unwrap();
    std::fs::write(
        dir.path().join("job-local-bad-hash/s0/0.ipc.blake3"),
        b"not-a-blake3-digest",
    )
    .unwrap();

    let err = LocalShuffleStore::new(dir.path())
        .read_partition(&path)
        .await
        .unwrap_err();
    assert!(
        matches!(err, ShuffleError::ContentHashMismatch { .. }),
        "expected ContentHashMismatch for malformed local shuffle hash sidecar, got {err}"
    );
}

#[tokio::test]
async fn local_store_read_missing_returns_partition_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalShuffleStore::new(dir.path());
    let path = ShufflePath {
        job_id: "ghost".into(),
        stage_id: "s0".into(),
        partition_id: 0,
    };
    let err = store.read_partition(&path).await.unwrap_err();
    assert!(
        matches!(err, ShuffleError::PartitionNotFound { .. }),
        "expected PartitionNotFound, got {err}"
    );
}

#[tokio::test]
async fn local_store_delete_job_removes_directory() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalShuffleStore::new(dir.path());
    let path = ShufflePath {
        job_id: "deljob".into(),
        stage_id: "s0".into(),
        partition_id: 0,
    };
    store.write_partition(&path, b"data").await.unwrap();
    let job_dir = dir.path().join("deljob");
    assert!(job_dir.exists());
    assert!(dir.path().join("deljob/s0/0.ipc.blake3").exists());

    store.delete_job("deljob").await.unwrap();
    assert!(!job_dir.exists());
}

#[tokio::test]
async fn local_store_delete_job_noop_when_missing() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalShuffleStore::new(dir.path());
    // Should not return an error.
    store.delete_job("nonexistent-job").await.unwrap();
}

