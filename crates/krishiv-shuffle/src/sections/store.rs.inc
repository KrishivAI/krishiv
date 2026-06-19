// ── ShuffleStore tests ────────────────────────────────────────────────

fn make_store_partition(job_id: &str, stage_id: &str, partition: u32) -> ShufflePartition {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    ShufflePartition {
        id: PartitionId {
            job_id: job_id.to_owned(),
            stage_id: stage_id.to_owned(),
            partition,
        },
        schema,
        batches: vec![batch],
    }
}

#[tokio::test]
async fn in_memory_shuffle_write_and_read_roundtrip() {
    let store = InMemoryShuffleStore::new();
    let partition = make_store_partition("job-1", "stage-1", 0);
    let id = partition.id.clone();
    store.write_partition(partition, 1).await.unwrap();
    let read_back = store.read_partition(&id).await.unwrap();
    assert!(read_back.is_some());
    let read_back = read_back.unwrap();
    assert_eq!(read_back.batches[0].num_rows(), 3);
}

#[tokio::test]
async fn in_memory_shuffle_read_missing_returns_none() {
    let store = InMemoryShuffleStore::new();
    let id = PartitionId {
        job_id: "ghost-job".to_owned(),
        stage_id: "s0".to_owned(),
        partition: 0,
    };
    let result = store.read_partition(&id).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn in_memory_shuffle_delete_job_partitions() {
    let store = InMemoryShuffleStore::new();
    let p0 = make_store_partition("job-del", "s0", 0);
    let p1 = make_store_partition("job-del", "s0", 1);
    let id0 = p0.id.clone();
    let id1 = p1.id.clone();
    store.write_partition(p0, 1).await.unwrap();
    store.write_partition(p1, 1).await.unwrap();

    store.delete_job_partitions("job-del").await.unwrap();

    assert!(store.read_partition(&id0).await.unwrap().is_none());
    assert!(store.read_partition(&id1).await.unwrap().is_none());
}

#[tokio::test]
async fn in_memory_shuffle_stale_lease_token_rejected() {
    let store = InMemoryShuffleStore::new();
    let partition = make_store_partition("job-stale", "s0", 0);
    // Write with token=5.
    store.write_partition(partition.clone(), 5).await.unwrap();
    // Try to overwrite with a lower token — should be rejected.
    let err = store.write_partition(partition, 3).await.unwrap_err();
    assert!(
        matches!(
            err,
            ShuffleError::StaleLeaseToken {
                expected: 5,
                actual: 3
            }
        ),
        "expected StaleLeaseToken(expected=5, actual=3), got {err}"
    );
}

#[tokio::test]
async fn in_memory_shuffle_equal_lease_token_overwrites() {
    let store = InMemoryShuffleStore::new();
    let partition = make_store_partition("job-eq", "s0", 0);
    let id = partition.id.clone();
    store.write_partition(partition.clone(), 2).await.unwrap();
    // Same token is allowed — overwrites with the new data.
    store.write_partition(partition, 2).await.unwrap();
    assert!(store.read_partition(&id).await.unwrap().is_some());
}

#[tokio::test]
async fn in_memory_registered_fresh_lease_rejects_stale_registration() {
    let store = InMemoryShuffleStore::new();
    let id = make_store_partition("job-zombie-register", "s0", 0).id;

    store.register_partition_lease(id.clone(), 8).await.unwrap();
    let err = store.register_partition_lease(id, 7).await.unwrap_err();

    assert!(
        matches!(
            err,
            ShuffleError::StaleLeaseToken {
                expected: 8,
                actual: 7
            }
        ),
        "expected StaleLeaseToken(expected=8, actual=7), got {err}"
    );
}

#[tokio::test]
async fn in_memory_registered_fresh_lease_rejects_stale_write_before_commit() {
    let store = InMemoryShuffleStore::new();
    let partition = make_store_partition("job-zombie", "s0", 0);
    let id = partition.id.clone();

    store.register_partition_lease(id.clone(), 8).await.unwrap();

    let err = store
        .write_partition(partition.clone(), 7)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            ShuffleError::StaleLeaseToken {
                expected: 8,
                actual: 7
            }
        ),
        "expected StaleLeaseToken(expected=8, actual=7), got {err}"
    );
    assert!(store.read_partition(&id).await.unwrap().is_none());

    store.write_partition(partition, 8).await.unwrap();
    assert!(store.read_partition(&id).await.unwrap().is_some());
}

#[tokio::test]
async fn local_disk_shuffle_write_and_read_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();
    let partition = make_store_partition("job-disk-1", "stage-1", 0);
    let id = partition.id.clone();
    store.write_partition(partition, 1).await.unwrap();
    let read_back = store.read_partition(&id).await.unwrap();
    assert!(read_back.is_some());
    let read_back = read_back.unwrap();
    assert_eq!(read_back.batches[0].num_rows(), 3);
}

#[tokio::test]
async fn local_disk_shuffle_delete_job_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();
    let p0 = make_store_partition("job-disk-del", "s0", 0);
    let id0 = p0.id.clone();
    store.write_partition(p0, 1).await.unwrap();

    store.delete_job_partitions("job-disk-del").await.unwrap();

    // The file should be gone so read returns None.
    assert!(store.read_partition(&id0).await.unwrap().is_none());
}

#[tokio::test]
async fn local_disk_shuffle_stale_token_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();
    let partition = make_store_partition("job-disk-stale", "s0", 0);
    store.write_partition(partition.clone(), 10).await.unwrap();
    let err = store.write_partition(partition, 7).await.unwrap_err();
    assert!(
        matches!(
            err,
            ShuffleError::StaleLeaseToken {
                expected: 10,
                actual: 7
            }
        ),
        "expected StaleLeaseToken(expected=10, actual=7), got {err}"
    );
}

#[tokio::test]
async fn local_disk_registered_fresh_lease_rejects_stale_registration() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();
    let id = make_store_partition("job-disk-zombie-register", "s0", 0).id;

    store
        .register_partition_lease(id.clone(), 11)
        .await
        .unwrap();
    let err = store.register_partition_lease(id, 10).await.unwrap_err();

    assert!(
        matches!(
            err,
            ShuffleError::StaleLeaseToken {
                expected: 11,
                actual: 10
            }
        ),
        "expected StaleLeaseToken(expected=11, actual=10), got {err}"
    );
}

#[tokio::test]
async fn local_disk_registered_newer_lease_replaces_old_registration() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();
    let id = make_store_partition("job-disk-lease-replace", "s0", 0).id;

    store
        .register_partition_lease(id.clone(), 11)
        .await
        .unwrap();
    store
        .register_partition_lease(id.clone(), 12)
        .await
        .unwrap();

    let err = store.register_partition_lease(id, 11).await.unwrap_err();
    assert!(
        matches!(
            err,
            ShuffleError::StaleLeaseToken {
                expected: 12,
                actual: 11
            }
        ),
        "expected StaleLeaseToken(expected=12, actual=11), got {err}"
    );
}

#[tokio::test]
async fn local_disk_registered_fresh_lease_rejects_stale_write_before_commit() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();
    let partition = make_store_partition("job-disk-zombie", "s0", 0);
    let id = partition.id.clone();

    store
        .register_partition_lease(id.clone(), 11)
        .await
        .unwrap();

    let err = store
        .write_partition(partition.clone(), 10)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            ShuffleError::StaleLeaseToken {
                expected: 11,
                actual: 10
            }
        ),
        "expected StaleLeaseToken(expected=11, actual=10), got {err}"
    );
    assert!(store.read_partition(&id).await.unwrap().is_none());

    store.write_partition(partition, 11).await.unwrap();
    assert!(store.read_partition(&id).await.unwrap().is_some());
}

