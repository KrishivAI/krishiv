// ── ObjectStoreShuffleStore ───────────────────────────────────────────

use crate::ObjectStoreShuffleStore;
use object_store::{ObjectStoreExt as _, memory::InMemory};

#[derive(Debug)]
struct FailingPutObjectStore {
    inner: Arc<dyn object_store::ObjectStore>,
}

impl FailingPutObjectStore {
    fn injected_error() -> object_store::Error {
        object_store::Error::Generic {
            store: "failing-put-object-store",
            source: Box::new(std::io::Error::other("injected object-store write failure")),
        }
    }
}

impl fmt::Display for FailingPutObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FailingPutObjectStore")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for FailingPutObjectStore {
    async fn put_opts(
        &self,
        _location: &object_store::path::Path,
        _payload: object_store::PutPayload,
        _opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        Err(Self::injected_error())
    }

    async fn put_multipart_opts(
        &self,
        _location: &object_store::path::Path,
        _opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        Err(Self::injected_error())
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        _from: &object_store::path::Path,
        _to: &object_store::path::Path,
        _options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        Err(Self::injected_error())
    }
}

fn make_object_store_partition(
    job_id: &str,
    stage_id: &str,
    partition: u32,
) -> ShufflePartition {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![partition as i32]))],
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
async fn object_store_shuffle_write_and_read_round_trip() {
    let inner = Arc::new(InMemory::new());
    let store = ObjectStoreShuffleStore::new(inner, "shuffle-test");

    let partition = make_object_store_partition("job-os-1", "s0", 0);
    let id = partition.id.clone();
    store.write_partition(partition, 0).await.unwrap();

    let read = store.read_partition(&id).await.unwrap().unwrap();
    assert_eq!(read.batches.len(), 1);
    assert_eq!(read.batches[0].num_rows(), 1);
}

#[tokio::test]
async fn object_store_shuffle_detects_tampered_ipc_bytes() {
    let inner = Arc::new(InMemory::new());
    let store = ObjectStoreShuffleStore::new(inner.clone(), "shuffle-test");

    let partition = make_object_store_partition("job-os-tamper", "s0", 0);
    let id = partition.id.clone();
    store.write_partition(partition, 0).await.unwrap();

    inner
        .put(
            &object_store::path::Path::from("shuffle-test/job-os-tamper/s0/0.ipc"),
            bytes::Bytes::from_static(b"not-arrow-ipc").into(),
        )
        .await
        .unwrap();

    let err = store.read_partition(&id).await.unwrap_err();
    assert!(
        matches!(err, ShuffleError::ContentHashMismatch { .. }),
        "expected ContentHashMismatch for tampered object-store bytes, got {err}"
    );
}

#[tokio::test]
async fn object_store_shuffle_restart_reads_with_persisted_hash_sidecar() {
    let inner = Arc::new(InMemory::new());
    let writer = ObjectStoreShuffleStore::new(inner.clone(), "shuffle-test");

    let partition = make_object_store_partition("job-os-restart", "s0", 0);
    let id = partition.id.clone();
    writer.write_partition(partition, 0).await.unwrap();

    let restarted_reader = ObjectStoreShuffleStore::new(inner, "shuffle-test");
    let read = restarted_reader.read_partition(&id).await.unwrap().unwrap();
    assert_eq!(read.batches.len(), 1);
    assert_eq!(read.batches[0].num_rows(), 1);
}

#[tokio::test]
async fn object_store_shuffle_restart_detects_tampered_ipc_bytes() {
    let inner = Arc::new(InMemory::new());
    let writer = ObjectStoreShuffleStore::new(inner.clone(), "shuffle-test");

    let partition = make_object_store_partition("job-os-restart-tamper", "s0", 0);
    let id = partition.id.clone();
    writer.write_partition(partition, 0).await.unwrap();

    inner
        .put(
            &object_store::path::Path::from("shuffle-test/job-os-restart-tamper/s0/0.ipc"),
            bytes::Bytes::from_static(b"not-arrow-ipc").into(),
        )
        .await
        .unwrap();

    let restarted_reader = ObjectStoreShuffleStore::new(inner, "shuffle-test");
    let err = restarted_reader.read_partition(&id).await.unwrap_err();
    assert!(
        matches!(err, ShuffleError::ContentHashMismatch { .. }),
        "expected ContentHashMismatch after restart, got {err}"
    );
}

#[tokio::test]
async fn object_store_shuffle_data_without_hash_sidecar_fails_closed() {
    let inner = Arc::new(InMemory::new());
    let writer = ObjectStoreShuffleStore::new(inner.clone(), "shuffle-test");

    let partition = make_object_store_partition("job-os-missing-hash", "s0", 0);
    let id = partition.id.clone();
    writer.write_partition(partition, 0).await.unwrap();

    inner
        .delete(&object_store::path::Path::from(
            "shuffle-test/job-os-missing-hash/s0/0.ipc.blake3",
        ))
        .await
        .unwrap();

    let restarted_reader = ObjectStoreShuffleStore::new(inner, "shuffle-test");
    let err = restarted_reader.read_partition(&id).await.unwrap_err();
    assert!(
        matches!(err, ShuffleError::ContentHashMismatch { .. }),
        "expected ContentHashMismatch for data object without persisted hash, got {err}"
    );
}

#[tokio::test]
async fn object_store_shuffle_malformed_hash_sidecar_fails_closed() {
    let inner = Arc::new(InMemory::new());
    let writer = ObjectStoreShuffleStore::new(inner.clone(), "shuffle-test");

    let partition = make_object_store_partition("job-os-bad-hash", "s0", 0);
    let id = partition.id.clone();
    writer.write_partition(partition, 0).await.unwrap();

    inner
        .put(
            &object_store::path::Path::from("shuffle-test/job-os-bad-hash/s0/0.ipc.blake3"),
            bytes::Bytes::from_static(b"not-a-blake3-digest").into(),
        )
        .await
        .unwrap();

    let restarted_reader = ObjectStoreShuffleStore::new(inner, "shuffle-test");
    let err = restarted_reader.read_partition(&id).await.unwrap_err();
    assert!(
        matches!(err, ShuffleError::ContentHashMismatch { .. }),
        "expected ContentHashMismatch for malformed hash sidecar, got {err}"
    );
}

#[tokio::test]
async fn object_store_shuffle_read_missing_returns_none() {
    let inner = Arc::new(InMemory::new());
    let store = ObjectStoreShuffleStore::new(inner, "shuffle-test");
    let id = PartitionId {
        job_id: "missing".into(),
        stage_id: "s0".into(),
        partition: 0,
    };
    let result = store.read_partition(&id).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn object_store_shuffle_delete_job_removes_all_partitions() {
    let inner = Arc::new(InMemory::new());
    let store = ObjectStoreShuffleStore::new(inner.clone(), "shuffle-test");

    store
        .write_partition(make_object_store_partition("job-del-os", "s0", 0), 0)
        .await
        .unwrap();
    store
        .write_partition(make_object_store_partition("job-del-os", "s0", 1), 0)
        .await
        .unwrap();

    store.delete_job_partitions("job-del-os").await.unwrap();

    let id0 = PartitionId {
        job_id: "job-del-os".into(),
        stage_id: "s0".into(),
        partition: 0,
    };
    let id1 = PartitionId {
        job_id: "job-del-os".into(),
        stage_id: "s0".into(),
        partition: 1,
    };
    assert!(store.read_partition(&id0).await.unwrap().is_none());
    assert!(store.read_partition(&id1).await.unwrap().is_none());
    let sidecar_result = inner
        .get(&object_store::path::Path::from(
            "shuffle-test/job-del-os/s0/0.ipc.blake3",
        ))
        .await;
    assert!(
        matches!(sidecar_result, Err(object_store::Error::NotFound { .. })),
        "delete_job_partitions must remove persisted hash sidecars"
    );
}

#[tokio::test]
async fn object_store_ipc_compression_roundtrip() {
    use crate::compression::ShuffleCompression;
    for codec in [
        ShuffleCompression::None,
        ShuffleCompression::Lz4,
        ShuffleCompression::Zstd,
    ] {
        let inner = Arc::new(InMemory::new());
        let store =
            ObjectStoreShuffleStore::new(inner, "compress-test").with_compression(codec);
        let partition = make_object_store_partition("job-compress", "s0", 0);
        let id = partition.id.clone();
        store
            .write_partition(partition, 1)
            .await
            .unwrap_or_else(|e| panic!("write failed for codec {:?}: {e}", codec));
        let read_back = store
            .read_partition(&id)
            .await
            .unwrap_or_else(|e| panic!("read failed for codec {:?}: {e}", codec));
        let read_back =
            read_back.unwrap_or_else(|| panic!("partition missing for codec {:?}", codec));
        assert_eq!(read_back.batches.len(), 1, "codec {:?}", codec);
        assert_eq!(read_back.batches[0].num_rows(), 1, "codec {:?}", codec);
    }
}

#[tokio::test]
async fn tiered_store_remote_write_failure_is_returned_to_caller() {
    let local_dir = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalDiskShuffleStore::new(local_dir.path()).unwrap());
    let failing_inner = Arc::new(FailingPutObjectStore {
        inner: Arc::new(InMemory::new()),
    });
    let remote = Arc::new(ObjectStoreShuffleStore::new(failing_inner, "tiered-fail"));
    let store = TieredShuffleStore::new(Arc::clone(&local), Arc::clone(&remote));

    let partition = make_object_store_partition("job-tiered-fail", "s0", 0);
    let id = partition.id.clone();

    let err = store.write_partition(partition, 1).await.unwrap_err();
    assert!(
        matches!(err, ShuffleError::Io(_)),
        "expected remote write I/O failure, got {err}"
    );
    assert!(
        local.read_partition(&id).await.unwrap().is_some(),
        "local write happens before remote commit and remains available for retry cleanup"
    );
    assert!(
        remote.read_partition(&id).await.unwrap().is_none(),
        "failed remote commit must not create a readable remote partition"
    );
}

#[tokio::test]
async fn tiered_store_reads_remote_copy_after_local_loss() {
    let first_local_dir = tempfile::tempdir().unwrap();
    let first_local = Arc::new(LocalDiskShuffleStore::new(first_local_dir.path()).unwrap());
    let remote = Arc::new(ObjectStoreShuffleStore::new(
        Arc::new(InMemory::new()),
        "tiered-remote-copy",
    ));
    let first_store = TieredShuffleStore::new(first_local, Arc::clone(&remote));

    let partition = make_object_store_partition("job-tiered-remote", "s0", 0);
    let id = partition.id.clone();
    first_store.write_partition(partition, 1).await.unwrap();

    let replacement_local_dir = tempfile::tempdir().unwrap();
    let replacement_local =
        Arc::new(LocalDiskShuffleStore::new(replacement_local_dir.path()).unwrap());
    let replacement_store = TieredShuffleStore::new(replacement_local, remote);

    let read = replacement_store
        .read_partition(&id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.batches.len(), 1);
    assert_eq!(read.batches[0].num_rows(), 1);
}

#[tokio::test]
async fn spills_to_disk_at_memory_limit() {
    let dir = tempfile::tempdir().unwrap();
    let spill = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());
    let store = InMemoryShuffleStore::new()
        .with_max_bytes(64)
        .with_spill_store(Arc::clone(&spill));

    let p0 = make_store_partition("job-spill", "s0", 0);
    let p1 = make_store_partition("job-spill", "s0", 1);
    let id0 = p0.id.clone();
    let id1 = p1.id.clone();

    store.write_partition(p0, 1).await.unwrap();
    store.write_partition(p1, 1).await.unwrap();

    assert!(store.read_partition(&id0).await.unwrap().is_some());
    assert!(store.read_partition(&id1).await.unwrap().is_some());

    let spilled_path = dir.path().join("job-spill").join("s0").join("0.parquet");
    assert!(
        spilled_path.exists(),
        "oldest partition should spill to LocalDiskShuffleStore"
    );
    assert!(
        store.read_partition(&id0).await.unwrap().is_some(),
        "spilled partition must remain readable through the in-memory store"
    );
    assert!(
        store.read_partition(&id1).await.unwrap().is_some(),
        "newest partition should remain readable in memory"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_spill_does_not_delete_newer_replacement() {
    fn partition_with_value(
        job_id: &str,
        stage_id: &str,
        partition: u32,
        value: i32,
    ) -> ShufflePartition {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![value; 128]))],
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

    let dir = tempfile::tempdir().unwrap();
    let spill = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());
    let store = Arc::new(
        InMemoryShuffleStore::new()
            .with_max_bytes(64)
            .with_spill_store(Arc::clone(&spill)),
    );

    let old_p0 = partition_with_value("job-spill-race", "s0", 0, 1);
    let id0 = old_p0.id.clone();
    store.write_partition(old_p0, 1).await.unwrap();

    let replace_store = Arc::clone(&store);
    let replace = tokio::spawn(async move {
        replace_store
            .write_partition(partition_with_value("job-spill-race", "s0", 0, 99), 2)
            .await
    });

    let spill_store = Arc::clone(&store);
    let trigger_spill = tokio::spawn(async move {
        spill_store
            .write_partition(partition_with_value("job-spill-race", "s0", 1, 2), 1)
            .await
    });

    replace.await.unwrap().unwrap();
    trigger_spill.await.unwrap().unwrap();

    let read = store
        .read_partition(&id0)
        .await
        .unwrap()
        .expect("newer replacement must remain readable");
    let values = read.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.value(0), 99);
}

#[tokio::test]
async fn parquet_store_writes_compressed() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path())
        .unwrap()
        .with_compression(ShuffleCompression::Zstd);
    let partition = make_store_partition("job-parquet-zstd", "s0", 0);
    let path = dir
        .path()
        .join("job-parquet-zstd")
        .join("s0")
        .join("0.parquet");

    store.write_partition(partition, 1).await.unwrap();

    let file = File::open(&path).unwrap();
    let metadata = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .metadata()
        .clone();
    assert!(
        metadata.row_groups().iter().any(|rg| {
            rg.columns()
                .iter()
                .any(|col| col.compression() != parquet::basic::Compression::UNCOMPRESSED)
        }),
        "Parquet row groups should be written with compression enabled"
    );
}

/// C14 regression: both disk_store and memory_store must have consistent
/// lease token semantics: register accepts token > current, write accepts
/// token >= current. This test verifies the memory store path for
/// monotonic lease replacement.
#[tokio::test]
async fn in_memory_store_monotonic_lease_replacement() {
    let store = InMemoryShuffleStore::new();
    let partition = make_store_partition("job-lease-mono", "s0", 0);
    let id = partition.id.clone();

    // Initial registration with token 1.
    store.register_partition_lease(id.clone(), 1).await.unwrap();
    store.write_partition(partition.clone(), 1).await.unwrap();
    assert!(store.read_partition(&id).await.unwrap().is_some());

    // Same token re-registration must succeed (monotonic replacement).
    store.register_partition_lease(id.clone(), 1).await.unwrap();
    assert!(
        store.read_partition(&id).await.unwrap().is_some(),
        "write must survive monotonic re-registration"
    );

    // Fresh write with same token after re-registration.
    store.write_partition(partition.clone(), 1).await.unwrap();

    // Stale token (0 < 1) must be rejected by both register and write.
    let err = store
        .register_partition_lease(id.clone(), 0)
        .await
        .unwrap_err();
    assert!(matches!(err, ShuffleError::StaleLeaseToken { .. }));

    let err = store
        .write_partition(partition.clone(), 0)
        .await
        .unwrap_err();
    assert!(matches!(err, ShuffleError::StaleLeaseToken { .. }));
}

/// C15 regression: spill store failure must not cause data loss or corrupt
/// bytes_used in the in-memory store.  The bytes_used decrement must happen
/// AFTER the spill write succeeds, not before.
#[tokio::test]
async fn spill_failure_does_not_corrupt_bytes_used() {
    let dir = tempfile::tempdir().unwrap();

    // Create a spill store at path, then use chattr +i to make it
    // immutable before the second write triggers eviction.
    let spill_path = dir.path().join("spill_fail");
    std::fs::create_dir_all(&spill_path).unwrap();
    let spill = Arc::new(LocalDiskShuffleStore::new(&spill_path).unwrap());

    let p0 = make_store_partition("job-spill-fail", "s0", 0);
    let p1 = make_store_partition("job-spill-fail", "s0", 1);
    let id0 = p0.id.clone();
    let id1 = p1.id.clone();
    let max_bytes = partition_memory_bytes(&p0);
    assert!(max_bytes > 0, "test partition must consume memory");
    assert_eq!(
        partition_memory_bytes(&p1),
        max_bytes,
        "test expects same-sized partitions so p1 can fit after p0 spills"
    );

    let store = InMemoryShuffleStore::new()
        .with_max_bytes(max_bytes)
        .with_spill_store(spill);

    // First write succeeds and stays in memory.
    store.write_partition(p0, 1).await.unwrap();

    use std::process::Command;
    let made_immutable = Command::new("chattr")
        .args(["+i", &spill_path.to_string_lossy()])
        .status()
        .is_ok_and(|s| s.success());
    if !made_immutable {
        // Fallback: set read-only permissions (chattr may require
        // CAP_LINUX_IMMUTABLE on some systems; 0o000 stops non-root writes).
        use std::fs::Permissions;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&spill_path, Permissions::from_mode(0o000)).unwrap();
    }

    // Second write triggers a spill of the oldest partition — this spill
    // will FAIL because the spill dir is immutable. Existing data must remain
    // accessible in memory and bytes_used must not be corrupted.
    let err = store.write_partition(p1.clone(), 1).await.unwrap_err();
    assert!(
        matches!(err, ShuffleError::Io(_)),
        "expected spill I/O failure, got {err}"
    );

    // The existing partition must still be readable. The failed incoming
    // partition is not committed; callers can retry after fixing the sink.
    assert!(
        store.read_partition(&id0).await.unwrap().is_some(),
        "p0 lost after failed spill"
    );
    assert!(
        store.read_partition(&id1).await.unwrap().is_none(),
        "p1 should not be committed after failed spill"
    );

    // Clear the immutable flag and verify the same write can be retried cleanly.
    let _ = Command::new("chattr")
        .args(["-i", &spill_path.to_string_lossy()])
        .status();
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&spill_path, Permissions::from_mode(0o755)).unwrap();
    store.write_partition(p1, 1).await.unwrap();
    assert!(
        store.read_partition(&id1).await.unwrap().is_some(),
        "p1 missing after retry"
    );
}

#[tokio::test]
async fn zombie_write_rejected_by_lease() {
    let inner = Arc::new(InMemory::new());
    let store = ObjectStoreShuffleStore::new(inner, "shuffle-lease-test");
    let partition = make_object_store_partition("job-zombie-os", "s0", 0);
    let id = partition.id.clone();

    store.register_partition_lease(id.clone(), 9).await.unwrap();
    let err = store
        .write_partition(partition.clone(), 8)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            ShuffleError::StaleLeaseToken {
                expected: 9,
                actual: 8
            }
        ),
        "expected stale lease rejection, got {err}"
    );
    assert!(store.read_partition(&id).await.unwrap().is_none());

    store.write_partition(partition, 9).await.unwrap();
    assert!(store.read_partition(&id).await.unwrap().is_some());
}

#[tokio::test]
async fn object_store_shuffle_restart_rejects_stale_lease() {
    let inner = Arc::new(InMemory::new());
    let writer = ObjectStoreShuffleStore::new(inner.clone(), "shuffle-lease-restart");
    let partition = make_object_store_partition("job-lease-restart", "s0", 0);
    let id = partition.id.clone();

    writer
        .register_partition_lease(id.clone(), 9)
        .await
        .unwrap();
    writer.write_partition(partition, 9).await.unwrap();

    let restarted = ObjectStoreShuffleStore::new(inner, "shuffle-lease-restart");
    let err = restarted
        .write_partition(make_object_store_partition("job-lease-restart", "s0", 0), 8)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            ShuffleError::StaleLeaseToken {
                expected: 9,
                actual: 8
            }
        ),
        "expected stale lease after restart, got {err}"
    );
}

#[tokio::test]
async fn disk_store_restart_rejects_stale_lease() {
    let dir = tempfile::tempdir().unwrap();
    let writer = LocalDiskShuffleStore::new(dir.path()).unwrap();
    let partition = ShufflePartition {
        id: PartitionId {
            job_id: "job-disk-lease".to_owned(),
            stage_id: "s0".to_owned(),
            partition: 0,
        },
        schema: Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
        batches: vec![
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1]))],
            )
            .unwrap(),
        ],
    };
    let id = partition.id.clone();
    writer
        .register_partition_lease(id.clone(), 9)
        .await
        .unwrap();
    writer.write_partition(partition, 9).await.unwrap();

    let restarted = LocalDiskShuffleStore::new(dir.path()).unwrap();
    let err = restarted
        .write_partition(
            ShufflePartition {
                id: id.clone(),
                schema: Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
                batches: vec![
                    RecordBatch::try_new(
                        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
                        vec![Arc::new(Int32Array::from(vec![2]))],
                    )
                    .unwrap(),
                ],
            },
            8,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            ShuffleError::StaleLeaseToken {
                expected: 9,
                actual: 8
            }
        ),
        "expected stale lease after disk restart, got {err}"
    );
}

#[tokio::test]
async fn disk_store_cleanup_temp_files_on_startup() {
    let dir = tempfile::tempdir().unwrap();
    let job_dir = dir.path().join("job1").join("s0");
    std::fs::create_dir_all(&job_dir).unwrap();

    let valid_file = job_dir.join("0.parquet");
    let temp_file1 = job_dir.join("0.tmp.1");
    let temp_file2 = job_dir.join("1.tmp.99");

    std::fs::write(&valid_file, b"parquet data").unwrap();
    std::fs::write(&temp_file1, b"temp data 1").unwrap();
    std::fs::write(&temp_file2, b"temp data 2").unwrap();

    assert!(valid_file.exists());
    assert!(temp_file1.exists());
    assert!(temp_file2.exists());

    let _store = LocalDiskShuffleStore::new(dir.path()).unwrap();

    assert!(valid_file.exists());
    assert!(!temp_file1.exists());
    assert!(!temp_file2.exists());
}

