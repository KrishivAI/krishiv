#[test]
fn shuffle_path_staging_name() {
    let path = ShufflePath {
        job_id: "job1".into(),
        stage_id: "s0".into(),
        partition_id: 3,
    };
    assert_eq!(path.staging_name(), "job1/s0/3.tmp");
}

#[test]
fn shuffle_path_final_name() {
    let path = ShufflePath {
        job_id: "job1".into(),
        stage_id: "s0".into(),
        partition_id: 3,
    };
    assert_eq!(path.final_name(), "job1/s0/3.ipc");
}

// ── ShuffleMetadata ───────────────────────────────────────────────────

fn make_path(partition_id: u32) -> ShufflePath {
    ShufflePath {
        job_id: "j".into(),
        stage_id: "s".into(),
        partition_id,
    }
}

#[test]
fn metadata_pending_to_available() {
    let mut meta = ShuffleMetadata::new();
    let p = make_path(0);
    meta.mark_pending(&p).unwrap();
    assert_eq!(meta.state(&p), Some(&PartitionState::Pending));
    meta.mark_available(&p);
    assert_eq!(meta.state(&p), Some(&PartitionState::Available));
}

#[test]
fn metadata_pending_to_failed() {
    let mut meta = ShuffleMetadata::new();
    let p = make_path(1);
    meta.mark_pending(&p).unwrap();
    meta.mark_failed(&p, "disk full".into());
    assert_eq!(
        meta.state(&p),
        Some(&PartitionState::Failed {
            reason: "disk full".into()
        })
    );
}

#[test]
fn metadata_all_available_requires_every_path() {
    let mut meta = ShuffleMetadata::new();
    let p0 = make_path(0);
    let p1 = make_path(1);
    meta.mark_available(&p0);
    meta.mark_pending(&p1).unwrap();

    assert!(!meta.all_available(&[p0.clone(), p1.clone()]));

    meta.mark_available(&p1);
    assert!(meta.all_available(&[p0, p1]));
}

#[test]
fn metadata_all_available_empty_slice() {
    let meta = ShuffleMetadata::new();
    assert!(meta.all_available(&[]));
}

#[test]
fn metadata_partition_cap_enforced() {
    let mut meta = ShuffleMetadata::new().with_max_partitions(2);
    meta.mark_pending(&make_path(0)).unwrap();
    meta.mark_pending(&make_path(1)).unwrap();
    let err = meta.mark_pending(&make_path(2)).unwrap_err();
    assert!(
        matches!(err, ShuffleError::TooManyPartitions { limit: 2 }),
        "expected TooManyPartitions(2), got: {err}"
    );
}

#[test]
fn metadata_cap_allows_update_of_existing_partition() {
    let mut meta = ShuffleMetadata::new().with_max_partitions(1);
    let p = make_path(0);
    meta.mark_pending(&p).unwrap();
    // Re-marking an existing key must succeed even at cap.
    meta.mark_pending(&p).unwrap();
}

#[test]
fn hash_partitioner_rejects_zero_buckets() {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
    let partitioner = HashPartitioner::new("key", 0);
    let err = partitioner.partition(&batch).unwrap_err();
    assert!(matches!(
        err,
        ShuffleError::InvalidPartitionCount { buckets: 0 }
    ));
}

