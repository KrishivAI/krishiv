// ── LocalDiskShuffleStore: data integrity ────────────────────────────

#[tokio::test]
async fn disk_store_write_read_preserves_arrow_data() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let names = Arc::new(StringArray::from(vec!["alice", "bob", "carol"]));
    let values = Arc::new(Int32Array::from(vec![10, 20, 30]));
    let batch = RecordBatch::try_new(schema.clone(), vec![names, values]).unwrap();
    let partition = ShufflePartition {
        id: PartitionId {
            job_id: "job-integrity".to_owned(),
            stage_id: "s0".to_owned(),
            partition: 0,
        },
        schema,
        batches: vec![batch],
    };
    let id = partition.id.clone();
    store.write_partition(partition, 1).await.unwrap();

    let read = store.read_partition(&id).await.unwrap().unwrap();
    assert_eq!(read.batches.len(), 1);
    let rb = &read.batches[0];
    assert_eq!(rb.num_rows(), 3);
    assert_eq!(rb.num_columns(), 2);

    let names = rb.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(names.value(0), "alice");
    assert_eq!(names.value(1), "bob");
    assert_eq!(names.value(2), "carol");

    let values = rb.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(values.value(0), 10);
    assert_eq!(values.value(1), 20);
    assert_eq!(values.value(2), 30);
}

#[tokio::test]
async fn disk_store_multiple_partitions_read_independent() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));

    let p0 = ShufflePartition {
        id: PartitionId {
            job_id: "job-multi".to_owned(),
            stage_id: "s0".to_owned(),
            partition: 0,
        },
        schema: schema.clone(),
        batches: vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![100, 200]))],
            )
            .unwrap(),
        ],
    };
    let p1 = ShufflePartition {
        id: PartitionId {
            job_id: "job-multi".to_owned(),
            stage_id: "s0".to_owned(),
            partition: 1,
        },
        schema: schema.clone(),
        batches: vec![
            RecordBatch::try_new(
                schema,
                vec![Arc::new(Int32Array::from(vec![300, 400, 500]))],
            )
            .unwrap(),
        ],
    };
    let id0 = p0.id.clone();
    let id1 = p1.id.clone();

    store.write_partition(p0, 1).await.unwrap();
    store.write_partition(p1, 1).await.unwrap();

    let r0 = store.read_partition(&id0).await.unwrap().unwrap();
    let r1 = store.read_partition(&id1).await.unwrap().unwrap();

    assert_eq!(r0.batches[0].num_rows(), 2);
    assert_eq!(r1.batches[0].num_rows(), 3);

    let v0 = r0.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(v0.value(0), 100);
    assert_eq!(v0.value(1), 200);

    let v1 = r1.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(v1.value(0), 300);
    assert_eq!(v1.value(1), 400);
    assert_eq!(v1.value(2), 500);
}

#[tokio::test]
async fn disk_store_delete_cleans_all_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));

    for i in 0..3 {
        store
            .write_partition(
                ShufflePartition {
                    id: PartitionId {
                        job_id: "job-cleanup".to_owned(),
                        stage_id: "s0".to_owned(),
                        partition: i,
                    },
                    schema: schema.clone(),
                    batches: vec![
                        RecordBatch::try_new(
                            schema.clone(),
                            vec![Arc::new(Int32Array::from(vec![i as i32]))],
                        )
                        .unwrap(),
                    ],
                },
                1,
            )
            .await
            .unwrap();
    }

    store.delete_job_partitions("job-cleanup").await.unwrap();

    for i in 0..3 {
        let id = PartitionId {
            job_id: "job-cleanup".to_owned(),
            stage_id: "s0".to_owned(),
            partition: i,
        };
        assert!(
            store.read_partition(&id).await.unwrap().is_none(),
            "partition {i} should be gone after delete"
        );
    }
}

#[tokio::test]
async fn disk_store_zstd_compression_preserves_data() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path())
        .unwrap()
        .with_compression(ShuffleCompression::Zstd);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let ids = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
    let labels = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, labels]).unwrap();
    let partition = ShufflePartition {
        id: PartitionId {
            job_id: "job-zstd".to_owned(),
            stage_id: "s0".to_owned(),
            partition: 0,
        },
        schema,
        batches: vec![batch],
    };
    let id = partition.id.clone();
    store.write_partition(partition, 1).await.unwrap();

    let read = store.read_partition(&id).await.unwrap().unwrap();
    let rb = &read.batches[0];
    assert_eq!(rb.num_rows(), 5);

    let ids = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    for i in 0..5 {
        assert_eq!(ids.value(i), (i + 1) as i64);
    }
    let labels = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    for (i, expected) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        assert_eq!(labels.value(i), *expected);
    }
}


// ── compression at rest ───────────────────────────────────────────────

/// Shuffle partitions must be compressed at rest by default.
///
/// Both stores constructed themselves with `ShuffleCompression::None`, and the
/// only production caller that ever set a codec was `shuffle_svc`. Every path
/// a distributed query actually uses — the executor's local, object, and
/// tiered backends — took the default, so partitions were written raw to the
/// slowest resources in the system while `with_compression` sat there tested
/// and uncalled.
///
/// Asserting *round-trip* alongside size is the point: Parquet records its
/// codec in file metadata, so a reader needs no configuration to read a
/// compressed partition. That property is what makes the default safe to flip
/// under partitions already on disk.
#[tokio::test]
async fn the_disk_store_compresses_by_default_and_still_round_trips() {
    fn shuffle_shaped(job: &str) -> ShufflePartition {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        // Join keys and a low-cardinality dimension: the shape real shuffle
        // payloads have, and the shape that compresses.
        let keys: Vec<i64> = (0..20_000).map(|i| i % 500).collect();
        let regions: Vec<&str> = (0..20_000)
            .map(|i| ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"][i % 5])
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(StringArray::from(regions)),
            ],
        )
        .unwrap();
        ShufflePartition {
            id: PartitionId {
                job_id: job.to_owned(),
                stage_id: "s0".to_owned(),
                partition: 0,
            },
            schema,
            batches: vec![batch],
        }
    }

    fn tree_bytes(dir: &std::path::Path) -> u64 {
        let mut total = 0;
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let meta = entry.metadata().unwrap();
            total += if meta.is_dir() {
                tree_bytes(&entry.path())
            } else {
                meta.len()
            };
        }
        total
    }

    let plain_dir = tempfile::tempdir().unwrap();
    let plain = LocalDiskShuffleStore::new(plain_dir.path())
        .unwrap()
        .with_compression(ShuffleCompression::None);
    plain
        .write_partition(shuffle_shaped("job-plain"), 1)
        .await
        .unwrap();

    let default_dir = tempfile::tempdir().unwrap();
    let defaulted = LocalDiskShuffleStore::new(default_dir.path()).unwrap();
    let expected = shuffle_shaped("job-default");
    let id = expected.id.clone();
    defaulted.write_partition(expected.clone(), 1).await.unwrap();

    let plain_bytes = tree_bytes(plain_dir.path());
    let default_bytes = tree_bytes(default_dir.path());
    assert!(
        default_bytes < plain_bytes,
        "the default store must compress at rest: {default_bytes} >= {plain_bytes} bytes"
    );

    // ...and the compressed partition must read back byte-for-byte identical
    // through a reader given no codec configuration at all.
    let read = defaulted.read_partition(&id).await.unwrap().unwrap();
    let got: usize = read.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(got, 20_000, "row count must survive the round trip");
    // Parquet reads back one batch per row group, so the rows must be
    // reassembled before comparing — the split is a storage detail, not a
    // difference in the data.
    let expected_batch = expected.batches.first().unwrap();
    let actual = arrow::compute::concat_batches(&read.schema, &read.batches).unwrap();
    assert_eq!(
        actual.column(0),
        expected_batch.column(0),
        "keys must survive"
    );
    assert_eq!(
        actual.column(1),
        expected_batch.column(1),
        "strings must survive"
    );
}
