// ── InMemoryShuffleStore: data integrity ─────────────────────────────

#[tokio::test]
async fn in_memory_write_read_preserves_arrow_values() {
    let store = InMemoryShuffleStore::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![42, 99])),
            Arc::new(StringArray::from(vec!["hello", "world"])),
        ],
    )
    .unwrap();
    let partition = ShufflePartition {
        id: PartitionId {
            job_id: "job-val".to_owned(),
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
    assert_eq!(rb.num_rows(), 2);

    let x = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(x.value(0), 42);
    assert_eq!(x.value(1), 99);

    let y = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(y.value(0), "hello");
    assert_eq!(y.value(1), "world");
}

#[tokio::test]
async fn in_memory_without_spill_accepts_write_within_cap() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
        .unwrap();
    let partition = ShufflePartition {
        id: PartitionId {
            job_id: "misconfig".to_owned(),
            stage_id: "s0".to_owned(),
            partition: 0,
        },
        schema,
        batches: vec![batch],
    };
    let id = partition.id.clone();
    let store = InMemoryShuffleStore::new().with_max_bytes(partition_memory_bytes(&partition));

    store.write_partition(partition, 1).await.unwrap();
    assert!(store.read_partition(&id).await.unwrap().is_some());
}

#[tokio::test]
async fn in_memory_without_spill_rejects_write_over_cap() {
    let store = InMemoryShuffleStore::new().with_max_bytes(1);
    let partition = make_store_partition("memory-cap", "s0", 0);
    let id = partition.id.clone();

    let err = store.write_partition(partition, 1).await.unwrap_err();
    assert!(
        matches!(
            err,
            ShuffleError::MemoryLimitExceeded {
                max_bytes: 1,
                current_bytes: 0,
                incoming_bytes: _
            }
        ),
        "expected MemoryLimitExceeded, got {err}"
    );
    assert!(
        store.read_partition(&id).await.unwrap().is_none(),
        "rejected partition must not be visible"
    );
}

#[tokio::test]
async fn in_memory_capacity_accounts_for_overwrite() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let id = PartitionId {
        job_id: "memory-overwrite-cap".to_owned(),
        stage_id: "s0".to_owned(),
        partition: 0,
    };
    let first = ShufflePartition {
        id: id.clone(),
        schema: schema.clone(),
        batches: vec![
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
                .unwrap(),
        ],
    };
    let max_bytes = partition_memory_bytes(&first);
    let replacement = ShufflePartition {
        id: id.clone(),
        schema,
        batches: vec![
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![9]))],
            )
            .unwrap(),
        ],
    };
    assert_eq!(
        partition_memory_bytes(&replacement),
        max_bytes,
        "test requires same-sized replacement partitions"
    );

    let store = InMemoryShuffleStore::new().with_max_bytes(max_bytes);
    store.write_partition(first, 1).await.unwrap();
    store.write_partition(replacement, 2).await.unwrap();

    let read = store.read_partition(&id).await.unwrap().unwrap();
    let values = read.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.value(0), 9);
}

#[tokio::test]
async fn in_memory_delete_job_removes_all_data() {
    let store = InMemoryShuffleStore::new();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));

    for i in 0..4 {
        store
            .write_partition(
                ShufflePartition {
                    id: PartitionId {
                        job_id: "job-clr".to_owned(),
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

    store.delete_job_partitions("job-clr").await.unwrap();

    for i in 0..4 {
        let id = PartitionId {
            job_id: "job-clr".to_owned(),
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
async fn in_memory_overwrite_replaces_data() {
    let store = InMemoryShuffleStore::new();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let id = PartitionId {
        job_id: "job-ow".to_owned(),
        stage_id: "s0".to_owned(),
        partition: 0,
    };

    let p1 = ShufflePartition {
        id: id.clone(),
        schema: schema.clone(),
        batches: vec![
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
                .unwrap(),
        ],
    };
    store.write_partition(p1, 1).await.unwrap();

    let p2 = ShufflePartition {
        id: id.clone(),
        schema: schema.clone(),
        batches: vec![
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![99, 100]))])
                .unwrap(),
        ],
    };
    store.write_partition(p2, 2).await.unwrap();

    let read = store.read_partition(&id).await.unwrap().unwrap();
    assert_eq!(read.batches[0].num_rows(), 2);
    let v = read.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(v.value(0), 99);
    assert_eq!(v.value(1), 100);
}

