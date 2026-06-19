#![forbid(unsafe_code)]

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use krishiv_shuffle::{
    CompressionCodec, HashPartitioner, InMemoryShuffleStore, LocalDiskShuffleStore, PartitionId,
    ShuffleCompression, ShufflePartition, ShuffleStore, cleanup_orphans, scan_orphans,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_multi_column_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let ids = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
    let names = Arc::new(StringArray::from(vec![
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
    ]));
    let values = Arc::new(Int32Array::from(vec![
        10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
    ]));
    RecordBatch::try_new(schema, vec![ids, names, values]).unwrap()
}

fn make_simple_batch(values: Vec<i32>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
}

fn make_store_partition(
    job_id: &str,
    stage_id: &str,
    partition: u32,
    values: Vec<i32>,
) -> ShufflePartition {
    let batch = make_simple_batch(values);
    let schema = batch.schema();
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

// ── Tests ─────────────────────────────────────────────────────────────────────

/// 1. Hash partition: partition data → write to store → read back → verify.
#[tokio::test(flavor = "multi_thread")]
async fn hash_partition_write_read_verify() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();

    let batch = make_multi_column_batch();
    let partitioner = HashPartitioner::new("id", 4);
    let partitions = partitioner.partition(&batch).unwrap();
    assert_eq!(partitions.len(), 4);

    // Write all partitions to the store.
    let mut ids = Vec::new();
    for (i, part) in partitions.into_iter().enumerate() {
        let id = PartitionId {
            job_id: "job-pipeline".to_owned(),
            stage_id: "stage-0".to_owned(),
            partition: i as u32,
        };
        let sp = ShufflePartition {
            id: id.clone(),
            schema: part.schema(),
            batches: vec![part],
        };
        store.write_partition(sp, 1).await.unwrap();
        ids.push(id);
    }

    // Read back all partitions and verify row counts.
    let mut total_rows = 0usize;
    for id in &ids {
        let read = store.read_partition(id).await.unwrap();
        assert!(read.is_some(), "partition must exist after write");
        total_rows += read
            .unwrap()
            .batches
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
    }
    assert_eq!(
        total_rows, 10,
        "all rows must be preserved after partitioning"
    );
}

/// 2. Shuffle with compression: LZ4 roundtrip with real Arrow data.
#[tokio::test(flavor = "multi_thread")]
async fn shuffle_compression_lz4_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path())
        .unwrap()
        .with_compression(ShuffleCompression::Lz4);

    let batch = make_multi_column_batch();
    let partition = ShufflePartition {
        id: PartitionId {
            job_id: "job-lz4".to_owned(),
            stage_id: "s0".to_owned(),
            partition: 0,
        },
        schema: batch.schema(),
        batches: vec![batch],
    };
    let id = partition.id.clone();
    store.write_partition(partition, 1).await.unwrap();

    let read = store.read_partition(&id).await.unwrap().unwrap();
    assert_eq!(read.batches.len(), 1);
    let rb = &read.batches[0];
    assert_eq!(rb.num_rows(), 10);

    // Verify data values.
    let ids = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    for i in 0..10 {
        assert_eq!(ids.value(i), (i + 1) as i64);
    }
    let names = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    let expected_names = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
    for (i, expected) in expected_names.iter().enumerate() {
        assert_eq!(names.value(i), *expected);
    }
}

/// 3. Shuffle with compression: ZSTD roundtrip with real Arrow data.
#[tokio::test(flavor = "multi_thread")]
async fn shuffle_compression_zstd_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path())
        .unwrap()
        .with_compression(ShuffleCompression::Zstd);

    let batch = make_multi_column_batch();
    let partition = ShufflePartition {
        id: PartitionId {
            job_id: "job-zstd".to_owned(),
            stage_id: "s0".to_owned(),
            partition: 0,
        },
        schema: batch.schema(),
        batches: vec![batch],
    };
    let id = partition.id.clone();
    store.write_partition(partition, 1).await.unwrap();

    let read = store.read_partition(&id).await.unwrap().unwrap();
    assert_eq!(read.batches.len(), 1);
    let rb = &read.batches[0];
    assert_eq!(rb.num_rows(), 10);

    let values = rb.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
    let expected_values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    for (i, expected) in expected_values.iter().enumerate() {
        assert_eq!(values.value(i), *expected);
    }
}

/// 4. Shuffle lease fencing: write with token → verify → reject stale token.
#[tokio::test(flavor = "multi_thread")]
async fn shuffle_lease_fencing_write_verify_reject_stale() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();

    let partition = make_store_partition("job-lease", "s0", 0, vec![1, 2, 3]);
    let id = partition.id.clone();

    // Write with token 5 — should succeed.
    store.write_partition(partition.clone(), 5).await.unwrap();

    // Read back — should be present.
    let read = store.read_partition(&id).await.unwrap();
    assert!(read.is_some(), "partition must exist after write");

    // Try to overwrite with stale token 3 — should be rejected.
    let err = store
        .write_partition(partition.clone(), 3)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            krishiv_shuffle::ShuffleError::StaleLeaseToken {
                expected: 5,
                actual: 3
            }
        ),
        "expected StaleLeaseToken(expected=5, actual=3), got: {err}"
    );

    // Overwrite with fresh token 6 — should succeed.
    let new_partition = make_store_partition("job-lease", "s0", 0, vec![10, 20, 30]);
    store.write_partition(new_partition, 6).await.unwrap();

    let read = store.read_partition(&id).await.unwrap().unwrap();
    let values = read.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.value(0), 10);
    assert_eq!(values.value(1), 20);
    assert_eq!(values.value(2), 30);
}

/// 5. Orphan cleanup: write data → mark job complete → cleanup orphans → verify gone.
#[tokio::test(flavor = "multi_thread")]
async fn orphan_cleanup_after_job_complete() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();

    // Write IPC files for two jobs.
    let write_ipc = |job_id: &str, stage_id: &str, partition_id: u32| {
        let job_dir = base.join(job_id).join(stage_id);
        std::fs::create_dir_all(&job_dir).unwrap();
        let file = job_dir.join(format!("{partition_id}.ipc"));
        std::fs::write(file, b"shuffle data").unwrap();
    };

    write_ipc("active-job", "s0", 0);
    write_ipc("active-job", "s0", 1);
    write_ipc("dead-job", "s0", 0);
    write_ipc("dead-job", "s0", 1);

    // Only active-job is still running.
    let mut active = HashSet::new();
    active.insert("active-job".to_owned());

    // Scan orphans — should find dead-job files.
    let orphans = scan_orphans(base, &active).unwrap();
    assert_eq!(orphans.len(), 2, "should find 2 orphan files from dead-job");
    for path in &orphans {
        assert!(
            path.to_string_lossy().contains("dead-job"),
            "orphan must belong to dead-job: {}",
            path.display()
        );
    }

    // Cleanup orphans.
    let count = cleanup_orphans(base, &active).unwrap();
    assert_eq!(count, 2, "should clean up 2 orphan files");

    // Verify dead-job files are gone.
    let orphans_after = scan_orphans(base, &active).unwrap();
    assert!(
        orphans_after.is_empty(),
        "no orphans should remain after cleanup"
    );

    // Verify active-job files are untouched.
    assert!(base.join("active-job").join("s0").join("0.ipc").exists());
    assert!(base.join("active-job").join("s0").join("1.ipc").exists());
}

/// 6. Multi-partition shuffle: partition into N → write all → read all → verify completeness.
#[tokio::test(flavor = "multi_thread")]
async fn multi_partition_shuffle_completeness() {
    let store = InMemoryShuffleStore::new();

    let batch = make_simple_batch(vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
    let num_buckets = 5u32;
    let partitioner = HashPartitioner::new("v", num_buckets);
    let partitions = partitioner.partition(&batch).unwrap();
    assert_eq!(partitions.len(), num_buckets as usize);

    // Write all partitions.
    let mut ids = Vec::new();
    for (i, part) in partitions.into_iter().enumerate() {
        let id = PartitionId {
            job_id: "job-multi".to_owned(),
            stage_id: "s0".to_owned(),
            partition: i as u32,
        };
        let sp = ShufflePartition {
            id: id.clone(),
            schema: part.schema(),
            batches: vec![part],
        };
        store.write_partition(sp, 1).await.unwrap();
        ids.push(id);
    }

    // Read all partitions and verify total row count.
    let mut total_rows = 0usize;
    let mut all_values = Vec::new();
    for id in &ids {
        let read = store.read_partition(id).await.unwrap();
        assert!(read.is_some(), "partition must exist");
        let partition = read.unwrap();
        for batch in &partition.batches {
            total_rows += batch.num_rows();
            let col = batch
                .column_by_name("v")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..col.len() {
                all_values.push(col.value(i));
            }
        }
    }

    assert_eq!(total_rows, 10, "total rows must equal input batch");
    all_values.sort();
    let mut expected: Vec<i32> = (1..=10).map(|x| x * 10).collect();
    expected.sort();
    assert_eq!(all_values, expected, "all values must be preserved");
}

/// 7. Hash partitioner with string keys → write → read → verify.
#[tokio::test(flavor = "multi_thread")]
async fn hash_partition_string_key_write_read() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("sales", DataType::Int64, false),
    ]));
    let regions = Arc::new(StringArray::from(vec![
        "us-east", "us-west", "eu-west", "ap-south", "us-east", "eu-west",
    ]));
    let sales = Arc::new(Int64Array::from(vec![100, 200, 150, 300, 120, 180]));
    let batch = RecordBatch::try_new(schema, vec![regions, sales]).unwrap();

    let partitioner = HashPartitioner::new("region", 3);
    let partitions = partitioner.partition(&batch).unwrap();
    assert_eq!(partitions.len(), 3);

    let mut total_rows = 0usize;
    for (i, part) in partitions.into_iter().enumerate() {
        let id = PartitionId {
            job_id: "job-region".to_owned(),
            stage_id: "s0".to_owned(),
            partition: i as u32,
        };
        let sp = ShufflePartition {
            id: id.clone(),
            schema: part.schema(),
            batches: vec![part],
        };
        store.write_partition(sp, 1).await.unwrap();

        let read = store.read_partition(&id).await.unwrap().unwrap();
        total_rows += read.batches.iter().map(|b| b.num_rows()).sum::<usize>();
    }

    assert_eq!(total_rows, 6, "all rows must be preserved");
}

/// 8. Lease fencing on InMemoryShuffleStore: register → write → reject stale.
#[tokio::test(flavor = "multi_thread")]
async fn memory_store_lease_fencing_full_cycle() {
    let store = InMemoryShuffleStore::new();
    let partition = make_store_partition("job-lease-mem", "s0", 0, vec![42]);
    let id = partition.id.clone();

    // Register lease with token 10.
    store
        .register_partition_lease(id.clone(), 10)
        .await
        .unwrap();

    // Write with matching token 10 — should succeed.
    store.write_partition(partition.clone(), 10).await.unwrap();

    // Read back — must be present.
    let read = store.read_partition(&id).await.unwrap();
    assert!(read.is_some());

    // Register newer lease with token 15.
    store
        .register_partition_lease(id.clone(), 15)
        .await
        .unwrap();

    // Write with stale token 10 — must be rejected.
    let err = store
        .write_partition(partition.clone(), 10)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            krishiv_shuffle::ShuffleError::StaleLeaseToken {
                expected: 15,
                actual: 10
            }
        ),
        "expected StaleLeaseToken(expected=15, actual=10), got: {err}"
    );

    // Write with fresh token 15 — should succeed.
    let new_partition = make_store_partition("job-lease-mem", "s0", 0, vec![99]);
    store.write_partition(new_partition, 15).await.unwrap();

    let read = store.read_partition(&id).await.unwrap().unwrap();
    let values = read.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.value(0), 99);
}

/// 9. Orphan scan on empty directory returns empty.
#[test]
fn orphan_scan_empty_directory() {
    let dir = tempfile::tempdir().unwrap();
    let active = HashSet::new();
    let result = scan_orphans(dir.path(), &active).unwrap();
    assert!(result.is_empty());
}

/// 10. Orphan scan with all jobs active returns empty.
#[test]
fn orphan_scan_all_active_returns_empty() {
    let dir = tempfile::tempdir().unwrap();

    let job_dir = dir.path().join("active-job").join("s0");
    std::fs::create_dir_all(&job_dir).unwrap();
    std::fs::write(job_dir.join("0.ipc"), b"data").unwrap();

    let mut active = HashSet::new();
    active.insert("active-job".to_owned());

    let result = scan_orphans(dir.path(), &active).unwrap();
    assert!(result.is_empty());
}

/// 11. Compression: LZ4 raw codec roundtrip.
#[test]
fn compression_lz4_raw_roundtrip() {
    let data: Vec<u8> = (0u8..=255).cycle().take(2048).collect();
    let compressed = CompressionCodec::Lz4.compress(&data).unwrap();
    let decompressed = CompressionCodec::Lz4.decompress(&compressed).unwrap();
    assert_eq!(decompressed, data);
}

/// 12. Compression: ZSTD raw codec roundtrip.
#[test]
fn compression_zstd_raw_roundtrip() {
    let data: Vec<u8> = (0u8..=255).cycle().take(2048).collect();
    let compressed = CompressionCodec::Zstd.compress(&data).unwrap();
    let decompressed = CompressionCodec::Zstd.decompress(&compressed).unwrap();
    assert_eq!(decompressed, data);
}

/// 13. Multi-column data integrity through compression: LZ4.
#[tokio::test(flavor = "multi_thread")]
async fn multi_column_data_integrity_lz4() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path())
        .unwrap()
        .with_compression(ShuffleCompression::Lz4);

    let batch = make_multi_column_batch();
    let partition = ShufflePartition {
        id: PartitionId {
            job_id: "job-integrity-lz4".to_owned(),
            stage_id: "s0".to_owned(),
            partition: 0,
        },
        schema: batch.schema(),
        batches: vec![batch],
    };
    let id = partition.id.clone();
    store.write_partition(partition, 1).await.unwrap();

    let read = store.read_partition(&id).await.unwrap().unwrap();
    let rb = &read.batches[0];
    assert_eq!(rb.num_rows(), 10);
    assert_eq!(rb.num_columns(), 3);

    // Verify all columns.
    let ids = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    for i in 0..10 {
        assert_eq!(ids.value(i), (i + 1) as i64);
    }
    let names = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    let expected = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
    for (i, e) in expected.iter().enumerate() {
        assert_eq!(names.value(i), *e);
    }
    let values = rb.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
    for i in 0..10 {
        assert_eq!(values.value(i), (i + 1) as i32 * 10);
    }
}

/// 14. Orphan cleanup with mixed active/inactive jobs and multiple stages.
#[test]
fn orphan_cleanup_mixed_active_inactive_multi_stage() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();

    // Create files for active-job (2 stages) and dead-job (2 stages).
    for job_id in &["active-job", "dead-job"] {
        for stage_id in &["s0", "s1"] {
            let stage_dir = base.join(job_id).join(stage_id);
            std::fs::create_dir_all(&stage_dir).unwrap();
            for p in 0..2 {
                std::fs::write(stage_dir.join(format!("{p}.ipc")), b"data").unwrap();
            }
        }
    }

    let mut active = HashSet::new();
    active.insert("active-job".to_owned());

    let orphans = scan_orphans(base, &active).unwrap();
    assert_eq!(orphans.len(), 4, "dead-job has 4 files across 2 stages");

    let count = cleanup_orphans(base, &active).unwrap();
    assert_eq!(count, 4);

    // Verify dead-job directories are cleaned up (empty dirs may remain, but no .ipc files).
    let orphans_after = scan_orphans(base, &active).unwrap();
    assert!(orphans_after.is_empty());

    // Verify active-job files untouched.
    assert!(base.join("active-job").join("s0").join("0.ipc").exists());
    assert!(base.join("active-job").join("s1").join("1.ipc").exists());
}

/// 15. Spill store: in-memory store spills to disk when exceeding byte cap.
#[tokio::test(flavor = "multi_thread")]
async fn in_memory_spill_to_disk_and_read_back() {
    let dir = tempfile::tempdir().unwrap();
    let spill = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());
    let store = InMemoryShuffleStore::new()
        .with_max_bytes(64)
        .with_spill_store(Arc::clone(&spill));

    // Write multiple partitions to trigger spill.
    let p0 = make_store_partition("job-spill-test", "s0", 0, vec![1, 2, 3]);
    let p1 = make_store_partition("job-spill-test", "s0", 4, vec![5, 6, 7]);
    let id0 = p0.id.clone();
    let id1 = p1.id.clone();

    store.write_partition(p0, 1).await.unwrap();
    store.write_partition(p1, 1).await.unwrap();

    // Both must still be readable.
    let r0 = store.read_partition(&id0).await.unwrap();
    let r1 = store.read_partition(&id1).await.unwrap();
    assert!(r0.is_some(), "p0 must be readable after spill");
    assert!(r1.is_some(), "p1 must be readable");

    // Verify values.
    let r0_data = r0.unwrap();
    let v0 = r0_data.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(v0.value(0), 1);
    assert_eq!(v0.value(1), 2);
    assert_eq!(v0.value(2), 3);
}
