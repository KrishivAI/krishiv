#[cfg(test)]
mod shuffle_tests {
    use std::collections::HashSet;
    use std::hash::Hasher;
    use std::sync::Arc;

    use arrow::array::{
        Array, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::{
        CompressionCodec, HashPartitioner, InMemoryShuffleStore, LocalDiskShuffleStore,
        LocalShuffleStore, PartitionId, PartitionState, ShuffleCompression, ShuffleError,
        ShuffleMetadata, ShufflePartition, ShufflePath, ShuffleStore, cleanup_orphans,
        scan_orphans,
    };

    // ── ShufflePath ───────────────────────────────────────────────────────

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

    // ── CompressionCodec ──────────────────────────────────────────────────

    #[test]
    fn compression_codec_default_is_none() {
        assert_eq!(CompressionCodec::default(), CompressionCodec::None);
    }

    #[test]
    fn local_store_default_compression_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalShuffleStore::new(dir.path());
        assert_eq!(store.compression(), CompressionCodec::None);
    }

    #[test]
    fn local_store_with_compression_lz4() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalShuffleStore::new(dir.path()).with_compression(CompressionCodec::Lz4);
        assert_eq!(store.compression(), CompressionCodec::Lz4);
    }

    // ── Compression round-trip tests ──────────────────────────────────────

    #[test]
    fn compression_codec_none_round_trip() {
        let data = b"hello shuffle world";
        let compressed = CompressionCodec::None.compress(data).unwrap();
        let decompressed = CompressionCodec::None.decompress(&compressed).unwrap();
        assert_eq!(&decompressed, data);
    }

    #[test]
    fn compression_codec_lz4_round_trip() {
        let data: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
        let compressed = CompressionCodec::Lz4.compress(&data).unwrap();
        let decompressed = CompressionCodec::Lz4.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data, "LZ4 round-trip must be byte-exact");
    }

    #[test]
    fn compression_codec_zstd_round_trip() {
        let data: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
        let compressed = CompressionCodec::Zstd.compress(&data).unwrap();
        let decompressed = CompressionCodec::Zstd.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data, "Zstd round-trip must be byte-exact");
    }

    #[tokio::test]
    async fn local_store_lz4_write_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalShuffleStore::new(dir.path()).with_compression(CompressionCodec::Lz4);
        let path = ShufflePath::new("job-1", "stage-1", 0);
        let data: Vec<u8> = (0u8..=255).cycle().take(512).collect();
        store.write_partition(&path, &data).await.unwrap();
        let read_back = store.read_partition(&path).await.unwrap();
        assert_eq!(
            read_back, data,
            "LZ4 write/read round-trip must be byte-exact"
        );
    }

    #[tokio::test]
    async fn local_store_zstd_write_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalShuffleStore::new(dir.path()).with_compression(CompressionCodec::Zstd);
        let path = ShufflePath::new("job-1", "stage-1", 0);
        let data: Vec<u8> = (0u8..=255).cycle().take(512).collect();
        store.write_partition(&path, &data).await.unwrap();
        let read_back = store.read_partition(&path).await.unwrap();
        assert_eq!(
            read_back, data,
            "Zstd write/read round-trip must be byte-exact"
        );
    }

    /// GAP-SH-02: Verify that the header codec byte governs decompression even
    /// when the reader store has a different codec configured.
    ///
    /// Write a partition with `CompressionCodec::None` (header byte = 0x00),
    /// then read it back through a store configured with `CompressionCodec::Lz4`.
    /// The reader must use the None codec recorded in the header and return the
    /// original uncompressed bytes without corruption.
    #[tokio::test]
    async fn shuffle_codec_header_mismatch_detected() {
        let dir = tempfile::tempdir().unwrap();
        let write_store =
            LocalShuffleStore::new(dir.path()).with_compression(CompressionCodec::None);
        let read_store = LocalShuffleStore::new(dir.path()).with_compression(CompressionCodec::Lz4);

        let path = ShufflePath::new("job-mismatch", "stage-0", 0);
        let data: Vec<u8> = (0u8..=127).cycle().take(256).collect();

        // Write with None codec — header byte 0x00 is embedded in the file.
        write_store.write_partition(&path, &data).await.unwrap();

        // Read with a store configured for Lz4 — must use the header's None codec,
        // not Lz4, and return the original bytes without error or corruption.
        let read_back = read_store.read_partition(&path).await.unwrap();
        assert_eq!(
            read_back, data,
            "read_partition must use the codec from the file header, not the store config"
        );
    }

    // ── Orphan detection ──────────────────────────────────────────────────

    fn write_ipc_file(base: &std::path::Path, job_id: &str, stage_id: &str, partition_id: u32) {
        let dir = base.join(job_id).join(stage_id);
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join(format!("{partition_id}.ipc"));
        std::fs::write(file, b"dummy").unwrap();
    }

    #[test]
    fn scan_orphans_empty_base_dir_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let active: HashSet<String> = HashSet::new();
        let result = scan_orphans(dir.path(), &active).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn scan_orphans_nonexistent_base_dir_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does_not_exist");
        let active: HashSet<String> = HashSet::new();
        let result = scan_orphans(&missing, &active).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn scan_orphans_all_active_no_orphans() {
        let dir = tempfile::tempdir().unwrap();
        write_ipc_file(dir.path(), "job1", "s0", 0);
        write_ipc_file(dir.path(), "job1", "s0", 1);

        let mut active: HashSet<String> = HashSet::new();
        active.insert("job1".into());

        let result = scan_orphans(dir.path(), &active).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn scan_orphans_inactive_job_returns_ipc_files() {
        let dir = tempfile::tempdir().unwrap();
        write_ipc_file(dir.path(), "dead_job", "s0", 0);
        write_ipc_file(dir.path(), "dead_job", "s0", 1);

        let active: HashSet<String> = HashSet::new();
        let mut result = scan_orphans(dir.path(), &active).unwrap();
        result.sort();

        assert_eq!(result.len(), 2);
        for path in &result {
            assert!(
                path.extension().and_then(|e| e.to_str()) == Some("ipc"),
                "expected .ipc extension"
            );
        }
    }

    #[test]
    fn scan_orphans_mixed_active_and_inactive() {
        let dir = tempfile::tempdir().unwrap();
        write_ipc_file(dir.path(), "active_job", "s0", 0);
        write_ipc_file(dir.path(), "dead_job", "s0", 0);
        write_ipc_file(dir.path(), "dead_job", "s1", 0);

        let mut active: HashSet<String> = HashSet::new();
        active.insert("active_job".into());

        let result = scan_orphans(dir.path(), &active).unwrap();
        assert_eq!(result.len(), 2);
        // None of the orphans should be under active_job.
        for path in &result {
            assert!(
                !path.to_string_lossy().contains("active_job"),
                "active job files should not be orphans"
            );
        }
    }

    #[test]
    fn cleanup_orphans_deletes_files_and_returns_count() {
        let dir = tempfile::tempdir().unwrap();
        write_ipc_file(dir.path(), "dead_job", "s0", 0);
        write_ipc_file(dir.path(), "dead_job", "s0", 1);

        let active: HashSet<String> = HashSet::new();
        let count = cleanup_orphans(dir.path(), &active).unwrap();
        assert_eq!(count, 2);

        // Files should be gone.
        let remaining = scan_orphans(dir.path(), &active).unwrap();
        assert!(remaining.is_empty());
    }

    // ── HashPartitioner ───────────────────────────────────────────────────

    fn make_int32_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let arr = Arc::new(Int32Array::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    fn make_utf8_batch(values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        let arr = Arc::new(StringArray::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    #[test]
    fn partitioner_int32_preserves_total_rows() {
        let batch = make_int32_batch(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        let partitioner = HashPartitioner::new("key", 4);
        let partitions = partitioner.partition(&batch).unwrap();
        assert_eq!(partitions.len(), 4);
        let total: usize = partitions.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total, 8);
    }

    #[test]
    fn partitioner_int32_each_row_in_correct_bucket() {
        let values = vec![10i32, 20, 30, 40, 50];
        let batch = make_int32_batch(values.clone());
        let buckets = 3u32;
        let partitioner = HashPartitioner::new("key", buckets);
        let partitions = partitioner.partition(&batch).unwrap();

        // Verify each row ends up in the expected bucket using XxHash64 (stable hash).
        for &v in &values {
            let mut hasher = twox_hash::XxHash64::with_seed(0);
            hasher.write(&(v as i64).to_le_bytes());
            let expected_bucket = (hasher.finish() % buckets as u64) as usize;
            let arr = partitions[expected_bucket]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let found = (0..arr.len()).any(|i| arr.value(i) == v);
            assert!(
                found,
                "value {v} not found in expected bucket {expected_bucket}"
            );
        }
    }

    #[test]
    fn partitioner_utf8_preserves_total_rows() {
        let batch = make_utf8_batch(vec!["alpha", "beta", "gamma", "delta"]);
        let partitioner = HashPartitioner::new("key", 2);
        let partitions = partitioner.partition(&batch).unwrap();
        assert_eq!(partitions.len(), 2);
        let total: usize = partitions.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn partitioner_utf8_each_row_in_correct_bucket() {
        let values = vec!["hello", "world", "foo", "bar"];
        let batch = make_utf8_batch(values.clone());
        let buckets = 3u32;
        let partitioner = HashPartitioner::new("key", buckets);
        let partitions = partitioner.partition(&batch).unwrap();

        for &v in &values {
            let mut hasher = twox_hash::XxHash64::with_seed(0);
            hasher.write(v.as_bytes());
            let expected_bucket = (hasher.finish() % buckets as u64) as usize;
            let arr = partitions[expected_bucket]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let found = (0..arr.len()).any(|i| arr.value(i) == v);
            assert!(
                found,
                "value {v} not found in expected bucket {expected_bucket}"
            );
        }
    }

    #[test]
    fn partitioner_unsupported_type_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::Float64,
            false,
        )]));
        let arr = Arc::new(arrow::array::Float64Array::from(vec![1.0f64]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let partitioner = HashPartitioner::new("key", 4);
        let err = partitioner.partition(&batch).unwrap_err();
        assert!(
            matches!(err, ShuffleError::TypeMismatch { .. }),
            "expected TypeMismatch error for unsupported type"
        );
    }

    #[test]
    fn partitioner_empty_batch_produces_empty_buckets() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let arr = Arc::new(Int32Array::from(Vec::<i32>::new()));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let partitioner = HashPartitioner::new("key", 3);
        let partitions = partitioner.partition(&batch).unwrap();
        assert_eq!(partitions.len(), 3);
        assert!(partitions.iter().all(|p| p.num_rows() == 0));
    }

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

    // ── ObjectStoreShuffleStore ───────────────────────────────────────────

    use crate::ObjectStoreShuffleStore;
    use object_store::memory::InMemory;

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
        let store = ObjectStoreShuffleStore::new(inner, "shuffle-test");

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

        // Create a spill store at path, then make the dir read-only so writes fail.
        let spill_path = dir.path().join("spill_fail");
        std::fs::create_dir_all(&spill_path).unwrap();
        use std::fs::Permissions;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&spill_path, Permissions::from_mode(0o000)).unwrap();
        let spill = Arc::new(LocalDiskShuffleStore::new(&spill_path).unwrap());

        // Use a tiny max_bytes so writes trigger spills immediately.
        let store = InMemoryShuffleStore::new()
            .with_max_bytes(64)
            .with_spill_store(spill);

        // Write partitions at or below max_bytes — first few should stay in memory.
        let p0 = make_store_partition("job-spill-fail", "s0", 0);
        let p1 = make_store_partition("job-spill-fail", "s0", 1);
        let id0 = p0.id.clone();
        let id1 = p1.id.clone();

        // First write succeeds (below max_bytes, stays in memory).
        store.write_partition(p0, 1).await.unwrap();
        // Second write triggers a spill of the oldest partition — this spill
        // will FAIL because the spill dir is read-only.  Data must remain
        // accessible in memory and bytes_used must not be corrupted.
        store.write_partition(p1, 1).await.unwrap();

        // Both partitions must still be readable.
        assert!(
            store.read_partition(&id0).await.unwrap().is_some(),
            "p0 lost after failed spill"
        );
        assert!(
            store.read_partition(&id1).await.unwrap().is_some(),
            "p1 missing after write"
        );

        // Restore permissions for cleanup.
        std::fs::set_permissions(&spill_path, Permissions::from_mode(0o755)).unwrap();
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

    // ── CompressionCodec: Arrow IPC roundtrip ────────────────────────────

    #[test]
    fn compression_lz4_arrow_ipc_roundtrip() {
        use arrow::ipc::reader::StreamReader;
        use arrow::ipc::writer::StreamWriter;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap();

        // Serialize to IPC bytes.
        let mut ipc_buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc_buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let compressed = CompressionCodec::Lz4.compress(&ipc_buf).unwrap();
        let decompressed = CompressionCodec::Lz4.decompress(&compressed).unwrap();
        assert_eq!(decompressed, ipc_buf);

        // Deserialize and verify values.
        let cursor = std::io::Cursor::new(&decompressed);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let rb = reader.next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 3);
        let ids = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 3);
    }

    #[test]
    fn compression_zstd_arrow_ipc_roundtrip() {
        use arrow::ipc::reader::StreamReader;
        use arrow::ipc::writer::StreamWriter;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
            ],
        )
        .unwrap();

        let mut ipc_buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc_buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let compressed = CompressionCodec::Zstd.compress(&ipc_buf).unwrap();
        let decompressed = CompressionCodec::Zstd.decompress(&compressed).unwrap();
        assert_eq!(decompressed, ipc_buf);

        let cursor = std::io::Cursor::new(&decompressed);
        let mut reader = StreamReader::try_new(cursor, None).unwrap();
        let rb = reader.next().unwrap().unwrap();
        assert_eq!(rb.num_rows(), 3);
        let names = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "alpha");
        assert_eq!(names.value(1), "beta");
        assert_eq!(names.value(2), "gamma");
    }

    #[test]
    fn compression_large_zstd_roundtrip() {
        let data: Vec<u8> = (0u32..8192).flat_map(|v| v.to_le_bytes()).collect();
        let compressed = CompressionCodec::Zstd.compress(&data).unwrap();
        assert!(
            compressed.len() < data.len(),
            "Zstd should compress 32KB of sequential u32s"
        );
        let decompressed = CompressionCodec::Zstd.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compression_large_lz4_roundtrip() {
        let data: Vec<u8> = (0u32..8192).flat_map(|v| v.to_le_bytes()).collect();
        let compressed = CompressionCodec::Lz4.compress(&data).unwrap();
        let decompressed = CompressionCodec::Lz4.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    // ── ShuffleMetadata: comprehensive state transitions ─────────────────

    #[test]
    fn metadata_state_transitions_pending_available() {
        let mut meta = ShuffleMetadata::new();
        let p = ShufflePath::new("j", "s", 0);
        assert_eq!(meta.state(&p), None);
        meta.mark_pending(&p).unwrap();
        assert_eq!(meta.state(&p), Some(&PartitionState::Pending));
        meta.mark_available(&p);
        assert_eq!(meta.state(&p), Some(&PartitionState::Available));
    }

    #[test]
    fn metadata_state_transitions_pending_failed() {
        let mut meta = ShuffleMetadata::new();
        let p = ShufflePath::new("j", "s", 1);
        meta.mark_pending(&p).unwrap();
        meta.mark_failed(&p, "timeout".into());
        assert_eq!(
            meta.state(&p),
            Some(&PartitionState::Failed {
                reason: "timeout".into()
            })
        );
    }

    #[test]
    fn metadata_available_count_and_total_count() {
        let mut meta = ShuffleMetadata::new();
        let p0 = ShufflePath::new("j", "s", 0);
        let p1 = ShufflePath::new("j", "s", 1);
        let p2 = ShufflePath::new("j", "s", 2);

        meta.mark_pending(&p0).unwrap();
        meta.mark_pending(&p1).unwrap();
        assert_eq!(meta.total_count(), 2);
        assert_eq!(meta.available_count(), 0);

        meta.mark_available(&p0);
        assert_eq!(meta.available_count(), 1);

        meta.mark_pending(&p2).unwrap();
        meta.mark_available(&p2);
        assert_eq!(meta.available_count(), 2);
        assert_eq!(meta.total_count(), 3);
    }

    #[test]
    fn metadata_available_count_excludes_failed() {
        let mut meta = ShuffleMetadata::new();
        let p = ShufflePath::new("j", "s", 0);
        meta.mark_pending(&p).unwrap();
        meta.mark_failed(&p, "oops".into());
        assert_eq!(meta.available_count(), 0);
        assert_eq!(meta.total_count(), 1);
    }

    #[test]
    fn metadata_state_returns_none_for_unknown_path() {
        let meta = ShuffleMetadata::new();
        let p = ShufflePath::new("missing", "s", 0);
        assert!(meta.state(&p).is_none());
    }

    #[test]
    fn metadata_cap_error_has_correct_limit() {
        let mut meta = ShuffleMetadata::new().with_max_partitions(3);
        meta.mark_pending(&ShufflePath::new("j", "s", 0)).unwrap();
        meta.mark_pending(&ShufflePath::new("j", "s", 1)).unwrap();
        meta.mark_pending(&ShufflePath::new("j", "s", 2)).unwrap();
        let err = meta
            .mark_pending(&ShufflePath::new("j", "s", 3))
            .unwrap_err();
        match err {
            ShuffleError::TooManyPartitions { limit } => assert_eq!(limit, 3),
            other => panic!("expected TooManyPartitions, got {other}"),
        }
    }

    #[test]
    fn metadata_all_available_treats_pending_as_not_available() {
        let mut meta = ShuffleMetadata::new();
        let p = ShufflePath::new("j", "s", 0);
        meta.mark_pending(&p).unwrap();
        assert!(!meta.all_available(&[p]));
    }

    #[test]
    fn metadata_all_available_treats_failed_as_not_available() {
        let mut meta = ShuffleMetadata::new();
        let p = ShufflePath::new("j", "s", 0);
        meta.mark_pending(&p).unwrap();
        meta.mark_failed(&p, "err".into());
        assert!(!meta.all_available(&[p]));
    }

    // ── HashPartitioner: additional key types ────────────────────────────

    fn make_int64_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        let arr = Arc::new(Int64Array::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    fn make_string_view_batch(values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::Utf8View,
            false,
        )]));
        let arr = Arc::new(StringViewArray::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    fn make_large_utf8_batch(values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::LargeUtf8,
            false,
        )]));
        let arr = Arc::new(LargeStringArray::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    #[test]
    fn partitioner_int64_preserves_total_rows() {
        let batch = make_int64_batch(vec![100, 200, 300, 400]);
        let partitioner = HashPartitioner::new("key", 3);
        let partitions = partitioner.partition(&batch).unwrap();
        assert_eq!(partitions.len(), 3);
        let total: usize = partitions.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn partitioner_int64_each_row_in_correct_bucket() {
        let values = vec![10i64, 20, 30];
        let batch = make_int64_batch(values.clone());
        let buckets = 2u32;
        let partitioner = HashPartitioner::new("key", buckets);
        let partitions = partitioner.partition(&batch).unwrap();

        for &v in &values {
            let mut hasher = twox_hash::XxHash64::with_seed(0);
            hasher.write(&v.to_le_bytes());
            let expected_bucket = (hasher.finish() % buckets as u64) as usize;
            let arr = partitions[expected_bucket]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let found = (0..arr.len()).any(|i| arr.value(i) == v);
            assert!(
                found,
                "value {v} not found in expected bucket {expected_bucket}"
            );
        }
    }

    #[test]
    fn partitioner_utf8view_preserves_total_rows() {
        let batch = make_string_view_batch(vec!["one", "two", "three", "four"]);
        let partitioner = HashPartitioner::new("key", 2);
        let partitions = partitioner.partition(&batch).unwrap();
        assert_eq!(partitions.len(), 2);
        let total: usize = partitions.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn partitioner_utf8view_each_row_in_correct_bucket() {
        let values = vec!["alpha", "beta", "gamma"];
        let batch = make_string_view_batch(values.clone());
        let buckets = 2u32;
        let partitioner = HashPartitioner::new("key", buckets);
        let partitions = partitioner.partition(&batch).unwrap();

        for &v in &values {
            let mut hasher = twox_hash::XxHash64::with_seed(0);
            hasher.write(v.as_bytes());
            let expected_bucket = (hasher.finish() % buckets as u64) as usize;
            let arr = partitions[expected_bucket]
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            let found = (0..arr.len()).any(|i| arr.value(i) == v);
            assert!(
                found,
                "value {v} not found in expected bucket {expected_bucket}"
            );
        }
    }

    #[test]
    fn partitioner_large_utf8_preserves_total_rows() {
        let batch = make_large_utf8_batch(vec!["wide", "data", "strings"]);
        let partitioner = HashPartitioner::new("key", 4);
        let partitions = partitioner.partition(&batch).unwrap();
        assert_eq!(partitions.len(), 4);
        let total: usize = partitions.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn partitioner_large_utf8_each_row_in_correct_bucket() {
        let values = vec!["lorem", "ipsum", "dolor"];
        let batch = make_large_utf8_batch(values.clone());
        let buckets = 3u32;
        let partitioner = HashPartitioner::new("key", buckets);
        let partitions = partitioner.partition(&batch).unwrap();

        for &v in &values {
            let mut hasher = twox_hash::XxHash64::with_seed(0);
            hasher.write(v.as_bytes());
            let expected_bucket = (hasher.finish() % buckets as u64) as usize;
            let arr = partitions[expected_bucket]
                .column(0)
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap();
            let found = (0..arr.len()).any(|i| arr.value(i) == v);
            assert!(
                found,
                "value {v} not found in expected bucket {expected_bucket}"
            );
        }
    }

    #[test]
    fn partitioner_single_bucket_putseverything_in_one_partition() {
        let batch = make_int32_batch(vec![1, 2, 3, 4, 5]);
        let partitioner = HashPartitioner::new("key", 1);
        let partitions = partitioner.partition(&batch).unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].num_rows(), 5);
    }

    #[test]
    fn partitioner_many_buckets_sparse_distribution() {
        let batch = make_int32_batch(vec![1, 2]);
        let partitioner = HashPartitioner::new("key", 16);
        let partitions = partitioner.partition(&batch).unwrap();
        assert_eq!(partitions.len(), 16);
        let total: usize = partitions.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total, 2);
        let nonempty = partitions.iter().filter(|p| p.num_rows() > 0).count();
        assert!(nonempty <= 2, "at most 2 buckets should be non-empty");
    }

    #[test]
    fn partitioner_missing_column_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int32,
            false,
        )]));
        let arr = Arc::new(Int32Array::from(vec![1]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let partitioner = HashPartitioner::new("key", 2);
        let err = partitioner.partition(&batch).unwrap_err();
        assert!(
            matches!(err, ShuffleError::Io(_)),
            "expected Io error for missing column, got {err}"
        );
    }
}
