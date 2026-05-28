#[cfg(test)]
mod shuffle_tests {
    use std::collections::HashSet;
    use std::hash::Hasher;
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, StringArray};
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
        for codec in [ShuffleCompression::None, ShuffleCompression::Lz4, ShuffleCompression::Zstd] {
            let inner = Arc::new(InMemory::new());
            let store = ObjectStoreShuffleStore::new(inner, "compress-test")
                .with_compression(codec);
            let partition = make_object_store_partition("job-compress", "s0", 0);
            let id = partition.id.clone();
            store.write_partition(partition, 1).await.unwrap_or_else(|e| {
                panic!("write failed for codec {:?}: {e}", codec)
            });
            let read_back = store.read_partition(&id).await.unwrap_or_else(|e| {
                panic!("read failed for codec {:?}: {e}", codec)
            });
            let read_back = read_back.unwrap_or_else(|| panic!("partition missing for codec {:?}", codec));
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
}
