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

#[tokio::test]
async fn corrupt_parquet_file_returns_content_hash_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();

    let partition = make_store_partition("job-corrupt", "s0", 0);
    let id = partition.id.clone();
    store.write_partition(partition, 1).await.unwrap();

    // Corrupt the parquet file on disk
    use std::io::Write;
    let path = dir.path().join("job-corrupt/s0/0.parquet");
    let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.write_all(b"CORRUPTED PARQUET DATA").unwrap();
    drop(f);

    let err = store.read_partition(&id).await.unwrap_err();
    assert!(
        matches!(err, ShuffleError::ContentHashMismatch { .. }),
        "expected ContentHashMismatch for corrupt parquet, got {err}"
    );
}

#[tokio::test]
async fn disk_store_restart_reads_with_persisted_hash_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();

    let partition = make_store_partition("job-hash-restart", "s0", 0);
    let id = partition.id.clone();
    store.write_partition(partition, 1).await.unwrap();
    drop(store);

    let restarted = LocalDiskShuffleStore::new(dir.path()).unwrap();
    let read = restarted.read_partition(&id).await.unwrap().unwrap();
    assert_eq!(read.batches.len(), 1);
    assert_eq!(read.batches[0].num_rows(), 3);
}

#[tokio::test]
async fn content_hash_mismatch_detected_after_restart_on_tampered_partition() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();

    let partition = make_store_partition("job-hash", "s0", 0);
    let id = partition.id.clone();
    store.write_partition(partition, 1).await.unwrap();
    drop(store);

    // Tamper the partition file on disk so hash won't match on re-read
    use std::io::Write;
    let path = dir.path().join("job-hash/s0/0.parquet");
    let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.write_all(b"TAMPERED DATA OVERWRITE").unwrap();
    drop(f);

    let store2 = LocalDiskShuffleStore::new(dir.path()).unwrap();
    let err = store2.read_partition(&id).await.unwrap_err();
    assert!(
        matches!(err, ShuffleError::ContentHashMismatch { .. }),
        "expected ContentHashMismatch for tampered parquet after restart, got {err}"
    );
}

#[tokio::test]
async fn disk_store_data_without_hash_sidecar_warns_not_fails_after_restart() {
    // SH5: a missing hash sidecar is not a corruption signal — it can
    // happen when the data file was committed but the sidecar rename was
    // lost (the "data-then-hash" rename order still has a small
    // crash window between the two renames). The read path must
    // tolerate the missing sidecar with a warning, not a hard
    // `ContentHashMismatch`.
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();

    let partition = make_store_partition("job-missing-disk-hash", "s0", 0);
    let id = partition.id.clone();
    store.write_partition(partition, 1).await.unwrap();
    drop(store);

    let hash_path = dir.path().join("job-missing-disk-hash/s0/0.parquet.blake3");
    std::fs::remove_file(hash_path).unwrap();

    let restarted = LocalDiskShuffleStore::new(dir.path()).unwrap();
    let read = restarted.read_partition(&id).await.expect("read must succeed");
    let sp = read.expect("partition present");
    assert_eq!(sp.batches.len(), 1);
}

#[tokio::test]
async fn disk_store_malformed_hash_sidecar_fails_closed_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalDiskShuffleStore::new(dir.path()).unwrap();

    let partition = make_store_partition("job-bad-disk-hash", "s0", 0);
    let id = partition.id.clone();
    store.write_partition(partition, 1).await.unwrap();
    drop(store);

    let hash_path = dir.path().join("job-bad-disk-hash/s0/0.parquet.blake3");
    std::fs::write(hash_path, b"not-a-blake3-digest").unwrap();

    let restarted = LocalDiskShuffleStore::new(dir.path()).unwrap();
    let err = restarted.read_partition(&id).await.unwrap_err();
    assert!(
        matches!(err, ShuffleError::ContentHashMismatch { .. }),
        "expected ContentHashMismatch for malformed local hash sidecar, got {err}"
    );
}

#[tokio::test]
async fn http_shuffle_svc_token_auth_enforced() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let dir = tempfile::tempdir().unwrap();

    // Bind TcpListener on dynamic port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Spawn shuffle service with configured token in state
    let store = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());
    let state = crate::shuffle_svc::ShuffleSvcState {
        store,
        token: Some("secure-api-key".to_owned()),
    };
    let app = axum::Router::new()
        .route(
            "/shuffle/{job_id}/{stage_id}/{partition}",
            axum::routing::get(crate::shuffle_svc::read_partition),
        )
        .with_state(state);

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // 1. Request without token should return 401 Unauthorized
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req =
        "GET /shuffle/job1/stage1/0 HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut resp = String::new();
    stream.read_to_string(&mut resp).await.unwrap();
    assert!(
        resp.contains("HTTP/1.1 401 Unauthorized"),
        "expected 401, got: {}",
        resp
    );

    // 2. Request with invalid token should return 401 Unauthorized
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = "GET /shuffle/job1/stage1/0 HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer bad-token\r\nConnection: close\r\n\r\n";
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut resp = String::new();
    stream.read_to_string(&mut resp).await.unwrap();
    assert!(
        resp.contains("HTTP/1.1 401 Unauthorized"),
        "expected 401, got: {}",
        resp
    );
}
