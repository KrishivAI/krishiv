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

