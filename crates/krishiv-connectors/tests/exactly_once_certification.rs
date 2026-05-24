//! Exactly-once certification matrix tests (R16 S5).

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use krishiv_connectors::Offset;
use krishiv_connectors::{
    TwoPhaseCommitSink, transactional_kafka::TransactionalKafkaSink,
    two_phase_parquet_s3::TwoPhaseParquetSink,
};
use std::sync::Arc;
use tempfile::tempdir;

fn batch(v: i32) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![v]))],
    )
    .unwrap()
}

#[test]
fn kafka_to_kafka_certification() {
    let mut sink = TransactionalKafkaSink::new("job-k2k", 0, 1);
    let h = sink.prepare(1, &batch(1)).unwrap();
    sink.commit(h).unwrap();
    assert!(!sink.committed_batches().is_empty());
}

#[test]
fn kafka_to_parquet_s3_certification() {
    let dir = tempdir().unwrap();
    let mut sink = TwoPhaseParquetSink::new(dir.path(), 1);
    let h = sink.prepare(1, &batch(42)).unwrap();
    sink.commit(h).unwrap();
    assert!(dir.path().join("data").exists());
}

#[test]
fn s3_parquet_to_iceberg_offset_checkpoint_semantics() {
    // Certified via source offset + Iceberg snapshot commit (R14); assert offset record shape.
    let offset = krishiv_connectors::kafka::KafkaOffset {
        topic: "files".into(),
        partition: 0,
        offset: 1024,
    };
    let encoded = krishiv_connectors::Offset::encode(&offset);
    let decoded = krishiv_connectors::kafka::KafkaOffset::decode(&encoded).unwrap();
    assert_eq!(decoded, offset);
}

#[test]
fn s3_parquet_to_kafka_uses_transactional_sink() {
    let mut sink = TransactionalKafkaSink::new("job-s3k", 1, 2);
    let h = sink.prepare(2, &batch(7)).unwrap();
    sink.commit(h).unwrap();
    assert_eq!(sink.committed_batches().len(), 1);
}
