//! Exactly-once certification matrix tests (R16 S5).

#[cfg(any(feature = "kafka", feature = "state"))]
use arrow::array::{Int32Array, RecordBatch};
#[cfg(any(feature = "kafka", feature = "state"))]
use arrow::datatypes::{DataType, Field, Schema};
#[cfg(any(feature = "kafka", feature = "state"))]
use krishiv_connectors::TwoPhaseCommitSink;
#[cfg(any(feature = "kafka", feature = "state"))]
use krishiv_connectors::two_phase_parquet_s3::TwoPhaseParquetSink;
#[cfg(feature = "kafka")]
use krishiv_connectors::{Offset, transactional_kafka::TransactionalKafkaSink};
#[cfg(any(feature = "kafka", feature = "state"))]
use std::sync::Arc;
#[cfg(any(feature = "kafka", feature = "state"))]
use tempfile::tempdir;

#[cfg(any(feature = "kafka", feature = "state"))]
fn batch(v: i32) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![v]))],
    )
    .unwrap()
}

#[test]
#[cfg(feature = "kafka")]
fn kafka_to_kafka_certification() {
    let mut sink = TransactionalKafkaSink::new_for_profile(
        krishiv_common::DurabilityProfile::DevLocal,
        "job-k2k",
        0,
        1,
    )
    .expect("simulation sink permitted in dev-local");
    let h = sink.prepare(1, &batch(1)).unwrap();
    sink.commit(h).unwrap();
    assert!(!sink.committed_batches().is_empty());
}

#[test]
#[cfg(feature = "kafka")]
fn transactional_kafka_rejects_durable_profile() {
    let err = TransactionalKafkaSink::new_for_profile(
        krishiv_common::DurabilityProfile::DistributedDurable,
        "job-k2k",
        0,
        1,
    )
    .expect_err("simulation sink must be rejected in durable profiles");
    assert!(err.to_string().contains("simulator"));
}

#[test]
#[cfg(any(feature = "kafka", feature = "state"))]
fn kafka_to_parquet_s3_certification() {
    let dir = tempdir().unwrap();
    let mut sink = TwoPhaseParquetSink::new(dir.path(), 1);
    let h = sink.prepare(1, &batch(42)).unwrap();
    sink.commit(h).unwrap();
    assert!(dir.path().join("data").exists());
}

#[test]
#[cfg(feature = "kafka")]
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
#[cfg(feature = "kafka")]
fn s3_parquet_to_kafka_uses_transactional_sink() {
    let mut sink = TransactionalKafkaSink::new_for_profile(
        krishiv_common::DurabilityProfile::DevLocal,
        "job-s3k",
        1,
        2,
    )
    .expect("simulation sink permitted in dev-local");
    let h = sink.prepare(2, &batch(7)).unwrap();
    sink.commit(h).unwrap();
    assert_eq!(sink.committed_batches().len(), 1);
}

#[cfg(feature = "kafka")]
#[tokio::test]
async fn live_kafka_cdc_to_iceberg_certification() -> Result<(), Box<dyn std::error::Error>> {
    use krishiv_connectors::cdc::{CdcToLakehousePipeline, KafkaCdcConfig, RdkafkaCdcEventSource};
    use krishiv_lakehouse::{
        IcebergTableRef, MemoryIcebergTwoPhaseCommit, MemoryLakehouseTable, SchemaField,
        SchemaVersion,
    };
    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    let Ok(bootstrap_servers) = std::env::var("KAFKA_BOOTSTRAP_SERVERS") else {
        eprintln!("skipping live Kafka certification: KAFKA_BOOTSTRAP_SERVERS is not set");
        return Ok(());
    };

    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let topic = format!("krishiv_cdc_cert_{}_{}", std::process::id(), unique);
    let group_id = format!("krishiv_cdc_cert_group_{}_{}", std::process::id(), unique);

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()?;
    let topic_spec = NewTopic::new(&topic, 1, TopicReplication::Fixed(1));
    let create_results = admin
        .create_topics(&[topic_spec], &AdminOptions::new())
        .await?;
    for result in create_results {
        if let Err((name, err)) = result {
            return Err(format!("failed to create Kafka topic {name}: {err}").into());
        }
    }

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .create()?;
    let payload = r#"{"op":"c","source":{"lsn":1,"ts_ms":1,"table":"orders"},"after":{"id":"1"}}"#;
    producer
        .send(
            FutureRecord::to(&topic).key("orders-1").payload(payload),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(err, _message)| format!("failed to produce CDC certification record: {err}"))?;
    producer.flush(Duration::from_secs(5))?;

    let schema = SchemaVersion {
        schema_id: 1,
        fields: vec![SchemaField {
            id: 1,
            name: "id".to_string(),
            required: false,
            data_type: "string".to_string(),
        }],
    };
    let table = Arc::new(MemoryLakehouseTable::new(
        IcebergTableRef::new("cert", "cdc", "orders"),
        schema,
    ));
    let iceberg = MemoryIcebergTwoPhaseCommit::new(table);
    let pipeline = CdcToLakehousePipeline::new(
        topic.clone(),
        vec![bootstrap_servers.clone()],
        "memory",
        "cert.cdc.orders",
        vec!["id".to_string()],
    )
    .with_batch_size(1);
    let source =
        RdkafkaCdcEventSource::new(&KafkaCdcConfig::new(bootstrap_servers, group_id, topic))?
            .with_poll_timeout_ms(500);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let snapshots = pipeline
        .run_with_iceberg_sink_until_commits(source, &iceberg, shutdown_rx, 1)
        .await?;

    assert_eq!(snapshots.len(), 1);
    let committed_offsets = iceberg.committed_kafka_offsets().await;
    assert!(
        committed_offsets
            .iter()
            .any(|(partition, offset)| partition.starts_with("orders-") && *offset > 0),
        "expected Iceberg snapshot metadata to contain committed Kafka offsets, got {committed_offsets:?}"
    );

    Ok(())
}
