//! CDC-to-lakehouse pipeline: Debezium 2.x over Kafka → Iceberg.

mod debezium;
mod pipeline;

#[cfg(feature = "kafka")]
mod kafka_source;

#[cfg(feature = "state")]
mod offset;

pub use debezium::{
    parse_debezium_envelope, parse_debezium_envelope_result, CdcEvent, CdcOp, DebeziumParseError,
    RawCdcRecord,
};
pub use pipeline::{
    build_batch_from_events, CdcBatchError, CdcEventSource, CdcSchemaRegistryFormat,
    CdcToLakehousePipeline, InMemoryCdcEventSource,
};

#[cfg(feature = "kafka")]
pub use kafka_source::{KafkaCdcConfig, RdkafkaCdcEventSource};

#[cfg(feature = "state")]
pub use offset::CdcOffsetTracker;


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdcop_from_debezium_parses_all_ops() {
        assert_eq!(CdcOp::from_debezium("c"), Some(CdcOp::Insert));
        assert_eq!(CdcOp::from_debezium("u"), Some(CdcOp::Update));
        assert_eq!(CdcOp::from_debezium("d"), Some(CdcOp::Delete));
        assert_eq!(CdcOp::from_debezium("r"), Some(CdcOp::SnapshotRead));
        assert_eq!(CdcOp::from_debezium("x"), None);
    }

    #[test]
    fn parse_insert_envelope() {
        let json = r#"{"op":"c","before":null,"after":{"id":1,"name":"alice"},"source":{"lsn":100,"ts_ms":1716201600000,"table":"orders"}}"#;
        let event = parse_debezium_envelope(json, 0, 0).unwrap();
        assert_eq!(event.op, CdcOp::Insert);
        assert!(event.before.is_none());
        let after = event.after.unwrap();
        let schema = after.schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            col_names.contains(&"id") || col_names.contains(&"name"),
            "after batch must have unpacked columns, got: {col_names:?}"
        );
        assert_eq!(event.source_lsn, Some(100));
        assert_eq!(event.table, "orders");
    }

    #[test]
    fn parse_delete_envelope() {
        let json = r#"{"op":"d","before":{"id":1,"name":"alice"},"after":null,"source":{"lsn":200,"ts_ms":1716201700000,"table":"orders"}}"#;
        let event = parse_debezium_envelope(json, 0, 1).unwrap();
        assert_eq!(event.op, CdcOp::Delete);
        assert!(event.before.is_some());
        assert!(event.after.is_none());
    }

    #[test]
    fn parse_malformed_envelope_returns_err() {
        assert!(parse_debezium_envelope("{}", 0, 0).is_err());
        assert!(parse_debezium_envelope("not json", 0, 0).is_err());
        assert!(parse_debezium_envelope(r#"{"op":"z"}"#, 0, 0).is_err());
    }

    #[test]
    fn strict_parser_reports_malformed_json_errors() {
        let err = parse_debezium_envelope_result("not json", 0, 0).unwrap_err();
        assert!(matches!(err, DebeziumParseError::InvalidJson(_)));
        let err = parse_debezium_envelope_result(r#"{"op":"z"}"#, 0, 0).unwrap_err();
        assert_eq!(err, DebeziumParseError::UnknownOp("z".into()));
    }

    #[test]
    fn pipeline_validate_rejects_empty_topic() {
        let p = CdcToLakehousePipeline::new(
            "",
            vec!["kafka:9092".into()],
            "cat",
            "tbl",
            vec!["id".into()],
        );
        assert!(p.validate().is_err());
    }

    #[test]
    fn pipeline_validate_accepts_valid_config() {
        let p = CdcToLakehousePipeline::new(
            "orders.cdc",
            vec!["kafka:9092".into()],
            "iceberg",
            "warehouse.orders",
            vec!["id".into()],
        );
        assert!(p.validate().is_ok());
    }

    #[test]
    fn registry_batch_concat_normalizes_compatible_schema_versions() {
        use arrow::array::{Array, Int32Array, Int64Array, StringArray};

        let first = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["second"])),
            ],
        )
        .unwrap();

        let merged = concat_registry_batches(&[first, second]).unwrap();

        assert_eq!(merged.num_rows(), 2);
        assert_eq!(merged.schema().field(0).data_type(), &DataType::Int64);
        assert!(merged.schema().field(1).is_nullable());
        assert!(merged.column(1).is_null(0));
    }

    #[test]
    fn registry_batch_concat_rejects_incompatible_type_drift() {
        use arrow::array::{Int64Array, StringArray};

        let first = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["1"]))],
        )
        .unwrap();

        let error = concat_registry_batches(&[first, second]).unwrap_err();

        assert!(error.contains("changed incompatibly"));
    }

    #[test]
    fn schema_evolution_state_rolls_back_after_incompatible_batch() {
        use arrow::array::{Int64Array, StringArray};

        let mut state = CdcSchemaEvolutionState::default();
        let initial = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        state.normalize(initial).unwrap();
        let incompatible = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["bad"]))],
        )
        .unwrap();

        assert!(state.normalize(incompatible).is_err());
        assert_eq!(state.schema.unwrap().field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn pipeline_validate_rejects_zero_batch_and_duplicate_primary_keys() {
        let zero_batch = CdcToLakehousePipeline::new(
            "orders.cdc",
            vec!["kafka:9092".into()],
            "iceberg",
            "warehouse.orders",
            vec!["id".into()],
        )
        .with_batch_size(0);
        let err = zero_batch.validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("batch_size must be greater than zero"),
            "unexpected error: {err}"
        );

        let duplicate_keys = CdcToLakehousePipeline::new(
            "orders.cdc",
            vec!["kafka:9092".into()],
            "iceberg",
            "warehouse.orders",
            vec!["id".into(), "id".into()],
        );
        let err = duplicate_keys.validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("primary_key_columns must not contain duplicates"),
            "unexpected error: {err}"
        );
    }

    #[cfg(not(feature = "schema-registry"))]
    #[test]
    fn pipeline_rejects_registry_config_when_capability_is_not_compiled() {
        let pipeline = CdcToLakehousePipeline::new(
            "orders.cdc",
            vec!["kafka:9092".into()],
            "iceberg",
            "warehouse.orders",
            vec!["id".into()],
        )
        .with_schema_registry("http://registry:8081");

        assert!(
            pipeline
                .validate()
                .unwrap_err()
                .to_string()
                .contains("schema-registry feature")
        );
    }

    #[cfg(feature = "schema-registry")]
    #[tokio::test]
    async fn registry_cdc_rejects_mixed_binary_and_plain_batches() {
        struct MixedSource {
            records: Option<Vec<RawCdcRecord>>,
        }

        impl CdcEventSource for MixedSource {
            fn poll_events(&mut self, _max: usize) -> Result<Vec<String>, ConnectorError> {
                Ok(Vec::new())
            }

            fn poll_records(&mut self, _max: usize) -> Result<Vec<RawCdcRecord>, ConnectorError> {
                Ok(self.records.take().unwrap_or_default())
            }
        }

        let pipeline = CdcToLakehousePipeline::new(
            "orders.cdc",
            vec!["kafka:9092".into()],
            "iceberg",
            "warehouse.orders",
            vec!["id".into()],
        )
        .with_schema_registry("http://registry:8081");
        let source = MixedSource {
            records: Some(vec![
                RawCdcRecord::with_bytes(br#"{"id":1}"#.to_vec(), 0, 1),
                RawCdcRecord::new(
                    r#"{"op":"c","source":{"table":"orders"},"after":{"id":2}}"#,
                    0,
                    2,
                ),
            ]),
        };
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let error = pipeline
            .run_with_source(source, |_| Ok(()), shutdown_rx)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("mixed batch"));
    }

    #[cfg(feature = "schema-registry")]
    #[tokio::test]
    async fn registry_cdc_requires_registry_for_binary_records() {
        struct BinarySource {
            records: Option<Vec<RawCdcRecord>>,
        }

        impl CdcEventSource for BinarySource {
            fn poll_events(&mut self, _max: usize) -> Result<Vec<String>, ConnectorError> {
                Ok(Vec::new())
            }

            fn poll_records(&mut self, _max: usize) -> Result<Vec<RawCdcRecord>, ConnectorError> {
                Ok(self.records.take().unwrap_or_default())
            }
        }

        let pipeline = CdcToLakehousePipeline::new(
            "orders.cdc",
            vec!["kafka:9092".into()],
            "iceberg",
            "warehouse.orders",
            vec!["id".into()],
        );
        let source = BinarySource {
            records: Some(vec![RawCdcRecord::with_bytes(
                br#"{"id":1}"#.to_vec(),
                0,
                1,
            )]),
        };
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let error = pipeline
            .run_with_source(source, |_| Ok(()), shutdown_rx)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("require schema_registry_url"));
    }

    #[test]
    fn build_batch_renames_reserved_payload_columns() {
        use arrow::array::StringArray;
        // Payload field named "_op" must become "_op_src"; metadata "_op" must still hold op type.
        let fields = vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("_op", DataType::Utf8, true),
        ];
        let schema = Arc::new(Schema::new(fields));
        let id_arr: StringArray = vec![Some("42")].into_iter().collect();
        let src_op_arr: StringArray = vec![Some("payload_op_value")].into_iter().collect();
        let after_batch =
            RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(src_op_arr)]).unwrap();
        let event = CdcEvent {
            op: CdcOp::Insert,
            before: None,
            after: Some(after_batch),
            source_lsn: Some(1),
            source_ts_ms: Some(1716201600000),
            partition_id: 0,
            offset: 0,
            table: "orders".to_string(),
        };
        let batch = build_batch_from_events(&[event]).unwrap();
        let schema = batch.schema();
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            col_names.contains(&"_op"),
            "metadata _op missing: {col_names:?}"
        );
        assert!(
            col_names.contains(&"_op_src"),
            "renamed _op_src missing: {col_names:?}"
        );
        // Metadata value is the operation type, not the payload value.
        let meta_idx = batch.schema().index_of("_op").unwrap();
        let meta_arr = batch
            .column(meta_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(meta_arr.value(0), "Insert");
        // Renamed source column preserves original payload value.
        let src_idx = batch.schema().index_of("_op_src").unwrap();
        let src_arr = batch
            .column(src_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(src_arr.value(0), "payload_op_value");
    }

    #[test]
    fn build_batch_stringifies_non_utf8_payload_columns() {
        use arrow::array::{BooleanArray, Int64Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let after_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(42_i64)])),
                Arc::new(BooleanArray::from(vec![Some(true)])),
            ],
        )
        .unwrap();
        let event = CdcEvent {
            op: CdcOp::Insert,
            before: None,
            after: Some(after_batch),
            source_lsn: Some(1),
            source_ts_ms: Some(1716201600000),
            partition_id: 0,
            offset: 0,
            table: "orders".to_string(),
        };

        let batch = build_batch_from_events(&[event]).unwrap();
        let id_idx = batch.schema().index_of("id").unwrap();
        let active_idx = batch.schema().index_of("active").unwrap();
        let id = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let active = batch
            .column(active_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id.value(0), "42");
        assert_eq!(active.value(0), "true");
    }

    #[tokio::test]
    async fn run_with_source_processes_events() {
        let pipeline = CdcToLakehousePipeline::new(
            "orders",
            vec!["broker:9092".to_string()],
            "my_catalog",
            "warehouse.orders",
            vec!["id".to_string()],
        );

        let json = r#"{"op":"c","source":{"lsn":1,"ts_ms":1716201600000,"partition":0,"offset":0,"table":"orders"},"after":{"id":1,"name":"alice"}}"#;
        let source = InMemoryCdcEventSource::new([json]);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut batches_received = Vec::new();

        pipeline
            .run_with_source(
                source,
                |batch| {
                    batches_received.push(batch);
                    Ok(())
                },
                shutdown_rx,
            )
            .await
            .expect("pipeline run failed");

        drop(shutdown_tx);
        assert_eq!(batches_received.len(), 1, "expected one batch");
        let schema = batches_received[0].schema();
        assert!(schema.index_of("_op").is_ok(), "expected _op column");
    }

    #[tokio::test]
    async fn run_with_source_commits_offsets_after_successful_sink() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CommitCountingSource {
            events: std::collections::VecDeque<String>,
            commits: Arc<AtomicUsize>,
        }

        impl CdcEventSource for CommitCountingSource {
            fn poll_events(&mut self, max: usize) -> Result<Vec<String>, ConnectorError> {
                let n = max.min(self.events.len());
                Ok(self.events.drain(..n).collect())
            }

            fn commit_offsets(&mut self) -> Result<(), ConnectorError> {
                self.commits.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let pipeline = CdcToLakehousePipeline::new(
            "orders",
            vec!["broker:9092".to_string()],
            "my_catalog",
            "warehouse.orders",
            vec!["id".to_string()],
        );
        let commits = Arc::new(AtomicUsize::new(0));
        let source = CommitCountingSource {
            events: [
                r#"{"op":"c","source":{"lsn":1,"ts_ms":1,"table":"orders"},"after":{"id":"1"}}"#
                    .to_string(),
            ]
            .into_iter()
            .collect(),
            commits: Arc::clone(&commits),
        };
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        pipeline
            .run_with_source(source, |_| Ok(()), shutdown_rx)
            .await
            .unwrap();

        assert_eq!(commits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn run_with_source_does_not_commit_offsets_when_sink_fails() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CommitCountingSource {
            events: std::collections::VecDeque<String>,
            commits: Arc<AtomicUsize>,
        }

        impl CdcEventSource for CommitCountingSource {
            fn poll_events(&mut self, max: usize) -> Result<Vec<String>, ConnectorError> {
                let n = max.min(self.events.len());
                Ok(self.events.drain(..n).collect())
            }

            fn commit_offsets(&mut self) -> Result<(), ConnectorError> {
                self.commits.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let pipeline = CdcToLakehousePipeline::new(
            "orders",
            vec!["broker:9092".to_string()],
            "my_catalog",
            "warehouse.orders",
            vec!["id".to_string()],
        );
        let commits = Arc::new(AtomicUsize::new(0));
        let source = CommitCountingSource {
            events: [
                r#"{"op":"c","source":{"lsn":1,"ts_ms":1,"table":"orders"},"after":{"id":"1"}}"#
                    .to_string(),
            ]
            .into_iter()
            .collect(),
            commits: Arc::clone(&commits),
        };
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let result = pipeline
            .run_with_source(
                source,
                |_| Err(ConnectorError::Cdc("sink failed".into())),
                shutdown_rx,
            )
            .await;

        assert!(
            result.unwrap_err().to_string().contains("sink failed"),
            "unexpected error"
        );
        assert_eq!(commits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn run_with_source_errors_on_malformed_json() {
        let pipeline = CdcToLakehousePipeline::new(
            "orders",
            vec!["broker:9092".to_string()],
            "my_catalog",
            "warehouse.orders",
            vec!["id".to_string()],
        );
        let source = InMemoryCdcEventSource::new(["not json"]);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let result = pipeline
            .run_with_source(source, |_| Ok(()), shutdown_rx)
            .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Debezium parse error")
        );
    }

    #[tokio::test]
    async fn run_with_source_normalizes_schema_evolution_across_batches() {
        let pipeline = CdcToLakehousePipeline::new(
            "orders",
            vec!["broker:9092".to_string()],
            "my_catalog",
            "warehouse.orders",
            vec!["id".to_string()],
        )
        .with_batch_size(1);
        let first = r#"{"op":"c","source":{"lsn":1,"ts_ms":1,"table":"orders"},"after":{"id":1}}"#;
        let second = r#"{"op":"c","source":{"lsn":2,"ts_ms":2,"table":"orders"},"after":{"id":2,"name":"bob"}}"#;
        let source = InMemoryCdcEventSource::new([first, second]);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut schemas = Vec::new();

        pipeline
            .run_with_source(
                source,
                |batch| {
                    schemas.push(
                        batch
                            .schema()
                            .fields()
                            .iter()
                            .map(|field| field.name().to_string())
                            .collect::<Vec<_>>(),
                    );
                    Ok(())
                },
                shutdown_rx,
            )
            .await
            .unwrap();

        assert!(schemas[1].contains(&"name".to_string()));
    }

    #[tokio::test]
    async fn run_with_iceberg_sink_commits_snapshot_then_offsets() {
        use crate::lakehouse::{
            IcebergTableRef, MemoryIcebergTwoPhaseCommit, MemoryLakehouseTable, SchemaField,
            SchemaVersion,
        };

        #[derive(Default)]
        struct CommitTrackingSource {
            events: std::collections::VecDeque<String>,
            commits: usize,
        }

        impl CdcEventSource for CommitTrackingSource {
            fn poll_events(&mut self, max: usize) -> Result<Vec<String>, ConnectorError> {
                let n = max.min(self.events.len());
                Ok(self.events.drain(..n).collect())
            }

            fn commit_offsets(&mut self) -> Result<(), ConnectorError> {
                self.commits += 1;
                Ok(())
            }
        }

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
            IcebergTableRef::new("cat", "ns", "orders"),
            schema,
        ));
        let iceberg = MemoryIcebergTwoPhaseCommit::new(table);
        let pipeline = CdcToLakehousePipeline::new(
            "orders",
            vec!["broker:9092".to_string()],
            "my_catalog",
            "warehouse.orders",
            vec!["id".to_string()],
        );
        let source = CommitTrackingSource {
            events: [
                r#"{"op":"c","source":{"lsn":1,"ts_ms":1,"table":"orders"},"after":{"id":"1"}}"#
                    .to_string(),
            ]
            .into_iter()
            .collect(),
            commits: 0,
        };
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let snapshots = pipeline
            .run_with_iceberg_sink(source, &iceberg, shutdown_rx)
            .await
            .unwrap();

        assert_eq!(snapshots.len(), 1);
        let offsets = iceberg.committed_kafka_offsets().await;
        assert_eq!(offsets.get("orders-0"), Some(&1));
    }

    #[tokio::test]
    async fn run_with_iceberg_sink_preserves_source_offsets() {
        use crate::lakehouse::{
            IcebergTableRef, MemoryIcebergTwoPhaseCommit, MemoryLakehouseTable, SchemaField,
            SchemaVersion,
        };

        struct MetadataSource {
            records: std::collections::VecDeque<RawCdcRecord>,
        }

        impl CdcEventSource for MetadataSource {
            fn poll_events(&mut self, max: usize) -> Result<Vec<String>, ConnectorError> {
                Ok(self
                    .poll_records(max)?
                    .into_iter()
                    .map(|record| record.payload)
                    .collect())
            }

            fn poll_records(&mut self, max: usize) -> Result<Vec<RawCdcRecord>, ConnectorError> {
                let n = max.min(self.records.len());
                Ok(self.records.drain(..n).collect())
            }
        }

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
            IcebergTableRef::new("cat", "ns", "orders"),
            schema,
        ));
        let iceberg = MemoryIcebergTwoPhaseCommit::new(table);
        let pipeline = CdcToLakehousePipeline::new(
            "orders",
            vec!["broker:9092".to_string()],
            "my_catalog",
            "warehouse.orders",
            vec!["id".to_string()],
        );
        let source = MetadataSource {
            records: [RawCdcRecord::new(
                r#"{"op":"c","source":{"lsn":1,"ts_ms":1,"table":"orders"},"after":{"id":"1"}}"#,
                7,
                41,
            )]
            .into_iter()
            .collect(),
        };
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let snapshots = pipeline
            .run_with_iceberg_sink(source, &iceberg, shutdown_rx)
            .await
            .unwrap();

        assert_eq!(snapshots.len(), 1);
        let offsets = iceberg.committed_kafka_offsets().await;
        assert_eq!(offsets.get("orders-7"), Some(&42));
    }

    #[tokio::test]
    async fn run_with_source_shutdown_stops_loop() {
        struct InfiniteSource;
        impl CdcEventSource for InfiniteSource {
            fn poll_events(&mut self, _max: usize) -> Result<Vec<String>, ConnectorError> {
                Ok(vec![])
            }
        }

        let pipeline = CdcToLakehousePipeline::new(
            "orders",
            vec!["broker:9092".to_string()],
            "my_catalog",
            "warehouse.orders",
            vec!["id".to_string()],
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        drop(shutdown_tx);

        let result = pipeline
            .run_with_source(InfiniteSource, |_| Ok(()), shutdown_rx)
            .await;
        assert!(result.is_ok(), "shutdown via empty source should succeed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_returns_err_without_source() {
        let pipeline = CdcToLakehousePipeline::new(
            "orders",
            vec!["broker:9092".to_string()],
            "my_catalog",
            "warehouse.orders",
            vec!["id".to_string()],
        );
        let result = pipeline.run().await;
        let err = result.expect_err("run() without durable sink must return Err");
        assert!(
            err.to_string()
                .contains("cannot prove downstream durability")
        );
    }
}
