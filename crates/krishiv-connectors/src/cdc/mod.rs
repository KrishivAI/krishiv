//! CDC-to-lakehouse pipeline: Debezium 2.x over Kafka → Iceberg.

mod debezium;
mod pipeline;

#[cfg(feature = "kafka")]
mod kafka_source;

#[cfg(feature = "state")]
mod offset;

pub use debezium::{
    CdcEvent, CdcOp, DebeziumParseError, RawCdcRecord, parse_debezium_envelope,
    parse_debezium_envelope_result,
};
pub use pipeline::{
    CdcBatchError, CdcEventSource, CdcSchemaRegistryFormat, CdcToLakehousePipeline,
    InMemoryCdcEventSource, build_batch_from_events,
};

#[cfg(feature = "kafka")]
pub use kafka_source::{KafkaCdcConfig, RdkafkaCdcEventSource};

#[cfg(feature = "state")]
pub use offset::CdcOffsetTracker;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::pipeline::{CdcSchemaEvolutionState, concat_registry_batches};
    use super::*;
    use crate::ConnectorError;

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
        // A live source that never runs dry: the loop can only exit via the
        // shutdown channel, not via the empty-poll exhaustion path.
        struct InfiniteSource;
        impl CdcEventSource for InfiniteSource {
            fn poll_events(&mut self, _max: usize) -> Result<Vec<String>, ConnectorError> {
                Ok(vec![
                    r#"{"op":"c","source":{"lsn":1,"ts_ms":1,"table":"orders"},"after":{"id":"1"}}"#
                        .to_string(),
                ])
            }

            fn is_live(&self) -> bool {
                true
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
        let handle = tokio::spawn(async move {
            pipeline
                .run_with_source(InfiniteSource, |_| Ok(()), shutdown_rx)
                .await
        });
        shutdown_tx
            .send(true)
            .expect("pipeline task holds receiver");

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("shutdown signal must stop the loop")
            .expect("pipeline task must not panic");
        assert!(result.is_ok(), "shutdown must exit the loop cleanly");
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

    /// In-memory source with real offset identity that honors `resume_from`
    /// by dropping records below the committed next-offset for its partition.
    struct ResumableRecordSource {
        records: std::collections::VecDeque<RawCdcRecord>,
        table: String,
    }

    impl ResumableRecordSource {
        fn orders(offsets: std::ops::Range<i64>) -> Self {
            Self {
                records: offsets
                    .map(|offset| {
                        RawCdcRecord::new(
                            format!(
                                r#"{{"op":"c","source":{{"lsn":{offset},"ts_ms":1,"table":"orders"}},"after":{{"id":"{offset}"}}}}"#
                            ),
                            0,
                            offset,
                        )
                    })
                    .collect(),
                table: "orders".to_string(),
            }
        }
    }

    impl CdcEventSource for ResumableRecordSource {
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

        fn resume_from(
            &mut self,
            offsets: &std::collections::BTreeMap<String, i64>,
        ) -> Result<(), ConnectorError> {
            let table = self.table.clone();
            self.records.retain(|record| {
                offsets
                    .get(&format!("{}-{}", table, record.partition_id))
                    .is_none_or(|next| record.offset >= *next)
            });
            Ok(())
        }
    }

    async fn orders_lakehouse() -> (
        Arc<crate::lakehouse::MemoryLakehouseTable>,
        crate::lakehouse::MemoryIcebergTwoPhaseCommit,
    ) {
        use crate::lakehouse::{
            IcebergTableRef, MemoryIcebergTwoPhaseCommit, MemoryLakehouseTable, SchemaField,
            SchemaVersion,
        };
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
        let tpc = MemoryIcebergTwoPhaseCommit::new(table.clone());
        (table, tpc)
    }

    async fn table_row_count(table: &crate::lakehouse::MemoryLakehouseTable) -> usize {
        use crate::lakehouse::{IcebergScanOptions, LakehouseTable};
        table
            .scan(&IcebergScanOptions::default())
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum()
    }

    fn orders_pipeline() -> CdcToLakehousePipeline {
        CdcToLakehousePipeline::new(
            "orders",
            vec!["broker:9092".to_string()],
            "my_catalog",
            "warehouse.orders",
            vec!["id".to_string()],
        )
    }

    /// Crash-window regression: a snapshot commits but the process dies before
    /// the source offset commit. On restart the source is positioned earlier,
    /// yet the rows already covered by the snapshot's committed offsets must
    /// not be appended a second time.
    #[tokio::test]
    async fn run_with_iceberg_sink_restart_does_not_duplicate_committed_rows() {
        let (table, tpc) = orders_lakehouse().await;
        let pipeline = orders_pipeline();

        let (_tx1, rx1) = tokio::sync::watch::channel(false);
        let snapshots = pipeline
            .run_with_iceberg_sink(ResumableRecordSource::orders(0..3), &tpc, rx1)
            .await
            .unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(table_row_count(&table).await, 3);

        // "Crash" before the source offset commit landed: restart the pipeline
        // with a fresh source positioned back at offset 0.
        let (_tx2, rx2) = tokio::sync::watch::channel(false);
        pipeline
            .run_with_iceberg_sink(ResumableRecordSource::orders(0..3), &tpc, rx2)
            .await
            .unwrap();

        assert_eq!(
            table_row_count(&table).await,
            3,
            "restart must skip rows covered by committed offsets, not re-append them"
        );
    }

    /// C6 regression: a partition whose only consumed records since the last
    /// commit are tombstones must still have its offset merged into the
    /// snapshot's committed offset summary.
    #[tokio::test]
    async fn run_with_iceberg_sink_commits_tombstone_tail_offsets() {
        struct TombstoneTailSource {
            records: std::collections::VecDeque<RawCdcRecord>,
        }

        impl CdcEventSource for TombstoneTailSource {
            fn poll_events(&mut self, _max: usize) -> Result<Vec<String>, ConnectorError> {
                Ok(Vec::new())
            }

            fn poll_records(&mut self, max: usize) -> Result<Vec<RawCdcRecord>, ConnectorError> {
                let n = max.min(self.records.len());
                Ok(self.records.drain(..n).collect())
            }

            fn pending_tombstone_offsets(&self) -> std::collections::BTreeMap<u32, i64> {
                [(1_u32, 5_i64)].into_iter().collect()
            }
        }

        let (_table, tpc) = orders_lakehouse().await;
        let pipeline = orders_pipeline();
        let source = TombstoneTailSource {
            records: [RawCdcRecord::new(
                r#"{"op":"c","source":{"lsn":1,"ts_ms":1,"table":"orders"},"after":{"id":"1"}}"#,
                0,
                0,
            )]
            .into_iter()
            .collect(),
        };
        let (_tx, rx) = tokio::sync::watch::channel(false);

        pipeline
            .run_with_iceberg_sink(source, &tpc, rx)
            .await
            .unwrap();

        let offsets = tpc.committed_kafka_offsets().await;
        assert_eq!(offsets.get("orders-0"), Some(&1), "event partition offset");
        assert_eq!(
            offsets.get("orders-1"),
            Some(&5),
            "tombstone-tail partition offset must be merged into the summary"
        );
    }

    /// The schema-registry decode path drops CDC envelope semantics, so a CDC
    /// pipeline must fail closed on binary records unless the caller opted in
    /// to append-only ingestion.
    #[cfg(feature = "schema-registry")]
    #[tokio::test]
    async fn registry_cdc_fails_closed_for_binary_records_without_append_only_optin() {
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
        )
        .with_schema_registry("http://registry:8081");
        let source = BinarySource {
            records: Some(vec![RawCdcRecord::with_bytes(
                br#"{"id":1}"#.to_vec(),
                0,
                1,
            )]),
        };
        let (_tx, rx) = tokio::sync::watch::channel(false);

        let error = pipeline
            .run_with_source(source, |_| Ok(()), rx)
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("append-only"),
            "expected the append-only fail-closed error, got: {error}"
        );
    }

    #[cfg(feature = "state")]
    mod state_offset_tests {
        use std::sync::{Arc, Mutex};

        use krishiv_state::{InMemoryStateBackend, Namespace, StateBackend, StateResult};

        use super::super::CdcOffsetTracker;
        use super::{ResumableRecordSource, orders_lakehouse, orders_pipeline, table_row_count};

        /// Test double sharing one in-memory backend across "restarts".
        #[derive(Clone)]
        struct SharedBackend(Arc<Mutex<InMemoryStateBackend>>);

        impl SharedBackend {
            fn lock(&self) -> std::sync::MutexGuard<'_, InMemoryStateBackend> {
                self.0
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
            }
        }

        impl StateBackend for SharedBackend {
            fn get(&self, ns: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
                self.lock().get(ns, key)
            }

            fn put(&mut self, ns: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()> {
                self.lock().put(ns, key, value)
            }

            fn delete(&mut self, ns: &Namespace, key: &[u8]) -> StateResult<()> {
                self.lock().delete(ns, key)
            }

            fn clear_namespace(&mut self, ns: &Namespace) -> StateResult<()> {
                self.lock().clear_namespace(ns)
            }

            fn list_namespaces(&self) -> StateResult<Vec<Namespace>> {
                self.lock().list_namespaces()
            }

            fn list_keys(&self, ns: &Namespace) -> StateResult<Vec<Vec<u8>>> {
                self.lock().list_keys(ns)
            }

            fn snapshot(&self) -> StateResult<Vec<u8>> {
                self.lock().snapshot()
            }

            fn load_snapshot(&mut self, bytes: &[u8]) -> StateResult<()> {
                self.lock().load_snapshot(bytes)
            }
        }

        /// The `state` feature's offset persistence must survive a restart:
        /// offsets committed by run 1 are read back by run 2 and used to seek
        /// the source past already-processed records.
        /// A backend that cannot be read must not become an empty offset map
        /// (which the pipeline reads as "start from the beginning").
        #[test]
        fn offset_tracker_refuses_an_unreadable_backend() {
            struct Unreadable;
            impl StateBackend for Unreadable {
                fn get(&self, _: &Namespace, _: &[u8]) -> StateResult<Option<Vec<u8>>> {
                    Err(krishiv_state::StateError::BackendUnavailable {
                        message: "down".into(),
                        source: None,
                    })
                }
                fn put(&mut self, _: &Namespace, _: Vec<u8>, _: Vec<u8>) -> StateResult<()> {
                    Ok(())
                }
                fn delete(&mut self, _: &Namespace, _: &[u8]) -> StateResult<()> {
                    Ok(())
                }
                fn clear_namespace(&mut self, _: &Namespace) -> StateResult<()> {
                    Ok(())
                }
                fn list_namespaces(&self) -> StateResult<Vec<Namespace>> {
                    Ok(Vec::new())
                }
                fn list_keys(&self, _: &Namespace) -> StateResult<Vec<Vec<u8>>> {
                    Err(krishiv_state::StateError::BackendUnavailable {
                        message: "down".into(),
                        source: None,
                    })
                }
                fn snapshot(&self) -> StateResult<Vec<u8>> {
                    Ok(Vec::new())
                }
                fn load_snapshot(&mut self, _: &[u8]) -> StateResult<()> {
                    Ok(())
                }
            }
            let err = CdcOffsetTracker::new(Box::new(Unreadable))
                .err()
                .expect("an unreadable backend is an error, not an empty map");
            assert!(err.to_string().contains("listing keys failed"), "{err}");
        }

        #[tokio::test]
        async fn offset_tracker_restart_resumes_from_persisted_offsets() {
            let backend = SharedBackend(Arc::new(Mutex::new(InMemoryStateBackend::default())));
            let pipeline = orders_pipeline();

            // Run 1: process three records, persisting offsets to the tracker.
            let (table1, tpc1) = orders_lakehouse().await;
            let mut tracker = CdcOffsetTracker::new(Box::new(backend.clone())).unwrap();
            let (_tx1, rx1) = tokio::sync::watch::channel(false);
            pipeline
                .run_with_iceberg_sink_and_offset_tracker(
                    ResumableRecordSource::orders(0..3),
                    &tpc1,
                    rx1,
                    &mut tracker,
                )
                .await
                .unwrap();
            assert_eq!(table_row_count(&table1).await, 3);
            assert_eq!(tracker.get_offset(0), Some(3));

            // Run 2 ("restart"): a fresh tracker over the same backend and a
            // fresh sink — resume must come from the persisted offsets alone.
            let (table2, tpc2) = orders_lakehouse().await;
            let mut tracker = CdcOffsetTracker::new(Box::new(backend)).unwrap();
            assert_eq!(
                tracker.get_offset(0),
                Some(3),
                "tracker must reload persisted offsets on startup"
            );
            let (_tx2, rx2) = tokio::sync::watch::channel(false);
            let snapshots = pipeline
                .run_with_iceberg_sink_and_offset_tracker(
                    ResumableRecordSource::orders(0..3),
                    &tpc2,
                    rx2,
                    &mut tracker,
                )
                .await
                .unwrap();

            assert!(
                snapshots.is_empty(),
                "already-committed records must be skipped after restart"
            );
            assert_eq!(table_row_count(&table2).await, 0);
        }
    }
}
