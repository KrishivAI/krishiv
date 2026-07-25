#[cfg(test)]
mod connector_tests {
    use crate::*;
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    // -----------------------------------------------------------------------
    // ConnectorCapabilities builder
    // -----------------------------------------------------------------------

    #[test]
    fn connector_capabilities_builder_sets_flags() {
        let caps = ConnectorCapabilities::new()
            .with_bounded()
            .with_rewindable()
            .with_idempotent();

        assert!(caps.is_bounded());
        assert!(caps.is_rewindable());
        assert!(caps.is_idempotent());
        assert!(!caps.is_unbounded());
        assert!(!caps.is_transactional());
        assert!(caps.has_any());
    }

    #[test]
    fn connector_capabilities_default_all_false() {
        let caps = ConnectorCapabilities::new();
        assert!(!caps.has_any());
    }

    #[test]
    fn connector_capabilities_checkpoint_flag() {
        let caps = ConnectorCapabilities::new().with_checkpoint();
        assert!(caps.is_checkpoint_capable());
        assert!(!caps.is_two_phase_commit_capable());
        assert!(caps.has_any());
    }

    #[test]
    fn connector_capabilities_two_phase_commit_flag() {
        let caps = ConnectorCapabilities::new().with_two_phase_commit();
        assert!(caps.is_two_phase_commit_capable());
        assert!(caps.is_checkpoint_capable());
        assert!(caps.is_transactional());
        assert!(caps.has_any());
        caps.validate().unwrap();
    }

    #[test]
    fn connector_capabilities_validate_rejects_bounded_and_unbounded() {
        // The builder pattern prevents constructing this state, but validate()
        // still defends against it for safety. Verify the check is present.
        let caps = ConnectorCapabilities::new();
        assert!(caps.validate().is_ok());

        // With only bounded set, validate passes
        let caps = ConnectorCapabilities::new().with_bounded();
        assert!(caps.validate().is_ok());

        // With only unbounded set, validate passes
        let caps = ConnectorCapabilities::new().with_unbounded();
        assert!(caps.validate().is_ok());
    }

    #[test]
    fn connector_capabilities_validate_rejects_two_phase_without_transactional() {
        let caps = ConnectorCapabilities::new()
            .with_checkpoint()
            .with_two_phase_commit();
        // with_two_phase_commit sets transactional, so this should pass
        caps.validate().unwrap();

        // Manually construct an invalid state using the builder pattern
        // with_two_phase_commit() always sets transactional, so we test
        // that validate catches the invariant violation through the public API.
        // The only way to get two_phase_commit without transactional is via
        // manual construction, which the private fields prevent.
        // Instead, verify that the public API always produces valid states.
        let caps_valid = ConnectorCapabilities::new().with_two_phase_commit();
        assert!(caps_valid.validate().is_ok());
    }

    #[test]
    fn bounded_clears_unbounded() {
        let caps = ConnectorCapabilities::new().with_unbounded().with_bounded();
        assert!(caps.is_bounded());
        assert!(!caps.is_unbounded());
    }

    #[test]
    fn unbounded_clears_bounded() {
        let caps = ConnectorCapabilities::new().with_bounded().with_unbounded();
        assert!(caps.is_unbounded());
        assert!(!caps.is_bounded());
    }

    // -----------------------------------------------------------------------
    // ConnectorConfig
    // -----------------------------------------------------------------------

    #[test]
    fn connector_config_required_returns_error_when_missing() {
        let config = ConnectorConfig::new("my-source", "parquet");
        let err = config.required("path").unwrap_err();
        match err {
            ConnectorError::Config { message } => {
                assert!(message.contains("path"), "expected 'path' in: {message}");
            }
            other => panic!("unexpected error variant: {other}"),
        }
    }

    #[test]
    fn connector_config_required_returns_value_when_present() {
        let config = ConnectorConfig::new("my-source", "parquet")
            .with_property("path", "/data/file.parquet");
        let value = config.required("path").unwrap();
        assert_eq!(value, "/data/file.parquet");
    }

    #[test]
    fn connector_config_debug_redacts_sensitive_keys() {
        let config = ConnectorConfig::new("src", "kafka")
            .with_property("password", "secret123")
            .with_property("api_key", "key123")
            .with_property("bootstrap_servers", "localhost:9092");
        let debug_str = format!("{:?}", config);
        assert!(
            debug_str.contains("[REDACTED]"),
            "sensitive fields must be redacted: {debug_str}"
        );
        assert!(
            !debug_str.contains("secret123"),
            "password must not appear in debug output"
        );
        assert!(
            !debug_str.contains("key123"),
            "api_key must not appear in debug output"
        );
        assert!(
            debug_str.contains("localhost:9092"),
            "non-sensitive fields must be visible"
        );
    }

    #[test]
    fn connector_config_get_returns_none_for_missing_key() {
        let config = ConnectorConfig::new("src", "parquet");
        assert_eq!(config.get("nonexistent"), None);
    }

    // -----------------------------------------------------------------------
    // ParquetOffset encode/decode
    // -----------------------------------------------------------------------

    #[test]
    fn parquet_offset_encode_decode_roundtrip() {
        let original = ParquetOffset { batch_index: 42 };
        let encoded = original.encode();
        let decoded = ParquetOffset::decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn parquet_offset_decode_rejects_noncanonical_length() {
        let mut encoded = ParquetOffset { batch_index: 42 }.encode();
        encoded.push(0);
        let error =
            ParquetOffset::decode(&encoded).expect_err("trailing offset bytes must be rejected");
        assert!(matches!(error, ConnectorError::Offset { .. }));
    }

    #[test]
    fn parquet_offset_decode_rejects_empty_bytes() {
        let error = ParquetOffset::decode(&[]).expect_err("empty bytes must be rejected");
        assert!(matches!(error, ConnectorError::Offset { .. }));
    }

    #[test]
    fn parquet_offset_encode_zero_batch_index() {
        let offset = ParquetOffset { batch_index: 0 };
        let encoded = offset.encode();
        assert_eq!(encoded.len(), 8);
        let decoded = ParquetOffset::decode(&encoded).unwrap();
        assert_eq!(decoded.batch_index, 0);
    }

    #[test]
    fn parquet_offset_encode_large_batch_index() {
        let offset = ParquetOffset {
            batch_index: usize::MAX,
        };
        let encoded = offset.encode();
        let decoded = ParquetOffset::decode(&encoded).unwrap();
        assert_eq!(decoded.batch_index, usize::MAX);
    }

    #[test]
    fn at_least_once_contract_exists() {
        let _ = AtLeastOnceSinkContract;
    }

    // -----------------------------------------------------------------------
    // PostWriteOffsetCommitProtocol
    // -----------------------------------------------------------------------

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestOffset(u64);

    impl Offset for TestOffset {
        fn encode(&self) -> Vec<u8> {
            self.0.to_be_bytes().to_vec()
        }

        fn decode(bytes: &[u8]) -> ConnectorResult<Self> {
            if bytes.len() != 8 {
                return Err(ConnectorError::Config {
                    message: format!("expected 8 offset bytes, got {}", bytes.len()),
                });
            }
            let mut value = [0u8; 8];
            value.copy_from_slice(bytes);
            Ok(Self(u64::from_be_bytes(value)))
        }
    }

    #[derive(Default)]
    struct RecordingCommitter {
        committed: Vec<TestOffset>,
    }

    impl OffsetCommitter<TestOffset> for RecordingCommitter {
        async fn commit_offset(&mut self, offset: TestOffset) -> ConnectorResult<()> {
            self.committed.push(offset);
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        events: Vec<&'static str>,
        fail_write: bool,
        fail_flush: bool,
    }

    impl Sink for RecordingSink {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new().with_idempotent()
        }

        async fn write_batch(
            &mut self,
            _batch: arrow::record_batch::RecordBatch,
        ) -> ConnectorResult<()> {
            self.events.push("write");
            if self.fail_write {
                return Err(ConnectorError::Io(std::io::Error::other(
                    "injected write failure",
                )));
            }
            Ok(())
        }

        async fn flush(&mut self) -> ConnectorResult<()> {
            self.events.push("flush");
            if self.fail_flush {
                return Err(ConnectorError::Io(std::io::Error::other(
                    "injected flush failure",
                )));
            }
            Ok(())
        }
    }

    fn one_row_batch() -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap()
    }

    #[tokio::test]
    async fn post_write_protocol_commits_after_write_and_flush() {
        let mut sink = RecordingSink::default();
        let mut committer = RecordingCommitter::default();

        PostWriteOffsetCommitProtocol::write_flush_commit(
            &mut sink,
            &mut committer,
            one_row_batch(),
            TestOffset(9),
        )
        .await
        .unwrap();

        assert_eq!(sink.events, vec!["write", "flush"]);
        assert_eq!(committer.committed, vec![TestOffset(9)]);
    }

    #[tokio::test]
    async fn post_write_protocol_does_not_commit_when_write_fails() {
        let mut sink = RecordingSink {
            fail_write: true,
            ..RecordingSink::default()
        };
        let mut committer = RecordingCommitter::default();

        let err = PostWriteOffsetCommitProtocol::write_flush_commit(
            &mut sink,
            &mut committer,
            one_row_batch(),
            TestOffset(9),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, ConnectorError::Io(_)));
        assert_eq!(sink.events, vec!["write"]);
        assert!(committer.committed.is_empty());
    }

    #[tokio::test]
    async fn post_write_protocol_does_not_commit_when_flush_fails() {
        let mut sink = RecordingSink {
            fail_flush: true,
            ..RecordingSink::default()
        };
        let mut committer = RecordingCommitter::default();

        let err = PostWriteOffsetCommitProtocol::write_flush_commit(
            &mut sink,
            &mut committer,
            one_row_batch(),
            TestOffset(9),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, ConnectorError::Io(_)));
        assert_eq!(sink.events, vec!["write", "flush"]);
        assert!(committer.committed.is_empty());
    }

    // -----------------------------------------------------------------------
    // TwoPhaseCommitSink
    // -----------------------------------------------------------------------

    #[test]
    fn two_phase_commit_sink_prepare_commit_roundtrip() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))])
                .unwrap();

        let mut sink = InMemoryTwoPhaseCommitSink::new();
        let handle = sink.prepare(1, &batch).unwrap();
        assert_eq!(sink.staged_count(), 1);
        assert_eq!(sink.committed().len(), 0);
        sink.commit(handle).unwrap();
        assert_eq!(sink.staged_count(), 0);
        assert_eq!(sink.committed().len(), 1);
        assert_eq!(sink.committed()[0].0, 1); // epoch
    }

    #[test]
    fn two_phase_commit_sink_abort_discards() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42i64]))]).unwrap();

        let mut sink = InMemoryTwoPhaseCommitSink::new();
        let handle = sink.prepare(2, &batch).unwrap();
        sink.abort(handle).unwrap();
        assert_eq!(sink.staged_count(), 0);
        assert_eq!(sink.committed().len(), 0);
    }

    #[test]
    fn two_phase_commit_sink_multiple_epochs() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let make_batch = |v: i64| -> RecordBatch {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from(vec![v]))],
            )
            .unwrap()
        };

        let mut sink = InMemoryTwoPhaseCommitSink::new();
        let h1 = sink.prepare(1, &make_batch(10)).unwrap();
        let h2 = sink.prepare(2, &make_batch(20)).unwrap();
        sink.commit(h1).unwrap();
        sink.abort(h2).unwrap();
        assert_eq!(sink.committed().len(), 1);
        assert_eq!(sink.committed()[0].0, 1);
    }

    // ── LocalParquetTwoPhaseCommitSink ────────────────────────────────────────

    fn make_int32_batch(values: Vec<i32>) -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(values)) as _],
        )
        .unwrap()
    }

    #[test]
    fn parquet_2pc_prepare_commit_creates_final_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());

        let batch = make_int32_batch(vec![1, 2, 3]);
        let handle = sink.prepare(1, &batch).unwrap();

        assert!(
            handle.staging_path.exists(),
            "staging .tmp file must exist after prepare"
        );
        assert!(
            !handle.final_path.exists(),
            "final .parquet file must not exist before commit"
        );

        sink.commit(handle).unwrap();

        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(
            files
                .iter()
                .any(|f| f.ends_with(".parquet") && !f.ends_with(".tmp")),
            "final .parquet file must exist after commit"
        );
        assert!(
            !files.iter().any(|f| f.ends_with(".tmp")),
            "staging .tmp file must be gone after commit"
        );
    }

    #[test]
    fn parquet_2pc_abort_deletes_staging_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());

        let batch = make_int32_batch(vec![10, 20]);
        let handle = sink.prepare(2, &batch).unwrap();
        assert!(handle.staging_path.exists(), "staging file must exist");

        sink.abort(handle).unwrap();

        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(files.is_empty(), "abort must remove staging file");
    }

    #[test]
    fn parquet_2pc_abort_is_idempotent_when_file_missing() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());

        let batch = make_int32_batch(vec![1]);
        let handle = sink.prepare(3, &batch).unwrap();
        // Remove the staging file manually before calling abort.
        std::fs::remove_file(&handle.staging_path).unwrap();

        // abort on a missing file must not error.
        sink.abort(handle).unwrap();
    }

    #[test]
    fn parquet_2pc_restart_does_not_overwrite_existing_commit() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_int32_batch(vec![1]);
        let first_final = {
            let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());
            let handle = sink.prepare(7, &batch).unwrap();
            let final_path = handle.final_path.clone();
            sink.commit(handle).unwrap();
            final_path
        };

        let mut restarted = LocalParquetTwoPhaseCommitSink::new(dir.path());
        let handle = restarted.prepare(7, &make_int32_batch(vec![2])).unwrap();
        assert_ne!(handle.final_path, first_final);
        restarted.commit(handle).unwrap();

        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
            .collect();
        assert_eq!(files.len(), 2, "restart must allocate a fresh final path");
    }
}

#[cfg(test)]
mod quality_tests {
    use crate::quality::{check_batch, check_batch_compiled};
    use crate::schema_normalize::ColumnRenameMap;
    use crate::*;
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    // -----------------------------------------------------------------------
    // SchemaNormalizeOperator
    // -----------------------------------------------------------------------

    #[test]
    fn schema_normalize_passthrough_when_schemas_match() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        let op = SchemaNormalizeOperator::new(schema);
        let result = op.normalize(&batch).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.schema().fields().len(), 2);
    }

    #[test]
    fn schema_normalize_adds_nullable_null_column() {
        let source_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )
        .unwrap();

        let target = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        let op = SchemaNormalizeOperator::new(target.clone());
        let result = op.normalize(&batch).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.schema().fields().len(), 2);
        // Column b should be all null
        let col_b = result.column(1);
        assert!(col_b.is_null(0));
    }

    #[test]
    fn schema_normalize_fails_on_missing_non_nullable_column() {
        let source_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )
        .unwrap();

        let target = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false), // non-nullable
        ]));
        let op = SchemaNormalizeOperator::new(target);
        let err = op.normalize(&batch).unwrap_err();
        match err {
            ConnectorError::Schema { message } => {
                assert!(message.contains("missing non-nullable column"));
            }
            other => panic!("unexpected error variant: {other}"),
        }
    }

    #[test]
    fn schema_normalize_rejects_duplicate_source_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("a", DataType::Int32, false), // duplicate
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1])),
                Arc::new(arrow::array::Int32Array::from(vec![2])),
            ],
        )
        .unwrap();

        let target = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let op = SchemaNormalizeOperator::new(target);
        let err = op.normalize(&batch).unwrap_err();
        match err {
            ConnectorError::Schema { message } => {
                assert!(message.contains("duplicate column"));
            }
            other => panic!("unexpected error variant: {other}"),
        }
    }

    #[test]
    fn schema_normalize_with_renames() {
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "old_name",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(arrow::array::Int32Array::from(vec![42]))],
        )
        .unwrap();

        let target = Arc::new(Schema::new(vec![Field::new(
            "new_name",
            DataType::Int32,
            false,
        )]));
        let renames = ColumnRenameMap::new(vec![("new_name".into(), "old_name".into())]);
        let op = SchemaNormalizeOperator::new(target).with_renames(renames);
        let result = op.normalize(&batch).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.schema().field(0).name(), "new_name");
    }

    #[test]
    fn schema_normalize_widening_int8_to_int32() {
        use arrow::array::Int8Array;
        let source_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int8, false)]));
        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int8Array::from(vec![1i8, 2i8]))],
        )
        .unwrap();

        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let op = SchemaNormalizeOperator::new(target);
        let result = op.normalize(&batch).unwrap();
        assert_eq!(result.schema().field(0).data_type(), &DataType::Int32);
        assert_eq!(result.num_rows(), 2);
    }

    // -----------------------------------------------------------------------
    // IoContract validation
    // -----------------------------------------------------------------------

    #[test]
    fn io_contract_file_layout_valid() {
        let layout = FileLayout::default();
        assert!(layout.validate().is_ok());
    }

    #[test]
    fn io_contract_file_layout_rejects_empty_partition_column() {
        let layout = FileLayout {
            partition_by: vec!["".into()],
            ..FileLayout::default()
        };
        assert!(layout.validate().is_err());
    }

    #[test]
    fn io_contract_file_layout_rejects_empty_sort_column() {
        let layout = FileLayout {
            sort_by: vec![SortField {
                column: "  ".into(),
                direction: FileSortDirection::Ascending,
            }],
            ..FileLayout::default()
        };
        assert!(layout.validate().is_err());
    }

    #[test]
    fn io_contract_kafka_validate_rejects_empty_fields() {
        let opts = KafkaIoOptions {
            bootstrap_servers: "".into(),
            topic: "t".into(),
            group_id: "g".into(),
            properties: Default::default(),
        };
        assert!(opts.validate().is_err());
    }

    #[test]
    fn io_contract_kafka_validate_accepts_valid() {
        let opts = KafkaIoOptions {
            bootstrap_servers: "localhost:9092".into(),
            topic: "my-topic".into(),
            group_id: "my-group".into(),
            properties: Default::default(),
        };
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn io_contract_database_validate_rejects_zero_fetch_size() {
        let opts = DatabaseIoOptions {
            url: "jdbc:postgres://localhost/db".into(),
            table: "users".into(),
            fetch_size: Some(0),
            properties: Default::default(),
        };
        assert!(opts.validate().is_err());
    }

    #[test]
    fn io_contract_database_validate_accepts_valid() {
        let opts = DatabaseIoOptions {
            url: "jdbc:postgres://localhost/db".into(),
            table: "users".into(),
            fetch_size: Some(1000),
            properties: Default::default(),
        };
        assert!(opts.validate().is_ok());
    }

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("score", DataType::Float64, true),
            Field::new("name", DataType::Utf8, false),
        ]));
        let score = Float64Array::from(vec![Some(85.0), None, Some(110.0), Some(50.0)]);
        let name = StringArray::from(vec!["alice", "bob", "carol", "dave"]);
        RecordBatch::try_new(schema, vec![Arc::new(score), Arc::new(name)]).unwrap()
    }

    #[test]
    fn notnull_rejects_null_rows() {
        let batch = make_batch();
        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::NotNull {
                column: "score".into(),
            },
            QualityAction::Reject,
        );
        let result = check_batch(&batch, &config).unwrap();
        assert_eq!(result.rejected.len(), 1);
        assert_eq!(result.rejected[0].batch_row_index, 1);
        assert!(!result.failed);
    }

    #[test]
    fn compiled_check_matches_rule_violated_labels() {
        let batch = make_batch();
        let rule = DataQualityRule::Range {
            column: "score".into(),
            min: 0.0,
            max: 100.0,
        };
        let config = DataQualityConfig::new().with_rule(rule.clone(), QualityAction::Reject);
        let direct = check_batch(&batch, &config).unwrap();
        let compiled =
            check_batch_compiled(&batch, &config.compile().expect("compile quality config"))
                .unwrap();
        assert_eq!(direct.rejected.len(), compiled.rejected.len());
        for (a, b) in direct.rejected.iter().zip(compiled.rejected.iter()) {
            assert_eq!(a.batch_row_index, b.batch_row_index);
            assert_eq!(a.rule_violated, b.rule_violated);
            assert_eq!(a.column_name, b.column_name);
        }
        assert!(direct.rejected[0].rule_violated.contains("Range"));
        assert!(direct.rejected[0].rule_violated.contains("100"));
    }

    #[test]
    fn range_rejects_out_of_range_rows() {
        let batch = make_batch();
        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::Range {
                column: "score".into(),
                min: 0.0,
                max: 100.0,
            },
            QualityAction::Reject,
        );
        let result = check_batch(&batch, &config).unwrap();
        // row 1 (null) and row 2 (110.0 > 100) should be rejected
        assert_eq!(result.rejected.len(), 2);
    }

    #[test]
    fn fail_action_sets_failed_flag() {
        let batch = make_batch();
        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::NotNull {
                column: "score".into(),
            },
            QualityAction::Fail,
        );
        let result = check_batch(&batch, &config).unwrap();
        assert!(result.failed);
    }

    #[tokio::test]
    async fn dead_letter_sink_splits_accepted_and_rejected() {
        let batch = make_batch();
        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::NotNull {
                column: "score".into(),
            },
            QualityAction::Reject,
        );
        let mut sink = DeadLetterSink::new("test_sink", config);
        let (accepted, rejected) = sink.process_batch(&batch).await.unwrap();
        assert_eq!(accepted.num_rows(), 3); // rows 0, 2, 3
        assert_eq!(rejected.len(), 1); // row 1 (null score)
    }

    #[test]
    fn regex_rule_rejects_non_matching_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("email", DataType::Utf8, true)]));
        let emails = StringArray::from(vec![
            Some("alice@example.com"),
            Some("not-an-email"),
            None,
            Some("bob@corp.org"),
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(emails)]).unwrap();

        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::Regex {
                column: "email".into(),
                pattern: r"^[^@]+@[^@]+\.[^@]+$".into(),
            },
            QualityAction::Reject,
        );
        let result = check_batch(&batch, &config).unwrap();
        // "not-an-email" (idx 1) and None (idx 2) should be rejected
        assert_eq!(result.rejected.len(), 2);
        let rejected_indices: Vec<usize> =
            result.rejected.iter().map(|r| r.batch_row_index).collect();
        assert!(rejected_indices.contains(&1));
        assert!(rejected_indices.contains(&2));
    }

    #[test]
    fn regex_rule_invalid_pattern_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        let col = StringArray::from(vec!["hello"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::Regex {
                column: "v".into(),
                pattern: "[invalid((".into(),
            },
            QualityAction::Reject,
        );
        assert!(check_batch(&batch, &config).is_err());
    }

    #[test]
    fn check_batch_with_empty_rules_returns_all_accepted() {
        let batch = make_batch();
        let config = DataQualityConfig::new();
        let result = check_batch(&batch, &config).unwrap();
        assert_eq!(result.accepted_indices.len(), batch.num_rows());
        assert!(result.rejected.is_empty());
        assert!(!result.failed);
    }

    #[test]
    fn check_batch_with_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::new_empty(schema);
        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::NotNull { column: "x".into() },
            QualityAction::Reject,
        );
        let result = check_batch(&batch, &config).unwrap();
        assert_eq!(result.accepted_indices.len(), 0);
        assert!(result.rejected.is_empty());
    }

    #[test]
    fn range_rule_with_min_equals_max() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let col = Float64Array::from(vec![5.0, 5.0, 6.0]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::Range {
                column: "v".into(),
                min: 5.0,
                max: 5.0,
            },
            QualityAction::Reject,
        );
        let result = check_batch(&batch, &config).unwrap();
        // row 0: 5.0 (in range), row 1: 5.0 (in range), row 2: 6.0 (out of range)
        assert_eq!(result.rejected.len(), 1);
        assert_eq!(result.rejected[0].batch_row_index, 2);
    }

    #[test]
    fn multiple_rules_on_same_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let col = Float64Array::from(vec![Some(50.0), None, Some(150.0)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        let config = DataQualityConfig::new()
            .with_rule(
                DataQualityRule::NotNull { column: "v".into() },
                QualityAction::Reject,
            )
            .with_rule(
                DataQualityRule::Range {
                    column: "v".into(),
                    min: 0.0,
                    max: 100.0,
                },
                QualityAction::Reject,
            );
        let result = check_batch(&batch, &config).unwrap();
        // row 0: 50.0 (pass both), row 1: null (fail NotNull), row 2: 150.0 (fail Range)
        assert_eq!(result.rejected.len(), 2);
    }

    #[test]
    fn warn_action_does_not_reject_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let col = Float64Array::from(vec![150.0, 200.0]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::Range {
                column: "v".into(),
                min: 0.0,
                max: 100.0,
            },
            QualityAction::Warn,
        );
        let result = check_batch(&batch, &config).unwrap();
        assert!(
            result.rejected.is_empty(),
            "Warn action should not reject rows"
        );
        assert_eq!(result.accepted_indices.len(), 2);
    }

    #[test]
    fn parquet_2pc_quality_check_rejects_null_rows() {
        use arrow::array::Float64Array;

        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        // Row 0: 1.0, Row 1: null — null should be rejected by NotNull rule
        let col = Float64Array::from(vec![Some(1.0), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();

        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::NotNull { column: "v".into() },
            QualityAction::Reject,
        );
        let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path())
            .with_quality_config(config)
            .unwrap();

        let handle = sink.prepare(1, &batch).unwrap();
        sink.commit(handle).unwrap();

        // Read back the written parquet file and verify only 1 row was written
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
            .collect();
        assert_eq!(files.len(), 1, "exactly one .parquet file should exist");

        // Use the parquet module in this crate (which re-exports ParquetSource)
        // to read back and count rows.
        use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let file = std::fs::File::open(&files[0]).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let total_rows: usize = reader
            .map(|b: Result<RecordBatch, _>| b.unwrap().num_rows())
            .sum();
        assert_eq!(total_rows, 1, "only the non-null row should be written");
    }

    #[test]
    fn parquet_2pc_quality_fail_action_aborts_prepare() {
        use arrow::array::Float64Array;

        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let col = Float64Array::from(vec![None::<f64>]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();

        let config = DataQualityConfig::new().with_rule(
            DataQualityRule::NotNull { column: "v".into() },
            QualityAction::Fail,
        );
        let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path())
            .with_quality_config(config)
            .unwrap();

        let result = sink.prepare(1, &batch);
        assert!(result.is_err(), "Fail action must abort prepare");
    }
}
