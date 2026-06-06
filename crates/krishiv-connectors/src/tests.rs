#[cfg(test)]
mod connector_tests {
    use crate::*;
    use std::any::Any;
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

    // -----------------------------------------------------------------------
    // CertificationSuite: source with no capabilities
    // -----------------------------------------------------------------------

    struct NullSource;

    impl Source for NullSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new() // all false
        }

        async fn read_batch(
            &mut self,
        ) -> ConnectorResult<Option<arrow::record_batch::RecordBatch>> {
            Ok(None)
        }

        fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
            None
        }
    }

    #[test]
    fn certification_suite_rejects_source_with_no_capabilities() {
        let source = NullSource;
        let result = CertificationSuite::run_source_capabilities_test(&source);
        assert!(result.is_err());
        match result.unwrap_err() {
            ConnectorError::Unsupported { .. } => {}
            other => panic!("expected Unsupported, got: {other}"),
        }
    }

    struct ModeLessSource;

    impl Source for ModeLessSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new().with_rewindable()
        }

        async fn read_batch(
            &mut self,
        ) -> ConnectorResult<Option<arrow::record_batch::RecordBatch>> {
            Ok(None)
        }

        fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
            Some(Box::new(0usize))
        }
    }

    #[test]
    fn certification_suite_rejects_source_without_boundedness_mode() {
        let source = ModeLessSource;
        let error = CertificationSuite::run_source_capabilities_test(&source)
            .expect_err("source must declare bounded or unbounded");
        assert!(
            matches!(error, ConnectorError::CertificationFailed { .. }),
            "unexpected error: {error}"
        );
    }

    struct BrokenRewindSource {
        cursor: usize,
    }

    impl Source for BrokenRewindSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new()
                .with_bounded()
                .with_rewindable()
        }

        async fn read_batch(
            &mut self,
        ) -> ConnectorResult<Option<arrow::record_batch::RecordBatch>> {
            if self.cursor > 0 {
                return Ok(None);
            }
            self.cursor += 1;
            Ok(Some(arrow::record_batch::RecordBatch::new_empty(Arc::new(
                Schema::empty(),
            ))))
        }

        fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
            Some(Box::new(self.cursor))
        }
    }

    #[tokio::test]
    async fn certification_rewind_test_detects_default_noop_reset() {
        let mut source = BrokenRewindSource { cursor: 0 };
        let error = CertificationSuite::run_rewind_test::<usize>(&mut source)
            .await
            .expect_err("default no-op reset must fail certification");
        assert!(
            error.to_string().contains("did not restore initial offset"),
            "unexpected error: {error}"
        );
    }

    struct BrokenCheckpointSource {
        cursor: usize,
    }

    impl Source for BrokenCheckpointSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new()
                .with_bounded()
                .with_checkpoint()
        }

        async fn read_batch(
            &mut self,
        ) -> ConnectorResult<Option<arrow::record_batch::RecordBatch>> {
            if self.cursor > 0 {
                return Ok(None);
            }
            self.cursor += 1;
            Ok(Some(arrow::record_batch::RecordBatch::new_empty(Arc::new(
                Schema::empty(),
            ))))
        }

        fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
            Some(Box::new(ParquetOffset {
                batch_index: self.cursor,
            }))
        }
    }

    impl CheckpointSource for BrokenCheckpointSource {
        type Offset = ParquetOffset;

        fn checkpoint_offset(&self) -> ConnectorResult<Self::Offset> {
            Ok(ParquetOffset {
                batch_index: self.cursor,
            })
        }

        fn restore_offset(&mut self, _offset: &Self::Offset) -> ConnectorResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn checkpoint_certification_detects_noop_restore() {
        let mut source = BrokenCheckpointSource { cursor: 0 };
        let error = CertificationSuite::run_checkpoint_restore_test(&mut source)
            .await
            .expect_err("checkpoint restore must change the source position");
        assert!(
            error.to_string().contains("did not recover initial offset"),
            "unexpected error: {error}"
        );
    }

    // -----------------------------------------------------------------------
    // CertificationSuite: bounded exhaustion test
    // -----------------------------------------------------------------------

    struct ThreeBatchSource {
        count: usize,
    }

    impl Source for ThreeBatchSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new().with_bounded()
        }

        async fn read_batch(
            &mut self,
        ) -> ConnectorResult<Option<arrow::record_batch::RecordBatch>> {
            if self.count < 3 {
                self.count += 1;
                Ok(Some(arrow::record_batch::RecordBatch::new_empty(
                    std::sync::Arc::new(arrow::datatypes::Schema::empty()),
                )))
            } else {
                Ok(None)
            }
        }

        fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
            None
        }
    }

    struct UnboundedSource;

    impl Source for UnboundedSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new().with_unbounded()
        }

        async fn read_batch(
            &mut self,
        ) -> ConnectorResult<Option<arrow::record_batch::RecordBatch>> {
            Ok(None)
        }

        fn current_offset(&self) -> Option<Box<dyn Any + Send>> {
            None
        }
    }

    #[tokio::test]
    async fn certification_exhaustion_test_passes_for_bounded_source() {
        let mut source = ThreeBatchSource { count: 0 };
        let result = CertificationSuite::run_bounded_exhaustion_test(&mut source).await;
        assert!(result.is_ok(), "bounded source exhaustion test should pass");
    }

    // -----------------------------------------------------------------------
    // ParquetOffset + AtLeastOnceSinkContract + CertificationSuite offset test
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
    fn at_least_once_contract_exists() {
        let _ = AtLeastOnceSinkContract;
    }

    #[test]
    fn certification_offset_round_trip_passes_for_parquet_offset() {
        let offset = ParquetOffset { batch_index: 7 };
        CertificationSuite::run_offset_round_trip_test(offset).unwrap();
    }

    #[tokio::test]
    async fn certification_exhaustion_test_rejects_unbounded_source() {
        let mut source = UnboundedSource;
        let err = CertificationSuite::run_bounded_exhaustion_test(&mut source)
            .await
            .unwrap_err();
        match err {
            ConnectorError::Unsupported { .. } => {}
            other => panic!("expected Unsupported, got: {other}"),
        }
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
                return Err(ConnectorError::IoStr {
                    message: "injected write failure".into(),
                });
            }
            Ok(())
        }

        async fn flush(&mut self) -> ConnectorResult<()> {
            self.events.push("flush");
            if self.fail_flush {
                return Err(ConnectorError::IoStr {
                    message: "injected flush failure".into(),
                });
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

        assert!(matches!(err, ConnectorError::IoStr { .. }));
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

        assert!(matches!(err, ConnectorError::IoStr { .. }));
        assert_eq!(sink.events, vec!["write", "flush"]);
        assert!(committer.committed.is_empty());
    }

    // -----------------------------------------------------------------------
    // TwoPhaseCommitSink
    // -----------------------------------------------------------------------

    struct DishonestTwoPhaseSink;

    impl TwoPhaseCommitSink for DishonestTwoPhaseSink {
        type Handle = ();

        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new().with_checkpoint()
        }

        fn prepare(
            &mut self,
            _epoch: u64,
            _batch: &arrow::record_batch::RecordBatch,
        ) -> ConnectorResult<Self::Handle> {
            Ok(())
        }

        fn commit(&mut self, _handle: Self::Handle) -> ConnectorResult<()> {
            Ok(())
        }

        fn abort(&mut self, _handle: Self::Handle) -> ConnectorResult<()> {
            Ok(())
        }
    }

    #[test]
    fn certification_rejects_two_phase_sink_without_transactional_capability() {
        let sink = DishonestTwoPhaseSink;
        let error = CertificationSuite::run_two_phase_commit_capabilities_test(&sink)
            .expect_err("2PC certification must inspect the actual sink capabilities");
        assert!(
            matches!(error, ConnectorError::CertificationFailed { .. }),
            "unexpected error: {error}"
        );
    }

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
    fn in_memory_two_phase_commit_lifecycle_is_retry_safe() {
        let batch = make_int32_batch(vec![1, 2, 3]);
        let mut sink = InMemoryTwoPhaseCommitSink::new();

        CertificationSuite::run_two_phase_commit_lifecycle_test(&mut sink, 9, &batch)
            .expect("in-memory 2PC sink must tolerate coordinator decision retries");

        assert_eq!(sink.staged_count(), 0);
        assert_eq!(
            sink.committed().len(),
            1,
            "repeated commit must not duplicate output"
        );
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
    use crate::*;
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

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
