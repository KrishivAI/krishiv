// ── GAP-6: stream:loop: tests ────────────────────────────────────────────

/// Verify that `stream:loop:` invokes `ContinuousWindowExecutor` and emits
/// closed-window batches after the watermark crosses the window boundary.
#[tokio::test]
async fn stream_loop_emits_window_via_continuous_executor() {
    use std::sync::Arc;

    // Build an input batch: events at t=100 and t=12_000 ms in a 10s window.
    // With lag=0 the second event advances the watermark past the first window.
    let batch = krishiv_common::test_fixtures::make_test_key_ts_batch(
        vec!["a", "a"],
        vec![100_i64, 12_000_i64],
    )
    .unwrap();

    let drainer = OneShotDrainer::new(vec![batch]);
    let window_spec = "stream:tw:key=key:time=ts:win=10000:lag=0:agg=count";
    let fragment = format!("stream:loop:loop-job-1|{window_spec}");

    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
        .with_continuous_drainer(drainer as Arc<dyn crate::ContinuousJobDrainer>);

    let assignment = make_loop_assignment(&fragment);
    let output = runner
        .execute_streaming_fragment(&assignment)
        .await
        .expect("stream:loop execution should succeed");

    assert_eq!(
        output.kind(),
        ExecutorTaskOutputKind::StreamingWindow,
        "stream:loop should emit StreamingWindow output"
    );
    // The first tumbling window [0, 10000) must be emitted when the watermark
    // advances past 10000 due to the event at ts=12000.
    assert!(
        output.row_count() > 0,
        "stream:loop should emit at least one window row"
    );
}

/// Verify that `stream:loop:` reuses the same `ContinuousWindowExecutor`
/// across multiple drain cycles (state is accumulated).
#[tokio::test]
async fn stream_loop_reuses_executor_state_across_drains() {
    use std::sync::Arc;

    // First drain: single event in [0, 10000).  Window not yet emitted.
    let batch1 =
        krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["a"], vec![500_i64]).unwrap();

    // Second drain: event that advances watermark past 10000, closing the window.
    let batch2 =
        krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["a"], vec![15_000_i64]).unwrap();

    // The runner is CLONED between drains to simulate a re-used runner; its
    // `loop_executors` Arc is shared so state survives.
    let window_spec = "stream:tw:key=key:time=ts:win=10000:lag=0:agg=count";
    let fragment = format!("stream:loop:loop-job-state|{window_spec}");

    // Drainer 1: returns batch1 then empty.
    struct SeqDrainer {
        inner: std::sync::Mutex<std::collections::VecDeque<Vec<RecordBatch>>>,
    }
    impl crate::ContinuousJobDrainer for SeqDrainer {
        fn drain_job(&self, _job_id: &str) -> Result<Vec<RecordBatch>, String> {
            Ok(self.inner.lock().unwrap().pop_front().unwrap_or_default())
        }
    }
    let drainer = Arc::new(SeqDrainer {
        inner: std::sync::Mutex::new(std::collections::VecDeque::from(vec![
            vec![batch1],
            vec![batch2],
        ])),
    });

    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
        .with_continuous_drainer(drainer as Arc<dyn crate::ContinuousJobDrainer>);

    // First drain: no window should be emitted yet.
    let out1 = runner
        .execute_streaming_fragment(&make_loop_assignment(&fragment))
        .await
        .expect("drain 1 ok");
    assert_eq!(out1.row_count(), 0, "no window emitted on first drain");

    // Second drain: reuse the same runner (shared loop_executors); window
    // should now be emitted.
    let out2 = runner
        .execute_streaming_fragment(&make_loop_assignment(&fragment))
        .await
        .expect("drain 2 ok");
    assert!(
        out2.row_count() > 0,
        "window must be emitted on second drain after watermark advances"
    );
}

/// Distributed stream:loop cycles consume coordinator-delivered InlineIpc
/// partitions when no local drainer is attached.
#[tokio::test]
async fn stream_loop_without_drainer_processes_inline_input() {
    let window_spec = krishiv_plan::window::encode_window_execution_spec(
        &krishiv_plan::window::WindowExecutionSpec::tumbling("key", "ts", 10_000),
    )
    .unwrap();
    let fragment = format!("stream:loop:no-drainer-job|{window_spec}");
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let batch = krishiv_common::test_fixtures::make_test_key_ts_batch(
        vec!["a", "a"],
        vec![100_i64, 12_000_i64],
    )
    .unwrap();
    let assignment = make_loop_assignment_with_batches(&fragment, &[batch]);
    let output = runner
        .execute_streaming_fragment(&assignment)
        .await
        .expect("inline continuous cycle must execute");
    assert!(
        output.row_count() > 0,
        "inline cycle must execute the registered window instead of echoing input"
    );
}

#[tokio::test]
async fn stream_loop_registry_source_restores_and_records_checkpoint_offset() {
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use krishiv_connectors::{
        ConnectorCapabilities, ConnectorConfig, ConnectorResult, DynSource, Source,
        registry::{
            ConnectorDescriptor, ConnectorKind, ConnectorRegistry, ConnectorRole, OpenSourceFuture,
            SourceDriver,
        },
    };

    #[derive(Clone)]
    struct RestoringSourceDriver {
        batches: Arc<Vec<RecordBatch>>,
    }

    impl SourceDriver for RestoringSourceDriver {
        fn descriptor(&self) -> ConnectorDescriptor {
            ConnectorDescriptor::new(
                ConnectorKind::Csv,
                ConnectorRole::Source,
                ConnectorCapabilities::new()
                    .with_bounded()
                    .with_rewindable()
                    .with_checkpoint(),
            )
        }

        fn validate(&self, _config: &ConnectorConfig) -> ConnectorResult<()> {
            Ok(())
        }

        fn open<'a>(&'a self, _config: &'a ConnectorConfig) -> OpenSourceFuture<'a> {
            let batches = Arc::clone(&self.batches);
            Box::pin(async move {
                Ok(Box::new(RestoringSource {
                    schema: batches
                        .first()
                        .map(|batch| batch.schema())
                        .unwrap_or_else(|| Arc::new(Schema::empty())),
                    batches,
                    cursor: 0,
                }) as Box<dyn DynSource>)
            })
        }
    }

    struct RestoringSource {
        schema: arrow::datatypes::SchemaRef,
        batches: Arc<Vec<RecordBatch>>,
        cursor: usize,
    }

    impl Source for RestoringSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new()
                .with_bounded()
                .with_rewindable()
                .with_checkpoint()
        }

        fn source_schema(&self) -> Option<arrow::datatypes::SchemaRef> {
            Some(self.schema.clone())
        }

        async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
            let batch = self.batches.get(self.cursor).cloned();
            if batch.is_some() {
                self.cursor = self.cursor.saturating_add(1);
            }
            Ok(batch)
        }

        fn current_offset(&self) -> Option<Box<dyn std::any::Any + Send>> {
            Some(Box::new(self.cursor))
        }

        fn encoded_checkpoint_offset(&self) -> ConnectorResult<Option<Vec<u8>>> {
            Ok(Some((self.cursor as u64).to_le_bytes().to_vec()))
        }

        fn restore_encoded_checkpoint_offset(&mut self, encoded: &[u8]) -> ConnectorResult<()> {
            if encoded.len() != 8 {
                return Err(krishiv_connectors::ConnectorError::Offset {
                    message: format!("expected 8 encoded cursor bytes, got {}", encoded.len()),
                });
            }
            let mut raw = [0_u8; 8];
            raw.copy_from_slice(encoded);
            self.cursor = usize::try_from(u64::from_le_bytes(raw)).map_err(|_| {
                krishiv_connectors::ConnectorError::Offset {
                    message: "encoded cursor exceeds usize".into(),
                }
            })?;
            Ok(())
        }
    }

    let first =
        krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["a"], vec![100_i64]).unwrap();
    let second =
        krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["a"], vec![12_000_i64]).unwrap();
    let mut registry = ConnectorRegistry::new();
    registry.register_source(Arc::new(RestoringSourceDriver {
        batches: Arc::new(vec![first, second]),
    }));

    let job = "registry-restore-job";
    let window_spec = "stream:tw:key=key:time=ts:win=10000:lag=0:agg=count";
    let fragment = format!("stream:loop:{job}|{window_spec}");
    let runner =
        ExecutorTaskRunner::new(ExecutorAssignmentInbox::new()).with_connector_registry(registry);
    runner.source_restore_offsets.insert(
        job.to_owned(),
        vec![crate::runner::RestoredSourceOffset {
            partition_id: "events-partition-0".into(),
            source_id: Some("events".into()),
            legacy_offset: 1,
            encoded_offset: 1_u64.to_le_bytes().to_vec(),
        }],
    );
    let assignment =
        make_loop_assignment(&fragment).with_input_partitions(vec![InputPartition::new(
            "events-partition-0",
            "registry-connector:csv:events:{}",
        )]);

    let output = runner
        .execute_streaming_fragment(&assignment)
        .await
        .expect("registry stream loop restore");

    assert_eq!(
        output.row_count(),
        0,
        "restored source starts at the second event; the first window must not close"
    );
    let task_id = TaskId::try_new("task-1").unwrap();
    let checkpoint_runner = runner.checkpoint_runners.get(&task_id).unwrap();
    let checkpoint_runner = checkpoint_runner.lock().unwrap();
    assert_eq!(checkpoint_runner.source_offsets.len(), 1);
    assert_eq!(
        checkpoint_runner.source_offsets[0].partition_id.as_str(),
        "events-partition-0"
    );
    assert_eq!(checkpoint_runner.source_offsets[0].offset, 2);
    assert_eq!(
        checkpoint_runner.source_offsets[0].encoded_offset,
        2_u64.to_le_bytes().to_vec()
    );
}

#[tokio::test]
async fn stream_loop_registry_source_persists_across_cycles() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use arrow::datatypes::Schema;
    use krishiv_connectors::{
        ConnectorCapabilities, ConnectorConfig, ConnectorResult, DynSource, Source,
        registry::{
            ConnectorDescriptor, ConnectorKind, ConnectorRegistry, ConnectorRole, OpenSourceFuture,
            SourceDriver,
        },
    };

    #[derive(Clone)]
    struct OneBatchPerCycleDriver {
        batches: Arc<Vec<RecordBatch>>,
        open_count: Arc<AtomicUsize>,
    }

    impl SourceDriver for OneBatchPerCycleDriver {
        fn descriptor(&self) -> ConnectorDescriptor {
            ConnectorDescriptor::new(
                ConnectorKind::Csv,
                ConnectorRole::Source,
                ConnectorCapabilities::new()
                    .with_bounded()
                    .with_rewindable()
                    .with_checkpoint(),
            )
        }

        fn validate(&self, _config: &ConnectorConfig) -> ConnectorResult<()> {
            Ok(())
        }

        fn open<'a>(&'a self, _config: &'a ConnectorConfig) -> OpenSourceFuture<'a> {
            let batches = Arc::clone(&self.batches);
            let open_count = Arc::clone(&self.open_count);
            Box::pin(async move {
                open_count.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(OneBatchPerCycleSource {
                    schema: batches
                        .first()
                        .map(|batch| batch.schema())
                        .unwrap_or_else(|| Arc::new(Schema::empty())),
                    batches,
                    cursor: 0,
                    yielded_before_idle: false,
                }) as Box<dyn DynSource>)
            })
        }
    }

    struct OneBatchPerCycleSource {
        schema: arrow::datatypes::SchemaRef,
        batches: Arc<Vec<RecordBatch>>,
        cursor: usize,
        yielded_before_idle: bool,
    }

    impl Source for OneBatchPerCycleSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new()
                .with_bounded()
                .with_rewindable()
                .with_checkpoint()
        }

        fn source_schema(&self) -> Option<arrow::datatypes::SchemaRef> {
            Some(self.schema.clone())
        }

        async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
            if self.yielded_before_idle {
                self.yielded_before_idle = false;
                return Ok(None);
            }
            let batch = self.batches.get(self.cursor).cloned();
            if batch.is_some() {
                self.cursor = self.cursor.saturating_add(1);
                self.yielded_before_idle = true;
            }
            Ok(batch)
        }

        fn current_offset(&self) -> Option<Box<dyn std::any::Any + Send>> {
            Some(Box::new(self.cursor))
        }

        fn encoded_checkpoint_offset(&self) -> ConnectorResult<Option<Vec<u8>>> {
            Ok(Some((self.cursor as u64).to_le_bytes().to_vec()))
        }

        fn restore_encoded_checkpoint_offset(&mut self, encoded: &[u8]) -> ConnectorResult<()> {
            if encoded.len() != 8 {
                return Err(krishiv_connectors::ConnectorError::Offset {
                    message: format!("expected 8 encoded cursor bytes, got {}", encoded.len()),
                });
            }
            let mut raw = [0_u8; 8];
            raw.copy_from_slice(encoded);
            self.cursor = usize::try_from(u64::from_le_bytes(raw)).map_err(|_| {
                krishiv_connectors::ConnectorError::Offset {
                    message: "encoded cursor exceeds usize".into(),
                }
            })?;
            self.yielded_before_idle = false;
            Ok(())
        }
    }

    let first =
        krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["a"], vec![100_i64]).unwrap();
    let second =
        krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["a"], vec![12_000_i64]).unwrap();
    let open_count = Arc::new(AtomicUsize::new(0));
    let mut registry = ConnectorRegistry::new();
    registry.register_source(Arc::new(OneBatchPerCycleDriver {
        batches: Arc::new(vec![first, second]),
        open_count: Arc::clone(&open_count),
    }));

    let job = "registry-persistent-job";
    let window_spec = "stream:tw:key=key:time=ts:win=10000:lag=0:agg=count";
    let fragment = format!("stream:loop:{job}|{window_spec}");
    let runner =
        ExecutorTaskRunner::new(ExecutorAssignmentInbox::new()).with_connector_registry(registry);
    let assignment =
        make_loop_assignment(&fragment).with_input_partitions(vec![InputPartition::new(
            "events-partition-0",
            "registry-connector:csv:events:{}",
        )]);

    let first_cycle = runner
        .execute_streaming_fragment(&assignment)
        .await
        .expect("first registry stream loop cycle");
    assert_eq!(first_cycle.row_count(), 0);

    let second_cycle = runner
        .execute_streaming_fragment(&assignment)
        .await
        .expect("second registry stream loop cycle");
    assert!(
        second_cycle.row_count() > 0,
        "second cycle must continue the same source cursor and close the first window"
    );
    assert_eq!(
        open_count.load(Ordering::SeqCst),
        1,
        "stream-loop registry source must be retained across cycles"
    );

    let task_id = TaskId::try_new("task-1").unwrap();
    let checkpoint_runner = runner.checkpoint_runners.get(&task_id).unwrap();
    let checkpoint_runner = checkpoint_runner.lock().unwrap();
    assert_eq!(
        checkpoint_runner.source_offsets[0].encoded_offset,
        2_u64.to_le_bytes().to_vec()
    );
}

#[tokio::test]
async fn stream_loop_registry_source_offset_contract_is_deployment_mode_invariant() {
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use krishiv_connectors::{
        ConnectorCapabilities, ConnectorConfig, ConnectorResult, DynSource, Source,
        registry::{
            ConnectorDescriptor, ConnectorKind, ConnectorRegistry, ConnectorRole, OpenSourceFuture,
            SourceDriver,
        },
    };

    #[derive(Clone)]
    struct ModeSourceDriver {
        batches: Arc<Vec<RecordBatch>>,
    }

    impl SourceDriver for ModeSourceDriver {
        fn descriptor(&self) -> ConnectorDescriptor {
            ConnectorDescriptor::new(
                ConnectorKind::Csv,
                ConnectorRole::Source,
                ConnectorCapabilities::new()
                    .with_bounded()
                    .with_rewindable()
                    .with_checkpoint(),
            )
        }

        fn validate(&self, _config: &ConnectorConfig) -> ConnectorResult<()> {
            Ok(())
        }

        fn open<'a>(&'a self, _config: &'a ConnectorConfig) -> OpenSourceFuture<'a> {
            let batches = Arc::clone(&self.batches);
            Box::pin(async move {
                Ok(Box::new(ModeSource {
                    schema: batches
                        .first()
                        .map(|batch| batch.schema())
                        .unwrap_or_else(|| Arc::new(Schema::empty())),
                    batches,
                    cursor: 0,
                }) as Box<dyn DynSource>)
            })
        }
    }

    struct ModeSource {
        schema: arrow::datatypes::SchemaRef,
        batches: Arc<Vec<RecordBatch>>,
        cursor: usize,
    }

    impl Source for ModeSource {
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::new()
                .with_bounded()
                .with_rewindable()
                .with_checkpoint()
        }

        fn source_schema(&self) -> Option<arrow::datatypes::SchemaRef> {
            Some(self.schema.clone())
        }

        async fn read_batch(&mut self) -> ConnectorResult<Option<RecordBatch>> {
            let batch = self.batches.get(self.cursor).cloned();
            if batch.is_some() {
                self.cursor = self.cursor.saturating_add(1);
            }
            Ok(batch)
        }

        fn current_offset(&self) -> Option<Box<dyn std::any::Any + Send>> {
            Some(Box::new(self.cursor))
        }

        fn encoded_checkpoint_offset(&self) -> ConnectorResult<Option<Vec<u8>>> {
            Ok(Some((self.cursor as u64).to_le_bytes().to_vec()))
        }

        fn restore_encoded_checkpoint_offset(&mut self, encoded: &[u8]) -> ConnectorResult<()> {
            if encoded.len() != 8 {
                return Err(krishiv_connectors::ConnectorError::Offset {
                    message: format!("expected 8 encoded cursor bytes, got {}", encoded.len()),
                });
            }
            let mut raw = [0_u8; 8];
            raw.copy_from_slice(encoded);
            self.cursor = usize::try_from(u64::from_le_bytes(raw)).map_err(|_| {
                krishiv_connectors::ConnectorError::Offset {
                    message: "encoded cursor exceeds usize".into(),
                }
            })?;
            Ok(())
        }
    }

    let profiles = [
        krishiv_common::DurabilityProfile::DevLocal,
        krishiv_common::DurabilityProfile::SingleNodeDurable,
        krishiv_common::DurabilityProfile::DistributedDurable,
    ];

    for profile in profiles {
        let first = krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["a"], vec![100_i64])
            .unwrap();
        let second =
            krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["a"], vec![12_000_i64])
                .unwrap();
        let mut registry = ConnectorRegistry::new();
        registry.register_source(Arc::new(ModeSourceDriver {
            batches: Arc::new(vec![first, second]),
        }));

        let state_dir = tempfile::tempdir().unwrap();
        let mut runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
            .with_connector_registry(registry);
        if profile != krishiv_common::DurabilityProfile::DevLocal {
            runner = runner.with_state_dir(state_dir.path().join(profile.as_str()));
        }

        let job = format!("mode-{}", profile.as_str());
        let partition_id = format!("events-{}-0", profile.as_str());
        let window_spec = "stream:tw:key=key:time=ts:win=10000:lag=0:agg=count";
        let fragment = format!("stream:loop:{job}|{window_spec}");
        runner.source_restore_offsets.insert(
            job.clone(),
            vec![crate::runner::RestoredSourceOffset {
                partition_id: partition_id.clone(),
                source_id: Some("events".into()),
                legacy_offset: 1,
                encoded_offset: 1_u64.to_le_bytes().to_vec(),
            }],
        );
        let assignment =
            make_loop_assignment(&fragment).with_input_partitions(vec![InputPartition::new(
                partition_id.clone(),
                "registry-connector:csv:events:{}",
            )]);

        let output = runner
            .execute_streaming_fragment(&assignment)
            .await
            .unwrap_or_else(|error| {
                panic!("{profile} registry stream-loop restore failed: {error}")
            });

        assert_eq!(
            output.row_count(),
            0,
            "{profile} must restore the connector cursor before the first post-restore read"
        );
        assert_eq!(
            runner.continuous_connector_sources.len(),
            1,
            "{profile} must retain the opened connector source for later cycles"
        );
        let checkpoint_runner = runner
            .checkpoint_runners
            .get(&TaskId::try_new("task-1").unwrap())
            .unwrap();
        let checkpoint_runner = checkpoint_runner.lock().unwrap();
        assert_eq!(
            checkpoint_runner.source_offsets.len(),
            1,
            "{profile} must stage exactly one source offset"
        );
        assert_eq!(
            checkpoint_runner.source_offsets[0].partition_id.as_str(),
            partition_id,
            "{profile} must preserve partition identity in checkpoint metadata"
        );
        assert_eq!(checkpoint_runner.source_offsets[0].offset, 2);
        assert_eq!(
            checkpoint_runner.source_offsets[0].encoded_offset,
            2_u64.to_le_bytes().to_vec(),
            "{profile} must stage the next encoded connector offset"
        );
    }
}

#[tokio::test]
async fn legacy_stream_continuous_fragment_is_rejected() {
    let fragment = "stream:continuous:legacy-job";
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let batch =
        krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["a"], vec![100_i64]).unwrap();
    let assignment = make_loop_assignment_with_batches(fragment, &[batch]);

    let error = runner
        .execute_streaming_fragment(&assignment)
        .await
        .expect_err("legacy fragment must not echo unprocessed input");

    assert!(
        matches!(error, crate::ExecutorError::InvalidAssignment { .. }),
        "legacy fragment must fail as an invalid assignment: {error}"
    );
}

// ── R7.2 source throttle wiring ──────────────────────────────────────────

/// Verify that the `SourceThrottleTable` on `ExecutorTaskRunner` is shared
/// across runner clones (simulating the heartbeat loop writing, runner tasks
/// reading) and that `apply` + `active_limit` behave correctly.
#[test]
fn source_throttle_table_shared_across_runner_clones() {
    use krishiv_proto::HeartbeatThrottleCommand;

    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let clone = runner.clone();

    // Simulate what the heartbeat loop does when it receives ThrottleDecisions.
    let cmds = vec![
        HeartbeatThrottleCommand {
            source_id: "src-kafka-0".into(),
            rows_per_second: Some(1000),
        },
        HeartbeatThrottleCommand {
            source_id: "src-kafka-1".into(),
            rows_per_second: None, // clear / unlimited
        },
    ];
    for tc in &cmds {
        runner
            .source_throttle_limits
            .apply(&tc.source_id, tc.rows_per_second);
    }

    // Reads through the clone (task runner side) must observe the updates.
    assert_eq!(
        clone.source_throttle_limits.active_limit("src-kafka-0"),
        Some(1000)
    );
    assert_eq!(
        clone.source_throttle_limits.active_limit("src-kafka-1"),
        None
    );
    assert_eq!(
        clone.source_throttle_limits.active_limit("src-kafka-99"),
        None
    );

    // check_and_log should not panic regardless of whether a limit is active.
    clone.source_throttle_limits.check_and_log("src-kafka-0");
    clone.source_throttle_limits.check_and_log("src-kafka-1");
    clone.source_throttle_limits.check_and_log("src-kafka-99");
}
