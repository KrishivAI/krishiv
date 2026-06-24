// ── Phase 3: executor-side recovery (restore, 2PC lifecycle) ─────────────

mod phase3_recovery {
    use super::*;
    use std::sync::Mutex;

    use krishiv_connectors::{EpochTransactionLog, LocalParquetTwoPhaseCommitSink};
    use krishiv_proto::{CheckpointCompleteCommand, RestoreFromCheckpointCommand};
    use krishiv_state::checkpoint::{
        CheckpointMetadata, CheckpointStorage, IntegrityManifest, LocalFsCheckpointStorage,
        OperatorSnapshotRef, SourceOffsetRecord, write_epoch_hint, write_epoch_metadata,
        write_manifest,
    };

    use crate::runner::{CheckpointStateHandle, kafka_offsets_from_source_records};

    #[derive(Default, Clone)]
    struct P3RecordingCoordinator {
        acks: Arc<Mutex<Vec<CheckpointAckRequest>>>,
    }

    #[tonic::async_trait]
    impl CoordinatorExecutorService for P3RecordingCoordinator {
        async fn register_executor(
            &self,
            request: tonic::Request<RegisterExecutorRequest>,
        ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
            let request = request.into_inner();
            Ok(tonic::Response::new(RegisterExecutorResponse::new(
                request.descriptor().executor_id().clone(),
                LeaseGeneration::initial(),
                TransportDisposition::Accepted,
            )))
        }

        async fn deregister_executor(
            &self,
            _request: tonic::Request<DeregisterExecutorRequest>,
        ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn executor_heartbeat(
            &self,
            request: tonic::Request<ExecutorHeartbeatRequest>,
        ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status> {
            Ok(tonic::Response::new(ExecutorHeartbeatResponse::new(
                request.into_inner().lease_generation(),
                TransportDisposition::Accepted,
            )))
        }

        async fn task_status(
            &self,
            _request: tonic::Request<TaskStatusRequest>,
        ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
            Ok(tonic::Response::new(TaskStatusResponse::new(
                TransportDisposition::Accepted,
            )))
        }

        async fn checkpoint_ack(
            &self,
            request: tonic::Request<CheckpointAckRequest>,
        ) -> Result<tonic::Response<CheckpointAckResponse>, tonic::Status> {
            self.acks.lock().unwrap().push(request.into_inner());
            Ok(tonic::Response::new(CheckpointAckResponse::Accepted))
        }
    }

    /// Seal a checkpoint epoch in storage exactly the way the coordinator
    /// commit does: metadata + integrity manifest + epoch hint (last).
    fn seal_epoch_from_acks(
        storage: &dyn CheckpointStorage,
        job_id: &str,
        epoch: u64,
        acks: &[CheckpointAckRequest],
    ) {
        let operator_snapshots: Vec<OperatorSnapshotRef> = acks
            .iter()
            .filter_map(|ack| {
                ack.snapshot_path.as_ref().map(|path| OperatorSnapshotRef {
                    operator_id: ack.operator_id.as_str().to_owned(),
                    task_id: ack.task_id.as_str().to_owned(),
                    snapshot_path: path.clone(),
                })
            })
            .collect();
        let source_offsets: Vec<SourceOffsetRecord> = acks
            .iter()
            .flat_map(|ack| ack.source_offsets.iter())
            .map(|so| SourceOffsetRecord {
                partition_id: so.partition_id.as_str().to_owned(),
                offset: so.offset,
            })
            .collect();
        let metadata = CheckpointMetadata {
            version: CheckpointMetadata::VERSION,
            epoch,
            job_id: job_id.to_owned(),
            fencing_token: 1,
            coordinator_id: Some("p3-test-coordinator".into()),
            timestamp_ms: 1,
            source_offsets,
            operator_snapshots: operator_snapshots.clone(),
            is_savepoint: false,
            savepoint_label: None,
            iceberg_snapshot_id: None,
            kafka_offsets: None,
        };
        let mut manifest = IntegrityManifest::new();
        manifest.insert_bytes(
            "metadata.json",
            &serde_json::to_vec_pretty(&metadata).unwrap(),
        );
        for snap in &operator_snapshots {
            let bytes = storage.read_bytes(&snap.snapshot_path).unwrap().unwrap();
            manifest.insert_bytes(
                &format!("{}/{}/state.bin", snap.operator_id, snap.task_id),
                &bytes,
            );
        }
        write_epoch_metadata(storage, job_id, epoch, &metadata).unwrap();
        write_manifest(storage, job_id, epoch, &manifest).unwrap();
        write_epoch_hint(storage, job_id, epoch).unwrap();
    }

    fn runner_with_running_task(
        job: &str,
        task: &str,
    ) -> (ExecutorTaskRunner, ExecutorTaskAssignment) {
        let job_id = JobId::try_new(job).unwrap();
        let task_id = TaskId::try_new(task).unwrap();
        let attempt = TaskAttemptRef::new(
            job_id.clone(),
            StageId::try_new("stage-p3").unwrap(),
            task_id.clone(),
            AttemptId::initial(),
        );
        let running = Arc::new(dashmap::DashMap::new());
        running.insert(task_id.as_str().to_string(), attempt.clone());
        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
            .with_executor_id(ExecutorId::try_new("exec-p3").unwrap())
            .with_running_attempts(running);
        let assignment = ExecutorTaskAssignment::new(
            attempt,
            ExecutorId::try_new("exec-p3").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("checkpoint"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "checkpoint"),
        );
        (runner, assignment)
    }

    fn restore_command(job: &str, epoch: u64) -> RestoreFromCheckpointCommand {
        RestoreFromCheckpointCommand {
            job_id: JobId::try_new(job).unwrap(),
            epoch,
            fencing_token: FencingToken::initial(),
        }
    }

    #[test]
    fn kafka_offsets_from_source_records_parses_dashed_topics() {
        let records = vec![
            SourceOffsetRecord {
                partition_id: "kafka-orders-eu-west-3".into(),
                offset: 17,
            },
            SourceOffsetRecord {
                partition_id: "kafka-events-0".into(),
                offset: 5,
            },
            SourceOffsetRecord {
                partition_id: "not-kafka".into(),
                offset: 99,
            },
        ];
        let offsets = kafka_offsets_from_source_records(&records);
        assert_eq!(offsets.len(), 2);
        assert_eq!(offsets[0].topic, "events");
        assert_eq!(offsets[0].partition, 0);
        assert_eq!(offsets[0].offset, 5);
        assert_eq!(offsets[1].topic, "orders-eu-west");
        assert_eq!(offsets[1].partition, 3);
        assert_eq!(offsets[1].offset, 17);
    }

    /// The flagship Phase 3 cycle: a stateful window job accumulates,
    /// checkpoints REAL operator state through the barrier path, the
    /// process "dies", a fresh runner restores from the checkpoint, and
    /// the window closes with the pre-kill accumulations intact.
    #[tokio::test]
    async fn full_checkpoint_kill_restore_cycle_preserves_window_state() {
        let job = "p3-cycle";
        let spec = "stream:tw:key=key:time=ts:win=10000:lag=0:agg=count";
        let fragment = format!("stream:loop:{job}|{spec}");
        let storage: Arc<dyn CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());

        // ── Process 1: accumulate two events, checkpoint, then "die". ──
        let (runner1, _assignment) = runner_with_running_task(job, "task-p3-cycle");
        runner1.continuous_inputs.insert(
            job.to_owned(),
            vec![krishiv_common::test_fixtures::make_test_key_ts_batch(
                vec!["a", "a"],
                vec![500_i64, 900_i64],
            )],
        );
        let out = runner1
            .execute_streaming_fragment(&make_loop_assignment(&fragment))
            .await
            .unwrap();
        assert_eq!(out.row_count(), 0, "window still open before watermark");

        let coordinator = P3RecordingCoordinator::default();
        let fallback = CheckpointStateHandle::from_backend(
            krishiv_state::RocksDbStateBackend::ephemeral().unwrap(),
        );
        runner1
            .initiate_checkpoint_for_job(
                &InitiateCheckpointRequest {
                    job_id: JobId::try_new(job).unwrap(),
                    epoch: 1,
                    fencing_token: FencingToken::initial(),
                },
                fallback,
                Arc::clone(&storage),
                coordinator.clone(),
            )
            .await
            .unwrap();
        let acks = coordinator.acks.lock().unwrap().clone();
        assert_eq!(acks.len(), 1);
        assert!(
            acks[0].snapshot_path.is_some(),
            "checkpoint of a continuous window job must carry REAL operator \
             state, not the vacuous generic backend"
        );
        seal_epoch_from_acks(storage.as_ref(), job, 1, &acks);
        drop(runner1); // the process dies; loop executor state is gone

        // ── Process 2: fresh runner restores, then the window closes. ──
        let (runner2, _assignment) = runner_with_running_task(job, "task-p3-cycle-2");
        let fallback2 = CheckpointStateHandle::from_backend(
            krishiv_state::RocksDbStateBackend::ephemeral().unwrap(),
        );
        runner2
            .restore_job_from_checkpoint(&restore_command(job, 1), &fallback2, storage.as_ref())
            .unwrap();

        // The watermark-advancing event closes window [0, 10000).
        runner2.continuous_inputs.insert(
            job.to_owned(),
            vec![krishiv_common::test_fixtures::make_test_key_ts_batch(
                vec!["a"],
                vec![15_000_i64],
            )],
        );
        let out = runner2
            .execute_streaming_fragment(&make_loop_assignment(&fragment))
            .await
            .unwrap();
        assert!(out.row_count() > 0, "restored window must close");
        // The count aggregate must include BOTH pre-kill events.
        let batch = &out.record_batches()[0];
        let count_col = batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count aggregate column");
        assert_eq!(
            count_col.value(0),
            2,
            "window count must include the two events accumulated before the kill"
        );
    }

    /// A restore command rolls a LIVE loop executor back: state diverging
    /// after the checkpoint is discarded (global rollback semantics).
    #[tokio::test]
    async fn restore_command_rolls_back_live_loop_executor_state() {
        let job = "p3-rollback";
        let spec = "stream:tw:key=key:time=ts:win=10000:lag=0:agg=count";
        let fragment = format!("stream:loop:{job}|{spec}");
        let storage: Arc<dyn CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());

        let (runner, _assignment) = runner_with_running_task(job, "task-p3-rollback");

        // One event, then checkpoint epoch 1.
        runner.continuous_inputs.insert(
            job.to_owned(),
            vec![krishiv_common::test_fixtures::make_test_key_ts_batch(
                vec!["a"],
                vec![500_i64],
            )],
        );
        runner
            .execute_streaming_fragment(&make_loop_assignment(&fragment))
            .await
            .unwrap();
        let coordinator = P3RecordingCoordinator::default();
        runner
            .initiate_checkpoint_for_job(
                &InitiateCheckpointRequest {
                    job_id: JobId::try_new(job).unwrap(),
                    epoch: 1,
                    fencing_token: FencingToken::initial(),
                },
                CheckpointStateHandle::from_backend(
                    krishiv_state::RocksDbStateBackend::ephemeral().unwrap(),
                ),
                Arc::clone(&storage),
                coordinator.clone(),
            )
            .await
            .unwrap();
        seal_epoch_from_acks(
            storage.as_ref(),
            job,
            1,
            &coordinator.acks.lock().unwrap().clone(),
        );

        // Post-checkpoint divergence: a second event lands in the window.
        runner.continuous_inputs.insert(
            job.to_owned(),
            vec![krishiv_common::test_fixtures::make_test_key_ts_batch(
                vec!["a"],
                vec![900_i64],
            )],
        );
        runner
            .execute_streaming_fragment(&make_loop_assignment(&fragment))
            .await
            .unwrap();

        // Global rollback to epoch 1 discards the divergent event.
        let fallback = CheckpointStateHandle::from_backend(
            krishiv_state::RocksDbStateBackend::ephemeral().unwrap(),
        );
        runner
            .restore_job_from_checkpoint(&restore_command(job, 1), &fallback, storage.as_ref())
            .unwrap();

        runner.continuous_inputs.insert(
            job.to_owned(),
            vec![krishiv_common::test_fixtures::make_test_key_ts_batch(
                vec!["a"],
                vec![15_000_i64],
            )],
        );
        let out = runner
            .execute_streaming_fragment(&make_loop_assignment(&fragment))
            .await
            .unwrap();
        let batch = &out.record_batches()[0];
        let count_col = batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            count_col.value(0),
            1,
            "rolled-back window must contain only the checkpointed event; \
             the rewound source re-delivers the rest"
        );
    }

    /// Restore seeds the Kafka restore table and resets per-task epochs.
    #[test]
    fn restore_command_seeds_kafka_offsets_and_resets_task_epochs() {
        let job = "p3-kafka-seed";
        let storage: Arc<dyn CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let (runner, _assignment) = runner_with_running_task(job, "task-p3-kafka");

        // A pre-existing task runner that acked epochs past the restore point.
        let task_id = TaskId::try_new("task-p3-kafka").unwrap();
        let mut stale = TaskRunner::new(task_id.clone());
        stale.last_acked_epoch = 9;
        runner
            .checkpoint_runners
            .insert(task_id.clone(), Arc::new(Mutex::new(stale)));

        // Seal epoch 2 with Kafka offsets and no operator snapshots.
        let ack = CheckpointAckRequest {
            job_id: JobId::try_new(job).unwrap(),
            operator_id: krishiv_proto::OperatorId::try_new("operator-task-p3-kafka").unwrap(),
            task_id: task_id.clone(),
            epoch: 2,
            fencing_token: FencingToken::initial(),
            source_offsets: vec![krishiv_proto::CheckpointSourceOffset {
                partition_id: krishiv_proto::PartitionId::try_new("kafka-events-1").unwrap(),
                offset: 77,
            }],
            snapshot_path: None,
        };
        seal_epoch_from_acks(storage.as_ref(), job, 2, &[ack]);

        let fallback = CheckpointStateHandle::from_backend(
            krishiv_state::RocksDbStateBackend::ephemeral().unwrap(),
        );
        runner
            .restore_job_from_checkpoint(&restore_command(job, 2), &fallback, storage.as_ref())
            .unwrap();

        let restored = runner.kafka_restore_offsets.get(job).unwrap();
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].topic, "events");
        assert_eq!(restored[0].partition, 1);
        assert_eq!(restored[0].offset, 77);

        let task_runner_arc = runner.checkpoint_runners.get(&task_id).unwrap();
        assert_eq!(
            task_runner_arc.lock().unwrap().last_acked_epoch,
            2,
            "task epoch must reset to the restored epoch so pre-rollback \
             barriers are rejected as stale"
        );
    }

    /// Barrier pre-commits transactional sinks before acking; the
    /// completion command makes the staged output visible.
    #[tokio::test]
    async fn barrier_pre_commits_and_complete_command_publishes_sink_output() {
        let job = "p3-2pc";
        let storage: Arc<dyn CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let sink_dir = tempdir().unwrap();
        let (runner, _assignment) = runner_with_running_task(job, "task-p3-2pc");

        let participant = runner
            .transaction_log
            .get_or_register(job, || {
                Ok(EpochTransactionLog::new(
                    LocalParquetTwoPhaseCommitSink::new(sink_dir.path()),
                ))
            })
            .unwrap();
        let batch =
            krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["a"], vec![500_i64]);
        participant.lock().unwrap().stage(&batch).unwrap();

        let coordinator = P3RecordingCoordinator::default();
        runner
            .initiate_checkpoint_for_job(
                &InitiateCheckpointRequest {
                    job_id: JobId::try_new(job).unwrap(),
                    epoch: 1,
                    fencing_token: FencingToken::initial(),
                },
                CheckpointStateHandle::from_backend(
                    krishiv_state::RocksDbStateBackend::ephemeral().unwrap(),
                ),
                Arc::clone(&storage),
                coordinator.clone(),
            )
            .await
            .unwrap();

        // Barrier staged the open buffer under epoch 1 BEFORE the ack…
        assert_eq!(coordinator.acks.lock().unwrap().len(), 1);
        assert_eq!(participant.lock().unwrap().prepared_epochs(), vec![1]);
        let files: Vec<String> = std::fs::read_dir(sink_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(files.iter().all(|f| f.ends_with(".parquet.tmp")));

        // …and the completion command publishes it.
        runner
            .handle_checkpoint_complete(&CheckpointCompleteCommand {
                job_id: JobId::try_new(job).unwrap(),
                epoch: 1,
                fencing_token: FencingToken::initial(),
            })
            .await;
        assert!(participant.lock().unwrap().prepared_epochs().is_empty());
        let files: Vec<String> = std::fs::read_dir(sink_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(files.len(), 1);
        assert!(
            files[0].ends_with(".parquet"),
            "completed checkpoint must publish the staged file, found {files:?}"
        );
    }

    /// Restore reconciles the transaction log: prepared output covered by
    /// the restored checkpoint commits, later output aborts.
    #[test]
    fn restore_command_reconciles_transaction_log() {
        let job = "p3-2pc-restore";
        let storage: Arc<dyn CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
        let sink_dir = tempdir().unwrap();
        let (runner, _assignment) = runner_with_running_task(job, "task-p3-2pc-restore");

        let participant = runner
            .transaction_log
            .get_or_register(job, || {
                Ok(EpochTransactionLog::new(
                    LocalParquetTwoPhaseCommitSink::new(sink_dir.path()),
                ))
            })
            .unwrap();
        let batch =
            krishiv_common::test_fixtures::make_test_key_ts_batch(vec!["a"], vec![500_i64]);
        {
            let mut guard = participant.lock().unwrap();
            guard.stage(&batch).unwrap();
            guard.pre_commit(1).unwrap(); // covered by the restored checkpoint
            guard.stage(&batch).unwrap();
            guard.pre_commit(2).unwrap(); // after the restored checkpoint
            guard.stage(&batch).unwrap(); // open, never prepared
        }

        let ack = CheckpointAckRequest {
            job_id: JobId::try_new(job).unwrap(),
            operator_id: krishiv_proto::OperatorId::try_new("operator-task-p3-2pc-restore")
                .unwrap(),
            task_id: TaskId::try_new("task-p3-2pc-restore").unwrap(),
            epoch: 1,
            fencing_token: FencingToken::initial(),
            source_offsets: vec![],
            snapshot_path: None,
        };
        seal_epoch_from_acks(storage.as_ref(), job, 1, &[ack]);

        let fallback = CheckpointStateHandle::from_backend(
            krishiv_state::RocksDbStateBackend::ephemeral().unwrap(),
        );
        runner
            .restore_job_from_checkpoint(&restore_command(job, 1), &fallback, storage.as_ref())
            .unwrap();

        let guard = participant.lock().unwrap();
        assert!(guard.prepared_epochs().is_empty());
        assert_eq!(guard.open_rows(), 0, "open buffer discarded on restore");
        drop(guard);
        let mut finals = 0;
        let mut staged = 0;
        for entry in std::fs::read_dir(sink_dir.path()).unwrap() {
            let name = entry.unwrap().file_name().to_string_lossy().into_owned();
            if name.ends_with(".parquet.tmp") {
                staged += 1;
            } else if name.ends_with(".parquet") {
                finals += 1;
            }
        }
        assert_eq!(finals, 1, "epoch-1 output committed (recover-and-commit)");
        assert_eq!(staged, 0, "epoch-2 output aborted (recover-and-abort)");
    }

    /// Repeated failure loop: every iteration feeds one batch, checkpoints
    /// real window state, kills the process, and restores a fresh one.
    /// The victim's emitted windows must match a control executor that
    /// never dies — across every iteration, not just one failure.
    async fn run_kill_restore_soak(iterations: usize) {
        use std::collections::HashMap;

        let job = "p3-soak";
        let spec_str = "stream:tw:key=key:time=ts:win=10000:lag=0:agg=count";
        let fragment = format!("stream:loop:{job}|{spec_str}");
        let storage: Arc<dyn CheckpointStorage> =
            Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());

        // Control: one executor fed the identical event sequence.
        let control_spec = krishiv_plan::window::WindowExecutionSpec {
            watermark_lag_ms: 0,
            ..krishiv_plan::window::WindowExecutionSpec::tumbling("key", "ts", 10_000)
        };
        let mut control =
            krishiv_dataflow::ContinuousWindowExecutor::new(control_spec).unwrap();

        let collect = |batches: &[RecordBatch], into: &mut HashMap<(String, i64), i64>| {
            for batch in batches {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let starts = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let counts = batch
                    .column(batch.num_columns() - 1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    *into
                        .entry((keys.value(row).to_owned(), starts.value(row)))
                        .or_default() += counts.value(row);
                }
            }
        };

        let mut control_emitted: HashMap<(String, i64), i64> = HashMap::new();
        let mut victim_emitted: HashMap<(String, i64), i64> = HashMap::new();
        let mut last_sealed_epoch: Option<u64> = None;

        for i in 0..iterations {
            let key = format!("k{}", i % 8);
            let ts = (i as i64) * 1_000;
            let batch = krishiv_common::test_fixtures::make_test_key_ts_batch(
                vec![key.as_str()],
                vec![ts],
            );

            // Control path.
            let out = control.drain(vec![batch.clone()]).unwrap();
            collect(&out, &mut control_emitted);

            // Victim path: a brand-new "process" every iteration.
            let (runner, _assignment) =
                runner_with_running_task(job, &format!("task-soak-{i}"));
            if let Some(epoch) = last_sealed_epoch {
                let fallback = CheckpointStateHandle::from_backend(
                    krishiv_state::RocksDbStateBackend::ephemeral().unwrap(),
                );
                runner
                    .restore_job_from_checkpoint(
                        &restore_command(job, epoch),
                        &fallback,
                        storage.as_ref(),
                    )
                    .unwrap_or_else(|e| panic!("restore at iteration {i} failed: {e}"));
            }
            runner.continuous_inputs.insert(job.to_owned(), vec![batch]);
            let out = runner
                .execute_streaming_fragment(&make_loop_assignment(&fragment))
                .await
                .unwrap_or_else(|e| panic!("drain at iteration {i} failed: {e}"));
            collect(out.record_batches(), &mut victim_emitted);

            // Checkpoint this iteration's state, sealing a new epoch.
            let epoch = (i as u64) + 1;
            let coordinator = P3RecordingCoordinator::default();
            runner
                .initiate_checkpoint_for_job(
                    &InitiateCheckpointRequest {
                        job_id: JobId::try_new(job).unwrap(),
                        epoch,
                        fencing_token: FencingToken::initial(),
                    },
                    CheckpointStateHandle::from_backend(
                        krishiv_state::RocksDbStateBackend::ephemeral().unwrap(),
                    ),
                    Arc::clone(&storage),
                    coordinator.clone(),
                )
                .await
                .unwrap_or_else(|e| panic!("checkpoint at iteration {i} failed: {e}"));
            seal_epoch_from_acks(
                storage.as_ref(),
                job,
                epoch,
                &coordinator.acks.lock().unwrap().clone(),
            );
            last_sealed_epoch = Some(epoch);
            // The process dies here: runner (and its loop executor) drop.
        }

        assert_eq!(
            victim_emitted, control_emitted,
            "after {iterations} kill/restore cycles the emitted windows must                  match an executor that never died"
        );
        assert!(
            !control_emitted.is_empty(),
            "soak must actually close windows to be meaningful"
        );
    }

    /// CI-sized soak: 25 kill/restore cycles.
    #[tokio::test]
    async fn soak_repeated_kill_restore_preserves_aggregates() {
        run_kill_restore_soak(25).await;
    }

    /// Long soak (run explicitly with `cargo test -- --ignored`).
    #[tokio::test]
    #[ignore = "long-running soak; exercises 300 kill/restore cycles"]
    async fn soak_repeated_kill_restore_long() {
        run_kill_restore_soak(300).await;
    }
}
