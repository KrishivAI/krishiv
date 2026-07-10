#[cfg(test)]
mod proto_tests {
    use crate::*;

    #[test]
    fn ids_reject_empty_values() {
        let error = JobId::try_new("   ").unwrap_err();

        assert_eq!(error.kind(), "job id");
    }

    #[test]
    fn numeric_ids_reject_zero_values() {
        let error = AttemptId::try_new(0).unwrap_err();

        assert_eq!(error.kind(), "attempt id");
        assert_eq!(error.reason(), "must be greater than zero");
        assert_eq!(AttemptId::initial().next().as_u32(), 2);
        assert_eq!(LeaseGeneration::initial().next().as_u64(), 2);
    }

    #[test]
    fn connector_capability_flags_default_all_false() {
        let flags = ConnectorCapabilityFlags::default();
        assert!(!flags.bounded);
        assert!(!flags.unbounded);
        assert!(!flags.rewindable);
        assert!(!flags.transactional);
        assert!(!flags.idempotent);
    }

    #[test]
    fn task_spec_with_connector_capabilities() {
        let source_caps = ConnectorCapabilityFlags {
            bounded: true,
            rewindable: true,
            ..Default::default()
        };
        let sink_caps = ConnectorCapabilityFlags {
            idempotent: true,
            bounded: true,
            ..Default::default()
        };
        let task = TaskSpec::new(TaskId::try_new("task-caps-1").unwrap(), "parquet scan")
            .with_source_capabilities(source_caps.clone())
            .with_sink_capabilities(sink_caps.clone());
        assert_eq!(task.source_capabilities.as_ref(), Some(&source_caps));
        assert_eq!(task.sink_capabilities.as_ref(), Some(&sink_caps));
    }

    #[test]
    fn job_spec_counts_stage_tasks() {
        let job = JobSpec::new(JobId::try_new("job-1").unwrap(), "demo", JobKind::Batch)
            .with_stage(
                StageSpec::new(StageId::try_new("stage-1").unwrap(), "scan")
                    .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "scan a"))
                    .with_task(TaskSpec::new(TaskId::try_new("task-2").unwrap(), "scan b")),
            );

        assert_eq!(job.task_count(), 2);
        assert_eq!(job.kind(), JobKind::Batch);
    }

    #[test]
    fn lifecycle_states_expose_terminal_and_capacity_rules() {
        assert!(JobState::Succeeded.is_terminal());
        assert!(TaskState::Failed.is_terminal());
        assert!(ExecutorState::Healthy.can_accept_work());
        assert!(!ExecutorState::Lost.can_accept_work());
        assert_eq!(ExecutorId::try_new("exec-1").unwrap().as_str(), "exec-1");
    }

    #[test]
    fn transport_version_exposes_compatibility() {
        let current = TransportVersion::CURRENT;

        assert_eq!(current.to_string(), "3.1");
        assert!(current.is_compatible_with(TransportVersion::R3_1));
        assert!(!TransportVersion::new(4, 0).is_compatible_with(current));
    }

    #[test]
    fn registration_request_carries_current_version() {
        let request = RegisterExecutorRequest::new(crate::ExecutorDescriptor::new(
            ExecutorId::try_new("exec-1").unwrap(),
            "pod-a",
            2,
        ));

        assert_eq!(request.version(), TransportVersion::CURRENT);
        assert_eq!(request.descriptor().slots(), 2);
    }

    #[test]
    fn heartbeat_request_carries_running_attempts_and_lease() {
        let attempt = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let heartbeat = ExecutorHeartbeatRequest::new(
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            ExecutorState::Healthy,
        )
        .with_running_attempts(vec![attempt]);

        assert_eq!(heartbeat.version(), TransportVersion::CURRENT);
        assert_eq!(heartbeat.lease_generation(), LeaseGeneration::initial());
        assert_eq!(
            heartbeat.running_attempts()[0].attempt_id(),
            AttemptId::initial()
        );
    }

    #[test]
    fn executor_task_assignment_carries_attempt_lease_and_contracts() {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let assignment = ExecutorTaskAssignment::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("scan parquet"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "return result"),
        )
        .with_input_partitions(vec![InputPartition::new("part-1", "first file split")])
        .with_key_group_range(KeyGroupRange::new(8192, 16383));

        assert_eq!(assignment.attempt_id(), AttemptId::initial());
        assert_eq!(assignment.lease_generation(), LeaseGeneration::initial());
        assert_eq!(assignment.input_partitions()[0].partition_id(), "part-1");
        assert_eq!(
            assignment.key_group_range(),
            KeyGroupRange::new(8192, 16383)
        );
        assert_eq!(
            assignment.output_contract().kind(),
            OutputContractKind::InlineRecordBatches
        );
    }

    #[test]
    fn executor_task_assignment_round_trips_through_wire_contract() {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let assignment = ExecutorTaskAssignment::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("scan parquet"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "return result"),
        )
        .with_input_partitions(vec![InputPartition::new("part-1", "first file split")])
        .with_key_group_range(KeyGroupRange::new(1024, 2047));

        let wire = crate::wire::executor_task_assignment_to_wire(assignment.clone()).unwrap();
        let round_trip = crate::wire::executor_task_assignment_from_wire(wire).unwrap();

        assert_eq!(round_trip, assignment);
    }

    #[test]
    fn iceberg_sink_descriptor_round_trips_through_wire_contract() {
        let descriptor = OutputContractDescriptor::IcebergSink {
            root: String::from("/var/lib/krishiv/tables/orders"),
            table: String::from("orders_live"),
            mode: crate::IcebergSinkMode::Upsert,
            key_columns: vec![String::from("window_start"), String::from("key")],
            op_column: Some(String::from("__op")),
        };
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-iceberg").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let assignment = ExecutorTaskAssignment::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("stream:loop:job-iceberg|stream:tw:..."),
            OutputContract::typed(OutputContractKind::Sink, descriptor.clone()),
        );

        let wire = crate::wire::executor_task_assignment_to_wire(assignment.clone()).unwrap();
        let round_trip = crate::wire::executor_task_assignment_from_wire(wire).unwrap();
        assert_eq!(round_trip, assignment);

        // The legacy description parses back to the same typed descriptor, so
        // string-form sink contracts (TaskSpec::with_sink_contract) stay
        // equivalent to the typed path.
        let reparsed = OutputContractDescriptor::parse_iceberg_sink(
            round_trip.output_contract().description(),
        )
        .expect("description carries the iceberg-sink prefix")
        .expect("well-formed contract");
        assert_eq!(reparsed, descriptor);
    }

    #[test]
    fn iceberg_sink_legacy_parse_rejects_malformed_contracts() {
        // Upsert without keys is invalid.
        assert!(
            OutputContractDescriptor::parse_iceberg_sink("iceberg-sink:/r|t|mode=upsert")
                .unwrap()
                .is_err()
        );
        // Missing mode is invalid.
        assert!(
            OutputContractDescriptor::parse_iceberg_sink("iceberg-sink:/r|t")
                .unwrap()
                .is_err()
        );
        // Non-iceberg contracts are None, not an error.
        assert!(
            OutputContractDescriptor::parse_iceberg_sink("object-parquet-sink:/a:/b").is_none()
        );
        // Append without keys is fine.
        let ok = OutputContractDescriptor::parse_iceberg_sink("iceberg-sink:/r|t|mode=append")
            .unwrap()
            .unwrap();
        assert!(matches!(
            ok,
            OutputContractDescriptor::IcebergSink {
                mode: crate::IcebergSinkMode::Append,
                ..
            }
        ));
    }

    #[test]
    fn executor_task_assignment_distinguishes_missing_and_zero_key_group_range() {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-kg-zero").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let assignment = ExecutorTaskAssignment::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("scan parquet"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "return result"),
        )
        .with_key_group_range(KeyGroupRange::new(0, 0));

        let wire = crate::wire::executor_task_assignment_to_wire(assignment.clone()).unwrap();
        let round_trip = crate::wire::executor_task_assignment_from_wire(wire.clone()).unwrap();
        assert_eq!(round_trip.key_group_range(), KeyGroupRange::new(0, 0));

        let mut legacy_wire = wire;
        legacy_wire.has_key_group_range = false;
        let legacy_round_trip =
            crate::wire::executor_task_assignment_from_wire(legacy_wire).unwrap();
        assert_eq!(legacy_round_trip.key_group_range(), KeyGroupRange::full());
    }

    #[test]
    fn typed_executor_task_assignment_round_trips_through_wire_contract() {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-typed").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let assignment = ExecutorTaskAssignment::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("connector-pipeline:kafka-to-parquet"),
            OutputContract::typed(
                OutputContractKind::Sink,
                OutputContractDescriptor::ParquetSink {
                    path: String::from("/tmp/out.parquet"),
                },
            ),
        )
        .with_input_partitions(vec![InputPartition::typed(
            "part-1",
            InputPartitionDescriptor::MemoryKafka {
                topic: String::from("events"),
                partition: 0,
                start_offset: 42,
                records: vec![MemoryKafkaRecord::new(7, "seven")],
            },
        )]);

        let wire = crate::wire::executor_task_assignment_to_wire(assignment.clone()).unwrap();
        let round_trip = crate::wire::executor_task_assignment_from_wire(wire).unwrap();

        assert_eq!(round_trip, assignment);
        assert!(matches!(
            round_trip.input_partitions()[0].descriptor(),
            Some(InputPartitionDescriptor::MemoryKafka { topic, .. }) if topic == "events"
        ));
        assert!(matches!(
            round_trip.output_contract().descriptor(),
            Some(OutputContractDescriptor::ParquetSink { path }) if path == "/tmp/out.parquet"
        ));
    }

    #[test]
    fn registration_descriptor_round_trips_task_endpoint() {
        let descriptor =
            ExecutorDescriptor::new(ExecutorId::try_new("exec-1").unwrap(), "pod-a", 2)
                .with_task_endpoint("http://127.0.0.1:9091");
        let request = RegisterExecutorRequest::new(descriptor.clone());

        let wire = crate::wire::register_executor_request_to_wire(request);
        let round_trip = crate::wire::register_executor_request_from_wire(wire).unwrap();

        assert_eq!(round_trip.descriptor(), &descriptor);
        assert_eq!(
            round_trip.descriptor().task_endpoint(),
            Some("http://127.0.0.1:9091")
        );
    }

    #[test]
    fn deregistration_round_trips_through_wire_contract() {
        let request = DeregisterExecutorRequest::new(
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::try_new(7).unwrap(),
        )
        .with_reason("shutdown");

        let wire = crate::wire::deregister_executor_request_to_wire(request.clone());
        let round_trip = crate::wire::deregister_executor_request_from_wire(wire).unwrap();

        assert_eq!(round_trip, request);
    }

    #[test]
    fn task_status_output_metadata_round_trips_through_wire_contract() {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let request = TaskStatusRequest::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            TaskState::Succeeded,
        )
        .with_output_metadata(TaskOutputMetadata::new("sql", 2, 1, 2));

        let wire = crate::wire::task_status_request_to_wire(request.clone());
        let round_trip = crate::wire::task_status_request_from_wire(wire).unwrap();

        assert_eq!(round_trip, request);
        assert_eq!(round_trip.output_metadata().unwrap().row_count(), 2);
    }

    /// G5: the continuous operator-state snapshot travels the wire intact —
    /// present when attached, absent (not an empty blob) when it wasn't.
    #[test]
    fn task_output_state_snapshot_round_trips_through_wire_contract() {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let meta = TaskOutputMetadata::new("streaming-window", 3, 1, 2)
            .with_watermark_ms(42_000)
            .with_state_snapshot(vec![1, 2, 3, 4]);
        let request = TaskStatusRequest::new(
            ids.clone(),
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            TaskState::Succeeded,
        )
        .with_output_metadata(meta);

        let wire = crate::wire::task_status_request_to_wire(request.clone());
        let round_trip = crate::wire::task_status_request_from_wire(wire).unwrap();
        assert_eq!(round_trip, request);
        let meta = round_trip.output_metadata().unwrap();
        assert_eq!(meta.state_snapshot(), Some(&[1u8, 2, 3, 4][..]));
        assert_eq!(meta.watermark_ms(), Some(42_000));

        // Without a snapshot the field stays absent through the round trip.
        let bare = TaskStatusRequest::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            TaskState::Succeeded,
        )
        .with_output_metadata(TaskOutputMetadata::new("streaming-window", 0, 0, 0));
        let wire = crate::wire::task_status_request_to_wire(bare);
        let round_trip = crate::wire::task_status_request_from_wire(wire).unwrap();
        assert_eq!(round_trip.output_metadata().unwrap().state_snapshot(), None);
    }

    #[test]
    fn task_cancellation_round_trips_through_wire_contract() {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let request = TaskCancellationRequest::new(ids).with_reason("user requested cancel");

        let wire = crate::wire::task_cancellation_request_to_wire(request.clone());
        let round_trip = crate::wire::task_cancellation_request_from_wire(wire).unwrap();

        assert_eq!(round_trip, request);
    }

    #[test]
    fn task_status_contract_can_report_stale_attempts() {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::try_new(3).unwrap(),
        );
        let request = TaskStatusRequest::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::try_new(7).unwrap(),
            TaskState::Succeeded,
        )
        .with_message("complete");
        let response = TaskStatusResponse::new(TransportDisposition::StaleAttempt)
            .with_message("newer attempt already owns this task");

        assert_eq!(request.attempt_id().as_u32(), 3);
        assert_eq!(request.lease_generation().as_u64(), 7);
        assert_eq!(request.message(), Some("complete"));
        assert_eq!(response.disposition(), TransportDisposition::StaleAttempt);
        assert_eq!(
            response.message(),
            Some("newer attempt already owns this task")
        );
    }

    #[test]
    fn executor_heartbeat_llm_quota_round_trips_on_wire() {
        use crate::wire::{
            executor_heartbeat_request_from_wire, executor_heartbeat_request_to_wire,
            executor_heartbeat_response_from_wire, executor_heartbeat_response_to_wire,
        };

        let request = ExecutorHeartbeatRequest::new(
            ExecutorId::try_new("exec-llm").unwrap(),
            LeaseGeneration::initial(),
            ExecutorState::Healthy,
        )
        .with_llm_quota_reports(vec![LlmQuotaReport {
            model: "gpt-4o".into(),
            requests_used: 42,
            tokens_used: 1000,
            period_ms: 60_000,
        }]);
        let wire_req = executor_heartbeat_request_to_wire(request.clone());
        let round_trip_req = executor_heartbeat_request_from_wire(wire_req).unwrap();
        assert_eq!(
            round_trip_req.llm_quota_reports(),
            request.llm_quota_reports()
        );

        let response = ExecutorHeartbeatResponse::new(
            LeaseGeneration::initial(),
            TransportDisposition::Accepted,
        )
        .with_llm_throttles(vec![LlmThrottleCommand {
            model: "gpt-4o".into(),
            max_requests_per_minute: 100,
            max_tokens_per_minute: 10_000,
        }]);
        let wire_resp = executor_heartbeat_response_to_wire(response.clone());
        let round_trip_resp = executor_heartbeat_response_from_wire(wire_resp).unwrap();
        assert_eq!(round_trip_resp.llm_throttles(), response.llm_throttles());
    }

    #[test]
    fn source_throttle_commands_round_trip_on_wire() {
        use crate::wire::{
            executor_heartbeat_response_from_wire, executor_heartbeat_response_to_wire,
        };

        // Active throttle: rows_per_second = Some(500).
        let response_with_limit = ExecutorHeartbeatResponse::new(
            LeaseGeneration::initial(),
            TransportDisposition::Accepted,
        )
        .with_throttle_commands(vec![HeartbeatThrottleCommand {
            source_id: "src-kafka-0".into(),
            rows_per_second: Some(500),
        }]);
        let wire = executor_heartbeat_response_to_wire(response_with_limit.clone());
        let rt = executor_heartbeat_response_from_wire(wire).unwrap();
        assert_eq!(
            rt.throttle_commands(),
            response_with_limit.throttle_commands()
        );

        // Paused source: rows_per_second = Some(0).
        let response_pause = ExecutorHeartbeatResponse::new(
            LeaseGeneration::initial(),
            TransportDisposition::Accepted,
        )
        .with_throttle_commands(vec![HeartbeatThrottleCommand {
            source_id: "src-kafka-0".into(),
            rows_per_second: Some(0),
        }]);
        let wire = executor_heartbeat_response_to_wire(response_pause.clone());
        let rt = executor_heartbeat_response_from_wire(wire).unwrap();
        assert_eq!(rt.throttle_commands(), response_pause.throttle_commands());

        // Cleared throttle: rows_per_second = None.
        let response_clear = ExecutorHeartbeatResponse::new(
            LeaseGeneration::initial(),
            TransportDisposition::Accepted,
        )
        .with_throttle_commands(vec![HeartbeatThrottleCommand {
            source_id: "src-kafka-0".into(),
            rows_per_second: None,
        }]);
        let wire = executor_heartbeat_response_to_wire(response_clear.clone());
        let rt = executor_heartbeat_response_from_wire(wire).unwrap();
        assert_eq!(rt.throttle_commands(), response_clear.throttle_commands());
    }

    #[test]
    fn fencing_token_initial_is_one() {
        assert_eq!(FencingToken::initial().as_u64(), 1);
    }

    #[test]
    fn fencing_token_next_increments() {
        assert_eq!(FencingToken::initial().next().as_u64(), 2);
    }

    #[test]
    fn fencing_token_zero_rejected() {
        assert!(FencingToken::try_new(0).is_err());
    }

    #[test]
    fn fencing_token_ordering() {
        assert!(FencingToken::initial() < FencingToken::initial().next());
    }

    #[test]
    fn trace_context_is_active_when_non_empty() {
        let ctx = crate::TraceContext::new("00-abc-def-01");
        assert!(ctx.is_active());
    }

    #[test]
    fn trace_context_inactive_by_default() {
        let ctx = crate::TraceContext::default();
        assert!(!ctx.is_active());
    }

    #[test]
    fn executor_heartbeat_request_carries_trace_context() {
        let req = ExecutorHeartbeatRequest::new(
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            ExecutorState::Healthy,
        )
        .with_trace_context(crate::TraceContext::new("00-trace-01-span-01-01"));
        assert!(req.trace_context().unwrap().is_active());
    }

    // ── R4a shuffle config tests ───────────────────────────────────────────────

    use crate::{ShuffleReadConfig, ShuffleWriteConfig};

    #[test]
    fn test_shuffle_write_config_round_trip() {
        let write_cfg = ShuffleWriteConfig {
            stage_id: StageId::try_new("stage-write").unwrap(),
            num_partitions: 4,
            key_columns: vec![String::from("user_id")],
            lease_token: 99,
        };
        let task = TaskSpec::new(TaskId::try_new("task-write-rt").unwrap(), "sql: select 1")
            .with_shuffle_write(write_cfg.clone());
        let cfg = task.shuffle_write().expect("shuffle_write must be set");
        assert_eq!(cfg.num_partitions, 4);
        assert_eq!(cfg.lease_token, 99);
        assert_eq!(cfg, &write_cfg);
    }

    #[test]
    fn test_shuffle_read_config_round_trip() {
        let read_cfg = ShuffleReadConfig {
            stage_id: StageId::try_new("stage-read").unwrap(),
            partition_id: 7,
            lease_token: 42,
        };
        let task = TaskSpec::new(TaskId::try_new("task-read-rt").unwrap(), "shuffle-read")
            .with_shuffle_read(read_cfg.clone());
        let cfg = task.shuffle_read().expect("shuffle_read must be set");
        assert_eq!(cfg.partition_id, 7);
        assert_eq!(cfg, &read_cfg);
    }

    #[test]
    fn task_spec_with_no_shuffle_configs_has_none() {
        let task = TaskSpec::new(TaskId::try_new("task-plain").unwrap(), "sql: select 1");
        assert!(task.shuffle_write().is_none());
        assert!(task.shuffle_read().is_none());
    }

    // ── P0.17: heartbeat request resource fields round-trip ───────────────────

    #[test]
    fn heartbeat_request_all_resource_fields_round_trip() {
        let request = ExecutorHeartbeatRequest::new(
            ExecutorId::try_new("exec-rt").unwrap(),
            LeaseGeneration::initial(),
            ExecutorState::Healthy,
        )
        .with_memory_used_bytes(512 * 1024 * 1024)
        .with_memory_limit_bytes(2 * 1024 * 1024 * 1024)
        .with_active_task_count(4)
        .with_cpu_cores_used(3.5)
        .with_network_bytes_sent(1_000_000)
        .with_network_bytes_recv(2_000_000);

        let wire = crate::wire::executor_heartbeat_request_to_wire(request.clone());
        let round_trip = crate::wire::executor_heartbeat_request_from_wire(wire).unwrap();

        assert_eq!(round_trip.memory_used_bytes(), request.memory_used_bytes());
        assert_eq!(
            round_trip.memory_limit_bytes(),
            request.memory_limit_bytes()
        );
        assert_eq!(round_trip.active_task_count(), request.active_task_count());
        assert_eq!(round_trip.cpu_cores_used(), request.cpu_cores_used());
        assert_eq!(
            round_trip.network_bytes_sent(),
            request.network_bytes_sent()
        );
        assert_eq!(
            round_trip.network_bytes_recv(),
            request.network_bytes_recv()
        );
        assert_eq!(round_trip, request);
    }

    #[test]
    fn executor_task_assignment_carries_shuffle_write_config() {
        let write_cfg = ShuffleWriteConfig {
            stage_id: StageId::try_new("stage-sw").unwrap(),
            num_partitions: 3,
            key_columns: vec![String::from("id")],
            lease_token: 1,
        };
        let assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-sw-assign").unwrap(),
                StageId::try_new("stage-sw").unwrap(),
                TaskId::try_new("task-sw-1").unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("sql: select id from t"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline"),
        )
        .with_shuffle_write(write_cfg.clone());

        assert_eq!(assignment.shuffle_write().unwrap().num_partitions, 3);
        assert!(assignment.shuffle_read().is_none());
    }

    // ── ID validation: try_new success and failure ───────────────────────────

    #[test]
    fn job_id_try_new_success() {
        let id = JobId::try_new("job-42").unwrap();
        assert_eq!(id.as_str(), "job-42");
    }

    #[test]
    fn job_id_try_new_rejects_empty() {
        assert!(JobId::try_new("").is_err());
    }

    #[test]
    fn job_id_try_new_rejects_whitespace() {
        assert!(JobId::try_new("   ").is_err());
    }

    #[test]
    fn task_id_try_new_success() {
        let id = TaskId::try_new("task-abc").unwrap();
        assert_eq!(id.as_str(), "task-abc");
    }

    #[test]
    fn task_id_try_new_rejects_empty() {
        assert!(TaskId::try_new("").is_err());
    }

    #[test]
    fn task_id_try_new_rejects_whitespace_only() {
        assert!(TaskId::try_new("  \t\n  ").is_err());
    }

    #[test]
    fn executor_id_try_new_success() {
        let id = ExecutorId::try_new("exec-7").unwrap();
        assert_eq!(id.as_str(), "exec-7");
    }

    #[test]
    fn executor_id_try_new_rejects_empty() {
        assert!(ExecutorId::try_new("").is_err());
    }

    #[test]
    fn stage_id_try_new_success() {
        let id = StageId::try_new("stage-x").unwrap();
        assert_eq!(id.as_str(), "stage-x");
    }

    #[test]
    fn stage_id_try_new_rejects_empty() {
        assert!(StageId::try_new("").is_err());
    }

    #[test]
    fn coordinator_id_try_new_success() {
        let id = CoordinatorId::try_new("coord-1").unwrap();
        assert_eq!(id.as_str(), "coord-1");
    }

    #[test]
    fn coordinator_id_try_new_rejects_empty() {
        assert!(CoordinatorId::try_new("").is_err());
    }

    #[test]
    fn id_error_display_format() {
        let err = JobId::try_new("").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("job id"));
        assert!(msg.contains("cannot be empty"));
    }

    #[test]
    fn id_error_kind_and_reason_accessors() {
        let err = TaskId::try_new("").unwrap_err();
        assert_eq!(err.kind(), "task id");
        assert_eq!(err.reason(), "cannot be empty");
    }

    #[test]
    fn attempt_id_try_new_zero_fails() {
        assert!(AttemptId::try_new(0).is_err());
    }

    #[test]
    fn attempt_id_try_new_positive_succeeds() {
        let id = AttemptId::try_new(5).unwrap();
        assert_eq!(id.as_u32(), 5);
    }

    #[test]
    fn lease_generation_try_new_zero_fails() {
        assert!(LeaseGeneration::try_new(0).is_err());
    }

    #[test]
    fn lease_generation_try_new_positive_succeeds() {
        let id = LeaseGeneration::try_new(99).unwrap();
        assert_eq!(id.as_u64(), 99);
    }

    #[test]
    fn fencing_token_try_new_zero_fails() {
        assert!(FencingToken::try_new(0).is_err());
    }

    #[test]
    fn fencing_token_try_new_positive_succeeds() {
        let id = FencingToken::try_new(42).unwrap();
        assert_eq!(id.as_u64(), 42);
    }

    // ── JobId, TaskId, ExecutorId Display ────────────────────────────────────

    #[test]
    fn job_id_display() {
        let id = JobId::try_new("job-display-test").unwrap();
        assert_eq!(format!("{id}"), "job-display-test");
    }

    #[test]
    fn task_id_display() {
        let id = TaskId::try_new("task-display-test").unwrap();
        assert_eq!(format!("{id}"), "task-display-test");
    }

    #[test]
    fn executor_id_display() {
        let id = ExecutorId::try_new("exec-display-test").unwrap();
        assert_eq!(format!("{id}"), "exec-display-test");
    }

    #[test]
    fn stage_id_display() {
        let id = StageId::try_new("stage-display-test").unwrap();
        assert_eq!(format!("{id}"), "stage-display-test");
    }

    #[test]
    fn coordinator_id_display() {
        let id = CoordinatorId::try_new("coord-display-test").unwrap();
        assert_eq!(format!("{id}"), "coord-display-test");
    }

    #[test]
    fn attempt_id_display() {
        let id = AttemptId::try_new(3).unwrap();
        assert_eq!(format!("{id}"), "3");
    }

    #[test]
    fn lease_generation_display() {
        let id = LeaseGeneration::try_new(12).unwrap();
        assert_eq!(format!("{id}"), "12");
    }

    #[test]
    fn fencing_token_display() {
        let id = FencingToken::try_new(7).unwrap();
        assert_eq!(format!("{id}"), "7");
    }

    #[test]
    fn ids_with_special_characters_preserved() {
        let id = JobId::try_new("job/with:special@chars").unwrap();
        assert_eq!(id.as_str(), "job/with:special@chars");
        assert_eq!(format!("{id}"), "job/with:special@chars");
    }
}

/// Fuzz-style "never panics" tests for protobuf → domain wire deserialisation.
///
/// `cargo-fuzz` requires a nightly toolchain and sanitizer support that this
/// workspace does not provision; `proptest` gives equivalent adversarial-input
/// coverage (arbitrary scalar/string/enum-tag generation, shrinking on
/// failure) entirely on stable. These tests feed `from_wire` malformed,
/// out-of-range, and empty field values — only `Ok`/`Err` are valid outcomes,
/// a panic is a bug.
///
/// ## Coverage (task #21)
///
/// Currently covered:
///   register_executor_request_from_wire
///   register_executor_response_from_wire
///   deregister_executor_request_from_wire
///   deregister_executor_response_from_wire
///   executor_heartbeat_request_from_wire
///   executor_heartbeat_response_from_wire
///   executor_task_assignment_from_wire
///   task_status_request_from_wire
///   task_status_response_from_wire
///   task_cancellation_request_from_wire
///   checkpoint_ack_request_from_wire
///   checkpoint_ack_response_from_wire  (plain test — proptest! requires ≥1 arg)
///   push_continuous_input_request_from_wire
///   drain_continuous_output_request_from_wire
///   drain_continuous_output_response_from_wire
///   trigger_savepoint_request_from_wire
///   restore_job_request_from_wire
#[cfg(test)]
mod wire_fuzz {
    use crate::wire::v1;
    use crate::wire::{
        checkpoint_ack_request_from_wire, checkpoint_ack_request_to_wire,
        checkpoint_ack_response_from_wire, deregister_executor_request_from_wire,
        deregister_executor_response_from_wire, drain_continuous_output_request_from_wire,
        drain_continuous_output_response_from_wire, executor_heartbeat_request_from_wire,
        executor_heartbeat_response_from_wire, executor_task_assignment_from_wire,
        push_continuous_input_request_from_wire, register_executor_request_from_wire,
        register_executor_response_from_wire, restore_job_request_from_wire,
        task_cancellation_request_from_wire, task_status_request_from_wire,
        task_status_response_from_wire, trigger_savepoint_request_from_wire,
    };
    use crate::{
        CheckpointAckRequest, CheckpointSourceOffset, FencingToken, JobId, OperatorId, PartitionId,
        TaskId,
    };
    use proptest::prelude::*;

    fn arb_string() -> impl Strategy<Value = String> {
        prop_oneof![Just(String::new()), "[a-zA-Z0-9_/:@.-]{0,32}", "\\PC{0,16}",]
    }

    fn arb_version() -> impl Strategy<Value = Option<v1::TransportVersion>> {
        prop::option::of(
            (0u32..=5, 0u32..=5).prop_map(|(major, minor)| v1::TransportVersion { major, minor }),
        )
    }

    #[test]
    fn checkpoint_ack_source_offset_encoded_bytes_roundtrip() {
        let encoded_offset = vec![1, 2, 3, 4, 5];
        let request = CheckpointAckRequest {
            job_id: JobId::try_new("job-encoded").unwrap(),
            operator_id: OperatorId::try_new("operator-encoded").unwrap(),
            task_id: TaskId::try_new("task-encoded").unwrap(),
            epoch: 7,
            fencing_token: FencingToken::initial(),
            source_offsets: vec![CheckpointSourceOffset {
                partition_id: PartitionId::try_new("source-0").unwrap(),
                offset: 42,
                encoded_offset: encoded_offset.clone(),
            }],
            snapshot_path: Some("job-encoded/checkpoints/000/state.bin".into()),
            unaligned_buffers: Vec::new(),
            sink_transactions: Vec::new(),
        };

        let wire = checkpoint_ack_request_to_wire(request.clone());
        assert_eq!(wire.source_offsets[0].encoded_offset, encoded_offset);
        let restored = checkpoint_ack_request_from_wire(wire).unwrap();
        assert_eq!(restored, request);
    }

    /// DUR-2: prepared-sink transaction refs must survive the wire round-trip
    /// (previously `from_wire` hard-coded `sink_transactions: Vec::new()`, so
    /// the executor's prepared transactions never reached the coordinator).
    #[test]
    fn checkpoint_ack_sink_transactions_roundtrip() {
        let request = CheckpointAckRequest {
            job_id: JobId::try_new("job-tx").unwrap(),
            operator_id: OperatorId::try_new("operator-tx").unwrap(),
            task_id: TaskId::try_new("task-tx").unwrap(),
            epoch: 9,
            fencing_token: FencingToken::initial(),
            source_offsets: vec![],
            snapshot_path: None,
            unaligned_buffers: Vec::new(),
            sink_transactions: vec![crate::SinkTransactionRef {
                sink_id: "iceberg-sink".to_owned(),
                epoch: 9,
                prepare_path: "job-tx/checkpoints/009/iceberg-sink.prepare".to_owned(),
                committed: false,
            }],
        };

        let wire = checkpoint_ack_request_to_wire(request.clone());
        assert_eq!(wire.sink_transactions.len(), 1);
        assert_eq!(wire.sink_transactions[0].sink_id, "iceberg-sink");
        let restored = checkpoint_ack_request_from_wire(wire).unwrap();
        assert_eq!(restored, request);
    }

    fn arb_descriptor() -> impl Strategy<Value = Option<v1::ExecutorDescriptor>> {
        prop::option::of(
            (
                arb_string(),
                arb_string(),
                any::<u64>(),
                arb_string(),
                arb_string(),
            )
                .prop_map(
                    |(executor_id, host, slots, task_endpoint, barrier_endpoint)| {
                        v1::ExecutorDescriptor {
                            executor_id,
                            host,
                            slots,
                            task_endpoint,
                            barrier_endpoint,
                        }
                    },
                ),
        )
    }

    // proptest! requires ≥1 strategy argument; zero-arg tests live outside.
    #[test]
    fn checkpoint_ack_response_none_never_panics() {
        let _ = checkpoint_ack_response_from_wire(v1::CheckpointAckResponse { result: None });
    }

    proptest! {
        #[test]
        fn register_executor_request_from_wire_never_panics(
            version in arb_version(),
            descriptor in arb_descriptor(),
            trace_parent in arb_string(),
            trace_state in arb_string(),
        ) {
            let wire = v1::RegisterExecutorRequest { version, descriptor, trace_parent, trace_state };
            let _ = register_executor_request_from_wire(wire);
        }

        #[test]
        fn register_executor_response_from_wire_never_panics(
            version in arb_version(),
            executor_id in arb_string(),
            lease_generation in any::<u64>(),
            disposition in any::<i32>(),
            message in arb_string(),
        ) {
            let wire = v1::RegisterExecutorResponse {
                version, executor_id, lease_generation, disposition, message,
            };
            let _ = register_executor_response_from_wire(wire);
        }

        #[test]
        fn deregister_executor_request_from_wire_never_panics(
            version in arb_version(),
            executor_id in arb_string(),
            lease_generation in any::<u64>(),
            reason in arb_string(),
        ) {
            let wire = v1::DeregisterExecutorRequest { version, executor_id, lease_generation, reason };
            let _ = deregister_executor_request_from_wire(wire);
        }

        #[test]
        fn deregister_executor_response_from_wire_never_panics(
            version in arb_version(),
            executor_id in arb_string(),
            lease_generation in any::<u64>(),
            disposition in any::<i32>(),
            message in arb_string(),
        ) {
            let wire = v1::DeregisterExecutorResponse {
                version, executor_id, lease_generation, disposition, message,
            };
            let _ = deregister_executor_response_from_wire(wire);
        }

        #[test]
        fn executor_heartbeat_request_from_wire_never_panics(
            version in arb_version(),
            executor_id in arb_string(),
            lease_generation in any::<u64>(),
            state in any::<i32>(),
            memory_used in any::<u64>(),
            memory_limit in any::<u64>(),
            active_task_count in any::<u32>(),
            cpu_cores_used in any::<f32>(),
            network_sent in any::<u64>(),
            network_recv in any::<u64>(),
            trace_parent in arb_string(),
            trace_state in arb_string(),
        ) {
            let wire = v1::ExecutorHeartbeatRequest {
                version,
                executor_id,
                lease_generation,
                state,
                running_attempts: vec![],
                memory_used_bytes: memory_used,
                memory_limit_bytes: memory_limit,
                active_task_count,
                cpu_cores_used: cpu_cores_used.into(),
                network_bytes_sent: network_sent,
                network_bytes_recv: network_recv,
                llm_quota_reports: vec![],
                streaming_progress: vec![],
                hot_key_reports: vec![],
                streaming_task_states: vec![],
                trace_parent,
                trace_state,
            };
            let _ = executor_heartbeat_request_from_wire(wire);
        }

        #[test]
        fn executor_heartbeat_response_from_wire_never_panics(
            version in arb_version(),
            lease_generation in any::<u64>(),
            disposition in any::<i32>(),
            message in arb_string(),
            trace_parent in arb_string(),
            trace_state in arb_string(),
        ) {
            let wire = v1::ExecutorHeartbeatResponse {
                version,
                lease_generation,
                disposition,
                message,
                llm_throttles: vec![],
                initiate_checkpoints: vec![],
                completed_checkpoints: vec![],
                restore_checkpoints: vec![],
                source_throttles: vec![],
                trace_parent,
                trace_state,
                global_watermarks: Default::default(),
            };
            let _ = executor_heartbeat_response_from_wire(wire);
        }

        #[test]
        fn executor_task_assignment_from_wire_never_panics(
            version in arb_version(),
            job_id in arb_string(),
            stage_id in arb_string(),
            task_id in arb_string(),
            attempt_id in any::<u32>(),
            executor_id in arb_string(),
            lease_generation in any::<u64>(),
            trace_parent in arb_string(),
            trace_state in arb_string(),
        ) {
            let wire = v1::ExecutorTaskAssignment {
                version,
                job_id,
                stage_id,
                task_id,
                attempt_id,
                executor_id,
                lease_generation,
                input_partitions: vec![],
                plan_fragment: None,
                output_contract: None,
                task_timeout_secs: 0,
                has_task_timeout_secs: false,
                key_group_range_start: 0,
                key_group_range_end: 0,
                has_key_group_range: false,
                cpu_limit_nanos: 0,
                has_cpu_limit_nanos: false,
                memory_limit_bytes: 0,
                has_memory_limit_bytes: false,
                shuffle_write: None,
                shuffle_read: None,
                requires_reattach: false,
                trace_parent,
                trace_state,
            };
            let _ = executor_task_assignment_from_wire(wire);
        }

        #[test]
        fn task_status_request_from_wire_never_panics(
            version in arb_version(),
            job_id in arb_string(),
            stage_id in arb_string(),
            task_id in arb_string(),
            attempt_id in any::<u32>(),
            executor_id in arb_string(),
            lease_generation in any::<u64>(),
            state in any::<i32>(),
            message in arb_string(),
            trace_parent in arb_string(),
            trace_state in arb_string(),
        ) {
            let wire = v1::TaskStatusRequest {
                version,
                job_id,
                stage_id,
                task_id,
                attempt_id,
                executor_id,
                lease_generation,
                state,
                message,
                output_metadata: None,
                trace_parent,
                trace_state,
                missing_shuffle_partitions: vec![],
            };
            let _ = task_status_request_from_wire(wire);
        }

        #[test]
        fn task_status_response_from_wire_never_panics(
            version in arb_version(),
            disposition in any::<i32>(),
            message in arb_string(),
            trace_parent in arb_string(),
            trace_state in arb_string(),
        ) {
            let wire = v1::TaskStatusResponse {
                version, disposition, message, trace_parent, trace_state,
            };
            let _ = task_status_response_from_wire(wire);
        }

        #[test]
        fn task_cancellation_request_from_wire_never_panics(
            version in arb_version(),
            job_id in arb_string(),
            stage_id in arb_string(),
            task_id in arb_string(),
            attempt_id in any::<u32>(),
            reason in arb_string(),
        ) {
            let wire = v1::TaskCancellationRequest {
                version, job_id, stage_id, task_id, attempt_id, reason,
            };
            let _ = task_cancellation_request_from_wire(wire);
        }

        #[test]
        fn checkpoint_ack_request_from_wire_never_panics(
            job_id in arb_string(),
            operator_id in arb_string(),
            task_id in arb_string(),
            epoch in any::<u64>(),
            fencing_token in any::<u64>(),
            snapshot_path in arb_string(),
        ) {
            let wire = v1::CheckpointAckRequest {
                job_id,
                operator_id,
                task_id,
                epoch,
                fencing_token,
                source_offsets: vec![],
                snapshot_path,
                sink_transactions: vec![],
            };
            let _ = checkpoint_ack_request_from_wire(wire);
        }

        #[test]
        fn push_continuous_input_request_from_wire_never_panics(
            version in arb_version(),
            job_id in arb_string(),
            task_id in arb_string(),
            payload in arb_string(),
        ) {
            let wire = v1::PushContinuousInputRequest {
                version,
                job_id,
                task_id,
                ipc_bytes: payload.into_bytes(),
            };
            let _ = push_continuous_input_request_from_wire(wire);
        }

        #[test]
        fn drain_continuous_output_request_from_wire_never_panics(
            version in arb_version(),
            job_id in arb_string(),
            task_id in arb_string(),
        ) {
            let wire = v1::DrainContinuousOutputRequest { version, job_id, task_id };
            let _ = drain_continuous_output_request_from_wire(wire);
        }

        #[test]
        fn drain_continuous_output_response_from_wire_never_panics(
            version in arb_version(),
            disposition in any::<i32>(),
            payload in arb_string(),
        ) {
            let wire = v1::DrainContinuousOutputResponse {
                version,
                disposition,
                ipc_bytes: payload.into_bytes(),
            };
            let _ = drain_continuous_output_response_from_wire(wire);
        }

        #[test]
        fn trigger_savepoint_request_from_wire_never_panics(
            job_id in arb_string(),
            label in arb_string(),
            stop in any::<bool>(),
        ) {
            let wire = v1::TriggerSavepointRequest { job_id, label, stop };
            let _ = trigger_savepoint_request_from_wire(wire);
        }

        #[test]
        fn restore_job_request_from_wire_never_panics(
            job_id in arb_string(),
            epoch in any::<u64>(),
            storage_path in arb_string(),
            from_savepoint in any::<bool>(),
        ) {
            let wire = v1::RestoreJobRequest { job_id, epoch, storage_path, from_savepoint };
            let _ = restore_job_request_from_wire(wire);
        }
    }
}
