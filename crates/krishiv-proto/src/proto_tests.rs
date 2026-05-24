#[cfg(test)]
mod proto_tests {
    use crate::{
        AttemptId, ConnectorCapabilityFlags, DeregisterExecutorRequest, ExecutorDescriptor,
        ExecutorHeartbeatRequest, ExecutorHeartbeatResponse, ExecutorId, ExecutorState,
        ExecutorTaskAssignment, FencingToken, InputPartition, InputPartitionDescriptor, JobId,
        JobKind, JobSpec, JobState, LeaseGeneration, LlmQuotaReport, LlmThrottleCommand,
        MemoryKafkaRecord, OutputContract, OutputContractDescriptor, OutputContractKind,
        PlanFragment, RegisterExecutorRequest, StageId, StageSpec, TaskAttemptRef,
        TaskCancellationRequest, TaskId, TaskOutputMetadata, TaskSpec, TaskState,
        TaskStatusRequest, TaskStatusResponse, TransportDisposition, TransportVersion,
    };

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
        .with_input_partitions(vec![InputPartition::new("part-1", "first file split")]);

        assert_eq!(assignment.attempt_id(), AttemptId::initial());
        assert_eq!(assignment.lease_generation(), LeaseGeneration::initial());
        assert_eq!(assignment.input_partitions()[0].partition_id(), "part-1");
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
        .with_input_partitions(vec![InputPartition::new("part-1", "first file split")]);

        let wire = crate::wire::executor_task_assignment_to_wire(assignment.clone());
        let round_trip = crate::wire::executor_task_assignment_from_wire(wire).unwrap();

        assert_eq!(round_trip, assignment);
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

        let wire = crate::wire::executor_task_assignment_to_wire(assignment.clone());
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
}
