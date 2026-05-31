//! Generated protobuf wire conversions.

use crate::checkpoint::{CheckpointAckRequest, CheckpointAckResponse, CheckpointSourceOffset};
use crate::executor::{
    DeregisterExecutorRequest, DeregisterExecutorResponse, ExecutorDescriptor,
    HeartbeatThrottleCommand, ShufflePartitionOutput, TaskOutputMetadata, TaskRuntimeStats,
};
use crate::ids::{
    AttemptId, ExecutorId, FencingToken, JobId, LeaseGeneration, StageId, TaskId, TransportVersion,
};
use crate::lifecycle::{ExecutorState, TaskState};
use crate::task::{
    ExecutorHeartbeatRequest, ExecutorHeartbeatResponse, ExecutorTaskAssignment, InputPartition,
    InputPartitionDescriptor, KeyGroupRange, MemoryKafkaRecord, OutputContract,
    OutputContractDescriptor, OutputContractKind, PlanFragment, RegisterExecutorRequest,
    RegisterExecutorResponse, TaskAttemptRef, TaskCancellationRequest, TaskStatusRequest,
    TaskStatusResponse, TransportDisposition,
};

pub mod v1 {
    tonic::include_proto!("krishiv.transport.v1");
}

/// Error raised when a protobuf message cannot be converted to a domain contract.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct WireError {
    message: String,
}

impl WireError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Human-readable conversion failure.
    pub fn message(&self) -> &str {
        &self.message
    }
}

type WireResult<T> = Result<T, WireError>;

/// Convert a domain registration request to protobuf.
pub fn register_executor_request_to_wire(
    value: RegisterExecutorRequest,
) -> v1::RegisterExecutorRequest {
    v1::RegisterExecutorRequest {
        version: Some(transport_version_to_wire(value.version())),
        descriptor: Some(executor_descriptor_to_wire(value.descriptor())),
    }
}

/// Convert a protobuf registration request to the domain contract.
pub fn register_executor_request_from_wire(
    value: v1::RegisterExecutorRequest,
) -> WireResult<RegisterExecutorRequest> {
    let version = transport_version_from_wire(required(value.version, "version")?)?;
    let descriptor = executor_descriptor_from_wire(required(value.descriptor, "descriptor")?)?;
    Ok(RegisterExecutorRequest::new(descriptor).with_version(version))
}

/// Convert a domain registration response to protobuf.
pub fn register_executor_response_to_wire(
    value: RegisterExecutorResponse,
) -> v1::RegisterExecutorResponse {
    v1::RegisterExecutorResponse {
        version: Some(transport_version_to_wire(value.version())),
        executor_id: value.executor_id().as_str().to_owned(),
        lease_generation: value.lease_generation().as_u64(),
        disposition: transport_disposition_to_wire(value.disposition()) as i32,
        message: value.message().unwrap_or_default().to_owned(),
    }
}

/// Convert a protobuf registration response to the domain contract.
pub fn register_executor_response_from_wire(
    value: v1::RegisterExecutorResponse,
) -> WireResult<RegisterExecutorResponse> {
    let version = transport_version_from_wire(required(value.version, "version")?)?;
    let executor_id = ExecutorId::try_new(value.executor_id).map_err(WireError::from_id)?;
    let lease_generation =
        LeaseGeneration::try_new(value.lease_generation).map_err(WireError::from_id)?;
    let disposition = transport_disposition_from_wire(value.disposition)?;
    let mut response = RegisterExecutorResponse::new(executor_id, lease_generation, disposition)
        .with_version(version);
    if !value.message.is_empty() {
        response = response.with_message(value.message);
    }
    Ok(response)
}

/// Convert a domain deregistration request to protobuf.
pub fn deregister_executor_request_to_wire(
    value: DeregisterExecutorRequest,
) -> v1::DeregisterExecutorRequest {
    v1::DeregisterExecutorRequest {
        version: Some(transport_version_to_wire(value.version())),
        executor_id: value.executor_id().as_str().to_owned(),
        lease_generation: value.lease_generation().as_u64(),
        reason: value.reason().unwrap_or_default().to_owned(),
    }
}

/// Convert a protobuf deregistration request to the domain contract.
pub fn deregister_executor_request_from_wire(
    value: v1::DeregisterExecutorRequest,
) -> WireResult<DeregisterExecutorRequest> {
    let version = transport_version_from_wire(required(value.version, "version")?)?;
    let executor_id = ExecutorId::try_new(value.executor_id).map_err(WireError::from_id)?;
    let lease_generation =
        LeaseGeneration::try_new(value.lease_generation).map_err(WireError::from_id)?;
    let mut request =
        DeregisterExecutorRequest::new(executor_id, lease_generation).with_version(version);
    if !value.reason.is_empty() {
        request = request.with_reason(value.reason);
    }
    Ok(request)
}

/// Convert a domain deregistration response to protobuf.
pub fn deregister_executor_response_to_wire(
    value: DeregisterExecutorResponse,
) -> v1::DeregisterExecutorResponse {
    v1::DeregisterExecutorResponse {
        version: Some(transport_version_to_wire(value.version())),
        executor_id: value.executor_id().as_str().to_owned(),
        lease_generation: value.lease_generation().as_u64(),
        disposition: transport_disposition_to_wire(value.disposition()) as i32,
        message: value.message().unwrap_or_default().to_owned(),
    }
}

/// Convert a protobuf deregistration response to the domain contract.
pub fn deregister_executor_response_from_wire(
    value: v1::DeregisterExecutorResponse,
) -> WireResult<DeregisterExecutorResponse> {
    let version = transport_version_from_wire(required(value.version, "version")?)?;
    let executor_id = ExecutorId::try_new(value.executor_id).map_err(WireError::from_id)?;
    let lease_generation =
        LeaseGeneration::try_new(value.lease_generation).map_err(WireError::from_id)?;
    let disposition = transport_disposition_from_wire(value.disposition)?;
    let mut response = DeregisterExecutorResponse::new(executor_id, lease_generation, disposition)
        .with_version(version);
    if !value.message.is_empty() {
        response = response.with_message(value.message);
    }
    Ok(response)
}

/// Convert a domain heartbeat request to protobuf.
///
/// P0.17: Maps ALL task-resource fields so none are silently dropped.
pub fn executor_heartbeat_request_to_wire(
    value: ExecutorHeartbeatRequest,
) -> v1::ExecutorHeartbeatRequest {
    v1::ExecutorHeartbeatRequest {
        version: Some(transport_version_to_wire(value.version())),
        executor_id: value.executor_id().as_str().to_owned(),
        lease_generation: value.lease_generation().as_u64(),
        state: executor_state_to_wire(value.state()) as i32,
        running_attempts: value
            .running_attempts()
            .iter()
            .map(task_attempt_ref_to_wire)
            .collect(),
        memory_used_bytes: value.memory_used_bytes().unwrap_or(0),
        memory_limit_bytes: value.memory_limit_bytes().unwrap_or(0),
        active_task_count: value.active_task_count().unwrap_or(0),
        cpu_cores_used: value.cpu_cores_used().unwrap_or(0.0),
        network_bytes_sent: value.network_bytes_sent().unwrap_or(0),
        network_bytes_recv: value.network_bytes_recv().unwrap_or(0),
        llm_quota_reports: value
            .llm_quota_reports()
            .iter()
            .map(llm_quota_report_to_wire)
            .collect(),
        streaming_progress: value
            .streaming_progress()
            .iter()
            .map(streaming_progress_report_to_wire)
            .collect(),
    }
}

/// Convert a protobuf heartbeat request to the domain contract.
///
/// P0.17: Restores ALL task-resource fields from the wire message.
pub fn executor_heartbeat_request_from_wire(
    value: v1::ExecutorHeartbeatRequest,
) -> WireResult<ExecutorHeartbeatRequest> {
    let version = transport_version_from_wire(required(value.version, "version")?)?;
    let executor_id = ExecutorId::try_new(value.executor_id).map_err(WireError::from_id)?;
    let lease_generation =
        LeaseGeneration::try_new(value.lease_generation).map_err(WireError::from_id)?;
    let state = executor_state_from_wire(value.state)?;
    let running_attempts = value
        .running_attempts
        .into_iter()
        .map(task_attempt_ref_from_wire)
        .collect::<WireResult<Vec<_>>>()?;

    let mut req = ExecutorHeartbeatRequest::new(executor_id, lease_generation, state)
        .with_version(version)
        .with_running_attempts(running_attempts);

    if value.memory_used_bytes > 0 {
        req = req.with_memory_used_bytes(value.memory_used_bytes);
    }
    if value.memory_limit_bytes > 0 {
        req = req.with_memory_limit_bytes(value.memory_limit_bytes);
    }
    if value.active_task_count > 0 {
        req = req.with_active_task_count(value.active_task_count);
    }
    if value.cpu_cores_used > 0.0 {
        req = req.with_cpu_cores_used(value.cpu_cores_used);
    }
    if value.network_bytes_sent > 0 {
        req = req.with_network_bytes_sent(value.network_bytes_sent);
    }
    if value.network_bytes_recv > 0 {
        req = req.with_network_bytes_recv(value.network_bytes_recv);
    }
    if !value.llm_quota_reports.is_empty() {
        req = req.with_llm_quota_reports(
            value
                .llm_quota_reports
                .into_iter()
                .map(llm_quota_report_from_wire)
                .collect(),
        );
    }
    if !value.streaming_progress.is_empty() {
        req = req.with_streaming_progress(
            value
                .streaming_progress
                .into_iter()
                .map(streaming_progress_report_from_wire)
                .collect(),
        );
    }

    Ok(req)
}

/// Convert a domain heartbeat response to protobuf.
pub fn executor_heartbeat_response_to_wire(
    value: ExecutorHeartbeatResponse,
) -> v1::ExecutorHeartbeatResponse {
    v1::ExecutorHeartbeatResponse {
        version: Some(transport_version_to_wire(value.version())),
        lease_generation: value.lease_generation().as_u64(),
        disposition: transport_disposition_to_wire(value.disposition()) as i32,
        message: value.message().unwrap_or_default().to_owned(),
        llm_throttles: value
            .llm_throttles()
            .iter()
            .map(llm_throttle_command_to_wire)
            .collect(),
        initiate_checkpoints: value
            .checkpoint_commands()
            .iter()
            .map(|cmd| v1::InitiateCheckpointCommand {
                job_id: cmd.job_id.as_str().to_owned(),
                epoch: cmd.epoch,
                fencing_token: cmd.fencing_token.as_u64(),
            })
            .collect(),
        source_throttles: value
            .throttle_commands()
            .iter()
            .map(heartbeat_throttle_command_to_wire)
            .collect(),
    }
}

/// Convert a protobuf heartbeat response to the domain contract.
pub fn executor_heartbeat_response_from_wire(
    value: v1::ExecutorHeartbeatResponse,
) -> WireResult<ExecutorHeartbeatResponse> {
    let version = transport_version_from_wire(required(value.version, "version")?)?;
    let lease_generation =
        LeaseGeneration::try_new(value.lease_generation).map_err(WireError::from_id)?;
    let disposition = transport_disposition_from_wire(value.disposition)?;
    let mut response =
        ExecutorHeartbeatResponse::new(lease_generation, disposition).with_version(version);
    if !value.message.is_empty() {
        response = response.with_message(value.message);
    }
    if !value.llm_throttles.is_empty() {
        response = response.with_llm_throttles(
            value
                .llm_throttles
                .into_iter()
                .map(llm_throttle_command_from_wire)
                .collect(),
        );
    }
    if !value.source_throttles.is_empty() {
        response = response.with_throttle_commands(
            value
                .source_throttles
                .into_iter()
                .map(heartbeat_throttle_command_from_wire)
                .collect(),
        );
    }
    if !value.initiate_checkpoints.is_empty() {
        use crate::ids::{FencingToken, JobId};
        use crate::task::InitiateCheckpointCommand;
        let cmds: Vec<InitiateCheckpointCommand> = value
            .initiate_checkpoints
            .into_iter()
            .filter_map(|cmd| {
                let job_id = JobId::try_new(cmd.job_id).ok()?;
                let fencing_token = FencingToken::try_new(cmd.fencing_token).ok()?;
                Some(InitiateCheckpointCommand {
                    job_id,
                    epoch: cmd.epoch,
                    fencing_token,
                })
            })
            .collect();
        response = response.with_checkpoint_commands(cmds);
    }
    Ok(response)
}

fn llm_quota_report_to_wire(value: &crate::LlmQuotaReport) -> v1::LlmQuotaReport {
    v1::LlmQuotaReport {
        model: value.model.clone(),
        requests_used: value.requests_used,
        tokens_used: value.tokens_used,
        period_ms: value.period_ms,
    }
}

fn llm_quota_report_from_wire(value: v1::LlmQuotaReport) -> crate::LlmQuotaReport {
    crate::LlmQuotaReport {
        model: value.model,
        requests_used: value.requests_used,
        tokens_used: value.tokens_used,
        period_ms: value.period_ms,
    }
}

fn streaming_progress_report_to_wire(
    value: &crate::StreamingProgressReport,
) -> v1::StreamingProgressReport {
    v1::StreamingProgressReport {
        job_id: value.job_id.clone(),
        task_id: value.task_id.clone(),
        watermark_ms: value.watermark_ms,
        rows_emitted: value.rows_emitted,
        batches_emitted: value.batches_emitted,
        state_bytes: value.state_bytes,
        source_offset: value.source_offset.clone(),
        timestamp_ms: value.timestamp_ms,
    }
}

fn streaming_progress_report_from_wire(
    value: v1::StreamingProgressReport,
) -> crate::StreamingProgressReport {
    crate::StreamingProgressReport {
        job_id: value.job_id,
        task_id: value.task_id,
        watermark_ms: value.watermark_ms,
        rows_emitted: value.rows_emitted,
        batches_emitted: value.batches_emitted,
        state_bytes: value.state_bytes,
        source_offset: value.source_offset,
        timestamp_ms: value.timestamp_ms,
    }
}

fn llm_throttle_command_to_wire(value: &crate::LlmThrottleCommand) -> v1::LlmThrottleCommand {
    v1::LlmThrottleCommand {
        model: value.model.clone(),
        max_requests_per_minute: value.max_requests_per_minute,
        max_tokens_per_minute: value.max_tokens_per_minute,
    }
}

fn llm_throttle_command_from_wire(value: v1::LlmThrottleCommand) -> crate::LlmThrottleCommand {
    crate::LlmThrottleCommand {
        model: value.model,
        max_requests_per_minute: value.max_requests_per_minute,
        max_tokens_per_minute: value.max_tokens_per_minute,
    }
}

fn heartbeat_throttle_command_to_wire(
    value: &HeartbeatThrottleCommand,
) -> v1::HeartbeatThrottleCommand {
    match value.rows_per_second {
        Some(rps) => v1::HeartbeatThrottleCommand {
            source_id: value.source_id.clone(),
            rows_per_second: rps,
            throttle_cleared: false,
        },
        None => v1::HeartbeatThrottleCommand {
            source_id: value.source_id.clone(),
            rows_per_second: 0,
            throttle_cleared: true,
        },
    }
}

fn heartbeat_throttle_command_from_wire(
    value: v1::HeartbeatThrottleCommand,
) -> HeartbeatThrottleCommand {
    let rows_per_second = if value.throttle_cleared {
        None
    } else if value.rows_per_second > 0 {
        Some(value.rows_per_second)
    } else {
        // rows_per_second == 0 and throttle_cleared == false: treat as unlimited
        // (defensive; prefer setting throttle_cleared = true on the sender side).
        None
    };
    HeartbeatThrottleCommand {
        source_id: value.source_id,
        rows_per_second,
    }
}

/// Convert a domain executor task assignment to protobuf.
pub fn executor_task_assignment_to_wire(
    value: ExecutorTaskAssignment,
) -> v1::ExecutorTaskAssignment {
    v1::ExecutorTaskAssignment {
        version: Some(transport_version_to_wire(value.version())),
        job_id: value.job_id().as_str().to_owned(),
        stage_id: value.stage_id().as_str().to_owned(),
        task_id: value.task_id().as_str().to_owned(),
        attempt_id: value.attempt_id().as_u32(),
        executor_id: value.executor_id().as_str().to_owned(),
        lease_generation: value.lease_generation().as_u64(),
        input_partitions: value
            .input_partitions()
            .iter()
            .map(input_partition_to_wire)
            .collect(),
        plan_fragment: Some(plan_fragment_to_wire(value.plan_fragment())),
        output_contract: Some(output_contract_to_wire(value.output_contract())),
        task_timeout_secs: value.task_timeout_secs().unwrap_or(0),
        key_group_range_start: value.key_group_range().start(),
        key_group_range_end: value.key_group_range().end(),
        has_key_group_range: true,
    }
}

/// Convert a protobuf executor task assignment to the domain contract.
pub fn executor_task_assignment_from_wire(
    value: v1::ExecutorTaskAssignment,
) -> WireResult<ExecutorTaskAssignment> {
    let version = transport_version_from_wire(required(value.version, "version")?)?;
    let ids = TaskAttemptRef::new(
        JobId::try_new(value.job_id).map_err(WireError::from_id)?,
        StageId::try_new(value.stage_id).map_err(WireError::from_id)?,
        TaskId::try_new(value.task_id).map_err(WireError::from_id)?,
        AttemptId::try_new(value.attempt_id).map_err(WireError::from_id)?,
    );
    let executor_id = ExecutorId::try_new(value.executor_id).map_err(WireError::from_id)?;
    let lease_generation =
        LeaseGeneration::try_new(value.lease_generation).map_err(WireError::from_id)?;
    let input_partitions = value
        .input_partitions
        .into_iter()
        .map(input_partition_from_wire)
        .collect::<WireResult<Vec<_>>>()?;
    let plan_fragment = plan_fragment_from_wire(required(value.plan_fragment, "plan_fragment")?)?;
    let output_contract =
        output_contract_from_wire(required(value.output_contract, "output_contract")?)?;

    let mut assignment = ExecutorTaskAssignment::new(
        ids,
        executor_id,
        lease_generation,
        plan_fragment,
        output_contract,
    )
    .with_version(version)
    .with_input_partitions(input_partitions);
    if value.task_timeout_secs > 0 {
        assignment = assignment.with_task_timeout_secs(value.task_timeout_secs);
    }
    if value.has_key_group_range {
        assignment = assignment.with_key_group_range(KeyGroupRange::new(
            value.key_group_range_start,
            value.key_group_range_end,
        ));
    }
    Ok(assignment)
}

/// Convert a domain task status request to protobuf.
pub fn task_status_request_to_wire(value: TaskStatusRequest) -> v1::TaskStatusRequest {
    v1::TaskStatusRequest {
        version: Some(transport_version_to_wire(value.version())),
        job_id: value.job_id().as_str().to_owned(),
        stage_id: value.stage_id().as_str().to_owned(),
        task_id: value.task_id().as_str().to_owned(),
        attempt_id: value.attempt_id().as_u32(),
        executor_id: value.executor_id().as_str().to_owned(),
        lease_generation: value.lease_generation().as_u64(),
        state: task_state_to_wire(value.state()) as i32,
        message: value.message().unwrap_or_default().to_owned(),
        output_metadata: value.output_metadata().map(task_output_metadata_to_wire),
    }
}

/// Convert a protobuf task status request to the domain contract.
pub fn task_status_request_from_wire(
    value: v1::TaskStatusRequest,
) -> WireResult<TaskStatusRequest> {
    let version = transport_version_from_wire(required(value.version, "version")?)?;
    let ids = TaskAttemptRef::new(
        JobId::try_new(value.job_id).map_err(WireError::from_id)?,
        StageId::try_new(value.stage_id).map_err(WireError::from_id)?,
        TaskId::try_new(value.task_id).map_err(WireError::from_id)?,
        AttemptId::try_new(value.attempt_id).map_err(WireError::from_id)?,
    );
    let executor_id = ExecutorId::try_new(value.executor_id).map_err(WireError::from_id)?;
    let lease_generation =
        LeaseGeneration::try_new(value.lease_generation).map_err(WireError::from_id)?;
    let state = task_state_from_wire(value.state)?;
    let mut request =
        TaskStatusRequest::new(ids, executor_id, lease_generation, state).with_version(version);
    if !value.message.is_empty() {
        request = request.with_message(value.message);
    }
    if let Some(output_metadata) = value.output_metadata {
        request = request.with_output_metadata(task_output_metadata_from_wire(output_metadata)?);
    }
    Ok(request)
}

/// Convert a domain task cancellation request to protobuf.
pub fn task_cancellation_request_to_wire(
    value: TaskCancellationRequest,
) -> v1::TaskCancellationRequest {
    v1::TaskCancellationRequest {
        version: Some(transport_version_to_wire(value.version())),
        job_id: value.job_id().as_str().to_owned(),
        stage_id: value.stage_id().as_str().to_owned(),
        task_id: value.task_id().as_str().to_owned(),
        attempt_id: value.attempt_id().as_u32(),
        reason: value.reason().unwrap_or_default().to_owned(),
    }
}

/// Convert a protobuf task cancellation request to the domain contract.
pub fn task_cancellation_request_from_wire(
    value: v1::TaskCancellationRequest,
) -> WireResult<TaskCancellationRequest> {
    let version = transport_version_from_wire(required(value.version, "version")?)?;
    let ids = TaskAttemptRef::new(
        JobId::try_new(value.job_id).map_err(WireError::from_id)?,
        StageId::try_new(value.stage_id).map_err(WireError::from_id)?,
        TaskId::try_new(value.task_id).map_err(WireError::from_id)?,
        AttemptId::try_new(value.attempt_id).map_err(WireError::from_id)?,
    );
    let mut request = TaskCancellationRequest::new(ids).with_version(version);
    if !value.reason.is_empty() {
        request = request.with_reason(value.reason);
    }
    Ok(request)
}

/// Convert a domain task status response to protobuf.
pub fn task_status_response_to_wire(value: TaskStatusResponse) -> v1::TaskStatusResponse {
    v1::TaskStatusResponse {
        version: Some(transport_version_to_wire(value.version())),
        disposition: transport_disposition_to_wire(value.disposition()) as i32,
        message: value.message().unwrap_or_default().to_owned(),
    }
}

/// Convert a protobuf task status response to the domain contract.
pub fn task_status_response_from_wire(
    value: v1::TaskStatusResponse,
) -> WireResult<TaskStatusResponse> {
    let version = transport_version_from_wire(required(value.version, "version")?)?;
    let disposition = transport_disposition_from_wire(value.disposition)?;
    let mut response = TaskStatusResponse::new(disposition).with_version(version);
    if !value.message.is_empty() {
        response = response.with_message(value.message);
    }
    Ok(response)
}

fn task_output_metadata_to_wire(value: &TaskOutputMetadata) -> v1::TaskOutputMetadata {
    v1::TaskOutputMetadata {
        output_kind: value.output_kind().to_owned(),
        row_count: value.row_count(),
        batch_count: value.batch_count(),
        column_count: value.column_count(),
        // Shuffle partition and runtime stats are carried in-process for R4;
        // proto encoding is deferred until the wire schema stabilises.
        shuffle_partition_ids: value
            .shuffle_partitions()
            .iter()
            .map(|p| p.partition_id)
            .collect(),
        shuffle_partition_bytes: value
            .shuffle_partitions()
            .iter()
            .map(|p| p.size_bytes)
            .collect(),
        shuffle_flight_endpoints: value
            .shuffle_partitions()
            .iter()
            .map(|p| p.flight_endpoint.clone())
            .collect(),
        input_rows: value.runtime_stats().map_or(0, |s| s.input_rows),
        output_rows: value.runtime_stats().map_or(0, |s| s.output_rows),
        cpu_nanos: value.runtime_stats().map_or(0, |s| s.cpu_nanos),
        spill_bytes: value.runtime_stats().map_or(0, |s| s.spill_bytes),
        inline_record_batch_ipc: value.inline_record_batch_ipc().to_vec(),
    }
}

fn task_output_metadata_from_wire(value: v1::TaskOutputMetadata) -> WireResult<TaskOutputMetadata> {
    if value.output_kind.trim().is_empty() {
        return Err(WireError::new("task output metadata kind cannot be empty"));
    }
    let shuffle_partitions: Vec<ShufflePartitionOutput> = value
        .shuffle_partition_ids
        .into_iter()
        .zip(value.shuffle_partition_bytes)
        .zip(value.shuffle_flight_endpoints)
        .map(|((id, bytes), endpoint)| ShufflePartitionOutput::new(id, bytes, endpoint))
        .collect();
    let mut meta = TaskOutputMetadata::new(
        value.output_kind,
        value.row_count,
        value.batch_count,
        value.column_count,
    );
    if !shuffle_partitions.is_empty() {
        meta = meta.with_shuffle_partitions(shuffle_partitions);
    }
    if !value.inline_record_batch_ipc.is_empty() {
        meta = meta.with_inline_record_batch_ipc(value.inline_record_batch_ipc);
    }
    let has_stats = value.input_rows > 0
        || value.output_rows > 0
        || value.cpu_nanos > 0
        || value.spill_bytes > 0;
    if has_stats {
        meta = meta.with_runtime_stats(TaskRuntimeStats {
            input_rows: value.input_rows,
            output_rows: value.output_rows,
            cpu_nanos: value.cpu_nanos,
            memory_bytes: 0,
            spill_bytes: value.spill_bytes,
        });
    }
    Ok(meta)
}

fn required<T>(value: Option<T>, field: &'static str) -> WireResult<T> {
    value.ok_or_else(|| WireError::new(format!("missing required field `{field}`")))
}

fn transport_version_to_wire(value: TransportVersion) -> v1::TransportVersion {
    v1::TransportVersion {
        major: value.major().into(),
        minor: value.minor().into(),
    }
}

fn transport_version_from_wire(value: v1::TransportVersion) -> WireResult<TransportVersion> {
    let major = value
        .major
        .try_into()
        .map_err(|_| WireError::new("transport version major is too large"))?;
    let minor = value
        .minor
        .try_into()
        .map_err(|_| WireError::new("transport version minor is too large"))?;
    Ok(TransportVersion::new(major, minor))
}

fn executor_descriptor_to_wire(value: &ExecutorDescriptor) -> v1::ExecutorDescriptor {
    v1::ExecutorDescriptor {
        executor_id: value.executor_id().as_str().to_owned(),
        host: value.host().to_owned(),
        slots: value.slots() as u64,
        task_endpoint: value.task_endpoint().unwrap_or_default().to_owned(),
        barrier_endpoint: value.barrier_endpoint().unwrap_or_default().to_owned(),
    }
}

fn executor_descriptor_from_wire(value: v1::ExecutorDescriptor) -> WireResult<ExecutorDescriptor> {
    let slots = value
        .slots
        .try_into()
        .map_err(|_| WireError::new("executor slots value is too large"))?;
    let mut descriptor = ExecutorDescriptor::new(
        ExecutorId::try_new(value.executor_id).map_err(WireError::from_id)?,
        value.host,
        slots,
    );
    if !value.task_endpoint.is_empty() {
        descriptor = descriptor.with_task_endpoint(value.task_endpoint);
    }
    if !value.barrier_endpoint.is_empty() {
        descriptor = descriptor.with_barrier_endpoint(value.barrier_endpoint);
    }
    Ok(descriptor)
}

fn task_attempt_ref_to_wire(value: &TaskAttemptRef) -> v1::TaskAttemptRef {
    v1::TaskAttemptRef {
        job_id: value.job_id().as_str().to_owned(),
        stage_id: value.stage_id().as_str().to_owned(),
        task_id: value.task_id().as_str().to_owned(),
        attempt_id: value.attempt_id().as_u32(),
    }
}

fn task_attempt_ref_from_wire(value: v1::TaskAttemptRef) -> WireResult<TaskAttemptRef> {
    Ok(TaskAttemptRef::new(
        JobId::try_new(value.job_id).map_err(WireError::from_id)?,
        StageId::try_new(value.stage_id).map_err(WireError::from_id)?,
        TaskId::try_new(value.task_id).map_err(WireError::from_id)?,
        AttemptId::try_new(value.attempt_id).map_err(WireError::from_id)?,
    ))
}

fn input_partition_to_wire(value: &InputPartition) -> v1::InputPartition {
    v1::InputPartition {
        partition_id: value.partition_id().to_owned(),
        description: value.description().to_owned(),
        descriptor: value.descriptor().map(input_partition_descriptor_to_wire),
    }
}

fn input_partition_from_wire(value: v1::InputPartition) -> WireResult<InputPartition> {
    if value.partition_id.trim().is_empty() {
        return Err(WireError::new("input partition id cannot be empty"));
    }
    let partition = InputPartition::new(value.partition_id, value.description);
    match value.descriptor {
        Some(descriptor) => {
            Ok(partition.with_descriptor(input_partition_descriptor_from_wire(descriptor)?))
        }
        None => Ok(partition),
    }
}

fn input_partition_descriptor_to_wire(
    value: &InputPartitionDescriptor,
) -> v1::InputPartitionDescriptor {
    match value {
        InputPartitionDescriptor::LocalParquet { table_name, path } => {
            v1::InputPartitionDescriptor {
                kind: v1::InputPartitionDescriptorKind::LocalParquet as i32,
                table_name: table_name.clone(),
                path: path.clone(),
                ..Default::default()
            }
        }
        InputPartitionDescriptor::ConnectorParquet { table_name, path } => {
            v1::InputPartitionDescriptor {
                kind: v1::InputPartitionDescriptorKind::ConnectorParquet as i32,
                table_name: table_name.clone().unwrap_or_default(),
                path: path.clone(),
                ..Default::default()
            }
        }
        InputPartitionDescriptor::ObjectParquet {
            table_name,
            base_dir,
            object_path,
        } => v1::InputPartitionDescriptor {
            kind: v1::InputPartitionDescriptorKind::ObjectParquet as i32,
            table_name: table_name.clone(),
            object_base_dir: base_dir.clone(),
            object_path: object_path.clone(),
            ..Default::default()
        },
        InputPartitionDescriptor::MemoryKafka {
            topic,
            partition,
            start_offset,
            records,
        } => v1::InputPartitionDescriptor {
            kind: v1::InputPartitionDescriptorKind::MemoryKafka as i32,
            kafka_topic: topic.clone(),
            kafka_partition: *partition,
            kafka_start_offset: *start_offset,
            memory_kafka_records: records
                .iter()
                .map(|record| v1::MemoryKafkaRecord {
                    id: record.id,
                    value: record.value.clone(),
                })
                .collect(),
            ..Default::default()
        },
        InputPartitionDescriptor::ShuffleFlight {
            table_name,
            flight_endpoint,
            job_id,
            upstream_stage_id,
            partition_id,
        } => v1::InputPartitionDescriptor {
            kind: v1::InputPartitionDescriptorKind::ShuffleFlight as i32,
            table_name: table_name.clone(),
            shuffle_flight_endpoint: flight_endpoint.clone(),
            shuffle_job_id: job_id.as_str().to_owned(),
            shuffle_upstream_stage_id: upstream_stage_id.as_str().to_owned(),
            shuffle_partition_id: *partition_id,
            ..Default::default()
        },
        InputPartitionDescriptor::InlineIpc { table_name, ipc_bytes } => {
            v1::InputPartitionDescriptor {
                kind: v1::InputPartitionDescriptorKind::InlineIpc as i32,
                table_name: table_name.clone(),
                ipc_bytes: ipc_bytes.clone(),
                ..Default::default()
            }
        }
    }
}

fn input_partition_descriptor_from_wire(
    value: v1::InputPartitionDescriptor,
) -> WireResult<InputPartitionDescriptor> {
    let kind = v1::InputPartitionDescriptorKind::try_from(value.kind).map_err(|_| {
        WireError::new(format!(
            "invalid or unrecognized input partition descriptor kind ID: {}",
            value.kind
        ))
    })?;

    match kind {
        v1::InputPartitionDescriptorKind::Unspecified => Err(WireError::new(
            "input partition descriptor kind must be specified",
        )),
        v1::InputPartitionDescriptorKind::LocalParquet => {
            require_non_empty(&value.table_name, "local parquet table name")?;
            require_non_empty(&value.path, "local parquet path")?;
            Ok(InputPartitionDescriptor::LocalParquet {
                table_name: value.table_name,
                path: value.path,
            })
        }
        v1::InputPartitionDescriptorKind::ConnectorParquet => {
            require_non_empty(&value.path, "connector parquet path")?;
            Ok(InputPartitionDescriptor::ConnectorParquet {
                table_name: non_empty_string(value.table_name),
                path: value.path,
            })
        }
        v1::InputPartitionDescriptorKind::ObjectParquet => {
            require_non_empty(&value.table_name, "object parquet table name")?;
            require_non_empty(&value.object_base_dir, "object parquet base dir")?;
            require_non_empty(&value.object_path, "object parquet path")?;
            Ok(InputPartitionDescriptor::ObjectParquet {
                table_name: value.table_name,
                base_dir: value.object_base_dir,
                object_path: value.object_path,
            })
        }
        v1::InputPartitionDescriptorKind::MemoryKafka => {
            require_non_empty(&value.kafka_topic, "memory kafka topic")?;
            if value.memory_kafka_records.is_empty() {
                return Err(WireError::new("memory kafka records cannot be empty"));
            }
            Ok(InputPartitionDescriptor::MemoryKafka {
                topic: value.kafka_topic,
                partition: value.kafka_partition,
                start_offset: value.kafka_start_offset,
                records: value
                    .memory_kafka_records
                    .into_iter()
                    .map(|record| MemoryKafkaRecord::new(record.id, record.value))
                    .collect(),
            })
        }
        v1::InputPartitionDescriptorKind::ShuffleFlight => {
            require_non_empty(&value.table_name, "shuffle flight table name")?;
            require_non_empty(
                &value.shuffle_upstream_stage_id,
                "shuffle upstream stage id",
            )?;
            require_non_empty(&value.shuffle_job_id, "shuffle job id")?;
            Ok(InputPartitionDescriptor::ShuffleFlight {
                table_name: value.table_name,
                flight_endpoint: value.shuffle_flight_endpoint,
                job_id: JobId::try_new(value.shuffle_job_id).map_err(WireError::from_id)?,
                upstream_stage_id: StageId::try_new(value.shuffle_upstream_stage_id)
                    .map_err(WireError::from_id)?,
                partition_id: value.shuffle_partition_id,
            })
        }
        v1::InputPartitionDescriptorKind::InlineIpc => {
            require_non_empty(&value.table_name, "inline ipc table name")?;
            Ok(InputPartitionDescriptor::InlineIpc {
                table_name: value.table_name,
                ipc_bytes: value.ipc_bytes,
            })
        }
    }
}

fn plan_fragment_to_wire(value: &PlanFragment) -> v1::PlanFragment {
    v1::PlanFragment {
        description: value.description().to_owned(),
    }
}

fn plan_fragment_from_wire(value: v1::PlanFragment) -> WireResult<PlanFragment> {
    if value.description.trim().is_empty() {
        return Err(WireError::new("plan fragment description cannot be empty"));
    }
    Ok(PlanFragment::new(value.description))
}

fn output_contract_to_wire(value: &OutputContract) -> v1::OutputContract {
    v1::OutputContract {
        kind: output_contract_kind_to_wire(value.kind()) as i32,
        description: value.description().to_owned(),
        descriptor: value.descriptor().map(output_contract_descriptor_to_wire),
    }
}

fn output_contract_from_wire(value: v1::OutputContract) -> WireResult<OutputContract> {
    if value.description.trim().is_empty() {
        return Err(WireError::new(
            "output contract description cannot be empty",
        ));
    }
    let contract = OutputContract::new(
        output_contract_kind_from_wire(value.kind)?,
        value.description,
    );
    match value.descriptor {
        Some(descriptor) => {
            Ok(contract.with_descriptor(output_contract_descriptor_from_wire(descriptor)?))
        }
        None => Ok(contract),
    }
}

fn output_contract_descriptor_to_wire(
    value: &OutputContractDescriptor,
) -> v1::OutputContractDescriptor {
    match value {
        OutputContractDescriptor::InlineRecordBatches => v1::OutputContractDescriptor {
            kind: v1::OutputContractDescriptorKind::InlineRecordBatches as i32,
            ..Default::default()
        },
        OutputContractDescriptor::LocalFile { path } => v1::OutputContractDescriptor {
            kind: v1::OutputContractDescriptorKind::LocalFile as i32,
            path: path.clone(),
            ..Default::default()
        },
        OutputContractDescriptor::Shuffle { partition } => v1::OutputContractDescriptor {
            kind: v1::OutputContractDescriptorKind::Shuffle as i32,
            shuffle_partition: partition.clone(),
            ..Default::default()
        },
        OutputContractDescriptor::ObjectParquetSink {
            base_dir,
            object_path,
        } => v1::OutputContractDescriptor {
            kind: v1::OutputContractDescriptorKind::ObjectParquetSink as i32,
            object_base_dir: base_dir.clone(),
            object_path: object_path.clone(),
            ..Default::default()
        },
        OutputContractDescriptor::ParquetSink { path } => v1::OutputContractDescriptor {
            kind: v1::OutputContractDescriptorKind::ParquetSink as i32,
            path: path.clone(),
            ..Default::default()
        },
    }
}

fn output_contract_descriptor_from_wire(
    value: v1::OutputContractDescriptor,
) -> WireResult<OutputContractDescriptor> {
    let kind = v1::OutputContractDescriptorKind::try_from(value.kind).map_err(|_| {
        WireError::new(format!(
            "invalid or unrecognized output contract descriptor kind ID: {}",
            value.kind
        ))
    })?;

    match kind {
        v1::OutputContractDescriptorKind::Unspecified => Err(WireError::new(
            "output contract descriptor kind must be specified",
        )),
        v1::OutputContractDescriptorKind::InlineRecordBatches => {
            Ok(OutputContractDescriptor::InlineRecordBatches)
        }
        v1::OutputContractDescriptorKind::LocalFile => {
            require_non_empty(&value.path, "local file output path")?;
            Ok(OutputContractDescriptor::LocalFile { path: value.path })
        }
        v1::OutputContractDescriptorKind::Shuffle => {
            require_non_empty(&value.shuffle_partition, "shuffle output partition")?;
            Ok(OutputContractDescriptor::Shuffle {
                partition: value.shuffle_partition,
            })
        }
        v1::OutputContractDescriptorKind::ObjectParquetSink => {
            require_non_empty(&value.object_base_dir, "object parquet sink base dir")?;
            require_non_empty(&value.object_path, "object parquet sink path")?;
            Ok(OutputContractDescriptor::ObjectParquetSink {
                base_dir: value.object_base_dir,
                object_path: value.object_path,
            })
        }
        v1::OutputContractDescriptorKind::ParquetSink => {
            require_non_empty(&value.path, "parquet sink path")?;
            Ok(OutputContractDescriptor::ParquetSink { path: value.path })
        }
    }
}

fn require_non_empty(value: &str, field: &'static str) -> WireResult<()> {
    if value.trim().is_empty() {
        Err(WireError::new(format!("{field} cannot be empty")))
    } else {
        Ok(())
    }
}

fn non_empty_string(value: String) -> Option<String> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn executor_state_to_wire(value: ExecutorState) -> v1::ExecutorState {
    match value {
        ExecutorState::Registered => v1::ExecutorState::Registered,
        ExecutorState::Healthy => v1::ExecutorState::Healthy,
        ExecutorState::Lost => v1::ExecutorState::Lost,
        ExecutorState::Draining => v1::ExecutorState::Draining,
        ExecutorState::Removed => v1::ExecutorState::Removed,
    }
}

fn executor_state_from_wire(value: i32) -> WireResult<ExecutorState> {
    match v1::ExecutorState::try_from(value)
        .map_err(|_| WireError::new(format!("unknown executor state value {value}")))?
    {
        v1::ExecutorState::Unspecified => {
            Err(WireError::new("executor state cannot be unspecified"))
        }
        v1::ExecutorState::Registered => Ok(ExecutorState::Registered),
        v1::ExecutorState::Healthy => Ok(ExecutorState::Healthy),
        v1::ExecutorState::Lost => Ok(ExecutorState::Lost),
        v1::ExecutorState::Draining => Ok(ExecutorState::Draining),
        v1::ExecutorState::Removed => Ok(ExecutorState::Removed),
    }
}

fn task_state_to_wire(value: TaskState) -> v1::TaskState {
    match value {
        TaskState::Pending => v1::TaskState::Pending,
        TaskState::Assigned => v1::TaskState::Assigned,
        TaskState::Running => v1::TaskState::Running,
        TaskState::Succeeded => v1::TaskState::Succeeded,
        TaskState::Failed => v1::TaskState::Failed,
        TaskState::Retrying => v1::TaskState::Retrying,
        TaskState::Cancelled => v1::TaskState::Cancelled,
    }
}

fn task_state_from_wire(value: i32) -> WireResult<TaskState> {
    match v1::TaskState::try_from(value)
        .map_err(|_| WireError::new(format!("unknown task state value {value}")))?
    {
        v1::TaskState::Unspecified => Err(WireError::new("task state cannot be unspecified")),
        v1::TaskState::Pending => Ok(TaskState::Pending),
        v1::TaskState::Assigned => Ok(TaskState::Assigned),
        v1::TaskState::Running => Ok(TaskState::Running),
        v1::TaskState::Succeeded => Ok(TaskState::Succeeded),
        v1::TaskState::Failed => Ok(TaskState::Failed),
        v1::TaskState::Retrying => Ok(TaskState::Retrying),
        v1::TaskState::Cancelled => Ok(TaskState::Cancelled),
    }
}

fn output_contract_kind_to_wire(value: OutputContractKind) -> v1::OutputContractKind {
    match value {
        OutputContractKind::InlineRecordBatches => v1::OutputContractKind::InlineRecordBatches,
        OutputContractKind::LocalFile => v1::OutputContractKind::LocalFile,
        OutputContractKind::Shuffle => v1::OutputContractKind::Shuffle,
        OutputContractKind::Sink => v1::OutputContractKind::Sink,
    }
}

fn output_contract_kind_from_wire(value: i32) -> WireResult<OutputContractKind> {
    match v1::OutputContractKind::try_from(value)
        .map_err(|_| WireError::new(format!("unknown output contract kind value {value}")))?
    {
        v1::OutputContractKind::Unspecified => {
            Err(WireError::new("output contract kind cannot be unspecified"))
        }
        v1::OutputContractKind::InlineRecordBatches => Ok(OutputContractKind::InlineRecordBatches),
        v1::OutputContractKind::LocalFile => Ok(OutputContractKind::LocalFile),
        v1::OutputContractKind::Shuffle => Ok(OutputContractKind::Shuffle),
        v1::OutputContractKind::Sink => Ok(OutputContractKind::Sink),
    }
}

fn transport_disposition_to_wire(value: TransportDisposition) -> v1::TransportDisposition {
    match value {
        TransportDisposition::Accepted => v1::TransportDisposition::Accepted,
        TransportDisposition::Rejected => v1::TransportDisposition::Rejected,
        TransportDisposition::Duplicate => v1::TransportDisposition::Duplicate,
        TransportDisposition::StaleAttempt => v1::TransportDisposition::StaleAttempt,
        TransportDisposition::StaleLease => v1::TransportDisposition::StaleLease,
        TransportDisposition::UnknownJob => v1::TransportDisposition::UnknownJob,
        TransportDisposition::UnknownTask => v1::TransportDisposition::UnknownTask,
        TransportDisposition::UnknownExecutor => v1::TransportDisposition::UnknownExecutor,
    }
}

fn transport_disposition_from_wire(value: i32) -> WireResult<TransportDisposition> {
    match v1::TransportDisposition::try_from(value)
        .map_err(|_| WireError::new(format!("unknown transport disposition value {value}")))?
    {
        v1::TransportDisposition::Unspecified => Err(WireError::new(
            "transport disposition cannot be unspecified",
        )),
        v1::TransportDisposition::Accepted => Ok(TransportDisposition::Accepted),
        v1::TransportDisposition::Rejected => Ok(TransportDisposition::Rejected),
        v1::TransportDisposition::Duplicate => Ok(TransportDisposition::Duplicate),
        v1::TransportDisposition::StaleAttempt => Ok(TransportDisposition::StaleAttempt),
        v1::TransportDisposition::StaleLease => Ok(TransportDisposition::StaleLease),
        v1::TransportDisposition::UnknownJob => Ok(TransportDisposition::UnknownJob),
        v1::TransportDisposition::UnknownTask => Ok(TransportDisposition::UnknownTask),
        v1::TransportDisposition::UnknownExecutor => Ok(TransportDisposition::UnknownExecutor),
    }
}

/// Convert a domain checkpoint ack request to protobuf.
pub fn checkpoint_ack_request_to_wire(value: CheckpointAckRequest) -> v1::CheckpointAckRequest {
    v1::CheckpointAckRequest {
        job_id: value.job_id.as_str().to_owned(),
        operator_id: value.operator_id,
        task_id: value.task_id.as_str().to_owned(),
        epoch: value.epoch,
        fencing_token: value.fencing_token.as_u64(),
        source_offsets: value
            .source_offsets
            .into_iter()
            .map(|o| v1::CheckpointSourceOffset {
                partition_id: o.partition_id,
                offset: o.offset,
            })
            .collect(),
        snapshot_path: value.snapshot_path.unwrap_or_default(),
    }
}

/// Convert a protobuf checkpoint ack request to the domain contract.
pub fn checkpoint_ack_request_from_wire(
    value: v1::CheckpointAckRequest,
) -> WireResult<CheckpointAckRequest> {
    let job_id = JobId::try_new(value.job_id).map_err(WireError::from_id)?;
    let task_id = TaskId::try_new(value.task_id).map_err(WireError::from_id)?;
    let fencing_token = FencingToken::try_new(value.fencing_token).map_err(WireError::from_id)?;
    let source_offsets = value
        .source_offsets
        .into_iter()
        .map(|o| CheckpointSourceOffset {
            partition_id: o.partition_id,
            offset: o.offset,
        })
        .collect();
    let snapshot_path = if value.snapshot_path.is_empty() {
        None
    } else {
        Some(value.snapshot_path)
    };
    Ok(CheckpointAckRequest {
        job_id,
        operator_id: value.operator_id,
        task_id,
        epoch: value.epoch,
        fencing_token,
        source_offsets,
        snapshot_path,
    })
}

/// Convert a domain checkpoint ack response to protobuf.
pub fn checkpoint_ack_response_to_wire(value: CheckpointAckResponse) -> v1::CheckpointAckResponse {
    use v1::checkpoint_ack_response::Result as WireResult;
    let result = match value {
        CheckpointAckResponse::Accepted => WireResult::Accepted(v1::CheckpointAckAccepted {}),
        CheckpointAckResponse::StaleEpoch { current_epoch } => {
            WireResult::StaleEpoch(v1::CheckpointAckStaleEpoch { current_epoch })
        }
        CheckpointAckResponse::JobNotFound => {
            WireResult::JobNotFound(v1::CheckpointAckJobNotFound {})
        }
        CheckpointAckResponse::StaleFencingToken { current_token } => {
            WireResult::StaleFencingToken(v1::CheckpointAckStaleFencingToken { current_token })
        }
    };
    v1::CheckpointAckResponse {
        result: Some(result),
    }
}

/// Convert a protobuf checkpoint ack response to the domain contract.
pub fn checkpoint_ack_response_from_wire(
    value: v1::CheckpointAckResponse,
) -> WireResult<CheckpointAckResponse> {
    use v1::checkpoint_ack_response::Result as WireVariant;
    match value.result {
        Some(WireVariant::Accepted(_)) => Ok(CheckpointAckResponse::Accepted),
        Some(WireVariant::StaleEpoch(s)) => Ok(CheckpointAckResponse::StaleEpoch {
            current_epoch: s.current_epoch,
        }),
        Some(WireVariant::JobNotFound(_)) => Ok(CheckpointAckResponse::JobNotFound),
        Some(WireVariant::StaleFencingToken(s)) => Ok(CheckpointAckResponse::StaleFencingToken {
            current_token: s.current_token,
        }),
        None => Err(WireError::new(
            "missing required field `checkpoint_ack_response.result`",
        )),
    }
}

impl WireError {
    fn from_id(value: crate::ids::IdError) -> Self {
        Self::new(value.to_string())
    }
}
