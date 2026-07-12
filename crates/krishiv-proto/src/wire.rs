//! Generated protobuf wire conversions.

use crate::checkpoint::{
    CheckpointAckRequest, CheckpointAckResponse, CheckpointSourceOffset, SinkTransactionRef,
};
use crate::executor::TraceContext;
use crate::executor::{
    DeregisterExecutorRequest, DeregisterExecutorResponse, ExecutorDescriptor,
    HeartbeatThrottleCommand, ShufflePartitionOutput, TaskOutputMetadata, TaskRuntimeStats,
};
use crate::ids::{
    AttemptId, ExecutorId, FencingToken, JobId, LeaseGeneration, OperatorId, PartitionId, StageId,
    TaskId, TransportVersion,
};
use crate::lifecycle::{ExecutorState, TaskState};
use crate::task::{
    ExecutorHeartbeatRequest, ExecutorHeartbeatResponse, ExecutorTaskAssignment, InputPartition,
    InputPartitionDescriptor, KeyGroupRange, MemoryKafkaRecord, MissingShufflePartition,
    OutputContract, OutputContractDescriptor, OutputContractKind, PlanFragment,
    PushTaskResultResponse, RegisterExecutorRequest, RegisterExecutorResponse, TaskAttemptRef,
    TaskCancellationRequest, TaskResultChunk, TaskStatusRequest, TaskStatusResponse,
    TransportDisposition,
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

fn trace_context_to_wire(ctx: Option<&TraceContext>) -> (String, String) {
    match ctx {
        Some(c) => (c.traceparent.clone(), c.tracestate.clone()),
        None => (String::new(), String::new()),
    }
}

fn trace_context_from_wire(traceparent: String, tracestate: String) -> Option<TraceContext> {
    if traceparent.is_empty() {
        None
    } else {
        Some(TraceContext {
            traceparent,
            tracestate,
        })
    }
}

/// Convert a domain registration request to protobuf.
pub fn register_executor_request_to_wire(
    value: RegisterExecutorRequest,
) -> v1::RegisterExecutorRequest {
    let (trace_parent, trace_state) = trace_context_to_wire(value.trace_context());
    v1::RegisterExecutorRequest {
        version: Some(transport_version_to_wire(value.version())),
        descriptor: Some(executor_descriptor_to_wire(value.descriptor())),
        trace_parent,
        trace_state,
    }
}

/// Convert a protobuf registration request to the domain contract.
pub fn register_executor_request_from_wire(
    value: v1::RegisterExecutorRequest,
) -> WireResult<RegisterExecutorRequest> {
    let version = transport_version_from_wire(required(value.version, "version")?)?;
    let descriptor = executor_descriptor_from_wire(required(value.descriptor, "descriptor")?)?;
    let mut req = RegisterExecutorRequest::new(descriptor).with_version(version);
    if let Some(ctx) = trace_context_from_wire(value.trace_parent, value.trace_state) {
        req = req.with_trace_context(ctx);
    }
    Ok(req)
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
    let (trace_parent, trace_state) = trace_context_to_wire(value.trace_context());
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
        hot_key_reports: value
            .hot_key_reports()
            .iter()
            .map(hot_key_report_to_wire)
            .collect(),
        streaming_task_states: value
            .streaming_task_states()
            .iter()
            .map(streaming_task_state_to_wire)
            .collect(),
        trace_parent,
        trace_state,
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

    // Always send all resource fields so the coordinator always has the latest
    // values. Previously these were guarded by `> 0` checks which caused a
    // round-trip asymmetry: legitimate zero values (e.g. executor reporting
    // zero memory usage) were serialized as 0, then deserialized back as None
    // because the > 0 guard discarded them. The coordinator could never observe
    // a zero report vs. no report at all.
    req = req
        .with_memory_used_bytes(value.memory_used_bytes)
        .with_memory_limit_bytes(value.memory_limit_bytes)
        .with_active_task_count(value.active_task_count)
        .with_cpu_cores_used(value.cpu_cores_used)
        .with_network_bytes_sent(value.network_bytes_sent)
        .with_network_bytes_recv(value.network_bytes_recv);
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
                .collect::<WireResult<Vec<_>>>()?,
        );
    }
    if !value.hot_key_reports.is_empty() {
        req = req.with_hot_key_reports(
            value
                .hot_key_reports
                .into_iter()
                .map(hot_key_report_from_wire)
                .collect::<WireResult<Vec<_>>>()?,
        );
    }
    if !value.streaming_task_states.is_empty() {
        let states = value
            .streaming_task_states
            .into_iter()
            .map(streaming_task_state_from_wire)
            .collect::<WireResult<Vec<_>>>()?;
        req = req.with_streaming_task_states(states);
    }
    if let Some(ctx) = trace_context_from_wire(value.trace_parent, value.trace_state) {
        req = req.with_trace_context(ctx);
    }

    Ok(req)
}

/// Convert a domain heartbeat response to protobuf.
pub fn executor_heartbeat_response_to_wire(
    value: ExecutorHeartbeatResponse,
) -> v1::ExecutorHeartbeatResponse {
    let (trace_parent, trace_state) = trace_context_to_wire(value.trace_context());
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
        completed_checkpoints: value
            .checkpoint_complete_commands()
            .iter()
            .map(|cmd| v1::CheckpointCompleteCommand {
                job_id: cmd.job_id.as_str().to_owned(),
                epoch: cmd.epoch,
                fencing_token: cmd.fencing_token.as_u64(),
            })
            .collect(),
        restore_checkpoints: value
            .restore_commands()
            .iter()
            .map(|cmd| v1::RestoreFromCheckpointCommand {
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
        trace_parent,
        trace_state,
        global_watermarks: value
            .global_watermarks()
            .iter()
            .map(|(k, &v)| (k.as_str().to_owned(), v))
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
        let cmds = value
            .initiate_checkpoints
            .into_iter()
            .map(|cmd| {
                let job_id = JobId::try_new(cmd.job_id).map_err(WireError::from_id)?;
                let fencing_token =
                    FencingToken::try_new(cmd.fencing_token).map_err(WireError::from_id)?;
                Ok(InitiateCheckpointCommand {
                    job_id,
                    epoch: cmd.epoch,
                    fencing_token,
                })
            })
            .collect::<WireResult<Vec<_>>>()?;
        response = response.with_checkpoint_commands(cmds);
    }
    if !value.completed_checkpoints.is_empty() {
        use crate::ids::{FencingToken, JobId};
        use crate::task::CheckpointCompleteCommand;
        let cmds = value
            .completed_checkpoints
            .into_iter()
            .map(|cmd| {
                let job_id = JobId::try_new(cmd.job_id).map_err(WireError::from_id)?;
                let fencing_token =
                    FencingToken::try_new(cmd.fencing_token).map_err(WireError::from_id)?;
                Ok(CheckpointCompleteCommand {
                    job_id,
                    epoch: cmd.epoch,
                    fencing_token,
                })
            })
            .collect::<WireResult<Vec<_>>>()?;
        response = response.with_checkpoint_complete_commands(cmds);
    }
    if !value.restore_checkpoints.is_empty() {
        use crate::ids::{FencingToken, JobId};
        use crate::task::RestoreFromCheckpointCommand;
        let cmds = value
            .restore_checkpoints
            .into_iter()
            .map(|cmd| {
                let job_id = JobId::try_new(cmd.job_id).map_err(WireError::from_id)?;
                let fencing_token =
                    FencingToken::try_new(cmd.fencing_token).map_err(WireError::from_id)?;
                Ok(RestoreFromCheckpointCommand {
                    job_id,
                    epoch: cmd.epoch,
                    fencing_token,
                })
            })
            .collect::<WireResult<Vec<_>>>()?;
        response = response.with_restore_commands(cmds);
    }
    if let Some(ctx) = trace_context_from_wire(value.trace_parent, value.trace_state) {
        response = response.with_trace_context(ctx);
    }
    if !value.global_watermarks.is_empty() {
        let watermarks: std::collections::HashMap<_, _> = value
            .global_watermarks
            .into_iter()
            .filter_map(|(k, v)| match JobId::try_new(k.clone()) {
                Ok(id) => Some((id, v)),
                Err(e) => {
                    tracing::warn!(key = %k, error = %e, "global_watermarks: skipping malformed JobId");
                    None
                }
            })
            .collect();
        response = response.with_global_watermarks(watermarks);
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
        job_id: value.job_id.as_str().to_owned(),
        task_id: value.task_id.as_str().to_owned(),
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
) -> WireResult<crate::StreamingProgressReport> {
    let job_id = JobId::try_new(value.job_id).map_err(WireError::from_id)?;
    let task_id = TaskId::try_new(value.task_id).map_err(WireError::from_id)?;
    Ok(crate::StreamingProgressReport {
        job_id,
        task_id,
        watermark_ms: value.watermark_ms,
        rows_emitted: value.rows_emitted,
        batches_emitted: value.batches_emitted,
        state_bytes: value.state_bytes,
        source_offset: value.source_offset,
        timestamp_ms: value.timestamp_ms,
    })
}

fn hot_key_report_to_wire(value: &crate::HeartbeatHotKeyReport) -> v1::HeartbeatHotKeyReport {
    v1::HeartbeatHotKeyReport {
        key: value.key.clone(),
        estimated_count: value.estimated_count,
        max_error: value.max_error,
        heat_score: value.heat_score,
        job_id: value.job_id.as_str().to_owned(),
        source_id: value.source_id.clone(),
    }
}

fn hot_key_report_from_wire(
    value: v1::HeartbeatHotKeyReport,
) -> WireResult<crate::HeartbeatHotKeyReport> {
    let job_id = JobId::try_new(value.job_id).map_err(WireError::from_id)?;
    Ok(crate::HeartbeatHotKeyReport {
        key: value.key,
        estimated_count: value.estimated_count,
        max_error: value.max_error,
        heat_score: value.heat_score,
        job_id,
        source_id: value.source_id,
    })
}

fn streaming_task_state_to_wire(value: &crate::StreamingTaskState) -> v1::StreamingTaskStateWire {
    v1::StreamingTaskStateWire {
        task_id: value.task_id.as_str().to_owned(),
        // Proto field is uint64; cast preserves bit pattern for negative sentinel values.
        watermark_ms: value.watermark_ms as u64,
        source_offset: value.source_offset.clone(),
    }
}

fn streaming_task_state_from_wire(
    value: v1::StreamingTaskStateWire,
) -> WireResult<crate::StreamingTaskState> {
    let task_id = TaskId::try_new(value.task_id).map_err(WireError::from_id)?;
    Ok(crate::StreamingTaskState::new(
        task_id,
        // Proto field is uint64; cast preserves bit pattern for negative sentinel values.
        value.watermark_ms as i64,
        value.source_offset,
    ))
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
    } else {
        // `throttle_cleared` is the presence flag. A zero rows/s command is an
        // intentional source pause, not the same as clearing the throttle.
        Some(value.rows_per_second)
    };
    HeartbeatThrottleCommand {
        source_id: value.source_id,
        rows_per_second,
    }
}

/// Convert a domain executor task assignment to protobuf.
pub fn executor_task_assignment_to_wire(
    value: ExecutorTaskAssignment,
) -> WireResult<v1::ExecutorTaskAssignment> {
    let (trace_parent, trace_state) = trace_context_to_wire(value.trace_context());
    Ok(v1::ExecutorTaskAssignment {
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
            .collect::<Result<Vec<_>, _>>()?,
        plan_fragment: Some(plan_fragment_to_wire(value.plan_fragment())),
        output_contract: Some(output_contract_to_wire(value.output_contract())),
        task_timeout_secs: value.task_timeout_secs().unwrap_or(0),
        has_task_timeout_secs: value.task_timeout_secs().is_some(),
        key_group_range_start: value.key_group_range().start(),
        key_group_range_end: value.key_group_range().end(),
        has_key_group_range: true,
        cpu_limit_nanos: value.cpu_limit_nanos().unwrap_or(0),
        has_cpu_limit_nanos: value.cpu_limit_nanos().is_some(),
        memory_limit_bytes: value.memory_limit_bytes().unwrap_or(0),
        has_memory_limit_bytes: value.memory_limit_bytes().is_some(),
        shuffle_write: value.shuffle_write().map(shuffle_write_config_to_wire),
        shuffle_read: value.shuffle_read().map(shuffle_read_config_to_wire),
        requires_reattach: value.requires_reattach(),
        trace_parent,
        trace_state,
    })
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
    if value.has_task_timeout_secs {
        assignment = assignment.with_task_timeout_secs(value.task_timeout_secs);
    }
    if value.has_key_group_range {
        assignment = assignment.with_key_group_range(
            KeyGroupRange::try_new(value.key_group_range_start, value.key_group_range_end)
                .map_err(WireError::from_id)?,
        );
    }
    if value.has_cpu_limit_nanos {
        assignment = assignment.with_cpu_limit_nanos(value.cpu_limit_nanos);
    }
    if value.has_memory_limit_bytes {
        assignment = assignment.with_memory_limit_bytes(value.memory_limit_bytes);
    }
    if let Some(sw) = value.shuffle_write {
        assignment = assignment.with_shuffle_write(shuffle_write_config_from_wire(sw)?);
    }
    if let Some(sr) = value.shuffle_read {
        assignment = assignment.with_shuffle_read(shuffle_read_config_from_wire(sr)?);
    }
    if value.requires_reattach {
        assignment = assignment.with_requires_reattach(true);
    }
    if let Some(ctx) = trace_context_from_wire(value.trace_parent, value.trace_state) {
        assignment = assignment.with_trace_context(ctx);
    }
    Ok(assignment)
}

/// Convert a domain task status request to protobuf.
pub fn task_status_request_to_wire(value: TaskStatusRequest) -> v1::TaskStatusRequest {
    let (trace_parent, trace_state) = trace_context_to_wire(value.trace_context());
    let missing_shuffle_partitions = value
        .missing_shuffle_partitions()
        .iter()
        .map(|m| v1::MissingShufflePartitionWire {
            stage_id: m.stage_id().as_str().to_owned(),
            partition_id: m.partition_id(),
        })
        .collect();
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
        trace_parent,
        trace_state,
        missing_shuffle_partitions,
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
    if let Some(ctx) = trace_context_from_wire(value.trace_parent, value.trace_state) {
        request = request.with_trace_context(ctx);
    }
    if !value.missing_shuffle_partitions.is_empty() {
        let mut missing = Vec::with_capacity(value.missing_shuffle_partitions.len());
        for m in value.missing_shuffle_partitions {
            let stage_id = StageId::try_new(m.stage_id).map_err(WireError::from_id)?;
            missing.push(MissingShufflePartition::new(stage_id, m.partition_id));
        }
        request = request.with_missing_shuffle_partitions(missing);
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
    let (trace_parent, trace_state) = trace_context_to_wire(value.trace_context());
    v1::TaskStatusResponse {
        version: Some(transport_version_to_wire(value.version())),
        disposition: transport_disposition_to_wire(value.disposition()) as i32,
        message: value.message().unwrap_or_default().to_owned(),
        trace_parent,
        trace_state,
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
    if let Some(ctx) = trace_context_from_wire(value.trace_parent, value.trace_state) {
        response = response.with_trace_context(ctx);
    }
    Ok(response)
}

/// Convert a domain task result chunk to protobuf.
pub fn task_result_chunk_to_wire(value: TaskResultChunk) -> v1::TaskResultChunk {
    v1::TaskResultChunk {
        version: Some(transport_version_to_wire(value.version())),
        job_id: value.job_id().as_str().to_owned(),
        stage_id: value.stage_id().as_str().to_owned(),
        task_id: value.task_id().as_str().to_owned(),
        attempt_id: value.attempt_id().as_u32(),
        last: value.last(),
        total_bytes: value.total_bytes(),
        data: value.into_data(),
    }
}

/// Convert a protobuf task result chunk to the domain contract.
pub fn task_result_chunk_from_wire(value: v1::TaskResultChunk) -> WireResult<TaskResultChunk> {
    let version = transport_version_from_wire(required(value.version, "version")?)?;
    let ids = TaskAttemptRef::new(
        JobId::try_new(value.job_id).map_err(WireError::from_id)?,
        StageId::try_new(value.stage_id).map_err(WireError::from_id)?,
        TaskId::try_new(value.task_id).map_err(WireError::from_id)?,
        AttemptId::try_new(value.attempt_id).map_err(WireError::from_id)?,
    );
    let mut chunk = TaskResultChunk::new(ids, value.data).with_version(version);
    if value.last {
        chunk = chunk.with_last(value.total_bytes);
    }
    Ok(chunk)
}

/// Convert a domain push-task-result response to protobuf.
pub fn push_task_result_response_to_wire(
    value: PushTaskResultResponse,
) -> v1::PushTaskResultResponse {
    v1::PushTaskResultResponse {
        version: Some(transport_version_to_wire(value.version())),
        disposition: transport_disposition_to_wire(value.disposition()) as i32,
        message: value.message().unwrap_or_default().to_owned(),
    }
}

/// Convert a protobuf push-task-result response to the domain contract.
pub fn push_task_result_response_from_wire(
    value: v1::PushTaskResultResponse,
) -> WireResult<PushTaskResultResponse> {
    let version = transport_version_from_wire(required(value.version, "version")?)?;
    let disposition = transport_disposition_from_wire(value.disposition)?;
    let mut response = PushTaskResultResponse::new(disposition).with_version(version);
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
        // Keep deprecated parallel arrays for backward compat with older decoders.
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
        memory_bytes: value.runtime_stats().map_or(0, |s| s.memory_bytes),
        shuffle_partitions: value
            .shuffle_partitions()
            .iter()
            .map(|p| v1::ShufflePartitionOutputWire {
                partition_id: p.partition_id,
                size_bytes: p.size_bytes,
                flight_endpoint: p.flight_endpoint.clone(),
            })
            .collect(),
        watermark_ms: value.watermark_ms().unwrap_or(0),
        has_watermark_ms: value.watermark_ms().is_some(),
        has_runtime_stats: value.runtime_stats().is_some(),
        hot_key_reports: value
            .hot_key_reports()
            .iter()
            .map(hot_key_report_to_wire)
            .collect(),
        sink_staged_files: value.sink_staged_files().to_vec(),
        state_snapshot: value
            .state_snapshot()
            .map(<[u8]>::to_vec)
            .unwrap_or_default(),
        spooled_result_total_bytes: value.spooled_result_total_bytes(),
    }
}

fn task_output_metadata_from_wire(value: v1::TaskOutputMetadata) -> WireResult<TaskOutputMetadata> {
    if value.output_kind.trim().is_empty() {
        return Err(WireError::new("task output metadata kind cannot be empty"));
    }
    // Prefer structured shuffle_partitions (field 14); fall back to deprecated parallel arrays.
    let shuffle_partitions: Vec<ShufflePartitionOutput> = if !value.shuffle_partitions.is_empty() {
        value
            .shuffle_partitions
            .into_iter()
            .map(|p| ShufflePartitionOutput::new(p.partition_id, p.size_bytes, p.flight_endpoint))
            .collect()
    } else {
        let ids_len = value.shuffle_partition_ids.len();
        let bytes_len = value.shuffle_partition_bytes.len();
        let endpoints_len = value.shuffle_flight_endpoints.len();
        if (bytes_len > 0 && bytes_len != ids_len)
            || (endpoints_len > 0 && endpoints_len != ids_len)
        {
            return Err(WireError::new(format!(
                "mismatched deprecated shuffle arrays: ids={ids_len} bytes={bytes_len} endpoints={endpoints_len}"
            )));
        }
        value
            .shuffle_partition_ids
            .into_iter()
            .zip(value.shuffle_partition_bytes)
            .zip(value.shuffle_flight_endpoints)
            .map(|((id, bytes), endpoint)| ShufflePartitionOutput::new(id, bytes, endpoint))
            .collect()
    };
    let mut meta = TaskOutputMetadata::new(
        value.output_kind,
        value.row_count,
        value.batch_count,
        value.column_count,
    );
    // Derive serialized_bytes before moving shuffle_partitions into meta.
    // This gives AQE rules the actual wire/disk cost of the shuffle exchange
    // rather than peak in-memory footprint, which can be 2-4× larger.
    let serialized_bytes: u64 = shuffle_partitions.iter().map(|p| p.size_bytes).sum();
    if !shuffle_partitions.is_empty() {
        meta = meta.with_shuffle_partitions(shuffle_partitions);
    }
    if !value.inline_record_batch_ipc.is_empty() {
        meta = meta.with_inline_record_batch_ipc(value.inline_record_batch_ipc);
    }
    // Use has_runtime_stats flag (field 17) when set; fall back to non-zero check for older messages.
    let has_stats = value.has_runtime_stats
        || value.input_rows > 0
        || value.output_rows > 0
        || value.cpu_nanos > 0
        || value.spill_bytes > 0
        || value.memory_bytes > 0;
    if has_stats {
        meta = meta.with_runtime_stats(TaskRuntimeStats {
            input_rows: value.input_rows,
            output_rows: value.output_rows,
            cpu_nanos: value.cpu_nanos,
            memory_bytes: value.memory_bytes,
            spill_bytes: value.spill_bytes,
            serialized_bytes,
        });
    }
    if value.has_watermark_ms {
        meta = meta.with_watermark_ms(value.watermark_ms);
    }
    if !value.hot_key_reports.is_empty() {
        let reports = value
            .hot_key_reports
            .into_iter()
            .map(hot_key_report_from_wire)
            .collect::<WireResult<Vec<_>>>()?;
        meta = meta.with_hot_key_reports(reports);
    }
    if !value.sink_staged_files.is_empty() {
        meta = meta.with_sink_staged_files(value.sink_staged_files);
    }
    if !value.state_snapshot.is_empty() {
        meta = meta.with_state_snapshot(value.state_snapshot);
    }
    if value.spooled_result_total_bytes > 0 {
        meta = meta.with_spooled_result_total_bytes(value.spooled_result_total_bytes);
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
        rack_id: value.rack_id().unwrap_or_default().to_owned(),
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
    if !value.rack_id.is_empty() {
        descriptor = descriptor.with_rack_id(value.rack_id);
    }
    Ok(descriptor)
}

fn shuffle_write_config_to_wire(
    value: &crate::io::ShuffleWriteConfig,
) -> v1::ShuffleWriteConfigWire {
    v1::ShuffleWriteConfigWire {
        stage_id: value.stage_id.as_str().to_owned(),
        num_partitions: value.num_partitions as u64,
        key_columns: value.key_columns.clone(),
        lease_token: value.lease_token,
    }
}

fn shuffle_write_config_from_wire(
    value: v1::ShuffleWriteConfigWire,
) -> WireResult<crate::io::ShuffleWriteConfig> {
    require_non_empty(&value.stage_id, "shuffle write stage id")?;
    let num_partitions = value
        .num_partitions
        .try_into()
        .map_err(|_| WireError::new("shuffle write num_partitions overflows usize"))?;
    Ok(crate::io::ShuffleWriteConfig {
        stage_id: StageId::try_new(value.stage_id).map_err(WireError::from_id)?,
        num_partitions,
        key_columns: value.key_columns,
        lease_token: value.lease_token,
    })
}

fn shuffle_read_config_to_wire(value: &crate::io::ShuffleReadConfig) -> v1::ShuffleReadConfigWire {
    v1::ShuffleReadConfigWire {
        stage_id: value.stage_id.as_str().to_owned(),
        partition_id: value.partition_id as u64,
        lease_token: value.lease_token,
    }
}

fn shuffle_read_config_from_wire(
    value: v1::ShuffleReadConfigWire,
) -> WireResult<crate::io::ShuffleReadConfig> {
    require_non_empty(&value.stage_id, "shuffle read stage id")?;
    let partition_id = value
        .partition_id
        .try_into()
        .map_err(|_| WireError::new("shuffle read partition_id overflows usize"))?;
    Ok(crate::io::ShuffleReadConfig {
        stage_id: StageId::try_new(value.stage_id).map_err(WireError::from_id)?,
        partition_id,
        lease_token: value.lease_token,
    })
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

fn input_partition_to_wire(value: &InputPartition) -> WireResult<v1::InputPartition> {
    Ok(v1::InputPartition {
        partition_id: value.partition_id().to_owned(),
        description: value.description().to_owned(),
        descriptor: value
            .descriptor()
            .map(input_partition_descriptor_to_wire)
            .transpose()?,
    })
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
) -> WireResult<v1::InputPartitionDescriptor> {
    match value {
        InputPartitionDescriptor::LocalParquet { table_name, path } => {
            Ok(v1::InputPartitionDescriptor {
                kind: v1::InputPartitionDescriptorKind::LocalParquet as i32,
                table_name: table_name.clone(),
                path: path.clone(),
                ..Default::default()
            })
        }
        InputPartitionDescriptor::ConnectorParquet { table_name, path } => {
            Ok(v1::InputPartitionDescriptor {
                kind: v1::InputPartitionDescriptorKind::ConnectorParquet as i32,
                table_name: table_name.clone().unwrap_or_default(),
                path: path.clone(),
                ..Default::default()
            })
        }
        InputPartitionDescriptor::ObjectParquet {
            table_name,
            base_dir,
            object_path,
        } => Ok(v1::InputPartitionDescriptor {
            kind: v1::InputPartitionDescriptorKind::ObjectParquet as i32,
            table_name: table_name.clone(),
            object_base_dir: base_dir.clone(),
            object_path: object_path.clone(),
            ..Default::default()
        }),
        InputPartitionDescriptor::MemoryKafka {
            topic,
            partition,
            start_offset,
            records,
        } => Ok(v1::InputPartitionDescriptor {
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
        }),
        InputPartitionDescriptor::ShuffleFlight {
            table_name,
            flight_endpoint,
            job_id,
            upstream_stage_id,
            partition_id,
        } => Ok(v1::InputPartitionDescriptor {
            kind: v1::InputPartitionDescriptorKind::ShuffleFlight as i32,
            table_name: table_name.clone(),
            shuffle_flight_endpoint: flight_endpoint.clone(),
            shuffle_job_id: job_id.as_str().to_owned(),
            shuffle_upstream_stage_id: upstream_stage_id.as_str().to_owned(),
            shuffle_partition_id: *partition_id,
            ..Default::default()
        }),
        InputPartitionDescriptor::InlineIpc {
            table_name,
            ipc_bytes,
        } => Ok(v1::InputPartitionDescriptor {
            kind: v1::InputPartitionDescriptorKind::InlineIpc as i32,
            table_name: table_name.clone(),
            ipc_bytes: ipc_bytes.clone(),
            ..Default::default()
        }),
        // InMemory is in-process only and must never reach the wire.
        InputPartitionDescriptor::InMemory { table_name, .. } => Err(WireError::new(format!(
            "InputPartitionDescriptor::InMemory (table_name={table_name:?}) \
                 is in-process only and cannot be serialised to the wire; \
                 use InlineIpc for remote task assignments"
        ))),
        InputPartitionDescriptor::WatermarkHint { watermark_ms } => {
            Ok(v1::InputPartitionDescriptor {
                kind: v1::InputPartitionDescriptorKind::WatermarkHint as i32,
                watermark_ms: *watermark_ms,
                ..Default::default()
            })
        }
        InputPartitionDescriptor::ContinuousRestore {
            snapshot_bytes,
            watermark_ms,
        } => Ok(v1::InputPartitionDescriptor {
            kind: v1::InputPartitionDescriptorKind::ContinuousRestore as i32,
            ipc_bytes: snapshot_bytes.clone(),
            watermark_ms: *watermark_ms,
            ..Default::default()
        }),
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
            if value.ipc_bytes.is_empty() {
                return Err(WireError::new("inline ipc bytes cannot be empty"));
            }
            Ok(InputPartitionDescriptor::InlineIpc {
                table_name: value.table_name,
                ipc_bytes: value.ipc_bytes,
            })
        }
        v1::InputPartitionDescriptorKind::WatermarkHint => {
            Ok(InputPartitionDescriptor::WatermarkHint {
                watermark_ms: value.watermark_ms,
            })
        }
        v1::InputPartitionDescriptorKind::ContinuousRestore => {
            if value.ipc_bytes.is_empty() {
                return Err(WireError::new(
                    "continuous restore snapshot bytes cannot be empty",
                ));
            }
            Ok(InputPartitionDescriptor::ContinuousRestore {
                snapshot_bytes: value.ipc_bytes,
                watermark_ms: value.watermark_ms,
            })
        }
    }
}

fn plan_fragment_to_wire(value: &PlanFragment) -> v1::PlanFragment {
    v1::PlanFragment {
        description: value.description().to_owned(),
        is_streaming: value.is_streaming(),
        delta_batch_payload: None,
    }
}

fn plan_fragment_from_wire(value: v1::PlanFragment) -> WireResult<PlanFragment> {
    if value.description.trim().is_empty() {
        return Err(WireError::new("plan fragment description cannot be empty"));
    }
    Ok(PlanFragment::new(value.description).with_streaming(value.is_streaming))
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
        OutputContractDescriptor::IcebergSink {
            root,
            table,
            mode,
            key_columns,
            op_column,
        } => v1::OutputContractDescriptor {
            kind: v1::OutputContractDescriptorKind::IcebergSink as i32,
            iceberg_root: root.clone(),
            iceberg_table: table.clone(),
            iceberg_mode: mode.as_str().to_owned(),
            iceberg_key_columns: key_columns.clone(),
            iceberg_op_column: op_column.clone().unwrap_or_default(),
            ..Default::default()
        },
        OutputContractDescriptor::KafkaSink {
            bootstrap_servers,
            topic,
            transactional_id_prefix,
        } => v1::OutputContractDescriptor {
            kind: v1::OutputContractDescriptorKind::KafkaSink as i32,
            kafka_bootstrap_servers: bootstrap_servers.clone(),
            kafka_topic: topic.clone(),
            kafka_transactional_id_prefix: transactional_id_prefix.clone(),
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
        v1::OutputContractDescriptorKind::IcebergSink => {
            require_non_empty(&value.iceberg_root, "iceberg sink table root")?;
            require_non_empty(&value.iceberg_table, "iceberg sink table name")?;
            let mode = crate::IcebergSinkMode::parse(&value.iceberg_mode).ok_or_else(|| {
                WireError::new(format!(
                    "iceberg sink mode '{}' must be append or upsert",
                    value.iceberg_mode
                ))
            })?;
            Ok(OutputContractDescriptor::IcebergSink {
                root: value.iceberg_root,
                table: value.iceberg_table,
                mode,
                key_columns: value.iceberg_key_columns,
                op_column: non_empty_string(value.iceberg_op_column),
            })
        }
        v1::OutputContractDescriptorKind::KafkaSink => {
            require_non_empty(&value.kafka_bootstrap_servers, "kafka sink bootstrap servers")?;
            require_non_empty(&value.kafka_topic, "kafka sink topic")?;
            require_non_empty(
                &value.kafka_transactional_id_prefix,
                "kafka sink transactional id prefix",
            )?;
            Ok(OutputContractDescriptor::KafkaSink {
                bootstrap_servers: value.kafka_bootstrap_servers,
                topic: value.kafka_topic,
                transactional_id_prefix: value.kafka_transactional_id_prefix,
            })
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
        operator_id: value.operator_id.as_str().to_owned(),
        task_id: value.task_id.as_str().to_owned(),
        epoch: value.epoch,
        fencing_token: value.fencing_token.as_u64(),
        source_offsets: value
            .source_offsets
            .into_iter()
            .map(|o| v1::CheckpointSourceOffset {
                partition_id: o.partition_id.as_str().to_owned(),
                offset: o.offset,
                encoded_offset: o.encoded_offset,
            })
            .collect(),
        snapshot_path: value.snapshot_path.unwrap_or_default(),
        // DUR-2: carry prepared-sink transaction refs so the coordinator can
        // persist participant identity + prepared paths into the checkpoint.
        sink_transactions: value
            .sink_transactions
            .into_iter()
            .map(|s| v1::SinkTransactionRef {
                sink_id: s.sink_id,
                epoch: s.epoch,
                prepare_path: s.prepare_path,
                committed: s.committed,
            })
            .collect(),
    }
}

/// Convert a protobuf checkpoint ack request to the domain contract.
pub fn checkpoint_ack_request_from_wire(
    value: v1::CheckpointAckRequest,
) -> WireResult<CheckpointAckRequest> {
    let job_id = JobId::try_new(value.job_id).map_err(WireError::from_id)?;
    let operator_id = OperatorId::try_new(value.operator_id).map_err(WireError::from_id)?;
    let task_id = TaskId::try_new(value.task_id).map_err(WireError::from_id)?;
    let fencing_token = FencingToken::try_new(value.fencing_token).map_err(WireError::from_id)?;
    let source_offsets = value
        .source_offsets
        .into_iter()
        .map(|o| {
            let partition_id = PartitionId::try_new(o.partition_id).map_err(WireError::from_id)?;
            Ok(CheckpointSourceOffset {
                partition_id,
                offset: o.offset,
                encoded_offset: o.encoded_offset,
            })
        })
        .collect::<WireResult<Vec<_>>>()?;
    let snapshot_path = if value.snapshot_path.is_empty() {
        None
    } else {
        Some(value.snapshot_path)
    };
    Ok(CheckpointAckRequest {
        job_id,
        operator_id,
        task_id,
        epoch: value.epoch,
        fencing_token,
        source_offsets,
        snapshot_path,
        unaligned_buffers: Vec::new(),
        // DUR-2: recover prepared-sink transaction refs from the wire.
        sink_transactions: value
            .sink_transactions
            .into_iter()
            .map(|s| SinkTransactionRef {
                sink_id: s.sink_id,
                epoch: s.epoch,
                prepare_path: s.prepare_path,
                committed: s.committed,
            })
            .collect(),
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

pub fn push_continuous_input_request_to_wire(
    value: crate::task::PushContinuousInputRequest,
) -> v1::PushContinuousInputRequest {
    v1::PushContinuousInputRequest {
        version: Some(transport_version_to_wire(value.version)),
        job_id: value.job_id.as_str().to_owned(),
        task_id: value.task_id.as_str().to_owned(),
        ipc_bytes: value.ipc_bytes,
    }
}

pub fn push_continuous_input_request_from_wire(
    value: v1::PushContinuousInputRequest,
) -> WireResult<crate::task::PushContinuousInputRequest> {
    Ok(crate::task::PushContinuousInputRequest {
        version: transport_version_from_wire(required(value.version, "version")?)?,
        job_id: crate::ids::JobId::try_new(value.job_id).map_err(WireError::from_id)?,
        task_id: crate::ids::TaskId::try_new(value.task_id).map_err(WireError::from_id)?,
        ipc_bytes: value.ipc_bytes,
    })
}

pub fn drain_continuous_output_request_to_wire(
    value: crate::task::DrainContinuousOutputRequest,
) -> v1::DrainContinuousOutputRequest {
    v1::DrainContinuousOutputRequest {
        version: Some(transport_version_to_wire(value.version)),
        job_id: value.job_id.as_str().to_owned(),
        task_id: value.task_id.as_str().to_owned(),
    }
}

pub fn drain_continuous_output_request_from_wire(
    value: v1::DrainContinuousOutputRequest,
) -> WireResult<crate::task::DrainContinuousOutputRequest> {
    Ok(crate::task::DrainContinuousOutputRequest {
        version: transport_version_from_wire(required(value.version, "version")?)?,
        job_id: crate::ids::JobId::try_new(value.job_id).map_err(WireError::from_id)?,
        task_id: crate::ids::TaskId::try_new(value.task_id).map_err(WireError::from_id)?,
    })
}

pub fn drain_continuous_output_response_to_wire(
    value: crate::task::DrainContinuousOutputResponse,
) -> v1::DrainContinuousOutputResponse {
    v1::DrainContinuousOutputResponse {
        version: Some(transport_version_to_wire(value.version)),
        disposition: transport_disposition_to_wire(value.disposition) as i32,
        ipc_bytes: value.ipc_bytes,
    }
}

pub fn drain_continuous_output_response_from_wire(
    value: v1::DrainContinuousOutputResponse,
) -> WireResult<crate::task::DrainContinuousOutputResponse> {
    Ok(crate::task::DrainContinuousOutputResponse {
        version: transport_version_from_wire(required(value.version, "version")?)?,
        disposition: transport_disposition_from_wire(value.disposition)?,
        ipc_bytes: value.ipc_bytes,
    })
}

// ── CoordinatorManagement wire conversions ───────────────────────────────────

pub fn trigger_savepoint_request_to_wire(
    value: crate::management::TriggerSavepointRequest,
) -> v1::TriggerSavepointRequest {
    v1::TriggerSavepointRequest {
        job_id: value.job_id.as_str().to_owned(),
        label: value.label,
        stop: value.stop,
    }
}

pub fn trigger_savepoint_request_from_wire(
    value: v1::TriggerSavepointRequest,
) -> WireResult<crate::management::TriggerSavepointRequest> {
    Ok(crate::management::TriggerSavepointRequest {
        job_id: JobId::try_new(value.job_id).map_err(WireError::from_id)?,
        label: value.label,
        stop: value.stop,
    })
}

pub fn trigger_savepoint_response_to_wire(
    value: crate::management::TriggerSavepointResponse,
) -> v1::TriggerSavepointResponse {
    v1::TriggerSavepointResponse {
        epoch: value.epoch,
        message: value.message,
    }
}

pub fn trigger_savepoint_response_from_wire(
    value: v1::TriggerSavepointResponse,
) -> crate::management::TriggerSavepointResponse {
    crate::management::TriggerSavepointResponse {
        epoch: value.epoch,
        message: value.message,
    }
}

pub fn restore_job_request_to_wire(
    value: crate::management::RestoreJobRequest,
) -> v1::RestoreJobRequest {
    v1::RestoreJobRequest {
        job_id: value.job_id.as_str().to_owned(),
        epoch: value.epoch,
        storage_path: value.storage_path,
        from_savepoint: value.from_savepoint,
    }
}

pub fn restore_job_request_from_wire(
    value: v1::RestoreJobRequest,
) -> WireResult<crate::management::RestoreJobRequest> {
    if value.epoch == 0 {
        return Err(WireError::new("restore job request: epoch must be > 0"));
    }
    Ok(crate::management::RestoreJobRequest {
        job_id: JobId::try_new(value.job_id).map_err(WireError::from_id)?,
        epoch: value.epoch,
        storage_path: value.storage_path,
        from_savepoint: value.from_savepoint,
    })
}

pub fn restore_job_response_to_wire(
    value: crate::management::RestoreJobResponse,
) -> v1::RestoreJobResponse {
    v1::RestoreJobResponse {
        accepted: value.accepted,
        message: value.message,
    }
}

pub fn restore_job_response_from_wire(
    value: v1::RestoreJobResponse,
) -> crate::management::RestoreJobResponse {
    crate::management::RestoreJobResponse {
        accepted: value.accepted,
        message: value.message,
    }
}

pub fn list_checkpoints_request_to_wire(
    value: crate::management::ListCheckpointsRequest,
) -> v1::ListCheckpointsRequest {
    v1::ListCheckpointsRequest {
        job_id: value.job_id.as_str().to_owned(),
    }
}

pub fn list_checkpoints_request_from_wire(
    value: v1::ListCheckpointsRequest,
) -> WireResult<crate::management::ListCheckpointsRequest> {
    Ok(crate::management::ListCheckpointsRequest {
        job_id: JobId::try_new(value.job_id).map_err(WireError::from_id)?,
    })
}

pub fn list_checkpoints_response_to_wire(
    value: crate::management::ListCheckpointsResponse,
) -> v1::ListCheckpointsResponse {
    v1::ListCheckpointsResponse {
        epochs: value
            .epochs
            .into_iter()
            .map(|e| v1::CheckpointEpochInfo {
                epoch: e.epoch,
                is_savepoint: e.is_savepoint,
                savepoint_label: e.savepoint_label.unwrap_or_default(),
            })
            .collect(),
    }
}

pub fn list_checkpoints_response_from_wire(
    value: v1::ListCheckpointsResponse,
) -> crate::management::ListCheckpointsResponse {
    crate::management::ListCheckpointsResponse {
        epochs: value
            .epochs
            .into_iter()
            .map(|e| crate::management::CheckpointEpochInfo {
                epoch: e.epoch,
                is_savepoint: e.is_savepoint,
                savepoint_label: if e.savepoint_label.is_empty() {
                    None
                } else {
                    Some(e.savepoint_label)
                },
            })
            .collect(),
    }
}

pub fn inspect_state_request_to_wire(
    value: crate::management::InspectStateRequest,
) -> v1::InspectStateRequest {
    v1::InspectStateRequest {
        job_id: value.job_id.as_str().to_owned(),
        operator_id: value.operator_id,
    }
}

pub fn inspect_state_request_from_wire(
    value: v1::InspectStateRequest,
) -> WireResult<crate::management::InspectStateRequest> {
    Ok(crate::management::InspectStateRequest {
        job_id: JobId::try_new(value.job_id).map_err(WireError::from_id)?,
        operator_id: value.operator_id,
    })
}

pub fn inspect_state_response_to_wire(
    value: crate::management::InspectStateResponse,
) -> v1::InspectStateResponse {
    v1::InspectStateResponse {
        snapshots: value
            .snapshots
            .into_iter()
            .map(|s| v1::StateSnapshotInfo {
                task_id: s.task_id,
                snapshot_path: s.snapshot_path,
            })
            .collect(),
    }
}

pub fn inspect_state_response_from_wire(
    value: v1::InspectStateResponse,
) -> crate::management::InspectStateResponse {
    crate::management::InspectStateResponse {
        snapshots: value
            .snapshots
            .into_iter()
            .map(|s| crate::management::StateSnapshotInfo {
                task_id: s.task_id,
                snapshot_path: s.snapshot_path,
            })
            .collect(),
    }
}
