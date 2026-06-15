//! Shared mapping between executor heartbeat requests and coordinator effects.

use krishiv_proto::{
    ExecutorHeartbeat, ExecutorHeartbeatRequest, ExecutorHeartbeatResponse,
    HeartbeatThrottleCommand, TransportDisposition,
};

use crate::adaptive::ExecutorHeartbeatEffects;

/// Build a domain [`ExecutorHeartbeat`] from a transport request.
pub fn executor_heartbeat_from_request(request: &ExecutorHeartbeatRequest) -> ExecutorHeartbeat {
    let mut heartbeat = ExecutorHeartbeat::new(request.executor_id().clone(), request.state())
        .with_lease_generation(request.lease_generation())
        .with_running_tasks(
            request
                .running_attempts()
                .iter()
                .map(|attempt| attempt.task_id().clone())
                .collect(),
        );
    if let Some(bytes) = request.memory_used_bytes() {
        heartbeat = heartbeat.with_memory_used_bytes(bytes);
    }
    if let Some(bytes) = request.memory_limit_bytes() {
        heartbeat = heartbeat.with_memory_limit_bytes(bytes);
    }
    if let Some(count) = request.active_task_count() {
        heartbeat = heartbeat.with_active_task_count(count);
    }
    if !request.streaming_task_states().is_empty() {
        heartbeat = heartbeat.with_streaming_task_states(request.streaming_task_states().to_vec());
    }
    if !request.hot_key_reports().is_empty() {
        heartbeat = heartbeat.with_hot_key_reports(request.hot_key_reports().to_vec());
    }
    if !request.streaming_progress().is_empty() {
        heartbeat = heartbeat.with_streaming_progress(request.streaming_progress().to_vec());
    }
    heartbeat
}

/// Convert coordinator heartbeat side effects into a transport response.
pub fn executor_heartbeat_response_from_effects(
    effects: ExecutorHeartbeatEffects,
) -> ExecutorHeartbeatResponse {
    let mut resp =
        ExecutorHeartbeatResponse::new(effects.lease_generation, TransportDisposition::Accepted);
    if !effects.source_throttles.is_empty() {
        let wire_cmds: Vec<HeartbeatThrottleCommand> = effects
            .source_throttles
            .into_iter()
            .map(|c| HeartbeatThrottleCommand {
                source_id: c.source_id,
                rows_per_second: c.rows_per_second,
            })
            .collect();
        resp = resp.with_throttle_commands(wire_cmds);
    }
    if !effects.checkpoint_commands.is_empty() {
        resp = resp.with_checkpoint_commands(effects.checkpoint_commands);
    }
    if !effects.checkpoint_complete_commands.is_empty() {
        resp = resp.with_checkpoint_complete_commands(effects.checkpoint_complete_commands);
    }
    if !effects.restore_commands.is_empty() {
        resp = resp.with_restore_commands(effects.restore_commands);
    }
    resp
}
