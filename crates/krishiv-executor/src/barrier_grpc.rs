//! Executor-side `BarrierService` gRPC server.

use std::time::Duration;

use krishiv_proto::wire::v1::{
    BarrierAck, CheckpointBarrier, StateHandle,
    barrier_service_server::{BarrierService, BarrierServiceServer},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataMap;

use crate::barrier_transport::{
    SharedBarrierAckRegistry, SharedBarrierInjector, SharedKeyGroupRanges,
};
use crate::grpc::{ExecutorTaskAuthConfig, bearer_token_from_metadata, constant_time_eq};

/// Serves barrier injection and returns acknowledgments after checkpoint completion.
#[derive(Clone)]
pub struct ExecutorBarrierService {
    pub injector: SharedBarrierInjector,
    pub ack_registry: SharedBarrierAckRegistry,
    pub task_id: String,
    pub key_group_range_start: u32,
    pub key_group_range_end: u32,
    pub key_group_ranges: SharedKeyGroupRanges,
    /// Optional auth config — when set, all barrier RPCs require a valid bearer token.
    pub auth_config: Option<ExecutorTaskAuthConfig>,
    /// Checkpoint URI to use in barrier acknowledgements (e.g. "file:///tmp/krishiv-checkpoints").
    pub checkpoint_uri: Option<String>,
    /// State backend kind ("fjall", "rocksdb", etc.).
    pub state_backend_kind: String,
    /// How long to wait for a checkpoint to complete before returning deadline_exceeded.
    pub checkpoint_timeout_secs: u64,
}

impl ExecutorBarrierService {
    pub fn new(injector: SharedBarrierInjector, task_id: impl Into<String>) -> Self {
        Self {
            injector,
            ack_registry: SharedBarrierAckRegistry::new(),
            task_id: task_id.into(),
            key_group_range_start: 0,
            key_group_range_end: 32_767,
            key_group_ranges: SharedKeyGroupRanges::new(),
            auth_config: None,
            checkpoint_uri: None,
            state_backend_kind: String::new(),
            checkpoint_timeout_secs: 120,
        }
    }

    /// Set the state backend kind reported in barrier acks (e.g. `"fjall"`, `"rocksdb"`).
    #[must_use]
    pub fn with_state_backend_kind(mut self, kind: impl Into<String>) -> Self {
        self.state_backend_kind = kind.into();
        self
    }

    /// Override the checkpoint completion timeout (default: 120 s).
    #[must_use]
    pub fn with_checkpoint_timeout_secs(mut self, secs: u64) -> Self {
        self.checkpoint_timeout_secs = secs;
        self
    }

    /// Require bearer-token auth matching the task gRPC auth config.
    #[must_use]
    pub fn with_auth_config(mut self, auth: ExecutorTaskAuthConfig) -> Self {
        self.auth_config = Some(auth);
        self
    }

    fn validate_auth(&self, metadata: &MetadataMap) -> Result<(), tonic::Status> {
        let Some(auth) = &self.auth_config else {
            return Ok(());
        };
        if auth.require_auth() && auth.bearer_token().is_none() {
            return Err(tonic::Status::unauthenticated(
                "barrier auth required but no token configured",
            ));
        }
        let Some(expected) = auth.bearer_token() else {
            return Ok(());
        };
        match bearer_token_from_metadata(metadata) {
            Some(actual) if constant_time_eq(actual.as_bytes(), expected.as_bytes()) => Ok(()),
            Some(_) => Err(tonic::Status::unauthenticated(
                "invalid barrier bearer token",
            )),
            None => Err(tonic::Status::unauthenticated(
                "missing barrier bearer token",
            )),
        }
    }

    /// Create with an explicit key group range (distributed mode).
    #[must_use]
    pub fn with_key_group_range(mut self, start: u32, end: u32) -> Self {
        self.key_group_range_start = start;
        self.key_group_range_end = end;
        self
    }

    /// Use the task-id keyed ranges populated from executor assignments.
    #[must_use]
    pub fn with_key_group_ranges(mut self, ranges: SharedKeyGroupRanges) -> Self {
        self.key_group_ranges = ranges;
        self
    }

    /// Share the barrier ack completion registry with the task runner.
    #[must_use]
    pub fn with_ack_registry(mut self, registry: SharedBarrierAckRegistry) -> Self {
        self.ack_registry = registry;
        self
    }

    /// Resolve the state-handle key-group range for a task ack.
    pub fn key_group_range_for_task(&self, task_id: &str) -> (u32, u32) {
        if let Some(range) = self.key_group_ranges.get(task_id) {
            (range.start(), range.end())
        } else {
            (self.key_group_range_start, self.key_group_range_end)
        }
    }
}

pub fn executor_barrier_grpc_server(
    service: ExecutorBarrierService,
) -> BarrierServiceServer<ExecutorBarrierService> {
    BarrierServiceServer::new(service)
}

#[tonic::async_trait]
impl BarrierService for ExecutorBarrierService {
    type BarrierStreamStream = ReceiverStream<Result<BarrierAck, tonic::Status>>;

    async fn barrier_stream(
        &self,
        request: tonic::Request<tonic::Streaming<CheckpointBarrier>>,
    ) -> Result<tonic::Response<Self::BarrierStreamStream>, tonic::Status> {
        self.validate_auth(request.metadata())?;
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(16);
        let injector = self.injector.clone();
        let ack_registry = self.ack_registry.clone();
        let task_id = self.task_id.clone();
        let state_backend_kind = self.state_backend_kind.clone();
        let checkpoint_timeout_secs = self.checkpoint_timeout_secs;
        tokio::spawn(async move {
            while let Ok(Some(barrier)) = inbound.message().await {
                injector.enqueue(barrier.clone());
                let ack_task_id = task_id_from_checkpoint_id(&barrier.checkpoint_id)
                    .unwrap_or_else(|| task_id.clone());
                let completion_rx = ack_registry.register_wait(&barrier.job_id, barrier.epoch);
                let ack = match tokio::time::timeout(
                    Duration::from_secs(checkpoint_timeout_secs),
                    completion_rx,
                )
                .await
                {
                    Ok(Ok(completion)) => Ok(BarrierAck {
                        epoch: barrier.epoch,
                        job_id: barrier.job_id.clone(),
                        task_id: ack_task_id,
                        state_handle: Some(StateHandle {
                            backend_kind: state_backend_kind.clone(),
                            checkpoint_uri: completion.checkpoint_uri,
                            key_group_range_start: completion.key_group_range_start,
                            key_group_range_end: completion.key_group_range_end,
                            schema_version: 1,
                        }),
                    }),
                    Ok(Err(_)) => Err(tonic::Status::internal(
                        "barrier checkpoint completion channel closed",
                    )),
                    Err(_) => Err(tonic::Status::deadline_exceeded(
                        "barrier checkpoint completion timeout",
                    )),
                };
                match ack {
                    Ok(ack) => {
                        if tx.send(Ok(ack)).await.is_err() {
                            break;
                        }
                    }
                    Err(status) => {
                        if tx.send(Err(status)).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

/// Client helper: send one barrier and wait for matching ack (tests / coordinator).
/// Parse `task:<task_id>/...` from checkpoint id (coordinator → executor contract).
fn task_id_from_checkpoint_id(checkpoint_id: &str) -> Option<String> {
    let rest = checkpoint_id.strip_prefix("task:")?;
    let task_id = rest.split('/').next()?;
    if task_id.is_empty() {
        None
    } else {
        Some(task_id.to_owned())
    }
}

pub async fn send_barrier_and_wait_ack(
    client: &mut krishiv_proto::wire::v1::barrier_service_client::BarrierServiceClient<
        tonic::transport::Channel,
    >,
    barrier: CheckpointBarrier,
    timeout: Duration,
) -> Result<BarrierAck, tonic::Status> {
    let (tx, rx) = mpsc::channel(4);
    tx.send(barrier)
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
    drop(tx);
    let mut stream = client
        .barrier_stream(ReceiverStream::new(rx))
        .await?
        .into_inner();
    match tokio::time::timeout(timeout, tokio_stream::StreamExt::next(&mut stream)).await {
        Ok(Some(Ok(ack))) => Ok(ack),
        Ok(Some(Err(status))) => Err(status),
        Ok(None) => Err(tonic::Status::internal("barrier stream closed")),
        Err(_) => Err(tonic::Status::deadline_exceeded("barrier ack timeout")),
    }
}

#[cfg(test)]
mod tests {
    use krishiv_proto::KeyGroupRange;

    use super::*;

    #[test]
    fn service_uses_registered_task_key_group_range() {
        let ranges = SharedKeyGroupRanges::new();
        ranges.set("task-1", KeyGroupRange::new(1024, 2047));
        let service = ExecutorBarrierService::new(SharedBarrierInjector::new(), "exec-1")
            .with_key_group_range(0, 32_767)
            .with_key_group_ranges(ranges);

        assert_eq!(service.key_group_range_for_task("task-1"), (1024, 2047));
        assert_eq!(service.key_group_range_for_task("task-2"), (0, 32_767));
    }

    #[test]
    fn checkpoint_id_task_parser_rejects_empty_task() {
        assert_eq!(
            task_id_from_checkpoint_id("task:stream-task/checkpoint-1").as_deref(),
            Some("stream-task")
        );
        assert!(task_id_from_checkpoint_id("task:/checkpoint-1").is_none());
    }
}
