//! Executor-side `BarrierService` gRPC server (R16 S1.2–S1.4).

use std::pin::Pin;
use std::time::Duration;

use krishiv_proto::wire::v1::{
    barrier_service_server::{BarrierService, BarrierServiceServer},
    BarrierAck, CheckpointBarrier, StateHandle,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::barrier_transport::SharedBarrierInjector;

/// Serves barrier injection and returns acknowledgments after enqueue.
#[derive(Clone)]
pub struct ExecutorBarrierService {
    pub injector: SharedBarrierInjector,
    pub task_id: String,
}

impl ExecutorBarrierService {
    pub fn new(injector: SharedBarrierInjector, task_id: impl Into<String>) -> Self {
        Self {
            injector,
            task_id: task_id.into(),
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
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(16);
        let injector = self.injector.clone();
        let task_id = self.task_id.clone();
        tokio::spawn(async move {
            while let Ok(Some(barrier)) = inbound.message().await {
                injector.enqueue(barrier.clone());
                let ack_task_id = task_id_from_checkpoint_id(&barrier.checkpoint_id)
                    .unwrap_or_else(|| task_id.clone());
                let ack = BarrierAck {
                    epoch: barrier.epoch,
                    job_id: barrier.job_id.clone(),
                    task_id: ack_task_id,
                    state_handle: Some(StateHandle {
                        backend_kind: "redb".into(),
                        checkpoint_uri: format!(
                            "checkpoint://{}/{}",
                            barrier.job_id, barrier.checkpoint_id
                        ),
                        key_group_range_start: 0,
                        key_group_range_end: 32_767,
                        schema_version: 1,
                    }),
                };
                if tx.send(Ok(ack)).await.is_err() {
                    break;
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
    tx.send(barrier).await.map_err(|e| tonic::Status::internal(e.to_string()))?;
    drop(tx);
    let mut stream = client
        .barrier_stream(ReceiverStream::new(rx))
        .await?
        .into_inner();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(tonic::Status::deadline_exceeded("barrier ack timeout"));
        }
        match tokio::time::timeout(remaining, tokio_stream::StreamExt::next(&mut stream)).await {
            Ok(Some(Ok(ack))) => return Ok(ack),
            Ok(Some(Err(status))) => return Err(status),
            Ok(None) => return Err(tonic::Status::internal("barrier stream closed")),
            Err(_) => return Err(tonic::Status::deadline_exceeded("barrier ack timeout")),
        }
    }
}
