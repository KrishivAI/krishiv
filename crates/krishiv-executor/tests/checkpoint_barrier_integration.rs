//! End-to-end gRPC checkpoint barrier transport (R16 S1.4).
//!
//! The barrier service only acks after the task runner signals checkpoint
//! completion through the shared ack registry, so this test plays both
//! sides: the coordinator (send barrier, wait for ack) and the runner
//! (drain the injector, complete the checkpoint).

use std::time::Duration;

use krishiv_executor::barrier_grpc::{
    ExecutorBarrierService, executor_barrier_grpc_server, send_barrier_and_wait_ack,
};
use krishiv_executor::barrier_transport::{
    BarrierAckCompletion, SharedBarrierAckRegistry, SharedBarrierInjector,
};
use krishiv_proto::wire::v1::{BarrierKind, CheckpointBarrier};

#[tokio::test]
async fn checkpoint_barrier_integration() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let injector = SharedBarrierInjector::new();
    let ack_registry = SharedBarrierAckRegistry::new();
    let service = ExecutorBarrierService::new(injector.clone(), "task-0")
        .with_ack_registry(ack_registry.clone())
        .with_state_backend_kind("fjall");
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(executor_barrier_grpc_server(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Simulated task runner: consume the injected barrier, then report the
    // checkpoint complete so the service releases the ack.
    let runner_injector = injector.clone();
    let runner = tokio::spawn(async move {
        loop {
            if let Some(barrier) = runner_injector.next_barrier() {
                ack_registry.complete(
                    &barrier.job_id,
                    barrier.epoch,
                    BarrierAckCompletion {
                        checkpoint_uri: "file:///tmp/krishiv-checkpoints/cp-7".to_string(),
                        key_group_range_start: 0,
                        key_group_range_end: 32_767,
                    },
                );
                return barrier;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client =
        krishiv_proto::wire::v1::barrier_service_client::BarrierServiceClient::new(channel);

    let barrier = CheckpointBarrier {
        epoch: 7,
        job_id: "job-barrier".into(),
        checkpoint_id: "cp-7".into(),
        barrier_kind: BarrierKind::Checkpoint as i32,
        timestamp_ms: 1,
    };
    let ack = send_barrier_and_wait_ack(&mut client, barrier, Duration::from_secs(5))
        .await
        .expect("barrier ack");
    assert_eq!(ack.epoch, 7);
    assert_eq!(ack.task_id, "task-0");
    let handle = ack.state_handle.expect("ack carries a state handle");
    assert_eq!(handle.backend_kind, "fjall");
    assert_eq!(handle.checkpoint_uri, "file:///tmp/krishiv-checkpoints/cp-7");
    assert_eq!(handle.key_group_range_end, 32_767);

    let injected = runner.await.expect("runner task");
    assert_eq!(injected.epoch, 7);
    assert_eq!(injected.checkpoint_id, "cp-7");
}
