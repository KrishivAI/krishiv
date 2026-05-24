//! End-to-end gRPC checkpoint barrier transport (R16 S1.4).

use std::time::Duration;

use krishiv_executor::barrier_grpc::{
    ExecutorBarrierService, executor_barrier_grpc_server, send_barrier_and_wait_ack,
};
use krishiv_executor::barrier_transport::SharedBarrierInjector;
use krishiv_proto::wire::v1::{BarrierKind, CheckpointBarrier};

#[tokio::test]
async fn checkpoint_barrier_integration() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let injector = SharedBarrierInjector::new();
    let service = ExecutorBarrierService::new(injector.clone(), "task-0");
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(executor_barrier_grpc_server(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
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
    assert!(injector.next_barrier().is_some());
}
