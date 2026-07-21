//! Coordinator-side barrier stream client (R16 S1.4).

use std::time::Duration;

use krishiv_proto::wire::v1::{BarrierAck, CheckpointBarrier};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Send a checkpoint barrier to an executor and return the returned ack.
pub async fn inject_barrier<T>(
    client: &mut krishiv_proto::wire::v1::barrier_service_client::BarrierServiceClient<T>,
    barrier: CheckpointBarrier,
    timeout: Duration,
) -> Result<BarrierAck, String>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<tonic::codegen::StdError>,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    let (tx, rx) = mpsc::channel(2);
    tx.send(barrier)
        .await
        .map_err(|e| format!("barrier send: {e}"))?;
    drop(tx);
    // Capture the start time before the RPC call so that the time spent
    // establishing the stream is subtracted from the remaining budget.
    let start = tokio::time::Instant::now();
    let mut responses = client
        .barrier_stream(ReceiverStream::new(rx))
        .await
        .map_err(|e| e.to_string())?
        .into_inner();
    let remaining = timeout.saturating_sub(start.elapsed());
    if remaining.is_zero() {
        return Err("barrier ack timeout".into());
    }
    match tokio::time::timeout(remaining, responses.message()).await {
        Ok(Ok(Some(ack))) => Ok(ack),
        Ok(Ok(None)) => Err("barrier stream closed".into()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("barrier ack timeout".into()),
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use krishiv_proto::wire::v1::barrier_service_client::BarrierServiceClient;
    use krishiv_proto::wire::v1::barrier_service_server::{BarrierService, BarrierServiceServer};
    use tokio_stream::Stream;
    use tonic::transport::Channel;

    use super::*;

    fn barrier(epoch: u64) -> CheckpointBarrier {
        CheckpointBarrier {
            epoch,
            job_id: "job-1".to_owned(),
            checkpoint_id: "ckpt-1".to_owned(),
            barrier_kind: 1,
            timestamp_ms: 0,
        }
    }

    fn ack(epoch: u64) -> BarrierAck {
        BarrierAck {
            epoch,
            job_id: "job-1".to_owned(),
            task_id: "task-1".to_owned(),
            state_handle: None,
        }
    }

    /// What the mock barrier server does when its `barrier_stream` RPC is
    /// invoked — covers the response shapes `inject_barrier` must handle.
    enum MockBehavior {
        /// Immediately return one ack.
        AckImmediately,
        /// Return an empty response stream (server-side stream closes with
        /// no items) — simulates a peer that accepted the call but never
        /// produced an ack.
        CloseWithoutAck,
        /// Accept the call but never send anything and never close —
        /// exercises `inject_barrier`'s timeout path.
        NeverRespond,
        /// Reject the call itself before a stream is ever established.
        RejectCall,
    }

    struct MockBarrierServer {
        behavior: MockBehavior,
    }

    #[tonic::async_trait]
    impl BarrierService for MockBarrierServer {
        type BarrierStreamStream =
            Pin<Box<dyn Stream<Item = Result<BarrierAck, tonic::Status>> + Send + 'static>>;

        async fn barrier_stream(
            &self,
            _request: tonic::Request<tonic::Streaming<CheckpointBarrier>>,
        ) -> Result<tonic::Response<Self::BarrierStreamStream>, tonic::Status> {
            match self.behavior {
                MockBehavior::AckImmediately => {
                    let stream = tokio_stream::once(Ok(ack(1)));
                    Ok(tonic::Response::new(Box::pin(stream)))
                }
                MockBehavior::CloseWithoutAck => {
                    let stream = tokio_stream::iter(std::iter::empty());
                    Ok(tonic::Response::new(Box::pin(stream)))
                }
                MockBehavior::NeverRespond => {
                    let stream = tokio_stream::pending();
                    Ok(tonic::Response::new(Box::pin(stream)))
                }
                MockBehavior::RejectCall => {
                    Err(tonic::Status::unavailable("mock: call rejected"))
                }
            }
        }
    }

    /// Spawns the mock server on a loopback port and returns a connected
    /// client channel, mirroring `stream_exchange.rs`'s `spawn_mock_peer`.
    async fn spawn_mock_barrier_server(
        behavior: MockBehavior,
    ) -> (BarrierServiceClient<Channel>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(BarrierServiceServer::new(MockBarrierServer { behavior }))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        });
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .expect("connect to mock barrier server");
        (BarrierServiceClient::new(channel), handle)
    }

    #[tokio::test]
    async fn inject_barrier_returns_the_ack_on_success() {
        let (mut client, server) = spawn_mock_barrier_server(MockBehavior::AckImmediately).await;
        let result = inject_barrier(&mut client, barrier(1), Duration::from_secs(5)).await;
        let ack = result.expect("mock server acked immediately");
        assert_eq!(ack.epoch, 1);
        assert_eq!(ack.task_id, "task-1");
        server.abort();
    }

    #[tokio::test]
    async fn inject_barrier_errors_when_the_stream_closes_without_an_ack() {
        let (mut client, server) =
            spawn_mock_barrier_server(MockBehavior::CloseWithoutAck).await;
        let err = inject_barrier(&mut client, barrier(1), Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(err.contains("barrier stream closed"), "got: {err}");
        server.abort();
    }

    #[tokio::test]
    async fn inject_barrier_errors_when_the_call_itself_is_rejected() {
        let (mut client, server) = spawn_mock_barrier_server(MockBehavior::RejectCall).await;
        let err = inject_barrier(&mut client, barrier(1), Duration::from_secs(5))
            .await
            .unwrap_err();
        // The RPC error surfaces via `.map_err(|e| e.to_string())` on the
        // initial `barrier_stream` call, not the ack-timeout path.
        assert!(!err.contains("timeout"), "got: {err}");
        server.abort();
    }

    #[tokio::test]
    async fn inject_barrier_times_out_on_a_peer_that_never_acks() {
        let (mut client, server) = spawn_mock_barrier_server(MockBehavior::NeverRespond).await;
        let budget = Duration::from_millis(200);
        let start = tokio::time::Instant::now();
        let err = inject_barrier(&mut client, barrier(1), budget)
            .await
            .unwrap_err();
        assert!(err.contains("timeout"), "got: {err}");
        // Bounded by the requested budget, not left to hang indefinitely;
        // generous upper bound to stay non-flaky under CI scheduling noise.
        assert!(start.elapsed() < budget * 10);
        server.abort();
    }

    #[tokio::test]
    async fn inject_barrier_reports_timeout_immediately_when_the_budget_is_already_spent() {
        // A zero timeout must hit the `remaining.is_zero()` fast path
        // without even attempting to read a response.
        let (mut client, server) = spawn_mock_barrier_server(MockBehavior::AckImmediately).await;
        let err = inject_barrier(&mut client, barrier(1), Duration::ZERO)
            .await
            .unwrap_err();
        assert!(err.contains("timeout"), "got: {err}");
        server.abort();
    }
}
