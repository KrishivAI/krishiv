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
