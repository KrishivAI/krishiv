//! Coordinator-side barrier stream client (R16 S1.4).

use std::time::Duration;

use krishiv_proto::wire::v1::CheckpointBarrier;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::barrier_tracker::CheckpointBarrierTracker;

/// Send a checkpoint barrier to an executor and record the returned ack.
pub async fn inject_barrier(
    client: &mut krishiv_proto::wire::v1::barrier_service_client::BarrierServiceClient<
        tonic::transport::Channel,
    >,
    barrier: CheckpointBarrier,
    tracker: &mut CheckpointBarrierTracker,
    timeout: Duration,
) -> Result<(), String> {
    let (tx, rx) = mpsc::channel(2);
    tx.send(barrier)
        .await
        .map_err(|e| format!("barrier send: {e}"))?;
    drop(tx);
    let mut responses = client
        .barrier_stream(ReceiverStream::new(rx))
        .await
        .map_err(|e| e.to_string())?
        .into_inner();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err("barrier ack timeout".into());
        }
        match tokio::time::timeout(remaining, responses.message()).await {
            Ok(Ok(Some(ack))) => {
                if !tracker.record_ack(&ack) {
                    return Err(format!(
                        "unexpected ack for job {} epoch {}",
                        ack.job_id, ack.epoch
                    ));
                }
                if tracker.is_complete() {
                    return Ok(());
                }
            }
            Ok(Ok(None)) => return Err("barrier stream closed".into()),
            Ok(Err(e)) => return Err(e.to_string()),
            Err(_) => return Err("barrier ack timeout".into()),
        }
    }
}
