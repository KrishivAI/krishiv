//! Phase 55: executor→executor keyed exchange for run-loop subtasks.
//!
//! A run-loop subtask that reads rows whose key-group belongs to a sibling
//! subtask forwards them over the peer executor's `push_continuous_input`
//! gRPC. Each peer channel is guarded by a [`CreditGate`] sized in bytes —
//! credits are consumed before the RPC and returned when the peer accepts, so
//! a slow peer applies backpressure to the sender instead of ballooning its
//! input buffer (Flink's credit-based flow control, FLINK-7282). The receiver
//! cap (`push_continuous_input`'s per-job pending-batch limit) stays
//! authoritative: a `resource_exhausted` rejection is retried with backoff and
//! surfaces as an error only after the retry budget is spent.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_common::backpressure::CreditGate;
use krishiv_proto::{JobId, TaskId, TransportDisposition, TransportVersion, wire};

use crate::{ExecutorError, ExecutorResult};

/// Per-peer exchange state: a lazily connected channel plus its credit gate.
struct PeerChannel {
    channel: tokio::sync::OnceCell<tonic::transport::Channel>,
    credits: Arc<CreditGate>,
}

/// Credit-gated exchange client, shared by every run-loop subtask on this
/// executor (cloned with the runner; all clones share the peer map).
#[derive(Clone, Default)]
pub struct StreamExchange {
    peers: Arc<DashMap<String, Arc<PeerChannel>>>,
}

impl std::fmt::Debug for StreamExchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamExchange")
            .field("peers", &self.peers.len())
            .finish()
    }
}

/// Per-peer credit window in bytes.
const EXCHANGE_CREDIT_BYTES: u64 = 8 * 1024 * 1024;
/// Send retry budget for receiver-full / transient transport errors.
const EXCHANGE_MAX_RETRIES: u32 = 5;

impl StreamExchange {
    fn peer(&self, endpoint: &str) -> Arc<PeerChannel> {
        self.peers
            .entry(endpoint.to_owned())
            .or_insert_with(|| {
                Arc::new(PeerChannel {
                    channel: tokio::sync::OnceCell::new(),
                    credits: CreditGate::new(EXCHANGE_CREDIT_BYTES),
                })
            })
            .clone()
    }

    /// Encode `batches` and push them to `endpoint` for `(job_id, task_id)`.
    ///
    /// Blocks (async) while the peer's credit window is exhausted; returns an
    /// error when the transport fails past the retry budget.
    pub async fn send(
        &self,
        job_id: &str,
        task_id: &str,
        endpoint: &str,
        batches: Vec<RecordBatch>,
    ) -> ExecutorResult<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let ipc_bytes = encode_ipc(&batches)?;
        let cost = ipc_bytes.len() as u64;
        let peer = self.peer(endpoint);

        // Credit acquisition: spin on the gate with a short async yield. The
        // gate caps in-flight bytes per channel; `ack` restores the window
        // after the peer's accept/reject.
        let mut waited = 0u32;
        while !peer.credits.try_send(cost.min(EXCHANGE_CREDIT_BYTES)) {
            waited = waited.saturating_add(1);
            if waited > 10_000 {
                return Err(ExecutorError::LocalExecution {
                    message: format!(
                        "stream exchange to {endpoint} starved for credits \
                         (peer not draining); job {job_id}"
                    ),
                });
            }
            tokio::time::sleep(std::time::Duration::from_micros(200)).await;
        }
        let acquired = cost.min(EXCHANGE_CREDIT_BYTES);
        let result = self
            .send_with_retries(job_id, task_id, endpoint, &peer, ipc_bytes)
            .await;
        peer.credits.ack(acquired);
        result
    }

    async fn send_with_retries(
        &self,
        job_id: &str,
        task_id: &str,
        endpoint: &str,
        peer: &PeerChannel,
        ipc_bytes: Vec<u8>,
    ) -> ExecutorResult<()> {
        let job = JobId::try_new(job_id).map_err(|e| ExecutorError::InvalidAssignment {
            message: format!("stream exchange job id '{job_id}': {e}"),
        })?;
        let task = TaskId::try_new(task_id).map_err(|e| ExecutorError::InvalidAssignment {
            message: format!("stream exchange task id '{task_id}': {e}"),
        })?;
        let request = krishiv_proto::task::PushContinuousInputRequest {
            version: TransportVersion::CURRENT,
            job_id: job,
            task_id: task,
            ipc_bytes,
        };
        let wire_request = wire::push_continuous_input_request_to_wire(request);

        for attempt in 0..EXCHANGE_MAX_RETRIES {
            let channel = peer
                .channel
                .get_or_try_init(|| async {
                    tonic::transport::Endpoint::from_shared(endpoint.to_owned())
                        .map_err(|e| ExecutorError::LocalExecution {
                            message: format!("stream exchange endpoint '{endpoint}': {e}"),
                        })?
                        .connect_timeout(std::time::Duration::from_secs(5))
                        .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
                        .keep_alive_while_idle(true)
                        .connect()
                        .await
                        .map_err(|e| ExecutorError::LocalExecution {
                            message: format!("stream exchange connect to '{endpoint}': {e}"),
                        })
                })
                .await?
                .clone();
            let max = krishiv_proto::max_grpc_message_bytes();
            let mut client = wire::v1::executor_task_client::ExecutorTaskClient::new(channel)
                .max_decoding_message_size(max)
                .max_encoding_message_size(max);
            // Same cluster bearer token the coordinator stamps on assignment
            // pushes — peer executors enforcing task-API auth accept us.
            let mut request = tonic::Request::new(wire_request.clone());
            if let Ok(token) = std::env::var(crate::grpc::EXECUTOR_TASK_BEARER_TOKEN_ENV) {
                let token = token.trim();
                if !token.is_empty()
                    && let Ok(value) =
                        tonic::metadata::MetadataValue::try_from(format!("Bearer {token}"))
                {
                    request.metadata_mut().insert("authorization", value);
                }
            }
            match client.push_continuous_input(request).await {
                Ok(response) => {
                    let decoded = wire::task_status_response_from_wire(response.into_inner())
                        .map_err(|e| ExecutorError::LocalExecution {
                            message: format!("stream exchange decode from '{endpoint}': {e}"),
                        })?;
                    return match decoded.disposition() {
                        TransportDisposition::Accepted | TransportDisposition::Duplicate => Ok(()),
                        other => Err(ExecutorError::LocalExecution {
                            message: format!(
                                "stream exchange to '{endpoint}' rejected with {other:?}"
                            ),
                        }),
                    };
                }
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::ResourceExhausted
                            | tonic::Code::Unavailable
                            | tonic::Code::DeadlineExceeded
                    ) && attempt + 1 < EXCHANGE_MAX_RETRIES =>
                {
                    // Receiver full or transient transport failure: back off
                    // and retry — the receiver cap is the authoritative limit.
                    let backoff_ms = 10u64.saturating_mul(1 << attempt.min(6));
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                }
                Err(status) => {
                    return Err(ExecutorError::LocalExecution {
                        message: format!("stream exchange push to '{endpoint}': {status}"),
                    });
                }
            }
        }
        Err(ExecutorError::LocalExecution {
            message: format!("stream exchange to '{endpoint}' exhausted its retry budget"),
        })
    }
}

fn encode_ipc(batches: &[RecordBatch]) -> ExecutorResult<Vec<u8>> {
    use arrow::ipc::writer::StreamWriter;
    let schema = batches
        .first()
        .ok_or_else(|| ExecutorError::LocalExecution {
            message: "stream exchange encode: empty batch set".into(),
        })?
        .schema();
    let mut buf = Vec::new();
    let mut writer =
        StreamWriter::try_new(&mut buf, &schema).map_err(|e| ExecutorError::LocalExecution {
            message: format!("stream exchange IPC writer: {e}"),
        })?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| ExecutorError::LocalExecution {
                message: format!("stream exchange IPC write: {e}"),
            })?;
    }
    writer.finish().map_err(|e| ExecutorError::LocalExecution {
        message: format!("stream exchange IPC finish: {e}"),
    })?;
    Ok(buf)
}
