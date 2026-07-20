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
/// Bound on a single `push_continuous_input` RPC to a peer executor (Phase 58
/// #180 sweep). The channel previously configured `tcp_keepalive` without
/// `http2_keep_alive_interval`/`keep_alive_timeout`, so a peer that goes dark
/// *after* connecting (network partition, executor kill) was not detected at
/// the HTTP/2 level at all — the call relied on OS TCP retransmission timeouts
/// (which can extend to minutes on an established connection) to ever return,
/// during which this retry loop cannot retry because it is still awaiting the
/// first attempt.
const EXCHANGE_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

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
                        .http2_keep_alive_interval(std::time::Duration::from_secs(15))
                        .keep_alive_timeout(std::time::Duration::from_secs(20))
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
            match tokio::time::timeout(
                EXCHANGE_RPC_TIMEOUT,
                client.push_continuous_input(request),
            )
            .await
            {
                Ok(Ok(response)) => {
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
                Ok(Err(status))
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
                Ok(Err(status)) => {
                    return Err(ExecutorError::LocalExecution {
                        message: format!("stream exchange push to '{endpoint}': {status}"),
                    });
                }
                Err(_elapsed) if attempt + 1 < EXCHANGE_MAX_RETRIES => {
                    // Phase 58 #180: a dead/partitioned peer with no explicit
                    // bound could hang this call for the OS TCP retransmission
                    // timeout (minutes) instead of retrying within budget.
                    tracing::warn!(
                        endpoint = %endpoint,
                        attempt,
                        timeout_secs = EXCHANGE_RPC_TIMEOUT.as_secs(),
                        "stream exchange push timed out; retrying"
                    );
                    let backoff_ms = 10u64.saturating_mul(1 << attempt.min(6));
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                }
                Err(_elapsed) => {
                    return Err(ExecutorError::LocalExecution {
                        message: format!(
                            "stream exchange push to '{endpoint}' timed out after {}s (all attempts)",
                            EXCHANGE_RPC_TIMEOUT.as_secs()
                        ),
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_proto::TaskStatusResponse;
    use krishiv_proto::TransportDisposition;
    use krishiv_proto::wire::v1::executor_task_server::{ExecutorTask, ExecutorTaskServer};

    use super::*;

    fn small_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }

    #[test]
    fn encode_ipc_rejects_empty_batch_set() {
        let err = encode_ipc(&[]).unwrap_err();
        assert!(matches!(err, ExecutorError::LocalExecution { .. }));
    }

    #[test]
    fn encode_ipc_round_trips_through_arrow_ipc_reader() {
        let batch = small_batch();
        let bytes = encode_ipc(std::slice::from_ref(&batch)).expect("encode must succeed");

        let mut reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .expect("bytes must be a valid arrow IPC stream");
        let decoded = reader
            .next()
            .expect("exactly one batch was written")
            .expect("decode must succeed");
        assert!(reader.next().is_none(), "no extra batches expected");

        assert_eq!(decoded.num_rows(), batch.num_rows());
        assert_eq!(decoded.schema(), batch.schema());
        let values = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column 0 must decode back to Int64Array");
        assert_eq!(values.values(), &[1, 2, 3]);
    }

    #[tokio::test]
    async fn send_is_a_noop_for_empty_batches_and_never_touches_the_network() {
        let exchange = StreamExchange::default();
        // A syntactically invalid endpoint: if the empty-batch fast path ever
        // fell through to a real connection attempt, `Endpoint::from_shared`
        // would reject this and turn the Ok into an Err.
        let result = exchange
            .send("job-x", "task-x", "not a url", Vec::new())
            .await;
        assert!(
            result.is_ok(),
            "an empty batch set must short-circuit before any network attempt"
        );
    }

    #[tokio::test]
    async fn send_rejects_invalid_job_id_before_connecting() {
        let exchange = StreamExchange::default();
        let err = exchange
            .send("", "task-x", "not a url", vec![small_batch()])
            .await
            .unwrap_err();
        assert!(matches!(err, ExecutorError::InvalidAssignment { .. }));
    }

    #[tokio::test]
    async fn send_rejects_invalid_task_id_before_connecting() {
        let exchange = StreamExchange::default();
        let err = exchange
            .send("job-x", "", "not a url", vec![small_batch()])
            .await
            .unwrap_err();
        assert!(matches!(err, ExecutorError::InvalidAssignment { .. }));
    }

    /// One scripted response for the Nth `push_continuous_input` call the mock
    /// peer receives; calls past the end of the script repeat the last entry.
    #[derive(Clone)]
    enum MockOutcome {
        Status(tonic::Code),
        Disposition(TransportDisposition),
    }

    #[derive(Clone)]
    struct MockPeerExecutor {
        calls: Arc<AtomicUsize>,
        script: Arc<Vec<MockOutcome>>,
        received: Arc<Mutex<Vec<(String, String)>>>,
    }

    #[tonic::async_trait]
    impl ExecutorTask for MockPeerExecutor {
        async fn assign_task(
            &self,
            _request: tonic::Request<krishiv_proto::wire::v1::ExecutorTaskAssignment>,
        ) -> Result<tonic::Response<krishiv_proto::wire::v1::TaskStatusResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not used in stream_exchange tests"))
        }

        async fn cancel_task(
            &self,
            _request: tonic::Request<krishiv_proto::wire::v1::TaskCancellationRequest>,
        ) -> Result<tonic::Response<krishiv_proto::wire::v1::TaskStatusResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not used in stream_exchange tests"))
        }

        async fn push_continuous_input(
            &self,
            request: tonic::Request<krishiv_proto::wire::v1::PushContinuousInputRequest>,
        ) -> Result<tonic::Response<krishiv_proto::wire::v1::TaskStatusResponse>, tonic::Status>
        {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let decoded = wire::push_continuous_input_request_from_wire(request.into_inner())
                .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
            self.received.lock().unwrap().push((
                decoded.job_id.as_str().to_owned(),
                decoded.task_id.as_str().to_owned(),
            ));
            let outcome = self
                .script
                .get(call)
                .or_else(|| self.script.last())
                .cloned()
                .unwrap_or(MockOutcome::Disposition(TransportDisposition::Accepted));
            match outcome {
                MockOutcome::Status(code) => Err(tonic::Status::new(code, "mock peer failure")),
                MockOutcome::Disposition(d) => Ok(tonic::Response::new(
                    wire::task_status_response_to_wire(TaskStatusResponse::new(d)),
                )),
            }
        }

        async fn drain_continuous_output(
            &self,
            _request: tonic::Request<krishiv_proto::wire::v1::DrainContinuousOutputRequest>,
        ) -> Result<tonic::Response<krishiv_proto::wire::v1::DrainContinuousOutputResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not used in stream_exchange tests"))
        }
    }

    async fn spawn_mock_peer(
        script: Vec<MockOutcome>,
    ) -> (
        std::net::SocketAddr,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<(String, String)>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let received = Arc::new(Mutex::new(Vec::new()));
        let service = MockPeerExecutor {
            calls: calls.clone(),
            script: Arc::new(script),
            received: received.clone(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(ExecutorTaskServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        });
        (addr, calls, received, handle)
    }

    #[tokio::test]
    async fn send_succeeds_when_peer_accepts() {
        let (addr, calls, received, server) =
            spawn_mock_peer(vec![MockOutcome::Disposition(TransportDisposition::Accepted)]).await;
        let exchange = StreamExchange::default();
        exchange
            .send(
                "job-a",
                "task-a",
                &format!("http://{addr}"),
                vec![small_batch()],
            )
            .await
            .expect("peer accepted the push; send must succeed");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            received.lock().unwrap().as_slice(),
            &[("job-a".to_owned(), "task-a".to_owned())]
        );
        server.abort();
    }

    #[tokio::test]
    async fn send_treats_duplicate_disposition_as_success() {
        let (addr, ..) =
            spawn_mock_peer(vec![MockOutcome::Disposition(TransportDisposition::Duplicate)]).await;
        let exchange = StreamExchange::default();
        exchange
            .send(
                "job-b",
                "task-b",
                &format!("http://{addr}"),
                vec![small_batch()],
            )
            .await
            .expect("an at-least-once duplicate must be treated as success");
    }

    #[tokio::test]
    async fn send_retries_transient_unavailable_then_succeeds() {
        let (addr, calls, ..) = spawn_mock_peer(vec![
            MockOutcome::Status(tonic::Code::Unavailable),
            MockOutcome::Disposition(TransportDisposition::Accepted),
        ])
        .await;
        let exchange = StreamExchange::default();
        exchange
            .send(
                "job-c",
                "task-c",
                &format!("http://{addr}"),
                vec![small_batch()],
            )
            .await
            .expect("must recover after a single transient failure");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn send_fails_on_non_retryable_rejection_without_retrying() {
        let (addr, calls, ..) =
            spawn_mock_peer(vec![MockOutcome::Status(tonic::Code::InvalidArgument)]).await;
        let exchange = StreamExchange::default();
        let err = exchange
            .send(
                "job-d",
                "task-d",
                &format!("http://{addr}"),
                vec![small_batch()],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ExecutorError::LocalExecution { .. }));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a permanent rejection must not be retried"
        );
    }

    /// Phase 58 #180 regression: a peer that accepts the TCP connection but
    /// never completes the HTTP/2 handshake (simulating a wedged executor or
    /// a network partition that drops traffic post-connect) must not hang
    /// this call past its own retry budget — every attempt is bounded by
    /// `EXCHANGE_RPC_TIMEOUT`. Slow by design: it exercises the full
    /// `EXCHANGE_MAX_RETRIES` budget rather than a single attempt, so the
    /// bound genuinely proves retry-loop termination, not just one timeout.
    #[tokio::test]
    async fn send_bounds_a_hung_peer_within_its_retry_budget() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                // Accept and hold the connection open without ever reading or
                // writing — the peer never completes the HTTP/2 handshake.
                std::mem::forget(socket);
            }
        });

        let exchange = StreamExchange::default();
        let generous_bound =
            EXCHANGE_RPC_TIMEOUT * EXCHANGE_MAX_RETRIES + std::time::Duration::from_secs(10);
        let start = tokio::time::Instant::now();
        let result = tokio::time::timeout(
            generous_bound,
            exchange.send(
                "job-hang",
                "task-hang",
                &format!("http://{addr}"),
                vec![small_batch()],
            ),
        )
        .await
        .expect(
            "send must return within its own retry budget even when every attempt hits a \
             peer that never responds — it must never hang indefinitely",
        );
        assert!(
            result.is_err(),
            "a permanently hung peer must exhaust the retry budget as an error, not succeed"
        );
        assert!(start.elapsed() < generous_bound);
    }
}
