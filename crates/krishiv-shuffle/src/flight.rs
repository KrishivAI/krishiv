//! Arrow Flight shuffle service (B4).
//!
//! Replaces the previous hand-rolled `\n`-delimited TCP framing with a real
//! `arrow-flight` gRPC service.  Tickets carry `<job_id>/<stage_id>/<partition>`
//! UTF-8 bytes; partitions stream back as Arrow IPC `FlightData` messages.
//!
//! Benefits over the legacy protocol:
//! * TLS / mTLS via the same `tonic::transport` plumbing as the rest of the
//!   control-plane, instead of plaintext TCP.
//! * Native flow-control through gRPC streaming.
//! * Standard tooling can introspect shuffle output (`flight-cli`, etc.).
//! * No bespoke 4-byte length-prefix parser that previously capped partitions
//!   at 256 MiB and offered no resume.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::FlightData;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::{StreamExt, TryStreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::{PartitionId, ShuffleStore, error::MAX_SHUFFLE_TICKET_LEN};

/// Extract the ticket bytes from a `FlightDescriptor`.
///
/// Prefers `path[0]` (string ticket); falls back to the raw `cmd` bytes.
fn descriptor_ticket_bytes(d: &FlightDescriptor) -> Result<&[u8], Status> {
    if let Some(path) = d.path.first() {
        return Ok(path.as_bytes());
    }
    if !d.cmd.is_empty() {
        return Ok(&d.cmd);
    }
    Err(Status::invalid_argument(
        "FlightDescriptor must have path[0] or cmd set to '<job>/<stage>/<partition>'",
    ))
}

fn parse_ticket(ticket_bytes: &[u8]) -> Result<(String, String, u32), Status> {
    if ticket_bytes.len() > MAX_SHUFFLE_TICKET_LEN {
        return Err(Status::invalid_argument(format!(
            "shuffle ticket exceeds {MAX_SHUFFLE_TICKET_LEN} bytes"
        )));
    }
    let ticket = std::str::from_utf8(ticket_bytes)
        .map_err(|e| Status::invalid_argument(format!("invalid ticket utf8: {e}")))?;
    let parts: Vec<&str> = ticket.trim().splitn(3, '/').collect();
    if parts.len() != 3 {
        return Err(Status::invalid_argument(
            "ticket must be '<job_id>/<stage_id>/<partition>'",
        ));
    }
    let partition_id = parts
        .get(2)
        .ok_or_else(|| Status::invalid_argument("ticket missing partition segment"))?
        .parse::<u32>()
        .map_err(|e| Status::invalid_argument(format!("partition id not a u32: {e}")))?;
    let job_id = (*parts
        .first()
        .ok_or_else(|| Status::invalid_argument("ticket missing job_id"))?)
    .to_string();
    let stage_id = (*parts
        .get(1)
        .ok_or_else(|| Status::invalid_argument("ticket missing stage_id"))?)
    .to_string();
    Ok((job_id, stage_id, partition_id))
}

/// Arrow Flight shuffle service backed by any [`ShuffleStore`] implementation.
///
/// A3: Generic over `S` so callers can back the service with `LocalDiskShuffleStore`,
/// `InMemoryShuffleStore`, or any future backend without changing this module.
#[derive(Clone)]
pub struct ShuffleFlightService<S: ShuffleStore + Send + Sync + 'static> {
    store: Arc<S>,
}

impl<S: ShuffleStore + Send + Sync + 'static> ShuffleFlightService<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

type BoxedFlightStream<T> =
    Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl<S: ShuffleStore + Send + Sync + 'static> FlightService for ShuffleFlightService<S> {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        // Anonymous handshake: shuffle service runs on the cluster network
        // and is fronted by the same TLS+auth proxy as the coordinator.
        let (tx, rx) = mpsc::channel::<Result<HandshakeResponse, Status>>(1);
        drop(tx);
        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::HandshakeStream))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        // Shuffle partitions are accessed by known ticket, not discovered.
        // Return an empty stream — clients always know their partition IDs.
        let (tx, rx) = mpsc::channel::<Result<FlightInfo, Status>>(1);
        drop(tx);
        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::ListFlightsStream
        ))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let ticket_bytes = descriptor_ticket_bytes(&descriptor)?;
        let (job_id, stage_id, partition) = parse_ticket(ticket_bytes)?;
        let id = PartitionId {
            job_id,
            stage_id,
            partition,
        };
        let part = self
            .store
            .read_partition(&id)
            .await
            .map_err(|e| Status::internal(format!("read_partition: {e}")))?
            .ok_or_else(|| Status::not_found(format!("partition not found: {id:?}")))?;

        let ticket_str = format!("{}/{}/{}", id.job_id, id.stage_id, id.partition);
        let ticket = Ticket {
            ticket: ticket_str.into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let total_records: i64 = part.batches.iter().map(|b| b.num_rows() as i64).sum();
        let info = FlightInfo::new()
            .try_with_schema(&part.schema)
            .map_err(|e| Status::internal(format!("schema encode: {e}")))?
            .with_descriptor(descriptor)
            .with_endpoint(endpoint)
            .with_total_records(total_records)
            .with_total_bytes(-1);
        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        // Shuffle partitions are write-once: once available, they are complete.
        let descriptor = request.into_inner();
        let ticket_bytes = descriptor_ticket_bytes(&descriptor)?;
        let (job_id, stage_id, partition) = parse_ticket(ticket_bytes)?;
        let id = PartitionId {
            job_id,
            stage_id,
            partition,
        };
        let exists = self
            .store
            .read_partition(&id)
            .await
            .map_err(|e| Status::internal(format!("read_partition: {e}")))?
            .is_some();
        if !exists {
            // Not yet written — tell the client to poll again later.
            return Ok(Response::new(PollInfo {
                info: None,
                flight_descriptor: Some(descriptor),
                progress: Some(0.0),
                expiration_time: None,
            }));
        }
        // Partition is ready — return full info.
        let inner_req = Request::new(descriptor.clone());
        let flight_info = self.get_flight_info(inner_req).await?.into_inner();
        Ok(Response::new(PollInfo {
            info: Some(flight_info),
            flight_descriptor: None,
            progress: Some(1.0),
            expiration_time: None,
        }))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let ticket_bytes = descriptor_ticket_bytes(&descriptor)?;
        let (job_id, stage_id, partition) = parse_ticket(ticket_bytes)?;
        let id = PartitionId {
            job_id,
            stage_id,
            partition,
        };
        let part = self
            .store
            .read_partition(&id)
            .await
            .map_err(|e| Status::internal(format!("read_partition: {e}")))?
            .ok_or_else(|| Status::not_found(format!("partition not found: {id:?}")))?;
        let schema_result =
            SchemaResult::try_from(SchemaAsIpc::new(&part.schema, &IpcWriteOptions::default()))
                .map_err(|e| Status::internal(format!("schema encode: {e}")))?;
        Ok(Response::new(schema_result))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let (job_id, stage_id, partition) = parse_ticket(&ticket.ticket)?;
        let id = PartitionId {
            job_id,
            stage_id,
            partition,
        };

        let partition_data = self
            .store
            .stream_partition(&id)
            .await
            .map_err(|e| Status::internal(format!("stream_partition: {e}")))?;
        let partition_data = partition_data
            .ok_or_else(|| Status::not_found(format!("partition {id:?} not found")))?;

        let schema: SchemaRef = partition_data.schema;
        let stream = partition_data
            .batches
            .map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e)));

        let encoder = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_options(IpcWriteOptions::default())
            .build(stream);

        let mapped = encoder.map_err(|e| Status::internal(format!("flight encode: {e}")));
        Ok(Response::new(Box::pin(mapped) as Self::DoGetStream))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        use arrow_flight::decode::FlightRecordBatchStream;

        let mut stream = request.into_inner();

        // The first FlightData message carries the FlightDescriptor with the
        // partition ticket and optional lease token.
        let first = stream
            .message()
            .await
            .map_err(|e| Status::invalid_argument(format!("reading first message: {e}")))?
            .ok_or_else(|| Status::invalid_argument("do_put stream was empty"))?;

        let descriptor = first.flight_descriptor.as_ref().ok_or_else(|| {
            Status::invalid_argument("first FlightData must carry a FlightDescriptor")
        })?;

        if descriptor.path.is_empty() {
            return Err(Status::invalid_argument(
                "FlightDescriptor.path[0] must be the partition ticket '<job>/<stage>/<partition>'",
            ));
        }
        let (job_id, stage_id, partition) = parse_ticket(
            descriptor
                .path
                .first()
                .ok_or_else(|| Status::invalid_argument("descriptor.path is empty"))?
                .as_bytes(),
        )?;
        // B6: Make lease_token required — reject absent or unparseable tokens.
        let lease_token: u64 = descriptor
            .path
            .get(1)
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| {
                Status::invalid_argument(
                    "missing or invalid lease_token in FlightDescriptor.path[1]",
                )
            })?;

        let id = PartitionId {
            job_id,
            stage_id,
            partition,
        };

        // B6: Register the lease before writing so the two-phase protocol is
        // honoured on the Flight path, matching the HTTP path.
        self.store
            .register_partition_lease(id.clone(), lease_token)
            .await
            .map_err(|e| Status::invalid_argument(format!("register_partition_lease: {e}")))?;

        // Re-assemble a stream that starts with the first (schema) message.
        let schema_msg = futures::stream::once(async move {
            Ok::<FlightData, arrow_flight::error::FlightError>(first)
        });
        let rest = stream.map_err(|e: tonic::Status| {
            arrow_flight::error::FlightError::from_external_error(Box::new(e))
        });
        let combined = schema_msg.chain(rest);

        let decoder = FlightRecordBatchStream::new_from_flight_data(combined);
        let batches: Vec<RecordBatch> = decoder
            .map_err(|e| Status::internal(format!("flight decode: {e}")))
            .try_collect()
            .await?;

        // Schema comes from the decoded stream; if empty, use the batch schema.
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| arrow::datatypes::SchemaRef::new(arrow::datatypes::Schema::empty()));

        let partition = crate::ShufflePartition {
            id,
            schema,
            batches,
        };
        self.store
            .write_partition(partition, lease_token)
            .await
            .map_err(|e| Status::internal(format!("write_partition: {e}")))?;

        // Return an empty PutResult stream — the write has been committed.
        let (tx, rx) = mpsc::channel::<Result<PutResult, Status>>(1);
        drop(tx);
        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::DoPutStream
        ))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}

/// Start the Arrow Flight shuffle server on `addr` backed by `store`.
///
/// Returns the bound local address and a join handle.  Aborting the handle
/// stops the server.
///
/// SEC-3 (Phase 63): the shuffle data plane carries intermediate query results
/// (real user data) between executors. The token is resolved from
/// `KRISHIV_SHUFFLE_TOKEN` / `KRISHIV_SHUFFLE_TOKEN_FILE`; under a
/// durable/production profile a missing token is a fail-closed startup error,
/// mirroring the HTTP shuffle service and the executor task-auth guard. When a
/// token is configured, every RPC must present `authorization: Bearer <token>`.
pub async fn serve<S: ShuffleStore + Send + Sync + 'static>(
    addr: SocketAddr,
    store: Arc<S>,
) -> io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let token = crate::token_auth::resolve_shuffle_token();
    crate::token_auth::require_shuffle_token_or_fail(
        token.is_some(),
        krishiv_common::resolve_durability_profile(),
    )
    .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e.to_string()))?;
    serve_with_token(addr, store, token).await
}

/// Start the shuffle Flight server with an explicit auth token.
///
/// Factored out of [`serve`] so tests can drive the interceptor hermetically
/// without mutating process-global environment state. `token == None` disables
/// the per-request check (only reachable under `DevLocal`, enforced by the
/// startup guard in [`serve`]).
pub(crate) async fn serve_with_token<S: ShuffleStore + Send + Sync + 'static>(
    addr: SocketAddr,
    store: Arc<S>,
    token: Option<String>,
) -> io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let service = ShuffleFlightService::new(store);
    let incoming = tonic::transport::server::TcpIncoming::from(listener);
    // One interceptor type for both auth-on and auth-off so `add_service`
    // receives a single concrete service type. When `token` is `None` the
    // interceptor is a pass-through.
    let intercepted = FlightServiceServer::with_interceptor(
        service,
        move |req: Request<()>| -> Result<Request<()>, Status> {
            let provided = req
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if crate::token_auth::bearer_ok(provided, token.as_deref()) {
                Ok(req)
            } else {
                Err(Status::unauthenticated(
                    "shuffle: missing or invalid bearer token (SEC-3)",
                ))
            }
        },
    );
    let handle = tokio::spawn(async move {
        if let Err(error) = Server::builder()
            .layer(krishiv_metrics::grpc::GrpcDurationLayer)
            .add_service(intercepted)
            .serve_with_incoming(incoming)
            .await
        {
            tracing::warn!(error = %error, "shuffle flight server exited with error");
        }
    });
    Ok((local_addr, handle))
}

/// Attach `authorization: Bearer <token>` to an outgoing shuffle RPC when a
/// shuffle token is configured for this process (SEC-3). No-op under `DevLocal`
/// with no token set.
fn attach_shuffle_auth<T>(request: &mut Request<T>) -> io::Result<()> {
    if let Some(tok) = crate::token_auth::cached_shuffle_token() {
        let value: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
            format!("Bearer {tok}").parse().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid shuffle token (not a valid header value): {e}"),
                )
            })?;
        request.metadata_mut().insert("authorization", value);
    }
    Ok(())
}

/// Default number of fetch attempts (1 initial try + 3 retries).
pub const DEFAULT_FETCH_MAX_ATTEMPTS: u32 = 4;
/// Default base delay between fetch retries; doubles per attempt.
pub const DEFAULT_FETCH_RETRY_BASE_MS: u64 = 100;
/// Upper bound on a single retry backoff delay.
const FETCH_RETRY_MAX_DELAY_MS: u64 = 5_000;

/// Retry policy for shuffle partition fetches over Flight.
///
/// Transient transport failures (connection refused, stream resets, decode
/// truncation) are retried with exponential backoff. `NotFound` (the
/// partition does not exist — typically the producer executor died and its
/// output is gone) and `InvalidInput` (malformed endpoint) fail immediately
/// so the scheduler can react instead of the consumer spinning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchRetryPolicy {
    /// Total attempts including the first one. Values below 1 behave as 1.
    pub max_attempts: u32,
    /// Backoff before retry `n` is `base_delay_ms * 2^(n-1)`, capped at 5 s.
    pub base_delay_ms: u64,
}

impl Default for FetchRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_FETCH_MAX_ATTEMPTS,
            base_delay_ms: DEFAULT_FETCH_RETRY_BASE_MS,
        }
    }
}

impl FetchRetryPolicy {
    /// Resolve a policy from raw env-var values. `None`, unparseable, and
    /// zero attempt counts fall back to the defaults; `base_delay_ms` of 0 is
    /// allowed (retry without sleeping, useful in tests).
    pub fn resolve(raw_max_attempts: Option<&str>, raw_base_delay_ms: Option<&str>) -> Self {
        let max_attempts = raw_max_attempts
            .and_then(|s| s.trim().parse::<u32>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_FETCH_MAX_ATTEMPTS);
        let base_delay_ms = raw_base_delay_ms
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_FETCH_RETRY_BASE_MS);
        Self {
            max_attempts,
            base_delay_ms,
        }
    }

    /// Resolve the policy from `KRISHIV_SHUFFLE_FETCH_RETRIES` (total
    /// attempts) and `KRISHIV_SHUFFLE_FETCH_RETRY_BASE_MS`.
    pub fn from_env() -> Self {
        Self::resolve(
            std::env::var("KRISHIV_SHUFFLE_FETCH_RETRIES")
                .ok()
                .as_deref(),
            std::env::var("KRISHIV_SHUFFLE_FETCH_RETRY_BASE_MS")
                .ok()
                .as_deref(),
        )
    }

    /// Backoff delay before retrying after failed attempt number `attempt`
    /// (1-based).
    fn delay_after_attempt(&self, attempt: u32) -> std::time::Duration {
        let factor = 1u64 << attempt.saturating_sub(1).min(16);
        std::time::Duration::from_millis(
            self.base_delay_ms
                .saturating_mul(factor)
                .min(FETCH_RETRY_MAX_DELAY_MS),
        )
    }
}

/// `true` when a fetch failure is plausibly transient and worth retrying.
fn is_retryable_fetch_error(error: &io::Error) -> bool {
    !matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::InvalidInput
    )
}

/// Client for fetching shuffle partitions over Arrow Flight.
pub struct FlightShuffleClient;

impl FlightShuffleClient {
    /// Fetch all [`RecordBatch`]es for one shuffle partition from a remote
    /// shuffle Flight server.
    ///
    /// `endpoint` accepts either `<host>:<port>` or a full URL
    /// (`http://<host>:<port>`).
    pub async fn fetch(
        endpoint: impl Into<String>,
        job_id: &str,
        stage_id: &str,
        partition_id: u32,
    ) -> io::Result<Vec<RecordBatch>> {
        let raw = endpoint.into();
        let url = if raw.starts_with("http://") || raw.starts_with("https://") {
            raw
        } else {
            format!("http://{raw}")
        };

        let channel = tonic::transport::Endpoint::from_shared(url)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
            .connect()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e.to_string()))?;

        let mut client = arrow_flight::flight_service_client::FlightServiceClient::new(channel);
        let ticket_text = format!("{job_id}/{stage_id}/{partition_id}");
        let ticket = Ticket {
            ticket: ticket_text.into_bytes().into(),
        };
        let mut do_get_req = Request::new(ticket);
        attach_shuffle_auth(&mut do_get_req)?;
        let stream = client
            .do_get(do_get_req)
            .await
            .map_err(|e| {
                if e.code() == tonic::Code::NotFound {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "partition {job_id}/{stage_id}/{partition_id} not found: {}",
                            e.message()
                        ),
                    )
                } else {
                    io::Error::other(e.to_string())
                }
            })?
            .into_inner();

        let decoder = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(arrow_flight::error::FlightError::from),
        );
        let batches: Vec<RecordBatch> = decoder
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
            .try_collect()
            .await?;
        Ok(batches)
    }

    /// Fetch one shuffle partition, retrying transient failures per `policy`.
    ///
    /// Permanent failures — `NotFound` (missing partition) and
    /// `InvalidInput` (malformed endpoint) — are returned immediately without
    /// retrying. All other errors are retried with exponential backoff until
    /// `policy.max_attempts` is exhausted; the last error is returned.
    pub async fn fetch_with_retry(
        endpoint: impl Into<String>,
        job_id: &str,
        stage_id: &str,
        partition_id: u32,
        policy: FetchRetryPolicy,
    ) -> io::Result<Vec<RecordBatch>> {
        let endpoint: String = endpoint.into();
        let max_attempts = policy.max_attempts.max(1);
        let mut attempt = 1u32;
        // T19: classify the endpoint as local (loopback) or remote so
        // the `local_blocks_fetched` / `remote_blocks_fetched` counters
        // are accurate.
        let is_local = endpoint.starts_with("http://localhost")
            || endpoint.starts_with("http://127.0.0.1")
            || endpoint.starts_with("http://[::1]")
            || !endpoint.contains("://");
        let fetch_started = std::time::Instant::now();
        loop {
            match Self::fetch(endpoint.clone(), job_id, stage_id, partition_id).await {
                Ok(batches) => {
                    let read_elapsed_us = fetch_started.elapsed().as_micros() as u64;
                    let bytes_read: u64 = batches
                        .iter()
                        .map(|b| b.get_array_memory_size() as u64)
                        .sum();
                    let rows_read: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                    krishiv_metrics::global_metrics().add_shuffle_read_bytes(bytes_read);
                    krishiv_metrics::global_metrics().add_shuffle_read_records(rows_read);
                    krishiv_metrics::global_metrics().add_shuffle_read_time_us(read_elapsed_us);
                    krishiv_metrics::global_metrics()
                        .add_shuffle_fetch_wait_time_us(read_elapsed_us);
                    if is_local {
                        krishiv_metrics::global_metrics().add_shuffle_local_blocks_fetched(1);
                    } else {
                        krishiv_metrics::global_metrics().add_shuffle_remote_blocks_fetched(1);
                    }
                    return Ok(batches);
                }
                Err(error) if attempt < max_attempts && is_retryable_fetch_error(&error) => {
                    let delay = policy.delay_after_attempt(attempt);
                    tracing::warn!(
                        endpoint = %endpoint,
                        job_id,
                        stage_id,
                        partition_id,
                        attempt,
                        max_attempts,
                        delay_ms = delay.as_millis() as u64,
                        error = %error,
                        "transient shuffle fetch failure; retrying"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(error) => {
                    // We fall here in two ways:
                    //  * a genuinely permanent error (`NotFound` / `InvalidInput`)
                    //    — pass it through unchanged, and
                    //  * a *transport* error (connection refused, unavailable,
                    //    deadline) that survived every retry attempt.
                    //
                    // The second case means the producing executor's Flight
                    // server is unreachable after `max_attempts` — operationally
                    // the partition is gone (the executor was killed / evicted).
                    // Surface it as `NotFound` so the task runner maps it to
                    // `ShufflePartitionMissing`, the consumer reports the
                    // partition missing, and the scheduler regenerates the
                    // producer on a healthy executor. Without this the consumer
                    // returns an opaque transport error that triggers NO shuffle
                    // regeneration; it just burns the task's retry budget against
                    // the dead endpoint and the whole job fails unrecoverably
                    // (observed live on a 3-node cluster: batch job "Failed",
                    // one reduce task never recovered after its producer's pod
                    // was deleted mid-fetch).
                    if is_retryable_fetch_error(&error) {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!(
                                "shuffle partition {job_id}/{stage_id}/{partition_id} \
                                 unreachable after {max_attempts} attempts (producer \
                                 executor gone): {error}"
                            ),
                        ));
                    }
                    return Err(error);
                }
            }
        }
    }

    /// Push a shuffle partition to a remote shuffle Flight server.
    ///
    /// `endpoint` accepts either `<host>:<port>` or a full `http://…` URL.
    /// `lease_token` must match or exceed the current lease generation for the
    /// partition (use `1` for the first write to an unregistered partition).
    pub async fn push(
        endpoint: impl Into<String>,
        job_id: &str,
        stage_id: &str,
        partition_id: u32,
        batches: Vec<RecordBatch>,
        lease_token: u64,
    ) -> io::Result<()> {
        let raw = endpoint.into();
        let url = if raw.starts_with("http://") || raw.starts_with("https://") {
            raw
        } else {
            format!("http://{raw}")
        };

        let channel = tonic::transport::Endpoint::from_shared(url)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
            .connect()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e.to_string()))?;

        let mut client = arrow_flight::flight_service_client::FlightServiceClient::new(channel);

        let ticket_text = format!("{job_id}/{stage_id}/{partition_id}");
        let descriptor = FlightDescriptor {
            r#type: arrow_flight::flight_descriptor::DescriptorType::Path as i32,
            path: vec![ticket_text, lease_token.to_string()],
            ..Default::default()
        };

        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| arrow::datatypes::SchemaRef::new(arrow::datatypes::Schema::empty()));

        let batch_stream = futures::stream::iter(
            batches
                .into_iter()
                .map(Ok::<_, arrow_flight::error::FlightError>),
        );
        let encoder = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_flight_descriptor(Some(descriptor))
            .with_options(IpcWriteOptions::default())
            .build(batch_stream);

        // Collect encoder output first to propagate encoding errors before streaming.
        let flight_data: Vec<FlightData> = encoder
            .try_collect()
            .await
            .map_err(|e| io::Error::other(format!("Arrow IPC encoding error: {e}")))?;
        let flight_stream = futures::stream::iter(flight_data);
        let mut do_put_req = Request::new(flight_stream);
        attach_shuffle_auth(&mut do_put_req)?;
        client
            .do_put(do_put_req)
            .await
            .map_err(|e: tonic::Status| io::Error::other(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::{LocalDiskShuffleStore, PartitionId, ShufflePartition};

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flight_server_serves_partition_and_client_reads_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());

        let batch = make_test_batch();

        let id = PartitionId {
            job_id: "job-flight-1".to_owned(),
            stage_id: "s0".to_owned(),
            partition: 0,
        };
        let partition = ShufflePartition {
            id: id.clone(),
            schema: batch.schema(),
            batches: vec![batch.clone()],
        };
        store.register_partition_lease(id.clone(), 1).await.unwrap();
        store.write_partition(partition, 1).await.unwrap();

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (local_addr, server_handle) = serve(addr, Arc::clone(&store)).await.unwrap();

        // Give tonic a moment to start accepting connections.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let endpoint = local_addr.to_string();
        let result = FlightShuffleClient::fetch(&endpoint, "job-flight-1", "s0", 0)
            .await
            .unwrap();

        server_handle.abort();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);
        assert_eq!(result[0].num_columns(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flight_client_returns_error_for_missing_partition() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (local_addr, server_handle) = serve(addr, Arc::clone(&store)).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let endpoint = local_addr.to_string();

        let result = FlightShuffleClient::fetch(&endpoint, "missing", "s0", 0).await;
        server_handle.abort();

        assert!(
            matches!(result, Err(ref e) if e.kind() == std::io::ErrorKind::NotFound),
            "expected NotFound, got: {result:?}"
        );
    }

    /// SEC-3 (Phase 63): when a shuffle token is configured, the Flight shuffle
    /// server must reject RPCs that carry no `Authorization` header or the wrong
    /// token, and accept only the exact `Bearer <token>`. Driven through a raw
    /// Flight client so the test sets headers explicitly and never mutates the
    /// process-global token cache used by [`FlightShuffleClient`].
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sec3_flight_shuffle_enforces_bearer_token() {
        use arrow_flight::flight_service_client::FlightServiceClient;

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());
        let batch = make_test_batch();
        let id = PartitionId {
            job_id: "job-auth".to_owned(),
            stage_id: "s0".to_owned(),
            partition: 0,
        };
        store.register_partition_lease(id.clone(), 1).await.unwrap();
        store
            .write_partition(
                ShufflePartition {
                    id: id.clone(),
                    schema: batch.schema(),
                    batches: vec![batch.clone()],
                },
                1,
            )
            .await
            .unwrap();

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (local_addr, server_handle) =
            serve_with_token(addr, Arc::clone(&store), Some("s3cret".to_owned()))
                .await
                .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let url = format!("http://{local_addr}");
        let ticket = Ticket {
            ticket: b"job-auth/s0/0".to_vec().into(),
        };

        let connect = || {
            let url = url.clone();
            async move {
                let channel = tonic::transport::Endpoint::from_shared(url)
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
                FlightServiceClient::new(channel)
            }
        };

        // 1) No credentials → Unauthenticated.
        let mut client = connect().await;
        let unauth = client.do_get(Request::new(ticket.clone())).await;
        assert_eq!(
            unauth.err().map(|e| e.code()),
            Some(tonic::Code::Unauthenticated),
            "missing token must be rejected"
        );

        // 2) Wrong token → Unauthenticated.
        let mut client = connect().await;
        let mut req = Request::new(ticket.clone());
        req.metadata_mut()
            .insert("authorization", "Bearer wrong".parse().unwrap());
        assert_eq!(
            client.do_get(req).await.err().map(|e| e.code()),
            Some(tonic::Code::Unauthenticated),
            "wrong token must be rejected"
        );

        // 3) Correct token → accepted.
        let mut client = connect().await;
        let mut req = Request::new(ticket);
        req.metadata_mut()
            .insert("authorization", "Bearer s3cret".parse().unwrap());
        let ok = client.do_get(req).await;
        server_handle.abort();
        assert!(ok.is_ok(), "valid token must be accepted: {ok:?}");
    }

    #[test]
    fn fetch_retry_policy_resolves_defaults_and_overrides() {
        assert_eq!(
            FetchRetryPolicy::resolve(None, None),
            FetchRetryPolicy::default()
        );
        assert_eq!(
            FetchRetryPolicy::resolve(Some("garbage"), Some("garbage")),
            FetchRetryPolicy::default()
        );
        // Zero attempts is meaningless; falls back to the default.
        assert_eq!(
            FetchRetryPolicy::resolve(Some("0"), None).max_attempts,
            DEFAULT_FETCH_MAX_ATTEMPTS
        );
        let policy = FetchRetryPolicy::resolve(Some("7"), Some("250"));
        assert_eq!(policy.max_attempts, 7);
        assert_eq!(policy.base_delay_ms, 250);
        // Zero base delay is allowed (retry without sleeping).
        assert_eq!(FetchRetryPolicy::resolve(None, Some("0")).base_delay_ms, 0);
    }

    #[test]
    fn fetch_retry_backoff_doubles_and_caps() {
        let policy = FetchRetryPolicy {
            max_attempts: 10,
            base_delay_ms: 100,
        };
        assert_eq!(policy.delay_after_attempt(1).as_millis(), 100);
        assert_eq!(policy.delay_after_attempt(2).as_millis(), 200);
        assert_eq!(policy.delay_after_attempt(3).as_millis(), 400);
        // Caps at 5 s no matter how many attempts have failed.
        assert_eq!(policy.delay_after_attempt(30).as_millis(), 5_000);
    }

    #[test]
    fn fetch_error_retryability_classification() {
        let transient = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        assert!(is_retryable_fetch_error(&transient));
        let decode = std::io::Error::new(std::io::ErrorKind::InvalidData, "truncated stream");
        assert!(is_retryable_fetch_error(&decode));
        let missing = std::io::Error::new(std::io::ErrorKind::NotFound, "partition gone");
        assert!(!is_retryable_fetch_error(&missing));
        let bad_endpoint = std::io::Error::new(std::io::ErrorKind::InvalidInput, "bad url");
        assert!(!is_retryable_fetch_error(&bad_endpoint));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_with_retry_recovers_after_server_becomes_available() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());

        let batch = make_test_batch();
        let id = PartitionId {
            job_id: "job-retry-1".to_owned(),
            stage_id: "s0".to_owned(),
            partition: 0,
        };
        let partition = ShufflePartition {
            id: id.clone(),
            schema: batch.schema(),
            batches: vec![batch.clone()],
        };
        store.register_partition_lease(id.clone(), 1).await.unwrap();
        store.write_partition(partition, 1).await.unwrap();

        // Reserve a port, then drop the listener so the first fetch attempt
        // gets connection-refused; start the real server on that port while
        // the client is backing off.
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        let server_store = Arc::clone(&store);
        let server_task = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            serve(addr, server_store).await
        });

        let policy = FetchRetryPolicy {
            max_attempts: 10,
            base_delay_ms: 100,
        };
        let result =
            FlightShuffleClient::fetch_with_retry(addr.to_string(), "job-retry-1", "s0", 0, policy)
                .await;

        if let Ok(Ok((_, handle))) = server_task.await {
            handle.abort();
        }

        let batches = result.expect("fetch must succeed once the server is up");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_with_retry_fails_fast_on_missing_partition() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (local_addr, server_handle) = serve(addr, Arc::clone(&store)).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let policy = FetchRetryPolicy {
            max_attempts: 5,
            base_delay_ms: 200,
        };
        let started = std::time::Instant::now();
        let result = FlightShuffleClient::fetch_with_retry(
            local_addr.to_string(),
            "missing",
            "s0",
            0,
            policy,
        )
        .await;
        let elapsed = started.elapsed();
        server_handle.abort();

        assert!(
            matches!(result, Err(ref e) if e.kind() == std::io::ErrorKind::NotFound),
            "expected NotFound, got: {result:?}"
        );
        assert!(
            elapsed < std::time::Duration::from_millis(200),
            "NotFound must fail fast without backoff sleeps; took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn fetch_with_retry_maps_unreachable_producer_to_not_found() {
        // Bind then immediately drop a listener to obtain a port that is
        // guaranteed closed (connection refused) — simulating a producer
        // executor whose Flight server was killed mid-fetch.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = listener.local_addr().unwrap();
        drop(listener);

        let policy = FetchRetryPolicy {
            max_attempts: 3,
            base_delay_ms: 0, // retry without sleeping
        };
        let result =
            FlightShuffleClient::fetch_with_retry(dead_addr.to_string(), "job", "s0", 0, policy)
                .await;

        // A dead producer's exhausted transport retries must surface as
        // NotFound so the task runner maps it to ShufflePartitionMissing, the
        // consumer reports the partition missing, and the scheduler
        // regenerates the producer — instead of an opaque transport error that
        // triggers no recovery and just fails the job.
        assert!(
            matches!(result, Err(ref e) if e.kind() == std::io::ErrorKind::NotFound),
            "unreachable producer after retries must map to NotFound, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn fetch_with_retry_converts_unreachable_even_with_no_retries() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = listener.local_addr().unwrap();
        drop(listener);

        // max_attempts = 1: the retry arm never fires, so the single failed
        // attempt falls straight through to the terminal arm. It must STILL be
        // converted to NotFound (producer gone) rather than leaking the raw
        // ConnectionRefused, which would trigger no shuffle regeneration.
        let policy = FetchRetryPolicy {
            max_attempts: 1,
            base_delay_ms: 0,
        };
        let result =
            FlightShuffleClient::fetch_with_retry(dead_addr.to_string(), "j", "s0", 0, policy)
                .await;
        assert!(
            matches!(result, Err(ref e) if e.kind() == std::io::ErrorKind::NotFound),
            "single-attempt unreachable must map to NotFound, got: {result:?}"
        );
    }
}
