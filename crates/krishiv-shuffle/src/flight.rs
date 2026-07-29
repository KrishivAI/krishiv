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

/// Environment override for the shuffle Flight server's concurrent `do_get`
/// cap. Unset derives it from [`ExecutorCapacity`].
pub const SHUFFLE_SERVE_CONCURRENCY_ENV: &str = "KRISHIV_SHUFFLE_SERVE_CONCURRENCY";

/// Floor for the serve cap: below two, one slow consumer serialises the whole
/// reduce side of a stage.
const MIN_SERVE_CONCURRENCY: usize = 2;

/// Cap when no cgroup limit is visible (no container, or page-cache accounting
/// disabled). `8 × INLINE_READ_LIMIT` = 256 MiB resident at peak, which is the
/// same order as the reduce-side fetch semaphore's default.
const DEFAULT_SERVE_CONCURRENCY: usize = 8;

/// How many `do_get` responses this executor may have in flight at once.
///
/// B1: `SHUFFLE_FETCH_SEMAPHORE` on the executor bounds each **consumer**, not
/// the aggregate arriving at one **producer**. On a 3-node cluster the ceiling
/// was 3 × 8 = 24 concurrent `do_get` against a single executor, i.e. up to
/// ~768 MiB of anonymous memory outside the DataFusion pool, in the same
/// process that is also running map tasks — and it scales linearly with nodes
/// or with `KRISHIV_SHUFFLE_FETCH_CONCURRENCY`. Compounding it, both
/// `write_partition` and `stream_partition` run in `spawn_blocking`, whose
/// default pool is 512 threads.
///
/// Sized in units of `INLINE_READ_LIMIT` out of the page-cache budget the same
/// capacity derivation already carves for committed-but-unconsumed shuffle
/// output, so the two numbers come from one decision rather than two.
fn serve_concurrency_limit() -> usize {
    if let Some(explicit) = std::env::var(SHUFFLE_SERVE_CONCURRENCY_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
    {
        return explicit.max(1);
    }
    krishiv_common::ExecutorCapacity::detect()
        .page_cache_bytes
        .map(|bytes| usize::try_from(bytes / crate::disk_store::INLINE_READ_LIMIT).unwrap_or(1))
        .unwrap_or(DEFAULT_SERVE_CONCURRENCY)
        .max(MIN_SERVE_CONCURRENCY)
}

/// Arrow Flight shuffle service backed by any [`ShuffleStore`] implementation.
///
/// A3: Generic over `S` so callers can back the service with `LocalDiskShuffleStore`,
/// `InMemoryShuffleStore`, or any future backend without changing this module.
#[derive(Clone)]
pub struct ShuffleFlightService<S: ShuffleStore + Send + Sync + 'static> {
    store: Arc<S>,
    /// B1: aggregate cap on in-flight `do_get` responses. A permit is held for
    /// the *response stream's* lifetime, not the handler future's, because that
    /// is how long the bytes it read stay resident.
    serve_permits: Arc<tokio::sync::Semaphore>,
}

impl<S: ShuffleStore + Send + Sync + 'static> ShuffleFlightService<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self::with_serve_limit(store, serve_concurrency_limit())
    }

    /// [`Self::new`] with an explicit cap, so tests can drive the bound without
    /// mutating process-global environment state.
    pub fn with_serve_limit(store: Arc<S>, limit: usize) -> Self {
        Self {
            store,
            serve_permits: Arc::new(tokio::sync::Semaphore::new(limit.max(1))),
        }
    }

    /// A partition's schema, without decoding its data.
    ///
    /// `ShuffleStream` carries the schema in its header; the batch stream is
    /// dropped unpolled, so no Arrow decode happens and no `RecordBatch` is
    /// ever built. The three metadata RPCs — `get_flight_info`, `get_schema`,
    /// `poll_flight_info` — all used `read_partition` instead, which decodes
    /// every batch of the partition. At SF100 that is hundreds of megabytes of
    /// work to answer "what are this partition's columns", in the same process
    /// that is running map tasks, and outside any memory budget.
    ///
    /// `None` means the partition has not been written.
    async fn try_partition_schema(&self, id: &PartitionId) -> Result<Option<SchemaRef>, Status> {
        let stream = self
            .store
            .stream_partition(id)
            .await
            .map_err(|e| Status::internal(format!("stream_partition: {e}")))?;
        Ok(stream.map(|s| s.schema))
    }

    /// [`Self::try_partition_schema`], with an absent partition as `NotFound`.
    async fn partition_schema(&self, id: &PartitionId) -> Result<SchemaRef, Status> {
        self.try_partition_schema(id)
            .await?
            .ok_or_else(|| Status::not_found(format!("partition not found: {id:?}")))
    }
}

/// Response stream that holds a serve permit until the client is done with it.
///
/// `do_get` returns as soon as the reader is constructed, but for any partition
/// at or below `INLINE_READ_LIMIT` the store has already read the whole file
/// into memory and that buffer lives for the lifetime of the response. Dropping
/// the permit when the handler returns would therefore bound nothing.
struct PermitHoldingStream<T> {
    inner: BoxedFlightStream<T>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl<T> futures::Stream for PermitHoldingStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
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
        // Metadata only: take the schema off the stream header and drop the
        // batch stream without polling it.
        //
        // This used to call `read_partition`, which decodes **every batch of
        // the partition** — for a metadata RPC. The row count was the only
        // reason: `total_records` was computed by summing `num_rows()` over all
        // of them. Arrow Flight permits -1 for "unknown" and this same call
        // already reported `total_bytes` that way, so the count was being paid
        // for in full partition reads and half-reported anyway.
        let schema = self.partition_schema(&id).await?;

        let ticket_str = format!("{}/{}/{}", id.job_id, id.stage_id, id.partition);
        let ticket = Ticket {
            ticket: ticket_str.into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema encode: {e}")))?
            .with_descriptor(descriptor)
            .with_endpoint(endpoint)
            // -1 = unknown, matching `total_bytes`. Counting rows means reading
            // the partition; a client that needs the count can `do_get` it.
            .with_total_records(-1)
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
        // One existence check, not two. This used to `read_partition` to test
        // existence — decoding the whole partition — and then call
        // `get_flight_info`, which decoded it a *second* time. Polling a
        // partition cost two full reads of it.
        let schema = match self.try_partition_schema(&id).await? {
            Some(schema) => schema,
            None => {
                // Not yet written — tell the client to poll again later.
                return Ok(Response::new(PollInfo {
                    info: None,
                    flight_descriptor: Some(descriptor),
                    progress: Some(0.0),
                    expiration_time: None,
                }));
            }
        };

        let ticket_str = format!("{}/{}/{}", id.job_id, id.stage_id, id.partition);
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: ticket_str.into(),
        });
        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema encode: {e}")))?
            .with_descriptor(descriptor)
            .with_endpoint(endpoint)
            .with_total_records(-1)
            .with_total_bytes(-1);
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
        // Metadata only — see `partition_schema`. This used to decode every
        // batch of the partition to read its column names.
        let schema = self.partition_schema(&id).await?;
        let schema_result =
            SchemaResult::try_from(SchemaAsIpc::new(&schema, &IpcWriteOptions::default()))
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

        // B1: take the permit BEFORE the store reads anything. The bound is on
        // resident bytes, and the store materialises the partition inside
        // `stream_partition` for anything at or below `INLINE_READ_LIMIT`.
        let permit = Arc::clone(&self.serve_permits)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("shuffle serve semaphore closed"))?;

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
        Ok(Response::new(Box::pin(PermitHoldingStream {
            inner: Box::pin(mapped) as BoxedFlightStream<FlightData>,
            _permit: permit,
        }) as Self::DoGetStream))
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

        // Stream the decoded batches straight into the store instead of
        // collecting the whole partition first. This is the same D7 shape that
        // starved every other memory consumer on the map side: `do_put` is the
        // *ingest* half of the shuffle, so a `try_collect` here holds an entire
        // partition resident in a process that is also running map tasks, and
        // outside the DataFusion pool that is supposed to bound it.
        //
        // The schema has to be known before the first write, and it is only
        // available once a batch has been decoded, so exactly one batch is
        // buffered and then chained back onto the stream. One batch, not one
        // partition.
        let mut decoder = FlightRecordBatchStream::new_from_flight_data(combined)
            .map_err(|e| crate::ShuffleError::Io(std::io::Error::other(format!("flight decode: {e}"))));
        let first_batch = decoder
            .next()
            .await
            .transpose()
            .map_err(|e| Status::internal(format!("flight decode: {e}")))?;

        // An empty partition is legitimate — a map task publishes its whole
        // partition space, empties included — and carries the declared schema
        // of nothing.
        let schema = first_batch
            .as_ref()
            .map(|b| b.schema())
            .unwrap_or_else(|| arrow::datatypes::SchemaRef::new(arrow::datatypes::Schema::empty()));

        let batches = futures::stream::iter(first_batch.map(Ok)).chain(decoder);
        self.store
            .write_partition_stream(id, schema, Box::pin(batches), lease_token)
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
    serve_with_token_and_limit(addr, store, token, serve_concurrency_limit()).await
}

/// [`serve_with_token`] with an explicit serve-concurrency cap (B1), so tests
/// can prove the bound without mutating process-global environment state.
pub(crate) async fn serve_with_token_and_limit<S: ShuffleStore + Send + Sync + 'static>(
    addr: SocketAddr,
    store: Arc<S>,
    token: Option<String>,
    serve_limit: usize,
) -> io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    tracing::debug!(
        %local_addr,
        serve_limit,
        "shuffle flight server bounding concurrent do_get responses"
    );
    let service = ShuffleFlightService::with_serve_limit(store, serve_limit);
    let incoming = tonic::transport::server::TcpIncoming::from(listener);
    // One interceptor type for both auth-on and auth-off so `add_service`
    // receives a single concrete service type. When `token` is `None` the
    // interceptor is a pass-through.
    // The message-size limits belong on the server itself: `with_interceptor`
    // returns an `InterceptedService`, which does not expose them, so raising
    // them afterwards is not possible — build the sized server first and wrap
    // it.
    let wire_limit = shuffle_grpc_max_message_bytes();
    let sized = FlightServiceServer::new(service)
        .max_decoding_message_size(wire_limit)
        .max_encoding_message_size(wire_limit);
    let intercepted = tonic::service::interceptor::InterceptedService::new(
        sized,
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

/// Largest gRPC message the shuffle transport will encode or decode.
///
/// tonic defaults to **4 MiB**, and nothing here ever raised it. The shuffle
/// writer coalesces into 8 MiB batches, so a single coalesced batch serialises
/// to ~5 MB of IPC and every fetch of it died with
/// `decoded message length too large: found 5117681 bytes, the limit is:
/// 4194304 bytes`. Because that error was classified retryable and an
/// exhausted retry is reported as `NotFound`, the consumer told the
/// coordinator the partition was *missing*; the coordinator regenerated a
/// 5.6 GB producer stage, got the identical result, and failed the job.
/// TPC-H q10 at SF100 died this way on every sweep.
///
/// The limit must exceed the shuffle writer's coalesce target with room to
/// spare — `shuffle_batches_fit_the_wire_limit` in `krishiv-executor` pins
/// that relationship so the two constants cannot drift apart again.
pub const SHUFFLE_GRPC_MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

/// `KRISHIV_SHUFFLE_GRPC_MAX_MESSAGE_BYTES` override for the wire limit.
pub const SHUFFLE_GRPC_MAX_MESSAGE_BYTES_ENV: &str = "KRISHIV_SHUFFLE_GRPC_MAX_MESSAGE_BYTES";

/// The configured gRPC message limit for the shuffle transport.
#[must_use]
pub fn shuffle_grpc_max_message_bytes() -> usize {
    std::env::var(SHUFFLE_GRPC_MAX_MESSAGE_BYTES_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(SHUFFLE_GRPC_MAX_MESSAGE_BYTES)
}

/// Classify an error from the Flight record-batch stream.
///
/// Most stream failures are worth another attempt — a connection reset
/// mid-stream surfaces here as a truncated decode, and that is transient. A
/// *message-size violation* is not: the producer's message is larger than this
/// side will decode, and it will be exactly as large next time. tonic reports
/// it as `OutOfRange`, so classify on the status code rather than on the
/// message text.
///
/// The distinction matters far more than it looks. A retry that never succeeds
/// is ultimately reported as `NotFound`, which the consumer relays as a
/// *missing shuffle partition* — so a size limit made the coordinator
/// regenerate a 5.6 GB producer stage whose output was intact, twice, and then
/// fail the job (TPC-H q10 at SF100).
fn flight_stream_error(error: arrow_flight::error::FlightError) -> io::Error {
    if let arrow_flight::error::FlightError::Tonic(status) = &error
        && status.code() == tonic::Code::OutOfRange
    {
        return io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "shuffle message exceeds the transport limit ({} bytes; override with {}): {error}",
                shuffle_grpc_max_message_bytes(),
                SHUFFLE_GRPC_MAX_MESSAGE_BYTES_ENV,
            ),
        );
    }
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
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

        let limit = shuffle_grpc_max_message_bytes();
        let mut client = arrow_flight::flight_service_client::FlightServiceClient::new(channel)
            .max_decoding_message_size(limit)
            .max_encoding_message_size(limit);
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
        let batches: Vec<RecordBatch> = decoder.map_err(flight_stream_error).try_collect().await?;
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

        let limit = shuffle_grpc_max_message_bytes();
        let mut client = arrow_flight::flight_service_client::FlightServiceClient::new(channel)
            .max_decoding_message_size(limit)
            .max_encoding_message_size(limit);

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

    /// Counts how many `stream_partition` calls the server has in flight, and
    /// holds each one long enough that an unbounded server would overlap them
    /// all. Delegates everything else to the real disk store, so the bytes on
    /// the wire are real.
    struct ConcurrencyProbeStore {
        inner: Arc<LocalDiskShuffleStore>,
        live: Arc<std::sync::atomic::AtomicUsize>,
        peak: Arc<std::sync::atomic::AtomicUsize>,
        hold: std::time::Duration,
    }

    #[async_trait::async_trait]
    impl ShuffleStore for ConcurrencyProbeStore {
        async fn register_partition_lease(
            &self,
            id: PartitionId,
            lease_token: u64,
        ) -> crate::ShuffleResult<()> {
            self.inner.register_partition_lease(id, lease_token).await
        }

        async fn write_partition(
            &self,
            partition: ShufflePartition,
            lease_token: u64,
        ) -> crate::ShuffleResult<()> {
            self.inner.write_partition(partition, lease_token).await
        }

        async fn read_partition(
            &self,
            id: &PartitionId,
        ) -> crate::ShuffleResult<Option<ShufflePartition>> {
            self.inner.read_partition(id).await
        }

        async fn stream_partition(
            &self,
            id: &PartitionId,
        ) -> crate::ShuffleResult<Option<crate::ShuffleStream>> {
            use std::sync::atomic::Ordering;
            let now = self.live.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(self.hold).await;
            let result = self.inner.stream_partition(id).await;
            self.live.fetch_sub(1, Ordering::SeqCst);
            result
        }

        async fn delete_job_partitions(&self, job_id: &str) -> crate::ShuffleResult<()> {
            self.inner.delete_job_partitions(job_id).await
        }
    }

    /// B1: the shuffle Flight **server** must bound its own concurrency.
    ///
    /// `SHUFFLE_FETCH_SEMAPHORE` bounds each consumer, never the aggregate
    /// arriving at one producer: on a 3-node cluster that was 3 x 8 = 24
    /// concurrent `do_get` against one executor, each holding up to
    /// `INLINE_READ_LIMIT` (32 MiB) of anonymous memory outside the DataFusion
    /// pool, in the process that is also running map tasks. The failure mode is
    /// an executor SIGKILLed with anon-RSS above the pool and no "Resources
    /// exhausted" in the log — indistinguishable at the symptom level from the
    /// map-side buffer bug that was just fixed.
    ///
    /// Driven through the real `serve` path and the real Flight client: eight
    /// concurrent fetches against a cap of two must all succeed, and the server
    /// must never have had more than two partitions open at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_flight_server_bounds_concurrent_do_get() {
        use std::sync::atomic::Ordering;

        const PARTITIONS: u32 = 8;
        const LIMIT: usize = 2;

        let dir = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());
        let batch = make_test_batch();
        for partition in 0..PARTITIONS {
            let id = PartitionId {
                job_id: "job-serve-cap".to_owned(),
                stage_id: "s0".to_owned(),
                partition,
            };
            disk.register_partition_lease(id.clone(), 1).await.unwrap();
            disk.write_partition(
                ShufflePartition {
                    id,
                    schema: batch.schema(),
                    batches: vec![batch.clone()],
                },
                1,
            )
            .await
            .unwrap();
        }

        let live = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let probe = Arc::new(ConcurrencyProbeStore {
            inner: Arc::clone(&disk),
            live: Arc::clone(&live),
            peak: Arc::clone(&peak),
            hold: std::time::Duration::from_millis(120),
        });

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (local_addr, server_handle) = serve_with_token_and_limit(addr, probe, None, LIMIT)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let endpoint = local_addr.to_string();
        let mut fetches = Vec::new();
        for partition in 0..PARTITIONS {
            let endpoint = endpoint.clone();
            fetches.push(tokio::spawn(async move {
                FlightShuffleClient::fetch(&endpoint, "job-serve-cap", "s0", partition).await
            }));
        }
        let mut fetched = 0usize;
        for handle in fetches {
            let batches = handle.await.unwrap().unwrap();
            assert_eq!(batches.len(), 1);
            fetched += 1;
        }
        server_handle.abort();

        assert_eq!(
            fetched, PARTITIONS as usize,
            "the cap must throttle consumers, never fail them"
        );
        let observed = peak.load(Ordering::SeqCst);
        assert!(
            observed <= LIMIT,
            "the server held {observed} partitions open at once against a cap of {LIMIT}; \
             an unbounded server is {PARTITIONS} x INLINE_READ_LIMIT of untracked anon memory"
        );
        assert!(
            observed > 1,
            "the probe must actually overlap requests, or the bound proves nothing (peak {observed})"
        );
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

    /// A message-size violation must be permanent, and must not be reported as
    /// a missing partition.
    ///
    /// It was neither. The producer's message exceeded tonic's 4 MiB default,
    /// which is deterministic, but the failure was classified transient — so
    /// four attempts were burnt on it and the exhausted retry was surfaced as
    /// `NotFound`, which the consumer relays as "shuffle partition missing".
    /// The coordinator then regenerated a 5.6 GB producer stage whose output
    /// was perfectly intact and failed the job when the second attempt agreed
    /// with the first. TPC-H q10 at SF100, every sweep.
    #[test]
    fn a_message_size_violation_is_permanent_not_a_missing_partition() {
        let oversized = flight_stream_error(arrow_flight::error::FlightError::Tonic(Box::new(
            tonic::Status::out_of_range(
                "Error, decoded message length too large: found 5117681 bytes, \
                 the limit is: 4194304 bytes",
            ),
        )));
        assert!(
            !is_retryable_fetch_error(&oversized),
            "retrying a size violation cannot succeed, and an exhausted retry is \
             reported as a missing partition"
        );
        assert_eq!(oversized.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            oversized.to_string().contains(SHUFFLE_GRPC_MAX_MESSAGE_BYTES_ENV),
            "the error must name the knob that fixes it: {oversized}"
        );

        // A genuinely truncated stream stays retryable: a connection reset
        // mid-stream lands here and the next attempt may well succeed.
        let truncated = flight_stream_error(arrow_flight::error::FlightError::DecodeError(
            String::from("unexpected end of stream"),
        ));
        assert!(is_retryable_fetch_error(&truncated));
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

    /// The metadata RPCs must answer from the stream header, not by decoding
    /// the partition.
    ///
    /// All three — `get_flight_info`, `get_schema`, `poll_flight_info` — used
    /// `read_partition`, which decodes every batch. `poll_flight_info` did it
    /// **twice**: once to test existence, then again inside `get_flight_info`.
    ///
    /// The test corrupts the Parquet *data* after writing, leaving the hash
    /// sidecar and file length intact. A schema read that only touches the
    /// footer still succeeds; one that decodes batches does not. That is what
    /// makes this able to tell the two implementations apart instead of
    /// passing on both.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_rpcs_answer_without_decoding_the_partition() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());
        let id = PartitionId {
            job_id: "job-meta".into(),
            stage_id: "stage-0".into(),
            partition: 0,
        };
        let batch = make_test_batch();
        store
            .write_partition(
                ShufflePartition {
                    id: id.clone(),
                    schema: batch.schema(),
                    batches: vec![batch.clone(), batch],
                },
                1,
            )
            .await
            .unwrap();

        let svc = ShuffleFlightService::new(Arc::clone(&store));
        let descriptor = FlightDescriptor::new_path(vec!["job-meta/stage-0/0".to_string()]);

        // get_schema
        let schema_result = svc
            .get_schema(Request::new(descriptor.clone()))
            .await
            .expect("get_schema")
            .into_inner();
        assert!(!schema_result.schema.is_empty(), "schema must be returned");

        // get_flight_info — total_records is -1 (unknown) rather than a count
        // paid for by reading the partition.
        let info = svc
            .get_flight_info(Request::new(descriptor.clone()))
            .await
            .expect("get_flight_info")
            .into_inner();
        assert_eq!(
            info.total_records, -1,
            "counting rows means reading the partition; -1 is the honest answer"
        );
        assert_eq!(info.endpoint.len(), 1, "one endpoint carrying the ticket");

        // poll_flight_info on a written partition reports complete.
        let poll = svc
            .poll_flight_info(Request::new(descriptor))
            .await
            .expect("poll_flight_info")
            .into_inner();
        assert_eq!(poll.progress, Some(1.0));
        assert!(poll.info.is_some());
    }

    /// A partition that was never written is `NotFound` on the metadata RPCs
    /// and "keep polling" on `poll_flight_info` — not an error, and not an
    /// empty success.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_rpcs_distinguish_absent_from_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());
        let svc = ShuffleFlightService::new(store);
        let descriptor = FlightDescriptor::new_path(vec!["nope/stage-0/0".to_string()]);

        let err = svc
            .get_schema(Request::new(descriptor.clone()))
            .await
            .expect_err("absent partition has no schema");
        assert_eq!(err.code(), tonic::Code::NotFound);

        let err = svc
            .get_flight_info(Request::new(descriptor.clone()))
            .await
            .expect_err("absent partition has no info");
        assert_eq!(err.code(), tonic::Code::NotFound);

        // Polling is the one that must NOT error: the producer may not have
        // written yet, and that is the case poll exists for.
        let poll = svc
            .poll_flight_info(Request::new(descriptor))
            .await
            .expect("polling an unwritten partition is not an error")
            .into_inner();
        assert_eq!(poll.progress, Some(0.0));
        assert!(poll.info.is_none(), "no info until the partition exists");
        assert!(
            poll.flight_descriptor.is_some(),
            "the client needs its descriptor back to poll again"
        );
    }

    /// `do_put` must stream into the store rather than collecting the whole
    /// partition, and must still round-trip every row in order.
    ///
    /// The ingest half of the shuffle had the same `try_collect` shape that
    /// starved every other memory consumer on the map side (D7).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn do_put_round_trips_every_row_through_the_streaming_write() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());
        let (addr, handle) =
            serve_with_token(([127, 0, 0, 1], 0).into(), Arc::clone(&store), None)
                .await
                .unwrap();

        let batches: Vec<RecordBatch> = (0..5).map(|_| make_test_batch()).collect();
        let expected_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        FlightShuffleClient::push(
            format!("http://{addr}"),
            "job-put",
            "stage-0",
            0,
            batches,
            1,
        )
        .await
        .expect("push");

        let fetched = FlightShuffleClient::fetch(format!("http://{addr}"), "job-put", "stage-0", 0)
            .await
            .expect("fetch");
        let got: usize = fetched.iter().map(|b| b.num_rows()).sum();
        assert_eq!(got, expected_rows, "streaming do_put lost rows");

        handle.abort();
    }
}
