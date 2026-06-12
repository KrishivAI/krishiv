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
    Action, ActionType, Criteria, Empty, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{StreamExt, TryStreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::{LocalDiskShuffleStore, PartitionId, ShuffleStore, error::MAX_SHUFFLE_TICKET_LEN};

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
    let partition_id = parts[2]
        .parse::<u32>()
        .map_err(|e| Status::invalid_argument(format!("partition id not a u32: {e}")))?;
    Ok((parts[0].to_owned(), parts[1].to_owned(), partition_id))
}

/// Arrow Flight shuffle service backed by a local-disk shuffle store.
#[derive(Clone)]
pub struct ShuffleFlightService {
    store: Arc<LocalDiskShuffleStore>,
}

impl ShuffleFlightService {
    pub fn new(store: Arc<LocalDiskShuffleStore>) -> Self {
        Self { store }
    }
}

type BoxedFlightStream<T> =
    Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for ShuffleFlightService {
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
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
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
        let (job_id, stage_id, partition) = parse_ticket(descriptor.path[0].as_bytes())?;
        let lease_token: u64 = descriptor
            .path
            .get(1)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1);

        let id = PartitionId {
            job_id,
            stage_id,
            partition,
        };

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
pub async fn serve(
    addr: SocketAddr,
    store: Arc<LocalDiskShuffleStore>,
) -> io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let service = ShuffleFlightService::new(store);
    let incoming = tonic::transport::server::TcpIncoming::from(listener);
    let handle = tokio::spawn(async move {
        if let Err(error) = Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
        {
            tracing::warn!(error = %error, "shuffle flight server exited with error");
        }
    });
    Ok((local_addr, handle))
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
        let stream = client
            .do_get(Request::new(ticket))
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
        // do_put requires a plain Stream<Item=FlightData>; collect encoder output
        // (already in memory) to satisfy the bound.
        let flight_data: Vec<FlightData> = encoder
            .try_collect()
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;

        client
            .do_put(Request::new(futures::stream::iter(flight_data)))
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;

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
}
