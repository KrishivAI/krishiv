use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::{LocalDiskShuffleStore, PartitionId, ShuffleStore, error::MAX_SHUFFLE_TICKET_LEN};

fn parse_ticket(ticket: &str) -> Option<(String, String, u32)> {
    let parts: Vec<&str> = ticket.trim().splitn(3, '/').collect();
    if parts.len() != 3 {
        return None;
    }
    let partition_id = parts[2].parse::<u32>().ok()?;
    Some((parts[0].to_owned(), parts[1].to_owned(), partition_id))
}

/// Serialize `batches` to Arrow IPC stream format. Returns `None` on any
/// serialization error so callers can fall back to sending an empty response
/// rather than partial / corrupted bytes.
fn serialize_ipc_partition(
    schema: &arrow::datatypes::Schema,
    batches: &[arrow::record_batch::RecordBatch],
) -> Option<Vec<u8>> {
    use arrow::ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema).ok()?;
    for batch in batches {
        writer.write(batch).ok()?;
    }
    writer.finish().ok()?;
    Some(buf)
}

async fn handle_connection(mut stream: TcpStream, store: Arc<LocalDiskShuffleStore>) {
    // Read ticket line terminated by '\n'.
    let mut buf = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        match stream.read_exact(&mut byte).await {
            Ok(_) => {
                if byte[0] == b'\n' {
                    break;
                }
                buf.push(byte[0]);
                if buf.len() > MAX_SHUFFLE_TICKET_LEN {
                    return;
                }
            }
            Err(_) => return,
        }
    }

    let ticket = match std::str::from_utf8(&buf) {
        Ok(s) => s.to_owned(),
        Err(_) => {
            let _ = stream.write_all(&0u32.to_be_bytes()).await;
            return;
        }
    };

    let Some((job_id, stage_id, partition_id)) = parse_ticket(&ticket) else {
        let _ = stream.write_all(&0u32.to_be_bytes()).await;
        return;
    };

    let id = PartitionId {
        job_id,
        stage_id,
        partition: partition_id,
    };
    let result = store.read_partition(&id).await;

    match result {
        Ok(Some(partition)) => {
            // Serialize to a local buffer first; send len=0 if serialization
            // fails so the client gets a clean "not found" rather than
            // partial / corrupted Arrow IPC bytes.
            let buf =
                serialize_ipc_partition(&partition.schema, &partition.batches).unwrap_or_default();
            let len = buf.len() as u32;
            let _ = stream.write_all(&len.to_be_bytes()).await;
            let _ = stream.write_all(&buf).await;
        }
        _ => {
            let _ = stream.write_all(&0u32.to_be_bytes()).await;
        }
    }
}

/// Start the shuffle IPC server on `addr` backed by `store`.
///
/// Returns the local address and a join handle. Call `abort()` on the handle
/// to shut down the server.
pub async fn serve(
    addr: SocketAddr,
    store: Arc<LocalDiskShuffleStore>,
) -> io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let store = Arc::clone(&store);
            tokio::spawn(handle_connection(stream, store));
        }
    });
    Ok((local_addr, handle))
}

/// Fetch all [`RecordBatch`]es for one shuffle partition from a remote server.
///
/// `endpoint` format: `<host>:<port>` (e.g. `"10.0.0.5:50051"`)
pub struct FlightShuffleClient;

impl FlightShuffleClient {
    pub async fn fetch(
        endpoint: impl Into<String>,
        job_id: &str,
        stage_id: &str,
        partition_id: u32,
    ) -> io::Result<Vec<RecordBatch>> {
        let endpoint = endpoint.into();
        let mut stream = TcpStream::connect(&endpoint).await?;

        let ticket = format!("{job_id}/{stage_id}/{partition_id}\n");
        stream.write_all(ticket.as_bytes()).await?;

        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("partition {job_id}/{stage_id}/{partition_id} not found"),
            ));
        }

        // Guard against a server sending a maliciously large length that
        // would cause an OOM allocation on the client side.
        const MAX_PARTITION_BYTES: usize = 256 * 1024 * 1024; // 256 MiB
        if len > MAX_PARTITION_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("partition length {len} exceeds maximum {MAX_PARTITION_BYTES} bytes"),
            ));
        }

        let mut data = vec![0u8; len];
        stream.read_exact(&mut data).await?;

        let cursor = std::io::Cursor::new(data);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut batches = Vec::new();
        for batch in reader {
            let batch = batch.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            batches.push(batch);
        }
        Ok(batches)
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

    #[tokio::test]
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

        let endpoint = local_addr.to_string();
        let result = FlightShuffleClient::fetch(&endpoint, "job-flight-1", "s0", 0)
            .await
            .unwrap();

        server_handle.abort();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);
        assert_eq!(result[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn flight_client_returns_error_for_missing_partition() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (local_addr, server_handle) = serve(addr, Arc::clone(&store)).await.unwrap();
        let endpoint = local_addr.to_string();

        let result = FlightShuffleClient::fetch(&endpoint, "missing", "s0", 0).await;
        server_handle.abort();

        assert!(
            matches!(result, Err(ref e) if e.kind() == std::io::ErrorKind::NotFound),
            "expected NotFound, got: {result:?}"
        );
    }
}
