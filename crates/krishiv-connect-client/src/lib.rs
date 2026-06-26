#![forbid(unsafe_code)]

//! P7: Spark Connect-style lightweight client for Krishiv.
//!
//! This crate provides a thin client API over Arrow Flight that can be
//! used from Rust, Python (via PyO3), or Go (via FFI) to connect to a
//! remote Krishiv cluster and execute SQL queries.
//!
//! # Architecture
//!
//! The client connects to a Krishiv Flight SQL endpoint and provides
//! a simplified API for SQL execution and table registration.
//!
//! # Usage
//!
//! ```ignore
//! use krishiv_connect_client::Session;
//!
//! #[tokio::main]
//! async fn main() {
//!     let session = Session::connect("http://localhost:50051").await.unwrap();
//!     let result = session.execute_sql("SELECT 1 as answer").await.unwrap();
//!     println!("Rows: {}", result.row_count());
//! }
//! ```

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use std::io::Cursor;

/// A lightweight session handle for connecting to a remote Krishiv cluster.
///
/// Wraps a Flight SQL client and provides a simplified API for SQL execution
/// and table registration.
pub struct Session {
    /// The endpoint URL used to connect.
    endpoint: String,
    /// gRPC channel (kept for future use).
    _channel: tonic::transport::Channel,
}

impl Session {
    /// Connect to a Krishiv cluster at the given URL.
    ///
    /// The URL should point to the Flight SQL service (typically port 50051).
    pub async fn connect(url: &str) -> Result<Self, ConnectError> {
        let channel = tonic::transport::Channel::from_shared(url.to_string())
            .map_err(|e| ConnectError::Transport(e.to_string()))?
            .connect()
            .await
            .map_err(|e| ConnectError::Transport(e.to_string()))?;

        Ok(Self {
            endpoint: url.to_string(),
            _channel: channel,
        })
    }

    /// Execute a SQL query and return a `QueryResult`.
    ///
    /// This is a simplified interface that sends the query via gRPC
    /// and collects the result.
    pub async fn execute_sql(&self, _query: &str) -> Result<QueryResult, ConnectError> {
        // In a full implementation, this would use FlightSQL's Execute method.
        // For now, return a placeholder that demonstrates the API shape.
        Err(ConnectError::Execution(
            "Flight SQL execution not yet wired — use the full Krishiv runtime".into(),
        ))
    }

    /// Register a table from a file path.
    pub async fn register_table(&self, _name: &str, _path: &str) -> Result<(), ConnectError> {
        Err(ConnectError::Execution(
            "Table registration not yet wired — use the full Krishiv runtime".into(),
        ))
    }

    /// Get the endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Close the session and release resources.
    pub fn close(self) {
        // Channel is dropped automatically
    }
}

/// A handle to a query result that can be collected into record batches.
pub struct QueryResult {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
}

impl QueryResult {
    /// Create a new query result from record batches.
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        let schema = batches.first().map(|b| b.schema()).unwrap_or_else(|| {
            use arrow::datatypes::{DataType, Field, Schema};
            std::sync::Arc::new(Schema::new(vec![Field::new(
                "placeholder",
                DataType::Null,
                true,
            )]))
        });
        Self { batches, schema }
    }

    /// Collect the query result into record batches.
    pub fn collect(self) -> Vec<RecordBatch> {
        self.batches
    }

    /// Get the schema of the result.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the number of rows in the result.
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Get the number of batches in the result.
    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }
}

/// Decode Arrow IPC bytes into record batches.
pub fn decode_ipc_batches(data: &[u8]) -> Result<Vec<RecordBatch>, ConnectError> {
    let cursor = Cursor::new(data.to_vec());
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| ConnectError::Serialization(e.to_string()))?;

    let mut batches = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| ConnectError::Serialization(e.to_string()))?;
        batches.push(batch);
    }
    Ok(batches)
}

/// Errors that can occur when using the connect client.
#[derive(Debug)]
pub enum ConnectError {
    /// Transport-level error (connection, DNS, etc.).
    Transport(String),
    /// SQL execution error.
    Execution(String),
    /// Serialization/deserialization error.
    Serialization(String),
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(msg) => write!(f, "transport error: {msg}"),
            Self::Execution(msg) => write!(f, "execution error: {msg}"),
            Self::Serialization(msg) => write!(f, "serialization error: {msg}"),
        }
    }
}

impl std::error::Error for ConnectError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display() {
        let err = ConnectError::Transport("connection refused".into());
        assert!(err.to_string().contains("connection refused"));

        let err = ConnectError::Execution("syntax error".into());
        assert!(err.to_string().contains("syntax error"));
    }

    #[test]
    fn query_result_helpers() {
        let result = QueryResult::new(vec![]);
        assert_eq!(result.row_count(), 0);
        assert_eq!(result.batch_count(), 0);
    }
}
