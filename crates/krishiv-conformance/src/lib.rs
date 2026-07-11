#![forbid(unsafe_code)]

//! SQL correctness corpus (Phase 51): sqllogictest drivers for the three
//! execution placements — embedded (`SqlEngine`), single-node (`Session`),
//! and distributed (`Session` over the in-process coordinator/executor
//! cluster). The corpus files in `corpus/` are the regression net for the
//! scale phases (52/54): expected results were recorded from the engine at
//! seed time and any behavior change fails the suite.

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};

/// Error type shared by the corpus drivers.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct DriverError(pub String);

fn batches_to_output(batches: &[RecordBatch]) -> Result<DBOutput<DefaultColumnType>, DriverError> {
    let Some(first) = batches.first() else {
        return Ok(DBOutput::StatementComplete(0));
    };
    let types = first
        .schema()
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            arrow::datatypes::DataType::Int8
            | arrow::datatypes::DataType::Int16
            | arrow::datatypes::DataType::Int32
            | arrow::datatypes::DataType::Int64
            | arrow::datatypes::DataType::UInt8
            | arrow::datatypes::DataType::UInt16
            | arrow::datatypes::DataType::UInt32
            | arrow::datatypes::DataType::UInt64 => DefaultColumnType::Integer,
            arrow::datatypes::DataType::Float16
            | arrow::datatypes::DataType::Float32
            | arrow::datatypes::DataType::Float64
            | arrow::datatypes::DataType::Decimal128(_, _)
            | arrow::datatypes::DataType::Decimal256(_, _) => DefaultColumnType::FloatingPoint,
            _ => DefaultColumnType::Text,
        })
        .collect();

    let mut rows: Vec<Vec<String>> = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut out_row = Vec::with_capacity(batch.num_columns());
            for col in batch.columns() {
                if col.is_null(row) {
                    out_row.push("NULL".to_string());
                } else {
                    let rendered = arrow::util::display::array_value_to_string(col, row)
                        .map_err(|e| DriverError(e.to_string()))?;
                    // SLT convention: empty strings are rendered as
                    // "(empty)" so blank cells stay unambiguous.
                    out_row.push(if rendered.is_empty() {
                        "(empty)".to_string()
                    } else {
                        rendered
                    });
                }
            }
            rows.push(out_row);
        }
    }
    Ok(DBOutput::Rows { types, rows })
}

/// Placement 1: embedded — straight through `SqlEngine`.
pub struct EmbeddedDriver {
    engine: krishiv_sql::SqlEngine,
}

impl EmbeddedDriver {
    pub fn new() -> Self {
        Self {
            engine: krishiv_sql::SqlEngine::new(),
        }
    }
}

impl Default for EmbeddedDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl AsyncDB for EmbeddedDriver {
    type Error = DriverError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, DriverError> {
        let df = self
            .engine
            .sql(sql)
            .await
            .map_err(|e| DriverError(e.to_string()))?;
        let batches = df.collect().await.map_err(|e| DriverError(e.to_string()))?;
        batches_to_output(&batches)
    }

    fn engine_name(&self) -> &str {
        "krishiv-embedded"
    }

    async fn shutdown(&mut self) {}
}

/// Placements 2+3: a `Session` in single-node or distributed mode, each
/// backed by an in-process Flight SQL coordinator spawned on a loopback
/// ephemeral port (the modes structurally require a coordinator URL — the
/// architecture invariant that non-embedded modes are never silently local).
pub struct SessionDriver {
    session: krishiv_api::Session,
    name: &'static str,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for SessionDriver {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl SessionDriver {
    async fn spawn_flight_server() -> Result<(String, tokio::task::JoinHandle<()>), DriverError> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| DriverError(format!("bind: {e}")))?;
        let addr = listener
            .local_addr()
            .map_err(|e| DriverError(format!("local_addr: {e}")))?;
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let server = krishiv_flight_sql::make_flight_sql_server()
            .map_err(|e| DriverError(format!("make server: {e}")))?;
        let handle = tokio::spawn(async move {
            // Server errors surface as connection failures in the driver;
            // the handle is aborted on driver drop, so exit is expected.
            let _ = tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(incoming)
                .await;
        });
        Ok((format!("http://{addr}"), handle))
    }

    async fn build(
        mode: krishiv_api::ExecutionMode,
        name: &'static str,
    ) -> Result<Self, DriverError> {
        let (url, server) = Self::spawn_flight_server().await?;
        let session = krishiv_api::Session::builder()
            .with_coordinator(&url)
            // with_coordinator switches the mode to Distributed; re-assert the
            // placement under test after it.
            .with_execution_mode(mode)
            .with_remote_execution(true)
            .build()
            .map_err(|e| DriverError(e.to_string()))?;
        Ok(Self {
            session,
            name,
            server,
        })
    }

    pub async fn single_node() -> Result<Self, DriverError> {
        Self::build(
            krishiv_api::ExecutionMode::SingleNode,
            "krishiv-single-node",
        )
        .await
    }

    pub async fn distributed_in_process() -> Result<Self, DriverError> {
        Self::build(
            krishiv_api::ExecutionMode::Distributed,
            "krishiv-distributed-inproc",
        )
        .await
    }
}

#[async_trait::async_trait]
impl AsyncDB for SessionDriver {
    type Error = DriverError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, DriverError> {
        let df = self
            .session
            .sql_async(sql)
            .await
            .map_err(|e| DriverError(e.to_string()))?;
        let result = df
            .collect_async()
            .await
            .map_err(|e| DriverError(e.to_string()))?;
        batches_to_output(result.batches())
    }

    fn engine_name(&self) -> &str {
        self.name
    }

    async fn shutdown(&mut self) {}
}
