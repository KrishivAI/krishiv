#![forbid(unsafe_code)]

//! Public Rust API stubs for Krishiv R1.
//!
//! This crate owns the long-term user-facing Rust API. The R1 bootstrap surface
//! is intentionally thin and avoids exposing DataFusion internals directly.

use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

use krishiv_plan::{ExecutionKind, LogicalPlan, PlanNode};

pub use krishiv_plan::{LogicalPlan as KrishivLogicalPlan, PhysicalPlan as KrishivPhysicalPlan};
pub use krishiv_runtime::{JobState, JobStatus, LocalJobRegistry};

/// API result alias.
pub type Result<T> = std::result::Result<T, KrishivError>;

/// Public API errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KrishivError {
    /// A requested capability is not available in the current release slice.
    Unsupported { feature: String },
    /// User-provided configuration is invalid.
    InvalidConfig { message: String },
    /// Runtime error surfaced through the public API.
    Runtime { message: String },
}

impl KrishivError {
    /// Create an unsupported-feature error.
    pub fn unsupported(feature: impl Into<String>) -> Self {
        Self::Unsupported {
            feature: feature.into(),
        }
    }
}

impl fmt::Display for KrishivError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { feature } => write!(f, "unsupported Krishiv feature: {feature}"),
            Self::InvalidConfig { message } => write!(f, "invalid Krishiv config: {message}"),
            Self::Runtime { message } => write!(f, "Krishiv runtime error: {message}"),
        }
    }
}

impl Error for KrishivError {}

impl From<krishiv_runtime::RuntimeError> for KrishivError {
    fn from(value: krishiv_runtime::RuntimeError) -> Self {
        Self::Runtime {
            message: value.to_string(),
        }
    }
}

/// Execution mode selected for a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// In-process execution for embedding Krishiv in a Rust application.
    Embedded,
    /// Single-node execution through the local Krishiv runtime.
    SingleNode,
    /// Reserved for the R2 Kubernetes/distributed runtime.
    Distributed,
}

impl fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Embedded => f.write_str("embedded"),
            Self::SingleNode => f.write_str("single-node"),
            Self::Distributed => f.write_str("distributed"),
        }
    }
}

/// R1 bootstrap stand-in for an Arrow field.
///
/// This type keeps the API compiling without a network dependency download.
/// It will be replaced by, or adapted to, Arrow schema types when Arrow is
/// introduced in the DataFusion integration slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    name: String,
    data_type: String,
}

impl Field {
    /// Create a field.
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
        }
    }

    /// Field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Field data type.
    pub fn data_type(&self) -> &str {
        &self.data_type
    }
}

/// R1 bootstrap stand-in for an Arrow schema.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    /// Create a schema.
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    /// Schema fields.
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }
}

/// Minimal scalar value for R1 stubs.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    /// Null value.
    Null,
    /// Boolean value.
    Boolean(bool),
    /// 64-bit integer value.
    Int64(i64),
    /// 64-bit float value.
    Float64(f64),
    /// UTF-8 string value.
    Utf8(String),
}

/// R1 bootstrap stand-in for an Arrow record batch.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordBatch {
    schema: Schema,
    rows: Vec<Vec<ScalarValue>>,
}

impl RecordBatch {
    /// Create a record batch.
    pub fn new(schema: Schema, rows: Vec<Vec<ScalarValue>>) -> Self {
        Self { schema, rows }
    }

    /// Batch schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Batch rows.
    pub fn rows(&self) -> &[Vec<ScalarValue>] {
        &self.rows
    }
}

/// Query result wrapper.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct QueryResult {
    batches: Vec<RecordBatch>,
}

impl QueryResult {
    /// Create a query result from batches.
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }

    /// Result batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }
}

/// Stream batch wrapper.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamBatch {
    sequence: u64,
    batch: RecordBatch,
}

impl StreamBatch {
    /// Create a stream batch.
    pub fn new(sequence: u64, batch: RecordBatch) -> Self {
        Self { sequence, batch }
    }

    /// Sequence number in the local stream.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Record batch payload.
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }
}

/// Builder for Krishiv sessions.
#[derive(Debug, Clone)]
pub struct SessionBuilder {
    mode: ExecutionMode,
}

impl Default for SessionBuilder {
    fn default() -> Self {
        Self {
            mode: ExecutionMode::Embedded,
        }
    }
}

impl SessionBuilder {
    /// Create a session builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Select an execution mode.
    #[must_use]
    pub fn with_execution_mode(mut self, mode: ExecutionMode) -> Self {
        self.mode = mode;
        self
    }

    /// Build a session.
    pub fn build(self) -> Result<Session> {
        Ok(Session {
            mode: self.mode,
            jobs: LocalJobRegistry::default(),
        })
    }
}

/// User-facing Krishiv session.
#[derive(Debug, Clone)]
pub struct Session {
    mode: ExecutionMode,
    jobs: LocalJobRegistry,
}

impl Session {
    /// Start building a session.
    pub fn builder() -> SessionBuilder {
        SessionBuilder::new()
    }

    /// Current execution mode.
    pub fn mode(&self) -> ExecutionMode {
        self.mode
    }

    /// Known local jobs.
    pub fn jobs(&self) -> &[JobStatus] {
        self.jobs.list()
    }

    /// Create a DataFrame from a SQL query.
    pub fn sql(&self, query: impl Into<String>) -> Result<DataFrame> {
        let sql_plan =
            krishiv_sql::plan_sql(query).map_err(|error| KrishivError::InvalidConfig {
                message: error.to_string(),
            })?;

        Ok(DataFrame::new(sql_plan.logical_plan().clone()))
    }

    /// Register a Parquet path as a DataFrame placeholder.
    pub fn read_parquet(&self, path: impl AsRef<Path>) -> Result<DataFrame> {
        let path = path.as_ref().to_path_buf();
        Ok(DataFrame::from_parquet_path(path))
    }

    /// Create a bounded local memory stream.
    pub fn memory_stream(&self, name: impl Into<String>, batches: Vec<StreamBatch>) -> Stream {
        Stream::new(name, batches)
    }
}

/// DataFrame API skeleton.
#[derive(Debug, Clone)]
pub struct DataFrame {
    logical_plan: LogicalPlan,
}

impl DataFrame {
    /// Create a DataFrame from a logical plan.
    pub fn new(logical_plan: LogicalPlan) -> Self {
        Self { logical_plan }
    }

    fn from_parquet_path(path: PathBuf) -> Self {
        let label = format!("parquet scan: {}", path.display());
        let logical_plan = LogicalPlan::new("parquet-read", ExecutionKind::Batch)
            .with_node(PlanNode::new("parquet-scan", label, ExecutionKind::Batch));

        Self { logical_plan }
    }

    /// Borrow the logical plan.
    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }

    /// Explain the current logical plan.
    pub fn explain(&self) -> String {
        self.logical_plan.describe()
    }

    /// Collect results.
    ///
    /// R1 bootstrap exposes the method shape but does not execute queries yet.
    pub fn collect(&self) -> Result<QueryResult> {
        Err(KrishivError::unsupported(
            "DataFrame::collect requires the DataFusion execution slice",
        ))
    }
}

/// Stream API skeleton.
#[derive(Debug, Clone)]
pub struct Stream {
    name: String,
    batches: Vec<StreamBatch>,
}

impl Stream {
    /// Create a stream.
    pub fn new(name: impl Into<String>, batches: Vec<StreamBatch>) -> Self {
        Self {
            name: name.into(),
            batches,
        }
    }

    /// Stream name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Borrow bootstrap batches.
    pub fn batches(&self) -> &[StreamBatch] {
        &self.batches
    }

    /// Collect bounded in-memory stream batches.
    pub fn collect_bounded(&self) -> Vec<StreamBatch> {
        self.batches.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{ExecutionMode, Field, RecordBatch, ScalarValue, Schema, Session, StreamBatch};

    #[test]
    fn session_builder_defaults_to_embedded() {
        let session = match Session::builder().build() {
            Ok(session) => session,
            Err(error) => panic!("unexpected API error: {error}"),
        };

        assert_eq!(session.mode(), ExecutionMode::Embedded);
    }

    #[test]
    fn session_builder_accepts_single_node() {
        let session = match Session::builder()
            .with_execution_mode(ExecutionMode::SingleNode)
            .build()
        {
            Ok(session) => session,
            Err(error) => panic!("unexpected API error: {error}"),
        };

        assert_eq!(session.mode(), ExecutionMode::SingleNode);
    }

    #[test]
    fn sql_creates_dataframe_plan() {
        let session = match Session::builder().build() {
            Ok(session) => session,
            Err(error) => panic!("unexpected API error: {error}"),
        };

        let dataframe = match session.sql("select 1") {
            Ok(dataframe) => dataframe,
            Err(error) => panic!("unexpected API error: {error}"),
        };

        assert!(dataframe.explain().contains("sql placeholder"));
    }

    #[test]
    fn memory_stream_collects_bounded_batches() {
        let session = match Session::builder().build() {
            Ok(session) => session,
            Err(error) => panic!("unexpected API error: {error}"),
        };
        let schema = Schema::new(vec![Field::new("value", "int64")]);
        let batch = RecordBatch::new(schema, vec![vec![ScalarValue::Int64(1)]]);
        let stream = session.memory_stream("numbers", vec![StreamBatch::new(0, batch)]);

        assert_eq!(stream.name(), "numbers");
        assert_eq!(stream.collect_bounded().len(), 1);
    }
}
