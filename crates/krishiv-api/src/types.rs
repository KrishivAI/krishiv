use arrow::record_batch::RecordBatch;
use std::fmt;

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

/// Query result wrapper around Arrow record batches.
#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    batches: Vec<RecordBatch>,
}

impl QueryResult {
    /// Create a query result from Arrow batches.
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }

    /// Result batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Total row count across all batches.
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// Format the result as an ASCII table for CLI and tests.
    pub fn pretty(&self) -> Result<String, crate::error::KrishivError> {
        krishiv_sql::pretty_batches(&self.batches).map_err(Into::into)
    }

    /// Consume self and return the owned batches.
    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.batches
    }
}

impl IntoIterator for QueryResult {
    type Item = RecordBatch;
    type IntoIter = std::vec::IntoIter<RecordBatch>;

    fn into_iter(self) -> Self::IntoIter {
        self.batches.into_iter()
    }
}

impl From<Vec<RecordBatch>> for QueryResult {
    fn from(batches: Vec<RecordBatch>) -> Self {
        QueryResult::new(batches)
    }
}

impl From<QueryResult> for Vec<RecordBatch> {
    fn from(result: QueryResult) -> Self {
        result.into_batches()
    }
}

/// Stream batch wrapper.
#[derive(Debug, Clone)]
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

    /// Consume self and return the owned batch.
    pub fn into_batch(self) -> RecordBatch {
        self.batch
    }
}

/// R1 local stream mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamMode {
    /// Bounded stream backed by known in-memory batches.
    Bounded,
    /// Unbounded stream placeholder for future local streaming tests.
    Unbounded,
}

impl fmt::Display for StreamMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bounded => f.write_str("bounded"),
            Self::Unbounded => f.write_str("unbounded"),
        }
    }
}
