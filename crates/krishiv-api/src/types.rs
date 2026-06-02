use arrow::record_batch::RecordBatch;
use std::fmt;

/// Execution mode selected for a session.
///
/// Controls HOW query routing works (local vs remote Flight).
/// Orthogonal to [`DeploymentTarget`], which says WHERE the cluster runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// In-process execution for embedding Krishiv in a Rust application.
    Embedded,
    /// Single-node execution through the local Krishiv runtime.
    SingleNode,
    /// Distributed execution routed to a remote coordinator over Arrow Flight.
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

/// Where the Krishiv cluster physically runs.
///
/// Orthogonal to [`ExecutionMode`] — both `Kubernetes` and `BareMetal`
/// use `Distributed` execution mode (Arrow Flight routing), but they differ
/// in how the cluster was provisioned and how nodes discover each other.
/// Stored in the session for telemetry labels and future auto-configuration
/// (k8s service-account token injection, bare-metal config-file discovery, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeploymentTarget {
    /// In-process — library embedding or tests.
    #[default]
    Embedded,
    /// Single host — local process or local Flight daemon.
    SingleNode,
    /// Bare-metal or VM cluster — coordinator/executors run as processes.
    BareMetal,
    /// Kubernetes cluster — operator-managed pods, service-account auth.
    Kubernetes,
}

impl fmt::Display for DeploymentTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Embedded => f.write_str("embedded"),
            Self::SingleNode => f.write_str("single-node"),
            Self::BareMetal => f.write_str("bare-metal"),
            Self::Kubernetes => f.write_str("kubernetes"),
        }
    }
}

impl From<ExecutionMode> for DeploymentTarget {
    fn from(mode: ExecutionMode) -> Self {
        match mode {
            ExecutionMode::Embedded => Self::Embedded,
            ExecutionMode::SingleNode => Self::SingleNode,
            // Distributed covers both; use BareMetal as the default.
            // from_env() sets Kubernetes explicitly when KRISHIV_MODE=k8s.
            ExecutionMode::Distributed => Self::BareMetal,
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
