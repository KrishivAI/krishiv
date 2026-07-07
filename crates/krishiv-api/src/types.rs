use arrow::record_batch::RecordBatch;
use std::fmt;

/// Execution mode selected for a session.
///
/// Controls **HOW** query routing works (local vs remote Flight).
/// Orthogonal to [`DeploymentTarget`], which says WHERE the cluster runs.
///
/// # Layering note (A-1)
///
/// Three enums carry the same concept at different layers:
///
/// - [`ExecutionMode`] (this enum) is the session's user-facing intent
///   (local vs remote). Set by [`SessionBuilder::with_execution_mode`] or
///   the `KRISHIV_MODE` env var.
/// - [`krishiv_engine_core::Placement`] is the engine spine's data-plane
///   location the engine code codes against. Reachable via the
///   `From<ExecutionMode> for Placement` impl below.
/// - [`krishiv_runtime::RuntimeMode`] + [`krishiv_runtime::ExecutionPlacement`]
///   carry the runtime's 2-D routing decision (mode × whether a coordinator
///   URL is present), which is where "local fallback vs remote required" is
///   validated. See `krishiv_runtime::ExecutionRuntime`.
///
/// The layering is intentional: collapsing them into one enum would lose the
/// runtime's 2-D validation. The `From` impls at the bottom of this file
/// are the only thing keeping the three in sync.
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

/// Canonical mapping from the user-facing [`ExecutionMode`] to the engine-core
/// data-plane [`Placement`](krishiv_engine_core::Placement) — "where a job's
/// data plane runs". This is the single source of truth, so the unified
/// `submit` path does not hand-roll the match.
///
/// The three mode/placement enums are deliberately *layered*, not redundant, so
/// they convert rather than collapse:
/// - [`ExecutionMode`] is the session's user-facing intent (local vs remote);
/// - `krishiv_runtime::{RuntimeMode, ExecutionPlacement}` carry the runtime's
///   2-D routing decision (mode × whether a coordinator URL is present), which
///   is where "local fallback vs remote required" is validated;
/// - [`Placement`](krishiv_engine_core::Placement) is the spine's data-plane
///   location the engines code against.
///
/// Collapsing them into one enum would lose the runtime's 2-D validation.
impl From<ExecutionMode> for krishiv_engine_core::Placement {
    fn from(mode: ExecutionMode) -> Self {
        match mode {
            ExecutionMode::Embedded => Self::Embedded,
            ExecutionMode::SingleNode => Self::SingleNode,
            ExecutionMode::Distributed => Self::Distributed,
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

    /// Convert the query result into a ``DeltaBatch`` with all rows as
    /// insertions (weight +1). This bridges the SQL/DataFrame API to the
    /// IVM engine: feed the result into ``IvmJob::feed()``.
    ///
    /// If the result spans multiple batches, they are concatenated into
    /// one DeltaBatch.
    pub fn into_delta_batch(self) -> Result<krishiv_delta::DeltaBatch, crate::error::KrishivError> {
        let combined = if self.batches.len() <= 1 {
            self.batches.first().cloned().unwrap_or_else(|| {
                RecordBatch::new_empty(std::sync::Arc::new(arrow::datatypes::Schema::empty()))
            })
        } else {
            let schema = self
                .batches
                .first()
                .map(|b| b.schema())
                .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
            arrow::compute::concat_batches(&schema, &self.batches).map_err(|e| {
                crate::error::KrishivError::Runtime {
                    message: format!("failed to concat batches for DeltaBatch: {e}"),
                }
            })?
        };
        krishiv_delta::DeltaBatch::from_inserts(combined).map_err(|e| {
            crate::error::KrishivError::Runtime {
                message: e.to_string(),
            }
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_engine_core::Placement;

    #[test]
    fn execution_mode_maps_to_canonical_placement() {
        assert_eq!(
            Placement::from(ExecutionMode::Embedded),
            Placement::Embedded
        );
        assert_eq!(
            Placement::from(ExecutionMode::SingleNode),
            Placement::SingleNode
        );
        assert_eq!(
            Placement::from(ExecutionMode::Distributed),
            Placement::Distributed
        );
    }
}
