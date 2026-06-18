//! Pipeline source adapters — turn connector input into `DeltaBatch` feeds.

use arrow::record_batch::RecordBatch;
use krishiv_connectors::DynSource;

/// A single CDC change event (maps directly onto `DeltaBatch::from_cdc`).
///
/// - INSERT: `before = None, after = Some(_)`
/// - DELETE: `before = Some(_), after = None`
/// - UPDATE: `before = Some(_), after = Some(_)`
#[derive(Clone, Debug, Default)]
pub struct CdcChange {
    pub before: Option<RecordBatch>,
    pub after: Option<RecordBatch>,
}

impl CdcChange {
    /// An INSERT change.
    pub fn insert(after: RecordBatch) -> Self {
        Self {
            before: None,
            after: Some(after),
        }
    }
    /// A DELETE change.
    pub fn delete(before: RecordBatch) -> Self {
        Self {
            before: Some(before),
            after: None,
        }
    }
    /// An UPDATE change.
    pub fn update(before: RecordBatch, after: RecordBatch) -> Self {
        Self {
            before: Some(before),
            after: Some(after),
        }
    }
}

/// How a pipeline source delivers input.
pub enum Ingest {
    /// In-memory record batches, fed as insertions (testing / embedding).
    Memory(Vec<RecordBatch>),
    /// In-memory CDC change events, fed via `DeltaBatch::from_cdc`.
    Cdc(Vec<CdcChange>),
    /// A pull-based connector source; each batch is fed as insertions until
    /// the source is exhausted (`read_batch` returns `None`).
    Connector(Box<dyn DynSource>),
}

impl std::fmt::Debug for Ingest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Ingest::Memory(b) => write!(f, "Ingest::Memory({} batches)", b.len()),
            Ingest::Cdc(c) => write!(f, "Ingest::Cdc({} changes)", c.len()),
            Ingest::Connector(_) => write!(f, "Ingest::Connector(..)"),
        }
    }
}
