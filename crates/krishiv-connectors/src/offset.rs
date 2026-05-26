//! offset.

use std::future::Future;

use crate::error::{ConnectorError, ConnectorResult};

// ---------------------------------------------------------------------------
// Offset
// ---------------------------------------------------------------------------

/// Serialisable cursor into a connector's data stream.
///
/// Implementors must be able to round-trip through `encode`/`decode` without
/// loss of information.
pub trait Offset {
    /// Serialise this offset to a byte vector.
    fn encode(&self) -> Vec<u8>;

    /// Deserialise an offset from a byte slice.
    fn decode(bytes: &[u8]) -> ConnectorResult<Self>
    where
        Self: Sized;
}

// ---------------------------------------------------------------------------
// CommitHandle
// ---------------------------------------------------------------------------

/// Handle returned by a sink after a batch is buffered, allowing the caller
/// to drive the two-phase commit boundary.
pub trait CommitHandle {
    /// Durably commit the buffered output.
    fn commit(&self) -> impl Future<Output = ConnectorResult<()>> + Send;
}

// ---------------------------------------------------------------------------
// OffsetCommitter
// ---------------------------------------------------------------------------

/// Commits a source offset after downstream output is durable.
///
/// This is intentionally separate from [`Source`] so executors can keep source
/// reads, sink writes, and offset commits ordered explicitly.  For at-least-once
/// delivery, callers must commit offsets only after the corresponding sink
/// output has been written and flushed.
pub trait OffsetCommitter<O: Offset> {
    /// Persist `offset` as consumed.
    fn commit_offset(&mut self, offset: O) -> impl Future<Output = ConnectorResult<()>> + Send;
}

// ---------------------------------------------------------------------------
// ParquetOffset
// ---------------------------------------------------------------------------

/// Cursor into a Parquet-backed source: index of the next batch to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetOffset {
    pub batch_index: usize,
}

impl Offset for ParquetOffset {
    fn encode(&self) -> Vec<u8> {
        (self.batch_index as u64).to_le_bytes().to_vec()
    }

    fn decode(bytes: &[u8]) -> ConnectorResult<Self> {
        if bytes.len() < 8 {
            return Err(ConnectorError::IoStr {
                message: "ParquetOffset decode: expected 8 bytes".into(),
            });
        }
        let n = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        Ok(Self {
            batch_index: n as usize,
        })
    }
}
