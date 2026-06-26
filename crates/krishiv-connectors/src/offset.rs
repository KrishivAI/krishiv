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

// ---------------------------------------------------------------------------
// MultiFileOffset
// ---------------------------------------------------------------------------

/// Cursor into a multi-file Parquet source (directory or S3 prefix).
///
/// Encodes as 16 bytes: `file_index` (u64 LE) followed by `batch_index` (u64 LE).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiFileOffset {
    /// Index into the sorted file list (0-based).
    pub file_index: usize,
    /// Batch index within `file_index` (number of batches already consumed).
    pub batch_index: usize,
}

impl Offset for MultiFileOffset {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        out.extend_from_slice(&(self.file_index as u64).to_le_bytes());
        out.extend_from_slice(&(self.batch_index as u64).to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> ConnectorResult<Self> {
        if bytes.len() != 16 {
            return Err(ConnectorError::Offset {
                message: format!(
                    "MultiFileOffset decode: expected exactly 16 bytes, got {}",
                    bytes.len()
                ),
            });
        }
        let file_index = usize::try_from(u64::from_le_bytes(bytes[0..8].try_into().unwrap()))
            .map_err(|_| ConnectorError::Offset {
                message: "MultiFileOffset decode: file_index exceeds usize".into(),
            })?;
        let batch_index = usize::try_from(u64::from_le_bytes(bytes[8..16].try_into().unwrap()))
            .map_err(|_| ConnectorError::Offset {
                message: "MultiFileOffset decode: batch_index exceeds usize".into(),
            })?;
        Ok(Self {
            file_index,
            batch_index,
        })
    }
}

impl Offset for ParquetOffset {
    fn encode(&self) -> Vec<u8> {
        (self.batch_index as u64).to_le_bytes().to_vec()
    }

    fn decode(bytes: &[u8]) -> ConnectorResult<Self> {
        if bytes.len() != 8 {
            return Err(ConnectorError::Offset {
                message: format!(
                    "ParquetOffset decode: expected exactly 8 bytes, got {}",
                    bytes.len()
                ),
            });
        }
        let batch_index = usize::try_from(u64::from_le_bytes(bytes.try_into().map_err(|_| {
            ConnectorError::Offset {
                message: "ParquetOffset decode: invalid byte width".into(),
            }
        })?))
        .map_err(|_| ConnectorError::Offset {
            message: "ParquetOffset decode: batch index exceeds this platform's usize".into(),
        })?;
        Ok(Self { batch_index })
    }
}
