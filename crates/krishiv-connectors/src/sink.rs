//! sink.

use std::future::Future;
use std::pin::Pin;

use crate::capabilities::ConnectorCapabilities;
use crate::error::ConnectorResult;
use crate::offset::{Offset, OffsetCommitter};

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// An async, push-based data sink that accepts Arrow [`RecordBatch`] values.
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
pub trait Sink {
    /// Return the capabilities this sink supports.
    fn capabilities(&self) -> ConnectorCapabilities;

    /// Write a single batch to the sink.
    fn write_batch(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> impl Future<Output = ConnectorResult<()>> + Send;

    /// Flush any buffered data and close the sink.
    fn flush(&mut self) -> impl Future<Output = ConnectorResult<()>> + Send;
}

/// Dyn-compatible version of [`Sink`] that boxes the async return types.
///
/// Because [`Sink`] uses `impl Future` returns it is not object-safe.  This
/// trait provides a blanket implementation over every `T: Sink + Send` and can
/// be used as `Box<dyn DynSink>` wherever dynamic dispatch is needed.
pub trait DynSink: Send {
    /// Return the capabilities advertised by the concrete sink.
    fn capabilities(&self) -> ConnectorCapabilities;

    fn write_batch_dyn(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<()>> + Send + '_>>;

    fn flush_dyn(&mut self) -> Pin<Box<dyn Future<Output = ConnectorResult<()>> + Send + '_>>;
}

impl<T: Sink + Send> DynSink for T {
    fn capabilities(&self) -> ConnectorCapabilities {
        Sink::capabilities(self)
    }

    fn write_batch_dyn(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Pin<Box<dyn Future<Output = ConnectorResult<()>> + Send + '_>> {
        Box::pin(self.write_batch(batch))
    }

    fn flush_dyn(&mut self) -> Pin<Box<dyn Future<Output = ConnectorResult<()>> + Send + '_>> {
        Box::pin(self.flush())
    }
}

/// Compatibility path for the canonical Parquet source offset type.
pub use crate::offset::ParquetOffset;

// ---------------------------------------------------------------------------
// AtLeastOnceSinkContract
// ---------------------------------------------------------------------------

/// Documents the at-least-once sink delivery contract.
///
/// A sink operating under at-least-once semantics guarantees:
/// - Every input batch is written to the downstream store at least once.
/// - On executor reassignment or crash-recovery, batches that were written
///   but not yet acknowledged may be replayed. Idempotent sinks (`is_idempotent()`)
///   handle replays safely. Non-idempotent sinks may produce duplicates.
/// - `flush()` must be called and awaited before an offset is committed to
///   the source. Committing the source offset before `flush()` completes risks
///   data loss on crash.
pub struct AtLeastOnceSinkContract;

/// Drives the R3.2 post-write offset commit protocol.
///
/// The protocol order is:
///
/// 1. write the output batch to the sink,
/// 2. flush the sink so the output is durable,
/// 3. commit the source offset.
///
/// If either the write or flush fails, the offset is not committed.  This keeps
/// reassignment/replay at-least-once: a task may write duplicate output to a
/// non-idempotent sink, but it does not acknowledge data that was not durably
/// written.
#[derive(Debug, Default, Clone, Copy)]
pub struct PostWriteOffsetCommitProtocol;

impl PostWriteOffsetCommitProtocol {
    /// Write `batch`, flush `sink`, and then commit `offset`.
    pub async fn write_flush_commit<S, C, O>(
        sink: &mut S,
        committer: &mut C,
        batch: arrow::record_batch::RecordBatch,
        offset: O,
    ) -> ConnectorResult<()>
    where
        S: Sink,
        C: OffsetCommitter<O>,
        O: Offset,
    {
        sink.write_batch(batch).await?;
        sink.flush().await?;
        committer.commit_offset(offset).await
    }
}
