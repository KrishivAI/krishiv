//! Source trait, checkpoint source trait, and dynamic dispatch adapter.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;

use crate::capabilities::ConnectorCapabilities;
use crate::error::ConnectorResult;
use crate::offset::Offset;

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// An async, pull-based data source that emits Arrow [`RecordBatch`] values.
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
pub trait Source {
    /// Return the capabilities this source supports.
    fn capabilities(&self) -> ConnectorCapabilities;

    /// Pull the next batch from the source.
    ///
    /// Returns `Ok(None)` when the source is exhausted (bounded sources only).
    fn read_batch(
        &mut self,
    ) -> impl Future<Output = ConnectorResult<Option<arrow::record_batch::RecordBatch>>> + Send;

    /// Return the current read offset, if available.
    ///
    /// The returned value is connector-specific and should be downcast by the
    /// caller if it needs the concrete offset type.
    fn current_offset(&self) -> Option<Box<dyn Any + Send>>;

    /// Rewind the source to its initial position.
    ///
    /// The default implementation is a no-op; sources that advertise
    /// [`ConnectorCapabilities::is_rewindable`] **must** override this method.
    /// A warning is logged in all build profiles when a rewindable source
    /// has not overridden reset() — in release this produces an
    /// observability signal rather than silently producing incorrect results.
    fn reset(&mut self) {
        if self.capabilities().is_rewindable() {
            tracing::warn!(
                "rewindable source with capabilities {:?} does not override reset(); \
                 reset will be a no-op and may produce incorrect results",
                self.capabilities()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// CheckpointSource
// ---------------------------------------------------------------------------

/// A source that can persist and restore an exact typed read position.
///
/// Implementations must reject offsets belonging to another source or offsets
/// that do not identify a valid read boundary. Restoring an accepted offset
/// must make the next [`Source::read_batch`] return the same result it returned
/// from that position originally.
pub trait CheckpointSource: Source {
    /// Connector-specific durable offset type.
    type Offset: Offset + Clone + PartialEq + std::fmt::Debug + Send + 'static;

    /// Return the exact offset of the next record or batch to read.
    fn checkpoint_offset(&self) -> ConnectorResult<Self::Offset>;

    /// Restore the source to an exact previously captured offset.
    fn restore_offset(&mut self, offset: &Self::Offset) -> ConnectorResult<()>;

    /// Encode the current checkpoint offset for durable metadata.
    fn encoded_checkpoint_offset(&self) -> ConnectorResult<Vec<u8>> {
        Ok(self.checkpoint_offset()?.encode())
    }

    /// Decode and restore a checkpoint offset from durable metadata.
    fn restore_encoded_offset(&mut self, encoded: &[u8]) -> ConnectorResult<()> {
        let offset = Self::Offset::decode(encoded)?;
        self.restore_offset(&offset)
    }
}

// ---------------------------------------------------------------------------
// DynSource
// ---------------------------------------------------------------------------

/// Dyn-compatible version of [`Source`] that boxes async return types.
///
/// Because [`Source`] uses `impl Future` returns it is not object-safe. This
/// trait provides a blanket implementation over every `T: Source + Send` and
/// can be used as `Box<dyn DynSource>` wherever dynamic dispatch is needed.
pub trait DynSource: Send {
    fn capabilities(&self) -> ConnectorCapabilities;

    fn read_batch_dyn(
        &mut self,
    ) -> Pin<
        Box<
            dyn Future<Output = ConnectorResult<Option<arrow::record_batch::RecordBatch>>>
                + Send
                + '_,
        >,
    >;

    fn current_offset_dyn(&self) -> Option<Box<dyn Any + Send>>;

    fn reset_dyn(&mut self);
}

impl<T: Source + Send> DynSource for T {
    fn capabilities(&self) -> ConnectorCapabilities {
        self.capabilities()
    }

    fn read_batch_dyn(
        &mut self,
    ) -> Pin<
        Box<
            dyn Future<Output = ConnectorResult<Option<arrow::record_batch::RecordBatch>>>
                + Send
                + '_,
        >,
    > {
        Box::pin(self.read_batch())
    }

    fn current_offset_dyn(&self) -> Option<Box<dyn Any + Send>> {
        self.current_offset()
    }

    fn reset_dyn(&mut self) {
        self.reset();
    }
}
