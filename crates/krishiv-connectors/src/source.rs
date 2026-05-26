//! source.

use std::any::Any;
use std::future::Future;

use crate::capabilities::ConnectorCapabilities;
use crate::error::ConnectorResult;

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
    /// A debug-mode assertion fires if a rewindable source does not override,
    /// catching capability mismatches during development.
    fn reset(&mut self) {
        debug_assert!(
            !self.capabilities().is_rewindable(),
            "source advertises rewindable capability but does not override reset()"
        );
    }
}
