//! Streaming data-quality hook shared by exec and connectors.

use arrow::record_batch::RecordBatch;

/// Result type for [`StreamQualityHook`] implementations.
pub type StreamQualityResult<T> = Result<T, String>;

/// Apply connector-side quality rules to a streaming output batch.
///
/// Defined in `krishiv-common` so `krishiv-connectors` can implement the hook
/// without depending on `krishiv-exec`.
pub trait StreamQualityHook: Send {
    fn filter(&mut self, batch: RecordBatch) -> StreamQualityResult<(RecordBatch, usize)>;
}
