//! Typed envelope for streaming data and control messages.
//!
//! `StreamEnvelope` provides a unified abstraction for data, watermarks,
//! checkpoint barriers, timers, and end-of-input signals in the streaming
//! runtime.

use arrow::record_batch::RecordBatch;
use krishiv_common::async_util::unix_now_ms;

pub use super::queue::CheckpointAlignment;

/// Kind of timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerKind {
    /// Processing-time timer (fires based on wall clock).
    Processing,
    /// Event-time timer (fires based on watermark).
    EventTime,
}

/// Typed envelope for streaming data and control messages.
///
/// All messages in the streaming runtime flow through this envelope,
/// providing a unified abstraction for data and control paths.
pub enum StreamEnvelope {
    /// Data batch with optional metadata.
    Data {
        /// The record batch containing the data.
        batch: RecordBatch,
        /// Source that produced this batch (for checkpoint tracking).
        source_id: Option<String>,
        /// Timestamp when batch was produced (for latency tracking).
        produced_at_ms: i64,
    },

    /// Watermark indicating event time progress.
    Watermark {
        /// Watermark value in epoch milliseconds.
        epoch_ms: i64,
        /// Source that produced this watermark.
        source_id: String,
    },

    /// Checkpoint barrier for coordinated snapshots.
    CheckpointBarrier {
        /// Epoch number for this checkpoint.
        epoch: u64,
        /// Alignment mode (aligned or unaligned).
        alignment: CheckpointAlignment,
    },

    /// Timer fire for stateful operators.
    Timer {
        /// Key that owns this timer.
        key: Vec<u8>,
        /// When the timer should fire (epoch ms).
        fire_time_ms: i64,
        /// Kind of timer (processing or event time).
        kind: TimerKind,
    },

    /// End of input signal (for bounded sources).
    EndOfInput,
}

impl StreamEnvelope {
    /// Create a data envelope with the current timestamp.
    pub fn data(batch: RecordBatch, source_id: Option<String>) -> Self {
        Self::Data {
            batch,
            source_id,
            produced_at_ms: unix_now_ms(),
        }
    }

    /// Create a watermark envelope.
    pub fn watermark(epoch_ms: i64, source_id: String) -> Self {
        Self::Watermark {
            epoch_ms,
            source_id,
        }
    }

    /// Create a checkpoint barrier envelope.
    pub fn checkpoint(epoch: u64, alignment: CheckpointAlignment) -> Self {
        Self::CheckpointBarrier { epoch, alignment }
    }

    /// Create a processing-time timer envelope.
    pub fn processing_timer(key: Vec<u8>, fire_time_ms: i64) -> Self {
        Self::Timer {
            key,
            fire_time_ms,
            kind: TimerKind::Processing,
        }
    }

    /// Create an event-time timer envelope.
    pub fn event_time_timer(key: Vec<u8>, fire_time_ms: i64) -> Self {
        Self::Timer {
            key,
            fire_time_ms,
            kind: TimerKind::EventTime,
        }
    }

    /// Returns true if this envelope carries data.
    pub fn is_data(&self) -> bool {
        matches!(self, Self::Data { .. })
    }

    /// Returns true if this envelope is a control message (not data).
    pub fn is_control(&self) -> bool {
        !self.is_data()
    }

    /// Returns true if this envelope is a checkpoint barrier.
    pub fn is_checkpoint(&self) -> bool {
        matches!(self, Self::CheckpointBarrier { .. })
    }

    /// Returns true if this envelope is end-of-input.
    pub fn is_end_of_input(&self) -> bool {
        matches!(self, Self::EndOfInput)
    }

    /// Extract the data batch if this is a Data envelope.
    pub fn into_data(self) -> Option<RecordBatch> {
        match self {
            Self::Data { batch, .. } => Some(batch),
            _ => None,
        }
    }

    /// Extract the source_id if this is a Data or Watermark envelope.
    pub fn source_id(&self) -> Option<&str> {
        match self {
            Self::Data { source_id, .. } => source_id.as_deref(),
            Self::Watermark { source_id, .. } => Some(source_id),
            _ => None,
        }
    }
}
