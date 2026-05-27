//! Multi-input checkpoint barrier alignment (R16 S1.3).

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Error when barrier alignment times out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointAlignmentTimeout {
    pub epoch: u64,
    pub waited_inputs: usize,
    pub expected_inputs: usize,
}

impl std::fmt::Display for CheckpointAlignmentTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "checkpoint alignment timeout for epoch {}: received barriers on {}/{} inputs",
            self.epoch, self.waited_inputs, self.expected_inputs
        )
    }
}

impl std::error::Error for CheckpointAlignmentTimeout {}

/// Buffers records on faster inputs until all inputs have seen the barrier epoch.
#[derive(Debug)]
pub struct BarrierAligner {
    input_count: usize,
    /// Set of input indices that have already reported a barrier for the current epoch.
    /// Using a HashSet prevents the same input from being counted more than once,
    /// which would cause premature alignment (fix for double-counting bug).
    barrier_inputs_seen: HashSet<usize>,
    current_epoch: Option<u64>,
    buffers: Vec<VecDeque<arrow::record_batch::RecordBatch>>,
    alignment_deadline: Option<Instant>,
    alignment_timeout: Duration,
}

impl BarrierAligner {
    pub fn new(input_count: usize, alignment_timeout: Duration) -> Self {
        Self {
            input_count: input_count.max(1),
            barrier_inputs_seen: HashSet::new(),
            current_epoch: None,
            buffers: (0..input_count.max(1)).map(|_| VecDeque::new()).collect(),
            alignment_deadline: None,
            alignment_timeout,
        }
    }

    /// Record a data batch on `input_index` while alignment is in progress.
    pub fn buffer_data(&mut self, input_index: usize, batch: arrow::record_batch::RecordBatch) {
        if let Some(buf) = self.buffers.get_mut(input_index) {
            buf.push_back(batch);
        }
    }

    /// Notify that `input_index` received barrier for `epoch`.
    ///
    /// Returns `Ok(true)` when all inputs have aligned and buffered data may be released.
    ///
    /// Each `input_index` is counted at most once per epoch — duplicate calls from
    /// the same input are idempotent and do not advance the alignment counter.
    pub fn on_barrier(
        &mut self,
        input_index: usize,
        epoch: u64,
    ) -> Result<bool, CheckpointAlignmentTimeout> {
        if self.current_epoch.is_none() {
            self.current_epoch = Some(epoch);
            self.barrier_inputs_seen.clear();
            self.alignment_deadline = Some(Instant::now() + self.alignment_timeout);
        }
        if self.current_epoch != Some(epoch) {
            return Ok(false);
        }
        // Insert returns false when the input_index was already present —
        // duplicates are silently ignored so one fast input cannot trigger
        // premature alignment by reporting multiple times.
        if input_index < self.input_count {
            self.barrier_inputs_seen.insert(input_index);
        }
        if self.barrier_inputs_seen.len() >= self.input_count {
            self.barrier_inputs_seen.clear();
            self.current_epoch = None;
            self.alignment_deadline = None;
            return Ok(true);
        }
        if let Some(deadline) = self.alignment_deadline
            && Instant::now() > deadline
        {
            return Err(CheckpointAlignmentTimeout {
                epoch,
                waited_inputs: self.barrier_inputs_seen.len(),
                expected_inputs: self.input_count,
            });
        }
        Ok(false)
    }

    /// Drain buffered batches for `input_index` after alignment completes.
    pub fn drain_buffer(&mut self, input_index: usize) -> Vec<arrow::record_batch::RecordBatch> {
        self.buffers
            .get_mut(input_index)
            .map(|q| q.drain(..).collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(v: i32) -> arrow::record_batch::RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![v]))],
        )
        .unwrap()
    }

    #[test]
    fn barrier_alignment_buffers_until_all_inputs_ready() {
        let mut aligner = BarrierAligner::new(2, Duration::from_secs(5));
        aligner.buffer_data(0, batch(1));
        assert!(!aligner.on_barrier(0, 1).unwrap());
        aligner.buffer_data(0, batch(2));
        assert!(aligner.on_barrier(1, 1).unwrap());
        let released = aligner.drain_buffer(0);
        assert_eq!(released.len(), 2);
    }
}
