//! Multi-input checkpoint-barrier **alignment** (Chandy–Lamport).
//!
//! An operator with several input channels must take a consistent snapshot: it
//! records its state only after the checkpoint barrier for an epoch has arrived
//! on **every** input. Between the first and last barrier of an epoch, the inputs
//! that already delivered their barrier are *blocked* — their post-barrier data
//! belongs to the next epoch and must be buffered, not folded into this epoch's
//! snapshot. When the final input delivers the barrier the epoch is *aligned*:
//! the operator snapshots and every input unblocks.
//!
//! [`BarrierAligner`] is the pure state machine for that protocol. A single-input
//! operator degenerates correctly — its first barrier aligns immediately, so
//! nothing ever blocks.

/// The outcome of recording a barrier on one input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarrierEvent {
    /// The barrier was recorded but the epoch is not yet aligned: other inputs
    /// still owe their barrier. The input that just delivered is now **blocked**
    /// — buffer its further data until alignment completes.
    Blocked,
    /// This barrier completed alignment — every input has now delivered the
    /// epoch's barrier. The operator should snapshot; all inputs unblock.
    Aligned,
    /// A stale or duplicate barrier (an already-aligned epoch, or a second
    /// barrier for the same epoch on the same input). Ignore it.
    Ignored,
}

/// Tracks checkpoint-barrier arrival across an operator's `num_inputs` input
/// channels and signals when an epoch is aligned.
#[derive(Debug, Clone)]
pub struct BarrierAligner {
    num_inputs: usize,
    /// The epoch currently aligning and, per input, whether its barrier arrived.
    current: Option<(u64, Vec<bool>)>,
    /// The highest epoch already aligned — used to ignore stale/duplicate barriers.
    last_aligned: Option<u64>,
}

impl BarrierAligner {
    /// Create an aligner for an operator with `num_inputs` inputs (clamped to at
    /// least one).
    pub fn new(num_inputs: usize) -> Self {
        Self {
            num_inputs: num_inputs.max(1),
            current: None,
            last_aligned: None,
        }
    }

    /// Number of input channels this aligner coordinates.
    pub fn num_inputs(&self) -> usize {
        self.num_inputs
    }

    /// The epoch currently aligning, if alignment is in progress.
    pub fn aligning_epoch(&self) -> Option<u64> {
        self.current.as_ref().map(|(epoch, _)| *epoch)
    }

    /// Whether `input` is currently blocked — it delivered the in-progress
    /// epoch's barrier and must buffer further data until alignment completes.
    pub fn is_blocked(&self, input: usize) -> bool {
        match &self.current {
            Some((_, seen)) => seen.get(input).copied().unwrap_or(false),
            None => false,
        }
    }

    /// Record the checkpoint barrier for `epoch` arriving on `input`.
    pub fn record_barrier(&mut self, epoch: u64, input: usize) -> BarrierEvent {
        if input >= self.num_inputs {
            return BarrierEvent::Ignored;
        }
        // Already snapshotted this (or a later) epoch ⇒ stale.
        if self.last_aligned.is_some_and(|done| epoch <= done) {
            return BarrierEvent::Ignored;
        }

        match &mut self.current {
            Some((cur_epoch, seen)) => {
                if epoch < *cur_epoch {
                    // A barrier older than the one we're aligning — stale.
                    return BarrierEvent::Ignored;
                }
                if epoch > *cur_epoch {
                    // A newer epoch began before the current one aligned. Barriers
                    // are monotonic per input and an epoch aligns before the next,
                    // so this only happens if the current epoch was abandoned;
                    // restart alignment on the newer epoch.
                    let mut next = vec![false; self.num_inputs];
                    if let Some(slot) = next.get_mut(input) {
                        *slot = true;
                    }
                    *cur_epoch = epoch;
                    *seen = next;
                    return self.finish_if_aligned(epoch);
                }
                // Same epoch: mark this input. A repeat barrier on the same input
                // is a no-op (already true) → Ignored.
                match seen.get_mut(input) {
                    Some(slot) if !*slot => *slot = true,
                    _ => return BarrierEvent::Ignored,
                }
                self.finish_if_aligned(epoch)
            }
            None => {
                let mut seen = vec![false; self.num_inputs];
                if let Some(slot) = seen.get_mut(input) {
                    *slot = true;
                }
                self.current = Some((epoch, seen));
                self.finish_if_aligned(epoch)
            }
        }
    }

    /// If every input has delivered the in-progress epoch's barrier, complete
    /// alignment (clear state, advance `last_aligned`) and report it.
    fn finish_if_aligned(&mut self, epoch: u64) -> BarrierEvent {
        let aligned = self
            .current
            .as_ref()
            .is_some_and(|(_, seen)| seen.iter().all(|&b| b));
        if aligned {
            self.current = None;
            self.last_aligned = Some(epoch);
            BarrierEvent::Aligned
        } else {
            BarrierEvent::Blocked
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_input_aligns_immediately() {
        let mut a = BarrierAligner::new(1);
        assert_eq!(a.record_barrier(1, 0), BarrierEvent::Aligned);
        assert!(!a.is_blocked(0));
        assert_eq!(a.aligning_epoch(), None);
    }

    #[test]
    fn two_inputs_block_then_align() {
        let mut a = BarrierAligner::new(2);
        // Left delivers first → blocked, not yet aligned.
        assert_eq!(a.record_barrier(7, 0), BarrierEvent::Blocked);
        assert!(a.is_blocked(0), "left buffers after its barrier");
        assert!(!a.is_blocked(1), "right still flows");
        assert_eq!(a.aligning_epoch(), Some(7));
        // Right delivers → aligned, both unblock.
        assert_eq!(a.record_barrier(7, 1), BarrierEvent::Aligned);
        assert!(!a.is_blocked(0));
        assert!(!a.is_blocked(1));
        assert_eq!(a.aligning_epoch(), None);
    }

    #[test]
    fn duplicate_barrier_on_same_input_is_ignored() {
        let mut a = BarrierAligner::new(2);
        assert_eq!(a.record_barrier(3, 0), BarrierEvent::Blocked);
        assert_eq!(a.record_barrier(3, 0), BarrierEvent::Ignored);
        assert!(a.is_blocked(0));
    }

    #[test]
    fn stale_and_already_aligned_barriers_are_ignored() {
        let mut a = BarrierAligner::new(2);
        a.record_barrier(5, 0);
        a.record_barrier(5, 1); // aligned epoch 5
        // A barrier for an epoch already aligned is stale.
        assert_eq!(a.record_barrier(5, 0), BarrierEvent::Ignored);
        assert_eq!(a.record_barrier(4, 1), BarrierEvent::Ignored);
        // The next epoch aligns normally.
        assert_eq!(a.record_barrier(6, 1), BarrierEvent::Blocked);
        assert_eq!(a.record_barrier(6, 0), BarrierEvent::Aligned);
    }

    #[test]
    fn out_of_range_input_is_ignored() {
        let mut a = BarrierAligner::new(2);
        assert_eq!(a.record_barrier(1, 2), BarrierEvent::Ignored);
        assert_eq!(a.record_barrier(1, 99), BarrierEvent::Ignored);
    }

    #[test]
    fn three_inputs_need_all_three() {
        let mut a = BarrierAligner::new(3);
        assert_eq!(a.record_barrier(2, 2), BarrierEvent::Blocked);
        assert_eq!(a.record_barrier(2, 0), BarrierEvent::Blocked);
        assert!(a.is_blocked(0) && a.is_blocked(2) && !a.is_blocked(1));
        assert_eq!(a.record_barrier(2, 1), BarrierEvent::Aligned);
    }

    #[test]
    fn newer_epoch_restarts_alignment() {
        let mut a = BarrierAligner::new(2);
        assert_eq!(a.record_barrier(1, 0), BarrierEvent::Blocked);
        // A newer epoch barrier arrives before epoch 1 aligned — restart on it.
        assert_eq!(a.record_barrier(2, 0), BarrierEvent::Blocked);
        assert_eq!(a.aligning_epoch(), Some(2));
        assert_eq!(a.record_barrier(2, 1), BarrierEvent::Aligned);
    }
}
