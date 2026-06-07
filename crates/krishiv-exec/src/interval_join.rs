//! Stream-stream interval join (R16 S3.2) with per-key partitioning.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

/// Interval join bounds relative to the opposite stream's event time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntervalJoinSpec {
    pub lower_bound_ms: i64,
    pub upper_bound_ms: i64,
    /// Column used as the join key for per-key partitioning.
    pub key_column: String,
    /// Maximum events buffered per side per key before the oldest are dropped.
    /// Prevents unbounded memory growth on slow-processing keys.
    pub max_buffer_per_side: usize,
}

impl IntervalJoinSpec {
    /// Construct a spec with a sensible default buffer limit (65 536 events per side per key).
    pub fn new(
        key_column: impl Into<String>,
        lower_bound_ms: i64,
        upper_bound_ms: i64,
    ) -> Self {
        Self {
            lower_bound_ms,
            upper_bound_ms,
            key_column: key_column.into(),
            max_buffer_per_side: 65_536,
        }
    }
}

/// Buffered event with event-time for interval matching.
#[derive(Debug, Clone)]
struct BufferedEvent {
    event_time_ms: i64,
    batch: Arc<RecordBatch>,
}

/// Per-key buffers for both join sides.
#[derive(Debug)]
struct IntervalJoinBuffers {
    left: VecDeque<BufferedEvent>,
    right: VecDeque<BufferedEvent>,
}

impl IntervalJoinBuffers {
    fn new() -> Self {
        Self {
            left: VecDeque::new(),
            right: VecDeque::new(),
        }
    }

    fn push_side(
        &mut self,
        is_left: bool,
        event_time_ms: i64,
        batch: Arc<RecordBatch>,
        lower_bound_ms: i64,
        upper_bound_ms: i64,
        max_buffer: usize,
    ) -> Vec<(Arc<RecordBatch>, Arc<RecordBatch>)> {
        let (this_buf, other_buf) = if is_left {
            (&mut self.left, &mut self.right)
        } else {
            (&mut self.right, &mut self.left)
        };
        let mut matches = Vec::with_capacity(other_buf.len());
        for other in other_buf.iter() {
            let delta = if is_left {
                other.event_time_ms - event_time_ms
            } else {
                event_time_ms - other.event_time_ms
            };
            if delta >= lower_bound_ms && delta <= upper_bound_ms {
                if is_left {
                    matches.push((Arc::clone(&batch), Arc::clone(&other.batch)));
                } else {
                    matches.push((Arc::clone(&other.batch), Arc::clone(&batch)));
                }
            }
        }
        this_buf.push_back(BufferedEvent {
            event_time_ms,
            batch,
        });
        // Enforce buffer limit by dropping the oldest events.
        while this_buf.len() > max_buffer {
            this_buf.pop_front();
        }
        matches
    }

    fn evict_before(&mut self, watermark_ms: i64, bound: u64) {
        let horizon = watermark_ms.saturating_sub(bound as i64);
        self.left.retain(|e| e.event_time_ms >= horizon);
        self.right.retain(|e| e.event_time_ms >= horizon);
    }

    fn is_empty(&self) -> bool {
        self.left.is_empty() && self.right.is_empty()
    }
}

/// Per-key interval join operator.
///
/// Each join key maintains independent left/right buffers, so events from
/// different keys are never matched against each other.
#[derive(Debug)]
pub struct PerKeyIntervalJoin {
    spec: IntervalJoinSpec,
    states: HashMap<String, IntervalJoinBuffers>,
}

impl PerKeyIntervalJoin {
    pub fn new(spec: IntervalJoinSpec) -> Self {
        Self {
            spec,
            states: HashMap::new(),
        }
    }

    /// Push an event onto the left side for `key`.
    ///
    /// Returns matched pairs as `(left, right)` `Arc<RecordBatch>` tuples so
    /// callers can fan out matches to multiple consumers without per-fan copies.
    pub fn push_left(
        &mut self,
        key: &str,
        event_time_ms: i64,
        batch: RecordBatch,
    ) -> Vec<(Arc<RecordBatch>, Arc<RecordBatch>)> {
        let max_buf = self.spec.max_buffer_per_side;
        let state = self
            .states
            .entry(key.to_owned())
            .or_insert_with(IntervalJoinBuffers::new);
        state.push_side(
            true,
            event_time_ms,
            Arc::new(batch),
            self.spec.lower_bound_ms,
            self.spec.upper_bound_ms,
            max_buf,
        )
    }

    /// Push an event onto the right side for `key`.
    ///
    /// Returns matched pairs as `(left, right)` `Arc<RecordBatch>` tuples so
    /// callers can fan out matches to multiple consumers without per-fan copies.
    pub fn push_right(
        &mut self,
        key: &str,
        event_time_ms: i64,
        batch: RecordBatch,
    ) -> Vec<(Arc<RecordBatch>, Arc<RecordBatch>)> {
        let max_buf = self.spec.max_buffer_per_side;
        let state = self
            .states
            .entry(key.to_owned())
            .or_insert_with(IntervalJoinBuffers::new);
        state.push_side(
            false,
            event_time_ms,
            Arc::new(batch),
            self.spec.lower_bound_ms,
            self.spec.upper_bound_ms,
            max_buf,
        )
    }

    /// Evict stale events across all keys.
    pub fn evict_before(&mut self, watermark_ms: i64) {
        let bound = self
            .spec
            .upper_bound_ms
            .unsigned_abs()
            .max(self.spec.lower_bound_ms.unsigned_abs());
        self.states.retain(|_, state| {
            state.evict_before(watermark_ms, bound);
            !state.is_empty()
        });
    }

    /// Number of active keys with buffered events.
    pub fn active_key_count(&self) -> usize {
        self.states.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(v: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![v]))],
        )
        .unwrap()
    }

    fn spec_with_limit(max_buffer: usize) -> IntervalJoinSpec {
        IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
            key_column: "id".into(),
            max_buffer_per_side: max_buffer,
        }
    }

    // ── Per-key join tests ─────────────────────────────────────────────

    #[test]
    fn overlapping_interval_emits_match() {
        let mut join = PerKeyIntervalJoin::new(spec_with_limit(1000));
        assert!(join.push_left("k", 1000, batch(1)).is_empty());
        let matches = join.push_right("k", 1050, batch(2));
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn non_overlapping_interval_drops() {
        let mut join = PerKeyIntervalJoin::new(IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 50,
            key_column: "id".into(),
            max_buffer_per_side: 1000,
        });
        join.push_left("k", 1000, batch(1));
        assert!(join.push_right("k", 2000, batch(2)).is_empty());
    }

    #[test]
    fn evict_before_removes_old_events() {
        let mut join = PerKeyIntervalJoin::new(spec_with_limit(1000));
        join.push_left("k", 1000, batch(1));
        join.push_left("k", 2000, batch(2));
        join.push_right("k", 1500, batch(3));
        join.evict_before(2100);
        let matches = join.push_left("k", 2100, batch(4));
        assert!(matches.is_empty());
    }

    #[test]
    fn multiple_right_events_match_single_left() {
        let mut join = PerKeyIntervalJoin::new(spec_with_limit(1000));
        join.push_right("k", 1000, batch(1));
        join.push_right("k", 1050, batch(2));
        join.push_right("k", 1100, batch(3));
        let matches = join.push_left("k", 1050, batch(4));
        assert_eq!(matches.len(), 3);
    }

    #[test]
    fn zero_bounds_exact_timestamp_match() {
        let mut join = PerKeyIntervalJoin::new(IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 0,
            key_column: "id".into(),
            max_buffer_per_side: 1000,
        });
        join.push_left("k", 1000, batch(1));
        let matches = join.push_right("k", 1000, batch(2));
        assert_eq!(matches.len(), 1);
        assert!(join.push_right("k", 1001, batch(3)).is_empty());
    }

    #[test]
    fn evict_before_empty_state_no_panic() {
        let mut join = PerKeyIntervalJoin::new(spec_with_limit(1000));
        join.evict_before(5000);
    }

    #[test]
    fn large_asymmetric_bounds() {
        let mut join = PerKeyIntervalJoin::new(IntervalJoinSpec {
            lower_bound_ms: -1000,
            upper_bound_ms: 100,
            key_column: "id".into(),
            max_buffer_per_side: 1000,
        });
        join.push_left("k", 1000, batch(1));
        let matches = join.push_right("k", 100, batch(2));
        assert_eq!(matches.len(), 1);
        let matches = join.push_right("k", 2100, batch(3));
        assert!(matches.is_empty());
    }

    #[test]
    fn per_key_join_separates_different_keys() {
        let mut join = PerKeyIntervalJoin::new(spec_with_limit(1000));

        assert!(join.push_left("a", 1000, batch(1)).is_empty());
        let matches = join.push_right("a", 1050, batch(2));
        assert_eq!(matches.len(), 1, "same-key events must match");

        join.push_left("b", 1000, batch(3));
        let matches = join.push_right("b", 1050, batch(4));
        assert_eq!(matches.len(), 1);

        assert_eq!(join.active_key_count(), 2);
    }

    #[test]
    fn per_key_join_keys_are_isolated() {
        let mut join = PerKeyIntervalJoin::new(spec_with_limit(1000));

        join.push_left("a", 1000, batch(10));
        join.push_right("b", 1000, batch(20));

        let a_matches = join.push_right("a", 1050, batch(11));
        let b_matches = join.push_left("b", 1050, batch(21));

        assert_eq!(a_matches.len(), 1);
        assert_eq!(b_matches.len(), 1);
    }

    #[test]
    fn per_key_evict_cleans_all_keys() {
        let mut join = PerKeyIntervalJoin::new(IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 50,
            key_column: "id".into(),
            max_buffer_per_side: 1000,
        });

        join.push_left("a", 100, batch(1));
        join.push_right("b", 200, batch(2));

        assert_eq!(join.active_key_count(), 2);

        join.evict_before(300);

        assert_eq!(join.active_key_count(), 0);
    }

    #[test]
    fn per_key_join_empty_returns_no_matches() {
        let mut join = PerKeyIntervalJoin::new(spec_with_limit(1000));
        let matches = join.push_left("z", 1000, batch(1));
        assert!(matches.is_empty());
        assert_eq!(join.active_key_count(), 1);
    }

    #[test]
    fn buffer_limit_drops_oldest_events_when_exceeded() {
        // max_buffer_per_side = 2: after pushing 3 left events, the oldest is dropped.
        let mut join = PerKeyIntervalJoin::new(spec_with_limit(2));
        join.push_left("k", 1000, batch(1)); // oldest
        join.push_left("k", 1010, batch(2));
        join.push_left("k", 1020, batch(3)); // this pushes oldest out

        // Right event at 1005 would match left events at 1000, 1010, 1020 if all present.
        // With buffer limit 2 the event at 1000 is evicted; only 1010 and 1020 remain.
        let matches = join.push_right("k", 1005, batch(99));
        assert_eq!(
            matches.len(),
            2,
            "with buffer limit 2 only 2 left events should remain to match"
        );
    }
}
