//! Stream-stream interval join (R16 S3.2) with per-key partitioning.

use std::collections::{HashMap, VecDeque};

use arrow::record_batch::RecordBatch;

/// Interval join bounds relative to the opposite stream's event time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntervalJoinSpec {
    pub lower_bound_ms: i64,
    pub upper_bound_ms: i64,
    /// Column used as the join key for per-key partitioning.
    pub key_column: String,
}

/// Buffered event with event-time for interval matching.
#[derive(Debug, Clone)]
struct BufferedEvent {
    event_time_ms: i64,
    batch: RecordBatch,
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
        batch: RecordBatch,
        lower_bound_ms: i64,
        upper_bound_ms: i64,
    ) -> Vec<(RecordBatch, RecordBatch)> {
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
                    matches.push((batch.clone(), other.batch.clone()));
                } else {
                    matches.push((other.batch.clone(), batch.clone()));
                }
            }
        }
        this_buf.push_back(BufferedEvent { event_time_ms, batch });
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
    pub fn push_left(
        &mut self,
        key: &str,
        event_time_ms: i64,
        batch: RecordBatch,
    ) -> Vec<(RecordBatch, RecordBatch)> {
        let state = self.states.entry(key.to_owned()).or_insert_with(IntervalJoinBuffers::new);
        state.push_side(true, event_time_ms, batch, self.spec.lower_bound_ms, self.spec.upper_bound_ms)
    }

    /// Push an event onto the right side for `key`.
    pub fn push_right(
        &mut self,
        key: &str,
        event_time_ms: i64,
        batch: RecordBatch,
    ) -> Vec<(RecordBatch, RecordBatch)> {
        let state = self.states.entry(key.to_owned()).or_insert_with(IntervalJoinBuffers::new);
        state.push_side(false, event_time_ms, batch, self.spec.lower_bound_ms, self.spec.upper_bound_ms)
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

/// Global interval join state — legacy wrapper for backward-compatible APIs.
/// Prefer [`PerKeyIntervalJoin`] for correct keyed-stream behavior.
#[derive(Debug)]
pub struct IntervalJoinState {
    left: VecDeque<BufferedEvent>,
    right: VecDeque<BufferedEvent>,
    spec: IntervalJoinSpec,
}

impl IntervalJoinState {
    pub fn new(spec: IntervalJoinSpec) -> Self {
        Self {
            left: VecDeque::new(),
            right: VecDeque::new(),
            spec,
        }
    }

    pub fn push_left(
        &mut self,
        event_time_ms: i64,
        batch: RecordBatch,
    ) -> Vec<(RecordBatch, RecordBatch)> {
        push_global_side(&mut self.left, &mut self.right, true, event_time_ms, batch, &self.spec)
    }

    pub fn push_right(
        &mut self,
        event_time_ms: i64,
        batch: RecordBatch,
    ) -> Vec<(RecordBatch, RecordBatch)> {
        push_global_side(&mut self.right, &mut self.left, false, event_time_ms, batch, &self.spec)
    }

    pub fn evict_before(&mut self, watermark_ms: i64) {
        let bound = self.spec.upper_bound_ms.unsigned_abs()
            .max(self.spec.lower_bound_ms.unsigned_abs());
        let horizon = watermark_ms.saturating_sub(bound as i64);
        self.left.retain(|e| e.event_time_ms >= horizon);
        self.right.retain(|e| e.event_time_ms >= horizon);
    }
}

fn push_global_side(
    this_buf: &mut VecDeque<BufferedEvent>,
    other_buf: &mut VecDeque<BufferedEvent>,
    is_left: bool,
    event_time_ms: i64,
    batch: RecordBatch,
    spec: &IntervalJoinSpec,
) -> Vec<(RecordBatch, RecordBatch)> {
    let mut matches = Vec::with_capacity(other_buf.len());
    for other in other_buf.iter() {
        let delta = if is_left {
            other.event_time_ms - event_time_ms
        } else {
            event_time_ms - other.event_time_ms
        };
        if delta >= spec.lower_bound_ms && delta <= spec.upper_bound_ms {
            if is_left {
                matches.push((batch.clone(), other.batch.clone()));
            } else {
                matches.push((other.batch.clone(), batch.clone()));
            }
        }
    }
    this_buf.push_back(BufferedEvent { event_time_ms, batch });
    matches
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(v: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![v]))],
        )
        .unwrap()
    }

    // ── Global join (backward compat) tests ─────────────────────────────

    #[test]
    fn overlapping_interval_emits_match() {
        let spec = IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
            key_column: "id".into(),
        };
        let mut state = IntervalJoinState::new(spec);
        assert!(state.push_left(1000, batch(1)).is_empty());
        let matches = state.push_right(1050, batch(2));
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn non_overlapping_interval_drops() {
        let spec = IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 50,
            key_column: "id".into(),
        };
        let mut state = IntervalJoinState::new(spec);
        state.push_left(1000, batch(1));
        assert!(state.push_right(2000, batch(2)).is_empty());
    }

    #[test]
    fn evict_before_removes_old_events() {
        let spec = IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
            key_column: "id".into(),
        };
        let mut state = IntervalJoinState::new(spec);
        state.push_left(1000, batch(1));
        state.push_left(2000, batch(2));
        state.push_right(1500, batch(3));
        state.evict_before(2100);
        let matches = state.push_left(2100, batch(4));
        assert!(matches.is_empty());
    }

    #[test]
    fn multiple_right_events_match_single_left() {
        let spec = IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
            key_column: "id".into(),
        };
        let mut state = IntervalJoinState::new(spec);
        state.push_right(1000, batch(1));
        state.push_right(1050, batch(2));
        state.push_right(1100, batch(3));
        let matches = state.push_left(1050, batch(4));
        assert_eq!(matches.len(), 3);
    }

    #[test]
    fn zero_bounds_exact_timestamp_match() {
        let spec = IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 0,
            key_column: "id".into(),
        };
        let mut state = IntervalJoinState::new(spec);
        state.push_left(1000, batch(1));
        let matches = state.push_right(1000, batch(2));
        assert_eq!(matches.len(), 1);
        assert!(state.push_right(1001, batch(3)).is_empty());
    }

    #[test]
    fn evict_before_empty_state_no_panic() {
        let spec = IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
            key_column: "id".into(),
        };
        let mut state = IntervalJoinState::new(spec);
        state.evict_before(5000);
    }

    #[test]
    fn large_asymmetric_bounds() {
        let spec = IntervalJoinSpec {
            lower_bound_ms: -1000,
            upper_bound_ms: 100,
            key_column: "id".into(),
        };
        let mut state = IntervalJoinState::new(spec);
        state.push_left(1000, batch(1));
        let matches = state.push_right(100, batch(2));
        assert_eq!(matches.len(), 1);
        let matches = state.push_right(2100, batch(3));
        assert!(matches.is_empty());
    }

    // ── Per-key join tests ─────────────────────────────────────────────

    #[test]
    fn per_key_join_separates_different_keys() {
        let spec = IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
            key_column: "id".into(),
        };
        let mut join = PerKeyIntervalJoin::new(spec);

        // Key "a" has matching events.
        assert!(join.push_left("a", 1000, batch(1)).is_empty());
        let matches = join.push_right("a", 1050, batch(2));
        assert_eq!(matches.len(), 1, "same-key events must match");

        // Key "b" has overlapping times but different key — must NOT match.
        join.push_left("b", 1000, batch(3));
        let matches = join.push_right("b", 1050, batch(4));
        assert_eq!(matches.len(), 1);

        assert_eq!(join.active_key_count(), 2);
    }

    #[test]
    fn per_key_join_keys_are_isolated() {
        let spec = IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
            key_column: "user_id".into(),
        };
        let mut join = PerKeyIntervalJoin::new(spec);

        // Key "a": left at 1000, no right → no match
        join.push_left("a", 1000, batch(10));

        // Key "b": right at 1000, no left → no match
        join.push_right("b", 1000, batch(20));

        // Push a matching pair for each key.
        let a_matches = join.push_right("a", 1050, batch(11));
        let b_matches = join.push_left("b", 1050, batch(21));

        assert_eq!(a_matches.len(), 1);
        assert_eq!(b_matches.len(), 1);
    }

    #[test]
    fn per_key_evict_cleans_all_keys() {
        let spec = IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 50,
            key_column: "id".into(),
        };
        let mut join = PerKeyIntervalJoin::new(spec);

        join.push_left("a", 100, batch(1));
        join.push_right("b", 200, batch(2));

        assert_eq!(join.active_key_count(), 2);

        // Evict events before watermark 300 with bound 50 → horizon = 250
        // all events at 100 and 200 are < 250 → evicted
        join.evict_before(300);

        // Both keys should be cleaned up since their buffers are empty.
        assert_eq!(join.active_key_count(), 0);
    }

    #[test]
    fn per_key_join_empty_returns_no_matches() {
        let spec = IntervalJoinSpec {
            lower_bound_ms: -100,
            upper_bound_ms: 100,
            key_column: "id".into(),
        };
        let mut join = PerKeyIntervalJoin::new(spec);
        let matches = join.push_left("z", 1000, batch(1));
        assert!(matches.is_empty());
        assert_eq!(join.active_key_count(), 1);
    }
}
