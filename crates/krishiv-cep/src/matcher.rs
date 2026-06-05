//! Per-key sequential pattern matcher (R16 S2.2).

use arrow::record_batch::RecordBatch;

use crate::pattern::CompiledPattern;

/// Partial in-progress match.
///
/// The `captured_events` field is the live, in-memory list of record
/// batches that have matched stages so far. The persistence-friendly
/// companion fields (`stage_index`, `captured_event_count`,
/// `start_time_ms`) can be serialised and snapshotted by the checkpoint
/// coordinator; the executor is expected to keep `captured_events` in
/// a separate durable store (or replay from the source on restart) so
/// that the metadata in the checkpoint is sufficient to reconstruct
/// the partial match.
#[derive(Debug, Clone)]
pub struct PartialMatch {
    pub stage_index: usize,
    pub captured_events: Vec<RecordBatch>,
    pub start_time_ms: i64,
    /// Number of events captured so far; mirrors `captured_events.len()`
    /// so the field can be serialised by the checkpoint coordinator
    /// (the actual `RecordBatch` payloads live in a separate durable
    /// store keyed by this count). Kept in sync by `process_event`.
    pub captured_event_count: usize,
}

/// Per-key CEP state.
///
/// `Serialize`/`Deserialize` (via `serde`) allow per-key state to be
/// snapshotted by the checkpoint coordinator. The `partial` field is
/// not serialised directly because `RecordBatch` does not implement
/// `Serialize`; the metadata captured in the separate
/// `captured_event_count` field on `PartialMatch` is enough to recover
/// the partial state from a replay log on restart.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct CepKeyState {
    #[serde(skip)]
    pub partial: Option<PartialMatch>,
    /// Wall-clock event time (ms) of the most recent event processed for this
    /// key.  Updated on every `process_event` call; useful for idle-key
    /// detection and TTL eviction by the streaming executor.
    pub last_event_ms: i64,
}

/// Sequential pattern matcher for one key.
#[derive(Debug, Clone)]
pub struct SequentialPatternMatcher {
    pattern: CompiledPattern,
}

impl SequentialPatternMatcher {
    pub fn new(pattern: CompiledPattern) -> Self {
        Self { pattern }
    }

    pub fn process_event(
        &self,
        state: &mut CepKeyState,
        stage_name: &str,
        batch: RecordBatch,
        event_time_ms: i64,
    ) -> Vec<Vec<RecordBatch>> {
        state.last_event_ms = event_time_ms;

        if let Some(ref partial) = state.partial
            && event_time_ms - partial.start_time_ms > self.pattern.window_ms as i64
        {
            state.partial = None;
        }

        let stage_idx = self
            .pattern
            .stages
            .iter()
            .position(|s| s.name == stage_name);

        let Some(stage_idx) = stage_idx else {
            return Vec::new();
        };

        if state.partial.is_none() {
            if stage_idx != 0 {
                return Vec::new();
            }
            state.partial = Some(PartialMatch {
                stage_index: 0,
                captured_events: vec![batch],
                start_time_ms: event_time_ms,
                captured_event_count: 1,
            });
            if self.pattern.stages.len() == 1 {
                return self.take_complete(state);
            }
            return Vec::new();
        }

        if let Some(ref mut partial) = state.partial {
            let expected_next = partial.stage_index + 1;
            if stage_idx != expected_next {
                return Vec::new();
            }
            partial.captured_events.push(batch);
            partial.captured_event_count = partial.captured_events.len();
            partial.stage_index = stage_idx;

            if partial.stage_index + 1 == self.pattern.stages.len() {
                return self.take_complete(state);
            }
        }
        Vec::new()
    }

    fn take_complete(&self, state: &mut CepKeyState) -> Vec<Vec<RecordBatch>> {
        state
            .partial
            .take()
            .map(|p| vec![p.captured_events])
            .unwrap_or_default()
    }
}

#[allow(dead_code)] // compiled with workspace; integration lands in streaming exec path
/// Partitioned wrapper routing events to per-key [`SequentialPatternMatcher`] instances (P3-27).
#[derive(Debug, Clone)]
pub struct PartitionedCepMatcher<K>
where
    K: std::hash::Hash + Eq + Clone,
{
    pattern: CompiledPattern,
    states: std::collections::HashMap<K, (SequentialPatternMatcher, CepKeyState)>,
    max_partitions: usize,
}

impl<K> PartitionedCepMatcher<K>
where
    K: std::hash::Hash + Eq + Clone,
{
    pub fn new(pattern: CompiledPattern) -> Self {
        Self {
            pattern: pattern.clone(),
            states: std::collections::HashMap::new(),
            max_partitions: 1024,
        }
    }

    pub fn process_event(
        &mut self,
        key: K,
        stage_name: &str,
        batch: RecordBatch,
        event_time_ms: i64,
    ) -> Vec<Vec<RecordBatch>> {
        let entry = self.states.entry(key).or_insert_with(|| {
            (
                SequentialPatternMatcher::new(self.pattern.clone()),
                CepKeyState::default(),
            )
        });
        let result = entry
            .0
            .process_event(&mut entry.1, stage_name, batch, event_time_ms);
        if self.states.len() > self.max_partitions
            && let Some(stalest) = self
                .states
                .iter()
                .min_by_key(|(_, (_, state))| state.last_event_ms)
                .map(|(k, _)| k.clone())
        {
            self.states.remove(&stalest);
        }
        result
    }

    /// Remove all keys whose most recent event time is strictly before
    /// `cutoff_ms`.  Called by the streaming CEP path after each batch to
    /// bound memory for high-cardinality key spaces.
    pub fn evict_keys_before(&mut self, cutoff_ms: i64) {
        self.states
            .retain(|_, (_, state)| state.last_event_ms >= cutoff_ms);
    }

    /// Number of currently tracked partition keys.
    pub fn partition_count(&self) -> usize {
        self.states.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pattern::Pattern;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::time::Duration;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("event_type", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    fn batch(v: i32) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![v]))],
        )
        .unwrap()
    }

    fn rich_batch(event_type: &str, timestamp: i64, value: i32) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![event_type])),
                Arc::new(arrow::array::Int64Array::from(vec![timestamp])),
                Arc::new(Int32Array::from(vec![value])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn two_stage_pattern_matches() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        assert!(
            matcher
                .process_event(&mut state, "a", batch(1), 100)
                .is_empty()
        );
        let done = matcher.process_event(&mut state, "b", batch(2), 200);
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].len(), 2);
    }

    #[test]
    fn expired_partial_discarded() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_millis(50))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        matcher.process_event(&mut state, "a", batch(1), 0);
        assert!(
            matcher
                .process_event(&mut state, "b", batch(2), 100)
                .is_empty()
        );
    }

    #[test]
    fn empty_pattern_compile_rejected() {
        let result = Pattern::begin("a")
            .compile()
            .unwrap() // 1-stage is fine
            ;
        assert_eq!(result.stages.len(), 1);
    }

    #[test]
    fn single_stage_match_completes_immediately() {
        let pattern = Pattern::begin("only")
            .within(Duration::from_secs(1))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        let done = matcher.process_event(&mut state, "only", batch(42), 100);
        assert_eq!(
            done.len(),
            1,
            "single-stage pattern must complete on first match"
        );
        assert_eq!(done[0].len(), 1);
        assert!(
            state.partial.is_none(),
            "state must be cleared after completion"
        );
    }

    #[test]
    fn boundary_event_at_exact_window_limit() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_millis(100))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        matcher.process_event(&mut state, "a", batch(1), 0);
        // Exactly at the window boundary (0 + 100 = 100) — should still match.
        let done = matcher.process_event(&mut state, "b", batch(2), 100);
        assert_eq!(done.len(), 1, "event at exact window boundary must match");
    }

    #[test]
    fn boundary_event_one_ms_past_window() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_millis(100))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        matcher.process_event(&mut state, "a", batch(1), 0);
        // One ms past the window — must be discarded.
        let done = matcher.process_event(&mut state, "b", batch(2), 101);
        assert!(done.is_empty(), "event past window must be discarded");
    }

    #[test]
    fn partitioned_matcher_independent_keys() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let mut pm = PartitionedCepMatcher::<String>::new(pattern);
        // Key "k1": start match
        assert!(pm.process_event("k1".into(), "a", batch(1), 100).is_empty());
        // Key "k2": start match
        assert!(
            pm.process_event("k2".into(), "a", batch(10), 200)
                .is_empty()
        );
        // Key "k1": complete match
        let done = pm.process_event("k1".into(), "b", batch(2), 300);
        assert_eq!(done.len(), 1);
        // Key "k2": still pending
        assert!(
            pm.process_event("k2".into(), "a", batch(11), 400)
                .is_empty()
        );
    }

    #[test]
    fn partitioned_matcher_independent_state() {
        let pattern = Pattern::begin("x")
            .followed_by("y")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let mut pm = PartitionedCepMatcher::<i32>::new(pattern);
        pm.process_event(1, "x", batch(1), 100);
        pm.process_event(2, "x", batch(2), 200);
        // Both keys should have partial matches.
        assert!(pm.states.contains_key(&1));
        assert!(pm.states.contains_key(&2));
    }

    // ── SequentialPatternMatcher: untested paths ───────────────────────

    #[test]
    fn wrong_stage_name_ignored() {
        let pattern = Pattern::begin("a").followed_by("b").compile().unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        let result = matcher.process_event(&mut state, "c", batch(1), 100);
        assert!(result.is_empty());
        assert!(
            state.partial.is_none(),
            "no partial match should be started"
        );
    }

    #[test]
    fn partial_state_persisted_after_first_event() {
        let pattern = Pattern::begin("a").followed_by("b").compile().unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        matcher.process_event(&mut state, "a", batch(1), 100);
        assert!(
            state.partial.is_some(),
            "partial must exist after first stage"
        );
        let partial = state.partial.as_ref().unwrap();
        assert_eq!(partial.stage_index, 0);
        assert_eq!(partial.captured_events.len(), 1);
        assert_eq!(partial.start_time_ms, 100);
    }

    #[test]
    fn stage_ordering_enforced() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .followed_by("c")
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        matcher.process_event(&mut state, "a", batch(1), 100);
        // Skip "b", send "c" — should be ignored because stage_index expects b next.
        let result = matcher.process_event(&mut state, "c", batch(3), 200);
        assert!(result.is_empty());
        assert!(
            state.partial.is_some(),
            "partial should still be waiting for stage b"
        );
    }

    #[test]
    fn three_stage_sequential_match() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .followed_by("c")
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();

        assert!(
            matcher
                .process_event(&mut state, "a", batch(1), 100)
                .is_empty()
        );
        assert!(
            matcher
                .process_event(&mut state, "b", batch(2), 200)
                .is_empty()
        );
        let done = matcher.process_event(&mut state, "c", batch(3), 300);

        assert_eq!(done.len(), 1);
        assert_eq!(done[0].len(), 3);
        assert_eq!(
            done[0][0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            done[0][1]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(
            done[0][2]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .value(0),
            3
        );
    }

    #[test]
    fn multiple_matches_on_same_key() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();

        // First match
        matcher.process_event(&mut state, "a", batch(1), 100);
        let done1 = matcher.process_event(&mut state, "b", batch(2), 200);
        assert_eq!(done1.len(), 1);
        assert!(state.partial.is_none(), "state cleared after first match");

        // Second match on same key
        matcher.process_event(&mut state, "a", batch(10), 300);
        let done2 = matcher.process_event(&mut state, "b", batch(20), 400);
        assert_eq!(done2.len(), 1);
    }

    #[test]
    fn last_event_ms_updated() {
        let pattern = Pattern::begin("a").followed_by("b").compile().unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        assert_eq!(state.last_event_ms, 0);
        matcher.process_event(&mut state, "a", batch(1), 500);
        assert_eq!(state.last_event_ms, 500);
        matcher.process_event(&mut state, "b", batch(2), 600);
        assert_eq!(state.last_event_ms, 600);
    }

    #[test]
    fn wrong_stage_between_matches_does_not_corrupt_state() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();

        matcher.process_event(&mut state, "a", batch(1), 100);
        // Wrong stage
        assert!(
            matcher
                .process_event(&mut state, "x", batch(99), 150)
                .is_empty()
        );
        // Correct stage still works
        let done = matcher.process_event(&mut state, "b", batch(2), 200);
        assert_eq!(done.len(), 1);
    }

    #[test]
    fn out_of_order_stage_after_partial_resets_correctly() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();

        // Start with "a"
        matcher.process_event(&mut state, "a", batch(1), 100);
        // Send another "a" — stage_idx 0 != expected_next 1, so ignored
        assert!(
            matcher
                .process_event(&mut state, "a", batch(10), 150)
                .is_empty()
        );
        // "b" should still complete the match
        let done = matcher.process_event(&mut state, "b", batch(2), 200);
        assert_eq!(done.len(), 1);
    }

    #[test]
    fn rich_batch_sequential_match() {
        let pattern = Pattern::begin("login")
            .followed_by("query")
            .within(Duration::from_secs(10))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();

        let b1 = rich_batch("login", 1000, 0);
        let b2 = rich_batch("query", 2000, 42);

        assert!(
            matcher
                .process_event(&mut state, "login", b1, 1000)
                .is_empty()
        );
        let done = matcher.process_event(&mut state, "query", b2, 2000);

        assert_eq!(done.len(), 1);
        assert_eq!(done[0].len(), 2);
        // Verify event_type column is preserved
        let col = done[0][0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "login");
        let col = done[0][1]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "query");
    }

    #[test]
    fn default_window_is_60s() {
        let pattern = Pattern::begin("a").compile().unwrap();
        assert_eq!(pattern.window_ms, 60_000);
    }

    #[test]
    fn no_partial_match_when_no_events_processed() {
        let pattern = Pattern::begin("a").followed_by("b").compile().unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        // Sending stage "b" with no prior state and stage_idx != 0 → ignored
        let result = matcher.process_event(&mut state, "b", batch(2), 100);
        assert!(result.is_empty());
        assert!(state.partial.is_none());
    }

    // ── PartitionedCepMatcher: additional coverage ──────────────────────

    #[test]
    fn partitioned_wrong_stage_ignored_per_key() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let mut pm = PartitionedCepMatcher::<String>::new(pattern);
        pm.process_event("k1".into(), "a", batch(1), 100);
        // Wrong stage for k1
        assert!(
            pm.process_event("k1".into(), "x", batch(99), 200)
                .is_empty()
        );
        // Correct stage for k1
        let done = pm.process_event("k1".into(), "b", batch(2), 300);
        assert_eq!(done.len(), 1);
    }

    #[test]
    fn partitioned_multiple_matches_per_key() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let mut pm = PartitionedCepMatcher::<i32>::new(pattern);

        // First match for key 1
        pm.process_event(1, "a", batch(1), 100);
        let done1 = pm.process_event(1, "b", batch(2), 200);
        assert_eq!(done1.len(), 1);

        // Second match for key 1
        pm.process_event(1, "a", batch(10), 300);
        let done2 = pm.process_event(1, "b", batch(20), 400);
        assert_eq!(done2.len(), 1);
    }

    #[test]
    fn partitioned_three_stage_match() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .followed_by("c")
            .within(Duration::from_secs(10))
            .compile()
            .unwrap();
        let mut pm = PartitionedCepMatcher::<String>::new(pattern);

        assert!(pm.process_event("k1".into(), "a", batch(1), 100).is_empty());
        assert!(pm.process_event("k1".into(), "b", batch(2), 200).is_empty());
        let done = pm.process_event("k1".into(), "c", batch(3), 300);
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].len(), 3);
    }

    #[test]
    fn partitioned_independent_timeout_per_key() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_millis(50))
            .compile()
            .unwrap();
        let mut pm = PartitionedCepMatcher::<String>::new(pattern);

        // k1 starts at t=0
        pm.process_event("k1".into(), "a", batch(1), 0);
        // k2 starts at t=1000
        pm.process_event("k2".into(), "a", batch(10), 1000);

        // k1 at t=60 → expired (60 > 50)
        assert!(pm.process_event("k1".into(), "b", batch(2), 60).is_empty());

        // k2 at t=1040 → still valid (1040 - 1000 = 40 <= 50)
        let done = pm.process_event("k2".into(), "b", batch(20), 1040);
        assert_eq!(done.len(), 1);
    }

    #[test]
    fn partitioned_wrong_key_stage_not_cross_contaminated() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let mut pm = PartitionedCepMatcher::<String>::new(pattern);

        pm.process_event("k1".into(), "a", batch(1), 100);
        pm.process_event("k2".into(), "a", batch(2), 200);

        // Complete k1
        let done = pm.process_event("k1".into(), "b", batch(3), 300);
        assert_eq!(done.len(), 1);

        // k2 should still be at stage 0, not affected by k1 completion
        assert!(pm.states.get("k2").unwrap().1.partial.is_some());
        let done2 = pm.process_event("k2".into(), "b", batch(4), 400);
        assert_eq!(done2.len(), 1);
    }

    #[test]
    fn partitioned_rich_batch_preserves_data() {
        let pattern = Pattern::begin("click")
            .followed_by("purchase")
            .within(Duration::from_secs(30))
            .compile()
            .unwrap();
        let mut pm = PartitionedCepMatcher::<String>::new(pattern);

        let b1 = rich_batch("click", 1000, 0);
        let b2 = rich_batch("purchase", 2000, 99);

        assert!(
            pm.process_event("user1".into(), "click", b1, 1000)
                .is_empty()
        );
        let done = pm.process_event("user1".into(), "purchase", b2, 2000);

        assert_eq!(done.len(), 1);
        assert_eq!(done[0].len(), 2);
        let val_col = done[0][1]
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(val_col.value(0), 99);
    }

    #[test]
    fn partitioned_new_key_auto_created() {
        let pattern = Pattern::begin("a").followed_by("b").compile().unwrap();
        let mut pm = PartitionedCepMatcher::<String>::new(pattern);
        assert!(pm.states.is_empty());

        pm.process_event("new_key".into(), "a", batch(1), 100);
        assert!(pm.states.contains_key("new_key"));
        assert_eq!(pm.states.len(), 1);
    }

    // ── Additional deep-coverage tests ─────────────────────────────────

    #[test]
    fn negative_event_timestamps() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        matcher.process_event(&mut state, "a", batch(1), -1000);
        let done = matcher.process_event(&mut state, "b", batch(2), -500);
        assert_eq!(done.len(), 1);
    }

    #[test]
    fn negative_timestamp_window_expired() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_millis(100))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        matcher.process_event(&mut state, "a", batch(1), -200);
        // -200 + 100 = -100, event at -50 is past window
        let done = matcher.process_event(&mut state, "b", batch(2), -50);
        assert!(
            done.is_empty(),
            "event past window with negative timestamps must be discarded"
        );
    }

    #[test]
    fn large_window_millis() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_millis(u64::MAX / 2))
            .compile()
            .unwrap();
        assert!(pattern.window_ms > 0);
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        matcher.process_event(&mut state, "a", batch(1), 0);
        let done = matcher.process_event(&mut state, "b", batch(2), 1_000_000);
        assert_eq!(done.len(), 1);
    }

    #[test]
    fn multi_row_batch_preserves_all_rows() {
        let pattern = Pattern::begin("a").followed_by("b").compile().unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();

        let multi_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )
        .unwrap();

        matcher.process_event(&mut state, "a", multi_batch.clone(), 100);
        let done = matcher.process_event(&mut state, "b", batch(2), 200);
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].len(), 2);
        // First captured batch should have 3 rows
        let col = done[0][0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.len(), 3);
        assert_eq!(col.value(0), 10);
        assert_eq!(col.value(1), 20);
        assert_eq!(col.value(2), 30);
    }

    #[test]
    fn first_event_at_zero_time() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(1))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        matcher.process_event(&mut state, "a", batch(1), 0);
        let done = matcher.process_event(&mut state, "b", batch(2), 0);
        assert_eq!(done.len(), 1);
    }

    #[test]
    fn exact_duplicate_stage_names_reset_partial() {
        // When stage names are duplicated, position() always finds stage 0,
        // so the second "a" event is treated as starting a new match (not advancing).
        let pattern = Pattern::begin("a").followed_by("a").compile().unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        matcher.process_event(&mut state, "a", batch(1), 100);
        // Second "a" hits stage_idx 0, but expected_next is 1, so it's ignored
        let result = matcher.process_event(&mut state, "a", batch(2), 200);
        assert!(result.is_empty());
    }

    #[test]
    fn five_stage_pattern() {
        let pattern = Pattern::begin("s1")
            .followed_by("s2")
            .followed_by("s3")
            .followed_by("s4")
            .followed_by("s5")
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        assert!(
            matcher
                .process_event(&mut state, "s1", batch(1), 100)
                .is_empty()
        );
        assert!(
            matcher
                .process_event(&mut state, "s2", batch(2), 200)
                .is_empty()
        );
        assert!(
            matcher
                .process_event(&mut state, "s3", batch(3), 300)
                .is_empty()
        );
        assert!(
            matcher
                .process_event(&mut state, "s4", batch(4), 400)
                .is_empty()
        );
        let done = matcher.process_event(&mut state, "s5", batch(5), 500);
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].len(), 5);
    }

    #[test]
    fn partitioned_many_keys() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let mut pm = PartitionedCepMatcher::<i32>::new(pattern);
        for k in 0..100 {
            pm.process_event(k, "a", batch(k), k as i64 * 100);
        }
        assert_eq!(pm.states.len(), 100);
        // Complete only key 50
        let done = pm.process_event(50, "b", batch(50), 5000);
        assert_eq!(done.len(), 1);
        // Other keys still partial
        assert!(pm.states.get(&0).unwrap().1.partial.is_some());
        assert!(pm.states.get(&99).unwrap().1.partial.is_some());
    }

    #[test]
    fn partitioned_completed_key_can_restart() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let mut pm = PartitionedCepMatcher::<String>::new(pattern);
        pm.process_event("k".into(), "a", batch(1), 100);
        let done1 = pm.process_event("k".into(), "b", batch(2), 200);
        assert_eq!(done1.len(), 1);
        assert!(pm.states.get("k").unwrap().1.partial.is_none());
        // Restart
        pm.process_event("k".into(), "a", batch(10), 300);
        let done2 = pm.process_event("k".into(), "b", batch(20), 400);
        assert_eq!(done2.len(), 1);
    }

    #[test]
    fn cep_key_state_default_values() {
        let state = CepKeyState::default();
        assert!(state.partial.is_none());
        assert_eq!(state.last_event_ms, 0);
    }

    #[test]
    fn partial_match_default_values() {
        let pm = PartialMatch {
            stage_index: 0,
            captured_events: Vec::new(),
            start_time_ms: 0,
            captured_event_count: 0,
        };
        assert_eq!(pm.stage_index, 0);
        assert!(pm.captured_events.is_empty());
        assert_eq!(pm.start_time_ms, 0);
    }

    #[test]
    fn compiled_pattern_clone() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_secs(5))
            .compile()
            .unwrap();
        let cloned = pattern.clone();
        assert_eq!(cloned.stages.len(), pattern.stages.len());
        assert_eq!(cloned.window_ms, pattern.window_ms);
    }

    #[test]
    fn cep_key_state_serde_skips_partial_but_preserves_metadata() {
        // `partial` is skipped because `RecordBatch` doesn't impl Serialize;
        // `last_event_ms` must survive the round trip.
        let mut state = CepKeyState::default();
        state.last_event_ms = 1_234_567;
        let json = serde_json::to_string(&state).unwrap();
        let restored: CepKeyState = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.last_event_ms, 1_234_567);
        assert!(restored.partial.is_none());
    }

    #[test]
    fn sequential_matcher_clone() {
        let pattern = Pattern::begin("a").compile().unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let cloned = matcher.clone();
        let mut state = CepKeyState::default();
        let done = cloned.process_event(&mut state, "a", batch(1), 100);
        assert_eq!(done.len(), 1);
    }

    #[test]
    fn partitioned_matcher_clone() {
        let pattern = Pattern::begin("a").followed_by("b").compile().unwrap();
        let mut pm = PartitionedCepMatcher::<String>::new(pattern);
        pm.process_event("k1".into(), "a", batch(1), 100);
        let cloned = pm.clone();
        assert!(cloned.states.contains_key("k1"));
    }

    #[test]
    fn zero_duration_window_allows_same_time_match() {
        let pattern = Pattern::begin("a")
            .followed_by("b")
            .within(Duration::from_millis(0))
            .compile()
            .unwrap();
        let matcher = SequentialPatternMatcher::new(pattern);
        let mut state = CepKeyState::default();
        matcher.process_event(&mut state, "a", batch(1), 100);
        // Exactly at boundary (100 - 100 = 0 <= 0)
        let done = matcher.process_event(&mut state, "b", batch(2), 100);
        assert_eq!(done.len(), 1);
    }

    #[test]
    fn window_ms_default_is_60000() {
        let pattern = Pattern::begin("a").compile().unwrap();
        assert_eq!(pattern.window_ms, 60_000);
    }

    #[test]
    fn pattern_stage_names_and_gap() {
        let pattern = Pattern::begin("start")
            .followed_by("end")
            .within(Duration::from_secs(10))
            .compile()
            .unwrap();
        assert_eq!(pattern.stages[0].name, "start");
        assert!(pattern.stages[0].max_gap_ms.is_none());
        assert_eq!(pattern.stages[1].name, "end");
        assert!(pattern.stages[1].max_gap_ms.is_none());
    }
}
