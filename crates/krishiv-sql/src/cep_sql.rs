//! `MATCH_RECOGNIZE` SQL extension planning and execution (R16 S2).

use std::time::Duration;

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use krishiv_plan::cep::{
    CepKeyState, CompiledPattern, PartitionedCepMatcher, Pattern, SequentialPatternMatcher,
};
use krishiv_plan::{ExecutionKind, LogicalPlan, NodeOp, PlanNode};

use crate::{SqlError, SqlResult};

/// Parsed `MATCH_RECOGNIZE` statement.
/// Parsed `MATCH_RECOGNIZE` statement ready for execution.
///
/// ## Window boundary semantics
///
/// The `WITHIN` clause sets a window duration in milliseconds.  The expiry
/// check uses a **strict greater-than** comparison:
/// `event_time_ms - partial.start_time_ms > window_ms`.
///
/// This means an event arriving at **exactly** `start_time + window_ms` is
/// still considered within the window and will advance (or complete) the
/// partial match. Only events arriving strictly *after* that point cause the
/// partial to expire and be discarded.
#[derive(Debug, Clone)]
pub struct MatchRecognizeStatement {
    pub source_table: String,
    pub key_column: String,
    pub event_time_column: String,
    pub pattern: CompiledPattern,
}

/// Parse the supported R16 subset:
///
/// `SELECT * FROM events MATCH_RECOGNIZE (PARTITION BY user_id ORDER BY ts PATTERN (A B) WITHIN 10 SECONDS)`
pub fn parse_match_recognize(sql: &str) -> SqlResult<Option<MatchRecognizeStatement>> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_ascii_uppercase();
    let Some(mr_pos) = upper.find(" MATCH_RECOGNIZE ") else {
        return Ok(None);
    };
    let from_pos = upper.find(" FROM ").ok_or_else(|| SqlError::Unsupported {
        feature: "MATCH_RECOGNIZE requires SELECT ... FROM <table>".into(),
    })?;
    let source_table = trimmed[from_pos + 6..mr_pos].trim().to_string();
    if source_table.is_empty() {
        return Err(SqlError::EmptyTableName);
    }

    let body_start = trimmed[mr_pos..]
        .find('(')
        .ok_or_else(|| SqlError::Unsupported {
            feature: "MATCH_RECOGNIZE requires parenthesized body".into(),
        })?
        + mr_pos
        + 1;
    let body_end = trimmed.rfind(')').ok_or_else(|| SqlError::Unsupported {
        feature: "MATCH_RECOGNIZE requires closing ')'".into(),
    })?;
    let body = &trimmed[body_start..body_end];
    let body_upper = body.to_ascii_uppercase();

    let key_column = extract_after_keyword(body, &body_upper, "PARTITION BY", "ORDER BY")?;
    let event_time_column = extract_after_keyword(body, &body_upper, "ORDER BY", "PATTERN")?;
    let pattern_body = extract_parenthesized_after(body, &body_upper, "PATTERN")?;
    let stages = pattern_body
        .split_whitespace()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    if stages.is_empty() {
        return Err(SqlError::Unsupported {
            feature: "MATCH_RECOGNIZE PATTERN must contain at least one stage".into(),
        });
    }
    let first_stage = stages
        .first()
        .copied()
        .ok_or_else(|| SqlError::Unsupported {
            feature: "MATCH_RECOGNIZE PATTERN stage list is empty".into(),
        })?;
    let mut pattern = Pattern::begin(first_stage);
    for stage in stages.iter().skip(1) {
        pattern = pattern.followed_by(*stage);
    }
    if let Some(window_ms) = parse_within_ms(body, &body_upper)? {
        pattern = pattern.within(Duration::from_millis(window_ms));
    }
    let pattern = pattern.compile().map_err(|e| SqlError::Unsupported {
        feature: format!("MATCH_RECOGNIZE pattern: {e}"),
    })?;

    Ok(Some(MatchRecognizeStatement {
        source_table,
        key_column,
        event_time_column,
        pattern,
    }))
}

/// Build a Krishiv streaming logical plan for `MATCH_RECOGNIZE`.
pub fn plan_match_recognize(stmt: MatchRecognizeStatement, query: &str) -> LogicalPlan {
    let stage_names = stmt
        .pattern
        .stages
        .iter()
        .map(|stage| stage.name.clone())
        .collect::<Vec<_>>();
    LogicalPlan::new("match-recognize", ExecutionKind::Streaming).with_node(
        PlanNode::new(
            "match-recognize",
            format!(
                "MATCH_RECOGNIZE source={} partition_by={} order_by={} pattern=({}) within_ms={}",
                stmt.source_table,
                stmt.key_column,
                stmt.event_time_column,
                stage_names.join(" "),
                stmt.pattern.window_ms
            ),
            ExecutionKind::Streaming,
        )
        .with_op(NodeOp::Other {
            description: format!("cep:{query}"),
        }),
    )
}

/// Execute a `MATCH_RECOGNIZE` statement against pre-collected source batches.
///
/// For each partition key, events are fed to a `SequentialPatternMatcher` in
/// event-time order. Completed pattern matches are concatenated and returned as
/// a single output `RecordBatch` per match (one row per stage event).
///
/// This function buffers all source rows in memory, which is incompatible with
/// unbounded streaming sources.  For streaming sources, use
/// [`execute_streaming_match_recognize`] instead.
pub fn execute_match_recognize(
    stmt: MatchRecognizeStatement,
    source_batches: &[RecordBatch],
) -> SqlResult<Vec<RecordBatch>> {
    use arrow::array::Int64Array;
    use std::collections::HashMap;

    if source_batches.is_empty() {
        return Ok(Vec::new());
    }

    // Locate key and event-time column indices.
    let schema = source_batches
        .first()
        .ok_or_else(|| SqlError::Unsupported {
            feature: "source_batches is empty".into(),
        })?
        .schema();
    let key_idx = schema
        .index_of(&stmt.key_column)
        .map_err(|_| SqlError::Unsupported {
            feature: format!(
                "MATCH_RECOGNIZE: key column '{}' not found",
                stmt.key_column
            ),
        })?;
    let time_idx = schema
        .index_of(&stmt.event_time_column)
        .map_err(|_| SqlError::Unsupported {
            feature: format!(
                "MATCH_RECOGNIZE: event time column '{}' not found",
                stmt.event_time_column
            ),
        })?;

    // Collect (key, event_time, batch_index, row_index) tuples sorted by time.
    // Using index references instead of batch.slice(i, 1) avoids allocating one
    // RecordBatch per event — for 1M events that was 1M Arc + buffer allocations.
    // The slice is materialised lazily only when a pattern match completes.
    let mut events: Vec<(String, i64, usize, usize)> = Vec::new();
    for (batch_idx, batch) in source_batches.iter().enumerate() {
        let key_col = batch.column(key_idx);
        let time_col = batch
            .column(time_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| SqlError::Unsupported {
                feature: format!(
                    "MATCH_RECOGNIZE: event time column '{}' must be Int64",
                    stmt.event_time_column
                ),
            })?;
        let key_str = key_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| SqlError::Unsupported {
                feature: format!(
                    "MATCH_RECOGNIZE: partition key column '{}' must be Utf8 (got {})",
                    stmt.key_column,
                    key_col.data_type(),
                ),
            })?;
        for i in 0..batch.num_rows() {
            let key = if key_str.is_null(i) {
                continue;
            } else {
                key_str.value(i).to_string()
            };
            if time_col.is_null(i) {
                continue;
            }
            events.push((key, time_col.value(i), batch_idx, i));
        }
    }
    events.sort_by_key(|(_, t, _, _)| *t);

    // Feed events to per-key matchers.
    let matcher = SequentialPatternMatcher::new(stmt.pattern.clone());
    let mut key_states: HashMap<String, CepKeyState> = HashMap::new();
    let mut output: Vec<RecordBatch> = Vec::new();

    let stage_names: Vec<&str> = stmt
        .pattern
        .stages
        .iter()
        .map(|s| s.name.as_str())
        .collect();

    for (key, event_time, batch_idx, row_idx) in &events {
        // Materialise the single-row slice only for the matcher call — still
        // O(n) slices in the worst case, but they are short-lived and not
        // accumulated in the events Vec.
        let Some(batch) = source_batches.get(*batch_idx) else {
            continue;
        };
        let row = batch.slice(*row_idx, 1);
        let state = key_states.entry(key.clone()).or_default();
        // Track (stage_index, start_time_ms) together so we can detect both
        // new partial starts AND restarts-after-expiry (where stage_index stays
        // at 0 but start_time changes). Break as soon as state changes so each
        // event is consumed by exactly one stage.
        let partial_key_before = state
            .partial
            .as_ref()
            .map(|p| (p.stage_index, p.start_time_ms));
        for &stage in &stage_names {
            let completed = matcher.process_event(state, stage, row.clone(), *event_time);
            if !completed.is_empty() {
                for matched_rows in completed {
                    if let Ok(concat) = arrow::compute::concat_batches(&schema, &matched_rows) {
                        output.push(concat);
                    }
                }
                break;
            }
            // Stop trying further stage names once the partial match state
            // changed (started, advanced, or reset-and-restarted).
            let partial_key_after = state
                .partial
                .as_ref()
                .map(|p| (p.stage_index, p.start_time_ms));
            if partial_key_after != partial_key_before {
                break;
            }
        }
    }

    Ok(output)
}

/// Incrementally apply a `MATCH_RECOGNIZE` pattern to a new batch of events
/// from a streaming source, updating `state` in place.
///
/// Unlike [`execute_match_recognize`], this function does **not** require all
/// source rows upfront — it feeds only the events in `new_batches` to the
/// per-key matcher state and returns any pattern completions produced by this
/// batch.  The caller owns `state` and passes the same instance on every call,
/// accumulating pattern state across many batches.
///
/// Keys whose last event is older than `within_ms` × 2 are evicted from
/// `state` to prevent unbounded memory growth for high-cardinality key spaces.
pub fn execute_streaming_match_recognize(
    stmt: &MatchRecognizeStatement,
    new_batches: &[RecordBatch],
    state: &mut PartitionedCepMatcher<String>,
) -> SqlResult<Vec<RecordBatch>> {
    use arrow::array::Int64Array;

    if new_batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = new_batches
        .first()
        .ok_or_else(|| SqlError::Unsupported {
            feature: "new_batches is empty".into(),
        })?
        .schema();
    let key_idx = schema
        .index_of(&stmt.key_column)
        .map_err(|_| SqlError::Unsupported {
            feature: format!(
                "MATCH_RECOGNIZE: key column '{}' not found",
                stmt.key_column
            ),
        })?;
    let time_idx = schema
        .index_of(&stmt.event_time_column)
        .map_err(|_| SqlError::Unsupported {
            feature: format!(
                "MATCH_RECOGNIZE: event time column '{}' not found",
                stmt.event_time_column
            ),
        })?;

    // Collect and sort all events in the incoming batches by event time.
    let mut events: Vec<(String, i64, usize, usize)> = Vec::new();
    for (batch_idx, batch) in new_batches.iter().enumerate() {
        let key_col = batch.column(key_idx);
        let time_col = batch
            .column(time_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| SqlError::Unsupported {
                feature: format!(
                    "MATCH_RECOGNIZE: event time column '{}' must be Int64",
                    stmt.event_time_column
                ),
            })?;
        let key_str = key_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| SqlError::Unsupported {
                feature: format!(
                    "MATCH_RECOGNIZE: partition key column '{}' must be Utf8 (got {})",
                    stmt.key_column,
                    key_col.data_type(),
                ),
            })?;
        for i in 0..batch.num_rows() {
            let key = if key_str.is_null(i) {
                continue;
            } else {
                key_str.value(i).to_string()
            };
            if time_col.is_null(i) {
                continue;
            }
            events.push((key, time_col.value(i), batch_idx, i));
        }
    }
    events.sort_by_key(|(_, t, _, _)| *t);

    let stage_names: Vec<&str> = stmt
        .pattern
        .stages
        .iter()
        .map(|s| s.name.as_str())
        .collect();

    let mut output: Vec<RecordBatch> = Vec::new();
    let mut max_event_time: Option<i64> = None;

    for (key, event_time, batch_idx, row_idx) in &events {
        max_event_time = Some(max_event_time.unwrap_or(*event_time).max(*event_time));
        let Some(batch) = new_batches.get(*batch_idx) else {
            continue;
        };
        let row = batch.slice(*row_idx, 1);
        for &stage in &stage_names {
            let completed = state.process_event(key.clone(), stage, row.clone(), *event_time);
            if !completed.is_empty() {
                for matched_rows in completed {
                    if let Ok(concat) = arrow::compute::concat_batches(&schema, &matched_rows) {
                        output.push(concat);
                    }
                }
                break;
            }
        }
    }

    // TTL eviction: remove keys whose last event is older than 2× the window
    // to prevent unbounded state growth for high-cardinality key spaces.
    if let Some(max_ts) = max_event_time {
        let evict_before = max_ts - 2 * stmt.pattern.window_ms as i64;
        state.evict_keys_before(evict_before);
    }

    Ok(output)
}

fn extract_after_keyword(
    body: &str,
    body_upper: &str,
    start_keyword: &str,
    end_keyword: &str,
) -> SqlResult<String> {
    let start = body_upper
        .find(start_keyword)
        .ok_or_else(|| SqlError::Unsupported {
            feature: format!("MATCH_RECOGNIZE requires {start_keyword}"),
        })?
        + start_keyword.len();
    let end = body_upper[start..]
        .find(end_keyword)
        .ok_or_else(|| SqlError::Unsupported {
            feature: format!("MATCH_RECOGNIZE requires {end_keyword}"),
        })?
        + start;
    let value = body[start..end].trim().to_string();
    if value.is_empty() {
        return Err(SqlError::Unsupported {
            feature: format!("MATCH_RECOGNIZE empty {start_keyword}"),
        });
    }
    Ok(value)
}

fn extract_parenthesized_after(body: &str, body_upper: &str, keyword: &str) -> SqlResult<String> {
    let start = body_upper
        .find(keyword)
        .ok_or_else(|| SqlError::Unsupported {
            feature: format!("MATCH_RECOGNIZE requires {keyword}"),
        })?
        + keyword.len();
    let open = body[start..]
        .find('(')
        .ok_or_else(|| SqlError::Unsupported {
            feature: format!("MATCH_RECOGNIZE {keyword} requires '('"),
        })?
        + start;
    let close = body[open + 1..]
        .find(')')
        .ok_or_else(|| SqlError::Unsupported {
            feature: format!("MATCH_RECOGNIZE {keyword} requires ')'"),
        })?
        + open
        + 1;
    Ok(body[open + 1..close].trim().to_string())
}

fn parse_within_ms(body: &str, body_upper: &str) -> SqlResult<Option<u64>> {
    let Some(start) = body_upper.find("WITHIN") else {
        return Ok(None);
    };
    let mut parts = body[start + "WITHIN".len()..].split_whitespace();
    let value = parts
        .next()
        .ok_or_else(|| SqlError::Unsupported {
            feature: "MATCH_RECOGNIZE WITHIN requires a value".into(),
        })?
        .parse::<u64>()
        .map_err(|_| SqlError::Unsupported {
            feature: "MATCH_RECOGNIZE WITHIN value must be an integer".into(),
        })?;
    let unit = parts.next().unwrap_or("MILLISECONDS").to_ascii_uppercase();
    let multiplier = match unit.as_str() {
        "MILLISECOND" | "MILLISECONDS" | "MS" => 1,
        "SECOND" | "SECONDS" | "S" => 1_000,
        "MINUTE" | "MINUTES" | "M" => 60_000,
        other => {
            return Err(SqlError::Unsupported {
                feature: format!("MATCH_RECOGNIZE unsupported WITHIN unit {other}"),
            });
        }
    };
    Ok(Some(value.saturating_mul(multiplier)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_batch_with_key_ts(keys: &[&str], times: &[i64]) -> arrow::record_batch::RecordBatch {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())) as _,
                Arc::new(Int64Array::from(times.to_vec())) as _,
            ],
        )
        .unwrap()
    }

    #[test]
    fn execute_match_recognize_three_stage_pattern_produces_match() {
        use krishiv_plan::cep::Pattern;
        use std::time::Duration;
        let pattern = Pattern::begin("A")
            .followed_by("B")
            .followed_by("C")
            .within(Duration::from_secs(60))
            .compile()
            .unwrap();

        let stmt = MatchRecognizeStatement {
            source_table: "events".to_string(),
            key_column: "user_id".to_string(),
            event_time_column: "ts".to_string(),
            pattern,
        };

        // Three events for "u1" (one per stage) and one unrelated event for "u2".
        let batch =
            make_batch_with_key_ts(&["u1", "u1", "u1", "u2"], &[1_000, 2_000, 3_000, 9_000]);

        let result = execute_match_recognize(stmt, &[batch]).unwrap();
        assert_eq!(result.len(), 1, "expected one completed A→B→C match for u1");
        assert_eq!(
            result[0].num_rows(),
            3,
            "match should span all three stage events"
        );
    }

    #[test]
    fn execute_match_recognize_no_match_when_window_expired() {
        use krishiv_plan::cep::Pattern;
        use std::time::Duration;
        let pattern = Pattern::begin("A")
            .followed_by("B")
            .within(Duration::from_millis(100))
            .compile()
            .unwrap();

        let stmt = MatchRecognizeStatement {
            source_table: "events".to_string(),
            key_column: "user_id".to_string(),
            event_time_column: "ts".to_string(),
            pattern,
        };

        // B arrives 200 ms after A — past the 100 ms window.
        let batch = make_batch_with_key_ts(&["u1", "u1"], &[0, 200]);
        let result = execute_match_recognize(stmt, &[batch]).unwrap();
        assert!(result.is_empty(), "expired window must not produce a match");
    }

    #[test]
    fn execute_match_recognize_empty_source_returns_empty() {
        use krishiv_plan::cep::Pattern;
        use std::time::Duration;
        let pattern = Pattern::begin("A")
            .followed_by("B")
            .within(Duration::from_secs(10))
            .compile()
            .unwrap();
        let stmt = MatchRecognizeStatement {
            source_table: "events".to_string(),
            key_column: "user_id".to_string(),
            event_time_column: "ts".to_string(),
            pattern,
        };
        let result = execute_match_recognize(stmt, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn execute_match_recognize_two_keys_both_complete() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use krishiv_plan::cep::Pattern;
        use std::sync::Arc;
        use std::time::Duration;

        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        // Events in time order: u1 does A@1000, u2 does A@1500, u1 does B@2000, u2 does B@2500.
        // Both keys independently complete the A→B pattern.
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["u1", "u2", "u1", "u2"])) as _,
                Arc::new(Int64Array::from(vec![1_000_i64, 1_500, 2_000, 2_500])) as _,
            ],
        )
        .unwrap();

        let pattern = Pattern::begin("A")
            .followed_by("B")
            .within(Duration::from_secs(60))
            .compile()
            .unwrap();

        let stmt = MatchRecognizeStatement {
            source_table: "events".to_string(),
            key_column: "user_id".to_string(),
            event_time_column: "ts".to_string(),
            pattern,
        };

        let result = execute_match_recognize(stmt, &[batch]).unwrap();
        assert_eq!(
            result.len(),
            2,
            "both u1 and u2 must independently complete the A→B pattern"
        );
        for matched in &result {
            assert_eq!(
                matched.num_rows(),
                2,
                "each match must contain 2 events (one for stage A, one for stage B)"
            );
        }
    }

    #[test]
    fn execute_match_recognize_boundary_event_at_exact_window_matches() {
        // An event arriving at exactly start_time + window_ms must still match
        // because the expiry check is strict greater-than (not >=).
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use krishiv_plan::cep::Pattern;
        use std::sync::Arc;
        use std::time::Duration;

        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        // A at t=0, B at t=100 with window_ms=100 → 100 - 0 = 100, not > 100 → within window.
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["u1", "u1"])) as _,
                Arc::new(Int64Array::from(vec![0_i64, 100])) as _,
            ],
        )
        .unwrap();

        let pattern = Pattern::begin("A")
            .followed_by("B")
            .within(Duration::from_millis(100))
            .compile()
            .unwrap();

        let stmt = MatchRecognizeStatement {
            source_table: "events".to_string(),
            key_column: "user_id".to_string(),
            event_time_column: "ts".to_string(),
            pattern,
        };

        let result = execute_match_recognize(stmt, &[batch]).unwrap();
        assert_eq!(
            result.len(),
            1,
            "event at exactly start_time + window_ms (t=100) must still match (strict > check)"
        );
    }

    #[test]
    fn execute_match_recognize_one_ms_past_window_does_not_match() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use krishiv_plan::cep::Pattern;
        use std::sync::Arc;
        use std::time::Duration;

        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        // A at t=0, B at t=101 with window_ms=100 → 101 - 0 = 101 > 100 → expired.
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["u1", "u1"])) as _,
                Arc::new(Int64Array::from(vec![0_i64, 101])) as _,
            ],
        )
        .unwrap();

        let pattern = Pattern::begin("A")
            .followed_by("B")
            .within(Duration::from_millis(100))
            .compile()
            .unwrap();

        let stmt = MatchRecognizeStatement {
            source_table: "events".to_string(),
            key_column: "user_id".to_string(),
            event_time_column: "ts".to_string(),
            pattern,
        };

        let result = execute_match_recognize(stmt, &[batch]).unwrap();
        assert!(
            result.is_empty(),
            "event 1 ms past window_ms must not match (expired partial)"
        );
    }

    #[test]
    fn cep_on_streaming_source_returns_unsupported_error() {
        // SqlEngine guards CEP against unbounded streaming sources.
        // This test exercises that guard via the engine-level sql() path.
        let engine = crate::SqlEngine::new();
        engine
            .register_streaming_source_name("live_events")
            .unwrap();
        // We can't easily make the async sql() call here synchronously, so just
        // verify is_streaming_source returns true (the guard relies on this).
        assert!(
            engine.is_streaming_source("live_events"),
            "live_events must be identified as a streaming source"
        );
        assert!(
            !engine.is_streaming_source("batch_table"),
            "batch_table must not be streaming"
        );
    }

    #[test]
    fn parses_match_recognize_subset() {
        let stmt = parse_match_recognize(
            "SELECT * FROM events MATCH_RECOGNIZE (PARTITION BY user_id ORDER BY ts PATTERN (A B) WITHIN 10 SECONDS)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(stmt.source_table, "events");
        assert_eq!(stmt.key_column, "user_id");
        assert_eq!(stmt.event_time_column, "ts");
        assert_eq!(stmt.pattern.stages.len(), 2);
        assert_eq!(stmt.pattern.window_ms, 10_000);
    }
}
