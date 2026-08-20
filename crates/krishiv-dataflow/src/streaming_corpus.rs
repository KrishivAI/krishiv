//! The shared cross-loop streaming corpus.
//!
//! One set of fixtures, one set of expectations, driven through every streaming
//! driver loop in the tree. This module holds **only data and pure helpers** —
//! it deliberately knows nothing about how any loop is stood up, because the
//! whole point is that the same expectations are readable from crates that
//! cannot see each other.
//!
//! ## Why this lives in krishiv-dataflow
//!
//! The corpus began private to `krishiv-api::streaming_conformance`, which could
//! only reach two of the four loops. The other two live in `krishiv-executor`,
//! and `krishiv-api` does not depend on it. `krishiv-dataflow` is the deepest
//! crate both depend on, so it is the only placement where one corpus is visible
//! to every arm. Anything added here must stay dependency-light for that reason.
//!
//! ## The `expected_without_flush` field is the load-bearing one
//!
//! Each entry records both what a **correct** loop emits and what a loop with no
//! end-of-stream flush emits *today*. That second field is what lets a harness
//! assert a broken arm's behaviour positively rather than asserting `a != b`.
//! Inequality alone passes vacuously when every arm is broken the same way — and
//! "every arm emits nothing" is precisely the failure this corpus exists to
//! catch, since an empty sink plus a `Completed` job is what silent truncation
//! looks like from outside.

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// The windowed query every corpus entry is run through.
///
/// A tumbling window keyed by `user_id` over event-time `ts`. Chosen because a
/// tumbling window is the one shape where "the watermark never passed the window
/// end" is trivially constructible, which is the divergence class that actually
/// bit this codebase twice.
pub const WINDOWED_SQL: &str = "SELECT user_id, SUM(amount) AS total \
     FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 10000) \
     GROUP BY user_id, window_start, window_end";

/// Window size in milliseconds, matching [`WINDOWED_SQL`].
pub const WINDOW_SIZE_MS: i64 = 10_000;
/// Grouping key column.
pub const KEY_COLUMN: &str = "user_id";
/// Event-time column.
pub const TIME_COLUMN: &str = "ts";
/// Aggregate input column.
pub const AGG_INPUT: &str = "amount";
/// Aggregate output column.
pub const AGG_OUTPUT: &str = "total";

/// One corpus entry: a fixture, and the windows each kind of loop closes.
#[derive(Debug, Clone, Copy)]
pub struct CorpusEntry {
    /// Stable identifier, used in assertion messages.
    pub name: &'static str,
    /// The CSV fixture, header included.
    pub csv: &'static str,
    /// `(key, total)` for every window a **correct** loop closes.
    pub expected: &'static [(&'static str, i64)],
    /// `(key, total)` for every window a loop with **no end-of-stream flush**
    /// closes today. The difference against `expected` is the defect's exact
    /// blast radius for this fixture.
    pub expected_without_flush: &'static [(&'static str, i64)],
    /// Why this fixture is in the corpus — what a loop can get wrong that it
    /// catches. Not decoration: an entry nobody can justify is an entry whose
    /// expectations nobody will maintain.
    pub why: &'static str,
}

impl CorpusEntry {
    /// Does this fixture distinguish a flushing loop from a non-flushing one?
    ///
    /// Every current entry does, but a future entry might exercise a different
    /// axis, and a harness asserting flush behaviour should skip those rather
    /// than assert a difference that was never the point.
    #[must_use]
    pub fn flush_is_observable(&self) -> bool {
        self.expected.len() != self.expected_without_flush.len()
    }

    /// The `"total":N` fragments a JSON sink must contain for a correct run.
    #[must_use]
    pub fn expected_json_fragments(&self) -> Vec<String> {
        self.expected
            .iter()
            .map(|(_, total)| format!("\"{AGG_OUTPUT}\":{total}"))
            .collect()
    }

    /// Parse the fixture into `(key, ts, amount)` triples.
    ///
    /// Returns `Err` rather than panicking on a malformed fixture. The fixtures
    /// are compile-time constants in this file, so an error here is always a bug
    /// in this file — but this module compiles as ordinary library code (it must,
    /// to be visible from another crate's tests), and the workspace forbids
    /// panicking outside `#[cfg(test)]`. Callers are test code and may unwrap.
    ///
    /// # Errors
    ///
    /// Returns `Err` naming the offending line if a row has fewer than three
    /// comma-separated fields or a non-integer `ts`/`amount`.
    pub fn rows(&self) -> Result<Vec<(&'static str, i64, i64)>, String> {
        self.csv
            .lines()
            .skip(1)
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                let mut parts = line.split(',');
                let bad = || format!("[{}] malformed fixture row: {line:?}", self.name);
                let key = parts.next().ok_or_else(bad)?;
                let ts = parts
                    .next()
                    .ok_or_else(bad)?
                    .trim()
                    .parse::<i64>()
                    .map_err(|e| format!("[{}] ts in {line:?}: {e}", self.name))?;
                let amount = parts
                    .next()
                    .ok_or_else(bad)?
                    .trim()
                    .parse::<i64>()
                    .map_err(|e| format!("[{}] amount in {line:?}: {e}", self.name))?;
                Ok((key, ts, amount))
            })
            .collect()
    }

    /// The fixture as one `RecordBatch`, typed the way a CSV source delivers it.
    ///
    /// # Errors
    ///
    /// Propagates a malformed fixture from [`CorpusEntry::rows`].
    pub fn batch(&self) -> Result<RecordBatch, String> {
        rows_to_batch(&self.rows()?)
    }

    /// The fixture as one single-row `RecordBatch` per event.
    ///
    /// Loops that step per push behave differently from loops handed the whole
    /// fixture at once — a per-row feed advances the watermark incrementally,
    /// which is what a real source does and what closes a window mid-run.
    ///
    /// # Errors
    ///
    /// Propagates a malformed fixture from [`CorpusEntry::rows`].
    pub fn batches_per_row(&self) -> Result<Vec<RecordBatch>, String> {
        self.rows()?
            .into_iter()
            .map(|row| rows_to_batch(std::slice::from_ref(&row)))
            .collect()
    }
}

/// Build the corpus schema: `user_id` Utf8, `ts` Int64, `amount` Int64.
#[must_use]
pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(KEY_COLUMN, DataType::Utf8, false),
        Field::new(TIME_COLUMN, DataType::Int64, false),
        Field::new(AGG_INPUT, DataType::Int64, false),
    ]))
}

fn rows_to_batch(rows: &[(&'static str, i64, i64)]) -> Result<RecordBatch, String> {
    let keys: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|(k, _, _)| *k).collect::<Vec<_>>(),
    ));
    let times: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|(_, t, _)| *t).collect::<Vec<_>>(),
    ));
    let amounts: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|(_, _, a)| *a).collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema(), vec![keys, times, amounts])
        .map_err(|e| format!("corpus rows do not build a valid RecordBatch: {e}"))
}

/// Render sink text as sorted non-empty lines.
///
/// Emission *order* is genuinely a loop's own business — one may drain per push
/// and another only at teardown — so comparing loops on order would report a
/// difference that is not a defect. Window *content* is not a loop's business,
/// and that is what this comparison preserves.
#[must_use]
pub fn render_sorted(raw: &str) -> String {
    let mut lines: Vec<&str> = raw.lines().filter(|l| !l.trim().is_empty()).collect();
    lines.sort_unstable();
    lines.join("\n")
}

/// Collapse closed-window batches to sorted `(key, total)` pairs.
///
/// Lets an arm that receives `RecordBatch`es (the executor fragments) compare
/// against the same expectations as an arm that receives a JSON sink file (the
/// embedded engine and the runtime seam), without either arm having to know the
/// other's output medium.
///
/// # Errors
///
/// Returns `Err` with a description if a batch lacks the key or aggregate column
/// or types them unexpectedly — a harness should surface that as a failure
/// rather than silently compare an empty vec.
pub fn totals_from_batches(batches: &[RecordBatch]) -> Result<Vec<(String, i64)>, String> {
    let mut out = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let key_idx = batch
            .schema()
            .index_of(KEY_COLUMN)
            .map_err(|_| format!("closed-window batch has no `{KEY_COLUMN}` column"))?;
        let total_idx = batch
            .schema()
            .index_of(AGG_OUTPUT)
            .map_err(|_| format!("closed-window batch has no `{AGG_OUTPUT}` column"))?;
        let keys = batch
            .column(key_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| format!("`{KEY_COLUMN}` is not Utf8"))?;
        let totals = batch
            .column(total_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| format!("`{AGG_OUTPUT}` is not Int64"))?;
        for row in 0..batch.num_rows() {
            out.push((keys.value(row).to_owned(), totals.value(row)));
        }
    }
    out.sort();
    Ok(out)
}

/// Normalise an expectation list for comparison against [`totals_from_batches`].
#[must_use]
pub fn sorted_expectation(pairs: &[(&str, i64)]) -> Vec<(String, i64)> {
    let mut out: Vec<(String, i64)> = pairs.iter().map(|(k, t)| ((*k).to_owned(), *t)).collect();
    out.sort();
    out
}

/// The corpus.
///
/// Chosen for the shapes where a *loop* difference shows rather than an operator
/// difference — the operator is shared, so anything it gets wrong it gets wrong
/// everywhere and identically. What a loop can get wrong is *when it stops
/// stepping*: whether it flushes at end-of-stream, whether it advances the
/// watermark on idle, whether the last batch is processed before teardown.
pub const CORPUS: &[CorpusEntry] = &[
    CorpusEntry {
        name: "trailing_window_never_closed_by_watermark",
        csv: "user_id,ts,amount\na,1000,10\na,5000,20\nb,6000,5\n",
        expected: &[("a", 30), ("b", 5)],
        // Nothing survives: every event is in [0,10000) and no later event ever
        // pushes the watermark past the window end.
        expected_without_flush: &[],
        why: "THE regression. Every event lands in one window and nothing later arrives, so \
              no watermark ever passes the window end and only an end-of-stream flush closes \
              it. A loop without one writes nothing and reports success — the exact silent \
              truncation of dd47d50/8756b41.",
    },
    CorpusEntry {
        name: "closed_window_plus_trailing_window",
        csv: "user_id,ts,amount\na,1000,10\na,5000,20\na,25000,7\n",
        expected: &[("a", 7), ("a", 30)],
        // The PARTIAL loss. [0,10000) closes normally when ts=25000 advances the
        // watermark; [20000,30000) needs the flush. An arm that emits exactly
        // one row is missing a flush; an arm that emits zero is a dead loop.
        // That distinction is why this entry exists.
        expected_without_flush: &[("a", 30)],
        why: "A window the watermark DOES close, plus a trailing one it does not. Catches the \
              opposite error — a loop that flushes eagerly and emits the open window twice, or \
              one that flushes instead of closing normally — and its partial loss distinguishes \
              a missing flush from a loop that never ran at all.",
    },
    CorpusEntry {
        name: "single_event_single_window",
        csv: "user_id,ts,amount\nz,42,99\n",
        expected: &[("z", 99)],
        expected_without_flush: &[],
        why: "Single event, single window, nothing else. The degenerate case where 'no output' \
              is easiest to mistake for 'no input'.",
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    /// The corpus must actually exercise the flush divergence.
    ///
    /// If every entry's `expected_without_flush` equalled `expected`, the corpus
    /// would be internally consistent, would compile, and would prove nothing —
    /// the harness arms would all agree and pass vacuously. This pins the one
    /// property that makes the corpus worth running.
    #[test]
    fn at_least_one_entry_distinguishes_flushing_from_not() {
        let observable = CORPUS.iter().filter(|e| e.flush_is_observable()).count();
        assert!(
            observable >= 2,
            "the corpus must contain at least two fixtures where a missing end-of-stream \
             flush changes the output, or it cannot detect the defect it exists for; \
             found {observable}"
        );
    }

    /// The partial-loss entry is what separates "missing flush" from "dead loop".
    #[test]
    fn one_entry_loses_only_part_of_its_output_without_a_flush() {
        let partial = CORPUS
            .iter()
            .find(|e| !e.expected_without_flush.is_empty() && e.flush_is_observable())
            .expect(
                "the corpus needs one fixture that survives a missing flush only PARTIALLY; \
                 without it, an arm emitting nothing is indistinguishable from an arm that \
                 never ran",
            );
        assert_eq!(partial.name, "closed_window_plus_trailing_window");
        assert_eq!(partial.expected.len(), 2);
        assert_eq!(partial.expected_without_flush.len(), 1);
    }

    #[test]
    fn fixtures_parse_into_the_rows_the_expectations_describe() {
        let entry = CORPUS[0];
        assert_eq!(
            entry.rows().expect("fixture parses"),
            vec![("a", 1000, 10), ("a", 5000, 20), ("b", 6000, 5)]
        );
        let batch = entry.batch().expect("fixture builds a batch");
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(
            entry
                .batches_per_row()
                .expect("fixture builds per-row batches")
                .len(),
            3
        );
    }

    /// Every fixture's expectations must be derivable from its own rows, so a
    /// hand-edited fixture cannot drift away from hand-edited expectations.
    #[test]
    fn expectations_match_windowed_sums_computed_from_the_fixtures() {
        for entry in CORPUS {
            // Group by (key, window index) — the same bucketing a tumbling
            // operator applies, restated independently of the operator so this
            // is a check rather than a tautology.
            let mut grouped: Vec<((String, i64), i64)> = Vec::new();
            for (key, ts, amount) in entry.rows().expect("fixture parses") {
                let window = ts / WINDOW_SIZE_MS;
                let slot = grouped
                    .iter_mut()
                    .find(|((k, w), _)| k == key && *w == window);
                match slot {
                    Some((_, total)) => *total += amount,
                    None => grouped.push(((key.to_owned(), window), amount)),
                }
            }
            let mut computed: Vec<(String, i64)> = grouped
                .into_iter()
                .map(|((k, _), total)| (k, total))
                .collect();
            computed.sort();
            assert_eq!(
                computed,
                sorted_expectation(entry.expected),
                "[{}] declared expectations do not match the windowed sums of its own fixture",
                entry.name
            );
        }
    }

    #[test]
    fn render_sorted_drops_blank_lines_and_orders_content() {
        assert_eq!(render_sorted("b\n\na\n"), "a\nb");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The second axis: query SHAPE
// ─────────────────────────────────────────────────────────────────────────────

/// One query shape, and the spec property compiling it must produce.
///
/// # Why this exists as a separate axis
///
/// [`CORPUS`] varies event **timing** — watermark advance, flush behaviour —
/// over ONE fixed query: a string key, no `WHERE`, one grouping column. Its
/// axis is the driver loop, and it is excellent at that.
///
/// It is therefore structurally blind to everything that goes wrong between a
/// user's query and the spec. An API audit found five silent wrong answers on
/// that axis — a dropped `WHERE`, a hardcoded key type, a discarded pipeline —
/// and the loop corpus could not have caught any of them, not by oversight but
/// because it has no second dimension.
///
/// This is that dimension. The assertion is
/// `krishiv-bench/tests/streaming_query_shapes.rs`, which can reach the
/// compiler (krishiv-sql does not depend on this crate, so it cannot host the
/// test); the data lives here beside the timing corpus so the two axes are
/// visibly siblings.
#[derive(Debug, Clone, Copy)]
pub struct QueryShape {
    /// Stable identifier.
    pub name: &'static str,
    /// The SQL to compile.
    pub sql: &'static str,
    /// Must this compile at all?
    pub compiles: bool,
    /// If it compiles, a substring the resulting spec's debug form must contain.
    ///
    /// Deliberately a property of the SPEC rather than of the output: these
    /// shapes exist to catch fields lost between the query and the spec, and a
    /// field that never arrives cannot be observed downstream.
    pub spec_must_contain: Option<&'static str>,
    /// If it does not compile, a substring the error must contain — so a
    /// refusal is checked for being *informative*, not merely for happening.
    pub error_must_contain: Option<&'static str>,
    /// What this shape catches.
    pub why: &'static str,
}

/// Query shapes every streaming compiler change is checked against.
pub const QUERY_SHAPES: &[QueryShape] = &[
    QueryShape {
        name: "baseline_keyed_tumbling",
        sql: "SELECT k, SUM(v) AS total FROM TUMBLE(TABLE e, DESCRIPTOR(ts), 10000)               GROUP BY k, window_start, window_end",
        compiles: true,
        spec_must_contain: Some("key_column: \"k\""),
        error_must_contain: None,
        why: "The canonical shape. Every rejection below is only meaningful if               this is accepted — otherwise they would all pass against a               compiler that refused everything.",
    },
    QueryShape {
        name: "top_level_where",
        sql: "SELECT k, SUM(v) AS total FROM TUMBLE(TABLE e, DESCRIPTOR(ts), 10000)               WHERE v > 100 GROUP BY k, window_start, window_end",
        compiles: true,
        spec_must_contain: Some("row_filter: Some"),
        error_must_contain: None,
        why: "The predicate was silently discarded for the lifetime of the               compiler: `WHERE v > 100` compiled, registered, and counted every               row. Asserts it REACHES the spec, because 'it compiles' was               exactly the state the defect was in.",
    },
    QueryShape {
        name: "multi_column_group_by",
        sql: "SELECT k, k2, COUNT(*) AS c FROM TUMBLE(TABLE e, DESCRIPTOR(ts), 10000)               GROUP BY k, k2, window_start, window_end",
        compiles: true,
        spec_must_contain: Some("\"k2\""),
        error_must_contain: None,
        why: "A composite key once silently collapsed to the first column and               aggregated across the second. Multi-key landed (register §48);               the assertion now guards the opposite failure — the SECOND               column must REACH the spec, because a collapse back to one key               would still compile.",
    },
    QueryShape {
        name: "global_aggregate_no_key",
        sql: "SELECT MAX(v) AS mx FROM TUMBLE(TABLE e, DESCRIPTOR(ts), 10000)               GROUP BY window_start, window_end",
        compiles: false,
        spec_must_contain: None,
        error_must_contain: Some("grouping key"),
        why: "Global aggregates are not expressible yet. Pinned as a refusal so               the day they are supported, this line has to change deliberately.",
    },
    QueryShape {
        name: "windowless_projection",
        sql: "SELECT k, v * 2 AS doubled FROM e",
        compiles: false,
        spec_must_contain: None,
        error_must_contain: Some("window"),
        why: "A stateless query is not a windowed one. It must be refused HERE               and routed to the stateless path by the caller — the routing               decision that used to swallow every other error on this list.",
    },
];
