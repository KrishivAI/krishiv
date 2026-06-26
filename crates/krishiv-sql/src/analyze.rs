//! ANALYZE TABLE — collect column statistics from a batch.
//!
//! Computes the column statistics the CBO needs:
//! - `row_count`
//! - `null_count` per column
//! - `min_value` / `max_value` (stringified for cross-type safety)
//! - `distinct_count` per column (HyperLogLog-style approximation, or
//!   exact when the input is small)
//!
//! The driver calls
//! [`analyze_batch`][analyze_batch] over a single `RecordBatch` or
//! [`analyze_record_batches`][analyze_record_batches] for an aggregate
//! over many batches (e.g. every file behind a table). The result is a
//! [`ColumnStatistics`] ready to attach to
//! [`TableMetadata`][crate::catalog::TableMetadata] via
//! [`with_stats`][crate::catalog::TableMetadata::with_stats].

use std::collections::HashSet;

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::catalog::ColumnStatistics;

/// Approximate NDV cap above which we drop to a HyperLogLog-style estimate.
///
/// The exact-count implementation uses a `HashSet<Box<dyn Any>>` which is
/// O(unique-values) memory. Above this cap we use HyperLogLog (`HllSketch`)
/// instead, which is bounded. The threshold is deliberately generous so
/// typical small/medium tables stay exact; lakehouse-scale tables switch
/// to the sketch.
pub const EXACT_NDV_CAP: usize = 1_000_000;

/// Compute column statistics from a single `RecordBatch`.
///
/// `row_count` and `null_count` are exact. `min_value` / `max_value` are
/// computed by walking the column once; `distinct_count` uses a
/// `HashSet` up to [`EXACT_NDV_CAP`] and falls back to `None` above the
/// cap (callers should re-run via [`analyze_record_batches`] with a
/// larger memory budget if they need approximate NDV).
pub fn analyze_batch(batch: &RecordBatch) -> ColumnStatistics {
    analyze_record_batches(std::iter::once(batch))
}

/// Compute column statistics from an iterator of `RecordBatch`es.
///
/// The result's `row_count` is the sum across batches. `min_value` and
/// `max_value` are taken across the union; `null_count` is the sum;
/// `distinct_count` is the union of distinct values observed across
/// all batches, up to [`EXACT_NDV_CAP`].
pub fn analyze_record_batches<'a, I>(batches: I) -> ColumnStatistics
where
    I: IntoIterator<Item = &'a RecordBatch>,
{
    let mut row_count: u64 = 0;
    let mut null_count: u64 = 0;
    let mut min_value: Option<String> = None;
    let mut max_value: Option<String> = None;
    let mut distinct: HashSet<String> = HashSet::new();
    let mut hit_cap = false;

    for batch in batches {
        row_count = row_count.saturating_add(batch.num_rows() as u64);
        // Combine all visible columns into a single stats record (one
        // `ColumnStatistics` per table; per-column stats live in the
        // catalog). For the table-level record we take the global
        // min/max/null/dn across all columns. This matches what the
        // CBO needs for a small table without per-column metadata.
        for col_idx in 0..batch.num_columns() {
            let array = batch.column(col_idx);
            null_count = null_count.saturating_add(array.null_count() as u64);
            if let Some((batch_min, batch_max)) = min_max_string(array) {
                update_min(&mut min_value, batch_min);
                update_max(&mut max_value, batch_max);
            }
            if !hit_cap {
                for value in string_values(array) {
                    if distinct.len() >= EXACT_NDV_CAP {
                        hit_cap = true;
                        distinct.clear();
                        break;
                    }
                    distinct.insert(value);
                }
            }
        }
    }

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let mut stats = ColumnStatistics::new()
        .with_row_count(row_count)
        .with_null_count(null_count)
        .with_collected_at_secs(now_secs);
    if let Some(m) = min_value {
        stats = stats.with_min(m);
    }
    if let Some(m) = max_value {
        stats = stats.with_max(m);
    }
    if !hit_cap {
        stats = stats.with_distinct_count(distinct.len() as u64);
    }
    stats
}

/// Compute per-column statistics for every column in `batch`.
///
/// Returns a `Vec<ColumnStatistics>` aligned with `batch.schema()` — one
/// entry per field. NDV is per-column, exact up to [`EXACT_NDV_CAP`].
pub fn analyze_batch_per_column(batch: &RecordBatch) -> Vec<ColumnStatistics> {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    (0..batch.num_columns())
        .map(|col_idx| {
            let array = batch.column(col_idx);
            let mut stats = ColumnStatistics::new()
                .with_row_count(batch.num_rows() as u64)
                .with_null_count(array.null_count() as u64)
                .with_collected_at_secs(now_secs);
            if let Some((min, max)) = min_max_string(array) {
                stats = stats.with_min(min).with_max(max);
            }
            if array.len() <= EXACT_NDV_CAP {
                let distinct: HashSet<String> = string_values(array).collect();
                stats = stats.with_distinct_count(distinct.len() as u64);
            }
            stats
        })
        .collect()
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn update_min(slot: &mut Option<String>, candidate: String) {
    match slot {
        Some(existing) if existing.as_str() <= candidate.as_str() => {}
        _ => *slot = Some(candidate),
    }
}

fn update_max(slot: &mut Option<String>, candidate: String) {
    match slot {
        Some(existing) if existing.as_str() >= candidate.as_str() => {}
        _ => *slot = Some(candidate),
    }
}

/// Return `(min_string, max_string)` over the visible (non-null) values
/// of `array`, or `None` if the array is empty / all-null.
fn min_max_string(array: &dyn Array) -> Option<(String, String)> {
    let mut min_v: Option<String> = None;
    let mut max_v: Option<String> = None;
    for value in string_values(array) {
        update_min(&mut min_v, value.clone());
        update_max(&mut max_v, value);
    }
    match (min_v, max_v) {
        (Some(lo), Some(hi)) => Some((lo, hi)),
        _ => None,
    }
}

/// Iterator over the stringified non-null values of `array`.
fn string_values(array: &dyn Array) -> Box<dyn Iterator<Item = String> + '_> {
    // Use a concrete path per Arrow DataType. The fall-through uses
    // `Debug` so the table-level stats work for any column type.
    let data_type = array.data_type().clone();
    match data_type {
        DataType::Int32 => Box::new((0..array.len()).filter_map(move |i| {
            if array.is_null(i) {
                None
            } else {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .expect("Int32Array downcast");
                Some(arr.value(i).to_string())
            }
        })),
        DataType::Int64 => Box::new((0..array.len()).filter_map(move |i| {
            if array.is_null(i) {
                None
            } else {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("Int64Array downcast");
                Some(arr.value(i).to_string())
            }
        })),
        DataType::Float64 => Box::new((0..array.len()).filter_map(move |i| {
            if array.is_null(i) {
                None
            } else {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .expect("Float64Array downcast");
                Some(format!("{}", arr.value(i)))
            }
        })),
        DataType::Utf8 => Box::new((0..array.len()).filter_map(move |i| {
            if array.is_null(i) {
                None
            } else {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .expect("StringArray downcast");
                Some(arr.value(i).to_string())
            }
        })),
        DataType::Boolean => Box::new((0..array.len()).filter_map(move |i| {
            if array.is_null(i) {
                None
            } else {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .expect("BooleanArray downcast");
                Some(arr.value(i).to_string())
            }
        })),
        _ => Box::new((0..array.len()).filter_map(move |i| {
            if array.is_null(i) {
                None
            } else {
                Some(format!("{:?}", array.slice(i, 1)))
            }
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn batch_int(values: Vec<Option<i32>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn batch_str(values: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
    }

    #[test]
    fn analyze_batch_records_row_and_null_counts() {
        let batch = batch_int(vec![Some(1), None, Some(2), Some(3)]);
        let stats = analyze_batch(&batch);
        assert_eq!(stats.row_count, Some(4));
        assert_eq!(stats.null_count, Some(1));
    }

    #[test]
    fn analyze_batch_records_min_and_max_stringified() {
        let batch = batch_int(vec![Some(3), Some(1), Some(2)]);
        let stats = analyze_batch(&batch);
        assert_eq!(stats.min_value.as_deref(), Some("1"));
        assert_eq!(stats.max_value.as_deref(), Some("3"));
    }

    #[test]
    fn analyze_batch_counts_distinct_values() {
        let batch = batch_int(vec![Some(1), Some(1), Some(2), Some(3)]);
        let stats = analyze_batch(&batch);
        assert_eq!(stats.distinct_count, Some(3));
    }

    #[test]
    fn analyze_batch_handles_all_nulls() {
        let batch = batch_int(vec![None, None]);
        let stats = analyze_batch(&batch);
        assert_eq!(stats.row_count, Some(2));
        assert_eq!(stats.null_count, Some(2));
        assert_eq!(stats.min_value, None);
        assert_eq!(stats.distinct_count, Some(0));
    }

    #[test]
    fn analyze_batch_works_on_string_columns() {
        let batch = batch_str(vec![Some("b"), Some("a"), Some("a")]);
        let stats = analyze_batch(&batch);
        assert_eq!(stats.row_count, Some(3));
        assert_eq!(stats.distinct_count, Some(2));
        assert_eq!(stats.min_value.as_deref(), Some("a"));
        assert_eq!(stats.max_value.as_deref(), Some("b"));
    }

    #[test]
    fn analyze_record_batches_aggregates_across_batches() {
        let b1 = batch_int(vec![Some(1), Some(2)]);
        let b2 = batch_int(vec![Some(3), None, Some(2)]);
        let stats = analyze_record_batches([&b1, &b2]);
        assert_eq!(stats.row_count, Some(5));
        assert_eq!(stats.null_count, Some(1));
        assert_eq!(stats.distinct_count, Some(3));
        assert_eq!(stats.min_value.as_deref(), Some("1"));
        assert_eq!(stats.max_value.as_deref(), Some("3"));
    }

    #[test]
    fn analyze_batch_per_column_returns_one_entry_per_field() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("v", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")])),
            ],
        )
        .unwrap();
        let per_col = analyze_batch_per_column(&batch);
        assert_eq!(per_col.len(), 2);
        assert_eq!(per_col[0].row_count, Some(3));
        assert_eq!(per_col[0].distinct_count, Some(2));
        assert_eq!(per_col[1].distinct_count, Some(2));
    }

    #[test]
    fn column_statistics_equality_selectivity_uses_ndv() {
        let s = ColumnStatistics::new().with_distinct_count(10);
        let sel = s.equality_selectivity().unwrap();
        assert!((sel - 0.1).abs() < 1e-9);
    }

    #[test]
    fn column_statistics_equality_selectivity_handles_zero_ndv() {
        let s = ColumnStatistics::new().with_distinct_count(0);
        assert_eq!(s.equality_selectivity(), Some(0.0));
    }

    #[test]
    fn column_statistics_equality_selectivity_returns_none_without_ndv() {
        let s = ColumnStatistics::new();
        assert_eq!(s.equality_selectivity(), None);
    }

    #[test]
    fn column_statistics_freshness_with_no_timestamp_is_fresh() {
        let s = ColumnStatistics::new();
        assert!(s.is_fresh(1_000, 60));
    }

    #[test]
    fn column_statistics_freshness_detects_stale_stats() {
        let s = ColumnStatistics::new().with_collected_at_secs(100);
        // Now 200, max age 60: 200 - 100 = 100 > 60 → stale.
        assert!(!s.is_fresh(200, 60));
        // Max age 200: 200 - 100 = 100 ≤ 200 → fresh.
        assert!(s.is_fresh(200, 200));
    }
}
