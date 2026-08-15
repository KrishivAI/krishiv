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
//! [`analyze_batch`] over a single `RecordBatch` or
//! [`analyze_record_batches`] for an aggregate
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
    let mut min_value: Option<Extremum> = None;
    let mut max_value: Option<Extremum> = None;
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
        stats = stats.with_min(m.text);
    }
    if let Some(m) = max_value {
        stats = stats.with_max(m.text);
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
                stats = stats.with_min(min.text).with_max(max.text);
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

/// An extreme value, kept with the sort key that produced it.
///
/// `key` is the Arrow row encoding of the value: a byte string whose
/// `Ord` agrees with the column's own type order. It is what makes the
/// comparison typed. `dt` guards the comparison, because two encodings
/// are only comparable when they came from the same data type.
#[derive(Clone)]
struct Extremum {
    key: Option<Vec<u8>>,
    text: String,
    dt: DataType,
}

/// Order two candidates: by sort key when they are comparable, else by text.
///
/// Falling back to text is the old behaviour and is wrong for numbers
/// ("10" < "9"), but it only applies where there is genuinely nothing
/// better: comparing values of two *different* types, which the
/// table-level record does when it folds every column into one min/max.
fn less(a: &Extremum, b: &Extremum) -> bool {
    match (&a.key, &b.key) {
        (Some(ak), Some(bk)) if a.dt == b.dt => ak < bk,
        _ => a.text < b.text,
    }
}

fn update_min(slot: &mut Option<Extremum>, candidate: Extremum) {
    match slot {
        Some(existing) if !less(&candidate, existing) => {}
        _ => *slot = Some(candidate),
    }
}

fn update_max(slot: &mut Option<Extremum>, candidate: Extremum) {
    match slot {
        Some(existing) if !less(existing, &candidate) => {}
        _ => *slot = Some(candidate),
    }
}

/// Byte-comparable sort keys for every row of `array`.
///
/// `None` when the type has no row encoding (the converter rejects it), in
/// which case callers fall back to comparing the rendered text.
fn sort_keys(array: &dyn Array) -> Option<Vec<Vec<u8>>> {
    use arrow::row::{RowConverter, SortField};
    let field = SortField::new(array.data_type().clone());
    let converter = RowConverter::new(vec![field]).ok()?;
    let rows = converter
        .convert_columns(&[arrow::array::make_array(array.to_data())])
        .ok()?;
    Some(
        (0..rows.num_rows())
            .map(|i| rows.row(i).as_ref().to_vec())
            .collect(),
    )
}

/// Render every value of `array` the way Arrow itself displays it.
///
/// The previous version matched five concrete types and fell through to
/// `format!("{:?}", array.slice(i, 1))` for everything else — the Debug of
/// a one-row array, i.e. a multi-line blob with the array's type header in
/// it. Decimal128 and Date32 both landed there, which is most of a TPC-H
/// table, so distinct counts counted formatted blobs and min/max compared
/// them. `ArrayFormatter` handles every Arrow type in one path.
fn value_texts(array: &dyn Array) -> Vec<Option<String>> {
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    match ArrayFormatter::try_new(array, &FormatOptions::default()) {
        Ok(formatter) => (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    Some(formatter.value(i).to_string())
                }
            })
            .collect(),
        // A type the formatter cannot render is still worth counting as
        // *something*, but never as a fabricated value.
        Err(_) => vec![None; array.len()],
    }
}

/// Return `(min, max)` over the visible (non-null) values of `array`.
fn min_max_string(array: &dyn Array) -> Option<(Extremum, Extremum)> {
    let mut min_v: Option<Extremum> = None;
    let mut max_v: Option<Extremum> = None;
    for value in extrema_candidates(array) {
        update_min(&mut min_v, value.clone());
        update_max(&mut max_v, value);
    }
    match (min_v, max_v) {
        (Some(lo), Some(hi)) => Some((lo, hi)),
        _ => None,
    }
}

/// Non-null values of `array` as comparable candidates.
fn extrema_candidates(array: &dyn Array) -> Vec<Extremum> {
    let keys = sort_keys(array);
    let dt = array.data_type().clone();
    value_texts(array)
        .into_iter()
        .enumerate()
        .filter_map(|(i, text)| {
            text.map(|text| Extremum {
                key: keys.as_ref().and_then(|k| k.get(i).cloned()),
                text,
                dt: dt.clone(),
            })
        })
        .collect()
}

/// Iterator over the stringified non-null values of `array`.
fn string_values(array: &dyn Array) -> impl Iterator<Item = String> + '_ {
    value_texts(array).into_iter().flatten()
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

    /// Numeric min/max must order as numbers.
    ///
    /// The old code stringified first and compared with `str::<=`, so this
    /// returned min "10", max "9". Every existing test used single digits,
    /// where lexicographic and numeric order happen to agree — which is why
    /// the bug survived: the suite could not tell the two apart.
    #[test]
    fn min_and_max_order_numerically_not_lexicographically() {
        let batch = batch_int(vec![Some(9), Some(10), Some(100), Some(2)]);
        let stats = analyze_batch(&batch);
        assert_eq!(stats.min_value.as_deref(), Some("2"));
        assert_eq!(stats.max_value.as_deref(), Some("100"));
    }

    /// Negative values order below positive ones.
    #[test]
    fn min_and_max_handle_negative_numbers() {
        let batch = batch_int(vec![Some(5), Some(-40), Some(-3)]);
        let stats = analyze_batch(&batch);
        assert_eq!(stats.min_value.as_deref(), Some("-40"));
        assert_eq!(stats.max_value.as_deref(), Some("5"));
    }

    /// Cross-batch merging must stay typed too — the per-batch extremes are
    /// correct individually and could still be combined with a string compare.
    #[test]
    fn min_and_max_stay_numeric_across_batches() {
        let b1 = batch_int(vec![Some(9)]);
        let b2 = batch_int(vec![Some(10)]);
        let stats = analyze_record_batches([&b1, &b2]);
        assert_eq!(stats.min_value.as_deref(), Some("9"));
        assert_eq!(stats.max_value.as_deref(), Some("10"));
    }

    /// Types outside the old five-way match fell through to
    /// `format!("{:?}", array.slice(i, 1))` — the Debug of a one-row array.
    /// Decimal128 and Date32 are most of a TPC-H table, so their stats were
    /// counts of formatted blobs rather than of values.
    #[test]
    fn a_date_column_produces_real_values_not_debug_blobs() {
        use arrow::array::Date32Array;
        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Date32Array::from(vec![Some(19000), Some(18000)]))],
        )
        .unwrap();
        let stats = analyze_batch(&batch);
        let min = stats.min_value.expect("a date column must produce a min");
        assert!(
            !min.contains('\n') && !min.contains("PrimitiveArray"),
            "min is a Debug blob, not a value: {min:?}"
        );
        assert_eq!(min, "2019-04-14");
        assert_eq!(stats.distinct_count, Some(2));
    }

    /// Decimals must order by value, and render as decimals.
    #[test]
    fn a_decimal_column_orders_by_value() {
        use arrow::array::Decimal128Array;
        let array = Decimal128Array::from(vec![Some(925i128), Some(1050), Some(30)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "p",
            DataType::Decimal128(10, 2),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let stats = analyze_batch(&batch);
        assert_eq!(stats.min_value.as_deref(), Some("0.30"));
        assert_eq!(stats.max_value.as_deref(), Some("10.50"));
        assert_eq!(stats.distinct_count, Some(3));
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
