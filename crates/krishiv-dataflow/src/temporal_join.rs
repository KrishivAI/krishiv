//! Stream-table temporal (as-of) join (R16 S3.1).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use indexmap::IndexMap;

use crate::{ExecError, ExecResult};

/// Specification for a stream-table temporal join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalJoinSpec {
    pub stream_time_col: String,
    pub join_keys: Vec<String>,
    pub inner_join: bool,
}

/// Versioned table state per join key.
#[derive(Debug, Default)]
pub struct VersionedTableState {
    versions: BTreeMap<i64, RecordBatch>,
    lookback_ms: i64,
}

impl VersionedTableState {
    pub fn new(lookback_ms: i64) -> Self {
        Self {
            versions: BTreeMap::new(),
            lookback_ms,
        }
    }

    pub fn upsert_version(&mut self, version_ms: i64, batch: RecordBatch) {
        self.versions.insert(version_ms, batch);
        if self.lookback_ms > 0 {
            let min_version = version_ms.saturating_sub(self.lookback_ms);
            while let Some((&k, _)) = self.versions.first_key_value() {
                if k < min_version {
                    self.versions.pop_first();
                } else {
                    break;
                }
            }
        } else {
            // Zero lookback retains only the latest version snapshot.
            while self.versions.len() > 1 {
                self.versions.pop_first();
            }
        }
    }

    pub fn lookup_as_of(&self, stream_time_ms: i64) -> Option<&RecordBatch> {
        self.versions
            .range(..=stream_time_ms)
            .next_back()
            .map(|(_, b)| b)
    }
}

/// Default cap on the number of distinct per-key states retained in memory.
const DEFAULT_TEMPORAL_JOIN_MAX_KEYS: usize = 100_000;

/// Temporal join operator: joins stream events against versioned table state
/// using as-of semantics per join key.
pub struct TemporalJoinOperator {
    spec: TemporalJoinSpec,
    /// Per-key versioned table state.
    keyed_state: HashMap<String, VersionedTableState>,
    lookback_ms: i64,
    max_keys: usize,
    access_order: IndexMap<String, ()>,
}

impl TemporalJoinOperator {
    pub fn new(spec: TemporalJoinSpec, lookback_ms: i64) -> Self {
        Self {
            spec,
            keyed_state: HashMap::new(),
            lookback_ms,
            max_keys: DEFAULT_TEMPORAL_JOIN_MAX_KEYS,
            access_order: IndexMap::new(),
        }
    }

    /// Register or update a table version for a Utf8 join-key column value.
    ///
    /// For non-UTF8 or composite keys, use [`Self::upsert_version_encoded`] with
    /// the same encoding produced by [`build_join_key`].
    pub fn upsert_version(&mut self, join_key: &str, version_ms: i64, batch: RecordBatch) {
        self.upsert_version_encoded(format!("S{join_key}"), version_ms, batch);
    }

    /// Register or update table state using a pre-encoded join key.
    pub fn upsert_version_encoded(
        &mut self,
        encoded_key: String,
        version_ms: i64,
        batch: RecordBatch,
    ) {
        self.touch_key(&encoded_key);
        self.keyed_state
            .entry(encoded_key)
            .or_insert_with(|| VersionedTableState::new(self.lookback_ms))
            .upsert_version(version_ms, batch);
        self.maybe_evict();
    }

    fn touch_key(&mut self, key: &str) {
        self.access_order.shift_remove(key);
        self.access_order.insert(key.to_owned(), ());
    }

    fn maybe_evict(&mut self) {
        if self.access_order.len() > self.max_keys
            && let Some((oldest, _)) = self.access_order.shift_remove_index(0)
        {
            self.keyed_state.remove(&oldest);
        }
    }

    /// Join a stream batch against all registered table state using as-of semantics.
    ///
    /// For each row in the stream batch:
    /// 1. Extract the join key(s) and event time
    /// 2. Look up the latest table version ≤ event time for that key
    /// 3. If found: emit joined row (stream columns + table columns)
    /// 4. If not found and `inner_join=false`: emit stream row with null table columns
    /// 5. If not found and `inner_join=true`: skip the row
    pub fn join(&self, stream_batch: &RecordBatch) -> ExecResult<RecordBatch> {
        let time_col_idx = stream_batch
            .schema()
            .index_of(&self.spec.stream_time_col)
            .map_err(|_| {
                ExecError::Arrow(format!(
                    "stream time column '{}' not found in stream batch",
                    self.spec.stream_time_col
                ))
            })?;

        let time_col = stream_batch.column(time_col_idx);
        let timestamps = time_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                ExecError::Arrow(format!(
                    "stream time column '{}' is not Int64",
                    self.spec.stream_time_col
                ))
            })?;

        let join_key_indices: Vec<usize> =
            self.spec
                .join_keys
                .iter()
                .map(|key| {
                    stream_batch.schema().index_of(key).map_err(|_| {
                        ExecError::Arrow(format!("join key column '{}' not found", key))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

        let mut output_columns: Vec<Vec<ArrayRef>> = Vec::new();
        let mut table_schema: Option<SchemaRef> = None;

        for row in 0..stream_batch.num_rows() {
            let event_ts = timestamps.value(row);
            let join_key = build_join_key(stream_batch, &join_key_indices, row);

            if let Some(state) = self.keyed_state.get(&join_key)
                && let Some(table_version) = state.lookup_as_of(event_ts)
            {
                if table_schema.is_none() {
                    table_schema = Some(table_version.schema());
                }
                // Emit one joined output row per row in the table version snapshot.
                for table_row in 0..table_version.num_rows() {
                    let mut row_cols: Vec<ArrayRef> = Vec::new();
                    for col_idx in 0..stream_batch.num_columns() {
                        row_cols.push(slice_column(stream_batch.column(col_idx), row, 1));
                    }
                    for col_idx in 0..table_version.num_columns() {
                        row_cols.push(slice_column(table_version.column(col_idx), table_row, 1));
                    }
                    output_columns.push(row_cols);
                }
            } else if !self.spec.inner_join {
                // Left outer: emit stream row with null table columns.
                let mut row_cols: Vec<ArrayRef> = Vec::new();
                for col_idx in 0..stream_batch.num_columns() {
                    row_cols.push(slice_column(stream_batch.column(col_idx), row, 1));
                }
                // Resolve table schema from a previous match or from keyed state.
                let resolved_schema = table_schema.clone().or_else(|| {
                    self.keyed_state.values().find_map(|state| {
                        state.versions.values().next().map(|batch| batch.schema())
                    })
                });
                if let Some(ref ts) = resolved_schema {
                    if table_schema.is_none() {
                        table_schema = Some(ts.clone());
                    }
                    for f_ref in ts.fields() {
                        let dt = f_ref.data_type();
                        row_cols.push(arrow::array::new_null_array(dt, 1));
                    }
                    output_columns.push(row_cols);
                } else {
                    // Table schema is not yet known (no table side data has arrived).
                    // This stream row is dropped rather than emitted with unknown-width
                    // null columns. In a left outer join this is a data loss risk;
                    // operators should buffer the stream side until the table schema
                    // is available. Log so this condition is observable.
                    tracing::warn!(
                        "temporal_join: left-outer row dropped — table schema unknown (no table data seen yet)"
                    );
                }
            }
        }

        if output_columns.is_empty() {
            let table_schema = table_schema.or_else(|| {
                self.keyed_state
                    .values()
                    .find_map(|state| state.versions.values().next().map(|batch| batch.schema()))
            });
            let schema =
                build_joined_schema(stream_batch.schema(), table_schema, !self.spec.inner_join)?;
            return Ok(RecordBatch::new_empty(schema));
        }

        // Combine all row columns into full columns.
        let total_cols = output_columns
            .first()
            .map(|r| r.len())
            .unwrap_or(0);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(total_cols);
        for col_idx in 0..total_cols {
            let col_arrays: Vec<_> = output_columns
                .iter()
                .map(|row| {
                    row.get(col_idx)
                        .cloned()
                        .ok_or_else(|| ExecError::Arrow(format!("col {col_idx} out of range")))
                })
                .collect::<ExecResult<_>>()?;
            arrays.push(arrow::compute::concat(
                &col_arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
            )?);
        }

        let schema =
            build_joined_schema(stream_batch.schema(), table_schema, !self.spec.inner_join)?;

        RecordBatch::try_new(schema, arrays).map_err(|e| ExecError::Arrow(e.to_string()))
    }
}

/// Build a composite join key using ASCII record separator `\x1c` to avoid
/// ambiguity with user data that may contain `|` or other printable characters.
fn build_join_key(batch: &RecordBatch, key_indices: &[usize], row: usize) -> String {
    key_indices
        .iter()
        .map(|&idx| format_column_value(batch.column(idx), row))
        .collect::<Vec<_>>()
        .join("\x1c")
}

fn format_column_value(array: &dyn Array, row: usize) -> String {
    // Tag every value with a type prefix so SQL NULLs and the string "NULL"
    // cannot produce the same join key.  Each variant uses a single-byte ASCII
    // control character that cannot appear in normal string data:
    //   \x00  = SQL NULL
    //   I<n>  = Int64 n
    //   S<s>  = Utf8 string s
    //   ?     = unsupported type (cannot match any real value)
    if array.is_null(row) {
        return "\x00".to_owned();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return format!("I{}", arr.value(row));
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return format!("S{}", arr.value(row));
    }
    "?".to_owned()
}

fn slice_column(array: &ArrayRef, offset: usize, length: usize) -> ArrayRef {
    array.slice(offset, length)
}

/// Build the schema for joined output rows.
///
/// `table_columns_nullable` must be `true` for left-outer joins: unmatched
/// stream rows emit `null` table-derived columns (see the "Left outer" branch
/// in [`TemporalJoinOperator::join`]), so the table-derived fields must be
/// declared nullable regardless of the source table schema's own nullability —
/// otherwise `RecordBatch::try_new` rejects the batch with "declared as
/// non-nullable but contains null values".
fn build_joined_schema(
    stream_schema: SchemaRef,
    table_schema: Option<SchemaRef>,
    table_columns_nullable: bool,
) -> ExecResult<SchemaRef> {
    let stream_fields: Vec<Field> = stream_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut fields = stream_fields;
    if let Some(ts) = &table_schema {
        for f_ref in ts.fields() {
            let f = f_ref.as_ref();
            let name = if stream_schema.field_with_name(f.name()).is_ok() {
                format!("table_{}", f.name())
            } else {
                f.name().clone()
            };
            let nullable = f.is_nullable() || table_columns_nullable;
            fields.push(Field::new(name, f.data_type().clone(), nullable));
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    // ── VersionedTableState tests ───────────────────────────────────────────

    fn version_batch(v: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![v]))],
        )
        .unwrap()
    }

    fn stream_batch(ids: Vec<&str>, timestamps: Vec<i64>) -> RecordBatch {
        let len = ids.len();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("ts", DataType::Int64, false),
                Field::new("val", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(Int64Array::from((0..len as i64).collect::<Vec<i64>>())),
            ],
        )
        .unwrap()
    }

    fn table_batch(id: &str, value: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("score", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![id])),
                Arc::new(Int64Array::from(vec![value])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn as_of_lookup_returns_latest_valid_version() {
        let mut state = VersionedTableState::new(10_000);
        state.upsert_version(1000, version_batch(1));
        state.upsert_version(2000, version_batch(2));
        assert_eq!(
            state
                .lookup_as_of(2500)
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(
            state
                .lookup_as_of(1500)
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        assert!(state.lookup_as_of(500).is_none());
    }

    #[test]
    fn upsert_evicts_old_versions_beyond_lookback() {
        let mut state = VersionedTableState::new(5000);
        state.upsert_version(1000, version_batch(1));
        state.upsert_version(2000, version_batch(2));
        state.upsert_version(7000, version_batch(3));
        assert!(state.lookup_as_of(1000).is_none());
        assert!(state.lookup_as_of(2000).is_some());
        assert!(state.lookup_as_of(7000).is_some());
    }

    #[test]
    fn upsert_exact_lookback_boundary() {
        let mut state = VersionedTableState::new(1000);
        state.upsert_version(1000, version_batch(1));
        state.upsert_version(2000, version_batch(2));
        assert!(state.lookup_as_of(1000).is_some());
    }

    #[test]
    fn lookup_as_of_exact_version_match() {
        let mut state = VersionedTableState::new(10_000);
        state.upsert_version(5000, version_batch(42));
        assert_eq!(
            state
                .lookup_as_of(5000)
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );
    }

    #[test]
    fn lookup_as_of_between_versions_returns_previous() {
        let mut state = VersionedTableState::new(10_000);
        state.upsert_version(1000, version_batch(10));
        state.upsert_version(3000, version_batch(30));
        assert_eq!(
            state
                .lookup_as_of(2000)
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            10
        );
    }

    #[test]
    fn lookup_as_of_empty_state_returns_none() {
        let state = VersionedTableState::new(10_000);
        assert!(state.lookup_as_of(1000).is_none());
    }

    #[test]
    fn upsert_replaces_same_version() {
        let mut state = VersionedTableState::new(10_000);
        state.upsert_version(1000, version_batch(1));
        state.upsert_version(1000, version_batch(99));
        let val = state
            .lookup_as_of(1000)
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(val, 99);
    }

    #[test]
    fn large_lookback_keeps_all_versions() {
        let mut state = VersionedTableState::new(i64::MAX as u64 as i64);
        state.upsert_version(0, version_batch(0));
        state.upsert_version(1000, version_batch(1));
        state.upsert_version(100_000, version_batch(2));
        assert!(state.lookup_as_of(0).is_some());
        assert!(state.lookup_as_of(1000).is_some());
        assert!(state.lookup_as_of(100_000).is_some());
    }

    #[test]
    fn zero_lookback_evicts_all_except_latest() {
        let mut state = VersionedTableState::new(0);
        state.upsert_version(1000, version_batch(1));
        state.upsert_version(2000, version_batch(2));
        assert!(state.lookup_as_of(1000).is_none());
        assert!(state.lookup_as_of(2000).is_some());
    }

    #[test]
    fn temporal_join_spec_fields() {
        let spec = TemporalJoinSpec {
            stream_time_col: "event_ts".into(),
            join_keys: vec!["id".into()],
            inner_join: true,
        };
        assert_eq!(spec.stream_time_col, "event_ts");
        assert_eq!(spec.join_keys, vec!["id"]);
        assert!(spec.inner_join);
    }

    // ── TemporalJoinOperator tests ──────────────────────────────────────────

    #[test]
    fn temporal_join_inner_join_matches() {
        let spec = TemporalJoinSpec {
            stream_time_col: "ts".into(),
            join_keys: vec!["id".into()],
            inner_join: true,
        };
        let mut op = TemporalJoinOperator::new(spec, 10_000);

        // Register table state for key "a".
        op.upsert_version("a", 1000, table_batch("a", 100));
        op.upsert_version("a", 2000, table_batch("a", 200));

        // Stream event at ts=2500 for key "a" → should match table version at 2000.
        let stream = stream_batch(vec!["a"], vec![2500]);
        let result = op.join(&stream).unwrap();
        assert_eq!(result.num_rows(), 1);
        // 3 stream cols + 2 table cols = 5 total.
        assert_eq!(result.num_columns(), 5);
        // The table "score" column should be 200 (from version 2000).
        let score_col = result.column(result.schema().index_of("score").unwrap());
        let scores = score_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(scores.value(0), 200);
    }

    #[test]
    fn temporal_join_inner_no_match_produces_empty() {
        let spec = TemporalJoinSpec {
            stream_time_col: "ts".into(),
            join_keys: vec!["id".into()],
            inner_join: true,
        };
        let mut op = TemporalJoinOperator::new(spec, 10_000);

        // Register only key "b", stream has key "a" → no match.
        op.upsert_version("b", 1000, table_batch("b", 50));

        let stream = stream_batch(vec!["a"], vec![2000]);
        let result = op.join(&stream).unwrap();
        assert_eq!(
            result.num_rows(),
            0,
            "inner join with no match must return empty"
        );
    }

    #[test]
    fn temporal_join_left_outer_unmatched_row_emits_null_table_columns() {
        let spec = TemporalJoinSpec {
            stream_time_col: "ts".into(),
            join_keys: vec!["id".into()],
            inner_join: false,
        };
        let mut op = TemporalJoinOperator::new(spec, 10_000);

        // Only key "a" has table state; "b" never appears in keyed_state, so
        // it must fall through to the left-outer "no match" branch.
        op.upsert_version("a", 1000, table_batch("a", 100));

        let stream = stream_batch(vec!["a", "b"], vec![1500, 1500]);
        let result = op.join(&stream).unwrap();

        assert_eq!(
            result.num_rows(),
            2,
            "left outer join must emit a row for every stream row, matched or not"
        );
        assert_eq!(result.num_columns(), 5);

        let score_col = result.column(result.schema().index_of("score").unwrap());
        let scores = score_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            scores.value(0),
            100,
            "matched row carries the joined table value"
        );
        assert!(
            scores.is_null(1),
            "unmatched row must carry null table columns rather than being dropped"
        );
    }

    #[test]
    fn temporal_join_as_of_returns_previous_version() {
        let spec = TemporalJoinSpec {
            stream_time_col: "ts".into(),
            join_keys: vec!["id".into()],
            inner_join: true,
        };
        let mut op = TemporalJoinOperator::new(spec, 10_000);

        // Table has versions at 1000 and 3000. Stream at 2000 → should get version 1000.
        op.upsert_version("k1", 1000, table_batch("k1", 10));
        op.upsert_version("k1", 3000, table_batch("k1", 30));

        let stream = stream_batch(vec!["k1"], vec![2000]);
        let result = op.join(&stream).unwrap();
        assert_eq!(result.num_rows(), 1);
        let score_col = result.column(result.schema().index_of("score").unwrap());
        let scores = score_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(scores.value(0), 10);
    }

    #[test]
    fn temporal_join_with_duplicate_join_keys() {
        let spec = TemporalJoinSpec {
            stream_time_col: "ts".into(),
            join_keys: vec!["id".into()],
            inner_join: true,
        };
        let mut op = TemporalJoinOperator::new(spec, 10_000);

        op.upsert_version("a", 1000, table_batch("a", 10));
        op.upsert_version("b", 1000, table_batch("b", 20));

        // Two stream events for different keys.
        let stream = stream_batch(vec!["a", "b"], vec![1500, 1500]);
        let result = op.join(&stream).unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn temporal_join_multi_row_table_version_emits_one_row_per_table_row() {
        // A table version batch with 2 rows — each stream event should produce
        // 2 joined output rows (one per table row), not just row 0.
        let spec = TemporalJoinSpec {
            stream_time_col: "ts".into(),
            join_keys: vec!["id".into()],
            inner_join: true,
        };
        let mut op = TemporalJoinOperator::new(spec, 10_000);

        // Table version with 2 rows for key "a".
        let two_row_table = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", arrow::datatypes::DataType::Utf8, false),
                Field::new("score", arrow::datatypes::DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        op.upsert_version("a", 1000, two_row_table);

        let stream = stream_batch(vec!["a"], vec![2000]);
        let result = op.join(&stream).unwrap();
        // Must emit 2 rows: one for each table row.
        assert_eq!(
            result.num_rows(),
            2,
            "multi-row table version must produce one output row per table row"
        );
        let score_col = result.column(result.schema().index_of("score").unwrap());
        let scores = score_col.as_any().downcast_ref::<Int64Array>().unwrap();
        let mut score_vals: Vec<i64> = (0..2).map(|i| scores.value(i)).collect();
        score_vals.sort();
        assert_eq!(score_vals, vec![10, 20]);
    }

    #[test]
    fn temporal_join_key_containing_pipe_not_confused_with_separator() {
        // Join key value contains "|" — must not be confused with composite key separator.
        let spec = TemporalJoinSpec {
            stream_time_col: "ts".into(),
            join_keys: vec!["id".into()],
            inner_join: true,
        };
        let mut op = TemporalJoinOperator::new(spec, 10_000);

        // Register table state for key "a|b" (contains a pipe).
        op.upsert_version("a|b", 1000, table_batch("a|b", 42));

        // Also register for key "a" to ensure "a|b" doesn't match "a".
        op.upsert_version("a", 1000, table_batch("a", 99));

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Utf8, false),
            Field::new("ts", arrow::datatypes::DataType::Int64, false),
            Field::new("val", arrow::datatypes::DataType::Int64, false),
        ]));
        let stream = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a|b"])),
                Arc::new(Int64Array::from(vec![1500i64])),
                Arc::new(Int64Array::from(vec![0i64])),
            ],
        )
        .unwrap();
        let result = op.join(&stream).unwrap();
        assert_eq!(result.num_rows(), 1);
        let score_col = result.column(result.schema().index_of("score").unwrap());
        let scores = score_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            scores.value(0),
            42,
            "key 'a|b' must match its own table version, not key 'a'"
        );
    }
}
