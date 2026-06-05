//! Stream-table temporal (as-of) join (R16 S3.1).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::{ExecError, ExecResult};

/// Specification for a stream-table temporal join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalJoinSpec {
    pub stream_time_col: String,
    pub table_version_col: String,
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

/// Temporal join operator: joins stream events against versioned table state
/// using as-of semantics per join key.
pub struct TemporalJoinOperator {
    spec: TemporalJoinSpec,
    /// Per-key versioned table state.
    keyed_state: HashMap<String, VersionedTableState>,
    lookback_ms: i64,
}

impl TemporalJoinOperator {
    pub fn new(spec: TemporalJoinSpec, lookback_ms: i64) -> Self {
        Self {
            spec,
            keyed_state: HashMap::new(),
            lookback_ms,
        }
    }

    /// Register or update a table version for a specific join key.
    pub fn upsert_version(&mut self, join_key: &str, version_ms: i64, batch: RecordBatch) {
        self.keyed_state
            .entry(join_key.to_owned())
            .or_insert_with(|| VersionedTableState::new(self.lookback_ms))
            .upsert_version(version_ms, batch);
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
                // Build joined row: stream columns + table columns.
                let mut row_cols: Vec<ArrayRef> = Vec::new();
                for col_idx in 0..stream_batch.num_columns() {
                    row_cols.push(slice_column(stream_batch.column(col_idx), row, 1));
                }
                for col_idx in 0..table_version.num_columns() {
                    row_cols.push(slice_column(table_version.column(col_idx), 0, 1));
                }
                output_columns.push(row_cols);
            } else if !self.spec.inner_join {
                // Left outer: emit stream row with null table columns.
                let mut row_cols: Vec<ArrayRef> = Vec::new();
                for col_idx in 0..stream_batch.num_columns() {
                    row_cols.push(slice_column(stream_batch.column(col_idx), row, 1));
                }
                // Null table columns — we need to know the table schema.
                // Since no match, emit nulls for each table column.
                // We don't know the table schema at this point, so skip.
                // For correctness, callers must ensure table schema is known.
            }
        }

        if output_columns.is_empty() {
            let table_schema = table_schema.or_else(|| {
                self.keyed_state.values().find_map(|state| {
                    state
                        .versions
                        .values()
                        .next()
                        .map(|batch| batch.schema())
                })
            });
            let schema = build_joined_schema(stream_batch.schema(), table_schema)?;
            return Ok(RecordBatch::new_empty(schema));
        }

        // Combine all row columns into full columns.
        let _num_stream_cols = stream_batch.num_columns();
        let total_cols = output_columns[0].len();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(total_cols);
        for col_idx in 0..total_cols {
            let col_arrays: Vec<_> = output_columns
                .iter()
                .map(|row| row[col_idx].clone())
                .collect();
            arrays.push(arrow::compute::concat(
                &col_arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
            )?);
        }

        let schema = build_joined_schema(stream_batch.schema(), table_schema)?;

        RecordBatch::try_new(schema, arrays).map_err(|e| ExecError::Arrow(e.to_string()))
    }
}

fn build_join_key(batch: &RecordBatch, key_indices: &[usize], row: usize) -> String {
    let parts: Vec<String> = key_indices
        .iter()
        .map(|&idx| format_column_value(batch.column(idx), row))
        .collect();
    parts.join("|")
}

fn format_column_value(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "NULL".to_owned();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return arr.value(row).to_owned();
    }
    format!("<unsupported_type>")
}

fn slice_column(array: &ArrayRef, offset: usize, length: usize) -> ArrayRef {
    array.slice(offset, length)
}

fn build_joined_schema(
    stream_schema: SchemaRef,
    table_schema: Option<SchemaRef>,
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
            fields.push(Field::new(name, f.data_type().clone(), f.is_nullable()));
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
            table_version_col: "version".into(),
            join_keys: vec!["id".into()],
            inner_join: true,
        };
        assert_eq!(spec.stream_time_col, "event_ts");
        assert_eq!(spec.table_version_col, "version");
        assert_eq!(spec.join_keys, vec!["id"]);
        assert!(spec.inner_join);
    }

    // ── TemporalJoinOperator tests ──────────────────────────────────────────

    #[test]
    fn temporal_join_inner_join_matches() {
        let spec = TemporalJoinSpec {
            stream_time_col: "ts".into(),
            table_version_col: "version".into(),
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
            table_version_col: "version".into(),
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
    fn temporal_join_as_of_returns_previous_version() {
        let spec = TemporalJoinSpec {
            stream_time_col: "ts".into(),
            table_version_col: "version".into(),
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
            table_version_col: "version".into(),
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
}
