use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::{ExecError, ExecResult};

/// Typed group-by / join key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggKey {
    Int32(i32),
    Int64(i64),
    /// `f64` stored as IEEE-754 bits for total-order hashing.
    Float64(u64),
    Utf8(String),
    Bool(bool),
}

impl fmt::Display for AggKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int32(v) => write!(f, "{v}"),
            Self::Int64(v) => write!(f, "{v}"),
            Self::Float64(bits) => write!(f, "{}", f64::from_bits(*bits)),
            Self::Utf8(s) => f.write_str(s),
            Self::Bool(v) => write!(f, "{v}"),
        }
    }
}

impl AggKey {
    pub(crate) fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Int32(a), Self::Int32(b)) => a.cmp(b),
            (Self::Int64(a), Self::Int64(b)) => a.cmp(b),
            (Self::Float64(a), Self::Float64(b)) => a.cmp(b),
            (Self::Utf8(a), Self::Utf8(b)) => a.cmp(b),
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            (a, b) => a.discriminant().cmp(&b.discriminant()),
        }
    }

    fn discriminant(&self) -> u8 {
        match self {
            Self::Int32(_) => 0,
            Self::Int64(_) => 1,
            Self::Float64(_) => 2,
            Self::Utf8(_) => 3,
            Self::Bool(_) => 4,
        }
    }
}

/// Extract a typed [`AggKey`] from one column at `row`.
///
/// Supported types: `Int32`, `Int64`, `Float64`, `Utf8`, `Bool`.
/// Avoids heap allocation for integer and boolean keys.
pub fn extract_agg_key(batch: &RecordBatch, col_idx: usize, row: usize) -> ExecResult<AggKey> {
    if col_idx >= batch.num_columns() {
        return Err(ExecError::InvalidInput(format!(
            "group key column index {col_idx} is out of bounds for {} columns",
            batch.num_columns()
        )));
    }
    if row >= batch.num_rows() {
        return Err(ExecError::InvalidInput(format!(
            "group key row index {row} is out of bounds for {} rows",
            batch.num_rows()
        )));
    }

    let col = batch.column(col_idx);
    if col.is_null(row) {
        return Err(ExecError::InvalidInput(format!(
            "group key column '{}' contains null at row {row}",
            batch.schema().field(col_idx).name()
        )));
    }

    match col.data_type() {
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                ExecError::UnsupportedType("declared Int32 key failed downcast".into())
            })?;
            Ok(AggKey::Int32(arr.value(row)))
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                ExecError::UnsupportedType("declared Int64 key failed downcast".into())
            })?;
            Ok(AggKey::Int64(arr.value(row)))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                ExecError::UnsupportedType("declared Float64 key failed downcast".into())
            })?;
            Ok(AggKey::Float64(arr.value(row).to_bits()))
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                ExecError::UnsupportedType("declared Utf8 key failed downcast".into())
            })?;
            Ok(AggKey::Utf8(arr.value(row).to_string()))
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                ExecError::UnsupportedType("declared Bool key failed downcast".into())
            })?;
            Ok(AggKey::Bool(arr.value(row)))
        }
        other => Err(ExecError::UnsupportedType(format!(
            "unsupported group key type: {other}"
        ))),
    }
}

/// Build the output schema for a join: all left fields + right fields minus the
/// right join key column.
pub(crate) fn join_output_schema(
    left: &RecordBatch,
    right: &RecordBatch,
    right_key: &str,
) -> Arc<Schema> {
    let mut fields: Vec<Field> = left
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    for field in right.schema().fields() {
        if field.name() != right_key {
            fields.push(field.as_ref().clone());
        }
    }
    Arc::new(Schema::new(fields))
}

fn join_output_schema_multi(
    left: &RecordBatch,
    right: &RecordBatch,
    right_keys: &[String],
) -> Arc<Schema> {
    // Cache key-set lookup to O(1) per field rather than O(k) linear scan.
    let right_key_set: std::collections::HashSet<&str> =
        right_keys.iter().map(|s| s.as_str()).collect();
    let mut fields: Vec<Field> = left
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    for f in right.schema().fields() {
        if right_key_set.contains(f.name().as_str()) {
            continue;
        }
        let name = if left.schema().field_with_name(f.name()).is_ok() {
            format!("right_{}", f.name())
        } else {
            f.name().clone()
        };
        fields.push(Field::new(
            name,
            f.as_ref().data_type().clone(),
            f.as_ref().is_nullable(),
        ));
    }
    Arc::new(Schema::new(fields))
}

/// Build the output `RecordBatch` from match index vectors.
pub(crate) fn build_join_batch(
    left: &RecordBatch,
    right: &RecordBatch,
    right_key: &str,
    left_indices: &[u32],
    right_indices: &[u32],
    out_schema: Arc<Schema>,
) -> ExecResult<RecordBatch> {
    let left_idx_arr = UInt32Array::from(left_indices.to_vec());
    let right_idx_arr = UInt32Array::from(right_indices.to_vec());

    let mut columns: Vec<ArrayRef> = Vec::new();

    // All left columns.
    for col in left.columns() {
        let taken = take(col.as_ref(), &left_idx_arr, None)?;
        columns.push(taken);
    }

    // Right columns excluding the right join key.
    let right_schema = right.schema();
    for (i, field) in right_schema.fields().iter().enumerate() {
        if field.name() != right_key {
            let taken = take(right.column(i).as_ref(), &right_idx_arr, None)?;
            columns.push(taken);
        }
    }

    Ok(RecordBatch::try_new(out_schema, columns)?)
}

// ── JoinKind ──────────────────────────────────────────────────────────────────

/// Join type for control over non-matching rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    /// Inner join — only emit rows where keys match on both sides.
    Inner,
    /// Left outer join — emit all left rows; right columns are null for unmatched.
    LeftOuter,
    /// Left semi join — emit left rows that have at least one match on the right.
    LeftSemi,
    /// Left anti join — emit left rows that have no match on the right.
    LeftAnti,
}

/// Composite multi-key for use with `HashJoin` when multiple key columns
/// participate in the join condition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompositeKey(Vec<AggKey>);

impl std::fmt::Display for CompositeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts: Vec<String> = self.0.iter().map(|k| k.to_string()).collect();
        write!(f, "{}", parts.join("|"))
    }
}

impl CompositeKey {
    pub fn new(keys: Vec<AggKey>) -> Self {
        Self(keys)
    }
}

// ── HashJoin ──────────────────────────────────────────────────────────────────

/// Equi-join on one or more named key columns with configurable join kind.
pub struct HashJoin {
    left_keys: Vec<String>,
    right_keys: Vec<String>,
    kind: JoinKind,
}

impl HashJoin {
    /// Create a new `HashJoin` with a single key column (inner join).
    pub fn new(left_key: impl Into<String>, right_key: impl Into<String>) -> Self {
        Self {
            left_keys: vec![left_key.into()],
            right_keys: vec![right_key.into()],
            kind: JoinKind::Inner,
        }
    }

    /// Multi-key join: `left_keys[i] = right_keys[i]`.
    pub fn with_multi_keys(left_keys: Vec<String>, right_keys: Vec<String>) -> Self {
        Self {
            left_keys,
            right_keys,
            kind: JoinKind::Inner,
        }
    }

    /// Set the join kind.
    #[must_use]
    pub fn with_kind(mut self, kind: JoinKind) -> Self {
        self.kind = kind;
        self
    }

    fn key_indices(batch: &RecordBatch, keys: &[String]) -> ExecResult<Vec<usize>> {
        keys.iter()
            .map(|k| {
                batch
                    .schema()
                    .index_of(k)
                    .map_err(|_| ExecError::ColumnNotFound(k.clone()))
            })
            .collect()
    }

    fn build_composite_key(
        batch: &RecordBatch,
        key_indices: &[usize],
        row: usize,
    ) -> ExecResult<CompositeKey> {
        let keys: Result<Vec<AggKey>, _> = key_indices
            .iter()
            .map(|&idx| extract_agg_key(batch, idx, row))
            .collect();
        Ok(CompositeKey::new(keys?))
    }

    pub fn join(&self, left: &RecordBatch, right: &RecordBatch) -> ExecResult<RecordBatch> {
        let out_schema = join_output_schema_multi(left, right, &self.right_keys);

        let left_key_indices = Self::key_indices(left, &self.left_keys)?;
        let right_key_indices = Self::key_indices(right, &self.right_keys)?;

        // Build phase.
        let mut build_map: HashMap<CompositeKey, Vec<u32>> =
            HashMap::with_capacity(right.num_rows());
        for row in 0..right.num_rows() {
            let key = Self::build_composite_key(right, &right_key_indices, row)?;
            build_map.entry(key).or_default().push(row as u32);
        }

        // Probe phase.
        let mut left_indices: Vec<u32> = Vec::new();
        let mut right_indices: Vec<u32> = Vec::new();
        let mut unmatched_left: Vec<u32> = Vec::new();

        for row in 0..left.num_rows() {
            let key = Self::build_composite_key(left, &left_key_indices, row)?;
            if let Some(right_rows) = build_map.get(&key) {
                for &r in right_rows {
                    left_indices.push(row as u32);
                    right_indices.push(r);
                }
            } else if matches!(self.kind, JoinKind::LeftOuter) {
                unmatched_left.push(row as u32);
            } else if self.kind == JoinKind::LeftSemi {
                // Already captured above via left_indices; nothing extra.
            }
        }

        match self.kind {
            JoinKind::LeftSemi | JoinKind::LeftAnti => {
                build_semi_anti_batch(left, &build_map, &left_key_indices, self.kind, out_schema)
            }
            _ => {
                if left_indices.is_empty() && unmatched_left.is_empty() {
                    return Ok(RecordBatch::new_empty(out_schema));
                }
                build_outer_join_batch(
                    left,
                    right,
                    &self.right_keys,
                    &left_indices,
                    &right_indices,
                    &unmatched_left,
                    out_schema,
                )
            }
        }
    }
}

// ── BroadcastJoin ─────────────────────────────────────────────────────────────

/// Broadcast inner join: the smaller (build) side is broadcast to all partitions.
pub struct BroadcastJoin {
    join_key: String,
}

impl BroadcastJoin {
    /// Create a new `BroadcastJoin` with the given join key column name.
    pub fn new(join_key: impl Into<String>) -> Self {
        Self {
            join_key: join_key.into(),
        }
    }

    /// Build from the broadcast (smaller) side.
    ///
    /// The batch is stored behind an `Arc` so multiple probes share the same
    /// backing allocation without per-probe copies.
    pub fn build(self, broadcast_batch: Arc<RecordBatch>) -> ExecResult<BuiltBroadcastJoin> {
        let key_idx = broadcast_batch
            .schema()
            .index_of(&self.join_key)
            .map_err(|_| ExecError::ColumnNotFound(self.join_key.clone()))?;

        let mut index: HashMap<AggKey, Vec<u32>> = HashMap::new();
        for row in 0..broadcast_batch.num_rows() {
            let key = extract_agg_key(&broadcast_batch, key_idx, row)?;
            index.entry(key).or_default().push(row as u32);
        }

        Ok(BuiltBroadcastJoin {
            join_key: self.join_key,
            broadcast: broadcast_batch,
            index,
        })
    }
}

/// A pre-built broadcast join table ready to probe.
pub struct BuiltBroadcastJoin {
    join_key: String,
    broadcast: Arc<RecordBatch>,
    /// Pre-built hash map: typed key → broadcast (right) row indices.
    index: HashMap<AggKey, Vec<u32>>,
}

impl BuiltBroadcastJoin {
    /// Inner join a probe-side batch against the pre-built broadcast table.
    ///
    /// Output schema: all probe columns + all broadcast columns (excluding the
    /// broadcast join key).
    pub fn probe(&self, probe: &RecordBatch) -> ExecResult<RecordBatch> {
        let out_schema = join_output_schema(probe, &self.broadcast, &self.join_key);

        let probe_key_idx = probe
            .schema()
            .index_of(&self.join_key)
            .map_err(|_| ExecError::ColumnNotFound(self.join_key.clone()))?;

        let mut left_indices: Vec<u32> = Vec::new();
        let mut right_indices: Vec<u32> = Vec::new();

        for row in 0..probe.num_rows() {
            let key = extract_agg_key(probe, probe_key_idx, row)?;
            if let Some(broadcast_rows) = self.index.get(&key) {
                for &r in broadcast_rows {
                    left_indices.push(row as u32);
                    right_indices.push(r);
                }
            }
        }

        if left_indices.is_empty() {
            return Ok(RecordBatch::new_empty(out_schema));
        }

        build_join_batch(
            probe,
            &self.broadcast,
            &self.join_key,
            &left_indices,
            &right_indices,
            out_schema,
        )
    }
}

// ── StreamTableJoin ───────────────────────────────────────────────────────────

/// Stream-table (stream-static) join operator (R5.2).
///
/// The `table` side is a static `RecordBatch` loaded at job startup.
/// Each streaming batch is inner-joined against the table on `join_key_column`.
/// This is a baseline nested-loop join; hash-join optimisation is post-R5.2.
pub struct StreamTableJoin {
    /// Static table side of the join.
    table: RecordBatch,
    /// Column name present in both the stream batch and the table.
    join_key_column: String,
    /// Cached hash map for the table side, built lazily on first use.
    cached_index: Option<Arc<HashMap<AggKey, Vec<u32>>>>,
}

impl StreamTableJoin {
    /// Create a stream-table join with the given static table.
    pub fn new(table: RecordBatch, join_key_column: impl Into<String>) -> Self {
        Self {
            table,
            join_key_column: join_key_column.into(),
            cached_index: None,
        }
    }

    fn table_index(&mut self) -> ExecResult<Arc<HashMap<AggKey, Vec<u32>>>> {
        if let Some(ref cached) = self.cached_index {
            return Ok(Arc::clone(cached));
        }
        let table_key_idx = self
            .table
            .schema()
            .index_of(&self.join_key_column)
            .map_err(|_| ExecError::ColumnNotFound(self.join_key_column.clone()))?;
        let mut index: HashMap<AggKey, Vec<u32>> = HashMap::new();
        for row in 0..self.table.num_rows() {
            let key = extract_agg_key(&self.table, table_key_idx, row)?;
            index.entry(key).or_default().push(row as u32);
        }
        let index = Arc::new(index);
        self.cached_index = Some(Arc::clone(&index));
        Ok(index)
    }

    /// Join `stream_batch` against the static table, returning the inner-join result.
    ///
    /// Output schema is the union of all columns from both sides.  If the same
    /// column name appears in both, the stream column takes precedence and the
    /// table column is dropped.
    pub fn process_batch(&mut self, stream_batch: &RecordBatch) -> ExecResult<RecordBatch> {
        let stream_key_idx = stream_batch
            .schema()
            .index_of(&self.join_key_column)
            .map_err(|_| ExecError::ColumnNotFound(self.join_key_column.clone()))?;
        let table_key_idx = self
            .table
            .schema()
            .index_of(&self.join_key_column)
            .map_err(|_| ExecError::ColumnNotFound(self.join_key_column.clone()))?;

        let table_index = self.table_index()?;

        // Collect matching (stream_row, table_row) index pairs.
        let mut stream_rows: Vec<u32> = Vec::new();
        let mut table_rows: Vec<u32> = Vec::new();
        for s_row in 0..stream_batch.num_rows() {
            let key = extract_agg_key(stream_batch, stream_key_idx, s_row)?;
            if let Some(t_rows) = table_index.get(&key) {
                for &t_row in t_rows {
                    stream_rows.push(s_row as u32);
                    table_rows.push(t_row);
                }
            }
        }

        if stream_rows.is_empty() {
            return self.empty_output(stream_batch, table_key_idx);
        }

        let stream_indices: ArrayRef = Arc::new(UInt32Array::from(stream_rows));
        let table_indices: ArrayRef = Arc::new(UInt32Array::from(table_rows));

        // Build output schema: all stream columns, then non-key table columns.
        let mut fields: Vec<Field> = stream_batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        for (i, f) in self.table.schema().fields().iter().enumerate() {
            if i != table_key_idx {
                fields.push(f.as_ref().clone());
            }
        }
        let schema = Arc::new(Schema::new(fields));

        let mut columns: Vec<ArrayRef> =
            Vec::with_capacity(stream_batch.columns().len() + self.table.columns().len());
        for col in stream_batch.columns() {
            columns.push(arrow::compute::take(col.as_ref(), &stream_indices, None)?);
        }
        for (i, col) in self.table.columns().iter().enumerate() {
            if i != table_key_idx {
                columns.push(arrow::compute::take(col.as_ref(), &table_indices, None)?);
            }
        }

        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn empty_output(
        &self,
        stream_batch: &RecordBatch,
        table_key_idx: usize,
    ) -> ExecResult<RecordBatch> {
        let mut fields: Vec<Field> = stream_batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        for (i, f) in self.table.schema().fields().iter().enumerate() {
            if i != table_key_idx {
                fields.push(f.as_ref().clone());
            }
        }
        let schema = Arc::new(Schema::new(fields));
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|f| arrow::array::new_empty_array(f.data_type()))
            .collect();
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

// ── Helper: semi/anti join batch builder ───────────────────────────────────

fn build_semi_anti_batch(
    left: &RecordBatch,
    build_map: &std::collections::HashMap<CompositeKey, Vec<u32>>,
    left_key_indices: &[usize],
    kind: JoinKind,
    out_schema: Arc<Schema>,
) -> ExecResult<RecordBatch> {
    let mut keep_rows: Vec<u32> = Vec::new();
    for row in 0..left.num_rows() {
        let key = build_composite_key_static(left, left_key_indices, row)?;
        let has_match = build_map.contains_key(&key);
        match kind {
            JoinKind::LeftSemi if has_match => keep_rows.push(row as u32),
            JoinKind::LeftAnti if !has_match => keep_rows.push(row as u32),
            _ => {}
        }
    }
    if keep_rows.is_empty() {
        return Ok(RecordBatch::new_empty(out_schema));
    }
    let indices: ArrayRef = Arc::new(UInt32Array::from(keep_rows));
    let columns: Vec<ArrayRef> = (0..left.num_columns())
        .map(|i| take(left.column(i), &indices, None).map_err(|e| ExecError::Arrow(e.to_string())))
        .collect::<ExecResult<Vec<_>>>()?;
    RecordBatch::try_new(out_schema, columns).map_err(|e| ExecError::Arrow(e.to_string()))
}

fn build_outer_join_batch(
    left: &RecordBatch,
    right: &RecordBatch,
    right_keys: &[String],
    left_indices: &[u32],
    right_indices: &[u32],
    unmatched_left: &[u32],
    out_schema: Arc<Schema>,
) -> ExecResult<RecordBatch> {
    // Left columns: matched rows first, then unmatched rows.
    let mut all_left: Vec<u32> = left_indices.to_vec();
    all_left.extend_from_slice(unmatched_left);
    let left_idx_arr: ArrayRef = Arc::new(UInt32Array::from(all_left));

    // Right columns: matched rows get their real index; unmatched rows get None
    // so Arrow's `take` produces null values rather than copying row 0.
    let mut right_idx_opt: Vec<Option<u32>> = right_indices.iter().map(|&r| Some(r)).collect();
    right_idx_opt.extend(std::iter::repeat(None).take(unmatched_left.len()));
    let right_idx_arr: ArrayRef = Arc::new(UInt32Array::from(right_idx_opt));

    // Right non-key columns must be nullable when there are unmatched left rows:
    // those rows receive Arrow nulls, which requires the column to be declared nullable.
    let n_left = left.num_columns();
    let nullable_schema: Arc<Schema> = if !unmatched_left.is_empty() {
        Arc::new(Schema::new(
            out_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    if i >= n_left {
                        Field::new(f.name(), f.data_type().clone(), true)
                    } else {
                        f.as_ref().clone()
                    }
                })
                .collect::<Vec<_>>(),
        ))
    } else {
        out_schema
    };

    let mut columns: Vec<ArrayRef> = Vec::new();
    for i in 0..n_left {
        columns.push(
            take(left.column(i), &left_idx_arr, None)
                .map_err(|e| ExecError::Arrow(e.to_string()))?,
        );
    }
    for (i, f) in right.schema().fields().iter().enumerate() {
        if right_keys.iter().any(|k| k == f.name()) {
            continue;
        }
        let col = right.column(i);
        columns.push(take(col, &right_idx_arr, None).map_err(|e| ExecError::Arrow(e.to_string()))?);
    }
    RecordBatch::try_new(nullable_schema, columns).map_err(|e| ExecError::Arrow(e.to_string()))
}

fn build_composite_key_static(
    batch: &RecordBatch,
    key_indices: &[usize],
    row: usize,
) -> ExecResult<CompositeKey> {
    let keys: Result<Vec<AggKey>, _> = key_indices
        .iter()
        .map(|&idx| extract_agg_key(batch, idx, row))
        .collect();
    Ok(CompositeKey::new(keys?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn left_outer_join_unmatched_rows_produce_null_right_columns() {
        // left: id=[1,2], val=[10,20]   right: id=[1], rval=[100]
        // row id=2 is unmatched → rval must be null (not row-0 value 100)
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let left = RecordBatch::try_new(
            left_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )
        .unwrap();

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("rval", DataType::Int32, false),
        ]));
        let right = RecordBatch::try_new(
            right_schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![100])),
            ],
        )
        .unwrap();

        let join = HashJoin::new("id", "id").with_kind(JoinKind::LeftOuter);
        let result = join.join(&left, &right).unwrap();

        assert_eq!(result.num_rows(), 2, "left outer must emit both left rows");

        let rval_col = result.column_by_name("rval").unwrap();
        let rvals = rval_col.as_any().downcast_ref::<Int32Array>().unwrap();

        // Find row with id=2 (unmatched) — its rval must be null.
        let ids = result.column_by_name("id").unwrap();
        let id_arr = ids.as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..result.num_rows() {
            if id_arr.value(i) == 2 {
                assert!(
                    rvals.is_null(i),
                    "unmatched left row must have null right column, got {}",
                    rvals.value(i)
                );
            }
        }
    }

    #[test]
    fn extract_agg_key_rejects_null_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
        )
        .unwrap();

        let err = extract_agg_key(&batch, 0, 1).unwrap_err();
        assert!(matches!(err, ExecError::InvalidInput(_)));
        assert!(err.to_string().contains("contains null at row 1"));
    }

    #[test]
    fn extract_agg_key_rejects_out_of_bounds_indices() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7]))]).unwrap();

        let column_err = extract_agg_key(&batch, 1, 0).unwrap_err();
        assert!(matches!(column_err, ExecError::InvalidInput(_)));
        assert!(column_err.to_string().contains("column index 1"));

        let row_err = extract_agg_key(&batch, 0, 1).unwrap_err();
        assert!(matches!(row_err, ExecError::InvalidInput(_)));
        assert!(row_err.to_string().contains("row index 1"));
    }
}
