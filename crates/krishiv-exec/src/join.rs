use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::{ExecError, ExecResult};

/// Typed group-by / aggregate key (P2-12).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum AggKey {
    Int32(i32),
    Int64(i64),
    /// `f64` stored as IEEE-754 bits for total-order hashing.
    Float64(u64),
    Utf8(String),
    Bool(bool),
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

/// Extract a typed key from one column at `row`.
pub(crate) fn extract_agg_key(
    batch: &RecordBatch,
    col_idx: usize,
    row: usize,
) -> ExecResult<AggKey> {
    let col = batch.column(col_idx);
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

/// Serialize a single row value from the given column to a `String` for use as
/// a hash-map key.  Supported types: `Int32`, `Int64`, `Utf8`.
pub fn format_key_value(batch: &RecordBatch, col_idx: usize, row: usize) -> ExecResult<String> {
    let col = batch.column(col_idx);
    match col.data_type() {
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                ExecError::UnsupportedType("declared Int32 key failed downcast".into())
            })?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                ExecError::UnsupportedType("declared Int64 key failed downcast".into())
            })?;
            Ok(arr.value(row).to_string())
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                ExecError::UnsupportedType("declared Utf8 key failed downcast".into())
            })?;
            Ok(arr.value(row).to_string())
        }
        other => Err(ExecError::UnsupportedType(format!(
            "unsupported join key type: {other}"
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

// ── HashJoin ──────────────────────────────────────────────────────────────────

/// Inner equi-join on a single named key column.
///
/// The left batch is the probe side; the right batch is the build side.
pub struct HashJoin {
    left_key: String,
    right_key: String,
}

impl HashJoin {
    /// Create a new `HashJoin` with the given join key column names.
    pub fn new(left_key: impl Into<String>, right_key: impl Into<String>) -> Self {
        Self {
            left_key: left_key.into(),
            right_key: right_key.into(),
        }
    }

    /// Inner hash join: left is probe side, right is build side.
    ///
    /// Returns a `RecordBatch` whose schema is all left columns followed by all
    /// right columns (excluding the right join key column to avoid duplication).
    pub fn join(&self, left: &RecordBatch, right: &RecordBatch) -> ExecResult<RecordBatch> {
        // Determine output schema.
        let out_schema = join_output_schema(left, right, &self.right_key);

        // Resolve column indices.
        let left_key_idx = left
            .schema()
            .index_of(&self.left_key)
            .map_err(|_| ExecError::ColumnNotFound(self.left_key.clone()))?;
        let right_key_idx = right
            .schema()
            .index_of(&self.right_key)
            .map_err(|_| ExecError::ColumnNotFound(self.right_key.clone()))?;

        // Build phase: hash map from serialized key → list of right row indices.
        // Using String as the key avoids the extra Arc<str> allocation per row
        // (format_key_value already returns a String).
        let mut build_map: HashMap<String, Vec<u32>> = HashMap::with_capacity(right.num_rows());
        for row in 0..right.num_rows() {
            let key = format_key_value(right, right_key_idx, row)?;
            build_map.entry(key).or_default().push(row as u32);
        }

        // Probe phase: collect (left_row, right_row) pairs.
        let mut left_indices: Vec<u32> = Vec::new();
        let mut right_indices: Vec<u32> = Vec::new();

        for row in 0..left.num_rows() {
            let key = format_key_value(left, left_key_idx, row)?;
            if let Some(right_rows) = build_map.get(&key) {
                for &r in right_rows {
                    left_indices.push(row as u32);
                    right_indices.push(r);
                }
            }
        }

        if left_indices.is_empty() {
            return Ok(RecordBatch::new_empty(out_schema));
        }

        build_join_batch(
            left,
            right,
            &self.right_key,
            &left_indices,
            &right_indices,
            out_schema,
        )
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
    pub fn build(self, broadcast_batch: &RecordBatch) -> ExecResult<BuiltBroadcastJoin> {
        let key_idx = broadcast_batch
            .schema()
            .index_of(&self.join_key)
            .map_err(|_| ExecError::ColumnNotFound(self.join_key.clone()))?;

        let mut index: HashMap<String, Vec<u32>> = HashMap::new();
        for row in 0..broadcast_batch.num_rows() {
            let key = format_key_value(broadcast_batch, key_idx, row)?;
            index.entry(key).or_default().push(row as u32);
        }

        Ok(BuiltBroadcastJoin {
            join_key: self.join_key,
            broadcast: broadcast_batch.clone(),
            index,
        })
    }
}

/// A pre-built broadcast join table ready to probe.
pub struct BuiltBroadcastJoin {
    join_key: String,
    broadcast: RecordBatch,
    /// Pre-built hash map: serialized key → broadcast (right) row indices.
    index: HashMap<String, Vec<u32>>,
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
            let key = format_key_value(probe, probe_key_idx, row)?;
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
    cached_index: Option<Arc<HashMap<String, Vec<u32>>>>,
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

    fn table_index(&mut self) -> ExecResult<Arc<HashMap<String, Vec<u32>>>> {
        if let Some(ref cached) = self.cached_index {
            return Ok(Arc::clone(cached));
        }
        let table_key_idx = self
            .table
            .schema()
            .index_of(&self.join_key_column)
            .map_err(|_| ExecError::ColumnNotFound(self.join_key_column.clone()))?;
        let mut index: HashMap<String, Vec<u32>> = HashMap::new();
        for row in 0..self.table.num_rows() {
            let key = format_key_value(&self.table, table_key_idx, row)?;
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
            let key = format_key_value(stream_batch, stream_key_idx, s_row)?;
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

        let mut columns: Vec<ArrayRef> = Vec::new();
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
