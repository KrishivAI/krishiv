#![forbid(unsafe_code)]

//! E3.3 — LookupJoin operator.
//!
//! Enriches a streaming left input with point-lookup results from a
//! [`LookupSource`] (e.g. an external KV store or dimension table).
//!
//! # Semantics
//! - For each row in the input batch, the operator extracts the join key and
//!   calls [`LookupSource::lookup`].
//! - If the lookup returns a result, the matching columns are appended.
//! - If the lookup returns `None` (miss) the row is emitted with nulls for
//!   the right-side columns (`null_on_miss = true`).
//! - The lookup is synchronous here; async wrappers can bridge async sources.
//!
//! # Timeout
//! [`LookupSource`] implementations signal a timeout by returning
//! `Err(LookupError::Timeout)`. On timeout, the operator emits the row with
//! null right-side columns (same as a miss) rather than dropping the row.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::join::extract_agg_key;
use crate::{ExecError, ExecResult};

// ── LookupSource trait ────────────────────────────────────────────────────────

/// Errors that can be returned from a point lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupError {
    /// The key was not found in the source.
    NotFound,
    /// The lookup exceeded the configured timeout.
    Timeout,
    /// An upstream store error occurred.
    Upstream(String),
}

impl std::fmt::Display for LookupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => f.write_str("key not found"),
            Self::Timeout => f.write_str("lookup timeout"),
            Self::Upstream(s) => write!(f, "lookup upstream error: {s}"),
        }
    }
}

/// A scalar value returned from a point lookup.
#[derive(Debug, Clone, PartialEq)]
pub enum LookupValue {
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Bool(bool),
    Null,
}

/// Result row from a [`LookupSource`]: a map of column name → value.
pub type LookupRow = HashMap<String, LookupValue>;

/// Trait for synchronous point-lookup sources.
///
/// Implementations back the [`LookupJoin`] operator with per-key reads from
/// an in-memory table, external KV store, or any other keyed store.
pub trait LookupSource: Send + Sync {
    /// Look up a key and return a row of column → value pairs, or an error.
    ///
    /// Return `Err(LookupError::NotFound)` for a cache miss.
    /// Return `Err(LookupError::Timeout)` to signal an SLA miss.
    fn lookup(&self, key: &str) -> Result<LookupRow, LookupError>;

    /// The schema (column names + Arrow data types) produced by this source.
    fn schema(&self) -> Vec<(String, DataType)>;
}

// ── LookupJoin ────────────────────────────────────────────────────────────────

/// Configuration for a lookup join.
#[derive(Debug, Clone)]
pub struct LookupJoinSpec {
    /// Key column on the input (left) stream.
    pub left_key: String,
}

/// Enriches each row of an input batch via a [`LookupSource`].
pub struct LookupJoin {
    spec: LookupJoinSpec,
    source: Box<dyn LookupSource>,
}

impl LookupJoin {
    pub fn new(spec: LookupJoinSpec, source: Box<dyn LookupSource>) -> Self {
        Self { spec, source }
    }

    /// Process one batch: look up each row and append right-side columns.
    ///
    /// Rows with a miss or timeout are emitted with `null` values for the
    /// right-side columns.
    pub fn join(&self, batch: &RecordBatch) -> ExecResult<RecordBatch> {
        let key_idx = batch
            .schema()
            .index_of(&self.spec.left_key)
            .map_err(|_| ExecError::ColumnNotFound(self.spec.left_key.clone()))?;

        let right_schema = self.source.schema();
        let n = batch.num_rows();

        // Collect per-column right-side values.
        let mut right_values: Vec<Vec<Option<LookupValue>>> = (0..right_schema.len())
            .map(|_| Vec::with_capacity(n))
            .collect();

        for row in 0..n {
            let key = extract_agg_key(batch, key_idx, row)?;
            let key_str = key.to_string();
            match self.source.lookup(&key_str) {
                Ok(lookup_row) => {
                    for (col_idx, (col_name, _)) in right_schema.iter().enumerate() {
                        right_values[col_idx].push(lookup_row.get(col_name).cloned());
                    }
                }
                Err(LookupError::NotFound) | Err(LookupError::Timeout) => {
                    for col_values in right_values.iter_mut() {
                        col_values.push(None);
                    }
                }
                Err(LookupError::Upstream(e)) => {
                    return Err(ExecError::Upstream(format!("lookup join: {e}")));
                }
            }
        }

        // Build output: left cols + right cols.
        let mut fields: Vec<Field> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| (**f).clone())
            .collect();
        for (col_name, dt) in &right_schema {
            fields.push(Field::new(col_name, dt.clone(), true));
        }
        let output_schema = Arc::new(Schema::new(fields));

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        for (col_idx, (_, dt)) in right_schema.iter().enumerate() {
            let arr = build_nullable_array(dt, &right_values[col_idx])?;
            columns.push(arr);
        }

        RecordBatch::try_new(output_schema, columns).map_err(|e| ExecError::Arrow(e.to_string()))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn build_nullable_array(dt: &DataType, values: &[Option<LookupValue>]) -> ExecResult<ArrayRef> {
    use arrow::array::{BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder};

    match dt {
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Some(LookupValue::Int32(i)) => b.append_value(*i),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Some(LookupValue::Int64(i)) => b.append_value(*i),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Some(LookupValue::Float64(f)) => b.append_value(*f),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(values.len(), values.len() * 8);
            for v in values {
                match v {
                    Some(LookupValue::Utf8(s)) => b.append_value(s),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    Some(LookupValue::Bool(b_val)) => b.append_value(*b_val),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => Err(ExecError::UnsupportedType(format!(
            "lookup join: unsupported right-side column type {other}"
        ))),
    }
}

// ── In-memory test source ─────────────────────────────────────────────────────

/// A simple in-memory lookup source backed by a `HashMap<String, LookupRow>`.
pub struct InMemoryLookupSource {
    data: HashMap<String, LookupRow>,
    schema: Vec<(String, DataType)>,
}

impl InMemoryLookupSource {
    pub fn new(schema: Vec<(String, DataType)>) -> Self {
        Self {
            data: HashMap::new(),
            schema,
        }
    }

    pub fn insert(&mut self, key: impl Into<String>, row: LookupRow) {
        self.data.insert(key.into(), row);
    }
}

impl LookupSource for InMemoryLookupSource {
    fn lookup(&self, key: &str) -> Result<LookupRow, LookupError> {
        self.data.get(key).cloned().ok_or(LookupError::NotFound)
    }

    fn schema(&self) -> Vec<(String, DataType)> {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_left_batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    fn make_source() -> InMemoryLookupSource {
        let schema = vec![("name".to_string(), DataType::Utf8)];
        let mut src = InMemoryLookupSource::new(schema);
        let mut row1 = HashMap::new();
        row1.insert("name".to_string(), LookupValue::Utf8("alice".to_string()));
        src.insert("1", row1);
        let mut row2 = HashMap::new();
        row2.insert("name".to_string(), LookupValue::Utf8("bob".to_string()));
        src.insert("2", row2);
        src
    }

    #[test]
    fn lookup_join_enriches_matched_rows() {
        let spec = LookupJoinSpec {
            left_key: "id".into(),
        };
        let join = LookupJoin::new(spec, Box::new(make_source()));
        let batch = make_left_batch(&[1, 2]);
        let result = join.join(&batch).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2, "original col + name col");
        let name_col = result.column_by_name("name").unwrap();
        let names = name_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");
    }

    #[test]
    fn lookup_join_null_on_miss() {
        let spec = LookupJoinSpec {
            left_key: "id".into(),
        };
        let join = LookupJoin::new(spec, Box::new(make_source()));
        let batch = make_left_batch(&[1, 99]); // 99 has no match
        let result = join.join(&batch).unwrap();
        assert_eq!(result.num_rows(), 2);
        let name_col = result.column_by_name("name").unwrap();
        let names = name_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "alice");
        assert!(names.is_null(1), "miss should produce null");
    }

    #[test]
    fn lookup_join_all_miss_returns_nulls() {
        let spec = LookupJoinSpec {
            left_key: "id".into(),
        };
        let join = LookupJoin::new(spec, Box::new(make_source()));
        let batch = make_left_batch(&[100, 200]);
        let result = join.join(&batch).unwrap();
        let name_col = result.column_by_name("name").unwrap();
        let names = name_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(names.is_null(0));
        assert!(names.is_null(1));
    }
}
