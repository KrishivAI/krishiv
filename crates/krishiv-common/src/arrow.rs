use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

/// Create a single-column `Int32` schema with the given field name.
pub fn make_single_int_schema(name: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]))
}

/// Create a single-column `RecordBatch` containing a single `Int32Array`.
///
/// # Panics
/// Panics only if the schema and array length are mismatched, which cannot
/// happen with the values provided here.
pub fn make_single_int_batch(field_name: &str, values: Vec<i32>) -> RecordBatch {
    let schema = make_single_int_schema(field_name);
    let array = Arc::new(Int32Array::from(values)) as ArrayRef;
    RecordBatch::try_new(schema, vec![array]).expect("schema and array length match")
}

/// Create a canonical test schema containing `"user_id"` (Utf8) and `"ts"` (Int64).
fn make_test_user_ts_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

/// Create a canonical test `RecordBatch` containing `"user_id"` (String) and `"ts"` (Int64).
///
/// # Panics
/// Panics only if the schema and array lengths are mismatched.
pub fn make_test_user_ts_batch(users: Vec<&str>, timestamps: Vec<i64>) -> RecordBatch {
    let schema = make_test_user_ts_schema();
    let user_array = Arc::new(StringArray::from(users)) as ArrayRef;
    let ts_array = Arc::new(Int64Array::from(timestamps)) as ArrayRef;
    RecordBatch::try_new(schema, vec![user_array, ts_array]).expect("schema and array length match")
}

/// Create a canonical test schema containing `"key"` (Utf8) and `"ts"` (Int64).
fn make_test_key_ts_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

/// Create a canonical test `RecordBatch` containing `"key"` (String) and `"ts"` (Int64).
///
/// # Panics
/// Panics only if the schema and array lengths are mismatched.
pub fn make_test_key_ts_batch(keys: Vec<&str>, timestamps: Vec<i64>) -> RecordBatch {
    let schema = make_test_key_ts_schema();
    let key_array = Arc::new(StringArray::from(keys)) as ArrayRef;
    let ts_array = Arc::new(Int64Array::from(timestamps)) as ArrayRef;
    RecordBatch::try_new(schema, vec![key_array, ts_array]).expect("schema and array length match")
}
