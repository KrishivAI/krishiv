#![forbid(unsafe_code)]

//! Shared test utilities for the Krishiv workspace.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

/// Build a [`RecordBatch`] from a schema and column arrays.
pub fn make_batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(schema, columns).expect("test batch columns match schema")
}

/// Build a single-column Int32 batch named `value`.
pub fn make_i32_batch(values: &[i32]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    make_batch(schema, vec![Arc::new(Int32Array::from(values.to_vec()))])
}

/// Configurable in-memory source that emits fixed batches.
#[derive(Debug, Clone, Default)]
pub struct MockSource {
    batches: Vec<RecordBatch>,
}

impl MockSource {
    /// Create a source that returns `batches` in order.
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }

    /// All batches this source will emit.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }
}

/// Collects batches written by a test sink.
#[derive(Debug, Clone, Default)]
pub struct MockSink {
    batches: Vec<RecordBatch>,
}

impl MockSink {
    /// Append a batch to the sink.
    pub fn write(&mut self, batch: RecordBatch) {
        self.batches.push(batch);
    }

    /// Collected batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }
}

/// In-memory session placeholder for integration tests (tables registered by name).
#[derive(Debug, Default)]
pub struct TestSession {
    tables: std::collections::HashMap<String, Vec<RecordBatch>>,
}

impl TestSession {
    /// Register table batches retrievable by name.
    pub fn register_table(&mut self, name: impl Into<String>, batches: Vec<RecordBatch>) {
        self.tables.insert(name.into(), batches);
    }

    /// Lookup registered batches.
    pub fn table_batches(&self, name: &str) -> Option<&[RecordBatch]> {
        self.tables.get(name).map(Vec::as_slice)
    }
}

/// Assert two batch slices have identical schemas and row contents.
pub fn assert_batches_eq(actual: &[RecordBatch], expected: &[RecordBatch]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "batch count mismatch: actual={}, expected={}",
        actual.len(),
        expected.len()
    );
    for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
        assert!(a.schema() == e.schema(), "schema mismatch at batch {i}");
        assert_eq!(a.num_rows(), e.num_rows());
        assert_eq!(a.num_columns(), e.num_columns());
    }
}
