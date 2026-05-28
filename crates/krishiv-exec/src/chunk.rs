//! Text chunking physical operator (R17).

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_ai::{Chunk, TextChunker};

use crate::{ExecError, ExecResult};

/// Explodes a text column into one row per chunk.
pub struct ChunkOperator {
    chunker: Arc<dyn TextChunker>,
    text_col: String,
}

impl ChunkOperator {
    /// Create a chunk operator for `text_col`.
    pub fn new(chunker: Arc<dyn TextChunker>, text_col: impl Into<String>) -> Self {
        Self {
            chunker,
            text_col: text_col.into(),
        }
    }

    /// Apply chunking to a batch containing `text_col`.
    pub fn execute(&self, batch: &RecordBatch) -> ExecResult<RecordBatch> {
        let text_array = batch
            .column_by_name(&self.text_col)
            .ok_or_else(|| ExecError::ColumnNotFound(self.text_col.clone()))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ExecError::UnsupportedType(format!("column {} is not utf8", self.text_col))
            })?;
        let mut chunk_texts = Vec::new();
        let mut chunk_indices = Vec::new();
        let mut row_indices = Vec::new();
        for row in 0..text_array.len() {
            if text_array.is_null(row) {
                continue;
            }
            let chunks: Vec<Chunk> = self.chunker.chunk(text_array.value(row));
            for c in chunks {
                chunk_texts.push(c.text);
                chunk_indices.push(c.chunk_index as i64);
                row_indices.push(row as i64);
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_index", DataType::Int64, false),
            Field::new("chunk_index", DataType::Int64, false),
            Field::new("chunk_text", DataType::Utf8, false),
        ]));
        let out = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(row_indices)),
                Arc::new(Int64Array::from(chunk_indices)),
                Arc::new(StringArray::from(chunk_texts)),
            ],
        )
        .map_err(|e| ExecError::Arrow(e.to_string()))?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_ai::RecursiveTextChunker;

    #[test]
    fn chunk_operator_produces_rows_per_chunk() {
        let chunker: Arc<dyn krishiv_ai::TextChunker> =
            Arc::new(RecursiveTextChunker::new(20, 0));
        let op = ChunkOperator::new(chunker, "text");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "text",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["hello world this is a test"]))],
        )
        .unwrap();

        let result = op.execute(&batch).unwrap();
        assert!(result.num_rows() > 0, "should produce at least one chunk");
        assert_eq!(result.schema().field(0).name(), "row_index");
        assert_eq!(result.schema().field(1).name(), "chunk_index");
        assert_eq!(result.schema().field(2).name(), "chunk_text");
    }

    #[test]
    fn chunk_operator_missing_column_returns_error() {
        let chunker: Arc<dyn krishiv_ai::TextChunker> =
            Arc::new(RecursiveTextChunker::new(20, 0));
        let op = ChunkOperator::new(chunker, "missing_col");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "text",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["hello"]))],
        )
        .unwrap();

        let result = op.execute(&batch);
        assert!(result.is_err(), "missing column must return error");
    }
}
