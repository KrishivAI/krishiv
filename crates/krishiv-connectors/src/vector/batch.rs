use std::collections::HashMap;

use super::traits::PayloadValue;

/// Columnar embedding write batch for vector sinks.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingBatch {
    pub doc_ids: Vec<String>,
    pub vectors: Vec<Vec<f32>>,
    pub payloads: Vec<HashMap<String, PayloadValue>>,
    pub epoch: u64,
}

impl EmbeddingBatch {
    /// Create a new embedding batch.
    pub fn new(
        doc_ids: Vec<String>,
        vectors: Vec<Vec<f32>>,
        payloads: Vec<HashMap<String, PayloadValue>>,
        epoch: u64,
    ) -> Self {
        Self {
            doc_ids,
            vectors,
            payloads,
            epoch,
        }
    }

    /// Number of points in this batch.
    pub fn len(&self) -> usize {
        self.doc_ids.len()
    }

    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.doc_ids.is_empty()
    }
}
