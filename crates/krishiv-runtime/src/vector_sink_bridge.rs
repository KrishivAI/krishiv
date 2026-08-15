#![forbid(unsafe_code)]
#![cfg(feature = "vector-sinks")]

//! Bridge from `krishiv-connectors::VectorSink` to `krishiv-ivm::IvmVectorSink`.
//!
//! Lets users plug any existing connector-level `VectorSink` (Qdrant, Pinecone,
//! pgvector, LanceDB, …) directly into [`krishiv_ivm::spawn_vector_view`].
//!
//! # Example
//!
//! ```rust,ignore
//! let qdrant = Arc::new(QdrantSink::new(&cfg).await?);
//! let bridge = VectorSinkBridge::new(qdrant);
//! let handle = spawn_vector_view(
//!     &flow,
//!     VectorViewSpec {
//!         view_name:     "doc_embeddings".into(),
//!         id_column:     "doc_id".into(),
//!         vector_column: "embedding".into(),
//!         sink:          Arc::new(bridge),
//!     },
//! )?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use krishiv_connectors::vector::{EmbeddingBatch, PayloadValue, VectorSink};
use krishiv_ivm::VectorFuture;

/// Wraps a `krishiv-connectors::VectorSink` and implements `IvmVectorSink`.
pub struct VectorSinkBridge {
    inner: Arc<dyn VectorSink>,
}

/// Constant epoch for bridge upserts. Point ids are `hash(doc_id || epoch)`,
/// so a fixed epoch keeps the id stable per `doc_id` and makes re-upserts
/// idempotent overwrites (ADR-R17.3). Callers that genuinely version their
/// embeddings should build an `EmbeddingBatch` with their own epoch directly.
const BRIDGE_EPOCH: u64 = 0;

impl VectorSinkBridge {
    pub fn new(sink: Arc<dyn VectorSink>) -> Self {
        Self { inner: sink }
    }
}

impl krishiv_ivm::IvmVectorSink for VectorSinkBridge {
    fn upsert_batch<'a>(&'a self, ids: &'a [String], vectors: &'a [Vec<f32>]) -> VectorFuture<'a> {
        Box::pin(async move {
            let payloads: Vec<HashMap<String, PayloadValue>> =
                (0..ids.len()).map(|_| HashMap::new()).collect();
            let batch = EmbeddingBatch::new(ids.to_vec(), vectors.to_vec(), payloads, BRIDGE_EPOCH);
            self.inner
                .upsert_batch(&batch)
                .await
                .map_err(|e| krishiv_ivm::IvmError::execution(e.to_string()))
        })
    }

    fn delete_batch<'a>(&'a self, ids: &'a [String]) -> VectorFuture<'a> {
        Box::pin(async move {
            self.inner
                .delete_by_ids(ids)
                .await
                .map_err(|e| krishiv_ivm::IvmError::execution(e.to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_connectors::vector::{InMemoryVectorSink, VectorSink};
    use krishiv_ivm::IvmVectorSink;

    #[tokio::test]
    async fn vector_bridge_upsert_is_idempotent_per_doc_id() {
        let inner = Arc::new(InMemoryVectorSink::new());
        let bridge = VectorSinkBridge::new(inner.clone() as Arc<dyn VectorSink>);
        let ids = vec!["doc-1".to_string()];
        let vectors = vec![vec![1.0_f32, 0.0]];
        bridge.upsert_batch(&ids, &vectors).await.unwrap();
        bridge.upsert_batch(&ids, &vectors).await.unwrap();
        let results = inner.query_nearest(&[1.0, 0.0], 10, None).await.unwrap();
        assert_eq!(results.len(), 1, "re-upsert must overwrite, not duplicate");
        assert_eq!(results[0].doc_id, "doc-1");
    }
}
