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
use krishiv_ivm::{IvmResult, VectorFuture};

/// Wraps a `krishiv-connectors::VectorSink` and implements `IvmVectorSink`.
pub struct VectorSinkBridge {
    inner: Arc<dyn VectorSink>,
    /// Monotonically increasing epoch counter for idempotent upserts.
    epoch: std::sync::atomic::AtomicU64,
}

impl VectorSinkBridge {
    pub fn new(sink: Arc<dyn VectorSink>) -> Self {
        Self {
            inner: sink,
            epoch: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl krishiv_ivm::IvmVectorSink for VectorSinkBridge {
    fn upsert_batch<'a>(&'a self, ids: &'a [String], vectors: &'a [Vec<f32>]) -> VectorFuture<'a> {
        Box::pin(async move {
            let epoch = self.epoch.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            let payloads: Vec<HashMap<String, PayloadValue>> =
                (0..ids.len()).map(|_| HashMap::new()).collect();
            let batch = EmbeddingBatch::new(ids.to_vec(), vectors.to_vec(), payloads, epoch);
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
