use std::collections::HashMap;
use std::sync::Arc;

use krishiv_vector_sinks::{
    EmbeddingBatch, InMemoryVectorSink, PayloadValue, VectorSink, VectorSinkConfig,
};
use sha2::{Digest, Sha256};

use crate::chunk::{Chunk, TextChunker};
use crate::embed::EmbeddingModel;
use crate::memo::{MemoEntry, MemoStore};

/// RAG refresh policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefreshPolicy {
    Manual,
    Schedule(String),
    Continuous,
}

/// RAG index pipeline result metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RagIndexResult {
    pub documents_total: usize,
    pub documents_embedded: usize,
    pub documents_skipped_memo: usize,
}

/// Incremental RAG index builder (ADR-R17.4).
pub struct RagIndexPipeline<C> {
    pub chunker: Arc<C>,
    pub embedder: Arc<dyn EmbeddingModel>,
    pub sink: Arc<dyn VectorSink>,
    pub memo: MemoStore,
    pub epoch: u64,
}

impl<C> RagIndexPipeline<C>
where
    C: TextChunker + 'static,
{
    /// Index documents `(doc_id, text)` with memoization.
    pub async fn index_documents(
        &self,
        documents: &[(String, String)],
    ) -> Result<RagIndexResult, String> {
        let mut embedded = 0usize;
        let mut skipped = 0usize;
        let mut doc_ids = Vec::new();
        let mut vectors = Vec::new();
        let mut payloads = Vec::new();

        for (doc_id, text) in documents {
            let hash = content_hash(text);
            if let Some(entry) = self.memo.get(&hash)? {
                if entry.embedding.len() == self.embedder.embedding_dim() {
                    skipped += 1;
                    continue;
                }
            }
            let chunks: Vec<Chunk> = self.chunker.chunk(text);
            if chunks.is_empty() {
                continue;
            }
            let texts: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
            let embs = self
                .embedder
                .embed_batch(&texts)
                .await
                .map_err(|e| e.to_string())?;
            for (chunk, vector) in chunks.iter().zip(embs.iter()) {
                let point_id = krishiv_vector_sinks::point_id_from_doc_epoch(
                    &format!("{doc_id}:{}", chunk.chunk_index),
                    self.epoch,
                );
                let mut payload = HashMap::new();
                payload.insert("text".into(), PayloadValue::String(chunk.text.clone()));
                payload.insert(
                    "chunk_index".into(),
                    PayloadValue::Int(chunk.chunk_index as i64),
                );
                payload.insert("doc_id".into(), PayloadValue::String(doc_id.clone()));
                doc_ids.push(format!("{doc_id}:{}", chunk.chunk_index));
                vectors.push(vector.clone());
                payloads.push(payload);
                self.memo.put(
                    &hash,
                    &MemoEntry {
                        content_hash: hash.clone(),
                        embedding: vector.clone(),
                        point_id,
                    },
                )?;
            }
            embedded += 1;
        }

        if !doc_ids.is_empty() {
            let batch = EmbeddingBatch::new(doc_ids, vectors, payloads, self.epoch);
            self.sink
                .upsert_batch(&batch)
                .await
                .map_err(|e| e.to_string())?;
        }

        Ok(RagIndexResult {
            documents_total: documents.len(),
            documents_embedded: embedded,
            documents_skipped_memo: skipped,
        })
    }
}

/// RAG query helper.
pub struct RagQuery {
    pub embedder: Arc<dyn EmbeddingModel>,
    pub sink: Arc<dyn VectorSink>,
}

impl RagQuery {
    /// Embed query text and search vector store.
    pub async fn query(
        &self,
        query_text: &str,
        top_k: usize,
    ) -> Result<Vec<krishiv_vector_sinks::ScoredChunk>, String> {
        let vector = self
            .embedder
            .embed_batch(&[query_text.to_string()])
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .next()
            .ok_or_else(|| "empty embedding".to_string())?;
        self.sink
            .query_nearest(&vector, top_k, None)
            .await
            .map_err(|e| e.to_string())
    }
}

fn content_hash(text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(all(test, feature = "fastembed-local"))]
mod tests {
    use super::*;
    use crate::chunk::RecursiveTextChunker;
    use crate::{EmbeddingDevice, EmbeddingModelRegistry, ModelKey};
    use crate::embed::HuggingFaceEmbeddingModel;

    struct CountingEmbedder {
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        inner: Arc<dyn EmbeddingModel>,
    }

    #[async_trait::async_trait]
    impl EmbeddingModel for CountingEmbedder {
        fn model_name(&self) -> &str {
            self.inner.model_name()
        }
        fn embedding_dim(&self) -> usize {
            self.inner.embedding_dim()
        }
        async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, crate::embed::EmbeddingError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.embed_batch(texts).await
        }
    }

    #[tokio::test]
    async fn rag_index_skips_unchanged_on_second_run() {
        let key = ModelKey {
            model_name: "all-MiniLM-L6-v2".into(),
            device: EmbeddingDevice::Cpu,
        };
        let inner = EmbeddingModelRegistry::get_or_load(key, || {
            Ok(Arc::new(
                HuggingFaceEmbeddingModel::load("all-MiniLM-L6-v2", EmbeddingDevice::Cpu).unwrap(),
            ) as Arc<dyn EmbeddingModel>)
        })
        .unwrap();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let embedder = Arc::new(CountingEmbedder {
            calls: Arc::clone(&calls),
            inner,
        });
        let dir = tempfile::tempdir().unwrap();
        let memo = MemoStore::open(dir.path().join("rag.redb")).unwrap();
        let sink: Arc<dyn VectorSink> = Arc::new(InMemoryVectorSink::new());
        let chunker = Arc::new(RecursiveTextChunker::new(200, 20));
        let docs = vec![
            ("d1".into(), "Hello world document one.".into()),
            ("d2".into(), "Second document for indexing.".into()),
        ];
        let pipeline = RagIndexPipeline {
            chunker,
            embedder: embedder.clone(),
            sink: sink.clone(),
            memo,
            epoch: 1,
        };
        pipeline.index_documents(&docs).await.unwrap();
        let first_calls = calls.load(std::sync::atomic::Ordering::SeqCst);
        let second = pipeline.index_documents(&docs).await.unwrap();
        assert!(second.documents_skipped_memo >= 1 || calls.load(std::sync::atomic::Ordering::SeqCst) <= first_calls + 1);
    }
}
