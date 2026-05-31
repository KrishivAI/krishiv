use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;

use crate::batch::EmbeddingBatch;
use crate::id::point_id_from_doc_epoch;
use crate::traits::{
    PayloadFilter, PayloadValue, ScoredChunk, VectorSink, VectorSinkError, VectorSinkResult,
};

#[derive(Debug, Clone)]
pub(crate) struct StoredPoint {
    pub doc_id: String,
    pub vector: Vec<f32>,
    pub payload: HashMap<String, PayloadValue>,
    pub _epoch: u64,
}

/// In-memory vector sink for tests and embedded RAG pipelines.
#[derive(Debug, Default)]
pub struct InMemoryVectorSink {
    pub(crate) points: RwLock<HashMap<String, StoredPoint>>,
}

impl InMemoryVectorSink {
    /// Create an empty in-memory sink.
    pub fn new() -> Self {
        Self::default()
    }

    fn cosine(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let na = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let nb = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if na == 0.0 || nb == 0.0 {
            0.0
        } else {
            dot / (na * nb)
        }
    }

    fn matches_filter(
        payload: &HashMap<String, PayloadValue>,
        filter: Option<&PayloadFilter>,
    ) -> bool {
        let Some(filter) = filter else {
            return true;
        };
        filter.equals.iter().all(|(k, v)| payload.get(k) == Some(v))
    }
}

#[async_trait]
impl VectorSink for InMemoryVectorSink {
    fn sink_name(&self) -> &str {
        "memory"
    }

    async fn upsert_batch(&self, batch: &EmbeddingBatch) -> VectorSinkResult<()> {
        if batch.doc_ids.len() != batch.vectors.len() || batch.doc_ids.len() != batch.payloads.len()
        {
            return Err(VectorSinkError::SchemaConflict(
                "doc_ids, vectors, and payloads length mismatch".into(),
            ));
        }
        let mut guard = self
            .points
            .write()
            .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
        for ((doc_id, vector), payload) in batch
            .doc_ids
            .iter()
            .zip(batch.vectors.iter())
            .zip(batch.payloads.iter())
        {
            let id = point_id_from_doc_epoch(doc_id, batch.epoch);
            guard.insert(
                id,
                StoredPoint {
                    doc_id: doc_id.clone(),
                    vector: vector.clone(),
                    payload: payload.clone(),
                    _epoch: batch.epoch,
                },
            );
        }
        Ok(())
    }

    async fn delete_by_ids(&self, ids: &[String]) -> VectorSinkResult<()> {
        let mut guard = self
            .points
            .write()
            .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
        for id in ids {
            guard.remove(id);
        }
        Ok(())
    }

    async fn query_nearest(
        &self,
        vector: &[f32],
        top_k: usize,
        filter: Option<&PayloadFilter>,
    ) -> VectorSinkResult<Vec<ScoredChunk>> {
        let guard = self
            .points
            .read()
            .map_err(|e| VectorSinkError::Query(e.to_string()))?;
        let mut scored: Vec<ScoredChunk> = guard
            .values()
            .filter(|p| Self::matches_filter(&p.payload, filter))
            .map(|p| {
                let chunk_index = p
                    .payload
                    .get("chunk_index")
                    .and_then(|v| match v {
                        PayloadValue::Int(i) => Some(*i as usize),
                        _ => None,
                    })
                    .unwrap_or(0);
                let text = p
                    .payload
                    .get("text")
                    .and_then(|v| match v {
                        PayloadValue::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
                ScoredChunk {
                    doc_id: p.doc_id.clone(),
                    chunk_index,
                    text,
                    score: Self::cosine(vector, &p.vector),
                    payload: p.payload.clone(),
                }
            })
            .collect();
        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(top_k);
        Ok(scored)
    }
}
