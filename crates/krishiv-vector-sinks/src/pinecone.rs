use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;

use crate::batch::EmbeddingBatch;
use crate::id::point_id_from_doc_epoch;
use crate::traits::{
    PayloadFilter, ScoredChunk, VectorSink, VectorSinkError, VectorSinkResult,
};

/// Pinecone REST upsert sink.
#[derive(Clone)]
pub struct PineconeSink {
    client: Client,
    host: String,
    api_key: String,
    namespace: Option<String>,
}

impl PineconeSink {
    /// Create a Pinecone sink. `host` is the index host (e.g. `index-abc.svc.pinecone.io`).
    pub fn new(host: impl Into<String>, api_key: impl Into<String>, namespace: Option<String>) -> Self {
        Self {
            client: Client::new(),
            host: host.into(),
            api_key: api_key.into(),
            namespace,
        }
    }
}

#[async_trait]
impl VectorSink for PineconeSink {
    fn sink_name(&self) -> &str {
        "pinecone"
    }

    async fn upsert_batch(&self, batch: &EmbeddingBatch) -> VectorSinkResult<()> {
        let vectors: Vec<serde_json::Value> = batch
            .doc_ids
            .iter()
            .zip(batch.vectors.iter())
            .zip(batch.payloads.iter())
            .map(|((doc_id, vector), payload)| {
                let id = point_id_from_doc_epoch(doc_id, batch.epoch);
                let metadata: HashMap<String, serde_json::Value> = payload
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_json()))
                    .collect();
                json!({
                    "id": id,
                    "values": vector,
                    "metadata": metadata,
                })
            })
            .collect();
        let mut body = json!({ "vectors": vectors });
        if let Some(ns) = &self.namespace {
            body["namespace"] = json!(ns);
        }
        let base = self.host.trim_end_matches('/');
        let url = if base.starts_with("http://") || base.starts_with("https://") {
            format!("{base}/vectors/upsert")
        } else {
            format!("https://{base}/vectors/upsert")
        };
        let response = self
            .client
            .post(&url)
            .header("Api-Key", &self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| VectorSinkError::Connection(e.to_string()))?;
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(VectorSinkError::RateLimit("pinecone rate limited".into()));
        }
        if !response.status().is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(VectorSinkError::Upsert(text));
        }
        Ok(())
    }

    async fn delete_by_ids(&self, ids: &[String]) -> VectorSinkResult<()> {
        let mut body = json!({ "ids": ids });
        if let Some(ns) = &self.namespace {
            body["namespace"] = json!(ns);
        }
        let url = format!("https://{}/vectors/delete", self.host);
        self.client
            .post(&url)
            .header("Api-Key", &self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| VectorSinkError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn query_nearest(
        &self,
        vector: &[f32],
        top_k: usize,
        _filter: Option<&PayloadFilter>,
    ) -> VectorSinkResult<Vec<ScoredChunk>> {
        let mut body = json!({
            "vector": vector,
            "topK": top_k,
            "includeMetadata": true,
        });
        if let Some(ns) = &self.namespace {
            body["namespace"] = json!(ns);
        }
        let url = format!("https://{}/query", self.host);
        let response = self
            .client
            .post(&url)
            .header("Api-Key", &self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| VectorSinkError::Connection(e.to_string()))?;
        let payload: serde_json::Value = response
            .json()
            .await
            .map_err(|e| VectorSinkError::Query(e.to_string()))?;
        let matches = payload
            .get("matches")
            .and_then(|m| m.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(matches
            .into_iter()
            .filter_map(|m| {
                let score = m.get("score")?.as_f64()? as f32;
                let id = m.get("id")?.as_str()?.to_string();
                Some(ScoredChunk {
                    doc_id: id,
                    chunk_index: 0,
                    text: String::new(),
                    score,
                    payload: HashMap::new(),
                })
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::VectorSink;

    #[tokio::test]
    async fn pinecone_upsert_retries_same_epoch() {
        let mut server = mockito::Server::new_async().await;
        let m = server
            .mock("POST", "/vectors/upsert")
            .with_status(200)
            .expect(2)
            .create_async()
            .await;
        let sink = PineconeSink::new(server.url(), "test-key", None);
        let batch = EmbeddingBatch::new(
            vec!["doc".into()],
            vec![vec![1.0, 0.0]],
            vec![HashMap::new()],
            7,
        );
        sink.upsert_batch(&batch).await.unwrap();
        sink.upsert_batch(&batch).await.unwrap();
        m.assert_async().await;
    }
}
