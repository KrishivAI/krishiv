use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;

use crate::batch::EmbeddingBatch;
use crate::id::point_id_from_doc_epoch;
use crate::traits::{PayloadFilter, ScoredChunk, VectorSink, VectorSinkError, VectorSinkResult};

/// Weaviate REST vector sink.
#[derive(Clone)]
pub struct WeaviateSink {
    client: Client,
    base_url: String,
    class_name: String,
    api_key: Option<String>,
}

impl WeaviateSink {
    /// Create a Weaviate sink targeting `base_url` (e.g. `http://localhost:8080`).
    pub fn new(
        base_url: impl Into<String>,
        class_name: impl Into<String>,
        api_key: Option<String>,
    ) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            class_name: class_name.into(),
            api_key,
        }
    }

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(key) = &self.api_key {
            req.header("Authorization", format!("Bearer {key}"))
        } else {
            req
        }
    }
}

#[async_trait]
impl VectorSink for WeaviateSink {
    fn sink_name(&self) -> &str {
        "weaviate"
    }

    async fn upsert_batch(&self, batch: &EmbeddingBatch) -> VectorSinkResult<()> {
        for ((doc_id, vector), payload) in batch
            .doc_ids
            .iter()
            .zip(batch.vectors.iter())
            .zip(batch.payloads.iter())
        {
            let id = point_id_from_doc_epoch(doc_id, batch.epoch);
            let mut properties: HashMap<String, serde_json::Value> = payload
                .iter()
                .map(|(k, v)| (k.clone(), v.to_json()))
                .collect();
            properties.insert("doc_id".into(), json!(doc_id));
            properties.insert("epoch".into(), json!(batch.epoch));
            let body = json!({
                "class": self.class_name,
                "id": id,
                "vector": vector,
                "properties": properties,
            });
            let url = format!("{}/v1/objects", self.base_url);
            let response = self
                .auth(self.client.put(&url).json(&body))
                .send()
                .await
                .map_err(|e| VectorSinkError::Connection(e.to_string()))?;
            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                return Err(VectorSinkError::Upsert(format!("{status}: {text}")));
            }
        }
        Ok(())
    }

    async fn delete_by_ids(&self, ids: &[String]) -> VectorSinkResult<()> {
        for id in ids {
            let url = format!("{}/v1/objects/{}", self.base_url, id);
            let response = self
                .auth(self.client.delete(&url))
                .send()
                .await
                .map_err(|e| VectorSinkError::Connection(e.to_string()))?;
            if !response.status().is_success()
                && response.status() != reqwest::StatusCode::NOT_FOUND
            {
                return Err(VectorSinkError::Upsert(response.status().to_string()));
            }
        }
        Ok(())
    }

    async fn query_nearest(
        &self,
        vector: &[f32],
        top_k: usize,
        _filter: Option<&PayloadFilter>,
    ) -> VectorSinkResult<Vec<ScoredChunk>> {
        let body = json!({
            "query": format!(
                "{{ Get {{ {class}(limit: {limit}, nearVector: {{ vector: [{vec}] }}) {{ properties {{ text chunk_index doc_id }} _additional {{ score }} }} }} }}",
                class = self.class_name,
                limit = top_k,
                vec = vector.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", ")
            ),
        });
        let url = format!("{}/v1/graphql", self.base_url);
        let response = self
            .auth(self.client.post(&url).json(&body))
            .send()
            .await
            .map_err(|e| VectorSinkError::Connection(e.to_string()))?;
        if !response.status().is_success() {
            return Err(VectorSinkError::Query(response.status().to_string()));
        }
        let payload: serde_json::Value = response
            .json()
            .await
            .map_err(|e| VectorSinkError::Query(e.to_string()))?;
        let mut out = Vec::new();
        let Some(hits) = payload
            .pointer(&format!("/data/Get/{}", self.class_name))
            .and_then(|v| v.as_array())
        else {
            return Ok(out);
        };
        for hit in hits {
            let score = hit
                .pointer("/_additional/score")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f32>().ok())
                .or_else(|| hit.get("score").and_then(|v| v.as_f64()).map(|f| f as f32))
                .unwrap_or(0.0);
            let props = hit.get("properties").or_else(|| hit.get("_additional"));
            let text = props
                .and_then(|p| p.get("text"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let chunk_index = props
                .and_then(|p| p.get("chunk_index"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as usize;
            let doc_id = props
                .and_then(|p| p.get("doc_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            out.push(ScoredChunk {
                doc_id,
                chunk_index,
                text,
                score,
                payload: HashMap::new(),
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::VectorSink;

    #[tokio::test]
    async fn weaviate_query_returns_results() {
        let mut server = mockito::Server::new_async().await;
        let body = serde_json::json!({
            "data": {
                "Get": {
                    "Document": [{
                        "properties": { "text": "hello", "chunk_index": 2 },
                        "_additional": { "score": "0.91" }
                    }]
                }
            }
        });
        let _m = server
            .mock("POST", "/v1/graphql")
            .with_status(200)
            .with_body(body.to_string())
            .create_async()
            .await;
        let sink = WeaviateSink::new(server.url(), "Document", None);
        let hits = sink.query_nearest(&[0.1, 0.2], 1, None).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].text, "hello");
        assert_eq!(hits[0].chunk_index, 2);
    }

    #[tokio::test]
    async fn weaviate_upsert_is_idempotent_with_mock() {
        let mut server = mockito::Server::new_async().await;
        let m = server
            .mock("PUT", "/v1/objects")
            .with_status(200)
            .expect(2)
            .create_async()
            .await;
        let sink = WeaviateSink::new(server.url(), "Document", None);
        let batch = EmbeddingBatch::new(
            vec!["d1".into()],
            vec![vec![0.1, 0.2]],
            vec![HashMap::new()],
            1,
        );
        sink.upsert_batch(&batch).await.unwrap();
        sink.upsert_batch(&batch).await.unwrap();
        m.assert_async().await;
    }
}
