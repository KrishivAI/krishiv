use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;

use super::batch::EmbeddingBatch;
use super::id::point_id_from_doc_epoch;
use super::traits::{
    PayloadFilter, PayloadValue, ScoredChunk, VectorSink, VectorSinkError, VectorSinkResult,
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
    pub fn new(
        host: impl Into<String>,
        api_key: impl Into<String>,
        namespace: Option<String>,
    ) -> Self {
        // 5 s connect / 30 s request budget per Pinecone call. Without a
        // request timeout, a stalled TCP connection or unresponsive API host
        // would hang the vector-ingest pipeline indefinitely. Falls back to
        // `Client::new()` if the builder itself fails (TLS init issues).
        let client = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self {
            client,
            host: host.into(),
            api_key: api_key.into(),
            namespace,
        }
    }

    /// Build a request URL, defaulting to https when `host` has no scheme.
    fn endpoint(&self, path: &str) -> String {
        let base = self.host.trim_end_matches('/');
        if base.starts_with("http://") || base.starts_with("https://") {
            format!("{base}/{path}")
        } else {
            format!("https://{base}/{path}")
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
                let mut metadata: HashMap<String, serde_json::Value> = payload
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_json()))
                    .collect();
                // Store the doc id so queries can map matches back to source
                // documents (the point id is an opaque hash).
                metadata.insert("doc_id".into(), json!(doc_id));
                json!({
                    "id": id,
                    "values": vector,
                    "metadata": metadata,
                })
            })
            .collect();
        let mut body = json!({ "vectors": vectors });
        if let Some(ns) = &self.namespace
            && let Some(obj) = body.as_object_mut()
        {
            obj.insert("namespace".to_string(), json!(ns));
        }
        let url = self.endpoint("vectors/upsert");
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
        if let Some(ns) = &self.namespace
            && let Some(obj) = body.as_object_mut()
        {
            obj.insert("namespace".to_string(), json!(ns));
        }
        let url = self.endpoint("vectors/delete");
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
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(VectorSinkError::Delete(format!(
                "pinecone delete returned {status}: {text}"
            )));
        }
        Ok(())
    }

    async fn query_nearest(
        &self,
        vector: &[f32],
        top_k: usize,
        filter: Option<&PayloadFilter>,
    ) -> VectorSinkResult<Vec<ScoredChunk>> {
        let mut body = json!({
            "vector": vector,
            "topK": top_k,
            "includeMetadata": true,
        });
        if let Some(ns) = &self.namespace
            && let Some(obj) = body.as_object_mut()
        {
            obj.insert("namespace".to_string(), json!(ns));
        }
        if let Some(filter) = filter
            && !filter.equals.is_empty()
            && let Some(obj) = body.as_object_mut()
        {
            let clauses: serde_json::Map<String, serde_json::Value> = filter
                .equals
                .iter()
                .map(|(k, v)| (k.clone(), json!({ "$eq": v.to_json() })))
                .collect();
            obj.insert("filter".to_string(), serde_json::Value::Object(clauses));
        }
        let url = self.endpoint("query");
        let response = self
            .client
            .post(&url)
            .header("Api-Key", &self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| VectorSinkError::Connection(e.to_string()))?;
        // Surface non-2xx responses as a typed error with the HTTP status,
        // rather than letting `response.json()` produce a confusing
        // "missing field 'matches'" error for a 5xx body.
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(VectorSinkError::Query(format!(
                "pinecone query returned {status}: {text}"
            )));
        }
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
                let metadata = m
                    .get("metadata")
                    .and_then(|v| v.as_object())
                    .cloned()
                    .unwrap_or_default();
                let doc_id = metadata
                    .get("doc_id")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .unwrap_or(id);
                let text = metadata
                    .get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let chunk_index = metadata
                    .get("chunk_index")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as usize;
                let payload: HashMap<String, PayloadValue> = metadata
                    .iter()
                    .filter_map(|(k, v)| json_to_payload_value(v).map(|pv| (k.clone(), pv)))
                    .collect();
                Some(ScoredChunk {
                    doc_id,
                    chunk_index,
                    text,
                    score,
                    payload,
                })
            })
            .collect())
    }
}

fn json_to_payload_value(v: &serde_json::Value) -> Option<PayloadValue> {
    match v {
        serde_json::Value::String(s) => Some(PayloadValue::String(s.clone())),
        serde_json::Value::Bool(b) => Some(PayloadValue::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(PayloadValue::Int(i))
            } else {
                n.as_f64().map(PayloadValue::Float)
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::traits::VectorSink;
    use super::*;

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

    #[tokio::test]
    async fn pinecone_delete_surfaces_non_2xx() {
        let mut server = mockito::Server::new_async().await;
        let m = server
            .mock("POST", "/vectors/delete")
            .with_status(500)
            .with_body("backend down")
            .create_async()
            .await;
        let sink = PineconeSink::new(server.url(), "test-key", None);
        let err = sink.delete_by_ids(&["id-1".into()]).await.unwrap_err();
        m.assert_async().await;
        match err {
            VectorSinkError::Delete(message) => {
                assert!(message.contains("500"));
                assert!(message.contains("backend down"));
            }
            other => panic!("expected Delete error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pinecone_query_uses_scheme_aware_url_and_reads_metadata() {
        let mut server = mockito::Server::new_async().await;
        let body = serde_json::json!({
            "matches": [{
                "id": "abcdef0123456789",
                "score": 0.87,
                "metadata": {
                    "doc_id": "d1",
                    "text": "hello",
                    "chunk_index": 4
                }
            }]
        });
        let m = server
            .mock("POST", "/query")
            .with_status(200)
            .with_body(body.to_string())
            .create_async()
            .await;
        // server.url() already carries an http:// scheme; the query must not
        // prepend a second scheme.
        let sink = PineconeSink::new(server.url(), "test-key", None);
        let hits = sink.query_nearest(&[1.0, 0.0], 5, None).await.unwrap();
        m.assert_async().await;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc_id, "d1");
        assert_eq!(hits[0].text, "hello");
        assert_eq!(hits[0].chunk_index, 4);
        assert_eq!(
            hits[0].payload.get("text"),
            Some(&super::super::traits::PayloadValue::String("hello".into()))
        );
    }

    #[tokio::test]
    async fn pinecone_query_sends_filter_in_body() {
        let mut server = mockito::Server::new_async().await;
        let m = server
            .mock("POST", "/query")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "filter": { "lang": { "$eq": "en" } }
            })))
            .with_status(200)
            .with_body(r#"{"matches": []}"#)
            .create_async()
            .await;
        let sink = PineconeSink::new(server.url(), "test-key", None);
        let mut equals = HashMap::new();
        equals.insert(
            "lang".to_string(),
            super::super::traits::PayloadValue::String("en".into()),
        );
        let filter = PayloadFilter { equals };
        sink.query_nearest(&[1.0, 0.0], 5, Some(&filter))
            .await
            .unwrap();
        m.assert_async().await;
    }

    #[tokio::test]
    async fn pinecone_upsert_stores_doc_id_in_metadata() {
        let mut server = mockito::Server::new_async().await;
        let m = server
            .mock("POST", "/vectors/upsert")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "vectors": [{ "metadata": { "doc_id": "doc" } }]
            })))
            .with_status(200)
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
        m.assert_async().await;
    }

    #[tokio::test]
    async fn pinecone_delete_surfaces_rate_limit() {
        let mut server = mockito::Server::new_async().await;
        let m = server
            .mock("POST", "/vectors/delete")
            .with_status(429)
            .create_async()
            .await;
        let sink = PineconeSink::new(server.url(), "test-key", None);
        let err = sink.delete_by_ids(&["id-1".into()]).await.unwrap_err();
        m.assert_async().await;
        assert!(matches!(err, VectorSinkError::RateLimit(_)));
    }
}
