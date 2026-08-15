use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;

use super::batch::EmbeddingBatch;
use super::id::point_id_from_doc_epoch;
use super::traits::{
    PayloadFilter, PayloadValue, ScoredChunk, VectorSink, VectorSinkError, VectorSinkResult,
    validate_identifier,
};

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
    ) -> VectorSinkResult<Self> {
        let class_name = class_name.into();
        validate_identifier(&class_name)?;
        // 5 s connect / 30 s request budget per Weaviate call. Without a
        // request timeout, a stalled TCP connection or unresponsive Weaviate
        // host would hang the vector-ingest pipeline indefinitely. Falls
        // back to `Client::new()` if the builder itself fails.
        let client = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| Client::new());
        Ok(Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            class_name,
            api_key,
        })
    }

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(key) = &self.api_key {
            req.header("Authorization", format!("Bearer {key}"))
        } else {
            req
        }
    }
}

/// Derive a deterministic RFC-4122 UUID (v5-style, name-based via SHA-256)
/// from a Krishiv point id. Weaviate rejects object ids that are not UUIDs.
fn uuid_from_point_id(point_id: &str) -> String {
    let digest = krishiv_common::hash::sha256_bytes_multi(&[point_id.as_bytes()]);
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50; // version 5
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC-4122 variant
    let hex = |range: std::ops::Range<usize>| -> String {
        bytes
            .get(range)
            .map(|group| group.iter().map(|b| format!("{b:02x}")).collect())
            .unwrap_or_default()
    };
    format!(
        "{}-{}-{}-{}-{}",
        hex(0..4),
        hex(4..6),
        hex(6..8),
        hex(8..10),
        hex(10..16)
    )
}

/// Render a `PayloadFilter` as a Weaviate GraphQL `where` argument.
fn filter_to_where(filter: &PayloadFilter) -> VectorSinkResult<String> {
    let mut operands = Vec::new();
    for (key, value) in &filter.equals {
        validate_identifier(key)
            .map_err(|_| VectorSinkError::Query(format!("invalid filter key: {key}")))?;
        let clause = match value {
            PayloadValue::String(s) => {
                let quoted = serde_json::Value::String(s.clone()).to_string();
                format!("{{ path: [\"{key}\"], operator: Equal, valueText: {quoted} }}")
            }
            PayloadValue::Int(i) => {
                format!("{{ path: [\"{key}\"], operator: Equal, valueInt: {i} }}")
            }
            PayloadValue::Float(f) => {
                format!("{{ path: [\"{key}\"], operator: Equal, valueNumber: {f} }}")
            }
            PayloadValue::Bool(b) => {
                format!("{{ path: [\"{key}\"], operator: Equal, valueBoolean: {b} }}")
            }
        };
        operands.push(clause);
    }
    Ok(format!(
        "{{ operator: And, operands: [{}] }}",
        operands.join(", ")
    ))
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
            let id = uuid_from_point_id(&point_id_from_doc_epoch(doc_id, batch.epoch));
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
            let url = format!("{}/v1/objects/{}", self.base_url, uuid_from_point_id(id));
            let response = self
                .auth(self.client.delete(&url))
                .send()
                .await
                .map_err(|e| VectorSinkError::Connection(e.to_string()))?;
            // 204 No Content is the success response for a Weaviate delete.
            // 404 is acceptable: the object may have been deleted by a prior
            // call. Anything else is a real error.
            let status = response.status();
            if status.is_success() || status == reqwest::StatusCode::NOT_FOUND {
                continue;
            }
            let text = response.text().await.unwrap_or_default();
            return Err(VectorSinkError::Delete(format!("{status}: {text}")));
        }
        Ok(())
    }

    async fn query_nearest(
        &self,
        vector: &[f32],
        top_k: usize,
        filter: Option<&PayloadFilter>,
    ) -> VectorSinkResult<Vec<ScoredChunk>> {
        let where_arg = match filter {
            Some(f) if !f.equals.is_empty() => format!(", where: {}", filter_to_where(f)?),
            _ => String::new(),
        };
        // Weaviate GraphQL contract: requested properties are top-level fields
        // on each hit, and nearVector exposes `distance` under `_additional`.
        let body = json!({
            "query": format!(
                "{{ Get {{ {class}(limit: {limit}, nearVector: {{ vector: [{vec}] }}{where_arg}) {{ text chunk_index doc_id _additional {{ distance }} }} }} }}",
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
            // Cosine distance is in [0, 2]; convert to a similarity score.
            let score = hit
                .pointer("/_additional/distance")
                .and_then(|v| v.as_f64())
                .map(|d| 1.0 - d as f32)
                .unwrap_or(0.0);
            let text = hit
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let chunk_index = hit.get("chunk_index").and_then(|v| v.as_i64()).unwrap_or(0) as usize;
            let doc_id = hit
                .get("doc_id")
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
    use super::super::traits::VectorSink;
    use super::*;

    #[tokio::test]
    async fn weaviate_query_returns_results() {
        let mut server = mockito::Server::new_async().await;
        // Real Weaviate GraphQL response shape: requested properties are
        // top-level fields on the hit; nearVector exposes `distance`.
        let body = serde_json::json!({
            "data": {
                "Get": {
                    "Document": [{
                        "text": "hello",
                        "chunk_index": 2,
                        "doc_id": "d1",
                        "_additional": { "distance": 0.09 }
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
        let sink = WeaviateSink::new(server.url(), "Document", None).unwrap();
        let hits = sink.query_nearest(&[0.1, 0.2], 1, None).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].text, "hello");
        assert_eq!(hits[0].chunk_index, 2);
        assert_eq!(hits[0].doc_id, "d1");
        assert!((hits[0].score - 0.91).abs() < 1e-6);
    }

    #[test]
    fn weaviate_object_id_is_rfc4122_uuid() {
        let id = uuid_from_point_id(&point_id_from_doc_epoch("d1", 1));
        let parts: Vec<&str> = id.split('-').collect();
        assert_eq!(parts.len(), 5);
        let lens: Vec<usize> = parts.iter().map(|p| p.len()).collect();
        assert_eq!(lens, vec![8, 4, 4, 4, 12]);
        assert!(parts[2].starts_with('5'), "version nibble must be 5: {id}");
        assert!(
            matches!(parts[3].as_bytes()[0], b'8' | b'9' | b'a' | b'b'),
            "variant nibble must be RFC-4122: {id}"
        );
        // Deterministic across calls.
        assert_eq!(id, uuid_from_point_id(&point_id_from_doc_epoch("d1", 1)));
    }

    #[tokio::test]
    async fn weaviate_upsert_sends_uuid_object_id() {
        let mut server = mockito::Server::new_async().await;
        let expected_id = uuid_from_point_id(&point_id_from_doc_epoch("d1", 1));
        let m = server
            .mock("PUT", "/v1/objects")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({ "id": expected_id }),
            ))
            .with_status(200)
            .create_async()
            .await;
        let sink = WeaviateSink::new(server.url(), "Document", None).unwrap();
        let batch = EmbeddingBatch::new(
            vec!["d1".into()],
            vec![vec![0.1, 0.2]],
            vec![HashMap::new()],
            1,
        );
        sink.upsert_batch(&batch).await.unwrap();
        m.assert_async().await;
    }

    #[tokio::test]
    async fn weaviate_query_sends_where_filter() {
        let mut server = mockito::Server::new_async().await;
        let body = serde_json::json!({ "data": { "Get": { "Document": [] } } });
        let m = server
            .mock("POST", "/v1/graphql")
            .match_request(|req| {
                let text = req
                    .body()
                    .map(|b| String::from_utf8_lossy(b).to_string())
                    .unwrap_or_default();
                text.contains("where:")
                    && text.contains("operator: Equal")
                    && text.contains(r#"path: [\"lang\"]"#)
                    && text.contains(r#"valueText: \"en\""#)
            })
            .with_status(200)
            .with_body(body.to_string())
            .create_async()
            .await;
        let sink = WeaviateSink::new(server.url(), "Document", None).unwrap();
        let mut equals = HashMap::new();
        equals.insert("lang".into(), PayloadValue::String("en".into()));
        let filter = PayloadFilter { equals };
        sink.query_nearest(&[0.1, 0.2], 1, Some(&filter))
            .await
            .unwrap();
        m.assert_async().await;
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
        let sink = WeaviateSink::new(server.url(), "Document", None).unwrap();
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
