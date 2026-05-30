#![forbid(unsafe_code)]

//! Vector store sinks for embedding upsert and nearest-neighbor query (R17).

pub mod batch;
pub mod config;
pub mod id;
pub mod memory;
pub mod pinecone;
pub mod registry;
pub mod traits;
pub mod weaviate;

pub mod lancedb_sink;

#[cfg(feature = "pgvector")]
pub mod pgvector;

#[cfg(feature = "qdrant")]
pub mod qdrant;

pub use batch::EmbeddingBatch;
pub use config::VectorSinkConfig;
pub use id::point_id_from_doc_epoch;
pub use memory::InMemoryVectorSink;
pub use pinecone::PineconeSink;
pub use registry::VectorSinkRegistry;
pub use traits::{
    PayloadFilter, PayloadValue, ScoredChunk, VectorSink, VectorSinkError, validate_identifier,
};
pub use weaviate::WeaviateSink;

pub use lancedb_sink::LanceDbSink;

#[cfg(feature = "pgvector")]
pub use pgvector::PgvectorSink;

#[cfg(feature = "qdrant")]
pub use qdrant::QdrantSink;

#[cfg(test)]
mod certification;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_batch(doc_ids: Vec<&str>, epoch: u64) -> EmbeddingBatch {
        let ids: Vec<String> = doc_ids.into_iter().map(String::from).collect();
        let vectors: Vec<Vec<f32>> = (0..ids.len())
            .map(|i| vec![i as f32, (i + 1) as f32])
            .collect();
        let payloads: Vec<HashMap<String, PayloadValue>> = (0..ids.len())
            .map(|i| {
                let mut m = HashMap::new();
                m.insert("text".into(), PayloadValue::String(format!("text-{i}")));
                m
            })
            .collect();
        EmbeddingBatch::new(ids, vectors, payloads, epoch)
    }

    #[test]
    fn embedding_batch_construction() {
        let batch = make_batch(vec!["d1", "d2", "d3"], 1);
        assert_eq!(batch.len(), 3);
        assert!(!batch.is_empty());
        assert_eq!(batch.epoch, 1);
        assert_eq!(batch.doc_ids[0], "d1");
        assert_eq!(batch.vectors[1], vec![1.0, 2.0]);
    }

    #[test]
    fn embedding_batch_empty() {
        let batch = EmbeddingBatch::new(vec![], vec![], vec![], 0);
        assert_eq!(batch.len(), 0);
        assert!(batch.is_empty());
    }

    #[test]
    fn payload_value_variants() {
        let s = PayloadValue::String("hello".into());
        let i = PayloadValue::Int(42);
        let f = PayloadValue::Float(3.15);
        let b = PayloadValue::Bool(true);
        assert_eq!(s.to_json(), serde_json::json!("hello"));
        assert_eq!(i.to_json(), serde_json::json!(42));
        assert_eq!(f.to_json(), serde_json::json!(3.15));
        assert_eq!(b.to_json(), serde_json::json!(true));
    }

    #[test]
    fn payload_value_equality() {
        assert_eq!(
            PayloadValue::String("a".into()),
            PayloadValue::String("a".into())
        );
        assert_ne!(PayloadValue::String("a".into()), PayloadValue::Int(1));
        assert_eq!(PayloadValue::Int(10), PayloadValue::Int(10));
        assert_ne!(PayloadValue::Float(1.0), PayloadValue::Float(2.0));
        assert_eq!(PayloadValue::Bool(false), PayloadValue::Bool(false));
    }

    #[test]
    fn payload_filter_construction() {
        let mut equals = HashMap::new();
        equals.insert("lang".into(), PayloadValue::String("en".into()));
        equals.insert("active".into(), PayloadValue::Bool(true));
        let filter = PayloadFilter { equals };
        assert_eq!(filter.equals.len(), 2);
        assert_eq!(
            filter.equals.get("lang"),
            Some(&PayloadValue::String("en".into()))
        );
    }

    #[test]
    fn payload_filter_default_is_empty() {
        let filter = PayloadFilter::default();
        assert!(filter.equals.is_empty());
    }

    #[tokio::test]
    async fn memory_sink_upsert_and_query() {
        let sink = InMemoryVectorSink::new();
        let batch = make_batch(vec!["d1", "d2"], 1);
        sink.upsert_batch(&batch).await.unwrap();
        let results = sink.query_nearest(&[1.0, 2.0], 10, None).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].score, 1.0);
    }

    #[tokio::test]
    async fn memory_sink_delete() {
        let sink = InMemoryVectorSink::new();
        let batch = make_batch(vec!["d1", "d2"], 1);
        sink.upsert_batch(&batch).await.unwrap();
        let id1 = id::point_id_from_doc_epoch("d1", 1);
        sink.delete_by_ids(&[id1]).await.unwrap();
        let results = sink.query_nearest(&[1.0, 2.0], 10, None).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "d2");
    }

    #[tokio::test]
    async fn memory_sink_delete_nonexistent_is_noop() {
        let sink = InMemoryVectorSink::new();
        let batch = make_batch(vec!["d1"], 1);
        sink.upsert_batch(&batch).await.unwrap();
        sink.delete_by_ids(&["0000000000000000".into()])
            .await
            .unwrap();
        let results = sink.query_nearest(&[1.0, 2.0], 10, None).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn memory_sink_query_with_filter() {
        let sink = InMemoryVectorSink::new();
        let batch = make_batch(vec!["d1", "d2"], 1);
        sink.upsert_batch(&batch).await.unwrap();
        let mut equals = HashMap::new();
        equals.insert("text".into(), PayloadValue::String("text-0".into()));
        let filter = PayloadFilter { equals };
        let results = sink
            .query_nearest(&[1.0, 2.0], 10, Some(&filter))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "d1");
    }

    #[tokio::test]
    async fn memory_sink_query_empty_store() {
        let sink = InMemoryVectorSink::new();
        let results = sink.query_nearest(&[1.0, 2.0], 10, None).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn memory_sink_top_k_limits_results() {
        let sink = InMemoryVectorSink::new();
        let batch = make_batch(vec!["d1", "d2", "d3", "d4", "d5"], 1);
        sink.upsert_batch(&batch).await.unwrap();
        let results = sink.query_nearest(&[1.0, 2.0], 2, None).await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn memory_sink_upsert_overwrites() {
        let sink = InMemoryVectorSink::new();
        let batch = make_batch(vec!["d1"], 1);
        sink.upsert_batch(&batch).await.unwrap();
        sink.upsert_batch(&batch).await.unwrap();
        let results = sink.query_nearest(&[1.0, 2.0], 10, None).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn memory_sink_upsert_batch_length_mismatch_errors() {
        let sink = InMemoryVectorSink::new();
        let bad = EmbeddingBatch {
            doc_ids: vec!["a".into()],
            vectors: vec![vec![1.0], vec![2.0]],
            payloads: vec![HashMap::new()],
            epoch: 1,
        };
        let err = sink.upsert_batch(&bad).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn memory_sink_cosine_similarity_ordering() {
        let sink = InMemoryVectorSink::new();
        let batch = EmbeddingBatch::new(
            vec!["near".into(), "far".into()],
            vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            vec![HashMap::new(), HashMap::new()],
            1,
        );
        sink.upsert_batch(&batch).await.unwrap();
        let results = sink.query_nearest(&[1.0, 0.0], 10, None).await.unwrap();
        assert_eq!(results[0].doc_id, "near");
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn vector_sink_registry_register_and_get() {
        let mut reg = VectorSinkRegistry::new();
        let sink: Arc<dyn VectorSink> = Arc::new(InMemoryVectorSink::new());
        reg.register("mem", sink);
        let got = reg.get("mem");
        assert!(got.is_some());
        assert_eq!(got.unwrap().sink_name(), "memory");
    }

    #[test]
    fn vector_sink_registry_get_missing_returns_none() {
        let reg = VectorSinkRegistry::new();
        assert!(reg.get("nope").is_none());
    }

    #[test]
    fn vector_sink_registry_multiple_sinks() {
        let mut reg = VectorSinkRegistry::new();
        let s1: Arc<dyn VectorSink> = Arc::new(InMemoryVectorSink::new());
        let s2: Arc<dyn VectorSink> = Arc::new(InMemoryVectorSink::new());
        reg.register("a", s1);
        reg.register("b", s2);
        assert!(reg.get("a").is_some());
        assert!(reg.get("b").is_some());
        assert!(reg.get("c").is_none());
    }

    #[test]
    fn scored_chunk_construction() {
        let mut payload = HashMap::new();
        payload.insert("key".into(), PayloadValue::Int(1));
        let chunk = ScoredChunk {
            doc_id: "d1".into(),
            chunk_index: 3,
            text: "hello".into(),
            score: 0.95,
            payload,
        };
        assert_eq!(chunk.doc_id, "d1");
        assert_eq!(chunk.chunk_index, 3);
        assert_eq!(chunk.score, 0.95);
    }

    // ── VectorSinkError tests ───────────────────────────────────────────────

    #[test]
    fn vector_sink_error_display_connection() {
        let err = VectorSinkError::Connection("timeout".into());
        assert!(err.to_string().contains("connection error"));
        assert!(err.to_string().contains("timeout"));
    }

    #[test]
    fn vector_sink_error_display_upsert() {
        let err = VectorSinkError::Upsert("conflict".into());
        assert!(err.to_string().contains("upsert error"));
        assert!(err.to_string().contains("conflict"));
    }

    #[test]
    fn vector_sink_error_display_schema_conflict() {
        let err = VectorSinkError::SchemaConflict("dim mismatch".into());
        assert!(err.to_string().contains("schema conflict"));
        assert!(err.to_string().contains("dim mismatch"));
    }

    #[test]
    fn vector_sink_error_display_rate_limit() {
        let err = VectorSinkError::RateLimit("too many".into());
        assert!(err.to_string().contains("rate limit"));
        assert!(err.to_string().contains("too many"));
    }

    #[test]
    fn vector_sink_error_display_timeout() {
        let err = VectorSinkError::Timeout("deadline".into());
        assert!(err.to_string().contains("timeout"));
        assert!(err.to_string().contains("deadline"));
    }

    #[test]
    fn vector_sink_error_display_query() {
        let err = VectorSinkError::Query("bad query".into());
        assert!(err.to_string().contains("query error"));
        assert!(err.to_string().contains("bad query"));
    }

    #[test]
    fn vector_sink_error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(VectorSinkError::Connection("test".into()));
        assert!(!err.to_string().is_empty());
    }

    // ── EmbeddingBatch edge cases ───────────────────────────────────────────

    #[test]
    fn embedding_batch_single_element() {
        let batch = EmbeddingBatch::new(
            vec!["d1".into()],
            vec![vec![1.0, 2.0, 3.0]],
            vec![HashMap::new()],
            0,
        );
        assert_eq!(batch.len(), 1);
        assert!(!batch.is_empty());
        assert_eq!(batch.vectors[0], vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn embedding_batch_epoch_zero() {
        let batch = EmbeddingBatch::new(vec![], vec![], vec![], 0);
        assert_eq!(batch.epoch, 0);
        assert!(batch.is_empty());
    }

    #[test]
    fn embedding_batch_epoch_max() {
        let batch = EmbeddingBatch::new(vec![], vec![], vec![], u64::MAX);
        assert_eq!(batch.epoch, u64::MAX);
    }

    // ── PayloadValue edge cases ─────────────────────────────────────────────

    #[test]
    fn payload_value_string_empty() {
        let v = PayloadValue::String("".into());
        assert_eq!(v.to_json(), serde_json::json!(""));
    }

    #[test]
    fn payload_value_int_zero() {
        let v = PayloadValue::Int(0);
        assert_eq!(v.to_json(), serde_json::json!(0));
    }

    #[test]
    fn payload_value_int_negative() {
        let v = PayloadValue::Int(-42);
        assert_eq!(v.to_json(), serde_json::json!(-42));
    }

    #[test]
    fn payload_value_float_zero() {
        let v = PayloadValue::Float(0.0);
        assert_eq!(v.to_json(), serde_json::json!(0.0));
    }

    #[test]
    fn payload_value_float_negative() {
        let v = PayloadValue::Float(-3.15);
        assert_eq!(v.to_json(), serde_json::json!(-3.15));
    }

    #[test]
    fn payload_value_bool_false() {
        let v = PayloadValue::Bool(false);
        assert_eq!(v.to_json(), serde_json::json!(false));
    }

    #[test]
    fn payload_value_int_max() {
        let v = PayloadValue::Int(i64::MAX);
        assert_eq!(v.to_json(), serde_json::json!(i64::MAX));
    }

    // ── PayloadFilter edge cases ────────────────────────────────────────────

    #[test]
    fn payload_filter_single_entry() {
        let mut equals: HashMap<String, PayloadValue> = HashMap::new();
        equals.insert("key".into(), PayloadValue::Int(1));
        let filter = PayloadFilter { equals };
        assert_eq!(filter.equals.len(), 1);
    }

    #[test]
    fn payload_filter_matches_all_entries() {
        let mut payload: HashMap<String, PayloadValue> = HashMap::new();
        payload.insert("lang".into(), PayloadValue::String("en".into()));
        payload.insert("active".into(), PayloadValue::Bool(true));

        let mut equals: HashMap<String, PayloadValue> = HashMap::new();
        equals.insert("lang".into(), PayloadValue::String("en".into()));
        equals.insert("active".into(), PayloadValue::Bool(true));
        let filter = PayloadFilter { equals };

        assert!(filter.equals.iter().all(|(k, v)| payload.get(k) == Some(v)));
    }

    #[test]
    fn payload_filter_no_match() {
        let mut payload: HashMap<String, PayloadValue> = HashMap::new();
        payload.insert("lang".into(), PayloadValue::String("en".into()));

        let mut equals: HashMap<String, PayloadValue> = HashMap::new();
        equals.insert("lang".into(), PayloadValue::String("fr".into()));
        let filter = PayloadFilter { equals };

        assert!(!filter.equals.iter().all(|(k, v)| payload.get(k) == Some(v)));
    }

    // ── InMemoryVectorSink additional tests ─────────────────────────────────

    #[tokio::test]
    async fn memory_sink_upsert_empty_batch() {
        let sink = InMemoryVectorSink::new();
        let batch = EmbeddingBatch::new(vec![], vec![], vec![], 1);
        sink.upsert_batch(&batch).await.unwrap();
        let results = sink.query_nearest(&[1.0, 2.0], 10, None).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn memory_sink_delete_all() {
        let sink = InMemoryVectorSink::new();
        let batch = make_batch(vec!["d1", "d2", "d3"], 1);
        sink.upsert_batch(&batch).await.unwrap();
        let ids: Vec<String> = batch
            .doc_ids
            .iter()
            .map(|id| id::point_id_from_doc_epoch(id, 1))
            .collect();
        sink.delete_by_ids(&ids).await.unwrap();
        let results = sink.query_nearest(&[1.0, 2.0], 10, None).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn memory_sink_query_top_k_zero() {
        let sink = InMemoryVectorSink::new();
        let batch = make_batch(vec!["d1"], 1);
        sink.upsert_batch(&batch).await.unwrap();
        let results = sink.query_nearest(&[1.0, 2.0], 0, None).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn memory_sink_upsert_different_epochs() {
        let sink = InMemoryVectorSink::new();
        let batch1 = EmbeddingBatch::new(
            vec!["d1".into()],
            vec![vec![1.0, 0.0]],
            vec![HashMap::<String, PayloadValue>::new()],
            1,
        );
        let batch2 = EmbeddingBatch::new(
            vec!["d1".into()],
            vec![vec![0.0, 1.0]],
            vec![HashMap::<String, PayloadValue>::new()],
            2,
        );
        sink.upsert_batch(&batch1).await.unwrap();
        sink.upsert_batch(&batch2).await.unwrap();
        // Both should exist (different point IDs due to different epochs)
        let results = sink.query_nearest(&[1.0, 0.0], 10, None).await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn memory_sink_name() {
        let sink = InMemoryVectorSink::new();
        assert_eq!(sink.sink_name(), "memory");
    }

    // ── VectorSinkRegistry additional tests ─────────────────────────────────

    #[test]
    fn registry_overwrites_same_name() {
        let mut reg = VectorSinkRegistry::new();
        let s1: Arc<dyn VectorSink> = Arc::new(InMemoryVectorSink::new());
        let s2: Arc<dyn VectorSink> = Arc::new(InMemoryVectorSink::new());
        reg.register("my_sink", s1);
        reg.register("my_sink", s2);
        let got = reg.get("my_sink").unwrap();
        assert_eq!(got.sink_name(), "memory");
    }

    #[test]
    fn registry_get_returns_arc() {
        let mut reg = VectorSinkRegistry::new();
        let s: Arc<dyn VectorSink> = Arc::new(InMemoryVectorSink::new());
        reg.register("test", s.clone());
        let got = reg.get("test").unwrap();
        // Both should point to the same underlying sink
        assert!(Arc::ptr_eq(&s, &got));
    }

    #[test]
    fn registry_from_config_memory() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let config = VectorSinkConfig::Memory;
            let sink = VectorSinkRegistry::from_config(&config).await.unwrap();
            assert_eq!(sink.sink_name(), "memory");
        });
    }

    #[test]
    fn registry_from_config_weaviate() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let config = VectorSinkConfig::Weaviate {
                base_url: "http://localhost:8080".into(),
                class_name: "Document".into(),
                api_key: None,
            };
            let sink = VectorSinkRegistry::from_config(&config).await.unwrap();
            assert_eq!(sink.sink_name(), "weaviate");
        });
    }

    // S2 regression: validate_identifier prevents injection
    #[test]
    fn validate_identifier_rejects_bad_names() {
        use crate::traits::validate_identifier;
        assert!(validate_identifier("good_name").is_ok());
        assert!(validate_identifier("_leading_underscore_ok").is_ok());
        assert!(validate_identifier("bad-name").is_err());
        assert!(validate_identifier("bad name").is_err());
        assert!(validate_identifier("123start").is_err());
        assert!(validate_identifier("select; drop").is_err());
        assert!(validate_identifier("").is_err());
    }

    #[test]
    fn registry_from_config_pinecone() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let config = VectorSinkConfig::Pinecone {
                host: "index.svc.pinecone.io".into(),
                api_key: "key".into(),
                namespace: Some("ns".into()),
            };
            let sink = VectorSinkRegistry::from_config(&config).await.unwrap();
            assert_eq!(sink.sink_name(), "pinecone");
        });
    }

    #[test]
    fn registry_from_config_lancedb() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp = tempfile::tempdir().unwrap();
        rt.block_on(async {
            let config = VectorSinkConfig::LanceDb {
                uri: temp.path().to_str().unwrap().into(),
                table: "test_table".into(),
                vector_dim: 2,
            };
            let sink = VectorSinkRegistry::from_config(&config).await.unwrap();
            assert_eq!(sink.sink_name(), "lancedb");
        });
    }

    // ── point_id_from_doc_epoch tests ───────────────────────────────────────

    #[test]
    fn point_id_is_hex() {
        let id = id::point_id_from_doc_epoch("doc-1", 1);
        assert_eq!(id.len(), 16);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn point_id_different_docs_different_ids() {
        let id1 = id::point_id_from_doc_epoch("doc-1", 1);
        let id2 = id::point_id_from_doc_epoch("doc-2", 1);
        assert_ne!(id1, id2);
    }

    #[test]
    fn point_id_different_epochs_different_ids() {
        let id1 = id::point_id_from_doc_epoch("doc-1", 1);
        let id2 = id::point_id_from_doc_epoch("doc-1", 2);
        assert_ne!(id1, id2);
    }

    // ── LanceDbSink tests ───────────────────────────────────────────────────

    #[tokio::test]
    async fn lancedb_sink_open_creates_dir() {
        let temp = tempfile::tempdir().unwrap();
        let sink = LanceDbSink::open(temp.path(), "test_table", 2)
            .await
            .unwrap();
        assert_eq!(sink.sink_name(), "lancedb");
        // The root uri dir should exist
        assert!(temp.path().is_dir());
    }

    #[tokio::test]
    async fn lancedb_sink_upsert_and_query() {
        let temp = tempfile::tempdir().unwrap();
        let sink = LanceDbSink::open(temp.path(), "test_table", 2)
            .await
            .unwrap();
        let batch = EmbeddingBatch::new(
            vec!["d1".into(), "d2".into()],
            vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            vec![
                HashMap::<String, PayloadValue>::new(),
                HashMap::<String, PayloadValue>::new(),
            ],
            1,
        );
        sink.upsert_batch(&batch).await.unwrap();
        let results = sink.query_nearest(&[1.0, 0.0], 10, None).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc_id, "d1");
    }

    #[tokio::test]
    async fn lancedb_sink_delete() {
        let temp = tempfile::tempdir().unwrap();
        let sink = LanceDbSink::open(temp.path(), "test_table", 2)
            .await
            .unwrap();
        let batch = EmbeddingBatch::new(
            vec!["d1".into()],
            vec![vec![1.0, 0.0]],
            vec![HashMap::<String, PayloadValue>::new()],
            1,
        );
        sink.upsert_batch(&batch).await.unwrap();
        let id = id::point_id_from_doc_epoch("d1", 1);
        sink.delete_by_ids(&[id]).await.unwrap();
        let results = sink.query_nearest(&[1.0, 0.0], 10, None).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn lancedb_sink_empty_query() {
        let temp = tempfile::tempdir().unwrap();
        let sink = LanceDbSink::open(temp.path(), "test_table", 2)
            .await
            .unwrap();
        let results = sink.query_nearest(&[1.0, 0.0], 10, None).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn lancedb_sink_dim_mismatch_errors() {
        let temp = tempfile::tempdir().unwrap();
        let sink = LanceDbSink::open(temp.path(), "test_table", 2)
            .await
            .unwrap();
        let batch = EmbeddingBatch::new(
            vec!["d1".into()],
            vec![vec![1.0, 0.0, 0.0]], // 3 dims, expected 2
            vec![HashMap::<String, PayloadValue>::new()],
            1,
        );
        let err = sink.upsert_batch(&batch).await.unwrap_err();
        assert!(err.to_string().contains("vector dim mismatch"));
    }
}
