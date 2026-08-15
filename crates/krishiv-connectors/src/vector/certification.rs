use std::collections::HashMap;
use std::sync::Arc;

use super::batch::EmbeddingBatch;
use super::memory::InMemoryVectorSink;
use super::traits::{PayloadValue, VectorSink};

/// Idempotency certification (ADR-R17.3), run against the sinks that need no
/// external service: the in-memory sink and the local-disk LanceDB sink.
#[tokio::test]
async fn upsert_batch_idempotent_for_memory_sink() {
    certify_idempotency(Arc::new(InMemoryVectorSink::new())).await;
}

#[tokio::test]
async fn upsert_batch_idempotent_for_lancedb_sink() {
    let temp = tempfile::tempdir().expect("tempdir");
    let sink = super::lancedb_sink::LanceDbSink::open(temp.path(), "cert_table", 3)
        .await
        .expect("open");
    certify_idempotency(Arc::new(sink)).await;
}

pub async fn certify_idempotency(sink: Arc<dyn VectorSink>) {
    let mut payload = HashMap::new();
    payload.insert("text".into(), PayloadValue::String("hello".into()));
    let batch = EmbeddingBatch::new(
        vec!["doc-a".into()],
        vec![vec![1.0, 0.0, 0.0]],
        vec![payload],
        99,
    );
    sink.upsert_batch(&batch).await.expect("first upsert");
    sink.upsert_batch(&batch).await.expect("second upsert");
    let results = sink
        .query_nearest(&[1.0, 0.0, 0.0], 10, None)
        .await
        .expect("query");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].doc_id, "doc-a");
}
