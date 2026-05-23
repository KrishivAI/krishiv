use std::sync::Arc;


use crate::config::VectorSinkConfig;
use crate::memory::InMemoryVectorSink;
use crate::pinecone::PineconeSink;
use crate::traits::{VectorSink, VectorSinkResult};
use crate::weaviate::WeaviateSink;

use crate::lancedb_sink::LanceDbSink;

#[cfg(feature = "pgvector")]
use crate::pgvector::PgvectorSink;

#[cfg(feature = "qdrant")]
use crate::qdrant::QdrantSink;

/// Registry of named vector sinks (R17 connector integration).
#[derive(Default)]
pub struct VectorSinkRegistry {
    sinks: Vec<(String, Arc<dyn VectorSink>)>,
}

impl VectorSinkRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a sink by name.
    pub fn register(&mut self, name: impl Into<String>, sink: Arc<dyn VectorSink>) {
        self.sinks.push((name.into(), sink));
    }

    /// Look up a sink by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn VectorSink>> {
        self.sinks
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, s)| Arc::clone(s))
    }

    /// Build a sink from configuration.
    pub async fn from_config(config: &VectorSinkConfig) -> VectorSinkResult<Arc<dyn VectorSink>> {
        match config {
            VectorSinkConfig::Memory => Ok(Arc::new(InMemoryVectorSink::new())),
            #[cfg(feature = "qdrant")]
            VectorSinkConfig::Qdrant {
                url,
                collection,
                vector_size,
                create_collection_if_missing,
            } => Ok(Arc::new(
                QdrantSink::connect(url, collection, *vector_size, *create_collection_if_missing)
                    .await?,
            )),
            #[cfg(not(feature = "qdrant"))]
            VectorSinkConfig::Qdrant { .. } => Err(VectorSinkError::Connection(
                "qdrant feature disabled".into(),
            )),
            #[cfg(feature = "pgvector")]
            VectorSinkConfig::Pgvector {
                database_url,
                table,
                vector_dim,
            } => Ok(Arc::new(
                PgvectorSink::connect(database_url, table, *vector_dim).await?,
            )),
            #[cfg(not(feature = "pgvector"))]
            VectorSinkConfig::Pgvector { .. } => Err(VectorSinkError::Connection(
                "pgvector feature disabled".into(),
            )),
            VectorSinkConfig::LanceDb {
                uri,
                table,
                vector_dim,
            } => Ok(Arc::new(LanceDbSink::open(uri, table, *vector_dim).await?)),
            
            VectorSinkConfig::Weaviate {
                base_url,
                class_name,
                api_key,
            } => Ok(Arc::new(WeaviateSink::new(
                base_url,
                class_name,
                api_key.clone(),
            ))),
            VectorSinkConfig::Pinecone {
                host,
                api_key,
                namespace,
            } => Ok(Arc::new(PineconeSink::new(host, api_key, namespace.clone()))),
        }
    }
}
