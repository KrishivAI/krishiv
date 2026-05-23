use serde::{Deserialize, Serialize};

/// Unified vector sink configuration for job specs and connector registry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VectorSinkConfig {
    Memory,
    Qdrant {
        url: String,
        collection: String,
        vector_size: u64,
        create_collection_if_missing: bool,
    },
    Pgvector {
        database_url: String,
        table: String,
        vector_dim: usize,
    },
    LanceDb {
        uri: String,
        table: String,
        vector_dim: usize,
    },
    Weaviate {
        base_url: String,
        class_name: String,
        api_key: Option<String>,
    },
    Pinecone {
        host: String,
        api_key: String,
        namespace: Option<String>,
    },
}
