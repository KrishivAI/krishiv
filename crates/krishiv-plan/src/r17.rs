//! R17 plan types: RAG index specs and hybrid feature store.

use serde::{Deserialize, Serialize};

/// Data source reference for RAG / feature store plans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataSource {
    pub name: String,
    pub format: String,
    pub path: Option<String>,
}

/// Chunker configuration for RAG indexing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum ChunkerConfig {
    RecursiveText { chunk_size: usize, overlap: usize },
    Sentence { max_sentences: usize },
    TokenAware { max_tokens: usize, tokenizer: String },
    MarkdownSection { min_level: u8 },
}

/// Embedder configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbedderConfig {
    pub model: String,
    pub api_key_env: Option<String>,
}

/// Vector sink configuration (delegates to krishiv-vector-sinks JSON).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorSinkPlanConfig {
    pub sink_type: String,
    pub options: serde_json::Value,
}

/// RAG refresh policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshPolicy {
    Manual,
    Schedule { cron: String },
    Continuous,
}

/// RAG index job specification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RagIndexSpec {
    pub source: DataSource,
    pub chunker: ChunkerConfig,
    pub embedder: EmbedderConfig,
    pub vector_store: VectorSinkPlanConfig,
    pub refresh: RefreshPolicy,
}

/// Feature definition for the hybrid feature store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureDef {
    pub name: String,
    pub dtype: String,
    pub ttl_ms: Option<u64>,
}

/// Feature schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureSchema {
    pub features: Vec<FeatureDef>,
    pub entity_key: Vec<String>,
}

/// Feature store plan object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureStore {
    pub name: String,
    pub batch_source: DataSource,
    pub stream_source: Option<DataSource>,
    pub feature_schema: FeatureSchema,
}
