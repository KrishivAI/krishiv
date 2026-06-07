use std::collections::HashMap;

use async_trait::async_trait;

use super::batch::EmbeddingBatch;

/// JSON-compatible payload value for vector store metadata.
#[derive(Debug, Clone, PartialEq)]
pub enum PayloadValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl PayloadValue {
    /// Render as JSON value for REST sinks.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Self::String(s) => serde_json::Value::String(s.clone()),
            Self::Int(i) => serde_json::json!(*i),
            Self::Float(f) => serde_json::json!(*f),
            Self::Bool(b) => serde_json::Value::Bool(*b),
        }
    }
}

/// Optional metadata filter for vector queries.
#[derive(Debug, Clone, Default)]
pub struct PayloadFilter {
    pub equals: HashMap<String, PayloadValue>,
}

/// One nearest-neighbor search result.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoredChunk {
    pub doc_id: String,
    pub chunk_index: usize,
    pub text: String,
    pub score: f32,
    pub payload: HashMap<String, PayloadValue>,
}

/// Errors from vector sink operations.
#[derive(Debug, thiserror::Error)]
pub enum VectorSinkError {
    #[error("vector sink connection error: {0}")]
    Connection(String),
    #[error("vector sink upsert error: {0}")]
    Upsert(String),
    #[error("vector sink delete error: {0}")]
    Delete(String),
    #[error("vector sink schema conflict: {0}")]
    SchemaConflict(String),
    #[error("vector sink rate limit: {0}")]
    RateLimit(String),
    #[error("vector sink timeout: {0}")]
    Timeout(String),
    #[error("vector sink query error: {0}")]
    Query(String),
}

pub type VectorSinkResult<T> = Result<T, VectorSinkError>;

/// Validates a SQL/GraphQL identifier (table name, class name, etc.) to prevent injection.
/// Allowed: ^[A-Za-z_][A-Za-z0-9_]*$ (per S2 in crate-stability-resolution-plan).
pub fn validate_identifier(name: &str) -> VectorSinkResult<()> {
    krishiv_common::validate::validate_sql_identifier(name)
        .map_err(|e| VectorSinkError::Connection(e.message))
}

/// Vector store sink contract (ADR-R17.3 idempotent upsert).
#[async_trait]
pub trait VectorSink: Send + Sync {
    /// Sink name for metrics and registry lookup.
    fn sink_name(&self) -> &str;

    /// Idempotent upsert for a batch at `batch.epoch`.
    async fn upsert_batch(&self, batch: &EmbeddingBatch) -> VectorSinkResult<()>;

    /// Delete points by Krishiv point ids.
    async fn delete_by_ids(&self, ids: &[String]) -> VectorSinkResult<()>;

    /// Nearest-neighbor search.
    async fn query_nearest(
        &self,
        vector: &[f32],
        top_k: usize,
        filter: Option<&PayloadFilter>,
    ) -> VectorSinkResult<Vec<ScoredChunk>>;
}
