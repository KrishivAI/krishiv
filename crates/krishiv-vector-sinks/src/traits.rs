use std::collections::HashMap;
use std::fmt;

use async_trait::async_trait;

use crate::batch::EmbeddingBatch;

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
#[derive(Debug)]
pub enum VectorSinkError {
    Connection(String),
    Upsert(String),
    SchemaConflict(String),
    RateLimit(String),
    Timeout(String),
    Query(String),
}

impl fmt::Display for VectorSinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection(m) => write!(f, "vector sink connection error: {m}"),
            Self::Upsert(m) => write!(f, "vector sink upsert error: {m}"),
            Self::SchemaConflict(m) => write!(f, "vector sink schema conflict: {m}"),
            Self::RateLimit(m) => write!(f, "vector sink rate limit: {m}"),
            Self::Timeout(m) => write!(f, "vector sink timeout: {m}"),
            Self::Query(m) => write!(f, "vector sink query error: {m}"),
        }
    }
}

impl std::error::Error for VectorSinkError {}

pub type VectorSinkResult<T> = Result<T, VectorSinkError>;

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
