#![forbid(unsafe_code)]

//! AI/ML operators for Krishiv: embeddings, chunking, LLM UDFs, RAG, semantic dedup (R17).

pub mod chunk;
pub mod dedup;
pub mod embed;
pub mod llm;
pub mod memo;
pub mod rag;
#[path = "vector_sinks.rs"]
pub mod vector_sinks;

pub use chunk::{
    Chunk, MarkdownSectionChunker, RecursiveTextChunker, SentenceChunker, TextChunker,
    TokenAwareChunker,
};
pub use dedup::{DedupStrategy, SemanticDedup, SemanticDedupConfig};
pub use embed::{
    EmbeddingDevice, EmbeddingError, EmbeddingModel, EmbeddingModelRegistry, ModelKey,
    OpenAiEmbeddingModel,
};
pub use llm::{
    LlmError, LlmRateLimiter, LlmResponse, LlmUdf, LlmUdfConfig, OpenAiLlmUdf, RateLimitConfig,
};
pub use memo::{MemoEntry, MemoStore, memo_key};
pub use rag::{RagIndexPipeline, RagIndexResult, RagQuery, RefreshPolicy};
pub use vector_sinks::{
    EmbeddingBatch, InMemoryVectorSink, LanceDbSink, PayloadFilter, PayloadValue, PineconeSink,
    ScoredChunk, VectorSink, VectorSinkConfig, VectorSinkError, VectorSinkRegistry,
    WeaviateSink, point_id_from_doc_epoch, validate_identifier,
};

#[cfg(feature = "fastembed-local")]
pub use embed::HuggingFaceEmbeddingModel;

#[cfg(feature = "pgvector")]
pub use vector_sinks::PgvectorSink;

#[cfg(feature = "qdrant")]
pub use vector_sinks::QdrantSink;
