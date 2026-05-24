#![forbid(unsafe_code)]

//! AI/ML operators for Krishiv: embeddings, chunking, LLM UDFs, RAG, semantic dedup (R17).

pub mod chunk;
pub mod dedup;
pub mod embed;
pub mod llm;
pub mod memo;
pub mod rag;

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

#[cfg(feature = "fastembed-local")]
pub use embed::HuggingFaceEmbeddingModel;
