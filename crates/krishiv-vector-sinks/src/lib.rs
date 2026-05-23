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
pub use traits::{PayloadFilter, PayloadValue, ScoredChunk, VectorSink, VectorSinkError};
pub use weaviate::WeaviateSink;

pub use lancedb_sink::LanceDbSink;

#[cfg(feature = "pgvector")]
pub use pgvector::PgvectorSink;

#[cfg(feature = "qdrant")]
pub use qdrant::QdrantSink;

#[cfg(test)]
mod certification;
