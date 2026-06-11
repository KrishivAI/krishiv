#![forbid(unsafe_code)]

//! Thin re-export shim for vector sinks (from krishiv-connectors).

#[cfg(feature = "vector-sinks")]
pub use krishiv_connectors::vector::*;

#[cfg(all(feature = "vector-sinks", feature = "pgvector"))]
pub use krishiv_connectors::PgvectorSink;

#[cfg(all(feature = "vector-sinks", feature = "qdrant"))]
pub use krishiv_connectors::QdrantSink;
