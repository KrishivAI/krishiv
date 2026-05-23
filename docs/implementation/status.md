# Krishiv Implementation Status

## Current Phase

**R17 IN PROGRESS (2026-05-23)** — AI/ML native data platform.

Release tracker: [`r17-ai-ml-data-platform.md`](r17-ai-ml-data-platform.md)

## R17 Implementation (2026-05-23)

Branch: `cursor/r17-ai-ml-platform-7aa2`

### Completed

- **`krishiv-vector-sinks`**: `VectorSink` trait, idempotent upsert (`hash(doc_id || epoch)`), sinks for Qdrant, pgvector, Lance-compatible Parquet, Weaviate, Pinecone, in-memory + registry.
- **`krishiv-ai`**: `EmbeddingModelRegistry`, OpenAI embeddings/LLM, HuggingFace via `fastembed` (feature `fastembed-local`), four chunkers, `ChunkOperator` in `krishiv-exec`, semantic dedup, `MemoStore` (redb), `RagIndexPipeline` / `RagQuery`.
- **`krishiv-plan`**: `RagIndexSpec`, `FeatureStore`, chunker/embedder plan types.
- **`krishiv-connectors`**: `FeatureStoreSink` with Parquet backfill and point-in-time lookup; streaming updates via `InMemoryFeatureStream`.
- **`krishiv-scheduler`**: `LlmQuotaAggregator` + throttle commands (`llm_quota` module).
- **`krishiv-executor`**: `llm_throttle` applies coordinator limits to `LlmRateLimiter`.
- **`krishiv-python`**: `krishiv.ai` submodule (chunkers, `rag_index`, `rag_query`).

### Validation

```
cargo test -p krishiv-vector-sinks          → 4 passed
cargo test -p krishiv-ai --no-default-features → 12 passed
cargo test -p krishiv-connectors feature_store → 2 passed
cargo check -p krishiv-vector-sinks -p krishiv-ai -p krishiv-python -p krishiv-exec -p krishiv-executor
```

### Next Task

- Wire proto `LlmQuotaReport` / `LlmThrottleCommand` through gRPC heartbeat (messages added to `.proto`; hand-written `krishiv-proto` conversion pending).
- Run `cargo clippy` fixes on new crates (`-D warnings`).
- `pytest` for `python/krishiv-ai/tests/` after `maturin develop` with `fastembed-local`.
- Full workspace `cargo test --workspace` after scheduler test metadata initializer fix.

Validation: `cargo test -p krishiv-vector-sinks -p krishiv-ai --no-default-features -p krishiv-connectors feature_store`
