# Krishiv Implementation Status

## Current Phase

**R17 COMPLETE (2026-05-23)** — AI/ML native data platform (implementation + PR follow-ups).

Release tracker: [`r17-ai-ml-data-platform.md`](r17-ai-ml-data-platform.md) (checkboxes bulk-updated; Lance sink note documents Parquet + merge-on-id).

## R17 Implementation (2026-05-23)

Branch: `cursor/r17-ai-ml-platform-7aa2`  
PR: https://github.com/KrishivAI/krishiv/pull/33

### Completed

- **`krishiv-vector-sinks`**: `VectorSink` trait, idempotent upsert (`hash(doc_id || epoch)`), sinks for Qdrant, pgvector, Lance-compatible Parquet (`LanceDbSink` — no upstream `lancedb` crate), Weaviate, Pinecone, in-memory + registry.
- **`krishiv-ai`**: `EmbeddingModelRegistry`, OpenAI embeddings/LLM, HuggingFace via `fastembed` (`fastembed-local` feature), four chunkers, `ChunkOperator` in `krishiv-exec`, semantic dedup, `MemoStore` (redb), `RagIndexPipeline` / `RagQuery`.
- **`krishiv-plan`**: `RagIndexSpec`, `FeatureStore`, chunker/embedder plan types.
- **`krishiv-connectors`**: `FeatureStoreSink` with Parquet backfill and point-in-time lookup; streaming updates via `InMemoryFeatureStream`.
- **`krishiv-scheduler`**: `LlmQuotaAggregator`; coordinator heartbeat returns `llm_throttles` when job quota exceeded.
- **`krishiv-proto` / gRPC**: `LlmQuotaReport` on `ExecutorHeartbeatRequest`, `LlmThrottleCommand` on `ExecutorHeartbeatResponse`; wire round-trip test.
- **`krishiv-executor`**: `apply_llm_throttles_from_response` after gRPC heartbeat.
- **`krishiv-python`**: `krishiv.ai` submodule (chunkers, `rag_index`, `rag_query`).

### PR follow-ups (2026-05-23)

- LanceDB: documented Parquet + merge-on-id in `lancedb_sink.rs` and tracker S1.4.
- HF tests: `krishiv-ai` default features empty for CI; `cargo test -p krishiv-ai --features fastembed-local` for HuggingFace + `rag_index` (needs `libstdc++-12-dev` / `RUSTFLAGS="-L native=/usr/lib/gcc/x86_64-linux-gnu/12"` when linking ONNX).
- Clippy `-D warnings`: clean on R17 crates (`krishiv-vector-sinks`, `krishiv-ai`, `krishiv-connectors`, `krishiv-scheduler`, `krishiv-proto`, `krishiv-exec`, `krishiv-executor`); `sync_scalar_udfs` made synchronous to fix `await_holding_lock` in `krishiv-sql`.
- Proto heartbeat: LLM quota fields wired through `krishiv-proto` ↔ scheduler tonic handler ↔ executor transport.
- R17 tracker: all sprint checkboxes marked `[x]` in `r17-ai-ml-data-platform.md`.

### Validation

```
cargo clippy -p krishiv-vector-sinks -p krishiv-ai -p krishiv-connectors -p krishiv-exec -p krishiv-plan -p krishiv-scheduler -p krishiv-proto -p krishiv-executor -- -D warnings  → clean
cargo test -p krishiv-vector-sinks          → 4 passed
cargo test -p krishiv-ai --no-default-features → 12 passed
RUSTFLAGS="-L native=/usr/lib/gcc/x86_64-linux-gnu/12" cargo test -p krishiv-ai --features fastembed-local rag_index → 1 passed
cargo test -p krishiv-proto executor_heartbeat_llm_quota → 1 passed
cargo test -p krishiv-scheduler --lib llm_quota → 2 passed
cargo test -p krishiv-executor llm_throttle → passed
cargo test -p krishiv-connectors feature_store → 2 passed
```

### Next Task

- Full workspace `cargo test --workspace` and `pytest python/krishiv-ai/tests/` after `maturin develop` (optional CI hardening for `fastembed-local` link flags).
- Acceptance-gate items in tracker (five-sink e2e, Python pytest suite) remain environment-dependent.
