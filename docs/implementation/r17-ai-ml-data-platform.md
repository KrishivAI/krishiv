# R17 AI/ML Native Data Platform Implementation Tracker

## Goal

Make Krishiv the native compute engine for AI/ML data pipelines by delivering embedding generation, RAG index building, vector store sinks, LLM UDFs with rate limiting, incremental re-indexing, semantic deduplication, text chunking operators, and a hybrid batch+stream feature store. The design treats embeddings as first-class data types and integrates AI workloads into the same job scheduling, governance, and checkpoint fabric used for SQL and streaming jobs.

## Scope

In scope:

- Vector store sinks: Qdrant, Pinecone, pgvector, Weaviate, LanceDB.
- Embedding UDFs: OpenAI API (remote), HuggingFace `sentence-transformers` (local, CPU/GPU/MPS).
- Text chunking operators: `recursive_text`, `sentence`, `token_aware`, `markdown_section`.
- RAG pipeline high-level API: `ks.rag_index(source, embedder, vector_store, chunker, refresh)`.
- LLM UDFs: `@ks.llm_udf(model, prompt, output_type, cache, rate_limit)`.
- Semantic deduplication: cosine similarity of embeddings with configurable threshold.
- Hybrid batch+stream feature store: backfill from Parquet, live updates from Kafka.
- `krishiv-ai` new Rust crate for embedding/chunking operators.
- `krishiv-vector-sinks` new Rust crate for Qdrant/pgvector/LanceDB clients.
- `krishiv.ai` new Python module extending the `krishiv` Python package.
- Incremental re-indexing via integration with R14's memoization engine.
- Idempotent upsert semantics for all vector store sinks (keyed on `doc_id` + `checkpoint_epoch`).

Out of scope:

- Model training or fine-tuning within Krishiv.
- GPU cluster management or device scheduling.
- Vector index management (shard rebalancing, index rebuilding) — Krishiv writes to vector stores but does not manage their internal indexes.
- Real-time serving or inference endpoints.
- Full MLflow/Weights & Biases integration (deferred post-R17).
- Multi-modal embeddings (image, audio) — text only in R17.

## Dependencies

- R12: stable connector architecture providing the sink interface that vector store sinks extend.
- R13: coordinator extensions for LLM UDF rate-limiter coordination across executors (shared rate limit buckets).
- R14: memoization engine and content-hash infrastructure required for incremental re-indexing via `ks.transform(memo=True)`.
- R8: Python bindings (PyO3, `spawn_blocking` UDF model) — the LLM UDF decorator builds on the same isolation boundary.
- R10: resource governance — LLM API calls are high-cost; quota enforcement from R7/R10 protects multi-tenant deployments.
- R15: no direct dependency; may run in parallel.
- R16: no direct dependency; may run in parallel.

## Architectural Decisions Required

### ADR-R17.1: LLM UDF Execution Isolation Model

**Problem**: LLM API calls are network I/O with high latency (100ms–30s) and strict rate limits. The execution model must prevent API calls from blocking Tokio worker threads and must prevent a crashed Python UDF from killing a streaming executor process.

**Options**:
- A: In-process async via Tokio `reqwest` — fast, shared rate limiter via `Arc<RateLimiter>`, but a panic in a Python callback kills the executor. Suitable for local models where panics are rare.
- B: Subprocess with IPC over Arrow IPC on a Unix socket — safe process isolation, 10–50ms overhead per batch. Suitable for untrusted user-provided UDF code.
- C: `spawn_blocking` per batch with a shared `Arc<RateLimiter>` — simple, inherits R8's UDF model, works if the UDF function body is synchronous Python with no internal async. Panics are caught at the `JoinHandle` boundary.

**Recommendation**: Option C for API-backed UDFs (OpenAI, Anthropic, Cohere) and Option A for local HuggingFace models. The `@ks.llm_udf` decorator inspects the `model` parameter at decoration time and selects the appropriate execution backend. API-backed UDFs: `spawn_blocking` with `Arc<RateLimiter>` shared across all tasks in the executor. Local UDFs: in-process Tokio async with the model loaded as a process-level singleton (per ADR-R17.2). This distinction must be encoded in the decorator implementation in Sprint 4.

**Risk if deferred**: If the isolation model is not decided before Sprint 4, LLM UDF tests will be written against an assumption that may change, requiring a rewrite of the decorator internals and all UDF-facing tests.

---

### ADR-R17.2: Embedding Model Lifecycle and Process-Level Singleton

**Problem**: HuggingFace models loaded via `tokenizers` / `transformers` range from 100MB to 7GB. Loading per batch is catastrophically slow. On GPU, the model must be pinned to the correct device for the executor process. PyO3 static state across calls requires careful GIL management.

**Options**:
- A: Load model per `spawn_blocking` call — simple code, unacceptable latency (seconds per batch).
- B: Load model once per Python module import, stored in a module-level variable — simple but requires Python GIL to be held during model forward pass, blocking other UDFs in the same process.
- C: Store model in a Rust `OnceLock<Arc<EmbeddingModel>>` or Python `functools.lru_cache(maxsize=None)` per model name — load once, reuse across batches. GIL is acquired only during the forward pass inside `spawn_blocking`, which is correct.

**Recommendation**: Option C. The `krishiv-ai` crate exposes an `EmbeddingModelRegistry` backed by `OnceLock<HashMap<ModelKey, Arc<dyn EmbeddingModel>>>`. On first call with a given `model_name`, the model is loaded and inserted; subsequent calls reuse the singleton. For GPU models, `ModelKey` includes the device ID so each executor process pins to its assigned device. The Python `@ks.embed(model="...")` decorator calls into the registry via PyO3. The `lru_cache` approach is used in the Python layer as a fallback for pure-Python model wrappers.

**Risk if deferred**: If model loading is not centralized, the first benchmark run will show seconds-per-batch latency for HuggingFace models, which will require an architecture change after the API surface is already public. The registry must be in place before Sprint 2's embedding UDF implementation.

---

### ADR-R17.3: Vector Store Sink Consistency Model

**Problem**: Vector stores are typically not transactional. A crash mid-batch write to Qdrant or pgvector results in partial writes that cannot be rolled back. The consistency model determines the delivery guarantee users can rely on.

**Options**:
- A: Accept at-least-once delivery to all vector stores — simple implementation, duplicates possible on retry. Caller must handle duplicates at query time (e.g., by deduplicating on `doc_id`).
- B: Idempotent upsert keyed on `(doc_id, checkpoint_epoch)` — each record carries a deterministic ID derived from its content hash and the checkpoint epoch; vector stores are called with upsert (not insert), making retry safe. Requires all five supported vector stores to support conditional upsert.
- C: Two-phase write using vector store's versioning — complex, not uniformly supported across Qdrant/Pinecone/pgvector/Weaviate/LanceDB.

**Recommendation**: Option B. All vector store sinks must implement `VectorSink::upsert_batch(batch: &EmbeddingBatch, epoch: u64)` where each point ID is derived as `hash(doc_id || epoch)`. This makes retry safe: if an executor crashes and restarts at the same epoch, the upsert is a no-op for already-written points. pgvector uses `INSERT ... ON CONFLICT (id) DO UPDATE`. Qdrant uses `upsert` with `id` derived from the hash. LanceDB uses `merge_insert`. This upsert contract is part of the `VectorSink` trait definition and is enforced by a trait-level certification test. The at-least-once fallback is documented for vector stores where idempotent upsert is not achievable.

**Risk if deferred**: Users who rely on exactly-once behavior for their RAG index will receive silent duplicates. The `VectorSink` trait must encode the upsert contract before Sprint 1 begins writing sink implementations.

---

### ADR-R17.4: RAG Pipeline Incremental Re-Indexing

**Problem**: When source data changes (new or updated documents in Iceberg or S3), only changed documents should be re-embedded. Embedding is expensive (API cost or GPU time); re-embedding unchanged documents wastes resources and inflates cost.

**Options**:
- A: Re-embed all documents on every `rag_index()` refresh run — simple, expensive, not viable for large corpora.
- B: Track source document content hashes using R14's memoization engine; skip embedding for documents whose hash has not changed since the last run — incremental, correct, requires R14's `redb`-backed memo store to scale to millions of keys.
- C: Use vector store's native deduplication (e.g., Qdrant's payload filter) to skip upsert for unchanged documents — avoids re-embedding but still requires embedding to be computed to compare.

**Recommendation**: Option B. `ks.rag_index()` wraps `ks.transform(memo=True)` from R14 under the hood. The memoization key is the document content hash; the memo value is the embedding vector and the vector store point ID. On each refresh, documents with matching content hashes are skipped entirely (no API call, no GPU pass). Documents with changed or missing hashes are re-embedded and upserted. If R14's `redb` memo store has not been benchmarked at millions of keys, a performance test must be added in Sprint 5 before `rag_index()` is declared stable. This ADR depends on R14's memoization engine being available; if R14 is delayed, `rag_index()` falls back to Option A with a deprecation warning.

**Risk if deferred**: Without incremental re-indexing, `rag_index()` becomes unusable for corpora larger than ~100K documents due to API cost. The memoization integration must be validated in Sprint 5 before the acceptance gate.

---

## Sprint 1 — Vector Store Sink Connectors

### S1.1 VectorSink Trait and krishiv-vector-sinks Crate
- [x] Create `crates/krishiv-vector-sinks/` crate.
- [x] Define `EmbeddingBatch { doc_ids: Vec<String>, vectors: Vec<Vec<f32>>, payloads: Vec<HashMap<String, Value>>, epoch: u64 }`.
- [x] Define `VectorSink` trait: `async fn upsert_batch(&self, batch: &EmbeddingBatch) -> Result<(), VectorSinkError>`, `async fn delete_by_ids(&self, ids: &[String]) -> Result<(), VectorSinkError>`, `fn sink_name(&self) -> &str`.
- [x] Define `VectorSinkError` enum: `Connection`, `Upsert`, `SchemaConflict`, `RateLimit`, `Timeout`.
- [x] Add trait-level certification test: `upsert_batch` called twice with same `epoch` produces the same result as called once (idempotency).

**Validation**: `cargo check -p krishiv-vector-sinks`

### S1.2 Qdrant Sink
- [x] Add `qdrant-client` dependency to `krishiv-vector-sinks`.
- [x] Implement `QdrantSink { client: QdrantClient, collection_name: String, vector_size: u64 }`.
- [x] Implement `upsert_batch`: derive point ID as `hash(doc_id || epoch)` (SHA-256, truncated to u64), call `qdrant_client::upsert_points()`.
- [x] Implement collection auto-creation if not exists (configurable: `create_collection_if_missing: bool`).
- [x] Write unit tests: upsert, idempotent retry, collection creation.

**Validation**: `cargo test -p krishiv-vector-sinks -- qdrant`

### S1.3 pgvector Sink
- [x] Add `sqlx` with PostgreSQL feature to `krishiv-vector-sinks`.
- [x] Implement `PgvectorSink { pool: PgPool, table_name: String, vector_dim: usize }`.
- [x] Implement table auto-creation: `CREATE TABLE IF NOT EXISTS ... (id TEXT PRIMARY KEY, vector vector(N), payload JSONB, epoch BIGINT)`.
- [x] Implement `upsert_batch`: `INSERT INTO ... ON CONFLICT (id) DO UPDATE SET vector=EXCLUDED.vector, payload=EXCLUDED.payload, epoch=EXCLUDED.epoch`.
- [x] Write unit tests: upsert, idempotent conflict resolution.

**Validation**: `cargo test -p krishiv-vector-sinks -- pgvector`

### S1.4 LanceDB, Weaviate, and Pinecone Sinks
- [x] Implement `LanceDbSink` via Parquet fragments + merge-on-id (no upstream `lancedb` crate — chrono conflict with DataFusion 53): `merge_insert` on `id` field derived from `hash(doc_id || epoch)`.
- [x] Implement `WeaviateSink` using `reqwest`-based Weaviate REST client: `PUT /v1/objects/{id}` for idempotent upsert.
- [x] Implement `PineconeSink` using `reqwest`-based Pinecone REST client: `upsert` endpoint with `id = hash(doc_id || epoch)`.
- [x] Add `VectorSinkConfig` enum to `krishiv-connectors` for unified sink configuration in job specs.
- [x] Register all sinks with `krishiv-connectors` connector registry.

**Validation**: `cargo test -p krishiv-vector-sinks`; `cargo check --workspace` clean.

---

## Sprint 2 — Embedding UDFs (API + Local)

### S2.1 EmbeddingModelRegistry and krishiv-ai Crate
- [x] Create `crates/krishiv-ai/` crate.
- [x] Define `EmbeddingModel` trait: `async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError>`, `fn embedding_dim(&self) -> usize`, `fn model_name(&self) -> &str`.
- [x] Implement `EmbeddingModelRegistry`: `OnceLock<Mutex<HashMap<ModelKey, Arc<dyn EmbeddingModel + Send + Sync>>>>`.
- [x] Implement `EmbeddingModelRegistry::get_or_load(key: ModelKey) -> Arc<dyn EmbeddingModel>`.
- [x] Define `ModelKey { model_name: String, device: EmbeddingDevice }` where `EmbeddingDevice = Cpu | Gpu(u8) | Mps`.

**Validation**: `cargo check -p krishiv-ai`

### S2.2 OpenAI Embedding UDF
- [x] Implement `OpenAiEmbeddingModel { api_key: String, model: String, dimensions: usize, rate_limiter: Arc<RateLimiter> }`.
- [x] Implement `embed_batch`: batch texts into groups of 100 (OpenAI limit), call `/v1/embeddings` via `reqwest`, parse response, collect float vectors.
- [x] Implement `Arc<RateLimiter>` shared across all `spawn_blocking` tasks in the executor process using `OnceLock`.
- [x] Implement exponential backoff on HTTP 429 responses.
- [x] Add `embed_openai(model: &str, api_key: &str)` factory function in `krishiv-ai`.
- [x] Write unit tests with mock HTTP server: batch splitting, rate limit retry, response parsing.

**Validation**: `cargo test -p krishiv-ai -- openai_embedding`

### S2.3 HuggingFace Local Embedding UDF
- [x] Implement `HuggingFaceEmbeddingModel { model_path: PathBuf, device: EmbeddingDevice, tokenizer: Tokenizer, model: Arc<SentenceTransformerModel> }`.
- [x] Implement model loading via `OnceLock` (per ADR-R17.2): load `tokenizer.json` and model weights once on first call.
- [x] Implement `embed_batch` for CPU: tokenize texts, run forward pass in `spawn_blocking`, return mean-pooled embeddings.
- [x] Implement GPU device selection: load model to `cuda:{device_id}` or `mps` based on `EmbeddingDevice`.
- [x] Add `embed_huggingface(model_name: &str, device: EmbeddingDevice)` factory function.
- [x] Write unit tests: model loads once (OnceLock not re-entered), embedding output shape is correct, CPU forward pass runs.

**Validation**: `cargo test -p krishiv-ai -- huggingface_embedding`

### S2.4 Python embed() API
- [x] Add `@ks.embed(model="openai/text-embedding-3-small")` and `@ks.embed(model="sentence-transformers/all-MiniLM-L6-v2")` decorators in `krishiv.ai` Python module.
- [x] Implement decorator: wraps a column transformation function, registers with `EmbeddingModelRegistry` via PyO3.
- [x] Add `DataFrame.with_embeddings(embed_fn, text_col, output_col)` method to Python DataFrame API.
- [x] Write Python unit tests: decorator registration, DataFrame embedding transformation with mock model.

**Validation**: `pytest python/krishiv-ai/tests/test_embed.py`

---

## Sprint 3 — Text Chunking Operators

### S3.1 Chunking Operator Framework
- [x] Define `TextChunker` trait in `krishiv-ai`: `fn chunk(&self, text: &str) -> Vec<Chunk>` where `Chunk { text: String, start_byte: usize, end_byte: usize, chunk_index: usize }`.
- [x] Define `ChunkOperator` physical operator in `krishiv-exec`: takes a `RecordBatch` with a text column, applies `TextChunker`, outputs an exploded `RecordBatch` with one row per chunk plus original metadata columns.
- [x] Implement `ChunkOperator` integration with `OperatorQueue` barrier protocol.
- [x] Add `DataFrame.chunk(text_col, chunker, output_col)` to Python DataFrame API.

**Validation**: `cargo check -p krishiv-ai -p krishiv-exec`

### S3.2 Chunking Strategy Implementations
- [x] Implement `RecursiveTextChunker { chunk_size: usize, chunk_overlap: usize, separators: Vec<String> }`: splits on paragraph → sentence → word → character boundaries recursively until under `chunk_size`.
- [x] Implement `SentenceChunker { max_sentences_per_chunk: usize, sentence_overlap: usize }`: splits on sentence boundaries (`.`, `!`, `?` followed by whitespace); groups into chunks of up to N sentences.
- [x] Implement `TokenAwareChunker { max_tokens: usize, token_overlap: usize, tokenizer: Arc<Tokenizer> }`: splits on tokenizer boundaries to stay under `max_tokens` per chunk; supports `cl100k_base` (OpenAI) and `sentencepiece` tokenizers.
- [x] Implement `MarkdownSectionChunker { min_heading_level: u8, max_chunk_size: Option<usize> }`: splits on Markdown heading boundaries (`#`, `##`, `###`), optionally further splitting large sections by `RecursiveTextChunker`.
- [x] Write unit tests for each chunker: correct chunk boundaries, overlap, edge cases (empty string, single word, no sentence boundaries).

**Validation**: `cargo test -p krishiv-ai -- chunking`

### S3.3 Python Chunking API
- [x] Expose all four chunkers as Python classes in `krishiv.ai`: `RecursiveTextChunker(chunk_size=512, overlap=64)`, `SentenceChunker(max_sentences=5)`, `TokenAwareChunker(max_tokens=512, tokenizer="cl100k_base")`, `MarkdownSectionChunker(min_level=2)`.
- [x] Add `ks.chunk(text, chunker)` helper function in `krishiv.ai`.
- [x] Write Python unit tests for all four chunkers.

**Validation**: `pytest python/krishiv-ai/tests/test_chunking.py`

---

## Sprint 4 — LLM UDFs & Rate Limiting

### S4.1 LLM UDF Framework
- [x] Define `LlmUdf` trait in `krishiv-ai`: `async fn call_batch(&self, prompts: &[String]) -> Result<Vec<LlmResponse>, LlmError>` where `LlmResponse { text: String, finish_reason: String, tokens_used: u32 }`.
- [x] Define `LlmUdfConfig { model: String, max_tokens: u32, temperature: f32, cache: bool, rate_limit: RateLimitConfig }`.
- [x] Define `RateLimitConfig { requests_per_minute: u32, tokens_per_minute: u64 }`.
- [x] Implement `LlmRateLimiter`: dual token bucket — one for request count, one for token count. Stored as `Arc<LlmRateLimiter>` per (model, executor process) in `OnceLock`.

**Validation**: `cargo check -p krishiv-ai`

### S4.2 OpenAI LLM UDF
- [x] Implement `OpenAiLlmUdf { api_key: String, model: String, config: LlmUdfConfig, rate_limiter: Arc<LlmRateLimiter> }`.
- [x] Implement `call_batch`: construct `ChatCompletion` requests with the configured prompt template, call `/v1/chat/completions`, parse responses.
- [x] Implement response caching: if `config.cache = true`, store `hash(prompt) → LlmResponse` in an in-process `DashMap` (bounded by LRU eviction at 10,000 entries).
- [x] Implement `spawn_blocking` isolation per ADR-R17.1: each `call_batch` call is dispatched to the `spawn_blocking` thread pool.
- [x] Implement exponential backoff on HTTP 429 with jitter.
- [x] Write unit tests: rate limit enforcement, cache hit/miss, retry on 429.

**Validation**: `cargo test -p krishiv-ai -- openai_llm`

### S4.3 Python @ks.llm_udf Decorator
- [x] Implement `@ks.llm_udf(model, prompt, output_type, cache=False, rate_limit=None)` decorator in `krishiv.ai`.
- [x] Implement prompt template rendering: `prompt` may contain `{col_name}` placeholders that are substituted per row.
- [x] Implement `output_type` parsing: `str` (raw text), `int`, `float`, `bool`, `dict` (JSON parse), Pydantic model class (parse JSON into model).
- [x] Select execution backend at decoration time per ADR-R17.1: API-backed models use `spawn_blocking`; local models use in-process async.
- [x] Add `DataFrame.apply_llm(llm_udf_fn, input_cols, output_col)` method to Python DataFrame API.
- [x] Write Python unit tests: decorator instantiation, prompt rendering, output_type coercion, `apply_llm` integration.

**Validation**: `pytest python/krishiv-ai/tests/test_llm_udf.py`

### S4.4 Coordinator Rate Limiter Coordination
- [x] Add `LlmQuotaReport { model: String, requests_used: u64, tokens_used: u64, period_ms: u64 }` to `ExecutorHeartbeat` proto.
- [x] Implement coordinator-side LLM quota aggregation: sum `requests_used` and `tokens_used` across all executors per model per period.
- [x] Implement `LlmThrottleCommand { model: String, max_requests_per_minute: u32, max_tokens_per_minute: u64 }` in `ExecutorHeartbeatResponse`.
- [x] Implement executor: on receiving `LlmThrottleCommand`, update the process-level `LlmRateLimiter` singleton.
- [x] Write unit tests: coordinator aggregates quota reports, issues throttle command when aggregate exceeds job-level LLM quota.

**Validation**: `cargo test -p krishiv-scheduler -- llm_quota`; `cargo test -p krishiv-executor -- llm_throttle`

---

## Sprint 5 — RAG Pipeline API & Semantic Dedup

### S5.1 RAG Index Pipeline API
- [x] Define `RagIndexSpec { source: DataSource, chunker: ChunkerConfig, embedder: EmbedderConfig, vector_store: VectorSinkConfig, refresh: RefreshPolicy }` in `krishiv-plan`.
- [x] Define `RefreshPolicy`: `Manual`, `Schedule(CronExpr)`, `Continuous` (streaming).
- [x] Implement `ks.rag_index(source, embedder, vector_store, chunker, refresh)` in `krishiv.ai`: compiles to a `RagIndexSpec` and submits as a Krishiv job.
- [x] Implement RAG job plan: `Source → Chunk → Embed → (MemoCheck) → VectorSink`.
- [x] Integrate R14 memoization (per ADR-R17.4): wrap `Embed → VectorSink` in `ks.transform(memo=True)` keyed on document content hash; skip embedding for unchanged documents.
- [x] Implement memoization store performance test: insert 1,000,000 keys into `redb`, verify lookup latency < 1ms p99.
- [x] Write integration test: `rag_index()` with 1,000 synthetic documents, verify only changed documents are re-embedded on second run.

**Validation**: `cargo test -p krishiv-api -- rag_index_integration`; `pytest python/krishiv-ai/tests/test_rag.py`

### S5.2 Semantic Deduplication
- [x] Implement `SemanticDedup` operator in `krishiv-ai`: given a `RecordBatch` with an embedding column (fixed-size list of f32), compute pairwise cosine similarity and mark duplicates above threshold.
- [x] Implement exact cosine similarity for small batches (N < 1000): O(N²) with SIMD dot product.
- [x] Implement approximate cosine similarity for large batches (N ≥ 1000): use LSH (locality-sensitive hashing) with `num_hash_tables=10`, `num_hash_functions=5` to reduce candidate pairs.
- [x] Define `SemanticDedupConfig { threshold: f32, strategy: DedupStrategy }` where `DedupStrategy = KeepFirst | KeepLast | KeepHighestScore(score_col)`.
- [x] Add `DataFrame.dedup_semantic(embedding_col, config)` to Python DataFrame API.
- [x] Write unit tests: identical embeddings deduplicated, threshold boundary, LSH candidate generation.

**Validation**: `cargo test -p krishiv-ai -- semantic_dedup`

### S5.3 RAG Query API
- [x] Add `ks.rag_query(query_text, embedder, vector_store, top_k, filters)` in `krishiv.ai`: embeds `query_text`, calls vector store's nearest-neighbor search, returns top-K chunks with scores.
- [x] Implement vector store query methods in each `VectorSink` implementation: `async fn query_nearest(&self, vector: &[f32], top_k: usize, filter: Option<PayloadFilter>) -> Result<Vec<ScoredChunk>>`.
- [x] Define `ScoredChunk { doc_id: String, chunk_index: usize, text: String, score: f32, payload: HashMap<String, Value> }`.
- [x] Write integration test: index 100 documents, query returns correct top-K with scores.

**Validation**: `pytest python/krishiv-ai/tests/test_rag_query.py`; `cargo test -p krishiv-vector-sinks -- query_nearest`

---

## Sprint 6 — Hybrid Feature Store

### S6.1 Feature Store Architecture
- [x] Define `FeatureStore { name: String, batch_source: DataSource, stream_source: Option<DataSource>, feature_schema: FeatureSchema }` in `krishiv-plan`.
- [x] Define `FeatureSchema { features: Vec<FeatureDef>, entity_key: Vec<String> }` where `FeatureDef { name: String, dtype: DataType, ttl_ms: Option<u64> }`.
- [x] Implement `FeatureStoreRegistry` in `krishiv-scheduler`: tracks registered feature stores and their materialization state.
- [x] Add `ks.feature_store(name, batch_source, stream_source, schema)` to `krishiv.ai` Python API.

**Validation**: `cargo check -p krishiv-scheduler`; `pytest python/krishiv-ai/tests/test_feature_store.py`

### S6.2 Batch Backfill from Parquet
- [x] Implement backfill job plan: `ParquetSource → FeatureTransform → FeatureStoreSink`.
- [x] Implement `FeatureStoreSink` in `krishiv-connectors`: writes features to a Parquet-backed feature table partitioned by entity key.
- [x] Implement `FeatureStoreReader`: point-in-time correct lookup — for a given entity key and timestamp, return the feature values valid at that timestamp (latest value with `created_at <= timestamp`).
- [x] Add `ks.backfill_features(feature_store, source_df)` method.
- [x] Write integration test: backfill 10,000 rows from Parquet, verify point-in-time correct lookup.

**Validation**: `cargo test -p krishiv-connectors -- feature_store_backfill`

### S6.3 Live Feature Updates from Kafka
- [x] Implement streaming feature materialization job: `KafkaSource → FeatureTransform → FeatureStoreSink` (append-only, live updates).
- [x] Implement TTL-based feature expiry: features with `ttl_ms` set are excluded from lookup results if `now - created_at > ttl_ms`.
- [x] Implement feature freshness metadata: `FeatureStoreSink` records `last_updated_ms` per entity key.
- [x] Add `ks.feature_store(...).with_streaming_source(kafka_config)` method.
- [x] Write integration test: stream 1,000 feature updates from mock Kafka source, verify live lookup reflects latest values.

**Validation**: `cargo test -p krishiv-connectors -- feature_store_streaming`

### S6.4 Acceptance Validation
- [x] Run full workspace test suite.
- [x] Run `cargo clippy --workspace -- -D warnings`.
- [x] Verify all five vector store sinks pass idempotency certification test.
- [x] Verify `rag_index()` incremental re-indexing skips unchanged documents (assert embedding API call count < total document count on second run).
- [x] Verify memoization store handles 1,000,000 keys with p99 lookup latency < 1ms.
- [x] Verify semantic dedup removes duplicates above threshold and retains below threshold.
- [x] Verify feature store point-in-time correct lookup.

**Validation**: `cargo test --workspace`; `cargo clippy --workspace -- -D warnings` clean; `pytest python/` passes.

---

## Acceptance Gate

- [x] All five vector store sinks (Qdrant, Pinecone, pgvector, Weaviate, LanceDB) pass the `VectorSink` idempotency certification test.
- [x] `@ks.embed(model="openai/text-embedding-3-small")` generates embeddings via OpenAI API with rate limiting enforced.
- [x] `@ks.embed(model="sentence-transformers/all-MiniLM-L6-v2")` generates embeddings locally; model is loaded exactly once per executor process.
- [x] All four text chunking strategies (`recursive_text`, `sentence`, `token_aware`, `markdown_section`) produce correct chunk boundaries verified by unit tests.
- [x] `@ks.llm_udf` with `model="gpt-4o"` executes via `spawn_blocking` with shared rate limiter; rate limit is enforced across all tasks in the executor.
- [x] Coordinator aggregates LLM quota reports and issues throttle commands when job-level quota is exceeded.
- [x] `ks.rag_index()` on second run calls the embedding API only for changed documents (incremental re-indexing verified by call count assertion).
- [x] `ks.rag_query()` returns top-K chunks with correct scores from all five vector stores.
- [x] `SemanticDedup` removes document pairs with cosine similarity above the configured threshold.
- [x] Feature store backfill and streaming update paths both support point-in-time correct lookup.
- [x] `cargo test --workspace` passes; `cargo clippy --workspace -- -D warnings` clean; `pytest python/` passes.
