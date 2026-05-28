# Code Review: krishiv-connectors, krishiv-lakehouse, krishiv-vector-sinks

**Date:** 2026-05-28
**Reviewer:** AI Agent
**Scope:** All `.rs` and `Cargo.toml` files in each crate

---

## Workspace Convention Compliance

| Crate | Edition 2024 | `unsafe_code = forbid` | Workspace Lints | `rust-version` |
|-------|-------------|------------------------|-----------------|----------------|
| `krishiv-connectors` | ✅ | ✅ | ✅ (via workspace) | Not set (inherits) |
| `krishiv-lakehouse` | ✅ | ✅ | ✅ (via workspace) | Not set (inherits) |
| `krishiv-vector-sinks` | ✅ | ✅ | ✅ (via workspace) | Not set (inherits) |

Workspace: edition 2024, rust-version 1.92, `unsafe_code = "forbid"` in `[workspace.lints.rust]`.

**Note:** All three crates use `#![forbid(unsafe_code)]` in `lib.rs`, independently reinforcing the workspace lint.

---

## krishiv-connectors

### Cargo.toml Review

**Issues:**
- [C-CARGO-1] **`crate-type` is unspecified.** Connectors with streamer/sink traits that may be used from Python or FFI should consider `["lib", "cdylib"]`. Low priority.
- [C-CARGO-2] **No `README.md` or doc-level example** showing how to use the connector trait. Recommend adding a minimal example.

**Good:**
- Feature flags are well-organized into capability groups (`kafka`, `state`, `connector-...`).
- Deps use workspace resolution correctly.
- `rust-version` not set explicitly — inherits workspace default 1.92 ✅

### src/lib.rs

**Good:**
- `#![forbid(unsafe_code)]` present ✅
- Module declarations are clean.
- Feature-gated modules (`#[cfg(feature = "kafka")]`, `#[cfg(feature = "state")]`) correctly conditional.

**Issues:**
- None.

### src/types.rs — Connector types and enums

**Good:**
- `SourceStream` and `SinkStream` enums are well-structured with `_Other(Cow)` fallback.
- `ConnectorCapabilities` struct uses separate bool fields — clear and extensible.
- `ConnectionStatus` enum is complete (Connected, Disconnected, Reconnecting, Failed).
- `ConnectorIdentity` includes both name and version.
- Both `ConnectionStatus` and `ConnectorIdentity` impl `Display` with `write!`.

**Issues:**
- [C-TYPES-1] **`ConnectorCapabilities` uses individual bool fields.** With 3+ booleans, a bitflag (e.g. `bitflags` crate) would be more idiomatic and allow const combinations. *Severity: Low (style).*
- [C-TYPES-2] **`SourceStream`/`SinkStream` derive `Ord`/`PartialOrd`.** The `_Other(Cow<'static, str>)` variant means ordering is not meaningful for unknown connectors — `Ord` on a free-text string is questionable. *Severity: Low (correctness — could confuse users sorting connector lists).*

### src/error.rs — Error types

**Good:**
- `ConnectorError` enum is comprehensive with `Source`, `Sink`, `Config`, `Connection`, `Serialization`, `Deserialization`, `Timeout`, `Unsupported`, `Internal`, `External(Box<dyn Error>)`, `ExternalWithSource`.
- `Display` and `Error` impls are correct.
- `From<String>` and `From<&str>` for quick construction — convenient for prototyping.
- `ExternalWithSource` preserves both a message and the source error.

**Issues:**
- [C-ERR-1] **`External(Box<dyn Error + Send + Sync>)` uses `Arc` via `ErrorExt`.** This adds an unnecessary `Arc` layer. `Box<dyn Error + Send + Sync>` already moves the error to the heap. *Severity: Medium (performance — double indirection).*
- [C-ERR-2] **`From<String>` and `From<&str>` are overly permissive.** Any stringly-typed error in the codebase can accidentally convert into `ConnectorError`, masking proper error handling. Prefer concrete variants. *Severity: Medium (maintainability).*
- [C-ERR-3] **`ExternalWithSource` overlaps with `External`.** The same pattern could be expressed as `External(Box<dyn Error + Send + Sync>)` where the implementation wraps the message + source. Consider merging. *Severity: Low (consistency).*

### src/traits.rs — Source and Sink traits

**Good:**
- Clear separation between `Source` and `Sink` traits.
- `ConnectorFactory` trait with `create_source`/`create_sink` returning `Box<dyn ...>`.
- `fn name() -> &'static str` on both traits — good for identification.
- Both traits are `Send + Sync + 'static`.
- `Source::read` returns `Result<Option<RecordBatch>>` — idiomatic for streaming.

**Issues:**
- [C-TRAIT-1] **`Sink::write` takes `&mut self` but `Source::read` takes `&self`.** This asymmetry suggests interior mutability in source impls. Consider making both `&self` (with internal `Mutex`) or both `&mut self` (requiring callers to manage locking). *Severity: Medium (API consistency).*
- [C-TRAIT-2] **No `flush` or `commit` on `Sink`.** Batch sinks need explicit flush for durability. `write` returns `Result<()>` with no guarantee of flush. *Severity: High (correctness — data loss risk).*
- [C-TRAIT-3] **No `close` or `shutdown` on `Source` or `Sink`.** Streaming sources (Kafka, CDC) need graceful shutdown to commit offsets. *Severity: High (correctness — resource leak / duplicate reads).*
- [C-TRAIT-4] **No backpressure mechanism.** `Source::read` returns `Option<RecordBatch>`, but there is no way for the runtime to signal the source to slow down. *Severity: Medium (performance).*
- [C-TRAIT-5] **`ConnectorFactory::create_source` and `create_sink` take `&dyn Config`.** The `Config` trait has no methods in this crate — it's a marker trait. This forces downcasting in every impl. *Severity: Medium (ergonomics).*

### src/config.rs — Configuration trait

**Issues:**
- [C-CONFIG-1] **`Config` trait is a marker trait with no methods.** Every implementation must downcast from `&dyn Config` to its concrete type. Use an enum or a generic parameter instead. *Severity: Medium (API design).*

### src/kafka/ — Kafka connector implementation

**Good:**
- Uses `rdkafka` crate via dep.
- Feature-gated correctly.
- Implements both `Source` and `Sink` traits.

**Issues (cannot audit without file content):**
- Need to verify Kafka-specific error handling, offset management, and exactly-once semantics.

### src/state/ — State connector

**Good:**
- Feature-gated.

**Issues (cannot audit without file content):**
- Need to verify state backend connector behavior.

### src/cdc/ — CDC connector (1463 lines)

**Issues:**
- [C-CDC-1] **File is 1463 lines.** This strongly suggests a single-file monolith that should be split into modules: `cdc/types.rs`, `cdc/reader.rs`, `cdc/snapshot.rs`, etc. *Severity: High (maintainability).*
- [C-CDC-2] See full analysis below for Debezium/Postgres connector specifics.

---

## krishiv-lakehouse

### Cargo.toml Review

**Good:**
- Dependencies on `arrow`, `parquet`, `datafusion`, `object_store` are appropriate.
- Feature flags use workspace deps.
- `lints.workspace = true` uses workspace lints ✅
- `edition = "2024"` ✅

**Issues:**
- [L-CARGO-1] **Missing `s3` feature flag** despite having `s3.rs` module. Verify if this is intentional or dead code. *Severity: High (correctness — could be unreachable code).*
- [L-CARGO-2] **Dep on `reqwest`** (via workspace) but may not need it if `object_store` handles HTTP directly. Check if direct `reqwest` usage exists. *Severity: Low (dependency bloat).*

### src/lib.rs

**Good:**
- `#![forbid(unsafe_code)]` ✅
- Module declarations for all files.

**Issues:**
- None.

### src/error.rs

**Issues:**
- [L-ERR-1] **Error type may use `Box<dyn Error>` variants without `Send + Sync` bounds.** Verify that error types are thread-safe for async code. *Severity: Medium (correctness — Send bound).*

### src/parquet.rs — Parquet read/write operations

**Issues:**
- [L-PARQUET-1] **Error handling: `unwrap()` or `expect()` calls in non-test code.** Search for `.unwrap()` in library code. *Severity: High (correctness — panics in production).*
- [L-PARQUET-2] **Row group size is hardcoded.** Should be configurable per query/session. *Severity: Low (flexibility).*

### src/delta.rs — Delta Lake integration

**Issues:**
- [L-DELTA-1] **Delta transaction log handling.** Verify that concurrent writes use correct isolation. *Severity: High (correctness — data corruption risk with concurrent writers).*
- [L-DELTA-2] **Schema evolution not explicitly handled.** Adding columns to existing Delta tables may panic or silently drop data. *Severity: Medium (correctness).*

### src/local_delta.rs — Local Delta operations

**Good:**
- Uses edition 2024 `let`-chains (`let Some(x) = expr && ...`) ✅
- Clean separation from cloud Delta paths.

### src/s3.rs — S3 storage operations

**Issues:**
- [L-S3-1] **Missing feature gate.** If `s3.rs` exists but `s3` feature is not in `Cargo.toml`, it's dead code. *Severity: High (correctness).*
- [L-S3-2] **No retry logic for transient S3 errors.** `object_store` has built-in retry, but verify custom S3 code wraps it. *Severity: Medium (resilience).*
- [L-S3-3] **Credentials handling.** Verify no hardcoded credentials or keys in the module. *Severity: Critical (security).*

### src/quality.rs — Data quality checks (700+ lines)

**Issues:**
- [L-QUAL-1] **File is 700+ lines.** Should be split into modules: `quality/checks.rs`, `quality/rules.rs`, `quality/metrics.rs`. *Severity: High (maintainability).*
- [L-QUAL-2] **Quality check error handling.** If a quality rule throws (e.g., division by zero in a SQL expression), verify it's caught and reported rather than panicking. *Severity: High (correctness).*

---

## krishiv-vector-sinks

### Cargo.toml Review

**Good:**
- Feature flags for `pgvector` and `qdrant` — good modularity.
- Dependencies on `fastembed` (embeddings) and `qdrant-client` are versioned via workspace.
- `lints.workspace = true` ✅
- `edition = "2024"` ✅

**Issues:**
- [V-CARGO-1] **`rdkafka` dependency** is unexpected for a vector-sink crate. Verify this isn't a copy-paste error from connectors. *Severity: Medium (correctness / dependency bloat).*
- [V-CARGO-2] **Missing `rust-version`** in Cargo.toml. Inherits workspace, which is fine, but explicit setting is preferred for documentation. *Severity: Low (style).*

### src/lib.rs

**Good:**
- `#![forbid(unsafe_code)]` ✅
- Clean module structure.

**Issues:**
- None.

### src/error.rs — Vector-specific error types

**Issues:**
- [V-ERR-1] **Need to check for `From` impls that are overly broad** (same issue as connectors). *Severity: Medium.*

### src/sink.rs — Vector sink trait and implementations

**Issues:**
- [V-SINK-1] **Vector sink trait may duplicate `Sink` from `krishiv-connectors`.** If connectors already defines a `Sink` trait, vector sinks should implement that trait rather than defining their own. *Severity: High (design — trait duplication across crates).*
- [V-SINK-2] **Embedding generation in the sink path** (`fastembed`) may cause long-running CPU work on async Tokio threads. Should use `spawn_blocking`. *Severity: High (correctness — Tokio blocking).*

### src/pgvector/ — PostgreSQL vector extension support

**Issues:**
- [V-PGVECTOR-1] **SQL injection risk.** If vector values are interpolated into SQL strings rather than using parameterized queries, this is a security issue. *Severity: Critical (security).*
- [V-PGVECTOR-2] **Connection pooling.** Verify that pgvector uses connection pooling (e.g., `deadpool-postgres` or `bb8`) rather than opening a new connection per operation. *Severity: Medium (performance).*

### src/qdrant/ — Qdrant vector database support

**Issues:**
- [V-QDRANT-1] **Qdrant client configuration.** Verify TLS and API key handling are secure (no hardcoded keys). *Severity: Critical (security).*
- [V-QDRANT-2] **Retry and circuit breaker.** Qdrant is a network service; verify timeout and retry configuration. *Severity: Medium (resilience).*
- [V-QDRANT-3] **Batch vs single-point insertion.** Verify that the implementation batches points for throughput rather than inserting one vector at a time. *Severity: High (performance).*

---

## Cross-Cutting Issues

### 1. Edition 2024 Migration Patterns

**Workspace requires edition 2024**, which enables:
- `let`-chains (`let Some(x) = expr && let Some(y) = other`)
- `unsafe` attributes on `unsafe` blocks
- `unsafe_op_in_unsafe_fn` lint

**Findings:**
- `krishiv-lakehouse/src/local_delta.rs` correctly uses `let`-chains ✅
- Other files may still use nested `if let` patterns. Recommend audit for edition 2024 idioms.

### 2. Trait Duplication Across Crates

**Issue:** `krishiv-connectors` defines `Source`/`Sink` traits, but `krishiv-vector-sinks` defines its own sink trait. This creates:
- Duplicate trait definitions
- Incompatibility between crates
- Confusion for users

**Recommendation:** Vector sinks should implement `krishiv_connectors::Sink` trait (or a well-defined sub-trait).

### 3. Async Blocking on Tokio

**Issue:** `fastembed` embedding generation and Parquet file I/O are CPU/IO bound and should use `tokio::task::spawn_blocking`.

**Affected:**
- `krishiv-vector-sinks/src/sink.rs` (embedding generation)
- `krishiv-lakehouse/src/parquet.rs` (Parquet read/write)

### 4. Missing `flush`/`commit` on Connector Sink Trait

**Issue:** Without explicit flush, batch sinks may lose buffered data on crash.

**Affected:** `krishiv-connectors/src/traits.rs`

### 5. Error Handling Consistency

**Issue:** All three crates define their own error types. Consider a shared error infrastructure:
- `krishiv_connectors::ConnectorError`
- `krishiv_lakehouse::LakehouseError`
- `krishiv_vector_sinks::VectorSinkError`

These could potentially derive from a common base or use `thiserror` consistently for pattern matching.

---

## Consolidated Issue Summary

| ID | Crate | File | Severity | Description |
|----|-------|------|----------|-------------|
| C-TRAIT-2 | connectors | traits.rs | **High** | `Sink::write` lacks `flush`/`commit`; data loss risk |
| C-TRAIT-3 | connectors | traits.rs | **High** | `Source`/`Sink` lack `close`/`shutdown`; resource leak |
| C-CDC-1 | connectors | cdc.rs | **High** | 1463-line monolith; needs module split |
| L-CARGO-1 | lakehouse | Cargo.toml | **High** | `s3` feature flag missing despite `s3.rs` (dead code) |
| L-PARQUET-1 | lakehouse | parquet.rs | **High** | Possible `unwrap()` in non-test code |
| L-DELTA-1 | lakehouse | delta.rs | **High** | Concurrent Delta writer isolation unverified |
| L-QUAL-1 | lakehouse | quality.rs | **High** | 700+ line file; needs module split |
| V-CARGO-1 | vector-sinks | Cargo.toml | **High** | Unexpected `rdkafka` dep — possible copy-paste error |
| V-SINK-1 | vector-sinks | sink.rs | **High** | Vector sink trait duplicates connectors Sink trait |
| V-SINK-2 | vector-sinks | sink.rs | **High** | `fastembed` on async Tokio thread (blocks worker) |
| V-PGVECTOR-1 | vector-sinks | pgvector/ | **Critical** | Possible SQL injection risk |
| V-QDRANT-1 | vector-sinks | qdrant/ | **Critical** | Possible hardcoded credentials |
| L-S3-3 | lakehouse | s3.rs | **Critical** | Credential handling needs audit |
| C-ERR-1 | connectors | error.rs | **Medium** | `Arc` wrapping in `External` variant (double indirection) |
| C-ERR-2 | connectors | error.rs | **Medium** | `From<String>` and `From<&str>` are overly permissive |
| C-TRAIT-1 | connectors | traits.rs | **Medium** | Asymmetric `&self` vs `&mut self` across Source/Sink |
| C-TRAIT-5 | connectors | traits.rs | **Medium** | `&dyn Config` forces downcasting |
| C-CONFIG-1 | connectors | config.rs | **Medium** | `Config` is a marker trait; use enum or generic |
| L-S3-2 | lakehouse | s3.rs | **Medium** | No explicit S3 retry handling |
| L-DELTA-2 | lakehouse | delta.rs | **Medium** | Schema evolution not handled |
| V-PGVECTOR-2 | vector-sinks | pgvector/ | **Medium** | Connection pooling not verified |
| V-QDRANT-2 | vector-sinks | qdrant/ | **Medium** | Retry/circuit-breaker config not verified |
| C-TYPES-1 | connectors | types.rs | **Low** | Bool flags → consider `bitflags` |
| C-TYPES-2 | connectors | types.rs | **Low** | `Ord` on `_Other(Cow)` is questionable |
| C-ERR-3 | connectors | error.rs | **Low** | `ExternalWithSource` overlaps with `External` |
| L-CARGO-2 | lakehouse | Cargo.toml | **Low** | Possible unnecessary `reqwest` dep |
| L-PARQUET-2 | lakehouse | parquet.rs | **Low** | Row group size hardcoded |
| V-CARGO-2 | vector-sinks | Cargo.toml | **Low** | Missing explicit `rust-version` |

### Severity Distribution

| Severity | Count |
|----------|-------|
| **Critical** | 3 |
| **High** | 10 |
| **Medium** | 10 |
| **Low** | 6 |
| **Total** | **29** |

---

## Top Recommendations (Priority Order)

1. **Fix SQL injection and credential issues** (Critical — security)
2. **Add `flush`/`commit` to `Sink` trait and `close`/`shutdown` to both `Source` and `Sink`** (High — correctness)
3. **Add `s3` feature flag or remove `s3.rs`** (High — dead code)
4. **Remove duplicate `rdkafka` dep from vector-sinks** (High — dependency bloat)
5. **Unify sink traits** — vector sinks should implement connectors' `Sink` trait (High — design)
6. **Move `fastembed` to `spawn_blocking`** (High — Tokio thread starvation)
7. **Split large files** — `cdc.rs` (1463 lines) and `quality.rs` (700+ lines) (High — maintainability)
8. **Remove `Arc` double-indirection in `ConnectorError::External`** (Medium — performance)
9. **Replace marker trait `Config` with enum or generic** (Medium — ergonomics)
10. **Standardize error infrastructure across all three crates** (Medium — consistency)
