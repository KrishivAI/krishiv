# Krishiv Implementation Status

## Current Phase

**Gap mitigation (all sprints) — PR #36 `cursor/gap-mitigation-7aa2` (2026-05-23)**

Plan: [`docs/engineering/gap-mitigation-plan.md`](../engineering/gap-mitigation-plan.md)

## Completed (optional follow-ups landed)

| ID | Item |
|----|------|
| P2-7 | Nexmark Q1/Q2/Q5/Q8 benches via `SqlEngine` + in-memory Arrow tables |
| P1-10 | `IcebergFsTable` — Parquet layers + `metadata.json`, restart durable |
| P2-11 | `spark_compat` / `spark_compat_date` downcasts → `DataFusionError` |
| P2-12 | Typed `AggKey` + `AggFunction::Avg` (Float64 output) |
| P2-13 | `upgrade_compat` typed `CheckpointMetadata` deserialize + validate |
| P3-7 | Processing-time timer O(1) cancel via identity index |
| P3-8 | `SharedStateMigrationRegistry` poison → `StateError::LockPoisoned` |
| P3-9 | OpenAI `call_one` native async (no `spawn_blocking`) |
| P3-10 | LLM rate-limiter map poison recovery + error log |
| P3-11 | `TokenAwareChunker` binary search; `tiktoken` feature for `tiktoken-rs` |
| P3-12 | Memo keys `{content_hash}:{chunk_index}`; per-chunk RAG skip |
| P3-13 | `MemoEntry.created_at_ms` + TTL eviction on `get` |
| P3-26 | `FederationClient` `async_trait` methods |

## Still deferred (infra / large)

- Full Iceberg catalog + `object_store` remote backend (beyond FS Parquet table)
- ONNX / `krishiv-ai` integration tests in CI without libstdc++
- Full workspace `cargo test --workspace`

## Validation

```bash
cargo check --workspace
cargo test -p krishiv-exec -p krishiv-state -p krishiv-federation -p krishiv-lakehouse -p krishiv-upgrade-tests -p krishiv-ai --lib
cargo test -p krishiv-upgrade-tests
cargo test -p krishiv-sql spark_compat
```

Optional bench: `cargo bench -p krishiv-bench --bench nexmark`

## Next command

Merge PR #36 after CI green.
