# R12 Maturity Gap and Risk Register

Generated: 2026-05-22  
Source: post-R12 codebase review (major subsystems maturity perspective)  
Related: [`r12-r20-roadmap.md`](r12-r20-roadmap.md), [`../implementation/r12-foundation-completeness.md`](../implementation/r12-foundation-completeness.md), [`architectural-decisions-r12-r20.md`](architectural-decisions-r12-r20.md)

## Purpose

This register records **all identified gaps and risks** after R12 implementation work,
with **target release**, **proposed resolution**, and **validation**. It supersedes
informal “complete” claims where library code exists but binaries, enforcement, or
integration paths remain stubbed.

Use this file when:

- Planning R12 carryover patches vs R13+ work.
- Updating acceptance gates in release trackers.
- Auditing documentation drift (matrix “certified” vs CI reality).

### Maturity layers (scoring reference)

| Layer | Meaning |
|-------|---------|
| L1 | Types and traits defined |
| L2 | Unit tests pass in isolation |
| L3 | Wired through library call chain |
| L4 | Process/binary/CLI uses the path |
| L5 | Certified (failover, security, CI contract) |

---

## Summary by target release

| Release | Gap count | Theme |
|---------|-----------|-------|
| **R12 carryover** | 12 | Fencing enforcement, Kafka compile, remote CLI RPCs, shuffle hot path, policy bypass, doc/CI honesty |
| **R13** | 14 | Python API, Flight/distributed backend, SingleNode/embedded streaming, executor binary, Session bridges |
| **R14** | 5 | CDC lakehouse, certification suite, mat views, connector replay tests |
| **R16** | 8 | HA coordinator, stateful streaming, barriers, object-store checkpoints |
| **R19** | 2 | Federation remote client, multi-region metadata |
| **R20** | 1 | Enterprise IAM / durable governance |

---

## Register (full)

### Control plane — scheduler & executor

| ID | Gap / risk | Severity | Layer today | Resolution release | Proposed resolution | Key paths | Validation |
|----|------------|----------|-------------|-------------------|---------------------|-----------|------------|
| GAP-CP-01 | `LeaderElection` trait defined but never wired; coordinator binary always starts `Coordinator::active()` | Critical | L1 | R16 (R9 HA) | Integrate `K8sLeaseElection` / etcd lease in `krishiv_coordinator`; standby coordinator rejects mutations until promoted | `krishiv-scheduler/src/lib.rs`, `bin/krishiv_coordinator.rs` | Multi-replica failover test; only one active mutator |
| GAP-CP-02 | Checkpoint fencing token never advances on coordinator failover; always `initial()` | High | L2 | R16 | Bump token on lease acquisition; persist in metadata; reject acks with `!=` current token | `CheckpointCoordinator`, scheduler ack path | Split-brain injection test |
| GAP-CP-03 | `validate_fencing_token()` not called before `write_epoch_metadata` in `commit_epoch` | Critical | L2 | **R12 carryover** | Call `krishiv_checkpoint::validate_fencing_token` before every metadata write and on restore | `krishiv-scheduler` `commit_epoch`, `krishiv-checkpoint` | Unit test: stale token write fails |
| GAP-CP-04 | Coordinator binary does not attach `MetadataStore` or call `recover_from_store` on startup | High | L3 | **R12 carryover** / R13 | CLI `--metadata-backend` + `--metadata-path`; startup recovery; rebuild `checkpoint_coordinators` from job specs | `bin/krishiv_coordinator.rs`, `Coordinator::recover_from_store` | Restart test: 3 jobs survive |
| GAP-CP-05 | Metadata persist failures are warn-only; submit succeeds if disk write fails | High | L3 | **R12 carryover** | Fail-closed on `save_job` / task-update persist for production mode; metric `metadata_persist_errors_total` | `Coordinator::submit_job`, task updates | Forced I/O error → submit returns error |
| GAP-CP-06 | `recover_from_store` iterates empty in-memory `checkpoint_coordinators` | High | L3 | **R12 carryover** | Reconstruct per-job `CheckpointCoordinator` from stored job spec + checkpoint storage | `recover_from_store` | Post-restart checkpoint list matches pre-restart |
| GAP-CP-07 | Executor registry not persisted; executors must re-register after coordinator restart | Medium | L3 | R13 | Persist executor heartbeats in metadata store or treat re-register as idempotent with lease bump | `MetadataStore` schema | Restart + heartbeat recovery test |
| GAP-CP-08 | `extract_auth_context` implemented but not used in gRPC handlers | Medium | L2 | R13 / R9 | Call at handler entry; map to scheduler auth context or reject | `CoordinatorExecutorGrpcService` | RPC without token → `Unauthenticated` |
| GAP-CP-09 | Executor binary: register/heartbeat only; no task gRPC server or `ExecutorTaskRunner` loop | High | L2 | R13 | Serve task assignment stream; run SQL/streaming fragments in process | `krishiv-executor/src/main.rs` | E2E: submit job → executor completes task |
| GAP-CP-10 | Scheduler `lib.rs` monolith (~9k lines) | Medium | L3 | R12 (ADR-12.6) | Decompose into `coordinator/`, `metadata/`, `grpc/`, `checkpoint/` modules | ADR-12.6 | `cargo test -p krishiv-scheduler` unchanged |
| GAP-CP-11 | Ack fencing uses `ack.fencing_token < current` (accepts future tokens) | Medium | L2 | R16 | Align with checkpoint `!=` semantics; document coordinator generation rules | `receive_ack` | Test rejects future and stale tokens |
| GAP-CP-12 | `JobSubmitter` / `GrpcJobSubmitter` traits only; no K8s job submission | Low | L1 | R19 | Implement submitter using operator CRD create | ADR in R19 | Kind e2e job created from CLI |

### Session API & runtime backends

| ID | Gap / risk | Severity | Layer today | Resolution release | Proposed resolution | Key paths | Validation |
|----|------------|----------|-------------|-------------------|---------------------|-----------|------------|
| GAP-RT-01 | `EmbeddedBackend` / `SingleNodeBackend` / `DistributedBackend` return `accepted: true` without remote dispatch | Critical | L1–L2 | R13 (dist), R12 S6.2–S6.3 (local) | Implement ADR-12.3 Flight SQL client in `DistributedBackend`; ADR-12.4 mpsc coordinator for `SingleNodeBackend` | `krishiv-runtime/src/lib.rs` | Mock Flight server returns batch; distributed collect matches |
| GAP-RT-02 | `ensure_local_mode` is no-op; distributed sessions still run local DataFusion | High | L2 | R13 | Restore guard or split APIs: `execute_local` vs `execute_remote` | `krishiv-api/src/lib.rs` | Distributed mode cannot `register_parquet` local path without explicit opt-in |
| GAP-RT-03 | `WindowedStream` / `KeyedStream` descriptors not lowered to executor `stream:tw:` fragments | High | L2 | R13 | Plan lowering: API types → `PhysicalPlan` → fragment string; `Session::execute_stream()` | `krishiv-api`, `krishiv-plan`, `krishiv-executor` | Windowed API e2e produces aggregates |
| GAP-RT-04 | `RemoteCoordinatorClient` lazy channel but RPC bodies return `Ok(())` without calls | High | L2 | **R12 carryover** | Wire `trigger_savepoint`, `list_checkpoints`, `inspect_state`, `restore` to proto RPCs | `krishiv/src/remote_client.rs` | Integration test against `krishiv-coordinator` |
| GAP-RT-05 | `Session::sql()` / `sql_async()` bypass `PolicyEnforcingSqlEngine` | High | L3 | **R12 carryover** / R13 | If policy configured: route all SQL through `sql_as` or return `AccessDenied` when principal missing | `krishiv-api` | Test: policy on + `sql()` → denied |
| GAP-RT-06 | `collect_with_stats()` uses fresh `SessionContext` (no registered tables) | Medium | L2 | **R12 carryover** | Reuse session `SqlEngine` context for stats | `krishiv-sql` | Stats on registered Parquet table works |
| GAP-RT-07 | `StateTtlConfig` (api) not connected to `TtlConfig` (state) | Medium | L1 | R13 | Conversion + `SessionBuilder::with_state_ttl` wiring to backend | `krishiv-api`, `krishiv-state` | TTL expiry e2e test |
| GAP-RT-08 | Streaming `collect_bounded` passes `coordinator_url: None` in distributed mode | Medium | L2 | R13 | Thread coordinator URL into `accept_plan_with_backend` | `krishiv-api` | Distributed stream uses remote backend |

### Streaming operators & state

| ID | Gap / risk | Severity | Layer today | Resolution release | Proposed resolution | Key paths | Validation |
|----|------------|----------|-------------|-------------------|---------------------|-----------|------------|
| GAP-ST-01 | `TumblingWindowOperator` uses in-memory `HashMap`, not `StateBackend` | Critical | L3 | R16 | Namespace per operator; put/get aggregates; snapshot in checkpoint ack | `krishiv-exec`, `krishiv-executor` | Restart from checkpoint restores windows |
| GAP-ST-02 | Event/processing timers not invoked from executor streaming path | High | L2 | R16 | Drain `TimerService` in operator loop; fire on watermark advance | `krishiv-state`, `krishiv-exec` | Timer firing unit + integration test |
| GAP-ST-03 | Executor receives `OperatorMessage::Barrier` but discards epoch (`let _ = epoch`) | High | L3 | R16 (ADR-16.3) | Propagate barrier to `CheckpointCoordinator`; complete ack path | `krishiv-executor` | Barrier triggers epoch commit in test |
| GAP-ST-04 | Only `stream:tw:` tumbling path routed; sliding/session/join not in executor | High | L3 | R16 | Fragment parsers for `stream:sw:`, `stream:session:`, `stream:join:` | `krishiv-executor` | Each operator type has executor test |
| GAP-ST-05 | Checkpoint snapshots may be empty for streaming jobs (state not in backend) | Critical | L3 | R16 | GAP-ST-01 + GAP-ST-03 | cross-crate | Chaos: kill mid-checkpoint → correct restore |
| GAP-ST-06 | `PhysicalPlan` is name-only; ADR-12.8 real operator wiring incomplete | High | L2 | R12 / R13 | Lower plan nodes to `krishiv-exec` operators in `ExecutionBackend::execute` | ADR-12.8, `krishiv-runtime` | Plan executes tumbling window without fragment string hack |

### Shuffle & AQE

| ID | Gap / risk | Severity | Layer today | Resolution release | Proposed resolution | Key paths | Validation |
|----|------------|----------|-------------|-------------------|---------------------|-----------|------------|
| GAP-SH-01 | `ShuffleCompression` on `LocalShuffleStore` not used on executor hot path (`ShuffleStore` writes uncompressed) | High | L2 | **R12 carryover** | Plumb codec into `LocalDiskShuffleStore` / IPC writes; negotiate in `JobSpec` | `krishiv-shuffle`, `krishiv-executor` | Executor shuffle round-trip compressed |
| GAP-SH-02 | No codec header on disk; reader/writer config mismatch corrupts data | Medium | L2 | **R12 carryover** | File header magic + codec enum bytes | `LocalShuffleStore` | Mixed-config read fails with clear error |
| GAP-SH-03 | `HashPartitioner` uses `DefaultHasher` (unstable across processes) | High | L2 | **R12 carryover** | Stable hash (e.g. `xxhash_rust` / `twox-hash`) on key bytes | `krishiv-shuffle` | Cross-process golden partition map test |
| GAP-SH-04 | `CoalesceRule` implemented but distributed executor may not consume coalesced plans | Medium | L3 | R13 | Wire optimizer output into task spec generation | `krishiv-optimizer`, scheduler | Job plan shows `CoalescePartitions` node executed |
| GAP-SH-05 | Dual shuffle APIs (`LocalShuffleStore` vs `ShuffleStore`) cause integration confusion | Low | L2 | R14 | Document canonical path; deprecate unused store | `krishiv-shuffle` | Architecture doc updated |

### Checkpoints & exactly-once

| ID | Gap / risk | Severity | Layer today | Resolution release | Proposed resolution | Key paths | Validation |
|----|------------|----------|-------------|-------------------|---------------------|-----------|------------|
| GAP-CK-01 | Restore does not validate metadata `fencing_token` vs live coordinator | High | L2 | **R12 carryover** | `restore_job_from_checkpoint` calls `validate_fencing_token` | `krishiv-checkpoint`, CLI restore | Stale epoch restore rejected |
| GAP-CK-02 | Only `LocalFsCheckpointStorage`; no S3/GCS backend | High | L2 | R16 / R18 | `ObjectStoreCheckpointStorage` implementing trait | `krishiv-checkpoint` | MinIO integration test |
| GAP-CK-03 | Sync checkpoint APIs in async coordinator (risk of blocking) | Medium | L3 | R13 | `spawn_blocking` wrapper at all storage call sites | scheduler, executor | Tokio metrics: no long blocks on worker |
| GAP-CK-04 | R9 tracker claims fencing at write boundary; code path gap (GAP-CP-03) | High | docs | **R12 carryover** | Fix code; align `r9-governance-and-operations.md` checklist with grep audit | docs + code | `rg validate_fencing_token` shows commit path |

### Connectors & lakehouse

| ID | Gap / risk | Severity | Layer today | Resolution release | Proposed resolution | Key paths | Validation |
|----|------------|----------|-------------|-------------------|---------------------|-----------|------------|
| GAP-CN-01 | Duplicate `RdkafkaCdcEventSource` struct (lines ~184 and ~626); `kafka` feature may not compile | Critical | L2 | **R12 carryover** | Merge into single module; one offset-commit policy | `krishiv-connectors/src/cdc.rs` | `cargo build -p krishiv-connectors --features kafka` |
| GAP-CN-02 | Default `KafkaSource`/`KafkaSink` return `Unsupported` without feature | High | L2 | R12 S3 / R13 | Real consumer/producer behind `kafka` feature; document requirement | `kafka.rs` | CI docker-compose Kafka test |
| GAP-CN-03 | Certification matrix “certified” LocalParquet; suite has 2 invariant tests | High | L2 | R14 | Full lifecycle tests: read, write, 2PC prepare/commit/abort, replay idempotency | `tests/certification.rs` | Matrix matches CI job |
| GAP-CN-04 | `CdcToLakehousePipeline::run()` stub; no Iceberg sink in pipeline | High | L2 | R14 | Wire `krishiv-lakehouse` sink in `run_with_source` | `cdc.rs`, `lakehouse` | CDC → Iceberg integration test |
| GAP-CN-05 | `ParquetSource` claims rewindable but lacks `reset()` | Medium | L2 | R13 | Implement `reset()` resetting row cursor | `parquet.rs` | Certification rewind test |
| GAP-CN-06 | S3 sink “idempotent” is last-writer-wins overwrite | Medium | L3 | R18 | S3 multipart 2PC or documented at-least-once only | `s3.rs`, matrix | Update matrix guarantee column |
| GAP-CN-07 | No S3/object-store `TwoPhaseCommitSink` despite matrix implications | High | L2 | R18 | Implement or downgrade matrix status to experimental | connectors, matrix | — |

### SQL, governance, Flight SQL

| ID | Gap / risk | Severity | Layer today | Resolution release | Proposed resolution | Key paths | Validation |
|----|------------|----------|-------------|-------------------|---------------------|-----------|------------|
| GAP-GV-01 | Post-execution column-name masking only; no row-level security | Medium | L3 | R20 | Row filters in `PolicyHook`; DataFusion analyzer hook | `krishiv-sql`, governance | Denied rows integration test |
| GAP-GV-02 | `MaterializedViewRegistry` in-memory only | Medium | L2 | R14 | Durable view metadata in catalog store | `krishiv-sql` | Restart refresh test |
| GAP-GV-03 | Flight SQL runs unauthenticated `sql_async` when auth without policy | Medium | L3 | R13 | Require both hooks or document; default deny | `krishiv-flight-sql` | Auth-only config rejects or uses policy |
| GAP-GV-04 | Static API keys only; no OIDC/JWT | Medium | L2 | R20 | OIDC `AuthProvider` for enterprise | `krishiv-governance` | Token exchange test |
| GAP-GV-05 | No durable audit store | Low | L2 | R20 | Pluggable `AuditSink` to object store / SIEM | governance | Export audit batch test |

### Observability, federation, operator, Python

| ID | Gap / risk | Severity | Layer today | Resolution release | Proposed resolution | Key paths | Validation |
|----|------------|----------|-------------|-------------------|---------------------|-----------|------------|
| GAP-OB-01 | Metrics counters/histograms sparse vs tracing | Low | L2 | R13 | Instrument scheduler hot path (submit, checkpoint, shuffle) | `krishiv-metrics`, scheduler | OTLP scrape shows job metrics |
| GAP-FD-01 | `krishiv-federation` in-memory only; no remote gRPC | Low | L1 | R19 | `RemoteFederationClient` + metadata consistency (ADR-19.1) | `krishiv-federation` | Two-region mock routing test |
| GAP-K8-01 | Operator strong but inherits scheduler durability gaps (GAP-CP-04–06) | High | L4 | R12 carryover + R13 | Fix scheduler startup before claiming HA operator | operator + coordinator binary | Kind e2e with sqlite metadata |
| GAP-PY-01 | `krishiv-python` `todo!()` in hot paths | High | L1 | R13 | Implement Session factories per R13 tracker | `krishiv-python` | `maturin test` passes |
| GAP-DOC-01 | `status.md` / trackers mark R12 items done while stubs remain (GAP-RT-01, GAP-RT-04) | High | docs | **R12 carryover** | Acceptance gate requires L4 evidence; link this register | `status.md`, r12 tracker | PR checklist uses gap IDs |

---

## R12 carryover sprint (recommended)

Close these before R13 Sprint 1; each maps to an acceptance gate item.

| Priority | Gap IDs | Deliverable |
|----------|---------|-------------|
| P0 | GAP-CP-03, GAP-CK-01, GAP-CK-04, GAP-CN-01 | Fencing enforced; kafka builds; docs accurate |
| P0 | GAP-RT-04 | Real remote coordinator RPCs |
| P1 | GAP-CP-04, GAP-CP-05, GAP-CP-06, GAP-SH-01, GAP-SH-03 | Durable coordinator + shuffle production path |
| P1 | GAP-RT-05, GAP-RT-06, GAP-DOC-01 | Policy fail-closed; status/tracker honesty |
| P2 | GAP-SH-02, GAP-CN-03 (partial) | Compression header; expand certification minimally |

---

## Documentation cross-references

| Document | Section to read |
|----------|-----------------|
| [`r12-r20-roadmap.md`](r12-r20-roadmap.md) | R12 — Maturity gaps & carryover |
| [`../implementation/r12-foundation-completeness.md`](../implementation/r12-foundation-completeness.md) | Maturity Gap and Risk Register |
| [`../implementation/status.md`](../implementation/status.md) | Active gaps & R12 carryover |
| [`../operations/production-hardening-guide.md`](../operations/production-hardening-guide.md) | Known limitations (post-R12 review) |
| [`compatibility-matrices.md`](compatibility-matrices.md) | Connector certification honesty |
| [`architectural-decisions-r12-r20.md`](architectural-decisions-r12-r20.md) | ADR-12.9 Maturity enforcement |

---

## Changelog

| Date | Change |
|------|--------|
| 2026-05-22 | Initial register from subsystem maturity code review |
