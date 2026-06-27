# Krishiv Streaming Architecture: Current-State-Aligned Plan

## Executive Summary

This document replaces the earlier greenfield-heavy streaming plan with an
architecture that matches the current Krishiv codebase and published engine
contracts. The target remains the same: lower streaming latency, durable
cloud-native state, and stronger recovery behavior. The path is different:
extend the existing runtime, state, checkpoint, SQL, and API crates instead of
creating parallel engines or duplicating abstractions that already exist.

Krishiv should evolve from drain-cycle streaming toward continuous execution by
preserving the Arrow `RecordBatch` data path and integrating the low-latency
buffering, stream-envelope, execution-profile, fusion, source-offset, and
transactional-sink pieces that already exist. Per-record APIs remain useful for
user process functions and timers, but DataFusion/Arrow relational operators
should stay batch-oriented.

Estimated effort for the full program: 20-30 weeks, assuming the existing
continuous window executor, queue/barrier support, checkpoint storage,
transactional sink registry, low-latency dataflow primitives, and prototype
disaggregated state backend are reused rather than reimplemented.

## Source of Truth

The Rust workspace is the source of truth. The relevant current ownership is:

| Area | Owning crate | Architectural decision |
|---|---|---|
| Runtime routing | `krishiv-runtime` | Keep one runtime model across embedded, single-node, and distributed modes. |
| Public API and pipelines | `krishiv-api` | Own public pipeline builders, local API streaming configuration, and embedded driver ergonomics without becoming a second distributed engine. |
| SQL parsing and DataFusion integration | `krishiv-sql` | Extend existing window TVFs and pipeline DDL rather than inventing a second SQL layer. |
| Logical/physical plan contracts | `krishiv-plan` | Carry typed streaming execution fields and versioned window/time semantics here. |
| Operators, queues, watermarks, windows | `krishiv-dataflow` | Own the existing stream envelopes, output buffers, execution profiles, queues, fusion detection, and stateful operators. |
| Executor task execution | `krishiv-executor` | Integrate continuous task loops, barriers, source offsets, and sink commit phases here. |
| Scheduler and coordinator fencing | `krishiv-scheduler` | Keep exactly one active coordinator per job and drive fenced checkpoint epochs here. |
| Typed wire/job contracts | `krishiv-proto` | Propagate typed execution policy fields; avoid stringly routed config. |
| State and checkpoint storage | `krishiv-state` | Evolve existing state traits/backends; do not create a separate state engine first. |
| Connectors and sink capabilities | `krishiv-connectors` | Keep exactly-once claims tied to connector capability and certification matrix. |
| Python bindings | `krishiv-python` | Expose the Rust API without introducing a divergent pipeline model. |

## Current Baseline

These capabilities already exist and should be treated as starting points, not
future work:

- Stateful continuous window execution via `ContinuousWindowExecutor`.
- Event-time watermarks, multi-source watermark specs, side outputs, dedup, and
  state-backed streaming operators.
- Streaming SQL TVF rewrite support for `TUMBLE`, `HOP`, and `SESSION`.
- `RunPolicy` as a coalescing knob: `Once`, `OnChange`, `EveryRows`, and
  `EveryMs`.
- `StreamEnvelope`, `OutputBufferPolicy`, `StreamingExecutionProfile`,
  `AutoProfileManager`, and a first-pass fusion detector in `krishiv-dataflow`.
- Checkpoint queue primitives with `CheckpointAlignment::{Aligned, Unaligned}`.
- Async checkpoint storage primitives plus sync compatibility wrappers.
- RocksDB-backed keyed state, TTL, portable snapshot bytes, savepoints, and
  queryable-state helpers.
- API-level `run_streaming` connector loops that read incrementally instead of
  normalizing every stream source to memory first.
- Executor `stream:loop:` window execution with retained `ContinuousWindowExecutor`
  state, persistent registry-backed connector source instances, generic encoded
  source-offset restore, and checkpoint acknowledgements carrying source offsets.
- Checkpoint metadata with source offsets and operator snapshot references,
  coordinator fencing validation, checkpoint timeout/abort paths, and
  executor-side transactional sink pre-commit/commit-through/restore handling.
- RocksDB incremental SST checkpoint manifests, plus a prototype DFS-primary
  key-per-file disaggregated state backend and async state operator helpers.
- ProcessFunction and timer APIs.
- Published engine and connector contracts that limit exactly-once claims to
  specific source/sink/checkpoint combinations.

The main missing piece is not "add streaming from scratch". It is an
executor-owned, envelope-driven continuous source/operator/sink runtime that
uses the same checkpoint and source-offset contract across embedded,
single-node, and distributed modes, without treating batch-sized drain cycles as
the latency floor.

## Deployment Mode Requirements

The streaming architecture must support embedded, single-node, and distributed
deployment modes through the same logical execution model. Deployment mode
selects placement, transport, durability defaults, and operational requirements;
it must not select a different streaming engine.

| Mode | Runtime placement | State and checkpoint defaults | Streaming requirements |
|---|---|---|---|
| Embedded | Runs inside the caller process through the in-process runtime path. | `dev-local` by default: ephemeral state and local/ephemeral checkpoints unless the caller configures durable paths. | Lowest-friction API/testing mode. Continuous loops must use bounded channels, support cancellation from the caller, and avoid hidden blocking on Tokio worker threads. No durable exactly-once claim unless durable checkpoint/state/sink capabilities are explicitly configured. |
| Single-node | Coordinator, executor, shuffle, state, and sinks may run on one host or a local daemon. | `single-node-durable`: local RocksDB state, local filesystem checkpoints, local disk shuffle, restart-durable on one host. | Must recover from process restart, preserve source offsets and operator snapshots, and use local endpoints safely. This is the first durable target for continuous streaming. |
| Distributed | Remote coordinator plus one or more executors with routable task, barrier, and shuffle endpoints. | `distributed-durable`: fenced scheduler metadata, object-store checkpoints, local RocksDB or object-store-backed state restored from checkpoints, and durable source/sink commits. | Must keep exactly one active coordinator per job, fence checkpoint epochs, require production auth where configured, support executor replacement, and avoid local fallback when remote placement is required. |

Mode-invariant decisions:

- The same typed `StreamingExecutionProfile`, `OutputBufferPolicy`, source
  offset model, and checkpoint metadata must flow through all modes.
- Checkpoint semantics remain coordinator-fenced epochs in every mode. Only the
  storage implementation changes.
- Connectors decide delivery guarantees through capability flags and the
  certification matrix. Deployment mode alone never upgrades a job to
  exactly-once.
- Embedded mode may share an address space with the caller, but it still uses
  the same source/operator/sink contracts as single-node and distributed modes.
- Distributed mode must not silently fall back to embedded or single-node
  execution when a remote coordinator/executor placement is required.

## Architecture Decisions

### Decision 1: Do Not Create `krishiv-mailbox` Initially

The earlier plan proposed a new `krishiv-mailbox` crate. That would split
operator runtime ownership away from `krishiv-dataflow`, which already owns
queues, barriers, windows, joins, and stateful operators.

Decision:

- Keep stream envelopes, low-latency output buffering, and continuous task-loop
  ownership inside `krishiv-dataflow` and `krishiv-executor`; the next work is
  to integrate the existing primitives into the hot execution path.
- Revisit a new crate only if the mailbox/runtime surface becomes large enough
  to be shared independently across multiple dataflow runtimes.

### Decision 2: Preserve Arrow `RecordBatch` on the Hot Path

Per-record execution is useful for timers, custom process functions, and fine
grained latency controls. It is not the right default for filter, projection,
aggregation, or SQL execution because Krishiv's internal data model is Arrow
`RecordBatch` and SQL execution is delegated to DataFusion.

Decision:

- Use small, time-bounded Arrow batches for low latency.
- Use `StreamEnvelope` values that carry `RecordBatch`, watermark, barrier,
  timer, and control messages. The current implementation lives in
  `krishiv-dataflow`; the remaining work is to make the executor runtime consume
  it directly.
- Avoid per-row wrappers around DataFusion filter/project operators.
- Keep row-level callbacks scoped to `ProcessFunction`, timers, and user-defined
  stateful logic.

### Decision 3: Evolve Coordinator-Fenced Epoch Barriers

The earlier plan described "barrier-based checkpointing" as if it replaced the
current protocol. Krishiv already defines checkpoints as coordinator-fenced
epochs. Barrier propagation should be the data-plane mechanism inside that
contract, not a new semantic model.

Decision:

- Keep coordinator-fenced epochs as the public checkpoint contract.
- Use barriers to align or unalign operator input streams.
- Make a checkpoint restorable only after source offsets, operator snapshots,
  in-flight unaligned buffers, and sink prepare records are durably recorded.
- Keep exactly-once guarantees tied to the engine and connector contracts.

### Decision 4: Evolve `krishiv-state`, Not a New `krishiv-hummock` Crate

RisingWave's Hummock design is a useful reference, but copying it as a new crate
too early would duplicate `krishiv-state` and bypass existing snapshot,
savepoint, TTL, and migration contracts.

Decision:

- Rename the target architecture generically as an object-store LSM state
  backend.
- Build it behind `krishiv-state` traits and durability profiles.
- Require manifest versioning, key-group ownership, compaction ownership, cache
  invalidation, garbage collection, and migration tests before calling it
  production-ready.
- Consider a separate crate only when compaction/storage internals justify a
  distinct ownership boundary.

### Decision 5: Separate Coalescing Policy From Execution Profile

`RunPolicy` controls how often a pipeline advances after input is fed. It is not
the same as a runtime execution profile.

Decision:

- Keep `RunPolicy` as the API coalescing knob.
- Propagate the existing typed `StreamingExecutionProfile` runtime behavior:
  `LowLatency`, `Throughput`, and `Auto`.
- Propagate the existing `OutputBufferPolicy` with `max_rows`, `max_bytes`, and
  `flush_interval_ms`.
- Add or formalize a `BacklogPolicy` with explicit hysteresis so automatic
  switching does not oscillate under bursty input.

### Decision 6: Normalize Event Time to UTC

Timezone support is a correctness issue, but adding timezone fields to every
record, watermark, and window risks ambiguous ordering and expensive data-plane
metadata.

Decision:

- Store event time and watermarks as UTC instants in the runtime.
- Use Arrow/DataFusion timestamp metadata and SQL expressions for timezone-aware
  parsing and display.
- Add optional window bucketing timezone to `WindowSpec` only when SQL semantics
  require civil-time windows.
- Never let timezone affect checkpoint ordering or watermark monotonicity.

### Decision 7: Certification Before Strong Claims

The plan must not describe Kafka, Iceberg, Parquet, object-store, or Python
pipelines as exactly-once unless the documented source/sink/checkpoint
combination is certified.

Decision:

- Keep "preview exactly-once" and "certified exactly-once" distinct.
- Add failure-injection tests before changing connector maturity labels.
- Preserve the engine-semantics and connector-contract matrices as normative.

## Framework Lessons, Corrected

External systems inform the plan but do not override Krishiv's invariants.

| Framework | Useful lesson | Constraint for Krishiv |
|---|---|---|
| Flink | Event-at-a-time control, barriers, unaligned checkpoints, buffer timeouts. | Keep Arrow batches for SQL/dataflow operators; use per-record control selectively. |
| Arroyo | Rust-native streaming, controller/worker split, remote checkpoint storage. | Krishiv already has scheduler/executor/runtime boundaries; do not duplicate them. |
| RisingWave | Object-store state, LSM compaction, meta/compute/compactor separation. | Start inside `krishiv-state`; add compaction ownership only after manifest/SST design is stable. |
| ArkFlow | Tokio-based Rust pipeline execution and buffer knobs. | ArkFlow is currently stateless; do not use it as evidence for state/checkpoint semantics. |

## Target Runtime Shape

```text
SourceReader
  -> StreamEnvelope::Data(RecordBatch)
  -> Dataflow task loop
       - mailbox-like priority for barriers/control
       - bounded data queues and backpressure
       - low-latency output buffer
       - optional operator fusion for forward edges
       - state access through krishiv-state
  -> SinkWriter
       - prepare(epoch)
       - commit(epoch) after coordinator checkpoint commit
       - abort(epoch) on failed checkpoint
```

Stream envelopes are typed and owned by `krishiv-dataflow`. If they become part
of a serialized wire path, add a versioned envelope rather than reusing an
unversioned in-process enum. The current code has both queue-level and
envelope-level `CheckpointAlignment`; Phase 0 should pick a canonical public
type or add explicit conversions so the two cannot drift.

```rust
pub enum StreamEnvelope {
    Data {
        batch: arrow::record_batch::RecordBatch,
        source_id: Option<String>,
        produced_at_ms: i64,
    },
    Watermark { epoch_ms: i64, source_id: String },
    CheckpointBarrier { epoch: u64, alignment: CheckpointAlignment },
    Timer { key: Vec<u8>, fire_time_ms: i64, kind: TimerKind },
    EndOfInput,
}
```

The executor should prioritize barriers and control messages without creating a
separate public engine. The scheduler remains responsible for fencing epochs and
committing checkpoints.

## Phase Plan

### Phase 0: Plan and Contract Cleanup

Duration: 1 week

Deliverables:

- Update this plan to match current crate ownership.
- Mark existing primitives as implemented, and retarget the remaining work to
  propagation, integration, and certification.
- Decide the canonical `CheckpointAlignment` type across `queue`, `envelope`,
  executor, and any future proto representation.
- Add a short design note for how `StreamingExecutionProfile` and
  `OutputBufferPolicy` flow through API, proto, executor, and checkpoint
  metadata.
- Add a short design note for checkpoint metadata changes needed by unaligned
  buffers and durable prepared-sink transaction references.
- Ensure any public claims link back to `docs/contracts/engine-semantics.md` and
  `docs/contracts/connectors.md`.

Validation:

- Documentation review.
- No code required unless names or references are wrong.

### Phase 1: True Continuous Pipeline Driver

Duration: 3-5 weeks

Goal: retire the remaining drain-cycle semantics while preserving the already
implemented API-level incremental connector loop and recent executor
source-offset work.

Work items:

- Add embedded, single-node-durable, and distributed-durable smoke tests proving
  registry-backed stream sources restore from the same encoded source-offset
  contract.
- Clarify source ownership: API-local `run_streaming` may drive embedded
  pipelines, but distributed streaming sources should be executor-owned and
  checkpointed through scheduler-fenced epochs.
- Replace `stream:loop:` registry source "read until `None`" cycles with bounded
  reads that respect downstream capacity and output-buffer policy.
- Define the source trait/backpressure path for bounded and unbounded reads:
  the source must await downstream capacity rather than relying only on polling
  loops.
- Add cancellation, graceful stop, and terminal error propagation for long-lived
  source/operator/sink tasks.
- Persist source offsets only through the checkpoint protocol for sources that
  support exact restore.
- Route the same driver through embedded, single-node, and distributed
  placement without changing stream semantics.
- Keep batch and IVM pipeline paths unchanged.

Files:

- `crates/krishiv-api/src/pipeline/driver.rs`
- `crates/krishiv-connectors/src/*`
- `crates/krishiv-executor/src/fragment/streaming.rs`
- `crates/krishiv-executor/src/runner/executor_task_runner.rs`
- `crates/krishiv-proto/src/job.rs`

Acceptance tests:

- Embedded, single-node-durable, and distributed-durable restore smoke tests use
  the same source-offset metadata.
- Unbounded test source emits multiple batches without materializing or draining
  the whole stream.
- Backpressure test proves source reads pause when sink/output capacity is full.
- Cancellation test stops source, operator, and sink tasks without leaked tasks.
- Rewindable source offset is not externally advanced before checkpoint commit.
- Batch and IVM behavior remain unchanged.

### Phase 2: Low-Latency Batch-Preserving Dataflow Runtime

Duration: 4-6 weeks

Goal: integrate the existing low-latency primitives into the executor/runtime
hot path without abandoning Arrow batches.

Work items:

- Stabilize or re-export the existing `StreamEnvelope`, `OutputBufferPolicy`,
  and `StreamingExecutionProfile` APIs where other crates need them.
- Canonicalize or explicitly convert queue and envelope checkpoint-alignment
  types.
- Wire output buffering by rows, bytes, and elapsed time into actual streaming
  emission and sink writes.
- Add an executor loop that prioritizes barriers/control messages while
  preserving bounded data queues for backpressure.
- Connect the existing fusion detector to planner/executor decisions for
  forward edges with same parallelism and no shuffle.
- Propagate `StreamingExecutionProfile::{LowLatency, Throughput, Auto}` through
  API, proto/job config, executor, and observability.
- Complete backlog detection with hysteresis:
  - `Auto` can increase batch size during backlog.
  - `Auto` can lower flush interval after backlog clears.
  - Profile changes must be observable and rate-limited.

Files:

- `crates/krishiv-dataflow/src/queue.rs`
- `crates/krishiv-dataflow/src/envelope.rs`
- `crates/krishiv-dataflow/src/buffer.rs`
- `crates/krishiv-dataflow/src/profile.rs`
- `crates/krishiv-dataflow/src/fusion.rs`
- `crates/krishiv-dataflow/src/continuous.rs`
- `crates/krishiv-dataflow/src/process_fn.rs`
- `crates/krishiv-executor/src/fragment/streaming.rs`
- `crates/krishiv-proto/src/job.rs`

Acceptance tests:

- Low-latency mode flushes by timeout even when row threshold is not reached.
- Throughput mode preserves or improves current bounded throughput baseline.
- Barriers overtake or align according to configured checkpoint alignment.
- Fused filter/project pipeline produces byte-identical batches compared with
  unfused execution.

### Phase 3: Checkpoint and Recovery Integration

Duration: 4-6 weeks

Goal: make barrier propagation, source offsets, operator snapshots, unaligned
buffers, and sink prepare/commit one coherent protocol.

Work items:

- Preserve existing checkpoint metadata support for source offsets and operator
  snapshot references.
- Extend checkpoint metadata to include:
  - in-flight buffers for unaligned checkpoints
  - durable sink transaction/prepared-commit references
  - execution profile and buffer policy used for the epoch
- Wire `CheckpointAlignment::Unaligned` into executor snapshot creation.
- Add restore logic for in-flight buffers.
- Fix known lazy-restore failure modes before expanding recovery tests.
- Verify existing coordinator-side checkpoint timeout/abort handling also drives
  sink abort and stale prepared-output cleanup.
- Add sink prepare/commit/abort tests for every preview exactly-once sink path.
- Add restart tests proving prepared sink output can be recovered after process
  loss, not only through an in-memory transaction registry.

Files:

- `crates/krishiv-state/src/checkpoint/*`
- `crates/krishiv-executor/src/fragment/streaming.rs`
- `crates/krishiv-executor/src/sections/recovery.rs.inc`
- `crates/krishiv-scheduler/src/barrier_dispatch.rs`
- `crates/krishiv-scheduler/src/job/scheduler.rs`
- `crates/krishiv-connectors/src/*`

Acceptance tests:

- Kill/restore preserves open window state and watermark.
- Unaligned checkpoint replay emits no duplicates for idempotent sink keys.
- Failed checkpoint aborts prepared sink output.
- Stale coordinator fencing token cannot commit an epoch.
- Savepoint restore preserves operator identity or fails with a typed migration
  error.
- Distributed recovery test proves executor replacement does not create a second
  active owner for the same job epoch.

### Phase 4: State Backend Evolution

Duration: 6-9 weeks

Goal: provide cloud-native state without breaking existing state contracts.

Work items:

- Keep `StateBackend` for synchronous local backends.
- Add an explicit async state trait for remote/object-store backends instead of
  wrapping synchronous calls in futures.
- For RocksDB, ensure async paths use `spawn_blocking` or a dedicated state I/O
  runtime; do not block Tokio worker threads.
- Keep the existing RocksDB incremental SST checkpoint path as the local durable
  state/checkpoint compatibility track.
- Replace or retire the prototype DFS key-per-file layout with an object-store
  LSM design:
  - immutable SST files
  - manifest per committed epoch
  - block index and bloom filters
  - memory block cache
  - optional local disk cache
  - key-group or vnode ownership
  - compaction worker ownership
  - tombstones and range delete support
  - garbage collection for unreferenced files
  - format versioning and migration tests
- Fix the current DFS backend's snapshot identity limitation before using it for
  recovery claims.
- Keep current portable snapshot bytes as a compatibility path.

Files:

- `crates/krishiv-state/src/backend.rs`
- `crates/krishiv-state/src/async_operator.rs`
- `crates/krishiv-state/src/dfs_backend.rs`
- `crates/krishiv-state/src/incremental_checkpoint.rs`
- `crates/krishiv-state/src/checkpoint/*`
- `crates/krishiv-common/src/*` for durability-profile configuration if needed.

Acceptance tests:

- Async state access does not block a Tokio worker thread.
- Restart from object-store manifest restores exactly the committed epoch.
- Compaction never removes files referenced by active checkpoints/savepoints.
- Cache eviction preserves correctness and exposes hit/miss metrics.
- Rescaling redistributes key groups without state loss.

### Phase 5: Event Time, Timezone, and SQL Semantics

Duration: 2-4 weeks

Goal: make event-time semantics correct without contaminating checkpoint ordering.

Work items:

- Normalize runtime event-time and watermarks to UTC milliseconds or Arrow
  timestamp values with explicit timezone metadata.
- Add a `window_timezone` option only for SQL civil-time bucketing.
- Keep watermark comparison timezone-free.
- Extend existing `TUMBLE`, `HOP`, and `SESSION` TVF rewrite tests for timezone
  inputs.
- Add `CREATE STREAMING TABLE` or `CREATE MATERIALIZED VIEW` only as syntax sugar
  over the existing pipeline/materialized-table path.
- Reject ambiguous local timestamps unless a timezone policy is configured.

Files:

- `crates/krishiv-plan/src/window.rs`
- `crates/krishiv-sql/src/streaming_tvf.rs`
- `crates/krishiv-sql/src/window_functions.rs`
- `crates/krishiv-api/src/window.rs`
- `crates/krishiv-python/src/stream.rs`

Acceptance tests:

- UTC watermarks are monotonic across timezone conversions.
- Civil-time tumbling windows around DST transitions behave according to the
  configured timezone policy.
- Existing timestamp-without-timezone behavior remains backward-compatible.

### Phase 6: Public Rust and Python API

Duration: 2-4 weeks

Goal: expose the runtime model without creating a second pipeline API.

Work items:

- Expose Rust builder methods that map to the existing
  `StreamingExecutionProfile` and `OutputBufferPolicy` runtime types.
- Keep `RunPolicy` as the coalescing/advance policy.
- Add Python methods that map directly to Rust:
  - `pl.execution_profile("low_latency" | "throughput" | "auto", ...)`
  - `pl.output_buffer(max_rows=..., max_bytes=..., flush_interval_ms=...)`
  - `pl.run("continuous")` only when the source is actually unbounded.
- Keep pipeline sinks capability-aware. A sink method may exist without an
  exactly-once claim.
- Add friendly errors for feature-gated connectors.

Files:

- `crates/krishiv-api/src/pipeline/mod.rs`
- `crates/krishiv-api/src/streaming_builder.rs`
- `crates/krishiv-python/src/pipeline_api.rs`
- `crates/krishiv-python/src/session.rs`
- `crates/krishiv-python/python/tests/*`

Acceptance tests:

- Rust and Python APIs produce the same typed profile config.
- Continuous run rejects bounded-only sources unless explicitly run as batch.
- Feature-gated connector errors name the missing Cargo feature and
  `maturin develop --features ...` command.

### Phase 7: Certification, Observability, and Benchmarks

Duration: 3-5 weeks

Goal: make performance and delivery claims measurable.

Work items:

- Add metrics:
  - source read latency
  - output buffer flush reason
  - checkpoint alignment time
  - unaligned in-flight bytes
  - checkpoint upload time
  - restore time
  - state cache hit/miss
  - object-store request count
  - sink prepare/commit/abort duration
  - backpressure duration
- Add chaos tests:
  - executor kill during checkpoint
  - coordinator failover during checkpoint
  - sink prepare success followed by coordinator abort
  - source offset restore after executor loss
  - object-store transient failure during checkpoint upload
- Add performance baselines:
  - current drain-cycle latency
  - low-latency buffered path p50/p95/p99
  - throughput path rows/sec
  - restore time by state size
  - memory usage with and without fusion
- Add deployment-mode smoke suites for embedded, single-node-durable, and
  distributed-durable profiles.

Acceptance criteria:

- Published targets are backed by benchmark output.
- Connector maturity labels are not upgraded without passing the failure matrix.
- CI has narrow smoke tests and an opt-in longer certification suite.

## Revised Gap Analysis

| Gap | Current state | Target state | Priority |
|---|---|---|---|
| True unbounded execution | API `run_streaming` reads connector batches incrementally; executor `stream:loop:` still runs bounded cycles over retained state. | Executor-owned continuous source/operator/sink loop with backpressure, cancellation, and mode-invariant semantics. | Critical |
| Latency floor | Coalescing/drain cycles still dominate the executor path; low-latency primitives exist but are not fully wired. | Time-bounded Arrow batches plus output buffering and fused forward operators in the hot path. | Critical |
| Checkpoint integration | Source offsets, operator snapshots, fencing, timeout/abort, and transactional sink registry exist; unaligned buffers and durable prepared-sink references are incomplete. | One fenced epoch protocol covering barriers, offsets, snapshots, in-flight data, and durable sink transaction refs. | Critical |
| Async state | Prototype async helpers can still call sync state methods in async contexts. | Explicit async backend trait or blocking isolation for sync backends. | High |
| Disaggregated state | RocksDB incremental SST checkpointing exists; DFS-primary key-file backend is prototype-grade and has snapshot identity limitations. | Manifested object-store LSM with block cache, compaction, GC, and rescaling, while preserving RocksDB checkpoint compatibility. | High |
| Timezone correctness | Event time is mostly raw millisecond semantics. | UTC runtime ordering plus timezone-aware SQL bucketing. | Medium |
| Public config | `RunPolicy` exists; typed runtime profile/buffer structs exist in `krishiv-dataflow` but are not propagated through API/proto/executor. | Separate coalescing policy from streaming execution profile across all runtime modes. | Medium |
| Certification | Preview exactly-once paths exist. | Failure-injection-backed certification per connector combination. | High |

## Rejected Approaches

| Approach | Reason rejected |
|---|---|
| Build a separate per-record engine | Violates the one-runtime model and loses Arrow/DataFusion efficiency. |
| Add `krishiv-mailbox` immediately | Duplicates `krishiv-dataflow` ownership before the abstraction is proven. |
| Add `krishiv-hummock` immediately | Duplicates `krishiv-state`; object-store LSM needs to evolve behind existing contracts first. |
| Per-row filter/project adapters | Would undercut vectorized Arrow/DataFusion execution. |
| Blanket exactly-once wording | Conflicts with published connector and engine contracts. |
| Timezone fields on every control/data record | Runtime ordering should be UTC; timezone belongs to parsing/display/window bucketing. |

## File Impact Summary

| File or module | Expected change |
|---|---|
| `crates/krishiv-api/src/pipeline/driver.rs` | Keep the API-local incremental connector loop, add cancellation/stop semantics, and align it with executor-owned source/checkpoint contracts. |
| `crates/krishiv-api/src/pipeline/mod.rs` | Expose typed streaming execution profile and output buffer config. |
| `crates/krishiv-proto/src/job.rs` | Propagate typed job-level streaming execution policy fields. |
| `crates/krishiv-dataflow/src/envelope.rs` | Keep the canonical stream envelope and version only if serialized. |
| `crates/krishiv-dataflow/src/queue.rs` | Canonicalize or convert checkpoint alignment and integrate envelopes into task runtime. |
| `crates/krishiv-dataflow/src/buffer.rs` | Wire output buffering into actual streaming emission. |
| `crates/krishiv-dataflow/src/profile.rs` | Feed runtime profile decisions into buffer policy and observability. |
| `crates/krishiv-dataflow/src/fusion.rs` | Connect fusion detection to planner/executor decisions. |
| `crates/krishiv-dataflow/src/continuous.rs` | Keep stateful windows; adapt to envelope-driven runtime. |
| `crates/krishiv-executor/src/fragment/streaming.rs` | Move from bounded `stream:loop:` cycles toward continuous envelope-driven reads, bounded connector polling, barrier handling, and source-offset staging. |
| `crates/krishiv-executor/src/runner/executor_task_runner.rs` | Carry runtime policy, checkpoint barriers, transactional sink lifecycle, and restore state across long-lived tasks. |
| `crates/krishiv-state/src/checkpoint/*` | Extend metadata for in-flight buffers, durable sink transaction refs, and profile config while preserving existing offsets/snapshots. |
| `crates/krishiv-state/src/dfs_backend.rs` | Retire or replace prototype key-file backend with object-store LSM internals. |
| `crates/krishiv-state/src/incremental_checkpoint.rs` | Preserve RocksDB SST checkpoint compatibility while object-store state evolves. |
| `crates/krishiv-state/src/async_operator.rs` | Remove hidden blocking from async state access. |
| `crates/krishiv-plan/src/window.rs` | Add timezone-aware bucketing config without changing watermark ordering. |
| `crates/krishiv-sql/src/streaming_tvf.rs` | Extend existing TVF tests and syntax support, do not duplicate window SQL. |
| `crates/krishiv-python/src/pipeline_api.rs` | Mirror Rust execution profile and output buffer APIs. |

## Validation Strategy

### Correctness

- Window output equivalence for fused and unfused execution.
- Watermark monotonicity across timezone conversions.
- Checkpoint restore after executor kill.
- Source offset restore for rewindable sources.
- Prepared sink output is committed only after coordinator checkpoint commit.
- Savepoint restore rejects incompatible operator IDs or serializer versions.

### Performance

- p50/p95/p99 end-to-end latency for low-latency profile.
- Throughput rows/sec for throughput profile.
- Checkpoint duration and alignment delay.
- Restore time by state size.
- State access p50/p95/p99 and cache hit rate.
- Object-store request count per checkpoint and per state read.

### Certification

- Kafka source plus transactional Kafka sink.
- Kafka source plus Iceberg two-phase sink.
- Kafka source plus two-phase Parquet/object-store sink.
- Rewindable source plus idempotent sink.
- Non-rewindable source failure behavior remains documented as best effort.

## Success Metrics

Targets must be measured against the current codebase before being published.
Initial engineering targets:

- Low-latency profile: p99 below 100 ms for in-process memory source/sink and
  simple stateless pipeline.
- Throughput profile: no regression versus current drain-cycle throughput for
  equivalent batch size.
- Checkpoint: no barrier deadlock under backpressure.
- Restore: successful restore of window state, watermark, source offset, and
  in-flight unaligned buffers.
- State: object-store backend can restore committed epoch without scanning all
  remote objects.
- Certification: no connector marked certified until failure-injection tests
  pass for its exact source/sink/checkpoint profile.

## Immediate Next Steps

1. Add embedded, single-node-durable, and distributed-durable smoke tests for
   connector-backed streaming restore using encoded source-offset metadata.
2. Canonicalize `CheckpointAlignment` across dataflow queues, stream envelopes,
   executor barriers, and future proto fields.
3. Propagate existing `StreamingExecutionProfile` and `OutputBufferPolicy`
   through Rust API, proto/job config, executor runtime, and observability.
4. Bound `stream:loop:` registry-source reads by downstream capacity and output
   buffer policy, then add cancellation and graceful-stop behavior.
5. Define checkpoint metadata version changes for unaligned in-flight buffers,
   durable prepared-sink transaction refs, and per-epoch runtime policy.
6. Fix async state access so sync RocksDB and DFS/object-store work cannot block
   Tokio worker threads.
7. Convert the prototype disaggregated backend into a manifest/SST design note
   before expanding code, and add benchmark baselines before publishing latency
   or recovery improvements.

## References

- `docs/README.md`
- `docs/contracts/engine-semantics.md`
- `docs/contracts/connectors.md`
- `docs/implementation/status.md`
- Flink architecture and checkpointing documentation
- Arroyo architecture documentation
- RisingWave architecture documentation
- ArkFlow introduction documentation
- Chandy-Lamport snapshots
- The Dataflow Model
