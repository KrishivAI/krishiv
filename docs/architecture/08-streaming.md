# Streaming

Krishiv's streaming engine is event-time dataflow over Arrow batches. One
operator core (`krishiv-dataflow`) is driven by several loops that differ in
lifecycle, and a closed policy type makes every loop answer the same
questions. This document covers the SQL and API shapes that reach the engine,
watermarks and windows, the operator library, the driver policy, the loops,
and the dials.

## Front doors

| Surface | Shape | Compiles to |
|---|---|---|
| SQL | `SELECT … FROM TUMBLE(t, ts, INTERVAL …)` / `HOP` / `SESSION`, `GROUP BY key, window_start` | `WindowExecutionSpec` via `krishiv_sql::streaming_window_plan::compile_streaming_window_sql`; non-windowed SQL on an unbounded source is `EngineError::Unsupported` |
| SQL | `MATCH_RECOGNIZE` | CEP pattern (`krishiv_plan::cep`) → `stream:cep:` |
| Rust / Python | `StreamingDataFrame` (`window`, `interval_join`, `process`, `broadcast_process`, `dedup`, `side_output`), `Pipeline` | `StreamingTaskSpec::{Window, Join, Pipeline, Stateless}` |
| HTTP | `/api/v1/continuous*` push/drain, `/api/v1/bounded-window` | cycle-model or bounded fragments |
| `ComputeEngine` | `StreamingEngine::run` (bounded) / `spawn_streaming_job` (unbounded) | the embedded loop |

The compiled spec carries: window kind and sizes, key column(s)
(`key_parts`, synthetic keys), event-time column, `watermark_lag_ms`,
per-source lags and `source_id_column` for multi-source watermarks,
aggregates (`sum`, `count`, `avg`, `min`, `max`, `count_distinct`, and
float-aware variants), optional `top_n`, `state_ttl_ms`, and
`allowed_lateness_ms`. The spec is validated (`validate_window_execution_spec`)
before any operator is built and is encoded compactly in `stream:*` fragment
bodies.

## Time and watermarks

- `WatermarkState`: watermark = max(event time seen) − lag, monotonic,
  cached per `advance`. A floor seeded from the upstream stage's output
  watermark (`with_initial_watermark`) is what makes late-event handling work
  past stage one — without it every stage restarted at `i64::MIN` and
  declared nothing late.
- Late events (event time < watermark) are dropped **before** `advance`,
  counted in `late_events_dropped`, and offered to a `LateEventHandler`
  (side output / dead-letter / metrics; `CountingLateEventHandler` by
  default). `allowed_lateness_ms` keeps a closed window open for late firing.
- `MultiSourceWatermarkState` combines per-source watermarks by minimum;
  the idle-source policy marks a source idle after 5 minutes (continuous
  executor) or `KRISHIV_WATERMARK_IDLE_MS` (30 s, run-loop) so one stalled
  partition does not freeze every window.
- `IdleTick::WallClock` loops advance the watermark on wall time every
  `KRISHIV_IDLE_TICK_MS` (500 ms) while sources are quiet, so a session
  window whose gap elapsed can close.

## Window operators (`krishiv-dataflow::window`)

| Operator | Semantics | State |
|---|---|---|
| `TumblingWindowOperator` / `StateBackedTumblingWindowOperator` | fixed, non-overlapping | per (key, window) accumulators |
| `SlidingWindowOperator` / state-backed | size + slide; a row belongs to ⌈size/slide⌉ windows — done as a fan-out, not a scalar projection | per (key, window) |
| `SessionWindowOperator` / state-backed | gap-based, merges on overlap | per key open session |
| `CountWindowOperator` | every N rows per key | per key buffer |

State-backed variants hold active windows in memory and write through to a
`StateBackend` (`07`) so a checkpoint is a `sync()`, not a serialisation of
the whole map. `ContinuousWindowExecutor` (`continuous.rs`) wraps one operator
with a `WatermarkTracker`, TTL, and per-key group state across drain cycles;
it is the single operator core every loop borrows.

## The operator library

| Module | Provides |
|---|---|
| `interval_join` | two-stream event-time join within `[lower, upper]` bounds; `WatermarkWindowJoinOperator` |
| `delta_join` | stateless append-only stream join (P8) |
| `join` | keyed hash join primitives, `extract_agg_key` |
| `dedup_operator` | first-seen dedup with TTL |
| `cep` | `MATCH_RECOGNIZE` matcher with partition-by, `AFTER MATCH SKIP`, quantifiers |
| `process_fn`, `group_state`, `state_descriptor` | Flink-style `ProcessFunction` with `ValueState`/`ListState`/`MapState`/`AggregatingState`, timers |
| `connected_streams`, `broadcast_state` | two-input operators; broadcast side updates a rule set every subtask sees |
| `side_output` | tagged secondary outputs (late data, rejects) |
| `pipeline` | chain of stateless operators executed per batch |
| `adaptive` | per-batch adaptive execution hints |
| `schema_normalize`, `scalar_expr`, `aggregate`, `memo` | typing, expression evaluation, aggregate kernels, memoisation |
| `queue`, `barrier_align` | the checkpoint-aware operator queue and alignment state machine (`07`) |
| `stream_driver`, `streaming_corpus` | the driver policy and the cross-loop conformance corpus |

## The driver policy

`stream_driver.rs` is the answer to the recurring defect class "a decision
made in one loop and quietly absent from another". `StreamingLoop` is a
closed enum of the loops that exist; `StreamingLoop::policy` is an exhaustive
match to a `DriverPolicy` with no `Default`, no builder, and no
`#[non_exhaustive]`; `const_coherence` fails the build on a self-contradictory
policy. Adding a loop does not compile until it answers every axis:

| Axis | Values |
|---|---|
| `IdleTick` | `None` (windows close only on events) · `WallClock` |
| `EndOfStream` | `NoFlush` (unbounded) · `FlushOnSourceExhausted` (bounded) · `FlushOnDirective` (control plane says so) · `DelegatedToRuntime` (no local operator) |
| `InputTyping` | `CoerceToSpec` · `PreCoerced` |
| `Lifecycle` | `OwnsWholeJob` · `TransientPerInvocation` · `LongLived` |
| `Egress` | `Backpressure` (lossless) · `CappedDropOldest` (lossy by construction, counted) |
| `NullKey` | `Fatal` · `QuarantineAndCount` |

Each loop *holds* a `StreamDriver` and borrows its operator to it per call;
the driver decides *when* to step, the loop keeps its lifecycle. The honest
ceiling is stated in the module: the gate catches new loops, not new
decisions implemented in one loop — the counterweight is `streaming_corpus`,
which runs the same inputs through every loop and fails on divergence.

## The loops

| Loop | Where | Lifecycle | Notes |
|---|---|---|---|
| embedded `run_streaming_continuous` | `krishiv-api` / `krishiv-engines` | owns whole job | notify-driven with an idle tick; the model the run-loop was promoted from |
| `stream:rloop:` run-loop | `krishiv-executor::fragment::run_loop` | long-lived | key-group parallel, peer exchange, live barriers, epoch sinks, bounded egress (`05`) |
| classed run-loops `rbatch` / `rjoin` / `rpipe` | `run_loop_classes.rs` | long-lived | stateless SQL, interval join, pipeline; same framing |
| cycle model `stream:loop:` / `continuous:` | `fragment/streaming.rs` | transient per invocation | one fenced cycle per assignment; escape hatch and HTTP drivers |
| bounded windows `tw`/`sw`/`ses`/`cw`/`spec`/`cep` | `fragment/streaming.rs` | owns whole job | finite source, flush on exhaustion |

## Dials (`krishiv_common::streaming_dials`)

One implementation, shared by the embedded loop and the executor — the module
exists because two copies of the same default once disagreed invisibly.

| Variable | Default | Meaning |
|---|---|---|
| `KRISHIV_IDLE_TICK_MS` | 500 | idle watermark advance |
| `KRISHIV_STREAM_PROFILE` | `latency` | `throughput` lingers `THROUGHPUT_LINGER_MS` = 5 ms before draining |
| `KRISHIV_STREAM_LINGER_MS` | profile | explicit linger |
| `KRISHIV_RLOOP_EGRESS_CAP` | 512 batches | egress ring; drops oldest; 0 rejected |
| `KRISHIV_RLOOP_EGRESS_BACKPRESSURE_MS` | 30 000 | wait for a consumer when the ring is the only sink; 0 = fault immediately |
| `KRISHIV_RLOOP_INPUT_BUFFER_CAP` | 64 batches | per `{job}#{task}` pushed-input cap before pushes are refused |
| `KRISHIV_WATERMARK_IDLE_MS` | 30 000 | split idleness for min-combining |

## Delivery guarantees

Operator state is exactly-once by barrier snapshot; source offsets are
recorded per epoch; sinks are exactly-once where they implement two-phase
commit and at-least-once otherwise (`PostWriteOffsetCommitProtocol`: write,
flush, then commit the offset). The job's reported guarantee is the weakest
link (`10`, `DeliveryGuarantee`). The egress ring is the one lossy component,
and it is counted and bounded rather than hidden.

## Related

- `05-executor-and-data-plane.md` — the process view of the run-loop.
- `07-state-checkpoints-savepoints.md` — barriers, state, restore.
- `09-incremental-view-maintenance.md` — the *other* continuous engine and
  when to use which.
- `../engineering-log/crate-audit-register.md` — the streaming corpus and
  the loop-divergence defects it closed.
