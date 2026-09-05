# Executor and data plane

`krishiv-executor` runs tasks. It receives typed fragments from the
coordinator, decides which execution model they need, runs them with a
capacity budget derived from the container, and reports results, shuffle
output, and checkpoint acknowledgements back. Everything that touches data
lives here or in the crates it drives (`krishiv-sql`, `krishiv-dataflow`,
`krishiv-ivm`, `krishiv-shuffle`, `krishiv-state`, `krishiv-connectors`).

## Task intake

An `ExecutorTaskAssignment` arrives over gRPC. The runner
(`ExecutorTaskRunner`, `runner/`) decodes the fragment through
`TypedTaskFragment::decode_for_profile` — under a durable profile or
`KRISHIV_PRODUCTION=1` an untyped legacy body is refused unless
`KRISHIV_ALLOW_LEGACY_FRAGMENTS` is set — and classifies it
(`execution_model.rs`):

| `ExecutionModel` | Lifetime | Fragment bodies | Terminal state |
|---|---|---|---|
| `Batch` | finite; `task_timeout_secs` applies | `dfplan:v1:…`, `sql:…`, connector/Parquet partitions | `Succeeded` / `Failed` |
| `Streaming` | until cancelled; timeout ignored | `stream:{cep,continuous,cw,loop,rbatch,rjoin,rloop,rpipe,ses,spec,sw,tw}:…` | `Cancelled` on stop, `Failed` on fatal error |
| `DeltaBatch` | one bounded IVM tick | `delta:attach:`, `delta:tick:`, `delta:detach:` | `Succeeded` / `Failed` |

The run-loop family (`is_run_loop_family`: `rloop`, `rjoin`, `rpipe`,
`rbatch`) is dispatched on a no-timeout arm and reports `Cancelled`, not
`Succeeded`, when stopped — a prefix missing from that list would silently
inherit a batch task's lifecycle, which is why the check is a single function.

## Batch execution

A `dfplan:v1:<partition>:<plan>` task deserialises the DataFusion physical
plan, replaces `ShuffleReadExec` leaves with readers over the assignment's
`ShuffleReadConfig`, executes exactly the named partition(s), and either
writes hash-partitioned output through `ShuffleWriteConfig` (`ShuffleMap`
stage) or returns rows (`Result` stage). Results under the inline threshold go
back in the `TaskStatus`; larger ones are spooled and streamed
(`runner/result_spool.rs`, `KRISHIV_INLINE_RESULT_MAX_BYTES`). Each task gets
its own DataFusion `SessionContext` with `target_partitions` from the capacity
model below and a memory reservation in the executor's shared pool.

## Capacity: one decision per process

`krishiv_common::executor_capacity::ExecutorCapacity::detect()` derives task
slots, the query memory pool, and per-task parallelism from one fact — the
cgroup's CPU and memory — instead of three independent variables that nothing
kept consistent:

| Quantity | Derivation | Override |
|---|---|---|
| slots | `available_parallelism()` | `KRISHIV_TASK_SLOTS` / `--slots` |
| per-task `target_partitions` | `cores / slots` (the *actual* slot count) | `KRISHIV_TASK_TARGET_PARALLELISM` |
| query pool | 0.6 × (cgroup limit − 512 MiB reserve), shared and hard-capped across slots | `KRISHIV_QUERY_MEMORY_LIMIT_BYTES` |
| push-shuffle store | 0.15 × post-reserve | `KRISHIV_SHUFFLE_STORE_BYTES` |
| shuffle page-cache ceiling | 0.05 × post-reserve (≥ 32 MiB or disabled) | `KRISHIV_SHUFFLE_PAGE_CACHE_BYTES` |

The fractions are where an SF100 OOM investigation left them: pools sized at
0.8 never reported pressure while executors were killed 11 times, because a
`MemoryPool` sees only what operators reserve through it. Page cache for
write-once shuffle files was a third of the container and is now dropped at
the source (`krishiv_common::page_cache`) rather than budgeted around. Adding
a slot divides the same budget more ways; overcommit is structurally
impossible rather than an arithmetic the operator must get right.

The pool is a `FairSpillPool`; operators that can spill do (`06`,
`SpillableJoinSelection` in `02`). A map task never holds a whole output
partition as an unspillable reservation — `ShuffleStore::write_partition_stream`
exists so one oversized reservation cannot saturate availability for every
other consumer.

## Streaming execution

The executor hosts every distributed streaming loop (`08-streaming.md` has
the operator semantics; this section is the process view).

**Run-loop (`stream:rloop:<job>|<subtask>/<parallelism>|<window spec>`)** —
the long-lived model. The task launches once and runs until `CancelTask`. The
subtask owns its source splits (registry connector sources filtered by
subtask index), wakes on pushed-input notifies with a 50 µs idle floor, owns a
contiguous key-group range, and forwards rows outside that range to the owning
peer over the executor→executor `push_continuous_input` RPC
(`stream_exchange.rs`). Each peer channel sits behind a `CreditGate` of 8 MiB;
credits are taken before the RPC and returned on acceptance, so a slow peer
backpressures the sender (Flink's credit model). The receiver's per-job
pending cap (`KRISHIV_RLOOP_INPUT_BUFFER_CAP`, 64 batches) stays authoritative;
`ResourceExhausted` is retried with backoff up to 5 times under a 10 s RPC
deadline. Barriers are drained at every iteration boundary, state is snapshot
only at barrier epochs, and staged sink output is prepared at the barrier and
committed by the checkpoint-complete notification. Emitted windows land in a
bounded per-job egress ring (`KRISHIV_RLOOP_EGRESS_CAP`, 512) served by the
drain API; when the ring is the job's only delivery path the loop waits
`KRISHIV_RLOOP_EGRESS_BACKPRESSURE_MS` (30 s) for a consumer before faulting
instead of dropping computed output.

**Classed run-loops** — `stream:rbatch:` (stateless per-batch SQL through
`StatelessBatchExecutor`), `stream:rjoin:` (two-source watermark interval
join, sides tagged `#L`/`#R` on the input buffer key), `stream:rpipe:`
(operator pipeline). Same framing, JSON spec payload.

**Cycle model (`stream:loop:`, `stream:continuous:`)** — one coordinator-fenced
input cycle per task assignment; retained as the escape hatch and for the
HTTP push/drain drivers.

**Bounded windows (`stream:tw:`, `sw:`, `ses:`, `cw:`, `spec:`, `cep:`)** —
finite window tasks over a bounded source, used by `bounded-window` HTTP jobs
and the SQL `TUMBLE`/`HOP`/`SESSION` batch path.

The latency instrument for all of them is
`krishiv_stream_record_latency_seconds` (source-read → operator-emit, µs
buckets).

## Resident IVM

`fragment/ivm.rs`: `delta:attach:{job}|{specs}|{state}|{fence}` seeds a flow
that stays resident in `ResidentIvmFlows`; each `delta:tick:` carries only
that tick's input deltas and a fence and answers with per-view *output
deltas*; `delta:detach:` releases it. Every payload part is base64 so a `|`
inside SQL cannot break framing. The fence turns a replayed or skipped tick
into an error rather than a double-apply, and the executor holds no
authority: on any failure the coordinator re-feeds pending deltas and
computes centrally. Two wire dialects coexist (`IVMD1` delta map; `IVMD2`
tick result with health), selected by the dialect the coordinator sent, so a
rolled-forward executor never answers an older coordinator in a format it
cannot read. The stateless per-tick full-state `delta:step:` path was deleted
and now fails loudly.

## Reporting

- **Heartbeat** every tick with `ExecutorHealthSnapshot` (memory used/limit,
  active tasks, CPU, network bytes) and the lease generation.
- **Task status** transitions; a report carrying a superseded lease or attempt
  is rejected by the coordinator.
- **Checkpoint ack** per barrier: operator snapshot refs, source offsets,
  prepared sink refs (`07`).
- **Metrics** on `/metrics` of the executor process (`13`).

## Process wiring

`krishiv executor` builds: capacity → shared memory pool → shuffle backend
from `KRISHIV_SHUFFLE_URI` (`06`) → state directory for durable profiles →
connector registry → gRPC server (`ExecutorService`: task assignment,
cancel, barrier, restore, `push_continuous_input`, drain) → coordinator
client with registration and heartbeat loop. `krishiv shuffle-svc` runs the
shuffle data service as a separate process when the deployment wants
external shuffle.

## Related

- `04-scheduler-and-coordinator.md` — who sends the assignments.
- `06-shuffle.md`, `07-state-checkpoints-savepoints.md`, `08-streaming.md`,
  `09-incremental-view-maintenance.md`.
- `16-performance.md` — the SF100 memory findings in full.
