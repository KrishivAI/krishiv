# Distributed batch execution — pre-sweep architectural review

**Date:** 2026-07-27
**Scope:** coordinator staging → dispatch → executor fragment → shuffle write/serve/fetch → result delivery, for **distributed batch** only.
**Method:** read-only. No build, no test run, no edit. `crates/krishiv-executor/src/fragment/` is under concurrent edit by the shuffle-drain fix agent; findings there are written against `HEAD` (9e762878) with the working-tree delta noted where it matters.
**Bias:** contracts between processes and resource bounds are weighted above local logic, because the last five fixes all passed unit tests and failed on the wire.

Severity key: **P0** correctness (wrong answers, or a job that cannot complete) · **P1** performance (a query that will not finish in budget) · **P2** hardening (makes the next failure diagnosable or bounded).

Counts: **7 P0 · 11 P1 · 12 P2** (30 findings).

---

## Lens A — Cross-process contracts

### A1 (P0) A partition with no attached location reads as *silently empty*, not as an error

The reduce side has three cases but only two are distinguishable:

- `crates/krishiv-scheduler/src/job/record.rs:286-300` — the coordinator builds `ShuffleFlight` input partitions from each producer's reported outputs and **skips any output whose `flight_endpoint` is empty** (`:287-289`).
- `crates/krishiv-executor/src/fragment/batch.rs:993-1008` — the executor keeps only entries where `!flight_endpoint.is_empty() && *flight_endpoint != own_endpoint`.
- `crates/krishiv-executor/src/fragment/batch.rs:930-940` — anything not in that map falls through to `store.read_partition(&id)` and `…​.unwrap_or_default()`.

So "written on this executor and genuinely empty" and "written on another executor and the coordinator never told me" produce the identical answer: zero rows, `Ok`. The contract is stated only as a doc comment (`batch.rs:845-850`) and is enforced by no code on either side.

Reachable whenever a producer reports an empty endpoint — an executor that came up before its shuffle Flight listener was configured, or a restarted container that lost `--shuffle-addr`. The existing test `dfplan_reader_local_miss_reads_empty` (`batch.rs:2306`) *asserts the hazardous behaviour*.

**Sweep failure it causes:** wrong row counts on a query that reports success. Worse than a crash, because the digest comparison is the only thing that would catch it.

**Fix (medium):** give the reader positive evidence. Ship the producing executor id (or the complete location set for `(stage_key, partition)`) with every consumer assignment and make an unlocated partition an error unless the producer id equals this executor's. Cheap interim (**small**): make `execute_dfplan_fragment` refuse a `ShuffleWriteConfig` assignment when `runner.shuffle` is `None`, and make the coordinator reject a `ShufflePartitionOutput` with an empty endpoint when the job has more than one executor.

### A2 (P1) A fully-filtered map task writes partitions with a zero-column schema

`crates/krishiv-executor/src/fragment/batch.rs:678` initialises `output_schema` to `Schema::empty()` and only replaces it from the first **non-empty** batch (`:704-706`). At drain (`:735-739`) a partition with no batches falls back to that schema. A map task whose entire input is filtered out therefore writes `num_partitions` Parquet files carrying no columns.

The reader declares the coordinator's schema regardless — `crates/krishiv-sql/src/distributed_plan.rs:902` wraps the merged stream in `RecordBatchStreamAdapter::new(schema, stream)`, which does **not** validate that yielded batches match. Downstream operators receive a batch whose schema disagrees with the plan.

The `dfplan` path is safe (it uses the executing stream's own schema, `batch.rs:1078`); `execute_shuffle_write_fragment` and `execute_inmem_shuffle_write` are not.

**Fix (small):** carry the fragment's declared output schema (available from the plan/stream before the first batch) into the fallback instead of `Schema::empty()`.

### A3 (P1) A consumer can report only one missing partition per attempt

`crates/krishiv-executor/src/runner/partition.rs:102-116` parses the **first** `KRV_SHUFFLE_MISSING(...)` marker in the error text and `collect_missing_shuffle_partitions` (`:124-147`) returns a single-element vector. `ShuffleReadExec::execute` (`distributed_plan.rs:883-901`) fails on the first read error, so a consumer facing 6 dead producers discovers them one round trip at a time. Each round trip costs a unit of the job-wide regeneration budget — see **C1**.

**Fix (small):** have the reader collect all failures for the partitions it must read before returning, and parse every marker in the text.

### A4 (P2) The missing-partition signal travels as a substring of a stringified error

`ShufflePartitionReader` is `Result<Vec<RecordBatch>, String>` (`distributed_plan.rs`, reader trait), so `batch.rs:906-916` encodes the structured fact as text and `partition.rs:97-112` parses it back out. Every intermediate layer that reformats or truncates the message silently disables shuffle regeneration. The only guard is a string-level unit test.

**Fix (medium):** widen the reader's error type to an enum with a typed `MissingPartition { stage_key, partition }` variant; the marker string then becomes a display detail rather than the protocol.

### A5 (P1) The plan round-trip guard rehearses decode against the wrong session

`verify_dfplan_roundtrip` (`distributed_plan.rs:1115-1121`) is the one contract on this path that is exercised from both sides — and it is a good guard, born from a real revert. But it decodes against `planning_session_context` (`:166-189`), while the executor decodes against `task_sql_engine` (`crates/krishiv-executor/src/fragment/common.rs:975-987`), which has a different SQL dialect, different `target_partitions`, a memory-limited runtime and four extra optimizer rules. Any decode that consults session config passes here and fails there.

**Fix (small):** build the verify context from the same constructor the executor uses.

### A6 (P0) The coordinator plans on a session the engine would never produce

`build_stages_for_parquet_query` (`distributed_plan.rs`) plans on `planning_session_context(target_partitions)` (`:166-189`), which is a bare `SessionContext::new_with_config[_rt]`. It carries **none** of the four rules that `SqlEngine` installs (`crates/krishiv-sql/src/lib.rs:1048, 1056, 1065, 1071` and again at `:1180, 1188, 1194, 1200`), and none of the config that `build_single_node_session_config` sets (`lib.rs:648-700`): batch size, DuckDB dialect, the four `enable_*_dynamic_filter_pushdown` switches.

This is the single highest-leverage finding in the document; its performance consequences are **D1, D3, D4** and its correctness consequence is that mid-execution cancel of a distributed fragment cannot preempt an amplifying operator (`CooperativeAmplifiers` absent — and `crates/krishiv-executor/src/runner/executor_task_runner.rs:840-845` explicitly documents that the cancel watch depends on it).

**Fix (small):** construct the staging planner's context from the same `SessionStateBuilder` as `SqlEngine::build_local`, overriding only `target_partitions` and `enable_round_robin_repartition`. Managed risk: the dialect flips to DuckDB (a *fix* — today a Phase-60 lambda query parses on the engine and fails to stage, silently degrading to one task), and plans change shape. `verify_dfplan_roundtrip` already guards shippability.

### A7 (P2) Heartbeat progress refresh matches tasks by `task_id` across every job

`crates/krishiv-scheduler/src/coordinator/executor_ops.rs:214-236` — `running.contains(task.task_id())` is evaluated over `self.job_coordinators.values()`, i.e. every job. Staged task ids are `dist-sN-tM` (`crates/krishiv-scheduler/src/distributed_batch.rs:153`), which are **not** unique across jobs. Executor A reporting job X's `dist-s0-t0` refreshes the stall clock for job Y's `dist-s0-t0`.

Fails open (the watchdog under-fires), so it is not a P0, but it is an identity bug on the heartbeat→state contract and it means the watchdog cannot be trusted during a concurrent sweep.

**Fix (small):** key the running set on `(job_id, task_id)` on the wire and in the comparison.

### A8 (P1) Executor shuffle GC trusts one heartbeat's live-job set, with no grace period

`crates/krishiv-executor/src/cli.rs:868-900` ticks every 60 s and calls `krishiv_shuffle::orphan::cleanup_orphans(&dir, &active)`. `crates/krishiv-shuffle/src/orphan.rs:29-32` deletes every `.parquet` / `.lease` / `.blake3` under any job directory **not in the set**, immediately, on the first observation. The set is `job_coordinators.keys()` (`crates/krishiv-scheduler/src/coordinator/mod.rs:1653-1658`).

The absence semantics are correctly handled (`cli.rs:880-883`: `None` skips the sweep, pinned on the wire by `crates/krishiv-proto/src/tests.rs:526`). What is *not* handled is a **present but incomplete** set: a coordinator failover, a sharded coordinator, or any window in which a running job is not yet in the map, and the executor deletes a live job's committed shuffle output within 60 s. The consumer then reports it missing, the producer regenerates, and the next tick deletes it again — a deterministic reproduction that ends in "regeneration budget exhausted" naming nothing.

This is the strongest single hypothesis on file for a *permanently* missing partition that survives a successful producer re-run.

**Fix (small):** require K consecutive absences (K≥3, i.e. ~3 min) **and** an mtime older than a grace period before reclaiming a job directory. Log every reclaim at INFO with the job id.

### A9 (P2) The lease-token protocol is inert on the staged path

`crates/krishiv-scheduler/src/distributed_batch.rs:174-177` sets `lease_token: 0` for every staged map task, deliberately. `crates/krishiv-shuffle/src/disk_store.rs:289-350` therefore always resolves to `next == 0 == incoming`, and the two-phase commit check at `:486-491` always passes. The anti-race machinery (BUG-4, B4) exists but never runs on the live path, so it is untested where it matters and would surprise anyone who later introduces speculative execution.

**Fix (small):** derive the token from the attempt id, or document at the write site that the staged path deliberately relies on replace-on-write instead.

### A10 (P2) Producer output metadata is not proven fresh after regeneration

`invalidate_specific_shuffle_partitions` (`crates/krishiv-scheduler/src/job/record.rs:821-866`) resets the producer task to `Pending` but leaves `task.output_metadata` in place, and `launch_assigned_task_assignments` (`:277-305`) reads `task.output_metadata` for **every** task in a stage with no state filter. The stage's `upstream_ready` gate (`:311-319`) is what keeps a consumer from launching against stale locations. That is a correct-but-implicit dependency between two functions 40 lines apart; a future change to the gate reintroduces stale-endpoint fetches with no test to catch it.

**Fix (small):** clear `output_metadata` when a task is reset to `Pending`, and filter the location scan on `TaskState::Succeeded`.

---

## Lens B — Resource bounds on the task data path

| Site | Bounded? | Pool-tracked? | Verdict |
|---|---|---|---|
| Map-side `ShuffleWriteBuffer` | yes (soft ceiling + spill) | **yes** | fixed by 9e762878 — **B8** |
| `write_partition` Parquet writer | streaming, hashed inline | n/a | fixed by a3808b01 |
| `stream_partition` inline read (≤32 MiB) | per-call yes, **aggregate no** | no | **B1** |
| Flight **server** `do_get` concurrency | **no** | no | **B1** |
| `FlightShuffleClient::fetch` → `Vec<RecordBatch>` | count only (8), **not bytes** | no | **B2** |
| `ShuffleReadExec` per-map-task slice | one partition at a time | no | **B3** |
| Local `read_partition` (same-executor) | no | no | **B2** |
| Inline result IPC encode | yes (8 MiB) | no | **B4** — verified safe |
| Result spool + chunk stream | yes (3 MiB chunks) | no | verified safe |
| ESS `SortShuffleWriter::flush` `Vec<u8>` | — | — | **B5** — unreachable |
| `push_store` IPC `Vec<u8>` | — | — | **B5** — unreachable |
| `MemoryBudget` from `memory_limit_bytes` | **None on all batch queries** | — | **B6** — still disarmed |

### B1 (P0) The shuffle Flight **server** is unbounded and untracked — failure 2's shape, on the serve side

`crates/krishiv-shuffle/src/flight.rs:397-456` builds a plain `tonic::transport::Server` with an auth interceptor and **no concurrency limit, no load-shed layer and no byte budget**. `do_get` (`:235-267`) calls `stream_partition`, which for any partition at or below `INLINE_READ_LIMIT` (32 MiB, `crates/krishiv-shuffle/src/disk_store.rs:47`) reads the whole file into a `Vec<u8>` (`:638-651`) and holds it for the lifetime of the response stream.

`SHUFFLE_FETCH_SEMAPHORE` (`crates/krishiv-executor/src/fragment/common.rs:839-847`, default 8) bounds each **consumer**, not the aggregate arriving at one **producer**. On a 3-node cluster the ceiling today is 3 × 8 = 24 concurrent `do_get` against a single executor, i.e. up to **768 MiB of anonymous memory outside the DataFusion pool**, on the same process that is also running map tasks. Raise `KRISHIV_SHUFFLE_FETCH_CONCURRENCY` or add nodes and it scales linearly.

Compounding: both `write_partition` (`disk_store.rs:413`) and `stream_partition` (`:607`) run in `spawn_blocking`, whose default pool is 512 threads — the theoretical worst case is 512 × 32 MiB.

**Sweep failure it causes:** an executor SIGKILLed with anon-RSS above the pool and no `Resources exhausted` in the log — indistinguishable at the symptom level from the bug just fixed, which is exactly how a fixed bug gets re-opened.

**Fix (small):** a server-side permit (or `tower` concurrency-limit layer) around `stream_partition`, sized from `ExecutorCapacity` — e.g. `page_cache_bytes / INLINE_READ_LIMIT` permits, floor 2. Better (**medium**): a byte-semaphore so a mix of 1 MiB and 32 MiB partitions is accounted honestly.

### B2 (P1) A whole shuffle partition is materialised on every read, by trait signature

`crates/krishiv-shuffle/src/flight.rs:613-616` `try_collect()`s the decoded stream into `Vec<RecordBatch>`. The local path does the same (`disk_store.rs:580-596`). Neither is avoidable today because `ShufflePartitionReader::read_partition` returns `BoxFuture<Result<Vec<RecordBatch>, String>>`, so the materialisation is baked into the interface.

Bounded by count (8 concurrent), never by bytes, never registered with the pool. At SF100 the measured average partition is ~3.5 MB (`disk_store.rs:618-621`), so 8 × 3.5 MB is fine — until skew produces one that is not, and skew is exactly what the sweep is trying to find.

**Fix (medium):** change the trait to return a `SendableRecordBatchStream`. `ShuffleReadExec::execute` already flattens per batch (`distributed_plan.rs:898-901`), so the consumer side of the change is three lines; the Flight client keeps its decoder stream instead of collecting it.

### B3 (P1) `ShuffleReadExec` holds one whole map partition per executing root partition

`distributed_plan.rs:883-901`. A task running *k* root partitions concurrently would hold *k* partitions; today *k* is effectively 1 because the root partitions run sequentially (**D2**), which means fixing D2 or D5 without fixing B2 multiplies an untracked peak. Sequence the fixes accordingly.

### B4 (P2, verified safe) The inline result path is bounded

`crates/krishiv-executor/src/runner/task_output.rs:58-80` builds one `Vec<u8>` for the whole result, but `drain_stream_with_spool` (`crates/krishiv-executor/src/runner/result_spool.rs:158`) diverts anything above `inline_result_max_bytes()` (8 MiB default, `:29`) to disk first, and the spool replays in 3 MiB chunks (`:33`). The 1 GiB gRPC ceiling (`crates/krishiv-proto/src/lib.rs:69`) is far above the inline threshold. **No action.** Recorded so the next OOM hunt can skip it.

### B5 (verified unreachable) ESS and push-shuffle are dead on this deployment

`crates/krishiv-executor/src/cli.rs:618-619`, `:629-630`, `:661-662` set `ess_index: None` and `push_store: None` at every construction site. That makes `crates/krishiv-executor/src/fragment/batch.rs:680-690` (SortShuffleWriter), `:760-783` (push-shuffle IPC `Vec<u8>`) and `:815-831` (ESS flush) unreachable, and `SortShuffleWriter::flush`'s single-`Vec<u8>` build with it. **Spend no fix budget here.**

### B6 (P1) `memory_limit_bytes` is still `None` on every batch task — and still arms things

The trap that disarmed failure 2's guard has been routed around for the shuffle buffer, not removed. `crates/krishiv-executor/src/runner/executor_task_runner.rs:817-824` builds **both** `udf_limits.max_memory_bytes` **and** the task's `MemoryBudget` from `assignment.memory_limit_bytes()`, which `crates/krishiv-scheduler/src/job/record.rs:469-471` populates only from the optional `JobSpec` namespace quota. Batch queries set none, so on the sweep every UDF runs with no memory cap and every remaining `MemoryBudget` consumer is `unlimited()`.

**Fix (small):** default `memory_limit_bytes` on batch assignments to the executor's per-slot share (`ExecutorCapacity::min_task_memory_share_bytes`, already used by `spillable_join.rs:88-89`), or delete the field from the batch path entirely so nobody trusts it again. Half-measures here are what produced the "code that would have reported the overflow was disarmed on exactly the deployment that overflowed" line in 9e762878.

### B7 (P2) Unbounded `spawn_blocking` fan-out on the shuffle store

See B1. Independently worth a bounded blocking pool or a semaphore, because the same pool serves writes and reads and a read storm can starve a map task's commit.

### B8 (P2, by design — record it) Two knowingly-unbounded spots in the new write buffer

`crates/krishiv-executor/src/fragment/shuffle_write_buffer.rs:589-605` (`account_unavoidable`) grows the pool past its limit for the one partition that must be resident, and logs. That is the correct trade and is deliberate. Separately, `drain_into_store` (`:210-216`) calls `concat_batches`, which momentarily **doubles** the drained partition while a single reservation covers it. Both are bounded by one partition, so peak is `ceiling + 2 × largest_partition`, not `ceiling + 1 ×`. Note it in the capacity doc so the next OOM investigation does not re-derive it.

*(Working-tree note: `drain_into_store` and its "the loop is `0..num_partitions`, never the partitions that got rows" contract block are the fix agent's uncommitted work. All three map-write paths at `HEAD` already loop `0..num_partitions` — `batch.rs:728`, `:1073`, `:1253` — so the empty-partition skip is **not** in the drain loop. See A8 and C1/C2 for where the q3 partition more plausibly went.)*

---

## Lens C — Retry and recovery loops

### C1 (P0) The regeneration budget counts consumer reports, not rounds, and 8 is below a single-node loss

`crates/krishiv-scheduler/src/job/record.rs:808-816` increments `shuffle_regen_total` **once per matched call**, and `crates/krishiv-scheduler/src/coordinator/job_lifecycle.rs:546-573` fails the whole job at `max_shuffle_regen` = 8 (`crates/krishiv-scheduler/src/config.rs:193`). The counter is cumulative per job and is never decremented on success.

Arithmetic for the sweep: 18 map tasks over 3 executors ≈ 6 producers per node. One executor loss produces up to 6 distinct matched invalidations (the pre-pass at `record.rs:788-806` correctly suppresses duplicates naming an already-reset producer, which is what keeps it from being 18). A **second** loss during the same query — precisely what an OOM-restart storm produces — pushes past 8 and the job is failed as "unrecoverable" while the cluster is healthy.

That is also the arithmetic that best explains eight budget units burned at ~2.5 s intervals: eight *distinct* producers matched in quick succession as consumers discovered a dead endpoint one at a time (**A3**), not one partition regenerating eight times.

**Fix (small):** decay the counter on forward progress — reset `shuffle_regen_total` to 0 whenever a regenerated producer re-reaches `Succeeded` and its consumers advance — or budget per `(stage_key, partition)` rather than per job. The property to preserve is "a *durably* lost partition still fails the job"; a per-partition budget of 2-3 does that strictly better than a per-job budget of 8.

### C2 (P0) Regeneration has no progress guarantee, and its terminal message names nothing

Between attempt *N* and *N+1* the only thing that changes is placement — and placement is not *forced* to change: `invalidate_specific_shuffle_partitions` clears `assigned_executor` (`record.rs:850`) but `preferred_nodes_by_stage` (`:883-910`) will re-pick the same host, since it prefers the node holding the most upstream bytes. If the cause is deterministic — a deleted job directory (**A8**), a location the coordinator never attached (**A1**), a zero-column partition (**A2**) — every attempt reproduces it exactly.

The failure the operator sees is `job {id} lost shuffle output and regenerated it 8 times (limit 8); the producing stage cannot durably retain its output` (`job_lifecycle.rs:559-563`). It names no partition, no producer task, no executor, no endpoint. That message is why failure 3 cost a debugging session.

**Fix (small, highest diagnostic value per line in this document):** on the **second** report of the same `(stage_key, partition)`, short-circuit to a terminal diagnostic error carrying, for both attempts: the producing task id, its assigned executor and that executor's incarnation id, the endpoint the consumer fetched, the producer's full reported `ShufflePartitionOutput` list for that stage key, and whether the consumer took the local or the remote branch. Emit before failing. A second identical failure is proof that retrying is not the answer, so there is nothing to lose by stopping there.

### C3 (P1) The stall watchdog bypasses the retry budget and hard-fails the job

`crates/krishiv-scheduler/src/coordinator/mod.rs:1734-1772` (`apply_stall_resets`) sets `task.state = Failed` directly, then `stage.refresh_state()`. `record.rs:1186-1190` turns any Failed task into `StageState::Failed`, and `JobRecord::refresh_state` turns any Failed stage into `JobState::Failed`. It never increments `failure_count` and never resets to `Pending`, so unlike every other failure path (`record.rs:1063-1131`) there is **no retry**.

It gets worse: once the job record is terminal, `job_lifecycle.rs:434-441` returns `Duplicate` for the executor's own subsequent `Failed`/`Cancelled` report, so the result of the cancel RPC that was just sent is discarded.

The heartbeat-driven `last_progress_ms` refresh (`executor_ops.rs:267-285`) has correctly narrowed *when* this fires — it is no longer a 30-minute hard task timeout — but when it does fire it is maximally destructive.

**Fix (small):** route stall resets through the same failure accounting as any other failure (increment `failure_count`, honour `max_task_attempts`, apply backoff), so a stalled task is retried once before the job dies.

### C4 (P2) Consumer retry budget and producer regeneration budget are unrelated numbers

`MISSING_SHUFFLE_MAX_ATTEMPTS = 30` (`record.rs:27`) against `max_shuffle_regen = 8`. The consumer keeps retrying for ~4× longer than the producer is allowed to be regenerated. Harmless today because `BudgetExhausted` cancels the job, but the two should be derived from one another (`consumer_attempts ≥ regen_budget + slack`) rather than independently chosen.

### C5 (P2) The circuit-breaker fetch-failure exemption is keyed on the wrong signal

`job_lifecycle.rs:482` exempts a failure from the per-executor breaker only when `missing_partitions` is non-empty. `FlightShuffleClient::fetch_with_retry` converts an exhausted *transport* retry to `NotFound` (`crates/krishiv-shuffle/src/flight.rs:681-711`), which is what makes the exemption usually work — but `InvalidInput` (a malformed endpoint) is classified non-retryable (`:550-556`) and passes through unconverted, so a coordinator that attaches a bad endpoint circuit-breaks the innocent consumer's executor. Same class as the Phase-58 wedge the exemption was written for.

**Fix (small):** exempt on "the failure originated in an upstream fetch", not on "a missing-partition report was produced".

### C6 (P2) Executor-loss and stall resets clear `last_progress_ms`

`mod.rs:1764` and `:2071` set it to `None`, so the watchdog falls back to `assigned_at_ms`. Correct on the first reset; a task reset twice in quick succession inherits a clock that was never started. Low impact, one line.

---

## Lens D — Known bottlenecks: verified, sized, scoped

### D1 (P1) q8/q9 shuffle all 600 M lineitem rows because a dynamic filter cannot cross a stage boundary

**Verified, and it is structural rather than a DataFusion-54 misuse.** A DataFusion dynamic filter is a shared `DynamicFilterPhysicalExpr` handle wired between a join's build side and its probe-side scan **inside one plan tree in one process**. `cut_exchanges` (`distributed_plan.rs:1135-1234`) splits that tree at the exchange, and each stage is separately proto-encoded (`encode_dfplan_bytes`, `:1101`) and executed in a different process. The handle cannot survive encoding, so the scan stage decodes a filter with no producer and it stays empty for the whole task — `dist-s4` in both q8 and q9 is the raw `lineitem` scan hash-partitioned on `l_partkey` with no join above it in the same fragment, so all 600 M rows are shuffled (18 tasks × 33 M rows, ~1.9-2.4 GiB of Arrow per task).

Secondary, and separately worth fixing: the coordinator's planning context does not set the four `enable_*_dynamic_filter_pushdown` options at all (**A6**), so they sit at DataFusion defaults and `KRISHIV_RUNTIME_FILTERS=off` is a **no-op on the distributed path** — the AQE/runtime-filter dual-run comparison has been measuring nothing distributed.

**Is Phase 54's runtime-filter machinery connected distributed?** No. `crates/krishiv-sql/src/lib.rs:587-682` is entirely DataFusion's in-process dynamic filter, toggled by config. There is no coordinator-mediated filter at all.

**Scoped fix, Spark's runtime-filter pattern (large):** the coordinator already has every piece it needs. (i) The stage builder recognises "scan stage *S* feeds join stage *J*" — it already computes `upstream_stage_indexes` (`:1262-1277`). (ii) *J*'s build-side stage is scheduled first and reports a min/max plus a bloom summary of the join key as task output metadata — there is already a channel of exactly this shape on the wire (`hot_key_reports`, `crates/krishiv-executor/src/runner/task_output.rs`). (iii) The coordinator injects it as a literal predicate into *S*'s fragment before dispatch; this is free, because `launch_assigned_task_assignments` already gates a stage on its upstreams being `Succeeded` (`record.rs:311-319`), so *S* is dispatched after the summary exists.

**Cheaper interim (medium):** teach `cut_exchanges` not to cut when the join's build side is small enough to broadcast, leaving the scan and the pruning join in one fragment.

### D2 (P1) Intra-task parallelism on the dfplan path is structurally 1

Two independent reasons, both verified:

1. `execute_dfplan_body` (`distributed_plan.rs:493-541`) creates one stream per listed root partition (`:528-537`) and joins them with `futures::stream::iter(streams).flatten()` (`:539`) — strictly sequential.
2. `task_engine_parallelism()` (`crates/krishiv-executor/src/fragment/common.rs:969-971`, from `ExecutorCapacity::task_parallelism`) sets `target_partitions` on the task engine, but the fragment is **decoded, not planned**: its partitioning was fixed by the coordinator at `resolve_stage_target_partitions` (`distributed_plan.rs:118-126`) and `find_unsupported_stage_node` (`:1250-1260`) *guarantees* no `RepartitionExec` survives inside a fragment. So `target_partitions` has no effect whatsoever on a dfplan task.

A 33 M-row `dist-s4` task is therefore single-threaded by construction, and the only lever today is raising `KRISHIV_STAGE_TARGET_PARTITIONS` (default = `slots × TASKS_PER_SLOT`, `TASKS_PER_SLOT = 2`, `:82`) — which buys task-level parallelism at the cost of `partitions²` shuffle fragments.

**Fix A (small):** replace `.flatten()` with `.flatten_unordered(k)` **for map stages only** — the sink is a hash partitioner, so output order is irrelevant, which is the same argument `:525-527` already makes. Must stay sequential for Result stages, whose concatenation order is load-bearing when the plan carries an ordering. Interacts with **B3**: *k* concurrent root partitions means *k* materialised shuffle slices.

**Fix B (medium, better):** have the stage builder emit fragments whose root is a `CoalescePartitionsExec` over *k* partitions, so one task drives *k* cores through a normal DataFusion operator with normal pool accounting, and shrink the task count correspondingly.

### D3 (P1) The q18 spillable-join rule is dead on the distributed path — twice over

1. **It is not registered.** `SpillableJoinSelection` is installed only on `SqlEngine::build_local` (`lib.rs:1056`) and `build_absolute_minimal` (`:1188`). The distributed plan is built by `build_stages_for_parquet_query` on `planning_session_context` (`distributed_plan.rs:166-189`), which registers no physical optimizer rules. Decoding a plan on the executor does not run optimizer rules either.
2. **Even if it were, its gate can never pass.** `spillable_join.rs:123-127` reads `hash_join.left().partition_statistics(None)` and returns `Ok(None)` on `Precision::Absent`. `ShuffleReadExec` implements neither `statistics` nor `partition_statistics` (`distributed_plan.rs:850-903` — no such method), so DataFusion's default `Statistics::new_unknown` applies and **every** shuffle-fed join side is `Absent`.

**Evidence-based conclusion: q18 will fail on the sweep exactly as it did before, and the fix that was written for it cannot fire.**

**Fix (small for (1)):** A6 — plan the staged query on the engine's own state builder. **Fix (medium for (2)):** give `ShuffleReadExec` real statistics. The coordinator already collects `ShufflePartitionOutput.size_bytes` per partition (`record.rs:286-300`) and already uses them for placement (`:893-905`); propagating them into the encoded `ShuffleReadNodePayload` (`:908-916`) as `Precision::Inexact` is a contained wire change. Note that (2) also unlocks better join ordering generally.

### D4 (P1) The q17 aggregate-semijoin fix does not apply to the distributed path either

Same root cause as D3. `SemiJoinReductionThroughAggregate` and `SemiJoinPushdownThroughInnerJoin` are **logical** rules registered via `with_optimizer_rule` on the same two `SqlEngine` constructors (`lib.rs:1065, 1071` and `:1194, 1200`) and absent from `planning_session_context`.

The recorded design (`crates/krishiv-sql/src/semi_join_reduction.rs:1-80`) is sound and its measurement is real — 221.03 s of a 252 s q17, 88 % of all compute, `spill_count=0`. Its own module docs also explain, correctly, why DataFusion's dynamic filter cannot substitute (the min/max spans the key domain and the filter belongs to a join *downstream* of the aggregate). But the rule rewrites the **logical** plan, and the distributed path never builds a logical plan through an engine that has the rule.

**Applies to the distributed path?** The rewrite itself does — a `LeftSemi` join against a filtered probe subtree encodes and stages fine, and would be cut into stages normally. Only the registration is missing.

**Fix (small):** A6. This is the single change that makes D3 and D4 both true at once.

### D5 (P1) `ShuffleReadExec` fetches map tasks strictly sequentially

`distributed_plan.rs:883-897` — `stream::iter(0..num_map_tasks).then(...)`. Map task *m+1*'s fetch does not begin until *m*'s batches have been consumed downstream. With 18 map tasks and a ~200 ms fetch that is ~3.6 s of pure serial latency per reduce partition before any real work, and one slow producer stalls the entire reduce.

**Is overlap safe?** Yes. The slices are concatenated and their union is order-independent — the identical argument the code already makes for root partitions at `:525-527`. Nothing downstream of `ShuffleReadExec` may assume ordering, because `Partitioning::UnknownPartitioning` (`:812`) and `EquivalenceProperties::new` (`:811`) declare none.

**Is it worth it?** Yes, but sequence it. Do it as a bounded **prefetch** (`.buffered(k)`), not `flatten_unordered`, and bound by **bytes** as well as *k* — with B2 unfixed, each in-flight slice is a whole materialised partition, so *k*=4 quadruples an already-untracked peak. Recommend: fix B2 first, then *k*=4 bounded by a byte semaphore shared with the fetch semaphore. **Scope: small after B2, do-not-do before it.**

### D6 (P2) Every task carries a full copy of its stage's plan

`distributed_plan.rs:1123-1125` — `task_bodies = (0..partition_count).map(|p| dfplan_task_body(&b64, p))`. At 163 tasks that is 163 base64 copies of the stage plans through the metadata store, the assignment RPC and every retry. Not a sweep blocker; a contained win with a wire-format bump (one plan per stage + a per-task partition index).

---

## Lens E — The missing test tier

### E1 What exists

| Harness | Processes | Real Flight? | Real disk store? | Coordinator builds locations? |
|---|---|---|---|---|
| `krishiv-runtime/src/in_process_cluster.rs` | 1 | no | no | n/a |
| `krishiv-shuffle/src/flight.rs` tests (`:812-1130`) | 1 | **yes** | yes | no |
| `krishiv-executor/src/fragment/batch.rs` tests (`:2226-2320`) | 1 | **yes** | **yes** | **no** — `remote_endpoints` is hand-built |
| `krishiv-chaos/tests/chaos_suite.rs` | 1 | no | no | no — self-described "invariant simulations" |
| `krishiv-conformance` (corpus, dual-run) | 1 | no | no | n/a |
| `scripts/run_bare_metal.sh` | 2 (coord + **one** executor) | yes | yes | yes |
| `scripts/phase58_chaos.sh` | k8s | yes | yes | yes |

**The gap, stated precisely:** there is no test in which *the coordinator builds the shuffle locations and a different process consumes them*. `run_bare_metal.sh` comes closest and misses by one executor — with a single executor every reported `flight_endpoint` equals `own_endpoint`, so `batch.rs:1002` classifies **every** partition as local and the remote branch is never taken. That is why the remote contract (A1, A3, failure 3) keeps breaking: the only harness that runs real processes structurally cannot reach the code.

The next-cheapest real signal is `phase58_chaos.sh`, which needs a built image and a k8s namespace — hours, and post-image, so it cannot gate a fix.

### E2 Spec: cluster-in-a-box

New dev-only crate `crates/krishiv-cluster-test` (`publish = false`, not in the default workspace test set until it is green), with:

```
crates/krishiv-cluster-test/
  src/lib.rs            Cluster::start(n) / .submit(sql) / .kill(i) / .restart_in_place(i)
                        / .delete_partition(job, stage_key, p) / .shuffle_dir(i)
  tests/cluster_in_a_box.rs   the scenarios below
```

- **Topology.** `Cluster::start(n)` spawns the real `krishiv coordinator` and *n* × `krishiv executor` as **child processes** (reuse the exact argv from `scripts/run_bare_metal.sh:33-60`), each with its own `--shuffle-dir` under a `tempfile::TempDir`, its own Flight port, `--slots 1`. `n = 3` by default so a partition is genuinely remote from two of three consumers. `Drop` kills the tree.
- **Data.** TPC-H SF0.01 generated once into Parquet by the existing `krishiv-bench` corpus code, cached under `target/cluster-test-data/` and reused.
- **Driver.** Submit through the **real** client path (the coordinator's batch-SQL endpoint / `krishiv sql`), never `InProcessCluster`. This is the load-bearing rule: the point is the wire.
- **Cheap nasty regimes, all env, no code changes:**
  `KRISHIV_STAGE_TARGET_PARTITIONS=4` · `KRISHIV_SHUFFLE_SPILL_THRESHOLD_BYTES=1024` · a tiny memory limit (so the pool is ~a few MB) · `KRISHIV_SHUFFLE_FETCH_CONCURRENCY=1` · `KRISHIV_MAX_SHUFFLE_REGEN=2` · `KRISHIV_INLINE_RESULT_MAX_BYTES=1024` · `KRISHIV_BATCH_SIZE=64`.
- **Faults:**
  1. `SIGKILL` one executor mid-stage, then restart it **in place** with the same executor id and endpoints (this is the incarnation fence; endpoint-change fencing must **not** be what saves it).
  2. Delete one committed partition file from an executor's shuffle dir between stages.
  3. Start one executor with no shuffle Flight address (exercises A1).
  4. Run the GC tick with a deliberately incomplete live-job set (exercises A8).
- **Assertions.** Result equality against the embedded single-process answer for the same query (values, not repr — the harness bug from 2026-07-26); plus, per fault, the *specific* recovery signal: a regeneration occurred, the job still succeeded, and the diagnostic named the partition. Plus a standing assertion on every query that `task_count > 1` — the silent single-task degradation is the failure with no other symptom.
- **Budget.** 3 processes × SF0.01 × ~6 queries, well under 2 minutes. Gate it in `security-durability-gate.yml` alongside the existing jobs.

### E3 Which recorded failures it would have caught

| Failure | Caught? | Why |
|---|---|---|
| 1. Page cache charged to cgroup | **No** — needs a cgroup limit. Add an optional cgroup-v2-constrained variant if the CI runner allows it; otherwise explicitly out of scope, and say so. |
| 2. Unbounded map-side shuffle buffer | **Yes.** With a ~MB pool and a 1 KB spill threshold, the pre-fix buffer is invisible to the pool: the assertion "peak buffered bytes are visible to the pool" fails, and with a genuinely tiny pool the process dies. This is the assertion 9e762878 correctly identifies as load-bearing, run against the real process. |
| 3. Missing shuffle partition → regeneration exhausted | **Yes, directly.** Fault 2 is exactly this scenario, and `KRISHIV_MAX_SHUFFLE_REGEN=2` makes it fail in seconds. Fault 4 would have caught A8 as a distinct cause. |
| 4. In-place OOM restart without incarnation recovery | **Yes.** Fault 1 is the fault that fix was written for; assert no phantom `Running` tasks and that the job completes without waiting 30 minutes. |
| 5. Wrong SessionContext in the decode guard / session-wide spillable join | **Partially.** The standing `task_count > 1` assertion catches the single-task degradation that went unnoticed for a whole session. The q2-shaped timeout is caught only if the corpus includes a multi-join query **and** the test asserts a wall-clock ceiling — recommend both. |

### E4 One additional test, cheaper than all of the above

A plain in-process `#[test]` in `krishiv-sql` asserting that the staged planner's context carries the **same optimizer rule set** as `SqlEngine` (compare rule names from both `SessionState`s). That single assertion is what would have caught A6/D3/D4 — three P1s and the reason two shipped performance fixes do not apply to the path being benchmarked — and it needs no processes at all.

---

## Recommended fix order: "all 22 green, then fast"

Sequenced so that each batch is independently verifiable and no fix multiplies an unbounded quantity introduced by an earlier one.

**Batch 0 — make the next failure legible (do first, ~half a day, no behaviour change).**
1. **C2** — second-identical-report short-circuit with a full diagnostic. *Everything below is easier to verify once this exists.*
2. **E4** — the optimizer-rule-parity unit test.
3. **A5** — verify the round-trip against the executor's own engine.

**Batch 1 — stop the job from failing while the cluster is healthy (P0s that block "all 22 green").**
4. **C1** — decay or re-scope the regeneration budget.
5. **C3** — route stall resets through the retry budget.
6. **A8** — grace period + K-consecutive-absence before shuffle GC reclaims a job dir.
7. **A1** — refuse a shuffle-write assignment with no Flight endpoint; error rather than silently read a locationless partition as empty.
8. **B1** — bound the Flight server's concurrent `do_get` from `ExecutorCapacity`.

**Batch 2 — build the harness, then re-run Batch 0-1 under it.**
9. **E2** — `krishiv-cluster-test`, with faults 1-4. Confirm each Batch-1 fix against the fault it was written for **before** the sweep, not after.

**Batch 3 — make the sweep's known-failing queries pass (P1 correctness-of-outcome).**
10. **A6** — plan staged queries on the engine's own `SessionStateBuilder`. This single change delivers **D3(1)**, **D4** (q17's 88 %), the q18 predicate reorder, `CooperativeAmplifiers` (so distributed cancel works), and makes `KRISHIV_RUNTIME_FILTERS` mean something distributed. Verify with the E2 harness plus a full local corpus re-plan (`krishiv-bench`'s `stage_dump` already replays the production stage builder — extend it to diff plans before/after).
11. **D3(2)** — propagate `size_bytes` into `ShuffleReadExec` statistics so the spillable-join gate can actually fire on q18.
12. **A2** — real fallback schema for a fully-filtered map task.
13. **B6** — arm or delete `memory_limit_bytes` on the batch path.

**Batch 4 — then fast.**
14. **B2** — stream shuffle reads instead of materialising `Vec<RecordBatch>` (prerequisite for 15 and 16).
15. **D5** — bounded byte-aware prefetch across map tasks in `ShuffleReadExec`.
16. **D2** — intra-task parallelism: `flatten_unordered(k)` for map stages first (small), `CoalescePartitionsExec` fragments after (medium).
17. **D1** — coordinator-mediated runtime filters for q8/q9. Large; do it last and behind a flag, with the dual-run corpus comparison as the correctness gate — and note that the dual-run switch only became meaningful at step 10.

**Deliberately not scheduled:** B5 (ESS / push-shuffle — confirmed unreachable), A9 (inert lease tokens — document instead), D6 (plan duplication — real but not on any critical path).

---

## Addendum 2026-07-27 — the storage substrate invalidates every timing in this document

Measured after the register was written, on the same 3-node cluster the
sweep runs on:

| path | storageclass | measured (`dd`, `iflag/oflag=direct`) |
|---|---|---|
| `minio-0` `/data` — **the sweep's data source** | **longhorn** | **2.4 MB/s read · 2.9 MB/s write** |
| `minio-bench-s1` `/data` | local-path | 325 MB/s write |
| `minio-bench-s3` `/data` | local-path | 298 MB/s write |

The Longhorn volume reported `robustness: healthy` and `rebuildStatus:
null` at the time — this is steady state, not a rebuild artifact. There was
concurrent mirror read load, so the idle figure is somewhat better; the
order of magnitude is the finding.

`minio-0` serves every executor scan, and its read path is
executor → S3 API over the pod network → MinIO → **Longhorn engine →
network → replica on another node**. Two network hops per byte, through a
replicated block device chosen for durability, over three separate VPS
hosts.

### What this invalidates

Every **elapsed-time** number cited anywhere in this register or in the
sweep records is dominated by storage I/O, not by the engine. 39 GiB at
single-digit MB/s is hours of pure I/O before any operator runs. Concretely:

- "q17: 221 s of a 252 s query" (D4) and "88 % of all compute" — the
  *proportion* survives (it came from operator-level accounting) but the
  absolute sizing does not.
- D1's "the single biggest sweep-time win" — the 600 M-row shuffle is a
  real ~2 GiB/task of network traffic and remains a genuine bottleneck,
  but its rank was inflated by a storage-bound baseline.
- D5's "3.6 s of serial latency per reduce partition" — noise against a
  1000 s query, material against the ~10x faster query this becomes.

### What this does **not** invalidate

Lenses A, B and C in full, and the *structural* claims of Lens D. None of
those were derived from a stopwatch: they are statements about what the
code does — an unregistered optimizer rule, a budget counted per job, a
partition that reads as empty, a `.flatten()` that is sequential by
construction. A faster disk changes none of them.

### Revised Batch 4 order

Storage was the binding constraint, so the *next* constraint has never been
observed. On first principles it is CPU, which promotes **D2**:

14. **D2** — intra-task parallelism (was 16). A `dist-s4` task is
    single-threaded by construction; with a 100x faster scan feeding it,
    one core per 33 M-row task becomes the binding limit. Fix A
    (`flatten_unordered(k)`, map stages only) is *small* and should be
    measured first.
15. **B2** — streaming shuffle reads (prerequisite for 16).
16. **D5** — bounded byte-aware prefetch.
17. **D1** — coordinator-mediated runtime filters. Unchanged position:
    still large, still last, still behind a flag.

### Standing rule this establishes

**No performance conclusion may be drawn from a cluster sweep until the
dataset is on local-path storage and the run is re-baselined.** The
regression budgets in Phase 66 must be rebuilt on that baseline; the
existing SF100 history is a record of Longhorn's throughput and should be
labelled as such rather than deleted.

### Correction to the addendum above — the network, not the disk, is the floor

The disk measurement above was real but it was not the binding constraint.
Measured back-to-back on the same link, same load (200 MiB, `nc`, s3 → s1):

| path | measured | vs local disk |
|---|---|---|
| node local disk (`local-path`) | ~300 MB/s | 1x |
| **host network** (bypasses the CNI) | **40 MiB/s** | 7x slower |
| **pod network** (flannel VXLAN — *what the engine actually uses*) | **11 MiB/s** | **27x slower** |

Pod MTU is 1450, correct for VXLAN over a 1500 host — this is not the
classic MTU-fragmentation bug. The ~3.6x overlay penalty is encapsulation
cost (these are three separate VPS hosts; flannel backend is `vxlan`,
`host-gw` is unavailable because they are not L2-adjacent).

**Every byte the engine shuffles crosses the pod network at ~11 MiB/s.**

### What that does to the numbers in this register

- q8/q9 shuffle ~36 GiB of Arrow (18 tasks x ~2 GiB). At 11 MiB/s that is
  **~55 minutes of pure network time**, floor, perfectly pipelined. The
  "no progress for 30 min" watchdog fires *during honest work*. The
  phantom-task bug (`eaec83d8`) was real and worth fixing, but it was
  never the whole story — this alone reproduces the symptom.
- A full `lineitem` scan from central MinIO is 39 GiB over the same
  network: ~1 hour before the first operator does anything useful.

### Revised Batch 4 order — again, and this one is measurement-backed

The previous revision promoted D2 (intra-task parallelism) on the
first-principles guess that CPU was next after storage. That guess was
wrong: **network is next, by a wide margin.** Bytes-on-the-wire beats
cores.

14. **D1** — coordinator-mediated runtime filters / broadcast-instead-of-cut.
    Was last in both prior orderings; it is now first, because it is the
    only fix that *removes bytes from the wire* rather than moving them
    faster. Every 1 GiB not shuffled is ~90 seconds returned.
15. **B2** — streaming shuffle reads (still a prerequisite for 16, and at
    11 MiB/s the materialise-then-consume pattern holds buffers ~27x
    longer than a local-disk mental model assumes).
16. **D5** — bounded byte-aware prefetch. Overlapping the wire matters
    *more* when the wire is the bottleneck, not less.
17. **D2** — intra-task parallelism. Still real, still small, but a
    second core cannot help a task waiting on an 11 MiB/s fetch. Measure
    it on scan/aggregate-heavy queries (q1, q6), not shuffle-heavy ones.

### Infrastructure item, outside this register's scope

The overlay costs ~3.6x and the node-local storage work now underway
removes the *scan* traffic from it, but not the *shuffle* traffic. Whether
the VXLAN penalty is recoverable (checksum-offload workarounds on virtio
NICs are a documented k3s/flannel issue with this exact signature) is worth
one careful experiment — but changing CNI settings on the production
cluster risks the cross-node routing outage this cluster already suffered
once (2026-07-22). **Not to be attempted unattended.**

### Honesty consequence for Phase 62 / Phase 66

This cluster is three VPS hosts on a ~90 Mbit/s effective pod network. It
is an excellent correctness and fault-tolerance testbed and a poor
throughput testbed. Distributed SF100 numbers measured here characterise
the link, not the engine, and must be published with the topology stated
or not published at all. Engine *compute* claims belong on the single-node
path until a better-connected cluster exists.
