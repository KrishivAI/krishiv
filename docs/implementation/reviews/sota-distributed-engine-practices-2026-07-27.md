# State-of-the-art reference: how mature engines solve our sweep-failure classes

Companion to `distributed-batch-review-2026-07-27.md`. Each section: the
failure class we hit live on the SF100 sweep, how state-of-the-art systems
solve it, and the adopt/adapt recommendation for Krishiv. Sources are the
systems' shipped designs (Spark 3.x/4.x, Trino, Flink 1.x/2.0, Velox,
DuckDB, FoundationDB/TiKV test practice), stated from their documented
architectures; verify exact APIs against current upstream before copying.

## 1. Shuffle contracts: metadata-driven, never convention-driven

**Our failure:** the "map tasks write every partition, including empty
ones" contract lived in a comment; a new writer skipped empty partitions
and remote consumers saw hard errors; 8 blind regeneration attempts
reproduced it deterministically.

**SOTA (Spark):** a map task's completion is inseparable from a
**MapStatus** — a per-partition size vector reported to the driver's
MapOutputTracker. Consumers ask the tracker what exists BEFORE fetching:
a zero-size partition is *known empty and never fetched*; a missing
MapStatus means the map task didn't finish. There is no convention to
violate — the metadata IS the contract, and an inconsistent writer is
caught at report time, not at consumer-fetch time. Trino similarly tracks
per-task output metadata centrally; Flink's result-partition registry
plays the same role.

**Adopt:** make `ShufflePartitionOutput` (we already report per-partition
`real_bytes` for AQE) the authoritative existence map: consumers consult
the coordinator-provided sizes in the assignment and skip zero-byte
partitions instead of fetching them; the store treats "serve a partition
the writer reported as nonzero but cannot find" as a distinct, loudly
diagnosed error from "fetch of something never reported". This eliminates
the empty-partition error class structurally and gives failure 3's loop a
precise diagnosis for free.

## 2. Memory: one accounting authority, reservation before allocation

**Our failure:** 4.5 GiB anon outside the pool (write buffers guarded by a
budget that was unlimited on every batch query); before that, page cache.

**SOTA:** DuckDB routes effectively *all* query memory through its buffer
manager. Velox requires every operator allocation to go through a
MemoryPool with reservation + spill hooks — third-party buffers included.
Spark's UnifiedMemoryManager taught the same lesson the hard way: any
buffer that bypasses the manager becomes an OOM class of its own
(Tungsten moved aggressively to managed/off-heap pages for this reason).
The rule everywhere: **a bounded system has exactly one budget authority,
and allocation without reservation is a bug by policy, not a judgment
call.**

**Adopt:** the review's lens-B inventory becomes a policy: every buffer on
the task data path either reserves from the DataFusion MemoryPool or
carries a written justification. Add a debug assertion mode (test-only)
where task-path allocations above N MB outside the pool panic — the
cluster-in-a-box harness runs with it on.

## 3. Recovery loops: attribute the failure, change something, or stop

**Our failures:** 8 regeneration attempts reproducing one missing
partition; 30-minute watchdog cycles re-dispatching a task onto the same
OOM.

**SOTA (Spark):** a FetchFailure is *attributed* — it names the map
output and epoch; the scheduler unregisters exactly that output,
re-runs exactly that producer, and counts stage attempts with a small
limit; repeated failures on one executor feed a blacklist/exclusion
mechanism so the retry lands somewhere else (something CHANGES between
attempts). Flink's regional failover restarts the minimal pipelined
region, with restart strategies that are budgeted and observable.
Nobody retries blind: the invariant is "every retry either changes an
input condition (placement, regenerated data, excluded node) or reduces a
budget with a diagnosis attached."

**Adopt:** (a) shuffle regeneration keys its budget per (stage, partition)
and, on the SECOND identical miss with no intervening executor loss,
fails fast with producer task id, partition, store path, and the
writer-reported size (which, with §1, pinpoints writer-vs-store
immediately); (b) task retries after OOMKill prefer a different executor
(we have consecutive_task_failures per executor already — use it as a
soft exclusion signal).

## 4. Runtime filters across stages: ship the filter, not 600M rows

**Our bottleneck:** q8/q9's dist-s4 scans lineitem with EMPTY dynamic
filters (the populating join is downstream) and shuffles all 600M rows.

**SOTA (Trino):** dynamic filtering is a first-class distributed
mechanism — build-side values (or min/max/bloom summaries) are collected
at runtime, sent to the coordinator, and *distributed to probe-side scan
operators on other workers*, which then prune splits/rows. Spark does the
static-shape version as Dynamic Partition Pruning and the adaptive
version through AQE stage reordering (materialize the small side first,
reuse its output as a subquery filter). The essential design point: the
filter must cross the stage boundary through the control plane, because
the data plane ordering guarantees it arrives too late.

**Adapt:** our Phase 54 runtime-filter machinery exists but (per memory
and the q17 finding) lands downstream on the distributed path. The fix
shape is Trino's: when the stage builder cuts a join whose build side is
small/selective, schedule the build stage first, collect min-max/bloom on
the join keys at the coordinator (we already collect per-stage AQE
stats), and inject the summary as a scan predicate into the probe-side
stage's fragment before dispatch. q8/q9's 600M-row shuffle becomes a
~few-M-row shuffle; q17's design in memory is the same family.

## 5. Liveness vs progress: attributed heartbeats, unique incarnations

**Our failure:** in-place OOM restart re-registered as the same executor;
Running tasks became phantoms recoverable only by a 30-minute watchdog.

**SOTA:** Spark never reuses an executor identity — a restarted executor
is a NEW executor id by construction, so stale state attribution is
impossible; lost-executor handling is immediate on the scheduler side.
Flink pairs TaskManager registration with monotonic resource/session ids
and fences stale generations. Our incarnation-id fix (eaec83d8) is the
same design; keep the invariant "identity ≠ endpoint" permanent.

## 6. The test tier that catches wire bugs: real processes, forced regimes

**Our meta-failure:** five for five, regressions lived between processes.

**SOTA practice:** Spark's own suite runs `local-cluster` mode — real
separate executor JVMs on localhost — precisely because local-threaded
mode hides serialization, classpath, and shuffle-wire bugs. Flink ships
MiniCluster with real RPC. The gold standard is FoundationDB's
deterministic simulation (whole-cluster in one process with injected
faults), approximated in Rust practice by TiKV's **failpoint injection**
(`fail` crate): production code carries named failpoints that tests
activate to force rare regimes — spill-now, drop-this-rpc, die-here.

**Adopt (the harness the review is spec'ing):** multi-PROCESS
cluster-in-a-box on localhost with real Flight shuffle + real disk store,
tiny SF, plus `fail`-style failpoints for: force spill (threshold ~1KB),
empty a partition, SIGKILL an executor mid-stage (exercises the
incarnation fence), drop one fetch (exercises attributed regeneration).
Minutes to run; gates every image build. Would have caught failures
1, 2, 3, and 4 before they reached the cluster.

## 7. Fetch pipeline: bound by bytes, overlap the wire

**Our shape:** ShuffleReadExec fetches map slices sequentially, each
fully materialized; concurrency capped by count (8), bytes unbounded.

**SOTA (Spark's ShuffleBlockFetcherIterator):** in-flight fetches are
bounded by BYTES (`maxBytesInFlight`, default 48MB) and per-address
request caps, with results streamed to the consumer as they arrive and
oversized blocks diverted to disk. Prefetch overlaps network and compute
without ever holding more than the byte budget.

**Adapt:** per-reduce-task byte-budgeted prefetch (next slice fetches
while current drains; budget reserved from the memory pool per §2),
which also removes the one-slow-mapper serialization stall.

## Priority mapping (feeds the review's fix order)

1. §1 metadata-driven shuffle existence (kills the active regression class
   AND gives diagnosable recovery) — small/medium.
2. §6 harness with failpoints — medium, highest regression-prevention ROI.
3. §3 attributed, progress-guaranteed recovery — small once §1 lands.
4. §4 cross-stage runtime filters — medium/large; the single biggest
   sweep-time win (q8/q9/q17 family).
5. §7 byte-budgeted overlapped fetch — small/medium; steady-state perf.
6. §2 policy + debug assertion — small; locks the class shut.
