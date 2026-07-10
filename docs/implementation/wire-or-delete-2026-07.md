# Wire-or-Delete Disposition Register — Phase 51 (2026-07-10)

Phase 51 exit-gate requirement: every built-but-unwired subsystem carries a
recorded decision — *wire in a named Track 6 phase* or *delete now*. Evidence
base: `production-readiness-audit-2026-07.md` (§3, §4, §7c, §8b, §9b).
"Claimed by" cites the platform phase document that owns the wiring work.

## Deleted (this phase, commit recorded below)

| Subsystem | Location | Rationale |
|---|---|---|
| Operator fusion (`FusedPipeline`/`FusedStage`) | `krishiv-dataflow/src/fusion.rs` (323 LOC) | Zero users in or out of crate (audit §9b). No Track 6 phase claims operator fusion — Phase 55's latency plan is batch/linger dials + barrier-epoch snapshots, not fusion. Recoverable from git history if a later phase wants a seed. |
| IVM disk-spill knob (`enable_disk_spill` + `SnapshotStore`) | `krishiv-ivm/src/flow.rs`, `snapshot_store.rs` (171 LOC) | Worse than parked — a **lying knob**: `inner.snapshot_store` was written but never read anywhere, so calling it silently did nothing. The tick ctx is spill-configured by default (`spill.rs` FairSpillPool); larger-than-RAM IVM state is Phase 57's executor-resident design, which does not build on this store. |

## Keep — wiring claimed by a named phase

| Subsystem | Location | Claimed by | Claim |
|---|---|---|---|
| `LocalityScheduler` | `krishiv-scheduler/src/job/scheduler.rs` (`cfg(test)`) | **Phase 53** | "promote `LocalityScheduler`" — locality-aware placement from scan splits + shuffle outputs |
| `FairScheduler` | same (`cfg(test)`) | **Phase 53** | "Fair pools GA: finish `FairScheduler`" |
| `key_group_range_for_task` | same (`cfg(test)`) | **Phase 55** (+64) | "Key-group sharding live: promote `key_group_range_for_task`" |
| `ExecutorPlacement::with_locality` | same (`pub(crate)`, test-only caller) | **Phase 53** | consumed by the LocalityScheduler promotion |
| Incremental checkpoints | `krishiv-state/src/incremental_checkpoint.rs`, `incremental_trace.rs` | **Phase 56** | "SST deltas (`incremental_checkpoint.rs` + `incremental_trace.rs`)" |
| `register_lateness` (lateness GC) | `krishiv-ivm/src/flow.rs:453` | **Phase 57** | "**activate `register_lateness`**" — retention task |
| Barrier dispatch consumer | `executor/runner/executor_task_runner.rs::drain_pending_barriers` (CLI-only caller) | **Phase 55** | "give `drain_pending_barriers` its live caller in the continuous task loop" |
| Push shuffle store (client half) | `krishiv-shuffle/src/push_shuffle.rs`; `task_output.rs::push_store` is never `Some` | **Phase 52** | "Shuffle wiring: map-side hash partition + sort-shuffle write to the local/ESS store". Server routes (`/ess/push*`) are already registered in `shuffle_svc.rs`; only the map-side writer is parked. |
| `UnifiedMemoryManager` Execution/State regions | `krishiv-common` (Shuffle region live via `spillable.rs`) | **Phase 56** | "one executor-wide arbiter" — audit §7c |
| Early-fire stub (`KRISHIV_STREAM_EARLY_FIRE_MS`) | `emit_open_windows_speculative` returns `None` for production operators | **Phase 55** | explicit wire-or-delete line item in the Phase 55 doc |
| `kafka_transactional_sink` (`RdkafkaTransactionalSink`) | `krishiv-connectors`, re-export only | **Phase 55** | sink-descriptor task: "transactional Kafka egress sink already exists unused" |
| `dataflow/profile.rs` (`StreamingExecutionProfile::low_latency`) | `krishiv-dataflow` | **Phase 55(e)** | the exact batch/linger dial 55(e) specifies. **Wiring must first resolve the name collision** with the wired `krishiv_proto::StreamingExecutionProfile`. |
| `dataflow/delta_join.rs` (`DeltaJoinOperator`) | `krishiv-dataflow` | **Phase 64** | "evaluate the RisingWave delta-join form" — seed implementation exists |

## Audit corrections (believed parked, actually wired)

- **`tiered_store.rs`** — reachable in production: `executor/cli.rs:517`
  calls `open_tiered_shuffle_backend` for tiered shuffle URIs. The audit's
  "no callers outside the crate" missed the executor CLI. No action.
- **`lease_persistence.rs`** — live internal dependency of the disk/object
  store lease path (`disk_store.rs`, `object_store.rs` encode/decode/enforce
  monotonic lease tokens on every partition write). No action.

## Rule going forward

A `cfg(test)`/dead-code-marked subsystem may only exist if this register (or
a successor) names the phase that wires it. Anything else is delete-on-sight
at the next audit.
