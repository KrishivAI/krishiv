#![forbid(unsafe_code)]

//! IVM job registry for the coordinator.
//!
//! Each IVM job is a long-lived flow held in-process. A job is either a single
//! [`IncrementalFlow`] or, when its first view is a key-shardable aggregate, an
//! auto-partitioned [`PartitionedIncrementalFlow`] — decided transparently at
//! view-registration time (see [`IvmJobRegistry::register_view`]).
//!
//! The coordinator's flow is the **single source of truth for every mode**
//! (embedded, single-node, distributed), which keeps executors replaceable.
//! For distributed mode with live executors, single-flow ticks are offloaded to
//! an executor: the coordinator drains pending locally, ships a full state
//! snapshot (`checkpoint_full`), and applies the returned view outputs via
//! `apply_computed_tick`; on any failure it re-feeds pending and computes
//! centrally. Partitioned jobs always compute centrally (shards already run in
//! parallel in-process).

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use krishiv_ivm::{
    DeltaBatch, IncrementalFlow, IncrementalViewSpec, IvmError, IvmResult,
    PartitionedIncrementalFlow, StepSummary, partition_key_from_sql,
};
use serde::{Deserialize, Serialize};

const IVM_DURABLE_SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct PersistedIvmViewSpec {
    name: String,
    body_sql: String,
    #[serde(with = "byte_blob")]
    output_schema_ipc: Vec<u8>,
    is_materialized: bool,
    is_recursive: bool,
    lateness: Vec<krishiv_ivm::LatenessSpec>,
}

#[derive(Debug, Serialize, Deserialize)]
enum PersistedIvmShape {
    Single,
    Partitioned { shards: usize, key_column: String },
}

/// Serialize `Vec<u8>` as base64 inside the JSON snapshot, accepting either
/// base64 or the old array-of-numbers form on the way back in.
///
/// IVM-AUD-DIST-G2: `checkpoint_full` and `output_schema_ipc` are raw bytes
/// with no byte-aware attribute, so `serde_json` wrote them as
/// `[137,80,78,71,…]` — roughly 4 bytes of JSON per byte of state, applied to
/// the *entire* flow snapshot on every persist. Base64 is ~1.37x instead.
/// Deserialization stays permissive so snapshots already on disk still load;
/// that is also why this is not a `version` bump (`restore_durable_snapshot`
/// rejects unknown versions, which would make every persisted job unloadable).
mod byte_blob {
    use serde::de::{SeqAccess, Visitor};
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        use base64::Engine as _;
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Vec<u8>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a base64 string or an array of byte values")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Vec<u8>, E> {
                use base64::Engine as _;
                base64::engine::general_purpose::STANDARD
                    .decode(v)
                    .map_err(E::custom)
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
                let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(b) = seq.next_element::<u8>()? {
                    out.push(b);
                }
                Ok(out)
            }
        }
        d.deserialize_any(V)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedIvmJob {
    version: u32,
    shape: PersistedIvmShape,
    views: Vec<PersistedIvmViewSpec>,
    #[serde(with = "byte_blob")]
    checkpoint_full: Vec<u8>,
    /// Whether the job was created pinned to a single flow
    /// ([`IvmJobRegistry::create_unpartitioned`]).
    ///
    /// `shape` alone cannot carry this. A job created unpartitioned but with no
    /// views yet persists as `Single` — indistinguishable from an ordinary job
    /// that simply has not registered a view. Rehydrate that without the pin
    /// and the client's first `GROUP BY` view auto-partitions a job it
    /// explicitly asked to keep single, silently breaking the view-DAG
    /// composition the pin exists for.
    ///
    /// Added under **version 1 with `serde(default)`**, deliberately not a
    /// version bump: `restore_durable_snapshot` rejects any version it does not
    /// recognise, so bumping would make every already-persisted IVM job
    /// unloadable on upgrade. Old snapshots default to `false`, which is
    /// exactly the behaviour they have today.
    #[serde(default)]
    pinned_single: bool,
    /// Whether the job had delta-checkpoint accumulation switched on
    /// ([`IvmJobRegistry::enable_delta_checkpoints`]).
    ///
    /// IVM-AUD-DIST-C1: the flag lives inside the flow and `checkpoint_full`
    /// does not carry it, so a rehydrated job silently stopped accumulating —
    /// the next `/checkpoint-delta` would answer an empty frame and the
    /// caller's incremental-backup chain would have a hole in it with nothing
    /// said.
    ///
    /// Added under **version 1 with `serde(default)`** for the same reason as
    /// `pinned_single`: `restore_durable_snapshot` rejects unknown versions, so
    /// a bump would make every already-persisted IVM job unloadable.
    #[serde(default)]
    delta_checkpoints: bool,
}

/// A coordinator-hosted IVM job: a single flow, or one auto-partitioned by key.
///
/// Both variants hold `Arc`s, so cloning is cheap and the handle can be passed
/// to async HTTP handlers. The enum exposes the full flow surface the IVM HTTP
/// API needs; `match self` dispatches to the right backing flow.
#[derive(Clone)]
pub enum IvmJob {
    /// Unpartitioned — the default, and the only shape for non-shardable views.
    Single(Arc<IncrementalFlow>),
    /// Key-partitioned across shards (single-column `GROUP BY` aggregates).
    Partitioned(Arc<PartitionedIncrementalFlow>),
}

impl IvmJob {
    /// True when this job is auto-partitioned.
    pub fn is_partitioned(&self) -> bool {
        matches!(self, IvmJob::Partitioned(_))
    }

    /// Names of the views registered on this job (either variant).
    pub fn view_names(&self) -> Vec<String> {
        match self {
            IvmJob::Single(flow) => flow.view_names().unwrap_or_default(),
            IvmJob::Partitioned(part) => part
                .view_specs()
                .map(|specs| specs.into_iter().map(|s| s.name).collect())
                .unwrap_or_default(),
        }
    }

    /// Register a view on the job. (Partitioning is decided by the registry
    /// *before* the job reaches this variant; here we just register.)
    pub fn register_view(&self, spec: IncrementalViewSpec) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.register_view(spec),
            IvmJob::Partitioned(p) => p.register_view(spec),
        }
    }

    /// Drop a view. Returns `true` if it existed.
    pub fn drop_view(&self, name: &str) -> IvmResult<bool> {
        match self {
            IvmJob::Single(f) => f.drop_view(name),
            IvmJob::Partitioned(p) => p.drop_view(name),
        }
    }

    /// Feed a `DeltaBatch` for a source (routed to its shard when partitioned).
    pub fn feed(&self, source: &str, delta: DeltaBatch) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.feed(source, delta),
            IvmJob::Partitioned(p) => p.feed(source, delta),
        }
    }

    /// Feed a full streaming snapshot, differentiated against the previous one.
    pub fn feed_snapshot(&self, source: &str, batches: &[RecordBatch]) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.feed_snapshot(source, batches),
            IvmJob::Partitioned(p) => p.feed_snapshot(source, batches),
        }
    }

    /// Advance one tick (shards step in parallel when partitioned).
    pub async fn step_datafusion(&self) -> IvmResult<StepSummary> {
        match self {
            IvmJob::Single(f) => f.step_datafusion().await,
            IvmJob::Partitioned(p) => p.step_datafusion().await,
        }
    }

    /// Queued input bytes not yet consumed by a tick (IVM-AUD-INT-F11).
    pub fn pending_bytes(&self) -> IvmResult<usize> {
        match self {
            IvmJob::Single(f) => f.pending_bytes(),
            IvmJob::Partitioned(p) => p.pending_bytes(),
        }
    }

    /// Current tick count.
    pub fn tick(&self) -> IvmResult<u64> {
        match self {
            IvmJob::Single(f) => f.tick(),
            IvmJob::Partitioned(p) => p.tick(),
        }
    }

    /// Read a source/view snapshot from the per-source map (the `/snap` surface).
    pub fn source_snapshot(&self, name: &str) -> IvmResult<Option<RecordBatch>> {
        match self {
            IvmJob::Single(f) => f.source_snapshot(name),
            IvmJob::Partitioned(p) => p.source_snapshot(name),
        }
    }

    /// Read a view's materialized snapshot (concatenated across shards).
    pub fn snapshot(&self, view: &str) -> IvmResult<Option<RecordBatch>> {
        match self {
            IvmJob::Single(f) => f.snapshot(view),
            IvmJob::Partitioned(p) => p.snapshot(view),
        }
    }

    /// Whether `view` was registered as materialized — see
    /// [`IncrementalFlow::view_is_materialized`]. A partitioned job reports the
    /// shard-0 answer; every shard registers the same spec.
    pub fn view_is_materialized(&self, view: &str) -> bool {
        match self {
            IvmJob::Single(f) => f.view_is_materialized(view),
            IvmJob::Partitioned(p) => p.view_is_materialized(view),
        }
    }

    /// Return the spec for a named view (`None` if not registered).
    pub fn view_spec(&self, view: &str) -> IvmResult<Option<IncrementalViewSpec>> {
        match self {
            IvmJob::Single(f) => f.view_spec(view),
            IvmJob::Partitioned(p) => p.view_spec(view),
        }
    }

    /// Return every registered view specification.
    pub fn view_specs(&self) -> IvmResult<Vec<IncrementalViewSpec>> {
        match self {
            IvmJob::Single(f) => f.view_specs(),
            IvmJob::Partitioned(p) => p.view_specs(),
        }
    }

    /// AUD-9 (loud degradation): classify how a view executes —
    /// `(incremental, human_reason)`, `None` if not registered.
    ///
    /// IVM-AUD-PART-12: the partitioned arm used to return a hardcoded
    /// `(true, "incremental — key-group partitioned aggregate")` without asking
    /// a shard anything, reasoning that a job is only partitioned because its
    /// first view was a key-group aggregate. That reasoning is about the shape
    /// of the SQL and is settled before any tick runs; whether a view got an
    /// O(Δ) plan is settled per shard on the first step and can come out
    /// DiffBased (an aggregate the planner declines to lower — `COUNT(DISTINCT
    /// …)`, `SUM(…) FILTER (…)` — a `ctx.sql` failure, a restore that cleared
    /// the cached plans). So the one surface built to expose a silent
    /// full-recompute fallback reported "incremental" straight through it.
    /// Both arms now ask the flow.
    pub fn view_plan_classification(&self, view: &str) -> IvmResult<Option<(bool, String)>> {
        match self {
            IvmJob::Single(f) => f.view_plan_classification(view),
            IvmJob::Partitioned(p) => p.view_plan_classification(view),
        }
    }

    /// Cumulative insert/retract counters for a view (#94); summed across
    /// shards when partitioned.
    pub fn view_delta_stats(&self, view: &str) -> IvmResult<Option<krishiv_ivm::ViewDeltaStats>> {
        match self {
            IvmJob::Single(f) => f.view_delta_stats(view),
            IvmJob::Partitioned(p) => p.view_delta_stats(view),
        }
    }

    /// Enable delta-checkpoint accumulation (every shard when partitioned).
    pub fn enable_delta_checkpoints(&self) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.enable_delta_checkpoints(),
            IvmJob::Partitioned(p) => p.enable_delta_checkpoints(),
        }
    }

    /// Enable content-addressed input dedup (every shard when partitioned).
    pub fn enable_input_dedup(&self) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.enable_input_dedup(),
            IvmJob::Partitioned(p) => p.enable_input_dedup(),
        }
    }

    /// Peek a view's latest output delta (merged across shards when partitioned).
    pub fn view_output_peek(&self, view: &str) -> IvmResult<Option<DeltaBatch>> {
        Ok(self.view_output_peek_at_tick(view)?.map(|(_, delta)| delta))
    }

    /// [`view_output_peek`](Self::view_output_peek) with the tick the delta was
    /// published at.
    ///
    /// IVM-AUD-PART-11: a partitioned job merges its shards' coalescing watch
    /// values, and without the tick there was nothing to merge them *by* — a
    /// shard that emitted this tick and one still holding a delta from five
    /// ticks ago were concatenated and served as "the latest delta".
    pub fn view_output_peek_at_tick(&self, view: &str) -> IvmResult<Option<(u64, DeltaBatch)>> {
        match self {
            IvmJob::Single(f) => Ok(match f.view_output_peek(view)? {
                Some(delta) => f.view_output_tick(view)?.map(|tick| (tick, delta)),
                None => None,
            }),
            IvmJob::Partitioned(p) => p.view_output_peek_at_tick(view),
        }
    }

    /// Spawn a vector-view background task (one per shard when partitioned, all
    /// writing the shared sink).
    ///
    /// The returned handles **own** the tasks: drop them and maintenance stops.
    /// The caller must keep them (see `IvmJobRegistry::register_vector_view`).
    pub fn spawn_vector_views(
        &self,
        spec: krishiv_ivm::VectorViewSpec,
    ) -> IvmResult<Vec<krishiv_ivm::VectorViewHandle>> {
        match self {
            IvmJob::Single(f) => Ok(vec![krishiv_ivm::spawn_vector_view(f, spec)?]),
            IvmJob::Partitioned(p) => p.spawn_vector_views(spec),
        }
    }

    /// Enable tick-granular provenance tracking (every shard when partitioned).
    ///
    /// IVM-AUD-PART-25: the three provenance calls stopped at `IncrementalFlow`
    /// and were forwarded by neither `PartitionedIncrementalFlow` nor this enum,
    /// so provenance was quietly unavailable to every auto-partitioned job.
    ///
    /// Note what it can record: provenance is written only for views that
    /// execute on the **DiffBased** path, and a job is partitioned precisely
    /// because its first view lowered to an incremental key-group aggregate —
    /// so on a partitioned job it is the job's *other*, non-incremental views
    /// that produce provenance, not the one that caused the partitioning.
    pub fn enable_provenance_tracking(&self) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.enable_provenance_tracking(),
            IvmJob::Partitioned(p) => p.enable_provenance_tracking(),
        }
    }

    /// Output row hashes recorded for `input_hash` (unioned across shards).
    ///
    /// Returned sorted, as a `Vec`, because every caller above this line is a
    /// serialization boundary.
    pub fn query_provenance(&self, input_hash: u64) -> IvmResult<Option<Vec<u64>>> {
        let hashes = match self {
            IvmJob::Single(f) => f.query_provenance(input_hash)?,
            IvmJob::Partitioned(p) => p.query_provenance(input_hash)?,
        };
        Ok(hashes.map(|set| {
            let mut v: Vec<u64> = set.into_iter().collect();
            v.sort_unstable();
            v
        }))
    }

    /// Drop the provenance mapping for `input_hash` (every shard when partitioned).
    pub fn forget_provenance(&self, input_hash: u64) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.forget_provenance(input_hash),
            IvmJob::Partitioned(p) => p.forget_provenance(input_hash),
        }
    }

    /// The LATENESS bounds that actually reached the flow (first shard when
    /// partitioned — every shard is registered from the same spec).
    pub fn declared_lateness(&self) -> IvmResult<Vec<krishiv_ivm::LatenessSpec>> {
        match self {
            IvmJob::Single(f) => f.declared_lateness(),
            IvmJob::Partitioned(p) => p.declared_lateness(),
        }
    }

    /// Full checkpoint (per-shard framed when partitioned).
    pub fn checkpoint(&self) -> IvmResult<Vec<u8>> {
        match self {
            IvmJob::Single(f) => f.checkpoint(),
            IvmJob::Partitioned(p) => p.checkpoint(),
        }
    }

    /// Restore a full checkpoint.
    pub fn restore(&self, bytes: &[u8]) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.restore(bytes),
            IvmJob::Partitioned(p) => p.restore(bytes),
        }
    }

    /// Full checkpoint: sources **and view state** (snapshot + baseline), so a
    /// restore converges maintained views after restart (G6). Prefer this over
    /// [`checkpoint`](Self::checkpoint), which captures sources only.
    pub fn checkpoint_full(&self) -> IvmResult<Vec<u8>> {
        match self {
            IvmJob::Single(f) => f.checkpoint_full(),
            IvmJob::Partitioned(p) => p.checkpoint_full(),
        }
    }

    /// Restore a full checkpoint (see [`checkpoint_full`](Self::checkpoint_full)).
    pub fn restore_full(&self, bytes: &[u8]) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.restore_full(bytes),
            IvmJob::Partitioned(p) => p.restore_full(bytes),
        }
    }

    /// Delta checkpoint (per-shard framed when partitioned).
    pub fn checkpoint_delta(&self) -> IvmResult<Vec<u8>> {
        match self {
            IvmJob::Single(f) => f.checkpoint_delta(),
            IvmJob::Partitioned(p) => p.checkpoint_delta(),
        }
    }

    /// Restore a delta checkpoint.
    pub fn restore_delta(&self, bytes: &[u8]) -> IvmResult<()> {
        match self {
            IvmJob::Single(f) => f.restore_delta(bytes),
            IvmJob::Partitioned(p) => p.restore_delta(bytes),
        }
    }
}

/// Default ingress backlog cap for one IVM job, in bytes.
///
/// IVM-AUD-INT-F11: there was no cap of any kind. `IncrementalFlow::pending` is
/// an unbounded `Vec` per source, the HTTP body limit is 512 MiB per request
/// and there is no concurrency limiter, so a producer feeding faster than
/// anything calls `/step` grew the coordinator's heap until the process died —
/// while every `/feed` answered `success: true`.
///
/// 1 GiB is chosen to be larger than any single accepted body (so the cap can
/// never make a legal request permanently unsatisfiable) while still bounding
/// the backlog. `KRISHIV_IVM_MAX_PENDING_BYTES=0` restores the old unbounded
/// behaviour for an operator who would rather have the OOM.
pub(crate) const DEFAULT_IVM_MAX_PENDING_BYTES: u64 = 1024 * 1024 * 1024;

/// Pure policy for [`default_max_pending_bytes`], split out for testing.
///
/// An explicit `0` means unlimited and is honoured. Anything unparseable falls
/// back to the default rather than to unlimited: a typo in an environment
/// variable must not silently switch protection off.
pub(crate) fn resolve_max_pending_bytes(env_override: Option<&str>) -> u64 {
    match env_override.map(str::trim).map(str::parse::<u64>) {
        Some(Ok(bytes)) => bytes,
        _ => DEFAULT_IVM_MAX_PENDING_BYTES,
    }
}

fn default_max_pending_bytes() -> u64 {
    resolve_max_pending_bytes(
        std::env::var("KRISHIV_IVM_MAX_PENDING_BYTES")
            .ok()
            .as_deref(),
    )
}

/// Hard cap on auto-derived IVM shard fan-out (keeps tiny jobs from spawning a
/// flow per core on large machines).
const MAX_AUTO_IVM_SHARDS: usize = 8;

/// Pure shard-count policy: honour a valid `KRISHIV_IVM_SHARDS` override
/// (N≥1; `1` disables partitioning), else derive from `parallelism` capped at
/// [`MAX_AUTO_IVM_SHARDS`]. Split out from environment/CPU lookup for testing.
fn resolve_ivm_shards(env_override: Option<&str>, parallelism: usize) -> usize {
    if let Some(raw) = env_override
        && let Ok(n) = raw.trim().parse::<usize>()
        && n >= 1
    {
        return n;
    }
    parallelism.clamp(1, MAX_AUTO_IVM_SHARDS)
}

/// Default partition fan-out for a shardable IVM job.
///
/// Escape hatch: `KRISHIV_IVM_SHARDS=N` pins the fan-out (N≥1; `1` disables
/// partitioning entirely, e.g. for debugging). Absent or invalid, it derives
/// from available parallelism, capped at [`MAX_AUTO_IVM_SHARDS`] — one shard per
/// core removes the single-core ceiling on keyed incremental views.
fn default_ivm_shards() -> usize {
    let env = std::env::var("KRISHIV_IVM_SHARDS").ok();
    let parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    resolve_ivm_shards(env.as_deref(), parallelism)
}

/// Phase 57 (AUD-6): one recorded dispatch decision for an IVM tick.
///
/// Every route a tick can take — resident executor, central because no
/// executors are live, central because the job is partitioned, or central
/// fallback after a resident dispatch failure — is recorded here and exposed
/// via `GET /api/v1/ivm/jobs/{job}/dispatch`. There are no silent fallbacks.
#[derive(Debug, Clone, serde::Serialize)]
pub struct IvmDispatchRecord {
    /// Flow tick this decision applied to.
    pub tick: u64,
    /// "resident" | "central-fallback" | "central-no-executors" |
    /// "central-partitioned".
    pub mode: String,
    /// Human-readable reason (error text for fallbacks; empty for resident).
    pub reason: String,
    /// Unix millis when the decision was recorded.
    pub at_unix_ms: i64,
}

/// Phase 57 (AUD-6): resident-dispatch bookkeeping for one IVM job.
///
/// # Why this is deliberately not durable (IVM-AUD-DIST-B3)
///
/// It was filed as a gap that `attached` / `fence` live only in this process
/// and are reset by [`IvmJobRegistry::restore_durable_snapshot`], so safety
/// after a restart "rests on `delta:attach:` happening to replace the executor
/// map entry". Persisting the fence would make it **less** safe, not more.
///
/// The invariant `submit_resident_ivm_step` maintains is that a `delta:tick:`
/// is only ever sent while `attached` is true, and `attached` is set true only
/// immediately after a `delta:attach:` this process sent succeeded. An attach
/// replaces the executor's whole entry — flow state *and* fence — so after it,
/// the coordinator's `fence` and the executor's agree by construction, whatever
/// either was before. Resetting to zero on rehydration therefore costs one
/// state ship and is always correct.
///
/// Carrying a fence across a restart removes exactly that guarantee: a tick at
/// fence N+1 reaching a *stale* resident flow that happens to sit at N would be
/// **accepted**, and the executor would apply deltas on top of state the new
/// coordinator has never mirrored. With the reset, that same stale flow answers
/// "fence mismatch" and the coordinator re-attaches. The fence's job is to
/// detect drift, and a persisted fence is the one value that would hide it.
///
/// Two things this does *not* protect against, both filed separately: a tick
/// landing on an executor that never received the attach (IVM-AUD-DIST-A2 —
/// there is no placement pin), and a demoted coordinator attaching at all
/// (IVM-AUD-DIST-E1 — fixed by `ensure_active_leader`).
#[derive(Debug, Clone, Default)]
pub struct IvmDispatchState {
    /// True while a resident executor flow is believed attached.
    pub attached: bool,
    /// Last fence acknowledged by the resident executor.
    pub fence: u64,
    /// Most recent dispatch decision.
    pub last: Option<IvmDispatchRecord>,
}

/// Registry of IVM jobs hosted on this coordinator process.
#[derive(Debug)]
pub struct IvmJobRegistry {
    jobs: Mutex<HashMap<String, IvmJob>>,
    /// Shard count used when a job's first view is auto-partitioned.
    default_shards: usize,
    /// Jobs pinned to a single (non-partitioned) flow. Auto-partitioning is
    /// skipped for these. Set at create time by composition-capable callers
    /// (`Session::view` view-DAGs / `to_incremental`): a partitioned job shards
    /// its output by key and cannot cascade to a derived view that reads the
    /// full base output, so composition requires a single job.
    pinned_single: Mutex<std::collections::HashSet<String>>,
    /// Per-job async step locks. Serialize concurrent `step` calls so two
    /// simultaneous ticks cannot drain each other's pending or double-advance
    /// the tick counter. Each job gets its own lock (created lazily, removed on
    /// `delete`) so unrelated jobs never contend.
    step_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Phase 57: per-job resident dispatch state (fence, attach, last decision).
    dispatch: Mutex<HashMap<String, IvmDispatchState>>,
    /// Jobs with delta-checkpoint accumulation switched on. Mirrors the flag
    /// inside the flow so [`durable_snapshot`](IvmJobRegistry::durable_snapshot)
    /// can carry it across a rehydration (IVM-AUD-DIST-C1).
    delta_checkpoints: Mutex<std::collections::HashSet<String>>,
    /// Ingress backlog cap in bytes, `0` = unlimited (IVM-AUD-INT-F11). Read by
    /// `ivm_http::ensure_pending_headroom` before every feed. Held here rather
    /// than read from the environment at each call so a test can set it.
    max_pending_bytes: std::sync::atomic::AtomicU64,
    /// Vector views registered on each job: `job_id -> view_name -> view`.
    ///
    /// IVM-AUD-DIST-H3: `POST /vector-views` built an `InMemoryVectorSink`,
    /// moved it into detached per-shard tasks and dropped the only `Arc` on the
    /// way out of the handler — so nothing could ever read what the tasks
    /// wrote, N calls with the same view name spawned N×shards permanent tasks,
    /// and deleting the job stopped none of them. Registering here is what makes
    /// the sink readable, the name unique, and the tasks stoppable.
    vector_views: Mutex<HashMap<String, HashMap<String, RegisteredVectorView>>>,
}

/// One vector view registered on a job: its spec, its sink, and the maintenance
/// tasks writing it.
#[derive(Debug)]
pub struct RegisteredVectorView {
    pub view_name: String,
    pub id_column: String,
    pub vector_column: String,
    pub sink_type: String,
    /// The in-memory sink the maintenance tasks write.
    ///
    /// Held here because it is the **only** way to read what they wrote:
    /// `IvmVectorSink` is a write-only trait, so a dropped `Arc` means the
    /// contents are unreachable for the rest of the process's life.
    sink: Arc<krishiv_ivm::InMemoryVectorSink>,
    /// One maintenance handle per shard. Dropping them aborts the tasks, which
    /// is how job deletion and view replacement stop maintenance.
    handles: Vec<krishiv_ivm::VectorViewHandle>,
}

impl RegisteredVectorView {
    pub fn new(
        view_name: String,
        id_column: String,
        vector_column: String,
        sink_type: String,
        sink: Arc<krishiv_ivm::InMemoryVectorSink>,
        handles: Vec<krishiv_ivm::VectorViewHandle>,
    ) -> Self {
        Self {
            view_name,
            id_column,
            vector_column,
            sink_type,
            sink,
            handles,
        }
    }

    /// Number of shard maintenance tasks.
    pub fn shards(&self) -> usize {
        self.handles.len()
    }

    /// Number of points currently in the sink.
    pub fn points(&self) -> usize {
        self.sink.len()
    }

    /// The vector stored for `id`, if any. The read path DIST-H3 was missing.
    pub fn get(&self, id: &str) -> Option<Vec<f32>> {
        self.sink.get(id)
    }

    /// Per-shard health of the maintenance tasks.
    pub fn shard_status(&self) -> Vec<krishiv_ivm::VectorViewStatus> {
        self.handles.iter().map(|h| h.health().status()).collect()
    }

    /// True when any shard's index is known to have diverged from the view.
    pub fn diverged(&self) -> bool {
        self.handles.iter().any(|h| h.health().is_diverged())
    }
}

impl std::fmt::Debug for IvmJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IvmJob::Single(_) => f.write_str("IvmJob::Single"),
            IvmJob::Partitioned(p) => write!(f, "IvmJob::Partitioned({} shards)", p.num_shards()),
        }
    }
}

impl Default for IvmJobRegistry {
    fn default() -> Self {
        Self {
            jobs: Mutex::new(HashMap::new()),
            default_shards: default_ivm_shards(),
            pinned_single: Mutex::new(std::collections::HashSet::new()),
            step_locks: Mutex::new(HashMap::new()),
            dispatch: Mutex::new(HashMap::new()),
            delta_checkpoints: Mutex::new(std::collections::HashSet::new()),
            max_pending_bytes: std::sync::atomic::AtomicU64::new(default_max_pending_bytes()),
            vector_views: Mutex::new(HashMap::new()),
        }
    }
}

impl IvmJobRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a registry with an explicit auto-partition fan-out (for tests).
    pub fn with_default_shards(default_shards: usize) -> Self {
        Self {
            jobs: Mutex::new(HashMap::new()),
            default_shards: default_shards.max(1),
            pinned_single: Mutex::new(std::collections::HashSet::new()),
            step_locks: Mutex::new(HashMap::new()),
            dispatch: Mutex::new(HashMap::new()),
            delta_checkpoints: Mutex::new(std::collections::HashSet::new()),
            max_pending_bytes: std::sync::atomic::AtomicU64::new(default_max_pending_bytes()),
            vector_views: Mutex::new(HashMap::new()),
        }
    }

    /// Create a job pinned to a single (non-partitioned) flow — auto-partitioning
    /// is skipped for it. Used by composition-capable callers (`to_incremental` /
    /// `Session::view` view-DAGs) so a base view can cascade to derived views.
    pub fn create_unpartitioned(&self, job_id: String) -> Result<(), IvmError> {
        if let Ok(mut pinned) = self.pinned_single.lock() {
            pinned.insert(job_id.clone());
        }
        self.create(job_id)
    }

    /// The ingress backlog cap in bytes; `0` means unlimited.
    pub fn max_pending_bytes(&self) -> u64 {
        self.max_pending_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Override the ingress backlog cap (`0` = unlimited).
    pub fn set_max_pending_bytes(&self, bytes: u64) {
        self.max_pending_bytes
            .store(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Switch on delta-checkpoint accumulation for `job_id`, and remember that
    /// it is on so a rehydration re-applies it.
    ///
    /// IVM-AUD-DIST-C1: nothing in the coordinator ever called
    /// [`IvmJob::enable_delta_checkpoints`], so `checkpoint_delta` answered a
    /// well-formed `count = 0` frame forever — an incremental backup that was
    /// reported as taken and contained nothing. The flag is per-flow and
    /// `checkpoint_full` does not carry it, so it is tracked here too and
    /// written into the durable snapshot; otherwise a coordinator restart or a
    /// standby promotion would silently switch accumulation back off.
    ///
    /// Idempotent, and monotone by construction: the flow has no
    /// "disable" (accumulated deltas are drained by `checkpoint_delta`).
    pub fn enable_delta_checkpoints(&self, job_id: &str) -> Result<(), IvmError> {
        let job = self
            .get(job_id)
            .ok_or_else(|| IvmError::execution(format!("IVM job not found: {job_id}")))?;
        job.enable_delta_checkpoints()?;
        if let Ok(mut on) = self.delta_checkpoints.lock() {
            on.insert(job_id.to_owned());
        }
        Ok(())
    }

    /// Whether delta-checkpoint accumulation is on for `job_id`.
    pub fn delta_checkpoints_enabled(&self, job_id: &str) -> bool {
        self.delta_checkpoints
            .lock()
            .map(|on| on.contains(job_id))
            .unwrap_or(false)
    }

    /// Snapshot the resident-dispatch state for a job (default when unset).
    pub fn dispatch_state(&self, job_id: &str) -> IvmDispatchState {
        self.dispatch
            .lock()
            .ok()
            .and_then(|m| m.get(job_id).cloned())
            .unwrap_or_default()
    }

    /// Mutate the resident-dispatch state for a job (created if absent).
    pub fn update_dispatch(&self, job_id: &str, f: impl FnOnce(&mut IvmDispatchState)) {
        let mut map = match self.dispatch.lock() {
            Ok(m) => m,
            Err(p) => p.into_inner(),
        };
        f(map.entry(job_id.to_string()).or_default());
    }

    /// Return the per-job async step lock (creating it if absent).
    ///
    /// The lock serializes concurrent `step`/dispatch calls for one job. It is
    /// intentionally per-job so independent jobs step in parallel.
    pub fn step_lock(&self, job_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        if let Some(lock) = self
            .step_locks
            .lock()
            .ok()
            .and_then(|m| m.get(job_id).cloned())
        {
            return lock;
        }
        let mut locks = match self.step_locks.lock() {
            Ok(l) => l,
            Err(p) => p.into_inner(),
        };
        locks
            .entry(job_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Create a new IVM job. Idempotent: returns `Ok` if the job already exists.
    pub fn create(&self, job_id: String) -> Result<(), IvmError> {
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|_| IvmError::execution("registry lock poisoned"))?;
        jobs.entry(job_id)
            .or_insert_with(|| IvmJob::Single(Arc::new(IncrementalFlow::new())));
        Ok(())
    }

    /// Register (or update) a view on a job, auto-partitioning when eligible.
    ///
    /// The partition decision is made here, on the **first** view of a job: if
    /// the job is still a fresh single flow (no views yet), the view is a
    /// single-column `GROUP BY` aggregate ([`partition_key_from_sql`]) **and**
    /// the key column is one the router can actually route, the job is upgraded
    /// in place to a [`PartitionedIncrementalFlow`] keyed on that column, sized
    /// by [`default_ivm_shards`]. Non-shardable first views leave the job
    /// single.
    ///
    /// # Every later view is checked against the shape the first one chose
    ///
    /// IVM-AUD-PART-2: subsequent views used to register on all shards with no
    /// check at all, which silently broke them in two distinct ways. A second
    /// view with no `GROUP BY` (`SELECT SUM(amount) FROM orders`) computes a
    /// *partial* aggregate per shard, so the job's answer for it is N rows
    /// where one was asked for. A second view grouped by a different column
    /// splits each of its groups across every shard that happens to hold one of
    /// its rows, so each group appears N times with partial values. Both look
    /// like a healthy job — no error, a plausible row count, wrong numbers.
    /// A later view must now be shardable by the **same** key, or it is
    /// rejected with an error naming the job's key.
    ///
    /// The alternative — silently keeping the job single by rebuilding it
    /// unpartitioned — is not available: the job may already hold sharded state
    /// and be mid-stream, and collapsing it would be a data move the caller
    /// never asked for. Rejection leaves the caller a working choice (a
    /// separate job, or a compatible `GROUP BY`).
    pub fn register_view(&self, job_id: &str, spec: IncrementalViewSpec) -> Result<(), IvmError> {
        let mut jobs = self
            .jobs
            .lock()
            .map_err(|_| IvmError::execution("registry lock poisoned"))?;
        let job = jobs
            .get(job_id)
            .ok_or_else(|| IvmError::execution(format!("IVM job not found: {job_id}")))?
            .clone();

        // A later view on an already-partitioned job must fit that job's
        // partitioning (IVM-AUD-PART-2).
        if let IvmJob::Partitioned(part) = &job {
            check_view_fits_partitioning(part.key_column(), part.num_shards(), &spec)?;
            return job.register_view(spec);
        }

        // Only a fresh, unpartitioned, view-less job is a candidate for upgrade —
        // and never a job pinned single by a composition-capable caller.
        let pinned = self
            .pinned_single
            .lock()
            .map(|p| p.contains(job_id))
            .unwrap_or(false);
        if let IvmJob::Single(flow) = &job
            && !pinned
            && flow.view_names().map(|v| v.is_empty()).unwrap_or(false)
            && self.default_shards > 1
            && let Some(key) = partition_key_from_sql(&spec.body_sql)
            && routable_key_type(&key, &spec).is_some()
        {
            let part = PartitionedIncrementalFlow::new(self.default_shards, key);
            part.register_view(spec)?;
            jobs.insert(job_id.to_string(), IvmJob::Partitioned(Arc::new(part)));
            return Ok(());
        }

        job.register_view(spec)
    }

    /// Look up a job. Returns `None` if not found.
    pub fn get(&self, job_id: &str) -> Option<IvmJob> {
        self.jobs.lock().ok()?.get(job_id).cloned()
    }

    /// Delete a job. Returns `true` if the job existed.
    pub fn delete(&self, job_id: &str) -> bool {
        let removed = self
            .jobs
            .lock()
            .map(|mut j| j.remove(job_id).is_some())
            .unwrap_or(false);
        // Drop the per-job step lock so a recreated same-id job gets a fresh one.
        let _ = self.step_locks.lock().map(|mut l| l.remove(job_id));
        let _ = self.pinned_single.lock().map(|mut p| p.remove(job_id));
        let _ = self.delta_checkpoints.lock().map(|mut d| d.remove(job_id));
        // Drop dispatch bookkeeping (a recreated job starts unattached).
        let _ = self.dispatch.lock().map(|mut d| d.remove(job_id));
        // Stop this job's vector-view maintenance tasks (DIST-H3: they used to
        // outlive the job forever, holding a flow that nothing else referenced).
        // Dropping a `VectorViewHandle` aborts its task.
        let _ = self
            .vector_views
            .lock()
            .map(|mut v| v.remove(job_id))
            .map(drop);
        removed
    }

    // ── vector views (IVM-AUD-DIST-H3) ────────────────────────────────────────

    /// Register a vector view on `job_id`, keeping its sink and its per-shard
    /// maintenance handles alive.
    ///
    /// Returns `Err` if a vector view of that name is already registered on the
    /// job: re-registering used to spawn a second full set of shard tasks
    /// against the same view, each with its own unreachable sink, and there was
    /// no way to stop either set. Drop the view first if you mean to replace it.
    pub fn register_vector_view(
        &self,
        job_id: &str,
        view: RegisteredVectorView,
    ) -> Result<(), IvmError> {
        let mut all = match self.vector_views.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let per_job = all.entry(job_id.to_string()).or_default();
        if per_job.contains_key(&view.view_name) {
            return Err(IvmError::execution(format!(
                "vector view '{}' is already registered on IVM job '{job_id}';                  delete it first to replace it",
                view.view_name
            )));
        }
        per_job.insert(view.view_name.clone(), view);
        Ok(())
    }

    /// Read something from a job's vector view under the registry lock.
    ///
    /// `None` when the job or the view is unknown. The closure form keeps the
    /// sink `Arc` inside the registry — handing it out would recreate exactly
    /// the lifetime confusion DIST-H3 filed.
    pub fn with_vector_view<R>(
        &self,
        job_id: &str,
        view_name: &str,
        f: impl FnOnce(&RegisteredVectorView) -> R,
    ) -> Option<R> {
        let all = match self.vector_views.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        all.get(job_id).and_then(|m| m.get(view_name)).map(f)
    }

    /// Apply `f` to every vector view registered on `job_id`.
    pub fn map_vector_views<R>(
        &self,
        job_id: &str,
        f: impl Fn(&RegisteredVectorView) -> R,
    ) -> Vec<R> {
        let all = match self.vector_views.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let Some(per_job) = all.get(job_id) else {
            return Vec::new();
        };
        let mut names: Vec<&String> = per_job.keys().collect();
        names.sort();
        names
            .into_iter()
            .filter_map(|n| per_job.get(n))
            .map(f)
            .collect()
    }

    /// Stop and forget a vector view. Returns `true` if it existed.
    pub fn delete_vector_view(&self, job_id: &str, view_name: &str) -> bool {
        let mut all = match self.vector_views.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        all.get_mut(job_id)
            .and_then(|m| m.remove(view_name))
            .is_some()
    }

    /// List all job IDs.
    pub fn job_ids(&self) -> Vec<String> {
        self.jobs
            .lock()
            .map(|j| j.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Serialize a complete job definition and full state checkpoint.
    pub fn durable_snapshot(&self, job_id: &str) -> IvmResult<Vec<u8>> {
        let job = self
            .get(job_id)
            .ok_or_else(|| IvmError::execution(format!("IVM job not found: {job_id}")))?;
        let shape = match &job {
            IvmJob::Single(_) => PersistedIvmShape::Single,
            IvmJob::Partitioned(flow) => PersistedIvmShape::Partitioned {
                shards: flow.num_shards(),
                key_column: flow.key_column().to_owned(),
            },
        };
        let views = job
            .view_specs()?
            .into_iter()
            .map(|spec| {
                let mut output_schema_ipc = Vec::new();
                {
                    let mut writer =
                        StreamWriter::try_new(&mut output_schema_ipc, &spec.output_schema)
                            .map_err(|e| IvmError::execution(e.to_string()))?;
                    writer
                        .finish()
                        .map_err(|e| IvmError::execution(e.to_string()))?;
                }
                Ok(PersistedIvmViewSpec {
                    name: spec.name,
                    body_sql: spec.body_sql,
                    output_schema_ipc,
                    is_materialized: spec.is_materialized,
                    is_recursive: spec.is_recursive,
                    lateness: spec.lateness,
                })
            })
            .collect::<IvmResult<Vec<_>>>()?;
        let persisted = PersistedIvmJob {
            version: IVM_DURABLE_SNAPSHOT_VERSION,
            shape,
            views,
            checkpoint_full: job.checkpoint_full()?,
            pinned_single: self
                .pinned_single
                .lock()
                .map(|p| p.contains(job_id))
                .unwrap_or(false),
            delta_checkpoints: self.delta_checkpoints_enabled(job_id),
        };
        serde_json::to_vec(&persisted).map_err(|e| IvmError::execution(e.to_string()))
    }

    /// Recreate a job from a durable definition and full-state checkpoint.
    pub fn restore_durable_snapshot(&self, job_id: &str, bytes: &[u8]) -> IvmResult<()> {
        let persisted: PersistedIvmJob =
            serde_json::from_slice(bytes).map_err(|e| IvmError::execution(e.to_string()))?;
        if persisted.version != IVM_DURABLE_SNAPSHOT_VERSION {
            return Err(IvmError::execution(format!(
                "unsupported IVM durable snapshot version {}",
                persisted.version
            )));
        }
        let job = match persisted.shape {
            PersistedIvmShape::Single => IvmJob::Single(Arc::new(IncrementalFlow::new())),
            PersistedIvmShape::Partitioned { shards, key_column } => IvmJob::Partitioned(Arc::new(
                PartitionedIncrementalFlow::new(shards, key_column),
            )),
        };
        for view in persisted.views {
            let reader = StreamReader::try_new(Cursor::new(view.output_schema_ipc), None)
                .map_err(|e| IvmError::execution(e.to_string()))?;
            job.register_view(IncrementalViewSpec {
                name: view.name,
                body_sql: view.body_sql,
                output_schema: reader.schema(),
                is_materialized: view.is_materialized,
                is_recursive: view.is_recursive,
                lateness: view.lateness,
            })?;
        }
        job.restore_full(&persisted.checkpoint_full)?;
        self.jobs
            .lock()
            .map_err(|_| IvmError::execution("registry lock poisoned"))?
            .insert(job_id.to_owned(), job);
        // Restore the single-flow pin before the job becomes visible for view
        // registration; without it a rehydrated view-DAG job auto-partitions on
        // its first GROUP BY view.
        if persisted.pinned_single
            && let Ok(mut pinned) = self.pinned_single.lock()
        {
            pinned.insert(job_id.to_owned());
        }
        // Re-arm delta-checkpoint accumulation (IVM-AUD-DIST-C1). Note what
        // this does NOT restore: the deltas accumulated before the snapshot
        // was taken. `checkpoint_full` captures the state they produced, so
        // the next `/checkpoint-delta` is an increment on top of *this*
        // snapshot, not on top of whatever full checkpoint the caller last
        // took — the same discontinuity a `checkpoint_delta()` call itself
        // creates, since it drains what it returns.
        if persisted.delta_checkpoints {
            self.enable_delta_checkpoints(job_id)?;
        }
        self.update_dispatch(job_id, |dispatch| *dispatch = IvmDispatchState::default());
        Ok(())
    }
}

/// The Arrow type `key` will be routed by, if the view's declared output
/// schema names it and the keyed router supports it.
///
/// IVM-AUD-PART-5: the partition decision never consulted the key's type, so a
/// `GROUP BY` on a `Date32`, `Timestamp`, `UInt64`, `Dictionary` or `Decimal`
/// column auto-partitioned happily and then failed **every** feed with
/// "unsupported partition key type" — auto-partitioning turned a working view
/// into a job that cannot accept data, with no way back (the shape is decided
/// once, at first registration).
///
/// The type consulted is the key column's declared *output* type, which is the
/// only one that exists at registration time — views are registered before any
/// source has arrived. For a plain-column `GROUP BY` that is by definition the
/// input column's type, so a caller whose declaration disagrees with what it
/// then feeds is already wrong about its own schema, and the router's own
/// type check still catches it at feed time.
///
/// A key the output schema does not mention (`SELECT COUNT(*) FROM orders
/// GROUP BY region`) is not partitioned: the shape is shardable, but there is
/// nothing to check the routed type against, and committing to a shape that
/// might reject every feed is exactly the failure above.
fn routable_key_type(key: &str, spec: &IncrementalViewSpec) -> Option<arrow::datatypes::DataType> {
    let field = spec
        .output_schema
        .fields()
        .iter()
        .find(|f| f.name().eq_ignore_ascii_case(key))?;
    krishiv_common::partition::is_supported_partition_key_type(field.data_type())
        .then(|| field.data_type().clone())
}

/// Reject a view that cannot be maintained correctly on a job already sharded
/// by `key_column`. See [`IvmJobRegistry::register_view`] for why.
fn check_view_fits_partitioning(
    key_column: &str,
    shards: usize,
    spec: &IncrementalViewSpec,
) -> Result<(), IvmError> {
    let name = &spec.name;
    match partition_key_from_sql(&spec.body_sql) {
        Some(key) if key.eq_ignore_ascii_case(key_column) => Ok(()),
        Some(key) => Err(IvmError::execution(format!(
            "view '{name}' groups by '{key}' but this job is sharded by \
             '{key_column}' across {shards} shards, so each of its groups would \
             be split across shards and reported {shards} times with partial \
             values. Register it on a separate job, or group it by \
             '{key_column}'."
        ))),
        None => Err(IvmError::execution(format!(
            "view '{name}' is not shardable by '{key_column}' (it is not a \
             single-column GROUP BY on that column over one table), but this \
             job is sharded by '{key_column}' across {shards} shards — it would \
             be computed once per shard over that shard's rows only. Register \
             it on a separate job."
        ))),
    }
}

/// What a durable IVM snapshot says about a job, without rehydrating it.
///
/// IVM-AUD-DIST-C4: `GET /api/v1/ivm/jobs` listed snapshot-only jobs with a
/// hardcoded `partitioned: false` and an empty `view_names`, which for the
/// canonical auto-partitioned job is simply a wrong answer — and the shape is
/// the one property of an IVM job a client cannot change later. The bytes were
/// already in hand (`list_ivm_snapshots` returns them); nothing read them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IvmSnapshotSummary {
    /// Whether the persisted job was auto-partitioned.
    pub partitioned: bool,
    /// The views the snapshot records, in persisted order.
    pub view_names: Vec<String>,
}

/// Read [`IvmSnapshotSummary`] out of durable snapshot bytes.
///
/// Deliberately cheap and total: it parses the JSON envelope only — it does
/// **not** rebuild the flow or apply `checkpoint_full` — so a listing endpoint
/// can describe every persisted job without pulling them all into memory
/// (which is the separate leak filed as IVM-AUD-DIST-H5). Returns `None` for
/// bytes this build cannot parse, so an unreadable snapshot degrades to the
/// old "no detail" answer rather than failing the whole listing.
pub fn read_ivm_snapshot_summary(bytes: &[u8]) -> Option<IvmSnapshotSummary> {
    let persisted: PersistedIvmJob = serde_json::from_slice(bytes).ok()?;
    if persisted.version != IVM_DURABLE_SNAPSHOT_VERSION {
        return None;
    }
    Some(IvmSnapshotSummary {
        partitioned: matches!(persisted.shape, PersistedIvmShape::Partitioned { .. }),
        view_names: persisted.views.into_iter().map(|v| v.name).collect(),
    })
}

/// Shared, reference-counted handle to the IVM job registry.
pub type SharedIvmJobRegistry = Arc<IvmJobRegistry>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn orders(regions: &[&str], amounts: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(regions.to_vec())),
                Arc::new(Int64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    fn revenue_spec() -> IncrementalViewSpec {
        IncrementalViewSpec {
            name: "revenue".into(),
            body_sql: "SELECT region, SUM(amount) AS total FROM orders GROUP BY region".into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("total", DataType::Float64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        }
    }

    #[test]
    fn create_unpartitioned_keeps_a_shardable_view_single() {
        // A GROUP BY view would normally auto-partition with shards>1, but a job
        // created unpartitioned (composition-capable / view-DAG) must stay Single
        // so a derived view can read its full output.
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create_unpartitioned("j".into()).unwrap();
        reg.register_view("j", revenue_spec()).unwrap();
        assert!(
            matches!(reg.get("j").unwrap(), IvmJob::Single(_)),
            "pinned-single job must not auto-partition"
        );
        // Sanity: a normal job with the same view DOES partition.
        reg.create("k".into()).unwrap();
        reg.register_view("k", revenue_spec()).unwrap();
        assert!(matches!(reg.get("k").unwrap(), IvmJob::Partitioned(_)));
    }

    fn passthrough_spec() -> IncrementalViewSpec {
        IncrementalViewSpec {
            name: "passthrough".into(),
            body_sql: "SELECT region, amount FROM orders".into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("amount", DataType::Int64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        }
    }

    /// A GROUP BY view auto-partitions the job; a pass-through view does not.
    #[test]
    fn register_view_auto_partitions_group_by() {
        let reg = IvmJobRegistry::with_default_shards(3);

        reg.create("agg".into()).unwrap();
        reg.register_view("agg", revenue_spec()).unwrap();
        assert!(reg.get("agg").unwrap().is_partitioned());

        reg.create("flat".into()).unwrap();
        reg.register_view("flat", passthrough_spec()).unwrap();
        assert!(!reg.get("flat").unwrap().is_partitioned());
    }

    /// With a single configured shard, even a GROUP BY view stays single.
    #[test]
    fn single_shard_registry_never_partitions() {
        let reg = IvmJobRegistry::with_default_shards(1);
        reg.create("agg".into()).unwrap();
        reg.register_view("agg", revenue_spec()).unwrap();
        assert!(!reg.get("agg").unwrap().is_partitioned());
    }

    /// End-to-end through the coordinator `IvmJob` surface: an auto-partitioned
    /// job feeds, steps, and snapshots to the same grand total as a single flow.
    #[tokio::test]
    async fn partitioned_job_matches_single_job_end_to_end() {
        let data = orders(
            &["US", "EU", "US", "APAC", "EU", "US"],
            &[100, 50, 25, 10, 75, 5],
        );
        let grand = |b: &RecordBatch| -> f64 {
            b.column(1)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap_or(0.0))
                .sum()
        };

        // Partitioned (3 shards).
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("j".into()).unwrap();
        reg.register_view("j", revenue_spec()).unwrap();
        let job = reg.get("j").unwrap();
        assert!(job.is_partitioned());
        job.feed("orders", DeltaBatch::from_inserts(data.clone()).unwrap())
            .unwrap();
        job.step_datafusion().await.unwrap();
        let part = job.snapshot_revenue().await;

        // Single (1 shard).
        let reg1 = IvmJobRegistry::with_default_shards(1);
        reg1.create("j".into()).unwrap();
        reg1.register_view("j", revenue_spec()).unwrap();
        let job1 = reg1.get("j").unwrap();
        assert!(!job1.is_partitioned());
        job1.feed("orders", DeltaBatch::from_inserts(data).unwrap())
            .unwrap();
        job1.step_datafusion().await.unwrap();
        let single = job1.snapshot_revenue().await;

        assert_eq!(grand(&part), 265.0);
        assert_eq!(grand(&part), grand(&single));
    }

    impl IvmJob {
        /// Test helper: read the `revenue` view's materialized snapshot.
        async fn snapshot_revenue(&self) -> RecordBatch {
            match self {
                IvmJob::Single(f) => f.snapshot("revenue").unwrap().unwrap(),
                IvmJob::Partitioned(p) => p.snapshot("revenue").unwrap().unwrap(),
            }
        }
    }

    /// Checkpoint/restore round-trips a partitioned job through the registry.
    #[tokio::test]
    async fn partitioned_job_checkpoint_restore() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("j".into()).unwrap();
        reg.register_view("j", revenue_spec()).unwrap();
        let job = reg.get("j").unwrap();
        job.feed(
            "orders",
            DeltaBatch::from_inserts(orders(&["US", "EU", "US", "APAC"], &[100, 50, 25, 10]))
                .unwrap(),
        )
        .unwrap();
        job.step_datafusion().await.unwrap();
        let before = job.source_snapshot("orders").unwrap().unwrap();
        let bytes = job.checkpoint().unwrap();

        // New registry/job of the same shape restores the source state.
        let reg2 = IvmJobRegistry::with_default_shards(3);
        reg2.create("j".into()).unwrap();
        reg2.register_view("j", revenue_spec()).unwrap();
        let job2 = reg2.get("j").unwrap();
        job2.restore(&bytes).unwrap();
        let after = job2.source_snapshot("orders").unwrap().unwrap();

        assert_eq!(before.num_rows(), after.num_rows());
    }

    #[tokio::test]
    async fn durable_snapshot_restores_definition_shape_and_materialized_state() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("durable".into()).unwrap();
        reg.register_view("durable", revenue_spec()).unwrap();
        let job = reg.get("durable").unwrap();
        job.feed(
            "orders",
            DeltaBatch::from_inserts(orders(&["US", "EU", "US"], &[100, 50, 25])).unwrap(),
        )
        .unwrap();
        job.step_datafusion().await.unwrap();
        let snapshot = reg.durable_snapshot("durable").unwrap();

        let restored = IvmJobRegistry::with_default_shards(1);
        restored
            .restore_durable_snapshot("durable", &snapshot)
            .unwrap();
        let restored_job = restored.get("durable").unwrap();
        assert!(restored_job.is_partitioned());
        assert_eq!(restored_job.view_specs().unwrap().len(), 1);
        assert_eq!(restored_job.snapshot_revenue().await.num_rows(), 2);
        assert_eq!(
            restored_job
                .source_snapshot("orders")
                .unwrap()
                .unwrap()
                .num_rows(),
            3
        );
    }

    /// The single-flow pin must survive rehydration.
    ///
    /// `api_ivm_create_job` persists immediately at create time, so a job
    /// created with `partitioned: false` reaches the store as `shape: Single`
    /// with **no views** — indistinguishable from an ordinary job that has not
    /// registered one yet. Nothing repopulates the IVM registry at startup, so
    /// after a coordinator restart the client's first `GROUP BY` view lands on
    /// a rehydrated job whose pin was lost, and auto-partitions a job that
    /// explicitly asked to stay single. The composition it was pinned for — a
    /// derived view reading the base view's full output — is then impossible,
    /// with no error anywhere.
    #[tokio::test]
    async fn a_pinned_single_job_keeps_its_pin_across_a_durable_round_trip() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create_unpartitioned("dag".into()).unwrap();
        // Snapshot taken before any view exists — what api_ivm_create_job does.
        let snapshot = reg.durable_snapshot("dag").unwrap();

        // A fresh process rehydrates it, then the client registers its first
        // view, which is shardable.
        let restored = IvmJobRegistry::with_default_shards(3);
        restored.restore_durable_snapshot("dag", &snapshot).unwrap();
        restored.register_view("dag", revenue_spec()).unwrap();

        assert!(
            matches!(restored.get("dag").unwrap(), IvmJob::Single(_)),
            "a job created unpartitioned must still be unpartitioned after \
             rehydration; auto-partitioning it here silently breaks the \
             view-DAG cascade the pin exists to protect"
        );
    }

    /// The counterweight: restoring must not pin *everything*. An ordinary job
    /// round-tripped the same way must still auto-partition on a shardable
    /// first view, or the fix above has simply disabled partitioning.
    #[tokio::test]
    async fn an_ordinary_job_still_auto_partitions_after_a_durable_round_trip() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("plain".into()).unwrap();
        let snapshot = reg.durable_snapshot("plain").unwrap();

        let restored = IvmJobRegistry::with_default_shards(3);
        restored
            .restore_durable_snapshot("plain", &snapshot)
            .unwrap();
        restored.register_view("plain", revenue_spec()).unwrap();

        assert!(
            restored.get("plain").unwrap().is_partitioned(),
            "an unpinned job must still auto-partition after rehydration"
        );
    }

    /// A snapshot written before the pin field existed must still load — the
    /// field is `serde(default)` under an unchanged version precisely so
    /// `restore_durable_snapshot`'s version check does not reject every
    /// already-persisted job on upgrade.
    #[tokio::test]
    async fn a_snapshot_without_the_pin_field_still_restores() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("legacy".into()).unwrap();
        let snapshot = reg.durable_snapshot("legacy").unwrap();

        // Strip the field, exactly as a pre-upgrade writer would have left it.
        let mut value: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
        assert!(
            value
                .as_object_mut()
                .unwrap()
                .remove("pinned_single")
                .is_some(),
            "precondition: the field is present in a current snapshot"
        );
        let legacy_bytes = serde_json::to_vec(&value).unwrap();

        let restored = IvmJobRegistry::with_default_shards(3);
        restored
            .restore_durable_snapshot("legacy", &legacy_bytes)
            .expect("a snapshot without the pin field must still load");
        assert!(restored.get("legacy").is_some());
    }

    /// IVM-AUD-DIST-G2. `checkpoint_full` and `output_schema_ipc` are raw bytes
    /// with no byte-aware serde attribute, so every persist wrote the entire
    /// flow state as a JSON array of decimal numbers — roughly 4 bytes of JSON
    /// per byte of state. Snapshots already on disk are in that form, so the
    /// reader must still accept it.
    #[tokio::test]
    async fn a_snapshot_encodes_bytes_compactly_and_still_reads_the_old_array_form() {
        let reg = IvmJobRegistry::with_default_shards(1);
        reg.create("blob".into()).unwrap();
        let snapshot = reg.durable_snapshot("blob").unwrap();

        let value: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
        assert!(
            value.get("checkpoint_full").is_some_and(|v| v.is_string()),
            "checkpoint_full must persist as a base64 string, not a number array"
        );

        // Rewrite it in the old array form and prove it still loads.
        let mut legacy = value.clone();
        let bytes: Vec<u8> = {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD
                .decode(value["checkpoint_full"].as_str().unwrap())
                .unwrap()
        };
        legacy["checkpoint_full"] = serde_json::Value::Array(
            bytes
                .iter()
                .map(|b| serde_json::Value::from(*b))
                .collect::<Vec<_>>(),
        );
        let legacy_bytes = serde_json::to_vec(&legacy).unwrap();
        assert!(
            legacy_bytes.len() > snapshot.len(),
            "precondition: the array form is the bigger one ({} vs {})",
            legacy_bytes.len(),
            snapshot.len()
        );

        let restored = IvmJobRegistry::with_default_shards(1);
        restored
            .restore_durable_snapshot("blob", &legacy_bytes)
            .expect("a snapshot in the old array form must still load");
        assert!(restored.get("blob").is_some());
    }

    // ── shard-count policy (escape hatch) ─────────────────────────────────────

    #[test]
    fn resolve_ivm_shards_honours_env_and_caps() {
        // Valid override wins, including 1 (= disable partitioning).
        assert_eq!(resolve_ivm_shards(Some("4"), 16), 4);
        assert_eq!(resolve_ivm_shards(Some("1"), 16), 1);
        assert_eq!(resolve_ivm_shards(Some(" 6 "), 2), 6); // trimmed
        // Invalid / zero / empty override → fall back to capped parallelism.
        assert_eq!(resolve_ivm_shards(Some("0"), 4), 4);
        assert_eq!(resolve_ivm_shards(Some("abc"), 4), 4);
        assert_eq!(resolve_ivm_shards(Some(""), 4), 4);
        assert_eq!(resolve_ivm_shards(None, 4), 4);
        // Parallelism is clamped to [1, MAX_AUTO_IVM_SHARDS].
        assert_eq!(resolve_ivm_shards(None, 0), 1);
        assert_eq!(resolve_ivm_shards(None, 100), MAX_AUTO_IVM_SHARDS);
    }

    // ── registry lifecycle edge cases ─────────────────────────────────────────

    #[test]
    fn register_view_on_missing_job_errors() {
        let reg = IvmJobRegistry::with_default_shards(3);
        assert!(reg.register_view("ghost", revenue_spec()).is_err());
    }

    #[test]
    fn create_is_idempotent_and_preserves_partitioning() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("j".into()).unwrap();
        reg.register_view("j", revenue_spec()).unwrap();
        assert!(reg.get("j").unwrap().is_partitioned());
        // A second create must not clobber the existing (partitioned) job.
        reg.create("j".into()).unwrap();
        assert!(reg.get("j").unwrap().is_partitioned());
    }

    #[test]
    fn delete_and_list_jobs() {
        let reg = IvmJobRegistry::with_default_shards(2);
        reg.create("a".into()).unwrap();
        reg.create("b".into()).unwrap();
        let mut ids = reg.job_ids();
        ids.sort();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
        assert!(reg.delete("a"));
        assert!(!reg.delete("a")); // already gone
        assert_eq!(reg.job_ids(), vec!["b".to_string()]);
    }

    #[test]
    fn only_first_view_drives_partition_decision() {
        let reg = IvmJobRegistry::with_default_shards(3);
        // First view is non-shardable → job stays single...
        reg.create("j".into()).unwrap();
        reg.register_view("j", passthrough_spec()).unwrap();
        assert!(!reg.get("j").unwrap().is_partitioned());
        // ...and a later GROUP BY view does NOT retroactively partition it.
        reg.register_view("j", revenue_spec()).unwrap();
        assert!(!reg.get("j").unwrap().is_partitioned());
    }

    /// A later view that shards by the same key is fine — that is the whole
    /// point of a partitioned job holding more than one view.
    #[test]
    fn a_second_view_on_the_same_key_registers_on_a_partitioned_job() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("j".into()).unwrap();
        reg.register_view("j", revenue_spec()).unwrap();
        assert!(reg.get("j").unwrap().is_partitioned());
        let spec2 = IncrementalViewSpec {
            name: "revenue2".into(),
            body_sql: "SELECT region, COUNT(*) AS n FROM orders GROUP BY region".into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("n", DataType::Int64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        };
        reg.register_view("j", spec2).unwrap();
        assert_eq!(reg.get("j").unwrap().view_names().len(), 2);
        // Re-registering the first view (an update) must not trip the check.
        reg.register_view("j", revenue_spec()).unwrap();
    }

    /// IVM-AUD-PART-2: only the *first* view drove the partition decision, and
    /// every later one was registered on all shards unchecked. This test used
    /// to assert exactly that gap ("a second GROUP BY view registers without
    /// error", with a view that happened to share the key), so it could not
    /// distinguish "checked and compatible" from "never checked".
    ///
    /// Both incompatible shapes are checked here because they fail differently:
    /// a global aggregate yields N partial rows where one was asked for, and a
    /// differently-grouped view splits each group across every shard.
    #[tokio::test]
    async fn an_incompatible_later_view_is_rejected_from_a_partitioned_job() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("j".into()).unwrap();
        reg.register_view("j", revenue_spec()).unwrap();
        assert!(reg.get("j").unwrap().is_partitioned());

        // (a) A second view with no GROUP BY: one row per shard, not one row.
        let global = IncrementalViewSpec {
            name: "grand_total".into(),
            body_sql: "SELECT SUM(amount) AS total FROM orders".into(),
            output_schema: Arc::new(Schema::new(vec![Field::new(
                "total",
                DataType::Float64,
                true,
            )])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        };
        let err = reg
            .register_view("j", global.clone())
            .expect_err("a global aggregate cannot be maintained on a sharded job")
            .to_string();
        assert!(
            err.contains("grand_total") && err.contains("region"),
            "{err}"
        );

        // (b) A second view grouped by another column: each of its groups is
        // split across every shard holding one of its rows.
        let other_key = IncrementalViewSpec {
            name: "by_amount".into(),
            body_sql: "SELECT amount, COUNT(*) AS n FROM orders GROUP BY amount".into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("amount", DataType::Int64, true),
                Field::new("n", DataType::Int64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        };
        let err = reg
            .register_view("j", other_key)
            .expect_err("a differently-grouped view splits its groups across shards")
            .to_string();
        assert!(err.contains("amount") && err.contains("region"), "{err}");

        // Neither was registered, and the job is unchanged.
        assert_eq!(
            reg.get("j").unwrap().view_names(),
            vec!["revenue".to_string()]
        );

        // The same global aggregate is perfectly fine on its own (single) job —
        // it is the *combination* that is unmaintainable, and the message says
        // so. This also shows what the caller's escape route actually is.
        reg.create("g".into()).unwrap();
        reg.register_view("g", global).unwrap();
        let solo = reg.get("g").unwrap();
        assert!(!solo.is_partitioned());
        solo.feed(
            "orders",
            krishiv_ivm::DeltaBatch::from_inserts(orders(&["US", "EU"], &[10, 20])).unwrap(),
        )
        .unwrap();
        solo.step_datafusion().await.unwrap();
        assert_eq!(solo.snapshot("grand_total").unwrap().unwrap().num_rows(), 1);
    }

    /// IVM-AUD-PART-5: the partition decision never looked at the key's type,
    /// so `GROUP BY` on a key the keyed router cannot hash auto-partitioned and
    /// then failed *every* feed with "unsupported partition key type" — a view
    /// that works unpartitioned turned into a job that accepts no data at all,
    /// irreversibly (the shape is chosen once).
    #[test]
    fn a_key_the_router_cannot_hash_does_not_partition_the_job() {
        use arrow::datatypes::TimeUnit;

        let by_key = |name: &str, key_type: DataType| IncrementalViewSpec {
            name: "daily".into(),
            body_sql: format!("SELECT {name}, SUM(amount) AS total FROM events GROUP BY {name}"),
            output_schema: Arc::new(Schema::new(vec![
                Field::new(name, key_type, true),
                Field::new("total", DataType::Float64, true),
            ])),
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        };

        let reg = IvmJobRegistry::with_default_shards(3);
        for (name, key_type) in [
            ("event_date", DataType::Date32),
            ("event_ts", DataType::Timestamp(TimeUnit::Microsecond, None)),
            ("big_id", DataType::UInt64),
            ("price", DataType::Decimal128(10, 2)),
            (
                "tag",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            ),
        ] {
            let job_id = format!("j_{name}");
            reg.create(job_id.clone()).unwrap();
            reg.register_view(&job_id, by_key(name, key_type.clone()))
                .unwrap();
            assert!(
                !reg.get(&job_id).unwrap().is_partitioned(),
                "a {key_type} key must not auto-partition: every feed would fail"
            );
        }

        // A routable key still partitions — the gate is about the type, not a
        // blanket refusal.
        reg.create("ok".into()).unwrap();
        reg.register_view("ok", by_key("account", DataType::Int64))
            .unwrap();
        assert!(reg.get("ok").unwrap().is_partitioned());

        // A key the output schema does not mention cannot be type-checked, so
        // the job stays single rather than gambling on it.
        reg.create("unprojected".into()).unwrap();
        reg.register_view(
            "unprojected",
            IncrementalViewSpec {
                name: "counts".into(),
                body_sql: "SELECT COUNT(*) AS n FROM orders GROUP BY region".into(),
                output_schema: Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)])),
                is_materialized: true,
                is_recursive: false,
                lateness: vec![],
            },
        )
        .unwrap();
        assert!(!reg.get("unprojected").unwrap().is_partitioned());
    }

    /// IVM-AUD-PART-12: the "loud degradation" surface hardcoded `(true,
    /// "incremental — key-group partitioned aggregate")` for every partitioned
    /// job without asking a shard. `COUNT(DISTINCT …)` is a legitimately
    /// shardable single-key aggregate that the planner will not lower, so every
    /// shard runs it as a full recompute — and the surface built to expose
    /// exactly that reported "incremental".
    #[tokio::test]
    async fn a_partitioned_view_reports_the_strategy_its_shards_actually_use() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("d".into()).unwrap();
        reg.register_view(
            "d",
            IncrementalViewSpec {
                name: "distinct_amounts".into(),
                body_sql: "SELECT region, COUNT(DISTINCT amount) AS n FROM orders GROUP BY region"
                    .into(),
                output_schema: Arc::new(Schema::new(vec![
                    Field::new("region", DataType::Utf8, true),
                    Field::new("n", DataType::Int64, true),
                ])),
                is_materialized: true,
                is_recursive: false,
                lateness: vec![],
            },
        )
        .unwrap();
        let job = reg.get("d").unwrap();
        assert!(job.is_partitioned());
        job.feed(
            "orders",
            krishiv_ivm::DeltaBatch::from_inserts(orders(&["US", "EU", "US"], &[1, 2, 1])).unwrap(),
        )
        .unwrap();
        job.step_datafusion().await.unwrap();

        let (incremental, why) = job
            .view_plan_classification("distinct_amounts")
            .unwrap()
            .unwrap();
        assert!(
            !incremental,
            "a view every shard runs as a full recompute must not report incremental: {why}"
        );

        // A view that does lower still reports incremental, so the surface has
        // not simply been made to say "no".
        reg.create("i".into()).unwrap();
        reg.register_view("i", revenue_spec()).unwrap();
        let agg = reg.get("i").unwrap();
        assert!(agg.is_partitioned());
        agg.feed(
            "orders",
            krishiv_ivm::DeltaBatch::from_inserts(orders(&["US", "EU"], &[1, 2])).unwrap(),
        )
        .unwrap();
        agg.step_datafusion().await.unwrap();
        assert!(
            agg.view_plan_classification("revenue").unwrap().unwrap().0,
            "a key-group aggregate that did lower must still report incremental"
        );
    }

    #[test]
    fn enable_flags_propagate_through_ivm_job() {
        // Both variants accept the enable_* config without error.
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("p".into()).unwrap();
        reg.register_view("p", revenue_spec()).unwrap();
        let part = reg.get("p").unwrap();
        assert!(part.is_partitioned());
        part.enable_delta_checkpoints().unwrap();
        part.enable_input_dedup().unwrap();

        let reg1 = IvmJobRegistry::with_default_shards(1);
        reg1.create("s".into()).unwrap();
        reg1.register_view("s", revenue_spec()).unwrap();
        let single = reg1.get("s").unwrap();
        single.enable_delta_checkpoints().unwrap();
        single.enable_input_dedup().unwrap();
    }

    /// Stream-bridge (`feed_snapshot`) works through the partitioned registry job.
    #[tokio::test]
    async fn feed_snapshot_through_partitioned_registry_job() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("j".into()).unwrap();
        reg.register_view("j", revenue_spec()).unwrap();
        let job = reg.get("j").unwrap();
        assert!(job.is_partitioned());
        job.feed_snapshot("orders", &[orders(&["US", "EU", "US"], &[10, 20, 30])])
            .unwrap();
        job.step_datafusion().await.unwrap();
        assert_eq!(job.snapshot_revenue().await.num_rows(), 2);
    }

    /// `view_output_peek` works through a partitioned registry job (merged delta).
    #[tokio::test]
    async fn view_output_peek_through_partitioned_job() {
        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("j".into()).unwrap();
        reg.register_view("j", revenue_spec()).unwrap();
        let job = reg.get("j").unwrap();
        assert!(job.is_partitioned());
        assert!(job.view_output_peek("revenue").unwrap().is_none()); // before any step
        job.feed(
            "orders",
            DeltaBatch::from_inserts(orders(&["US", "EU", "US"], &[1, 2, 3])).unwrap(),
        )
        .unwrap();
        job.step_datafusion().await.unwrap();
        let peek = job.view_output_peek("revenue").unwrap().unwrap();
        assert_eq!(peek.num_rows(), 2); // US, EU merged across shards
    }

    /// Vector-view fan-out: one task per shard (partitioned) vs. one (single).
    /// Regression: Single-job registry must expose a non-null snapshot after step
    /// when `is_materialized = true`.
    #[tokio::test]
    async fn single_job_snapshot_non_null_after_step() {
        use arrow::array::Float64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use krishiv_ivm::DeltaBatch;

        let sales_schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Float64,
            false,
        )]));
        let view_schema = Arc::new(Schema::new(vec![Field::new(
            "total",
            DataType::Float64,
            true,
        )]));
        let spec = IncrementalViewSpec {
            name: "total_sales".into(),
            body_sql: "SELECT SUM(amount) AS total FROM sales".into(),
            output_schema: view_schema,
            is_materialized: true,
            is_recursive: false,
            lateness: vec![],
        };

        // Use default_shards=1 so no auto-partition (stays Single).
        let reg = IvmJobRegistry::with_default_shards(1);
        reg.create("job-a".into()).unwrap();
        reg.register_view("job-a", spec).unwrap();

        let sales_batch = RecordBatch::try_new(
            sales_schema,
            vec![Arc::new(Float64Array::from(vec![100.0_f64, 200.0, 50.0]))],
        )
        .unwrap();
        let job = reg.get("job-a").unwrap();
        job.feed("sales", DeltaBatch::from_inserts(sales_batch).unwrap())
            .unwrap();
        let summary = job.step_datafusion().await.unwrap();
        assert_eq!(summary.active_views, 1, "expected 1 active view");
        assert_eq!(summary.total_output_rows, 1, "expected 1 output row");

        let snap = job
            .snapshot("total_sales")
            .expect("snapshot() failed")
            .expect("snapshot is None for materialized view after step");
        assert_eq!(snap.num_rows(), 1);
        let total = snap
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((total - 350.0).abs() < 1e-9, "expected 350.0, got {total}");
    }

    #[tokio::test]
    async fn spawn_vector_views_fans_out_per_shard() {
        use krishiv_ivm::{InMemoryVectorSink, VectorViewSpec};

        let make_spec = || VectorViewSpec {
            view_name: "revenue".into(),
            id_column: "region".into(),
            vector_column: "v".into(),
            sink: InMemoryVectorSink::new(),
        };

        let reg = IvmJobRegistry::with_default_shards(3);
        reg.create("p".into()).unwrap();
        reg.register_view("p", revenue_spec()).unwrap();
        let handles = reg
            .get("p")
            .unwrap()
            .spawn_vector_views(make_spec())
            .unwrap();
        assert_eq!(handles.len(), 3);
        for h in handles {
            h.abort();
        }

        let reg1 = IvmJobRegistry::with_default_shards(1);
        reg1.create("s".into()).unwrap();
        reg1.register_view("s", revenue_spec()).unwrap();
        let handles = reg1
            .get("s")
            .unwrap()
            .spawn_vector_views(make_spec())
            .unwrap();
        assert_eq!(handles.len(), 1);
        for h in handles {
            h.abort();
        }
    }

    /// A view whose output carries a string id and a `FixedSizeList<Float32>`
    /// vector, so a vector view over it actually indexes something.
    fn vec_view_spec() -> IncrementalViewSpec {
        IncrementalViewSpec {
            name: "docs".into(),
            body_sql: "SELECT * FROM src".into(),
            output_schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new(
                    "v",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        2,
                    ),
                    false,
                ),
            ])),
            is_materialized: false,
            is_recursive: false,
            lateness: vec![],
        }
    }

    fn vec_delta(id: &str, v: [f32; 2]) -> DeltaBatch {
        use arrow::array::{FixedSizeListArray, Float32Array};
        let vectors = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            2,
            Arc::new(Float32Array::from(v.to_vec())),
            None,
        );
        let batch = RecordBatch::try_new(
            vec_view_spec().output_schema,
            vec![
                Arc::new(StringArray::from(vec![id])) as _,
                Arc::new(vectors) as _,
            ],
        )
        .unwrap();
        DeltaBatch::from_inserts(batch).unwrap()
    }

    /// IVM-AUD-DIST-H3: deleting a job must stop its vector-view maintenance.
    ///
    /// The tasks hold their own `Arc` to the flow, so before the registry kept
    /// their handles nothing could ever stop them: deleting the job removed the
    /// registry entry and the tasks went on indexing the flow forever.
    #[tokio::test]
    async fn deleting_a_job_stops_its_vector_view_tasks() {
        use krishiv_ivm::{InMemoryVectorSink, VectorViewSpec};

        let reg = IvmJobRegistry::with_default_shards(1);
        reg.create_unpartitioned("v".into()).unwrap();
        let job = reg.get("v").unwrap();
        job.register_view(vec_view_spec()).unwrap();

        let sink = InMemoryVectorSink::new();
        let handles = job
            .spawn_vector_views(VectorViewSpec {
                view_name: "docs".into(),
                id_column: "id".into(),
                vector_column: "v".into(),
                sink: Arc::clone(&sink) as Arc<dyn krishiv_ivm::IvmVectorSink>,
            })
            .unwrap();
        reg.register_vector_view(
            "v",
            RegisteredVectorView::new(
                "docs".into(),
                "id".into(),
                "v".into(),
                "in_memory".into(),
                Arc::clone(&sink),
                handles,
            ),
        )
        .unwrap();

        // Keep the flow alive independently of the registry — exactly the way
        // the detached tasks used to.
        let IvmJob::Single(flow) = &job else {
            panic!("expected a single flow");
        };

        let publish = |id: &str, v: [f32; 2]| {
            flow.apply_remote_tick(
                HashMap::new(),
                HashMap::from([("docs".to_string(), vec_delta(id, v))]),
            )
            .unwrap();
        };

        publish("before", [1.0, 2.0]);
        for _ in 0..2000 {
            if sink.get("before").is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            sink.get("before").is_some(),
            "the vector view must be indexing before the job is deleted"
        );

        assert!(reg.delete("v"));

        publish("after", [3.0, 4.0]);
        for _ in 0..2000 {
            tokio::task::yield_now().await;
        }
        assert!(
            sink.get("after").is_none(),
            "deleting the job must stop its vector-view maintenance; the task kept \
             indexing a job that no longer exists"
        );
    }

    /// A vector view name is unique per job: re-registering used to spawn a
    /// second full set of shard tasks against the same view, each writing a
    /// sink nobody held (IVM-AUD-DIST-H3).
    #[tokio::test]
    async fn a_vector_view_name_is_unique_per_job() {
        use krishiv_ivm::InMemoryVectorSink;

        let reg = IvmJobRegistry::with_default_shards(1);
        reg.create("v".into()).unwrap();
        let make = || {
            RegisteredVectorView::new(
                "docs".into(),
                "id".into(),
                "v".into(),
                "in_memory".into(),
                InMemoryVectorSink::new(),
                Vec::new(),
            )
        };
        reg.register_vector_view("v", make()).unwrap();
        let err = reg
            .register_vector_view("v", make())
            .expect_err("the second registration of the same name must be refused");
        assert!(err.to_string().contains("already registered"), "{err}");

        assert!(reg.delete_vector_view("v", "docs"));
        reg.register_vector_view("v", make())
            .expect("after deleting it, the name is free again");
    }

    // ── per-job step lock ─────────────────────────────────────────────────────

    /// The step lock is per-job: same job → same lock, different jobs → different
    /// locks. Deleting a job drops its lock so a recreated same-id job gets a
    /// fresh one.
    #[test]
    fn step_lock_is_per_job_and_lifecycle_aware() {
        let reg = IvmJobRegistry::with_default_shards(1);
        let a1 = reg.step_lock("job-a");
        let a2 = reg.step_lock("job-a");
        let b = reg.step_lock("job-b");
        // Same job → same lock Arc.
        assert!(
            Arc::ptr_eq(&a1, &a2),
            "repeated step_lock must return the same Arc"
        );
        // Different job → different lock.
        assert!(
            !Arc::ptr_eq(&a1, &b),
            "different jobs must have different locks"
        );

        // Delete + recreate → fresh lock (old one not resurrected).
        reg.delete("job-a");
        let a3 = reg.step_lock("job-a");
        assert!(
            !Arc::ptr_eq(&a1, &a3),
            "deleted job must get a fresh lock on recreate"
        );
    }

    /// The step lock actually serializes: a held lock blocks a second acquirer
    /// until the first is released.
    #[tokio::test]
    async fn step_lock_serializes_concurrent_acquirers() {
        let reg = IvmJobRegistry::with_default_shards(1);
        let lock = reg.step_lock("job-s");

        let g1 = lock.lock().await;
        // While g1 is held, a second acquire should not complete immediately.
        let try_second =
            tokio::time::timeout(std::time::Duration::from_millis(50), lock.lock()).await;
        assert!(
            try_second.is_err(),
            "second acquire must block while first is held"
        );

        drop(g1);
        // Now the second acquire succeeds.
        let _g2 = lock.lock().await;
    }
}
