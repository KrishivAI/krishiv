//! One coherent capacity decision per executor process.
//!
//! Task placement capacity (`slots`), the query memory budget, and per-task
//! DataFusion parallelism are not three independent settings — they are three
//! consequences of one fact: how much CPU and memory this process actually
//! has. Historically each was configured through its own environment variable
//! with its own default, and nothing forced them to agree:
//!
//! | knob | old default |
//! |---|---|
//! | `KRISHIV_TASK_SLOTS` | `available_parallelism()` |
//! | `KRISHIV_QUERY_MEMORY_LIMIT_BYTES` | 25% of the cgroup limit, **per task** |
//! | `KRISHIV_TASK_TARGET_PARALLELISM` | `cores / KRISHIV_TASK_SLOTS` |
//!
//! Two structural problems followed. First, `slots` per-task pools of 25% each
//! claim `0.25 × slots` of the container — at four slots that is the entire
//! container in DataFusion pools alone, before any allocation that does not go
//! through a pool. Second, the parallelism share read the *environment
//! variable* rather than the executor's actual slot count, so `--slots N` on
//! the command line silently left it wrong.
//!
//! [`ExecutorCapacity::detect`] replaces all three with a single derivation.
//! Because the memory budget is a *shared, hard-capped* pool rather than a
//! per-task allowance (see `krishiv-executor`'s shared query pool), slot count
//! no longer has to encode memory: adding a slot divides the same budget more
//! ways instead of claiming more of the machine. Overcommit stops being an
//! arithmetic property the operator has to get right and becomes a structural
//! impossibility.
//!
//! This is Spark's model — `spark.executor.cores` task slots over one unified
//! `spark.memory.fraction` region — reached from the same reasoning.
//!
//! # What this model does *not* cover: page cache
//!
//! Every fraction below divides *anonymous* memory. A cgroup is also charged
//! for page cache, including cache for files the container itself wrote, and
//! that charge counts against `memory.max`.
//!
//! Measured on the SF100 benchmark (4500 MiB executors), the gap between what
//! the cgroup was charged and what the process held as RSS was 1.5–1.8 GB —
//! a third of the container — and it was almost entirely page cache for shuffle
//! files this executor had written and would never read again. Three separate
//! attempts to bound heap-side shuffle buffers therefore changed nothing: the
//! memory was never on the heap, and the pool correctly reported headroom right
//! up to the OOM kill.
//!
//! That is fixed at the source rather than by shrinking budgets here — see
//! [`crate::page_cache`], which drops the cache behind write-once shuffle files
//! once they are durable. The fractions below are deliberately left where the
//! OOM investigation put them; re-raising [`QUERY_POOL_FRACTION`] is a separate,
//! evidence-led change that needs a clean SF100 run to justify, not an
//! assumption that the eviction bought back exactly that much.
//!
//! [`crate::memory_budget::cgroup_memory_usage`] exposes the split so this is
//! observable next time instead of requiring the whole investigation again.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Environment override for task placement capacity.
pub const TASK_SLOTS_ENV: &str = "KRISHIV_TASK_SLOTS";
/// Environment override for the executor-wide query memory pool, in bytes.
pub const QUERY_MEMORY_LIMIT_ENV: &str = "KRISHIV_QUERY_MEMORY_LIMIT_BYTES";
/// Environment override for per-task DataFusion `target_partitions`.
pub const TASK_TARGET_PARALLELISM_ENV: &str = "KRISHIV_TASK_TARGET_PARALLELISM";
/// Environment override for the push-shuffle store's memory ceiling, in bytes.
pub const SHUFFLE_STORE_LIMIT_ENV: &str = "KRISHIV_SHUFFLE_STORE_BYTES";

/// Bytes held back from the cgroup limit for everything that is not query
/// execution: the binary itself, tokio, gRPC and object-store client buffers,
/// Arrow allocations outside any pool, and allocator fragmentation. A pool
/// sized at the full cgroup limit gets the container OOM-killed long before
/// the pool reports pressure, because the pool only sees what it accounts for.
const PROCESS_OVERHEAD_RESERVE_BYTES: u64 = 512 * 1024 * 1024;

/// Fraction of post-reserve memory handed to the query pool.
///
/// The remainder is not slack — it is the memory the pool cannot see. A
/// `MemoryPool` accounts for what operators *reserve through it*; it does not
/// account for Arrow batches in flight between operators, shuffle write and
/// fetch buffers, result spool chunks, or allocator fragmentation and
/// retention. Sizing the pool at most of the container therefore still
/// overcommits, just less obviously: the pool reports headroom while the
/// process RSS walks past the cgroup limit and the kernel kills it.
///
/// This was 0.8, and it was still too high — the SF100 cluster run OOM-killed
/// executors 11, 7 and 3 times with pools that never reported pressure. 0.6
/// matches Spark's `spark.memory.fraction` default, which exists for exactly
/// this reason after the same lesson.
const QUERY_POOL_FRACTION: f64 = 0.6;

/// Fraction of post-reserve memory the push-shuffle store may hold.
///
/// The store buffers map output in memory until the reduce side fetches it, so
/// it grows with shuffle volume, not with the query pool. It used to carry its
/// own hard-coded 2 GiB default, set once in `PushShuffleStore` and never
/// reconciled with the container: two budgets, each sized as though it owned
/// the machine. On the 4500Mi executors that came to 2.34 GiB of query pool
/// plus 0.5 GiB of reserve plus 2 GiB of shuffle store — 4.84 GiB of claims
/// inside a 4.39 GiB container — and on the 2500Mi executor, 3.66 GiB inside
/// 2.44 GiB. Both are over before a single row is read, which is why moving
/// [`QUERY_POOL_FRACTION`] from 0.8 to 0.6 did not stop the OOM kills: it
/// shrank one claim and left the other untouched.
///
/// Deriving it here makes the container the single source of truth. The sum of
/// both fractions is held below 1.0 by [`ACCOUNTED_FRACTION_CEILING`], so what
/// remains is genuine slack for the allocations no budget can see.
const SHUFFLE_STORE_FRACTION: f64 = 0.15;

/// Fraction of post-reserve memory allowed to sit in page cache for shuffle
/// output this executor wrote.
///
/// Small on purpose. The benefit is serving a same-node reduce read from RAM
/// instead of a 270 MB/s disk, and at the ~3.5 MB partitions this workload
/// produces, 5% of post-reserve still holds ~55 partitions — a real working
/// set. The first attempt claimed 12.5% of the *whole* container, which took
/// total claims to 90% and got executors OOM-killed; the cache was never the
/// thing that needed to be large.
const SHUFFLE_PAGE_CACHE_FRACTION: f64 = 0.05;

/// Below this a page-cache ceiling is not worth enforcing — the bookkeeping
/// costs more than the handful of partitions it would retain.
const MIN_VIABLE_PAGE_CACHE_BYTES: u64 = 32 * 1024 * 1024;

/// Environment override for the shuffle page-cache ceiling, in bytes.
pub const SHUFFLE_PAGE_CACHE_ENV: &str = "KRISHIV_SHUFFLE_PAGE_CACHE_BYTES";

/// Upper bound on the share of post-reserve memory any set of explicit budgets
/// may claim. The remainder covers Arrow batches in flight between operators,
/// object-store and gRPC buffers, and allocator retention — none of which is
/// reservable, all of which is real. Enforced by a test rather than left as a
/// comment, so adding a third budget cannot quietly reintroduce the overcommit.
///
/// Public because it is part of this executor's memory contract: an operator
/// sizing a container needs to know that roughly a fifth of it past the
/// process reserve is deliberately left unclaimed.
pub const ACCOUNTED_FRACTION_CEILING: f64 = 0.8;

/// Smallest push-shuffle store worth enforcing. Below this the store rejects
/// pushes that a healthy executor should absorb, turning a memory setting into
/// spurious task failures; an unbounded store is the safer degenerate case on a
/// container this small because the query pool is already the binding limit.
const MIN_VIABLE_SHUFFLE_STORE_BYTES: u64 = 32 * 1024 * 1024;

/// Floor for a single task's fair share of the shared pool. Below this a task
/// spills more than it computes, so slots are capped rather than divided
/// further. Rarely binding: it only matters on containers under ~2 GiB.
const MIN_TASK_MEMORY_BYTES: u64 = 256 * 1024 * 1024;

/// Smallest query pool worth building. Under this the shared pool is dropped
/// entirely and DataFusion's unbounded pool is used, because a pool this small
/// fails queries that would otherwise have completed using memory the process
/// demonstrably has.
const MIN_VIABLE_POOL_BYTES: u64 = 64 * 1024 * 1024;

/// A slot count supplied on the command line (`--slots N`); `0` means unset.
///
/// The environment variable is not the only way an operator sets slots, and a
/// CLI flag that reached the advertised slot count but not the derivation that
/// sizes memory and per-task parallelism is exactly how the three drifted
/// apart. Setting the process environment from Rust is `unsafe` under edition
/// 2024 and this workspace forbids unsafe, so the flag is published here
/// instead and [`ExecutorCapacity::detect`] consults it ahead of the variable.
static SLOTS_OVERRIDE: AtomicUsize = AtomicUsize::new(0);

/// Publish a command-line slot count to [`ExecutorCapacity::detect`].
///
/// Call once during startup, before any task engine is built — capacity is
/// cached on first use, so a later call cannot take effect.
pub fn set_slots_override(slots: usize) {
    SLOTS_OVERRIDE.store(slots, Ordering::Relaxed);
}

/// The published command-line slot count, if any.
#[must_use]
pub fn slots_override() -> Option<usize> {
    match SLOTS_OVERRIDE.load(Ordering::Relaxed) {
        0 => None,
        slots => Some(slots),
    }
}

/// How each field of an [`ExecutorCapacity`] was arrived at — recorded so the
/// startup log can explain the capacity rather than just assert it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacitySource {
    /// Derived from detected CPU and memory.
    Derived,
    /// Taken from an environment variable or CLI flag.
    Configured,
}

/// The capacity of one executor process, derived once at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorCapacity {
    /// Task placement capacity advertised to the coordinator: how many task
    /// fragments this executor runs concurrently.
    pub slots: NonZeroUsize,
    /// Bytes available for query execution across **all** slots combined.
    /// `None` means unbounded (no cgroup limit and no override).
    ///
    /// This is a shared budget, not a per-task allowance: a task running alone
    /// may use all of it, and `slots` tasks running together divide it fairly.
    pub query_pool_bytes: Option<u64>,
    /// Bytes the push-shuffle store may hold across all partitions before it
    /// back-pressures producers. `None` means unbounded (no cgroup limit, no
    /// override, or a container too small for the bound to be useful).
    ///
    /// Carved from the same container budget as `query_pool_bytes` — the two
    /// are shares of one quantity, not independent settings.
    pub shuffle_store_bytes: Option<u64>,
    /// Ceiling on page cache held by committed-but-unconsumed shuffle output.
    /// `None` means unbounded (no cgroup limit, or explicitly disabled).
    pub page_cache_bytes: Option<u64>,
    /// DataFusion `target_partitions` for one task's engine — this executor's
    /// per-slot share of its cores.
    pub task_parallelism: NonZeroUsize,
    /// Cores detected (or configured) for this process.
    pub cores: NonZeroUsize,
    /// The cgroup memory limit this derivation saw, if any.
    pub memory_limit_bytes: Option<u64>,
    /// Whether `slots` was derived or configured.
    pub slots_source: CapacitySource,
    /// Whether `query_pool_bytes` was derived or configured.
    pub memory_source: CapacitySource,
}

impl ExecutorCapacity {
    /// Derive capacity from this process's real CPU and memory.
    ///
    /// `available_parallelism` honours cgroup CPU limits and CPU pinning, and
    /// [`crate::cgroup_memory_limit_bytes`] reads cgroup v2 `memory.max` (v1
    /// `memory.limit_in_bytes`), so a container gets its container's capacity
    /// with no operator input. Outside a container both fall back to the
    /// machine, which is the correct answer there too.
    #[must_use]
    pub fn detect() -> Self {
        Self::derive(
            std::thread::available_parallelism()
                .map(NonZeroUsize::get)
                .unwrap_or(1),
            crate::cgroup_memory_limit_bytes(),
            slots_override().or_else(|| crate::env_usize(TASK_SLOTS_ENV)),
            crate::env_u64(QUERY_MEMORY_LIMIT_ENV),
            crate::env_usize(TASK_TARGET_PARALLELISM_ENV),
            crate::env_u64(SHUFFLE_STORE_LIMIT_ENV),
            crate::env_u64(SHUFFLE_PAGE_CACHE_ENV),
        )
    }

    /// The pure derivation behind [`detect`](Self::detect), with every input
    /// passed explicitly so it is testable without touching process state
    /// (the workspace forbids `unsafe`, so `set_var` is unavailable in tests).
    ///
    /// `slots_override` / `memory_override` / `parallelism_override` are the
    /// operator's explicit choices; `None` means "derive it".
    #[must_use]
    pub fn derive(
        cores: usize,
        memory_limit_bytes: Option<u64>,
        slots_override: Option<usize>,
        memory_override: Option<u64>,
        parallelism_override: Option<usize>,
        shuffle_store_override: Option<u64>,
        page_cache_override: Option<u64>,
    ) -> Self {
        let cores = NonZeroUsize::new(cores.max(1)).unwrap_or(NonZeroUsize::MIN);

        // The shared query pool: what is left of the container after holding
        // back process overhead, scaled by the accounting-slack fraction.
        // An explicit override is taken literally — including an explicit 0,
        // which means "no limit" and is how the pool is disabled.
        let (query_pool_bytes, memory_source) = match memory_override {
            Some(0) => (None, CapacitySource::Configured),
            Some(bytes) => (Some(bytes), CapacitySource::Configured),
            None => {
                let derived = memory_limit_bytes.and_then(|limit| {
                    let usable = limit.saturating_sub(PROCESS_OVERHEAD_RESERVE_BYTES);
                    #[expect(
                        clippy::cast_precision_loss,
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        reason = "byte counts are far below f64's exact-integer range; \
                                  the product is non-negative and bounded by `usable`"
                    )]
                    let pool = (usable as f64 * QUERY_POOL_FRACTION) as u64;
                    (pool >= MIN_VIABLE_POOL_BYTES).then_some(pool)
                });
                (derived, CapacitySource::Derived)
            }
        };

        // The shuffle store's share of the same container. Derived from the
        // identical `usable` quantity the query pool comes from, so the two
        // cannot together exceed what the container has.
        let shuffle_store_bytes = match shuffle_store_override {
            Some(0) => None,
            Some(bytes) => Some(bytes),
            None => memory_limit_bytes.and_then(|limit| {
                let usable = limit.saturating_sub(PROCESS_OVERHEAD_RESERVE_BYTES);
                #[expect(
                    clippy::cast_precision_loss,
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "byte counts are far below f64's exact-integer range; \
                              the product is non-negative and bounded by `usable`"
                )]
                let share = (usable as f64 * SHUFFLE_STORE_FRACTION) as u64;
                (share >= MIN_VIABLE_SHUFFLE_STORE_BYTES).then_some(share)
            }),
        };

        // Page cache for shuffle output written by this executor. It belongs in
        // this derivation, not beside it: the kernel charges it to the same
        // `memory.max` as everything above, so a budget decided elsewhere is
        // simply an unaccounted claim.
        //
        // It first shipped as 12.5% of the *whole* container, decided inside
        // `page_cache` and invisible to [`ExecutorCapacity::accounted_bytes`].
        // That put total claims at 90% of a 4500 MiB executor and left 435 MiB
        // for everything unaccounted, and s2/s3 were OOM-killed on q8 with
        // `anon=3312 file=585 cur=3935`. The ceiling test below exists to stop
        // exactly that, and could not see a budget declared outside the struct.
        let page_cache_bytes = match page_cache_override {
            Some(0) => None,
            Some(bytes) => Some(bytes),
            None => memory_limit_bytes.and_then(|limit| {
                let usable = limit.saturating_sub(PROCESS_OVERHEAD_RESERVE_BYTES);
                #[expect(
                    clippy::cast_precision_loss,
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "byte counts are far below f64's exact-integer range; \
                              the product is non-negative and bounded by `usable`"
                )]
                let share = (usable as f64 * SHUFFLE_PAGE_CACHE_FRACTION) as u64;
                (share >= MIN_VIABLE_PAGE_CACHE_BYTES).then_some(share)
            }),
        };

        // Slots are a concurrency decision, bounded by memory only to stop the
        // shared pool being divided into shares too small to compute with.
        // With an unbounded pool nothing bounds it but cores.
        let (slots, slots_source) = match slots_override.filter(|&n| n > 0) {
            Some(configured) => (configured, CapacitySource::Configured),
            None => {
                let memory_bound = query_pool_bytes
                    .map(|pool| usize::try_from(pool / MIN_TASK_MEMORY_BYTES).unwrap_or(usize::MAX))
                    .unwrap_or(usize::MAX);
                (cores.get().min(memory_bound).max(1), CapacitySource::Derived)
            }
        };
        let slots = NonZeroUsize::new(slots).unwrap_or(NonZeroUsize::MIN);

        // Each concurrently running task gets its share of the cores. This is
        // derived from the *resolved* slot count, not from the environment:
        // reading the env var here is what made `--slots N` a no-op for
        // per-task parallelism.
        let task_parallelism = parallelism_override
            .and_then(NonZeroUsize::new)
            .unwrap_or_else(|| {
                NonZeroUsize::new((cores.get() / slots.get()).max(1)).unwrap_or(NonZeroUsize::MIN)
            });

        Self {
            slots,
            query_pool_bytes,
            shuffle_store_bytes,
            page_cache_bytes,
            task_parallelism,
            cores,
            memory_limit_bytes,
            slots_source,
            memory_source,
        }
    }

    /// Total bytes this executor has explicitly budgeted, including the
    /// process-overhead reserve. Everything above this and below the container
    /// limit is the unreservable remainder — Arrow batches between operators,
    /// client buffers, allocator retention.
    ///
    /// Exists so the invariant "budgets fit inside the container" can be
    /// asserted rather than believed.
    #[must_use]
    pub fn accounted_bytes(&self) -> u64 {
        PROCESS_OVERHEAD_RESERVE_BYTES
            .saturating_add(self.query_pool_bytes.unwrap_or(0))
            .saturating_add(self.shuffle_store_bytes.unwrap_or(0))
            .saturating_add(self.page_cache_bytes.unwrap_or(0))
    }

    /// The fair share of the query pool one task sees when all slots are busy.
    /// Reporting only — the pool itself is shared and rebalances live, so a
    /// task running alone gets more than this.
    #[must_use]
    pub fn min_task_memory_share_bytes(&self) -> Option<u64> {
        self.query_pool_bytes
            .map(|pool| pool / u64::try_from(self.slots.get()).unwrap_or(1))
    }

    /// One-line human summary for the startup log.
    #[must_use]
    pub fn summary(&self) -> String {
        let pool = self.query_pool_bytes.map_or_else(
            || String::from("unbounded"),
            |bytes| format!("{} MiB", bytes / (1024 * 1024)),
        );
        let share = self.min_task_memory_share_bytes().map_or_else(
            || String::from("unbounded"),
            |bytes| format!("{} MiB", bytes / (1024 * 1024)),
        );
        let shuffle = self.shuffle_store_bytes.map_or_else(
            || String::from("unbounded"),
            |bytes| format!("{} MiB", bytes / (1024 * 1024)),
        );
        format!(
            "slots={} ({:?}) cores={} query_pool={} ({:?}, shared; \
             >={share} per task when all slots busy) shuffle_store={shuffle} \
             task_parallelism={}",
            self.slots,
            self.slots_source,
            self.cores,
            pool,
            self.memory_source,
            self.task_parallelism,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;

    #[test]
    fn container_capacity_is_derived_without_any_operator_input() {
        // The SF100 benchmark executor: 4 cores, 2.5 GiB container.
        let cap = ExecutorCapacity::derive(4, Some(2_684_354_560), None, None, None, None, None);
        assert_eq!(cap.slots.get(), 4, "4 cores support 4 concurrent tasks");
        assert_eq!(cap.slots_source, CapacitySource::Derived);
        // (2.5 GiB - 512 MiB) * 0.6 ≈ 1.25 GiB, shared across all slots. The
        // 40% left over is what the pool cannot see: Arrow batches in flight,
        // shuffle buffers, allocator retention.
        let pool = cap.query_pool_bytes.expect("bounded container yields a pool");
        assert!(
            (1_150_000_000..1_350_000_000).contains(&pool),
            "pool {pool} outside the expected ~1.25 GiB band"
        );
        assert!(
            pool < 2_684_354_560 / 2,
            "the pool must leave the majority of a small container to \
             everything that does not allocate through it"
        );
        assert_eq!(cap.task_parallelism.get(), 1, "4 cores / 4 slots");
    }

    #[test]
    fn total_pool_never_exceeds_the_container_however_many_slots() {
        // The bug this model exists to make impossible: per-task pools at 25%
        // of the cgroup each meant total claimed memory grew with slot count.
        // The shared pool is a single number, so it cannot.
        for slots in 1..=64 {
            let cap = ExecutorCapacity::derive(64, Some(4 * GIB), Some(slots), None, None, None, None);
            let pool = cap.query_pool_bytes.expect("bounded container yields a pool");
            assert!(
                pool < 4 * GIB,
                "slots={slots} claimed {pool} of a {} byte container",
                4 * GIB
            );
        }
    }

    #[test]
    fn slots_are_capped_when_memory_cannot_feed_them() {
        // 16 cores in a 1.5 GiB container: pool ≈ 819 MiB, which supports 3
        // shares of 256 MiB — running 16 tasks would give each 51 MiB.
        let cap = ExecutorCapacity::derive(16, Some(1_610_612_736), None, None, None, None, None);
        assert!(
            cap.slots.get() < 16,
            "memory-starved executor advertised {} slots",
            cap.slots
        );
        let share = cap.min_task_memory_share_bytes().expect("bounded");
        assert!(
            share >= MIN_TASK_MEMORY_BYTES,
            "per-task share {share} fell below the {MIN_TASK_MEMORY_BYTES} floor"
        );
    }

    #[test]
    fn explicit_slots_drive_the_parallelism_share() {
        // The regression: per-task parallelism used to be computed from the
        // KRISHIV_TASK_SLOTS env var, so `--slots 1` on a 4-core box left the
        // share at 1 instead of 4 and used a quarter of the CPU.
        let cap = ExecutorCapacity::derive(4, Some(8 * GIB), Some(1), None, None, None, None);
        assert_eq!(cap.slots.get(), 1);
        assert_eq!(cap.task_parallelism.get(), 4, "one task should get all cores");
    }

    #[test]
    fn parallelism_override_wins_over_the_derived_share() {
        let cap = ExecutorCapacity::derive(8, Some(8 * GIB), Some(4), None, Some(3), None, None);
        assert_eq!(cap.task_parallelism.get(), 3);
    }

    #[test]
    fn explicit_zero_memory_means_unbounded_not_zero() {
        let cap = ExecutorCapacity::derive(4, Some(8 * GIB), None, Some(0), None, None, None);
        assert_eq!(cap.query_pool_bytes, None);
        assert_eq!(cap.memory_source, CapacitySource::Configured);
        assert!(cap.min_task_memory_share_bytes().is_none());
    }

    #[test]
    fn no_cgroup_limit_yields_an_unbounded_pool_and_cpu_bound_slots() {
        let cap = ExecutorCapacity::derive(8, None, None, None, None, None, None);
        assert_eq!(cap.query_pool_bytes, None);
        assert_eq!(cap.slots.get(), 8, "nothing but cores bounds an unbounded pool");
    }

    #[test]
    fn a_small_container_still_gets_a_pool_because_unbounded_means_oom_kill() {
        // 900 MiB: after the 512 MiB overhead reserve, 60% of what is left is
        // ~233 MiB for queries. Tight, but a tight pool spills — an unbounded
        // pool in a container this size gets the process killed instead.
        let cap = ExecutorCapacity::derive(2, Some(943_718_400), None, None, None, None, None);
        let pool = cap
            .query_pool_bytes
            .expect("a small container is still bounded");
        assert!(pool >= MIN_VIABLE_POOL_BYTES);
        assert_eq!(cap.slots.get(), 1, "233 MiB cannot feed two 256 MiB shares");
        assert_eq!(cap.task_parallelism.get(), 2, "the one task gets both cores");
    }

    #[test]
    fn a_container_below_the_viable_pool_floor_falls_back_to_unbounded() {
        // 600 MiB leaves ~88 MiB after the reserve, and 60% of that is under
        // the floor. A pool that small fails queries the process has the
        // memory to complete, so it is not built.
        let cap = ExecutorCapacity::derive(2, Some(629_145_600), None, None, None, None, None);
        assert_eq!(cap.query_pool_bytes, None);
        assert_eq!(cap.slots.get(), 2, "nothing but cores bounds an unbounded pool");
    }

    #[test]
    fn zero_cores_and_zero_slots_still_produce_a_usable_capacity() {
        let cap = ExecutorCapacity::derive(0, Some(8 * GIB), Some(0), None, None, None, None);
        assert_eq!(cap.cores.get(), 1);
        assert_eq!(cap.slots.get(), 1);
        assert_eq!(cap.task_parallelism.get(), 1);
    }

    #[test]
    fn summary_names_every_derived_quantity() {
        let summary = ExecutorCapacity::derive(4, Some(4 * GIB), None, None, None, None, None).summary();
        for expected in [
            "slots=",
            "cores=",
            "query_pool=",
            "shuffle_store=",
            "task_parallelism=",
        ] {
            assert!(summary.contains(expected), "summary missing {expected}: {summary}");
        }
    }

    /// The invariant the OOM kills violated: everything this executor
    /// explicitly budgets has to fit inside the container it runs in.
    ///
    /// Before the shuffle store was derived here it carried a hard-coded 2 GiB
    /// default, so a 4500Mi executor claimed 4.84 GiB and a 2500Mi executor
    /// claimed 3.66 GiB. Both were over the container before reading a row,
    /// and both were killed mid-sweep. Sweeping the container size rather than
    /// checking one value keeps a future fraction change from being correct at
    /// 4 GiB and fatal at 2 GiB.
    #[test]
    fn budgets_always_fit_inside_the_container() {
        for limit_mib in [512_u64, 1024, 2500, 4096, 4500, 8192, 16384, 65536] {
            let limit = limit_mib * 1024 * 1024;
            let cap = ExecutorCapacity::derive(8, Some(limit), None, None, None, None, None);
            let accounted = cap.accounted_bytes();
            assert!(
                accounted <= limit,
                "container {limit_mib}Mi: budgets total {accounted} bytes > limit {limit} \
                 (query_pool={:?} shuffle_store={:?} page_cache={:?})",
                cap.query_pool_bytes,
                cap.shuffle_store_bytes,
                cap.page_cache_bytes,
            );

            // And they must leave the unreservable remainder real slack, not
            // just squeak under the limit: Arrow batches in flight, client
            // buffers and allocator retention all live in what is left.
            let usable = limit.saturating_sub(PROCESS_OVERHEAD_RESERVE_BYTES);
            let explicit = accounted - PROCESS_OVERHEAD_RESERVE_BYTES;
            #[expect(
                clippy::cast_precision_loss,
                reason = "byte counts are far below f64's exact-integer range"
            )]
            let claimed = explicit as f64 / usable.max(1) as f64;
            assert!(
                claimed <= ACCOUNTED_FRACTION_CEILING,
                "container {limit_mib}Mi: budgets claim {claimed:.2} of usable memory, \
                 above the {ACCOUNTED_FRACTION_CEILING} ceiling"
            );
        }
    }

    /// The two production executor shapes from the SF100 cluster, pinned. Both
    /// were OOM-killed; neither may over-commit again.
    #[test]
    fn the_executors_that_were_oom_killed_now_fit() {
        for limit_mib in [2500_u64, 4500] {
            let limit = limit_mib * 1024 * 1024;
            let cap = ExecutorCapacity::derive(4, Some(limit), None, None, None, None, None);
            assert!(
                cap.shuffle_store_bytes.is_some_and(|b| b < 2 * GIB),
                "container {limit_mib}Mi still allows a 2 GiB shuffle store: {:?}",
                cap.shuffle_store_bytes,
            );
            assert!(cap.accounted_bytes() <= limit, "container {limit_mib}Mi over-commits");
        }
    }

    #[test]
    fn shuffle_store_override_is_taken_literally_and_zero_means_unbounded() {
        let configured =
            ExecutorCapacity::derive(4, Some(4 * GIB), None, None, None, Some(123 * MIB), None);
        assert_eq!(configured.shuffle_store_bytes, Some(123 * MIB));

        let unbounded = ExecutorCapacity::derive(4, Some(4 * GIB), None, None, None, Some(0), None);
        assert_eq!(unbounded.shuffle_store_bytes, None);
    }

    /// With no cgroup limit there is no container to divide, so neither budget
    /// is invented — the same rule the query pool already follows.
    #[test]
    fn no_cgroup_limit_leaves_both_budgets_unbounded() {
        let cap = ExecutorCapacity::derive(8, None, None, None, None, None, None);
        assert_eq!(cap.query_pool_bytes, None);
        assert_eq!(cap.shuffle_store_bytes, None);
        assert_eq!(cap.accounted_bytes(), PROCESS_OVERHEAD_RESERVE_BYTES);
    }
}
