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

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Environment override for task placement capacity.
pub const TASK_SLOTS_ENV: &str = "KRISHIV_TASK_SLOTS";
/// Environment override for the executor-wide query memory pool, in bytes.
pub const QUERY_MEMORY_LIMIT_ENV: &str = "KRISHIV_QUERY_MEMORY_LIMIT_BYTES";
/// Environment override for per-task DataFusion `target_partitions`.
pub const TASK_TARGET_PARALLELISM_ENV: &str = "KRISHIV_TASK_TARGET_PARALLELISM";

/// Bytes held back from the cgroup limit for everything that is not query
/// execution: the binary itself, tokio, gRPC and object-store client buffers,
/// Arrow allocations outside any pool, and allocator fragmentation. A pool
/// sized at the full cgroup limit gets the container OOM-killed long before
/// the pool reports pressure, because the pool only sees what it accounts for.
const PROCESS_OVERHEAD_RESERVE_BYTES: u64 = 512 * 1024 * 1024;

/// Fraction of post-reserve memory handed to the query pool. The remainder
/// absorbs the gap between what DataFusion accounts for and what it actually
/// allocates (batch overshoot between pool checks, Arrow buffer rounding).
const QUERY_POOL_FRACTION: f64 = 0.8;

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
            task_parallelism,
            cores,
            memory_limit_bytes,
            slots_source,
            memory_source,
        }
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
        format!(
            "slots={} ({:?}) cores={} query_pool={} ({:?}, shared; \
             >={share} per task when all slots busy) task_parallelism={}",
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

    #[test]
    fn container_capacity_is_derived_without_any_operator_input() {
        // The SF100 benchmark executor: 4 cores, 2.5 GiB container.
        let cap = ExecutorCapacity::derive(4, Some(2_684_354_560), None, None, None);
        assert_eq!(cap.slots.get(), 4, "4 cores support 4 concurrent tasks");
        assert_eq!(cap.slots_source, CapacitySource::Derived);
        // (2.5 GiB - 512 MiB) * 0.8 ≈ 1.6 GiB, shared across all slots.
        let pool = cap.query_pool_bytes.expect("bounded container yields a pool");
        assert!(
            (1_600_000_000..1_800_000_000).contains(&pool),
            "pool {pool} outside the expected ~1.6 GiB band"
        );
        assert_eq!(cap.task_parallelism.get(), 1, "4 cores / 4 slots");
    }

    #[test]
    fn total_pool_never_exceeds_the_container_however_many_slots() {
        // The bug this model exists to make impossible: per-task pools at 25%
        // of the cgroup each meant total claimed memory grew with slot count.
        // The shared pool is a single number, so it cannot.
        for slots in 1..=64 {
            let cap = ExecutorCapacity::derive(64, Some(4 * GIB), Some(slots), None, None);
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
        let cap = ExecutorCapacity::derive(16, Some(1_610_612_736), None, None, None);
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
        let cap = ExecutorCapacity::derive(4, Some(8 * GIB), Some(1), None, None);
        assert_eq!(cap.slots.get(), 1);
        assert_eq!(cap.task_parallelism.get(), 4, "one task should get all cores");
    }

    #[test]
    fn parallelism_override_wins_over_the_derived_share() {
        let cap = ExecutorCapacity::derive(8, Some(8 * GIB), Some(4), None, Some(3));
        assert_eq!(cap.task_parallelism.get(), 3);
    }

    #[test]
    fn explicit_zero_memory_means_unbounded_not_zero() {
        let cap = ExecutorCapacity::derive(4, Some(8 * GIB), None, Some(0), None);
        assert_eq!(cap.query_pool_bytes, None);
        assert_eq!(cap.memory_source, CapacitySource::Configured);
        assert!(cap.min_task_memory_share_bytes().is_none());
    }

    #[test]
    fn no_cgroup_limit_yields_an_unbounded_pool_and_cpu_bound_slots() {
        let cap = ExecutorCapacity::derive(8, None, None, None, None);
        assert_eq!(cap.query_pool_bytes, None);
        assert_eq!(cap.slots.get(), 8, "nothing but cores bounds an unbounded pool");
    }

    #[test]
    fn a_small_container_still_gets_a_pool_because_unbounded_means_oom_kill() {
        // 600 MiB: after the 512 MiB overhead reserve only ~70 MiB is left for
        // queries. That is tight, but a tight pool spills — an unbounded pool
        // in a container this size gets the process killed instead.
        let cap = ExecutorCapacity::derive(2, Some(629_145_600), None, None, None);
        let pool = cap.query_pool_bytes.expect("a small container is still bounded");
        assert!(pool >= MIN_VIABLE_POOL_BYTES);
        // Memory, not cores, decides how many tasks may share it.
        assert_eq!(cap.slots.get(), 1, "70 MiB cannot feed two 256 MiB shares");
        assert_eq!(cap.task_parallelism.get(), 2, "the one task gets both cores");
    }

    #[test]
    fn a_container_below_the_viable_pool_floor_falls_back_to_unbounded() {
        // 550 MiB leaves ~30 MiB after the reserve. A pool that small fails
        // queries the process has the memory to complete, so it is not built.
        let cap = ExecutorCapacity::derive(2, Some(576_716_800), None, None, None);
        assert_eq!(cap.query_pool_bytes, None);
        assert_eq!(cap.slots.get(), 2, "nothing but cores bounds an unbounded pool");
    }

    #[test]
    fn zero_cores_and_zero_slots_still_produce_a_usable_capacity() {
        let cap = ExecutorCapacity::derive(0, Some(8 * GIB), Some(0), None, None);
        assert_eq!(cap.cores.get(), 1);
        assert_eq!(cap.slots.get(), 1);
        assert_eq!(cap.task_parallelism.get(), 1);
    }

    #[test]
    fn summary_names_every_derived_quantity() {
        let summary = ExecutorCapacity::derive(4, Some(4 * GIB), None, None, None).summary();
        for expected in ["slots=", "cores=", "query_pool=", "task_parallelism="] {
            assert!(summary.contains(expected), "summary missing {expected}: {summary}");
        }
    }
}
