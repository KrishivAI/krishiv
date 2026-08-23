//! Spill-capable `SessionContext` construction for IVM ticks.
//!
//! Every IVM tick that reaches DataFusion (diff-based full recompute, plan
//! fallback, `delta:step:` executor fragments) historically ran on
//! `SessionContext::new()` — an unbounded memory pool. A large snapshot feed
//! (e.g. a 10M-row batch-refresh landing through the stream bridge) could
//! then take down the whole engine process instead of spilling.
//!
//! [`spill_session_context`] mirrors the batch/streaming SQL engines: a
//! `FairSpillPool` sized by `KRISHIV_QUERY_MEMORY_LIMIT_BYTES` (falling back
//! to 25% of the container's cgroup memory limit) so sorts, hash joins, and
//! aggregations spill to disk under pressure. No applicable limit → plain
//! unbounded context, exactly as before.
//!
//! # One budget, N flows
//!
//! [`ivm_memory_limit_bytes`] answers "what may this *process* use", so every
//! caller that builds more than one flow has to divide it. A partitioned IVM
//! job is N independent [`IncrementalFlow`](crate::IncrementalFlow)s, each of
//! which builds its own context; before IVM-AUD-PART-13 each took the whole
//! process budget, so the default 8-shard job authorised itself to use 200% of
//! the container. [`shard_memory_limit_bytes`] is the division, and
//! `PartitionedIncrementalFlow::new` is the one caller that needs it.
//!
//! # Spill destination and disk ceiling
//!
//! Spill files go to the OS temp directory unless `KRISHIV_IVM_SPILL_DIR`
//! names another one, and the directory is capped at
//! `KRISHIV_IVM_SPILL_MAX_DISK_BYTES` (default
//! [`DEFAULT_SPILL_DISK_LIMIT_BYTES`]). Without the cap, converting a memory
//! limit into unbounded disk use only moves the outage: a container whose
//! writable layer fills is as dead as one that OOMs, and DataFusion's own
//! default ceiling is 100 GB.

use std::path::PathBuf;

use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

/// DataFusion's default merge-phase sort reservation (10 MiB); a pool smaller
/// than ~4x this would fail sorts outright instead of spilling.
const DEFAULT_SORT_SPILL_RESERVATION_BYTES: usize = 10 * 1024 * 1024;
const MIN_SORT_SPILL_RESERVATION_BYTES: usize = 64 * 1024;

/// The smallest pool this module will build (256 KiB).
///
/// IVM-AUD-PART-14: the sort reservation is `limit / 4` clamped into
/// `[64 KiB, 10 MiB]`, and the *lower* clamp was never checked against `limit`
/// itself. Ask for a 100 KiB pool and you got a 64 KiB merge reservation
/// inside it — 64% of the whole pool handed to one operator's merge phase
/// before a single row was read — so every sort failed outright rather than
/// spilling, which is the one outcome the module exists to prevent. Requests
/// below this floor are raised to it and logged; a pool this small is already
/// a misconfiguration, and refusing to build one at all would silently restore
/// the unbounded pool.
pub const MIN_SPILL_POOL_BYTES: usize = 4 * MIN_SORT_SPILL_RESERVATION_BYTES;

/// Default ceiling on bytes an IVM spill directory may hold (10 GiB).
pub const DEFAULT_SPILL_DISK_LIMIT_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Resolve the IVM tick memory limit: `KRISHIV_QUERY_MEMORY_LIMIT_BYTES`
/// when set (`0`/unparseable → unlimited), else 25% of the cgroup memory
/// limit when the process runs in a memory-limited container.
///
/// This is a **process-wide** ceiling. A caller that builds several flows must
/// divide it with [`shard_memory_limit_bytes`].
pub fn ivm_memory_limit_bytes() -> Option<usize> {
    match std::env::var("KRISHIV_QUERY_MEMORY_LIMIT_BYTES").ok() {
        Some(raw) => raw.trim().parse::<usize>().ok().filter(|&n| n > 0),
        None => krishiv_common::cgroup_memory_limit_bytes()
            .map(|limit| (limit / 4) as usize)
            .filter(|&n| n > 0),
    }
}

/// One flow's share of a process budget spread over `shards` concurrent flows.
///
/// `None` in, `None` out (an unlimited process stays unlimited). The share is
/// floored at [`MIN_SPILL_POOL_BYTES`] so a large shard count cannot divide the
/// budget down to a pool no query can run in.
pub fn shard_memory_limit_bytes(total: Option<usize>, shards: usize) -> Option<usize> {
    let shards = shards.max(1);
    total.map(|total| (total / shards).max(MIN_SPILL_POOL_BYTES))
}

/// Directory IVM spill files are written to, if the operator named one.
fn spill_dir_from_env() -> Option<PathBuf> {
    std::env::var("KRISHIV_IVM_SPILL_DIR")
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
        .map(PathBuf::from)
}

/// Ceiling on the spill directory's size, in bytes.
fn spill_disk_limit_bytes() -> u64 {
    std::env::var("KRISHIV_IVM_SPILL_MAX_DISK_BYTES")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_SPILL_DISK_LIMIT_BYTES)
}

/// Build a `SessionContext` whose memory pool spills to disk at `limit`
/// bytes; `None` limit returns a default unbounded context.
pub fn spill_session_context_with_limit(limit: Option<usize>) -> SessionContext {
    let Some(limit) = limit else {
        return SessionContext::new();
    };
    let effective = limit.max(MIN_SPILL_POOL_BYTES);
    if effective != limit {
        tracing::warn!(
            requested_bytes = limit,
            using_bytes = effective,
            "IVM spill pool below the minimum workable size; raised to the floor"
        );
    }
    // Never larger than a quarter of the pool it lives in, and never below the
    // minimum DataFusion needs to merge at all.
    let scaled = (effective / 4).min(DEFAULT_SORT_SPILL_RESERVATION_BYTES);
    debug_assert!(
        scaled >= MIN_SORT_SPILL_RESERVATION_BYTES && scaled <= effective / 4,
        "sort reservation {scaled} does not fit pool {effective}"
    );
    let config = SessionConfig::new().with_sort_spill_reservation_bytes(scaled);
    let mut builder = RuntimeEnvBuilder::new()
        .with_memory_pool(std::sync::Arc::new(FairSpillPool::new(effective)))
        .with_max_temp_directory_size(spill_disk_limit_bytes());
    if let Some(dir) = spill_dir_from_env() {
        builder = builder.with_temp_file_path(dir);
    }
    let runtime_env = match builder.build_arc() {
        Ok(env) => env,
        Err(error) => {
            tracing::warn!(%error, "spill runtime env construction failed; using unbounded context");
            return SessionContext::new();
        }
    };
    SessionContext::new_with_config_rt(config, runtime_env)
}

/// Build the default spill-capable `SessionContext` for an IVM tick from the
/// environment/cgroup-derived limit.
pub fn spill_session_context() -> SessionContext {
    spill_session_context_with_limit(ivm_memory_limit_bytes())
}

#[cfg(test)]
mod tests {
    use datafusion::execution::memory_pool::MemoryConsumer;

    use super::*;

    /// Reserve `bytes` against a context's pool, reporting whether the pool
    /// allowed it. This is the only externally observable property of a
    /// `FairSpillPool` that does not depend on DataFusion's query planner, and
    /// it is exactly the property the module promises.
    fn pool_allows(ctx: &SessionContext, bytes: usize) -> bool {
        let pool = ctx.runtime_env().memory_pool.clone();
        // Dropping the reservation returns the bytes to the pool, so probes do
        // not accumulate across calls.
        let reservation = MemoryConsumer::new("ivm-spill-test").register(&pool);
        reservation.try_grow(bytes).is_ok()
    }

    /// IVM-AUD-PART-15: this test used to assert only that `SELECT 1` returned
    /// one row — true of every `SessionContext` ever built, including the
    /// unbounded one, so it passed with the whole spill module deleted. It now
    /// asserts the thing the module is for: the returned context's pool is
    /// *bounded at the requested limit*.
    #[tokio::test]
    async fn limited_context_is_bounded_at_the_limit_and_still_executes_sql() {
        let limit = 64 * 1024 * 1024;
        let ctx = spill_session_context_with_limit(Some(limit));
        assert!(
            pool_allows(&ctx, limit),
            "a request for exactly the limit must fit"
        );
        assert!(
            !pool_allows(&ctx, limit + 1),
            "a request above the limit must be refused — the pool is not bounded"
        );
        let batches = ctx
            .sql("SELECT 1 AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    /// The `None` arm must hand back an *unbounded* pool, not a very large one:
    /// the previous version of this test could not tell the two apart.
    #[tokio::test]
    async fn unlimited_context_has_no_pool_ceiling() {
        let ctx = spill_session_context_with_limit(None);
        assert!(
            pool_allows(&ctx, usize::MAX / 2),
            "the no-limit context must not impose a ceiling"
        );
        let batches = ctx
            .sql("SELECT 1 AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    /// The 100k-row sort both bounded-pool tests below use.
    const SORT_QUERY: &str = "SELECT v FROM (SELECT CAST(random() * 1000000 AS BIGINT) AS v \
                              FROM (SELECT unnest(range(0, 100000)) )) ORDER BY v";

    async fn run_sort(ctx: &SessionContext) -> Result<usize, String> {
        match ctx.sql(SORT_QUERY).await.expect("planning").collect().await {
            Ok(batches) => Ok(batches.iter().map(|b| b.num_rows()).sum()),
            Err(e) => Err(e.to_string()),
        }
    }

    /// IVM-AUD-PART-15: the old version of this test ran a sort, accepted both
    /// success and failure, and measured no memory — so it passed with the
    /// whole spill module deleted, which is the only outcome it needed to rule
    /// out. It now pins the difference the module makes: the *same* sort that
    /// an unbounded context completes is stopped by a 2 MiB pool, with an error
    /// naming memory rather than the process being taken down.
    #[tokio::test]
    async fn a_pool_stops_a_sort_the_unbounded_context_completes() {
        assert_eq!(
            run_sort(&spill_session_context_with_limit(None)).await,
            Ok(100_000),
            "baseline: with no pool the sort completes"
        );

        let limit = 2 * 1024 * 1024;
        let ctx = spill_session_context_with_limit(Some(limit));
        let reservation = ctx
            .copied_config()
            .options()
            .execution
            .sort_spill_reservation_bytes;
        assert!(
            reservation <= limit / 4,
            "sort merge reservation {reservation} claims more than a quarter of \
             the {limit}-byte pool"
        );
        let error = run_sort(&ctx)
            .await
            .expect_err("a 2 MiB pool must refuse this sort, not absorb it")
            .to_lowercase();
        assert!(
            error.contains("resources exhausted") || error.contains("not enough memory"),
            "the pool must be what stopped it: {error}"
        );
    }

    /// The bound is a ceiling, not a wall: given a realistic budget the same
    /// sort runs to completion inside it.
    #[tokio::test]
    async fn a_realistic_pool_completes_the_sort_inside_its_ceiling() {
        let limit = 64 * 1024 * 1024;
        let ctx = spill_session_context_with_limit(Some(limit));
        assert_eq!(run_sort(&ctx).await, Ok(100_000));
        assert!(!pool_allows(&ctx, limit + 1), "still bounded afterwards");
    }

    /// IVM-AUD-PART-14: a pool below the workable floor is raised to it, so the
    /// merge reservation can never claim more of the pool than it fits in.
    #[tokio::test]
    async fn a_pool_below_the_floor_is_raised_to_the_floor() {
        let ctx = spill_session_context_with_limit(Some(100 * 1024));
        assert!(pool_allows(&ctx, MIN_SPILL_POOL_BYTES));
        let reservation = ctx
            .copied_config()
            .options()
            .execution
            .sort_spill_reservation_bytes;
        assert!(
            reservation <= MIN_SPILL_POOL_BYTES / 4,
            "reservation {reservation} does not fit the floored pool"
        );
    }

    /// IVM-AUD-PART-13: N flows must divide one budget, not replicate it.
    #[test]
    fn shard_budget_divides_rather_than_replicates() {
        let total = 800 * 1024 * 1024;
        let share = shard_memory_limit_bytes(Some(total), 8).unwrap();
        assert_eq!(share, total / 8);
        assert!(
            share * 8 <= total,
            "8 shards at {share} bytes each exceed the {total}-byte budget"
        );
        // A single shard gets the whole budget; unlimited stays unlimited.
        assert_eq!(shard_memory_limit_bytes(Some(total), 1), Some(total));
        assert_eq!(shard_memory_limit_bytes(None, 8), None);
        // Division never produces an unusable pool.
        assert_eq!(
            shard_memory_limit_bytes(Some(MIN_SPILL_POOL_BYTES), 64),
            Some(MIN_SPILL_POOL_BYTES)
        );
    }

    /// The spill directory is capped, so converting an OOM into unbounded disk
    /// use is not a fix.
    #[tokio::test]
    async fn spill_directory_is_capped() {
        let ctx = spill_session_context_with_limit(Some(64 * 1024 * 1024));
        assert_eq!(
            ctx.runtime_env().disk_manager.max_temp_directory_size(),
            DEFAULT_SPILL_DISK_LIMIT_BYTES
        );
    }
}
