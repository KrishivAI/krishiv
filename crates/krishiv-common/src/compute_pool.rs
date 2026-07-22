//! Phase 65: one process-global Rayon compute pool for the engine's own serial
//! CPU-bound kernels.
//!
//! Rayon **complements** Tokio here — it is for compute kernels only (shuffle
//! per-bucket sort/encode/compress, checkpoint manifest hashing, Kafka block
//! decode, streaming/IVM per-key tick loops, Iceberg sink parquet encode). All
//! I/O, RPC, polling, and coordination stay on Tokio. DataFusion operators
//! (expression eval, hash agg/join, sort, scan-decode) are already parallel on
//! Tokio and must **not** be run through this pool — that fights DataFusion's
//! scheduler and memory accounting.
//!
//! There is exactly one pool for the whole process (no per-crate pools). It is
//! sized coherently with the slot model so that, under full slot occupancy,
//! `slots × DF-partitions × pool-threads` does not blow past the core count:
//! the default reserves one core for the Tokio reactor + coordination, and the
//! `KRISHIV_COMPUTE_THREADS` env override pins it explicitly.

use std::sync::OnceLock;

/// Env override for the compute-pool thread count. `0`/unset auto-sizes from the
/// available core count (reserving one for the async reactor).
pub const COMPUTE_THREADS_ENV: &str = "KRISHIV_COMPUTE_THREADS";

static POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();

/// Resolve the configured compute-pool thread count.
///
/// `KRISHIV_COMPUTE_THREADS` (when `> 0`) wins; otherwise auto-size to
/// `max(1, cores − 1)` — one core is left for the Tokio reactor and scheduler
/// coordination so pool work never fully starves async I/O.
pub fn configured_compute_threads() -> usize {
    if let Some(n) = crate::env_registry::env_usize(COMPUTE_THREADS_ENV).filter(|&n| n > 0) {
        return n;
    }
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    cores.saturating_sub(1).max(1)
}

/// The process-global compute pool, built lazily on first use.
pub fn compute_pool() -> &'static rayon::ThreadPool {
    POOL.get_or_init(|| {
        let threads = configured_compute_threads();
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("krishiv-compute-{i}"))
            .build()
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, "failed to build the krishiv compute pool; aborting process");
                std::process::abort()
            })
    })
}

/// Run a CPU-bound closure on the compute pool and await its result from an
/// async context **without blocking the Tokio reactor**.
///
/// The closure is dispatched to the Rayon pool and its result delivered back
/// over a oneshot channel, so the calling Tokio worker yields instead of
/// spinning on the kernel. Prefer this over letting a heavy serial kernel run
/// inline on a Tokio worker (stalls the reactor) or shipping it to
/// `spawn_blocking` (whose thread pool grows unboundedly and is meant for
/// blocking *I/O*, not CPU work).
pub async fn run_on_compute_pool<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    compute_pool().spawn(move || {
        // Ignore send errors: they only occur if the awaiter was dropped
        // (e.g. the task was cancelled), in which case the result is unwanted.
        let _ = tx.send(f());
    });
    // The sender is only dropped without sending if the kernel closure panicked
    // — and with `panic = "abort"` the process is already gone by then, so this
    // arm is unreachable in practice. Handle it explicitly rather than unwrap.
    rx.await.unwrap_or_else(|_| {
        tracing::error!("compute-pool worker dropped its result sender without sending; aborting");
        std::process::abort()
    })
}

/// Map `items` in parallel on the compute pool, preserving input order.
///
/// A thin, order-preserving convenience over `rayon`'s parallel iterator for
/// the common "serial `for` over N independent buckets" kernel shape (e.g. the
/// shuffle writer's per-bucket sort→encode→compress). Runs **synchronously** on
/// the caller — intended for kernels already off the async path (inside a
/// `spawn_blocking`/pool context or a sync writer flush). From async code, wrap
/// the whole call in [`run_on_compute_pool`] instead.
pub fn par_map<I, T, R, F>(items: I, f: F) -> Vec<R>
where
    I: IntoIterator<Item = T>,
    T: Send,
    R: Send,
    F: Fn(T) -> R + Sync + Send,
{
    use rayon::prelude::*;
    let items: Vec<T> = items.into_iter().collect();
    compute_pool().install(|| items.into_par_iter().map(f).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn configured_threads_is_at_least_one() {
        // Auto-sized (env unset in the test process) must never be 0.
        assert!(configured_compute_threads() >= 1);
    }

    #[test]
    fn compute_pool_is_a_singleton() {
        let a = compute_pool() as *const rayon::ThreadPool;
        let b = compute_pool() as *const rayon::ThreadPool;
        assert_eq!(a, b, "there must be exactly one process-global pool");
        assert!(compute_pool().current_num_threads() >= 1);
    }

    #[test]
    fn par_map_preserves_order_and_runs_all() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let out = par_map(0..1000i64, move |x| {
            c.fetch_add(1, Ordering::Relaxed);
            x * 2
        });
        assert_eq!(out.len(), 1000);
        assert_eq!(calls.load(Ordering::Relaxed), 1000, "every item processed");
        // Order preserved despite parallel execution.
        assert!(out.iter().enumerate().all(|(i, &v)| v == (i as i64) * 2));
    }

    #[tokio::test]
    async fn run_on_compute_pool_returns_result_without_blocking() {
        // A CPU kernel dispatched from async returns its value.
        let sum = run_on_compute_pool(|| (0u64..1_000_000).sum::<u64>()).await;
        assert_eq!(sum, 499_999_500_000);
    }

    #[tokio::test]
    async fn many_concurrent_pool_dispatches_all_complete() {
        // The reactor stays responsive while N kernels run on the pool.
        let mut handles = Vec::new();
        for i in 0..32u64 {
            handles.push(tokio::spawn(async move {
                run_on_compute_pool(move || (0..=i).sum::<u64>()).await
            }));
        }
        let mut total = 0u64;
        for h in handles {
            total += h.await.unwrap();
        }
        // Σ_{i=0..31} (i(i+1)/2)
        let expected: u64 = (0..32u64).map(|i| i * (i + 1) / 2).sum();
        assert_eq!(total, expected);
    }
}
