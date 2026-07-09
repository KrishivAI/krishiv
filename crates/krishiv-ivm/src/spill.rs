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

use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

/// DataFusion's default merge-phase sort reservation (10 MiB); a pool smaller
/// than ~4x this would fail sorts outright instead of spilling.
const DEFAULT_SORT_SPILL_RESERVATION_BYTES: usize = 10 * 1024 * 1024;
const MIN_SORT_SPILL_RESERVATION_BYTES: usize = 64 * 1024;

/// Resolve the IVM tick memory limit: `KRISHIV_QUERY_MEMORY_LIMIT_BYTES`
/// when set (`0`/unparseable → unlimited), else 25% of the cgroup memory
/// limit when the process runs in a memory-limited container.
pub fn ivm_memory_limit_bytes() -> Option<usize> {
    match std::env::var("KRISHIV_QUERY_MEMORY_LIMIT_BYTES").ok() {
        Some(raw) => raw.trim().parse::<usize>().ok().filter(|&n| n > 0),
        None => krishiv_common::cgroup_memory_limit_bytes()
            .map(|limit| (limit / 4) as usize)
            .filter(|&n| n > 0),
    }
}

/// Build a `SessionContext` whose memory pool spills to disk at `limit`
/// bytes; `None` limit returns a default unbounded context.
pub fn spill_session_context_with_limit(limit: Option<usize>) -> SessionContext {
    let Some(limit) = limit else {
        return SessionContext::new();
    };
    let scaled = (limit / 4).clamp(
        MIN_SORT_SPILL_RESERVATION_BYTES,
        DEFAULT_SORT_SPILL_RESERVATION_BYTES,
    );
    let config = SessionConfig::new().with_sort_spill_reservation_bytes(scaled);
    let runtime_env = match RuntimeEnvBuilder::new()
        .with_memory_pool(std::sync::Arc::new(FairSpillPool::new(limit)))
        .build_arc()
    {
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
    use super::*;

    #[tokio::test]
    async fn limited_context_executes_sql() {
        let ctx = spill_session_context_with_limit(Some(64 * 1024 * 1024));
        let batches = ctx
            .sql("SELECT 1 AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn unlimited_context_is_default() {
        let ctx = spill_session_context_with_limit(None);
        let batches = ctx
            .sql("SELECT 1 AS v")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn tiny_pool_spills_or_errors_instead_of_growing() {
        // A 2 MiB pool cannot hold a 100k-row sort's working set in memory;
        // with the spill pool it must either complete (spilled) or fail with
        // a resources error — never grow unbounded. Completing is the
        // expected outcome; accepting a resources error keeps the test
        // honest about DataFusion version behavior differences.
        let ctx = spill_session_context_with_limit(Some(2 * 1024 * 1024));
        let result = ctx
            .sql(
                "SELECT v FROM (SELECT CAST(random() * 1000000 AS BIGINT) AS v \
                 FROM (SELECT unnest(range(0, 100000)) )) ORDER BY v",
            )
            .await;
        match result {
            Ok(df) => {
                let collected = df.collect().await;
                if let Err(e) = collected {
                    let msg = e.to_string().to_lowercase();
                    assert!(
                        msg.contains("resources") || msg.contains("memory"),
                        "unexpected failure kind: {msg}"
                    );
                }
            }
            Err(e) => panic!("planning failed: {e}"),
        }
    }
}
