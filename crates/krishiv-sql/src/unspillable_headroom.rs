//! A memory pool that keeps a slice of the budget out of reach of spillable
//! consumers, so an operator that *cannot* spill can still make progress.
//!
//! ## The failure this exists for
//!
//! `FairSpillPool` treats its two consumer classes asymmetrically. A spillable
//! consumer is capped at its fair share of `pool_size - unspillable`; an
//! **unspillable** one gets whatever is left after *both* classes:
//!
//! ```text
//! // datafusion-execution-54/src/memory_pool/pool.rs, FairSpillPool::try_grow
//! false => {
//!     let available = self.pool_size
//!         .saturating_sub(state.unspillable + state.spillable);
//! ```
//!
//! So N spillable consumers, each politely inside its own `pool/N` share, can
//! between them occupy the entire pool — and the pool has no way to make any of
//! them give it back. Spilling in DataFusion is driven by a consumer's *own*
//! `try_grow` failing; there is no callback the pool can invoke to reclaim.
//! The next unspillable consumer to ask for memory therefore gets zero, however
//! little it wants.
//!
//! TPC-H q10 and q11 at SF100 died exactly there:
//!
//! ```text
//! Failed to allocate additional 877.0 B for HashJoinInput with 0.0 B already
//! allocated for this reservation - 0.0 B remain available for the total
//! memory pool: fair(pool_size: 2.3 GB)
//! ```
//!
//! 877 bytes, refused by a 2.3 GB pool. A hash join build side cannot spill
//! (DataFusion has no spilling `HashJoinExec`), so these are the *small* joins
//! [`crate::spillable_join::SpillableJoinSelection`] correctly declined to
//! convert — they need very little, and there was nothing left to give.
//!
//! ## What this does
//!
//! Bounds the *total* spillable footprint at `pool_size - headroom`, leaving
//! `headroom` that only unspillable consumers can occupy. Spillable consumers
//! hit their ceiling earlier and spill, which is the behaviour they are built
//! for and already exercise; unspillable ones keep a floor they can always draw
//! on. The fair-share rule between spillable consumers is unchanged — that is
//! still `FairSpillPool`'s job, and this delegates to it.
//!
//! This is a ceiling on spillers, not a reservation: when no unspillable
//! consumer is running, the headroom simply goes unused, which costs a query
//! that spills slightly earlier than it strictly had to. That is the trade —
//! a little more spilling against queries that cannot run at all.

use std::sync::{Arc, Mutex};

use datafusion::execution::memory_pool::{MemoryPool, MemoryReservation};

/// Fraction of the pool held back for consumers that cannot spill.
///
/// A quarter, because the joins that land here are by construction the ones
/// under `SpillableJoinSelection`'s conversion threshold — anything larger has
/// already become a (spillable) sort-merge join — and a handful of those fit
/// comfortably in a quarter of any pool worth having. Too large a headroom
/// makes every spilling query spill sooner for no benefit.
pub const DEFAULT_UNSPILLABLE_HEADROOM_NUMERATOR: usize = 1;
/// Denominator of [`DEFAULT_UNSPILLABLE_HEADROOM_NUMERATOR`].
pub const DEFAULT_UNSPILLABLE_HEADROOM_DENOMINATOR: usize = 4;

/// Environment override for the headroom, as a percentage of the pool.
///
/// `0` disables it and restores plain `FairSpillPool` behaviour.
pub const UNSPILLABLE_HEADROOM_PERCENT_ENV: &str = "KRISHIV_UNSPILLABLE_HEADROOM_PERCENT";

/// Headroom in bytes for a pool of `pool_size`, honouring the env override.
#[must_use]
pub fn headroom_bytes(pool_size: usize) -> usize {
    let percent = std::env::var(UNSPILLABLE_HEADROOM_PERCENT_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|p| *p <= 100);
    match percent {
        Some(p) => pool_size / 100 * p,
        None => {
            pool_size / DEFAULT_UNSPILLABLE_HEADROOM_DENOMINATOR
                * DEFAULT_UNSPILLABLE_HEADROOM_NUMERATOR
        }
    }
}

/// See the module docs.
#[derive(Debug)]
pub struct UnspillableHeadroomPool {
    inner: Arc<dyn MemoryPool>,
    /// Ceiling on the *total* bytes held by spillable consumers.
    spillable_ceiling: usize,
    /// Bytes currently held by spillable consumers, tracked here because the
    /// inner pool does not expose the split.
    spillable_used: Mutex<usize>,
    pool_size: usize,
}

impl UnspillableHeadroomPool {
    /// Wrap `inner` (a pool of `pool_size` bytes), holding `headroom` back from
    /// spillable consumers.
    ///
    /// A `headroom` of 0, or one at least as large as the pool, disables the
    /// ceiling — the wrapper then delegates everything unchanged rather than
    /// bounding spillers to nothing, which would deadlock every spilling query.
    #[must_use]
    pub fn new(inner: Arc<dyn MemoryPool>, pool_size: usize, headroom: usize) -> Self {
        let spillable_ceiling = if headroom == 0 || headroom >= pool_size {
            pool_size
        } else {
            pool_size - headroom
        };
        Self {
            inner,
            spillable_ceiling,
            spillable_used: Mutex::new(0),
            pool_size,
        }
    }

    /// The ceiling spillable consumers are held to, in bytes.
    #[must_use]
    pub fn spillable_ceiling(&self) -> usize {
        self.spillable_ceiling
    }

    fn add_spillable(&self, additional: usize) {
        if let Ok(mut used) = self.spillable_used.lock() {
            *used = used.saturating_add(additional);
        }
    }

    fn sub_spillable(&self, shrink: usize) {
        if let Ok(mut used) = self.spillable_used.lock() {
            *used = used.saturating_sub(shrink);
        }
    }
}

impl std::fmt::Display for UnspillableHeadroomPool {
    /// Mirrors `FairSpillPool`'s form, with the ceiling — this string is what
    /// an exhaustion error prints, and "fair(pool_size: 2.3 GB)" alone was not
    /// enough to tell q10's failure apart from a genuinely full pool.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "fair+unspillable-headroom(pool_size: {}, spillable_ceiling: {})",
            human_bytes(self.pool_size),
            human_bytes(self.spillable_ceiling)
        )
    }
}

impl MemoryPool for UnspillableHeadroomPool {
    fn name(&self) -> &str {
        "fair+unspillable-headroom"
    }

    fn register(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        // Infallible by contract, so the ceiling cannot be enforced here — only
        // tracked, so `try_grow` keeps seeing the truth.
        if reservation.consumer().can_spill() {
            self.add_spillable(additional);
        }
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        if reservation.consumer().can_spill() {
            self.sub_spillable(shrink);
        }
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::error::Result<()> {
        if !reservation.consumer().can_spill() {
            return self.inner.try_grow(reservation, additional);
        }
        // Hold the spillable counter across the delegated call so a concurrent
        // spiller cannot slip past the ceiling between the check and the grow.
        // Lock order is always ours-then-inner's, so this cannot deadlock.
        let Ok(mut used) = self.spillable_used.lock() else {
            return self.inner.try_grow(reservation, additional);
        };
        let requested = used.saturating_add(additional);
        if requested > self.spillable_ceiling {
            return Err(datafusion::error::DataFusionError::ResourcesExhausted(
                format!(
                    "spillable consumers are capped at {} of the {} pool so that operators \
                 which cannot spill (hash join build sides) keep a usable floor; \
                 '{}' asked for {additional} more with {} already held across all \
                 spillable consumers. This consumer should spill. Set {}=0 to \
                 restore unbounded fair-share behaviour.",
                    human_bytes(self.spillable_ceiling),
                    human_bytes(self.pool_size),
                    reservation.consumer().name(),
                    human_bytes(*used),
                    UNSPILLABLE_HEADROOM_PERCENT_ENV,
                ),
            ));
        }
        self.inner.try_grow(reservation, additional)?;
        *used = requested;
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

fn human_bytes(bytes: usize) -> String {
    const MIB: usize = 1024 * 1024;
    if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::{FairSpillPool, MemoryConsumer};

    fn pool(size: usize, headroom: usize) -> Arc<dyn MemoryPool> {
        Arc::new(UnspillableHeadroomPool::new(
            Arc::new(FairSpillPool::new(size)),
            size,
            headroom,
        ))
    }

    /// The q10/q11 failure, reproduced against the unwrapped pool and fixed by
    /// the wrapper.
    ///
    /// A spillable consumer takes everything `FairSpillPool` will give it —
    /// which is the whole pool when it is the only spiller — and a hash join
    /// build side is then refused a few hundred bytes.
    #[test]
    fn a_spiller_cannot_starve_a_consumer_that_cannot_spill() {
        const SIZE: usize = 1024 * 1024;

        // Without headroom: the spiller takes the pool and the join gets nothing.
        let bare: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(SIZE));
        let spiller = MemoryConsumer::new("ShuffleWriteBuffer")
            .with_can_spill(true)
            .register(&bare);
        spiller
            .try_grow(SIZE)
            .expect("the only spiller may take it all");
        let join = MemoryConsumer::new("HashJoinInput").register(&bare);
        let error = join
            .try_grow(877)
            .expect_err("this is the q10/q11 failure and it must reproduce");
        assert!(error.to_string().contains("HashJoinInput"), "got: {error}");

        // With headroom: the spiller is capped, and the join is served.
        let guarded = pool(SIZE, SIZE / 4);
        let spiller = MemoryConsumer::new("ShuffleWriteBuffer")
            .with_can_spill(true)
            .register(&guarded);
        let error = spiller
            .try_grow(SIZE)
            .expect_err("a spiller must not be able to take the whole pool");
        assert!(
            error.to_string().contains("cannot spill"),
            "the refusal must say why, got: {error}"
        );
        spiller
            .try_grow(SIZE / 4 * 3)
            .expect("up to the ceiling is still allowed");
        let join = MemoryConsumer::new("HashJoinInput").register(&guarded);
        join.try_grow(877)
            .expect("the headroom exists precisely for this");
    }

    /// Several spillers, each inside its own fair share, must not sum past the
    /// ceiling — the sum is what starved the join, not any single consumer.
    #[test]
    fn the_ceiling_bounds_spillers_in_aggregate_not_individually() {
        const SIZE: usize = 1024 * 1024;
        let guarded = pool(SIZE, SIZE / 4);
        let mut held = Vec::new();
        for i in 0..4 {
            let c = MemoryConsumer::new(format!("spiller{i}"))
                .with_can_spill(true)
                .register(&guarded);
            // A quarter each: individually fine, collectively over the ceiling.
            if i < 3 {
                c.try_grow(SIZE / 4).expect("within the ceiling");
            } else {
                c.try_grow(SIZE / 4)
                    .expect_err("the fourth quarter crosses the ceiling");
            }
            held.push(c);
        }
        let join = MemoryConsumer::new("HashJoinInput").register(&guarded);
        join.try_grow(SIZE / 8).expect("headroom is intact");
    }

    /// Shrinking returns capacity to the spillable budget; a consumer that
    /// spilled must be able to grow again afterwards.
    #[test]
    fn shrinking_returns_capacity_to_the_spillable_budget() {
        const SIZE: usize = 1024 * 1024;
        let guarded = pool(SIZE, SIZE / 4);
        let spiller = MemoryConsumer::new("s")
            .with_can_spill(true)
            .register(&guarded);
        spiller.try_grow(SIZE / 4 * 3).expect("fills the ceiling");
        spiller
            .try_grow(1)
            .expect_err("nothing left under the ceiling");
        spiller.shrink(SIZE / 2); // it spilled
        spiller
            .try_grow(SIZE / 4)
            .expect("capacity came back after spilling");
    }

    /// **Every** bounded `EngineMemory` must install the guard — most of all
    /// `Private`, which is what the executor uses.
    ///
    /// The first version of this fix wrapped only `EngineMemory::shared_pool`.
    /// Every executor task engine is built with `EngineMemory::Private`
    /// (krishiv-executor `task_sql_engine`), so the protection was absent from
    /// the one deployment q10 and q11 fail in — the code shipped, the tests
    /// passed, and nothing on the cluster changed. Asserting behaviour through
    /// the real constructor is what makes that visible.
    #[test]
    fn both_bounded_engine_memories_install_the_guard() {
        const SIZE: usize = 1024 * 1024;
        for (label, pool) in [
            ("Private", crate::EngineMemory::Private(SIZE).pool()),
            ("Shared", Some(crate::EngineMemory::shared_pool(SIZE))),
        ] {
            let pool = pool.unwrap_or_else(|| panic!("{label} must install a pool"));
            assert_eq!(
                pool.name(),
                "fair+unspillable-headroom",
                "{label} installed an unguarded pool"
            );
            // Behaviour, not just the name: a lone spiller must be refused the
            // whole budget so a hash join build side keeps a floor.
            let spiller = MemoryConsumer::new("s")
                .with_can_spill(true)
                .register(&pool);
            assert!(
                spiller.try_grow(SIZE).is_err(),
                "{label}: a lone spiller took the entire pool, so the guard is absent"
            );
            spiller
                .try_grow(SIZE / 4 * 3)
                .unwrap_or_else(|e| panic!("{label}: the ceiling itself must be reachable — {e}"));
            let join = MemoryConsumer::new("HashJoinInput").register(&pool);
            join.try_grow(877)
                .unwrap_or_else(|e| panic!("{label}: headroom absent — {e}"));
        }
    }

    /// Zero headroom is the documented escape hatch and must behave exactly
    /// like the pool it wraps.
    #[test]
    fn zero_headroom_delegates_unchanged() {
        const SIZE: usize = 1024 * 1024;
        let guarded = pool(SIZE, 0);
        let spiller = MemoryConsumer::new("s")
            .with_can_spill(true)
            .register(&guarded);
        spiller
            .try_grow(SIZE)
            .expect("with no headroom a lone spiller may still take everything");
    }

    /// A headroom larger than the pool must not bound spillers to nothing.
    #[test]
    fn absurd_headroom_does_not_deadlock_every_spiller() {
        const SIZE: usize = 1024 * 1024;
        let guarded = pool(SIZE, SIZE * 4);
        let spiller = MemoryConsumer::new("s")
            .with_can_spill(true)
            .register(&guarded);
        spiller
            .try_grow(SIZE)
            .expect("ceiling disabled, not zeroed");
    }
}
