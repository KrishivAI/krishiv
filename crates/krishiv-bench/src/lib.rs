#![forbid(unsafe_code)]

//! Benchmark harness library for Krishiv.
//!
//! This crate contains Criterion benchmark suites for TPC-H and Nexmark.
//! Binary benchmarks live under `benches/`.

/// Placeholder to satisfy `cargo check --lib`.
pub fn bench_harness() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn harness_exists() {
        bench_harness();
    }
}
