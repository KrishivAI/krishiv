# krishiv-bench

Criterion-based benchmarks for Krishiv's SQL and runtime performance.

## Overview

`krishiv-bench` contains performance benchmarks:

- TPC-H (SF 10) — analytical query throughput
- TPC-H distributed — multi-executor scaling
- Nexmark — streaming window aggregation
- TPC-DS smoke — additional query coverage

## Usage

```bash
# Run all benchmarks
cargo bench -p krishiv-bench

# Save a baseline
cargo bench -p krishiv-bench -- --save-baseline main

# Compare against baseline
cargo bench -p krishiv-bench -- --baseline main
```

This crate is for testing only and is excluded from production builds.

## License

Apache-2.0
