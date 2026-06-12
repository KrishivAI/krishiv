# Benchmarking and Performance Evidence

Performance claims must be reproducible. A benchmark result without the source
revision, command, hardware, dataset, and configuration is diagnostic data—not
a published project claim.

## Benchmark suites

- Criterion microbenchmarks: `cargo bench -p krishiv-bench`
- TPC-H batch SQL harness: `just bench-tpch`
- Nexmark streaming harness: `just bench-nexmark`

Use generated data only for development. Published comparisons must state the
dataset generator/version, scale factor, storage format, partitioning, and
whether caches were warm.

## Reproducible run

Create a machine-readable environment record before the run:

```bash
python3 scripts/benchmark_manifest.py --suite criterion \
  --command "cargo bench -p krishiv-bench" \
  --output target/benchmark-manifest.json
cargo bench -p krishiv-bench
```

The manifest records the commit, dirty-worktree state, Rust version, operating
system, CPU, suite, command, and UTC timestamp. Add workload-specific settings
such as scale factor, object-store endpoint, executor count, slots, memory, and
checkpoint interval to the result notes.

## Pull-request policy

- Correctness tests always take precedence over benchmark improvements.
- A statistically credible regression in a critical operator requires an
  explanation or a follow-up issue before merge.
- Do not compare unlike hardware, dependency versions, datasets, or execution
  modes as if they were equivalent.
- Benchmark artifacts are retained by CI for inspection; CI does not yet provide
  a permanent historical performance database.

## Publishing comparisons

When comparing Krishiv with Spark, Flink, or another engine, publish all engine
versions, equivalent semantics, configuration files, queries, raw output, and
reproduction commands. Clearly separate batch latency, streaming throughput,
checkpoint cost, recovery time, and resource consumption.
