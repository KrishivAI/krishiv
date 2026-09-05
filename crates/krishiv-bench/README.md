# krishiv-bench

Benchmark harnesses and corpora gates: TPC-H (embedded and distributed),
NEXMark streaming, TPC-DS smoke, the IVM O(Δ) plan-coverage gate over the
TPC-H/NEXMark corpus, and Criterion micro-benchmarks. Binaries under
`src/bin/` (`tpch_corpus`, `tpch_verify`, `nexmark_*`, `stage_dump`,
`lateness_soak`) drive datasets pointed to by `KRISHIV_TPCH_DATA_DIR*`.

Excluded from `just test-integration`; runs in `bench.yml` / `nightly.yml`.

```bash
cargo bench -p krishiv-bench
just bench-tpch
just bench-nexmark
```

Documentation: `docs/BENCHMARKING.md`, `docs/benchmarks-tpcds.md`,
`docs/architecture/16-performance.md`.

License: Apache-2.0.
