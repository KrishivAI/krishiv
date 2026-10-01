# TPC-H SF100 — Sail vs Krishiv vs DuckDB, 2026-10-01

Box: 12 cores, 61 GB, data `/home/gopal/krishiv-bench-data/tpch/sf100` (39 GB
Parquet). Engines run serially, passes interleaved (`--interleave`). Krishiv
`KRISHIV_JOIN_REORDER=off` (the size-only rule of `ea8720e` regressed q5/q8/q9/q10;
see `docs/engineering-log/status.md` 2026-10-01). Sail 
`SAIL_OPTIMIZER__ENABLE_JOIN_REORDER=true` in `local` mode, as in its own
published benchmark. DuckDB default settings.

Medians over every completed sample: three interleaved passes (the run was
OOM-killed during Sail q21 of pass 3 — Sail reached 30 GB RSS) plus the one-pass
run that produced the JSON next to this file. Sample count in parentheses.
Digests (JSON): Krishiv and Sail agree on all 22 queries; both differ from DuckDB
on q1 only, a 6th-decimal rounding of `avg_disc`.

| q | Krishiv s | Sail s | DuckDB s | Sail/Krishiv |
|---|---|---|---|---|
| q1 | 21.5 (4) | 21.1 (4) | 12.1 (3) | 0.98 |
| q2 | 3.4 (4) | 3.5 (4) | 2.3 (3) | 1.02 |
| q3 | 12.1 (4) | 9.8 (4) | 8.4 (3) | 0.81 |
| q4 | 6.5 (4) | 4.7 (4) | 5.8 (3) | 0.72 |
| q5 | 17.9 (4) | 16.7 (4) | 9.7 (3) | 0.93 |
| q6 | 7.3 (4) | 5.7 (4) | 3.7 (3) | 0.78 |
| q7 | 31.0 (4) | 12.7 (4) | 9.1 (3) | 0.41 |
| q8 | 18.8 (4) | 17.4 (4) | 11.4 (3) | 0.93 |
| q9 | 34.0 (4) | 30.3 (4) | 23.1 (3) | 0.89 |
| q10 | 14.9 (4) | 11.6 (4) | 9.7 (3) | 0.77 |
| q11 | 4.3 (4) | 2.9 (4) | 2.1 (3) | 0.67 |
| q12 | 10.9 (4) | 9.5 (4) | 5.8 (3) | 0.87 |
| q13 | 16.0 (4) | 12.0 (4) | 13.0 (3) | 0.75 |
| q14 | 7.6 (4) | 6.3 (4) | 7.1 (3) | 0.83 |
| q15 | 7.2 (4) | 11.9 (4) | 6.2 (3) | 1.65 |
| q16 | 3.0 (4) | 2.4 (4) | 2.6 (3) | 0.81 |
| q17 | 17.1 (4) | 32.2 (4) | 9.0 (3) | 1.88 |
| q18 | 26.8 (4) | 46.6 (4) | 14.8 (3) | 1.74 |
| q19 | 12.5 (4) | 13.6 (4) | 9.9 (3) | 1.09 |
| q20 | 13.1 (4) | 10.4 (4) | 8.0 (3) | 0.79 |
| q21 | 43.3 (4) | 36.3 (3) | 26.2 (3) | 0.84 |
| q22 | 2.7 (4) | 2.4 (3) | 3.2 (3) | 0.89 |
| **sum of medians** | **332** | **320** | **203** | 0.96 |
