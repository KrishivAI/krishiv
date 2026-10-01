# TPC-H SF100 — Sail vs Krishiv vs DuckDB on DataFusion 55, 2026-10-02

Same box and method as `tpch-sf100-sail-vs-krishiv-2026-10-01.md`, now with both engines on DataFusion 55.1 / arrow 59.2 (Krishiv `ad76eca`, Sail 0.7.2). Three interleaved passes; median per query; peak RSS is the largest of the three passes, sampled from the engine process every 50 ms. Krishiv `KRISHIV_JOIN_REORDER=off`; Sail join reorder on, plus a separate single pass with it off (last column). Krishiv and Sail agree on all 22 answers; both differ from DuckDB on q1 by `avg_disc` rounding only. Zero answer changes vs the 2026-10-01 run for every engine.

| q | Krishiv s | Sail s | DuckDB s | Sail/Krishiv | Krishiv RSS GB | Sail RSS GB | DuckDB RSS GB | Sail reorder-off s |
|---|---|---|---|---|---|---|---|---|
| q1 | 21.2 | 20.8 | 11.1 | 0.98 | 0.3 | 0.7 | 1.5 | 19.4 |
| q2 | 3.0 | 3.1 | 2.0 | 1.04 | 0.5 | 2.1 | 1.9 | 4.2 |
| q3 | 11.5 | 9.9 | 8.0 | 0.86 | 1.3 | 2.1 | 2.7 | 9.6 |
| q4 | 6.3 | 4.8 | 5.2 | 0.75 | 0.7 | 2.0 | 3.2 | 4.5 |
| q5 | 17.6 | 15.9 | 9.3 | 0.91 | 2.3 | 4.8 | 3.0 | 15.8 |
| q6 | 7.3 | 5.5 | 3.8 | 0.75 | 0.3 | 4.9 | 3.0 | 5.4 |
| q7 | 31.2 | 12.7 | 9.2 | 0.41 | 8.3 | 3.4 | 3.9 | 30.1 |
| q8 | 18.3 | 17.2 | 11.0 | 0.94 | 4.3 | 3.2 | 3.6 | 16.0 |
| q9 | 34.1 | 27.5 | 21.5 | 0.81 | 11.5 | 11.8 | 16.3 | 29.5 |
| q10 | 14.8 | 11.2 | 9.5 | 0.76 | 4.3 | 8.8 | 7.4 | 12.5 |
| q11 | 4.0 | 2.7 | 1.9 | 0.68 | 0.5 | 4.6 | 4.4 | 4.3 |
| q12 | 9.9 | 8.9 | 4.9 | 0.90 | 0.5 | 1.9 | 3.1 | 8.9 |
| q13 | 16.2 | 11.7 | 12.8 | 0.72 | 1.3 | 3.1 | 8.4 | 11.6 |
| q14 | 6.9 | 5.7 | 6.2 | 0.83 | 1.1 | 3.1 | 8.3 | 5.6 |
| q15 | 6.9 | 11.7 | 6.0 | 1.69 | 0.4 | 2.3 | 4.3 | 11.7 |
| q16 | 2.8 | 2.0 | 2.6 | 0.74 | 1.8 | 3.5 | 4.5 | 2.0 |
| q17 | 17.2 | 29.5 | 9.3 | 1.71 | 0.4 | 3.5 | 4.4 | 29.4 |
| q18 | 25.9 | 40.7 | 14.2 | 1.57 | 10.6 | 24.6 | 11.2 | 40.4 |
| q19 | 10.7 | 12.1 | 8.7 | 1.12 | 0.4 | 11.2 | 4.6 | 12.1 |
| q20 | 12.2 | 10.0 | 8.0 | 0.82 | 3.7 | 6.5 | 4.1 | 9.9 |
| q21 | 52.7 | 30.1 | 25.1 | 0.57 | 25.2 | 30.7 | 7.3 | 43.8 |
| q22 | 2.5 | 2.1 | 3.1 | 0.86 | 0.4 | 6.1 | 5.0 | 1.9 |
| **sum of medians** | **333** | **296** | **193** | 0.89 | peak 25.2 | peak 30.7 | peak 16.3 | **329** |

Sail faster by >10% on 14 queries, slower by >10% on 4. With its join reorder off Sail's total is 329 s against Krishiv's 333 s with reorder off: the aggregate gap is the reorder.

