# Join reorder with filter selectivity — A/B, 2026-10-02

`old` = `ad76eca` (output-estimate rule, no selectivity, inversion guard); `new` = this commit. Both binaries `KRISHIV_JOIN_REORDER=on`, one process per query, order alternating each round, median of rounds (`skills/benchmarking/ab_krishiv.py`). Every row's answer digest is identical between binaries.

## TPC-H SF100, 2 rounds — sum of medians: old 331.6 s, new 296.6 s (1.12x); digest mismatches: 0

| query | old med s | new med s | new/old | answer |
|---|---|---|---|---|
| pricing_summary | 20.9 | 19.9 | 0.95 | same |
| minimum_cost_supplier | 3.0 | 3.0 | 1.00 | same |
| shipping_priority | 11.4 | 11.6 | 1.02 | same |
| order_priority_checking | 6.0 | 6.2 | 1.03 | same |
| local_supplier_volume | 17.7 | 17.9 | 1.01 | same |
| forecasting_revenue_change | 7.1 | 7.1 | 1.00 | same |
| volume_shipping | 31.4 | 13.7 | 0.44 | same |
| national_market_share | 18.5 | 18.2 | 0.98 | same |
| product_type_profit_measure | 33.6 | 33.9 | 1.01 | same |
| returned_item_reporting | 14.3 | 14.3 | 1.00 | same |
| important_stock_identification | 4.2 | 2.2 | 0.54 | same |
| shipping_modes_and_order_priority | 9.9 | 10.3 | 1.04 | same |
| customer_distribution | 15.9 | 16.5 | 1.04 | same |
| promotion_effect | 7.2 | 7.2 | 1.01 | same |
| top_supplier | 6.8 | 7.2 | 1.07 | same |
| parts_supplier_relationship | 2.6 | 2.7 | 1.06 | same |
| small_quantity_order_revenue | 17.2 | 17.0 | 0.99 | same |
| large_volume_customer | 25.6 | 25.9 | 1.01 | same |
| discounted_revenue | 10.9 | 11.1 | 1.02 | same |
| potential_part_promotion | 12.5 | 12.2 | 0.98 | same |
| suppliers_who_kept_orders_waiting | 52.5 | 35.7 | 0.68 | same |
| global_sales_opportunity | 2.6 | 2.6 | 1.01 | same |

Plans change on q7, q8, q11 and q21 only.

## TPC-DS SF1, 3 rounds — sum of medians: old 12.9 s, new 12.5 s (1.03x); digest mismatches: 0

| query | old med ms | new med ms | new/old | answer |
|---|---|---|---|---|
| q1 | 40 | 40 | 1.01 | same |
| q2 | 90 | 80 | 0.83 | same |
| q3 | 70 | 60 | 0.86 | same |
| q4 | 650 | 510 | 0.79 | same |
| q5 | 110 | 110 | 0.99 | same |
| q6 | 80 | 80 | 1.06 | same |
| q7 | 140 | 190 | 1.35 | same |
| q8 | 80 | 80 | 1.07 | same |
| q9 | 220 | 240 | 1.09 | same |
| q10 | 100 | 100 | 0.97 | same |
| q11 | 400 | 310 | 0.77 | same |
| q12 | 60 | 50 | 0.82 | same |
| q13 | 160 | 160 | 0.99 | same |
| q14 | 300 | 260 | 0.86 | same |
| q15 | 40 | 40 | 0.91 | same |
| q16 | 40 | 40 | 0.97 | same |
| q17 | 180 | 120 | 0.66 | same |
| q18 | 120 | 100 | 0.84 | same |
| q19 | 80 | 80 | 1.06 | same |
| q20 | 50 | 50 | 1.08 | same |
| q21 | 40 | 40 | 1.03 | same |
| q22 | 190 | 200 | 1.03 | same |
| q23 | 370 | 360 | 0.98 | same |
| q24 | 180 | 200 | 1.07 | same |
| q25 | 170 | 170 | 0.98 | same |
| q26 | 70 | 70 | 0.99 | same |
| q27 | 140 | 180 | 1.29 | same |
| q28 | 240 | 270 | 1.10 | same |
| q29 | 200 | 120 | 0.58 | same |
| q30 | 60 | 60 | 0.99 | same |
| q31 | 160 | 170 | 1.07 | same |
| q32 | 40 | 40 | 0.94 | same |
| q33 | 80 | 90 | 1.12 | same |
| q34 | 90 | 90 | 0.97 | same |
| q35 | 100 | 110 | 1.07 | same |
| q36 | 90 | 100 | 1.04 | same |
| q37 | 50 | 40 | 0.93 | same |
| q38 | 90 | 100 | 1.09 | same |
| q39 | 110 | 110 | 0.98 | same |
| q40 | 50 | 50 | 1.00 | same |
| q41 | 30 | 30 | 0.93 | same |
| q42 | 50 | 60 | 1.16 | same |
| q43 | 70 | 80 | 1.10 | same |
| q44 | 120 | 120 | 1.00 | same |
| q45 | 50 | 50 | 1.04 | same |
| q46 | 120 | 120 | 0.95 | same |
| q47 | 200 | 180 | 0.86 | same |
| q48 | 140 | 150 | 1.06 | same |
| q49 | 120 | 130 | 1.03 | same |
| q50 | 130 | 90 | 0.67 | same |
| q51 | 350 | 370 | 1.04 | same |
| q52 | 50 | 50 | 0.92 | same |
| q53 | 70 | 70 | 1.00 | same |
| q54 | 80 | 90 | 1.07 | same |
| q55 | 60 | 50 | 0.92 | same |
| q56 | 90 | 90 | 1.00 | same |
| q57 | 90 | 90 | 0.94 | same |
| q58 | 100 | 100 | 1.09 | same |
| q59 | 110 | 120 | 1.10 | same |
| q60 | 90 | 90 | 1.02 | same |
| q61 | 180 | 120 | 0.66 | same |
| q62 | 60 | 50 | 0.91 | same |
| q63 | 70 | 70 | 0.92 | same |
| q64 | 490 | 500 | 1.02 | same |
| q65 | 140 | 120 | 0.92 | same |
| q66 | 120 | 100 | 0.82 | same |
| q67 | 530 | 500 | 0.95 | same |
| q68 | 130 | 120 | 0.95 | same |
| q69 | 90 | 80 | 0.95 | same |
| q70 | 130 | 120 | 0.89 | same |
| q71 | 80 | 80 | 0.95 | same |
| q72 | 210 | 250 | 1.18 | same |
| q73 | 90 | 90 | 0.97 | same |
| q74 | 230 | 190 | 0.83 | same |
| q75 | 210 | 220 | 1.04 | same |
| q76 | 80 | 80 | 0.99 | same |
| q77 | 90 | 100 | 1.06 | same |
| q78 | 320 | 330 | 1.03 | same |
| q79 | 110 | 120 | 1.13 | same |
| q80 | 210 | 230 | 1.11 | same |
| q81 | 60 | 60 | 1.04 | same |
| q82 | 50 | 60 | 1.11 | same |
| q83 | 70 | 60 | 0.91 | same |
| q84 | 60 | 50 | 0.94 | same |
| q85 | 120 | 100 | 0.79 | same |
| q86 | 40 | 40 | 1.03 | same |
| q87 | 110 | 100 | 0.91 | same |
| q88 | 270 | 270 | 1.00 | same |
| q89 | 90 | 90 | 1.03 | same |
| q90 | 40 | 40 | 0.97 | same |
| q91 | 70 | 70 | 1.06 | same |
| q92 | 40 | 40 | 0.96 | same |
| q93 | 130 | 130 | 1.05 | same |
| q94 | 50 | 60 | 1.23 | same |
| q95 | 190 | 190 | 0.96 | same |
| q96 | 50 | 50 | 0.99 | same |
| q97 | 100 | 100 | 1.03 | same |
| q98 | 100 | 100 | 1.09 | same |
| q99 | 60 | 60 | 0.97 | same |

28 plans change (q3 q4 q7 q11 q13 q15 q16 q17 q18 q21 q23 q24 q25 q29 q45 q48 q49 q50 q61 q66 q72 q74 q84 q85 q90 q91 q94 q95). Two intermediate rules were measured and rejected on the way: capping distinct counts by each side's own rows doubled TPC-H q9 (33 → 65 s); letting any relation anchor the chain made TPC-DS q72 18× slower (210 ms → 3.8 s). See the module docs of `crates/krishiv-sql/src/join_reorder.rs`.

