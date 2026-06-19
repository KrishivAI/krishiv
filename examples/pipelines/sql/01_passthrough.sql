-- 01 · Hello pipeline: source → passthrough view → sink (simplest DP).
CREATE SOURCE orders AS
  SELECT * FROM (VALUES (1, 100), (2, 50), (3, 25)) AS t(id, amount);
CREATE INCREMENTAL VIEW all_orders AS SELECT id, amount FROM orders;
CREATE SINK out FROM all_orders;
START PIPELINE out;
