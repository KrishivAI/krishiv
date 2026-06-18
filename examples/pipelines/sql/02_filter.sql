-- 02 · Filter: keep only large orders.
CREATE SOURCE orders AS
  SELECT * FROM (VALUES (1, 100), (2, 50), (3, 250)) AS t(id, amount);
CREATE INCREMENTAL VIEW big_orders AS SELECT id, amount FROM orders WHERE amount >= 100;
CREATE SINK out FROM big_orders;
START PIPELINE out;
