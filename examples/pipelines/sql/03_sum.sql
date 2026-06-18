-- 03 · Aggregate: total revenue (SUM).
CREATE SOURCE orders AS
  SELECT * FROM (VALUES (1, 100), (2, 50), (3, 25)) AS t(id, amount);
CREATE INCREMENTAL VIEW revenue AS SELECT SUM(amount) AS total FROM orders;
CREATE SINK out FROM revenue;
START PIPELINE out;
