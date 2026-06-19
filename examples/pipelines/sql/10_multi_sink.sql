-- 10 · Multiple sinks: revenue and order count from one source.
CREATE SOURCE orders AS
  SELECT * FROM (VALUES (1,'US',100),(2,'EU',50),(3,'US',25)) AS t(id, region, amount);
CREATE INCREMENTAL VIEW revenue AS SELECT SUM(amount) AS total FROM orders;
CREATE INCREMENTAL VIEW counts AS SELECT region, COUNT(*) AS n FROM orders GROUP BY region;
CREATE SINK revenue_out FROM revenue;
CREATE SINK counts_out FROM counts;
START PIPELINE revenue_out;
START PIPELINE counts_out;
