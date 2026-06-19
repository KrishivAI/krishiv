-- 04 · Group-by: revenue per region.
CREATE SOURCE orders AS
  SELECT * FROM (VALUES (1,'US',100),(2,'EU',50),(3,'US',25),(4,'EU',75)) AS t(id, region, amount);
CREATE INCREMENTAL VIEW by_region AS
  SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM orders GROUP BY region;
CREATE SINK out FROM by_region;
START PIPELINE out;
