-- 11 · HAVING: regions with revenue over a threshold.
CREATE SOURCE orders AS
  SELECT * FROM (VALUES (1,'US',100),(2,'EU',50),(3,'US',80),(4,'APAC',10)) AS t(id, region, amount);
CREATE INCREMENTAL VIEW big_regions AS
  SELECT region, SUM(amount) AS total FROM orders GROUP BY region HAVING SUM(amount) > 60;
CREATE SINK out FROM big_regions;
START PIPELINE out;
