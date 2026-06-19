-- 07 · Chained views: clean → aggregate (intermediate transform).
CREATE SOURCE raw AS
  SELECT * FROM (VALUES (1, 100), (2, -5), (3, 25)) AS t(id, amount);
CREATE INCREMENTAL VIEW clean AS SELECT id, amount FROM raw WHERE amount > 0;
CREATE INCREMENTAL VIEW total AS SELECT SUM(amount) AS s, COUNT(*) AS n FROM clean;
CREATE SINK out FROM total;
START PIPELINE out;
