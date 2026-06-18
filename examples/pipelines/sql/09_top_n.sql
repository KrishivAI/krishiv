-- 09 · Top-N: highest-value products.
CREATE SOURCE sales AS
  SELECT * FROM (VALUES ('A',300),('B',120),('C',500),('D',90)) AS t(product, revenue);
CREATE INCREMENTAL VIEW top2 AS
  SELECT product, revenue FROM sales ORDER BY revenue DESC LIMIT 2;
CREATE SINK out FROM top2;
START PIPELINE out;
