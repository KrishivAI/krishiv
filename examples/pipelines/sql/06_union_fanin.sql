-- 06 · Fan-in: union two regional order streams into one view.
CREATE SOURCE orders_us AS SELECT * FROM (VALUES (1, 100), (2, 50)) AS t(id, amount);
CREATE SOURCE orders_eu AS SELECT * FROM (VALUES (3, 75), (4, 25)) AS t(id, amount);
CREATE INCREMENTAL VIEW all_orders AS
  SELECT id, amount FROM orders_us
  UNION ALL
  SELECT id, amount FROM orders_eu;
CREATE SINK out FROM all_orders;
START PIPELINE out;
