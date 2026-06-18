-- 05 · Join: orders ⋈ customers.
CREATE SOURCE orders AS
  SELECT * FROM (VALUES (1, 10, 100), (2, 20, 50)) AS t(order_id, customer_id, amount);
CREATE SOURCE customers AS
  SELECT * FROM (VALUES (10, 'Alice'), (20, 'Bob')) AS t(customer_id, name);
CREATE INCREMENTAL VIEW enriched AS
  SELECT o.order_id, c.name, o.amount
  FROM orders o JOIN customers c ON o.customer_id = c.customer_id;
CREATE SINK out FROM enriched;
START PIPELINE out;
