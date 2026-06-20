-- Bootstrap tables for CDC examples.

CREATE TABLE orders (
    order_id   BIGSERIAL PRIMARY KEY,
    customer   TEXT NOT NULL,
    product    TEXT NOT NULL,
    amount     NUMERIC(10,2) NOT NULL,
    status     TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE products (
    product_id  BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    category    TEXT NOT NULL,
    unit_price  NUMERIC(10,2) NOT NULL
);

INSERT INTO products (name, category, unit_price) VALUES
  ('Laptop Pro',    'electronics', 1299.99),
  ('Wireless Mouse','electronics',   29.99),
  ('Desk Chair',    'furniture',    349.99),
  ('Monitor 27"',   'electronics',  499.99),
  ('USB Hub',       'electronics',   39.99);

-- Logical replication publication for Debezium.
CREATE PUBLICATION krishiv_pub FOR TABLE orders, products;
