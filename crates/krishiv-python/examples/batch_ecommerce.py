#!/usr/bin/env python3
"""E-commerce SQL Join and aggregate batch execution on local Parquet files in Python."""

import os
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
import krishiv as ks

def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        orders_path = os.path.join(temp_dir, "orders.parquet")
        customers_path = os.path.join(temp_dir, "customers.parquet")

        write_orders_parquet(orders_path)
        write_customers_parquet(customers_path)

        # Build embedded session
        session = ks.Session.embedded()

        # Register tables
        session.register_parquet("orders", orders_path)
        session.register_parquet("customers", customers_path)

        # Join customers and orders to calculate revenue by segment
        df = session.sql(
            "SELECT c.segment, "
            "       COUNT(o.order_id) as total_orders, "
            "       SUM(o.amount) as total_revenue "
            "FROM orders o "
            "JOIN customers c ON o.customer_id = c.customer_id "
            "WHERE o.status = 'COMPLETED' "
            "GROUP BY c.segment "
            "ORDER BY total_revenue DESC"
        )

        result = df.collect()
        print(result.pretty())

def write_orders_parquet(path):
    schema = pa.schema([
        ("order_id", pa.int64()),
        ("customer_id", pa.int64()),
        ("amount", pa.float64()),
        ("status", pa.string()),
    ])
    table = pa.Table.from_pydict({
        "order_id": [101, 102, 103, 104],
        "customer_id": [1, 2, 1, 3],
        "amount": [150.0, 45.5, 99.9, 1200.0],
        "status": ["COMPLETED", "COMPLETED", "COMPLETED", "PENDING"],
    }, schema=schema)
    pq.write_table(table, path)

def write_customers_parquet(path):
    schema = pa.schema([
        ("customer_id", pa.int64()),
        ("segment", pa.string()),
    ])
    table = pa.Table.from_pydict({
        "customer_id": [1, 2, 3],
        "segment": ["VIP", "Standard", "VIP"],
    }, schema=schema)
    pq.write_table(table, path)

if __name__ == "__main__":
    main()
