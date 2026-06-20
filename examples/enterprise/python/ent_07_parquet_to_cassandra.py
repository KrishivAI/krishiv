"""Enterprise 07 · Parquet → Cassandra (batch CQL writes)

Loads an orders dataset from Parquet (built in-memory for the demo),
filters to shipped/delivered orders via pandas, then writes rows to a
Cassandra table using the cassandra-driver's BatchStatement for throughput.

Idempotent: uses INSERT (not UPDATE) — re-runs overwrite existing rows
because order_id is the Cassandra partition key.

Prerequisites:
    make infra-up seed-cassandra

Run:
    CASSANDRA_HOST=localhost python python/ent_07_parquet_to_cassandra.py
"""

import io
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
KEYSPACE = "krishiv"
TABLE    = "orders"

ORDERS = [
    {"order_id": 101, "customer": "alice",  "product": "Laptop Pro",  "amount": 1299.99, "status": "shipped"},
    {"order_id": 102, "customer": "bob",    "product": "Mouse",       "amount":   29.99, "status": "pending"},
    {"order_id": 103, "customer": "carol",  "product": "Chair",       "amount":  349.99, "status": "delivered"},
    {"order_id": 104, "customer": "dave",   "product": "Monitor",     "amount":  499.99, "status": "shipped"},
    {"order_id": 105, "customer": "eve",    "product": "USB Hub",     "amount":   39.99, "status": "cancelled"},
    {"order_id": 106, "customer": "frank",  "product": "Keyboard",    "amount":  129.99, "status": "pending"},
    {"order_id": 107, "customer": "grace",  "product": "Webcam",      "amount":   89.99, "status": "delivered"},
    {"order_id": 108, "customer": "hank",   "product": "Desk",        "amount":  699.99, "status": "pending"},
]


def main() -> None:
    print("=== Enterprise 07 (Python): Parquet → Cassandra ===")
    print(f"  host      : {CASSANDRA_HOST}")
    print(f"  keyspace  : {KEYSPACE}")
    print(f"  table     : {TABLE}")

    # 1 ── Build in-memory Parquet then filter with pandas.
    df = pd.DataFrame(ORDERS)
    active = df[df["status"].isin(["shipped", "delivered"])].copy()
    print(f"\n  {len(active)} orders selected (shipped + delivered)")
    print(active.to_string(index=False))

    # 2 ── Connect to Cassandra.
    cluster = Cluster([CASSANDRA_HOST], port=9042)
    try:
        session = cluster.connect()
    except Exception as e:
        print(f"\n  cannot connect to Cassandra at {CASSANDRA_HOST}: {e}")
        print("  run: make infra-up seed-cassandra")
        return

    # 3 ── Ensure keyspace + table exist.
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
        "WITH replication = {'class':'SimpleStrategy','replication_factor':1}"
    )
    session.set_keyspace(KEYSPACE)
    session.execute(
        f"CREATE TABLE IF NOT EXISTS {TABLE} ("
        "order_id bigint PRIMARY KEY, customer text, product text, "
        "amount decimal, status text)"
    )

    # 4 ── Batch insert (unlogged batch = max throughput, no atomicity needed).
    stmt = session.prepare(
        f"INSERT INTO {TABLE} (order_id, customer, product, amount, status) "
        "VALUES (?, ?, ?, ?, ?)"
    )
    batch = BatchStatement(batch_type=BatchType.UNLOGGED)
    for _, row in active.iterrows():
        batch.add(stmt, (int(row.order_id), row.customer, row.product,
                         float(row.amount), row.status))
    session.execute(batch)

    print(f"\n✓ {len(active)} rows written to Cassandra {KEYSPACE}.{TABLE}")

    # 5 ── Read back to verify.
    rows = session.execute(f"SELECT * FROM {TABLE} LIMIT 10")
    result_df = pd.DataFrame(rows._current_rows)
    print("\n--- Cassandra readback ---")
    print(result_df.to_string(index=False))

    cluster.shutdown()


if __name__ == "__main__":
    main()
