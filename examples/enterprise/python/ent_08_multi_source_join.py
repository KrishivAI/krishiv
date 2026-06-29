"""Enterprise 08 · Multi-source join: Kafka events + Parquet lookup → enriched output

Stream-table join using pandas merge:
  1. Product catalog loaded from in-memory Parquet (the "table" side)
  2. Order events collected from Kafka (the "stream" side)
  3. pandas merge on product_id → enriched order with product metadata
  4. Enriched output written to Parquet

This mirrors krishiv.stream_table_join() in the Python API.

Prerequisites:
    make infra-up seed-kafka

Run:
    KAFKA_BOOTSTRAP=localhost:9092 python python/ent_08_multi_source_join.py
"""

import json
import os
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from confluent_kafka import Consumer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
OUT_PATH = "/tmp/krishiv-enterprise-py-08-enriched.parquet"

CATALOG = pd.DataFrame([
    {"product_id": 1, "name": "Laptop Pro",  "category": "electronics", "unit_price": 1299.99},
    {"product_id": 2, "name": "Mouse",       "category": "electronics", "unit_price":   29.99},
    {"product_id": 3, "name": "Desk Chair",  "category": "furniture",   "unit_price":  349.99},
    {"product_id": 4, "name": "Monitor 4K",  "category": "electronics", "unit_price":  499.99},
    {"product_id": 5, "name": "USB Hub",     "category": "electronics", "unit_price":   39.99},
])

SYNTHETIC_EVENTS = [
    {"order_id": 1001, "product_id": 1, "customer": "alice", "qty": 1},
    {"order_id": 1002, "product_id": 2, "customer": "bob",   "qty": 3},
    {"order_id": 1003, "product_id": 3, "customer": "carol", "qty": 1},
    {"order_id": 1004, "product_id": 1, "customer": "dave",  "qty": 2},
    {"order_id": 1005, "product_id": 5, "customer": "eve",   "qty": 5},
    {"order_id": 1006, "product_id": 4, "customer": "frank", "qty": 1},
]


def main() -> None:
    print("=== Enterprise 08 (Python): Kafka + Parquet → Enriched output ===")

    events = _collect_kafka(timeout_s=12)
    if not events:
        print("  no live Kafka — using synthetic order events")
        events = SYNTHETIC_EVENTS

    events_df = pd.DataFrame(events)
    print(f"\n  {len(events_df)} stream events  ×  {len(CATALOG)} catalog rows")

    # Stream-table join.
    enriched = events_df.merge(
        CATALOG.rename(columns={"name": "product_name"}),
        on="product_id",
        how="left",
    )
    enriched["line_total"] = (enriched["qty"] * enriched["unit_price"]).round(2)
    enriched = enriched.sort_values("line_total", ascending=False)

    print("\n--- Enriched orders ---")
    cols = ["order_id", "customer", "product_name", "category", "unit_price", "qty", "line_total"]
    print(enriched[cols].to_string(index=False))

    # Category revenue summary.
    summary = enriched.groupby("category").agg(
        orders=("order_id", "count"),
        revenue=("line_total", "sum"),
    ).reset_index()
    print("\n--- Revenue by category ---")
    print(summary.to_string(index=False))

    # Persist.
    table = pa.Table.from_pandas(enriched)
    pq.write_table(table, OUT_PATH)
    print(f"\n✓ {len(enriched)} enriched rows written to {OUT_PATH}")


def _collect_kafka(timeout_s: float) -> list[dict]:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "krishiv-ent-py-08",
        "enable.auto.commit": True,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(["orders"])
    events: list[dict] = []
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                break
            try:
                events.append(json.loads(msg.value().decode("utf-8")))
            except Exception:
                continue
    finally:
        consumer.close()
    return events


if __name__ == "__main__":
    main()
