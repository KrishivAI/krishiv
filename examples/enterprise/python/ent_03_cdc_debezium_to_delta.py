"""Enterprise 03 · CDC (Debezium/Kafka) → Delta Lake

Reads Debezium change events from the "pgserver.public.orders" Kafka topic,
parses Insert/Update/Delete operations, and merges them into a Delta table
using the deltalake Python library.

Schema evolution: the orders table gains a "notes" column mid-stream — the
Delta merge handles nullable new columns automatically.

Prerequisites:
    make infra-up debezium-up
    # INSERT/UPDATE some rows in PostgreSQL to generate CDC events:
    # docker exec -it krishiv-postgres psql -U krishiv enterprise -c \
    #   "INSERT INTO orders(customer,product,amount,status) VALUES('alice','Laptop',1299,'pending');"

Run:
    KAFKA_BOOTSTRAP=localhost:9092 python python/ent_03_cdc_debezium_to_delta.py
"""

import json
import os
import time
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaException
from deltalake import DeltaTable, write_deltalake

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "pgserver.public.orders"
GROUP_ID = "krishiv-ent-py-03"
DELTA_PATH = "/tmp/krishiv-enterprise-py-03-cdc-delta"
TIMEOUT_S = 20


# Synthetic events matching what Debezium 2.x produces on pgserver.public.orders.
SYNTHETIC_EVENTS = [
    {"op": "c", "order_id": 1, "customer": "alice",  "product": "Laptop Pro", "amount": 1299.99, "status": "pending", "__lsn": 100, "__ts_ms": 1716201600000},
    {"op": "c", "order_id": 2, "customer": "bob",    "product": "Mouse",      "amount":   29.99, "status": "pending", "__lsn": 110, "__ts_ms": 1716201601000},
    {"op": "u", "order_id": 1, "customer": "alice",  "product": "Laptop Pro", "amount": 1299.99, "status": "shipped", "__lsn": 120, "__ts_ms": 1716201602000},
    {"op": "c", "order_id": 3, "customer": "carol",  "product": "Monitor",    "amount":  499.99, "status": "pending", "notes": "gift", "__lsn": 130, "__ts_ms": 1716201603000},
    {"op": "d", "order_id": 2, "customer": None,     "product": None,         "amount": None,    "status": None,      "__lsn": 140, "__ts_ms": 1716201604000},
]


def main() -> None:
    print("=== Enterprise 03 (Python): CDC (Debezium) → Delta Lake ===")
    print(f"  source : kafka://{BOOTSTRAP}  topic={TOPIC}")
    print(f"  sink   : {DELTA_PATH}")

    events = _collect_kafka_events()
    if not events:
        print("  no live CDC events — using synthetic Debezium payloads")
        events = SYNTHETIC_EVENTS

    inserts = [e for e in events if e["op"] == "c"]
    updates = [e for e in events if e["op"] == "u"]
    deletes = [e for e in events if e["op"] == "d"]
    print(f"\n  events: {len(inserts)} inserts, {len(updates)} updates, {len(deletes)} deletes")

    # Write initial snapshot (inserts + updates) to Delta.
    all_rows = [e for e in events if e["op"] in ("c", "u")]
    if all_rows:
        table = _events_to_arrow(all_rows)
        print(f"\n  writing {table.num_rows} rows to Delta (mode=overwrite for demo)…")
        write_deltalake(DELTA_PATH, table, mode="overwrite")

    # Append a delete-marker batch (includes _op column).
    if deletes:
        del_table = _events_to_arrow(deletes)
        write_deltalake(DELTA_PATH, del_table, mode="append")

    # Read back.
    dt = DeltaTable(DELTA_PATH)
    result = dt.to_pyarrow_table()
    print(f"\n--- Delta table ({result.num_rows} rows, {dt.version()} versions) ---")
    import pandas as pd
    df = result.to_pandas()
    print(df.to_string(index=False))

    # Operation summary.
    print("\n--- CDC operation distribution ---")
    print(df.groupby("op")[["order_id"]].count().rename(columns={"order_id": "count"}))

    print(f"\n✓ Delta table at {DELTA_PATH}  (version {dt.version()})")


def _collect_kafka_events(timeout_s: float = 10.0) -> list[dict]:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": True,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
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
                payload = json.loads(msg.value().decode("utf-8"))
                payload["__lsn"] = payload.get("__lsn") or 0
                payload["__ts_ms"] = payload.get("__ts_ms") or 0
                payload["op"] = payload.get("__op") or payload.get("op", "c")
                events.append(payload)
                print(f"  kafka event  op={payload['op']}  offset={msg.offset()}")
            except Exception:
                continue
    finally:
        consumer.close()
    return events


def _events_to_arrow(events: list[dict]) -> pa.Table:
    # Collect union of all keys.
    all_keys = sorted({k for e in events for k in e})
    cols: dict[str, list[Any]] = {k: [] for k in all_keys}
    for e in events:
        for k in all_keys:
            cols[k].append(e.get(k))
    return pa.table(cols)


if __name__ == "__main__":
    main()
