"""Enterprise 04 · Kafka → 10-second tumbling window → console

Reads order events from Kafka, assigns them to 10-second tumbling windows
based on event timestamp (field "ts" as epoch ms), and emits per-window
aggregates: count, sum_amount. Late events (arriving > 2 s after the
watermark) are logged but still counted in their assigned window.

Prerequisites:
    make infra-up seed-kafka

Run:
    KAFKA_BOOTSTRAP=localhost:9092 python python/ent_04_kafka_tumbling_window.py
"""

import json
import os
import time
from collections import defaultdict

import pyarrow as pa
import pandas as pd
from confluent_kafka import Consumer, KafkaException

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
WINDOW_MS = 10_000
WATERMARK_LAG_MS = 2_000
TIMEOUT_S = 20

SYNTHETIC_EVENTS = [
    {"customer": "alice", "amount": 120.0,  "ts": 1_716_200_000_000},
    {"customer": "bob",   "amount": 45.0,   "ts": 1_716_200_001_000},
    {"customer": "carol", "amount": 999.0,  "ts": 1_716_200_003_000},
    {"customer": "alice", "amount": 340.0,  "ts": 1_716_200_005_000},
    {"customer": "dave",  "amount": 77.0,   "ts": 1_716_200_008_000},
    {"customer": "bob",   "amount": 210.0,  "ts": 1_716_200_012_000},
    {"customer": "eve",   "amount": 55.0,   "ts": 1_716_200_013_000},
    {"customer": "alice", "amount": 130.0,  "ts": 1_716_200_015_000},
    {"customer": "carol", "amount": 820.0,  "ts": 1_716_200_018_000},
    {"customer": "frank", "amount": 60.0,   "ts": 1_716_200_022_000},
]


def window_start(ts_ms: int) -> int:
    return (ts_ms // WINDOW_MS) * WINDOW_MS


def main() -> None:
    print("=== Enterprise 04 (Python): Kafka → 10s Tumbling Window ===")

    events = _collect_kafka(TIMEOUT_S)
    if not events:
        print("  no live Kafka data — using synthetic time-series events")
        events = SYNTHETIC_EVENTS

    print(f"  processing {len(events)} events…")

    # Assign events to windows.
    windows: dict[tuple, dict] = defaultdict(lambda: {"count": 0, "sum_amount": 0.0})
    watermark = 0
    late_count = 0

    for e in sorted(events, key=lambda x: x.get("ts", 0)):
        ts = e.get("ts", 0)
        customer = e.get("customer", "?")
        amount = float(e.get("amount", 0))

        wstart = window_start(ts)
        wend = wstart + WINDOW_MS
        key = (wstart, wend, customer)

        if ts < watermark - WATERMARK_LAG_MS:
            late_count += 1

        windows[key]["count"] += 1
        windows[key]["sum_amount"] += amount
        watermark = max(watermark, ts)

    # Format results.
    rows = []
    for (ws, we, customer), agg in sorted(windows.items()):
        rows.append({
            "window_start": ws,
            "window_end":   we,
            "customer":     customer,
            "order_count":  agg["count"],
            "total_amount": round(agg["sum_amount"], 2),
        })

    df = pd.DataFrame(rows).sort_values(["window_start", "total_amount"], ascending=[True, False])
    print("\n--- Tumbling window results (10s buckets) ---")
    print(df.to_string(index=False))

    summary = df.groupby("window_start").agg(
        customers=("customer", "nunique"),
        orders=("order_count", "sum"),
        revenue=("total_amount", "sum"),
    ).reset_index()
    print("\n--- Per-window summary ---")
    print(summary.to_string(index=False))

    if late_count:
        print(f"\n  late events (>{WATERMARK_LAG_MS}ms behind watermark): {late_count}")


def _collect_kafka(timeout_s: float) -> list[dict]:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "krishiv-ent-py-04",
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
