"""Enterprise 01 · Kafka → Parquet (at-least-once)

Reads up to 50 messages from the "orders" Kafka topic using a manual-commit
consumer (at-least-once) and writes them to a local Parquet file via
PyArrow + Krishiv's write_parquet. Offsets are committed only after the
write succeeds, so a mid-write crash leaves the consumer behind the last
committed offset and re-reads on restart.

Prerequisites:
    make infra-up seed-kafka

Run:
    KAFKA_BOOTSTRAP=localhost:9092 python python/ent_01_kafka_parquet_at_least_once.py
"""

import json
import os
import time
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaException, TopicPartition

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "orders"
GROUP_ID = "krishiv-ent-py-01"
OUT_PATH = "/tmp/krishiv-enterprise-py-01-orders.parquet"
MAX_ROWS = 50
TIMEOUT_S = 15


def make_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })


def main() -> None:
    print("=== Enterprise 01 (Python): Kafka → Parquet (at-least-once) ===")
    print(f"  source  : kafka://{BOOTSTRAP}  topic={TOPIC}")
    print(f"  sink    : {OUT_PATH}")

    consumer = make_consumer()
    consumer.subscribe([TOPIC])

    rows: list[dict] = []
    deadline = time.monotonic() + TIMEOUT_S

    try:
        while len(rows) < MAX_ROWS and time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                rows.append(payload)
                print(f"  consumed offset={msg.offset()} partition={msg.partition()}")
            except json.JSONDecodeError as e:
                print(f"  skip malformed message: {e}")
                continue

            # Flush + commit every 10 rows for at-least-once durability.
            if len(rows) % 10 == 0:
                _flush_and_commit(consumer, rows, msg)

    finally:
        consumer.close()

    if not rows:
        print("  no messages received — run: make seed-kafka")
        return

    # Final flush.
    _write_parquet(rows, OUT_PATH)
    print(f"\n✓ wrote {len(rows)} rows to {OUT_PATH}")
    _show_sample(OUT_PATH)


def _flush_and_commit(consumer: Consumer, rows: list[dict], last_msg) -> None:
    _write_parquet(rows, OUT_PATH)
    consumer.commit(message=last_msg, asynchronous=False)
    print(f"  committed offset={last_msg.offset()} rows={len(rows)}")


def _write_parquet(rows: list[dict], path: str) -> None:
    # Normalise — all rows must have the same keys.
    keys = sorted({k for r in rows for k in r})
    table = pa.table({k: [r.get(k) for r in rows] for k in keys})
    pq.write_table(table, path)


def _show_sample(path: str) -> None:
    table = pq.read_table(path)
    print(f"\n--- sample (first 5 rows of {table.num_rows()}) ---")
    print(table.slice(0, 5).to_pandas().to_string(index=False))


if __name__ == "__main__":
    main()
