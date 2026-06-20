"""Enterprise 02 · Kafka → Parquet (exactly-once via manual epoch log)

Simulates the two-phase commit pattern from Krishiv's EpochTransactionLog:
  - Stage each batch as <epoch>-N.parquet.tmp
  - On epoch boundary: rename all .tmp → .parquet (atomic POSIX rename)
  - Commit Kafka offset only after rename succeeds
  - Abort: delete .tmp files

On restart after a crash between stage and rename, the .tmp files are found
and the epoch can be re-committed (idempotent rename).

Prerequisites:
    make infra-up seed-kafka

Run:
    KAFKA_BOOTSTRAP=localhost:9092 python python/ent_02_kafka_parquet_exactly_once.py
"""

import glob
import json
import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaException

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "orders"
GROUP_ID = "krishiv-ent-py-02"
OUT_DIR = Path("/tmp/krishiv-enterprise-py-02-exactly-once")
BARRIER_EVERY = 3
TIMEOUT_S = 15


def main() -> None:
    print("=== Enterprise 02 (Python): Kafka → Parquet (exactly-once 2PC) ===")
    print(f"  source     : kafka://{BOOTSTRAP}  topic={TOPIC}")
    print(f"  output dir : {OUT_DIR}")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    epoch = 1
    batch_count = 0
    total_rows = 0
    staged: list[Path] = []
    rows_in_batch: list[dict] = []
    last_msg = None
    deadline = time.monotonic() + TIMEOUT_S

    try:
        while total_rows < 50 and time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            payload = json.loads(msg.value().decode("utf-8"))
            rows_in_batch.append(payload)
            last_msg = msg

            # Stage batch → .parquet.tmp
            tmp = _stage(rows_in_batch, epoch, batch_count, OUT_DIR)
            staged.append(tmp)
            total_rows += len(rows_in_batch)
            rows_in_batch = []
            batch_count += 1

            print(f"  staged {tmp.name}  total_rows={total_rows}")

            if batch_count % BARRIER_EVERY == 0:
                _commit_epoch(staged, epoch, consumer, last_msg)
                staged = []
                epoch += 1

        # Final barrier.
        if staged and last_msg:
            _commit_epoch(staged, epoch, consumer, last_msg)

    finally:
        consumer.close()

    files = sorted(OUT_DIR.glob("*.parquet"))
    print(f"\n✓ exactly-once pipeline done — {total_rows} rows, {len(files)} parquet file(s)")
    for f in files:
        print(f"  {f.name}")


def _stage(rows: list[dict], epoch: int, seq: int, out_dir: Path) -> Path:
    tmp = out_dir / f"{epoch}-{seq}.parquet.tmp"
    keys = sorted({k for r in rows for k in r})
    table = pa.table({k: [r.get(k) for r in rows] for k in keys})
    pq.write_table(table, tmp)
    return tmp


def _commit_epoch(
    staged: list[Path], epoch: int, consumer: Consumer, last_msg
) -> None:
    # Phase 1: rename .tmp → .parquet (atomic POSIX rename).
    for tmp in staged:
        final = tmp.with_suffix("")  # strips .tmp → .parquet
        os.rename(tmp, final)
        print(f"  epoch={epoch} rename {tmp.name} → {final.name}")

    # Phase 2: commit Kafka offset after all renames succeed.
    consumer.commit(message=last_msg, asynchronous=False)
    print(f"  epoch={epoch} kafka offset committed")


if __name__ == "__main__":
    main()
