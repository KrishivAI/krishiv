"""Enterprise 05 · Kinesis → Parquet (with sequence-number checkpoint)

Reads IoT sensor records from a Kinesis shard (LocalStack), writes them to
Parquet, and persists the last sequence number as a checkpoint file. On
re-run, reads resume with AfterSequenceNumber so each record is processed
at least once (Kinesis does not support exactly-once without de-dup logic).

Prerequisites:
    make infra-up seed-aws

Run:
    AWS_ENDPOINT_URL=http://localhost:4566 \
    AWS_ACCESS_KEY_ID=krishiv AWS_SECRET_ACCESS_KEY=krishiv \
    python python/ent_05_kinesis_to_parquet.py
"""

import base64
import json
import os
import time

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

STREAM = "krishiv-events"
SHARD = "shardId-000000000000"
OUT_PATH = "/tmp/krishiv-enterprise-py-05-kinesis.parquet"
CHECKPOINT_FILE = "/tmp/krishiv-enterprise-py-05-checkpoint.txt"
BATCH_SIZE = 100
TIMEOUT_S = 10


def get_kinesis_client():
    kwargs = {
        "region_name": "us-east-1",
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", "krishiv"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "krishiv"),
    }
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("kinesis", **kwargs)


def main() -> None:
    print("=== Enterprise 05 (Python): Kinesis → Parquet ===")
    print(f"  stream : {STREAM}")
    print(f"  output : {OUT_PATH}")

    client = get_kinesis_client()

    # Restore from checkpoint.
    if os.path.exists(CHECKPOINT_FILE):
        seq = open(CHECKPOINT_FILE).read().strip()
        print(f"  resuming from checkpoint seq={seq}")
        it_type = "AFTER_SEQUENCE_NUMBER"
        it_args = {"StartingSequenceNumber": seq}
    else:
        print("  starting from TRIM_HORIZON (no checkpoint)")
        it_type = "TRIM_HORIZON"
        it_args = {}

    resp = client.get_shard_iterator(
        StreamName=STREAM,
        ShardId=SHARD,
        ShardIteratorType=it_type,
        **it_args,
    )
    shard_it = resp["ShardIterator"]

    records: list[dict] = []
    last_seq: str | None = None
    deadline = time.monotonic() + TIMEOUT_S

    while time.monotonic() < deadline:
        resp = client.get_records(ShardIterator=shard_it, Limit=BATCH_SIZE)
        for rec in resp.get("Records", []):
            try:
                data = json.loads(base64.b64decode(rec["Data"]).decode("utf-8"))
            except Exception:
                data = {"raw": rec["Data"].decode("utf-8", errors="replace")}
            data["_sequence_number"] = rec["SequenceNumber"]
            data["_partition_key"]   = rec["PartitionKey"]
            records.append(data)
            last_seq = rec["SequenceNumber"]
            print(f"  record  seq={rec['SequenceNumber'][:16]}…")

        shard_it = resp.get("NextShardIterator")
        if not shard_it or not resp.get("Records"):
            break

    if not records:
        print("  no records received — run: make seed-aws")
        return

    # Write to Parquet.
    keys = sorted({k for r in records for k in r})
    table = pa.table({k: [r.get(k) for r in records] for k in keys})
    pq.write_table(table, OUT_PATH)

    if last_seq:
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(last_seq)
        print(f"\n  checkpoint saved: {last_seq}")

    print(f"\n✓ {len(records)} records written to {OUT_PATH}")
    print("\n--- Sample (first 5) ---")
    print(table.slice(0, 5).to_pandas().to_string(index=False))


if __name__ == "__main__":
    main()
