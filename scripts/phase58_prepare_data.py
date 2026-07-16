#!/usr/bin/env python3
"""Generate deterministic shared input for the live Phase 58 chaos gate."""

from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.parquet as pq


def main() -> int:
    target = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/krishiv-phase58-data")
    target.mkdir(parents=True, exist_ok=True)

    # Keep the Flight action payload below tonic's 4 MiB default while still
    # producing multiple Parquet row groups and enough work for fault overlap.
    rows = 50_000
    user_ids = pa.array([f"user-{index % 128}" for index in range(rows)])
    table = pa.table(
        {
            "user_id": user_ids,
            "ts": pa.array(range(rows), type=pa.int64()),
            "value": pa.array((index % 17 for index in range(rows)), type=pa.int64()),
        }
    )
    pq.write_table(table, target / "events.parquet", compression="zstd", row_group_size=32_768)
    partition_dir = target / "events"
    partition_dir.mkdir(exist_ok=True)
    rows_per_part = rows // 4
    for part in range(4):
        pq.write_table(
            table.slice(part * rows_per_part, rows_per_part),
            partition_dir / f"part-{part}.parquet",
            compression="zstd",
        )

    with (target / "changes.csv").open("w", encoding="utf-8") as output:
        output.write("k,v,_weight\n")
        for index in range(10_000):
            output.write(f"key-{index % 64},{index % 101},1\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
