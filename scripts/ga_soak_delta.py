#!/usr/bin/env python3
"""Regenerate the DeltaBatch payload baked into deploy/k8s/ga-soak/soak.yaml.

The soak's IVM leg is driven by curl, which cannot build Arrow IPC, so the
delta is a constant in the manifest. This is how that constant is produced —
without it the next person editing the soak's schema has no way to rebuild it.

Wire format: b"DLT1" || Arrow IPC stream, with an i64 `_weight` column
(positive = insert), per krishiv-delta::serialize_delta_batch.

Payload: three orders across two regions -> east = 10 + 30 = 40, west = 20,
which is why the soak asserts the materialized view settles at exactly 2 rows
from exactly 2 inserted rows.
"""

import base64
import io

import pyarrow as pa

table = pa.table(
    {
        "region": pa.array(["east", "west", "east"], pa.string()),
        "amount": pa.array([10, 20, 30], pa.int64()),
        "_weight": pa.array([1, 1, 1], pa.int64()),
    }
)
sink = io.BytesIO()
with pa.ipc.new_stream(sink, table.schema) as writer:
    writer.write_table(table)
print(base64.standard_b64encode(b"DLT1" + sink.getvalue()).decode())
