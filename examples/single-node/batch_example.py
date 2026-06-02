#!/usr/bin/env python3
"""Batch Example: Web Server Access Log Analytics."""

import os
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
import krishiv as ks

def main():
    if "KRISHIV_COORDINATOR_URL" not in os.environ:
        print("Warning: KRISHIV_COORDINATOR_URL is not set. Running in embedded mode.")
        
    with tempfile.TemporaryDirectory() as temp_dir:
        parquet_path = os.path.join(temp_dir, "access_logs.parquet")
        write_logs_parquet(parquet_path)

        session = ks.Session.from_env()
        session.register_parquet("access_logs", parquet_path)

        print("Analyzing top error-producing endpoints...")
        result = (
            session.sql(
                "SELECT endpoint, COUNT(*) as error_count "
                "FROM access_logs "
                "WHERE status >= 400 "
                "GROUP BY endpoint "
                "ORDER BY error_count DESC"
            )
            .collect()
        )
        print(result.pretty())

def write_logs_parquet(path):
    schema = pa.schema([
        ("timestamp", pa.timestamp('ms')),
        ("endpoint", pa.string()),
        ("status", pa.int32()),
    ])
    table = pa.Table.from_pydict({
        "timestamp": [1700000000000, 1700000001000, 1700000002000, 1700000003000, 1700000004000],
        "endpoint": ["/api/v1/login", "/api/v1/data", "/api/v1/login", "/api/v1/checkout", "/api/v1/login"],
        "status": [200, 404, 500, 200, 403],
    }, schema=schema)
    pq.write_table(table, path)

if __name__ == "__main__":
    main()
