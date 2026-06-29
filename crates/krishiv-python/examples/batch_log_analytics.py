#!/usr/bin/env python3
"""Application log analytics and SLA error rate calculation using SQL in Python."""

import os
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
import krishiv as ks

def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        parquet_path = os.path.join(temp_dir, "app_logs.parquet")
        write_logs_parquet(parquet_path)

        session = ks.Session.from_env()
        session.register_parquet("app_logs", parquet_path)

        # Calculate request and error counts, and error percentage per service
        df = session.sql(
            "SELECT service_name, "
            "       COUNT(*) as total_requests, "
            "       SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) as server_errors, "
            "       (SUM(CASE WHEN status_code >= 500 THEN 1.0 ELSE 0.0 END) / COUNT(*)) * 100.0 as error_rate_pct "
            "FROM app_logs "
            "GROUP BY service_name "
            "ORDER BY error_rate_pct DESC"
        )

        result = df.collect()
        print(result.pretty())

def write_logs_parquet(path):
    schema = pa.schema([
        ("service_name", pa.string()),
        ("status_code", pa.int64()),
    ])
    table = pa.Table.from_pydict({
        "service_name": [
            "auth-service", "payment-service", "auth-service",
            "payment-service", "catalog-service", "auth-service"
        ],
        "status_code": [200, 500, 200, 200, 200, 503],
    }, schema=schema)
    pq.write_table(table, path)

if __name__ == "__main__":
    main()
