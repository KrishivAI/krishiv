#!/usr/bin/env python3
"""Local Apache Hudi Copy-On-Write ingestion and SQL read batch example in Python."""

import os
import tempfile
import krishiv as ks

def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        hudi_path = os.path.join(temp_dir, "my_hudi_table")

        # 1. Build the embedded session
        session = ks.Session.embedded()

        # 2. Prepare some mock users using SQL DataFrame projection
        df_source = session.sql(
            "SELECT 1 AS user_id, 'Alice' AS name "
            "UNION ALL "
            "SELECT 2 AS user_id, 'Bob' AS name "
            "UNION ALL "
            "SELECT 3 AS user_id, 'Charlie' AS name"
        )

        # 3. Append to the Hudi table locally
        print("--- Ingesting into Hudi Table ---")
        write_res = ks.write_hudi_append(session, hudi_path, df_source)
        print(f"Hudi Ingestion Successful! Instant: {write_res.instant}, "
              f"Rows Inserted: {write_res.rows_inserted}")

        # 4. Read the Hudi table locally
        print("\n--- Reading Hudi Snapshot ---")
        df = ks.read_hudi(session, hudi_path, query_type="snapshot")

        # 5. Collect and print the results
        result = df.collect()
        print(result.pretty())

if __name__ == "__main__":
    main()
