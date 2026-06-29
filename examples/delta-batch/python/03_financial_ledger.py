"""Example 3: Financial Ledger — Delta table with overwrite for daily balance snapshots.

Simulates a bank that writes daily balance snapshots. Each day's snapshot
replaces the previous one, but time-travel lets you query historical balances.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))

import tempfile, shutil
import krishiv as ks
import pyarrow as pa

def main():
    tmpdir = tempfile.mkdtemp(prefix="delta_ledger_")
    delta_path = os.path.join(tmpdir, "accounts")
    try:
        session = ks.Session()

        schema = pa.schema([
            pa.field("account_id", pa.int64()),
            pa.field("holder_name", pa.string()),
            pa.field("balance", pa.float64()),
            pa.field("snapshot_date", pa.string()),
        ])

        # End of day 1
        snap1 = pa.record_batch(
            [[1, 2, 3],
             ["Alice", "Bob", "Carol"],
             [10000.00, 5000.00, 15000.00],
             ["2025-01-01", "2025-01-01", "2025-01-01"]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(snap1)], mode="overwrite")
        print("Snapshot 2025-01-01: 3 accounts")

        # End of day 2: balances changed
        snap2 = pa.record_batch(
            [[1, 2, 3],
             ["Alice", "Bob", "Carol"],
             [10500.00, 4800.00, 16200.00],
             ["2025-01-02", "2025-01-02", "2025-01-02"]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(snap2)], mode="overwrite")
        print("Snapshot 2025-01-02: balances updated")

        # End of day 3: Carol withdrew, new account added
        snap3 = pa.record_batch(
            [[1, 2, 3, 4],
             ["Alice", "Bob", "Carol", "Dave"],
             [11000.00, 4500.00, 12000.00, 8000.00],
             ["2025-01-03", "2025-01-03", "2025-01-03", "2025-01-03"]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(snap3)], mode="overwrite")
        print("Snapshot 2025-01-03: Carol withdrew, Dave added")

        # Latest balances
        df = ks.read_delta(session, delta_path)
        print("\n--- Current balances (v2) ---")
        print(df.collect_pretty())

        # Time-travel to day 1
        df_v0 = ks.read_delta(session, delta_path, version=0)
        print("\n--- Historical: Balances on 2025-01-01 (v0) ---")
        print(df_v0.collect_pretty())

        # Time-travel to day 2
        df_v1 = ks.read_delta(session, delta_path, version=1)
        print("\n--- Historical: Balances on 2025-01-02 (v1) ---")
        print(df_v1.collect_pretty())

        print("\nFinancial ledger example completed successfully!")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

if __name__ == "__main__":
    main()
