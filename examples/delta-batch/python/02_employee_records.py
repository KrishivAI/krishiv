"""Example 2: Employee Records — Delta table with append and time-travel audit.

Simulates an HR system that tracks employee onboarding over multiple days.
Each day's new hires are appended to the Delta table.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))

import tempfile, shutil
import krishiv as ks
import pyarrow as pa

def main():
    tmpdir = tempfile.mkdtemp(prefix="delta_employee_")
    delta_path = os.path.join(tmpdir, "employees")
    try:
        session = ks.Session()

        schema = pa.schema([
            pa.field("emp_id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("department", pa.string()),
            pa.field("salary", pa.int64()),
            pa.field("hire_date", pa.string()),
        ])

        # Day 1: Engineering hires
        day1 = pa.record_batch(
            [[101, 102, 103],
             ["Alice Chen", "Bob Park", "Carol Liu"],
             ["Engineering", "Engineering", "Engineering"],
             [120000, 115000, 130000],
             ["2025-01-15", "2025-01-15", "2025-01-15"]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(day1)], mode="overwrite")
        print("Day 1: Hired 3 engineers")

        # Day 2: Marketing hires
        day2 = pa.record_batch(
            [[201, 202],
             ["Dave Kim", "Eve Singh"],
             ["Marketing", "Marketing"],
             [95000, 98000],
             ["2025-01-16", "2025-01-16"]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(day2)], mode="append")
        print("Day 2: Hired 2 marketers")

        # Day 3: Design hire
        day3 = pa.record_batch(
            [[301],
             ["Frank Lee"],
             ["Design"],
             [110000],
             ["2025-01-17"]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(day3)], mode="append")
        print("Day 3: Hired 1 designer")

        # Query latest: all employees
        df_all = ks.read_delta(session, delta_path)
        print("\n--- All employees (latest) ---")
        print(df_all.collect_pretty())

        # Audit: what did the team look like after day 1?
        df_day1 = ks.read_delta(session, delta_path, version=0)
        print("\n--- Audit: Team after Day 1 (version 0) ---")
        print(df_day1.collect_pretty())

        # Audit: what did the team look like after day 2?
        df_day2 = ks.read_delta(session, delta_path, version=1)
        print("\n--- Audit: Team after Day 2 (version 1) ---")
        print(df_day2.collect_pretty())

        print("\nEmployee records example completed successfully!")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

if __name__ == "__main__":
    main()
