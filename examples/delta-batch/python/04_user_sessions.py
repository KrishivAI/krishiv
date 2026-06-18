"""Example 4: User Sessions — Delta table tracking user activity across sessions.

Simulates a web analytics system that tracks user sessions. Each append
represents a new batch of completed sessions. Queries show session counts
and engagement metrics over time.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))

import tempfile, shutil
import krishiv as ks
import pyarrow as pa

def main():
    tmpdir = tempfile.mkdtemp(prefix="delta_sessions_")
    delta_path = os.path.join(tmpdir, "user_sessions")
    try:
        session = ks.Session()

        schema = pa.schema([
            pa.field("session_id", pa.string()),
            pa.field("user_id", pa.int64()),
            pa.field("page_views", pa.int64()),
            pa.field("duration_sec", pa.int64()),
            pa.field("device", pa.string()),
        ])

        # Batch 1: Morning sessions
        morning = pa.record_batch(
            [["s1", "s2", "s3", "s4"],
             [1001, 1002, 1001, 1003],
             [5, 12, 3, 8],
             [120, 450, 60, 200],
             ["mobile", "desktop", "mobile", "tablet"]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(morning)], mode="overwrite")
        print("Batch 1: 4 morning sessions logged")

        # Batch 2: Afternoon sessions
        afternoon = pa.record_batch(
            [["s5", "s6", "s7"],
             [1004, 1005, 1001],
             [15, 7, 20],
             [600, 180, 900],
             ["desktop", "mobile", "desktop"]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(afternoon)], mode="append")
        print("Batch 2: 3 afternoon sessions appended")

        # Batch 3: Evening sessions
        evening = pa.record_batch(
            [["s8", "s9", "s10", "s11", "s12"],
             [1006, 1007, 1002, 1008, 1001],
             [25, 10, 18, 6, 2],
             [1200, 300, 720, 150, 45],
             ["desktop", "mobile", "desktop", "tablet", "mobile"]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(evening)], mode="append")
        print("Batch 3: 5 evening sessions appended")

        # Query all sessions
        df_all = ks.read_delta(session, delta_path)
        print("\n--- All sessions (latest) ---")
        print(df_all.collect_pretty())

        # Query just the morning sessions (time-travel to v0)
        df_morning = ks.read_delta(session, delta_path, version=0)
        print("\n--- Morning sessions only (v0) ---")
        print(df_morning.collect_pretty())

        print("\nUser sessions example completed successfully!")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

if __name__ == "__main__":
    main()
