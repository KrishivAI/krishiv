"""Example 7: Multi-Version Data Lineage — Delta table with complex version history.

Simulates a data science workflow where a feature store is updated
multiple times. Queries demonstrate version comparison and lineage tracking.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))

import tempfile, shutil
import krishiv as ks
import pyarrow as pa

def main():
    tmpdir = tempfile.mkdtemp(prefix="delta_lineage_")
    delta_path = os.path.join(tmpdir, "feature_store")
    try:
        session = ks.Session()

        schema = pa.schema([
            pa.field("feature_id", pa.string()),
            pa.field("model_v", pa.string()),
            pa.field("importance", pa.float64()),
            pa.field("category", pa.string()),
        ])

        # v0: initial feature set
        v0 = pa.record_batch(
            [["f1", "f2", "f3"],
             ["v1", "v1", "v1"],
             [0.85, 0.72, 0.61],
             ["numeric", "categorical", "text"]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(v0)], mode="overwrite")
        print("v0: Initial feature set (3 features)")

        # v1: add more features
        v1 = pa.record_batch(
            [["f4", "f5"],
             ["v2", "v2"],
             [0.90, 0.55],
             ["numeric", "text"]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(v1)], mode="append")
        print("v1: Added 2 features")

        # v2: retrain, importances change
        v2 = pa.record_batch(
            [["f1", "f2", "f3", "f4", "f5", "f6"],
             ["v3", "v3", "v3", "v3", "v3", "v3"],
             [0.88, 0.65, 0.70, 0.92, 0.48, 0.78],
             ["numeric", "categorical", "text", "numeric", "text", "numeric"]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(v2)], mode="append")
        print("v2: Model v3 feature set (6 features, retrained)")

        # Query latest
        df = ks.read_delta(session, delta_path)
        print("\n--- Latest feature set (v3, 6 features) ---")
        print(df.collect_pretty())

        # Compare v0 features
        df_v0 = ks.read_delta(session, delta_path, version=0)
        print("\n--- Original features (v0, 3 features) ---")
        print(df_v0.collect_pretty())

        # Register for SQL analysis
        session.register_record_batches("features", [
            ks.Batch(b) for b in ks.read_delta(session, delta_path).collect().to_arrow().to_batches()
        ])

        # Feature importance ranking
        df_rank = session.sql("SELECT feature_id, importance, category FROM features ORDER BY importance DESC")
        print("\n--- Feature Importance Ranking ---")
        print(df_rank.collect_pretty())

        # Category breakdown
        df_cat = session.sql("SELECT category, COUNT(*) as count, AVG(importance) as avg_importance FROM features GROUP BY category")
        print("\n--- Category Breakdown ---")
        print(df_cat.collect_pretty())

        print("\nMulti-version data lineage example completed successfully!")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

if __name__ == "__main__":
    main()
