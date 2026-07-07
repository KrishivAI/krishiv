"""
IVM (Incremental View Maintenance) example.

Demonstrates the DeltaBatch and IvmJob Python API:
- Create an incremental view from SQL
- Feed delta batches (insertions, updates, retractions)
- Step the view to process deltas
- Read materialized snapshots
- Checkpoint and restore state

To run:
    PYTHONPATH=crates/krishiv-python/python:$PYTHONPATH python3 examples/ivm_example.py
"""

import krishiv
import pyarrow as pa


def main():
    session = krishiv.Session()
    job = session.ivm("sales_pipeline")

    # Define the view output schema using krishiv.Schema.
    class Revenue(krishiv.Schema):
        region: str
        total: float

    # Register an incremental view backed by SQL.
    job.register_view(
        "revenue",
        "SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
        Revenue,
        is_materialized=True,
    )

    # ── Feed batch 1: insertions only ──────────────────────────
    batch1 = pa.RecordBatch.from_pydict(
        {"region": ["us", "eu", "us"], "amount": [100.0, 200.0, 50.0]}
    )
    job.feed("sales", krishiv.DeltaBatch.from_inserts(batch1))
    summary = job.step()
    print(f"Tick {summary.tick}: {summary.active_views} active views, "
          f"{summary.total_output_rows} output rows")

    # Read the materialized snapshot.
    snap = job.snapshot("revenue")
    print(f"Revenue after batch 1:\n{snap}")
    # Expected: us=150, eu=200

    # ── Feed batch 2: more insertions ──────────────────────────
    batch2 = pa.RecordBatch.from_pydict(
        {"region": ["eu", "jp"], "amount": [50.0, 300.0]}
    )
    job.feed("sales", krishiv.DeltaBatch.from_inserts(batch2))
    summary = job.step()
    snap = job.snapshot("revenue")
    print(f"Revenue after batch 2:\n{snap}")
    # Expected: us=150, eu=250, jp=300

    # ── Update: retract old + insert new ───────────────────────
    before = pa.RecordBatch.from_pydict(
        {"region": ["us"], "amount": [100.0]}  # retract first "us" row
    )
    after = pa.RecordBatch.from_pydict(
        {"region": ["us"], "amount": [200.0]}  # replace with new value
    )
    job.feed("sales", krishiv.DeltaBatch.from_update(before, after))
    summary = job.step()
    snap = job.snapshot("revenue")
    print(f"Revenue after update:\n{snap}")
    # Expected: us=250, eu=250, jp=300

    # ── Checkpoint & restore ───────────────────────────────────
    ckpt = job.checkpoint()
    print(f"Checkpoint size: {len(ckpt)} bytes")

    job.restore(ckpt)
    snap = job.snapshot("revenue")
    print(f"Revenue after restore:\n{snap}")
    # Should match pre-restore state

    # ── Serialize a DeltaBatch ────────────────────────────────
    db = krishiv.DeltaBatch.from_inserts(batch1)
    serialized = db.serialize()
    restored = krishiv.DeltaBatch.deserialize(serialized)
    assert restored.num_rows == db.num_rows, "serialization roundtrip failed"
    print(f"Serialization roundtrip: {db.num_rows} rows preserved")


if __name__ == "__main__":
    main()
