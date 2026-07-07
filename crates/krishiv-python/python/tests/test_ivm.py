"""IVM (DeltaBatch) integration tests for the Python API.

These tests validate the full Python IVM lifecycle:
1. Create an incremental view
2. Feed delta batches
3. Step the view
4. Read snapshots
5. Checkpoint and restore
"""

import krishiv


def test_delta_batch_from_inserts():
    """DeltaBatch.from_inserts creates a batch with +1 weights."""
    batch = krishiv.make_example_batch()
    db = krishiv.DeltaBatch.from_inserts(batch)
    assert db.num_rows > 0
    assert not db.is_empty()
    assert db.is_insert_only()
    assert "DeltaBatch(rows=" in repr(db)


def test_delta_batch_to_batch_roundtrip():
    """DeltaBatch → to_batch → from_weighted roundtrip preserves rows."""
    batch = krishiv.make_example_batch()
    db = krishiv.DeltaBatch.from_inserts(batch)
    weighted = db.to_batch()
    db2 = krishiv.DeltaBatch.from_weighted(weighted)
    assert db2.num_rows == db.num_rows


def test_delta_batch_filter_positive():
    """filter_positive returns insertions without weight column."""
    batch = krishiv.make_example_batch()
    db = krishiv.DeltaBatch.from_inserts(batch)
    pos = db.filter_positive()
    assert pos is not None
    assert pos.num_rows == db.num_rows  # All rows are insertions


def test_delta_batch_negate_drop_zeros():
    """negate + negate returns original weight pattern."""
    batch = krishiv.make_example_batch()
    db = krishiv.DeltaBatch.from_inserts(batch)
    neg = db.negate()
    assert not neg.is_insert_only()  # All weights are -1
    neg2 = neg.negate()
    assert neg2.is_insert_only()  # Back to +1


def test_delta_batch_serialize_deserialize():
    """Serialize → deserialize roundtrip preserves data."""
    batch = krishiv.make_example_batch()
    db = krishiv.DeltaBatch.from_inserts(batch)
    serialized = db.serialize()
    restored = krishiv.DeltaBatch.deserialize(serialized)
    assert restored.num_rows == db.num_rows
    assert restored.is_insert_only()


def test_delta_batch_from_update():
    """from_update produces retract+insert pairs."""
    import pyarrow as pa

    before = pa.RecordBatch.from_pydict({"x": [1, 2], "y": ["a", "b"]})
    after = pa.RecordBatch.from_pydict({"x": [1, 3], "y": ["a", "c"]})
    db = krishiv.DeltaBatch.from_update(before, after)
    # One retraction (-1) + one insertion (+1) = 2 rows total
    assert db.num_rows == 2


def test_ivm_basic_flow():
    """Full IVM lifecycle: register → feed → step → snapshot."""
    session = krishiv.Session()
    job = session.ivm("test_sales")

    class SalesTotal(krishiv.Schema):
        region: str
        total: float

    job.register_view(
        "revenue",
        "SELECT region, SUM(amount) AS total FROM sales GROUP BY region",
        SalesTotal,
        is_materialized=True,
    )

    import pyarrow as pa

    batch = pa.RecordBatch.from_pydict(
        {
            "region": ["us", "eu", "us", "jp"],
            "amount": [100.0, 200.0, 50.0, 300.0],
        }
    )
    db = krishiv.DeltaBatch.from_inserts(batch)
    job.feed("sales", db)
    summary = job.step()
    assert summary.active_views >= 1
    assert summary.total_output_rows >= 1

    snap = job.snapshot("revenue")
    assert snap is not None
    assert snap.num_rows > 0


def test_ivm_checkpoint_restore():
    """Checkpoint → restore preserves view state."""
    session = krishiv.Session()
    job = session.ivm("test_ckpt")

    class CountView(krishiv.Schema):
        cnt: int

    job.register_view(
        "cnt",
        "SELECT COUNT(*) AS cnt FROM src",
        CountView,
        is_materialized=True,
    )

    import pyarrow as pa

    batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3, 4, 5]})
    db = krishiv.DeltaBatch.from_inserts(batch)
    job.feed("src", db)
    job.step()
    snap1 = job.snapshot("cnt")

    ckpt = job.checkpoint()
    assert len(ckpt) > 0
    job.restore(ckpt)
    snap2 = job.snapshot("cnt")

    assert snap2 is not None
    assert snap2.num_rows == snap1.num_rows
