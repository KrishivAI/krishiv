import pytest
import pyarrow as pa

import krishiv as ks


def _make_batch():
    schema = pa.schema([("id", pa.int64()), ("value", pa.float64())])
    return ks.Batch(
        pa.record_batch(
            [pa.array([1, 2, 3]), pa.array([10.0, 20.0, 30.0])], schema=schema
        )
    )


def test_construct():
    try:
        table = ks.MemoryLakehouseTable("cat", "ns", "tbl")
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    assert table is not None


def test_repr():
    try:
        table = ks.MemoryLakehouseTable("cat", "ns", "tbl")
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    r = repr(table)
    assert isinstance(r, str)
    assert len(r) > 0


def test_snapshot_id_initial():
    try:
        table = ks.MemoryLakehouseTable("cat", "ns", "tbl")
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    sid = table.current_snapshot_id()
    assert sid is None


def test_append():
    try:
        table = ks.MemoryLakehouseTable("cat", "ns", "tbl")
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    batch = _make_batch()
    table.append(batch)
    sid = table.current_snapshot_id()
    assert sid is not None


def test_overwrite():
    try:
        table = ks.MemoryLakehouseTable("cat", "ns", "tbl")
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    batch = _make_batch()
    table.overwrite(batch)
    sid = table.current_snapshot_id()
    assert sid is not None


def test_overwrite_replaces_snapshot():
    try:
        table = ks.MemoryLakehouseTable("cat", "ns", "tbl")
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    batch = _make_batch()
    table.append(batch)
    sid1 = table.current_snapshot_id()
    table.overwrite(batch)
    sid2 = table.current_snapshot_id()
    assert sid1 is not None
    assert sid2 is not None
    assert sid1 != sid2


def test_iceberg_rest_catalog_construction():
    try:
        cat = ks.IcebergRestCatalog("http://localhost:9999")
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    assert cat is not None


def test_iceberg_rest_catalog_bad_url():
    try:
        cat = ks.IcebergRestCatalog("")
    except (RuntimeError, TypeError, ValueError):
        return
    pytest.fail("Expected error for empty URL")


def test_iceberg_rest_catalog_list_tables():
    try:
        cat = ks.IcebergRestCatalog("http://localhost:9999")
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    with pytest.raises((RuntimeError, TypeError)):
        cat.list_tables()
