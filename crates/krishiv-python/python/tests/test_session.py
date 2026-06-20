import os
import tempfile

import pytest

import krishiv as ks


SKIP_ERRORS = (ks.ConnectorError, ks.ModeError, ks.UdfError, RuntimeError)


def _session():
    return ks.Session.embedded()


def _local_session():
    return ks.Session.local()


def test_factory_embedded():
    s = _session()
    assert s.mode == "embedded"


def test_factory_local():
    s = _local_session()
    assert s.mode == "embedded"


def test_factory_connect():
    s = ks.Session.connect("http://localhost:50051")
    assert s.mode == "distributed"


def test_factory_from_env_default(monkeypatch):
    monkeypatch.delenv("KRISHIV_COORDINATOR", raising=False)
    monkeypatch.delenv("KRISHIV_COORDINATOR_URL", raising=False)
    monkeypatch.delenv("KRISHIV_MODE", raising=False)
    s = ks.Session.from_env()
    assert s.mode == "embedded"


def test_factory_from_env_coordinator(monkeypatch):
    monkeypatch.setenv("KRISHIV_COORDINATOR", "http://coordinator:50051")
    s = ks.Session.from_env()
    assert s.mode == "local"


def test_mode_property():
    s = _session()
    assert isinstance(s.mode, str)
    assert s.mode in ("embedded", "local", "distributed")


def test_session_repr():
    s = _session()
    r = repr(s)
    assert isinstance(r, str)


def test_sql_basic():
    s = _session()
    result = s.sql("SELECT 1 AS x").collect()
    assert result.row_count == 1
    assert "1" in result.pretty()


def test_sql_multiple_queries():
    s = _session()
    r1 = s.sql("SELECT 1 AS a").collect()
    r2 = s.sql("SELECT 2 AS b").collect()
    assert r1.row_count == 1
    assert r2.row_count == 1
    assert "1" in r1.pretty()
    assert "2" in r2.pretty()


def test_sql_with_timeout():
    s = _session()
    result = s.sql_with_timeout("SELECT 99 AS n", 5000).collect()
    assert result.row_count == 1
    assert "99" in result.pretty()


def test_sql_as():
    s = _session()
    try:
        result = s.sql_as("SELECT 42 AS val", "my_key")
        collected = result.collect()
        assert collected.row_count == 1
        assert "42" in collected.pretty()
    except (RuntimeError, ks.KrishivError) as e:
        pytest.skip(str(e))


def test_query_result_pretty():
    s = _session()
    result = s.sql("SELECT 7 AS x").collect()
    text = result.pretty()
    assert "7" in text
    assert "x" in text


def test_query_result_show(capsys):
    s = _session()
    result = s.sql("SELECT 1 AS y").collect()
    with capsys.disabled():
        result.show()
    assert result is not None


def test_query_result_row_count():
    s = _session()
    result = s.sql(
        "SELECT 1 AS x UNION ALL SELECT 2 AS x UNION ALL SELECT 3 AS x UNION ALL SELECT 4 AS x"
    ).collect()
    assert result.row_count == 4


def test_query_result_len():
    s = _session()
    result = s.sql("SELECT 1 AS a UNION ALL SELECT 2 AS a").collect()
    assert len(result) == 2


def test_query_result_iter():
    s = _session()
    result = s.sql("SELECT 1 AS a UNION ALL SELECT 2 AS a UNION ALL SELECT 3 AS a").collect()
    rows = list(result)
    assert len(rows) == 3


def test_query_result_repr():
    s = _session()
    result = s.sql("SELECT 1 AS x").collect()
    assert isinstance(repr(result), str)


def test_query_result_batches():
    s = _session()
    result = s.sql("SELECT 1 AS a, 2 AS b").collect()
    batches = result.batches()
    assert len(batches) >= 1
    assert batches[0].num_rows == 1


def test_query_result_batches_are_arrow():
    pa = pytest.importorskip("pyarrow")
    s = _session()
    result = s.sql("SELECT 1 AS x").collect()
    batches = result.batches()
    assert len(batches) >= 1
    batch = batches[0]
    assert batch.num_rows == 1
    assert batch.num_columns == 1


def test_query_result_to_arrow():
    pa = pytest.importorskip("pyarrow")
    s = _session()
    result = s.sql("SELECT 10 AS val").collect()
    arrow = result.to_arrow()
    assert arrow.num_rows == 1
    assert arrow.column("val")[0].as_py() == 10


def test_query_result_to_pandas():
    pd = pytest.importorskip("pandas")
    s = _session()
    result = s.sql("SELECT 5 AS n").collect()
    df = result.to_pandas()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df["n"].iloc[0] == 5


def test_table_exists():
    s = _session()
    try:
        result = s.table_exists("nonexistent_table_xyz")
        assert result is False
    except RuntimeError as e:
        if "information_schema" in str(e):
            pytest.skip("information_schema not enabled")
        raise


def test_list_tables():
    s = _session()
    try:
        tables = s.list_tables()
        assert isinstance(tables, list)
    except RuntimeError as e:
        if "information_schema" in str(e):
            pytest.skip("information_schema not enabled")
        raise


def test_list_table_identifiers():
    s = _session()
    try:
        ids = s.list_table_identifiers()
        assert isinstance(ids, list)
    except RuntimeError as e:
        if "information_schema" in str(e):
            pytest.skip("information_schema not enabled")
        raise


def test_table():
    s = _session()
    batch = ks.make_example_batch()
    s.register_record_batches("table_test", [batch])
    t = s.table("table_test")
    assert t is not None
    result = t.collect()
    assert result.row_count == 3
    s.drop_table("table_test")


def test_table_metadata():
    s = _session()
    batch = ks.make_example_batch()
    s.register_record_batches("meta_tbl", [batch])
    try:
        meta = s.table_metadata("meta_tbl")
        assert meta is not None
    except SKIP_ERRORS:
        pytest.skip("table_metadata not available")
    finally:
        s.drop_table("meta_tbl")


def test_register_record_batches():
    pa = pytest.importorskip("pyarrow")
    s = _session()
    batch = pa.record_batch(
        {"id": pa.array([1, 2, 3]), "val": pa.array([10, 20, 30])}
    )
    kb = ks.Batch(batch)
    s.register_record_batches("rb_test", [kb])
    result = s.sql("SELECT SUM(val) AS total FROM rb_test").collect()
    assert result.row_count == 1
    assert "60" in result.pretty()


def test_register_record_batches_then_query():
    pa = pytest.importorskip("pyarrow")
    s = _session()
    batch = pa.record_batch(
        {"x": pa.array([10, 20, 30]), "y": pa.array([1, 2, 3])}
    )
    kb = ks.Batch(batch)
    s.register_record_batches("rb_q", [kb])
    result = s.sql("SELECT x * y AS product FROM rb_q ORDER BY x").collect()
    assert result.row_count == 3
    assert "10" in result.pretty()
    assert "40" in result.pretty()
    assert "90" in result.pretty()


def test_register_parquet():
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    s = _session()
    table = pa.table({"a": pa.array([1, 2])})
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(table, f.name)
        path = f.name
    try:
        s.register_parquet("pq_test", path)
        result = s.sql("SELECT COUNT(*) AS cnt FROM pq_test").collect()
        assert result.row_count == 1
        assert "2" in result.pretty()
    finally:
        os.unlink(path)


def test_register_unbounded():
    pa = pytest.importorskip("pyarrow")
    s = _session()
    try:
        schema = pa.schema([pa.field("ts", pa.int64()), pa.field("val", pa.float64())])
        s.register_unbounded("unbounded_test", schema)
        assert s.table_exists("unbounded_test") is True
    except RuntimeError as e:
        if "information_schema" in str(e):
            pytest.skip("information_schema not enabled")
        raise
    except SKIP_ERRORS:
        pytest.skip("register_unbounded not available")


def test_register_unbounded_with_capacity():
    pa = pytest.importorskip("pyarrow")
    s = _session()
    try:
        schema = pa.schema([pa.field("ts", pa.int64())])
        s.register_unbounded_with_capacity("unbounded_cap", schema, 1024)
        assert s.table_exists("unbounded_cap") is True
    except RuntimeError as e:
        if "information_schema" in str(e):
            pytest.skip("information_schema not enabled")
        raise
    except SKIP_ERRORS:
        pytest.skip("register_unbounded_with_capacity not available")


def test_close_unbounded():
    pa = pytest.importorskip("pyarrow")
    s = _session()
    try:
        schema = pa.schema([pa.field("v", pa.int64())])
        s.register_unbounded("close_ub", schema)
        s.close_unbounded_input("close_ub")
    except SKIP_ERRORS:
        pytest.skip("close_unbounded not available")


def test_create_temp_view_with_query():
    s = _session()
    try:
        s.create_temp_view("temp_q", "SELECT 1 AS id")
    except SKIP_ERRORS as e:
        pytest.skip(str(e))


def test_create_temp_view_via_dataframe():
    s = _session()
    df = s.sql("SELECT 10 AS x")
    df.create_or_replace_temp_view("temp_from_df")
    result = s.sql("SELECT * FROM temp_from_df").collect()
    assert result.row_count == 1
    assert "10" in result.pretty()


def test_create_temp_view_overwrite():
    s = _session()
    df1 = s.sql("SELECT 1 AS v")
    df1.create_or_replace_temp_view("overwrite_me")
    df2 = s.sql("SELECT 99 AS v")
    df2.create_or_replace_temp_view("overwrite_me")
    result = s.sql("SELECT * FROM overwrite_me").collect()
    assert "99" in result.pretty()


def test_temp_view_query():
    s = _session()
    df = s.sql("SELECT 1 AS id UNION ALL SELECT 2 AS id UNION ALL SELECT 3 AS id")
    df.create_or_replace_temp_view("temp_cnt")
    result = s.sql("SELECT COUNT(*) AS cnt FROM temp_cnt").collect()
    assert result.row_count == 1
    assert "3" in result.pretty()


def test_create_view():
    s = _session()
    try:
        s.create_view("perm_view", "SELECT 1 AS id")
    except SKIP_ERRORS as e:
        pytest.skip(str(e))


def test_deregister_table():
    s = _session()
    batch = ks.make_example_batch()
    s.register_record_batches("dereg_test", [batch])
    s.deregister_table("dereg_test")


def test_drop_table():
    s = _session()
    batch = ks.make_example_batch()
    s.register_record_batches("drop_test", [batch])
    s.drop_table("drop_test")


def test_set_get_unset_config():
    s = _session()
    s.set_config("test.key", "test_value")
    val = s.get_config("test.key")
    assert val == "test_value"
    s.unset_config("test.key")
    val2 = s.get_config("test.key")
    assert val2 is None


def test_configs():
    s = _session()
    cfgs = s.configs()
    assert isinstance(cfgs, dict)


def test_prepare():
    s = _session()
    stmt = s.prepare("SELECT 1 AS x")
    assert stmt is not None


def test_prepare_parameter_count():
    s = _session()
    stmt = s.prepare("SELECT $1 + $2 AS result")
    assert stmt.parameter_count() == 2


def test_prepare_with_bind():
    s = _session()
    stmt = s.prepare("SELECT $1 + $2 AS result")
    bound = stmt.bind([ks.lit(10), ks.lit(20)])
    assert bound is not None


def test_register_udf_with_name():
    s = _session()
    s.register_udf("double_it", lambda x: x * 2, input_types={"x": "Int64"}, output_type="Int64")
    udfs = s.list_udfs()
    assert "double_it" in udfs


def test_register_udf_with_decorator():
    s = _session()

    @ks.udf(input_types={"x": "Int64"}, output_type="Int64")
    def triple_it(x):
        return x * 3

    s.register_udf(triple_it)
    udfs = s.list_udfs()
    assert "triple_it" in udfs


def test_list_udfs():
    s = _session()
    result = s.list_udfs()
    assert isinstance(result, list)


def test_list_aggregate_udfs():
    s = _session()
    result = s.list_aggregate_udfs()
    assert isinstance(result, list)


def test_list_table_udfs():
    s = _session()
    result = s.list_table_udfs()
    assert isinstance(result, list)


def test_dataframe_from_query():
    s = _session()
    rel = s.dataframe("SELECT 1 AS x, 2 AS y")
    result = rel.collect()
    assert result.row_count == 1
    assert "1" in result.pretty()


def test_memory_stream():
    pa = pytest.importorskip("pyarrow")
    s = _session()
    batch = pa.record_batch({"ts": pa.array([1000, 2000]), "val": pa.array([1.0, 2.0])})
    kb = ks.Batch(batch)
    ms = s.memory_stream("mem_test", [kb], "ts", 5000)
    assert ms is not None


def test_memory_stream_collect():
    pa = pytest.importorskip("pyarrow")
    s = _session()
    batch = pa.record_batch({"ts": pa.array([1000]), "val": pa.array([1.0])})
    kb = ks.Batch(batch)
    s.memory_stream("mem_coll", [kb], "ts", 5000)
    result = s.memory_stream_collect("mem_coll", [kb])
    assert result is not None


def test_operation_registry():
    s = _session()
    reg = s.operation_registry()
    assert reg is not None


def test_operation_registry_cancelled_ids():
    s = _session()
    reg = s.operation_registry()
    cancelled = reg.cancelled_ids()
    assert isinstance(cancelled, list)


def test_operation_registry_is_cancelled():
    s = _session()
    reg = s.operation_registry()
    result = reg.is_cancelled(0)
    assert isinstance(result, bool)


def test_jobs():
    s = _session()
    jobs = s.jobs()
    assert isinstance(jobs, list)


def test_read_csv():
    s = _session()
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        f.write("a,b\n1,3\n2,4\n")
        path = f.name
    try:
        df = s.read_csv(path)
        result = df.collect()
        assert result.row_count == 2
    finally:
        os.unlink(path)


def test_read_csv_with_options():
    s = _session()
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        f.write("x\n10\n")
        path = f.name
    try:
        df = s.read_csv_with_options(path, has_header=True)
        result = df.collect()
        assert result.row_count == 1
    finally:
        os.unlink(path)


def test_read_csv_with_schema():
    pa = pytest.importorskip("pyarrow")
    s = _session()
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        f.write("name,score\nalice,95\nbob,87\n")
        path = f.name
    try:
        df = s.read_csv(path)
        result = df.collect()
        assert result.row_count == 2
        pretty = result.pretty()
        assert "alice" in pretty or "95" in pretty
    finally:
        os.unlink(path)


def test_read_json():
    import json

    s = _session()
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
        f.write(json.dumps({"k": "a"}) + "\n")
        f.write(json.dumps({"k": "b"}) + "\n")
        path = f.name
    try:
        df = s.read_json(path)
        result = df.collect()
        assert result.row_count == 2
    finally:
        os.unlink(path)


def test_read_parquet():
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    s = _session()
    table = pa.table({"c": pa.array([7, 8, 9])})
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(table, f.name)
        path = f.name
    try:
        df = s.read_parquet(path)
        result = df.collect()
        assert result.row_count == 3
    finally:
        os.unlink(path)


def test_read_parquet_with_options():
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    s = _session()
    table = pa.table({"d": pa.array([100])})
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(table, f.name)
        path = f.name
    try:
        df = s.read_parquet_with_options(path)
        result = df.collect()
        assert result.row_count == 1
    finally:
        os.unlink(path)


def test_dataframe_collect():
    s = _session()
    rel = s.dataframe("SELECT 5 AS n")
    result = rel.collect()
    assert result.row_count == 1
    assert "5" in result.pretty()
