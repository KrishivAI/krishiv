"""Comprehensive DataFrame method tests."""

import pytest

import krishiv as ks


@pytest.fixture
def session():
    return ks.Session.local()


@pytest.fixture
def simple_df(session):
    return session.sql("SELECT 1 AS x, 2 AS y")


@pytest.fixture
def multi_row_df(session):
    return session.sql(
        "SELECT 1 AS x, 10 AS y, 'a' AS g "
        "UNION ALL SELECT 2, 20, 'a' "
        "UNION ALL SELECT 3, 30, 'b' "
        "UNION ALL SELECT 4, 40, 'b'"
    )


def test_sql_create_and_collect(session):
    df = session.sql("SELECT 42 AS answer")
    result = df.collect()
    assert result.row_count == 1
    table = result.to_arrow()
    assert table.column("answer")[0].as_py() == 42


def test_collect_pretty(session):
    df = session.sql("SELECT 1 AS x, 'hello' AS s")
    result = df.collect()
    text = result.pretty()
    assert "x" in text
    assert "s" in text


def test_collect_batches(session):
    df = session.sql("SELECT 1 AS x")
    result = df.collect_batches()
    assert result.row_count == 1


def test_collect_with_stats(session):
    df = session.sql("SELECT 1 AS x, 2 AS y")
    result, stats = df.collect_with_stats()
    assert result.row_count == 1
    assert "output_rows" in stats
    assert stats["output_rows"] == 1


def test_collect_async(session):
    df = session.sql("SELECT 1 AS x")
    result = df.collect_async()
    assert result.row_count == 1


def test_select(session):
    df = session.sql("SELECT 1 AS a, 2 AS b, 3 AS c")
    result = df.select(["a", "c"]).collect()
    table = result.to_arrow()
    assert table.column_names == ["a", "c"]


def test_select_columns(session):
    df = session.sql("SELECT 1 AS a, 2 AS b, 3 AS c")
    result = df.select_columns([ks.col("a"), ks.col("c")]).collect()
    table = result.to_arrow()
    assert table.column_names == ["a", "c"]


def test_select_exprs(session):
    df = session.sql("SELECT 1 AS a, 2 AS b")
    result = df.select_exprs(["a + b AS total"]).collect()
    table = result.to_arrow()
    assert "total" in table.column_names
    assert table.column("total")[0].as_py() == 3


def test_filter(session):
    df = session.sql(
        "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3"
    )
    result = df.filter("x > 1").collect()
    assert result.row_count == 2


def test_filter_column(session):
    df = session.sql(
        "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3"
    )
    result = df.filter_column(ks.col("x") > 1).collect()
    assert result.row_count == 2


def test_with_column(session):
    df = session.sql("SELECT 1 AS x")
    result = df.with_column("double_x", "x * 2").collect()
    table = result.to_arrow()
    assert "double_x" in table.column_names
    assert table.column("double_x")[0].as_py() == 2


def test_drop_columns(session):
    df = session.sql("SELECT 1 AS a, 2 AS b, 3 AS c")
    result = df.drop_columns(["b"]).collect()
    table = result.to_arrow()
    assert table.column_names == ["a", "c"]


def test_drop_nulls(session):
    df = session.sql(
        "SELECT 1 AS x UNION ALL SELECT NULL UNION ALL SELECT 3"
    )
    result = df.drop_nulls().collect()
    assert result.row_count == 2


def test_fill_null(session):
    df = session.sql(
        "SELECT 1 AS x UNION ALL SELECT NULL UNION ALL SELECT 3"
    )
    result = df.fill_null("x", "99").collect()
    table = result.to_arrow()
    assert table.column("x")[1].as_py() == 99


def test_group_by_columns_agg_columns(session, multi_row_df):
    grouped = multi_row_df.group_by_columns([ks.col("g")]).agg_columns(
        [ks.sum(ks.col("y")).alias("total"), ks.count(ks.col("x")).alias("cnt")]
    )
    result = grouped.collect()
    assert result.row_count == 2
    text = result.pretty()
    assert "total" in text
    assert "cnt" in text


def test_group_by_strings_agg_columns(session, multi_row_df):
    grouped = multi_row_df.group_by(["g"]).agg_columns(
        [ks.sum(ks.col("y")).alias("total")]
    )
    result = grouped.collect()
    assert result.row_count == 2


def test_group_by_count(multi_row_df):
    grouped = multi_row_df.group_by(["g"]).count()
    result = grouped.collect()
    assert result.row_count == 2
    table = result.to_arrow()
    assert "count" in table.column_names


def test_sort(session):
    df = session.sql(
        "SELECT 3 AS x UNION ALL SELECT 1 UNION ALL SELECT 2"
    )
    result = df.sort(["x"]).collect()
    table = result.to_arrow()
    values = [table.column("x")[i].as_py() for i in range(3)]
    assert values == [1, 2, 3]


def test_order_by(session):
    df = session.sql(
        "SELECT 3 AS x UNION ALL SELECT 1 UNION ALL SELECT 2"
    )
    result = df.order_by(["x"]).collect()
    table = result.to_arrow()
    values = [table.column("x")[i].as_py() for i in range(3)]
    assert values == [1, 2, 3]


def test_limit(session):
    df = session.sql(
        "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4"
    )
    result = df.limit(2).collect()
    assert result.row_count == 2


def test_distinct(session):
    df = session.sql(
        "SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2"
    )
    result = df.distinct().collect()
    assert result.row_count == 2


def test_union(session):
    df1 = session.sql("SELECT 1 AS x")
    df2 = session.sql("SELECT 2 AS x")
    result = df1.union(df2).collect()
    assert result.row_count == 2


def test_union_distinct(session):
    df1 = session.sql("SELECT 1 AS x")
    df2 = session.sql("SELECT 1 AS x UNION ALL SELECT 2 AS x")
    result = df1.union_distinct(df2).collect()
    assert result.row_count == 2


def test_except_(session):
    df1 = session.sql(
        "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3"
    )
    df2 = session.sql("SELECT 2 AS x")
    result = df1.except_(df2).collect()
    assert result.row_count == 2
    table = result.to_arrow()
    values = sorted(table.column("x")[i].as_py() for i in range(2))
    assert values == [1, 3]


def test_except_all(session):
    df1 = session.sql(
        "SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2"
    )
    df2 = session.sql("SELECT 1 AS x")
    result = df1.except_all(df2).collect()
    assert result.row_count == 1


def test_except_distinct(session):
    df1 = session.sql(
        "SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2"
    )
    df2 = session.sql("SELECT 1 AS x")
    result = df1.except_distinct(df2).collect()
    assert result.row_count == 1
    table = result.to_arrow()
    assert table.column("x")[0].as_py() == 2


def test_intersect(session):
    df1 = session.sql(
        "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3"
    )
    df2 = session.sql(
        "SELECT 2 AS x UNION ALL SELECT 3 UNION ALL SELECT 4"
    )
    result = df1.intersect(df2).collect()
    assert result.row_count == 2


def test_intersect_distinct(session):
    df1 = session.sql(
        "SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2"
    )
    df2 = session.sql("SELECT 1 AS x UNION ALL SELECT 2 AS x")
    result = df1.intersect_distinct(df2).collect()
    assert result.row_count == 2


def test_join_on(session):
    left = session.sql("SELECT 1 AS left_id, 'alice' AS name")
    right = session.sql("SELECT 1 AS right_id, 100 AS score")
    result = left.join_on(right, ["left_id"], ["right_id"]).collect()
    table = result.to_arrow()
    assert "name" in table.column_names
    assert "score" in table.column_names
    assert table.column("score")[0].as_py() == 100


def test_rename(session):
    df = session.sql("SELECT 1 AS a, 2 AS b")
    result = df.rename("a", "alpha").collect()
    table = result.to_arrow()
    assert "alpha" in table.column_names
    assert "a" not in table.column_names


def test_describe(session):
    df = session.sql("SELECT 1 AS x, 10.5 AS y")
    result = df.describe().collect()
    assert result.row_count >= 1
    text = result.pretty()
    assert "count" in text.lower() or "mean" in text.lower()


def test_explain(session, simple_df):
    plan = simple_df.explain()
    assert isinstance(plan, str)
    assert len(plan) > 0


def test_explain_logical(session, simple_df):
    plan = simple_df.explain_logical()
    assert isinstance(plan, str)
    assert len(plan) > 0


def test_explain_mode(session, simple_df):
    plan = simple_df.explain_mode("physical")
    assert isinstance(plan, str)
    assert len(plan) > 0


def test_cache(session, simple_df):
    cached = simple_df.cache()
    result = cached.collect()
    assert result.row_count == 1


def test_persist(session, simple_df):
    persisted = simple_df.persist()
    result = persisted.collect()
    assert result.row_count == 1


def test_unpersist(session, simple_df):
    persisted = simple_df.persist()
    persisted.unpersist()
    result = persisted.collect()
    assert result.row_count == 1


def test_num_rows(session, simple_df):
    assert simple_df.num_rows() == 1


def test_schema(session, simple_df):
    s = simple_df.schema()
    assert s is not None


def test_columns(session, simple_df):
    cols = simple_df.columns()
    assert "x" in cols
    assert "y" in cols


def test_boundedness(session, simple_df):
    b = simple_df.boundedness()
    assert b is not None


def test_is_bounded(session, simple_df):
    assert simple_df.is_bounded() is True


def test_sample(session):
    df = session.sql(
        "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3 "
        "UNION ALL SELECT 4 UNION ALL SELECT 5"
    )
    result = df.sample(0.5).collect()
    assert 1 <= result.row_count <= 5


def test_sample_full(session):
    df = session.sql(
        "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3"
    )
    result = df.sample(1.0).collect()
    assert result.row_count == 3


def test_repartition(session):
    df = session.sql(
        "SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3"
    )
    result = df.repartition(2).collect()
    assert result.row_count == 3


def test_create_or_replace_temp_view(session):
    df = session.sql("SELECT 100 AS val")
    df.create_or_replace_temp_view("tmp_view_test")
    result = session.sql("SELECT val FROM tmp_view_test").collect()
    assert result.row_count == 1
    table = result.to_arrow()
    assert table.column("val")[0].as_py() == 100


def test_create_or_replace_temp_view_overwrite(session):
    session.sql("SELECT 1 AS val").create_or_replace_temp_view("tmp_ow")
    session.sql("SELECT 2 AS val").create_or_replace_temp_view("tmp_ow")
    result = session.sql("SELECT val FROM tmp_ow").collect()
    table = result.to_arrow()
    assert table.column("val")[0].as_py() == 2


def test_alias(session):
    df = session.sql("SELECT 1 AS x")
    result = df.alias("renamed").collect()
    assert result.row_count == 1


def test_show(session, simple_df, capsys):
    simple_df.show()
    captured = capsys.readouterr()
    assert captured.out is not None


def test_repr(session, simple_df):
    r = repr(simple_df)
    assert isinstance(r, str)
    assert len(r) > 0


def test_to_streaming(session, simple_df):
    stream = simple_df.to_streaming()
    assert stream is not None


def test_pivot(session):
    df = session.sql(
        "SELECT 'r1' AS cat, 'c1' AS attr, 10 AS val "
        "UNION ALL SELECT 'r1', 'c2', 20 "
        "UNION ALL SELECT 'r2', 'c1', 30 "
        "UNION ALL SELECT 'r2', 'c2', 40"
    )
    result = df.pivot("cat", "attr", "val", ["c1", "c2"]).collect()
    assert result.row_count == 2
    table = result.to_arrow()
    assert "c1" in table.column_names
    assert "c2" in table.column_names


def test_unpivot(session):
    df = session.sql("SELECT 1 AS id, 10 AS v1, 20 AS v2")
    result = df.unpivot(["v1", "v2"], "variable", "value").collect()
    assert result.row_count == 2
    table = result.to_arrow()
    assert "variable" in table.column_names
    assert "value" in table.column_names


def test_chain_operations(session):
    df = session.sql(
        "SELECT 1 AS x, 10 AS y, 'a' AS g "
        "UNION ALL SELECT 2, 20, 'a' "
        "UNION ALL SELECT 3, 30, 'b' "
        "UNION ALL SELECT NULL, 40, 'b'"
    )
    result = (
        df.drop_nulls()
        .filter("y > 10")
        .sort(["x"])
        .limit(2)
        .collect()
    )
    assert result.row_count <= 2


def test_with_column_and_select(session):
    df = session.sql("SELECT 1 AS a, 2 AS b")
    result = df.with_column("total", "a + b").select(["total"]).collect()
    table = result.to_arrow()
    assert table.column("total")[0].as_py() == 3


def test_group_by_distinct_multi_col(session):
    df = session.sql(
        "SELECT 1 AS a, 10 AS b "
        "UNION ALL SELECT 1, 10 "
        "UNION ALL SELECT 2, 20"
    )
    result = df.distinct().collect()
    assert result.row_count == 2
