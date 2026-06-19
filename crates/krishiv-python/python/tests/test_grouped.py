import krishiv as ks
from krishiv.sql import functions as F

SESSION = ks.Session.local()

TWO_GROUP_SQL = (
    "SELECT 1 AS grp1, 'a' AS grp2, 10 AS val "
    "UNION ALL SELECT 1 AS grp1, 'a' AS grp2, 20 AS val "
    "UNION ALL SELECT 2 AS grp1, 'b' AS grp2, 30 AS val "
    "UNION ALL SELECT 2 AS grp1, 'b' AS grp2, 40 AS val"
)


def _pretty(dataframe):
    result = dataframe.collect()
    assert result.row_count >= 1
    return result.pretty()


def test_agg_columns_sum():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg_columns([ks.sum(ks.col("val")).alias("total")])
    text = _pretty(result)
    assert "total" in text
    assert "30" in text


def test_agg_columns_count():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg_columns([ks.count(ks.col("val")).alias("cnt")])
    text = _pretty(result)
    assert "cnt" in text
    assert "2" in text


def test_agg_columns_avg():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg_columns([ks.avg(ks.col("val")).alias("average")])
    text = _pretty(result)
    assert "average" in text
    assert "15" in text


def test_agg_columns_min_max():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg_columns([
        ks.min(ks.col("val")).alias("lo"),
        ks.max(ks.col("val")).alias("hi"),
    ])
    text = _pretty(result)
    assert "lo" in text
    assert "hi" in text
    assert "10" in text
    assert "20" in text


def test_agg_columns_multiple_aggregations():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg_columns([
        ks.count(ks.col("val")).alias("cnt"),
        ks.sum(ks.col("val")).alias("total"),
        ks.avg(ks.col("val")).alias("avg"),
        ks.min(ks.col("val")).alias("lo"),
        ks.max(ks.col("val")).alias("hi"),
    ])
    text = _pretty(result)
    assert "cnt" in text
    assert "total" in text
    assert "avg" in text
    assert "lo" in text
    assert "hi" in text
    assert "30" in text
    assert "15" in text


def test_agg_sql_string_syntax():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg(["SUM(val) AS total", "COUNT(val) AS cnt"])
    text = _pretty(result)
    assert "total" in text
    assert "cnt" in text
    assert "30" in text
    assert "2" in text


def test_agg_sql_string_single():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg(["AVG(val) AS average"])
    text = _pretty(result)
    assert "average" in text
    assert "15" in text


def test_count():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.count()
    text = _pretty(result)
    assert "count" in text
    assert "2" in text


def test_count_multiple_groups():
    df = SESSION.sql(TWO_GROUP_SQL)
    grouped = df.group_by_columns([ks.col("grp1"), ks.col("grp2")])
    result = grouped.count()
    text = _pretty(result)
    assert "count" in text
    assert "2" in text
    assert "1" in text


def test_cube_two_columns():
    df = SESSION.sql(TWO_GROUP_SQL)
    grouped = df.group_by_columns([ks.col("grp1"), ks.col("grp2")])
    result = grouped.cube(
        [ks.col("grp1"), ks.col("grp2")],
        [ks.sum(ks.col("val")).alias("total")],
    )
    text = _pretty(result)
    assert "total" in text
    assert "grp1" in text
    assert "grp2" in text


def test_cube_with_count():
    df = SESSION.sql(TWO_GROUP_SQL)
    grouped = df.group_by_columns([ks.col("grp1"), ks.col("grp2")])
    result = grouped.cube(
        [ks.col("grp1"), ks.col("grp2")],
        [ks.count(ks.col("val")).alias("cnt")],
    )
    text = _pretty(result)
    assert "cnt" in text
    assert "grp1" in text
    assert "grp2" in text


def test_cube_single_column():
    df = SESSION.sql(
        "SELECT 'x' AS cat, 1 AS val "
        "UNION ALL SELECT 'x' AS cat, 2 AS val "
        "UNION ALL SELECT 'y' AS cat, 3 AS val"
    )
    grouped = df.group_by_columns([ks.col("cat")])
    result = grouped.cube(
        [ks.col("cat")],
        [ks.sum(ks.col("val")).alias("total")],
    )
    text = _pretty(result)
    assert "cat" in text
    assert "total" in text
    assert "6" in text


def test_rollup_two_columns():
    df = SESSION.sql(TWO_GROUP_SQL)
    grouped = df.group_by_columns([ks.col("grp1"), ks.col("grp2")])
    result = grouped.rollup(
        [ks.col("grp1"), ks.col("grp2")],
        [ks.sum(ks.col("val")).alias("total")],
    )
    text = _pretty(result)
    assert "total" in text
    assert "grp1" in text
    assert "grp2" in text


def test_rollup_with_multiple_aggregations():
    df = SESSION.sql(TWO_GROUP_SQL)
    grouped = df.group_by_columns([ks.col("grp1"), ks.col("grp2")])
    result = grouped.rollup(
        [ks.col("grp1"), ks.col("grp2")],
        [ks.sum(ks.col("val")).alias("total"), ks.count(ks.col("val")).alias("cnt")],
    )
    text = _pretty(result)
    assert "total" in text
    assert "cnt" in text


def test_rollup_single_column():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val UNION ALL SELECT 2 AS grp, 30 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.rollup(
        [ks.col("grp")],
        [ks.sum(ks.col("val")).alias("total")],
    )
    text = _pretty(result)
    assert "total" in text
    assert "grp" in text


def test_single_group_key():
    df = SESSION.sql(
        "SELECT 'x' AS grp, 5 AS val UNION ALL SELECT 'y' AS grp, 15 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg_columns([ks.sum(ks.col("val")).alias("s")])
    text = _pretty(result)
    assert "s" in text
    assert "5" in text
    assert "15" in text


def test_agg_columns_alias_naming():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg_columns([
        ks.sum(ks.col("val")).alias("custom_sum_name"),
    ])
    text = _pretty(result)
    assert "custom_sum_name" in text


def test_agg_sql_alias_naming():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg(["SUM(val) AS custom_name"])
    text = _pretty(result)
    assert "custom_name" in text


def test_cube_preserves_group_columns():
    df = SESSION.sql(TWO_GROUP_SQL)
    grouped = df.group_by_columns([ks.col("grp1"), ks.col("grp2")])
    result = grouped.cube(
        [ks.col("grp1"), ks.col("grp2")],
        [ks.sum(ks.col("val")).alias("total")],
    )
    text = _pretty(result)
    assert "grp1" in text
    assert "grp2" in text
    assert "total" in text


def test_rollup_preserves_group_columns():
    df = SESSION.sql(TWO_GROUP_SQL)
    grouped = df.group_by_columns([ks.col("grp1"), ks.col("grp2")])
    result = grouped.rollup(
        [ks.col("grp1"), ks.col("grp2")],
        [ks.sum(ks.col("val")).alias("total")],
    )
    text = _pretty(result)
    assert "grp1" in text
    assert "grp2" in text
    assert "total" in text


def test_cube_single_column_group():
    df = SESSION.sql(
        "SELECT 1 AS val UNION ALL SELECT 2 AS val UNION ALL SELECT 3 AS val"
    )
    grouped = df.group_by_columns([ks.col("val")])
    result = grouped.cube(
        [ks.col("val")],
        [ks.count(ks.col("val")).alias("cnt")],
    )
    text = _pretty(result)
    assert "cnt" in text
    assert "val" in text


def test_rollup_single_column_group():
    df = SESSION.sql(
        "SELECT 1 AS val UNION ALL SELECT 2 AS val UNION ALL SELECT 3 AS val"
    )
    grouped = df.group_by_columns([ks.col("val")])
    result = grouped.rollup(
        [ks.col("val")],
        [ks.sum(ks.col("val")).alias("total")],
    )
    text = _pretty(result)
    assert "total" in text
    assert "val" in text


def test_pretty_output_is_string():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg_columns([ks.sum(ks.col("val")).alias("total")])
    collected = result.collect()
    output = collected.pretty()
    assert isinstance(output, str)


def test_agg_with_nulls_in_data():
    df = SESSION.sql(
        "SELECT 1 AS grp, CAST(NULL AS BIGINT) AS val "
        "UNION ALL SELECT 1 AS grp, 10 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg_columns([
        ks.sum(ks.col("val")).alias("s"),
        ks.count(ks.col("val")).alias("c"),
    ])
    text = _pretty(result)
    assert "s" in text
    assert "c" in text
    assert "10" in text
    assert "1" in text


def test_count_all():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 2 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg_columns([ks.count_all().alias("total")])
    text = _pretty(result)
    assert "total" in text
    assert "1" in text


def test_cube_with_f_functions():
    df = SESSION.sql(TWO_GROUP_SQL)
    grouped = df.group_by_columns([ks.col("grp1"), ks.col("grp2")])
    result = grouped.cube(
        [ks.col("grp1"), ks.col("grp2")],
        [F.sum("val").alias("total")],
    )
    text = _pretty(result)
    assert "total" in text


def test_rollup_with_f_functions():
    df = SESSION.sql(TWO_GROUP_SQL)
    grouped = df.group_by_columns([ks.col("grp1"), ks.col("grp2")])
    result = grouped.rollup(
        [ks.col("grp1"), ks.col("grp2")],
        [F.sum("val").alias("total"), F.count("val").alias("cnt")],
    )
    text = _pretty(result)
    assert "total" in text
    assert "cnt" in text


def test_agg_columns_with_f_functions():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg_columns([F.sum("val").alias("total")])
    text = _pretty(result)
    assert "total" in text
    assert "30" in text


def test_multiple_group_keys_agg_columns():
    df = SESSION.sql(TWO_GROUP_SQL)
    grouped = df.group_by_columns([ks.col("grp1"), ks.col("grp2")])
    result = grouped.agg_columns([
        ks.sum(ks.col("val")).alias("total"),
        ks.count(ks.col("val")).alias("cnt"),
    ])
    text = _pretty(result)
    assert "total" in text
    assert "cnt" in text
    assert "grp1" in text
    assert "grp2" in text


def test_multiple_group_keys_count():
    df = SESSION.sql(TWO_GROUP_SQL)
    grouped = df.group_by_columns([ks.col("grp1"), ks.col("grp2")])
    result = grouped.count()
    text = _pretty(result)
    assert "count" in text
    assert "grp1" in text
    assert "grp2" in text


def test_cube_null_combinations():
    df = SESSION.sql(TWO_GROUP_SQL)
    grouped = df.group_by_columns([ks.col("grp1"), ks.col("grp2")])
    result = grouped.cube(
        [ks.col("grp1"), ks.col("grp2")],
        [ks.sum(ks.col("val")).alias("total")],
    )
    text = _pretty(result)
    assert "|" in text


def test_rollup_null_combinations():
    df = SESSION.sql(TWO_GROUP_SQL)
    grouped = df.group_by_columns([ks.col("grp1"), ks.col("grp2")])
    result = grouped.rollup(
        [ks.col("grp1"), ks.col("grp2")],
        [ks.sum(ks.col("val")).alias("total")],
    )
    text = _pretty(result)
    assert "|" in text


def test_collected_row_count():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg_columns([ks.sum(ks.col("val")).alias("total")])
    collected = result.collect()
    assert collected.row_count == 1


def test_collected_to_arrow():
    df = SESSION.sql(
        "SELECT 1 AS grp, 10 AS val UNION ALL SELECT 1 AS grp, 20 AS val"
    )
    grouped = df.group_by_columns([ks.col("grp")])
    result = grouped.agg_columns([ks.sum(ks.col("val")).alias("total")])
    collected = result.collect()
    arrow = collected.to_arrow()
    assert arrow.num_rows == 1
