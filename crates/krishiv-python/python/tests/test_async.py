"""Native asyncio iteration over windowed streams."""

import pytest

import krishiv as ks


@pytest.mark.asyncio
async def test_native_async_iteration_local_sql():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    windowed = stream.key_by("n").tumbling_window(1)
    seen = 0
    async for batch in windowed:
        assert batch.num_rows >= 1
        seen += 1
        if seen >= 1:
            break
    assert seen == 1


@pytest.mark.asyncio
async def test_connect_async():
    session = await ks.connect_async("http://localhost:50051")
    assert session.mode == "distributed"


@pytest.mark.asyncio
async def test_session_sql_async_returns_lazy_dataframe():
    session = ks.Session.local()
    df = await session.sql_async("SELECT 7 AS n")
    assert isinstance(df, ks.DataFrame)

    result = await df.collect_async()
    assert result.row_count == 1
    assert "7" in result.pretty()
