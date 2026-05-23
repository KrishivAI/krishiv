"""Native asyncio iteration over windowed streams."""

import pytest

import krishiv as ks


@pytest.mark.asyncio
async def test_native_async_iteration_local_sql():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n", "ts", 0)
    windowed = stream.tumbling_window(1)
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
