"""Native asyncio iteration over windowed streams."""

import asyncio
import contextlib

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


async def _assert_yields_to_event_loop(awaitable_factory, label):
    """Assert that awaiting `awaitable_factory()` suspends at least once.

    A concurrently scheduled `ticker` task increments a counter every time it
    gets a turn on the event loop. Because asyncio is single-threaded and
    cooperative, a coroutine that never hits a genuine suspension point (e.g.
    one that runs its "async" work synchronously inline instead of returning
    a real awaitable/bridging through a thread) blocks every other scheduled
    task -- including `ticker` -- for its entire duration. If `ticker` made
    zero progress by the time the awaited call returns, the call never gave
    the event loop a chance to run anything else: it was not genuinely async.

    This is the exact bug pattern found and fixed in this session (Session.
    sql_async / DataFrame.collect_async silently re-blocking the event loop)
    and is deterministic -- no timing/sleep-duration assumptions.
    """
    progress = {"ticks": 0}

    async def ticker():
        while True:
            await asyncio.sleep(0)
            progress["ticks"] += 1

    ticker_task = asyncio.ensure_future(ticker())
    try:
        await awaitable_factory()
    finally:
        ticker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ticker_task

    assert progress["ticks"] > 0, (
        f"{label} completed without ever yielding to the event loop -- it "
        "likely blocks synchronously instead of returning a genuine "
        "awaitable (the historical ADR-0002 'fake async' regression)"
    )


@pytest.mark.asyncio
async def test_session_sql_async_yields_to_event_loop():
    session = ks.Session.local()
    await _assert_yields_to_event_loop(
        lambda: session.sql_async("SELECT 1 AS n"), "Session.sql_async"
    )


@pytest.mark.asyncio
async def test_dataframe_collect_async_yields_to_event_loop():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n")
    await _assert_yields_to_event_loop(df.collect_async, "DataFrame.collect_async")
