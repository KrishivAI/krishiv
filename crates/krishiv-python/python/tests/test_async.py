"""Async-friendly streaming helpers."""

import asyncio

import pytest

import krishiv as ks


@pytest.mark.asyncio
async def test_async_collect_local_sql():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n", "ts", 0)
    windowed = stream.tumbling_window(1)
    batches = await asyncio.to_thread(windowed.collect)
    assert isinstance(batches, list)


@pytest.mark.asyncio
async def test_connect_async():
    session = await ks.connect_async("http://localhost:50051")
    assert session.mode == "distributed"
