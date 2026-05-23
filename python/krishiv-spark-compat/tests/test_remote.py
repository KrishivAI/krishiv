"""Remote Spark Connect integration tests (R15 S2.2)."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
sys.path = [p for p in sys.path if "krishiv-python" not in p]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "krishiv/compat/spark/_proto"))

from krishiv.compat.spark import SparkSession  # noqa: E402


@pytest.fixture(scope="module")
def spark_connect_server():
    addr = "127.0.0.1:17070"
    env = {**os.environ, "KRISHIV_SPARK_CONNECT_ADDR": addr}
    proc = subprocess.Popen(
        ["cargo", "run", "-p", "krishiv-spark-connect", "--example", "spark_connect_smoke", "--quiet"],
        cwd=str(REPO),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    deadline = time.time() + 60
    while time.time() < deadline:
        if proc.poll() is not None:
            err = proc.stderr.read().decode() if proc.stderr else ""
            pytest.fail(f"spark connect smoke server exited: {err}")
        try:
            import socket

            with socket.create_connection((addr.split(":")[0], int(addr.split(":")[1])), timeout=0.5):
                break
        except OSError:
            time.sleep(0.2)
    else:
        proc.kill()
        pytest.fail("spark connect smoke server did not start")
    yield addr
    proc.terminate()
    proc.wait(timeout=10)


def test_remote_spark_session_execute_sql(spark_connect_server):
    host, port = spark_connect_server.split(":")
    spark = SparkSession.builder.remote(f"sc://{host}:{port}").getOrCreate()
    assert spark._client is not None
    version = spark._client.spark_version()
    assert version.startswith("3.5")
    batches = spark._client.execute_sql("SELECT 42 AS answer")
    assert batches
    spark._client.close()


def test_remote_session_builder_parses_sc_url():
    spark = SparkSession.builder.remote("sc://coordinator:7070").getOrCreate()
    assert spark._remote == "sc://coordinator:7070"
