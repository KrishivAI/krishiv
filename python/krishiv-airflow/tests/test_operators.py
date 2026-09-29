import io
import json
import urllib.error

import pytest

from krishiv_airflow.operators import KrishivJobSensor, KrishivSubmitJobOperator


def test_submit_operator_builds_job_id():
    op = KrishivSubmitJobOperator(job_id="job-1", job_name="demo", tasks=2)
    assert op.job_id == "job-1"


def test_submit_operator_refuses_a_coordinator_url():
    # `krishiv submit` has no remote path: with a coordinator URL the job
    # would silently run inside the CLI process instead.
    op = KrishivSubmitJobOperator(
        job_id="job-1", job_name="demo", coordinator_url="http://coord:7070"
    )
    with pytest.raises(ValueError, match="coordinator"):
        op.execute({})


def _respond(monkeypatch, *, status=200, body=None, seen=None):
    def fake_urlopen(request, timeout=None):
        if seen is not None:
            seen.append(request)
        if status != 200:
            raise urllib.error.HTTPError(request.full_url, status, "err", {}, io.BytesIO(b""))
        return io.BytesIO(json.dumps(body).encode())

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)


def test_sensor_reads_the_jobs_own_state(monkeypatch):
    seen = []
    _respond(monkeypatch, body={"job_id": "job-1", "state": "Succeeded"}, seen=seen)
    op = KrishivJobSensor(job_id="job-1", coordinator_url="http://coord:8080")
    assert op.poke({}) is True
    assert seen[0].full_url == "http://coord:8080/api/v1/jobs/job-1"


def test_sensor_waits_while_running_or_not_yet_visible(monkeypatch):
    op = KrishivJobSensor(job_id="job-1", coordinator_url="http://coord:8080")
    _respond(monkeypatch, body={"job_id": "job-1", "state": "Running"})
    assert op.poke({}) is False
    _respond(monkeypatch, status=404)
    assert op.poke({}) is False


def test_sensor_raises_on_failure(monkeypatch):
    _respond(monkeypatch, body={"job_id": "job-1", "state": "Failed"})
    op = KrishivJobSensor(job_id="job-1", coordinator_url="http://coord:8080")
    with pytest.raises(RuntimeError, match="Failed"):
        op.poke({})


def test_sensor_ignores_other_jobs_output(monkeypatch):
    # The old sensor matched state names anywhere in `krishiv jobs` output,
    # so another job's "Completed" (or a `job-10` substring) counted.
    _respond(monkeypatch, body={"job_id": "job-1", "state": "Running", "note": "job-10 Succeeded"})
    op = KrishivJobSensor(job_id="job-1", coordinator_url="http://coord:8080")
    assert op.poke({}) is False


def test_sensor_sends_the_bearer_token(monkeypatch):
    seen = []
    _respond(monkeypatch, body={"job_id": "job-1", "state": "Running"}, seen=seen)
    op = KrishivJobSensor(job_id="job-1", coordinator_url="http://coord:8080", token="t0k")
    op.poke({})
    assert seen[0].get_header("Authorization") == "Bearer t0k"
