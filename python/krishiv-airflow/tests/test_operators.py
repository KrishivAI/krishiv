from krishiv_airflow.operators import KrishivJobSensor, KrishivSubmitJobOperator


def test_submit_operator_builds_job_id():
    op = KrishivSubmitJobOperator(job_id="job-1", job_name="demo", tasks=2)
    assert op.job_id == "job-1"


def test_sensor_poke_detects_completed(monkeypatch):
    op = KrishivJobSensor(job_id="job-1")

    class R:
        stdout = "job-1 Completed"

    monkeypatch.setattr("subprocess.run", lambda *a, **k: R)
    assert op.poke({}) is True
