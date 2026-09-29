"""Airflow operators for Krishiv coordinator jobs (R15 S4.2)."""

from __future__ import annotations

import json
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Optional, Sequence


class KrishivSubmitJobOperator:
    """Submit a Krishiv job via the CLI, run in-process by the CLI.

    ``krishiv submit`` has no remote-submission path: it runs the job inside
    the CLI process. A ``coordinator_url`` is therefore refused rather than
    silently ignored — the job would run locally while a sensor watched the
    coordinator for a job that never arrives there.
    """

    template_fields: Sequence[str] = ("job_id", "job_name")

    def __init__(
        self,
        *,
        job_id: str,
        job_name: str,
        tasks: int = 1,
        coordinator_url: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self.job_id = job_id
        self.job_name = job_name
        self.tasks = tasks
        self.coordinator_url = coordinator_url
        self.kwargs = kwargs
        self.xcom_job_id: Optional[str] = None

    def execute(self, context: Any) -> str:
        if self.coordinator_url:
            raise ValueError(
                "KrishivSubmitJobOperator cannot submit to a remote coordinator: "
                "`krishiv submit` runs the job in the CLI process. Submit through "
                "the coordinator's API instead."
            )
        cmd = [
            "krishiv",
            "submit",
            "--job-id",
            self.job_id,
            "--name",
            self.job_name,
            "--tasks",
            str(self.tasks),
            "--launch",
        ]
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        self.xcom_job_id = self.job_id
        return self.job_id


class KrishivJobSensor:
    """Poll a job's state on the coordinator until it reaches a terminal state.

    Reads ``GET {coordinator_url}/api/v1/jobs/{job_id}`` and compares the job's
    own ``state`` field. (It used to scan ``krishiv jobs`` output, which lists
    only jobs in the CLI's own process, for state names anywhere in the text.)
    """

    def __init__(
        self,
        *,
        job_id: str,
        coordinator_url: str,
        token: Optional[str] = None,
        poke_interval: int = 30,
        timeout_s: float = 30.0,
        success_states: Optional[set[str]] = None,
        failure_states: Optional[set[str]] = None,
        **kwargs: Any,
    ) -> None:
        self.job_id = job_id
        self.coordinator_url = coordinator_url.rstrip("/")
        self.token = token
        self.poke_interval = poke_interval
        self.timeout_s = timeout_s
        self.success_states = success_states or {"Succeeded"}
        self.failure_states = failure_states or {"Failed", "Cancelled"}
        self.kwargs = kwargs

    def _job_state(self) -> Optional[str]:
        url = f"{self.coordinator_url}/api/v1/jobs/{urllib.parse.quote(self.job_id, safe='')}"
        request = urllib.request.Request(url, headers={"Accept": "application/json"})
        if self.token:
            request.add_header("Authorization", f"Bearer {self.token}")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as response:
                body = json.load(response)
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None  # not visible yet (or already garbage-collected)
            raise
        return body.get("state")

    def poke(self, context: Any) -> bool:
        state = self._job_state()
        if state is None:
            return False
        if state in self.failure_states:
            raise RuntimeError(f"job {self.job_id} finished in state {state}")
        return state in self.success_states
