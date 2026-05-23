"""Airflow operators for Krishiv coordinator jobs (R15 S4.2)."""

from __future__ import annotations

import json
import subprocess
from typing import Any, Optional, Sequence


class KrishivSubmitJobOperator:
    """Submit a Krishiv job via the CLI (coordinator integration)."""

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
        if self.coordinator_url:
            cmd = ["krishiv", "-c", self.coordinator_url, *cmd[1:]]
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        self.xcom_job_id = self.job_id
        return self.job_id


class KrishivJobSensor:
    """Poll job status via `krishiv jobs` until terminal state."""

    def __init__(
        self,
        *,
        job_id: str,
        poke_interval: int = 30,
        success_states: Optional[set[str]] = None,
        failure_states: Optional[set[str]] = None,
        **kwargs: Any,
    ) -> None:
        self.job_id = job_id
        self.poke_interval = poke_interval
        self.success_states = success_states or {"Completed", "Succeeded"}
        self.failure_states = failure_states or {"Failed", "Cancelled"}
        self.kwargs = kwargs

    def poke(self, context: Any) -> bool:
        result = subprocess.run(
            ["krishiv", "jobs"],
            check=True,
            capture_output=True,
            text=True,
        )
        output = result.stdout
        if self.job_id not in output:
            return False
        for state in self.success_states:
            if state in output and self.job_id in output:
                return True
        for state in self.failure_states:
            if state in output and self.job_id in output:
                raise RuntimeError(f"job {self.job_id} failed with state {state}")
        return False
