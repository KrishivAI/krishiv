#!/usr/bin/env python3
"""Write a reproducibility manifest for a benchmark run."""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def command_output(*command: str, fallback: str = "unknown") -> str:
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.DEVNULL).strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return fallback


def git_dirty() -> bool | None:
    output = command_output("git", "status", "--porcelain", fallback="")
    if output == "" and command_output("git", "rev-parse", "--is-inside-work-tree", fallback="false") != "true":
        return None
    return bool(output)


def build_manifest(suite: str, command: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "suite": suite,
        "command": command,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": command_output("git", "rev-parse", "HEAD"),
        "git_dirty": git_dirty(),
        "rustc": command_output("rustc", "--version"),
        "os": platform.platform(),
        "architecture": platform.machine(),
        "cpu": platform.processor() or command_output(
            "sh", "-c", "sed -n 's/^model name[[:space:]]*: //p' /proc/cpuinfo | head -1"
        ),
        "python": platform.python_version(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite", required=True)
    parser.add_argument("--command", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    manifest = build_manifest(args.suite, args.command)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote benchmark manifest to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
