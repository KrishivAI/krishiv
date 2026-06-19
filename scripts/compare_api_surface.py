#!/usr/bin/env python3
"""Classify public API snapshot changes against a baseline directory or Git ref."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tomllib
from pathlib import Path

SURFACES = ("rust-public.json", "python-public.json", "sql-public.json")


def flatten(value: object, prefix: str = "") -> dict[str, str]:
    result: dict[str, str] = {}
    if isinstance(value, dict):
        identity = value.get("id") or value.get("name")
        current = f"{prefix}/{identity}" if identity else prefix
        scalar = {
            k: v for k, v in value.items()
            if not isinstance(v, (dict, list)) and k != "documentation"
        }
        if scalar and current:
            result[current] = json.dumps(scalar, sort_keys=True)
        for key, child in value.items():
            if isinstance(child, (dict, list)):
                result.update(flatten(child, f"{current}/{key}".strip("/")))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            child_prefix = prefix if isinstance(child, dict) else f"{prefix}/{index}".strip("/")
            result.update(flatten(child, child_prefix))
    return result


def load_from_ref(root: Path, ref: str, filename: str) -> object | None:
    completed = subprocess.run(
        ["git", "show", f"{ref}:api/{filename}"], cwd=root, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False,
    )
    return json.loads(completed.stdout) if completed.returncode == 0 else None


def approved_breaking(root: Path) -> set[str]:
    path = root / "api/approved-breaking.toml"
    if not path.exists():
        return set()
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    return {entry["id"] for entry in data.get("breaking", []) if entry.get("reason") and entry.get("replacement")}


def compare(baseline: object | None, current: object, surface: str) -> dict[str, list[str]]:
    before = flatten(baseline or {})
    after = flatten(current)
    before_keys, after_keys = set(before), set(after)
    return {
        "additive": sorted(f"{surface}:{key}" for key in after_keys - before_keys),
        "breaking": sorted(f"{surface}:{key}" for key in before_keys - after_keys),
        "semantic": sorted(
            f"{surface}:{key}" for key in before_keys & after_keys if before[key] != after[key]
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--against-ref", default="origin/main")
    parser.add_argument("--report", type=Path, default=Path("api/api-change-report.json"))
    args = parser.parse_args()
    root = args.root.resolve()
    report = {"additive": [], "breaking": [], "semantic": []}
    for filename in SURFACES:
        current = json.loads((root / "api" / filename).read_text(encoding="utf-8"))
        changes = compare(load_from_ref(root, args.against_ref, filename), current, filename)
        for kind in report:
            report[kind].extend(changes[kind])
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    unapproved = sorted(set(report["breaking"] + report["semantic"]) - approved_breaking(root))
    print(f"API changes: {len(report['additive'])} additive, {len(report['breaking'])} breaking, {len(report['semantic'])} semantic")
    if unapproved:
        print("Unapproved breaking/semantic API changes:", file=sys.stderr)
        for item in unapproved:
            print(f"- {item}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
