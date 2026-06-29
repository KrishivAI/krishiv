#!/usr/bin/env python3
"""Validate release metadata and optional tag/version agreement."""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path

SEMVER_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?$")
WORKSPACE_VERSION_RE = re.compile(r'^version\s*=\s*"([^"]+)"', re.MULTILINE)


def workspace_version(cargo_toml: Path) -> str:
    text = cargo_toml.read_text(encoding="utf-8")
    marker = text.find("[workspace.package]")
    if marker < 0:
        raise ValueError("Cargo.toml has no [workspace.package] section")
    match = WORKSPACE_VERSION_RE.search(text, marker)
    if not match or not SEMVER_RE.fullmatch(match.group(1)):
        raise ValueError("workspace package version is missing or is not semantic versioning")
    return match.group(1)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--tag", help="Expected release tag, for example v0.2.0")
    args = parser.parse_args()
    root = args.root.resolve()
    version = workspace_version(root / "Cargo.toml")
    changelog = (root / "CHANGELOG.md").read_text(encoding="utf-8")
    if "## [Unreleased]" not in changelog:
        raise SystemExit("CHANGELOG.md must contain an [Unreleased] section")
    if args.tag and args.tag != f"v{version}":
        raise SystemExit(f"tag {args.tag!r} does not match workspace version v{version}")
    metadata = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=root,
        check=False,
        stdout=subprocess.DEVNULL,
    )
    if metadata.returncode:
        raise SystemExit("cargo metadata validation failed")
    print(f"Release metadata is valid for v{version}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
