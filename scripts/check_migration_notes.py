#!/usr/bin/env python3
"""Verify that every preview API removal listed in approved-breaking.toml has a
migration note in CHANGELOG.md.

A migration note is considered present when the CHANGELOG.md [Unreleased]
section (or any version section) contains the `replacement` value from the
approved-breaking entry.  This is a lightweight heuristic; human review of
release notes is still required before tagging a release.

Exit 0 when all removals have notes; exit 1 otherwise.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    try:
        import tomli as tomllib  # type: ignore[no-reattr]
    except ModuleNotFoundError:
        sys.exit("Requires Python 3.11+ (tomllib) or `pip install tomli`")


def load_breaking_entries(path: Path) -> list[dict]:
    if not path.exists():
        return []
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    return data.get("breaking", [])


def changelog_text(root: Path) -> str:
    changelog = root / "CHANGELOG.md"
    if not changelog.exists():
        return ""
    return changelog.read_text(encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    args = parser.parse_args()
    root = args.root.resolve()

    entries = load_breaking_entries(root / "api" / "approved-breaking.toml")
    if not entries:
        print("No approved-breaking entries found — nothing to check.")
        return 0

    changelog = changelog_text(root)
    missing: list[str] = []

    for entry in entries:
        entry_id = entry.get("id", "<unknown>")
        replacement = entry.get("replacement", "")
        if replacement and replacement not in changelog:
            missing.append(
                f"  {entry_id}: replacement {replacement!r} not found in CHANGELOG.md"
            )

    if missing:
        print("Migration notes missing from CHANGELOG.md:", file=sys.stderr)
        for m in missing:
            print(m, file=sys.stderr)
        print(
            "\nAdd a migration note mentioning the replacement API to the [Unreleased] "
            "section of CHANGELOG.md.",
            file=sys.stderr,
        )
        return 1

    print(
        f"Migration notes OK — {len(entries)} approved breaking change(s) all have "
        "replacement mentions in CHANGELOG.md."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
