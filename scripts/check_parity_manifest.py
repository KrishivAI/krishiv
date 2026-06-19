#!/usr/bin/env python3
"""Validate the stable-api.toml parity manifest for unexplained stable gaps.

A stable gap is a capability whose `stability = "stable"` but at least one
applicable language column (rust/python/sql) is not "implemented".  Preview and
experimental capabilities are allowed to have gaps.

Exit 0 when there are no unexplained gaps; exit 1 otherwise.
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


APPLICABLE_VALUES = {"implemented", "partial", "not_applicable", "blocked"}
GAP_VALUES = {"partial", "blocked", "todo"}
STABLE_STABILITIES = {"stable"}
PREVIEW_STABILITIES = {"preview", "experimental", "internal"}


def check_manifest(path: Path) -> list[str]:
    """Return a list of gap descriptions; empty list means no gaps."""
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    gaps: list[str] = []

    for cap in data.get("capabilities", []):
        cap_id = cap.get("id", "<unknown>")
        stability = cap.get("stability", "")

        if stability not in STABLE_STABILITIES:
            # Only enforce for stable capabilities
            continue

        for lang in ("rust", "python", "sql"):
            val = cap.get(lang, "")
            if val in GAP_VALUES:
                gaps.append(
                    f"  {cap_id}: {lang} = {val!r} (stable capability has unexplained gap)"
                )

    return gaps


def check_phases(path: Path) -> list[str]:
    """Warn about any phase still in 'todo' status."""
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    warnings: list[str] = []
    for phase in data.get("phases", []):
        if phase.get("status") == "todo":
            warnings.append(
                f"  Phase {phase['id']} ({phase.get('name', '')}) is still 'todo'"
            )
    return warnings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument(
        "--warn-phases",
        action="store_true",
        help="Also warn about phases still in 'todo' status",
    )
    args = parser.parse_args()

    manifest = args.root.resolve() / "api" / "stable-api.toml"
    if not manifest.exists():
        print(f"ERROR: {manifest} not found", file=sys.stderr)
        return 1

    gaps = check_manifest(manifest)
    if gaps:
        print("Unexplained stable API parity gaps:", file=sys.stderr)
        for g in gaps:
            print(g, file=sys.stderr)
        return 1

    if args.warn_phases:
        warnings = check_phases(manifest)
        if warnings:
            print("Phases not yet implemented (informational):")
            for w in warnings:
                print(w)

    print("Parity manifest OK — no unexplained stable gaps.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
