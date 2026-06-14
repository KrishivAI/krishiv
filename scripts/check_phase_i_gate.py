#!/usr/bin/env python3
"""Master Phase I release gate script.

Runs every Phase I gate check in order and reports a pass/fail summary.
All checks must pass for a 1.0 release to proceed.

Gate items (from docs/implementation/stable-api-todo.md):
  1. Stable API baseline — no unreviewed breaking changes (compare_api_surface.py)
  2. Parity manifest — no unexplained stable gaps (check_parity_manifest.py)
  3. Migration notes — all preview removals documented (check_migration_notes.py)
  4. Release metadata — workspace version and CHANGELOG valid (check_release.py)
  5. Cargo workspace — compiles clean (cargo check)

Type/null/time/decimal/ordering/overflow conformance, mode conformance, delivery
certification, and TPC-H/Nexmark baseline regression are run via:
  cargo test -p krishiv-api --lib conformance
  cargo test -p krishiv-api --lib mode_conformance
  cargo test -p krishiv-api --lib delivery_cert
  cargo test -p krishiv-bench --lib phase_i

Usage:
  python3 scripts/check_phase_i_gate.py [--root PATH] [--skip-cargo]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str], cwd: Path, label: str) -> bool:
    print(f"\n{'─'*60}")
    print(f"Gate: {label}")
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd)
    ok = result.returncode == 0
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}]")
    return ok


def python_gate(script: str, root: Path, label: str, extra_args: list[str] | None = None) -> bool:
    args = [sys.executable, f"scripts/{script}", "--root", str(root)]
    if extra_args:
        args.extend(extra_args)
    return run(args, root, label)


def cargo_gate(args: list[str], root: Path, label: str) -> bool:
    return run(["cargo"] + args, root, label)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument(
        "--skip-cargo",
        action="store_true",
        help="Skip cargo-based gates (useful in environments without Rust toolchain)",
    )
    parser.add_argument(
        "--against-ref",
        default="origin/main",
        help="Git ref to compare API surface against (default: origin/main)",
    )
    parsed = parser.parse_args()
    root = parsed.root.resolve()

    results: list[tuple[str, bool]] = []

    def gate(label: str, ok: bool) -> None:
        results.append((label, ok))

    # ── 1. API baseline ────────────────────────────────────────────────────
    gate(
        "API baseline (no unreviewed breaking changes)",
        python_gate(
            "compare_api_surface.py",
            root,
            "API baseline (no unreviewed breaking changes)",
            ["--against-ref", parsed.against_ref],
        ),
    )

    # ── 2. Parity manifest ─────────────────────────────────────────────────
    gate(
        "Parity manifest (no unexplained stable gaps)",
        python_gate(
            "check_parity_manifest.py",
            root,
            "Parity manifest (no unexplained stable gaps)",
            ["--warn-phases"],
        ),
    )

    # ── 3. Migration notes ─────────────────────────────────────────────────
    gate(
        "Migration notes (all preview removals documented)",
        python_gate(
            "check_migration_notes.py",
            root,
            "Migration notes (all preview removals documented)",
        ),
    )

    # ── 4. Release metadata ────────────────────────────────────────────────
    gate(
        "Release metadata (workspace version + CHANGELOG)",
        python_gate(
            "check_release.py",
            root,
            "Release metadata (workspace version + CHANGELOG)",
        ),
    )

    if not parsed.skip_cargo:
        # ── 5. Workspace compile ───────────────────────────────────────────
        gate(
            "Workspace compiles (cargo check --workspace)",
            cargo_gate(
                ["check", "--workspace"],
                root,
                "Workspace compiles (cargo check --workspace)",
            ),
        )

        # ── 6. Type/null/time conformance ─────────────────────────────────
        gate(
            "Type/null/time/decimal/ordering/overflow conformance",
            cargo_gate(
                ["test", "-p", "krishiv-api", "--lib", "conformance", "--", "--test-threads=1"],
                root,
                "Type/null/time/decimal/ordering/overflow conformance",
            ),
        )

        # ── 7. Mode conformance ───────────────────────────────────────────
        gate(
            "Embedded/single-node mode conformance",
            cargo_gate(
                ["test", "-p", "krishiv-api", "--lib", "mode_conformance", "--", "--test-threads=1"],
                root,
                "Embedded/single-node mode conformance",
            ),
        )

        # ── 8. Streaming delivery certification ───────────────────────────
        gate(
            "Streaming delivery certification",
            cargo_gate(
                ["test", "-p", "krishiv-api", "--lib", "delivery_cert", "--", "--test-threads=1"],
                root,
                "Streaming delivery certification",
            ),
        )

        # ── 9. TPC-H/Nexmark baseline ─────────────────────────────────────
        gate(
            "TPC-H/Nexmark baseline (phase_i gate)",
            cargo_gate(
                ["test", "-p", "krishiv-bench", "--lib", "phase_i", "--", "--test-threads=1"],
                root,
                "TPC-H/Nexmark baseline (phase_i gate)",
            ),
        )

    # ── Summary ────────────────────────────────────────────────────────────
    print(f"\n{'═'*60}")
    print("Phase I Gate Summary")
    print(f"{'═'*60}")
    all_pass = True
    for label, ok in results:
        status = "✓ PASS" if ok else "✗ FAIL"
        print(f"  {status}  {label}")
        if not ok:
            all_pass = False

    print(f"{'─'*60}")
    if all_pass:
        print("All Phase I gate checks passed. Ready for 1.0 release tag.")
        return 0
    else:
        failed = sum(1 for _, ok in results if not ok)
        print(f"{failed} gate check(s) failed. Fix before tagging 1.0.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
