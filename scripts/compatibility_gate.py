#!/usr/bin/env python3
"""Phase 62 compatibility gate: every versioned promise in docs/COMPATIBILITY.md
must match the code and be enforced by a real test.

`docs/COMPATIBILITY.md` states version contracts in prose ("writers emit v3;
readers accept v1-v3"). Prose drifts: before this gate existed the document
claimed checkpoint metadata was v2 while `CheckpointMetadata::VERSION` had been
3 for some time, so the published promise was simply wrong. This script closes
that by checking two things per versioned surface:

1. **Doc matches code** - the version numbers named in the document equal the
   constants the engine actually compiles.
2. **Promise is enforced** - the named test function exists in the tree, so a
   promise cannot be documented without a test that would fail if a reader
   started silently accepting an unsupported version.

Deliberately parses source text rather than linking the crates: the constants
live in four crates behind different feature sets, and a compiled gate would
only cover whichever features that build happened to enable (the
`krishiv-sql` lint blindspot, applied to compatibility). Any constant it cannot
find is a hard failure, never a skip - a silent pass is the failure mode this
whole gate exists to prevent.

Run: python3 scripts/compatibility_gate.py   (exit 0 = clean)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DOC = REPO / "docs" / "COMPATIBILITY.md"


class GateError(Exception):
    """A promise that is wrong, unenforced, or unverifiable."""


def read(rel: str) -> str:
    path = REPO / rel
    if not path.is_file():
        raise GateError(f"expected source file is missing: {rel}")
    return path.read_text(encoding="utf-8")


def const_u32(rel: str, name: str) -> int:
    """Read `pub const <name>: u32 = <n>;` out of a source file."""
    text = read(rel)
    match = re.search(rf"const\s+{re.escape(name)}\s*:\s*u32\s*=\s*(\d+)\s*;", text)
    if not match:
        raise GateError(f"constant {name} not found in {rel} (renamed or removed?)")
    return int(match.group(1))


def transport_version(rel: str) -> str:
    """Read the `CURRENT` transport contract version as `major.minor`."""
    text = read(rel)
    current = re.search(r"const\s+CURRENT\s*:\s*Self\s*=\s*Self::(\w+)\s*;", text)
    if not current:
        raise GateError(f"TransportVersion::CURRENT not found in {rel}")
    alias = current.group(1)
    parts = re.search(
        rf"const\s+{re.escape(alias)}\s*:\s*Self\s*=\s*Self\s*{{\s*major:\s*(\d+)\s*,\s*minor:\s*(\d+)\s*}}",
        text,
    )
    if not parts:
        raise GateError(f"transport version alias {alias} not resolvable in {rel}")
    return f"{parts.group(1)}.{parts.group(2)}"


def test_exists(rel: str, fn: str) -> None:
    """A promise is only enforced if its named test function really exists."""
    text = read(rel)
    if not re.search(rf"\bfn\s+{re.escape(fn)}\s*\(", text):
        raise GateError(
            f"enforcing test `{fn}` not found in {rel} - the promise it backs is unenforced"
        )


def require_in_doc(doc: str, phrase: str, promise: str) -> None:
    if phrase not in doc:
        raise GateError(
            f"{promise}: docs/COMPATIBILITY.md does not state {phrase!r} - "
            "the document has drifted from the code"
        )


def main() -> int:
    failures: list[str] = []

    try:
        doc = DOC.read_text(encoding="utf-8")
    except OSError as error:
        print(f"FAIL: cannot read {DOC}: {error}")
        return 1

    checks = []

    # ── Checkpoint metadata ──────────────────────────────────────────────────
    def checkpoint() -> str:
        rel = "crates/krishiv-state/src/checkpoint/metadata.rs"
        current = const_u32(rel, "VERSION")
        minimum = const_u32(rel, "MIN_SUPPORTED_VERSION")
        require_in_doc(
            doc,
            f"Writers emit v{current}; readers accept supported v{minimum}-v{current} metadata.",
            "checkpoint metadata",
        )
        test_exists(
            "crates/krishiv-state/src/checkpoint/tests.rs",
            "write_epoch_metadata_rejects_incompatible_version",
        )
        return f"checkpoint metadata: writers v{current}, readers v{minimum}-v{current}"

    # ── Savepoint metadata ───────────────────────────────────────────────────
    def savepoint() -> str:
        rel = "crates/krishiv-state/src/savepoint.rs"
        version = const_u32(rel, "SAVEPOINT_FORMAT_VERSION")
        require_in_doc(
            doc,
            f"Import validates the declared format version (v{version}) before restore.",
            "savepoint metadata",
        )
        test_exists(rel, "import_rejects_unknown_format_version")
        return f"savepoint metadata: v{version}"

    # ── Task-fragment envelope ───────────────────────────────────────────────
    def fragment() -> str:
        rel = "crates/krishiv-plan/src/task_fragment.rs"
        version = const_u32(rel, "TASK_FRAGMENT_VERSION")
        require_in_doc(
            doc,
            f"Readers reject unsupported versions (current v{version}) instead of "
            "silently interpreting them.",
            "task-fragment envelope",
        )
        test_exists(rel, "rejects_unknown_fragment_version")
        return f"task-fragment envelope: v{version}"

    # ── Wire protocol (coordinator <-> executor transport) ───────────────────
    def transport() -> str:
        rel = "crates/krishiv-proto/src/ids.rs"
        version = transport_version(rel)
        require_in_doc(
            doc,
            f"Handshake carries the transport contract version (current R{version})",
            "wire protocol",
        )
        test_exists("crates/krishiv-proto/src/tests.rs", "transport_version_exposes_compatibility")
        # The doc's claim is only true if a server actually rejects on mismatch,
        # not merely decodes the number. Pin the reject sites so deleting one is
        # a gate failure rather than a silently weakened promise.
        for site in (
            "crates/krishiv-scheduler/src/grpc.rs",
            "crates/krishiv-executor/src/grpc.rs",
        ):
            if "is_compatible_with" not in read(site):
                raise GateError(
                    f"{site} no longer checks transport compatibility - the "
                    "documented reject-on-mismatch promise is unenforced there"
                )
        return f"wire protocol: R{version}"

    # ── IVM resident tick wire (coordinator <-> executor tick result) ────────
    def ivm_tick_wire() -> str:
        rel = "crates/krishiv-ivm/src/flow.rs"
        version = const_u32(rel, "IVM_TICK_WIRE_VERSION")
        require_in_doc(
            doc,
            f"Readers reject unsupported tick-result versions (current v{version}) "
            "instead of silently interpreting them.",
            "IVM resident tick wire",
        )
        test_exists(rel, "decode_tick_result_rejects_unknown_magic")
        return f"IVM resident tick wire: v{version}"

    checks = [
        ("checkpoint metadata", checkpoint),
        ("savepoint metadata", savepoint),
        ("task-fragment envelope", fragment),
        ("wire protocol", transport),
        ("IVM resident tick wire", ivm_tick_wire),
    ]

    for name, check in checks:
        try:
            print(f"  ok   {check()}")
        except GateError as error:
            failures.append(f"{name}: {error}")

    if failures:
        print("\nFAIL: compatibility promises are wrong or unenforced:")
        for failure in failures:
            print(f"  - {failure}")
        print(
            "\nFix the document to match the code (or the code to match the "
            "promise). Do not delete the check."
        )
        return 1

    print("\n✓ every versioned compatibility promise matches the code and has an enforcing test")
    return 0


if __name__ == "__main__":
    sys.exit(main())
