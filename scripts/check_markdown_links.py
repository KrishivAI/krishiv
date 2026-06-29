#!/usr/bin/env python3
"""Validate repository-local links in Markdown files without network access."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from urllib.parse import unquote

LINK_RE = re.compile(r"(?<!!)\[[^]]*\]\(([^)]+)\)")
SKIP_PREFIXES = ("http://", "https://", "mailto:")
SKIP_DIRS = {".git", "target", ".venv", "node_modules"}


def markdown_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*.md")
        if not any(part in SKIP_DIRS for part in path.relative_to(root).parts)
    )


def broken_links(root: Path) -> list[str]:
    failures: list[str] = []
    for document in markdown_files(root):
        text = document.read_text(encoding="utf-8")
        for line_number, line in enumerate(text.splitlines(), start=1):
            for match in LINK_RE.finditer(line):
                raw_target = match.group(1).strip().split(maxsplit=1)[0].strip("<>")
                if not raw_target or raw_target.startswith(("#", *SKIP_PREFIXES)):
                    continue
                target_text = unquote(raw_target.split("#", 1)[0])
                if not target_text:
                    continue
                target = (document.parent / target_text).resolve()
                try:
                    target.relative_to(root.resolve())
                except ValueError:
                    failures.append(
                        f"{document.relative_to(root)}:{line_number}: link escapes repository: {raw_target}"
                    )
                    continue
                if not target.exists():
                    failures.append(
                        f"{document.relative_to(root)}:{line_number}: missing target: {raw_target}"
                    )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", default=".", type=Path)
    args = parser.parse_args()
    failures = broken_links(args.root.resolve())
    if failures:
        print("Broken Markdown links:")
        print("\n".join(f"- {failure}" for failure in failures))
        return 1
    print("All repository-local Markdown links resolve.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
