#!/usr/bin/env python3
"""Generate and validate public API inventories, reports, and Python stubs."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path

VALID_PHASE_STATUS = {"todo", "in_progress", "implemented", "blocked"}
VALID_CAPABILITY_STATUS = {"todo", "partial", "implemented", "blocked", "not_applicable"}
VALID_STABILITY = {"stable", "preview", "experimental", "internal"}
PYCLASS_RE = re.compile(r'#\[pyclass(?:\(([^]]*)\))?\]\s*\npub struct\s+(\w+)', re.MULTILINE)
PYCLASS_NAME_RE = re.compile(r'name\s*=\s*"([^"]+)"')
PYMETHOD_RE = re.compile(r'^\s*(?:pub\s+)?fn\s+([A-Za-z_]\w*)\s*\(([^)]*)\)', re.MULTILINE)
PUBLIC_SIGNATURE_RE = re.compile(
    r'(?m)^(?P<attrs>(?:#\[[^\n]+\]\n)*)\s*pub\s+(?P<async>async\s+)?'
    r'(?P<kind>struct|enum|trait|type|fn)\s+(?P<name>\w+)(?P<tail>[^\n{;]*)'
)
PUBLIC_METHOD_RE = re.compile(
    r'(?m)^(?P<attrs>(?:\s*#\[[^\n]+\]\n)*)\s*pub\s+(?P<async>async\s+)?fn\s+'
    r'(?P<name>\w+)\s*\((?P<args>[^)]*)\)(?P<ret>\s*->\s*[^\n{]+)?'
)
PUBLIC_USE_RE = re.compile(r'^pub use\s+([^;]+);', re.MULTILINE)
PYFUNCTION_REG_RE = re.compile(r'wrap_pyfunction!\((?:\w+::)?(\w+),\s*m\)')
DEPRECATED_NOTE_RE = re.compile(r'deprecated\s*\(\s*note\s*=\s*"([^"]+)"')


def source_files(root: Path, relative: str) -> list[Path]:
    base = root / relative
    return sorted(base.rglob("*.rs")) if base.exists() else []


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def metadata(path: Path, root: Path, text: str, match: re.Match[str], name: str, kind: str) -> dict[str, object]:
    attrs = match.groupdict().get("attrs", "") or ""
    note = DEPRECATED_NOTE_RE.search(attrs)
    line = line_number(text, match.start())
    return {
        "id": f"{path.relative_to(root)}::{name}",
        "name": name,
        "kind": kind,
        "stability": "preview",
        "documentation": f"{path.relative_to(root)}#L{line}",
        "deprecated": note is not None,
        "replacement": note.group(1) if note else None,
    }


def matching_brace(text: str, opening: int) -> int:
    depth = 0
    for index in range(opening, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    return len(text)


def python_inventory(root: Path) -> dict[str, object]:
    classes: list[dict[str, object]] = []
    seen: dict[str, Path] = {}
    duplicates: list[str] = []
    rust_to_public: dict[str, str] = {}
    texts: dict[Path, str] = {}
    for path in source_files(root, "crates/krishiv-python/src"):
        text = path.read_text(encoding="utf-8")
        texts[path] = text
        for match in PYCLASS_RE.finditer(text):
            args, rust_name = match.groups()
            name_match = PYCLASS_NAME_RE.search(args or "")
            public_name = name_match.group(1) if name_match else rust_name
            rust_to_public[rust_name] = public_name
            if public_name in seen:
                duplicates.append(
                    f"Python class {public_name!r} is declared in both "
                    f"{seen[public_name].relative_to(root)} and {path.relative_to(root)}"
                )
            else:
                seen[public_name] = path
            item = metadata(path, root, text, match, public_name, "class")
            item["rust_type"] = rust_name
            item["methods"] = []
            classes.append(item)
    by_rust = {item["rust_type"]: item for item in classes}
    impl_re = re.compile(r'#\[pymethods\]\s*impl\s+(\w+)\s*\{', re.MULTILINE)
    for path, text in texts.items():
        for impl_match in impl_re.finditer(text):
            rust_name = impl_match.group(1)
            if rust_name not in by_rust:
                continue
            opening = text.find("{", impl_match.start())
            body_end = matching_brace(text, opening)
            body = text[opening + 1:body_end]
            base_offset = opening + 1
            methods = []
            for method_match in PYMETHOD_RE.finditer(body):
                name = method_match.group(1)
                absolute_start = base_offset + method_match.start()
                line = line_number(text, absolute_start)
                methods.append({
                    "id": f"{by_rust[rust_name]['name']}.{name}",
                    "name": name,
                    "kind": "method",
                    "stability": "preview",
                    "documentation": f"{path.relative_to(root)}#L{line}",
                    "deprecated": False,
                    "replacement": None,
                })
            by_rust[rust_name]["methods"] = sorted(methods, key=lambda item: item["name"])
    functions: list[dict[str, object]] = []
    lib = texts.get(root / "crates/krishiv-python/src/lib.rs", "")
    for name in sorted(set(PYFUNCTION_REG_RE.findall(lib))):
        functions.append({
            "id": name,
            "name": name,
            "kind": "function",
            "stability": "preview",
            "documentation": "crates/krishiv-python/src/lib.rs",
            "deprecated": False,
            "replacement": None,
        })
    return {"classes": sorted(classes, key=lambda item: item["name"]), "functions": functions, "duplicates": duplicates}


def unique_ids(items: list[dict[str, object]]) -> list[dict[str, object]]:
    counts: dict[str, int] = {}
    for item in items:
        base = str(item["id"])
        counts[base] = counts.get(base, 0) + 1
        if counts[base] > 1:
            item["id"] = f"{base}#{counts[base]}"
    return items

def rust_inventory(root: Path) -> dict[str, object]:
    files = source_files(root, "crates/krishiv-api/src") + source_files(root, "crates/krishiv/src")
    items: list[dict[str, object]] = []
    signatures: list[str] = []
    reexports: set[str] = set()
    for path in files:
        text = path.read_text(encoding="utf-8")
        for match in PUBLIC_SIGNATURE_RE.finditer(text):
            name, kind = match.group("name"), match.group("kind")
            item = metadata(path, root, text, match, name, kind)
            signature = " ".join(match.group(0).split())
            item["signature"] = signature
            item["id"] = f"{path.relative_to(root)}::{kind}::{name}::{signature}"
            items.append(item)
            signatures.append(f"{item['id']}")
        reexports.update(" ".join(value.split()) for value in PUBLIC_USE_RE.findall(text))
    items = unique_ids(items)
    return {
        "items": sorted(items, key=lambda item: item["id"]),
        "reexports": sorted(reexports),
        "report": sorted(str(item["id"]) for item in items),
    }


def sql_inventory(root: Path) -> dict[str, object]:
    items: list[dict[str, object]] = []
    modules: set[str] = set()
    for path in source_files(root, "crates/krishiv-sql/src"):
        text = path.read_text(encoding="utf-8")
        modules.add(str(path.relative_to(root / "crates/krishiv-sql/src")).removesuffix(".rs"))
        for match in PUBLIC_SIGNATURE_RE.finditer(text):
            name, kind = match.group("name"), match.group("kind")
            item = metadata(path, root, text, match, name, kind)
            signature = " ".join(match.group(0).split())
            item["signature"] = signature
            item["id"] = f"{path.relative_to(root)}::{kind}::{name}::{signature}"
            items.append(item)
    items = unique_ids(items)
    return {"items": sorted(items, key=lambda item: item["id"]), "modules": sorted(modules)}


def validate_manifest(root: Path) -> list[str]:
    data = tomllib.loads((root / "api/stable-api.toml").read_text(encoding="utf-8"))
    failures: list[str] = []
    phases = data.get("phases", [])
    phase_ids = [phase.get("id") for phase in phases]
    expected = list("ABCDEFGHI")
    if phase_ids != expected:
        failures.append(f"phases must be declared exactly as {expected}, got {phase_ids}")
    for phase in phases:
        if phase.get("status") not in VALID_PHASE_STATUS:
            failures.append(f"phase {phase.get('id')} has invalid status {phase.get('status')!r}")
        if not phase.get("exit_criteria"):
            failures.append(f"phase {phase.get('id')} has no exit criteria")
    capability_ids: set[str] = set()
    for capability in data.get("capabilities", []):
        identifier = capability.get("id")
        if identifier in capability_ids:
            failures.append(f"duplicate capability id {identifier!r}")
        capability_ids.add(identifier)
        if capability.get("phase") not in expected:
            failures.append(f"capability {identifier!r} has unknown phase")
        if capability.get("stability") not in VALID_STABILITY:
            failures.append(f"capability {identifier!r} has invalid stability")
        for language in ("rust", "python", "sql"):
            if capability.get(language) not in VALID_CAPABILITY_STATUS:
                failures.append(f"capability {identifier!r} has invalid {language} status {capability.get(language)!r}")
        if not capability.get("owner"):
            failures.append(f"capability {identifier!r} has no owner")
    return failures


def render_python_stub(inventory: dict[str, object]) -> str:
    lines = [
        '"""Generated preview type surface for the native ``krishiv`` module."""',
        "",
        "from typing import Any",
        "",
    ]
    for item in inventory["classes"]:
        lines.append(f"class {item['name']}:")
        methods = item.get("methods", [])
        if not methods:
            lines.append("    ...")
        for method in methods:
            name = method["name"]
            if name == "new":
                lines.append("    def __init__(self, *args: Any, **kwargs: Any) -> None: ...")
            elif name.startswith("__"):
                lines.append(f"    def {name}(self, *args: Any, **kwargs: Any) -> Any: ...")
            else:
                lines.append(f"    def {name}(self, *args: Any, **kwargs: Any) -> Any: ...")
        lines.append("")
    for function in inventory["functions"]:
        lines.append(f"def {function['name']}(*args: Any, **kwargs: Any) -> Any: ...")
    return "\n".join(lines).rstrip() + "\n"


def inventories(root: Path) -> tuple[dict[str, object], dict[str, str]]:
    rust = rust_inventory(root)
    python = python_inventory(root)
    sql = sql_inventory(root)
    json_outputs = {
        "rust-public.json": {key: value for key, value in rust.items() if key != "report"},
        "python-public.json": python,
        "sql-public.json": sql,
    }
    text_outputs = {
        "rust-public-api.txt": "\n".join(rust["report"]) + "\n",
        "../crates/krishiv-python/python/krishiv/krishiv.pyi": render_python_stub(python),
    }
    return json_outputs, text_outputs


def encoded(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    failures = validate_manifest(root)
    generated_json, generated_text = inventories(root)
    failures.extend(generated_json["python-public.json"]["duplicates"])
    for filename, value in generated_json.items():
        destination = root / "api" / filename
        content = encoded(value)
        if args.write:
            destination.write_text(content, encoding="utf-8")
        elif not destination.exists() or destination.read_text(encoding="utf-8") != content:
            failures.append(f"stale or missing API inventory {destination.relative_to(root)}; run with --write")
    for filename, content in generated_text.items():
        destination = (root / "api" / filename).resolve()
        if args.write:
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(content, encoding="utf-8")
        elif not destination.exists() or destination.read_text(encoding="utf-8") != content:
            failures.append(f"stale or missing generated API artifact {destination.relative_to(root)}; run with --write")
    if failures:
        print("API surface validation failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print(f"API manifest, inventories, reports, and stubs {'updated' if args.write else 'validated'} successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
