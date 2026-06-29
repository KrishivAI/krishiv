#!/usr/bin/env python3
"""Generate a minimal SBOM (Software Bill of Materials) and release artifact checksums.

Produces:
  - `release/sbom.json`   — CycloneDX-compatible SBOM derived from `cargo metadata`.
  - `release/checksums.sha256` — SHA-256 checksums for any binaries in `release/`.

Usage:
  python3 scripts/generate_sbom.py [--root PATH] [--output-dir PATH]

The output directory defaults to `release/` at the repository root.  If it does
not exist it is created.  This script does not build binaries; run the relevant
`just build-*` target first.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def cargo_metadata(root: Path) -> dict:
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    return json.loads(result.stdout)


def package_to_component(pkg: dict) -> dict:
    return {
        "type": "library",
        "name": pkg["name"],
        "version": pkg["version"],
        "purl": f"pkg:cargo/{pkg['name']}@{pkg['version']}",
        "licenses": [{"license": {"id": lic}} for lic in pkg.get("license", "").split(" OR ") if lic],
        "description": pkg.get("description", ""),
    }


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def generate_sbom(root: Path, output_dir: Path) -> None:
    meta = cargo_metadata(root)
    workspace_version = next(
        (p["version"] for p in meta.get("packages", []) if p["name"] == "krishiv"),
        "0.0.0",
    )

    components = [package_to_component(p) for p in meta.get("packages", [])]

    sbom = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.4",
        "version": 1,
        "serialNumber": f"urn:uuid:krishiv-{workspace_version}-sbom",
        "metadata": {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "component": {
                "type": "application",
                "name": "krishiv",
                "version": workspace_version,
            },
        },
        "components": components,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    sbom_path = output_dir / "sbom.json"
    sbom_path.write_text(json.dumps(sbom, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"SBOM written to {sbom_path} ({len(components)} components)")


def generate_checksums(output_dir: Path) -> None:
    binaries = [
        p for p in output_dir.iterdir()
        if p.is_file() and p.suffix not in {".json", ".sha256", ".txt"}
    ]
    if not binaries:
        print("No binaries found in output dir — skipping checksum generation.")
        return

    lines = []
    for path in sorted(binaries):
        digest = sha256_file(path)
        lines.append(f"{digest}  {path.name}")
        print(f"  {digest[:16]}…  {path.name}")

    checksum_path = output_dir / "checksums.sha256"
    checksum_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Checksums written to {checksum_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()

    root = args.root.resolve()
    output_dir = args.output_dir.resolve() if args.output_dir else root / "release"

    try:
        generate_sbom(root, output_dir)
        generate_checksums(output_dir)
    except subprocess.CalledProcessError as exc:
        print(f"cargo metadata failed: {exc.stderr.decode()}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
