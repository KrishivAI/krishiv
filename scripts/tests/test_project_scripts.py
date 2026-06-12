from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


links = load_script("check_markdown_links")
release = load_script("check_release")


class MarkdownLinkTests(unittest.TestCase):
    def test_reports_missing_local_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "README.md").write_text("[missing](docs/missing.md)\n", encoding="utf-8")
            self.assertEqual(len(links.broken_links(root)), 1)

    def test_accepts_existing_and_remote_targets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "target.md").write_text("ok\n", encoding="utf-8")
            (root / "README.md").write_text(
                "[local](target.md) [remote](https://example.com)\n", encoding="utf-8"
            )
            self.assertEqual(links.broken_links(root), [])


class ReleaseTests(unittest.TestCase):
    def test_reads_workspace_version(self):
        with tempfile.TemporaryDirectory() as directory:
            cargo = Path(directory) / "Cargo.toml"
            cargo.write_text('[workspace]\n[workspace.package]\nversion = "0.3.1"\n', encoding="utf-8")
            self.assertEqual(release.workspace_version(cargo), "0.3.1")

    def test_rejects_invalid_version(self):
        with tempfile.TemporaryDirectory() as directory:
            cargo = Path(directory) / "Cargo.toml"
            cargo.write_text('[workspace.package]\nversion = "next"\n', encoding="utf-8")
            with self.assertRaises(ValueError):
                release.workspace_version(cargo)


if __name__ == "__main__":
    unittest.main()
