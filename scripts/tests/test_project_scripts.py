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
api_surface = load_script("check_api_surface")
api_compare = load_script("compare_api_surface")


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


class ApiSurfaceTests(unittest.TestCase):
    def test_detects_duplicate_python_class_names(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "crates/krishiv-python/src"
            source.mkdir(parents=True)
            (source / "one.rs").write_text(
                '#[pyclass(name = "DataFrame")]\npub struct One;\n', encoding="utf-8"
            )
            (source / "two.rs").write_text(
                '#[pyclass(name = "DataFrame")]\npub struct Two;\n', encoding="utf-8"
            )
            inventory = api_surface.python_inventory(root)
            self.assertEqual(len(inventory["duplicates"]), 1)

    def test_repository_python_class_names_are_unique(self):
        inventory = api_surface.python_inventory(ROOT)
        self.assertEqual(inventory["duplicates"], [])

    def test_manifest_covers_all_stable_api_phases(self):
        self.assertEqual(api_surface.validate_manifest(ROOT), [])

    def test_inventory_items_have_stability_docs_and_deprecation_metadata(self):
        inventories, _ = api_surface.inventories(ROOT)
        for filename, inventory in inventories.items():
            if filename == "python-public.json":
                items = inventory["functions"] + inventory["classes"]
                items += [method for cls in inventory["classes"] for method in cls["methods"]]
            else:
                items = inventory["items"]
            self.assertTrue(items, filename)
            for item in items:
                self.assertIn(item["stability"], api_surface.VALID_STABILITY)
                self.assertTrue(item["documentation"])
                self.assertIn("deprecated", item)
                self.assertIn("replacement", item)

    def test_generated_stub_covers_canonical_dataframe(self):
        inventory = api_surface.python_inventory(ROOT)
        stub = api_surface.render_python_stub(inventory)
        self.assertIn("class DataFrame:", stub)
        self.assertIn("def collect(", stub)
        self.assertIn("async def collect_async(self) -> QueryResult: ...", stub)
        self.assertIn("async def sql_async(self, query: str) -> DataFrame: ...", stub)
        self.assertNotIn("from typing import Any", stub)
        self.assertNotIn("-> Any", stub)
        self.assertIn("class Relation:", stub)

    def test_compare_classifies_additive_breaking_and_semantic(self):
        baseline = {"items": [{"id": "one", "signature": "old"}, {"id": "gone"}]}
        current = {"items": [{"id": "one", "signature": "new"}, {"id": "added"}]}
        changes = api_compare.compare(baseline, current, "rust")
        self.assertTrue(any("added" in item for item in changes["additive"]))
        self.assertTrue(any("gone" in item for item in changes["breaking"]))
        self.assertTrue(any("one" in item for item in changes["semantic"]))


if __name__ == "__main__":
    unittest.main()
