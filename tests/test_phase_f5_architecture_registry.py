from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.governance.canonical import canonical_hash, safe_portable_path
from src.governance.cli import collect
from src.governance.errors import GovernanceError
from src.governance.frozen import load_baseline, verify_baseline
from src.governance.yaml_loader import load_yaml


ROOT = Path(__file__).resolve().parents[1]


class PhaseF5ArchitectureRegistryTests(unittest.TestCase):
    def test_current_registry_validates(self) -> None:
        data = collect(ROOT)
        self.assertEqual(data["ownership"]["summary"]["coverage_percent"], 100)
        self.assertFalse(data["component_graph"]["unreviewed_cross_component_cycles"])

    def test_canonical_hash_is_key_order_independent(self) -> None:
        self.assertEqual(canonical_hash({"b": [2], "a": 1}), canonical_hash({"a": 1, "b": [2]}))

    def test_duplicate_yaml_keys_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "bad.yaml"
            path.write_text("a: 1\na: 2\n", encoding="utf-8")
            with self.assertRaisesRegex(GovernanceError, "DUPLICATE_YAML_KEY"):
                load_yaml(path)

    def test_absolute_or_traversal_paths_fail_closed(self) -> None:
        for value in ("C:/secret", "/secret", "../secret"):
            with self.assertRaises(GovernanceError):
                safe_portable_path(value)

    def test_f4_baseline_is_immutable(self) -> None:
        verify_baseline(ROOT, load_baseline(ROOT))
