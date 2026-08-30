from __future__ import annotations

import unittest
from pathlib import Path

from src.governance.cli import collect


ROOT = Path(__file__).resolve().parents[1]


class PhaseF5DependencyRegistryTests(unittest.TestCase):
    def test_direct_dependency_coverage_is_complete(self) -> None:
        self.assertEqual(collect(ROOT)["supply_chain"]["summary"]["direct_adoption_coverage_percent"], 100)

    def test_static_edges_are_declared_and_resolved(self) -> None:
        graph = collect(ROOT)["dependency_graph"]
        self.assertTrue(graph["internal_import_edges"])
        self.assertFalse(graph["dynamic_loading"])
