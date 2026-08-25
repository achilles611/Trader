from __future__ import annotations

import asyncio
from dataclasses import replace
import inspect
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.copytrade.control_center import create_control_center_app
from src.copytrade.config import CopyTradeConfig


class PaperControlCenterTests(unittest.TestCase):
    def test_startup_rejects_active_ledger_below_cold_root_before_factory(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder)
            cold_root = root / "cold"
            paper_path = cold_root / "LaneIII" / "paper.sqlite3"
            defaults = CopyTradeConfig()
            config = replace(
                defaults,
                storage=replace(defaults.storage, cold_root=cold_root),
                artifacts=replace(defaults.artifacts, database_path=root / "hot" / "copytrade.sqlite3"),
            )
            called: list[Path] = []

            def forbidden_factory(path: Path):
                called.append(path)
                raise AssertionError("paper ledger factory must not run")

            with patch.dict(os.environ, {"BEELZEBUB_L3G_PAPER_LEDGER": str(paper_path)}):
                app = create_control_center_app(config, paper_ledger_factory=forbidden_factory)

                async def attempt_startup() -> None:
                    async with app.router.lifespan_context(app):
                        pass

                with self.assertRaisesRegex(RuntimeError, "active ledger path.*cold storage root") as raised:
                    asyncio.run(attempt_startup())
                self.assertIn(str(paper_path.resolve()), str(raised.exception))
            self.assertEqual(called, [])

    def test_only_closed_parameterless_paper_post_controls_exist(self) -> None:
        app = create_control_center_app(CopyTradeConfig())
        routes = {route.path: route for route in app.routes if hasattr(route, "methods")}
        expected = {
            "/api/lane-iii/paper/arm", "/api/lane-iii/paper/pause",
            "/api/lane-iii/paper/resume", "/api/lane-iii/paper/flatten-and-disarm",
        }
        self.assertTrue(expected.issubset(routes))
        self.assertFalse(any("order" in path and path.startswith("/api/lane-iii/paper") for path in routes))
        for path in expected:
            self.assertEqual(set(routes[path].methods), {"POST"})
            self.assertEqual(inspect.signature(routes[path].endpoint).parameters, {})
            self.assertNotIn("{", path)


if __name__ == "__main__":
    unittest.main()
