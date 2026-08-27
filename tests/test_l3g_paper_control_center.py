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

    def test_paper_controls_are_closed_and_commission_entry_requires_only_its_lifecycle_credential(self) -> None:
        app = create_control_center_app(CopyTradeConfig())
        routes = {route.path: route for route in app.routes if hasattr(route, "methods")}
        expected = {
            "/api/lane-iii/paper/arm", "/api/lane-iii/paper/pause",
            "/api/lane-iii/paper/resume", "/api/lane-iii/paper/flatten-and-disarm",
            "/api/lane-iii/paper/commissioning-arm", "/api/lane-iii/paper/commission-entry", "/api/lane-iii/paper/commission-exit",
            "/api/lane-iii/paper/commissioning-rehearsal", "/api/lane-iii/paper/commissioning-start",
        }
        self.assertTrue(expected.issubset(routes))
        self.assertFalse(any("order" in path and path.startswith("/api/lane-iii/paper") for path in routes))
        for path in expected:
            self.assertEqual(set(routes[path].methods), {"POST"})
            self.assertNotIn("{", path)
        for path in expected - {"/api/lane-iii/paper/commission-entry", "/api/lane-iii/paper/commissioning-start"}:
            self.assertEqual(inspect.signature(routes[path].endpoint).parameters, {})
        self.assertEqual(tuple(inspect.signature(routes["/api/lane-iii/paper/commission-entry"].endpoint).parameters), ("body",))
        self.assertEqual(tuple(inspect.signature(routes["/api/lane-iii/paper/commissioning-start"].endpoint).parameters), ("body",))
        arm_source = inspect.getsource(routes["/api/lane-iii/paper/commissioning-arm"].endpoint)
        entry_source = inspect.getsource(routes["/api/lane-iii/paper/commission-entry"].endpoint)
        start_source = inspect.getsource(routes["/api/lane-iii/paper/commissioning-start"].endpoint)
        rehearsal_source = inspect.getsource(routes["/api/lane-iii/paper/commissioning-rehearsal"].endpoint)
        self.assertIn("commissioning_arm(require_commissioning_ledger_verification)", arm_source)
        self.assertNotIn("require_commissioning_ledger_verification", entry_source)
        self.assertIn("commissioning_start", start_source)
        self.assertIn("commissioning_rehearsal", rehearsal_source)

    def test_local_ledger_verification_routes_are_separate_from_execution_authority(self) -> None:
        app = create_control_center_app(CopyTradeConfig())
        routes = {route.path: route for route in app.routes if hasattr(route, "methods")}
        expected = {
            "/api/lane-iii/paper/ledger-verification",
            "/api/lane-iii/paper/ledger-verification/cancel",
            "/api/lane-iii/paper/ledger-verification/schedule",
        }
        self.assertTrue(expected.issubset(routes))
        self.assertEqual(set(routes["/api/lane-iii/paper/ledger-verification"].methods), {"POST"})
        self.assertEqual(set(routes["/api/lane-iii/paper/ledger-verification/cancel"].methods), {"POST"})
        schedule_methods = set().union(*(set(route.methods) for route in app.routes if getattr(route, "path", None) == "/api/lane-iii/paper/ledger-verification/schedule"))
        self.assertEqual(schedule_methods, {"POST", "GET"})
        source = Path(__file__).resolve().parents[1] / "src" / "l3g_paper" / "verification.py"
        verifier_source = source.read_text(encoding="utf-8")
        self.assertNotIn("PaperExecutionTransport", verifier_source)
        self.assertNotIn("commission_entry", verifier_source)
        self.assertNotIn("live_capital", verifier_source)


if __name__ == "__main__":
    unittest.main()
