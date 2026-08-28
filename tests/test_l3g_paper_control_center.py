from __future__ import annotations

import asyncio
from dataclasses import replace
import inspect
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.copytrade.control_center import UnsafePaperExecutionShutdown, create_control_center_app
from src.copytrade.config import CopyTradeConfig
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import PaperExecutionTransport
from src.l3g_paper.runtime import LaneIIIPaperRuntime


class PaperControlCenterTests(unittest.TestCase):
    def test_unresolved_watchdog_defers_transport_and_ledger_shutdown(self) -> None:
        """A failed AddOn grace must retain its signed callback path.

        The test uses exact runtime/ledger/transport classes because the
        production factory rejects substitutes.  Their lifecycle methods are
        patched only to make the watchdog outcome deterministic and to trace
        shutdown ordering without opening listener sockets.
        """
        events: list[str] = []
        ledgers: list[PaperLedger] = []
        original_close = PaperLedger.close

        class ListeningObserver:
            class Status:
                def as_dict(self) -> dict[str, object]:
                    return {
                        "state": "STOPPED", "host": "127.0.0.1", "port": 48135,
                        "accepted_connections": 0, "rejected_connections": 0,
                        "received_frames": 0, "emitted_events": 0,
                        "invalid_events": 0, "error": None,
                    }

            def set_observation_sinks(self, **_: object) -> None:
                events.append("listener_sinks")

            def start(self) -> SimpleNamespace:
                events.append("listener_start")
                return SimpleNamespace(
                    state="LISTENING", host="127.0.0.1", port=48135, error=None,
                )

            def stop(self) -> None:
                events.append("listener_stop")

            def status(self) -> Status:
                return self.Status()

        def ledger_factory(path: Path) -> PaperLedger:
            ledger = PaperLedger(path)
            ledgers.append(ledger)
            return ledger

        def runtime_start(_: LaneIIIPaperRuntime) -> None:
            events.append("runtime_start")

        def runtime_stop(_: LaneIIIPaperRuntime) -> dict[str, object]:
            events.append("runtime_stop")
            return {
                "required": True,
                "completed": False,
                "reason": "TEST_UNRESOLVED_WATCHDOG",
                "remaining_seconds": 0.0,
                "flat_confirmed": False,
            }

        def watchdog_status(_: LaneIIIPaperRuntime) -> dict[str, object]:
            events.append("watchdog_status")
            return {
                "required": True,
                "reason": "TEST_UNRESOLVED_WATCHDOG",
                "remaining_seconds": 0.0,
                "flat_confirmed": False,
            }

        def transport_start(self: PaperExecutionTransport, **_: object) -> object:
            events.append("transport_start")
            return self.status()

        def transport_stop(self: PaperExecutionTransport, **_: object) -> object:
            events.append("transport_stop")
            return self.status()

        def traced_ledger_close(self: PaperLedger) -> dict[str, object]:
            events.append("ledger_close")
            return original_close(self)

        with TemporaryDirectory() as folder:
            root = Path(folder)
            defaults = CopyTradeConfig()
            config = replace(
                defaults,
                storage=replace(defaults.storage, cold_root=root / "cold"),
                artifacts=replace(defaults.artifacts, database_path=root / "hot" / "copytrade.sqlite3"),
            )
            app = create_control_center_app(
                config,
                ninjatrader_listener_factory=ListeningObserver,
                paper_ledger_factory=ledger_factory,
            )
            endpoint = next(route.endpoint for route in app.routes if route.path == "/api/lane-iii/paper")

            async def exercise() -> dict[str, object]:
                with self.assertRaises(UnsafePaperExecutionShutdown):
                    async with app.router.lifespan_context(app):
                        self.assertEqual(events[-1], "listener_start")
                return await endpoint()

            try:
                with (
                    patch.object(LaneIIIPaperRuntime, "start", runtime_start),
                    patch.object(LaneIIIPaperRuntime, "stop", runtime_stop),
                    patch.object(LaneIIIPaperRuntime, "watchdog_shutdown_status", watchdog_status),
                    patch.object(PaperExecutionTransport, "start", transport_start),
                    patch.object(PaperExecutionTransport, "stop", transport_stop),
                    patch.object(PaperLedger, "close", traced_ledger_close),
                ):
                    status = asyncio.run(exercise())
                    receipt = status["ledger_shutdown"]
                    self.assertIsInstance(receipt, dict)
                    self.assertFalse(receipt["clean_shutdown"])
                    self.assertTrue(receipt["transport_stop_skipped"])
                    self.assertTrue(receipt["ledger_close_deferred"])
                    self.assertTrue(receipt["lifecycle_shutdown_aborted"])
                    self.assertTrue(receipt["manual_intervention_required"])
                    self.assertIn("WATCHDOG_FLAT_UNCONFIRMED_OR_NON_DURABLE", str(receipt["error"]))
                    self.assertEqual(
                        receipt["runtime_watchdog_shutdown"],
                        {
                            "required": True,
                            "reason": "TEST_UNRESOLVED_WATCHDOG",
                            "remaining_seconds": 0.0,
                            "flat_confirmed": False,
                            "completed": False,
                        },
                    )
                    self.assertIn("watchdog_status", events)
                    self.assertLess(events.index("runtime_stop"), events.index("watchdog_status"))
                    self.assertNotIn("transport_stop", events)
                    self.assertNotIn("ledger_close", events)
                    self.assertIsNotNone(app.state.lane_iii_paper_transport)
            finally:
                # The production path deliberately leaves this writer alive
                # when the independent watchdog is unresolved.  The test
                # must close it explicitly after asserting that behavior.
                for ledger in ledgers:
                    original_close(ledger)

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
