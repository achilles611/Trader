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

from fastapi import HTTPException
from starlette.requests import Request

from src.copytrade.control_center import UnsafePaperExecutionShutdown, create_control_center_app
from src.copytrade.config import CopyTradeConfig
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import PaperExecutionTransport
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.lsc.shadow_runtime import LSCShadowConfig, LSCShadowRuntime
from tests.lsc.test_codec import observation


class PaperControlCenterTests(unittest.TestCase):
    def test_lsc_shadow_is_default_off_and_selected_lifecycle_is_authority_independent(self) -> None:
        events: list[str] = []
        listener_instances: list[object] = []
        original_ledger_close = PaperLedger.close

        class ListeningObserver:
            class Status:
                def as_dict(self) -> dict[str, object]:
                    return {
                        "state": "LISTENING", "host": "127.0.0.1", "port": 48135,
                        "accepted_connections": 1, "rejected_connections": 0,
                        "received_frames": 1, "emitted_events": 1,
                        "invalid_events": 0, "error": None,
                    }

            def __init__(self) -> None:
                self.on_observation = None
                listener_instances.append(self)

            def set_observation_sinks(self, **sinks: object) -> None:
                events.append("listener_sinks")
                self.on_observation = sinks["on_observation"]

            def start(self) -> SimpleNamespace:
                events.append("listener_start")
                return SimpleNamespace(
                    state="LISTENING", host="127.0.0.1", port=48135, error=None,
                )

            def stop(self) -> None:
                events.append("listener_stop")

            def status(self) -> Status:
                return self.Status()

        with TemporaryDirectory() as folder:
            root = Path(folder)
            defaults = CopyTradeConfig()
            config = replace(
                defaults,
                storage=replace(defaults.storage, cold_root=root / "cold"),
                artifacts=replace(
                    defaults.artifacts,
                    database_path=root / "hot" / "copytrade.sqlite3",
                ),
            )
            default_app = create_control_center_app(config)
            self.assertIsNone(default_app.state.lsc_shadow)
            self.assertFalse((root / "lsc").exists())

            runtime = LSCShadowRuntime(LSCShadowConfig(
                root=root / "lsc",
                journal_id="test-integration",
                profile_id="beelzebub_pong_discernment_v1",
                native_contract_id="MNQ DEC26",
                canonical_contract_id="MNQZ6",
            ))
            original_lsc_start = runtime.start
            original_lsc_submit = runtime.submit
            original_lsc_close = runtime.close

            def traced_lsc_start() -> dict[str, object]:
                events.append("lsc_start")
                return original_lsc_start()

            def traced_lsc_submit(value: object) -> bool:
                events.append("lsc_submit")
                return original_lsc_submit(value)  # type: ignore[arg-type]

            def traced_lsc_close(*, timeout_seconds: float = 30.0) -> dict[str, object]:
                events.append("lsc_close")
                return original_lsc_close(timeout_seconds=timeout_seconds)

            runtime.start = traced_lsc_start  # type: ignore[method-assign]
            runtime.submit = traced_lsc_submit  # type: ignore[method-assign]
            runtime.close = traced_lsc_close  # type: ignore[method-assign]

            def runtime_start(_: LaneIIIPaperRuntime) -> None:
                events.append("runtime_start")

            def runtime_stop(_: LaneIIIPaperRuntime) -> dict[str, object]:
                events.append("runtime_stop")
                return {
                    "required": False, "completed": True, "flat_confirmed": True,
                    "durable_confirmation": True, "reason": "TEST_FLAT",
                }

            def transport_start(self: PaperExecutionTransport, **_: object) -> object:
                events.append("transport_start")
                return self.status()

            def transport_stop(self: PaperExecutionTransport, **_: object) -> object:
                events.append("transport_stop")
                return self.status()

            def ledger_close(self: PaperLedger) -> dict[str, object]:
                events.append("ledger_close")
                return original_ledger_close(self)

            app = create_control_center_app(
                config,
                ninjatrader_listener_factory=ListeningObserver,
                paper_ledger_factory=lambda path: PaperLedger(path),
                lsc_shadow_factory=lambda: runtime,
            )

            async def exercise() -> None:
                async with app.router.lifespan_context(app):
                    self.assertIs(app.state.lsc_shadow, runtime)
                    listener = listener_instances[-1]
                    assert listener.on_observation is not None
                    listener.on_observation(observation("QUOTE", 1))

            with (
                patch.object(LaneIIIPaperRuntime, "start", runtime_start),
                patch.object(LaneIIIPaperRuntime, "stop", runtime_stop),
                patch.object(PaperExecutionTransport, "start", transport_start),
                patch.object(PaperExecutionTransport, "stop", transport_stop),
                patch.object(PaperLedger, "close", ledger_close),
            ):
                asyncio.run(exercise())

            self.assertLess(events.index("lsc_start"), events.index("listener_sinks"))
            self.assertLess(events.index("listener_sinks"), events.index("listener_start"))
            self.assertEqual(events.count("lsc_submit"), 1)
            self.assertLess(events.index("runtime_stop"), events.index("transport_stop"))
            self.assertLess(events.index("transport_stop"), events.index("ledger_close"))
            self.assertLess(events.index("ledger_close"), events.index("lsc_close"))
            self.assertIsNone(app.state.lsc_shadow)
            self.assertTrue(app.state.lsc_shadow_shutdown_receipt["proof_valid"])

    def test_pending_evidence_recovery_endpoint_is_scoped_and_dispatches_without_arming(self):
        app = create_control_center_app(CopyTradeConfig())
        endpoint = next(route.endpoint for route in app.routes
                        if getattr(route, 'path', None) == '/api/lane-iii/paper/pending-lockout-evidence-recovery')
        state = inspect.getclosurevars(endpoint).nonlocals['ninjatrader_runtime']
        calls = []
        def recovery(request_id, phase, evidence, preflight):
            calls.append((request_id, phase, evidence, callable(preflight)))
            return {'phase': phase, 'armed': False, 'lockout_cleared': False}
        state['paper'] = SimpleNamespace(recover_pending_lockout_evidence=recovery)
        def request(host, action):
            return Request({'type': 'http', 'method': 'POST', 'path': '/',
                'scheme': 'http', 'server': (host, 8090), 'query_string': b'',
                'headers': [(b'x-beelzebub-pending-lockout-recovery-action', action.encode())]})
        action = 'lane-iii-pending-lockout-evidence-recovery-v1'
        body = {'request_id': 'TEST-ONLY-evidence', 'phase': 'RECORD', 'evidence': {'test_only': True}}
        for host, header in (('outside.example', action), ('127.0.0.1', 'wrong')):
            with self.assertRaises(HTTPException) as caught:
                asyncio.run(endpoint(request(host, header), body))
            self.assertEqual(caught.exception.status_code, 403)
        with self.assertRaises(HTTPException) as caught:
            asyncio.run(endpoint(request('127.0.0.1', action), dict(body, arm=True)))
        self.assertEqual(caught.exception.status_code, 400)
        self.assertEqual(calls, [])
        result = asyncio.run(endpoint(request('127.0.0.1', action), body))
        self.assertFalse(result['armed'])
        self.assertFalse(result['lockout_cleared'])
        self.assertEqual(calls, [('TEST-ONLY-evidence', 'RECORD', {'test_only': True}, True)])

    def test_independent_collection_dispatch_has_no_unbound_profile_name(self) -> None:
        """TEST ONLY: execute the exact live callback branch, not a source assertion."""
        from src.copytrade.control_center import (
            _ObservationIngressMetrics,
            _dispatch_pong_observation,
            _pong_market_collection_only,
            _pong_market_paper_only,
        )
        for kind in ("TRADE", "QUOTE", "DEPTH"):
            observation = SimpleNamespace(observation_type=kind)
            self.assertTrue(_pong_market_collection_only(True, "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2", observation))
            self.assertTrue(_pong_market_collection_only(True, "BEELZEBUB_PONG_DISCERNMENT_V1", observation))
            self.assertFalse(_pong_market_collection_only(
                True, "BEELZEBUB_PONG_DISCERNMENT_V1", observation,
                activation_authorized=True,
            ))
            self.assertTrue(_pong_market_paper_only(True, "BEELZEBUB_PONG_DISCERNMENT_V1", observation))
            self.assertFalse(_pong_market_collection_only(False, "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2", observation))
        for kind in ("ACCOUNT", "POSITION", "ORDER", "HEALTH", "EXECUTION"):
            self.assertFalse(_pong_market_collection_only(True, "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2",
                                                        SimpleNamespace(observation_type=kind)))
            self.assertFalse(_pong_market_paper_only(True, "BEELZEBUB_PONG_DISCERNMENT_V1",
                                                    SimpleNamespace(observation_type=kind)))

        calls: list[str] = []
        collector = SimpleNamespace(ingest=lambda _: calls.append("collector"))
        pong = SimpleNamespace(on_observation=lambda _: calls.append("pong"))
        fanout = SimpleNamespace(
            on_paper_observation=lambda _: calls.append("paper"),
            on_observation=lambda _: calls.extend(("shadow", "paper")),
        )
        metrics = _ObservationIngressMetrics()
        market = SimpleNamespace(observation_type="DEPTH")
        _dispatch_pong_observation(
            market, collector=collector, pong_host=pong, fanout=fanout,
            metrics=metrics, independent=True,
            profile="BEELZEBUB_PONG_DISCERNMENT_V1", activation_authorized=False,
        )
        self.assertEqual(calls, ["collector", "pong"])
        snapshot = metrics.snapshot()
        self.assertEqual(snapshot["routes"], {"PONG_COLLECTION_ONLY": 1})
        self.assertEqual(snapshot["inputs"], 1)
        self.assertEqual(snapshot["stages"]["collector_enqueue"]["count"], 1)
        self.assertEqual(snapshot["stages"]["pong_host"]["count"], 1)
        self.assertEqual(snapshot["stages"]["composite_total"]["count"], 1)

        calls.clear()
        account = SimpleNamespace(observation_type="ACCOUNT")
        _dispatch_pong_observation(
            account, collector=collector, pong_host=pong, fanout=fanout,
            metrics=metrics, independent=True,
            profile="BEELZEBUB_PONG_DISCERNMENT_V1", activation_authorized=False,
        )
        self.assertEqual(calls, ["collector", "pong", "shadow", "paper"])
        self.assertEqual(metrics.snapshot()["routes"]["FULL_LEGACY_FANOUT"], 1)

    def test_unresolved_watchdog_defers_transport_and_ledger_shutdown(self) -> None:
        """A failed AddOn grace must retain its signed callback path.

        The test uses exact runtime/ledger/transport classes because the
        production factory rejects substitutes.  Their lifecycle methods are
        patched only to make the watchdog outcome deterministic and to trace
        shutdown ordering without opening listener sockets.
        """
        events: list[str] = []
        ledgers: list[PaperLedger] = []
        lsc_runtimes: list[LSCShadowRuntime] = []
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
            lsc_runtime = LSCShadowRuntime(LSCShadowConfig(
                root=root / "lsc",
                journal_id="test-unresolved-watchdog",
                profile_id="beelzebub_pong_discernment_v1",
                native_contract_id="MNQ DEC26",
                canonical_contract_id="MNQZ6",
            ))
            lsc_runtimes.append(lsc_runtime)
            original_lsc_start = lsc_runtime.start
            original_lsc_close = lsc_runtime.close

            def traced_lsc_start() -> dict[str, object]:
                events.append("lsc_start")
                return original_lsc_start()

            def traced_lsc_close(*, timeout_seconds: float = 30.0) -> dict[str, object]:
                events.append("lsc_close")
                return original_lsc_close(timeout_seconds=timeout_seconds)

            lsc_runtime.start = traced_lsc_start  # type: ignore[method-assign]
            lsc_runtime.close = traced_lsc_close  # type: ignore[method-assign]
            app = create_control_center_app(
                config,
                ninjatrader_listener_factory=ListeningObserver,
                paper_ledger_factory=ledger_factory,
                lsc_shadow_factory=lambda: lsc_runtime,
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
                    self.assertIn("lsc_start", events)
                    self.assertNotIn("lsc_close", events)
                    self.assertIsNotNone(app.state.lane_iii_paper_transport)
                    self.assertIs(app.state.lsc_shadow, lsc_runtime)
            finally:
                # The production path deliberately leaves this writer alive
                # when the independent watchdog is unresolved.  The test
                # must close it explicitly after asserting that behavior.
                for ledger in ledgers:
                    original_close(ledger)
                for lsc in lsc_runtimes:
                    # Close the test-owned daemon only after the production
                    # HOLD behavior above has been established.
                    lsc.close()

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

    def test_explicit_new_production_ledger_requires_epoch_before_creation(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder)
            paper_path = root / "hot" / "fresh-paper.sqlite3"
            defaults = CopyTradeConfig()
            config = replace(
                defaults,
                storage=replace(defaults.storage, cold_root=root / "cold"),
                artifacts=replace(defaults.artifacts, database_path=root / "hot" / "copytrade.sqlite3"),
            )
            environment = {
                key: value for key, value in os.environ.items()
                if not key.startswith("BEELZEBUB_")
            }
            environment.update({
                "BEELZEBUB_L3G_PAPER_LEDGER": str(paper_path),
                # Make the profile explicit so remembered selector state cannot
                # replace the ledger path under test.
                "BEELZEBUB_L3G_PAPER_PROFILE": "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2",
            })
            with (
                patch.dict(os.environ, environment, clear=True),
                patch("src.copytrade.control_center.PaperLedger") as ledger_constructor,
            ):
                app = create_control_center_app(config)

                async def attempt_startup() -> None:
                    async with app.router.lifespan_context(app):
                        pass

                with self.assertRaisesRegex(
                    RuntimeError,
                    "New production paper ledger requires BEELZEBUB_L3G_PAPER_LEDGER_EPOCH",
                ):
                    asyncio.run(attempt_startup())
            ledger_constructor.assert_not_called()
            self.assertFalse(paper_path.exists())

    def test_paper_controls_are_closed_and_commission_entry_requires_only_its_lifecycle_credential(self) -> None:
        app = create_control_center_app(CopyTradeConfig())
        routes = {route.path: route for route in app.routes if hasattr(route, "methods")}
        expected = {
            "/api/lane-iii/paper/arm", "/api/lane-iii/paper/pause",
            "/api/lane-iii/paper/resume", "/api/lane-iii/paper/flatten-and-disarm",
            "/api/lane-iii/paper/commissioning-arm", "/api/lane-iii/paper/commission-entry", "/api/lane-iii/paper/commission-exit",
            "/api/lane-iii/paper/commissioning-rehearsal", "/api/lane-iii/paper/commissioning-start",
            "/api/lane-iii/paper/operational-start",
            "/api/lane-iii/paper/ledger-capacity-recovery",
            "/api/lane-iii/paper/bounded-trial",
        }
        self.assertTrue(expected.issubset(routes))
        self.assertFalse(any("order" in path and path.startswith("/api/lane-iii/paper") for path in routes))
        for path in expected - {"/api/lane-iii/paper/bounded-trial"}:
            self.assertEqual(set(routes[path].methods), {"POST"})
            self.assertNotIn("{", path)
        bounded_methods = set().union(*(
            set(route.methods)
            for route in app.routes
            if getattr(route, "path", None) == "/api/lane-iii/paper/bounded-trial"
        ))
        self.assertEqual(bounded_methods, {"GET", "POST"})
        for path in expected - {"/api/lane-iii/paper/commission-entry", "/api/lane-iii/paper/commissioning-start", "/api/lane-iii/paper/operational-start", "/api/lane-iii/paper/ledger-capacity-recovery", "/api/lane-iii/paper/bounded-trial"}:
            self.assertEqual(inspect.signature(routes[path].endpoint).parameters, {})
        self.assertEqual(tuple(inspect.signature(routes["/api/lane-iii/paper/commission-entry"].endpoint).parameters), ("body",))
        self.assertEqual(tuple(inspect.signature(routes["/api/lane-iii/paper/commissioning-start"].endpoint).parameters), ("body",))
        self.assertEqual(tuple(inspect.signature(routes["/api/lane-iii/paper/operational-start"].endpoint).parameters), ("body",))
        self.assertEqual(
            tuple(inspect.signature(
                routes["/api/lane-iii/paper/ledger-capacity-recovery"].endpoint,
            ).parameters),
            ("request", "body"),
        )
        arm_source = inspect.getsource(routes["/api/lane-iii/paper/commissioning-arm"].endpoint)
        entry_source = inspect.getsource(routes["/api/lane-iii/paper/commission-entry"].endpoint)
        start_source = inspect.getsource(routes["/api/lane-iii/paper/commissioning-start"].endpoint)
        operational_source = inspect.getsource(routes["/api/lane-iii/paper/operational-start"].endpoint)
        rehearsal_source = inspect.getsource(routes["/api/lane-iii/paper/commissioning-rehearsal"].endpoint)
        self.assertIn("commissioning_arm(require_commissioning_ledger_verification)", arm_source)
        self.assertNotIn("require_commissioning_ledger_verification", entry_source)
        self.assertIn("commissioning_start", start_source)
        self.assertIn("operational_paper_start", operational_source)
        self.assertNotIn("commissioning_start", operational_source)
        lifecycle_source = inspect.getsource(create_control_center_app)
        self.assertIn("OPERATIONAL_FULL_LEDGER_VERIFICATION_REQUIRED", lifecycle_source)
        self.assertIn('ledger_verifier.start("full")', lifecycle_source)
        self.assertIn('ledger_verifier.start("auto")', lifecycle_source)
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

        lifecycle_source = inspect.getsource(create_control_center_app)
        verifier_refusal = lifecycle_source.index(
            'if verifier_shutdown.get("completed") is not True:'
        )
        ledger_close = lifecycle_source.index("paper_ledger.close()", verifier_refusal)
        self.assertLess(verifier_refusal, ledger_close)
        self.assertIn("VERIFIER_READER_NOT_QUIESCED", lifecycle_source[verifier_refusal:ledger_close])


class GracefulShutdownEndpointTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        root = Path(self.temporary.name)
        defaults = CopyTradeConfig()
        config = replace(
            defaults,
            storage=replace(defaults.storage, cold_root=root / "cold"),
            artifacts=replace(defaults.artifacts, database_path=root / "hot" / "copytrade.sqlite3"),
        )
        self.app = create_control_center_app(config)
        self.endpoint = next(
            route.endpoint for route in self.app.routes
            if route.path == "/api/system/graceful-shutdown"
        )
        self.runtime = inspect.getclosurevars(self.endpoint).nonlocals["ninjatrader_runtime"]
        self.status = {
            "state": "READY_DISARMED",
            "paper_execution": "DISARMED",
            "session_armed_state": "DISARMED",
            "current_position": "FLAT",
            "current_quantity": 0,
            "broker_snapshot_position": "FLAT",
            "broker_snapshot_position_quantity": 0,
            "working_owned_orders": 0,
            "working_entry_orders": 0,
            "unresolved_command": False,
            "unresolved_native_order": False,
            "unresolved_execution": False,
            "entry_owner": "NONE",
            "operational_paper_session": None,
            "reconciliation_current": True,
            "ledger": {
                "deferred_queue_depth": 0,
                "deferred_pending_queue_depth": 0,
                "deferred_inflight_queue_depth": 0,
                "deferred_pending_barrier_count": 0,
                "deferred_writer_error": None,
            },
        }
        self.runtime["paper"] = SimpleNamespace(status=lambda: self.status)

    def tearDown(self) -> None:
        self.app.state.paper_autostart.stop()
        self.temporary.cleanup()

    @staticmethod
    def request(*, authenticated: bool = True, host: str = "127.0.0.1:8090") -> Request:
        headers = [(b"host", host.encode())]
        if authenticated:
            headers.append((
                b"x-beelzebub-graceful-shutdown-action",
                b"beezconsole-graceful-shutdown-v1",
            ))
        return Request({
            "type": "http", "http_version": "1.1", "method": "POST",
            "scheme": "http", "path": "/api/system/graceful-shutdown",
            "raw_path": b"/api/system/graceful-shutdown", "query_string": b"",
            "headers": headers, "client": ("127.0.0.1", 50000),
            "server": ("127.0.0.1", 8090),
        })

    async def test_exact_disarmed_flat_state_requests_lifespan_shutdown(self) -> None:
        requested = asyncio.Event()
        self.app.state.request_server_shutdown = requested.set
        result = await self.endpoint(
            self.request(), {"request_id": "shutdown-endpoint-test-0001"},
        )
        self.assertTrue(result["accepted"])
        await asyncio.wait_for(requested.wait(), timeout=1)

    async def test_exact_terminal_locked_flat_state_requests_lifespan_shutdown(self) -> None:
        self.status["state"] = "LOCKED_OUT"
        self.status["paper_execution"] = "LOCKED"
        requested = asyncio.Event()
        self.app.state.request_server_shutdown = requested.set
        result = await self.endpoint(
            self.request(), {"request_id": "shutdown-endpoint-test-locked-0001"},
        )
        self.assertTrue(result["accepted"])
        await asyncio.wait_for(requested.wait(), timeout=1)

    async def test_refuses_inconsistent_terminal_shutdown_state(self) -> None:
        self.status["state"] = "LOCKED_OUT"
        with self.assertRaises(HTTPException) as refused:
            await self.endpoint(
                self.request(), {"request_id": "shutdown-endpoint-test-locked-0002"},
            )
        self.assertEqual(refused.exception.status_code, 409)

    async def test_refuses_missing_confirmation_and_uncontrolled_host(self) -> None:
        for request in (self.request(authenticated=False), self.request(host="example.com")):
            with self.subTest(headers=dict(request.headers)):
                with self.assertRaises(HTTPException) as refused:
                    await self.endpoint(request, {"request_id": "shutdown-endpoint-test-0002"})
                self.assertEqual(refused.exception.status_code, 403)
        with self.assertRaises(HTTPException) as unavailable:
            await self.endpoint(
                self.request(), {"request_id": "shutdown-endpoint-test-0003"},
            )
        self.assertEqual(unavailable.exception.status_code, 503)


if __name__ == "__main__":
    unittest.main()
