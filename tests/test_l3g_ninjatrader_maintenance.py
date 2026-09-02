from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest

from fastapi import HTTPException
from starlette.requests import Request

from src.copytrade.config import CopyTradeConfig
from src.copytrade.control_center import create_control_center_app
from src.l3g_paper.ninjatrader_maintenance import (
    DesktopProbe,
    MaintenanceStage,
    MaintenanceTimeouts,
    NinjaTraderMaintenanceService,
)


def ready_paper() -> dict[str, object]:
    return {
        "state": "READY_DISARMED",
        "paper_execution": "DISARMED",
        "session_armed_state": "DISARMED",
        "live_capital": "DENIED",
        "account_class": "LOCAL_SIMULATION",
        "market_instrument": "MNQ SEP26",
        "current_position": "FLAT",
        "current_quantity": 0,
        "broker_snapshot_position": "FLAT",
        "broker_snapshot_position_quantity": 0,
        "position_snapshot_complete": True,
        "order_snapshot_complete": True,
        "working_owned_orders": 0,
        "working_entry_orders": 0,
        "unresolved_command": False,
        "unresolved_native_order": False,
        "unresolved_execution": False,
        "entry_owner": "NONE",
        "reconciliation_current": True,
        "commissioning_lifecycle": {"active": False},
        "operational_paper_session": None,
        "last_command": {"command_sequence": 3},
        "transport": {
            "state": "AUTHENTICATED",
            "authenticated_client": True,
            "reconciled": True,
            "addon_provenance": {"status": "MATCH", "protocol_version": "test-v1"},
        },
        "market_observer": {
            "market_observer_state": "ACTIVE",
            "market_observer_active": True,
            "market_observer_freshness": {"fresh": True, "reason": "CURRENT"},
            "last_level_one_at": "2026-09-02T17:00:00Z",
            "last_depth_at": "2026-09-02T17:00:00Z",
            "observer_attachment": {
                "state": "OBSERVER_ATTACHED",
                "configured_instrument": "MNQ SEP26",
                "instrument": "MNQ SEP26",
                "chart_found": True,
                "observer_attached": True,
                "observed_at": "2026-09-02T17:00:00Z",
            },
        },
        "ledger": {
            "unverified_tail_rows": 0,
            "operational_ledger": {"tail_tip_sequence": 358},
        },
    }


def disarmed_live() -> dict[str, object]:
    return {"live_authority": "DISARMED", "live_capital": "DENIED"}


def passing_ledger() -> dict[str, object]:
    return {
        "status": "PASS",
        "verification_id": "lv-test",
        "verified_through_sequence": 358,
        "captured_tip_sequence": 358,
        "chain_valid": True,
        "checkpoint_valid": True,
    }


class MutableStatus:
    def __init__(self, value: dict[str, object]) -> None:
        self.value = value
        self.lock = threading.RLock()

    def __call__(self) -> dict[str, object]:
        with self.lock:
            return deepcopy(self.value)


class FakeDesktop:
    def __init__(
        self,
        *,
        process: bool,
        control_center: bool = True,
        login_window: bool = False,
        close_exits: bool = True,
        start_succeeds: bool = True,
        on_start=None,
    ) -> None:
        self.process = process
        self.control_center = control_center if process else False
        self.login_window = login_window if process else False
        self.close_exits = close_exits
        self.start_succeeds = start_succeeds
        self.on_start = on_start
        self.probe_calls = 0
        self.configure_calls: list[str] = []
        self.start_calls = 0
        self.close_calls = 0

    def probe(self) -> DesktopProbe:
        self.probe_calls += 1
        return DesktopProbe(self.process, self.login_window, self.control_center, None)

    def configure_instrument(self, instrument: str) -> bool:
        self.configure_calls.append(instrument)
        return instrument == "MNQ SEP26"

    def start(self) -> bool:
        self.start_calls += 1
        if not self.start_succeeds:
            return False
        self.process = True
        if self.on_start is not None:
            self.on_start(self)
        return True

    def request_graceful_shutdown(self) -> bool:
        self.close_calls += 1
        if self.close_exits:
            self.process = False
            self.control_center = False
            self.login_window = False
        return True


class LedgerSource:
    def __init__(self, value: dict[str, object] | None = None) -> None:
        self.value = value or passing_ledger()
        self.start_calls = 0

    def status(self) -> dict[str, object]:
        return deepcopy(self.value)

    def start(self) -> dict[str, object]:
        self.start_calls += 1
        return self.status()


FAST_TIMEOUTS = MaintenanceTimeouts(
    process_start_seconds=0.04,
    graceful_shutdown_seconds=0.04,
    operator_login_seconds=0.04,
    addon_seconds=0.04,
    chart_seconds=0.04,
    market_data_seconds=0.04,
    reconciliation_seconds=0.04,
    ledger_seconds=0.04,
    poll_seconds=0.001,
)


class NinjaTraderMaintenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.paper = MutableStatus(ready_paper())
        self.live = MutableStatus(disarmed_live())
        self.ledger = LedgerSource()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def service(self, desktop: FakeDesktop, *, timeouts: MaintenanceTimeouts = FAST_TIMEOUTS) -> NinjaTraderMaintenanceService:
        return NinjaTraderMaintenanceService(
            paper_status=self.paper,
            live_status=self.live,
            ledger_status=self.ledger.status,
            start_ledger_verification=self.ledger.start,
            desktop=desktop,
            audit_path=Path(self.temporary.name) / "maintenance.jsonl",
            timeouts=timeouts,
        )

    @staticmethod
    def make_observer_unhealthy(paper: dict[str, object]) -> None:
        observer = paper["market_observer"]
        assert isinstance(observer, dict)
        observer["market_observer_state"] = "STALE"
        observer["market_observer_active"] = False
        observer["market_observer_freshness"] = {"fresh": False, "reason": "STALE_OBSERVATION_TIMESTAMP"}
        observer["observer_attachment"] = {
            "state": "OBSERVER_MISSING", "configured_instrument": "MNQ SEP26",
            "instrument": "MNQ SEP26", "chart_found": True,
            "observer_attached": False, "observed_at": "2026-09-02T17:00:00Z",
        }

    def run_service(self, service: NinjaTraderMaintenanceService, request: str = "ntm-test-0001") -> dict[str, object]:
        service.start(request)
        service.wait(2)
        return service.status()

    def test_process_absent_launches_once_and_finishes_ready(self) -> None:
        desktop = FakeDesktop(process=False, on_start=lambda value: setattr(value, "control_center", True))
        status = self.run_service(self.service(desktop))
        self.assertEqual(status["stage"], "READY")
        self.assertEqual(desktop.start_calls, 1)
        self.assertEqual(desktop.close_calls, 0)
        self.assertEqual(desktop.configure_calls, ["MNQ SEP26"])
        self.assertEqual(status["button"]["label"], "NinjaTrader Ready")

    def test_already_running_healthy_is_idempotent_and_does_not_restart(self) -> None:
        desktop = FakeDesktop(process=True)
        status = self.run_service(self.service(desktop))
        self.assertEqual(status["stage"], "READY")
        self.assertEqual((desktop.start_calls, desktop.close_calls), (0, 0))
        self.assertEqual(self.ledger.start_calls, 1)

    def test_unhealthy_observer_uses_gated_graceful_restart_then_recovers(self) -> None:
        self.make_observer_unhealthy(self.paper.value)

        def recover(value: FakeDesktop) -> None:
            value.control_center = True
            self.paper.value = ready_paper()

        desktop = FakeDesktop(process=True, on_start=recover)
        status = self.run_service(self.service(desktop))
        self.assertEqual(status["stage"], "READY")
        self.assertEqual((desktop.close_calls, desktop.start_calls), (1, 1))
        self.assertEqual(status["actions"]["graceful_shutdowns"], 1)
        self.assertEqual(status["actions"]["forced_shutdowns"], 0)

    def assert_restart_refused(self, expected: str) -> dict[str, object]:
        self.make_observer_unhealthy(self.paper.value)
        desktop = FakeDesktop(process=True)
        status = self.run_service(self.service(desktop))
        self.assertEqual(status["stage"], "BLOCKED")
        self.assertIn(expected, status["blockers"])
        self.assertEqual((desktop.close_calls, desktop.start_calls), (0, 0))
        return status

    def test_stale_reconciliation_refuses_restart(self) -> None:
        self.paper.value["reconciliation_current"] = False
        self.paper.value["state"] = "WAITING_FOR_EXECUTION_BRIDGE"
        self.assert_restart_refused("RECONCILIATION_NOT_CURRENT")

    def test_non_flat_refuses_restart(self) -> None:
        self.paper.value["current_position"] = "LONG"
        self.paper.value["current_quantity"] = 1
        self.paper.value["broker_snapshot_position"] = "LONG"
        self.paper.value["broker_snapshot_position_quantity"] = 1
        self.assert_restart_refused("POSITION_NOT_FLAT")

    def test_working_order_refuses_restart(self) -> None:
        self.paper.value["working_owned_orders"] = 1
        self.assert_restart_refused("WORKING_OWNED_ORDERS_PRESENT")

    def test_each_unresolved_state_refuses_restart(self) -> None:
        for field, blocker in (
            ("unresolved_command", "UNRESOLVED_COMMAND_STATE"),
            ("unresolved_native_order", "UNRESOLVED_NATIVE_ORDER_STATE"),
            ("unresolved_execution", "UNRESOLVED_EXECUTION_STATE"),
        ):
            with self.subTest(field=field):
                self.paper.value = ready_paper()
                self.paper.value[field] = True
                self.assert_restart_refused(blocker)

    def test_unverified_ledger_tail_refuses_restart(self) -> None:
        self.ledger.value["verified_through_sequence"] = 357
        self.assert_restart_refused("LEDGER_UNVERIFIED_TAIL")

    def test_unknown_paper_or_live_authority_refuses_restart(self) -> None:
        for mutation, blocker in (
            (("paper", "account_class", "UNKNOWN"), "ACCOUNT_NOT_LOCAL_SIMULATION"),
            (("live", "live_authority", "UNKNOWN"), "LIVE_AUTHORITY_NOT_DISARMED"),
        ):
            with self.subTest(blocker=blocker):
                self.paper.value = ready_paper()
                self.live.value = disarmed_live()
                target, field, value = mutation
                (self.paper.value if target == "paper" else self.live.value)[field] = value
                self.assert_restart_refused(blocker)

    def test_graceful_shutdown_timeout_never_force_kills(self) -> None:
        self.make_observer_unhealthy(self.paper.value)
        desktop = FakeDesktop(process=True, close_exits=False)
        status = self.run_service(self.service(desktop))
        self.assertIn("GRACEFUL_SHUTDOWN_TIMEOUT_REQUIRES_OPERATOR_CONFIRMATION", status["blockers"])
        self.assertEqual(status["actions"]["forced_shutdowns"], 0)
        self.assertIn("no force kill", str(status["manual_action"]))

    def test_waits_for_operator_login_without_credential_or_connection_action(self) -> None:
        def login_wait(value: FakeDesktop) -> None:
            value.login_window = True
            value.control_center = False

        desktop = FakeDesktop(process=False, on_start=login_wait)
        status = self.run_service(self.service(desktop))
        self.assertEqual(status["blockers"], ["WAITING_FOR_OPERATOR_LOGIN"])
        self.assertIn("login manually", str(status["manual_action"]))
        self.assertEqual(desktop.start_calls, 1)

    def test_addon_provenance_mismatch_blocks_after_launch(self) -> None:
        transport = self.paper.value["transport"]
        assert isinstance(transport, dict)
        transport["addon_provenance"] = {"status": "MISMATCH"}
        desktop = FakeDesktop(process=False, on_start=lambda value: setattr(value, "control_center", True))
        status = self.run_service(self.service(desktop))
        self.assertIn("ADDON_PROVENANCE_MISMATCH", status["blockers"])

    def test_wrong_chart_instrument_blocks_with_one_manual_step(self) -> None:
        observer = self.paper.value["market_observer"]
        assert isinstance(observer, dict)
        observer["observer_attachment"] = {
            "state": "WRONG_CHART_INSTRUMENT", "configured_instrument": "MNQ SEP26",
            "instrument": "MNQ DEC26", "chart_found": True,
            "observer_attached": False, "observed_at": "2026-09-02T17:00:00Z",
        }
        desktop = FakeDesktop(process=False, on_start=lambda value: setattr(value, "control_center", True))
        status = self.run_service(self.service(desktop))
        self.assertIn("WRONG_CHART_INSTRUMENT", status["blockers"])
        self.assertIn("MNQ SEP26", str(status["manual_action"]))

    def test_existing_correct_chart_and_attached_observer_are_reused(self) -> None:
        desktop = FakeDesktop(process=False, on_start=lambda value: setattr(value, "control_center", True))
        status = self.run_service(self.service(desktop))
        self.assertEqual(status["stage"], "READY")
        self.assertTrue(status["chart"]["found"])
        self.assertFalse(status["chart"]["created"])
        self.assertTrue(status["observer"]["attached"])

    def test_duplicate_click_reuses_one_operation_and_one_launch(self) -> None:
        def login_wait(value: FakeDesktop) -> None:
            value.login_window = True
            value.control_center = False

        timeouts = MaintenanceTimeouts(**{**FAST_TIMEOUTS.__dict__, "operator_login_seconds": 0.15})
        desktop = FakeDesktop(process=False, on_start=login_wait)
        service = self.service(desktop, timeouts=timeouts)
        first = service.start("ntm-duplicate-0001")
        second = service.start("ntm-duplicate-0001")
        service.wait(2)
        self.assertEqual(first["operation_id"], second["operation_id"])
        self.assertEqual(desktop.start_calls, 1)

    def test_market_data_freshness_timeout_blocks(self) -> None:
        observer = self.paper.value["market_observer"]
        assert isinstance(observer, dict)
        observer["market_observer_state"] = "STALE"
        observer["market_observer_active"] = False
        observer["market_observer_freshness"] = {"fresh": False, "reason": "STALE_OBSERVATION_TIMESTAMP"}
        desktop = FakeDesktop(process=False, on_start=lambda value: setattr(value, "control_center", True))
        status = self.run_service(self.service(desktop))
        self.assertIn("MARKET_DATA_FRESHNESS_TIMEOUT", status["blockers"])

    def test_success_records_zero_execution_commands_and_orders(self) -> None:
        desktop = FakeDesktop(process=False, on_start=lambda value: setattr(value, "control_center", True))
        service = self.service(desktop)
        status = self.run_service(service)
        actions = status["actions"]
        self.assertEqual(actions["execution_command_baseline"], 3)
        self.assertEqual(actions["current_execution_command_count"], 3)
        self.assertEqual(actions["execution_commands_sent_by_task"], 0)
        self.assertEqual(actions["orders_submitted_by_task"], 0)
        self.assertEqual(actions["orders_cancelled_by_task"], 0)
        self.assertTrue(service.audit_path.is_file())
        audit = service.audit_path.read_text(encoding="utf-8")
        self.assertIn('"execution_command_baseline":3', audit)
        self.assertIn('"execution_commands_sent_by_task":0', audit)

    def test_unwritable_audit_fails_before_any_desktop_action(self) -> None:
        desktop = FakeDesktop(process=False, on_start=lambda value: setattr(value, "control_center", True))
        audit_directory = Path(self.temporary.name) / "audit-is-a-directory"
        audit_directory.mkdir()
        service = NinjaTraderMaintenanceService(
            paper_status=self.paper,
            live_status=self.live,
            ledger_status=self.ledger.status,
            start_ledger_verification=self.ledger.start,
            desktop=desktop,
            audit_path=audit_directory,
            timeouts=FAST_TIMEOUTS,
        )
        status = service.start("ntm-audit-test-0001")
        self.assertEqual(status["blockers"], ["MAINTENANCE_AUDIT_UNAVAILABLE"])
        self.assertFalse(status["audit"]["durable"])
        self.assertEqual((desktop.start_calls, desktop.close_calls), (0, 0))

    def test_maintenance_sources_have_no_execution_login_or_force_kill_path(self) -> None:
        root = Path(__file__).parents[1]
        source = (root / "src" / "l3g_paper" / "ninjatrader_maintenance.py").read_text(encoding="utf-8")
        helper = (root / "tools" / "ninjatrader_autologin.ps1").read_text(encoding="utf-8")
        for forbidden in (
            "PaperExecutionCommand", "PaperExecutionIntent", ".arm(", "submit_login(",
            "connect_lucid(", "Stop-Process", "taskkill", ".Kill(", "TerminateProcess",
        ):
            self.assertNotIn(forbidden, source + "\n" + helper)
        self.assertIn("CloseMainWindow()", helper)
        self.assertIn("WindowPattern]::Pattern", helper)
        self.assertIn("Resolve-NinjaExecutable", helper)
        self.assertIn("request_id", source)


class FakeMaintenanceEndpointService:
    def __init__(self) -> None:
        self.requests: list[str] = []
        self.action_token = "endpoint-fixture-token"

    def status(self) -> dict[str, object]:
        return {"stage": "IDLE", "requests": list(self.requests)}

    def start(self, request_id: str) -> dict[str, object]:
        self.requests.append(request_id)
        return {"stage": "CHECKING", "request_id": request_id}

    def stop(self) -> None:
        return None


class NinjaTraderMaintenanceEndpointTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        root = Path(self.temporary.name)
        defaults = CopyTradeConfig()
        config = replace(
            defaults,
            storage=replace(defaults.storage, cold_root=root / "cold"),
            artifacts=replace(defaults.artifacts, database_path=root / "hot" / "copytrade.sqlite3"),
        )
        self.service = FakeMaintenanceEndpointService()
        self.app = create_control_center_app(
            config,
            ninjatrader_maintenance_factory=lambda **_: self.service,
        )
        self.get_endpoint = next(
            route.endpoint for route in self.app.routes
            if route.path == "/api/lane-iii/ninjatrader-maintenance" and "GET" in route.methods
        )
        self.post_endpoint = next(
            route.endpoint for route in self.app.routes
            if route.path == "/api/lane-iii/ninjatrader-maintenance" and "POST" in route.methods
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def request(*, host: str = "127.0.0.1:8090", origin: str | None = None, authenticated: bool = True) -> Request:
        headers = [(b"host", host.encode())]
        if origin is not None:
            headers.append((b"origin", origin.encode()))
        if authenticated:
            headers.append((b"x-beelzebub-maintenance-action", b"ninjatrader-observer-repair-v1"))
            headers.append((b"x-beelzebub-maintenance-token", b"endpoint-fixture-token"))
        return Request({
            "type": "http", "http_version": "1.1", "method": "POST",
            "scheme": "http", "path": "/api/lane-iii/ninjatrader-maintenance",
            "raw_path": b"/api/lane-iii/ninjatrader-maintenance", "query_string": b"",
            "headers": headers, "client": ("127.0.0.1", 50000), "server": ("127.0.0.1", 8090),
        })

    async def test_status_is_read_only_and_action_accepts_only_request_id(self) -> None:
        self.assertEqual(await self.get_endpoint(), {"stage": "IDLE", "requests": []})
        result = await self.post_endpoint(self.request(), {"request_id": "ntm-api-test-0001"})
        self.assertEqual(result["request_id"], "ntm-api-test-0001")
        self.assertEqual(self.service.requests, ["ntm-api-test-0001"])
        with self.assertRaises(HTTPException) as refused:
            await self.post_endpoint(
                self.request(),
                {"request_id": "ntm-api-test-0002", "path": "C:\\untrusted.exe"},
            )
        self.assertEqual(refused.exception.status_code, 400)

    async def test_action_requires_local_origin_and_fixed_authentication_header(self) -> None:
        for request in (
            self.request(authenticated=False),
            self.request(origin="https://attacker.example"),
            self.request(host="attacker.example"),
        ):
            with self.subTest(headers=dict(request.headers)):
                with self.assertRaises(HTTPException) as refused:
                    await self.post_endpoint(request, {"request_id": "ntm-api-test-0003"})
                self.assertEqual(refused.exception.status_code, 403)
        self.assertEqual(self.service.requests, [])


if __name__ == "__main__":
    unittest.main()
