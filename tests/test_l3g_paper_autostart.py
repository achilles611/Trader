from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from fastapi import HTTPException
from starlette.requests import Request

from src.copytrade.config import CopyTradeConfig
from src.copytrade.control_center import create_control_center_app
from src.l3g_paper.paper_autostart import PaperAutoStartService


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
        "operational_paper_session": {"active": False},
        "ledger": {"highest_sequence": 125, "unverified_tail_rows": 0},
    }


def ready_maintenance() -> dict[str, object]:
    return {
        "stage": "READY",
        "in_progress": False,
        "readiness": "READY",
        "operation_kind": "NORMAL_STARTUP",
        "observer": {"attached": True, "subscription_mode": "NATIVE_ADDON", "instrument": "MNQ SEP26"},
        "actions": {"graceful_shutdowns": 0, "forced_shutdowns": 0},
    }


def passing_full_verification() -> dict[str, object]:
    return {
        "status": "PASS",
        "verification_id": "lv-full-test",
        "verification_mode": "full",
        "last_full_verification_id": "lv-full-test",
        "captured_tip_sequence": 125,
        "verified_through_sequence": 125,
        "chain_valid": True,
        "checkpoint_valid": True,
    }


class PaperAutoStartTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.paper = ready_paper()
        self.maintenance = ready_maintenance()
        self.verification = passing_full_verification()
        self.ensure_requests: list[str] = []
        self.full_starts = 0
        self.operational_requests: list[str] = []

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def service(self) -> PaperAutoStartService:
        def ensure(request_id: str) -> dict[str, object]:
            self.ensure_requests.append(request_id)
            return deepcopy(self.maintenance)

        def full() -> dict[str, object]:
            self.full_starts += 1
            return deepcopy(self.verification)

        def operational(request_id: str) -> dict[str, object]:
            self.operational_requests.append(request_id)
            self.paper["operational_paper_session"] = {"active": True}
            return {"started": True, "state": "PAPER_RUNNING"}

        return PaperAutoStartService(
            paper_status=lambda: deepcopy(self.paper),
            ensure_ninjatrader=ensure,
            ninjatrader_status=lambda: deepcopy(self.maintenance),
            start_full_verification=full,
            ledger_status=lambda: deepcopy(self.verification),
            start_operational_paper=operational,
            audit_path=Path(self.temporary.name) / "paper-autostart.jsonl",
            startup_timeout_seconds=0.2,
            ledger_timeout_seconds=0.2,
            poll_seconds=0.001,
        )

    def test_one_request_composes_normal_start_full_proof_and_operational_start(self) -> None:
        service = self.service()
        started = service.start("paper-auto-test-0001")
        self.assertIsNotNone(started["operation_id"])
        service.wait(2)
        status = service.status()
        self.assertEqual(status["stage"], "RUNNING")
        self.assertEqual(self.ensure_requests, ["paper-auto-test-0001"])
        self.assertEqual(self.full_starts, 1)
        self.assertEqual(self.operational_requests, ["paper-auto-test-0001"])
        self.assertEqual(status["authority"], "PERSISTENT_PAPER_SIM101_ONLY")
        audit = Path(self.temporary.name, "paper-autostart.jsonl")
        records = [json.loads(line) for line in audit.read_text(encoding="utf-8").splitlines()]
        self.assertTrue(any(record["event"] == "OPERATION_STARTED" for record in records))
        self.assertTrue(any(record["stage"] == "RUNNING" for record in records))

    def test_duplicate_request_reuses_operation(self) -> None:
        service = self.service()
        first = service.start("paper-auto-test-0002")
        second = service.start("paper-auto-test-0002")
        service.wait(2)
        self.assertEqual(first["operation_id"], second["operation_id"])
        self.assertEqual(self.ensure_requests, ["paper-auto-test-0002"])
        self.assertEqual(self.operational_requests, ["paper-auto-test-0002"])

    def test_nonflat_state_blocks_before_desktop_or_verifier_action(self) -> None:
        self.paper.update({"current_position": "LONG", "current_quantity": 1})
        service = self.service()
        status = service.start("paper-auto-test-0003")
        self.assertEqual(status["stage"], "BLOCKED")
        self.assertIn("POSITION_NOT_FLAT", status["blockers"])
        self.assertEqual(self.ensure_requests, [])
        self.assertEqual(self.full_starts, 0)
        self.assertEqual(self.operational_requests, [])

    def test_native_observer_or_reconciliation_failure_blocks_before_full_scan(self) -> None:
        self.maintenance = {
            **self.maintenance,
            "stage": "BLOCKED",
            "readiness": "BLOCKED",
            "blockers": ["AUTOMATIC_OBSERVER_NOT_VERIFIED"],
        }
        service = self.service()
        service.start("paper-auto-test-0004")
        service.wait(2)
        status = service.status()
        self.assertEqual(status["stage"], "BLOCKED")
        self.assertEqual(status["blockers"], ["AUTOMATIC_OBSERVER_NOT_VERIFIED"])
        self.assertEqual(self.full_starts, 0)
        self.assertEqual(self.operational_requests, [])

    def test_full_chain_or_checkpoint_failure_never_starts_paper(self) -> None:
        self.verification.update({"status": "FAIL", "chain_valid": False, "checkpoint_valid": False})
        service = self.service()
        service.start("paper-auto-test-0005")
        service.wait(2)
        status = service.status()
        self.assertEqual(status["stage"], "BLOCKED")
        self.assertIn("FULL_LEDGER_VERIFICATION_NOT_PASS", status["blockers"])
        self.assertIn("LEDGER_CHAIN_OR_CHECKPOINT_INVALID", status["blockers"])
        self.assertEqual(self.operational_requests, [])

    def test_verifier_must_cover_its_captured_tip(self) -> None:
        self.verification["verified_through_sequence"] = 124
        service = self.service()
        service.start("paper-auto-test-0006")
        service.wait(2)
        status = service.status()
        self.assertEqual(status["stage"], "BLOCKED")
        self.assertIn("FULL_LEDGER_CAPTURED_TIP_NOT_VERIFIED", status["blockers"])
        self.assertEqual(self.operational_requests, [])

    def test_existing_incremental_scan_completes_before_required_full_scan(self) -> None:
        incremental = {
            **passing_full_verification(),
            "verification_id": "lv-incremental-test",
            "verification_mode": "incremental",
        }
        reports = [incremental, passing_full_verification()]
        starts: list[str] = []

        def start_full() -> dict[str, object]:
            starts.append(reports[0]["verification_id"])
            return deepcopy(reports[0])

        def ledger_status() -> dict[str, object]:
            result = deepcopy(reports[0])
            if len(starts) == 1 and reports[0]["verification_mode"] == "incremental":
                reports.pop(0)
            return result

        service = PaperAutoStartService(
            paper_status=lambda: deepcopy(self.paper),
            ensure_ninjatrader=lambda request_id: deepcopy(self.maintenance),
            ninjatrader_status=lambda: deepcopy(self.maintenance),
            start_full_verification=start_full,
            ledger_status=ledger_status,
            start_operational_paper=lambda request_id: (
                self.paper.__setitem__("operational_paper_session", {"active": True})
                or {"started": True}
            ),
            audit_path=Path(self.temporary.name) / "paper-autostart-existing-verifier.jsonl",
            startup_timeout_seconds=0.2,
            ledger_timeout_seconds=0.2,
            poll_seconds=0.001,
        )
        service.start("paper-auto-test-0007")
        service.wait(2)
        self.assertEqual(service.status()["stage"], "RUNNING")
        self.assertEqual(starts, ["lv-incremental-test", "lv-full-test"])

    def test_stopped_operational_session_returns_to_idle_display_state(self) -> None:
        service = self.service()
        service.start("paper-auto-test-0008")
        service.wait(2)
        self.paper["operational_paper_session"] = {"active": False}
        status = service.status()
        self.assertEqual(status["stage"], "IDLE")
        self.assertEqual(status["button"]["label"], "Start Paper Trading")


class PaperAutoStartEndpointTests(unittest.IsolatedAsyncioTestCase):
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
        self.service = self.app.state.paper_autostart
        self.get_endpoint = next(
            route.endpoint for route in self.app.routes
            if route.path == "/api/lane-iii/paper/auto-start" and "GET" in route.methods
        )
        self.post_endpoint = next(
            route.endpoint for route in self.app.routes
            if route.path == "/api/lane-iii/paper/auto-start" and "POST" in route.methods
        )

    def tearDown(self) -> None:
        self.service.stop()
        self.temporary.cleanup()

    def request(self, *, host: str = "127.0.0.1:8090", authenticated: bool = True) -> Request:
        headers = [(b"host", host.encode())]
        if authenticated:
            headers.extend([
                (b"x-beelzebub-paper-autostart-action", b"sim101-paper-autostart-v1"),
                (b"x-beelzebub-paper-autostart-token", self.service.action_token.encode()),
            ])
        return Request({
            "type": "http", "http_version": "1.1", "method": "POST",
            "scheme": "http", "path": "/api/lane-iii/paper/auto-start",
            "raw_path": b"/api/lane-iii/paper/auto-start", "query_string": b"",
            "headers": headers, "client": ("127.0.0.1", 50000), "server": ("127.0.0.1", 8090),
        })

    async def test_status_is_read_only_and_action_accepts_only_request_id(self) -> None:
        status = await self.get_endpoint()
        self.assertEqual(status["schema"], "lane-iii-paper-autostart-v1")
        result = await self.post_endpoint(self.request(), {"request_id": "paper-auto-api-0001"})
        self.assertEqual(result["request_id"], "paper-auto-api-0001")
        self.assertEqual(result["stage"], "BLOCKED")
        with self.assertRaises(HTTPException) as refused:
            await self.post_endpoint(self.request(), {"request_id": "paper-auto-api-0002", "path": "untrusted"})
        self.assertEqual(refused.exception.status_code, 400)

    async def test_action_requires_loopback_and_fixed_session_authentication(self) -> None:
        for request in (
            self.request(host="example.com"),
            self.request(authenticated=False),
        ):
            with self.subTest(host=request.url.hostname):
                with self.assertRaises(HTTPException) as refused:
                    await self.post_endpoint(request, {"request_id": "paper-auto-api-0003"})
                self.assertEqual(refused.exception.status_code, 403)


if __name__ == "__main__":
    unittest.main()
