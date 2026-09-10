from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
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
        "paper_account": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "market_instrument": "MNQ SEP26",
        "maximum_quantity": 1,
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


def ready_operational_readiness() -> dict[str, object]:
    return {
        "schema": "lane-iii-phase-g-operational-paper-readiness-v1",
        "result": "READY",
        "blocking_reasons": [],
        "commissioning_warmup": {
            "status": "WARMED",
            "required_families": {
                family: {"seen": True, "provenance": {"evidence_id": f"evidence-{family}"}}
                for family in ("STRUCTURAL_CONTEXT", "ORDER_FLOW", "RESTING_LIQUIDITY")
            },
        },
        "strategy_evidence": {"status": "ACTIVE"},
    }


def warming_operational_readiness(*, continuity_gap: bool = False) -> dict[str, object]:
    reasons = ["COMMISSIONING_SESSION_NOT_WARMED", "PAPER_EVIDENCE_NOT_WARMED"]
    if continuity_gap:
        reasons.append("PAPER_CONTINUITY_UNUSABLE")
    return {
        "schema": "lane-iii-phase-g-operational-paper-readiness-v1",
        "result": "BLOCKED",
        "blocking_reasons": reasons,
        "session": {"current": True, "session_kind": "NEW_YORK_RTH"},
        "observer": {
            "status": "ACTIVE",
            "continuity_healthy": not continuity_gap,
            "local_bridge_healthy": True,
            "market_price_connected": True,
        },
        "continuity": {
            "local_sequence_gap": continuity_gap,
            "depth_reset_recovery": False,
            "recovery_condition": (
                "FRESH_POLICY_EVIDENCE_REWARM_REQUIRED" if continuity_gap else None
            ),
        },
        "market_freshness": {
            name: {"fresh": True} for name in ("quote", "classified_trade", "depth_mutation")
        },
        "commissioning_warmup": {
            "status": "NOT_WARMED",
            "required_families": {
                "STRUCTURAL_CONTEXT": {"seen": True, "provenance": {"evidence_id": "structural"}},
                "ORDER_FLOW": {"seen": True, "provenance": {"evidence_id": "flow"}},
                "RESTING_LIQUIDITY": {"seen": False, "provenance": None},
            },
        },
        "strategy_evidence": {"status": "INCOMPLETE"},
    }


class ManualClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def wait(self, seconds: float) -> bool:
        self.now += seconds
        return False


class PaperAutoStartTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.paper = ready_paper()
        self.maintenance = ready_maintenance()
        self.verification = passing_full_verification()
        self.readiness = ready_operational_readiness()
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
            operational_readiness=lambda: deepcopy(self.readiness),
            start_operational_paper=operational,
            begin_startup_observation_pause=lambda: {"paused": True, "drained": True},
            end_startup_observation_pause=lambda: {"paused": False},
            stop_operational_paper=lambda: (
                self.paper.__setitem__("operational_paper_session", {"active": False})
                or {"flat_confirmed": True}
            ),
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

    def test_account_instrument_and_quantity_boundary_cannot_be_widened(self) -> None:
        cases = (
            ("paper_account", "Lucid25kflex01", "PAPER_ACCOUNT_NOT_SIM101"),
            ("account_class", "LIVE", "ACCOUNT_NOT_LOCAL_SIMULATION"),
            ("market_instrument", "NQ SEP26", "INSTRUMENT_NOT_MNQ_SEP26"),
            ("maximum_quantity", 2, "MAXIMUM_QUANTITY_NOT_ONE"),
            ("live_capital", "ALLOWED", "LIVE_CAPITAL_NOT_DENIED"),
        )
        for index, (field, value, blocker) in enumerate(cases):
            with self.subTest(field=field):
                self.paper = ready_paper()
                self.paper[field] = value
                service = self.service()
                status = service.start(f"paper-auto-boundary-{index}")
                self.assertEqual(status["stage"], "BLOCKED")
                self.assertIn(blocker, status["blockers"])
                self.assertEqual(self.ensure_requests, [])
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
            operational_readiness=lambda: ready_operational_readiness(),
            start_operational_paper=lambda request_id: (
                self.paper.__setitem__("operational_paper_session", {"active": True})
                or {"started": True}
            ),
            begin_startup_observation_pause=lambda: {"paused": True, "drained": True},
            end_startup_observation_pause=lambda: {"paused": False},
            stop_operational_paper=lambda: {"flat_confirmed": True},
            audit_path=Path(self.temporary.name) / "paper-autostart-existing-verifier.jsonl",
            startup_timeout_seconds=0.2,
            ledger_timeout_seconds=0.2,
            poll_seconds=0.001,
        )
        service.start("paper-auto-test-0007")
        service.wait(2)
        self.assertEqual(service.status()["stage"], "RUNNING")
        self.assertEqual(starts, ["lv-incremental-test", "lv-full-test"])

    def test_delayed_resting_liquidity_waits_then_starts_once(self) -> None:
        self.paper["entry_profile_version"] = "BEELZEBUB_SCALPER_V2"
        clock = ManualClock()
        reports = [
            warming_operational_readiness(),
            warming_operational_readiness(),
            ready_operational_readiness(),
            ready_operational_readiness(),
        ]
        service = self.service()
        service._clock = clock
        service._wait = clock.wait
        service._custom_wait = clock.wait
        service._readiness_timeout_seconds = 10.0
        service._operational_readiness = lambda: deepcopy(
            reports.pop(0) if len(reports) > 1 else reports[0]
        )
        service.start("paper-auto-delayed-depth")
        service.wait(2)
        status = service.status()
        self.assertEqual(status["stage"], "RUNNING")
        self.assertEqual(self.operational_requests, ["paper-auto-delayed-depth"])
        self.assertEqual(status["warmup"]["covered_family_count"], 3)
        self.assertEqual(status["warmup"]["missing_families"], [])
        self.assertEqual(self.full_starts, 2)

    def test_authentic_continuity_rewarm_waits_then_starts_once(self) -> None:
        self.paper["entry_profile_version"] = "BEELZEBUB_SCALPER_V2"
        clock = ManualClock()
        reports = [
            warming_operational_readiness(continuity_gap=True),
            ready_operational_readiness(),
            ready_operational_readiness(),
        ]
        service = self.service()
        service._clock = clock
        service._wait = clock.wait
        service._custom_wait = clock.wait
        service._operational_readiness = lambda: deepcopy(
            reports.pop(0) if len(reports) > 1 else reports[0]
        )
        service.start("paper-auto-continuity-rewarm")
        service.wait(2)
        self.assertEqual(service.status()["stage"], "RUNNING")
        self.assertEqual(self.operational_requests, ["paper-auto-continuity-rewarm"])

    def test_unproven_continuity_recovery_is_hard_blocked(self) -> None:
        self.paper["entry_profile_version"] = "BEELZEBUB_SCALPER_V2"
        self.readiness = warming_operational_readiness(continuity_gap=True)
        self.readiness["continuity"]["recovery_condition"] = None
        service = self.service()
        service.start("paper-auto-unresolved-gap")
        service.wait(2)
        status = service.status()
        self.assertEqual(status["stage"], "BLOCKED")
        self.assertIn("SCALPER_CONTINUITY_RECOVERY_UNPROVEN", status["blockers"])
        self.assertEqual(self.operational_requests, [])

    def test_missing_evidence_times_out_with_progress_and_no_start(self) -> None:
        self.paper["entry_profile_version"] = "BEELZEBUB_SCALPER_V2"
        clock = ManualClock()
        self.readiness = warming_operational_readiness()
        service = self.service()
        service._clock = clock
        service._wait = clock.wait
        service._custom_wait = clock.wait
        service._readiness_timeout_seconds = 2.0
        service.start("paper-auto-warmup-timeout")
        service.wait(2)
        status = service.status()
        self.assertEqual(status["stage"], "BLOCKED")
        self.assertIn("SCALPER_EVIDENCE_WARMUP_TIMEOUT", status["blockers"])
        self.assertEqual(status["warmup"]["missing_families"], ["RESTING_LIQUIDITY"])
        self.assertGreaterEqual(status["warmup"]["elapsed_seconds"], 2.0)
        self.assertEqual(self.operational_requests, [])

    def test_hard_unknown_and_mixed_readiness_fail_without_start(self) -> None:
        cases = (
            ["DAILY_LOSS_ALLOWANCE_INSUFFICIENT"],
            ["UNKNOWN_GATE"],
            ["COMMISSIONING_SESSION_NOT_WARMED", "RECONCILIATION_INCOMPLETE"],
        )
        for index, reasons in enumerate(cases):
            with self.subTest(reasons=reasons):
                self.operational_requests.clear()
                self.paper["entry_profile_version"] = "BEELZEBUB_SCALPER_V2"
                self.readiness = warming_operational_readiness()
                self.readiness["blocking_reasons"] = reasons
                service = self.service()
                service.start(f"paper-auto-hard-block-{index}")
                service.wait(2)
                self.assertEqual(service.status()["stage"], "BLOCKED")
                self.assertEqual(self.operational_requests, [])

    def test_readiness_regression_immediately_before_start_never_calls_start(self) -> None:
        self.paper["entry_profile_version"] = "BEELZEBUB_SCALPER_V2"
        reports = [ready_operational_readiness(), warming_operational_readiness()]
        service = self.service()
        service._operational_readiness = lambda: deepcopy(reports.pop(0))
        service.start("paper-auto-readiness-regression")
        service.wait(2)
        status = service.status()
        self.assertEqual(status["stage"], "BLOCKED")
        self.assertIn("OPERATIONAL_READINESS_REGRESSED", status["blockers"])
        self.assertEqual(self.operational_requests, [])

    def test_expired_strategy_evidence_never_calls_start(self) -> None:
        self.paper["entry_profile_version"] = "BEELZEBUB_SCALPER_V2"
        self.readiness = ready_operational_readiness()
        self.readiness["strategy_evidence"] = {"status": "INCOMPLETE"}
        service = self.service()
        service.start("paper-auto-expired-strategy-evidence")
        service.wait(2)
        status = service.status()
        self.assertEqual(status["stage"], "BLOCKED")
        self.assertIn("SCALPER_READY_EVIDENCE_PROOF_INVALID", status["blockers"])
        self.assertEqual(self.operational_requests, [])

    def test_non_scalper_profile_preserves_existing_start_path(self) -> None:
        readiness_calls = 0

        def readiness() -> dict[str, object]:
            nonlocal readiness_calls
            readiness_calls += 1
            return warming_operational_readiness()

        service = self.service()
        service._operational_readiness = readiness
        service.start("paper-auto-non-scalper-path")
        service.wait(2)
        self.assertEqual(service.status()["stage"], "RUNNING")
        self.assertEqual(readiness_calls, 0)
        self.assertEqual(self.full_starts, 1)
        self.assertEqual(self.operational_requests, ["paper-auto-non-scalper-path"])

    def test_stop_during_final_readiness_cannot_reach_start(self) -> None:
        self.paper["entry_profile_version"] = "BEELZEBUB_SCALPER_V2"
        entered = threading.Event()
        release = threading.Event()
        readiness_calls = 0

        def readiness() -> dict[str, object]:
            nonlocal readiness_calls
            readiness_calls += 1
            if readiness_calls == 2:
                entered.set()
                self.assertTrue(release.wait(2))
            return ready_operational_readiness()

        service = self.service()
        service._operational_readiness = readiness
        service.start("paper-auto-cancel-final-readiness")
        self.assertTrue(entered.wait(2))
        service.stop(timeout_seconds=0.01)
        release.set()
        service.wait(2)
        self.assertEqual(service.status()["stage"], "CANCELLED")
        self.assertEqual(self.operational_requests, [])

    def test_requests_during_warmup_reuse_one_operation_and_one_start(self) -> None:
        self.paper["entry_profile_version"] = "BEELZEBUB_SCALPER_V2"
        entered = threading.Event()
        release = threading.Event()
        readiness_calls = 0

        def readiness() -> dict[str, object]:
            nonlocal readiness_calls
            readiness_calls += 1
            if readiness_calls == 1:
                entered.set()
                self.assertTrue(release.wait(2))
                return warming_operational_readiness()
            return ready_operational_readiness()

        service = self.service()
        service._operational_readiness = readiness
        first = service.start("paper-auto-warmup-idempotent")
        self.assertTrue(entered.wait(2))
        same = service.start("paper-auto-warmup-idempotent")
        different = service.start("paper-auto-warmup-different")
        self.assertEqual(first["operation_id"], same["operation_id"])
        self.assertEqual(first["operation_id"], different["operation_id"])
        release.set()
        service.wait(2)
        self.assertEqual(service.status()["stage"], "RUNNING")
        self.assertEqual(self.operational_requests, ["paper-auto-warmup-idempotent"])

    def test_alternating_waitable_reasons_share_one_fixed_deadline(self) -> None:
        self.paper["entry_profile_version"] = "BEELZEBUB_SCALPER_V2"
        clock = ManualClock()
        calls = 0

        def readiness() -> dict[str, object]:
            nonlocal calls
            calls += 1
            report = warming_operational_readiness(continuity_gap=calls % 2 == 0)
            return report

        service = self.service()
        service._clock = clock
        service._wait = clock.wait
        service._custom_wait = clock.wait
        service._readiness_timeout_seconds = 2.0
        service._operational_readiness = readiness
        service.start("paper-auto-one-fixed-deadline")
        service.wait(2)
        self.assertEqual(service.status()["stage"], "BLOCKED")
        self.assertIn("SCALPER_EVIDENCE_WARMUP_TIMEOUT", service.status()["blockers"])
        self.assertGreaterEqual(clock.now, 2.0)
        self.assertLess(clock.now, 2.01)
        self.assertEqual(self.operational_requests, [])

    def test_post_warmup_full_must_cover_fixed_current_tip(self) -> None:
        self.paper["entry_profile_version"] = "BEELZEBUB_SCALPER_V2"
        service = self.service()
        service._begin_startup_observation_pause = lambda: (
            self.paper["ledger"].__setitem__("highest_sequence", 126)
            or {"paused": True, "drained": True}
        )
        service.start("paper-auto-final-full-current-tip")
        service.wait(2)
        status = service.status()
        self.assertEqual(status["stage"], "BLOCKED")
        self.assertIn("FULL_LEDGER_VERIFICATION_NOT_CURRENT", status["blockers"])
        self.assertEqual(self.full_starts, 2)
        self.assertEqual(self.operational_requests, [])

    def test_tuple_and_nested_start_refusal_reasons_are_retained(self) -> None:
        service = self.service()
        service._start_operational_paper = lambda request_id: {
            "started": False,
            "reason_codes": ("PAPER_CONTINUITY_UNUSABLE",),
            "readiness": {
                "blocking_reasons": ["COMMISSIONING_SESSION_NOT_WARMED"],
            },
        }
        service.start("paper-auto-exact-refusal")
        service.wait(2)
        self.assertEqual(service.status()["blockers"], [
            "PAPER_CONTINUITY_UNUSABLE",
            "COMMISSIONING_SESSION_NOT_WARMED",
            "OPERATIONAL_PAPER_START_REFUSED",
        ])

    def test_stopped_operational_session_returns_to_idle_display_state(self) -> None:
        service = self.service()
        service.start("paper-auto-test-0008")
        service.wait(2)
        self.paper["operational_paper_session"] = {"active": False}
        status = service.status()
        self.assertEqual(status["stage"], "IDLE")
        self.assertEqual(status["button"]["label"], "Start Paper Trading")

    def test_perpetual_autostart_never_completes_while_flat(self) -> None:
        self.paper.update({
            "entry_profile_version": "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2",
            "position_requirement": {
                "state": "BLOCKED_FLAT",
                "primary_blocker": "MARKET_DATA_CONNECTION_LOST",
                "blocking_reasons": ["MARKET_DATA_CONNECTION_LOST"],
                "source_signal": None,
            },
        })
        service = self.service()
        service.start("paper-auto-perpetual-flat")
        service.wait(2)
        status = service.status()
        self.assertEqual(status["stage"], "BLOCKED")
        self.assertEqual(status["blockers"], ["MARKET_DATA_CONNECTION_LOST"])
        self.assertEqual(
            status["button"]["label"],
            "FLAT — BLOCKED: MARKET_DATA_CONNECTION_LOST",
        )

    def test_perpetual_autostart_completes_only_after_position_and_stop_proof(self) -> None:
        self.paper["entry_profile_version"] = "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2"

        def operational(request_id: str) -> dict[str, object]:
            self.operational_requests.append(request_id)
            self.paper.update({
                "operational_paper_session": {"active": True},
                "state": "SHORT",
                "paper_execution": "POSITIONED",
                "session_armed_state": "ARMED_PERPETUAL",
                "current_position": "SHORT",
                "current_quantity": 1,
                "current_position_quantity": 1,
                "broker_snapshot_position": "SHORT",
                "broker_snapshot_position_quantity": 1,
                "working_owned_orders": 1,
                "working_entry_orders": 0,
                "protective_stop_state": "WORKING",
                "foreign_activity": False,
                "position_requirement": {
                    "required": True,
                    "state": "POSITIONED",
                    "actual_position": "SHORT",
                    "actual_quantity": 1,
                    "desired_position": "SHORT",
                    "primary_blocker": None,
                    "blocking_reasons": [],
                    "source_signal": {
                        "direction": "SHORT",
                        "candle_close_utc": "2026-09-01T20:50:00Z",
                        "signal_hash": "signal-hash-short",
                        "ledger_sequence": 126,
                        "record_hash": "record-hash-short",
                        "ledger_verified": True,
                    },
                },
            })
            return {"started": True, "state": "SHORT"}

        service = self.service()
        service._start_operational_paper = operational
        service.start("paper-auto-perpetual-positioned")
        service.wait(2)
        status = service.status()
        self.assertEqual(status["stage"], "RUNNING")
        self.assertEqual(status["button"]["label"], "Paper Trading Running")

    def test_perpetual_autostart_rejects_position_against_wrong_latest_bias(self) -> None:
        self.paper["entry_profile_version"] = "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2"

        def operational(request_id: str) -> dict[str, object]:
            self.operational_requests.append(request_id)
            self.paper.update({
                "operational_paper_session": {"active": True},
                "state": "SHORT",
                "paper_execution": "POSITIONED",
                "session_armed_state": "ARMED_PERPETUAL",
                "current_position": "SHORT",
                "current_quantity": 1,
                "current_position_quantity": 1,
                "broker_snapshot_position": "SHORT",
                "broker_snapshot_position_quantity": 1,
                "working_owned_orders": 1,
                "working_entry_orders": 0,
                "protective_stop_state": "WORKING",
                "foreign_activity": False,
                "position_requirement": {
                    "required": True,
                    "state": "POSITIONED",
                    "actual_position": "SHORT",
                    "actual_quantity": 1,
                    "desired_position": "LONG",
                    "primary_blocker": None,
                    "blocking_reasons": [],
                    "source_signal": {
                        "direction": "LONG",
                        "candle_close_utc": "2026-09-01T20:50:00Z",
                        "signal_hash": "signal-hash-long",
                        "ledger_sequence": 126,
                        "record_hash": "record-hash-long",
                        "ledger_verified": True,
                    },
                },
            })
            return {"started": True, "state": "SHORT"}

        service = self.service()
        service._start_operational_paper = operational
        service.start("paper-auto-perpetual-wrong-bias")
        service.wait(2)
        status = service.status()
        self.assertEqual(status["stage"], "BLOCKED")
        self.assertEqual(status["blockers"], ["PERPETUAL_POSITION_NOT_PROVEN"])
        self.assertEqual(
            status["button"]["label"],
            "POSITION UNPROVEN — BLOCKED: PERPETUAL_POSITION_NOT_PROVEN",
        )


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
