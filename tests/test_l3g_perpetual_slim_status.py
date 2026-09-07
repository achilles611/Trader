from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import unittest

from src.l3g_paper.contracts import FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION
from src.l3g_paper.slim_status import derive_slim_paper_status


NOW = datetime(2026, 9, 6, 18, 0, tzinfo=timezone.utc)
OBSERVER = {"market_observer_active": True, "market_observer_state": "ACTIVE"}
READINESS = {"result": "BLOCKED", "blocking_reasons": ["STATE_NOT_READY_DISARMED"]}


def verification() -> dict[str, object]:
    return {
        "status": "PASS",
        "chain_valid": True,
        "checkpoint_valid": True,
        "full_scan_required": False,
        "quick_check": "ok",
        "completed_at": NOW.isoformat().replace("+00:00", "Z"),
    }


def positioned_runtime(direction: str = "LONG") -> dict[str, object]:
    return {
        "mode": "PAPER_SIM101",
        "state": direction,
        "paper_execution": "POSITIONED",
        "paper_account": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "market_instrument": "MNQ SEP26",
        "maximum_quantity": 1,
        "live_capital": "DENIED",
        "entry_profile_version": FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        "current_position": direction,
        "current_quantity": 1,
        "current_position_quantity": 1,
        "broker_snapshot_position": direction,
        "broker_snapshot_position_quantity": 1,
        "working_owned_orders": 1,
        "working_entry_orders": 0,
        "foreign_activity": False,
        "protective_stop_state": "WORKING",
        "position_snapshot_complete": True,
        "order_snapshot_complete": True,
        "reconciliation_current": True,
        "unresolved_command": False,
        "unresolved_native_order": False,
        "unresolved_execution": False,
        "lockout_or_fault_reason": None,
        "position_requirement": {
            "required": True,
            "state": "POSITIONED",
            "actual_position": direction,
            "actual_quantity": 1,
            "desired_position": direction,
            "primary_blocker": None,
            "blocking_reasons": [],
            "source_signal": {
                "direction": direction,
                "candle_close_utc": "2026-09-06T17:55:00Z",
                "signal_hash": "a" * 64,
                "ledger_sequence": 101,
                "record_hash": "b" * 64,
                "ledger_verified": True,
            },
        },
        "continuity": {
            "healthy": True,
            "local_bridge_healthy": True,
            "market_price_connected": True,
        },
        "market_freshness": {
            "quote": {"fresh": True},
            "classified_trade": {"fresh": True},
            "depth_mutation": {"fresh": True},
        },
        "transport": {
            "state": "AUTHENTICATED",
            "authenticated_client": True,
            "reconciled": True,
            "addon_provenance": {"status": "MATCH"},
        },
        "operational_paper_session": {
            "active": True,
            "stopping": False,
            "request_id": "perpetual-test",
        },
        "ledger": {
            "operational_ledger": {
                "active": True,
                "online_append_integrity": True,
            },
        },
        "paper_session_pnl": {"realized": "0", "unrealized": "1.25"},
    }


def flat_runtime(reason: str = "NO_COMPLETED_FIVE_MINUTE_SIGNAL") -> dict[str, object]:
    value = positioned_runtime()
    value.update({
        "state": "PAPER_RUNNING",
        "paper_execution": "RUNNING",
        "current_position": "FLAT",
        "current_quantity": 0,
        "current_position_quantity": 0,
        "broker_snapshot_position": "FLAT",
        "broker_snapshot_position_quantity": 0,
        "working_owned_orders": 0,
        "protective_stop_state": "NONE",
        "position_requirement": {
            "required": True,
            "state": "BLOCKED_FLAT",
            "actual_position": "FLAT",
            "actual_quantity": 0,
            "desired_position": None,
            "primary_blocker": reason,
            "blocking_reasons": [reason],
            "source_signal": None,
        },
    })
    return value


def status(runtime: dict[str, object]) -> dict[str, object]:
    return derive_slim_paper_status(
        runtime, verification(), OBSERVER, READINESS, now=NOW,
    )


class PerpetualSlimStatusTests(unittest.TestCase):
    def test_aligned_long_and_short_are_green_only_with_durable_signal_proof(self) -> None:
        for direction in ("LONG", "SHORT"):
            with self.subTest(direction=direction):
                result = status(positioned_runtime(direction))
                self.assertEqual(result["light"], "GREEN")
                self.assertEqual(result["label"], "PAPER TRADING ACTIVE")
                self.assertIsNone(result["primary_blocker"])

    def test_flat_v2_renders_the_exact_runtime_blocker_in_red(self) -> None:
        result = status(flat_runtime("NO_COMPLETED_FIVE_MINUTE_SIGNAL"))

        self.assertEqual(result["light"], "RED")
        self.assertEqual(
            result["label"],
            "FLAT — BLOCKED: NO_COMPLETED_FIVE_MINUTE_SIGNAL",
        )
        self.assertEqual(result["primary_blocker"], "NO_COMPLETED_FIVE_MINUTE_SIGNAL")
        self.assertTrue(result["paper_active"])
        self.assertFalse(result["can_start"])

    def test_flat_pending_v2_is_still_truthfully_red_and_exact(self) -> None:
        paper = flat_runtime("ENTRY_PENDING")
        paper["state"] = "ENTRY_PENDING"
        paper["position_requirement"]["state"] = "ENTRY_PENDING"  # type: ignore[index]

        result = status(paper)

        self.assertEqual(result["light"], "RED")
        self.assertEqual(result["label"], "FLAT — BLOCKED: ENTRY_PENDING")

    def test_flat_v2_without_a_complete_runtime_reason_fails_closed(self) -> None:
        cases = ({}, {"required": False}, {"required": True, "actual_position": "FLAT"})
        for requirement in cases:
            with self.subTest(requirement=requirement):
                paper = flat_runtime()
                paper["position_requirement"] = requirement
                result = status(paper)
                self.assertEqual(result["light"], "RED")
                self.assertEqual(
                    result["label"],
                    "FLAT — BLOCKED: PERPETUAL_POSITION_REQUIREMENT_UNAVAILABLE",
                )

    def test_positioned_v2_rejects_requirement_position_and_activity_mismatches(self) -> None:
        cases: dict[str, tuple[str, object]] = {
            "requirement state": ("position_requirement.state", "BLOCKED_FLAT"),
            "requirement quantity": ("position_requirement.actual_quantity", 2),
            "desired direction": ("position_requirement.desired_position", "SHORT"),
            "signal direction": ("position_requirement.source_signal.direction", "SHORT"),
            "unverified signal": ("position_requirement.source_signal.ledger_verified", False),
            "runtime quantity": ("current_quantity", 2),
            "broker direction": ("broker_snapshot_position", "SHORT"),
            "broker quantity": ("broker_snapshot_position_quantity", 2),
            "duplicate owned work": ("working_owned_orders", 2),
            "owned protective missing": ("working_owned_orders", 0),
            "working entry": ("working_entry_orders", 1),
            "foreign activity": ("foreign_activity", True),
            "unresolved command": ("unresolved_command", True),
        }
        for name, (path, replacement) in cases.items():
            with self.subTest(name=name):
                paper = deepcopy(positioned_runtime())
                target: dict[str, object] = paper
                parts = path.split(".")
                for part in parts[:-1]:
                    target = target[part]  # type: ignore[assignment]
                target[parts[-1]] = replacement
                result = status(paper)
                self.assertEqual(result["light"], "RED")
                self.assertNotEqual(result["label"], "PAPER TRADING ACTIVE")

    def test_legacy_operational_flat_keeps_its_existing_green_semantics(self) -> None:
        paper = flat_runtime()
        paper["entry_profile_version"] = "BEELZEBUB_FIVE_MINUTE_BIAS_V1"
        del paper["position_requirement"]
        del paper["foreign_activity"]
        del paper["working_owned_orders"]

        result = status(paper)

        self.assertEqual(result["light"], "GREEN")
        self.assertEqual(result["label"], "PAPER TRADING ACTIVE")


if __name__ == "__main__":
    unittest.main()
