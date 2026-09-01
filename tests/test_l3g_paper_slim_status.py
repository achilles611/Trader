from __future__ import annotations

from datetime import datetime, timezone
import unittest

from src.l3g_paper.slim_status import derive_slim_paper_status


NOW = datetime(2026, 9, 1, 14, 0, tzinfo=timezone.utc)


def verification(*, status: str = "PASS") -> dict[str, object]:
    return {
        "status": status,
        "chain_valid": status == "PASS",
        "checkpoint_valid": status == "PASS",
        "full_scan_required": False,
        "quick_check": "ok",
        "completed_at": NOW.isoformat().replace("+00:00", "Z"),
    }


def runtime(*, state: str = "READY_DISARMED") -> dict[str, object]:
    positioned = state in {"LONG", "SHORT"}
    return {
        "mode": "PAPER_SIM101",
        "state": state,
        "paper_execution": "POSITIONED" if positioned else "DISARMED",
        "paper_account": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "market_instrument": "MNQ SEP26",
        "maximum_quantity": 1,
        "live_capital": "DENIED",
        "current_position": state if positioned else "FLAT",
        "current_quantity": 1 if positioned else 0,
        "current_position_quantity": 1 if positioned else 0,
        "broker_snapshot_position": state if positioned else "FLAT",
        "broker_snapshot_position_quantity": 1 if positioned else 0,
        "working_entry_orders": 0,
        "protective_stop_state": "WORKING" if positioned else "NONE",
        "position_snapshot_complete": True,
        "order_snapshot_complete": True,
        "reconciliation_current": True,
        "unresolved_command": False,
        "unresolved_native_order": False,
        "unresolved_execution": False,
        "lockout_or_fault_reason": None,
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
        "ledger": {
            "commissioning_ledger_state": "VERIFIED_TO_CURRENT_TIP",
            "writer_capacity_healthy": True,
        },
        "paper_session_pnl": {"realized": "12.50", "unrealized": "-2.25"},
    }


def operational_runtime(*, online_append_integrity: bool = True) -> dict[str, object]:
    value = runtime(state="PAPER_RUNNING")
    value["paper_execution"] = "RUNNING"
    value["operational_paper_session"] = {
        "active": True,
        "stopping": False,
        "request_id": "operational-start-001",
    }
    value["ledger"] = {
        **value["ledger"],
        "operational_ledger": {
            "active": True,
            "online_append_integrity": online_append_integrity,
            "tail_state": (
                "LEGITIMATE_AUTHORITY_MUTATION_TAIL_AWAITING_BATCH_VERIFICATION"
                if online_append_integrity else "UNTRUSTED_AUTHORITY_MUTATION_TAIL"
            ),
        },
    }
    return value


OBSERVER = {"market_observer_active": True, "market_observer_state": "ACTIVE"}


class SlimPaperStatusTests(unittest.TestCase):
    def status(
        self,
        paper: dict[str, object] | None = None,
        proof: dict[str, object] | None = None,
        verifier: dict[str, object] | None = None,
        observer: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return derive_slim_paper_status(
            paper if paper is not None else runtime(),
            verifier if verifier is not None else verification(),
            observer if observer is not None else OBSERVER,
            proof if proof is not None else {"result": "READY", "blocking_reasons": []},
            now=NOW,
        )

    def test_green_requires_canonical_ready_rehearsal(self) -> None:
        result = self.status()
        self.assertEqual(result["light"], "GREEN")
        self.assertTrue(result["can_start"])
        self.assertFalse(result["paper_active"])
        self.assertEqual(result["pnl"]["total"], "10.25")

    def test_representative_blocked_or_unknown_states_are_red(self) -> None:
        cases = {
            "unavailable": (None, None),
            "addon mismatch": (runtime(), {"result": "BLOCKED", "blocking_reasons": ["ADDON_BUILD_MISMATCH"]}),
            "stale quote": (runtime(), {"result": "BLOCKED", "blocking_reasons": ["QUOTE_STALE"]}),
            "lockout": (runtime(state="LOCKED_OUT"), {"result": "BLOCKED", "blocking_reasons": ["STATE_NOT_READY_DISARMED"]}),
            "unknown identity": (runtime(), {"result": "BLOCKED", "blocking_reasons": ["COMMISSIONING_ACCOUNT_INSTRUMENT_MISMATCH"]}),
        }
        for name, (paper, proof) in cases.items():
            with self.subTest(name=name):
                result = derive_slim_paper_status(
                    paper, verification(), OBSERVER, proof, now=NOW,
                )
                self.assertEqual(result["light"], "RED")
                self.assertFalse(result["can_start"])

    def test_known_connecting_verifying_reconciling_and_warmup_states_are_yellow(self) -> None:
        cases = {
            "connecting": (runtime(state="STARTING"), verification(), {"result": "BLOCKED", "blocking_reasons": ["STATE_NOT_READY_DISARMED"]}),
            "reconciling": (runtime(state="RECONCILING"), verification(), {"result": "BLOCKED", "blocking_reasons": ["STATE_NOT_READY_DISARMED"]}),
            "verifying": (runtime(), verification(status="IN_PROGRESS"), {"result": "BLOCKED", "blocking_reasons": ["COMMISSIONING_LEDGER_VERIFICATION_IN_PROGRESS"]}),
            "warming": (runtime(), verification(), {"result": "BLOCKED", "blocking_reasons": ["COMMISSIONING_SESSION_NOT_WARMED"]}),
        }
        for name, (paper, verifier, proof) in cases.items():
            with self.subTest(name=name):
                result = self.status(paper, proof, verifier)
                self.assertEqual(result["light"], "YELLOW")
                self.assertFalse(result["can_start"])

    def test_healthy_active_paper_operation_is_green_but_cannot_start_again(self) -> None:
        result = self.status(runtime(state="LONG"), {"result": "BLOCKED", "blocking_reasons": ["STATE_NOT_READY_DISARMED"]})
        self.assertEqual(result["light"], "GREEN")
        self.assertEqual(result["label"], "PAPER TRADING ACTIVE")
        self.assertTrue(result["paper_active"])
        self.assertFalse(result["can_start"])

    def test_operational_session_allows_a_healthy_online_append_tail_but_rejects_corruption(self) -> None:
        healthy = self.status(
            operational_runtime(),
            {"result": "BLOCKED", "blocking_reasons": ["STATE_NOT_READY_DISARMED"]},
        )
        self.assertEqual(healthy["light"], "GREEN")
        self.assertTrue(healthy["paper_active"])
        self.assertEqual(healthy["label"], "PAPER TRADING ACTIVE")
        self.assertFalse(healthy["can_start"])

        corrupt = self.status(
            operational_runtime(online_append_integrity=False),
            {"result": "BLOCKED", "blocking_reasons": ["STATE_NOT_READY_DISARMED"]},
        )
        self.assertEqual(corrupt["light"], "RED")
        self.assertTrue(corrupt["paper_active"])
        self.assertEqual(corrupt["primary_blocker"], "OPERATIONAL_LEDGER_INTEGRITY_FAILED")

    def test_unhealthy_active_paper_operation_fails_closed(self) -> None:
        paper = runtime(state="SHORT")
        paper["protective_stop_state"] = "PENDING"
        result = self.status(paper, {"result": "BLOCKED", "blocking_reasons": ["STATE_NOT_READY_DISARMED"]})
        self.assertEqual(result["light"], "RED")
        self.assertEqual(result["primary_blocker"], "PROTECTIVE_STOP_REJECTED")

    def test_missing_or_stale_pnl_never_becomes_zero(self) -> None:
        missing = runtime()
        missing["paper_session_pnl"] = {}
        stale = runtime()
        stale["reconciliation_current"] = False
        for name, paper, expected in (("missing", missing, "MISSING"), ("stale", stale, "STALE")):
            with self.subTest(name=name):
                result = self.status(paper)
                self.assertEqual(result["pnl"]["state"], expected)
                self.assertIsNone(result["pnl"]["total"])
                self.assertEqual(result["light"], "RED")
                self.assertFalse(result["can_start"])
        active_missing = runtime(state="LONG")
        active_missing["paper_session_pnl"] = {}
        active = self.status(
            active_missing,
            {"result": "BLOCKED", "blocking_reasons": ["STATE_NOT_READY_DISARMED"]},
        )
        self.assertEqual(active["light"], "RED")
        self.assertEqual(active["primary_blocker"], "PAPER_SESSION_PNL_UNAVAILABLE")

    def test_failed_verification_exposes_only_one_concise_reason(self) -> None:
        failed = verification(status="FAIL")
        failed["errors"] = [{"message": "checkpoint signature is invalid"}, {"message": "must not be shown"}]
        result = self.status(runtime(), {"result": "BLOCKED", "blocking_reasons": ["COMMISSIONING_LEDGER_VERIFICATION_FAILED"]}, failed)
        self.assertEqual(result["light"], "RED")
        self.assertEqual(result["ledger_verification"]["message"], "Verification failed: checkpoint signature is invalid")


if __name__ == "__main__":
    unittest.main()
