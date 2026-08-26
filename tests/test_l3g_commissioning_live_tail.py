from __future__ import annotations

from datetime import datetime, timezone
from itertools import chain, repeat
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.l3g_paper.commissioning import CommissioningLedgerGateError, evaluate_commissioning_ledger_gate
from src.l3g_paper.health import ledger_health_projection
from src.l3g_paper.ledger import (
    COMMISSIONING_NO_AUTHORITY_EFFECT,
    PaperLedger,
    is_commissioning_safe_unverified_tail_record,
)


NOW = "2026-08-26T17:30:00Z"


def observation(number: int = 1, kind: str = "QUOTE") -> dict[str, object]:
    return {
        "observation_id": f"nt-passive-{number}",
        "observation_type": kind,
        "local_monotonic_sequence": number,
        "source_payload_hash": f"hash-{number}",
    }


def informational_account_observation(number: int = 1) -> dict[str, object]:
    return {
        **observation(number, "ACCOUNT"),
        "authority_effect": COMMISSIONING_NO_AUTHORITY_EFFECT,
        "observation_semantics": "INFORMATIONAL_ACCOUNT_ITEM",
        "observation_payload_keys": ["item", "value"],
        "observation_account_alias": "Sim101",
        "observation_account_class": "LOCAL_SIMULATION",
    }


def evidence(number: int = 1) -> dict[str, object]:
    return {
        "evidence_id": f"l3g-pe-{number}",
        "family": "ORDER_FLOW",
        "scientific_eligibility": False,
        "book_completeness": "UNVERIFIED",
        "sequence_authority": "LOCAL_CALLBACK_ORDER_ONLY",
    }


def decision(number: int = 1, value: str = "NO_TRADE") -> dict[str, object]:
    return {
        "paper_decision_id": f"l3g-pd-{number}",
        "decision": value,
        "direction": {"NO_TRADE": "FLAT", "LONG": "LONG", "SHORT": "SHORT", "EXIT": "FLAT"}[value],
        "authority_effect": COMMISSIONING_NO_AUTHORITY_EFFECT,
        "commissioning": False,
        "strategy_generated": True,
        "scientific_evidence": False,
        "scientific_eligibility": False,
    }


def runtime_snapshot() -> dict[str, object]:
    return {
        "commissioning_id": "l3g-commissioning-test",
        "account": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "instrument": "MNQ SEP26",
        "current_position": "FLAT",
        "current_position_quantity": 0,
        "broker_snapshot_position": "FLAT",
        "broker_snapshot_position_quantity": 0,
        "working_owned_orders": 0,
        "working_entry_orders": 0,
        "position_snapshot_complete": True,
        "order_snapshot_complete": True,
        "reconciliation_current": True,
        "unresolved_command": False,
        "unresolved_native_order": False,
        "unresolved_execution": False,
        "entry_owner": "NONE",
        "commissioning_ownership_active": False,
        "live_capital": "DENIED",
        "runtime_state": "READY_DISARMED",
        "session_kind": "NEW_YORK_RTH",
        "session_family": "NEW_YORK",
        "session_id": "MNQU6:NEW_YORK_RTH:2026-08-26",
        "trade_date": "2026-08-26",
        "session_profile_hash": "profile",
        "session_generation": 1,
        "transport": {
            "reconciled": True,
            "account": "Sim101",
            "account_class": "LOCAL_SIMULATION",
            "instrument": "MNQ SEP26",
            "live_capital": "DENIED",
        },
    }


class CommissioningLiveTailTests(unittest.TestCase):
    def prepared_gate(self, ledger: PaperLedger, anchor: int) -> tuple[dict[str, object], dict[str, object]]:
        tail = ledger.commissioning_tail_snapshot(anchor, last_full_verified_sequence=anchor)
        anchor_hash = tail["verified_anchor_record_hash"]
        verification = {
            "status": "PASS",
            "chain_valid": True,
            "checkpoint_valid": True,
            "full_scan_required": False,
            "quick_check": "inherited_from_full",
            "completed_at": NOW,
            "verification_id": "lv-incremental-test",
            "verified_through_sequence": anchor,
            "tip_hash": anchor_hash,
            "last_full_verified_sequence": anchor,
            "last_full_verified_hash": anchor_hash,
            "last_full_verification_id": "lv-full-test",
            "last_full_quick_check_at": NOW,
            "ledger_identity": tail["ledger_identity"],
            "ledger_epoch": tail["ledger_epoch"],
            "ledger_schema_version": tail["ledger_schema_version"],
        }
        return verification, tail

    def evaluate(self, ledger: PaperLedger, anchor: int) -> dict[str, object]:
        verification, tail = self.prepared_gate(ledger, anchor)
        return evaluate_commissioning_ledger_gate(
            verification,
            tail,
            runtime_snapshot(),
            checkpoint_matches_report=True,
            freshness_seconds=900,
            now=datetime.fromisoformat(NOW.replace("Z", "+00:00")),
        )

    def test_passive_observation_evidence_and_explicit_no_effect_decision_tail_is_accepted(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            ledger.append("SESSION_AUTHORITY", {"reason": "verified anchor"})
            anchor = int(ledger.health_status()["highest_sequence"])
            for number, kind in enumerate(("QUOTE", "TRADE", "DEPTH"), start=1):
                ledger.append("OBSERVATION_ENVELOPE", observation(number, kind))
            ledger.append("OBSERVATION_ENVELOPE", informational_account_observation(4))
            ledger.append("EVIDENCE", evidence())
            for number, value in enumerate(("NO_TRADE", "LONG", "SHORT", "EXIT"), start=1):
                ledger.append("DECISION", decision(number, value))
            result = self.evaluate(ledger, anchor)
            self.assertEqual(result["ledger_trust_state"], "VERIFIED_ANCHOR_WITH_PASSIVE_LIVE_TAIL")
            self.assertEqual(result["last_authority_mutation_sequence"], anchor)
            self.assertEqual(result["unverified_tail_rows"], 9)
            self.assertEqual(result["tail_authority_classification"], "PASSIVE_ONLY")
            self.assertIn("OBSERVATION:OBSERVATION_ENVELOPE:QUOTE", result["tail_record_kinds"])
            self.assertIn(
                "OBSERVATION:OBSERVATION_ENVELOPE:ACCOUNT_ITEM_INFORMATIONAL:AUTHORITY_EFFECT_NONE",
                result["tail_record_kinds"],
            )
            ledger.close()

    def test_unknown_decision_order_execution_risk_ownership_and_incident_tails_deny(self) -> None:
        forbidden = (
            ("DECISION", {**decision(), "authority_effect": "UNKNOWN"}),
            ("ORDER_EVENT_ACCEPTED", {"order_id": "order-1"}),
            ("EXECUTION_FILL", {"execution_id": "execution-1"}),
            ("RISK_EVENT_MUTATION", {"risk_authority": "changed"}),
            ("COMMISSIONING_OWNERSHIP_RESERVED", {"commissioning_id": "commissioning-1"}),
            ("INCIDENT_TRANSPORT_AMBIGUITY", {"reason": "UNKNOWN"}),
        )
        for kind, payload in forbidden:
            with self.subTest(kind=kind), TemporaryDirectory() as directory:
                ledger = PaperLedger(Path(directory) / "paper.sqlite3")
                ledger.append("SESSION_AUTHORITY", {"reason": "verified anchor"})
                anchor = int(ledger.health_status()["highest_sequence"])
                ledger.append(kind, payload)
                with self.assertRaises(CommissioningLedgerGateError) as raised:
                    self.evaluate(ledger, anchor)
                self.assertEqual(raised.exception.code, "COMMISSIONING_LEDGER_TAIL_UNTRUSTED")
                self.assertTrue(raised.exception.launch_auto)
                ledger.close()

    def test_account_or_order_observation_is_not_blanket_allowed_by_observation_domain(self) -> None:
        for payload in (observation(1, "ORDER"), observation(2, "ACCOUNT")):
            with self.subTest(observation_type=payload["observation_type"]):
                record = {
                    "domain": "OBSERVATION",
                    "kind": "OBSERVATION_ENVELOPE",
                    "payload": payload,
                }
                self.assertFalse(is_commissioning_safe_unverified_tail_record(record))

    def test_mixed_fifty_thousand_passive_one_forbidden_fifty_thousand_passive_denies(self) -> None:
        passive = {"domain": "OBSERVATION", "kind": "OBSERVATION_ENVELOPE", "payload": observation()}
        forbidden = {"domain": "ORDER_EVENT", "kind": "ORDER_EVENT_ACCEPTED", "payload": {"order_id": "one"}}
        records = chain(repeat(passive, 50_000), (forbidden,), repeat(passive, 50_000))
        self.assertEqual(sum(not is_commissioning_safe_unverified_tail_record(record) for record in records), 1)

    def test_authority_watermark_survives_passive_rows_after_a_forbidden_mixed_tail_and_restart(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            ledger.append("SESSION_AUTHORITY", {"reason": "verified anchor"})
            anchor = int(ledger.health_status()["highest_sequence"])
            for number in range(50):
                ledger.append("OBSERVATION_ENVELOPE", observation(number + 1))
            ledger.append("ORDER_EVENT_ACCEPTED", {"order_id": "one"})
            forbidden_sequence = int(ledger.health_status()["highest_sequence"])
            for number in range(50, 100):
                ledger.append("OBSERVATION_ENVELOPE", observation(number + 1))
            snapshot = ledger.commissioning_tail_snapshot(anchor, last_full_verified_sequence=anchor)
            self.assertEqual(snapshot["last_authority_mutation_sequence"], forbidden_sequence)
            ledger.close()
            reopened = PaperLedger(path)
            snapshot = reopened.commissioning_tail_snapshot(anchor, last_full_verified_sequence=anchor)
            self.assertEqual(snapshot["last_authority_mutation_sequence"], forbidden_sequence)
            with self.assertRaises(CommissioningLedgerGateError) as raised:
                self.evaluate(reopened, anchor)
            self.assertEqual(raised.exception.code, "COMMISSIONING_LEDGER_TAIL_UNTRUSTED")
            reopened.close()

    def test_stale_anchor_and_broker_reconciliation_mismatch_deny(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            ledger.append("SESSION_AUTHORITY", {"reason": "verified anchor"})
            anchor = int(ledger.health_status()["highest_sequence"])
            verification, tail = self.prepared_gate(ledger, anchor)
            verification["completed_at"] = "2026-08-26T16:00:00Z"
            with self.assertRaises(CommissioningLedgerGateError) as stale:
                evaluate_commissioning_ledger_gate(
                    verification,
                    tail,
                    runtime_snapshot(),
                    checkpoint_matches_report=True,
                    freshness_seconds=900,
                    now=datetime.fromisoformat(NOW.replace("Z", "+00:00")),
                )
            self.assertEqual(stale.exception.code, "COMMISSIONING_LEDGER_ANCHOR_STALE")
            verification["completed_at"] = NOW
            mismatched = {**runtime_snapshot(), "current_position_quantity": 1}
            with self.assertRaises(CommissioningLedgerGateError) as broker:
                evaluate_commissioning_ledger_gate(
                    verification,
                    tail,
                    mismatched,
                    checkpoint_matches_report=True,
                    freshness_seconds=900,
                    now=datetime.now(timezone.utc).replace(year=2026, month=8, day=26, hour=17, minute=30, second=0, microsecond=0),
                )
            self.assertEqual(broker.exception.code, "COMMISSIONING_RUNTIME_NOT_RECONCILED")
            ledger.close()

    def test_health_projection_preserves_verified_anchor_and_authority_tail_distinction(self) -> None:
        verification = {"status": "PASS", "chain_valid": True, "verified_through_sequence": 100}
        passive = ledger_health_projection(
            {
                "highest_sequence": 150,
                "authority_watermark": {
                    "last_authority_mutation_sequence": 99,
                    "classified_through_sequence": 150,
                },
            },
            verification,
        )
        self.assertEqual(passive["commissioning_ledger_state"], "VERIFIED_ANCHOR_WITH_PASSIVE_LIVE_TAIL")
        authority = ledger_health_projection(
            {
                "highest_sequence": 150,
                "authority_watermark": {
                    "last_authority_mutation_sequence": 125,
                    "classified_through_sequence": 150,
                },
            },
            verification,
        )
        self.assertEqual(authority["commissioning_ledger_state"], "UNVERIFIED_AUTHORITY_TAIL")


if __name__ == "__main__":
    unittest.main()
