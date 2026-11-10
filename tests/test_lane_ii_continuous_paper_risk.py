from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from scripts.lane_ii_continuous import paper_risk as risk


def proposal(identity="p13-m", profile="5m-P13", market="m", **changes):
    return {"reservation_id": identity, "profile": profile, "market": market, "side": "Up", "signal_wall": 1000.,
            "quantity": 9, "fee_rate": .2, "maximum_allin": 5 if profile == "5m-P13" else 6,
            "basis": 4.59, "entry_fee": .09, "quote_reference": {"file": "http-1.bin", "sha256": "a" * 64}, **changes}


def initialized(root):
    anchor = risk.initialize_campaign(root / "campaign.sqlite", root / "ANCHOR.json", campaign_id="fixture", roster_sha256="r" * 64)
    ledger = risk.RiskLedger(anchor["path"], anchor["sha256"], roster_sha256="r" * 64)
    ledger.begin_segment("short")
    return anchor, ledger


def entry(ledger, payload, prefix):
    ledger.apply(prefix + "reserve", "RESERVE_INTENT", payload)
    ledger.apply(prefix + "prepare", "PREPARE_ENTRY", payload)
    data = {key: payload[key] for key in ("profile", "market", "side", "quantity", "basis", "entry_fee", "quote_reference")}
    ledger.apply(prefix + "entry", "COMMIT_ENTRY", {"reservation_id": payload["reservation_id"], "financial": data})


class PaperRiskTests(unittest.TestCase):
    def test_both_profiles_share_one_cap_and_reject_before_intent(self):
        with tempfile.TemporaryDirectory() as tmp:
            anchor, ledger = initialized(Path(tmp))
            try:
                first = proposal(quantity=100, fee_rate=.4)  # 5 principal/all-in +10 exit bound.
                self.assertTrue(ledger.apply("one", "RESERVE_INTENT", first)["accepted"])
                second = proposal("p2-m", "5m-P2", quantity=100, fee_rate=.4)
                result = ledger.apply("two", "RESERVE_INTENT", second)
                self.assertFalse(result["accepted"])
                self.assertEqual(result["reason"], "SHARED_PAPER_RISK_CAP")
                self.assertEqual(risk.decimal(risk.snapshot(ledger.state)["consumed_or_reserved"]), 15)
                self.assertNotIn("p2-m", ledger.state["reservations"])
            finally:
                ledger.close()

    def test_gross_losses_and_fees_never_refill_from_profit(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, ledger = initialized(Path(tmp))
            try:
                for index, gross in enumerate((3.6, 6.3)):
                    p = proposal("lot" + str(index), market="m" + str(index))
                    entry(ledger, p, str(index))
                    proceeds = gross - .15
                    financial = {"profile": p["profile"], "market": p["market"], "side": "Up", "exit_fee": .15,
                                 "proceeds": proceeds, "net": proceeds - p["basis"], "fills": [{"quantity": 9}]}
                    ledger.apply(str(index) + "exit", "EXIT", {"reservation_id": p["reservation_id"], "financial": financial})
                result = risk.snapshot(ledger.state)
                self.assertEqual(risk.decimal(result["gross_losses"]), risk.decimal(".9"))
                self.assertEqual(risk.decimal(result["paid_fees"]), risk.decimal(".48"))
                self.assertEqual(risk.decimal(result["remaining"]), risk.decimal("23.62"))
                self.assertFalse(result["profits_refill_consumed_losses_or_fees"])
            finally:
                ledger.close()

    def test_unknown_reservation_survives_reopen_and_blocks_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            anchor, ledger = initialized(Path(tmp))
            p = proposal()
            ledger.apply("intent", "RESERVE_INTENT", p)
            ledger.apply("prepare", "PREPARE_ENTRY", p)
            ledger.close()
            reopened = risk.RiskLedger(anchor["path"], anchor["sha256"], roster_sha256="r" * 64)
            try:
                self.assertEqual(risk.snapshot(reopened.state)["unknown_reservations"], [p["reservation_id"]])
                with self.assertRaisesRegex(ValueError, "CONTINUITY_UNKNOWN_OR_OPEN"):
                    reopened.begin_segment("fresh", "GENESIS")
                with self.assertRaisesRegex(ValueError, "AMBIGUOUS"):
                    risk.transition(reopened.state, "CANCEL_INTENT", {"reservation_id": p["reservation_id"]})
            finally:
                reopened.close()

    def test_duplicate_facts_are_idempotent_and_changed_retries_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, ledger = initialized(Path(tmp))
            try:
                payload = proposal()
                result = ledger.apply("intent", "RESERVE_INTENT", payload)
                before = deepcopy(ledger.state)
                self.assertEqual(result, ledger.apply("intent", "RESERVE_INTENT", payload))
                self.assertEqual(before, ledger.state)
                with self.assertRaisesRegex(ValueError, "IDEMPOTENCY_CONTENT_CHANGED"):
                    ledger.apply("intent", "RESERVE_INTENT", {**payload, "quantity": 8})
            finally:
                ledger.close()

    def test_missing_expected_database_does_not_initialize_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            anchor, ledger = initialized(root)
            ledger.close()
            (root / "campaign.sqlite").unlink()
            with self.assertRaises(FileNotFoundError):
                risk.RiskLedger(anchor["path"], anchor["sha256"], roster_sha256="r" * 64)
            self.assertFalse((root / "campaign.sqlite").exists())

    def test_concurrent_writer_cannot_observe_same_remaining_dollars(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, ledger = initialized(Path(tmp))
            other = sqlite3.connect(ledger.path, timeout=.05, isolation_level=None)
            try:
                other.execute("BEGIN IMMEDIATE")
                ledger.connection.execute("PRAGMA busy_timeout=50")
                with self.assertRaises(sqlite3.OperationalError):
                    ledger.apply("lock", "RESERVE_INTENT", proposal())
                self.assertEqual(ledger.state["reservations"], {})
            finally:
                other.rollback();other.close();ledger.close()

    def test_exact_exit_fee_bound_and_proven_settlement(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, ledger = initialized(Path(tmp))
            try:
                p = proposal()
                entry(ledger, p, "s")
                wrong = {"profile": p["profile"], "market": "m", "side": "Up", "exit_fee": .45001,
                         "proceeds": 1., "net": -3.59, "fills": [{"quantity": 9}]}
                with self.assertRaisesRegex(ValueError, "FEE_EXCEEDS"):
                    ledger.apply("wrong", "EXIT", {"reservation_id": p["reservation_id"], "financial": wrong})
                settlement = {"profile": p["profile"], "market": "m", "side": "Up", "payout": 0, "net": -4.59,
                              "raw_references": [{"file": "gamma"}, {"file": "clob"}]}
                ledger.apply("settled", "SETTLEMENT", {"reservation_id": p["reservation_id"], "financial": settlement})
                self.assertEqual(risk.decimal(risk.snapshot(ledger.state)["consumed_or_reserved"]), risk.decimal("4.59"))
            finally:
                ledger.close()

    def test_terminal_closure_and_cash_carry_are_bound_without_budget_reset(self):
        from scripts.lane_ii_continuous.decision_capture import account_carryover, restore_account
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            anchor, ledger = initialized(root)
            profile = SimpleNamespace(id="5m-P13", initial_cash=3000., realized=-1.2, entries=1, exits=1, settlements=0, fees=.2, positions={})
            carry = account_carryover([profile])
            capture = root / "run/capture"
            capture.mkdir(parents=True)
            receipt = {"source_manifest_sha256": "s" * 64, "state": "COMPLETE", "actual_end_utc": "2026-11-08T00:10:00Z", "cutoff_utc": "2026-11-08T00:10:00Z",
                       "paper_risk_terminal_state": deepcopy(ledger.state), "paper_risk": risk.snapshot(ledger.state), "account_carryover": carry}
            (capture / "receipt.json").write_bytes(risk.canonical(receipt))
            ledger.finish_segment(capture / "receipt.json", carry)
            closure = {"schema": "LANE_II_PAPER_RISK_CLOSURE_V1", "receipt": ledger.state["last_terminal"], "state": ledger.state,
                       "snapshot": risk.snapshot(ledger.state), "anchor_sha256": anchor["sha256"]}
            (capture.parent / "RISK_CLOSURE.json").write_bytes(risk.canonical(closure))
            self.assertTrue(risk.validate_closure(capture.parent, anchor["sha256"])["flat"])
            previous = ledger.state["last_terminal"]["receipt_sha256"]
            ledger.close()
            ledger = risk.RiskLedger(anchor["path"], anchor["sha256"], roster_sha256="r" * 64)
            try:
                with self.assertRaisesRegex(ValueError, "PREDECESSOR_RECEIPT"):
                    ledger.begin_segment("eight", "GENESIS")
                ledger.begin_segment("eight", previous)
                restored = SimpleNamespace(initial_cash=3000., realized=0., entries=0, exits=0, settlements=0, fees=0., positions={})
                restore_account(restored, carry["5m-P13"])
                self.assertEqual(restored.initial_cash, 2998.8)
                self.assertEqual(restored.realized, 0)
                self.assertEqual(restored.entries, 0)
                self.assertEqual(carry["5m-P13"]["cumulative_entries"], 1)
            finally:
                ledger.close()


if __name__ == "__main__":
    unittest.main()
