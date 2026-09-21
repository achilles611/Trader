from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.l3g_paper.contracts import (
    PONG_ENTRY_PROFILE_VERSION,
    paper_policy_for_topology,
    paper_risk_for_topology,
    resolve_paper_profile,
)
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.pong_contract import DEC26_TOPOLOGY
from tests.test_receiver_periodic_corrective_policy import quote


def pong_ledger(path: Path, *, persist: bool = False) -> PaperLedger:
    profile = resolve_paper_profile(PONG_ENTRY_PROFILE_VERSION)
    return PaperLedger(
        path,
        policy=paper_policy_for_topology(profile.policy, DEC26_TOPOLOGY),
        risk=paper_risk_for_topology(profile.risk, DEC26_TOPOLOGY),
        persist_high_frequency_records=persist,
    )


class ReceiverPeriodicCorrectiveLedgerTests(unittest.TestCase):
    def test_exact_suppression_skips_prepare_and_returns_atomic_capacity(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = pong_ledger(Path(directory) / "paper.sqlite3")
            try:
                with patch.object(ledger, "_prepare", side_effect=AssertionError("prepare called")):
                    receipt = ledger.admit_suppressed_passive_market_observation(quote(1))
                self.assertIsNotNone(receipt)
                self.assertEqual(receipt["persistence_action"], "SUPPRESSED")
                self.assertEqual(receipt["suppressed_high_frequency_records_total"], 1)
                self.assertEqual(
                    receipt["suppressed_high_frequency_records_by_domain"]["OBSERVATION"], 1,
                )
                self.assertEqual(ledger.deferred_capacity()["queue_depth"], 0)
            finally:
                ledger.close()

    def test_suppression_refuses_persistence_or_unverified_identity(self) -> None:
        with TemporaryDirectory() as directory:
            persistent = pong_ledger(Path(directory) / "persistent.sqlite3", persist=True)
            ordinary = pong_ledger(Path(directory) / "ordinary.sqlite3")
            try:
                observation = quote(1)
                self.assertIsNone(
                    persistent.admit_suppressed_passive_market_observation(observation)
                )
                object.__setattr__(observation, "signature_verified", None)
                self.assertIsNone(
                    ordinary.admit_suppressed_passive_market_observation(observation)
                )
                self.assertEqual(
                    ordinary.deferred_capacity()["suppressed_high_frequency_records_total"], 0,
                )
            finally:
                persistent.close()
                ordinary.close()


if __name__ == "__main__":
    unittest.main()
