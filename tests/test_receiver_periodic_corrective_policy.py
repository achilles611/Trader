from __future__ import annotations

import unittest

from src.l3g_paper.contracts import (
    PONG_ENTRY_PROFILE_VERSION,
    paper_policy_for_topology,
    resolve_paper_profile,
)
from src.l3g_paper.policy import ExperimentalPaperPolicy
from src.l3g_paper.pong_contract import DEC26_TOPOLOGY
from tests.test_l3g_pong_inputs import verified


def pong_policy() -> ExperimentalPaperPolicy:
    profile = resolve_paper_profile(PONG_ENTRY_PROFILE_VERSION)
    return ExperimentalPaperPolicy(
        paper_policy_for_topology(profile.policy, DEC26_TOPOLOGY)
    )


def quote(sequence: int, observed: str = "2026-09-11T12:30:00Z"):
    return verified("QUOTE", {
        "contract_id": DEC26_TOPOLOGY.native_instrument,
        "bid": 100, "ask": 101, "bid_size": 4, "ask_size": 5,
        "source_event_sequence": sequence,
        "publication_policy": "EVENT_DRIVEN_EVERY_BID_ASK_CALLBACK_V1",
    }, local_sequence=sequence, observed=observed)


class ReceiverPeriodicCorrectivePolicyTests(unittest.TestCase):
    def test_state_only_ingest_reuses_exact_continuity_and_suppression_state(self) -> None:
        ordinary = pong_policy()
        state_only = pong_policy()

        for observation in (
            quote(1),
            quote(3, "2026-09-11T12:30:00.002Z"),
        ):
            decision = ordinary.ingest_runtime(observation)
            reason = state_only.ingest_pong_runtime_state(observation)
            self.assertIsNotNone(decision)
            self.assertEqual(reason, decision.reason_code)

        ordinary_status = ordinary.status()
        state_only_status = state_only.status()
        ordinary_status.pop("last_paper_decision")
        state_only_status.pop("last_paper_decision")
        self.assertEqual(state_only_status, ordinary_status)
        self.assertIsNone(state_only.status()["last_paper_decision"])
        self.assertEqual(state_only.status()["counters"]["local_sequence_gaps"], 1)

    def test_state_only_api_refuses_non_pong_and_control_frames(self) -> None:
        profile = resolve_paper_profile(None)
        non_pong = ExperimentalPaperPolicy(profile.policy)
        with self.assertRaisesRegex(ValueError, "exact Pong"):
            non_pong.ingest_pong_runtime_state(quote(1))


if __name__ == "__main__":
    unittest.main()
