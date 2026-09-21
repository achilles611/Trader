from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation
from src.l3g_paper.contracts import PaperRuntimeState
from src.l3g_paper.pong_qualifying import (
    PONG_NO_FHB_QUALIFYING_ARCHITECTURE,
    PONG_QUALIFYING_INTERVAL_MS,
    PONG_QUALIFYING_SCHEDULING_MODE,
)
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from tests.test_receiver_periodic_corrective_ledger import pong_ledger
from tests.test_receiver_periodic_corrective_policy import quote


FAST_IDENTITY = {
    "architecture": PONG_NO_FHB_QUALIFYING_ARCHITECTURE,
    "selected_profile": "BEELZEBUB_PONG_DISCERNMENT_V1",
    "paper_topology_version": "PONG_CONTRACT_TOPOLOGY_DEC26_V1",
    "pong_topology_version": "PONG_CONTRACT_TOPOLOGY_DEC26_V1",
    "scheduling_mode": PONG_QUALIFYING_SCHEDULING_MODE,
    "interval_ms": PONG_QUALIFYING_INTERVAL_MS,
    "activation_authorized": False,
}


class ReceiverPeriodicCorrectiveRuntimeTests(unittest.TestCase):
    def test_fast_path_preserves_snapshot_warmup_authority_and_policy_state(self) -> None:
        with TemporaryDirectory() as directory:
            full_ledger = pong_ledger(Path(directory) / "full.sqlite3")
            fast_ledger = pong_ledger(Path(directory) / "fast.sqlite3")
            full = LaneIIIPaperRuntime(full_ledger)
            fast = LaneIIIPaperRuntime(
                fast_ledger, qualifying_market_fast_path=FAST_IDENTITY,
            )
            try:
                full._deferred_admission_receipt_healthy_locked = lambda _receipt: True  # type: ignore[method-assign]
                fast._deferred_admission_receipt_healthy_locked = lambda _receipt: True  # type: ignore[method-assign]
                for observation in (
                    quote(1),
                    quote(3, "2026-09-11T12:30:00.002Z"),
                ):
                    full.ingest(observation)
                    fast.ingest_qualifying_disarmed_pong_market(observation)

                self.assertEqual(fast._snapshot, full._snapshot)
                self.assertEqual(fast._last_quote, full._last_quote)
                self.assertEqual(
                    fast._commissioning_warmup_seen, full._commissioning_warmup_seen,
                )
                self.assertEqual(
                    fast._commissioning_authority_epoch,
                    full._commissioning_authority_epoch,
                )
                full_policy = full.policy.status()
                fast_policy = fast.policy.status()
                full_policy.pop("last_paper_decision")
                fast_policy.pop("last_paper_decision")
                self.assertEqual(fast_policy, full_policy)
                projection = fast.pong_controller_snapshot()["qualifying_market_fast_path"]
                self.assertEqual(projection["records"], 2)
                self.assertEqual(projection["full_fallback_records"], 0)
            finally:
                full_ledger.close()
                fast_ledger.close()

    def test_state_drift_and_persistence_use_full_ingest(self) -> None:
        with TemporaryDirectory() as directory:
            drift_ledger = pong_ledger(Path(directory) / "drift.sqlite3")
            persistent_ledger = pong_ledger(
                Path(directory) / "persistent.sqlite3", persist=True,
            )
            drift = LaneIIIPaperRuntime(
                drift_ledger, qualifying_market_fast_path=FAST_IDENTITY,
            )
            persistent = LaneIIIPaperRuntime(
                persistent_ledger, qualifying_market_fast_path=FAST_IDENTITY,
            )
            try:
                drift._deferred_admission_receipt_healthy_locked = lambda _receipt: True  # type: ignore[method-assign]
                persistent_fallbacks = []
                persistent.ingest = persistent_fallbacks.append  # type: ignore[method-assign]
                drift._state = PaperRuntimeState.PAPER_RUNNING
                drift.ingest_qualifying_disarmed_pong_market(quote(1))
                persistent_observation = quote(1)
                persistent.ingest_qualifying_disarmed_pong_market(persistent_observation)
                drift_projection = drift.pong_controller_snapshot()[
                    "qualifying_market_fast_path"
                ]
                persistent_projection = persistent.pong_controller_snapshot()[
                    "qualifying_market_fast_path"
                ]
                self.assertEqual(drift_projection["records"], 0)
                self.assertEqual(
                    drift_projection["full_fallback_reasons"]["RUNTIME_STATE_DRIFT"], 1,
                )
                self.assertEqual(
                    persistent_projection["full_fallback_reasons"][
                        "LEDGER_POLICY_REQUIRES_FULL_INGEST"
                    ], 1,
                )
                self.assertEqual(drift.policy.status()["counters"]["quotes"], 1)
                self.assertEqual(persistent_fallbacks, [persistent_observation])
            finally:
                drift_ledger.close()
                persistent_ledger.close()

    def test_control_and_backward_time_always_take_full_ingest(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = pong_ledger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(
                ledger, qualifying_market_fast_path=FAST_IDENTITY,
            )
            try:
                fallbacks = []
                runtime.ingest = fallbacks.append  # type: ignore[method-assign]
                control = NinjaTraderObservation(
                    "control-1", "native-session", "HEALTH",
                    "2026-09-11T12:30:00Z", 1, {"state": "HEALTHY"},
                )
                runtime.ingest_qualifying_disarmed_pong_market(control)
                runtime._session_resolver.resolve("2026-09-11T12:30:01Z")
                backward = quote(2, "2026-09-11T12:30:00.500Z")
                runtime.ingest_qualifying_disarmed_pong_market(backward)
                self.assertEqual(fallbacks, [control, backward])
                projection = runtime.pong_controller_snapshot()[
                    "qualifying_market_fast_path"
                ]
                self.assertEqual(
                    projection["full_fallback_reasons"]["CONTROL_OR_NON_MARKET"], 1,
                )
                self.assertEqual(
                    projection["full_fallback_reasons"][
                        "EVENT_TIMESTAMP_MOVED_BACKWARD"
                    ], 1,
                )
            finally:
                ledger.close()


if __name__ == "__main__":
    unittest.main()
