from __future__ import annotations

import unittest

from src.copytrade.control_center import _qualifying_market_fast_path_identity
from src.l3g_paper.contracts import PONG_ENTRY_PROFILE_VERSION
from src.l3g_paper.pong_contract import DEC26_TOPOLOGY
from src.l3g_paper.pong_qualifying import (
    PONG_NO_FHB_QUALIFYING_ARCHITECTURE,
    PONG_QUALIFYING_INTERVAL_MS,
    PONG_QUALIFYING_SCHEDULING_MODE,
)


class ReceiverPeriodicCorrectiveDispatchTests(unittest.TestCase):
    def test_only_exact_disarmed_qualifying_identity_selects_fast_path(self) -> None:
        values = {
            "architecture": PONG_NO_FHB_QUALIFYING_ARCHITECTURE,
            "selected_profile": PONG_ENTRY_PROFILE_VERSION,
            "paper_topology_version": DEC26_TOPOLOGY.version,
            "pong_topology_version": DEC26_TOPOLOGY.version,
            "scheduling_mode": PONG_QUALIFYING_SCHEDULING_MODE,
            "interval_ms": PONG_QUALIFYING_INTERVAL_MS,
            "activation_authorized": False,
        }
        self.assertEqual(_qualifying_market_fast_path_identity(**values), values)
        for field, replacement in (
            ("architecture", "OTHER"),
            ("selected_profile", "OTHER"),
            ("paper_topology_version", "OTHER"),
            ("pong_topology_version", "OTHER"),
            ("scheduling_mode", "EVENT_DRIVEN"),
            ("interval_ms", 21),
            ("activation_authorized", True),
        ):
            changed = {**values, field: replacement}
            self.assertIsNone(_qualifying_market_fast_path_identity(**changed), field)


if __name__ == "__main__":
    unittest.main()
