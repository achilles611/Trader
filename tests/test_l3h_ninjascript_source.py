from __future__ import annotations

from pathlib import Path
import unittest


class L3HNinjaScriptSourceTests(unittest.TestCase):
    def test_live_addon_is_a_separate_fail_closed_artifact(self) -> None:
        source = (Path(__file__).resolve().parents[1] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubLiveExecutionAddOn.cs").read_text(encoding="utf-8")
        for required in (
            "class BeelzebubLiveExecutionAddOn", "lane-iii-phase-h-live-execution-v1", "l3h-live-addon-protocol-v1",
            "private const int Port = 48137", "l3h.execution.local.key", "BZ-L3H-", "armed = false",
            "DENY_BAD_SIGNATURE", "DENY_REPLAY", "DENY_WRONG_ACCOUNT", "DENY_WRONG_CONTRACT", "DENY_QTY",
            "DENY_POSITION_NONFLAT", "DENY_FOREIGN_ORDER", "DENY_PROTECTION_UNAVAILABLE", "DENY_NOT_ARMED", "DENY_DAILY_LOSS",
            "NativeKillFlattenDisarm", "account.Flatten", "StopMarket", "CONTROL_HEARTBEAT_LOST",
        ):
            self.assertIn(required, source)
        self.assertNotIn("BeelzebubPaperExecutionAddOn", source)
        self.assertNotIn("l3g.paper.local.key", source)


if __name__ == "__main__":
    unittest.main()
