from __future__ import annotations

from pathlib import Path
import unittest


class L3HNinjaScriptSourceTests(unittest.TestCase):
    def test_live_addon_is_a_separate_fail_closed_artifact(self) -> None:
        source = (Path(__file__).resolve().parents[1] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubLiveExecutionAddOn.cs").read_text(encoding="utf-8")
        for required in (
            "class BeelzebubLiveExecutionAddOn", "lane-iii-phase-h-live-execution-v1", "l3h-live-addon-protocol-v1",
            "private const int Port = 48137", "l3h.execution.local.key", "BZ-L3H-", "mechanicallyArmed = false",
            "DENY_BAD_SIGNATURE", "DENY_REPLAY", "DENY_WRONG_ACCOUNT", "DENY_WRONG_CONTRACT", "DENY_QTY",
            "DENY_POSITION_NONFLAT", "DENY_FOREIGN_ORDER", "DENY_PROTECTION_UNAVAILABLE", "DENY_NOT_ARMED", "DENY_DAILY_LOSS",
            "NativeKillFlattenDisarm", "account.Flatten", "StopMarket", "CONTROL_HEARTBEAT_LOST",
            "Environment.SpecialFolder.LocalApplicationData", "keys\", \"l3h.execution.local.key",
            "L3H_UNKNOWN_STATE_TRANSPORT_LOSS", "unknown_state",
            "IsNativeKillFlattenOrder", "String.Equals(order.Name, \"Close\"",
            "L3H_KILL_LATE_PROTECTIVE_CANCEL", "CurrentQuantity() == 0",
            "CurrentPositionState", "MarketPosition.Short", "IsNativeKillFlattenOrder(entry)",
            "KILL_LATCH_POSITION_RETRY", "killLatch && CurrentQuantity() != 0",
            "ValidateAndConsumeLiveAuthorization", "lane-iii-phase-h-live-admission-v1",
            "DENY_LIVE_REQUIRES_ONE_SHOT_AUTHORIZATION", "consumedLiveAuthorizations",
            "authorizationSessionId", "gatewaySessionId", "NativeAccountFingerprint",
            "Provider.Simulator", "Options.IsDemo", "Options.CanManageOrders", "liveSendCount",
            "exposureGuardActive", "DENY_LIVE_ATOMIC_FACTS_CHANGED", "RefreshExposureGuard",
        ):
            self.assertIn(required, source)
        self.assertNotIn("private bool armed", source)
        self.assertNotIn("BeelzebubPaperExecutionAddOn", source)
        self.assertNotIn("l3g.paper.local.key", source)


if __name__ == "__main__":
    unittest.main()
