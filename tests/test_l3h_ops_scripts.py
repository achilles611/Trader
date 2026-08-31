from __future__ import annotations

from pathlib import Path
import unittest


class L3HOperationsScriptTests(unittest.TestCase):
    def test_safe_operator_tooling_exists_and_does_not_auto_arm(self) -> None:
        root = Path(__file__).resolve().parents[1] / "scripts"
        required = {
            "bootstrap-l3h.ps1", "l3h_bootstrap.ps1", "l3h_status.ps1", "l3h_deploy_ninjatrader.ps1",
            "l3h_verify_install.ps1", "l3h_kill.ps1", "l3h_recover.ps1", "l3h_audit.ps1", "l3h_prepare_sim101.py", "l3h_sim101_commission.py", "l3h_gateway_service.py",
            "l3h3_live_authorization_commission.py",
        }
        self.assertTrue(all((root / name).is_file() for name in required))
        bootstrap = (root / "l3h_bootstrap.ps1").read_text(encoding="utf-8")
        status = (root / "l3h_status.ps1").read_text(encoding="utf-8")
        kill = (root / "l3h_kill.ps1").read_text(encoding="utf-8")
        self.assertIn("l3h.execution.local.key", bootstrap)
        self.assertIn("live_armed = $false", bootstrap)
        self.assertIn("live_armed = $false", status)
        self.assertIn("Global\\BeelzebubL3HNativeKill", kill)
        self.assertNotIn("ENTER_LONG", bootstrap + status)
        provision = (root / "l3h_prepare_sim101.py").read_text(encoding="utf-8")
        self.assertIn("AccountClass.LOCAL_SIMULATION", provision)
        self.assertIn("live_capital=False", provision)
        harness = (root / "l3h_sim101_commission.py").read_text(encoding="utf-8")
        self.assertIn("LOCAL_SIMULATION", harness)
        self.assertIn("live_capital", harness)
        gateway_service = (root / "l3h_gateway_service.py").read_text(encoding="utf-8")
        self.assertIn("live_capital", gateway_service)
        self.assertNotIn("dispatch(", gateway_service)
        l3h3 = (root / "l3h3_live_authorization_commission.py").read_text(encoding="utf-8")
        self.assertIn('"live_authority": "DISARMED"', l3h3)
        self.assertIn('"live_canary": "NOT_RUN"', l3h3)
        self.assertIn('"live_send_count": 0', l3h3)
        self.assertNotIn(".dispatch(", l3h3)


if __name__ == "__main__":
    unittest.main()
