from __future__ import annotations

from pathlib import Path
import unittest


class L3HOperationsScriptTests(unittest.TestCase):
    def test_safe_operator_tooling_exists_and_does_not_auto_arm(self) -> None:
        root = Path(__file__).resolve().parents[1] / "scripts"
        required = {
            "bootstrap-l3h.ps1", "l3h_bootstrap.ps1", "l3h_status.ps1", "l3h_deploy_ninjatrader.ps1",
            "l3h_verify_install.ps1", "l3h_kill.ps1", "l3h_recover.ps1", "l3h_audit.ps1",
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


if __name__ == "__main__":
    unittest.main()
