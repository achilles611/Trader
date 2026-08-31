from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.copytrade.config import CopyTradeConfig
from src.copytrade.control_center import create_control_center_app


class L3HControlCenterTests(unittest.TestCase):
    def test_live_status_is_read_only_and_fail_closed_without_a_capability(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            defaults = CopyTradeConfig()
            config = replace(
                defaults,
                storage=replace(defaults.storage, cold_root=root / "cold"),
                artifacts=replace(defaults.artifacts, database_path=root / "hot" / "copytrade.sqlite3"),
            )
            app = create_control_center_app(config)
            endpoint = next(route.endpoint for route in app.routes if route.path == "/api/lane-iii/live")
            status = asyncio.run(endpoint())
        self.assertEqual(status["terminal_status"], "BLOCKED_CAPABILITY_MISSING")
        self.assertEqual(status["live_capital"], "DENIED")
        self.assertFalse(status["one_control_start"]["enabled"])
        self.assertFalse(status["emergency_control"]["enabled"])
        self.assertEqual(status["components"]["LIVE_AUTHORITY"], {"state": "RED", "reason": "DISARMED_FAIL_CLOSED"})
        self.assertEqual(set(status["components"]), {
            "ACCOUNT", "ACCOUNT_CLASS", "CONTRACT", "SESSION", "MARKET_DATA", "ACCOUNT_TRUTH", "POSITION_TRUTH",
            "ORDER_TRUTH", "EXECUTION_GATEWAY", "NT_RISK_GUARD", "PROTECTION", "RECONCILIATION", "LEDGER",
            "CAPABILITY", "KILL_PATHS", "DISK", "STRATEGY", "LIVE_AUTHORITY",
        })
        self.assertFalse(any(route.path == "/api/lane-iii/live/activate" for route in app.routes))


if __name__ == "__main__":
    unittest.main()
