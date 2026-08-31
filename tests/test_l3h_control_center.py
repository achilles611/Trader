from __future__ import annotations

import asyncio
from dataclasses import replace
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.copytrade.config import CopyTradeConfig
from src.copytrade.control_center import create_control_center_app
from src.l3h_live.status import fail_closed_status


class L3HControlCenterTests(unittest.TestCase):
    def test_mechanical_commissioning_and_l3h3_boundary_status_remain_separate_and_disarmed(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            gateway_path = root / "l3h-gateway-status.json"
            gateway_path.write_text(json.dumps({
                "schema": "l3h-gateway-status-v1", "account_class": "LOCAL_SIMULATION",
                "live_capital": "DENIED", "live_armed": False,
                "gateway": {"state": "AUTHENTICATED", "loopback_only": True, "port": 48137},
                "reconciliation": {"account": "Sim101", "contract": "MNQ SEP26", "position": "FLAT", "quantity": 0,
                                   "owned_working_orders": 0, "foreign_or_unknown_orders": 0},
            }), encoding="utf-8")
            (root / "l3h-sim101-mechanical-results.json").write_text(json.dumps({"stages": {
                "probe": {"runtime_hello": "PASS", "gateway_auth": "PASS"},
                "restart-proof": {"restart": "PASS"}, "reconnect": {"reconnect": "PASS"},
                "negative": {
                    "bad_signature": {"reason": "DENY_BAD_SIGNATURE"}, "replay": {"reason": "DENY_REPLAY"},
                    "duplicate": {"reason": "DUPLICATE_COMMAND_NOOP"}, "wrong_contract": {"reason": "DENY_WRONG_CONTRACT"},
                    "qty_2_reject": {"reason": "DENY_QTY"},
                },
                "long-kill-command": {"long": {"protection": "PASS"}, "command_kill": {"reconciliation": {"owned_working_orders": 0}}},
                "short-await-menu-kill": {"native_menu_kill": "PASS"}, "long-await-script-kill": {"script_kill": "PASS"},
                "unknown-transport": {"unknown_state": "PASS"}, "foreign-await": {"foreign_activity": "PASS"},
            }}), encoding="utf-8")
            authorization_path = root / "l3h3-live-authorization-status.json"
            authorization_path.write_text(json.dumps({
                "schema": "lane-iii-phase-h3-commissioning-result-v1", "terminal_status": "BLOCKED_LIVE_ACCOUNT_IDENTITY",
                "live_authority": "DISARMED", "live_canary": "NOT_RUN", "live_send_count": 0,
                "maximum_quantity": 1, "contract": "MNQ SEP26", "live_account_identity": "UNVERIFIED",
                "authorization_boundary": "IMPLEMENTED", "account_class": "UNKNOWN", "authorized_account": None,
                "addon_provenance": "L3H3_SOURCE_IMPLEMENTED_RUNTIME_UNVERIFIED",
                "preflight_age_seconds": None, "authorization_expires_at": None, "quarantine": False, "locked": False,
            }), encoding="utf-8")
            status = fail_closed_status(mechanical_status_path=gateway_path, authorization_status_path=authorization_path)
        self.assertEqual(status["mechanical_commissioning"], "COMMISSIONED")
        self.assertEqual(status["terminal_status"], "BLOCKED_LIVE_ACCOUNT_IDENTITY")
        self.assertEqual(status["authorization_boundary"], "IMPLEMENTED")
        self.assertEqual(status["addon_provenance"], "L3H3_SOURCE_IMPLEMENTED_RUNTIME_UNVERIFIED")
        self.assertEqual(status["live_account_identity"], "UNVERIFIED")
        self.assertEqual(status["live_authority"], "DISARMED")
        self.assertEqual(status["live_canary"], "NOT_RUN")
        self.assertEqual(status["live_send_count"], 0)

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
        self.assertEqual(status["live_authority"], "DISARMED")
        self.assertEqual(status["live_canary"], "NOT_RUN")
        self.assertEqual(status["live_account_identity"], "UNVERIFIED")
        self.assertEqual(set(status["components"]), {
            "ACCOUNT", "ACCOUNT_CLASS", "CONTRACT", "SESSION", "MARKET_DATA", "ACCOUNT_TRUTH", "POSITION_TRUTH",
            "ORDER_TRUTH", "EXECUTION_GATEWAY", "NT_RISK_GUARD", "PROTECTION", "RECONCILIATION", "LEDGER",
            "CAPABILITY", "KILL_PATHS", "DISK", "STRATEGY", "LIVE_AUTHORITY", "LIVE_ACCOUNT_IDENTITY",
            "AUTHORIZATION_BOUNDARY", "LIVE_CANARY", "QUARANTINE", "LOCK",
        })
        self.assertFalse(any(route.path == "/api/lane-iii/live/activate" for route in app.routes))


if __name__ == "__main__":
    unittest.main()
