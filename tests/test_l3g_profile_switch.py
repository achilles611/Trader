from __future__ import annotations

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest

from fastapi import HTTPException, Request

from src.copytrade.config import CopyTradeConfig
from src.copytrade.control_center import create_control_center_app, profile_switch_runtime_root
from src.l3g_paper.contracts import (
    FIVE_MINUTE_PROFILE,
    HIGH_CONFIDENCE_POLICY,
    HIGH_CONFIDENCE_PROFILE,
    HIGH_CONFIDENCE_RISK_PROFILE,
    PAPER_PROFILE_CATALOG,
    RISK_PROFILE,
    SCALPER_PROFILE,
    resolve_paper_profile,
)
from src.l3g_paper.profile_switch import PaperProfileSwitchService, _manifest
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.runtime import LaneIIIPaperRuntime


def flat_status() -> dict[str, object]:
    return {
        "state": "READY_DISARMED",
        "paper_execution": "DISARMED",
        "session_armed_state": "DISARMED",
        "current_position": "FLAT",
        "current_quantity": 0,
        "broker_snapshot_position": "FLAT",
        "broker_snapshot_position_quantity": 0,
        "working_owned_orders": 0,
        "working_entry_orders": 0,
        "unresolved_command": False,
        "unresolved_native_order": False,
        "unresolved_execution": False,
        "entry_owner": "NONE",
        "operational_paper_session": None,
        "reconciliation_current": True,
        "entry_profile_version": "BEELZEBUB_SCALPER_V2",
        "live_capital": "DENIED",
        "paper_account": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "market_instrument": "MNQ SEP26",
        "ledger": {
            "deferred_queue_depth": 0,
            "deferred_pending_queue_depth": 0,
            "deferred_inflight_queue_depth": 0,
            "deferred_pending_barrier_count": 0,
            "deferred_writer_error": None,
        },
    }


class ProfileCatalogTests(unittest.TestCase):
    def test_switch_storage_stays_anchored_to_runtime_for_nested_profile_runs(self) -> None:
        nested = Path("N:/Beelzebub/runtime/profiles/beelzebub_scalper_v2/runs/run-1/hot/lane_iii_paper.sqlite3")
        self.assertEqual(profile_switch_runtime_root(nested), Path("N:/Beelzebub/runtime").resolve())

    def test_catalog_contains_three_closed_policy_and_risk_bundles(self) -> None:
        self.assertEqual(
            tuple(profile.selection_key for profile in PAPER_PROFILE_CATALOG),
            (
                "NY_HIGH_CONFLUENCE_COMMISSIONING_V1",
                "BEELZEBUB_SCALPER_V2",
                "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
            ),
        )
        self.assertEqual(HIGH_CONFIDENCE_POLICY.configuration_hash, "070587789049231d268cb742404eb6dcc21d91014c9529aa109419a563729a91")
        self.assertEqual(HIGH_CONFIDENCE_RISK_PROFILE.configuration_hash, "eef09f7b185cc197aced3e7b91dd03f1571be52be3635747d2d209821ebcaa34")
        self.assertIs(resolve_paper_profile("NY_HIGH_CONFLUENCE_COMMISSIONING"), HIGH_CONFIDENCE_PROFILE)
        self.assertIs(resolve_paper_profile("BEELZEBUB_SCALPER"), SCALPER_PROFILE)
        self.assertIs(resolve_paper_profile("BEELZEBUB_FIVE_MINUTE_BIAS"), FIVE_MINUTE_PROFILE)

    def test_five_minute_risk_does_not_interrupt_an_in_session_hold(self) -> None:
        self.assertGreaterEqual(FIVE_MINUTE_PROFILE.risk.maximum_position_age_seconds, 86_400)
        self.assertGreaterEqual(FIVE_MINUTE_PROFILE.risk.maximum_session_entries, 96)
        self.assertFalse(FIVE_MINUTE_PROFILE.risk.approved_for_live)

    def test_policy_and_risk_are_bound_together_in_a_fresh_ledger_epoch(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(
                path,
                epoch_id="L3G-PAPER-EPOCH-HIGH-CONFIDENCE-TEST",
                policy=HIGH_CONFIDENCE_PROFILE.policy,
                risk=HIGH_CONFIDENCE_PROFILE.risk,
            )
            runtime = LaneIIIPaperRuntime(ledger)
            self.assertEqual(runtime.risk.profile.configuration_hash, HIGH_CONFIDENCE_PROFILE.risk.configuration_hash)
            record = ledger.append("SESSION_AUTHORITY", runtime.authority.authority_payload())
            self.assertTrue(record)
            self.assertEqual(ledger.recent(limit=1)[0]["payload"]["risk_profile_hash"], HIGH_CONFIDENCE_PROFILE.risk.configuration_hash)
            ledger.close()
            with self.assertRaisesRegex(RuntimeError, "PAPER_PROFILE_LEDGER_EPOCH_MISMATCH"):
                PaperLedger(path, policy=HIGH_CONFIDENCE_PROFILE.policy, risk=RISK_PROFILE)


class ProfileSwitchServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.status_value = flat_status()
        self.flatten_calls = 0
        self.launched: list[tuple[Path, int]] = []
        self.shutdown = threading.Event()
        self.service = PaperProfileSwitchService(
            current_profile=SCALPER_PROFILE,
            paper_status=lambda: dict(self.status_value),
            flatten_and_disarm=self.flatten,
            verifier_status=lambda: {"status": "PASS"},
            request_shutdown=self.shutdown.set,
            runtime_root=self.root / "runtime",
            project_root=self.root,
            python_executable=Path(__file__),
            git_sha="a" * 40,
            parent_pid=12345,
            launch_supervisor=lambda path, pid: self.launched.append((path, pid)),
            poll_seconds=0.001,
            stop_timeout_seconds=1,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def flatten(self) -> dict[str, object]:
        self.flatten_calls += 1
        self.status_value = flat_status()
        return {"initiated": True}

    def test_prepares_fresh_isolated_run_then_requests_controlled_shutdown(self) -> None:
        result = self.service.start("switch-request-0001", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertEqual(result["target_profile"], "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        status = self.service.status()
        self.assertEqual(status["stage"], "SHUTDOWN_REQUESTED")
        self.assertEqual(len(self.launched), 1)
        manifest_path, parent_pid = self.launched[0]
        self.assertEqual(parent_pid, 12345)
        manifest = _manifest(manifest_path)
        self.assertEqual(manifest["target_profile"], "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertEqual(manifest["paper_policy_hash"], FIVE_MINUTE_PROFILE.policy.configuration_hash)
        self.assertEqual(manifest["risk_profile_hash"], FIVE_MINUTE_PROFILE.risk.configuration_hash)
        self.assertFalse(Path(str(manifest["ledger_path"])).exists())
        self.assertIn("profiles", Path(str(manifest["ledger_path"])).parts)
        self.assertTrue(str(manifest["ledger_epoch"]).startswith("L3G-PAPER-EPOCH-BEELZEBUB_FIVE_MINUTE_BIAS_V1-"))

    def test_active_runtime_is_flattened_before_supervisor_launch(self) -> None:
        self.status_value["state"] = "LONG"
        self.status_value["paper_execution"] = "ARMED"
        self.status_value["session_armed_state"] = "ARMED"
        self.status_value["current_position"] = "LONG"
        self.status_value["current_quantity"] = 1
        self.status_value["broker_snapshot_position"] = "LONG"
        self.status_value["broker_snapshot_position_quantity"] = 1
        self.status_value["operational_paper_session"] = {"active": True}
        self.service.start("switch-request-0002", "NY_HIGH_CONFLUENCE_COMMISSIONING_V1")
        self.assertTrue(self.shutdown.wait(1))
        self.assertEqual(self.flatten_calls, 1)
        self.assertEqual(len(self.launched), 1)

    def test_same_or_unknown_profile_is_refused_without_files(self) -> None:
        with self.assertRaisesRegex(ValueError, "already active"):
            self.service.start("switch-request-0003", "BEELZEBUB_SCALPER_V2")
        with self.assertRaisesRegex(ValueError, "Unknown"):
            self.service.start("switch-request-0004", "UNCOMPILED")
        self.assertFalse((self.root / "runtime").exists())

    def test_only_complete_shutdown_receipt_releases_supervisor_gate(self) -> None:
        self.service.start("switch-request-0005", "NY_HIGH_CONFLUENCE_COMMISSIONING_V1")
        self.assertTrue(self.shutdown.wait(1))
        self.service.record_shutdown_receipt({"clean_shutdown": False})
        self.assertEqual(self.service.status()["stage"], "BLOCKED_SAFE")

        state_path = Path(str(self.service.status()["manifest_path"])).with_name("state.json")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual(state["blockers"], ["CURRENT_PROFILE_CONTROLLED_SHUTDOWN_UNPROVEN"])

    def test_manifest_integrity_is_fail_closed(self) -> None:
        self.service.start("switch-request-0006", "NY_HIGH_CONFLUENCE_COMMISSIONING_V1")
        self.assertTrue(self.shutdown.wait(1))
        manifest_path = Path(str(self.service.status()["manifest_path"]))
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
        value["target_profile"] = "BEELZEBUB_SCALPER_V2"
        manifest_path.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "MANIFEST_INTEGRITY"):
            _manifest(manifest_path)


class ProfileSwitchRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_routes_expose_catalog_and_require_local_action_authentication(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            defaults = CopyTradeConfig()
            from dataclasses import replace
            config = replace(
                defaults,
                storage=replace(defaults.storage, cold_root=root / "cold"),
                artifacts=replace(defaults.artifacts, database_path=root / "hot" / "copytrade.sqlite3"),
            )
            app = create_control_center_app(config)
            routes = [route for route in app.routes if getattr(route, "path", None) == "/api/lane-iii/paper/profile-switch"]
            self.assertEqual({method for route in routes for method in route.methods}, {"GET", "POST"})
            get_endpoint = next(route.endpoint for route in routes if "GET" in route.methods)
            post_endpoint = next(route.endpoint for route in routes if "POST" in route.methods)
            catalog = await get_endpoint()
            self.assertEqual(len(catalog["profiles"]), 3)
            request = Request({
                "type": "http", "http_version": "1.1", "method": "POST",
                "scheme": "http", "path": "/api/lane-iii/paper/profile-switch",
                "raw_path": b"/api/lane-iii/paper/profile-switch", "query_string": b"",
                "headers": [(b"host", b"127.0.0.1:8090")],
                "client": ("127.0.0.1", 50000), "server": ("127.0.0.1", 8090),
            })
            with self.assertRaises(HTTPException) as refused:
                await post_endpoint(request, {"request_id": "switch-request-0007", "target_profile": "BEELZEBUB_FIVE_MINUTE_BIAS_V1"})
            self.assertEqual(refused.exception.status_code, 403)
            app.state.paper_autostart.stop()


if __name__ == "__main__":
    unittest.main()
