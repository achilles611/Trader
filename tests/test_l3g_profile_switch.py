from __future__ import annotations

import asyncio
from decimal import Decimal
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
import threading
from typing import Mapping
import unittest
from unittest.mock import patch

from fastapi import HTTPException, Request

from src.copytrade.config import CopyTradeConfig
from src.copytrade.control_center import create_control_center_app, profile_switch_runtime_root
from src.l3g_paper.contracts import (
    FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
    FIVE_MINUTE_PROFILE,
    HIGH_CONFIDENCE_POLICY,
    HIGH_CONFIDENCE_PROFILE,
    HIGH_CONFIDENCE_RISK_PROFILE,
    PAPER_PROFILE_CATALOG,
    RISK_PROFILE,
    SCALPER_PROFILE,
    PaperDirection,
    PaperRuntimeState,
    resolve_paper_profile,
)
from src.l3g_paper.profile_switch import (
    PROFILE_SELECTION_SCHEMA,
    PaperProfileSwitchService,
    _bounded_full_verification,
    _claim_target_launch,
    _manifest,
    _orphaned_faulted_target_cleanup_eligible,
    _launch_child,
    _pid_exists,
    _target_binding_matches,
    _target_faulted_stopping_flat_proven,
    exact_flat_shutdown_ready,
    finalize_stale_target_cleanup,
    remembered_profile_selection,
    supervise,
)
from src.l3g_paper.ninjatrader_transport import (
    ADDON_PROTOCOL_VERSION,
    expected_addon_source_fingerprint,
)
from src.l3g_paper.ledger import (
    PaperLedger,
    risk_continuity_anchor_path,
    risk_continuity_guard_path,
)
from src.l3g_paper.risk_continuity import (
    read_risk_continuity_artifact,
    write_risk_continuity_artifact,
)
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.risk import PaperRiskSnapshot
from src.l3g_paper.sessions import PaperSessionResolver
from src.l3g_paper.verification import VerificationPaths, run_local_verification


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
        "risk": {
            "locked_out": False,
            "lockout_reason": None,
            "lockout_trade_date": None,
        },
        "live_capital": "DENIED",
        "paper_account": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "market_instrument": "MNQ SEP26",
        "risk_continuity": {
            "schema": "lane-iii-paper-risk-continuity-snapshot-v2",
            "generated_at": "2026-09-04T20:00:00Z",
            "account_name": "Sim101",
            "account_class": "LOCAL_SIMULATION",
            "instrument": "MNQ SEP26",
            "source_profile": "BEELZEBUB_SCALPER_V2",
            "authority_lockout": {
                "locked_out": False,
                "lockout_reason": None,
                "lockout_trade_date": None,
            },
            "source_ledger": {
                "path": "N:/fixture/source.sqlite3",
                "ledger_identity": "fixture-source-ledger",
                "ledger_epoch": "L3G-PAPER-EPOCH-FIXTURE-SOURCE",
                "risk_boundary_sequence": 0,
                "risk_boundary_hash": None,
                "coverage_complete": True,
            },
            "trade_dates": [],
            "profile_trade_dates": [],
            "sessions": [],
            "entry_execution_ids": [],
            "exit_execution_ids": [],
        },
        "ledger": {
            "deferred_queue_depth": 0,
            "deferred_pending_queue_depth": 0,
            "deferred_inflight_queue_depth": 0,
            "deferred_pending_barrier_count": 0,
            "deferred_writer_error": None,
        },
    }


def target_paper_status(
    profile: str,
    *,
    direction: str | None = None,
    flat_blocker: str | None = None,
    operational: bool = True,
) -> dict[str, object]:
    value: dict[str, object] = {
        "mode": "PAPER_SIM101",
        "state": "PAPER_RUNNING" if operational else "READY_DISARMED",
        "paper_execution": "RUNNING" if operational else "DISARMED",
        "session_armed_state": "ARMED" if operational else "DISARMED",
        "paper_account": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "market_instrument": "MNQ SEP26",
        "maximum_quantity": 1,
        "live_capital": "DENIED",
        "entry_profile_version": profile,
        "current_position": "FLAT",
        "current_quantity": 0,
        "current_position_quantity": 0,
        "broker_snapshot_position": "FLAT",
        "broker_snapshot_position_quantity": 0,
        "working_owned_orders": 0,
        "working_entry_orders": 0,
        "entry_owner": "NONE",
        "foreign_activity": False,
        "protective_stop_state": "NONE",
        "position_snapshot_complete": True,
        "order_snapshot_complete": True,
        "reconciliation_current": True,
        "unresolved_command": False,
        "unresolved_native_order": False,
        "unresolved_execution": False,
        "lockout_or_fault_reason": flat_blocker,
        "operational_paper_session": (
            {"active": True, "stopping": False}
            if operational else None
        ),
        "transport": {
            "addon_protocol_version": ADDON_PROTOCOL_VERSION,
            "addon_source_fingerprint": expected_addon_source_fingerprint(),
            "expected_addon_source_fingerprint": expected_addon_source_fingerprint(),
            "addon_build_fingerprint": "b" * 64,
            "addon_build_timestamp": "2026-09-07T00:00:00Z",
            "addon_provenance_valid": True,
            "commands_sent": 0,
        },
    }
    if profile == FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION:
        value["position_requirement"] = {
            "required": True,
            "state": "BLOCKED_FLAT",
            "actual_position": "FLAT",
            "actual_quantity": 0,
            "desired_position": None,
            "primary_blocker": flat_blocker or "NO_COMPLETED_FIVE_MINUTE_SIGNAL",
            "blocking_reasons": [
                flat_blocker or "NO_COMPLETED_FIVE_MINUTE_SIGNAL",
            ],
            "source_signal": None,
        }
    if direction in {"LONG", "SHORT"}:
        value.update({
            "state": direction,
            "paper_execution": "POSITIONED",
            "current_position": direction,
            "current_quantity": 1,
            "current_position_quantity": 1,
            "broker_snapshot_position": direction,
            "broker_snapshot_position_quantity": 1,
            "working_owned_orders": 1,
            "protective_stop_state": "WORKING",
            "lockout_or_fault_reason": None,
            "position_requirement": {
                "required": True,
                "state": "POSITIONED",
                "actual_position": direction,
                "actual_quantity": 1,
                "desired_position": direction,
                "primary_blocker": None,
                "blocking_reasons": [],
                "source_signal": {
                    "direction": direction,
                    "candle_close_utc": "2026-09-07T00:00:00Z",
                    "signal_hash": "a" * 64,
                    "ledger_sequence": 101,
                    "record_hash": "b" * 64,
                    "ledger_verified": True,
                },
            },
        })
    return value


def clean_shutdown_receipt() -> dict[str, object]:
    return {
        "schema": "l3g-ledger-controlled-shutdown-v1",
        "clean_shutdown": True,
        "admission_sealed": True,
        "writer_stopped": True,
        "checkpoint": {"complete": True},
        "expected_tip_sequence": 17,
        "durable_tip_sequence": 17,
        "expected_tip_hash": "f" * 64,
        "durable_tip_hash": "f" * 64,
        "risk_continuity_boundary": {
            "path": "N:/fixture/source.sqlite3",
            "ledger_identity": "fixture-source-ledger",
            "ledger_epoch": "L3G-PAPER-EPOCH-FIXTURE-SOURCE",
            "risk_boundary_sequence": 0,
            "risk_boundary_hash": None,
        },
        "verifier_shutdown": {"completed": True},
        "runtime_watchdog_shutdown": {"completed": True},
    }


class ProfileCatalogTests(unittest.TestCase):
    def test_switch_storage_stays_anchored_to_runtime_for_nested_profile_runs(self) -> None:
        nested = Path("N:/Beelzebub/runtime/profiles/beelzebub_scalper_v2/runs/run-1/hot/lane_iii_paper.sqlite3")
        self.assertEqual(profile_switch_runtime_root(nested), Path("N:/Beelzebub/runtime").resolve())

    def test_catalog_contains_four_closed_policy_and_risk_bundles(self) -> None:
        self.assertEqual(
            tuple(profile.selection_key for profile in PAPER_PROFILE_CATALOG),
            (
                "NY_HIGH_CONFLUENCE_COMMISSIONING_V1",
                "BEELZEBUB_SCALPER_V2",
                "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
                "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2",
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

    def test_legacy_named_perpetual_profile_decides_every_thirty_seconds(self) -> None:
        profile = resolve_paper_profile("BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2")
        self.assertEqual(profile.policy.decision_interval_seconds, 30)
        self.assertIn("legacy", profile.display_name.lower())
        self.assertIn("30-second", profile.description)

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


class RiskContinuityTests(unittest.TestCase):
    @staticmethod
    def context(at: str):
        result = PaperSessionResolver().resolve(at, generation=3)
        if result.context.session_kind.value == "OFF_SESSION":
            raise AssertionError(f"Fixture time is off-session: {at}")
        return result.context

    @staticmethod
    def append_accounted_lifecycle(
        ledger: PaperLedger, *, context, entry_id: str, exit_id: str, realized: str,
    ) -> None:
        ledger.set_session_context(context)
        ledger.append(
            "RISK_EVENT_ENTRY_ACCOUNTED",
            {**context.payload(), "native_execution_id": entry_id, "effect": "ENTRY_COUNT_INCREMENTED_ONCE"},
            identity="risk-entry-" + entry_id,
        )
        ledger.append(
            "RISK_EVENT_EXIT_ACCOUNTED",
            {
                **context.payload(), "exit_execution_id": exit_id, "realized_pnl": realized,
                "effect": "REALIZED_PNL_AND_LOSS_STREAK_APPLIED_ONCE",
            },
            identity="risk-exit-" + exit_id,
        )

    def test_restart_restores_limits_and_duplicate_ids_without_double_accounting(self) -> None:
        context = self.context("2026-09-03T14:00:00Z")
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            self.append_accounted_lifecycle(
                ledger, context=context, entry_id="restart-entry", exit_id="restart-exit", realized="-25",
            )
            ledger.close()

            reopened = PaperLedger(path)
            runtime = LaneIIIPaperRuntime(reopened)
            runtime._set_session_context(context, reason="RESTART_TEST")
            status = runtime.status()
            self.assertEqual(status["session_entries"], 1)
            self.assertEqual(status["trade_date_entry_count"], 1)
            self.assertEqual(status["daily_realized_pnl"], "-25")
            self.assertEqual(status["consecutive_losses"], 1)

            runtime._state = PaperRuntimeState.ENTRY_PENDING
            runtime._apply_execution({
                "message_type": "EXECUTION_EVENT", "order_role": "ENTRY", "price": "100",
                "quantity": 1, "direction": "LONG", "native_execution_id": "restart-entry",
            })
            self.assertEqual(runtime.status()["trade_date_entry_count"], 1)
            self.assertTrue(runtime.risk.status()["locked_out"])
            self.assertEqual(
                runtime.risk.status()["lockout_reason"],
                "RISK_CONTINUITY_EXECUTION_EVIDENCE_CONFLICT",
            )
            self.assertEqual(
                runtime.status()["risk_continuity_fault"],
                "RISK_CONTINUITY_EXECUTION_EVIDENCE_CONFLICT",
            )
            self.assertFalse(runtime.risk_continuity_snapshot()["source_ledger"]["coverage_complete"])
            with self.assertRaisesRegex(
                RuntimeError, "RISK_CONTINUITY_SOURCE_LEDGER_COVERAGE_INCOMPLETE",
            ):
                write_risk_continuity_artifact(
                    Path(directory) / "conflicting-live-callback.json",
                    operation_id="conflicting-live-callback",
                    source_profile="BEELZEBUB_SCALPER_V2",
                    target_profile="BEELZEBUB_FIVE_MINUTE_BIAS_V1",
                    snapshot=runtime.risk_continuity_snapshot(),
                )
            self.assertEqual(len(reopened.recent_kinds(("INCIDENT_CONFLICTING_EXECUTION_CALLBACK",))), 1)
            reopened.close()

    def test_exact_flat_readiness_uses_actual_nested_runtime_risk_status(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._state = PaperRuntimeState.READY_DISARMED
            runtime._snapshot = PaperRiskSnapshot(
                "2026-09-03T14:00:00Z",
                position_snapshot_complete=True,
                order_snapshot_complete=True,
                reconciliation_current=True,
            )
            self.assertTrue(exact_flat_shutdown_ready(runtime.status()))

            runtime.risk.lock_out("ENTRY_SLIPPAGE_LIMIT", trade_date="2026-09-03")
            self.assertTrue(exact_flat_shutdown_ready(runtime.status()))
            ledger.close()

    def test_identical_duplicate_raw_execution_is_counted_once(self) -> None:
        context = PaperSessionResolver().resolve(
            "2026-09-03T14:00:00Z", generation=3,
        ).context
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            ledger.set_session_context(context)
            entry = {
                "order_role": "ENTRY", "price": "100", "quantity": 1,
                "direction": "LONG", "native_execution_id": "raw-entry",
                "account_name": "Sim101", "instrument": "MNQ SEP26",
            }
            ledger.append("EXECUTION", entry, identity="raw-entry-first")
            ledger.append("EXECUTION", entry, identity="raw-entry-identical-replay")
            ledger.append("EXECUTION", {
                "order_role": "EXIT", "price": "99", "quantity": 1,
                "direction": "FLAT", "native_execution_id": "raw-exit",
                "account_name": "Sim101", "instrument": "MNQ SEP26",
            }, identity="raw-exit-first")
            ledger.close()

            reopened = PaperLedger(path)
            runtime = LaneIIIPaperRuntime(reopened)
            snapshot = runtime.risk_continuity_snapshot()
            self.assertTrue(snapshot["source_ledger"]["coverage_complete"])
            self.assertEqual(snapshot["trade_dates"][0]["entry_count"], 1)
            self.assertEqual(snapshot["trade_dates"][0]["realized_pnl"], "-2")
            reopened.close()

    def test_conflicting_duplicate_raw_execution_blocks_recovery(self) -> None:
        context = self.context("2026-09-03T14:00:00Z")
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            ledger.set_session_context(context)
            base = {
                "order_role": "ENTRY", "price": "100", "quantity": 1,
                "direction": "LONG", "native_execution_id": "conflict-entry",
                "account_name": "Sim101", "instrument": "MNQ SEP26",
            }
            ledger.append("EXECUTION", base, identity="conflict-entry-first")
            ledger.append("EXECUTION", {**base, "price": "101"}, identity="conflict-entry-second")
            ledger.close()

            reopened = PaperLedger(path)
            runtime = LaneIIIPaperRuntime(reopened)
            self.assertEqual(
                runtime.risk.status()["lockout_reason"],
                "RISK_CONTINUITY_EXECUTION_EVIDENCE_CONFLICT",
            )
            self.assertFalse(runtime.risk_continuity_snapshot()["source_ledger"]["coverage_complete"])
            reopened.close()

    def test_duplicate_execution_conflicts_are_fail_closed_by_field(self) -> None:
        context = self.context("2026-09-03T14:00:00Z")
        other_context = self.context("2026-09-03T20:05:00Z")
        cases = {
            "price": ({"price": "101"}, context),
            "quantity": ({"quantity": 2}, context),
            "direction": ({"direction": "SHORT"}, context),
            "context": ({}, other_context),
            "profile": ({"risk_profile_version": "BEELZEBUB_FIVE_MINUTE_BIAS_V1"}, context),
            "role": ({"order_role": "EXIT"}, context),
        }
        for name, (changes, second_context) in cases.items():
            with self.subTest(name=name), TemporaryDirectory() as directory:
                path = Path(directory) / "paper.sqlite3"
                ledger = PaperLedger(path)
                ledger.set_session_context(context)
                base = {
                    "order_role": "ENTRY", "price": "100", "quantity": 1,
                    "direction": "LONG", "native_execution_id": "same-execution-id",
                    "account_name": "Sim101", "instrument": "MNQ SEP26",
                    "risk_profile_version": "BEELZEBUB_SCALPER_V2",
                }
                ledger.append("EXECUTION", base, identity=f"{name}-first")
                ledger.set_session_context(second_context)
                ledger.append("EXECUTION", {**base, **changes}, identity=f"{name}-second")
                ledger.close()

                reopened = PaperLedger(path)
                runtime = LaneIIIPaperRuntime(reopened)
                self.assertEqual(
                    runtime.risk.status()["lockout_reason"],
                    "RISK_CONTINUITY_EXECUTION_EVIDENCE_CONFLICT",
                )
                self.assertFalse(
                    runtime.risk_continuity_snapshot()["source_ledger"]["coverage_complete"],
                )
                reopened.close()

    def test_raw_and_accounted_exit_pnl_conflict_blocks_recovery(self) -> None:
        context = self.context("2026-09-03T14:00:00Z")
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            ledger.set_session_context(context)
            common = {
                "account_name": "Sim101", "instrument": "MNQ SEP26",
                "risk_profile_version": "BEELZEBUB_SCALPER_V2",
            }
            ledger.append("EXECUTION", {
                **common, "order_role": "ENTRY", "price": "100", "quantity": 1,
                "direction": "LONG", "native_execution_id": "pnl-entry",
            }, identity="pnl-entry-raw")
            ledger.append("RISK_EVENT_ENTRY_ACCOUNTED", {
                **context.payload(), "risk_profile_version": "BEELZEBUB_SCALPER_V2",
                "native_execution_id": "pnl-entry", "effect": "ENTRY_COUNT_INCREMENTED_ONCE",
            }, identity="pnl-entry-accounted")
            ledger.append("EXECUTION", {
                **common, "order_role": "EXIT", "price": "99", "quantity": 1,
                "direction": "FLAT", "native_execution_id": "pnl-exit",
            }, identity="pnl-exit-raw")
            ledger.append("RISK_EVENT_EXIT_ACCOUNTED", {
                **context.payload(), "risk_profile_version": "BEELZEBUB_SCALPER_V2",
                "exit_execution_id": "pnl-exit", "realized_pnl": "-200",
                "effect": "REALIZED_PNL_AND_LOSS_STREAK_APPLIED_ONCE",
            }, identity="pnl-exit-accounted")
            ledger.close()

            reopened = PaperLedger(path)
            runtime = LaneIIIPaperRuntime(reopened)
            self.assertEqual(
                runtime.risk.status()["lockout_reason"],
                "RISK_CONTINUITY_EXIT_EVIDENCE_CONFLICT",
            )
            self.assertFalse(runtime.risk_continuity_snapshot()["source_ledger"]["coverage_complete"])
            reopened.close()

    def test_imported_snapshot_rejects_cross_role_execution_id(self) -> None:
        snapshot = flat_status()["risk_continuity"]
        snapshot["entry_execution_ids"] = ["cross-role"]
        snapshot["exit_execution_ids"] = ["cross-role"]
        with TemporaryDirectory() as directory:
            target = PaperLedger(Path(directory) / "cross-role-target.sqlite3")
            runtime = LaneIIIPaperRuntime(target, risk_continuity=snapshot)
            self.assertEqual(
                runtime.risk.status()["lockout_reason"],
                "RISK_CONTINUITY_EXECUTION_ROLE_CONFLICT",
            )
            target.close()

    def test_non_daily_lockout_survives_restart_next_date_and_profile_switch(self) -> None:
        context = self.context("2026-09-03T14:00:00Z")
        next_context = self.context("2026-09-06T22:30:00Z")
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = PaperLedger(root / "source.sqlite3")
            runtime = LaneIIIPaperRuntime(source)
            runtime._set_session_context(context, reason="LOCKOUT_SOURCE")
            runtime.risk.lock_out("ENTRY_SLIPPAGE_LIMIT", trade_date=context.trade_date)
            source.close()

            restarted_ledger = PaperLedger(root / "source.sqlite3")
            restarted = LaneIIIPaperRuntime(restarted_ledger)
            restarted._set_session_context(next_context, reason="NEXT_TRADE_DATE")
            self.assertEqual(restarted.risk.status()["lockout_reason"], "ENTRY_SLIPPAGE_LIMIT")
            continuity = restarted.risk_continuity_snapshot()
            restarted_ledger.close()

            target = PaperLedger(
                root / "target.sqlite3", policy=FIVE_MINUTE_PROFILE.policy,
                risk=FIVE_MINUTE_PROFILE.risk,
            )
            switched = LaneIIIPaperRuntime(target, risk_continuity=continuity)
            self.assertEqual(switched.risk.status()["lockout_reason"], "ENTRY_SLIPPAGE_LIMIT")
            target.close()

    def test_daily_loss_cannot_replace_persistent_safety_lockouts(self) -> None:
        context = self.context("2026-09-03T14:00:00Z")
        next_context = self.context("2026-09-06T22:30:00Z")
        for reason in (
            "ENTRY_SLIPPAGE_LIMIT", "PROTECTIVE_STOP_REJECTED", "FOREIGN_EXECUTION_CLASSIFICATION",
        ):
            with self.subTest(reason=reason), TemporaryDirectory() as directory:
                path = Path(directory) / "paper.sqlite3"
                ledger = PaperLedger(path)
                runtime = LaneIIIPaperRuntime(ledger)
                runtime._set_session_context(context, reason="PERSISTENT_LOCKOUT_SOURCE")
                runtime.risk.lock_out(reason, trade_date=context.trade_date)
                runtime._position = PaperDirection.LONG
                runtime._entry_session_context = context
                runtime._snapshot = PaperRiskSnapshot(
                    "2026-09-03T14:00:00Z",
                    daily_realized_pnl=Decimal("-200"),
                )
                runtime._request_operational_stop_locked = lambda *_: None  # type: ignore[method-assign]
                runtime._request_exit = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
                runtime._evaluate_risk_exit("2026-09-03T14:00:00Z")
                self.assertEqual(runtime.risk.status()["lockout_reason"], reason)
                ledger.close()

                reopened = PaperLedger(path)
                recovered = LaneIIIPaperRuntime(reopened)
                recovered._set_session_context(next_context, reason="NEXT_TRADE_DATE")
                self.assertEqual(recovered.risk.status()["lockout_reason"], reason)
                self.assertTrue(recovered.risk.status()["locked_out"])
                reopened.close()

    def test_failed_lockout_ledger_append_leaves_restart_fail_closed_marker(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            runtime = LaneIIIPaperRuntime(ledger)
            original_append = ledger.append
            failed = False

            def fail_once(kind: str, *args: object, **kwargs: object) -> object:
                nonlocal failed
                if kind == "RISK_EVENT_AUTHORITY_LOCKOUT" and not failed:
                    failed = True
                    raise RuntimeError("fixture lockout append failure")
                return original_append(kind, *args, **kwargs)

            with patch.object(ledger, "append", side_effect=fail_once):
                runtime.risk.lock_out("ENTRY_SLIPPAGE_LIMIT", trade_date="2026-09-03")
            self.assertEqual(
                runtime.status()["risk_continuity_fault"],
                "RISK_LOCKOUT_EVIDENCE_PERSISTENCE_FAILED",
            )
            marker = Path(str(path) + ".risk-authority-pending.json")
            self.assertTrue(marker.is_file())
            ledger.close()

            reopened = PaperLedger(path)
            recovered = LaneIIIPaperRuntime(reopened)
            self.assertEqual(
                recovered.risk.status()["lockout_reason"],
                "RISK_LOCKOUT_EVIDENCE_PERSISTENCE_FAILED",
            )
            self.assertTrue(recovered.status()["entries_paused"])
            self.assertFalse(marker.exists())
            reopened.close()

    def test_lockout_publication_precedes_observable_authority_transition(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            with patch(
                "src.l3g_paper.runtime._atomic_json",
                side_effect=SystemExit("fixture crash during marker publication"),
            ):
                with self.assertRaisesRegex(SystemExit, "fixture crash"):
                    runtime.risk.lock_out("ENTRY_SLIPPAGE_LIMIT", trade_date="2026-09-03")
            self.assertFalse(runtime.risk.status()["locked_out"])
            self.assertIsNone(runtime.risk.status()["lockout_reason"])
            self.assertEqual(
                ledger.recent_kinds(("RISK_EVENT_AUTHORITY_LOCKOUT",)),
                [],
            )
            ledger.close()

    def test_marker_oserror_falls_back_to_durable_lockout_row_for_restart(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            runtime = LaneIIIPaperRuntime(ledger)
            with patch(
                "src.l3g_paper.runtime._atomic_json",
                side_effect=OSError("fixture marker publication failure"),
            ):
                runtime.risk.lock_out("ENTRY_SLIPPAGE_LIMIT", trade_date="2026-09-03")
            self.assertTrue(runtime.risk.status()["locked_out"])
            self.assertEqual(
                len(ledger.recent_kinds(("RISK_EVENT_AUTHORITY_LOCKOUT",))), 1,
            )
            ledger.close()

            reopened = PaperLedger(path)
            recovered = LaneIIIPaperRuntime(reopened)
            self.assertEqual(
                recovered.risk.status()["lockout_reason"], "ENTRY_SLIPPAGE_LIMIT",
            )
            reopened.close()

    def test_dual_lockout_persistence_failure_cannot_publish_authority_state(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            original_append = ledger.append

            def fail_lockout_append(kind: str, *args: object, **kwargs: object) -> object:
                if kind == "RISK_EVENT_AUTHORITY_LOCKOUT":
                    raise OSError("fixture ledger lockout failure")
                return original_append(kind, *args, **kwargs)

            with (
                patch(
                    "src.l3g_paper.runtime._atomic_json",
                    side_effect=OSError("fixture marker publication failure"),
                ),
                patch.object(ledger, "append", side_effect=fail_lockout_append),
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "RISK_LOCKOUT_EVIDENCE_PERSISTENCE_FAILED",
                ):
                    runtime.risk.lock_out("ENTRY_SLIPPAGE_LIMIT", trade_date="2026-09-03")
            self.assertFalse(runtime.risk.status()["locked_out"])
            self.assertTrue(runtime.status()["entries_paused"])
            self.assertEqual(
                runtime.status()["risk_continuity_fault"],
                "RISK_LOCKOUT_EVIDENCE_PERSISTENCE_FAILED",
            )
            ledger.close()

    def test_daily_loss_lockout_clears_only_on_next_exchange_trade_date(self) -> None:
        context = self.context("2026-09-03T14:00:00Z")
        next_context = self.context("2026-09-06T22:30:00Z")
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._set_session_context(context, reason="DAILY_LIMIT_SOURCE")
            runtime.risk.lock_out("DAILY_LOSS_LIMIT", trade_date=context.trade_date)
            ledger.close()

            same_ledger = PaperLedger(path)
            restarted = LaneIIIPaperRuntime(same_ledger)
            restarted._set_session_context(context, reason="DAILY_LIMIT_SOURCE")
            self.assertEqual(restarted.risk.status()["lockout_reason"], "DAILY_LOSS_LIMIT")
            restarted._set_session_context(next_context, reason="NEXT_TRADE_DATE")
            self.assertFalse(restarted.risk.status()["locked_out"])
            same_ledger.close()

            final_ledger = PaperLedger(path)
            final = LaneIIIPaperRuntime(final_ledger)
            self.assertFalse(final.risk.status()["locked_out"])
            final_ledger.close()

    def test_exchange_sessions_share_one_cumulative_trade_date_budget(self) -> None:
        asia = self.context("2026-09-02T22:30:00Z")
        rth = self.context("2026-09-03T14:00:00Z")
        ny_after = self.context("2026-09-03T20:30:00Z")
        next_asia = self.context("2026-09-06T22:30:00Z")
        self.assertEqual(asia.trade_date, rth.trade_date)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            self.append_accounted_lifecycle(
                ledger, context=asia, entry_id="asia-entry", exit_id="asia-exit", realized="-10",
            )
            ledger.close()

            reopened = PaperLedger(path)
            runtime = LaneIIIPaperRuntime(reopened)
            runtime._set_session_context(rth, reason="ASIA_TO_RTH_SAME_TRADE_DATE")
            self.assertEqual(runtime.status()["trade_date_entry_count"], 1)
            self.assertEqual(runtime.status()["daily_realized_pnl"], "-10")
            self.assertEqual(runtime.status()["consecutive_losses"], 1)
            self.assertEqual(runtime.status()["session_entries"], 0)
            runtime.risk.lock_out("DAILY_LOSS_LIMIT", trade_date=rth.trade_date)
            runtime._set_session_context(ny_after, reason="SAME_NEW_YORK_FAMILY")
            self.assertEqual(runtime.status()["trade_date_entry_count"], 1)
            self.assertEqual(runtime.status()["session_entries"], 0)
            self.assertTrue(runtime.risk.status()["locked_out"])
            runtime._set_session_context(next_asia, reason="NEXT_EXCHANGE_TRADE_DATE")
            self.assertEqual(runtime.status()["trade_date_entry_count"], 0)
            self.assertEqual(runtime.status()["daily_realized_pnl"], "0")
            self.assertFalse(runtime.risk.status()["locked_out"])
            reopened.close()

    def test_unclosed_recovered_lifecycle_blocks_entries_but_not_runtime_creation(self) -> None:
        context = self.context("2026-09-03T14:00:00Z")
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            ledger.set_session_context(context)
            ledger.append(
                "EXECUTION",
                {
                    "order_role": "ENTRY", "price": "100", "quantity": 1,
                    "direction": "LONG", "native_execution_id": "unclosed-entry",
                },
                identity="raw-unclosed-entry",
            )
            runtime = LaneIIIPaperRuntime(ledger)
            self.assertEqual(
                runtime.status()["risk_continuity_fault"],
                "RISK_CONTINUITY_OPEN_LIFECYCLE_UNRESOLVED",
            )
            self.assertTrue(runtime.status()["entries_paused"])
            self.assertFalse(runtime.status()["risk_continuity"]["source_ledger"]["coverage_complete"])
            ledger.close()

    def test_repeated_startup_import_is_idempotent_on_the_same_target_ledger(self) -> None:
        snapshot = flat_status()["risk_continuity"]
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            first = LaneIIIPaperRuntime(ledger, risk_continuity=snapshot)  # type: ignore[arg-type]
            self.assertEqual(first.status()["risk_continuity"]["entry_execution_ids"], [])
            ledger.close()

            reopened = PaperLedger(path)
            second = LaneIIIPaperRuntime(reopened, risk_continuity=snapshot)  # type: ignore[arg-type]
            self.assertEqual(second.status()["risk_continuity"]["entry_execution_ids"], [])
            imports = [
                record for record in reopened.risk_continuity_records()
                if record["kind"] == "RISK_EVENT_CONTINUITY_IMPORTED"
            ]
            self.assertEqual(len(imports), 1)
            reopened.close()

    def test_duplicate_raw_entry_and_exit_receipts_recover_one_lifecycle(self) -> None:
        context = self.context("2026-09-03T14:00:00Z")
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            ledger.set_session_context(context)
            entry = {
                "order_role": "ENTRY", "price": "100", "quantity": 1,
                "direction": "LONG", "native_execution_id": "duplicate-entry",
            }
            exit_fill = {
                "order_role": "EXIT", "price": "101", "quantity": 1,
                "direction": "LONG", "native_execution_id": "duplicate-exit",
            }
            ledger.append("EXECUTION", {**entry, "receipt_id": "entry-receipt-1"}, identity="raw-entry-1")
            ledger.append("EXECUTION", {**entry, "receipt_id": "entry-receipt-2"}, identity="raw-entry-2")
            ledger.append("EXECUTION", {**exit_fill, "receipt_id": "exit-receipt-1"}, identity="raw-exit-1")
            ledger.append("EXECUTION", {**exit_fill, "receipt_id": "exit-receipt-2"}, identity="raw-exit-2")
            ledger.close()

            reopened = PaperLedger(path)
            runtime = LaneIIIPaperRuntime(reopened)
            runtime._set_session_context(context, reason="DUPLICATE_RAW_RECOVERY")
            status = runtime.status()
            self.assertEqual(status["trade_date_entry_count"], 1)
            self.assertEqual(status["daily_realized_pnl"], "2")
            self.assertEqual(status["consecutive_losses"], 0)
            reopened.close()

    def test_empty_native_exit_id_uses_one_receipt_identity_for_pnl_recovery(self) -> None:
        context = self.context("2026-09-03T14:00:00Z")
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._set_session_context(context, reason="EMPTY_NATIVE_ID_TEST")
            runtime._state = PaperRuntimeState.EXIT_PENDING
            runtime._position = PaperDirection.LONG
            runtime._position_quantity = 1
            runtime._entry_fill_price = Decimal("100")
            runtime._entry_fill_quantity = 1
            runtime._entry_direction = PaperDirection.LONG
            runtime._entry_session_context = context
            runtime._apply_execution({
                "message_type": "EXECUTION_EVENT", "order_role": "EXIT", "price": "99",
                "quantity": 1, "native_execution_id": "", "receipt_id": "fallback-exit-receipt",
            })
            ledger.close()

            reopened = PaperLedger(path)
            recovered = LaneIIIPaperRuntime(reopened)
            recovered._set_session_context(context, reason="EMPTY_NATIVE_ID_TEST")
            status = recovered.status()
            self.assertEqual(status["daily_realized_pnl"], "-2")
            self.assertEqual(status["consecutive_losses"], 1)
            self.assertIn("receipt:fallback-exit-receipt", status["risk_continuity"]["exit_execution_ids"])
            reopened.close()

    def test_unknown_durable_execution_role_invalidates_handoff_coverage(self) -> None:
        context = self.context("2026-09-03T14:00:00Z")
        with TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = PaperLedger(root / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._set_session_context(context, reason="UNKNOWN_EXECUTION_ROLE_TEST")
            message = {
                "message_type": "EXECUTION_EVENT", "order_role": "UNCOMPILED_ROLE",
                "price": "100", "quantity": 1, "native_execution_id": "unknown-role-fill",
            }
            ledger.append("EXECUTION", message, identity="raw-unknown-role")
            runtime._state = PaperRuntimeState.READY_DISARMED
            runtime._apply_execution(message)
            snapshot = runtime.status()["risk_continuity"]
            self.assertEqual(runtime.state, PaperRuntimeState.LOCKED_OUT)
            self.assertFalse(snapshot["source_ledger"]["coverage_complete"])
            with self.assertRaisesRegex(RuntimeError, "RISK_CONTINUITY_SOURCE_LEDGER_COVERAGE_INCOMPLETE"):
                write_risk_continuity_artifact(
                    root / "blocked.json", operation_id="unknown-role-operation",
                    source_profile="BEELZEBUB_SCALPER_V2",
                    target_profile="BEELZEBUB_FIVE_MINUTE_BIAS_V1", snapshot=snapshot,
                )
            ledger.close()

    def test_corrupt_continuity_revokes_entries_but_reconciliation_remains_available(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger, risk_continuity={"schema": "corrupt"})
            self.assertEqual(runtime.status()["risk_continuity_fault"], "RISK_CONTINUITY_SNAPSHOT_INVALID")
            runtime._apply_reconciliation({
                "message_type": "RECONCILIATION",
                "receipt_id": "corrupt-continuity-flat-reconciliation",
                "account_name": "Sim101",
                "account_class": "LOCAL_SIMULATION",
                "instrument": "MNQ SEP26",
                "position_quantity": 0,
                "working_order_count": 0,
                "working_entry_count": 0,
                "position_snapshot_complete": True,
                "order_snapshot_complete": True,
                "protective_stop_state": "NONE",
            })
            status = runtime.status()
            self.assertTrue(status["reconciliation_current"])
            self.assertEqual(status["current_position"], "FLAT")
            self.assertEqual(status["risk_continuity_fault"], "RISK_CONTINUITY_SNAPSHOT_INVALID")
            self.assertFalse(status["risk_continuity"]["source_ledger"]["coverage_complete"])
            ledger.close()


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
            parent_pid=2_147_483_647,
            launch_supervisor=lambda path, pid: self.launched.append((path, pid)),
            poll_seconds=0.001,
            stop_timeout_seconds=1,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @unittest.skipUnless(os.name == "nt", "Windows process handles are host-specific.")
    def test_windows_pid_probe_distinguishes_running_and_signaled_processes(self) -> None:
        child = subprocess.Popen(
            [sys.executable, "-c", "pass"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.assertTrue(_pid_exists(child.pid))
        self.assertEqual(child.wait(timeout=10), 0)
        self.assertFalse(_pid_exists(child.pid))

    def test_bounded_source_verifier_returns_its_exact_full_report(self) -> None:
        ledger_path = self.root / "bounded-verifier" / "paper.sqlite3"
        audit_root = self.root / "bounded-verifier" / "audit"
        ledger = PaperLedger(ledger_path)
        ledger.close()

        report = _bounded_full_verification(
            ledger_path,
            audit_root,
            timeout_seconds=30,
            python_executable=sys.executable,
        )

        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["verification_mode"], "full")
        self.assertEqual(report["ledger_path"], str(ledger_path.resolve()))
        verification_id = str(report["verification_id"])
        self.assertTrue(
            any(
                path.name.endswith(f"-{verification_id}.json")
                for path in VerificationPaths(audit_root).reports.glob("*.json")
            ),
        )

    def test_bounded_source_verifier_cancels_and_fails_closed_on_timeout(self) -> None:
        ledger_path = self.root / "timed-out-verifier" / "paper.sqlite3"
        audit_root = self.root / "timed-out-verifier" / "audit"
        ledger = PaperLedger(ledger_path)
        ledger.close()

        class TimedOutProcess:
            def __init__(self) -> None:
                self.wait_calls = 0

            def wait(self, *, timeout: float) -> int:
                self.wait_calls += 1
                if self.wait_calls == 1:
                    raise subprocess.TimeoutExpired("bounded-verifier", timeout)
                return 1

        process = TimedOutProcess()
        with self.assertRaisesRegex(
            RuntimeError,
            "PERPETUAL_STARTUP_SEED_SOURCE_VERIFICATION_TIMEOUT",
        ):
            _bounded_full_verification(
                ledger_path,
                audit_root,
                timeout_seconds=0.001,
                cancellation_grace_seconds=0.001,
                popen=lambda *_args, **_kwargs: process,  # type: ignore[arg-type]
            )
        self.assertEqual(process.wait_calls, 2)
        self.assertEqual(
            len(list(audit_root.glob("ledger-verification-*.cancel"))), 1,
        )

    def test_faulted_stopping_cleanup_is_exactly_scoped_to_checkpoint_fault(self) -> None:
        operation_id = "profile-switch-" + "a" * 32
        manifest = {
            "operation_id": operation_id,
            "target_profile": FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        }
        paper = target_paper_status(
            FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        )
        paper.update({
            "state": "FAULTED",
            "paper_execution": "LOCKED",
            "session_armed_state": "DISARMED",
            "lockout_or_fault_reason": (
                "FIVE_MINUTE_SIGNAL_CHECKPOINT_DURABILITY_FAILED"
            ),
            "operational_paper_session": {
                "active": True,
                "request_id": f"profile-switch-{operation_id}",
                "started_at": "2026-09-08T05:19:07Z",
                "context": {"session_id": "MNQU6:ASIA:2026-09-08"},
                "stopping": True,
                "stopping_reason": (
                    "FIVE_MINUTE_SIGNAL_CHECKPOINT_DURABILITY_FAILED"
                ),
            },
        })
        self.assertTrue(_target_faulted_stopping_flat_proven(paper, manifest))

        mutations = {
            "wrong_profile": lambda value: value.__setitem__(
                "entry_profile_version", "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
            ),
            "armed": lambda value: value.__setitem__(
                "session_armed_state", "ARMED",
            ),
            "different_fault": lambda value: value.__setitem__(
                "lockout_or_fault_reason", "DIFFERENT_FAULT",
            ),
            "not_stopping": lambda value: value[
                "operational_paper_session"
            ].__setitem__("stopping", False),
            "wrong_request": lambda value: value[
                "operational_paper_session"
            ].__setitem__("request_id", "different-request"),
            "positioned": lambda value: value.__setitem__(
                "current_position_quantity", 1,
            ),
            "working_order": lambda value: value.__setitem__(
                "working_owned_orders", 1,
            ),
            "stale_reconciliation": lambda value: value.__setitem__(
                "reconciliation_current", False,
            ),
            "unresolved_execution": lambda value: value.__setitem__(
                "unresolved_execution", True,
            ),
        }
        for name, mutation in mutations.items():
            with self.subTest(name=name):
                changed = json.loads(json.dumps(paper))
                mutation(changed)
                self.assertFalse(
                    _target_faulted_stopping_flat_proven(changed, manifest),
                )

    def test_orphaned_faulted_cleanup_requires_exact_supervisor_projection(self) -> None:
        operation_id = "profile-switch-" + "b" * 32
        fault = "FIVE_MINUTE_SIGNAL_CHECKPOINT_DURABILITY_FAILED"
        manifest = {
            "operation_id": operation_id,
            "target_profile": FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        }
        paper = target_paper_status(
            FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        )
        paper.update({
            "state": "FAULTED",
            "paper_execution": "LOCKED",
            "session_armed_state": "DISARMED",
            "lockout_or_fault_reason": fault,
            "operational_paper_session": {
                "active": True,
                "request_id": f"profile-switch-{operation_id}",
                "started_at": "2026-09-08T05:19:07Z",
                "context": {"session_id": "MNQU6:ASIA:2026-09-08"},
                "stopping": True,
                "stopping_reason": fault,
            },
        })
        state = {
            "stage": "AUTOSTARTING_TARGET",
            "in_progress": True,
            "target_cleanup": None,
            "blockers": ["TARGET_NON_OPERATIONAL_FLAT_PROOF_UNAVAILABLE"],
            "target_pid": 31346,
            "target_runtime_pid": 31347,
            "target_autostart": {
                "schema": "lane-iii-paper-autostart-v1",
                "authority": "PERSISTENT_PAPER_SIM101_ONLY",
                "stage": "BLOCKED",
                "in_progress": False,
                "blockers": [fault],
                "request_id": f"profile-switch-{operation_id}",
            },
        }
        self.assertTrue(
            _orphaned_faulted_target_cleanup_eligible(state, paper, manifest),
        )

        mutations = {
            "terminalized": lambda value: value.__setitem__(
                "stage", "BLOCKED_SAFE",
            ),
            "cleanup_present": lambda value: value.__setitem__(
                "target_cleanup", {},
            ),
            "different_blocker": lambda value: value.__setitem__(
                "blockers", ["DIFFERENT_BLOCKER"],
            ),
            "missing_target_pid": lambda value: value.__setitem__(
                "target_pid", None,
            ),
            "autostart_running": lambda value: value[
                "target_autostart"
            ].__setitem__("in_progress", True),
            "autostart_ready": lambda value: value[
                "target_autostart"
            ].__setitem__("stage", "RUNNING"),
            "different_request": lambda value: value[
                "target_autostart"
            ].__setitem__("request_id", "different-request"),
        }
        for name, mutation in mutations.items():
            with self.subTest(name=name):
                changed = json.loads(json.dumps(state))
                mutation(changed)
                self.assertFalse(
                    _orphaned_faulted_target_cleanup_eligible(
                        changed, paper, manifest,
                    ),
                )

    def flatten(self) -> dict[str, object]:
        self.flatten_calls += 1
        self.status_value = flat_status()
        return {"initiated": True}

    def supervisor_ready_manifest(
        self, request_id: str = "switch-supervisor-ready-0001",
        target_profile: str = "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
    ) -> tuple[Path, dict[str, object]]:
        self.shutdown.clear()
        self.service.start(request_id, target_profile)
        self.assertTrue(self.shutdown.wait(1))
        manifest_path = Path(str(self.service.status()["manifest_path"]))
        self.service.record_shutdown_receipt(clean_shutdown_receipt())
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest.update({
            "project_root": str(Path(__file__).resolve().parents[1]),
            "python_executable": sys.executable,
            "git_sha": subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1],
                check=True, capture_output=True, text=True,
            ).stdout.strip(),
        })
        manifest.pop("manifest_sha256")
        manifest["manifest_sha256"] = sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        return manifest_path, manifest

    def complete_supervisor_with_stubs(
        self, manifest_path: Path, manifest: dict[str, object], launches: list[list[str]],
    ) -> int:
        class StubProcess:
            pid = 424_242

            @staticmethod
            def poll() -> None:
                return None

        continuity = read_risk_continuity_artifact(manifest_path.with_name("risk-continuity.json"))
        binding = {
            "pid": StubProcess.pid + 1,
            "parent_pid": StubProcess.pid,
            "ledger": manifest["ledger_path"], "audit": manifest["audit_root"],
            "git_sha": manifest["git_sha"], "entry_profile_version": manifest["target_profile"],
            "paper_policy_hash": manifest["paper_policy_hash"],
            "risk_profile_hash": manifest["risk_profile_hash"],
            "ledger_epoch": manifest["ledger_epoch"],
            "ledger_identity": None,
            "risk_continuity_artifact_sha256": continuity["artifact_sha256"],
        }
        binding_calls = 0

        def launch(command: list[str], **_: object) -> StubProcess:
            launches.append(command)
            target = resolve_paper_profile(str(manifest["target_profile"]))
            target_ledger = PaperLedger(
                Path(str(manifest["ledger_path"])), epoch_id=str(manifest["ledger_epoch"]),
                policy=target.policy, risk=target.risk,
            )
            binding["ledger_identity"] = target_ledger.risk_continuity_boundary()["ledger_identity"]
            target_ledger.publish_risk_continuity_anchor()
            target_ledger.close()
            Path(str(manifest["audit_root"])).mkdir(parents=True, exist_ok=True)
            return StubProcess()

        def http(url: str, *, method: str = "GET", **_: object) -> dict[str, object]:
            nonlocal binding_calls
            if url.endswith("/api/runtime-binding"):
                binding_calls += 1
                state = json.loads(manifest_path.with_name("state.json").read_text(encoding="utf-8"))
                if binding_calls == 1:
                    self.assertEqual(state["stage"], "TARGET_PROCESS_CREATED")
                else:
                    self.assertIn(
                        state["stage"],
                        {"AUTOSTARTING_TARGET", "TARGET_ACTIVE_FLAT_BLOCKED"},
                    )
                    self.assertEqual(state["target_pid"], StubProcess.pid)
                    self.assertEqual(state["target_runtime_pid"], binding["pid"])
                    self.assertEqual(state["target_runtime_binding"], binding)
                return dict(binding)
            if method == "POST":
                return {"accepted": True}
            if url.endswith("/api/lane-iii/paper"):
                return target_paper_status(str(manifest["target_profile"]))
            state = json.loads(manifest_path.with_name("state.json").read_text(encoding="utf-8"))
            return {"stage": "RUNNING" if state["stage"] == "AUTOSTARTING_TARGET" else "READY", "action_token": "stub-token"}

        result = supervise(
            manifest_path, 2_147_483_647, timeout_seconds=1, poll_seconds=0.001,
            pid_probe=lambda _: False, port_probe=lambda: True, wait=lambda _: None,
            http_json=http, launch_child=launch,
        )
        if result == 0:
            state = json.loads(
                manifest_path.with_name("state.json").read_text(encoding="utf-8"),
            )
            self.assertEqual(state["stage"], "RUNNING")
            self.assertEqual(state["target_pid"], StubProcess.pid)
            self.assertEqual(state["target_runtime_pid"], binding["pid"])
            self.assertEqual(state["target_runtime_binding"], binding)
        return result

    def target_launch_fixture(
        self,
        manifest_path: Path,
        manifest: dict[str, object],
        process: object,
    ) -> tuple[dict[str, object], object]:
        continuity = read_risk_continuity_artifact(
            manifest_path.with_name("risk-continuity.json"),
        )
        binding: dict[str, object] = {
            "pid": int(getattr(process, "pid")) + 1,
            "parent_pid": getattr(process, "pid"),
            "ledger": manifest["ledger_path"],
            "audit": manifest["audit_root"],
            "git_sha": manifest["git_sha"],
            "entry_profile_version": manifest["target_profile"],
            "paper_policy_hash": manifest["paper_policy_hash"],
            "risk_profile_hash": manifest["risk_profile_hash"],
            "ledger_epoch": manifest["ledger_epoch"],
            "ledger_identity": None,
            "risk_continuity_artifact_sha256": continuity["artifact_sha256"],
        }

        def launch(*_args: object, **_kwargs: object) -> object:
            target = resolve_paper_profile(str(manifest["target_profile"]))
            target_ledger = PaperLedger(
                Path(str(manifest["ledger_path"])),
                epoch_id=str(manifest["ledger_epoch"]),
                policy=target.policy,
                risk=target.risk,
            )
            binding["ledger_identity"] = target_ledger.risk_continuity_boundary()[
                "ledger_identity"
            ]
            target_ledger.publish_risk_continuity_anchor()
            target_ledger.close()
            Path(str(manifest["audit_root"])).mkdir(parents=True, exist_ok=True)
            return process

        return binding, launch

    def test_prepares_fresh_isolated_run_then_requests_controlled_shutdown(self) -> None:
        result = self.service.start("switch-request-0001", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertEqual(result["target_profile"], "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        status = self.service.status()
        self.assertEqual(status["stage"], "SHUTDOWN_REQUESTED")
        self.assertEqual(status["runtime_root"], str((self.root / "runtime").resolve()))
        self.assertEqual(len(self.launched), 1)
        manifest_path, parent_pid = self.launched[0]
        self.assertEqual(parent_pid, 2_147_483_647)
        manifest = _manifest(manifest_path)
        self.assertEqual(manifest["target_profile"], "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertEqual(manifest["paper_policy_hash"], FIVE_MINUTE_PROFILE.policy.configuration_hash)
        self.assertEqual(manifest["risk_profile_hash"], FIVE_MINUTE_PROFILE.risk.configuration_hash)
        self.assertFalse(Path(str(manifest["ledger_path"])).exists())
        self.assertIn("profiles", Path(str(manifest["ledger_path"])).parts)
        self.assertTrue(str(manifest["ledger_epoch"]).startswith("L3G-PAPER-EPOCH-BEELZEBUB_FIVE_MINUTE_BIAS_V1-"))
        continuity = read_risk_continuity_artifact(
            manifest_path.with_name("risk-continuity.json"),
            operation_id=str(manifest["operation_id"]),
            target_profile="BEELZEBUB_FIVE_MINUTE_BIAS_V1",
        )
        self.assertEqual(continuity["source_profile"], "BEELZEBUB_SCALPER_V2")

    def test_requested_selection_is_separate_from_last_established_run(self) -> None:
        runtime_root = self.root / "remembered-runtime"
        current_run = runtime_root / "profiles" / "beelzebub_scalper_v2" / "runs" / "current"
        current_ledger = PaperLedger(
            current_run / "hot" / "lane_iii_paper.sqlite3",
            epoch_id="L3G-PAPER-EPOCH-CURRENT-SCALPER",
        )
        current_ledger_identity = current_ledger.risk_continuity_boundary()["ledger_identity"]
        current_ledger.publish_risk_continuity_anchor()
        current_ledger.close()
        (current_run / "audit").mkdir(parents=True, exist_ok=True)
        current_binding = {
            "ledger": str(current_run / "hot" / "lane_iii_paper.sqlite3"),
            "audit": str(current_run / "audit"),
            "git_sha": "a" * 40,
            "entry_profile_version": "BEELZEBUB_SCALPER_V2",
            "ledger_epoch": "L3G-PAPER-EPOCH-CURRENT-SCALPER",
            "ledger_identity": current_ledger_identity,
        }
        service = PaperProfileSwitchService(
            current_profile=SCALPER_PROFILE,
            paper_status=lambda: flat_status(), flatten_and_disarm=lambda: {},
            verifier_status=lambda: {"status": "PASS"}, request_shutdown=lambda: None,
            runtime_root=runtime_root, project_root=self.root,
            python_executable=Path(__file__), git_sha="a" * 40,
            parent_pid=2_147_483_647, launch_supervisor=lambda *_: None,
            current_runtime_binding=current_binding,
        )
        service.start("switch-request-remember-source", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        assert service._thread is not None
        service._thread.join(timeout=1)
        selection = service.status()["selection"]
        self.assertEqual(selection["requested"]["profile"], "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertEqual(selection["established"]["profile"], "BEELZEBUB_SCALPER_V2")
        self.assertEqual(selection["established"]["ledger_path"], str(Path(current_binding["ledger"]).resolve()))

        remembered = remembered_profile_selection(runtime_root, git_sha="a" * 40)
        assert remembered is not None
        self.assertEqual(remembered["ledger_identity"], current_ledger_identity)

        manifest_path = Path(str(service.status()["manifest_path"]))
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["current_profile"] = "NY_HIGH_CONFLUENCE_COMMISSIONING_V1"
        manifest.pop("manifest_sha256")
        manifest["manifest_sha256"] = sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(
            RuntimeError, "PROFILE_SELECTION_ESTABLISHED_OPERATION_INVALID",
        ):
            remembered_profile_selection(runtime_root, git_sha="a" * 40)

    def test_post_startup_ledger_identity_is_bound_before_first_switch(self) -> None:
        runtime_root = self.root / "runtime"
        current_run = runtime_root / "profiles" / "beelzebub_scalper_v2" / "runs" / "current"
        ledger_path = current_run / "hot" / "lane_iii_paper.sqlite3"
        audit_root = current_run / "audit"
        ledger = PaperLedger(
            ledger_path, epoch_id="L3G-PAPER-EPOCH-CURRENT-SCALPER",
        )
        identity = ledger.risk_continuity_boundary()["ledger_identity"]
        ledger.publish_risk_continuity_anchor()
        ledger.close()
        audit_root.mkdir(parents=True)
        incomplete_binding = {
            "ledger": str(ledger_path), "audit": str(audit_root),
            "git_sha": "a" * 40,
            "entry_profile_version": "BEELZEBUB_SCALPER_V2",
            "ledger_epoch": "L3G-PAPER-EPOCH-CURRENT-SCALPER",
        }
        shutdown = threading.Event()
        service = PaperProfileSwitchService(
            current_profile=SCALPER_PROFILE,
            paper_status=lambda: flat_status(), flatten_and_disarm=lambda: {},
            verifier_status=lambda: {"status": "PASS"}, request_shutdown=shutdown.set,
            runtime_root=runtime_root, project_root=self.root,
            python_executable=Path(__file__), git_sha="a" * 40,
            parent_pid=2_147_483_647, launch_supervisor=lambda *_: None,
            current_runtime_binding=incomplete_binding,
        )
        service.bind_current_runtime_binding({**incomplete_binding, "ledger_identity": identity})
        service.start("switch-request-post-startup-binding", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        assert service._thread is not None
        service._thread.join(timeout=1)
        self.assertTrue(shutdown.is_set())
        state = service.status()
        self.assertEqual(state["stage"], "SHUTDOWN_REQUESTED")
        self.assertEqual(state["selection"]["established"]["ledger_identity"], identity)

    def test_inherited_non_idle_operation_accepts_one_exact_startup_binding(self) -> None:
        runtime_root = self.root / "runtime"
        operation_id = "profile-switch-" + "1" * 32
        state_path = runtime_root / "profile-switch" / "operations" / operation_id / "state.json"
        state_path.parent.mkdir(parents=True)
        state_path.write_text(json.dumps({
            "schema": "lane-iii-paper-profile-switch-v1",
            "operation_id": operation_id,
            "request_id": "switch-request-inherited-binding",
            "stage": "AUTOSTARTING_TARGET",
            "in_progress": True,
            "current_profile": "BEELZEBUB_SCALPER_V2",
            "target_profile": "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
            "blockers": [],
            "updated_at": "2026-09-05T00:00:00Z",
        }), encoding="utf-8")
        binding = {
            "ledger": str(runtime_root / "profiles" / "target" / "paper.sqlite3"),
            "audit": str(runtime_root / "profiles" / "target" / "audit"),
            "git_sha": "a" * 40,
            "entry_profile_version": "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
            "ledger_epoch": "L3G-PAPER-EPOCH-TARGET",
        }
        with patch.dict("os.environ", {"BEELZEBUB_PROFILE_SWITCH_OPERATION": operation_id}):
            service = PaperProfileSwitchService(
                current_profile=FIVE_MINUTE_PROFILE,
                paper_status=lambda: flat_status(), flatten_and_disarm=lambda: {},
                verifier_status=lambda: {"status": "PASS"}, request_shutdown=lambda: None,
                runtime_root=runtime_root, project_root=self.root,
                python_executable=Path(__file__), git_sha="a" * 40,
                current_runtime_binding=binding,
            )
        resolved = {**binding, "ledger_identity": "l3g-ledger-" + "2" * 32}
        service.bind_current_runtime_binding(resolved)
        self.assertEqual(service._current_runtime_binding, resolved)
        with self.assertRaisesRegex(RuntimeError, "PROFILE_SWITCH_RUNTIME_BINDING_TOO_LATE"):
            service.bind_current_runtime_binding({
                **resolved, "ledger_identity": "l3g-ledger-" + "3" * 32,
            })

    def test_invalid_selection_state_terminally_blocks_preparation(self) -> None:
        selection_path = self.root / "runtime" / "profile-switch" / "profile-selection.json"
        selection_path.parent.mkdir(parents=True, exist_ok=True)
        selection_path.write_text("{invalid", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "PROFILE_SELECTION_STATE_INVALID"):
            self.service.start(
                "switch-request-invalid-selection",
                "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
            )
        state = self.service.status()
        self.assertEqual(state["stage"], "BLOCKED_SAFE")
        self.assertFalse(state["in_progress"])
        self.assertEqual(state["blockers"], ["PROFILE_SELECTION_STATE_INVALID"])
        self.assertFalse(self.shutdown.is_set())
        self.assertEqual(self.launched, [])
        self.assertEqual(selection_path.read_text(encoding="utf-8"), "{invalid")

    def test_requested_selection_write_failure_terminally_blocks_preparation(self) -> None:
        with patch(
            "src.l3g_paper.profile_switch._record_requested_selection",
            side_effect=OSError("fixture write refusal"),
        ):
            with self.assertRaisesRegex(
                RuntimeError, "PROFILE_SELECTION_REQUEST_PERSISTENCE_FAILED",
            ):
                self.service.start(
                    "switch-request-selection-write-fails",
                    "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
                )
        state = self.service.status()
        self.assertEqual(state["stage"], "BLOCKED_SAFE")
        self.assertFalse(state["in_progress"])
        self.assertEqual(
            state["blockers"], ["PROFILE_SELECTION_REQUEST_PERSISTENCE_FAILED"],
        )
        self.assertFalse(self.shutdown.is_set())
        self.assertEqual(self.launched, [])

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

    def test_shutdown_receipt_requires_typed_nonmissing_tip_proof(self) -> None:
        self.service.start("switch-request-0021", "NY_HIGH_CONFLUENCE_COMMISSIONING_V1")
        self.assertTrue(self.shutdown.wait(1))
        receipt = clean_shutdown_receipt()
        for key in ("expected_tip_sequence", "expected_tip_hash", "durable_tip_sequence", "durable_tip_hash"):
            receipt.pop(key)
        self.service.record_shutdown_receipt(receipt)
        self.assertEqual(self.service.status()["stage"], "BLOCKED_SAFE")
        self.assertEqual(self.service.status()["blockers"], ["CURRENT_PROFILE_CONTROLLED_SHUTDOWN_UNPROVEN"])

    def test_manifest_integrity_is_fail_closed(self) -> None:
        self.service.start("switch-request-0006", "NY_HIGH_CONFLUENCE_COMMISSIONING_V1")
        self.assertTrue(self.shutdown.wait(1))
        manifest_path = Path(str(self.service.status()["manifest_path"]))
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
        value["target_profile"] = "BEELZEBUB_SCALPER_V2"
        manifest_path.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "MANIFEST_INTEGRITY"):
            _manifest(manifest_path)

    def test_profile_switch_preserves_account_financials_and_profile_specific_counters(self) -> None:
        context = PaperSessionResolver().resolve("2026-09-03T14:00:00Z", generation=2).context
        self.status_value["risk_continuity"] = {
            **self.status_value["risk_continuity"],  # type: ignore[dict-item]
            "trade_dates": [{
                "trade_date": context.trade_date,
                "realized_pnl": "-40",
                "unrealized_pnl": "0",
                "entry_count": 7,
            }],
            "profile_trade_dates": [{
                "profile": "BEELZEBUB_SCALPER_V2",
                "trade_date": context.trade_date,
                "entry_count": 7,
                "consecutive_losses": 2,
            }],
            "sessions": [{
                "profile": "BEELZEBUB_SCALPER_V2",
                "session_kind": context.session_kind.value,
                "session_family": context.session_family.value,
                "session_id": context.session_id,
                "trade_date": context.trade_date,
                "session_profile_hash": context.session_profile_hash,
                "session_generation": context.session_generation,
                "entry_count": 7,
                "realized_pnl": "-40",
            }],
            "entry_execution_ids": [f"prior-entry-{index}" for index in range(7)],
            "exit_execution_ids": [f"prior-exit-{index}" for index in range(7)],
        }
        self.service.start("switch-request-0008", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        manifest_path = Path(str(self.service.status()["manifest_path"]))
        continuity = read_risk_continuity_artifact(manifest_path.with_name("risk-continuity.json"))

        target_path = self.root / "target" / "paper.sqlite3"
        ledger = PaperLedger(
            target_path, epoch_id="L3G-PAPER-EPOCH-CONTINUITY-TARGET",
            policy=FIVE_MINUTE_PROFILE.policy, risk=FIVE_MINUTE_PROFILE.risk,
        )
        runtime = LaneIIIPaperRuntime(ledger, risk_continuity=continuity["snapshot"])  # type: ignore[arg-type]
        runtime._set_session_context(context, reason="PROFILE_SWITCH_TARGET")
        status = runtime.status()
        self.assertEqual(status["trade_date_entry_count"], 0)
        self.assertEqual(status["account_trade_date_entry_count"], 7)
        self.assertEqual(status["daily_realized_pnl"], "-40")
        self.assertEqual(status["consecutive_losses"], 0)
        self.assertEqual(status["session_pnl"], "-40")
        self.assertIn("prior-entry-0", status["risk_continuity"]["entry_execution_ids"])
        profile_buckets = {
            item["profile"]: item for item in status["risk_continuity"]["profile_trade_dates"]
        }
        self.assertEqual(profile_buckets["BEELZEBUB_SCALPER_V2"]["entry_count"], 7)
        self.assertEqual(profile_buckets["BEELZEBUB_SCALPER_V2"]["consecutive_losses"], 2)
        self.assertEqual(profile_buckets["BEELZEBUB_FIVE_MINUTE_BIAS_V1"]["entry_count"], 0)
        ledger.close()

        restored_ledger = PaperLedger(
            self.root / "restored" / "paper.sqlite3",
            epoch_id="L3G-PAPER-EPOCH-CONTINUITY-RESTORED",
            policy=SCALPER_PROFILE.policy, risk=SCALPER_PROFILE.risk,
        )
        restored = LaneIIIPaperRuntime(
            restored_ledger, risk_continuity=status["risk_continuity"],  # type: ignore[arg-type]
        )
        restored._set_session_context(context, reason="PROFILE_SWITCH_BACK")
        self.assertEqual(restored.status()["trade_date_entry_count"], 7)
        self.assertEqual(restored.status()["consecutive_losses"], 2)
        restored_ledger.close()

    def test_conflicting_request_is_refused_and_status_reloads_supervisor_failure(self) -> None:
        self.service.start("switch-request-0009", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        with self.assertRaisesRegex(RuntimeError, "PROFILE_SWITCH_ALREADY_IN_PROGRESS"):
            self.service.start("switch-request-0010", "NY_HIGH_CONFLUENCE_COMMISSIONING_V1")
        self.assertTrue(self.shutdown.wait(1))
        state_path = Path(str(self.service.status()["manifest_path"])).with_name("state.json")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state.update({"stage": "BLOCKED_SAFE", "in_progress": False, "blockers": ["TARGET_AUTOSTART_BLOCKED"]})
        state_path.write_text(json.dumps(state), encoding="utf-8")
        self.assertEqual(self.service.status()["blockers"], ["TARGET_AUTOSTART_BLOCKED"])

    def test_durable_shutdown_requested_stage_refuses_a_second_operation_after_worker_exit(self) -> None:
        self.service.start("switch-request-0014", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        assert self.service._thread is not None
        self.service._thread.join(timeout=1)
        self.assertFalse(self.service._thread.is_alive())
        self.assertEqual(self.service.status()["stage"], "SHUTDOWN_REQUESTED")

        with self.assertRaisesRegex(RuntimeError, "PROFILE_SWITCH_ALREADY_IN_PROGRESS"):
            self.service.start("switch-request-0015", "NY_HIGH_CONFLUENCE_COMMISSIONING_V1")

    def test_inherited_autostarting_operation_refuses_a_second_operation(self) -> None:
        self.service.start("switch-request-0016", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        state_path = Path(str(self.service.status()["manifest_path"])).with_name("state.json")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state.update({"stage": "AUTOSTARTING_TARGET", "in_progress": True})
        state_path.write_text(json.dumps(state), encoding="utf-8")

        with patch.dict("os.environ", {"BEELZEBUB_PROFILE_SWITCH_OPERATION": str(state["operation_id"])}):
            inherited = PaperProfileSwitchService(
                current_profile=FIVE_MINUTE_PROFILE,
                paper_status=lambda: flat_status(), flatten_and_disarm=lambda: {},
                verifier_status=lambda: {"status": "PASS"}, request_shutdown=lambda: None,
                runtime_root=self.root / "runtime", project_root=self.root,
                python_executable=Path(__file__), git_sha="a" * 40,
            )
        replay = inherited.start("switch-request-0016", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertEqual(replay["stage"], "AUTOSTARTING_TARGET")
        with self.assertRaisesRegex(RuntimeError, "PROFILE_SWITCH_ALREADY_IN_PROGRESS"):
            inherited.start("switch-request-0017", "NY_HIGH_CONFLUENCE_COMMISSIONING_V1")

    def test_missing_inherited_state_is_reported_and_remains_fail_closed(self) -> None:
        with patch.dict("os.environ", {"BEELZEBUB_PROFILE_SWITCH_OPERATION": "missing-operation"}):
            inherited = PaperProfileSwitchService(
                current_profile=FIVE_MINUTE_PROFILE,
                paper_status=lambda: flat_status(), flatten_and_disarm=lambda: {},
                verifier_status=lambda: {"status": "PASS"}, request_shutdown=lambda: None,
                runtime_root=self.root / "runtime", project_root=self.root,
                python_executable=Path(__file__), git_sha="a" * 40,
            )
        self.assertEqual(inherited.status()["stage"], "BLOCKED_SAFE")
        self.assertEqual(inherited.status()["blockers"], ["PROFILE_SWITCH_STATE_INVALID"])
        with self.assertRaisesRegex(RuntimeError, "PROFILE_SWITCH_STATE_INVALID"):
            inherited.start("switch-request-0019", "NY_HIGH_CONFLUENCE_COMMISSIONING_V1")

    def test_disappearing_operation_state_replaces_cached_status_with_a_blocker(self) -> None:
        self.service.start("switch-request-0020", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        state_path = Path(str(self.service.status()["manifest_path"])).with_name("state.json")
        state_path.unlink()

        status = self.service.status()
        self.assertEqual(status["stage"], "BLOCKED_SAFE")
        self.assertEqual(status["blockers"], ["PROFILE_SWITCH_STATE_INVALID"])

    def test_target_process_shutdown_cannot_overwrite_originating_switch_outcome(self) -> None:
        self.service.start("switch-request-0011", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        state_path = Path(str(self.service.status()["manifest_path"])).with_name("state.json")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state.update({"stage": "RUNNING", "in_progress": False, "blockers": []})
        state_path.write_text(json.dumps(state), encoding="utf-8")

        target = PaperProfileSwitchService(
            current_profile=FIVE_MINUTE_PROFILE,
            paper_status=lambda: flat_status(), flatten_and_disarm=lambda: {},
            verifier_status=lambda: {"status": "PASS"}, request_shutdown=lambda: None,
            runtime_root=self.root / "runtime", project_root=self.root,
            python_executable=Path(__file__), git_sha="a" * 40,
        )
        target._state_path = state_path
        target._state = dict(state)
        target.record_shutdown_receipt({"clean_shutdown": False})
        self.assertEqual(json.loads(state_path.read_text(encoding="utf-8"))["stage"], "RUNNING")

    def test_supervisor_refuses_a_tampered_risk_continuity_artifact_before_launch(self) -> None:
        self.service.start("switch-request-0012", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        manifest_path = Path(str(self.service.status()["manifest_path"]))
        self.service.record_shutdown_receipt(clean_shutdown_receipt())
        continuity_path = manifest_path.with_name("risk-continuity.json")
        artifact = json.loads(continuity_path.read_text(encoding="utf-8"))
        artifact["target_profile"] = "BEELZEBUB_SCALPER_V2"
        continuity_path.write_text(json.dumps(artifact), encoding="utf-8")

        self.assertEqual(supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.01,
            poll_seconds=0.001, pid_probe=lambda _: False,
            port_probe=lambda: True, wait=lambda _: None,
        ), 4)
        state = json.loads(manifest_path.with_name("state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["stage"], "BLOCKED_SAFE")
        self.assertEqual(state["blockers"], ["RISK_CONTINUITY_ARTIFACT_INTEGRITY_FAILED"])

    def test_supervisor_refuses_a_pid_that_is_not_bound_by_the_manifest(self) -> None:
        self.service.start("switch-request-0018", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        manifest_path = Path(str(self.service.status()["manifest_path"]))
        self.service.record_shutdown_receipt(clean_shutdown_receipt())

        self.assertEqual(supervise(manifest_path, 2_147_483_646, timeout_seconds=0.01), 2)
        state = json.loads(manifest_path.with_name("state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["blockers"], ["PROFILE_SWITCH_PARENT_PID_MISMATCH"])

    def test_supervisor_revalidates_a_forged_closed_state_receipt(self) -> None:
        self.service.start("switch-request-0022", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        manifest_path = Path(str(self.service.status()["manifest_path"]))
        state_path = manifest_path.with_name("state.json")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        receipt = clean_shutdown_receipt()
        receipt.pop("expected_tip_sequence")
        state.update({"stage": "CURRENT_PROFILE_CLOSED", "in_progress": False, "shutdown_receipt": receipt})
        state_path.write_text(json.dumps(state), encoding="utf-8")

        self.assertEqual(supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.01,
            poll_seconds=0.001, pid_probe=lambda _: False,
            port_probe=lambda: True, wait=lambda _: None,
        ), 3)
        self.assertEqual(
            json.loads(state_path.read_text(encoding="utf-8"))["blockers"],
            ["CURRENT_PROFILE_CONTROLLED_SHUTDOWN_UNPROVEN"],
        )

    def test_supervisor_preserves_malformed_state_and_appends_truthful_blocker(self) -> None:
        manifest_path, _ = self.supervisor_ready_manifest("switch-request-malformed-state")
        state_path = manifest_path.with_name("state.json")
        damaged = b'{"schema":"truncated"'
        state_path.write_bytes(damaged)

        self.assertEqual(supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.01, poll_seconds=0.001,
            pid_probe=lambda _: False, port_probe=lambda: True, wait=lambda _: None,
        ), 3)
        self.assertEqual(state_path.read_bytes(), damaged)
        audit = [
            json.loads(line)
            for line in manifest_path.with_name("supervisor-audit.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(audit[-1]["stage"], "BLOCKED_SAFE")
        self.assertEqual(audit[-1]["blockers"], ["PROFILE_SWITCH_STATE_INVALID"])

    def test_supervisor_waits_for_late_receipt_after_process_release(self) -> None:
        self.service.start("switch-request-delayed-receipt", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        manifest_path = Path(str(self.service.status()["manifest_path"]))
        waits = 0

        def publish_receipt(_: float) -> None:
            nonlocal waits
            waits += 1
            if waits == 1:
                self.service.record_shutdown_receipt(clean_shutdown_receipt())

        result = supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.2, poll_seconds=0.001,
            pid_probe=lambda _: False, port_probe=lambda: True, wait=publish_receipt,
        )
        self.assertEqual(result, 4)
        self.assertGreaterEqual(waits, 1)
        state = json.loads(manifest_path.with_name("state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["blockers"], ["TARGET_RUNTIME_PATH_VALIDATION_FAILED"])

    def test_supervisor_requires_receipt_process_exit_and_port_release_together(self) -> None:
        manifest_path, _ = self.supervisor_ready_manifest("switch-request-slow-release")
        pid_states = iter((True, False, False))
        port_states = iter((False, False, True))
        launches: list[list[str]] = []

        class FailedStubProcess:
            pid = 8080

            @staticmethod
            def poll() -> int:
                return 1

        result = supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.2, poll_seconds=0.001,
            pid_probe=lambda _: next(pid_states, False),
            port_probe=lambda: next(port_states, True),
            wait=lambda _: None,
            launch_child=lambda command, **_: launches.append(command) or FailedStubProcess(),  # type: ignore[arg-type]
        )
        self.assertEqual(result, 12)
        self.assertEqual(len(launches), 1)
        state = json.loads(manifest_path.with_name("state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["stage"], "TARGET_CLEANUP_UNPROVEN")
        self.assertIn("TARGET_PROCESS_EXITED_DURING_STARTUP", state["blockers"])
        self.assertFalse(state["target_cleanup"]["target_runtime_pid_absent"])

    def test_occupied_control_port_blocks_before_target_creation(self) -> None:
        manifest_path, _ = self.supervisor_ready_manifest("switch-request-occupied-port")
        launches: list[list[str]] = []
        result = supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.01, poll_seconds=0.001,
            pid_probe=lambda _: False, port_probe=lambda: False, wait=lambda _: None,
            launch_child=lambda command, **_: launches.append(command),  # type: ignore[arg-type,return-value]
        )
        self.assertEqual(result, 3)
        self.assertEqual(launches, [])
        state = json.loads(manifest_path.with_name("state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["blockers"], ["CURRENT_PROFILE_RESOURCES_NOT_RELEASED"])

    def test_late_receipt_cannot_overwrite_terminal_supervisor_failure(self) -> None:
        self.service.start("switch-request-terminal-late", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        manifest_path = Path(str(self.service.status()["manifest_path"]))
        self.assertEqual(supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.01, poll_seconds=0.001,
            pid_probe=lambda _: False, port_probe=lambda: True, wait=lambda _: None,
        ), 3)
        self.service.record_shutdown_receipt(clean_shutdown_receipt())
        state = json.loads(manifest_path.with_name("state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["stage"], "BLOCKED_SAFE")
        self.assertEqual(state["blockers"], ["CURRENT_PROFILE_CONTROLLED_SHUTDOWN_UNPROVEN"])

    def test_target_is_created_once_and_established_selection_survives_restart(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest("switch-request-establish-target")
        launches: list[list[str]] = []
        self.assertEqual(self.complete_supervisor_with_stubs(manifest_path, manifest, launches), 0)
        self.assertEqual(len(launches), 1)
        self.assertEqual(supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.01,
            pid_probe=lambda _: False, port_probe=lambda: True,
        ), 0)
        self.assertEqual(len(launches), 1)
        remembered = remembered_profile_selection(
            self.root / "runtime", git_sha=str(manifest["git_sha"]),
        )
        assert remembered is not None
        self.assertEqual(remembered["profile"], "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertEqual(remembered["ledger_path"], manifest["ledger_path"])
        self.assertEqual(remembered["ledger_epoch"], manifest["ledger_epoch"])

        defaults = CopyTradeConfig()
        from dataclasses import replace
        config = replace(
            defaults,
            storage=replace(defaults.storage, cold_root=self.root / "restart-cold"),
            artifacts=replace(defaults.artifacts, database_path=self.root / "restart-hot" / "copytrade.sqlite3"),
        )
        environment = {
            "BEELZEBUB_PROFILE_SWITCH_ROOT": str(self.root / "runtime"),
            "BEELZEBUB_PROFILE_SWITCH_OPERATION": "",
            "BEELZEBUB_L3G_PAPER_PROFILE": "",
            "BEELZEBUB_L3G_PAPER_LEDGER": "",
            "BEELZEBUB_L3G_PAPER_LEDGER_EPOCH": "",
            "BEELZEBUB_LEDGER_AUDIT_ROOT": "",
            "BEELZEBUB_RISK_CONTINUITY_PATH": "",
            "BEELZEBUB_GIT_SHA": str(manifest["git_sha"]),
        }
        with patch.dict("os.environ", environment):
            def reached_after_binding(_: PaperLedger) -> LaneIIIPaperRuntime:
                raise RuntimeError("INERT_TEST_REACHED_AFTER_RUNTIME_BINDING")

            app = create_control_center_app(
                config, lane_iii_paper_factory=reached_after_binding,
            )
            async def enter_lifespan() -> None:
                async with app.router.lifespan_context(app):
                    pass
            with self.assertRaisesRegex(
                RuntimeError, "INERT_TEST_REACHED_AFTER_RUNTIME_BINDING",
            ):
                asyncio.run(enter_lifespan())
        route = next(route for route in app.routes if getattr(route, "path", None) == "/api/runtime-binding")
        binding = asyncio.run(route.endpoint())
        self.assertEqual(binding["entry_profile_version"], "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertEqual(binding["ledger"], manifest["ledger_path"])
        self.assertEqual(binding["ledger_epoch"], manifest["ledger_epoch"])
        self.assertEqual(binding["ledger_identity"], remembered["ledger_identity"])
        self.assertEqual(
            app.state.paper_profile_switch._current_runtime_binding["ledger_identity"],
            remembered["ledger_identity"],
        )
        self.assertEqual(binding["profile_selection_source"], "REMEMBERED_ESTABLISHED_RUN")
        self.assertEqual(binding["pid"], os.getpid())
        self.assertEqual(binding["parent_pid"], os.getppid())
        app.state.paper_autostart.stop()

    def test_atomic_launch_claim_refuses_concurrent_duplicate_supervisor(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest("switch-request-duplicate-claim")
        self.assertTrue(_claim_target_launch(manifest_path, manifest))
        launches: list[list[str]] = []
        self.assertEqual(supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.2, poll_seconds=0.001,
            pid_probe=lambda _: False, port_probe=lambda: True, wait=lambda _: None,
            launch_child=lambda command, **_: launches.append(command),  # type: ignore[arg-type,return-value]
        ), 10)
        self.assertEqual(launches, [])
        audit = manifest_path.with_name("supervisor-audit.jsonl").read_text(encoding="utf-8")
        self.assertIn("DUPLICATE_TARGET_LAUNCH_REFUSED", audit)

    def test_target_process_failure_is_distinct_from_creation(self) -> None:
        manifest_path, _ = self.supervisor_ready_manifest("switch-request-target-fails")
        notices: list[str] = []

        class FailedProcess:
            pid = 31337

            @staticmethod
            def poll() -> int:
                return 1

        self.assertEqual(supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.2, poll_seconds=0.001,
            pid_probe=lambda _: False, port_probe=lambda: True, wait=lambda _: None,
            launch_child=lambda *_, **__: FailedProcess(),  # type: ignore[arg-type]
            notify_operator=notices.append,
        ), 12)
        state = json.loads(manifest_path.with_name("state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["stage"], "TARGET_CLEANUP_UNPROVEN")
        self.assertEqual(state["target_pid"], 31337)
        self.assertIn("TARGET_PROCESS_EXITED_DURING_STARTUP", state["blockers"])
        self.assertIn("TARGET_PROCESS_EXIT_UNPROVEN", state["blockers"])
        self.assertIsNone(state["target_cleanup"]["target_runtime_pid"])
        self.assertFalse(state["target_cleanup"]["target_runtime_pid_absent"])
        self.assertEqual(len(notices), 1)
        self.assertIn("BEELZEBUB_FIVE_MINUTE_BIAS_V1", notices[0])
        self.assertIn("No automatic retry or fallback profile was started", notices[0])

    def test_binding_timeout_cannot_prove_unknown_runtime_child_cleanup(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-binding-timeout-cleanup",
        )

        class Process:
            pid = 31340

            def __init__(self) -> None:
                self.returncode: int | None = None
                self.terminate_calls = 0
                self.kill_calls = 0

            def poll(self) -> int | None:
                return self.returncode

            def terminate(self) -> None:
                self.terminate_calls += 1
                self.returncode = 0

            def wait(self, *, timeout: float) -> int:
                del timeout
                assert self.returncode is not None
                return self.returncode

            def kill(self) -> None:
                self.kill_calls += 1
                self.returncode = -9

        process = Process()
        _binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )
        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.02,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=lambda *_args, **_kwargs: {
                "entry_profile_version": "BEELZEBUB_SCALPER_V2",
            },
        )
        self.assertEqual(result, 12)
        self.assertEqual(process.terminate_calls, 1)
        self.assertEqual(process.kill_calls, 0)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(state["stage"], "TARGET_CLEANUP_UNPROVEN")
        self.assertFalse(state["target_cleanup"]["safe_terminal_proven"])
        self.assertFalse(state["target_cleanup"]["target_runtime_pid_absent"])

    def test_binding_timeout_kills_launcher_but_unknown_child_remains_unproven(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-binding-kill-fallback",
        )

        class Process:
            pid = 31341

            def __init__(self) -> None:
                self.returncode: int | None = None
                self.terminate_calls = 0
                self.kill_calls = 0

            def poll(self) -> int | None:
                return self.returncode

            def terminate(self) -> None:
                self.terminate_calls += 1

            def wait(self, *, timeout: float) -> int:
                if self.returncode is None:
                    raise subprocess.TimeoutExpired("target", timeout)
                return self.returncode

            def kill(self) -> None:
                self.kill_calls += 1
                self.returncode = -9

        process = Process()
        _binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )
        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.02,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=lambda *_args, **_kwargs: {},
        )
        self.assertEqual(result, 12)
        self.assertEqual(process.terminate_calls, 1)
        self.assertEqual(process.kill_calls, 1)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertTrue(state["target_cleanup"]["kill_attempted"])
        self.assertEqual(state["stage"], "TARGET_CLEANUP_UNPROVEN")
        self.assertFalse(state["target_cleanup"]["target_runtime_pid_absent"])

    def test_exited_wrapper_with_occupied_target_port_is_not_safe(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-wrapper-exit-port-held",
        )

        class ExitedProcess:
            pid = 31342

            @staticmethod
            def poll() -> int:
                return 1

        _binding, launch = self.target_launch_fixture(
            manifest_path, manifest, ExitedProcess(),
        )
        port_calls = 0

        def port_probe() -> bool:
            nonlocal port_calls
            port_calls += 1
            return port_calls == 1

        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.02,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=port_probe,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
        )
        self.assertEqual(result, 12)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(state["stage"], "TARGET_CLEANUP_UNPROVEN")
        self.assertTrue(state["in_progress"])
        self.assertIn("TARGET_CONTROL_PORT_RELEASE_UNPROVEN", state["blockers"])

    def test_post_binding_exit_cannot_be_safe_without_flat_exposure_proof(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-post-binding-exit-unproven",
        )

        class ExitsAfterBinding:
            pid = 31343

            def __init__(self) -> None:
                self.poll_calls = 0

            def poll(self) -> int | None:
                self.poll_calls += 1
                return None if self.poll_calls == 1 else 1

        process = ExitsAfterBinding()
        binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )
        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.2,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=lambda *_args, **_kwargs: dict(binding),
        )
        self.assertEqual(result, 12)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(state["stage"], "TARGET_CLEANUP_UNPROVEN")
        self.assertTrue(state["target_cleanup"]["cleanup_proven"])
        self.assertFalse(state["target_cleanup"]["native_exposure_absent"])
        self.assertFalse(state["target_cleanup"]["safe_terminal_proven"])
        self.assertIn("TARGET_NATIVE_EXPOSURE_RELEASE_UNPROVEN", state["blockers"])

    def test_post_binding_exit_cannot_reuse_a_stale_flat_projection(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-post-binding-stale-flat",
        )

        class ExitsAfterFlatProjection:
            pid = 31349

            def __init__(self) -> None:
                self.poll_calls = 0

            def poll(self) -> int | None:
                self.poll_calls += 1
                return None if self.poll_calls <= 5 else 1

        process = ExitsAfterFlatProjection()
        binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )

        def http(
            url: str, *, method: str = "GET", **_kwargs: object,
        ) -> dict[str, object]:
            if url.endswith("/api/runtime-binding"):
                return dict(binding)
            if url.endswith("/api/lane-iii/paper"):
                return target_paper_status(
                    str(manifest["target_profile"]), operational=False,
                )
            if method == "POST":
                return {"accepted": True}
            return {"stage": "READY", "action_token": "target-token"}

        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.2,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=http,
        )
        self.assertEqual(result, 12)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(state["stage"], "TARGET_CLEANUP_UNPROVEN")
        self.assertNotIn("target_paper_status", state)
        self.assertFalse(state["target_cleanup"]["native_exposure_absent"])

    def test_raw_sim101_authority_is_required_before_autostart_post(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-authority-before-post",
            FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        )

        class ExitsAfterAuthorityRefusal:
            pid = 31350

            def __init__(self) -> None:
                self.alive = True

            def poll(self) -> int | None:
                return None if self.alive else 1

        process = ExitsAfterAuthorityRefusal()
        binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )
        post_calls = 0

        def http(
            url: str, *, method: str = "GET", **_kwargs: object,
        ) -> dict[str, object]:
            nonlocal post_calls
            if url.endswith("/api/runtime-binding"):
                return dict(binding)
            if method == "POST":
                post_calls += 1
                return {"accepted": True}
            if url.endswith("/api/lane-iii/paper"):
                return {
                    **target_paper_status(
                        FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
                        operational=False,
                    ),
                    "paper_account": "NOT_SIM101",
                }
            return {"stage": "READY", "action_token": "target-token"}

        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.2,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: setattr(process, "alive", False),
            launch_child=launch,  # type: ignore[arg-type]
            http_json=http,
        )
        self.assertEqual(result, 12)
        self.assertEqual(post_calls, 0)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(state["stage"], "TARGET_CLEANUP_UNPROVEN")
        self.assertIn("TARGET_NATIVE_EXPOSURE_RELEASE_UNPROVEN", state["blockers"])

    def test_binding_replacement_after_status_reads_blocks_autostart_post(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-replacement-before-post",
            FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        )

        class Process:
            pid = 31351

            def __init__(self) -> None:
                self.returncode: int | None = None

            def poll(self) -> int | None:
                return self.returncode

            def terminate(self) -> None:
                self.returncode = 0

            def wait(self, *, timeout: float) -> int:
                del timeout
                assert self.returncode is not None
                return self.returncode

            def kill(self) -> None:
                self.returncode = -9

        process = Process()
        binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )
        binding_calls = 0
        post_calls = 0

        def http(
            url: str, *, method: str = "GET", **_kwargs: object,
        ) -> dict[str, object]:
            nonlocal binding_calls, post_calls
            if url.endswith("/api/runtime-binding"):
                binding_calls += 1
                if binding_calls >= 3:
                    return {
                        **binding,
                        "ledger_identity": "l3g-ledger-" + "f" * 32,
                    }
                return dict(binding)
            if method == "POST":
                post_calls += 1
                return {"accepted": True}
            if url.endswith("/api/lane-iii/paper"):
                return target_paper_status(
                    FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
                    operational=False,
                )
            return {"stage": "READY", "action_token": "target-token"}

        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.2,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=http,
        )
        self.assertEqual(result, 12)
        self.assertEqual(post_calls, 0)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(state["stage"], "TARGET_CLEANUP_UNPROVEN")
        self.assertIn("TARGET_RUNTIME_BINDING_MISMATCH", state["blockers"])
        selection = json.loads(
            (self.root / "runtime" / "profile-switch" / "profile-selection.json").read_text(
                encoding="utf-8",
            ),
        )
        established = selection.get("established")
        self.assertFalse(
            isinstance(established, dict)
            and established.get("operation_id") == manifest["operation_id"],
        )

    def test_process_exit_after_auto_status_blocks_autostart_post(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-process-exit-before-post",
            FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        )

        class Process:
            pid = 31352

            def __init__(self) -> None:
                self.returncode: int | None = None

            def poll(self) -> int | None:
                return self.returncode

        process = Process()
        binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )
        post_calls = 0

        def http(
            url: str, *, method: str = "GET", **_kwargs: object,
        ) -> dict[str, object]:
            nonlocal post_calls
            if url.endswith("/api/runtime-binding"):
                return dict(binding)
            if method == "POST":
                post_calls += 1
                return {"accepted": True}
            if url.endswith("/api/lane-iii/paper"):
                return target_paper_status(
                    FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
                    operational=False,
                )
            process.returncode = 1
            return {"stage": "READY", "action_token": "target-token"}

        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.2,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=http,
        )
        self.assertEqual(result, 12)
        self.assertEqual(post_calls, 0)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(state["stage"], "TARGET_CLEANUP_UNPROVEN")
        self.assertIn("TARGET_PROCESS_EXITED_DURING_STARTUP", state["blockers"])
        self.assertFalse(state["target_cleanup"]["native_exposure_absent"])

    def test_terminal_snapshot_cannot_prove_safe_after_process_exit(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-terminal-status-then-exit",
        )

        class Process:
            pid = 31353

            def __init__(self) -> None:
                self.returncode: int | None = None

            def poll(self) -> int | None:
                return self.returncode

        process = Process()
        binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )

        def http(
            url: str, *, method: str = "GET", **_kwargs: object,
        ) -> dict[str, object]:
            if url.endswith("/api/runtime-binding"):
                return dict(binding)
            if method == "POST":
                return {"accepted": True}
            if url.endswith("/api/lane-iii/paper"):
                return target_paper_status(
                    str(manifest["target_profile"]), operational=False,
                )
            process.returncode = 1
            return {
                "stage": "BLOCKED",
                "in_progress": False,
                "blockers": ["NINJATRADER_CONNECTION_UNAVAILABLE"],
            }

        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.2,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=http,
        )
        self.assertEqual(result, 12)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(state["stage"], "TARGET_CLEANUP_UNPROVEN")
        self.assertFalse(state["target_cleanup"]["native_exposure_absent"])
        self.assertIn("TARGET_NATIVE_EXPOSURE_RELEASE_UNPROVEN", state["blockers"])

    def test_running_position_snapshot_is_not_persisted_after_process_exit(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-running-status-then-exit",
            FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        )

        class Process:
            pid = 31354

            def __init__(self) -> None:
                self.returncode: int | None = None

            def poll(self) -> int | None:
                return self.returncode

        process = Process()
        binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )

        def http(
            url: str, *, method: str = "GET", **_kwargs: object,
        ) -> dict[str, object]:
            if url.endswith("/api/runtime-binding"):
                return dict(binding)
            if method == "POST":
                return {"accepted": True}
            if url.endswith("/api/lane-iii/paper"):
                return target_paper_status(
                    FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
                    direction="LONG",
                )
            process.returncode = 1
            return {"stage": "RUNNING", "action_token": "target-token"}

        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.2,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=http,
        )
        self.assertEqual(result, 12)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertNotEqual(state["stage"], "RUNNING")
        selection = json.loads(
            (self.root / "runtime" / "profile-switch" / "profile-selection.json").read_text(
                encoding="utf-8",
            ),
        )
        established = selection.get("established")
        self.assertFalse(
            isinstance(established, dict)
            and established.get("operation_id") == manifest["operation_id"],
        )

    def test_autostart_post_requires_a_new_status_cycle(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-post-status-cycle",
            FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        )

        class Process:
            pid = 31355

            @staticmethod
            def poll() -> None:
                return None

        process = Process()
        binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )
        paper_calls = 0
        auto_calls = 0
        post_calls = 0

        def http(
            url: str, *, method: str = "GET", **_kwargs: object,
        ) -> dict[str, object]:
            nonlocal paper_calls, auto_calls, post_calls
            if url.endswith("/api/runtime-binding"):
                return dict(binding)
            if method == "POST":
                post_calls += 1
                return {"accepted": True}
            if url.endswith("/api/lane-iii/paper"):
                paper_calls += 1
                if paper_calls == 1:
                    return target_paper_status(
                        FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
                        operational=False,
                    )
                return target_paper_status(
                    FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
                    direction="SHORT",
                )
            auto_calls += 1
            if auto_calls == 1:
                return {
                    "stage": "BLOCKED",
                    "in_progress": False,
                    "blockers": ["PRE_ACTION_SNAPSHOT"],
                    "action_token": "target-token",
                }
            return {"stage": "RUNNING", "action_token": "target-token"}

        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.2,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=http,
        )
        self.assertEqual(result, 0)
        self.assertEqual(post_calls, 1)
        self.assertEqual(paper_calls, 2)
        self.assertEqual(auto_calls, 2)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(state["stage"], "RUNNING")
        self.assertEqual(state["target_paper_status"]["current_position"], "SHORT")

    def test_perpetual_auto_running_stays_flat_blocked_until_position_proven(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-perpetual-flat-then-positioned",
            FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        )

        class Process:
            pid = 31344
            terminate_calls = 0

            @staticmethod
            def poll() -> None:
                return None

            def terminate(self) -> None:
                self.terminate_calls += 1

        process = Process()
        binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )
        paper_calls = 0
        saw_flat_projection = False

        def http(url: str, *, method: str = "GET", **_kwargs: object) -> dict[str, object]:
            nonlocal paper_calls, saw_flat_projection
            if url.endswith("/api/runtime-binding"):
                return dict(binding)
            if method == "POST":
                return {"accepted": True}
            if url.endswith("/api/lane-iii/paper"):
                paper_calls += 1
                if paper_calls == 1:
                    return target_paper_status(
                        FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
                        flat_blocker="MARKET_DATA_CONNECTION_LOST",
                    )
                state = json.loads(
                    manifest_path.with_name("state.json").read_text(encoding="utf-8"),
                )
                saw_flat_projection = state["stage"] == "TARGET_ACTIVE_FLAT_BLOCKED"
                selection = json.loads(
                    (self.root / "runtime" / "profile-switch" / "profile-selection.json").read_text(
                        encoding="utf-8",
                    ),
                )
                self.assertIsNone(selection["established"])
                return target_paper_status(
                    FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
                    direction="SHORT",
                )
            return {"stage": "RUNNING", "action_token": "target-token"}

        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.2,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=http,
        )
        self.assertEqual(result, 0)
        self.assertTrue(saw_flat_projection)
        self.assertEqual(process.terminate_calls, 0)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(state["stage"], "RUNNING")
        self.assertEqual(state["target_paper_status"]["current_position"], "SHORT")

    def test_scalper_evidence_warmup_keeps_target_alive_and_projects_nested_blockers(self) -> None:
        self.status_value["entry_profile_version"] = FIVE_MINUTE_PROFILE.policy.entry_profile_version
        risk_continuity = self.status_value["risk_continuity"]
        assert isinstance(risk_continuity, dict)
        risk_continuity["source_profile"] = FIVE_MINUTE_PROFILE.policy.entry_profile_version
        self.service = PaperProfileSwitchService(
            current_profile=FIVE_MINUTE_PROFILE,
            paper_status=lambda: dict(self.status_value),
            flatten_and_disarm=self.flatten,
            verifier_status=lambda: {"status": "PASS"},
            request_shutdown=self.shutdown.set,
            runtime_root=self.root / "runtime",
            project_root=self.root,
            python_executable=Path(__file__),
            git_sha="a" * 40,
            parent_pid=2_147_483_647,
            launch_supervisor=lambda path, pid: self.launched.append((path, pid)),
            poll_seconds=0.001,
            stop_timeout_seconds=1,
        )
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-scalper-warmup", "BEELZEBUB_SCALPER_V2",
        )

        class Process:
            pid = 31343
            terminate_calls = 0

            @staticmethod
            def poll() -> None:
                return None

            def terminate(self) -> None:
                self.terminate_calls += 1

        process = Process()
        binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )
        auto_calls = 0
        paper_calls = 0
        saw_warmup_projection = False

        def http(url: str, *, method: str = "GET", **_kwargs: object) -> dict[str, object]:
            nonlocal auto_calls, paper_calls, saw_warmup_projection
            if url.endswith("/api/runtime-binding"):
                return dict(binding)
            if method == "POST":
                return {"accepted": True}
            if url.endswith("/api/lane-iii/paper"):
                paper_calls += 1
                if paper_calls >= 3:
                    state = json.loads(
                        manifest_path.with_name("state.json").read_text(encoding="utf-8"),
                    )
                    saw_warmup_projection = state["stage"] == "WARMING_TARGET_EVIDENCE"
                    self.assertEqual(state["blockers"], [
                        "COMMISSIONING_SESSION_NOT_WARMED",
                        "PAPER_EVIDENCE_NOT_WARMED",
                        "PAPER_CONTINUITY_UNUSABLE",
                        "SCALPER_EVIDENCE_FAMILY_MISSING_RESTING_LIQUIDITY",
                        "LOCAL_SEQUENCE_GAP_REWARM_REQUIRED",
                    ])
                    self.assertEqual(
                        state["target_warmup_progress"]["missing_families"],
                        ["RESTING_LIQUIDITY"],
                    )
                    return target_paper_status("BEELZEBUB_SCALPER_V2")
                return target_paper_status("BEELZEBUB_SCALPER_V2", operational=False)
            auto_calls += 1
            if auto_calls == 1:
                return {"stage": "IDLE", "in_progress": False, "action_token": "target-token"}
            if auto_calls == 2:
                return {
                    "stage": "WAITING_FOR_EVIDENCE",
                    "in_progress": True,
                    "action_token": "target-token",
                    "blockers": [],
                    "warmup": {
                        "elapsed_seconds": 3.0,
                        "covered_family_count": 2,
                        "missing_families": ["RESTING_LIQUIDITY"],
                        "readiness_blockers": [
                            "COMMISSIONING_SESSION_NOT_WARMED",
                            "PAPER_EVIDENCE_NOT_WARMED",
                            "PAPER_CONTINUITY_UNUSABLE",
                        ],
                    },
                    "readiness": {
                        "continuity": {
                            "local_sequence_gap": True,
                            "depth_reset_recovery": False,
                        },
                    },
                }
            return {"stage": "RUNNING", "in_progress": False, "action_token": "target-token"}

        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.2,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=http,
        )
        self.assertEqual(result, 0)
        self.assertTrue(saw_warmup_projection)
        self.assertEqual(process.terminate_calls, 0)

    def test_perpetual_blocked_autostart_retains_active_flat_target(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-perpetual-autostart-blocked",
            FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        )

        class Process:
            pid = 31345
            terminate_calls = 0

            @staticmethod
            def poll() -> None:
                return None

            def terminate(self) -> None:
                self.terminate_calls += 1

        process = Process()
        binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )
        auto_calls = 0
        paper_calls = 0

        def http(url: str, *, method: str = "GET", **_kwargs: object) -> dict[str, object]:
            nonlocal auto_calls, paper_calls
            if url.endswith("/api/runtime-binding"):
                return dict(binding)
            if method == "POST":
                return {"accepted": True}
            if url.endswith("/api/lane-iii/paper"):
                paper_calls += 1
                if paper_calls <= 2:
                    return target_paper_status(
                        FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
                        flat_blocker="EXCHANGE_CLOSED",
                    )
                state = json.loads(
                    manifest_path.with_name("state.json").read_text(encoding="utf-8"),
                )
                self.assertEqual(state["stage"], "TARGET_ACTIVE_FLAT_BLOCKED")
                return target_paper_status(
                    FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
                    direction="LONG",
                )
            auto_calls += 1
            if auto_calls <= 2:
                return {
                    "stage": "BLOCKED",
                    "in_progress": False,
                    "action_token": "target-token",
                    "blockers": ["EXCHANGE_CLOSED"],
                }
            return {"stage": "RUNNING", "action_token": "target-token"}

        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.2,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=http,
        )
        self.assertEqual(result, 0)
        self.assertEqual(process.terminate_calls, 0)

    def test_non_operational_terminal_autostart_is_cleanup_proven(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-non-operational-terminal",
        )

        class Process:
            pid = 31346

            def __init__(self) -> None:
                self.returncode: int | None = None
                self.terminate_calls = 0

            def poll(self) -> int | None:
                return self.returncode

            def terminate(self) -> None:
                self.terminate_calls += 1
                self.returncode = 0

            def wait(self, *, timeout: float) -> int:
                del timeout
                assert self.returncode is not None
                return self.returncode

            def kill(self) -> None:
                self.returncode = -9

        process = Process()
        binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )

        def http(url: str, *, method: str = "GET", **_kwargs: object) -> dict[str, object]:
            if url.endswith("/api/runtime-binding"):
                return dict(binding)
            if method == "POST":
                return {"accepted": True}
            if url.endswith("/api/lane-iii/paper"):
                return target_paper_status(str(manifest["target_profile"]), operational=False)
            return {
                "stage": "BLOCKED",
                "in_progress": False,
                "action_token": "target-token",
                "blockers": ["NINJATRADER_CONNECTION_UNAVAILABLE"],
            }

        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.2,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=http,
        )
        self.assertEqual(result, 7)
        self.assertEqual(process.terminate_calls, 1)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(state["stage"], "BLOCKED_SAFE")
        self.assertTrue(state["target_cleanup"]["native_exposure_absent"])

    def test_cleanup_is_unproven_while_bound_runtime_child_pid_survives(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-runtime-child-survives",
            FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        )

        class Process:
            pid = 31360

            def __init__(self) -> None:
                self.returncode: int | None = None

            def poll(self) -> int | None:
                return self.returncode

            def terminate(self) -> None:
                self.returncode = 0

            def wait(self, *, timeout: float) -> int:
                del timeout
                assert self.returncode is not None
                return self.returncode

            def kill(self) -> None:
                self.returncode = -9

        process = Process()
        binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )

        def http(url: str, *, method: str = "GET", **_kwargs: object) -> dict[str, object]:
            if url.endswith("/api/runtime-binding"):
                return dict(binding)
            if method == "POST":
                return {"accepted": True}
            if url.endswith("/api/lane-iii/paper"):
                return target_paper_status(str(manifest["target_profile"]), operational=False)
            return {
                "stage": "BLOCKED",
                "in_progress": False,
                "action_token": "target-token",
                "blockers": ["NINJATRADER_CONNECTION_UNAVAILABLE"],
            }

        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.2,
            poll_seconds=0.001,
            pid_probe=lambda pid: pid == binding["pid"],
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=http,
            target_cleanup_timeout_seconds=0.01,
        )
        self.assertEqual(result, 12)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(state["stage"], "TARGET_CLEANUP_UNPROVEN")
        self.assertEqual(state["target_runtime_pid"], binding["pid"])
        self.assertFalse(state["target_cleanup"]["target_runtime_pid_absent"])
        self.assertIn("TARGET_PROCESS_EXIT_UNPROVEN", state["blockers"])

        state_path = manifest_path.with_name("state.json")
        audit_path = manifest_path.with_name("supervisor-audit.jsonl")
        state_before_replay = state_path.read_bytes()
        audit_before_replay = audit_path.read_bytes()
        self.assertEqual(
            supervise(
                manifest_path,
                2_147_483_647,
                timeout_seconds=0.01,
                poll_seconds=0.001,
                pid_probe=lambda _pid: False,
                port_probe=lambda: True,
                wait=lambda _seconds: None,
            ),
            12,
        )
        self.assertEqual(state_path.read_bytes(), state_before_replay)
        self.assertEqual(audit_path.read_bytes(), audit_before_replay)

        target_ledger = Path(str(manifest["ledger_path"]))
        companion_paths = (
            target_ledger,
            Path(str(target_ledger) + "-wal"),
            Path(str(target_ledger) + "-shm"),
        )
        companion_paths[1].write_bytes(b"retained-wal-evidence")
        companion_paths[2].write_bytes(b"retained-shm-evidence")
        selection_path = self.root / "runtime" / "profile-switch" / "profile-selection.json"
        selection_before = selection_path.read_bytes()

        def file_identity(path: Path) -> tuple[str, int, int]:
            value = path.read_bytes()
            stat = path.stat()
            return sha256(value).hexdigest(), stat.st_mtime_ns, stat.st_size

        ledger_before = {path: file_identity(path) for path in companion_paths}
        fake_proof = {
            "status": "PASS",
            "commands_sent": 0,
            "session_count": 2,
            "proof_hash": "a" * 64,
            "completed_at": "2026-09-07T00:00:00Z",
        }

        def probe(**_kwargs: object) -> dict[str, object]:
            return dict(fake_proof)

        def validate(proof: Mapping[str, object], **_kwargs: object) -> dict[str, object]:
            return dict(proof)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as occupied:
            occupied.bind(("127.0.0.1", 0))
            occupied.listen(1)
            occupied_port = int(occupied.getsockname()[1])
            with self.assertRaisesRegex(RuntimeError, "PORT_LEASE_UNAVAILABLE"):
                finalize_stale_target_cleanup(
                    self.root / "runtime",
                    str(manifest["operation_id"]),
                    pid_probe=lambda _pid: False,
                    native_probe=probe,
                    native_proof_validator=validate,
                    _test_control_endpoint=("127.0.0.1", occupied_port),
                )
        self.assertEqual(state_path.read_bytes(), state_before_replay)

        missing_parent = json.loads(state_before_replay.decode("utf-8"))
        missing_parent["target_runtime_binding"].pop("parent_pid")
        state_path.write_text(json.dumps(missing_parent), encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "STATE_BINDING_INVALID"):
            finalize_stale_target_cleanup(
                self.root / "runtime",
                str(manifest["operation_id"]),
                pid_probe=lambda _pid: False,
                native_probe=probe,
                native_proof_validator=validate,
                _test_control_endpoint=("127.0.0.1", 0),
            )
        state_path.write_bytes(state_before_replay)

        def unavailable_probe(**_kwargs: object) -> Mapping[str, object]:
            raise RuntimeError("FRESH_NATIVE_RECONCILIATION_UNAVAILABLE")

        with self.assertRaisesRegex(RuntimeError, "FRESH_NATIVE_RECONCILIATION_UNAVAILABLE"):
            finalize_stale_target_cleanup(
                self.root / "runtime",
                str(manifest["operation_id"]),
                pid_probe=lambda _pid: False,
                native_probe=unavailable_probe,
                native_proof_validator=validate,
                _test_control_endpoint=("127.0.0.1", 0),
            )
        self.assertEqual(state_path.read_bytes(), state_before_replay)

        pid_calls: dict[int, int] = {}

        def resurrect_runtime_pid(pid: int) -> bool:
            pid_calls[pid] = pid_calls.get(pid, 0) + 1
            return pid == binding["pid"] and pid_calls[pid] == 2

        with self.assertRaisesRegex(RuntimeError, "PROCESS_STILL_ACTIVE"):
            finalize_stale_target_cleanup(
                self.root / "runtime",
                str(manifest["operation_id"]),
                pid_probe=resurrect_runtime_pid,
                native_probe=probe,
                native_proof_validator=validate,
                _test_control_endpoint=("127.0.0.1", 0),
            )
        self.assertEqual(state_path.read_bytes(), state_before_replay)

        def mutate_state(**_kwargs: object) -> dict[str, object]:
            changed = json.loads(state_before_replay.decode("utf-8"))
            changed["unexpected_mutation"] = True
            state_path.write_text(json.dumps(changed), encoding="utf-8")
            return dict(fake_proof)

        with self.assertRaisesRegex(RuntimeError, "INPUT_CHANGED"):
            finalize_stale_target_cleanup(
                self.root / "runtime",
                str(manifest["operation_id"]),
                pid_probe=lambda _pid: False,
                native_probe=mutate_state,
                native_proof_validator=validate,
                _test_control_endpoint=("127.0.0.1", 0),
            )
        state_path.write_bytes(state_before_replay)
        self.assertEqual(audit_path.read_bytes(), audit_before_replay)

        final_pid_calls: dict[int, int] = {}

        def mutate_during_final_pid_round(pid: int) -> bool:
            final_pid_calls[pid] = final_pid_calls.get(pid, 0) + 1
            if pid == binding["pid"] and final_pid_calls[pid] == 2:
                changed = json.loads(state_before_replay.decode("utf-8"))
                changed["mutation_during_final_pid_round"] = True
                state_path.write_text(json.dumps(changed), encoding="utf-8")
            return False

        with self.assertRaisesRegex(RuntimeError, "INPUT_CHANGED"):
            finalize_stale_target_cleanup(
                self.root / "runtime",
                str(manifest["operation_id"]),
                pid_probe=mutate_during_final_pid_round,
                native_probe=probe,
                native_proof_validator=validate,
                _test_control_endpoint=("127.0.0.1", 0),
            )
        state_path.write_bytes(state_before_replay)
        self.assertEqual(audit_path.read_bytes(), audit_before_replay)

        # Recreate the exact durable orphan seen when the original supervisor
        # disappears after recording the blocked faulted target but before it
        # serializes a cleanup attempt. The offline finalizer must supply all
        # missing process/native proofs; this projection supplies none.
        orphaned = json.loads(state_before_replay.decode("utf-8"))
        fault = "FIVE_MINUTE_SIGNAL_CHECKPOINT_DURABILITY_FAILED"
        faulted_paper = target_paper_status(
            FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        )
        faulted_paper.update({
            "state": "FAULTED",
            "paper_execution": "LOCKED",
            "session_armed_state": "DISARMED",
            "lockout_or_fault_reason": fault,
            "operational_paper_session": {
                "active": True,
                "request_id": f"profile-switch-{manifest['operation_id']}",
                "started_at": "2026-09-08T05:19:07Z",
                "context": {"session_id": "MNQU6:ASIA:2026-09-08"},
                "stopping": True,
                "stopping_reason": fault,
            },
        })
        orphaned.update({
            "stage": "AUTOSTARTING_TARGET",
            "in_progress": True,
            "blockers": ["TARGET_NON_OPERATIONAL_FLAT_PROOF_UNAVAILABLE"],
            "target_cleanup": None,
            "target_paper_status": faulted_paper,
            "target_autostart": {
                "schema": "lane-iii-paper-autostart-v1",
                "authority": "PERSISTENT_PAPER_SIM101_ONLY",
                "stage": "BLOCKED",
                "in_progress": False,
                "blockers": [fault],
                "request_id": f"profile-switch-{manifest['operation_id']}",
            },
        })
        state_path.write_text(json.dumps(orphaned), encoding="utf-8")
        state_before_replay = state_path.read_bytes()

        recovered = finalize_stale_target_cleanup(
            self.root / "runtime",
            str(manifest["operation_id"]),
            pid_probe=lambda _pid: False,
            native_probe=probe,
            native_proof_validator=validate,
            _test_control_endpoint=("127.0.0.1", 0),
            _test_attestation_key=b"cleanup-attestation-test-key-32b",
        )
        self.assertEqual(recovered["stage"], "BLOCKED_SAFE")
        self.assertFalse(recovered["in_progress"])
        self.assertTrue(recovered["target_cleanup"]["cleanup_proven"])
        self.assertTrue(recovered["target_cleanup"]["safe_terminal_proven"])
        self.assertTrue(recovered["target_cleanup"]["native_exposure_absent"])
        self.assertEqual(
            recovered["target_cleanup"]["late_revalidation"]["pid_probe_rounds"],
            2,
        )
        self.assertEqual(
            recovered["target_cleanup"]["late_revalidation"]["native_commands_sent"],
            0,
        )
        self.assertIn(
            manifest["parent_pid"],
            recovered["target_cleanup"]["late_revalidation"]["probed_absent_pids"],
        )
        self.assertNotIn("TARGET_PROCESS_EXIT_UNPROVEN", recovered["blockers"])
        self.assertNotIn(
            "TARGET_NATIVE_EXPOSURE_RELEASE_UNPROVEN", recovered["blockers"],
        )
        self.assertNotIn("TARGET_CLEANUP_UNPROVEN", recovered["blockers"])
        self.assertEqual(selection_path.read_bytes(), selection_before)
        self.assertEqual(
            {path: file_identity(path) for path in companion_paths},
            ledger_before,
        )

        def no_second_probe(**_kwargs: object) -> Mapping[str, object]:
            raise AssertionError("idempotent finalization must not probe again")

        repeated = finalize_stale_target_cleanup(
            self.root / "runtime",
            str(manifest["operation_id"]),
            pid_probe=lambda _pid: (_ for _ in ()).throw(
                AssertionError("idempotent finalization must not probe PIDs again")
            ),
            native_probe=no_second_probe,
            native_proof_validator=validate,
            _test_control_endpoint=("127.0.0.1", 0),
            _test_attestation_key=b"cleanup-attestation-test-key-32b",
        )
        self.assertEqual(repeated, recovered)

        terminal_state_bytes = state_path.read_bytes()

        foreign_proof_path = self.root / "do-not-read-ledger.sqlite3"
        foreign_proof_path.write_bytes(b"immutable foreign evidence")
        foreign_proof_before = foreign_proof_path.read_bytes()
        foreign_path_state = json.loads(terminal_state_bytes.decode("utf-8"))
        foreign_path_state["target_cleanup"]["late_revalidation"][
            "native_proof_path"
        ] = str(foreign_proof_path)
        state_path.write_text(json.dumps(foreign_path_state), encoding="utf-8")
        original_path_read_bytes = Path.read_bytes

        def reject_foreign_read(path: Path) -> bytes:
            if path.resolve() == foreign_proof_path.resolve():
                raise AssertionError("foreign proof path was read before confinement")
            return original_path_read_bytes(path)

        with patch.object(Path, "read_bytes", reject_foreign_read):
            with self.assertRaisesRegex(RuntimeError, "NATIVE_PROOF_INVALID"):
                finalize_stale_target_cleanup(
                    self.root / "runtime",
                    str(manifest["operation_id"]),
                    pid_probe=lambda _pid: False,
                    native_probe=no_second_probe,
                    native_proof_validator=validate,
                    _test_control_endpoint=("127.0.0.1", 0),
                    _test_attestation_key=b"cleanup-attestation-test-key-32b",
                )
        self.assertEqual(foreign_proof_path.read_bytes(), foreign_proof_before)
        state_path.write_bytes(terminal_state_bytes)

        def assert_terminal_tamper_rejected(mutator) -> None:
            tampered = json.loads(terminal_state_bytes.decode("utf-8"))
            mutator(tampered)
            state_path.write_text(json.dumps(tampered), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "NATIVE_PROOF_INVALID"):
                finalize_stale_target_cleanup(
                    self.root / "runtime",
                    str(manifest["operation_id"]),
                    pid_probe=lambda _pid: False,
                    native_probe=no_second_probe,
                    native_proof_validator=validate,
                    _test_control_endpoint=("127.0.0.1", 0),
                    _test_attestation_key=b"cleanup-attestation-test-key-32b",
                )
            state_path.write_bytes(terminal_state_bytes)

        assert_terminal_tamper_rejected(
            lambda value: value["target_cleanup"]["late_revalidation"].__setitem__(
                "source_state_sha256", "0" * 64,
            ),
        )
        assert_terminal_tamper_rejected(
            lambda value: value["target_cleanup"].__setitem__(
                "target_pid_absent", False,
            ),
        )
        assert_terminal_tamper_rejected(
            lambda value: value["blockers"].append("TARGET_CLEANUP_UNPROVEN"),
        )
        assert_terminal_tamper_rejected(
            lambda value: value["target_paper_status"].__setitem__(
                "broker_snapshot_position_quantity", 1,
            ),
        )

    def test_active_autostart_timeout_after_post_is_cleanup_unproven(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest(
            "switch-request-active-autostart-timeout",
        )

        class Process:
            pid = 31347

            def __init__(self) -> None:
                self.returncode: int | None = None

            def poll(self) -> int | None:
                return self.returncode

            def terminate(self) -> None:
                self.returncode = 0

            def wait(self, *, timeout: float) -> int:
                del timeout
                assert self.returncode is not None
                return self.returncode

            def kill(self) -> None:
                self.returncode = -9

        process = Process()
        binding, launch = self.target_launch_fixture(
            manifest_path, manifest, process,
        )
        post_calls = 0

        def http(
            url: str, *, method: str = "GET", **_kwargs: object,
        ) -> dict[str, object]:
            nonlocal post_calls
            if url.endswith("/api/runtime-binding"):
                return dict(binding)
            if url.endswith("/api/lane-iii/paper"):
                return target_paper_status(
                    str(manifest["target_profile"]), operational=False,
                )
            if method == "POST":
                post_calls += 1
                return {"accepted": True}
            return {
                "stage": "STARTING_PAPER",
                "in_progress": True,
                "action_token": "target-token",
            }

        result = supervise(
            manifest_path,
            2_147_483_647,
            timeout_seconds=0.02,
            poll_seconds=0.001,
            pid_probe=lambda _pid: False,
            port_probe=lambda: True,
            wait=lambda _seconds: None,
            launch_child=launch,  # type: ignore[arg-type]
            http_json=http,
        )
        self.assertEqual(result, 12)
        self.assertEqual(post_calls, 1)
        state = json.loads(
            manifest_path.with_name("state.json").read_text(encoding="utf-8"),
        )
        self.assertEqual(state["stage"], "TARGET_CLEANUP_UNPROVEN")
        self.assertFalse(state["target_cleanup"]["safe_terminal_proven"])
        self.assertFalse(state["target_cleanup"]["native_exposure_absent"])

    def test_target_process_creation_failure_is_terminal_and_not_retried(self) -> None:
        manifest_path, _ = self.supervisor_ready_manifest("switch-request-popen-fails")
        launches = 0
        notices: list[str] = []

        def fail_launch(*_: object, **__: object) -> object:
            nonlocal launches
            launches += 1
            raise OSError("fixture process creation failure")

        self.assertEqual(supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.2, poll_seconds=0.001,
            pid_probe=lambda _: False, port_probe=lambda: True, wait=lambda _: None,
            launch_child=fail_launch,  # type: ignore[arg-type]
            notify_operator=notices.append,
        ), 5)
        state = json.loads(manifest_path.with_name("state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["stage"], "BLOCKED_SAFE")
        self.assertEqual(state["blockers"], ["TARGET_PROCESS_CREATION_FAILED"])
        self.assertEqual(launches, 1)
        self.assertEqual(len(notices), 1)
        self.assertEqual(supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.01,
            pid_probe=lambda _: False, port_probe=lambda: True,
            launch_child=fail_launch,  # type: ignore[arg-type]
            notify_operator=notices.append,
        ), 10)
        self.assertEqual(launches, 1)
        self.assertEqual(len(notices), 1)

    def test_missing_remembered_ledger_blocks_startup_without_recreation(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest("switch-request-missing-ledger")
        launches: list[list[str]] = []
        self.assertEqual(self.complete_supervisor_with_stubs(manifest_path, manifest, launches), 0)
        ledger_path = Path(str(manifest["ledger_path"]))
        ledger_path.unlink()
        defaults = CopyTradeConfig()
        from dataclasses import replace
        config = replace(
            defaults,
            storage=replace(defaults.storage, cold_root=self.root / "missing-ledger-cold"),
            artifacts=replace(
                defaults.artifacts,
                database_path=self.root / "missing-ledger-hot" / "copytrade.sqlite3",
            ),
        )
        environment = {
            "BEELZEBUB_PROFILE_SWITCH_ROOT": str(self.root / "runtime"),
            "BEELZEBUB_PROFILE_SWITCH_OPERATION": "",
            "BEELZEBUB_L3G_PAPER_PROFILE": "",
            "BEELZEBUB_L3G_PAPER_LEDGER": "",
            "BEELZEBUB_L3G_PAPER_LEDGER_EPOCH": "",
            "BEELZEBUB_LEDGER_AUDIT_ROOT": "",
            "BEELZEBUB_RISK_CONTINUITY_PATH": "",
            "BEELZEBUB_GIT_SHA": str(manifest["git_sha"]),
        }
        with patch.dict("os.environ", environment):
            with self.assertRaisesRegex(
                RuntimeError, "PROFILE_SELECTION_ESTABLISHED_EVIDENCE_MISSING",
            ):
                create_control_center_app(config)
        self.assertFalse(ledger_path.exists())

    def test_zero_byte_remembered_ledger_blocks_startup_without_recreation(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest("switch-request-zero-ledger")
        launches: list[list[str]] = []
        self.assertEqual(self.complete_supervisor_with_stubs(manifest_path, manifest, launches), 0)
        ledger_path = Path(str(manifest["ledger_path"]))
        ledger_path.write_bytes(b"")
        defaults = CopyTradeConfig()
        from dataclasses import replace
        config = replace(
            defaults,
            storage=replace(defaults.storage, cold_root=self.root / "zero-ledger-cold"),
            artifacts=replace(
                defaults.artifacts,
                database_path=self.root / "zero-ledger-hot" / "copytrade.sqlite3",
            ),
        )
        environment = {
            "BEELZEBUB_PROFILE_SWITCH_ROOT": str(self.root / "runtime"),
            "BEELZEBUB_PROFILE_SWITCH_OPERATION": "",
            "BEELZEBUB_L3G_PAPER_PROFILE": "",
            "BEELZEBUB_L3G_PAPER_LEDGER": "",
            "BEELZEBUB_L3G_PAPER_LEDGER_EPOCH": "",
            "BEELZEBUB_LEDGER_AUDIT_ROOT": "",
            "BEELZEBUB_RISK_CONTINUITY_PATH": "",
            "BEELZEBUB_GIT_SHA": str(manifest["git_sha"]),
        }
        with patch.dict("os.environ", environment):
            with self.assertRaisesRegex(
                RuntimeError, "PROFILE_SELECTION_ESTABLISHED_LEDGER_INVALID",
            ):
                create_control_center_app(config)
        self.assertEqual(ledger_path.stat().st_size, 0)

    def test_replaced_remembered_ledger_identity_is_rejected(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest("switch-request-replaced-ledger")
        launches: list[list[str]] = []
        self.assertEqual(self.complete_supervisor_with_stubs(manifest_path, manifest, launches), 0)
        ledger_path = Path(str(manifest["ledger_path"]))
        ledger_path.unlink()
        risk_continuity_anchor_path(ledger_path).unlink()
        risk_continuity_guard_path(ledger_path).unlink()
        replacement = PaperLedger(
            ledger_path,
            epoch_id=str(manifest["ledger_epoch"]),
            policy=FIVE_MINUTE_PROFILE.policy,
            risk=FIVE_MINUTE_PROFILE.risk,
        )
        replacement.close()
        with self.assertRaisesRegex(
            RuntimeError, "PROFILE_SELECTION_ESTABLISHED_LEDGER_IDENTITY_MISMATCH",
        ):
            remembered_profile_selection(
                self.root / "runtime", git_sha=str(manifest["git_sha"]),
            )

    def test_older_same_identity_ledger_cannot_rollback_risk_anchor(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest("switch-request-risk-rollback")
        launches: list[list[str]] = []
        self.assertEqual(self.complete_supervisor_with_stubs(manifest_path, manifest, launches), 0)
        ledger_path = Path(str(manifest["ledger_path"]))
        older_image = ledger_path.read_bytes()

        context = PaperSessionResolver().resolve(
            "2026-09-03T14:00:00Z", generation=3,
        ).context
        current = PaperLedger(
            ledger_path,
            epoch_id=str(manifest["ledger_epoch"]),
            policy=FIVE_MINUTE_PROFILE.policy,
            risk=FIVE_MINUTE_PROFILE.risk,
        )
        RiskContinuityTests.append_accounted_lifecycle(
            current, context=context,
            entry_id="rollback-protected-entry",
            exit_id="rollback-protected-exit",
            realized="-75",
        )
        current.close()
        ledger_path.write_bytes(older_image)

        with self.assertRaisesRegex(
            RuntimeError, "PROFILE_SELECTION_ESTABLISHED_RISK_ANCHOR_MISMATCH",
        ):
            remembered_profile_selection(
                self.root / "runtime", git_sha=str(manifest["git_sha"]),
            )

    def test_ledger_and_local_anchor_rollback_is_blocked_by_independent_guard(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest("switch-request-guarded-rollback")
        launches: list[list[str]] = []
        self.assertEqual(self.complete_supervisor_with_stubs(manifest_path, manifest, launches), 0)
        ledger_path = Path(str(manifest["ledger_path"]))
        anchor_path = risk_continuity_anchor_path(ledger_path)
        guard_path = risk_continuity_guard_path(ledger_path)
        self.assertNotIn(self.root / "runtime", guard_path.parents)
        initial = run_local_verification(
            ledger_path, Path(str(manifest["audit_root"])), requested_mode="full",
        )
        self.assertEqual(initial["status"], "PASS")
        checkpoint_path = VerificationPaths(Path(str(manifest["audit_root"]))).checkpoint
        checkpoint_before = checkpoint_path.read_text(encoding="utf-8")
        older_image = ledger_path.read_bytes()
        older_anchor = anchor_path.read_bytes()

        context = PaperSessionResolver().resolve(
            "2026-09-03T14:00:00Z", generation=3,
        ).context
        current = PaperLedger(
            ledger_path,
            epoch_id=str(manifest["ledger_epoch"]),
            policy=FIVE_MINUTE_PROFILE.policy,
            risk=FIVE_MINUTE_PROFILE.risk,
        )
        RiskContinuityTests.append_accounted_lifecycle(
            current, context=context,
            entry_id="co-rollback-protected-entry",
            exit_id="co-rollback-protected-exit",
            realized="-75",
        )
        current.close()
        newer_guard = guard_path.read_bytes()
        ledger_path.write_bytes(older_image)
        anchor_path.write_bytes(older_anchor)

        with self.assertRaisesRegex(
            RuntimeError, "PROFILE_SELECTION_ESTABLISHED_RISK_GUARD_MISMATCH",
        ):
            remembered_profile_selection(
                self.root / "runtime", git_sha=str(manifest["git_sha"]),
            )
        report = run_local_verification(
            ledger_path, Path(str(manifest["audit_root"])), requested_mode="full",
        )
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["errors"][0]["code"], "RISK_CONTINUITY_GUARD_BEYOND_LEDGER")
        self.assertEqual(checkpoint_path.read_text(encoding="utf-8"), checkpoint_before)
        self.assertEqual(guard_path.read_bytes(), newer_guard)

    def test_missing_established_guard_blocks_runtime_arm_and_full_verification(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest("switch-request-missing-guard")
        launches: list[list[str]] = []
        self.assertEqual(self.complete_supervisor_with_stubs(manifest_path, manifest, launches), 0)
        ledger_path = Path(str(manifest["ledger_path"]))
        anchor_path = risk_continuity_anchor_path(ledger_path)
        guard_path = risk_continuity_guard_path(ledger_path)
        initial = run_local_verification(
            ledger_path, Path(str(manifest["audit_root"])), requested_mode="full",
        )
        self.assertEqual(initial["status"], "PASS")
        checkpoint_path = VerificationPaths(Path(str(manifest["audit_root"]))).checkpoint
        checkpoint_before = checkpoint_path.read_text(encoding="utf-8")
        older_image = ledger_path.read_bytes()
        older_anchor = anchor_path.read_bytes()

        context = PaperSessionResolver().resolve(
            "2026-09-03T14:00:00Z", generation=3,
        ).context
        current = PaperLedger(
            ledger_path,
            epoch_id=str(manifest["ledger_epoch"]),
            policy=FIVE_MINUTE_PROFILE.policy,
            risk=FIVE_MINUTE_PROFILE.risk,
        )
        RiskContinuityTests.append_accounted_lifecycle(
            current, context=context,
            entry_id="missing-guard-entry", exit_id="missing-guard-exit", realized="-75",
        )
        current.close()
        ledger_path.write_bytes(older_image)
        anchor_path.write_bytes(older_anchor)
        guard_path.unlink()

        restored_ledger = PaperLedger(
            ledger_path,
            epoch_id=str(manifest["ledger_epoch"]),
            policy=FIVE_MINUTE_PROFILE.policy,
            risk=FIVE_MINUTE_PROFILE.risk,
        )
        runtime = LaneIIIPaperRuntime(restored_ledger)
        self.assertTrue(runtime.status()["entries_paused"])
        self.assertEqual(
            runtime.risk.status()["lockout_reason"], "RISK_CONTINUITY_GUARD_MISSING",
        )
        self.assertFalse(guard_path.exists())
        self.assertFalse(runtime.arm()["armed"])
        self.assertEqual(restored_ledger.recent_kinds(("COMMAND",)), [])
        restored_ledger.close()

        report = run_local_verification(
            ledger_path, Path(str(manifest["audit_root"])), requested_mode="full",
        )
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["errors"][0]["code"], "RISK_CONTINUITY_GUARD_MISSING")
        self.assertEqual(checkpoint_path.read_text(encoding="utf-8"), checkpoint_before)
        self.assertFalse(guard_path.exists())

    def test_guard_location_is_independent_of_mutable_profile_root_environment(self) -> None:
        ledger_path = self.root / "runtime" / "hot" / "paper.sqlite3"
        with patch.dict(
            "os.environ", {"BEELZEBUB_PROFILE_SWITCH_ROOT": str(self.root / "first-root")},
        ):
            first = risk_continuity_guard_path(ledger_path)
        with patch.dict(
            "os.environ", {"BEELZEBUB_PROFILE_SWITCH_ROOT": str(self.root / "second-root")},
        ):
            second = risk_continuity_guard_path(ledger_path)
        self.assertEqual(first, second)
        self.assertNotIn(self.root / "runtime", first.parents)

    def test_arbitrary_nested_configured_root_is_rejected_as_unsafe(self) -> None:
        runtime_root = self.root / "selector-state"
        ledger_path = runtime_root / "current" / "lane_iii_paper.sqlite3"
        guard_path = risk_continuity_guard_path(ledger_path)
        self.assertIn(runtime_root.resolve(), guard_path.parents)
        with self.assertRaisesRegex(
            RuntimeError, "PROFILE_SWITCH_RISK_GUARD_LAYOUT_UNSAFE",
        ):
            PaperProfileSwitchService(
                current_profile=SCALPER_PROFILE,
                paper_status=lambda: flat_status(),
                flatten_and_disarm=lambda: {},
                verifier_status=lambda: {"status": "PASS"},
                request_shutdown=lambda: None,
                runtime_root=runtime_root,
                project_root=self.root,
                python_executable=Path(__file__),
                git_sha="a" * 40,
                current_runtime_binding={
                    "ledger": str(ledger_path),
                    "ledger_identity": "l3g-ledger-" + "1" * 32,
                },
            )

    def test_app_rejects_unsafe_configured_root_before_opening_paper_ledger(self) -> None:
        from dataclasses import replace

        runtime_root = self.root / "selector-state-app"
        ledger_path = runtime_root / "current" / "lane_iii_paper.sqlite3"
        defaults = CopyTradeConfig()
        config = replace(
            defaults,
            storage=replace(defaults.storage, cold_root=self.root / "cold"),
            artifacts=replace(
                defaults.artifacts,
                database_path=self.root / "control" / "copytrade.sqlite3",
            ),
        )
        environment = {
            "BEELZEBUB_PROFILE_SWITCH_ROOT": str(runtime_root),
            "BEELZEBUB_L3G_PAPER_LEDGER": str(ledger_path),
            "BEELZEBUB_L3G_PAPER_LEDGER_EPOCH": "L3G-PAPER-EPOCH-UNSAFE-LAYOUT",
            "BEELZEBUB_L3G_PAPER_PROFILE": SCALPER_PROFILE.selection_key,
            "BEELZEBUB_PROFILE_SWITCH_OPERATION": "",
            "BEELZEBUB_LEDGER_AUDIT_ROOT": "",
            "BEELZEBUB_RISK_CONTINUITY_PATH": "",
        }
        with patch.dict("os.environ", environment):
            with self.assertRaisesRegex(
                RuntimeError, "PROFILE_SWITCH_RISK_GUARD_LAYOUT_UNSAFE",
            ):
                create_control_center_app(config)
        self.assertFalse(ledger_path.exists())

    def test_remembered_nested_configured_root_with_internal_guard_is_invalid(self) -> None:
        runtime_root = (self.root / "selector-state-remembered").resolve()
        ledger_path = runtime_root / "current" / "lane_iii_paper.sqlite3"
        selection_path = runtime_root / "profile-switch" / "profile-selection.json"
        selection_path.parent.mkdir(parents=True)
        selection_path.write_text(json.dumps({
            "schema": PROFILE_SELECTION_SCHEMA,
            "requested": None,
            "established": {
                "profile": SCALPER_PROFILE.selection_key,
                "operation_id": "source-before-profile-switch-" + "1" * 32,
                "ledger_path": str(ledger_path),
                "ledger_identity": "l3g-ledger-" + "1" * 32,
                "ledger_epoch": "L3G-PAPER-EPOCH-UNSAFE-LAYOUT",
                "risk_anchor_path": str(risk_continuity_anchor_path(ledger_path)),
                "risk_guard_path": str(risk_continuity_guard_path(ledger_path)),
                "audit_root": str(runtime_root / "current" / "audit"),
                "git_sha": "a" * 40,
                "established_at": "2026-09-05T00:00:00Z",
            },
            "updated_at": "2026-09-05T00:00:00Z",
        }), encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "PROFILE_SELECTION_STATE_INVALID"):
            remembered_profile_selection(runtime_root, git_sha="a" * 40)

    def test_default_artifacts_tree_restore_cannot_reset_risk_allowance(self) -> None:
        runtime_root = self.root / "artifacts"
        ledger_path = runtime_root / "lane_iii_paper.sqlite3"
        audit_root = runtime_root / "audit"
        epoch = "L3G-PAPER-EPOCH-DEFAULT-ARTIFACTS"
        ledger = PaperLedger(
            ledger_path, epoch_id=epoch,
            policy=SCALPER_PROFILE.policy, risk=SCALPER_PROFILE.risk,
        )
        identity = ledger.risk_continuity_boundary()["ledger_identity"]
        ledger.close()
        audit_root.mkdir(parents=True)
        guard_path = risk_continuity_guard_path(ledger_path)
        self.assertEqual(profile_switch_runtime_root(ledger_path), runtime_root.resolve())
        self.assertNotIn(runtime_root.resolve(), guard_path.parents)

        shutdown = threading.Event()
        service = PaperProfileSwitchService(
            current_profile=SCALPER_PROFILE,
            paper_status=lambda: flat_status(),
            flatten_and_disarm=lambda: {},
            verifier_status=lambda: {"status": "PASS"},
            request_shutdown=shutdown.set,
            runtime_root=runtime_root,
            project_root=self.root,
            python_executable=Path(__file__),
            git_sha="a" * 40,
            parent_pid=2_147_483_647,
            launch_supervisor=lambda *_: None,
            poll_seconds=0.001,
            stop_timeout_seconds=1,
            current_runtime_binding={
                "ledger": str(ledger_path),
                "audit": str(audit_root),
                "git_sha": "a" * 40,
                "entry_profile_version": SCALPER_PROFILE.selection_key,
                "paper_policy_hash": SCALPER_PROFILE.policy.configuration_hash,
                "risk_profile_hash": SCALPER_PROFILE.risk.configuration_hash,
                "ledger_epoch": epoch,
                "ledger_identity": identity,
            },
        )
        service.start("default-artifacts-rollback", FIVE_MINUTE_PROFILE.selection_key)
        assert service._thread is not None
        service._thread.join(timeout=1)
        self.assertTrue(shutdown.is_set())
        self.assertIsNotNone(
            remembered_profile_selection(runtime_root, git_sha="a" * 40),
        )

        snapshot_root = self.root / "artifacts-older-snapshot"
        shutil.copytree(runtime_root, snapshot_root)
        context = PaperSessionResolver().resolve(
            "2026-09-03T14:00:00Z", generation=3,
        ).context
        current = PaperLedger(
            ledger_path, epoch_id=epoch,
            policy=SCALPER_PROFILE.policy, risk=SCALPER_PROFILE.risk,
        )
        RiskContinuityTests.append_accounted_lifecycle(
            current, context=context,
            entry_id="default-tree-rollback-entry",
            exit_id="default-tree-rollback-exit", realized="-75",
        )
        current.close()
        newer_guard = guard_path.read_bytes()

        shutil.rmtree(runtime_root)
        shutil.copytree(snapshot_root, runtime_root)
        self.assertEqual(guard_path.read_bytes(), newer_guard)
        with self.assertRaisesRegex(
            RuntimeError, "PROFILE_SELECTION_ESTABLISHED_RISK_GUARD_MISMATCH",
        ):
            remembered_profile_selection(runtime_root, git_sha="a" * 40)

        restored = PaperLedger(
            ledger_path, epoch_id=epoch,
            policy=SCALPER_PROFILE.policy, risk=SCALPER_PROFILE.risk,
        )
        runtime = LaneIIIPaperRuntime(restored)
        self.assertTrue(runtime.status()["entries_paused"])
        self.assertEqual(
            runtime.risk.status()["lockout_reason"],
            "RISK_CONTINUITY_GUARD_BEYOND_LEDGER",
        )
        self.assertFalse(runtime.arm()["armed"])
        restored.close()
        report = run_local_verification(
            ledger_path, audit_root, requested_mode="full",
        )
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(
            report["errors"][0]["code"], "RISK_CONTINUITY_GUARD_BEYOND_LEDGER",
        )
        self.assertEqual(guard_path.read_bytes(), newer_guard)

    def test_existing_risk_history_without_any_guard_requires_explicit_adoption(self) -> None:
        ledger_path = self.root / "runtime" / "hot" / "paper.sqlite3"
        ledger = PaperLedger(ledger_path)
        ledger.append(
            "EXECUTION",
            {
                "order_role": "ENTRY", "price": "100", "quantity": 1,
                "direction": "LONG", "native_execution_id": "pre-adoption-entry",
            },
            identity="pre-adoption-entry-record",
        )
        ledger.close()
        risk_continuity_anchor_path(ledger_path).unlink()
        risk_continuity_guard_path(ledger_path).unlink()

        reopened = PaperLedger(ledger_path)
        runtime = LaneIIIPaperRuntime(reopened)
        self.assertTrue(runtime.status()["entries_paused"])
        self.assertEqual(
            runtime.risk.status()["lockout_reason"],
            "RISK_CONTINUITY_GUARD_ADOPTION_REQUIRED",
        )
        self.assertFalse(risk_continuity_guard_path(ledger_path).exists())
        reopened.close()
        report = run_local_verification(
            ledger_path, self.root / "runtime" / "audit", requested_mode="full",
        )
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(
            report["errors"][0]["code"],
            "RISK_CONTINUITY_GUARD_ADOPTION_REQUIRED",
        )

    def test_arbitrary_established_operation_id_is_rejected(self) -> None:
        manifest_path, manifest = self.supervisor_ready_manifest("switch-request-arbitrary-operation")
        launches: list[list[str]] = []
        self.assertEqual(self.complete_supervisor_with_stubs(manifest_path, manifest, launches), 0)
        selection_path = self.root / "runtime" / "profile-switch" / "profile-selection.json"
        selection = json.loads(selection_path.read_text(encoding="utf-8"))
        selection["established"]["operation_id"] = "arbitrary-operation"
        selection_path.write_text(json.dumps(selection), encoding="utf-8")
        with self.assertRaisesRegex(
            RuntimeError, "PROFILE_SELECTION_ESTABLISHED_OPERATION_INVALID",
        ):
            remembered_profile_selection(
                self.root / "runtime", git_sha=str(manifest["git_sha"]),
            )

    def test_conflicting_target_identity_never_becomes_ready(self) -> None:
        manifest_path, _ = self.supervisor_ready_manifest("switch-request-wrong-target-binding")

        class RunningProcess:
            pid = 31338

            def __init__(self) -> None:
                self.returncode: int | None = None

            def poll(self) -> int | None:
                return self.returncode

            def terminate(self) -> None:
                self.returncode = 0

            def wait(self, *, timeout: float) -> int:
                del timeout
                assert self.returncode is not None
                return self.returncode

            def kill(self) -> None:
                self.returncode = -9

        self.assertEqual(supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.02, poll_seconds=0.001,
            pid_probe=lambda _: False, port_probe=lambda: True, wait=lambda _: None,
            launch_child=lambda *_, **__: RunningProcess(),  # type: ignore[arg-type]
            http_json=lambda *_args, **_kwargs: {"entry_profile_version": "BEELZEBUB_SCALPER_V2"},
        ), 12)
        state = json.loads(manifest_path.with_name("state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["stage"], "TARGET_CLEANUP_UNPROVEN")
        self.assertIn("TARGET_RUNTIME_BINDING_TIMEOUT", state["blockers"])
        self.assertFalse(state["target_cleanup"]["target_runtime_pid_absent"])

    @unittest.skipUnless(os.name == "nt", "Production launcher flags are Windows-specific.")
    def test_production_windows_child_launcher_with_harmless_stub(self) -> None:
        marker = self.root / "harmless-child-created.txt"
        log_path = self.root / "harmless-child.log"
        command = [
            sys.executable, "-c",
            "import pathlib,sys; pathlib.Path(sys.argv[1]).write_text('stub-only', encoding='utf-8')",
            str(marker),
        ]
        with log_path.open("wb") as output:
            process = _launch_child(command, cwd=self.root, environment=os.environ, output=output)
        self.assertEqual(process.wait(timeout=10), 0)
        self.assertEqual(marker.read_text(encoding="utf-8"), "stub-only")

    def test_supervisor_requires_snapshot_to_match_final_source_risk_boundary(self) -> None:
        self.service.start("switch-request-0023", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        manifest_path = Path(str(self.service.status()["manifest_path"]))
        receipt = clean_shutdown_receipt()
        receipt["risk_continuity_boundary"] = {
            **receipt["risk_continuity_boundary"],  # type: ignore[dict-item]
            "risk_boundary_sequence": 1,
            "risk_boundary_hash": "e" * 64,
        }
        self.service.record_shutdown_receipt(receipt)

        self.assertEqual(supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.01,
            poll_seconds=0.001, pid_probe=lambda _: False,
            port_probe=lambda: True, wait=lambda _: None,
        ), 4)
        state = json.loads(manifest_path.with_name("state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["blockers"], ["RISK_CONTINUITY_SOURCE_BOUNDARY_MISMATCH"])

    def test_target_binding_requires_the_unique_manifest_ledger_epoch(self) -> None:
        continuity = {"artifact_sha256": "c" * 64}
        manifest = {
            "ledger_path": "N:/runtime/fresh/paper.sqlite3", "audit_root": "N:/runtime/fresh/audit",
            "git_sha": "a" * 40, "target_profile": "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
            "paper_policy_hash": "p" * 64, "risk_profile_hash": "r" * 64,
            "ledger_epoch": "L3G-PAPER-EPOCH-UNIQUE",
        }
        binding = {
            "pid": 31337,
            "parent_pid": 31336,
            "ledger": manifest["ledger_path"], "audit": manifest["audit_root"],
            "git_sha": manifest["git_sha"], "entry_profile_version": manifest["target_profile"],
            "paper_policy_hash": manifest["paper_policy_hash"],
            "risk_profile_hash": manifest["risk_profile_hash"],
            "ledger_epoch": manifest["ledger_epoch"],
            "ledger_identity": "l3g-ledger-" + "d" * 32,
            "risk_continuity_artifact_sha256": continuity["artifact_sha256"],
        }
        self.assertTrue(
            _target_binding_matches(
                binding, manifest, continuity, target_pid=31337,
            )
        )
        self.assertTrue(
            _target_binding_matches(
                {**binding, "pid": 31338, "parent_pid": 31337},
                manifest,
                continuity,
                target_pid=31337,
            )
        )
        self.assertFalse(
            _target_binding_matches(
                {**binding, "pid": 31338},
                manifest,
                continuity,
                target_pid=31337,
            )
        )
        self.assertFalse(
            _target_binding_matches(
                {**binding, "ledger_epoch": "OTHER-EPOCH"},
                manifest,
                continuity,
                target_pid=31337,
            )
        )
        self.assertFalse(
            _target_binding_matches(
                {**binding, "ledger_identity": None},
                manifest,
                continuity,
                target_pid=31337,
            )
        )
        self.assertFalse(
            _target_binding_matches(
                {**binding, "pid": 31338, "parent_pid": 31339},
                manifest,
                continuity,
                target_pid=31337,
            )
        )

    def test_supervisor_refuses_a_self_consistent_artifact_from_the_wrong_source_profile(self) -> None:
        self.service.start("switch-request-0013", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        manifest_path = Path(str(self.service.status()["manifest_path"]))
        self.service.record_shutdown_receipt(clean_shutdown_receipt())
        continuity_path = manifest_path.with_name("risk-continuity.json")
        artifact = json.loads(continuity_path.read_text(encoding="utf-8"))
        artifact["source_profile"] = "NY_HIGH_CONFLUENCE_COMMISSIONING_V1"
        artifact["snapshot"]["source_profile"] = "NY_HIGH_CONFLUENCE_COMMISSIONING_V1"
        artifact.pop("artifact_sha256")
        artifact["artifact_sha256"] = sha256(
            json.dumps(artifact, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        continuity_path.write_text(json.dumps(artifact), encoding="utf-8")
        state_path = manifest_path.with_name("state.json")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["risk_continuity_artifact_sha256"] = artifact["artifact_sha256"]
        state_path.write_text(json.dumps(state), encoding="utf-8")

        self.assertEqual(supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.01,
            poll_seconds=0.001, pid_probe=lambda _: False,
            port_probe=lambda: True, wait=lambda _: None,
        ), 4)
        state = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual(state["blockers"], ["RISK_CONTINUITY_SOURCE_PROFILE_MISMATCH"])

    def test_supervisor_records_invalid_continuity_time_as_an_exact_safe_blocker(self) -> None:
        self.service.start("switch-request-0024", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        manifest_path = Path(str(self.service.status()["manifest_path"]))
        self.service.record_shutdown_receipt(clean_shutdown_receipt())
        continuity_path = manifest_path.with_name("risk-continuity.json")
        artifact = json.loads(continuity_path.read_text(encoding="utf-8"))
        artifact["snapshot"]["generated_at"] = "not-a-time"
        artifact.pop("artifact_sha256")
        artifact["artifact_sha256"] = sha256(
            json.dumps(artifact, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        continuity_path.write_text(json.dumps(artifact), encoding="utf-8")

        self.assertEqual(supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.01,
            poll_seconds=0.001, pid_probe=lambda _: False,
            port_probe=lambda: True, wait=lambda _: None,
        ), 4)
        state = json.loads(manifest_path.with_name("state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["blockers"], ["RISK_CONTINUITY_GENERATED_AT_INVALID"])

    def test_supervisor_records_invalid_utf8_artifact_as_unreadable(self) -> None:
        self.service.start("switch-request-0025", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        self.assertTrue(self.shutdown.wait(1))
        manifest_path = Path(str(self.service.status()["manifest_path"]))
        self.service.record_shutdown_receipt(clean_shutdown_receipt())
        manifest_path.with_name("risk-continuity.json").write_bytes(b"\xff\xfe\xfa")

        self.assertEqual(supervise(
            manifest_path, 2_147_483_647, timeout_seconds=0.01,
            poll_seconds=0.001, pid_probe=lambda _: False,
            port_probe=lambda: True, wait=lambda _: None,
        ), 4)
        state = json.loads(manifest_path.with_name("state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["blockers"], ["RISK_CONTINUITY_ARTIFACT_UNREADABLE"])

    def test_artifact_creation_time_must_equal_its_snapshot_time(self) -> None:
        path = self.root / "mismatched-time.json"
        write_risk_continuity_artifact(
            path, operation_id="time-operation", source_profile="BEELZEBUB_SCALPER_V2",
            target_profile="BEELZEBUB_FIVE_MINUTE_BIAS_V1",
            snapshot=flat_status()["risk_continuity"],
        )
        artifact = json.loads(path.read_text(encoding="utf-8"))
        artifact["created_at"] = "2026-09-04T20:00:01Z"
        artifact.pop("artifact_sha256")
        artifact["artifact_sha256"] = sha256(
            json.dumps(artifact, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        path.write_text(json.dumps(artifact), encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "RISK_CONTINUITY_ARTIFACT_TIME_MISMATCH"):
            read_risk_continuity_artifact(path)

    def test_supervisor_uses_independent_budgets_for_each_bounded_startup_stage(self) -> None:
        defaults = supervise.__kwdefaults__ or {}
        self.assertGreaterEqual(defaults.get("timeout_seconds", 0), 1080)
        source = (
            Path(__file__).parents[1] / "src" / "l3g_paper" / "profile_switch.py"
        ).read_text(encoding="utf-8")
        body = source[source.index("def supervise("):source.index("\ndef main(")]
        self.assertGreaterEqual(
            body.count("deadline = monotonic() + timeout_seconds"),
            3,
        )


class ProfileSwitchRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_continuity_startup_refuses_a_factory_that_cannot_accept_the_snapshot(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            artifact_path = root / "risk-continuity.json"
            write_risk_continuity_artifact(
                artifact_path, operation_id="custom-factory-operation",
                source_profile="BEELZEBUB_SCALPER_V2",
                target_profile="BEELZEBUB_FIVE_MINUTE_BIAS_V1",
                snapshot=flat_status()["risk_continuity"],
            )
            defaults = CopyTradeConfig()
            from dataclasses import replace
            config = replace(
                defaults,
                storage=replace(defaults.storage, cold_root=root / "cold"),
                artifacts=replace(defaults.artifacts, database_path=root / "hot" / "copytrade.sqlite3"),
            )
            factory_called = False

            def incompatible_factory(ledger: PaperLedger) -> LaneIIIPaperRuntime:
                nonlocal factory_called
                factory_called = True
                return LaneIIIPaperRuntime(ledger)

            environment = {
                "BEELZEBUB_PROFILE_SWITCH_OPERATION": "custom-factory-operation",
                "BEELZEBUB_RISK_CONTINUITY_PATH": str(artifact_path),
                "BEELZEBUB_L3G_PAPER_PROFILE": "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
                "BEELZEBUB_L3G_PAPER_LEDGER": str(root / "target" / "paper.sqlite3"),
                "BEELZEBUB_L3G_PAPER_LEDGER_EPOCH": "L3G-PAPER-EPOCH-CUSTOM-FACTORY",
                "BEELZEBUB_LEDGER_AUDIT_ROOT": str(root / "target" / "audit"),
            }
            with patch.dict("os.environ", environment):
                with self.assertRaisesRegex(RuntimeError, "RISK_CONTINUITY_CUSTOM_RUNTIME_FACTORY_UNSUPPORTED"):
                    create_control_center_app(config, lane_iii_paper_factory=incompatible_factory)
            self.assertFalse(factory_called)

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
            self.assertEqual(len(catalog["profiles"]), 4)
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
