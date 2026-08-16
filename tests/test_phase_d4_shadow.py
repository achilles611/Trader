from __future__ import annotations

import tempfile
import threading
import unittest
import json
import sqlite3
from datetime import timedelta
from pathlib import Path
from typing import Any

from src.copytrade.config import ArtifactConfig, CopyTradeConfig, ShadowObservationConfig, SourceConfig
from src.copytrade.control_center import CopyControlCenter
from src.copytrade.execution import DeterministicExecutionSimulator, ExecutionEngine, SimulatorPlan
from src.copytrade.execution_contracts import ExecutionSafetyContext, ExecutionState
from src.copytrade.models import CopySignal, as_utc, stable_id
from src.copytrade.shadow import (
    SHADOW_EXECUTION_DOMAIN,
    HyperliquidReadOnlyShadowAdapter,
    ShadowObservation,
    ShadowObservationService,
    compare_shadow_observation,
    shadow_execution_account_id,
)
from src.copytrade.storage import CopyTradeDatabase


ACCOUNT_A = "0x1111111111111111111111111111111111111111"
ACCOUNT_B = "0x2222222222222222222222222222222222222222"
WALLET = "0x3333333333333333333333333333333333333333"
TIME = as_utc("2026-01-01T00:00:00+00:00")


class FakePublicInfoClient:
    """Read-only deterministic /info fixture; it deliberately has no writes."""

    def __init__(self, responses: dict[str, Any]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def info(self, payload: dict[str, Any]) -> Any:
        self.calls.append(dict(payload))
        response = self.responses[payload["type"]]
        if isinstance(response, BaseException):
            raise response
        return response


class FailingShadowAdapter:
    """Fault injector at the capability boundary, not a venue write adapter."""

    adapter_name = "failing_read_only_shadow"
    adapter_mode = "READ_ONLY_SHADOW"
    venue = "hyperliquid"

    def observe_account(self, account_id: str, *, max_age_seconds: float, received_at: object | None = None) -> Any:
        raise RuntimeError("deliberately unpersisted transport diagnostic")


class ForeignAccountShadowAdapter:
    """Returns a valid-looking observation for the wrong account to attack scope binding."""

    adapter_name = "foreign_account_read_only_shadow"
    adapter_mode = "READ_ONLY_SHADOW"
    venue = "hyperliquid"

    def __init__(self, observation: ShadowObservation) -> None:
        self.observation = observation

    def observe_account(self, account_id: str, *, max_age_seconds: float, received_at: object | None = None) -> ShadowObservation:
        return self.observation


class GatedShadowAdapter:
    """Deterministically holds one otherwise-valid observation in flight."""

    adapter_name = "gated_read_only_shadow"
    adapter_mode = "READ_ONLY_SHADOW"
    venue = "hyperliquid"

    def __init__(self, delegate: HyperliquidReadOnlyShadowAdapter, started: threading.Event, release: threading.Event) -> None:
        self.delegate = delegate
        self.started = started
        self.release = release

    def observe_account(self, account_id: str, *, max_age_seconds: float, received_at: object | None = None) -> ShadowObservation:
        self.started.set()
        if not self.release.wait(timeout=1):
            raise RuntimeError("test refresh release was not received")
        return self.delegate.observe_account(account_id, max_age_seconds=max_age_seconds, received_at=received_at)


def payloads(
    observed_at: object = TIME, *, positions: list[dict[str, Any]] | None = None,
    orders: list[dict[str, Any]] | None = None, include_timestamp: bool = True,
) -> dict[str, Any]:
    stamp = int(as_utc(observed_at).timestamp() * 1000)
    time_field = {"time": stamp} if include_timestamp else {}
    return {
        "clearinghouseState": {
            **time_field,
            "marginSummary": {"accountValue": "100.5", "totalNtlPos": "120"}, "withdrawable": "80",
            "assetPositions": positions if positions is not None else [
                {"position": {"coin": "BTC", "szi": "1.5"}},
                {"position": {"coin": "ETH", "szi": "-2"}},
            ],
        },
        "openOrders": {
            **time_field,
            "orders": orders if orders is not None else [
                {"oid": "external-1", "coin": "BTC", "side": "B", "sz": "0.5", "status": "open"},
            ],
        },
        "meta": {
            **time_field,
            "universe": [{"name": "BTC", "szDecimals": 4, "maxLeverage": "50"}],
        },
    }


def shadow_config(account: str = ACCOUNT_A) -> ShadowObservationConfig:
    return ShadowObservationConfig(enabled=True, venue="hyperliquid", account_id=account, max_age_seconds=60.0)


def signal(name: str, *, action: str = "open") -> CopySignal:
    return CopySignal(
        signal_id=stable_id("phase_d4_signal", name), target_wallet=WALLET, campaign_id="d4",
        source_event_id=stable_id("phase_d4_source", name), symbol="BTC", action=action, direction="long",
        target_price=100.0, target_quantity=1.0, target_notional=100.0, allocation_fraction=0.1,
        requested_capital=100.0, created_at=TIME, source_event_timestamp=TIME,
        target_position_before=1.0 if action == "close" else 0.0,
    )


class PhaseD4ReadOnlyShadowTests(unittest.TestCase):
    def adapter(self, fixture: FakePublicInfoClient) -> HyperliquidReadOnlyShadowAdapter:
        return HyperliquidReadOnlyShadowAdapter(SourceConfig(), public_client=fixture, clock=lambda: TIME)

    def database(self, root: Path) -> CopyTradeDatabase:
        database = CopyTradeDatabase(root / "copy.sqlite3")
        database.initialize()
        return database

    def test_capability_isolation_and_live_configuration_rejection(self) -> None:
        adapter = self.adapter(FakePublicInfoClient(payloads()))
        for forbidden in ("submit", "cancel", "amend", "replace", "transfer", "withdraw", "sign"):
            self.assertFalse(hasattr(adapter, forbidden), forbidden)
        with tempfile.TemporaryDirectory() as temp:
            database = self.database(Path(temp))
            with self.assertRaises(ValueError):
                ExecutionEngine(database, adapter)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            CopyTradeConfig(mode="live", live_enabled=True).validate()
        with self.assertRaises(ValueError):
            CopyTradeConfig(
                source=SourceConfig(info_url="https://localhost/info"), shadow_observation=shadow_config(),
            ).validate()
        with self.assertRaises(ValueError):
            HyperliquidReadOnlyShadowAdapter(SourceConfig(info_url="http://api.hyperliquid.xyz/info"))
        with self.assertRaises(ValueError):
            CopyTradeConfig(shadow_observation=ShadowObservationConfig(enabled=True, account_id=ACCOUNT_A, max_age_seconds=float("inf"))).validate()

    def test_fresh_normalization_preserves_signed_positions_balances_orders_and_metadata(self) -> None:
        fixture = FakePublicInfoClient(payloads())
        observation = self.adapter(fixture).observe_account(
            ACCOUNT_A, max_age_seconds=60.0, received_at=TIME,
        )
        self.assertEqual((observation.state, observation.freshness), ("COMPLETE", "FRESH"))
        self.assertEqual(observation.normalized["positions"], [
            {"symbol": "BTC", "signed_quantity": 1.5}, {"symbol": "ETH", "signed_quantity": -2.0},
        ])
        self.assertEqual(observation.normalized["balances"]["account_value"], 100.5)
        self.assertEqual(observation.normalized["open_orders"][0]["side"], "BUY")
        self.assertEqual(observation.normalized["instruments"][0]["quantity_precision"], 4)
        self.assertEqual({call["type"] for call in fixture.calls}, {"clearinghouseState", "openOrders", "meta"})

    def test_stale_unknown_and_malformed_evidence_are_explicitly_incomplete(self) -> None:
        stale = self.adapter(FakePublicInfoClient(payloads(TIME - timedelta(seconds=61)))).observe_account(
            ACCOUNT_A, max_age_seconds=60.0, received_at=TIME,
        )
        self.assertEqual((stale.state, stale.freshness), ("INCOMPLETE", "STALE"))
        self.assertEqual(stale.components["positions"]["reason"], "venue_timestamp_stale")

        unknown = self.adapter(FakePublicInfoClient(payloads(include_timestamp=False))).observe_account(
            ACCOUNT_A, max_age_seconds=60.0, received_at=TIME,
        )
        self.assertEqual((unknown.state, unknown.freshness), ("INCOMPLETE", "UNKNOWN"))
        self.assertEqual(unknown.components["open_orders"]["reason"], "venue_timestamp_missing")

        malformed_data = payloads()
        malformed_data["clearinghouseState"]["assetPositions"] = [{"position": {"coin": "BTC", "szi": "NaN"}}]
        malformed = self.adapter(FakePublicInfoClient(malformed_data)).observe_account(
            ACCOUNT_A, max_age_seconds=60.0, received_at=TIME,
        )
        self.assertEqual(malformed.state, "INCOMPLETE")
        self.assertEqual(malformed.components["positions"]["reason"], "numeric_value_non_finite")
        self.assertFalse(malformed.components["positions"]["empty"])

    def test_freshness_boundaries_and_future_venue_time_fail_closed(self) -> None:
        one_before = self.adapter(FakePublicInfoClient(payloads(TIME - timedelta(seconds=59)))).observe_account(
            ACCOUNT_A, max_age_seconds=60.0, received_at=TIME,
        )
        exact_boundary = self.adapter(FakePublicInfoClient(payloads(TIME - timedelta(seconds=60)))).observe_account(
            ACCOUNT_A, max_age_seconds=60.0, received_at=TIME,
        )
        one_after = self.adapter(FakePublicInfoClient(payloads(TIME - timedelta(seconds=61)))).observe_account(
            ACCOUNT_A, max_age_seconds=60.0, received_at=TIME,
        )
        future = self.adapter(FakePublicInfoClient(payloads(TIME + timedelta(seconds=1)))).observe_account(
            ACCOUNT_A, max_age_seconds=60.0, received_at=TIME,
        )
        self.assertEqual((one_before.state, exact_boundary.state), ("COMPLETE", "COMPLETE"))
        self.assertEqual(one_after.components["positions"]["freshness"], "STALE")
        self.assertEqual((future.state, future.components["positions"]["reason"]), ("INCOMPLETE", "venue_timestamp_in_future"))

    def test_empty_is_distinct_from_failure_and_failed_refresh_replaces_current_health(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.database(Path(temp))
            fixture = FakePublicInfoClient(payloads(positions=[], orders=[]))
            service = ShadowObservationService(database, self.adapter(fixture), shadow_config())
            healthy = service.refresh(received_at=TIME)
            self.assertEqual(healthy["state"], "COMPLETE")
            latest = healthy["latest_observation"]
            self.assertTrue(latest["components"]["positions"]["empty"])
            self.assertTrue(latest["components"]["open_orders"]["empty"])

            fixture.responses["openOrders"] = TimeoutError("simulated timeout")
            failed = service.refresh(received_at=TIME + timedelta(seconds=1))
            self.assertEqual((failed["state"], failed["freshness"]), ("INCOMPLETE", "UNKNOWN"))
            self.assertFalse(failed["latest_observation"]["components"]["open_orders"]["empty"])
            self.assertEqual(failed["latest_observation"]["components"]["open_orders"]["reason"], "open_order_observation_unavailable")
            self.assertEqual(failed["latest_observation"]["raw_evidence"]["open_orders"], {"error_class": "TimeoutError"})
            self.assertEqual(len(failed["history"]), 2)

    def test_partial_payload_and_unexpected_observer_failure_are_current_incomplete_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.database(Path(temp))
            partial_payloads = payloads(positions=[])
            partial_payloads["clearinghouseState"].pop("marginSummary")
            partial = ShadowObservationService(
                database, self.adapter(FakePublicInfoClient(partial_payloads)), shadow_config(),
            ).refresh(received_at=TIME)
            self.assertEqual(partial["state"], "INCOMPLETE")
            self.assertEqual(partial["latest_observation"]["components"]["balances"]["reason"], "balance_payload_missing_margin_summary")

            failed = ShadowObservationService(database, FailingShadowAdapter(), shadow_config()).refresh(
                received_at=TIME + timedelta(seconds=1),
            )
            latest = failed["latest_observation"]
            self.assertEqual((failed["state"], failed["freshness"]), ("INCOMPLETE", "UNKNOWN"))
            self.assertEqual(latest["reason"], "shadow_adapter_observation_failed")
            self.assertEqual(latest["raw_evidence"], {"observer": {"error_class": "RuntimeError"}})
            self.assertNotIn("deliberately", str(latest["raw_evidence"]))

    def test_restart_keeps_newest_mixed_component_health_without_success_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = self.database(root)
            ShadowObservationService(database, self.adapter(FakePublicInfoClient(payloads(positions=[], orders=[]))), shadow_config()).refresh(
                received_at=TIME, attempted_at=TIME,
            )
            mixed_payload = payloads(positions=[])
            mixed_payload["openOrders"] = TimeoutError("open order channel unavailable")
            latest = ShadowObservationService(
                database, self.adapter(FakePublicInfoClient(mixed_payload)), shadow_config(),
            ).refresh(received_at=TIME + timedelta(seconds=2), attempted_at=TIME + timedelta(seconds=1))
            self.assertEqual(latest["state"], "INCOMPLETE")
            self.assertEqual(latest["latest_observation"]["components"]["positions"]["state"], "OBSERVED")
            self.assertEqual(latest["latest_observation"]["components"]["open_orders"]["state"], "INCOMPLETE")
            restarted = CopyTradeDatabase(root / "copy.sqlite3")
            restarted.initialize()
            current = restarted.latest_shadow_observation(
                execution_domain=SHADOW_EXECUTION_DOMAIN,
                execution_account_id=shadow_execution_account_id("hyperliquid", ACCOUNT_A),
            )
            self.assertEqual(current["components"]["open_orders"]["reason"], "open_order_observation_unavailable")

    def test_unsupported_fresh_venue_symbol_is_a_discrepancy_not_a_match(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.database(Path(temp))
            unknown_symbol = payloads(
                positions=[{"position": {"coin": "UNLISTED", "szi": "1"}}],
                orders=[{"oid": "unlisted-order", "coin": "UNLISTED", "side": "B", "sz": "1", "status": "open"}],
            )
            result = ShadowObservationService(
                database, self.adapter(FakePublicInfoClient(unknown_symbol)), shadow_config(),
            ).refresh(received_at=TIME)
            comparison = result["latest_observation"]["comparison"]
            self.assertEqual(comparison["positions"]["items"][0]["state"], "UNSUPPORTED_SYMBOL")
            self.assertEqual(comparison["open_orders"]["items"][0]["state"], "UNSUPPORTED_SYMBOL")

    def test_hostile_payloads_keep_dust_and_fail_closed_without_unbounded_or_nonfinite_provenance(self) -> None:
        dust = self.adapter(FakePublicInfoClient(payloads(
            positions=[{"position": {"coin": "BTC", "szi": "1e-13"}}], orders=[],
        ))).observe_account(ACCOUNT_A, max_age_seconds=60.0, received_at=TIME)
        self.assertEqual(dust.normalized["positions"], [{"symbol": "BTC", "signed_quantity": 1e-13}])

        duplicate = payloads(positions=[
            {"position": {"coin": "BTC", "szi": "1"}}, {"position": {"coin": "BTC", "szi": "-1"}},
        ])
        duplicate_observation = self.adapter(FakePublicInfoClient(duplicate)).observe_account(
            ACCOUNT_A, max_age_seconds=60.0, received_at=TIME,
        )
        self.assertEqual(duplicate_observation.components["positions"]["reason"], "position_symbol_duplicate")
        self.assertFalse(duplicate_observation.components["positions"]["empty"])

        unknown_status = payloads()
        unknown_status["openOrders"]["orders"][0]["status"] = "mystery"
        unknown_order = self.adapter(FakePublicInfoClient(unknown_status)).observe_account(
            ACCOUNT_A, max_age_seconds=60.0, received_at=TIME,
        )
        self.assertEqual(unknown_order.components["open_orders"]["reason"], "open_order_status_unknown")
        self.assertFalse(unknown_order.components["open_orders"]["empty"])

        infinite_balance = payloads()
        infinite_balance["clearinghouseState"]["marginSummary"]["accountValue"] = "Infinity"
        invalid_balance = self.adapter(FakePublicInfoClient(infinite_balance)).observe_account(
            ACCOUNT_A, max_age_seconds=60.0, received_at=TIME,
        )
        self.assertEqual(invalid_balance.components["balances"]["reason"], "numeric_value_non_finite")
        self.assertFalse(invalid_balance.components["balances"]["empty"])

        hostile = payloads()
        hostile["meta"]["untrusted"] = [
            {"text": "x" * 5_000, "nan": float("nan"), "infinity": float("inf")} for _ in range(200)
        ]
        bounded = self.adapter(FakePublicInfoClient(hostile)).observe_account(
            ACCOUNT_A, max_age_seconds=60.0, received_at=TIME,
        )
        serialized = json.dumps(bounded.raw_evidence, allow_nan=False)
        self.assertLess(len(serialized), 300_000)
        self.assertIn("<non_finite_float>", serialized)

    def test_attempt_order_beats_late_commit_and_tied_failure_fails_closed_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = self.database(root)
            scope = shadow_execution_account_id("hyperliquid", ACCOUNT_A)
            started, release = threading.Event(), threading.Event()
            slow_service = ShadowObservationService(
                database,
                GatedShadowAdapter(self.adapter(FakePublicInfoClient(payloads(positions=[], orders=[]))), started, release),
                shadow_config(),
                clock=lambda: TIME,
            )
            worker_errors: list[BaseException] = []

            def slow_refresh() -> None:
                try:
                    slow_service.refresh(received_at=TIME + timedelta(seconds=10), attempted_at=TIME)
                except BaseException as exc:  # pragma: no cover - assertion below makes this diagnostic
                    worker_errors.append(exc)

            worker = threading.Thread(target=slow_refresh)
            worker.start()
            self.assertTrue(started.wait(timeout=1))
            ShadowObservationService(database, FailingShadowAdapter(), shadow_config(), clock=lambda: TIME).refresh(
                received_at=TIME + timedelta(seconds=2), attempted_at=TIME + timedelta(seconds=1),
            )
            release.set()
            worker.join(timeout=1)
            self.assertFalse(worker.is_alive())
            self.assertEqual(worker_errors, [])
            latest = database.latest_shadow_observation(
                execution_domain=SHADOW_EXECUTION_DOMAIN, execution_account_id=scope,
            )
            self.assertEqual(latest["reason"], "shadow_adapter_observation_failed")

            tied_success = self.adapter(FakePublicInfoClient(payloads(positions=[], orders=[]))).observe_account(
                ACCOUNT_A, max_age_seconds=60.0, received_at=TIME + timedelta(seconds=20),
            )
            ShadowObservationService(database, FailingShadowAdapter(), shadow_config(), clock=lambda: TIME).refresh(
                received_at=TIME + timedelta(seconds=3), attempted_at=TIME + timedelta(seconds=2),
            )
            database.record_shadow_observation(tied_success.as_storage_record(
                compare_shadow_observation(database, tied_success), attempted_at=TIME + timedelta(seconds=2),
            ))
            restarted = CopyTradeDatabase(root / "copy.sqlite3")
            restarted.initialize()
            current = restarted.latest_shadow_observation(
                execution_domain=SHADOW_EXECUTION_DOMAIN, execution_account_id=scope,
            )
            self.assertEqual(current["state"], "INCOMPLETE")
            self.assertEqual(current["reason"], "shadow_adapter_observation_failed")

    def test_scope_binding_blocks_cross_account_observation_and_disabled_refresh_reads_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.database(Path(temp))
            foreign_observation = self.adapter(FakePublicInfoClient(payloads())).observe_account(
                ACCOUNT_B, max_age_seconds=60.0, received_at=TIME,
            )
            result = ShadowObservationService(
                database, ForeignAccountShadowAdapter(foreign_observation), shadow_config(), clock=lambda: TIME,
            ).refresh(received_at=TIME, attempted_at=TIME)
            self.assertEqual(result["latest_observation"]["reason"], "shadow_observation_scope_mismatch")
            self.assertIsNone(database.latest_shadow_observation(
                execution_domain=SHADOW_EXECUTION_DOMAIN,
                execution_account_id=shadow_execution_account_id("hyperliquid", ACCOUNT_B),
            ))

            fixture = FakePublicInfoClient(payloads())
            disabled = ShadowObservationService(
                database, self.adapter(fixture), ShadowObservationConfig(), clock=lambda: TIME,
            ).refresh()
            self.assertEqual(disabled["state"], "NOT_CONFIGURED")
            self.assertEqual(fixture.calls, [])

    def test_pre_hardening_shadow_schema_migrates_before_current_order_index(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "copy.sqlite3"
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    """CREATE TABLE phase_d_shadow_observations (
                        observation_id TEXT PRIMARY KEY, execution_domain TEXT NOT NULL,
                        execution_account_id TEXT NOT NULL, venue TEXT NOT NULL, account_id TEXT NOT NULL,
                        state TEXT NOT NULL, freshness TEXT NOT NULL, observed_at TEXT, received_at TEXT NOT NULL,
                        reason TEXT NOT NULL, components_json TEXT NOT NULL DEFAULT '{}',
                        normalized_json TEXT NOT NULL DEFAULT '{}', comparison_json TEXT NOT NULL DEFAULT '{}',
                        raw_evidence_json TEXT NOT NULL DEFAULT '{}'
                    )"""
                )
                connection.execute(
                    """INSERT INTO phase_d_shadow_observations(
                        observation_id, execution_domain, execution_account_id, venue, account_id, state, freshness,
                        observed_at, received_at, reason, components_json, normalized_json, comparison_json, raw_evidence_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        "legacy-d4", SHADOW_EXECUTION_DOMAIN, shadow_execution_account_id("hyperliquid", ACCOUNT_A),
                        "hyperliquid", ACCOUNT_A, "COMPLETE", "FRESH", TIME.isoformat(), TIME.isoformat(),
                        "legacy", "{}", "{}", "{}", "{}",
                    ),
                )
                connection.commit()
            finally:
                connection.close()
            database = CopyTradeDatabase(path)
            database.initialize()
            migrated = database.latest_shadow_observation(
                execution_domain=SHADOW_EXECUTION_DOMAIN,
                execution_account_id=shadow_execution_account_id("hyperliquid", ACCOUNT_A),
            )
            self.assertEqual(migrated["attempted_at"], TIME.isoformat())

    def test_persistence_restart_and_account_domain_isolation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = self.database(root)
            service = ShadowObservationService(database, self.adapter(FakePublicInfoClient(payloads())), shadow_config())
            service.refresh(received_at=TIME)
            restarted = CopyTradeDatabase(root / "copy.sqlite3")
            restarted.initialize()
            account_scope = shadow_execution_account_id("hyperliquid", ACCOUNT_A)
            self.assertIsNotNone(restarted.latest_shadow_observation(
                execution_domain=SHADOW_EXECUTION_DOMAIN, execution_account_id=account_scope,
            ))
            account_b = restarted.shadow_read_model(
                configured=True, venue="hyperliquid", account_id=ACCOUNT_B,
                execution_domain=SHADOW_EXECUTION_DOMAIN,
                execution_account_id=shadow_execution_account_id("hyperliquid", ACCOUNT_B),
            )
            self.assertEqual(account_b["state"], "NOT_YET_OBSERVED")
            self.assertEqual(restarted.phase_d_local_positions(), {})
            self.assertEqual(restarted.execution_read_model()["shadow"]["state"], "NOT_CONFIGURED")

    def test_control_center_exposes_configured_read_only_shadow_and_refreshes_only_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = self.database(root)
            config = CopyTradeConfig(
                artifacts=ArtifactConfig(database_path=root / "copy.sqlite3", obsidian_root=root / "obsidian"),
                shadow_observation=shadow_config(),
            )
            center = CopyControlCenter(
                config, database, shadow_adapter=self.adapter(FakePublicInfoClient(payloads(positions=[], orders=[]))),
            )
            before = center.execution_health()["shadow"]
            self.assertEqual(before["state"], "NOT_YET_OBSERVED")
            refreshed = center.refresh_shadow_observation()
            self.assertTrue(refreshed["read_only"])
            self.assertEqual(refreshed["state"], "COMPLETE")
            self.assertEqual(center.execution_health()["shadow"]["state"], "COMPLETE")

    def test_shadow_agreement_cannot_clear_open_order_or_integrity_authority(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.database(Path(temp))
            simulator = DeterministicExecutionSimulator()
            engine = ExecutionEngine(database, simulator, safety_context=ExecutionSafetyContext())
            simulator.inject_external_order("BTC", 1.0)
            self.assertEqual(engine.verify_flat()["state"], "INCOMPLETE")
            self.assertEqual(engine.reconcile_positions()["state"], "VERIFIED_FLAT")
            ShadowObservationService(database, self.adapter(FakePublicInfoClient(payloads(positions=[], orders=[]))), shadow_config()).refresh(
                received_at=TIME,
            )
            self.assertTrue(database.execution_open_order_reconciliation_unhealthy())
            self.assertEqual(database.execution_read_model()["execution_health"]["state"], "OPEN_ORDER_RECONCILIATION_INCOMPLETE")

        with tempfile.TemporaryDirectory() as temp:
            database = self.database(Path(temp))
            simulator = DeterministicExecutionSimulator([SimulatorPlan("acknowledged")])
            engine = ExecutionEngine(database, simulator, safety_context=ExecutionSafetyContext())
            intent = engine.process_signal(signal("integrity"))
            submission = database.get_execution_submission(intent.intent_id)
            self.assertIsNotNone(submission)
            simulator.emit_fill(submission.client_order_id, 0.6, venue_fill_id="d4-overfill-a")  # type: ignore[union-attr]
            simulator.emit_fill(submission.client_order_id, 0.6, venue_fill_id="d4-overfill-b")  # type: ignore[union-attr]
            self.assertEqual(engine.reconcile_intent(intent.intent_id).state, ExecutionState.RECONCILIATION_REQUIRED)
            ShadowObservationService(database, self.adapter(FakePublicInfoClient(payloads(positions=[], orders=[]))), shadow_config()).refresh(
                received_at=TIME,
            )
            self.assertEqual(engine.reconcile_open_orders()["state"], "MATCHED")
            self.assertTrue(database.execution_safety_health()["integrity_unhealthy"])
            self.assertEqual(database.execution_read_model()["execution_health"]["state"], "INTEGRITY_FAILURE")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
