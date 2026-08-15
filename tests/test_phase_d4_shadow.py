from __future__ import annotations

import tempfile
import unittest
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
    ShadowObservationService,
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
            # The fixture's timestamp is deliberately old relative to the
            # real service receipt clock: Control Center must retain that as
            # current stale evidence rather than fabricate a healthy result.
            self.assertEqual(refreshed["state"], "INCOMPLETE")
            self.assertEqual(center.execution_health()["shadow"]["state"], "INCOMPLETE")

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
